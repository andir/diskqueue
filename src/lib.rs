//! (Unordered) queue of items persisted to disk of a given
//! serde-serializable type.
//! The on-disk structure is considered an implementation detail and
//! not a public interface. The data on disk is versioned and an
//! effort is made to not consume incompatible data schemas across
//! versions. Items that can't be consumed (e.g. due to incompatible
//! versions) aren't removed from the queue and might bubble up again.
//!
//! Example usage:
//! ```rust
//!   # let t = tempfile::tempdir().unwrap();
//!   # let path = t.path().join("foo").to_path_buf();
//!   use diskqueue::Queue;
//!   let q: Queue<i32> = Queue::new(path).unwrap();
//!   q.enqueue(123).unwrap();
//!   let v = q.dequeue().unwrap();
//!   assert_eq!(v, Some(123));
//! ```
use fs4::FileExt;
use std::marker::PhantomData;
use thiserror::Error;

/// Exports of [serde] and [serde_json] to allow consumers ensuring
/// that they use a compatible version.
pub use serde;
pub use serde_json;

const ENTRIES_DIRECTORY: &str = "entries";
const TMP_DIRECTORY: &str = "tmp";

#[derive(Debug, Error)]
pub enum Error {
    #[error("The given directory already exists")]
    DirectoryAlreadyExists,
    #[error("The given directory is not a valid queue directory")]
    NotAQueueDirectory,
    #[error("Failed to JSON serialize/deserialize: {0:?}")]
    JSONSerialize(#[from] serde_json::Error),
    #[error("I/O error: {0:?}")]
    IO(#[from] std::io::Error),
    #[error("The data in the queue has an incompatible data format: {0}")]
    IncompatibleQueueDataVersion(u32),
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Instance of a queue returning elements of type `T`.
///
/// Example usage:
/// ```rust
///   # let t = tempfile::tempdir().unwrap();
///   # let path = t.path().join("foo").to_path_buf();
///   use diskqueue::Queue;
///   let q: Queue<i32> = Queue::new(path).unwrap();
///   q.enqueue(123).unwrap();
///   let v = q.dequeue().unwrap();
///   assert_eq!(v, Some(123));
/// ```
pub struct Queue<T> {
    path: std::path::PathBuf,
    _phantom_data: PhantomData<T>,
}

const fn current_version() -> u32 {
    1
}

/// Internal wrapper structure for the data that is being stored on
/// disk. This allows us to reject data that doesn't match our
/// expected data version (e.g. multiple consumers of different
/// versions).
#[derive(serde::Serialize, serde::Deserialize)]
struct Payload<T> {
    version: u32,
    data: T,
}

impl<T> Payload<T> {
    #[inline(always)]
    const fn new(payload: T) -> Self {
        Self {
            version: current_version(),
            data: payload,
        }
    }

    #[inline]
    fn get(self) -> T {
        self.data
    }
}

impl<T> Queue<T>
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    /// Try opening a queue at the given location or create a new
    /// queue directory.
    pub fn open_or_create(path: std::path::PathBuf) -> Result<Self> {
        if let Ok(s) = Self::open(path.clone()) {
            return Ok(s);
        }

        Self::new(path)
    }

    /// Create a new queue directory at the given path. The path must
    /// not exist.
    pub fn new(path: std::path::PathBuf) -> Result<Self> {
        if path.exists() {
            return Err(Error::DirectoryAlreadyExists);
        }

        std::fs::create_dir(&path)?;
        std::fs::create_dir(path.join(TMP_DIRECTORY))?;
        std::fs::create_dir(path.join(ENTRIES_DIRECTORY))?;

        Ok(Self {
            path,
            _phantom_data: Default::default(),
        })
    }

    /// Create a new queue instance for the given path. If the path
    /// doesn't follow the queue directory structure an error is
    /// returned.
    pub fn open(path: std::path::PathBuf) -> Result<Self> {
        if !path.is_dir() {
            return Err(Error::NotAQueueDirectory);
        }

        if !path.join(ENTRIES_DIRECTORY).is_dir() {
            return Err(Error::NotAQueueDirectory);
        }

        if !path.join(TMP_DIRECTORY).is_dir() {
            return Err(Error::NotAQueueDirectory);
        }

        Ok(Self {
            path,
            _phantom_data: Default::default(),
        })
    }

    /// Add the given item to the queue.
    /// Returns the unique id of the item.
    pub fn enqueue(&self, payload: T) -> Result<u64> {
        // We try a couple of times until we have a random "unique"
        // id that doesn't already exist in the temporary directory.
        // Once that file has been written we try to move/rename it
        // into the actual entries directory.
        // FIXME: Is renaming on windows/MacOS atomic? Would locking
        // the file prevent it being consumed before the rename has
        // finished?
        loop {
            let uid = rand::random::<u64>();
            let uids = uid.to_string();
            let path = self.path.join(TMP_DIRECTORY).join(&uids);
            if path.exists() {
                continue;
            }
            let mut fh = match std::fs::File::create(&path) {
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    continue;
                }
                Ok(o) => o,
                Err(e) => return Err(e.into()),
            };
            let payload = Payload::new(payload);
            serde_json::to_writer(&mut fh, &payload)?;

            std::fs::rename(path, self.path.join(ENTRIES_DIRECTORY).join(uids))?;

            return Ok(uid);
        }
    }

    /// Consume a single item from the queue. Returns `Ok(None)` if
    /// there was no item that could be consumed, otherwise returns
    /// `Ok(Some(T))`.  Retries up to 5 times on transient errors
    /// e.g. another process having locked or removed the item
    /// already. If it fails to read five times Ok(None) is returned.
    pub fn dequeue(&self) -> Result<Option<T>> {
        let mut counter = 0;
        loop {
            if counter >= 5 {
                // We failed to read a file more than 5 times, just give up
                return Ok(None);
            }
            counter += 1;
            return match self.dequeue_try_once() {
                Err(Error::IO(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                    continue;
                }
                Err(Error::IO(e)) if e.kind() == fs4::lock_contended_error().kind() => {
                    continue;
                }
                Err(e) => return Err(e),
                Ok(r) => Ok(r),
            };
        }
    }

    /// Tries to dequeue a single item from the queue. Compared to
    /// [Queue::dequeue] this function doesn't retry and might return
    /// transient errors.
    /// Returns `Ok(Some(T))` on success and `Ok(None)` when the queue
    /// is empty.
    pub fn dequeue_try_once(&self) -> Result<Option<T>> {
        let p = self.path.join(ENTRIES_DIRECTORY);
        // FIXME: read_dir currently reads up to some larger amount of
        // data from the kernel. Since we are only interested in one
        // entry we should be using another rust binding or interact
        // with the OS calls directly (hopefully not).
        // For now this is "okay" as I don't expect high-volume users.
        let d = match std::fs::read_dir(&p)?.next() {
            Some(p) => p?.path(),
            None => return Ok(None),
        };

        let mut fh = std::fs::File::open(&d)?;

        // We must lock the file before moving any further. It could
        // be that another process has already consumed it but not yet
        // unliked it.
        // IDEA: Should be unlink before we have finished reading?
        // Crashing readers would then "eat" data but we could move
        // them to some "scratch" location?
        if let Err(e) = fh.try_lock_exclusive() {
            return Err(e.into());
        }
        let data: Payload<T> = serde_json::from_reader(&mut fh)?;

        if data.version != current_version() {
            return Err(Error::IncompatibleQueueDataVersion(data.version));
        }

        std::fs::remove_file(&d)?;

        Ok(Some(data.get()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_dequeue_try_once_new_path() {
        let dir = tempfile::tempdir().unwrap();
        let queue: Queue<u32> = Queue::new(dir.path().join("dir").to_path_buf()).unwrap();
        let id1 = queue.enqueue(32).unwrap();
        let id2 = queue.enqueue(128).unwrap();
        assert_ne!(id1, id2);

        let e1 = queue.dequeue_try_once().unwrap();
        let e2 = queue.dequeue_try_once().unwrap();
        assert!([e1, e2].contains(&Some(32)));
        assert!([e1, e2].contains(&Some(128)));
        let e3 = queue.dequeue_try_once().unwrap();
        assert_eq!(e3, None);
    }

    #[test]
    fn queue_dequeue_new_path() {
        let dir = tempfile::tempdir().unwrap();
        let queue: Queue<String> = Queue::new(dir.path().join("dir").to_path_buf()).unwrap();

        assert_eq!(queue.dequeue().ok(), Some(None));

        queue.enqueue("foo".to_owned()).unwrap();
        queue.enqueue("bar".to_owned()).unwrap();

        let e1 = queue.dequeue().unwrap();
        let e2 = queue.dequeue().unwrap();

        assert!([&e1, &e2].contains(&&Some("foo".to_owned())));
        assert!([&e1, &e2].contains(&&Some("bar".to_owned())));

        assert_eq!(queue.dequeue().ok(), Some(None));
    }

    #[test]
    fn queue_dequeue_already_locked() {
        let dir = tempfile::tempdir().unwrap();
        let queue: Queue<String> = Queue::new(dir.path().join("dir").to_path_buf()).unwrap();

        assert_eq!(queue.dequeue().ok(), Some(None));

        let id = queue.enqueue("foo".to_owned()).unwrap();

        let p = dir
            .path()
            .join("dir")
            .join(ENTRIES_DIRECTORY)
            .join(&format!("{}", id));
        let fh = std::fs::File::open(p).unwrap();
        fh.lock_exclusive().unwrap();

        let e1 = queue.dequeue().unwrap();
        assert_eq!(e1, None);
        drop(fh);
        let e1 = queue.dequeue().unwrap();
        assert_eq!(e1, Some("foo".to_string()));
    }
}
