//! Example that queues a text message on disk and reads them again

use clap::Parser;
use diskqueue::Queue;

#[derive(Parser, serde::Serialize, serde::Deserialize)]
struct Message {
    text: String,
}

#[derive(Parser)]
enum Command {
    Queue(Message),
    Dequeue,
}

#[derive(Parser)]
struct Arguments {
    queue_directory: std::path::PathBuf,
    #[clap(subcommand)]
    command: Command,
}

fn main() {
    let args = Arguments::parse();

    let queue = Queue::open_or_create(args.queue_directory).unwrap();

    match args.command {
        Command::Dequeue => {
            let message: Option<Message> = queue.dequeue().unwrap();
            match message {
                None => println!("No messages."),
                Some(m) => println!("Message: {}", m.text),
            }
        }
        Command::Queue(message) => {
            queue.enqueue(message).unwrap();
        }
    }
}
