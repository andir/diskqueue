let
  sources = import ./npins;
  pkgs = import sources.nixpkgs { overlays = [ (self: super: {
    npins = self.callPackage sources.npins { };
  }) ]; };

in pkgs.mkShell {
  nativeBuildInputs = [
    pkgs.npins
    pkgs.rustc
    pkgs.cargo
    pkgs.rust-analyzer
    pkgs.rustfmt
    pkgs.clippy
  ];
}
