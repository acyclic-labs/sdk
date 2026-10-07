//! Emit the descriptor produced by the Rust Actors build script.
use std::io::{self, Write};

fn main() -> io::Result<()> {
    io::stdout().write_all(acyclic_actors::FILE_DESCRIPTOR_SET)
}
