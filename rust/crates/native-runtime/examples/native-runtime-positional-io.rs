//! Positional file I/O with an explicit durability boundary.

use acyclic_native_runtime::{Durability, read_at, sync_file, write_all_at};
use std::io;

fn main() -> io::Result<()> {
    let file = tempfile::tempfile()?;
    write_all_at(&file, 0, b"native-io", None)?;
    sync_file(&file, Durability::Full, None)?;
    let mut bytes = [0; 9];
    let read = read_at(&file, 0, &mut bytes)?;
    if read != bytes.len() || &bytes != b"native-io" {
        return Err(io::Error::other(
            "native positional I/O round trip changed bytes",
        ));
    }
    println!("{{\"bytes\":{read},\"round_trip\":true}}");
    Ok(())
}
