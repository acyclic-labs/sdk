#![allow(missing_docs, reason = "contract example binary, not public API")]

use acyclic_native_runtime::{Durability, read_at, sync_file, write_all_at};
use std::fs::OpenOptions;
use std::io;

fn main() -> io::Result<()> {
    let path =
        std::env::temp_dir().join(format!("acyclic-native-runtime-{}.tmp", std::process::id()));
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&path)?;
    write_all_at(&file, 0, b"native-io")?;
    sync_file(&file, Durability::Full)?;
    let mut bytes = [0; 9];
    let read = read_at(&file, 0, &mut bytes)?;
    drop(file);
    let _ = std::fs::remove_file(path);
    if read != bytes.len() || &bytes != b"native-io" {
        return Err(io::Error::other(
            "native positional I/O round trip changed bytes",
        ));
    }
    println!("{{\"bytes\":{},\"round_trip\":true}}", read);
    Ok(())
}
