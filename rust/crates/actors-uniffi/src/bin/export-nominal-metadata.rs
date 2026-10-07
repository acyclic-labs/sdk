//! Emit binding configuration from the Rust-owned nominal metadata table.

use std::{env, fs, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("uniffi.nominal.toml"));
    fs::write(&output, acyclic_actors_uniffi::nominal_metadata_toml())?;
    println!("wrote {}", output.display());
    Ok(())
}
