//! Emit the canonical Actors protobuf rendered by the Rust contract.

use std::{
    error::Error,
    fs,
    io::{self, Write},
};

fn main() -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!("acyclic-actors-contract-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    acyclic_actors::contract::render_proto_files(&root)?;
    let proto = fs::read(root.join("actors/v1/actors.proto"))?;
    fs::remove_dir_all(root)?;
    io::stdout().write_all(&proto)?;
    Ok(())
}
