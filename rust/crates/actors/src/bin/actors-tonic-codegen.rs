//! Maintainer-only prost/tonic generation from the Rust-owned Actors schema.

use std::{env, io, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = env::args_os().nth(1).map(PathBuf::from).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "output directory is required")
    })?;
    let proto_root = output.join("proto");
    let generated_root = output.join("rust");
    std::fs::create_dir_all(&generated_root)?;
    acyclic_actors::wire::render_proto_files(&proto_root)?;
    let proto = proto_root.join("actors/v1/actors.proto");
    let descriptor = output.join("acyclic-actors-v1.bin");
    let mut prost = tonic_prost_build::Config::new();
    prost
        .protoc_executable(protoc_bin_vendored::protoc_bin_path()?)
        .out_dir(&generated_root)
        .file_descriptor_set_path(&descriptor)
        .extern_path(".acyclic.actors.v1", "::acyclic_actors::wire");
    tonic_prost_build::configure()
        .out_dir(&generated_root)
        .build_client(true)
        .build_server(false)
        .build_transport(false)
        .compile_with_config(
            prost,
            &[proto],
            &[proto_root, protoc_bin_vendored::include_path()?],
        )?;
    Ok(())
}
