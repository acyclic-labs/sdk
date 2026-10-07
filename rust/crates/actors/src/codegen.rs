//! Shared maintainer-side schema and tonic generation.

use std::{fs, io, path::Path};

/// Generate the canonical Actors proto, descriptor, and transport facade.
pub fn generate(output_root: impl AsRef<Path>) -> io::Result<()> {
    let output_root = output_root.as_ref();
    let proto_root = output_root.join("proto");
    let rust_root = output_root.join("rust");
    fs::create_dir_all(&rust_root)?;
    crate::contract::render_proto_files(&proto_root)?;

    let proto = proto_root.join("actors/v1/actors.proto");
    let descriptor = output_root.join("acyclic-actors-v1.bin");
    let mut prost = tonic_prost_build::Config::new();
    prost
        .protoc_executable(protoc_bin_vendored::protoc_bin_path().map_err(io::Error::other)?)
        .out_dir(&rust_root)
        .file_descriptor_set_path(&descriptor)
        .extern_path(".acyclic.actors.v1", "crate::wire");
    tonic_prost_build::configure()
        .out_dir(&rust_root)
        .build_client(true)
        .build_server(true)
        .build_transport(false)
        .emit_rerun_if_changed(false)
        .server_mod_attribute(".", "#[cfg(not(target_arch = \"wasm32\"))]")
        .compile_with_config(
            prost,
            &[proto],
            &[
                proto_root,
                protoc_bin_vendored::include_path().map_err(io::Error::other)?,
            ],
        )
        .map_err(|error| io::Error::other(format!("Actors tonic generation failed: {error}")))?;
    Ok(())
}
