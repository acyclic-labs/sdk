//! Shared maintainer-side schema and tonic generation.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

#[path = "codegen/path.rs"]
mod path;
use path::protoc_path;

/// Generate the Actors descriptor and transport from the packaged proto.
pub fn generate(output_root: impl AsRef<Path>) -> io::Result<()> {
    let output_root = output_root.as_ref();
    let proto_root = PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR")
            .ok_or_else(|| io::Error::other("CARGO_MANIFEST_DIR is unset"))?,
    )
    .join("proto");
    let rust_root = output_root.join("rust");
    fs::create_dir_all(&rust_root)?;

    let proto = proto_root.join("actors/v1/actors.proto");
    let descriptor = protoc_path(&output_root.join("acyclic-actors-v1.bin"));
    let protoc_proto_root = protoc_path(&proto_root);
    let protoc_rust_root = protoc_path(&rust_root);
    let protoc_proto = protoc_path(&proto);
    let protoc = protoc_path(&protoc_bin_vendored::protoc_bin_path().map_err(io::Error::other)?);
    let include_path = protoc_path(&protoc_bin_vendored::include_path().map_err(io::Error::other)?);
    let mut prost =
        acyclic_proto_build::config().map_err(|error| io::Error::other(error.to_string()))?;
    prost
        .protoc_executable(protoc)
        .out_dir(&protoc_rust_root)
        .file_descriptor_set_path(&descriptor)
        .extern_path(".acyclic.actors.v1", "crate::wire");
    acyclic_proto_build::actor_services()
        .out_dir(&protoc_rust_root)
        .compile_with_config(prost, &[protoc_proto], &[protoc_proto_root, include_path])
        .map_err(|error| io::Error::other(format!("Actors tonic generation failed: {error}")))?;
    fs::write(
        &descriptor,
        acyclic_proto_build::canonicalize(&fs::read(&descriptor)?, &["actors/v1/actors.proto"])
            .map_err(|error| io::Error::other(error.to_string()))?,
    )?;
    if std::env::var("CARGO_CFG_TARGET_ARCH").map_err(io::Error::other)? != "wasm32" {
        let transport = rust_root.join("acyclic.actors.v1.rs");
        let observed = acyclic_grpc_observability::codegen::observe_clients(
            &fs::read_to_string(&transport)?,
            "actors",
        )?;
        fs::write(transport, observed)?;
    }
    Ok(())
}
