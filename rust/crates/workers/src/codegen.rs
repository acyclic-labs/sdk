//! Maintained vendored protoc and tonic consume the Rust-rendered schema.
use std::{fs, io, path::Path};
pub fn generate(output: impl AsRef<Path>) -> io::Result<()> {
    let output = output.as_ref();
    let proto = output.join("proto");
    let rust = output.join("rust");
    fs::create_dir_all(&rust)?;
    crate::contract::render_proto_files(&proto)?;
    let mut config = tonic_prost_build::Config::new();
    config
        .protoc_executable(protoc_bin_vendored::protoc_bin_path().map_err(io::Error::other)?)
        .out_dir(&rust)
        .file_descriptor_set_path(output.join("acyclic-workers-v1.bin"))
        .extern_path(".acyclic.workers.v1", "crate::wire");
    tonic_prost_build::configure()
        .out_dir(&rust)
        .build_client(true)
        .build_server(true)
        .build_transport(false)
        .emit_rerun_if_changed(false)
        .server_mod_attribute(".", "#[cfg(not(target_arch = \"wasm32\"))]")
        .compile_with_config(
            config,
            &[proto.join("workers/v1/workers.proto")],
            &[
                proto,
                protoc_bin_vendored::include_path().map_err(io::Error::other)?,
            ],
        )
        .map_err(io::Error::other)?;
    if std::env::var("CARGO_CFG_TARGET_ARCH").map_err(io::Error::other)? != "wasm32" {
        let transport = rust.join("acyclic.workers.v1.rs");
        let observed = acyclic_grpc_observability::codegen::observe_clients(
            &fs::read_to_string(&transport)?,
            "workers",
        )?;
        fs::write(transport, observed)?;
    }
    Ok(())
}
