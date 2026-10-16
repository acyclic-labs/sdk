//! Build protobuf bindings from each published crate's own protocol sources.

use std::path::{Path, PathBuf};

pub use tonic_prost_build::{Builder, Config};

/// Configure the pinned compiler and the public Objects metadata map shape.
///
/// # Errors
/// Returns compiler discovery and Cargo output directory errors.
pub fn config(descriptor: &str) -> Result<Config, Box<dyn std::error::Error>> {
    let mut config = Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    config.btree_map([".acyclic.objects.v1.ObjectMetadata.user"]);
    config.file_descriptor_set_path(PathBuf::from(std::env::var("OUT_DIR")?).join(descriptor));
    Ok(config)
}

/// Preserve the public service client/server shapes when transport is enabled.
#[must_use]
pub fn services(enabled: bool) -> Builder {
    tonic_prost_build::configure()
        .build_client(enabled)
        .build_server(enabled)
        .emit_rerun_if_changed(false)
}

/// Actors shares semantic wire aliases and exposes servers only on native hosts.
#[must_use]
pub fn actor_services() -> Builder {
    services(true)
        .build_transport(false)
        .server_mod_attribute(".", "#[cfg(not(target_arch = \"wasm32\"))]")
}

/// Compile messages and selected services; only protocol changes rerun codegen.
///
/// # Errors
/// Returns filesystem and protobuf compilation errors.
pub fn compile(config: Config, services: Builder, protos: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    let include = protoc_bin_vendored::include_path()?;
    let protos: Vec<_> = protos.iter().map(Path::new).collect();
    services.compile_with_config(config, &protos, &[Path::new("proto"), include.as_path()])?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
