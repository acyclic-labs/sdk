//! Build protobuf bindings from each published crate's own protocol sources.

use std::path::PathBuf;

mod descriptor;
pub use descriptor::canonicalize;

pub use tonic_prost_build::{Builder, Config};

/// Descriptor roots and retained wire identities, in original generation order.
pub const FAMILIES: &[(&str, &str, &[&str])] = &[
    ("auth", "auth-v1.bin", &["proto/auth/v1/auth.proto"]),
    ("actors", "actors-v1.bin", &["proto/actors/v1/actors.proto"]),
    (
        "filesystem",
        "filesystem-v2.bin",
        &["proto/filesystem/v2/filesystem.proto"],
    ),
    (
        "inference",
        "inference-v1.bin",
        &["proto/inference/v1/inference.proto"],
    ),
    (
        "machines",
        "machines-v1.bin",
        &["proto/machines/v1/machines.proto"],
    ),
    (
        "objects",
        "objects-v1.bin",
        &["proto/objects/v1/objects.proto"],
    ),
    (
        "stream",
        "stream-v1.bin",
        &[
            "proto/stream/v1/stream.proto",
            "proto/stream/v1/session.proto",
        ],
    ),
];

/// Configure the pinned compiler and the public Objects metadata map shape.
///
/// # Errors
/// Returns compiler discovery and Cargo output directory errors.
pub fn config() -> Result<Config, Box<dyn std::error::Error>> {
    let mut config = Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    config.btree_map([".acyclic.objects.v1.ObjectMetadata.user"]);
    config.protoc_arg("--retain_options");
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
pub fn compile(
    mut config: Config,
    services: Builder,
    protos: &[&str],
    descriptor: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let include = protoc_bin_vendored::include_path()?;
    let manifest =
        PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").ok_or("CARGO_MANIFEST_DIR is unset")?);
    let output =
        PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR is unset")?).join(descriptor);
    config.file_descriptor_set_path(&output);
    let paths: Vec<_> = protos.iter().map(|path| manifest.join(path)).collect();
    services.compile_with_config(config, &paths, &[manifest.join("proto"), include])?;
    std::fs::write(&output, canonicalize(&std::fs::read(&output)?, protos)?)?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}

/// Regenerate a complete descriptor closure without building any SDK crate.
///
/// # Errors
/// Returns compiler, descriptor and filesystem errors.
pub fn descriptor(
    manifest: &std::path::Path,
    protos: &[&str],
    output: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = config()?;
    config.file_descriptor_set_path(output);
    let paths: Vec<_> = protos.iter().map(|path| manifest.join(path)).collect();
    config.load_fds(
        &paths,
        &[manifest.join("proto"), protoc_bin_vendored::include_path()?],
    )?;
    std::fs::write(output, canonicalize(&std::fs::read(output)?, protos)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
