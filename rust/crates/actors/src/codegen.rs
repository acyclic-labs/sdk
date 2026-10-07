//! Shared maintainer-side schema and tonic generation.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

/// Vendored protoc on Windows cannot open drive paths with the extended-length
/// `\\?\` prefix that Cargo may place in `OUT_DIR`. Keep the long-path form
/// for filesystem operations, but pass a normal drive path to protoc.
fn protoc_path(path: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        let value = path.as_os_str().to_string_lossy();
        if let Some(stripped) = value.strip_prefix("\\\\?\\")
            && stripped.as_bytes().get(1) == Some(&b':')
        {
            return PathBuf::from(stripped);
        }
    }
    path.to_path_buf()
}

/// Generate the canonical Actors proto, descriptor, and transport facade.
pub fn generate(output_root: impl AsRef<Path>) -> io::Result<()> {
    let output_root = output_root.as_ref();
    let proto_root = protoc_path(&output_root.join("proto"));
    let rust_root = protoc_path(&output_root.join("rust"));
    fs::create_dir_all(&rust_root)?;
    crate::contract::render_proto_files(&proto_root)?;

    let proto = proto_root.join("actors/v1/actors.proto");
    let descriptor = protoc_path(&output_root.join("acyclic-actors-v1.bin"));
    let protoc = protoc_path(&protoc_bin_vendored::protoc_bin_path().map_err(io::Error::other)?);
    let include_path = protoc_path(&protoc_bin_vendored::include_path().map_err(io::Error::other)?);
    let mut prost = tonic_prost_build::Config::new();
    prost
        .protoc_executable(protoc)
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
        .compile_with_config(prost, &[proto], &[proto_root, include_path])
        .map_err(|error| io::Error::other(format!("Actors tonic generation failed: {error}")))?;
    Ok(())
}
