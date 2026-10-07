//! Shared maintainer-side schema and tonic generation.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

/// Vendored protoc on Windows cannot open paths with the extended-length
/// `\\?\` prefix that Cargo may place in `OUT_DIR`. Keep the long-path form
/// for filesystem operations, but pass a normal drive or UNC path to protoc.
fn protoc_path(path: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        use std::ffi::OsString;
        use std::os::windows::ffi::{OsStrExt, OsStringExt};

        let value: Vec<u16> = path.as_os_str().encode_wide().collect();
        let prefix: Vec<u16> = "\\\\?\\".encode_utf16().collect();
        if let Some(stripped) = value.strip_prefix(prefix.as_slice()) {
            if stripped.get(1) == Some(&(':' as u16)) {
                return PathBuf::from(OsString::from_wide(stripped));
            }
            let unc_prefix: Vec<u16> = "UNC\\".encode_utf16().collect();
            if let Some(server_share_path) = stripped.strip_prefix(unc_prefix.as_slice()) {
                let mut normal = "\\\\".encode_utf16().collect::<Vec<_>>();
                normal.extend_from_slice(server_share_path);
                return PathBuf::from(OsString::from_wide(&normal));
            }
        }
    }
    path.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::protoc_path;
    use std::path::PathBuf;

    #[test]
    fn leaves_ordinary_paths_unchanged() {
        let path = PathBuf::from("Q:/sdk/proto/actors.proto");
        assert_eq!(protoc_path(&path), path);
    }

    #[cfg(windows)]
    #[test]
    fn removes_verbatim_drive_prefix_without_losing_unicode() {
        let path = PathBuf::from(r"\\?\C:\sdk\é\actors.proto");
        assert_eq!(
            protoc_path(&path),
            PathBuf::from(r"C:\sdk\é\actors.proto")
        );
    }

    #[cfg(windows)]
    #[test]
    fn converts_verbatim_unc_to_normal_unc() {
        let path = PathBuf::from(r"\\?\UNC\server\share\é\actors.proto");
        assert_eq!(
            protoc_path(&path),
            PathBuf::from(r"\\server\share\é\actors.proto")
        );
    }

    #[cfg(windows)]
    #[test]
    fn preserves_device_namespace_paths() {
        let path = PathBuf::from(r"\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy1\actors.proto");
        assert_eq!(protoc_path(&path), path);
    }
}

/// Generate the canonical Actors proto, descriptor, and transport facade.
pub fn generate(output_root: impl AsRef<Path>) -> io::Result<()> {
    let output_root = output_root.as_ref();
    let proto_root = output_root.join("proto");
    let rust_root = output_root.join("rust");
    fs::create_dir_all(&rust_root)?;
    crate::contract::render_proto_files(&proto_root)?;

    let proto = proto_root.join("actors/v1/actors.proto");
    let descriptor = protoc_path(&output_root.join("acyclic-actors-v1.bin"));
    let protoc_proto_root = protoc_path(&proto_root);
    let protoc_rust_root = protoc_path(&rust_root);
    let protoc_proto = protoc_path(&proto);
    let protoc = protoc_path(&protoc_bin_vendored::protoc_bin_path().map_err(io::Error::other)?);
    let include_path = protoc_path(&protoc_bin_vendored::include_path().map_err(io::Error::other)?);
    let mut prost = tonic_prost_build::Config::new();
    prost
        .protoc_executable(protoc)
        .out_dir(&protoc_rust_root)
        .file_descriptor_set_path(&descriptor)
        .extern_path(".acyclic.actors.v1", "crate::wire");
    tonic_prost_build::configure()
        .out_dir(&protoc_rust_root)
        .build_client(true)
        .build_server(true)
        .build_transport(false)
        .emit_rerun_if_changed(false)
        .server_mod_attribute(".", "#[cfg(not(target_arch = \"wasm32\"))]")
        .compile_with_config(prost, &[protoc_proto], &[protoc_proto_root, include_path])
        .map_err(|error| io::Error::other(format!("Actors tonic generation failed: {error}")))?;
    Ok(())
}
