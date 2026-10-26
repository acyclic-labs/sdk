use std::path::{Path, PathBuf};

/// Vendored protoc on Windows cannot open paths with the extended-length
pub(super) fn protoc_path(path: &Path) -> PathBuf {
    acyclic_host_platform::paths::strip_verbatim(path).into_owned()
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
        assert_eq!(protoc_path(&path), PathBuf::from(r"C:\sdk\é\actors.proto"));
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
