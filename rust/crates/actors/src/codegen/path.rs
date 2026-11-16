use std::path::{Path, PathBuf};

/// Vendored protoc on Windows cannot open paths with the extended-length
pub(super) fn protoc_path(path: &Path) -> PathBuf {
    acyclic_host_platform::paths::strip_verbatim(path).into_owned()
}
