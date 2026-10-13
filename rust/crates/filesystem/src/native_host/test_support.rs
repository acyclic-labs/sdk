use super::*;

pub(super) fn test_root(path: &Path) -> io::Result<HostRoot> {
    HostRoot::open(path, &mut ordinary_work()).map_err(ordinary_error)
}
#[cfg(unix)]
pub(super) fn test_path(path: &str) -> &Path {
    Path::new(path)
}
#[cfg(windows)]
pub(super) fn test_path(path: &str) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt as _;
    OsStr::new(path).encode_wide().collect()
}
