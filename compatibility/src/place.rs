//! How the build script writes into `OUT_DIR`: so that it can always rerun over
//! its own previous output, whatever mode that output was left with.
use std::{
    fs, io,
    path::{Path, PathBuf},
};

/// Writes `bytes` to `to`. They go to a new file beside it, created with the
/// default mode rather than a source's, which then replaces `to`. A previous
/// run's `to` is removed first, never opened: a read-only file cannot be opened
/// for writing on any OS, nor renamed over on Windows, but every OS removes it.
/// `fs::copy` would instead carry a read-only source's mode to `to`, and the
/// next run would then fail with `PermissionDenied`.
pub(crate) fn place(to: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut partial = to.as_os_str().to_owned();
    partial.push(".partial");
    let partial = PathBuf::from(partial);
    fs::write(&partial, bytes)?;
    match fs::remove_file(to) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => fs::rename(&partial, to),
    }
}

#[cfg(test)]
mod tests {
    use super::place;
    use std::{fs, io};

    /// A read-only file left by an earlier run is replaced by a writable one
    /// holding the new bytes, and no partial file is left behind.
    #[test]
    fn replaces_a_read_only_previous_output() -> io::Result<()> {
        let id = std::process::id();
        let dir = std::env::temp_dir().join(format!("acyclic-proto-build-place-{id}"));
        fs::create_dir_all(&dir)?;
        let target = dir.join("protoc");
        fs::write(&target, b"old")?;
        let mut permissions = fs::metadata(&target)?.permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&target, permissions)?;
        place(&target, b"new")?;
        assert_eq!(fs::read(&target)?, b"new");
        assert!(!fs::metadata(&target)?.permissions().readonly());
        assert!(!dir.join("protoc.partial").exists());
        fs::remove_dir_all(&dir)
    }
}
