//! Checked cleanup of directories owned by a native workspace host.
use std::{fs, io, path::Path};

/// Removes a direct child, rejecting paths and symlinks outside the managed parent.
pub fn remove_child_directory(root: &Path, target: &Path) -> io::Result<()> {
    remove_child_directory_with_probe(root, target, Path::try_exists)
}

fn remove_child_directory_with_probe(
    root: &Path,
    target: &Path,
    probe: impl FnOnce(&Path) -> io::Result<bool>,
) -> io::Result<()> {
    let root = root.canonicalize()?;
    let parent = target
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "discard target has no parent"))?
        .canonicalize()?;
    if parent != root {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "discard target is outside the managed workspace root",
        ));
    }
    match probe(target) {
        Ok(true) => {}
        Ok(false) => return Ok(()),
        #[cfg(windows)]
        Err(error) if error.raw_os_error() == Some(369) => return Ok(()),
        Err(error) => return Err(error),
    }
    let target = match target.canonicalize() {
        Ok(target) => target,
        #[cfg(windows)]
        Err(error) if error.raw_os_error() == Some(369) => return Ok(()),
        Err(error) => return Err(error),
    };
    if target.parent() != Some(root.as_path()) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "discard target is outside the managed workspace root",
        ));
    }
    match fs::remove_dir_all(target) {
        Ok(()) => Ok(()),
        #[cfg(windows)]
        Err(error) if error.raw_os_error() == Some(369) => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(windows)]
    #[test]
    fn cleanup_accepts_detached_projfs_probe_without_hiding_other_errors() -> io::Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("managed");
        let child = root.join("child");
        fs::create_dir_all(&child)?;
        remove_child_directory_with_probe(&root, &child, |path| {
            assert_eq!(path, child);
            Err(io::Error::from_raw_os_error(369))
        })?;
        assert!(child.is_dir());
        let error = remove_child_directory_with_probe(&root, &child, |_| {
            Err(io::Error::from_raw_os_error(5))
        })
        .expect_err("access denial must not be treated as a detached directory");
        assert_eq!(error.raw_os_error(), Some(5));
        assert!(child.is_dir());
        let foreign = temp.path().join("foreign");
        fs::create_dir(&foreign)?;
        let error = remove_child_directory_with_probe(&root, &foreign, |_| {
            panic!("foreign paths must be rejected before probing")
        })
        .expect_err("foreign cleanup must fail");
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(foreign.is_dir());
        Ok(())
    }

    #[test]
    fn cleanup_refuses_foreign_children() -> io::Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("managed");
        let child = root.join("child");
        let foreign = temp.path().join("foreign");
        fs::create_dir_all(&child)?;
        fs::create_dir(&foreign)?;
        assert_eq!(
            remove_child_directory(&root, &foreign)
                .err()
                .ok_or_else(|| io::Error::other("foreign cleanup unexpectedly succeeded"))?
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(foreign.is_dir());
        remove_child_directory(&root, &child)?;
        assert!(!child.exists());
        remove_child_directory(&root, &child)?;
        Ok(())
    }
    #[cfg(unix)]
    #[test]
    fn cleanup_refuses_symlinks_to_foreign_directories() -> io::Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("managed");
        let foreign = temp.path().join("foreign");
        fs::create_dir(&root)?;
        fs::create_dir(&foreign)?;
        let link = root.join("link");
        std::os::unix::fs::symlink(&foreign, &link)?;
        assert_eq!(
            remove_child_directory(&root, &link)
                .err()
                .ok_or_else(|| io::Error::other("symlink cleanup unexpectedly succeeded"))?
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(foreign.is_dir());
        Ok(())
    }
}
