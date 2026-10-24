//! Credentials are private at creation, including the staging pathname.
#[cfg(test)]
use std::fs::File;
use std::{io, path::Path};

pub(crate) fn create(
    parent: &Path,
) -> io::Result<acyclic_native_runtime::private_file::StagedFile> {
    acyclic_native_runtime::private_file::StagedFile::create(parent, ".codex-")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    #[test]
    fn credentials_are_private_before_writing_and_after_replacement() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("auth.json");
        std::fs::write(&path, b"old")?;
        let mut staged = create(root.path())?;
        assert!(private(staged.as_file())?);
        staged.as_file_mut().write_all(b"fixture credentials")?;
        assert!(private(staged.as_file())?);
        staged.publish(&path, acyclic_native_runtime::RenameMode::Replace)?;
        assert!(private(&File::open(&path)?)?);
        Ok(())
    }
    #[test]
    fn negative_control_inherited_permissions_fail_the_private_check() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("inherited.json");
        let mut file = File::create(&path)?;
        file.write_all(b"fixture credentials")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            file.set_permissions(std::fs::Permissions::from_mode(0o644))?;
        }
        assert!(!private(&file)?);
        Ok(())
    }
    #[cfg(unix)]
    fn private(file: &File) -> io::Result<bool> {
        use std::os::unix::fs::PermissionsExt as _;
        Ok(file.metadata()?.permissions().mode() & 0o777 == 0o600)
    }
    #[cfg(windows)]
    use acyclic_native_runtime::private_file::is_private as private;
}
