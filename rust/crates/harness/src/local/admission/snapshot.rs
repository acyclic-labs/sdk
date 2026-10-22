//! Immutable bytes and permissions only; provider/host caches are never copied.
use super::*;

#[cfg(all(windows, not(acyclic_harness_loom)))]
mod tests;

#[cfg(windows)]
#[derive(Debug, PartialEq)]
struct PermissionWrite;

#[derive(Debug, PartialEq)]
pub(in crate::local) struct Snapshot(Vec<(PathBuf, Option<Vec<u8>>, std::fs::Permissions)>);

impl Snapshot {
    pub fn capture(root: &Path) -> Result<Self> {
        let mut pending = vec![PathBuf::new()];
        let mut entries = Vec::new();
        while let Some(relative) = pending.pop() {
            let path = root.join(&relative);
            let metadata = std::fs::symlink_metadata(&path).map_err(storage)?;
            assert!(!metadata.is_symlink(), "fixture contains a symlink");
            let bytes = if metadata.is_dir() {
                let mut children = std::fs::read_dir(&path)
                    .map_err(storage)?
                    .map(|entry| entry.map(|entry| relative.join(entry.file_name())))
                    .collect::<std::io::Result<Vec<_>>>()
                    .map_err(storage)?;
                children.sort();
                pending.extend(children.into_iter().rev());
                None
            } else {
                Some(std::fs::read(path).map_err(storage)?)
            };
            entries.push((relative, bytes, metadata.permissions()));
        }
        Ok(Self(entries))
    }

    pub fn restore(&self, root: &Path) -> Result<()> {
        assert!(!root.exists(), "snapshot destination must be independent");
        for (relative, bytes, permissions) in &self.0 {
            let path = root.join(relative);
            match bytes {
                Some(bytes) => std::fs::write(&path, bytes).map_err(storage)?,
                None => std::fs::create_dir(&path).map_err(storage)?,
            }
            // Windows creates these independent entries writable. Only the
            // readonly bit needs restoring; avoid opening every writable entry
            // again just to set the attribute it already has.
            #[cfg(windows)]
            if !permissions.readonly() {
                continue;
            }
            #[cfg(windows)]
            acyclic_test_support::differential::record(PermissionWrite);
            std::fs::set_permissions(path, permissions.clone()).map_err(storage)?;
        }
        assert_eq!(&Self::capture(root)?, self, "snapshot bytes changed");
        Ok(())
    }
}
