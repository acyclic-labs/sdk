//! Immutable bytes and permissions only; provider/host caches are never copied.
use super::*;
use acyclic_native_runtime::private_file;
use std::io::{Read as _, Write as _};

#[cfg(not(acyclic_harness_loom))]
mod privacy_tests;

#[cfg(all(windows, not(acyclic_harness_loom)))]
mod tests;

#[cfg(windows)]
#[derive(Debug, PartialEq)]
struct PermissionWrite;

#[derive(Debug, PartialEq)]
pub(in crate::local) struct Snapshot(Vec<Entry>);

#[derive(Debug, PartialEq)]
struct Entry {
    relative: PathBuf,
    bytes: Option<Vec<u8>>,
    permissions: std::fs::Permissions,
    private: bool,
}

fn directory_is_private(path: &Path) -> Result<bool> {
    match private_file::validate_directory(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => Ok(false),
        Err(error) => Err(storage(error)),
    }
}

fn capture_file(path: &Path) -> Result<(Vec<u8>, bool)> {
    // Classify custody and capture bytes through the same held descriptor.
    // std::fs::Permissions alone loses a protected Windows DACL.
    let mut file = std::fs::File::open(path).map_err(storage)?;
    let private = private_file::is_private(&file).map_err(storage)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(storage)?;
    Ok((bytes, private))
}

impl Snapshot {
    pub fn capture(root: &Path) -> Result<Self> {
        let mut pending = vec![(
            PathBuf::new(),
            std::fs::symlink_metadata(root).map_err(storage)?,
        )];
        let mut entries = Vec::new();
        while let Some((relative, metadata)) = pending.pop() {
            let path = root.join(&relative);
            assert!(!metadata.is_symlink(), "fixture contains a symlink");
            let (bytes, private) = if metadata.is_dir() {
                let mut children = std::fs::read_dir(&path)
                    .map_err(storage)?
                    .map(|entry| {
                        let entry = entry?;
                        // Enumeration already supplies the no-follow metadata on
                        // Windows. These independent fixtures have no concurrent writer.
                        Ok((relative.join(entry.file_name()), entry.metadata()?))
                    })
                    .collect::<std::io::Result<Vec<_>>>()
                    .map_err(storage)?;
                children.sort_by(|(left, _), (right, _)| left.cmp(right));
                pending.extend(children.into_iter().rev());
                (None, directory_is_private(&path)?)
            } else {
                let (bytes, private) = capture_file(&path)?;
                (Some(bytes), private)
            };
            entries.push(Entry {
                relative,
                bytes,
                permissions: metadata.permissions(),
                private,
            });
        }
        Ok(Self(entries))
    }

    pub fn restore(&self, root: &Path) -> Result<()> {
        assert!(!root.exists(), "snapshot destination must be independent");
        for Entry {
            relative,
            bytes,
            permissions,
            private,
        } in &self.0
        {
            let path = root.join(relative);
            match bytes {
                Some(bytes) if *private => private_file::create(&path)
                    .map_err(storage)?
                    .write_all(bytes)
                    .map_err(storage)?,
                Some(bytes) => std::fs::write(&path, bytes).map_err(storage)?,
                None if *private => private_file::create_directory(&path).map_err(storage)?,
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
