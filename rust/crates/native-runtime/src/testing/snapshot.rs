//! Immutable bytes and permissions only; provider/host caches are never copied.
use crate::private_file;
use std::io::{Read as _, Write as _};
use std::{
    io,
    path::{Path, PathBuf},
};

#[cfg(test)]
mod privacy_tests;

#[cfg(test)]
mod tests;
#[cfg(all(test, windows))]
mod windows_tests;

#[cfg(all(test, windows))]
#[derive(Debug, PartialEq)]
struct PermissionWrite;

/// Independent fixture bytes, permissions and private-file custody metadata.
#[derive(Debug, PartialEq)]
pub struct Snapshot(Vec<Entry>);

#[derive(Debug, PartialEq)]
struct Entry {
    relative: PathBuf,
    bytes: Option<Vec<u8>>,
    permissions: std::fs::Permissions,
    private: bool,
}

fn directory_is_private(path: &Path) -> io::Result<bool> {
    match private_file::validate_directory(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => Ok(false),
        Err(error) => Err(error),
    }
}

fn capture_file(path: &Path) -> io::Result<(Vec<u8>, bool)> {
    // Classify custody and capture bytes through the same held descriptor.
    // std::fs::Permissions alone loses a protected Windows DACL.
    let mut file = std::fs::File::open(path)?;
    let private = private_file::is_private(&file)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok((bytes, private))
}

impl Snapshot {
    /// Captures a quiescent fixture tree without copying provider or host caches.
    ///
    /// # Errors
    /// Returns an I/O error if an entry cannot be read or classified, or if the
    /// tree contains a symlink or an entry other than a regular file or directory.
    pub fn capture(root: &Path) -> io::Result<Self> {
        let mut pending = vec![(PathBuf::new(), std::fs::symlink_metadata(root)?)];
        let mut entries = Vec::new();
        while let Some((relative, metadata)) = pending.pop() {
            let path = root.join(&relative);
            if metadata.is_symlink() || !(metadata.is_dir() || metadata.is_file()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "fixture snapshot accepts only regular files and directories",
                ));
            }
            let (bytes, private) = if metadata.is_dir() {
                let mut children = std::fs::read_dir(&path)?
                    .map(|entry| {
                        let entry = entry?;
                        // Enumeration already supplies the no-follow metadata on
                        // Windows. These independent fixtures have no concurrent writer.
                        Ok((relative.join(entry.file_name()), entry.metadata()?))
                    })
                    .collect::<std::io::Result<Vec<_>>>()?;
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

    /// Restores an independent tree, installing permissions after its descendants.
    ///
    /// # Errors
    /// Returns an I/O error if creating, writing, setting permissions or verifying
    /// the restored tree fails. A partially restored destination may remain.
    ///
    /// # Panics
    /// Panics if the destination already exists or its recaptured bytes,
    /// permissions or private-file custody differ from this snapshot.
    pub fn restore(&self, root: &Path) -> io::Result<()> {
        assert!(!root.exists(), "snapshot destination must be independent");
        for Entry {
            relative,
            bytes,
            permissions: _,
            private,
        } in &self.0
        {
            let path = root.join(relative);
            match bytes {
                Some(bytes) if *private => private_file::create(&path)?.write_all(bytes)?,
                Some(bytes) => std::fs::write(&path, bytes)?,
                None if *private => private_file::create_directory(&path)?,
                None => std::fs::create_dir(&path)?,
            }
        }
        // Install permissions after descendants exist, including readonly directories.
        for Entry {
            relative,
            permissions,
            ..
        } in self.0.iter().rev()
        {
            let path = root.join(relative);
            // Windows creates these independent entries writable. Only the
            // readonly bit needs restoring; avoid opening every writable entry
            // again just to set the attribute it already has.
            #[cfg(windows)]
            if !permissions.readonly() {
                continue;
            }
            #[cfg(all(test, windows))]
            acyclic_test_support::differential::record(PermissionWrite);
            std::fs::set_permissions(path, permissions.clone())?;
        }
        assert_eq!(&Self::capture(root)?, self, "snapshot bytes changed");
        Ok(())
    }
}
