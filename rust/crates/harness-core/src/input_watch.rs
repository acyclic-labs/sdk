//! Host input invalidation over the shared native directory watch service.
use crate::{Error, Result};
use acyclic_auth::credential::{CredentialCacheError, CredentialValidity, CredentialWatch};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Mutex,
};

/// A source set's revision. Registrations share native roots across consumers;
/// readers subscribe before observing and reread only after an event.
pub struct InputWatch {
    paths: Vec<PathBuf>,
    changed: tokio::sync::watch::Sender<u64>,
    held: Mutex<Vec<acyclic_fs::HeldRootWatch>>,
}

impl InputWatch {
    /// Watch files, including their creation when their directories are absent.
    pub fn new(paths: Vec<PathBuf>) -> Result<Self> {
        acyclic_fs::watch::initialize_native_watch_services()
            .map_err(|_| Error::Storage("cannot initialize input watch service".into()))?;
        let paths = paths
            .into_iter()
            .map(std::path::absolute)
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(|_| Error::Storage("cannot resolve input watch path".into()))?;
        let watch = Self {
            paths,
            changed: tokio::sync::watch::channel(0).0,
            held: Mutex::new(Vec::new()),
        };
        watch.arm()?;
        Ok(watch)
    }

    /// Rearm after invalidation so newly created directories become watched.
    pub fn arm(&self) -> Result<()> {
        let mut directories = Vec::new();
        let mut aliases = BTreeMap::<PathBuf, BTreeSet<PathBuf>>::new();
        for path in &self.paths {
            let mut directory = path.parent().unwrap_or(Path::new("."));
            while !directory.is_dir() {
                directory = directory
                    .parent()
                    .ok_or_else(|| Error::Storage("input watch has no existing ancestor".into()))?;
            }
            // Configured host inputs may use directory aliases. Watch each
            // alias's entry before resolving it, so a retarget racing discovery
            // invalidates the observation and the next arm binds its new target.
            for ancestor in directory.ancestors() {
                if std::fs::symlink_metadata(ancestor)
                    .map_err(|_| Error::Storage("cannot inspect input watch ancestor".into()))?
                    .file_type()
                    .is_symlink()
                {
                    let parent = ancestor
                        .parent()
                        .ok_or_else(|| Error::Storage("input watch alias has no parent".into()))?;
                    aliases
                        .entry(parent.canonicalize().map_err(|_| {
                            Error::Storage("cannot resolve input watch alias parent".into())
                        })?)
                        .or_default()
                        .insert(PathBuf::from(ancestor.file_name().ok_or_else(|| {
                            Error::Storage("input watch alias has no name".into())
                        })?));
                }
            }
            directories.push((directory, path));
        }
        let mut held = Vec::new();
        for (root, paths) in aliases {
            held.push(self.hold(root, paths)?);
        }
        let mut roots = BTreeMap::<PathBuf, BTreeSet<PathBuf>>::new();
        for (directory, path) in directories {
            roots
                .entry(
                    directory
                        .canonicalize()
                        .map_err(|_| Error::Storage("cannot resolve input watch root".into()))?,
                )
                .or_default()
                .insert(
                    path.strip_prefix(directory)
                        .map_err(|_| {
                            Error::Storage("input watch path is outside its ancestor".into())
                        })?
                        .to_owned(),
                );
        }
        for (root, paths) in roots {
            held.push(self.hold(root, paths)?);
        }
        *self
            .held
            .lock()
            .map_err(|_| Error::Storage("input watch lock poisoned".into()))? = held;
        Ok(())
    }

    fn hold(&self, root: PathBuf, paths: BTreeSet<PathBuf>) -> Result<acyclic_fs::HeldRootWatch> {
        let changed = self.changed.clone();
        acyclic_fs::hold_root(&root)
            .map_err(|_| Error::Storage("cannot hold input watch root".into()))?
            .watch_changes(move |event| {
                let relevant = match event {
                    Ok(event) if event.kind.is_access() => false,
                    Ok(event) => {
                        event.paths.is_empty()
                            || event.paths.iter().any(|event| {
                                let event = event.strip_prefix(&root).unwrap_or(event);
                                paths
                                    .iter()
                                    .any(|path| path == event || path.starts_with(event))
                            })
                    }
                    Err(_) => true,
                };
                if relevant {
                    changed.send_modify(|revision| *revision = revision.wrapping_add(1));
                }
            })
            .map_err(|_| Error::Storage("cannot register input watch".into()))
    }

    /// Current event revision, without reading any input.
    pub fn revision(&self) -> u64 {
        *self.changed.borrow()
    }

    /// Explicit lifecycle mutations use the same event as native file writes.
    pub fn invalidate(&self) {
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    /// One-shot Auth watch, registered before the caller reads credentials.
    pub fn credential_watch(&self) -> CredentialWatch {
        let mut changed = self.changed.subscribe();
        let mut revision = [0; 32];
        revision[..8].copy_from_slice(&changed.borrow_and_update().to_be_bytes());
        CredentialWatch::from_custodian(
            CredentialValidity {
                now_unix_seconds: 0,
                revision: Some(revision.into()),
            },
            async move {
                changed
                    .changed()
                    .await
                    .map_err(|_| CredentialCacheError::Unavailable)
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn aliased_inputs_report_target_writes_and_alias_retargets() -> Result<()> {
        acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
        for retarget in [false, true] {
            let home = tempfile::tempdir().map_err(Error::process_io)?;
            let target = home.path().join("target");
            let replacement = home.path().join("replacement");
            for directory in [&target, &replacement] {
                std::fs::create_dir(directory).map_err(Error::process_io)?;
                std::fs::write(directory.join("auth.json"), b"old").map_err(Error::process_io)?;
            }
            let alias = home.path().join("alias");
            let link = |target: &Path| {
                #[cfg(unix)]
                let result = std::os::unix::fs::symlink(target, &alias);
                #[cfg(windows)]
                let result = std::os::windows::fs::symlink_dir(target, &alias);
                result.map_err(Error::process_io)
            };
            link(&target)?;
            let watch = InputWatch::new(vec![alias.join("auth.json")])?;
            let changed = watch.credential_watch();
            if retarget {
                #[cfg(unix)]
                std::fs::remove_file(&alias).map_err(Error::process_io)?;
                #[cfg(windows)]
                std::fs::remove_dir(&alias).map_err(Error::process_io)?;
                link(&replacement)?;
            } else {
                // The notification names the target directory, whereas the
                // caller configured its alias. It must reach the same source.
                std::fs::write(target.join("auth.json"), b"new").map_err(Error::process_io)?;
            }
            changed
                .await
                .map_err(|_| Error::Storage("input watch closed".into()))?;
            assert_ne!(watch.revision(), 0);
        }
        Ok(())
    }
}
