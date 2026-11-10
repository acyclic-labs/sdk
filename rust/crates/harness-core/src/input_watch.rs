//! Host input invalidation over the shared native directory watch service.
use crate::{Error, Result};
use acyclic_auth::credential::{CredentialCacheError, CredentialValidity, CredentialWatch};
use std::{
    collections::BTreeSet,
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
        self.arm_after_hold(|_| Ok(()))
    }

    fn arm_after_hold(&self, mut after_hold: impl FnMut(&Path) -> Result<()>) -> Result<()> {
        let mut held = Vec::new();
        let mut registered = BTreeSet::new();
        let mut pending = self.paths.clone();
        let mut resolved = BTreeSet::new();
        while let Some(path) = pending.pop() {
            let mut directory = path.parent().unwrap_or(Path::new("."));
            while !directory.is_dir() {
                directory = directory
                    .parent()
                    .ok_or_else(|| Error::Storage("input watch has no existing ancestor".into()))?;
            }
            // Bind readable ancestor entries before resolving descendants.
            // Keep parent and self-change registrations on either side of
            // searchable ordinary ancestors that cannot themselves be opened.
            let ancestors: Vec<_> = directory.ancestors().collect();
            let entries = ancestors
                .into_iter()
                .rev()
                .filter(|entry| entry.parent().is_some())
                .chain(std::iter::once(path.as_path()));
            let mut identity = None;
            for entry in entries {
                let alias = entry != path
                    && std::fs::symlink_metadata(entry)
                        .map_err(|_| Error::Storage("cannot inspect input watch ancestor".into()))?
                        .file_type()
                        .is_symlink();
                // The shared Darwin FSEvents hub already reports root and
                // ancestor moves. Registering ordinary parents there expands
                // recursive coverage unnecessarily (up to the filesystem root).
                if cfg!(target_os = "macos") && entry != path && !alias {
                    continue;
                }
                let parent = if entry == path {
                    directory
                } else {
                    entry
                        .parent()
                        .ok_or_else(|| Error::Storage("input watch entry has no parent".into()))?
                };
                let root = parent
                    .canonicalize()
                    .map_err(|_| Error::Storage("cannot resolve input watch root".into()))?;
                let relative = entry
                    .strip_prefix(parent)
                    .map_err(|_| Error::Storage("input watch path is outside its ancestor".into()))?
                    .to_owned();
                identity = Some(root.join(&relative));
                let registration = (root.clone(), relative.clone());
                if registered.insert(registration.clone()) {
                    match self.hold(root, BTreeSet::from([relative])) {
                        Ok(watch) => held.push(watch),
                        Err(acyclic_fs::demand::DemandError::Io(error))
                            if entry != path
                                && !alias
                                && error.kind() == std::io::ErrorKind::PermissionDenied =>
                        {
                            registered.remove(&registration);
                            continue;
                        }
                        Err(_) => return Err(Error::Storage("cannot register input watch".into())),
                    }
                    after_hold(entry)?;
                }
            }
            let identity =
                identity.ok_or_else(|| Error::Storage("input watch has no entry".into()))?;
            if !resolved.insert(identity) {
                continue;
            }
            // The alias entry is now held. A retarget after this read remains
            // an invalidation; callers subscribe before arming and observing.
            match std::fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    let target = std::fs::read_link(&path)
                        .map_err(|_| Error::Storage("cannot read input watch alias".into()))?;
                    pending.push(if target.is_absolute() {
                        target
                    } else {
                        path.parent().unwrap_or(Path::new(".")).join(target)
                    });
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err(Error::Storage("cannot inspect input watch path".into())),
            }
        }
        *self
            .held
            .lock()
            .map_err(|_| Error::Storage("input watch lock poisoned".into()))? = held;
        Ok(())
    }

    fn hold(
        &self,
        root: PathBuf,
        paths: BTreeSet<PathBuf>,
    ) -> std::result::Result<acyclic_fs::HeldRootWatch, acyclic_fs::demand::DemandError> {
        let changed = self.changed.clone();
        acyclic_fs::hold_root(&root)?.watch_changes(move |event| {
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
        for kind in [0, 1, 2] {
            for retarget in [false, true] {
                let home = tempfile::tempdir().map_err(Error::process_io)?;
                let target = home.path().join("target");
                let replacement = home.path().join("replacement");
                for directory in [&target, &replacement] {
                    std::fs::create_dir(directory).map_err(Error::process_io)?;
                    std::fs::write(directory.join("auth.json"), b"old")
                        .map_err(Error::process_io)?;
                }
                let alias = home.path().join("alias");
                let intermediate = home.path().join("intermediate");
                let link = |target: &Path, alias: &Path, file: bool| {
                    #[cfg(unix)]
                    let result = {
                        let _ = file;
                        std::os::unix::fs::symlink(target, alias)
                    };
                    #[cfg(windows)]
                    let result = if file {
                        std::os::windows::fs::symlink_file(target, alias)
                    } else {
                        std::os::windows::fs::symlink_dir(target, alias)
                    };
                    result.map_err(Error::process_io)
                };
                if kind == 2 {
                    link(&target.join("auth.json"), &intermediate, true)?;
                    link(Path::new("intermediate"), &alias, true)?;
                } else {
                    let selected = if kind == 0 {
                        target.clone()
                    } else {
                        target.join("auth.json")
                    };
                    link(&selected, &alias, kind != 0)?;
                }
                let configured = if kind == 0 {
                    alias.join("auth.json")
                } else {
                    alias.clone()
                };
                let watch = InputWatch::new(vec![configured])?;
                let changed = watch.credential_watch();
                if retarget {
                    let selected = if kind == 2 { &intermediate } else { &alias };
                    #[cfg(unix)]
                    std::fs::remove_file(selected).map_err(Error::process_io)?;
                    #[cfg(windows)]
                    if kind == 0 {
                        std::fs::remove_dir(selected)
                    } else {
                        std::fs::remove_file(selected)
                    }
                    .map_err(Error::process_io)?;
                    let replacement = if kind == 0 {
                        replacement
                    } else {
                        replacement.join("auth.json")
                    };
                    link(&replacement, selected, kind != 0)?;
                } else {
                    std::fs::write(target.join("auth.json"), b"new").map_err(Error::process_io)?;
                }
                changed
                    .await
                    .map_err(|_| Error::Storage("input watch closed".into()))?;
                assert_ne!(watch.revision(), 0, "alias kind={kind} retarget={retarget}");
            }
        }
        Ok(())
    }
    #[tokio::test]
    async fn startup_alias_retarget_binds_the_target_after_registration() -> Result<()> {
        acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
        let home = tempfile::tempdir().map_err(Error::process_io)?;
        let alias = home.path().join("settings");
        let old = home.path().join("old");
        let new = home.path().join("new");
        std::fs::write(&old, b"old").map_err(Error::process_io)?;
        std::fs::write(&new, b"new").map_err(Error::process_io)?;
        let link = |target: &Path| {
            #[cfg(unix)]
            let result = std::os::unix::fs::symlink(target, &alias);
            #[cfg(windows)]
            let result = std::os::windows::fs::symlink_file(target, &alias);
            result.map_err(Error::process_io)
        };
        link(&old)?;
        let watch = InputWatch {
            paths: vec![alias.clone()],
            changed: tokio::sync::watch::channel(0).0,
            held: Mutex::new(Vec::new()),
        };
        let mut registered = Vec::new();
        watch.arm_after_hold(|path| {
            registered.push(path.to_owned());
            if path == alias {
                std::fs::remove_file(&alias).map_err(Error::process_io)?;
                link(&new)?;
            }
            Ok(())
        })?;
        assert!(
            registered.contains(&new),
            "startup retained the pre-registration target"
        );
        assert!(!registered.contains(&old));
        let changed = watch.credential_watch();
        std::fs::write(&new, b"changed").map_err(Error::process_io)?;
        changed
            .await
            .map_err(|_| Error::Storage("input watch closed".into()))?;
        Ok(())
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn searchable_ancestors_do_not_need_directory_read_permission() -> Result<()> {
        use std::os::unix::fs::PermissionsExt as _;
        acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
        let home = tempfile::tempdir().map_err(Error::process_io)?;
        let shared = home.path().join("shared");
        let own = shared.join("own");
        std::fs::create_dir_all(&own).map_err(Error::process_io)?;
        let settings = own.join("settings.json");
        std::fs::write(&settings, b"old").map_err(Error::process_io)?;
        std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o111))
            .map_err(Error::process_io)?;
        if std::fs::read_dir(&shared).is_ok() {
            use std::os::unix::process::CommandExt as _;
            std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o700))
                .map_err(Error::process_io)?;
            if std::env::var_os("ACYCLIC_INPUT_WATCH_PERMISSION_CHILD").is_some() {
                return Err(Error::Storage(
                    "unprivileged permission control still bypasses directory permissions".into(),
                ));
            }
            // Privileged runners must exercise the same real filesystem denial,
            // rather than skip it or claim that chmod revoked their privilege.
            let status = std::process::Command::new(
                std::env::current_exe().map_err(Error::process_io)?,
            )
            .args([
                "--exact",
                "input_watch::tests::searchable_ancestors_do_not_need_directory_read_permission",
                "--nocapture",
            ])
            .env("ACYCLIC_INPUT_WATCH_PERMISSION_CHILD", "1")
            .uid(65534)
            .gid(65534)
            .status()
            .map_err(Error::process_io)?;
            assert!(
                status.success(),
                "unprivileged native input watch control failed"
            );
            return Ok(());
        }
        let observed = async {
            assert_eq!(
                std::fs::read_dir(&shared).err().map(|error| error.kind()),
                Some(std::io::ErrorKind::PermissionDenied)
            );
            let watch = InputWatch::new(vec![settings.clone()])?;
            assert_eq!(std::fs::read(&settings).map_err(Error::process_io)?, b"old");
            let changed = watch.credential_watch();
            std::fs::write(&settings, b"new").map_err(Error::process_io)?;
            changed
                .await
                .map_err(|_| Error::Storage("input watch closed".into()))
        }
        .await;
        std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o700))
            .map_err(Error::process_io)?;
        observed
    }
    #[tokio::test]
    async fn replaced_ordinary_ancestor_invalidates_the_original_input() -> Result<()> {
        acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
        let home = tempfile::tempdir().map_err(Error::process_io)?;
        let current = home.path().join("current");
        let replacement = home.path().join("replacement");
        for directory in [&current, &replacement] {
            std::fs::create_dir_all(directory.join("codex")).map_err(Error::process_io)?;
            std::fs::write(directory.join("codex/config.toml"), b"settings")
                .map_err(Error::process_io)?;
        }
        let watch = InputWatch::new(vec![current.join("codex/config.toml")])?;
        let changed = watch.credential_watch();
        std::fs::rename(&current, home.path().join("old")).map_err(Error::process_io)?;
        std::fs::rename(&replacement, &current).map_err(Error::process_io)?;
        changed
            .await
            .map_err(|_| Error::Storage("input watch closed".into()))?;
        watch.arm()?;
        let changed = watch.credential_watch();
        std::fs::write(current.join("codex/config.toml"), b"new").map_err(Error::process_io)?;
        changed
            .await
            .map_err(|_| Error::Storage("input watch closed".into()))?;
        Ok(())
    }
}
