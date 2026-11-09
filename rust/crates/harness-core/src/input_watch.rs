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
        let mut roots = BTreeSet::new();
        for path in &self.paths {
            let mut directory = path.parent().unwrap_or(Path::new("."));
            while !directory.is_dir() {
                directory = directory
                    .parent()
                    .ok_or_else(|| Error::Storage("input watch has no existing ancestor".into()))?;
            }
            roots.insert(directory.to_owned());
        }
        let mut held = Vec::new();
        for root in roots {
            let changed = self.changed.clone();
            let paths = self.paths.clone();
            held.push(
                acyclic_fs::hold_root(&root)
                    .map_err(|_| Error::Storage("cannot hold input watch root".into()))?
                    .watch_changes(move |event| {
                        let relevant = match event {
                            Ok(event) if event.kind.is_access() => false,
                            Ok(event) => {
                                event.paths.is_empty()
                                    || event.paths.iter().any(|event| {
                                        let event = if event.is_absolute() {
                                            event.clone()
                                        } else {
                                            root.join(event)
                                        };
                                        paths
                                            .iter()
                                            .any(|path| path == &event || path.starts_with(&event))
                                    })
                            }
                            Err(_) => true,
                        };
                        if relevant {
                            changed.send_modify(|revision| *revision = revision.wrapping_add(1));
                        }
                    })
                    .map_err(|_| Error::Storage("cannot register input watch".into()))?,
            );
        }
        *self
            .held
            .lock()
            .map_err(|_| Error::Storage("input watch lock poisoned".into()))? = held;
        Ok(())
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
