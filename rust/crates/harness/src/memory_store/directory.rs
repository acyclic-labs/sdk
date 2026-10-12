use super::*;
use crate::conversation::MAX_PRIVATE_DIRECTORY_PAGE;

/// One entry in a bounded directory page.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub(crate) struct MemoryStoreEntry {
    pub(crate) name: String,
    pub(crate) kind: MemoryStoreEntryKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum MemoryStoreEntryKind {
    File,
    Directory,
}

/// A generation-pinned page produced by the shared content core.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MemoryStorePage {
    pub(crate) generation: u64,
    pub(crate) entries: Vec<MemoryStoreEntry>,
    pub(crate) has_more: bool,
}

impl MemoryStore {
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn list(
        &self,
        path: &str,
        generation: Option<u64>,
        after: Option<&str>,
        maximum: usize,
    ) -> Result<MemoryStorePage> {
        if maximum == 0 || maximum as u64 > MAX_PRIVATE_DIRECTORY_PAGE as u64 {
            return Err(Error::Invalid(
                "private directory page limit is invalid".into(),
            ));
        }
        let generation = generation.unwrap_or(self.generation);
        if generation > self.generation {
            return Err(Error::Invalid(
                "private directory generation is unavailable".into(),
            ));
        }
        if after.is_some_and(|value| value.contains('/')) {
            return Err(Error::Invalid(
                "directory cursor must name one entry".into(),
            ));
        }
        let prefix = if path.is_empty() {
            String::new()
        } else {
            format!("{path}/")
        };
        let mut found = BTreeMap::<String, MemoryStoreEntryKind>::new();
        for (name, history) in &self.paths {
            if history
                .revisions
                .first()
                .is_some_and(|revision| revision.generation > generation)
                || !name.starts_with(&prefix)
            {
                continue;
            }
            let Some(remainder) = name.strip_prefix(&prefix) else {
                continue;
            };
            let segment = remainder.split('/').next().unwrap_or_default();
            if segment.is_empty() || (path.is_empty() && segment == ".system") {
                continue;
            }
            let kind = if remainder.contains('/') {
                MemoryStoreEntryKind::Directory
            } else {
                MemoryStoreEntryKind::File
            };
            found.entry(segment.to_owned()).or_insert(kind);
        }
        let mut names = found
            .into_iter()
            .filter(|(name, _)| after.is_none_or(|cursor| name.as_str() > cursor));
        let entries = names
            .by_ref()
            .take(maximum)
            .map(|(name, kind)| MemoryStoreEntry { name, kind })
            .collect();
        let has_more = names.next().is_some();
        Ok(MemoryStorePage {
            generation,
            entries,
            has_more,
        })
    }

    pub(crate) fn read_path(&self, path: &str, generation: Option<u64>) -> Result<FileRef> {
        let generation = generation.unwrap_or(self.generation);
        if generation > self.generation {
            return Err(Error::Invalid(
                "private directory generation is unavailable".into(),
            ));
        }
        self.paths
            .get(path)
            .and_then(|history| {
                history
                    .revisions
                    .iter()
                    .rev()
                    .find(|revision| revision.generation <= generation)
            })
            .map(|revision| revision.file.clone())
            .ok_or_else(|| Error::NotFound("owner has no file at this path".into()))
    }

    pub(crate) fn path_conflicts(&self, path: &str) -> bool {
        self.paths
            .keys()
            .any(|existing| paths_conflict(existing, path))
    }

    pub(super) fn retain_path(
        &mut self,
        reference: &FileRef,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        let _ = io;
        let path = reference.path();
        let changed = self
            .paths
            .get(path)
            .and_then(|history| history.revisions.last())
            .is_none_or(|prior| prior.file != *reference);
        if changed {
            let generation = self
                .generation
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("content generation overflow".into()))?;
            let growth = match self.paths.get(path) {
                None => Some(4),
                Some(history) if history.revisions.capacity() == history.revisions.len() => Some(
                    history
                        .revisions
                        .capacity()
                        .checked_mul(2)
                        .ok_or_else(|| Error::Invalid("path history capacity overflow".into()))?
                        .max(4),
                ),
                _ => None,
            };
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                let history = self.paths.get(path);
                if let Some(slots) = growth {
                    let bytes = slots
                        .checked_mul(std::mem::size_of::<PathRevision>())
                        .ok_or_else(|| Error::Invalid("path history ownership overflow".into()))?
                        as u64;
                    io.retain_owned(bytes)?;
                    io.allocation_attempt(bytes)?;
                }
                if history.is_none() {
                    io.retain_owned(path.len() as u64)?;
                    io.allocation_attempt(path.len() as u64)?;
                    io.preflight(acyclic_fs::WorkCounters {
                        bytes_copied: path.len() as u64,
                        ..Default::default()
                    })?;
                }
            }
            let retained_reference =
                crate::executor::with_history_io(io, |work| reference.copy(work))?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let custody = native_cache_custody(
                io,
                crate::filesystem::UploadRecord::logical_owned_bytes(&retained_reference)?,
            )?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let key_custody = if !self.paths.contains_key(path) {
                native_cache_custody(io, path.len() as u64)?
            } else {
                None
            };
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let capacity_custody = match growth {
                Some(slots) => {
                    native_cache_custody(io, (slots * std::mem::size_of::<PathRevision>()) as u64)?
                }
                None => None,
            };
            self.generation = generation;
            if !self.paths.contains_key(path) {
                self.paths.insert(
                    path.to_owned(),
                    PathHistory {
                        revisions: Vec::new(),
                        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                        capacity_custody: None,
                        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                        key_custody,
                    },
                );
                #[cfg(feature = "filesystem")]
                if let Some(io) = io {
                    io.charge(acyclic_fs::WorkCounters {
                        bytes_copied: path.len() as u64,
                        ..Default::default()
                    })?;
                }
            }
            let history = self
                .paths
                .get_mut(path)
                .ok_or_else(|| Error::Storage("retained path history is missing".into()))?;
            if let Some(slots) = growth {
                let additional = slots - history.revisions.len();
                history.revisions.reserve_exact(additional);
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                {
                    history.capacity_custody = capacity_custody;
                }
            }
            history.revisions.push(PathRevision {
                generation,
                file: retained_reference,
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                custody,
            });
        }
        Ok(())
    }
}

pub(super) fn paths_conflict(existing: &str, path: &str) -> bool {
    existing != path
        && (existing
            .strip_prefix(path)
            .is_some_and(|suffix| suffix.starts_with('/'))
            || path
                .strip_prefix(existing)
                .is_some_and(|suffix| suffix.starts_with('/')))
}
