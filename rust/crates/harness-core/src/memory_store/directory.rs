use super::*;
pub(super) mod conflicts;
#[cfg(any(test, all(feature = "wasm", target_arch = "wasm32")))]
use crate::conversation::MAX_PRIVATE_DIRECTORY_PAGE;
pub(super) use conflicts::paths_conflict;

#[cfg(any(test, all(feature = "wasm", target_arch = "wasm32")))]
mod page;
#[cfg(any(test, all(feature = "wasm", target_arch = "wasm32")))]
pub(crate) use page::DirectoryIndex;
#[cfg(any(test, all(feature = "wasm", target_arch = "wasm32")))]
#[doc(hidden)]
pub use page::MemoryStorePage;
#[cfg(test)]
pub(crate) use page::{MemoryStoreEntry, MemoryStoreEntryKind};

impl MemoryStore {
    #[cfg(any(test, all(feature = "wasm", target_arch = "wasm32")))]
    #[doc(hidden)]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    #[cfg(any(test, all(feature = "wasm", target_arch = "wasm32")))]
    #[doc(hidden)]
    pub fn list(
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
        Ok(self
            .directory_index
            .page(path, generation, after, maximum, |_| {}))
    }

    #[doc(hidden)]
    pub fn read_path(&self, path: &str, generation: Option<u64>) -> Result<FileRef> {
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

    #[cfg(any(test, all(feature = "wasm", target_arch = "wasm32")))]
    #[doc(hidden)]
    pub fn path_conflicts(&self, path: &str) -> bool {
        conflicts::conflicts(&self.paths, path)
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
            self.admit_path_growth(path, growth, io)?;
            let retained_reference =
                crate::executor::with_history_io(io, |work| reference.copy(work))?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let custody = native_cache_custody(
                io,
                crate::filesystem::UploadRecord::logical_owned_bytes(&retained_reference)?,
            )?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let key_custody = if self.paths.contains_key(path) {
                None
            } else {
                native_cache_custody(io, path.len() as u64)?
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
                        _key_custody: key_custody,
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
                _custody: custody,
            });
            #[cfg(any(test, all(feature = "wasm", target_arch = "wasm32")))]
            self.directory_index.publish(path, generation);
        }
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    fn admit_path_growth(
        &self,
        path: &str,
        growth: Option<usize>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
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
        Ok(())
    }
}
