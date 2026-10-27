use super::*;
pub(super) mod conflicts;
use crate::conversation::MAX_PRIVATE_DIRECTORY_PAGE;

mod page;
pub(crate) use page::DirectoryIndex;
#[doc(hidden)]
pub use page::MemoryStorePage;
#[cfg(test)]
pub(crate) use page::{MemoryStoreEntry, MemoryStoreEntryKind};

impl MemoryStore {
    #[doc(hidden)]
    pub fn generation(&self) -> u64 {
        self.generation
    }

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

    #[doc(hidden)]
    pub fn path_conflicts(&self, path: &str) -> bool {
        conflicts::conflicts(&self.paths, path)
    }

    pub(super) fn retain_path(&mut self, reference: &FileRef) -> Result<()> {
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
            let retained_reference = reference.copy(&mut None)?;
            self.generation = generation;
            if !self.paths.contains_key(path) {
                self.paths.insert(
                    path.to_owned(),
                    PathHistory {
                        revisions: Vec::new(),
                    },
                );
            }
            let history = self
                .paths
                .get_mut(path)
                .ok_or_else(|| Error::Storage("retained path history is missing".into()))?;
            if let Some(slots) = growth {
                let additional = slots - history.revisions.len();
                history.revisions.reserve_exact(additional);
            }
            history.revisions.push(PathRevision {
                generation,
                file: retained_reference,
            });
            self.directory_index.publish(path, generation);
        }
        Ok(())
    }
}
