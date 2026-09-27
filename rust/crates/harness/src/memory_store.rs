//! Shared bounded in-memory content state used by native and WASM Harness hosts.
//!
//! The native filesystem provider remains responsible for durable journals and
//! authenticated provider operations. This small core owns the common
//! ephemeral semantics that must be identical in a local host: immutable file
//! identity, byte deduplication, path heads, generation history, and bounded
//! residency.

#[cfg(any(test, all(feature = "wasm", target_arch = "wasm32")))]
use crate::contract::canonical_json_digest;
#[cfg(any(test, all(feature = "wasm", target_arch = "wasm32")))]
use crate::conversation::FileDescriptor;
use crate::conversation::{FileRef, VolumeRef, is_internal_path};
use crate::{Error, Result};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

#[derive(Clone)]
struct StoredFile {
    reference: FileRef,
    bytes: Arc<[u8]>,
}

#[derive(Clone)]
struct PathRevision {
    generation: u64,
    file: FileRef,
}

/// One entry in a bounded directory page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MemoryStoreEntry {
    pub(crate) name: String,
    pub(crate) kind: MemoryStoreEntryKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MemoryStoreEntryKind {
    File,
    Directory,
}

/// A generation-pinned page produced by the shared content core.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MemoryStorePage {
    pub(crate) generation: u64,
    pub(crate) entries: Vec<MemoryStoreEntry>,
    pub(crate) has_more: bool,
}

/// Bounded owner-private immutable content state.
pub(crate) struct MemoryStore {
    volume: VolumeRef,
    maximum_file_bytes: u64,
    maximum_path_bytes: u64,
    maximum_resident_bytes: u64,
    maximum_resident_files: u64,
    resident_bytes: u64,
    generation: u64,
    files: HashMap<String, StoredFile>,
    blobs: HashMap<String, Arc<[u8]>>,
    paths: BTreeMap<String, Vec<PathRevision>>,
}

impl MemoryStore {
    pub(crate) fn new(
        volume: VolumeRef,
        maximum_file_bytes: u64,
        maximum_path_bytes: u64,
        maximum_resident_bytes: u64,
        maximum_resident_files: u64,
    ) -> Result<Self> {
        volume.validate()?;
        if maximum_resident_files == 0 {
            return Err(Error::Invalid(
                "maximum resident file count must be positive".into(),
            ));
        }
        Ok(Self {
            volume,
            maximum_file_bytes,
            maximum_path_bytes,
            maximum_resident_bytes,
            maximum_resident_files,
            resident_bytes: 0,
            generation: 0,
            files: HashMap::new(),
            blobs: HashMap::new(),
            paths: BTreeMap::new(),
        })
    }

    #[cfg(any(test, all(feature = "wasm", target_arch = "wasm32")))]
    pub(crate) fn stage(
        &mut self,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        update_path: bool,
    ) -> Result<FileRef> {
        if bytes.len() as u64 > self.maximum_file_bytes {
            return Err(Error::Invalid("staged file exceeds harness limits".into()));
        }
        if is_internal_path(path) {
            return Err(Error::Invalid("internal storage paths are reserved".into()));
        }
        if path.len() as u64 > self.maximum_path_bytes {
            return Err(Error::Invalid("staged file exceeds harness limits".into()));
        }
        let descriptor = FileDescriptor::from_bytes(bytes, media_type)?;
        let digest = canonical_json_digest(&(
            &path,
            descriptor.sha256(),
            descriptor.byte_length(),
            descriptor.media_type(),
            &display_name,
        ))?;
        let reference = FileRef::new(
            self.volume.clone(),
            path,
            hex_bytes(&digest),
            descriptor,
            display_name,
        )?;
        self.stage_reference(&reference, bytes, update_path)
    }

    /// Admits a provider-created reference while retaining the same bounded
    /// bytes, path-head, and generation semantics as WASM staging. Native
    /// filesystem refs carry a provider generation in `version`, so the
    /// shared core must retain that exact ref instead of deriving a second
    /// identity for it.
    pub(crate) fn stage_reference(
        &mut self,
        reference: &FileRef,
        bytes: &[u8],
        update_path: bool,
    ) -> Result<FileRef> {
        reference.validate()?;
        if reference.volume() != &self.volume {
            return Err(Error::Unauthorized("file belongs to another volume".into()));
        }
        reference.descriptor().verify(bytes)?;
        let path = reference.path();
        if bytes.len() as u64 > self.maximum_file_bytes {
            return Err(Error::Invalid("staged file exceeds harness limits".into()));
        }
        if is_internal_path(path) {
            return Err(Error::Invalid("internal storage paths are reserved".into()));
        }
        if path.len() as u64 > self.maximum_path_bytes {
            return Err(Error::Invalid("staged file exceeds harness limits".into()));
        }
        let file_key = Self::file_key(reference)?;
        if let Some(prior) = self.files.get(&file_key)
            && (prior.reference != *reference || prior.bytes.as_ref() != bytes)
        {
            return Err(Error::Invalid(
                "immutable file version was reused for another file contract".into(),
            ));
        }
        if self.path_conflicts(path) {
            return Err(Error::Invalid(
                "file path conflicts with an existing directory".into(),
            ));
        }
        if !self.files.contains_key(&file_key) {
            if self.files.len() as u64 >= self.maximum_resident_files {
                return Err(Error::Invalid(
                    "memory content retention limit exceeded".into(),
                ));
            }
            let blob_key = Self::blob_key(reference);
            let resident = if let Some(existing) = self.blobs.get(&blob_key) {
                if existing.as_ref() != bytes {
                    return Err(Error::Invalid(
                        "content digest was reused for different bytes".into(),
                    ));
                }
                existing.clone()
            } else {
                let length = bytes.len() as u64;
                if length
                    > self
                        .maximum_resident_bytes
                        .saturating_sub(self.resident_bytes)
                {
                    return Err(Error::Invalid(
                        "memory content retention limit exceeded".into(),
                    ));
                }
                self.resident_bytes = self.resident_bytes.saturating_add(length);
                let resident: Arc<[u8]> = Arc::from(bytes);
                self.blobs.insert(blob_key, resident.clone());
                resident
            };
            self.files.insert(
                file_key,
                StoredFile {
                    reference: reference.clone(),
                    bytes: resident,
                },
            );
        }
        if update_path {
            let changed = self
                .paths
                .get(path)
                .and_then(|history| history.last())
                .is_none_or(|prior| prior.file != *reference);
            if changed {
                self.generation = self
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("content generation overflow".into()))?;
                self.paths
                    .entry(path.to_owned())
                    .or_default()
                    .push(PathRevision {
                        generation: self.generation,
                        file: reference.clone(),
                    });
            }
        }
        Ok(reference.clone())
    }

    pub(crate) fn read(&self, file: &FileRef) -> Result<Vec<u8>> {
        file.validate()?;
        if file.volume() != &self.volume {
            return Err(Error::Unauthorized("file belongs to another volume".into()));
        }
        let key = Self::file_key(file)?;
        let stored = self
            .files
            .get(&key)
            .filter(|stored| stored.reference == *file)
            .ok_or_else(|| Error::NotFound("owner has no resident file".into()))?;
        file.descriptor().verify(&stored.bytes)?;
        Ok(stored.bytes.to_vec())
    }

    #[cfg(all(feature = "wasm", target_arch = "wasm32"))]
    pub(crate) fn has(&self, file: &FileRef) -> Result<bool> {
        file.validate()?;
        if file.volume() != &self.volume {
            return Ok(false);
        }
        let key = Self::file_key(file)?;
        Ok(self
            .files
            .get(&key)
            .is_some_and(|stored| stored.reference == *file))
    }

    pub(crate) fn path_conflicts(&self, path: &str) -> bool {
        self.paths.keys().any(|existing| {
            existing != path
                && (existing.starts_with(&format!("{path}/"))
                    || path.starts_with(&format!("{existing}/")))
        })
    }

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
        if maximum == 0 || maximum > 4096 {
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
        let names = found
            .keys()
            .filter(|name| after.is_none_or(|cursor| name.as_str() > cursor))
            .cloned()
            .collect::<Vec<_>>();
        let has_more = names.len() > maximum;
        let entries = names
            .into_iter()
            .take(maximum)
            .filter_map(|name| {
                found
                    .get(&name)
                    .copied()
                    .map(|kind| MemoryStoreEntry { kind, name })
            })
            .collect();
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
                    .iter()
                    .rev()
                    .find(|revision| revision.generation <= generation)
            })
            .map(|revision| revision.file.clone())
            .ok_or_else(|| Error::NotFound("owner has no file at this path".into()))
    }

    fn file_key(file: &FileRef) -> Result<String> {
        // Version is the provider's immutable identity. Descriptor and
        // display name remain part of the contract and are checked against
        // an existing entry below, but must not create a second resident copy
        // when the same provider version is retried with altered metadata.
        String::from_utf8(crate::contract::canonical_json_bytes(&(
            file.volume(),
            file.path(),
            file.version(),
        ))?)
        .map_err(|error| Error::Invalid(format!("file reference is not UTF-8: {error}")))
    }

    fn blob_key(file: &FileRef) -> String {
        format!(
            "{}:{}",
            file.descriptor().byte_length(),
            hex_bytes(file.descriptor().sha256())
        )
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AgentId;
    use crate::conversation::{VolumeClass, VolumeOwner};
    use crate::resources::ProviderRef;

    fn store(maximum_bytes: u64, maximum_files: u64) -> Result<MemoryStore> {
        let provider = ProviderRef::new("local", "memory", "2")?;
        let volume = VolumeRef::new(
            provider,
            "test-volume",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::new()),
        )?;
        MemoryStore::new(volume, 4_096, 4_096, maximum_bytes, maximum_files)
    }

    #[test]
    fn stages_deduplicated_bytes_under_file_and_byte_bounds() -> Result<()> {
        let mut store = store(3, 16)?;
        let first = store.stage("notes/one.txt", b"abc", "text/plain", "one.txt", true)?;
        for index in 0..15 {
            let path = format!("notes/copy-{index}.txt");
            let same = store.stage(&path, b"abc", "text/plain", "copy.txt", true)?;
            assert_eq!(first.descriptor(), same.descriptor());
        }
        assert_eq!(store.generation(), 16);
        assert!(
            store
                .stage(
                    "notes/different.txt",
                    b"d",
                    "text/plain",
                    "different.txt",
                    true
                )
                .is_err()
        );
        assert_eq!(store.read(&first)?, b"abc");
        Ok(())
    }

    #[test]
    fn provider_version_rejects_changed_file_contract() -> Result<()> {
        let mut store = store(32, 8)?;
        let first = store.stage("notes/current.txt", b"one", "text/plain", "one.txt", true)?;
        let changed_display = FileRef::new(
            store.volume.clone(),
            first.path(),
            first.version(),
            first.descriptor().clone(),
            "renamed.txt",
        )?;
        assert!(
            store
                .stage_reference(&changed_display, b"one", false)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn path_history_is_generation_pinned_and_rejects_conflicts() -> Result<()> {
        let mut store = store(32, 8)?;
        let first = store.stage("notes/current.txt", b"one", "text/plain", "one.txt", true)?;
        let generation = store.generation();
        let second = store.stage("notes/current.txt", b"two", "text/plain", "two.txt", true)?;
        assert!(store.path_conflicts("notes"));
        assert_eq!(
            store.read_path("notes/current.txt", Some(generation))?,
            first
        );
        assert_eq!(store.read_path("notes/current.txt", None)?, second);
        let page = store.list("notes", Some(generation), None, 16)?;
        assert_eq!(
            page.entries,
            vec![MemoryStoreEntry {
                name: "current.txt".into(),
                kind: MemoryStoreEntryKind::File,
            }]
        );
        Ok(())
    }
}
