//! Shared bounded in-memory content state used by native and WASM Harness hosts.

#[cfg(any(test, all(feature = "wasm", target_arch = "wasm32")))]
use crate::contract::canonical_json_digest;
#[cfg(any(test, all(feature = "wasm", target_arch = "wasm32")))]
use crate::conversation::FileDescriptor;
use crate::conversation::{FileRef, MAX_PRIVATE_DIRECTORY_PAGE, VolumeRef, is_internal_path};
use crate::{Error, Result};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

struct StoredFile {
    reference: FileRef,
    bytes: Arc<[u8]>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    custody: Option<acyclic_fs::NativeAllocation>,
}

struct PathRevision {
    generation: u64,
    file: FileRef,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    custody: Option<acyclic_fs::NativeAllocation>,
}

struct StoredBlob {
    bytes: Arc<[u8]>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    custody: Option<acyclic_fs::NativeAllocation>,
}

#[derive(Default)]
struct PathHistory {
    revisions: Vec<PathRevision>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    capacity_custody: Option<acyclic_fs::NativeAllocation>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    key_custody: Option<acyclic_fs::NativeAllocation>,
}

impl std::ops::Deref for PathHistory {
    type Target = Vec<PathRevision>;
    fn deref(&self) -> &Self::Target { &self.revisions }
}

impl std::ops::DerefMut for PathHistory {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.revisions }
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
    blobs: HashMap<String, StoredBlob>,
    paths: BTreeMap<String, PathHistory>,
}

impl MemoryStore {
    pub(crate) fn new(
        volume: VolumeRef,
        maximum_file_bytes: u64,
        maximum_path_bytes: u64,
        maximum_resident_bytes: u64,
        maximum_resident_files: u64,
    ) -> Result<Self> {
        volume.validate(&mut None)?;
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

    /// Admits a provider-created reference while retaining the same bounded bytes, path-head, and generation semantics as WASM staging.
    pub(crate) fn stage_reference(
        &mut self,
        reference: &FileRef,
        bytes: &[u8],
        update_path: bool,
    ) -> Result<FileRef> {
        self.retain_reference(reference, bytes, update_path)?;
        Ok(reference.clone())
    }

    pub(crate) fn retain_reference(
        &mut self, reference: &FileRef, bytes: &[u8], update_path: bool,
    ) -> Result<()> {
        #[cfg(feature = "filesystem")]
        { self.retain_reference_inner(reference, bytes, update_path, None) }
        #[cfg(not(feature = "filesystem"))]
        { self.retain_reference_inner(reference, bytes, update_path) }
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn retain_reference_with_io(
        &mut self, reference: &FileRef, bytes: &[u8], update_path: bool,
        io: &crate::filesystem::PreparationIo,
    ) -> Result<()> {
        self.retain_reference_inner(reference, bytes, update_path, Some(io))
    }

    fn retain_reference_inner(
        &mut self, reference: &FileRef, bytes: &[u8], update_path: bool,
        #[cfg(feature = "filesystem")] io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<()> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io { io.remaining()?; }
        reference.validate(&mut None)?;
        if reference.volume() != &self.volume {
            return Err(Error::Unauthorized("file belongs to another volume".into()));
        }
        if bytes.len() as u64 != reference.descriptor().byte_length() {
            reference.descriptor().verify(bytes, &mut None)?;
        }
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.preflight(acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64, ..Default::default() })?;
        }
        let verified = reference.descriptor().verify(bytes, &mut None);
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.charge(acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64, ..Default::default() })?;
        }
        verified?;
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
        #[cfg(feature = "filesystem")]
        let file_key = match io {
            Some(io) => {
                let encoded = io.encode_json(
                    &(reference.volume(), reference.path(), reference.version()), io.remaining()?.bytes_encoded, true,
                )?;
                let encoded_hold = io.guard_bytes(&encoded)?;
                let text = std::str::from_utf8(encoded.as_ref())
                    .map_err(|error| Error::Invalid(format!("file reference is not UTF-8: {error}")))?;
                let copy = acyclic_fs::WorkCounters { bytes_copied: text.len() as u64, ..Default::default() };
                io.preflight(copy)?;
                io.claim(text.len() as u64)?;
                io.allocation_attempt(text.len() as u64)?;
                let mut key = String::new();
                key.try_reserve_exact(text.len()).map_err(Error::Allocation)?;
                io.claim((key.capacity() - text.len()) as u64)?;
                key.push_str(text);
                io.charge(copy)?;
                drop(encoded); drop(encoded_hold);
                key
            },
            None => Self::file_key(reference)?,
        };
        #[cfg(not(feature = "filesystem"))]
        let file_key = Self::file_key(reference)?;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            let examine = acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() };
            io.preflight(examine)?;
            io.charge(examine)?;
        }
        if let Some(prior) = self.files.get(&file_key) {
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                let compare = acyclic_fs::WorkCounters { source_bytes_read: bytes.len() as u64, ..Default::default() };
                io.preflight(compare)?;
                io.charge(compare)?;
            }
            if prior.reference != *reference || prior.bytes.as_ref() != bytes {
                return Err(Error::Invalid("immutable file version was reused for another file contract".into()));
            }
        }
        let mut conflict = false;
        for existing in self.paths.keys() {
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                let examine = acyclic_fs::WorkCounters {
                    items_examined: 1, source_bytes_read: (existing.len() + path.len()) as u64, ..Default::default()
                };
                io.preflight(examine)?;
                io.charge(examine)?;
            }
            if paths_conflict(existing, path) { conflict = true; break; }
        }
        if conflict {
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
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                let key_bytes = decimal_digits(reference.descriptor().byte_length()) + 1 + 64;
                io.claim(key_bytes)?;
                let rehash = if self.files.len() == self.files.capacity() { self.files.len() } else { 0 };
                let slots = acyclic_fs::WorkCounters { items_examined: rehash as u64 + 1, ..Default::default() };
                io.preflight(slots)?;
                io.charge(slots)?;
                io.allocation_attempt(key_bytes)?;
                io.preflight(acyclic_fs::WorkCounters { bytes_encoded: key_bytes, bytes_copied: key_bytes, ..Default::default() })?;
            }
            let blob_key = Self::blob_key(reference);
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                io.charge(acyclic_fs::WorkCounters { bytes_encoded: blob_key.len() as u64, bytes_copied: blob_key.len() as u64, ..Default::default() })?;
            }
            let resident = if let Some(existing) = self.blobs.get(&blob_key) {
                #[cfg(feature = "filesystem")]
                if let Some(io) = io {
                    let compare = acyclic_fs::WorkCounters { source_bytes_read: bytes.len() as u64, items_examined: 1, ..Default::default() };
                    io.preflight(compare)?;
                    io.charge(compare)?;
                }
                if existing.bytes.as_ref() != bytes {
                    return Err(Error::Invalid(
                        "content digest was reused for different bytes".into(),
                    ));
                }
                existing.bytes.clone()
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
                #[cfg(feature = "filesystem")]
                if let Some(io) = io {
                    io.claim(length)?;
                    let copy = acyclic_fs::WorkCounters { bytes_copied: length, items_examined: 1, ..Default::default() };
                    io.preflight(copy)?;
                    let rehash = if self.blobs.len() == self.blobs.capacity() { self.blobs.len() } else { 0 };
                    let slots = acyclic_fs::WorkCounters { items_examined: rehash as u64, ..Default::default() };
                    io.preflight(slots)?;
                    io.charge(slots)?;
                }
                #[cfg(feature = "filesystem")]
                if let Some(io) = io {
                    io.allocation_attempt(length)?;
                }
                let resident: Arc<[u8]> = Arc::from(bytes);
                #[cfg(feature = "filesystem")]
                if let Some(io) = io {
                    io.charge(acyclic_fs::WorkCounters { bytes_copied: length, items_examined: 1, ..Default::default() })?;
                }
                self.resident_bytes = self.resident_bytes.saturating_add(length);
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let custody = native_cache_custody(io, length + blob_key.len() as u64)?;
                self.blobs.insert(blob_key, StoredBlob {
                    bytes: resident.clone(),
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    custody,
                });
                resident
            };
            #[cfg(feature = "filesystem")]
            let retained_reference = clone_reference_with_io(reference, io)?;
            #[cfg(not(feature = "filesystem"))]
            let retained_reference = reference.clone();
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let custody = native_cache_custody(io,
                file_key.len() as u64 + crate::filesystem::UploadRecord::logical_owned_bytes(&retained_reference)?)?;
            self.files.insert(
                file_key,
                StoredFile {
                    reference: retained_reference,
                    bytes: resident,
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    custody,
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
                let generation = self.generation.checked_add(1)
                    .ok_or_else(|| Error::Invalid("content generation overflow".into()))?;
                let growth = match self.paths.get(path) {
                    None => Some(4),
                    Some(history) if history.capacity() == history.len() => Some(
                        history.capacity().checked_mul(2)
                            .ok_or_else(|| Error::Invalid("path history capacity overflow".into()))?.max(4),
                    ),
                    _ => None,
                };
                #[cfg(feature = "filesystem")]
                if let Some(io) = io {
                    let history = self.paths.get(path);
                    if let Some(slots) = growth {
                        let bytes = slots.checked_mul(std::mem::size_of::<PathRevision>())
                            .ok_or_else(|| Error::Invalid("path history ownership overflow".into()))? as u64;
                        io.claim(bytes)?;
                        io.allocation_attempt(bytes)?;
                    }
                    if history.is_none() {
                        io.claim(path.len() as u64)?;
                        io.allocation_attempt(path.len() as u64)?;
                        io.preflight(acyclic_fs::WorkCounters { bytes_copied: path.len() as u64, ..Default::default() })?;
                    }
                }
                #[cfg(feature = "filesystem")]
                let retained_reference = clone_reference_with_io(reference, io)?;
                #[cfg(not(feature = "filesystem"))]
                let retained_reference = reference.clone();
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let custody = native_cache_custody(io,
                    crate::filesystem::UploadRecord::logical_owned_bytes(&retained_reference)?)?;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let key_custody = if !self.paths.contains_key(path) {
                    native_cache_custody(io, path.len() as u64)?
                } else { None };
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let capacity_custody = match growth {
                    Some(slots) => native_cache_custody(io, (slots * std::mem::size_of::<PathRevision>()) as u64)?,
                    None => None,
                };
                self.generation = generation;
                if !self.paths.contains_key(path) {
                    self.paths.insert(path.to_owned(), PathHistory {
                        revisions: Vec::new(),
                        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                        capacity_custody: None,
                        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                        key_custody,
                    });
                    #[cfg(feature = "filesystem")]
                    if let Some(io) = io {
                        io.charge(acyclic_fs::WorkCounters { bytes_copied: path.len() as u64, ..Default::default() })?;
                    }
                }
                let history = self.paths.get_mut(path)
                    .ok_or_else(|| Error::Storage("retained path history is missing".into()))?;
                if let Some(slots) = growth {
                    let additional = slots - history.len();
                    history.reserve_exact(additional);
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    { history.capacity_custody = capacity_custody; }
                }
                history.push(PathRevision {
                    generation, file: retained_reference,
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    custody,
                });
            }
        }
        #[cfg(feature = "filesystem")]
        if let Some(io) = io { io.remaining()?; }
        Ok(())
    }

    pub(crate) fn read(&self, file: &FileRef) -> Result<Vec<u8>> {
        file.validate(&mut None)?;
        if file.volume() != &self.volume {
            return Err(Error::Unauthorized("file belongs to another volume".into()));
        }
        let key = Self::file_key(file)?;
        let stored = self
            .files
            .get(&key)
            .filter(|stored| stored.reference == *file)
            .ok_or_else(|| Error::NotFound("owner has no resident file".into()))?;
        file.descriptor().verify(&stored.bytes, &mut None)?;
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
            paths_conflict(existing, path)
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
        // Version is the provider's immutable identity.
        String::from_utf8(crate::contract::canonical_json_bytes(&(
            file.volume(),
            file.path(),
            file.version(),
        ))?)
        .map_err(|error| Error::Invalid(format!("file reference is not UTF-8: {error}")))
    }

    fn blob_key(file: &FileRef) -> String {
        let mut output = String::with_capacity(decimal_digits(file.descriptor().byte_length()) as usize + 1 + 64);
        use std::fmt::Write as _;
        write!(&mut output, "{}:", file.descriptor().byte_length()).expect("String formatting cannot fail");
        output.push_str(&blake3::Hash::from_bytes(*file.descriptor().sha256()).to_hex());
        output
    }
}

fn paths_conflict(existing: &str, path: &str) -> bool {
    existing != path
        && (existing.strip_prefix(path).is_some_and(|suffix| suffix.starts_with('/'))
            || path.strip_prefix(existing).is_some_and(|suffix| suffix.starts_with('/')))
}

fn decimal_digits(mut number: u64) -> u64 {
    let mut digits = 1;
    while number >= 10 { number /= 10; digits += 1; }
    digits
}

#[cfg(any(test, all(feature = "wasm", target_arch = "wasm32")))]
fn hex_bytes(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 15) as usize] as char);
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

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_cache_custody(
    io: Option<&crate::filesystem::PreparationIo>, bytes: u64,
) -> Result<Option<acyclic_fs::NativeAllocation>> {
    match io {
        Some(io) if io.original_native_work().is_some() => io.take_native_custody(bytes).map(Some),
        _ => Ok(None),
    }
}

#[cfg(feature = "filesystem")]
fn clone_reference_with_io(
    reference: &FileRef, io: Option<&crate::filesystem::PreparationIo>,
) -> Result<FileRef> {
    let Some(io) = io else { return Ok(reference.clone()); };
    let volume = reference.volume().copy(io)?;
    let values = [reference.path(), reference.version(), reference.descriptor().media_type(), reference.display_name()];
    let copied = values.iter().map(|value| value.len() as u64).sum();
    let counters = acyclic_fs::WorkCounters { bytes_copied: copied, ..Default::default() };
    io.preflight(counters)?;
    for value in values { io.claim(value.len() as u64)?; io.allocation_attempt(value.len() as u64)?; }
    let descriptor = crate::conversation::FileDescriptor::new(
        *reference.descriptor().sha256(), reference.descriptor().byte_length(), reference.descriptor().media_type(),
    )?;
    let result = FileRef::new(volume, reference.path(), reference.version(), descriptor, reference.display_name())?;
    io.charge(counters)?;
    Ok(result)
}
