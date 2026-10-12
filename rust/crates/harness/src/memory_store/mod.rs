//! Bounded in-memory content shared by native and WASM hosts.

#[cfg(any(test, all(feature = "wasm", target_arch = "wasm32")))]
use crate::executor::json::canonical_json_digest;
#[cfg(any(test, all(feature = "wasm", target_arch = "wasm32")))]
use crate::conversation::FileDescriptor;
use crate::conversation::{FileRef, VolumeRef, is_internal_path};
use crate::{Error, Result};
use std::collections::{BTreeMap, HashMap};

struct StoredFile {
    reference: FileRef,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    custody: Option<acyclic_fs::NativeAllocation>,
}

struct StoredBlob {
    bytes: Box<[u8]>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    custody: Option<acyclic_fs::NativeAllocation>,
}

struct PathRevision {
    generation: u64,
    file: FileRef,
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
    blobs: HashMap<(u64, [u8; 32]), StoredBlob>,
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
        let descriptor = FileDescriptor::from_bytes(bytes, media_type, &mut None)?;
        let digest = canonical_json_digest(&(
            &path,
            descriptor.sha256(),
            descriptor.byte_length(),
            descriptor.media_type(),
            &display_name,
        ), u64::MAX, &mut None)?;
        let reference = FileRef::new(
            self.volume.clone(),
            path.to_owned(),
            blake3::Hash::from_bytes(digest).to_hex().to_string(),
            descriptor,
            display_name.to_owned(),
            &mut None,
        )?;
        self.retain_reference(&reference, bytes, update_path, None)?;
        Ok(reference)
    }
}

mod directory;
mod retention;
pub(crate) use directory::{MemoryStoreEntry, MemoryStoreEntryKind, MemoryStorePage};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use retention::native_cache_custody;
#[cfg(test)]
mod tests;
