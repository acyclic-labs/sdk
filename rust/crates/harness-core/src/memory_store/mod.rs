//! Bounded owner-private content for the browser host.

use crate::conversation::FileDescriptor;
use crate::conversation::{FileRef, VolumeRef, is_internal_path};
use crate::executor::json::canonical_json_digest;
use crate::{Error, Result};
use std::collections::{BTreeMap, HashMap};

struct StoredFile {
    reference: FileRef,
}

struct StoredBlob {
    bytes: Box<[u8]>,
}

struct PathRevision {
    generation: u64,
    file: FileRef,
}

#[derive(Default)]
struct PathHistory {
    revisions: Vec<PathRevision>,
}

/// Bounded owner-private immutable content state.
#[doc(hidden)]
pub struct MemoryStore {
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
    directory_index: directory::DirectoryIndex,
}

impl MemoryStore {
    #[doc(hidden)]
    pub fn new(
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
            directory_index: directory::DirectoryIndex::default(),
        })
    }

    #[doc(hidden)]
    pub fn stage(
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
        let digest = canonical_json_digest(
            &(
                &path,
                descriptor.sha256(),
                descriptor.byte_length(),
                descriptor.media_type(),
                &display_name,
            ),
            u64::MAX,
            &mut None,
        )?;
        let reference = FileRef::new(
            self.volume.clone(),
            path.to_owned(),
            blake3::Hash::from_bytes(digest).to_hex().to_string(),
            descriptor,
            display_name.to_owned(),
            &mut None,
        )?;
        self.retain_reference(&reference, bytes, update_path)?;
        Ok(reference)
    }
}

mod directory;
#[doc(hidden)]
pub use directory::MemoryStorePage;
mod retention;
#[cfg(test)]
mod tests;
