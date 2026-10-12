use super::directory::paths_conflict;
use super::*;
use crate::executor::{PreparationIo, with_history_io};

impl MemoryStore {
    pub(crate) fn retain_reference(
        &mut self,
        reference: &FileRef,
        bytes: &[u8],
        update_path: bool,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        with_history_io(io, |work| reference.validate(work))?;
        if reference.volume() != &self.volume {
            return Err(Error::Unauthorized("file belongs to another volume".into()));
        }
        with_history_io(io, |work| reference.descriptor().verify(bytes, work))?;
        let path = reference.path();
        if bytes.len() as u64 > self.maximum_file_bytes
            || path.len() as u64 > self.maximum_path_bytes
        {
            return Err(Error::Invalid("staged file exceeds harness limits".into()));
        }
        if is_internal_path(path) {
            return Err(Error::Invalid("internal storage paths are reserved".into()));
        }
        #[cfg(feature = "filesystem")]
        let file_key = match io {
            Some(io) => {
                let encoded = io.encode(
                    &(reference.volume(), reference.path(), reference.version()),
                    io.remaining()?.bytes_encoded,
                    true,
                )?;
                let hold = io.guard_bytes(&encoded)?;
                let text = std::str::from_utf8(encoded.as_ref()).map_err(|error| {
                    Error::Invalid(format!("file reference is not UTF-8: {error}"))
                })?;
                let mut key = String::new();
                use crate::store::history::HistoryIo as _;
                let mut observer = io.history_io();
                observer.admit_copy(text.len() as u64)?;
                observer.reserve_text(&mut key, text.len())?;
                key.push_str(text);
                observer.copy_bytes(text.len() as u64)?;
                drop(encoded);
                drop(hold);
                key
            }
            None => Self::file_key(reference)?,
        };
        #[cfg(not(feature = "filesystem"))]
        let file_key = Self::file_key(reference)?;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            let examine = acyclic_fs::WorkCounters {
                items_examined: 1,
                ..Default::default()
            };
            io.preflight(examine)?;
            io.charge(examine)?;
        }
        if let Some(prior) = self.files.get(&file_key) {
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                let compare = acyclic_fs::WorkCounters {
                    source_bytes_read: bytes.len() as u64,
                    ..Default::default()
                };
                io.preflight(compare)?;
                io.charge(compare)?;
            }
            let blob_key = (
                prior.reference.descriptor().byte_length(),
                *prior.reference.descriptor().sha256(),
            );
            if prior.reference != *reference
                || self.blobs.get(&blob_key).map(|blob| blob.bytes.as_ref()) != Some(bytes)
            {
                return Err(Error::Invalid(
                    "immutable file version was reused for another file contract".into(),
                ));
            }
        }
        for existing in self.paths.keys() {
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                let examine = acyclic_fs::WorkCounters {
                    items_examined: 1,
                    source_bytes_read: (existing.len() + path.len()) as u64,
                    ..Default::default()
                };
                io.preflight(examine)?;
                io.charge(examine)?;
            }
            if paths_conflict(existing, path) {
                return Err(Error::Invalid(
                    "file path conflicts with an existing directory".into(),
                ));
            }
        }
        if !self.files.contains_key(&file_key) {
            if self.files.len() as u64 >= self.maximum_resident_files {
                return Err(Error::Invalid(
                    "memory content retention limit exceeded".into(),
                ));
            }
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                let rehash = if self.files.len() == self.files.capacity() {
                    self.files.len()
                } else {
                    0
                };
                let slots = acyclic_fs::WorkCounters {
                    items_examined: rehash as u64 + 1,
                    ..Default::default()
                };
                io.preflight(slots)?;
                io.charge(slots)?;
            }
            let blob_key = (
                reference.descriptor().byte_length(),
                *reference.descriptor().sha256(),
            );
            if let Some(existing) = self.blobs.get(&blob_key) {
                #[cfg(feature = "filesystem")]
                if let Some(io) = io {
                    let compare = acyclic_fs::WorkCounters {
                        source_bytes_read: bytes.len() as u64,
                        items_examined: 1,
                        ..Default::default()
                    };
                    io.preflight(compare)?;
                    io.charge(compare)?;
                }
                if existing.bytes.as_ref() != bytes {
                    return Err(Error::Invalid(
                        "content digest was reused for different bytes".into(),
                    ));
                }
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
                let key_bytes = std::mem::size_of_val(&blob_key) as u64;
                #[cfg(feature = "filesystem")]
                if let Some(io) = io {
                    io.retain_owned(length + key_bytes)?;
                    let copy = acyclic_fs::WorkCounters {
                        bytes_copied: length,
                        items_examined: 1,
                        ..Default::default()
                    };
                    io.preflight(copy)?;
                    let rehash = if self.blobs.len() == self.blobs.capacity() {
                        self.blobs.len()
                    } else {
                        0
                    };
                    let slots = acyclic_fs::WorkCounters {
                        items_examined: rehash as u64,
                        ..Default::default()
                    };
                    io.preflight(slots)?;
                    io.charge(slots)?;
                    io.allocation_attempt(length)?;
                }
                let resident = Box::<[u8]>::from(bytes);
                #[cfg(feature = "filesystem")]
                if let Some(io) = io {
                    io.charge(acyclic_fs::WorkCounters {
                        bytes_copied: length,
                        items_examined: 1,
                        ..Default::default()
                    })?;
                }
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let custody = native_cache_custody(io, length + key_bytes)?;
                self.blobs.insert(
                    blob_key,
                    StoredBlob {
                        bytes: resident,
                        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                        custody,
                    },
                );
                self.resident_bytes += length;
            }
            let retained_reference = with_history_io(io, |work| reference.copy(work))?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let custody = native_cache_custody(
                io,
                file_key.len() as u64
                    + crate::filesystem::UploadRecord::logical_owned_bytes(&retained_reference)?,
            )?;
            self.files.insert(
                file_key,
                StoredFile {
                    reference: retained_reference,
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    custody,
                },
            );
        }
        if update_path {
            self.retain_path(reference, io)?;
        }
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.remaining()?;
        }
        Ok(())
    }

    pub(crate) fn read(&self, file: &FileRef) -> Result<Vec<u8>> {
        file.validate(&mut None)?;
        if file.volume() != &self.volume {
            return Err(Error::Unauthorized("file belongs to another volume".into()));
        }
        let key = Self::file_key(file)?;
        self.files
            .get(&key)
            .filter(|stored| stored.reference == *file)
            .ok_or_else(|| Error::NotFound("owner has no resident file".into()))?;
        let blob_key = (file.descriptor().byte_length(), *file.descriptor().sha256());
        let stored = self
            .blobs
            .get(&blob_key)
            .ok_or_else(|| Error::Storage("retained file bytes are missing".into()))?;
        file.descriptor().verify(&stored.bytes, &mut None)?;
        Ok(stored.bytes.to_vec())
    }

    #[cfg(all(feature = "wasm", target_arch = "wasm32"))]
    pub(crate) fn has(&self, file: &FileRef) -> Result<bool> {
        file.validate(&mut None)?;
        if file.volume() != &self.volume {
            return Ok(false);
        }
        let key = Self::file_key(file)?;
        Ok(self
            .files
            .get(&key)
            .is_some_and(|stored| stored.reference == *file))
    }

    fn file_key(file: &FileRef) -> Result<String> {
        String::from_utf8(crate::contract::canonical_json_bytes(&(
            file.volume(),
            file.path(),
            file.version(),
        ))?)
        .map_err(|error| Error::Invalid(format!("file reference is not UTF-8: {error}")))
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn native_cache_custody(
    io: Option<&PreparationIo>,
    bytes: u64,
) -> Result<Option<acyclic_fs::NativeAllocation>> {
    match io {
        Some(io) if io.original_native_work().is_some() => io.take_native_custody(bytes).map(Some),
        _ => Ok(None),
    }
}
