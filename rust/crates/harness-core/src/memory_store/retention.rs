use super::*;

impl MemoryStore {
    pub(crate) fn retain_reference(
        &mut self,
        reference: &FileRef,
        bytes: &[u8],
        update_path: bool,
    ) -> Result<()> {
        reference.validate(&mut None)?;
        if reference.volume() != &self.volume {
            return Err(Error::Unauthorized("file belongs to another volume".into()));
        }
        reference.descriptor().verify(bytes, &mut None)?;
        let path = reference.path();
        if bytes.len() as u64 > self.maximum_file_bytes
            || path.len() as u64 > self.maximum_path_bytes
        {
            return Err(Error::Invalid("staged file exceeds harness limits".into()));
        }
        if is_internal_path(path) {
            return Err(Error::Invalid("internal storage paths are reserved".into()));
        }
        let file_key = Self::file_key(reference)?;
        if let Some(prior) = self.files.get(&file_key) {
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
            self.retain_blob(reference, bytes)?;
            let retained_reference = reference.copy(&mut None)?;
            self.files.insert(
                file_key,
                StoredFile {
                    reference: retained_reference,
                },
            );
        }
        if update_path {
            self.retain_path(reference)?;
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn read(&self, file: &FileRef) -> Result<Vec<u8>> {
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
    #[doc(hidden)]
    pub fn has(&self, file: &FileRef) -> Result<bool> {
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

    fn retain_blob(&mut self, reference: &FileRef, bytes: &[u8]) -> Result<()> {
        let blob_key = (
            reference.descriptor().byte_length(),
            *reference.descriptor().sha256(),
        );
        if let Some(existing) = self.blobs.get(&blob_key) {
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
            let resident = Box::<[u8]>::from(bytes);
            self.blobs.insert(blob_key, StoredBlob { bytes: resident });
            self.resident_bytes += length;
        }
        Ok(())
    }
}
