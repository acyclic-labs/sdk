use super::*;
mod canonical;

/// Immutable byte descriptor checked before a file is admitted.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct FileDescriptor {
    pub(super) sha256: [u8; 32],
    #[schemars(range(max = crate::conversation::MAX_EXACT_JS_INTEGER))]
    #[serde(deserialize_with = "deserialize_file_length")]
    pub(super) byte_length: u64,
    #[serde(deserialize_with = "deserialize_media_type")]
    pub(super) media_type: String,
}

impl FileDescriptor {
    #[cfg(feature = "filesystem")]
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        self.media_type.capacity() as u64
    }

    /// Constructs a descriptor after validating its media type.
    pub fn new(sha256: [u8; 32], byte_length: u64, media_type: String) -> Result<Self> {
        let value = Self {
            sha256,
            byte_length,
            media_type,
        };
        value.validate(&mut None)?;
        Ok(value)
    }

    /// Computes a descriptor under the optional work observer.
    pub fn from_bytes(
        bytes: &[u8],
        media_type: &str,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
            io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect {
                items: media_type.len() as u64 + 1,
            })?;
        }
        if let Some(reason) = Self::validation_fields(bytes.len() as u64, media_type) {
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                reason,
            ));
        }
        if let Some(io) = work.as_deref_mut() {
            io.hashed_bytes(bytes.len() as u64)?;
        }
        let sha256 = Sha256::digest(bytes).into();
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let media_type = crate::core::copy_text(media_type, &mut quota)?;
        quota.transfer();
        Ok(Self {
            sha256,
            byte_length: bytes.len() as u64,
            media_type,
        })
    }

    /// Verifies through the existing borrowed cumulative history observer.
    pub fn verify(
        &self,
        bytes: &[u8],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if self.matches_bytes(bytes, work)? {
            return Ok(());
        }
        Err(conversation_error(
            work,
            crate::wire::ErrorCode::Invalid,
            "file content does not match its descriptor",
        ))
    }

    pub(crate) fn matches_bytes(
        &self,
        bytes: &[u8],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        #[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
        acyclic_test_support::differential::record(("verify", bytes.len() as u64));
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if bytes.len() as u64 == self.byte_length {
            if let Some(io) = work.as_deref_mut() {
                io.hashed_bytes(bytes.len() as u64)?;
            }
            if Sha256::digest(bytes).as_slice() == self.sha256 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Validates metadata under the caller's optional work observer.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        Self::validation_fields(self.byte_length, &self.media_type).map_or(Ok(()), |reason| {
            Err(conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                reason,
            ))
        })
    }

    pub(super) fn validation_fields(byte_length: u64, media_type: &str) -> Option<&'static str> {
        if byte_length > MAX_EXACT_JS_INTEGER {
            return Some("file byte length exceeds cross-language precision");
        }
        let Some((kind, subtype)) = media_type.split_once('/') else {
            return Some("media type is invalid");
        };
        if kind.is_empty()
            || subtype.is_empty()
            || media_type.chars().any(|character| {
                !character.is_ascii_alphanumeric()
                    && !matches!(character, '/' | '-' | '+' | '.' | '_')
            })
        {
            return Some("media type is invalid");
        }
        None
    }

    /// SHA-256 digest of the exact bytes.
    #[must_use]
    pub const fn sha256(&self) -> &[u8; 32] {
        &self.sha256
    }

    /// Exact file length.
    #[must_use]
    pub const fn byte_length(&self) -> u64 {
        self.byte_length
    }

    /// Declared MIME type.
    #[must_use]
    pub fn media_type(&self) -> &str {
        &self.media_type
    }
}

/// An immutable version of a file within a scoped volume.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct FileRef {
    pub(super) volume: VolumeRef,
    #[serde(deserialize_with = "deserialize_path")]
    pub(super) path: String,
    #[serde(deserialize_with = "deserialize_label")]
    pub(super) version: String,
    pub(super) descriptor: FileDescriptor,
    #[serde(deserialize_with = "deserialize_label")]
    pub(super) display_name: String,
}

impl FileRef {
    pub(crate) fn identity_digest(
        &self,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<[u8; 32]> {
        crate::executor::json::borrowed_json_digest(
            &canonical::Canonical(self),
            maximum_bytes,
            work,
        )
    }

    #[cfg(feature = "objects")]
    pub(crate) fn with_version(mut self, version: String) -> Result<Self> {
        validate_label(&version, &mut None)?;
        self.version = version;
        Ok(self)
    }

    /// Creates a reference after checking paths, version and descriptor.
    pub fn new(
        volume: VolumeRef,
        path: String,
        version: String,
        descriptor: FileDescriptor,
        display_name: String,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let value = Self {
            volume,
            path,
            version,
            descriptor,
            display_name,
        };
        value.validate(work)?;
        Ok(value)
    }

    /// Validates metadata under the caller's optional work observer.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.volume.validate(work)?;
        validate_content_path(&self.path, work)?;
        validate_label(&self.version, work)?;
        validate_label(&self.display_name, work)?;
        self.descriptor.validate(work)
    }

    /// Owning volume.
    #[must_use]
    pub const fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    /// Normalized volume-relative path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Immutable provider-owned revision.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Integrity and rendering metadata.
    #[must_use]
    pub const fn descriptor(&self) -> &FileDescriptor {
        &self.descriptor
    }

    /// Human-facing file name.
    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    /// Owner-issued capability for reading precisely this immutable version.
    pub fn read_capability(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<String> {
        self.validate(work)?;
        content_label(
            acyclic_fs::access::FILE_READ_PREFIX,
            self.identity_digest(
                crate::store::default_projection_read_limits().maximum_bytes,
                work,
            )?,
            work,
        )
    }
}

impl FileRef {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        [
            self.path.capacity(),
            self.version.capacity(),
            self.descriptor.media_type.capacity(),
            self.display_name.capacity(),
        ]
        .into_iter()
        .try_fold(self.volume.logical_owned_bytes()?, |bytes, capacity| {
            bytes.checked_add(capacity as u64)
        })
        .ok_or_else(|| {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            {
                Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "file capacity inventory overflows",
                }
            }
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            {
                Error::Invalid("file capacity inventory overflows".into())
            }
        })
    }

    #[cfg(feature = "filesystem")]
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn logical_clone_owned_bytes(&self) -> Result<u64> {
        let owner = match &self.volume.owner {
            VolumeOwner::Project(text) | VolumeOwner::Session(text) => text.len() as u64,
            VolumeOwner::Agent(_) => 0,
        };
        let provider = &self.volume.provider;
        [
            provider.namespace().len(),
            provider.family().len(),
            provider.version().len(),
            self.volume.id.len(),
            self.path.len(),
            self.version.len(),
            self.descriptor.media_type.len(),
            self.display_name.len(),
        ]
        .into_iter()
        .try_fold(owner, |bytes, length| bytes.checked_add(length as u64))
        .ok_or_else(|| {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            {
                Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "reference clone bytes overflow",
                }
            }
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            {
                Error::Invalid("file clone ownership overflow".into())
            }
        })
    }

    /// Copies file fields under the optional work observer.
    pub fn copy(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let file = self.clone_in_quota(&mut quota)?;
        quota.transfer();
        Ok(file)
    }

    pub(crate) fn clone_in_quota(
        &self,
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Self> {
        if quota.work().is_none() {
            return Ok(self.clone());
        }
        self.validate(quota.work())?;
        let volume = self.volume.clone_in_quota(quota)?;
        let path = crate::core::copy_text(&self.path, quota)?;
        let version = crate::core::copy_text(&self.version, quota)?;
        Ok(Self {
            volume,
            path,
            version,
            descriptor: FileDescriptor {
                sha256: self.descriptor.sha256,
                byte_length: self.descriptor.byte_length,
                media_type: crate::core::copy_text(&self.descriptor.media_type, quota)?,
            },
            display_name: crate::core::copy_text(&self.display_name, quota)?,
        })
    }
}
