use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Stages a pinned file, optionally checking a generation and charging preparation IO.
    #[allow(clippy::too_many_arguments, reason = "complete staged file identity")]
    pub async fn put_content(
        &self, volume: &VolumeRef, grant: &ContentGrant, path: &str, bytes: &[u8],
        media_type: &str, display_name: &str, maximum_bytes: u64, key: &IdempotencyKey,
        expected_generation: Option<&GenerationRef>, io: Option<&PreparationIo>,
    ) -> Result<FileRef> where A: 'static, O: 'static {
        if expected_generation.is_some() { crate::tool::edit::validate_edit_bound(maximum_bytes)?; }
        self.put_content_impl(volume, grant, path, bytes, media_type, display_name,
            maximum_bytes, key, None, expected_generation, io, None, None).await
    }

    /// Recovers the original upload generation and authenticated metadata.
    pub async fn content_for_operation(
        &self, volume: &VolumeRef, grant: &ContentGrant, key: &IdempotencyKey,
        path: &str, maximum_bytes: u64, io: Option<&PreparationIo>,
    ) -> Result<Option<FileRef>> {
        self.content_for_operation_impl(volume, grant, key, Some(path), maximum_bytes, io).await
    }

    pub(crate) async fn original_internal_content_for_operation_with_io(
        &self, volume: &VolumeRef, verifier: &AuthorityVerifier, scope: &Scope,
        key: &IdempotencyKey, path: &str, maximum_bytes: u64,
        class: InternalContentClass, io: &PreparationIo,
    ) -> Result<Option<FileRef>> {
        crate::executor::with_history_io(Some(io), |work|
            ContentGrant::authorize_volume(verifier, scope, volume, VolumeOperation::Read, io.remaining()?.bytes_encoded, work))?;
        self.content_for_operation_record_io(
            volume, key, Some(path), maximum_bytes, Some(class), Some(io)).await
    }

    pub(in crate::filesystem) async fn content_for_operation_record_io(
        &self, volume: &VolumeRef, key: &IdempotencyKey,
        expected_path: Option<&str>, maximum_bytes: u64,
        internal_class: Option<InternalContentClass>, io: Option<&PreparationIo>,
    ) -> Result<Option<FileRef>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            volume.validate(&mut Some(&mut crate::store::history::native_work_io(work)?))?;
        } else {
            volume.validate(&mut None)?;
        }
        #[cfg(target_arch = "wasm32")]
        volume.validate(&mut None)?;
        if volume.provider() != &self.provider {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "resource belongs to another Filesystem provider"));
        }
        if let Some(path) = expected_path {
            crate::executor::with_history_io(io, |work| crate::conversation::validate_content_path(path, work))?;
            if internal_class.is_some_and(|class| !path.starts_with(class.prefix()))
                || (internal_class.is_none() && (path == ".system" || path.starts_with(".system/")
                    || path == "turns/.canonical" || path.starts_with("turns/.canonical/"))) {
                return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, if internal_class.is_some() {
                    "content path is outside its authorized storage class"
                } else { "internal storage paths are reserved" }));
            }
        }
        let workspace = fork_preparer_io::content_workspace(&self.provider, volume, io)?;
        let Some(original) = fork_preparer_io::operation_generation(self, &workspace, key, io).await? else { return Ok(None); };
        let receipt_path = content_identity_path_io(key.as_str(), true, ".system/harness-uploads/", io)?;
        const RECORD_BYTES: u64 = 6 * (crate::conversation::MAX_PATH_BYTES as u64
            + 32 * crate::conversation::MAX_LABEL_BYTES as u64 + 1024);
        let receipt = match io {
            Some(io) => {
                read_generation_with_io(&original, &receipt_path, RECORD_BYTES, io).await?
            }
            None => original.read(&receipt_path, RECORD_BYTES, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error))?,
        };
        let (recorded, expected): (FileRef, Option<GenerationRef>) = match io {
            Some(io) => io.decode_json(&receipt)?,
            None => crate::contract::json_from_slice(&receipt).map_err(|error| Error::Storage(error.to_string()))?,
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            recorded.validate(&mut Some(&mut crate::store::history::native_work_io(work)?))?;
        } else {
            recorded.validate(&mut None)?;
        }
        #[cfg(target_arch = "wasm32")]
        recorded.validate(&mut None)?;
        if recorded.volume() != volume || recorded.version() != "pending"
            || expected_path.is_some_and(|path| path != recorded.path()) {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Conflict, "original upload metadata differs from requested identity"));
        }
        if internal_class.is_some_and(|class| !recorded.path().starts_with(class.prefix()))
            || (internal_class.is_none() && (recorded.path() == ".system" || recorded.path().starts_with(".system/")
                || recorded.path() == "turns/.canonical" || recorded.path().starts_with("turns/.canonical/"))) {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, if internal_class.is_some() {
                "original content is outside its authorized storage class"
            } else { "internal storage paths are reserved" }));
        }
        let canonical = match io {
            Some(io) => io.encode(&(&recorded, expected.as_ref()), RECORD_BYTES, true)?,
            None => crate::contract::canonical_json_bytes(&(&recorded, expected.as_ref()))?.into(),
        };
        if canonical.as_ref() != receipt.as_ref() {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Conflict, "original upload receipt is not canonical"));
        }
        let metadata_path = content_metadata_path_io(recorded.path(), true, io)?;
        let metadata = match io {
            Some(io) => {
                read_generation_with_io(&original, &metadata_path, RECORD_BYTES, io).await?
            }
            None => original.read(&metadata_path, RECORD_BYTES, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error))?,
        };
        let encoded = match io {
            Some(io) => io.encode(&recorded, RECORD_BYTES, false)?,
            None => serde_json::to_vec(&recorded).map_err(|error| Error::Invalid(error.to_string()))?.into(),
        };
        if encoded.as_ref() != metadata.as_ref() {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Conflict, "original upload metadata differs from its canonical receipt"));
        }
        let generation = fork_preparer_io::generation_ref(self, &original, io)?;
        match io {
            Some(io) => fork_preparer_io::retain(&original, Some(io)).await?,
            None => {
                original.pin(format!("harness-generation-{}", hex::encode(generation.as_resource().key())), acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
                    .await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error))?;
            }
        }
        let version = fork_preparer_io::hex_key(generation.as_resource().key(), io)?;
        let reference = crate::executor::with_history_io(io, |work| FileRef::new(
            volume.copy(work)?, fork_preparer_io::clone_text(recorded.path(), io)?, version,
            FileDescriptor::new(*recorded.descriptor().sha256(), recorded.descriptor().byte_length(),
                fork_preparer_io::clone_text(recorded.descriptor().media_type(), io)?)?,
            fork_preparer_io::clone_text(recorded.display_name(), io)?, work))?;
        self.read_pinned_io(&reference, maximum_bytes, io).await?;
        Ok(Some(reference))
    }

    /// Trusted journal staging retains the same atomic metadata and retry checks as public uploads, but may write only its assigned subtree.
    #[allow(
        clippy::too_many_arguments,
        reason = "internal stage mirrors the complete file contract"
    )]
    pub(crate) async fn put_internal_content(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        maximum_bytes: u64,
        idempotency_key: &IdempotencyKey,
        class: InternalContentClass,
    ) -> Result<FileRef> where A: 'static, O: 'static {
        self.put_content_impl(volume,
        grant,
        path,
        bytes,
        media_type,
        display_name,
        maximum_bytes,
        idempotency_key,
        Some(class),
        None,
        None, None, None)
        .await
    }

    /// Internal staging keeps the same original whole-volume authority and publisher engine; only its assigned private path class is different.
    #[allow(clippy::too_many_arguments, reason = "complete original internal publication identity")]
    pub(crate) async fn put_internal_content_original_with_io(
        &self, volume: &VolumeRef, verifier: &AuthorityVerifier, scope: &Scope,
        path: &str, bytes: &[u8], media_type: &str, display_name: &str,
        maximum_bytes: u64, idempotency_key: &IdempotencyKey,
        class: InternalContentClass, io: &PreparationIo,
    ) -> Result<FileRef> where A: 'static, O: 'static {
        let grant_hold;
        let grant = ContentGrant::verify(verifier, scope, volume, VolumeOperation::Write, (io).remaining()?.bytes_encoded, &mut Some(&mut (io).history_io()))?;
        grant_hold = io.guard_owned(grant.logical_owned_bytes()?)?;
        let result = self.put_content_impl(volume, &grant, path, bytes, media_type, display_name,
        maximum_bytes, idempotency_key, Some(class), None, Some(io), None, None).await;
        drop(grant);
        drop(grant_hold);
        result
    }

    /// Stages a complete canonical attachment list for a manifest-backed event.
    pub async fn put_attachment_manifest(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        items: &[Attachment],
        maximum_bytes: u64,
        idempotency_key: &IdempotencyKey,
    ) -> Result<ReferencedAttachments> where A: 'static, O: 'static {
        let bytes = crate::conversation::encode_attachment_manifest(items)?;
        let manifest = self
            .put_content(
                volume,
                grant,
                path,
                &bytes,
                "application/vnd.acyclic.harness.attachments+json",
                "attachments.json",
                maximum_bytes,
                idempotency_key,
             None, None)
            .await?;
        Ok(ReferencedAttachments::Manifest {
            manifest,
            item_count: u32::try_from(items.len())
                .map_err(|_| Error::Invalid("attachment count exceeds limit".into()))?,
        })
    }

}
