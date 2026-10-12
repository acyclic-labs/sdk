use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Stages a pinned file, optionally checking a generation and charging preparation IO.
    #[allow(clippy::too_many_arguments, reason = "complete staged file identity")]
    pub async fn put_content(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        maximum_bytes: u64,
        key: &IdempotencyKey,
        expected_generation: Option<&GenerationRef>,
        io: Option<&PreparationIo>,
    ) -> Result<FileRef>
    where
        A: 'static,
        O: 'static,
    {
        if expected_generation.is_some() {
            crate::tool::edit::validate_edit_bound(maximum_bytes)?;
        }
        self.put_content_impl(
            volume,
            grant,
            path,
            bytes,
            media_type,
            display_name,
            maximum_bytes,
            key,
            None,
            expected_generation,
            io,
            None,
            None,
        )
        .await
    }

    /// Recovers the original upload generation and authenticated metadata.
    pub async fn content_for_operation(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        key: &IdempotencyKey,
        path: &str,
        maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Option<FileRef>> {
        self.content_for_operation_impl(volume, grant, key, Some(path), maximum_bytes, io)
            .await
    }

    pub(crate) async fn original_internal_content_for_operation_with_io(
        &self,
        volume: &VolumeRef,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        key: &IdempotencyKey,
        path: &str,
        maximum_bytes: u64,
        class: InternalContentClass,
        io: &PreparationIo,
    ) -> Result<Option<FileRef>> {
        crate::executor::with_history_io(Some(io), |work| {
            ContentGrant::authorize_volume(
                verifier,
                scope,
                volume,
                VolumeOperation::Read,
                io.remaining()?.bytes_encoded,
                work,
            )
        })?;
        self.content_for_operation_record_io(
            volume,
            key,
            Some(path),
            maximum_bytes,
            Some(class),
            Some(io),
        )
        .await
    }

    pub(in crate::filesystem) async fn content_for_operation_record_io(
        &self,
        volume: &VolumeRef,
        key: &IdempotencyKey,
        expected_path: Option<&str>,
        maximum_bytes: u64,
        internal_class: Option<InternalContentClass>,
        io: Option<&PreparationIo>,
    ) -> Result<Option<FileRef>> {
        crate::executor::with_history_io(io, |work| volume.validate(work))?;
        if volume.provider() != &self.provider {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "resource belongs to another Filesystem provider",
            ));
        }
        if let Some(path) = expected_path {
            crate::executor::with_history_io(io, |work| {
                crate::conversation::validate_content_path(path, work)
            })?;
            if internal_class.is_some_and(|class| !path.starts_with(class.prefix()))
                || (internal_class.is_none()
                    && (path == ".system"
                        || path.starts_with(".system/")
                        || path == "turns/.canonical"
                        || path.starts_with("turns/.canonical/")))
            {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    if internal_class.is_some() {
                        "content path is outside its authorized storage class"
                    } else {
                        "internal storage paths are reserved"
                    },
                ));
            }
        }
        let workspace = fork_preparer_io::content_workspace(&self.provider, volume, io)?;
        let Some(original) =
            fork_preparer_io::operation_generation(self, &workspace, key, io).await?
        else {
            return Ok(None);
        };
        let receipt_path =
            content_identity_path_io(key.as_str(), true, ".system/harness-uploads/", io)?;
        const RECORD_BYTES: u64 = 6
            * (crate::conversation::MAX_PATH_BYTES as u64
                + 32 * crate::conversation::MAX_LABEL_BYTES as u64
                + 1024);
        let receipt = read_generation(&original, &receipt_path, RECORD_BYTES, io).await?;
        let (recorded, expected): (FileRef, Option<GenerationRef>) = match io {
            Some(io) => io.decode_json(&receipt)?,
            None => crate::contract::json_from_slice(&receipt)
                .map_err(|error| Error::Storage(error.to_string()))?,
        };
        crate::executor::with_history_io(io, |work| recorded.validate(work))?;
        if recorded.volume() != volume
            || recorded.version() != "pending"
            || expected_path.is_some_and(|path| path != recorded.path())
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "original upload metadata differs from requested identity",
            ));
        }
        if internal_class.is_some_and(|class| !recorded.path().starts_with(class.prefix()))
            || (internal_class.is_none()
                && (recorded.path() == ".system"
                    || recorded.path().starts_with(".system/")
                    || recorded.path() == "turns/.canonical"
                    || recorded.path().starts_with("turns/.canonical/")))
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                if internal_class.is_some() {
                    "original content is outside its authorized storage class"
                } else {
                    "internal storage paths are reserved"
                },
            ));
        }
        let canonical = match io {
            Some(io) => io.encode(&(&recorded, expected.as_ref()), RECORD_BYTES, true)?,
            None => crate::contract::canonical_json_bytes(&(&recorded, expected.as_ref()))?.into(),
        };
        if canonical.as_ref() != receipt.as_ref() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "original upload receipt is not canonical",
            ));
        }
        let metadata_path = content_metadata_path_io(recorded.path(), true, io)?;
        let metadata = read_generation(&original, &metadata_path, RECORD_BYTES, io).await?;
        let encoded = match io {
            Some(io) => io.encode(&recorded, RECORD_BYTES, false)?,
            None => serde_json::to_vec(&recorded)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .into(),
        };
        if encoded.as_ref() != metadata.as_ref() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "original upload metadata differs from its canonical receipt",
            ));
        }
        let generation = fork_preparer_io::generation_ref(&self.provider, original.id(), io)?;
        match io {
            Some(io) => fork_preparer_io::retain(&original, Some(io)).await?,
            None => {
                original
                    .pin(
                        format!(
                            "harness-generation-{}",
                            hex::encode(generation.as_resource().key())
                        ),
                        acyclic_fs::WorkBudget::UNBOUNDED,
                        &acyclic_fs::CancellationToken::new(),
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| map_error(failure.error))?;
            }
        }
        let version = fork_preparer_io::hex_key(generation.as_resource().key(), io)?;
        let reference = crate::executor::with_history_io(io, |work| {
            FileRef::new(
                volume.copy(work)?,
                stock_string_copy(recorded.path(), io)?,
                version,
                FileDescriptor::new(
                    *recorded.descriptor().sha256(),
                    recorded.descriptor().byte_length(),
                    stock_string_copy(recorded.descriptor().media_type(), io)?,
                )?,
                stock_string_copy(recorded.display_name(), io)?,
                work,
            )
        })?;
        self.read_pinned_io(&reference, maximum_bytes, io).await?;
        Ok(Some(reference))
    }

    /// Stages journal payloads under the assigned internal subtree.
    #[allow(clippy::too_many_arguments, reason = "complete internal file identity")]
    pub(crate) async fn put_internal_content(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        maximum_bytes: u64,
        key: &IdempotencyKey,
        class: InternalContentClass,
        io: Option<&PreparationIo>,
    ) -> Result<FileRef>
    where
        A: 'static,
        O: 'static,
    {
        self.put_content_impl(
            volume,
            grant,
            path,
            bytes,
            media_type,
            display_name,
            maximum_bytes,
            key,
            Some(class),
            None,
            io,
            None,
            None,
        )
        .await
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
    ) -> Result<ReferencedAttachments>
    where
        A: 'static,
        O: 'static,
    {
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
                None,
                None,
            )
            .await?;
        Ok(ReferencedAttachments::Manifest {
            manifest,
            item_count: u32::try_from(items.len())
                .map_err(|_| Error::Invalid("attachment count exceeds limit".into()))?,
        })
    }
}

use crate::executor::{stock_boundary, stock_string_copy};
