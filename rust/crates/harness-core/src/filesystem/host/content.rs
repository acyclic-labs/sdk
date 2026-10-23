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
        access: crate::conversation::ContentAccess<'_, '_>,
    ) -> Result<FileRef>
    where
        A: 'static,
        O: 'static,
    {
        let crate::conversation::ContentAccess { ceiling, io } = access;

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
            ceiling,
            io,
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
        access: crate::conversation::ContentAccess<'_, '_>,
    ) -> Result<Option<FileRef>> {
        let crate::conversation::ContentAccess { ceiling, io } = access;

        grant.require(volume, VolumeOperation::Write, io)?;
        let observed = self
            .content_for_operation_record_io(
                PublicationLookup {
                    volume,
                    key,
                    expected_path: Some(path),
                    maximum_bytes,
                    internal_class: None,
                    purpose: PublicationPurpose::Metadata,
                    scope: grant.policy_scope(),
                    ceiling,
                },
                io,
            )
            .await?;
        Ok(observed.map(|(file, _)| file))
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
            None,
            io,
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
                crate::conversation::ContentAccess::default(),
            )
            .await?;
        Ok(ReferencedAttachments::Manifest {
            manifest,
            item_count: u32::try_from(items.len())
                .map_err(|_| Error::Invalid("attachment count exceeds limit".into()))?,
        })
    }
}
