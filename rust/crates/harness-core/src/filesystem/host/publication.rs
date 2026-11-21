use super::*;
use std::marker::PhantomData;

/// Workspace-file receipts are an unsupported persisted format. Refuse their
/// original operation rather than reading a user path or applying it again.
pub(in crate::filesystem) fn publication_evidence<T>(
    evidence: Option<T>,
    io: Option<&PreparationIo>,
) -> Result<T> {
    evidence.ok_or_else(|| {
        stock_boundary(
            io,
            crate::wire::ErrorCode::Unsupported,
            "unsupported workspace-file publication format; start fresh host state",
        )
    })
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::filesystem) enum PublicationPurpose {
    WriteReceipt,
    Metadata,
    Content,
}

#[derive(Clone, Copy)]
pub(in crate::filesystem) struct PublicationLookup<'a> {
    pub volume: &'a VolumeRef,
    pub key: &'a IdempotencyKey,
    pub expected_path: Option<&'a str>,
    pub maximum_bytes: Option<u64>,
    pub internal: bool,
    pub purpose: PublicationPurpose,
    pub scope: &'a Scope,
    pub ceiling: Option<&'a crate::Capabilities>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(in crate::filesystem) async fn content_for_operation_record_io(
        &self,
        request: PublicationLookup<'_>,
        io: Option<&PreparationIo>,
    ) -> Result<Option<(FileRef, Option<crate::conversation::Custody>)>> {
        let owned;
        let io = if let Some(io) = io {
            io
        } else {
            owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &owned
        };
        let PublicationLookup {
            volume,
            key,
            expected_path,
            internal,
            ..
        } = request;
        volume.validate(&mut Some(&mut io.history_io()))?;
        if volume.provider() != &self.provider {
            return Err(io.invalid("resource belongs to another Filesystem provider"));
        }
        if let Some(path) = expected_path {
            crate::conversation::validate_content_path(path, &mut Some(&mut io.history_io()))?;
        }
        let workspace_ref = bounded_io::content_workspace(&self.provider, volume, Some(io))?;
        let hold = io.guard_owned(workspace_ref.logical_owned_bytes()?)?;
        let workspace_ref = (workspace_ref, hold);
        let root = self.open(&workspace_ref.0, Some(io)).await?;
        let operation = bounded_io::operation_key(key, Some(io))?;
        let Some((workspace, binding)) = self
            .content_operation_workspace(&root, operation, internal, io)
            .await?
        else {
            return Ok(None);
        };
        let Some(publication) = io.sdk(
            workspace
                .operation_publication(operation, None, io, io.cancellation())
                .await,
        )?
        else {
            return Ok(None);
        };
        let receipt = publication_evidence(publication.evidence(), Some(io))?;
        let original = publication.generation();
        let recorded = decode_publication_receipt(receipt, io)?;
        recorded.0.0.validate(&mut Some(&mut io.history_io()))?;
        let version = bounded_io::hex_key(&operation.into_bytes(), Some(io))?;
        let _version_hold = io.guard_owned(version.capacity() as u64)?;
        if recorded.0.0.volume() != volume
            || recorded.0.0.version() != version
            || expected_path.is_some_and(|path| path != recorded.0.0.path())
        {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "original upload metadata differs from requested identity",
            ));
        }
        let view = self
            .receipt_access(&workspace, original.id(), &request, &recorded.0.0, io)
            .await?;
        let root_view = if workspace.id() == root.id() {
            None
        } else {
            Some(
                self.receipt_access(
                    &root,
                    binding
                        .as_ref()
                        .ok_or_else(|| io.invalid("missing content workspace binding"))?
                        .id(),
                    &request,
                    &recorded.0.0,
                    io,
                )
                .await?,
            )
        };
        if let Some(binding) = binding {
            bounded_io::retain(&binding, Some(io)).await?;
        }
        verify_upload_metadata(original, &recorded.0, receipt, io).await?;
        let recovered = self
            .recovered_upload(original, recorded, &request, io)
            .await?;
        super::super::access::require_current(&workspace, &view.0, io, self.clock()).await?;
        if let Some(view) = root_view {
            super::super::access::require_current(&root, &view.0, io, self.clock()).await?;
        }
        Ok(Some(recovered))
    }
    async fn receipt_access<'a>(
        &self,
        workspace: &Workspace<A, O>,
        generation: acyclic_fs_format::object_reference::GenerationId,
        request: &PublicationLookup<'_>,
        file: &FileRef,
        io: &'a PreparationIo,
    ) -> Result<super::super::access::CompiledAccess<'a>> {
        let PublicationLookup {
            scope,
            volume,
            ceiling,
            purpose,
            ..
        } = *request;
        // A known publication operation returns its original write receipt.
        // Check write authority on the recorded path before disclosing it;
        // standalone metadata discovery still requires directory authority.
        let write_receipt = purpose == PublicationPurpose::WriteReceipt;
        if write_receipt {
            for grants in [Some(scope.capabilities()), ceiling].into_iter().flatten() {
                if !crate::core::volume_operation_granted_with_work(
                    grants,
                    volume,
                    VolumeOperation::Write,
                    &mut Some(&mut io.history_io()),
                )? {
                    return Err(stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Unauthorized,
                        "receipt caller lacks write authority",
                    ));
                }
            }
        } else {
            for grants in [Some(scope.capabilities()), ceiling] {
                super::super::access::require_directory_ceiling(
                    grants,
                    volume,
                    file.path()
                        .rsplit_once('/')
                        .map_or("", |(parent, _)| parent),
                    Some(io),
                )?;
            }
        }
        let label = crate::core::filesystem_scope_label(
            scope.capabilities(),
            &mut Some(&mut io.history_io()),
        )?;
        let now = self.access_now()?;
        let view = crate::filesystem::access::access_view(
            workspace,
            scope,
            Some(generation),
            super::super::access::capability_payload(
                label,
                if write_receipt {
                    VolumeOperation::Write
                } else {
                    VolumeOperation::Read
                },
                io,
            )?,
            ceiling,
            io,
            now,
        )
        .await?;
        if write_receipt {
            if !view
                .0
                .decide(acyclic_fs::access::Action::Create, file.path(), now)
                .allowed
            {
                self.require(&view.0, acyclic_fs::access::Action::Write, file.path(), io)?;
            }
        } else {
            self.require(&view.0, acyclic_fs::access::Action::List, file.path(), io)?;
        }
        if purpose == PublicationPurpose::Content {
            self.require(&view.0, acyclic_fs::access::Action::Read, file.path(), io)?;
        }
        Ok(view)
    }
}

async fn verify_upload_metadata<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    original: &acyclic_fs::Generation<A, O>,
    recorded: &(FileRef, Option<GenerationRef>),
    receipt: &[u8],
    io: &PreparationIo,
) -> Result<()> {
    let canonical = io.encode(
        &(1_u8, (&recorded.0, &recorded.1)),
        io.remaining()?.bytes_encoded,
        true,
    )?;
    let hold = io.guard_bytes(&canonical)?;
    let canonical = (canonical, hold);
    if canonical.0.as_ref() != receipt {
        return Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Conflict,
            "original upload receipt is not canonical",
        ));
    }
    let metadata_key = content_metadata_key_io(recorded.0.volume(), recorded.0.path(), Some(io))?;
    let _hold = io.guard_owned(metadata_key.retained_bytes() as u64)?;
    let metadata =
        read_file_annotation(original, recorded.0.path(), &metadata_key, None, Some(io)).await?;
    let _metadata_hold = metadata
        .as_ref()
        .map(|bytes| io.guard_bytes(bytes))
        .transpose()?;
    if metadata.as_deref() != Some(receipt) {
        return Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Conflict,
            "original upload metadata differs from its canonical receipt",
        ));
    }
    Ok(())
}

fn decode_publication_receipt<'a>(
    bytes: &[u8],
    io: &'a PreparationIo,
) -> Result<((FileRef, Option<GenerationRef>), DecodeReservation<'a>)> {
    let ((version, record), hold) = io.decode(
        PhantomData::<(u8, (FileRef, Option<GenerationRef>))>,
        bytes,
        crate::wire::ErrorCode::Storage,
    )?;
    if version != 1 {
        return Err(io.invalid("unsupported upload publication evidence"));
    }
    Ok((record, hold))
}

/// Decodes the sole versioned metadata format; no legacy workspace receipt fallback.
pub(in crate::filesystem) fn decode_content_metadata<'a>(
    bytes: &[u8],
    io: &'a PreparationIo,
) -> Result<(FileRef, DecodeReservation<'a>)> {
    let ((file, expected), mut hold) = decode_publication_receipt(bytes, io)?;
    drop(expected);
    hold.substitute(file.logical_owned_bytes()?)?;
    Ok((file, hold))
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    async fn recovered_upload<'a>(
        &self,
        original: &acyclic_fs::Generation<A, O>,
        mut recorded: ((FileRef, Option<GenerationRef>), DecodeReservation<'a>),
        request: &PublicationLookup<'_>,
        io: &'a PreparationIo,
    ) -> Result<(FileRef, Option<crate::conversation::Custody>)> {
        let version = bounded_io::hex_key(&original.id().to_bytes(), Some(io))?;
        let version_hold = io.guard_owned(version.capacity() as u64)?;
        recorded
            .0
            .0
            .pin_version(version, &mut Some(&mut io.history_io()))?;
        let bytes = self
            .read_retained_file(
                original,
                &recorded.0.0,
                request
                    .maximum_bytes
                    .map_or(recorded.0.0.descriptor().byte_length(), |maximum| {
                        maximum.min(recorded.0.0.descriptor().byte_length())
                    }),
                Some(io),
            )?
            .await?;
        bounded_io::retain(original, Some(io)).await?;
        let version_bytes = version_hold.bytes();
        version_hold.into_retained()?;
        recorded.1.adopt(version_bytes)?;
        drop(recorded.0.1.take());
        recorded.1.substitute(recorded.0.0.logical_owned_bytes()?)?;
        recorded.1.into_retained()?;
        // The payload carries its read charge for as long as it is held.
        let payload = (request.purpose == PublicationPurpose::Content).then_some(bytes);
        Ok((self.seal_publication(recorded.0.0, Some(io))?, payload))
    }
}
