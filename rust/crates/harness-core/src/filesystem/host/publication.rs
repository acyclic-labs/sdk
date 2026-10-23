use super::*;
use std::marker::PhantomData;

#[derive(Clone, Copy)]
pub(in crate::filesystem) struct PublicationLookup<'a> {
    pub volume: &'a VolumeRef,
    pub key: &'a IdempotencyKey,
    pub expected_path: Option<&'a str>,
    pub maximum_bytes: u64,
    pub internal_class: Option<InternalContentClass>,
    pub read_content: bool,
    pub scope: &'a Scope,
    pub ceiling: Option<&'a crate::Capabilities>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(in crate::filesystem) async fn content_for_operation_record_io(
        &self,
        request: PublicationLookup<'_>,
        io: Option<&PreparationIo>,
    ) -> Result<Option<(FileRef, Option<acyclic_stream::OwnedBytes>)>> {
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
            internal_class,
            ..
        } = request;
        volume.validate(&mut Some(&mut io.history_io()))?;
        if volume.provider() != &self.provider {
            return Err(io.invalid("resource belongs to another Filesystem provider"));
        }
        if let Some(path) = expected_path {
            crate::conversation::validate_content_path(path, &mut Some(&mut io.history_io()))?;
            InternalContentClass::require_path(internal_class, path, Some(io))?;
        }
        let workspace_ref = fork_preparer_io::content_workspace(&self.provider, volume, Some(io))?;
        let hold = io.guard_owned(workspace_ref.logical_owned_bytes()?)?;
        let workspace_ref = (workspace_ref, hold);
        let workspace = self.open(&workspace_ref.0, Some(io)).await?;
        let operation = fork_preparer_io::operation_key(key, Some(io))?;
        let Some(original) = io.sdk(
            workspace
                .operation_generation(operation, io, io.cancellation())
                .await,
        )?
        else {
            return Ok(None);
        };
        let receipt_identity = if internal_class.is_some() {
            expected_path
                .ok_or_else(|| io.invalid("internal upload recovery requires its original path"))?
        } else {
            key.as_str()
        };
        let receipt_path =
            content_identity_path_io(receipt_identity, true, ".system/harness-uploads/", Some(io))?;
        let hold = io.guard_owned(receipt_path.capacity() as u64)?;
        let receipt_path = (receipt_path, hold);
        let receipt =
            read_generation(&original, &receipt_path.0, UPLOAD_RECORD_BYTES, Some(io)).await?;
        let hold = io.guard_bytes(&receipt)?;
        let receipt = (receipt, hold);
        let recorded = io.decode(
            PhantomData::<(FileRef, Option<GenerationRef>)>,
            &receipt.0,
            crate::wire::ErrorCode::Storage,
        )?;
        recorded.0.0.validate(&mut Some(&mut io.history_io()))?;
        let version = fork_preparer_io::hex_key(&operation.into_bytes(), Some(io))?;
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
        InternalContentClass::require_path(internal_class, recorded.0.0.path(), Some(io))?;
        let view = self
            .receipt_access(&workspace, original.id(), &request, &recorded.0.0, io)
            .await?;
        verify_upload_metadata(&original, &recorded.0, &receipt.0, io).await?;
        let recovered = self
            .recovered_upload(&original, recorded, &request, io)
            .await?;
        super::super::access::require_current(&workspace, &view.0, io).await?;
        Ok(Some(recovered))
    }
    async fn receipt_access<'a>(
        &self,
        workspace: &Workspace<A, O>,
        generation: acyclic_fs::GenerationId,
        request: &PublicationLookup<'_>,
        file: &FileRef,
        io: &'a PreparationIo,
    ) -> Result<super::super::access::CompiledAccess<'a>> {
        let PublicationLookup {
            scope,
            volume,
            ceiling,
            read_content,
            ..
        } = *request;
        // Recovery of a caller-selected path returns the original write receipt.
        // Discovery of an unknown path still requires directory disclosure.
        let write_receipt = !read_content && request.expected_path.is_some();
        if write_receipt {
            for grants in [Some(scope.capabilities()), ceiling].into_iter().flatten() {
                if !crate::core::volume_write_granted_with_work(
                    grants,
                    volume,
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
        )
        .await?;
        if write_receipt {
            if !view
                .0
                .decide(
                    acyclic_fs::access::Action::Create,
                    file.path(),
                    super::super::access::access_now(),
                )
                .allowed
            {
                super::super::access::require(
                    &view.0,
                    acyclic_fs::access::Action::Write,
                    file.path(),
                    io,
                )?;
            }
        } else {
            super::super::access::require(
                &view.0,
                acyclic_fs::access::Action::List,
                file.path(),
                io,
            )?;
        }
        if read_content {
            super::super::access::require(
                &view.0,
                acyclic_fs::access::Action::Read,
                file.path(),
                io,
            )?;
        }
        Ok(view)
    }
}

const UPLOAD_RECORD_BYTES: u64 = 6
    * (crate::conversation::MAX_PATH_BYTES as u64
        + 32 * crate::conversation::MAX_LABEL_BYTES as u64
        + 1024);

async fn verify_upload_metadata<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    original: &acyclic_fs::Generation<A, O>,
    recorded: &(FileRef, Option<GenerationRef>),
    receipt: &acyclic_stream::OwnedBytes,
    io: &PreparationIo,
) -> Result<()> {
    let canonical = io.encode(&(&recorded.0, &recorded.1), UPLOAD_RECORD_BYTES, true)?;
    let hold = io.guard_bytes(&canonical)?;
    let canonical = (canonical, hold);
    if canonical.0.as_ref() != receipt.as_ref() {
        return Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Conflict,
            "original upload receipt is not canonical",
        ));
    }
    let metadata_path = content_metadata_path_io(recorded.0.path(), true, Some(io))?;
    let hold = io.guard_owned(metadata_path.capacity() as u64)?;
    let metadata_path = (metadata_path, hold);
    let metadata =
        read_generation(original, &metadata_path.0, UPLOAD_RECORD_BYTES, Some(io)).await?;
    let hold = io.guard_bytes(&metadata)?;
    let metadata = (metadata, hold);
    let encoded = io.encode(&recorded.0, UPLOAD_RECORD_BYTES, false)?;
    let hold = io.guard_bytes(&encoded)?;
    let encoded = (encoded, hold);
    if encoded.0.as_ref() != metadata.0.as_ref() {
        return Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Conflict,
            "original upload metadata differs from its canonical receipt",
        ));
    }
    Ok(())
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    async fn recovered_upload<'a>(
        &self,
        original: &acyclic_fs::Generation<A, O>,
        mut recorded: ((FileRef, Option<GenerationRef>), DecodeReservation<'a>),
        request: &PublicationLookup<'_>,
        io: &'a PreparationIo,
    ) -> Result<(FileRef, Option<acyclic_stream::OwnedBytes>)> {
        let version = fork_preparer_io::hex_key(&original.id().digest().into_bytes(), Some(io))?;
        let version_hold = io.guard_owned(version.capacity() as u64)?;
        recorded
            .0
            .0
            .pin_version(version, &mut Some(&mut io.history_io()))?;
        let bytes = self
            .read_retained_file(original, &recorded.0.0, request.maximum_bytes, Some(io))
            .await?;
        let hold = io.guard_bytes(&bytes)?;
        let bytes = (bytes, hold);
        fork_preparer_io::retain(original, Some(io)).await?;
        let version_bytes = version_hold.bytes();
        version_hold.into_retained()?;
        recorded.1.adopt(version_bytes)?;
        drop(recorded.0.1.take());
        recorded.1.substitute(recorded.0.0.logical_owned_bytes()?)?;
        recorded.1.into_retained()?;
        let payload = if request.read_content {
            if let Some(hold) = bytes.1 {
                hold.into_retained()?;
            }
            Some(bytes.0)
        } else {
            None
        };
        Ok((recorded.0.0, payload))
    }
}
