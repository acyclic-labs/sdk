use super::*;
use std::marker::PhantomData;

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
            maximum_bytes,
            internal_class,
            read_content,
            scope,
            ceiling,
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
        const RECORD_BYTES: u64 = 6
            * (crate::conversation::MAX_PATH_BYTES as u64
                + 32 * crate::conversation::MAX_LABEL_BYTES as u64
                + 1024);
        let receipt = read_generation(&original, &receipt_path.0, RECORD_BYTES, Some(io)).await?;
        let hold = io.guard_bytes(&receipt)?;
        let receipt = (receipt, hold);
        let mut recorded = io.decode(
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
        // A receipt newly discloses a path and descriptor. Write authority alone
        // cannot disclose either, even when the payload is already cached.
        super::super::access::require_directory_ceiling(
            Some(scope.capabilities()),
            volume,
            recorded
                .0
                .0
                .path()
                .rsplit_once('/')
                .map_or("", |(parent, _)| parent),
            Some(io),
        )?;
        super::super::access::require_directory_ceiling(
            ceiling,
            volume,
            recorded
                .0
                .0
                .path()
                .rsplit_once('/')
                .map_or("", |(parent, _)| parent),
            Some(io),
        )?;
        let label = crate::core::filesystem_scope_label(
            scope.capabilities(),
            &mut Some(&mut io.history_io()),
        )?;
        let view = crate::filesystem::access::access_view(
            &workspace,
            scope,
            Some(original.id()),
            super::super::access::capability_payload(label, VolumeOperation::Read, io)?,
            ceiling,
            io,
        )
        .await?;
        super::super::access::require(
            &view.0,
            acyclic_fs::access::Action::List,
            recorded.0.0.path(),
            io,
        )?;
        if read_content {
            super::super::access::require(
                &view.0,
                acyclic_fs::access::Action::Read,
                recorded.0.0.path(),
                io,
            )?;
        }
        let canonical = io.encode(&(&recorded.0.0, &recorded.0.1), RECORD_BYTES, true)?;
        let hold = io.guard_bytes(&canonical)?;
        let canonical = (canonical, hold);
        if canonical.0.as_ref() != receipt.0.as_ref() {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "original upload receipt is not canonical",
            ));
        }
        let metadata_path = content_metadata_path_io(recorded.0.0.path(), true, Some(io))?;
        let hold = io.guard_owned(metadata_path.capacity() as u64)?;
        let metadata_path = (metadata_path, hold);
        let metadata = read_generation(&original, &metadata_path.0, RECORD_BYTES, Some(io)).await?;
        let hold = io.guard_bytes(&metadata)?;
        let metadata = (metadata, hold);
        let encoded = io.encode(&recorded.0.0, RECORD_BYTES, false)?;
        let hold = io.guard_bytes(&encoded)?;
        let encoded = (encoded, hold);
        if encoded.0.as_ref() != metadata.0.as_ref() {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "original upload metadata differs from its canonical receipt",
            ));
        }
        let absolute = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{}", recorded.0.0.path()), work)
        })?;
        let hold = io.guard_owned(absolute.capacity() as u64)?;
        let absolute = (absolute, hold);
        let bytes = read_generation(&original, &absolute.0, maximum_bytes, Some(io)).await?;
        let hold = io.guard_bytes(&bytes)?;
        let bytes = (bytes, hold);
        recorded
            .0
            .0
            .descriptor()
            .verify(&bytes.0, &mut Some(&mut io.history_io()))?;
        fork_preparer_io::retain(&original, Some(io)).await?;
        let version = fork_preparer_io::hex_key(&original.id().digest().into_bytes(), Some(io))?;
        let version_hold = io.guard_owned(version.capacity() as u64)?;
        recorded
            .0
            .0
            .pin_version(version, &mut Some(&mut io.history_io()))?;
        let version_bytes = version_hold.bytes();
        version_hold.into_retained()?;
        recorded.1.adopt(version_bytes)?;
        drop(recorded.0.1.take());
        recorded.1.substitute(recorded.0.0.logical_owned_bytes()?)?;
        recorded.1.into_retained()?;
        let payload = if read_content {
            if let Some(hold) = bytes.1 {
                hold.into_retained()?;
            }
            Some(bytes.0)
        } else {
            None
        };
        super::super::access::require_current(&workspace, &view.0, io).await?;
        Ok(Some((recorded.0.0, payload)))
    }
}
