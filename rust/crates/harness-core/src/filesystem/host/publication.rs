use super::*;
use std::marker::PhantomData;

/// The action decided against the original authenticated publication cut.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(rename_all = "snake_case")]
pub(in crate::filesystem) enum PublicationAction {
    Create,
    Write,
}

impl PublicationAction {
    pub(in crate::filesystem) const fn policy(self) -> acyclic_fs::access::Action {
        match self {
            Self::Create => acyclic_fs::access::Action::Create,
            Self::Write => acyclic_fs::access::Action::Write,
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub(in crate::filesystem) struct UploadReceipt {
    pub version: u8,
    pub file: FileRef,
    pub expected_generation: Option<GenerationRef>,
    pub action: PublicationAction,
}

#[derive(Serialize)]
struct UploadReceiptRef<'a> {
    pub version: u8,
    pub file: &'a FileRef,
    pub expected_generation: Option<&'a GenerationRef>,
    pub action: PublicationAction,
}

impl UploadReceipt {
    pub(in crate::filesystem) fn validate(&self, io: Option<&PreparationIo>) -> Result<()> {
        if self.version != 1 {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "unsupported upload receipt version",
            ));
        }
        crate::executor::with_history_io(io, |work| self.file.validate(work))
    }
}

#[derive(Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct ReplayAction<'a> {
    version: u8,
    action: PublicationAction,
    #[serde(borrow, rename = "file")]
    _file: &'a serde_json::value::RawValue,
    #[serde(borrow, rename = "expected_generation")]
    _expected_generation: Option<&'a serde_json::value::RawValue>,
}

/// The subsequent canonical request comparison validates the complete original
/// file and source identity. Borrow them here instead of copying that graph.
pub(in crate::filesystem) fn upload_replay_action(
    bytes: &[u8],
    io: Option<&PreparationIo>,
) -> Result<PublicationAction> {
    let (record, _hold) = match io {
        Some(io) => {
            let (record, hold) = io.decode_seed(
                PhantomData::<ReplayAction<'_>>,
                bytes,
                crate::executor::json::JsonValueSchema::None,
                crate::tool::TOOL_SERDE_DIAGNOSTIC_LABEL_BYTES,
                crate::wire::ErrorCode::Storage,
            )?;
            (record, Some(hold))
        }
        None => (
            crate::executor::json::json_from_slice_bounded(
                PhantomData::<ReplayAction<'_>>,
                bytes,
                crate::executor::json::JsonBounds {
                    maximum_bytes: bytes.len() as u64,
                    maximum_items: u64::MAX,
                    maximum_depth: crate::executor::json::RECORD_MAXIMUM_DEPTH,
                },
                None,
                Some(crate::executor::json::JsonValueSchema::None),
                &mut |_| Ok(()),
            )?,
            None,
        ),
    };
    if record.version != 1 {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "unsupported upload receipt version",
        ));
    }
    Ok(record.action)
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
    pub maximum_bytes: u64,
    pub internal_class: Option<InternalContentClass>,
    pub purpose: PublicationPurpose,
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
        let workspace_ref = bounded_io::content_workspace(&self.provider, volume, Some(io))?;
        let hold = io.guard_owned(workspace_ref.logical_owned_bytes()?)?;
        let workspace_ref = (workspace_ref, hold);
        let root = self.open(&workspace_ref.0, Some(io)).await?;
        let operation = bounded_io::operation_key(key, Some(io))?;
        let Some((workspace, binding)) = self
            .content_operation_workspace(&root, operation, internal_class.is_some(), io)
            .await?
        else {
            return Ok(None);
        };
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
            content_receipt_path_io(receipt_identity, internal_class.is_some(), true, Some(io))?;
        let hold = io.guard_owned(receipt_path.capacity() as u64)?;
        let receipt_path = (receipt_path, hold);
        let receipt =
            read_generation(&original, &receipt_path.0, UPLOAD_RECORD_BYTES, Some(io)).await?;
        let hold = io.guard_bytes(&receipt)?;
        let receipt = (receipt, hold);
        let recorded = io.decode(
            PhantomData::<UploadReceipt>,
            &receipt.0,
            crate::wire::ErrorCode::Storage,
        )?;
        recorded.0.validate(Some(io))?;
        let version = bounded_io::hex_key(&operation.into_bytes(), Some(io))?;
        let _version_hold = io.guard_owned(version.capacity() as u64)?;
        if recorded.0.file.volume() != volume
            || recorded.0.file.version() != version
            || expected_path.is_some_and(|path| path != recorded.0.file.path())
        {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "original upload metadata differs from requested identity",
            ));
        }
        InternalContentClass::require_path(internal_class, recorded.0.file.path(), Some(io))?;
        let view = self
            .receipt_access(
                &workspace,
                original.id(),
                &request,
                &recorded.0.file,
                recorded.0.action,
                io,
            )
            .await?;
        let root_view = if workspace.id() != root.id() {
            Some(
                self.receipt_access(
                    &root,
                    binding
                        .as_ref()
                        .ok_or_else(|| io.invalid("missing content workspace binding"))?
                        .id(),
                    &request,
                    &recorded.0.file,
                    recorded.0.action,
                    io,
                )
                .await?,
            )
        } else {
            None
        };
        if let Some(binding) = binding {
            bounded_io::retain(&binding, Some(io)).await?;
        }
        verify_upload_metadata(&original, &recorded.0, &receipt.0, io).await?;
        let recovered = self
            .recovered_upload(&original, recorded, &request, io)
            .await?;
        super::super::access::require_current(&workspace, &view.0, io).await?;
        if let Some(view) = root_view {
            super::super::access::require_current(&root, &view.0, io).await?;
        }
        Ok(Some(recovered))
    }
    async fn receipt_access<'a>(
        &self,
        workspace: &Workspace<A, O>,
        generation: acyclic_fs::GenerationId,
        request: &PublicationLookup<'_>,
        file: &FileRef,
        action: PublicationAction,
        io: &'a PreparationIo,
    ) -> Result<super::super::access::CompiledAccess<'a>> {
        let PublicationLookup {
            scope,
            volume,
            ceiling,
            purpose,
            ..
        } = *request;
        // Recovery of a caller-selected path returns the original write receipt.
        // Discovery of an unknown path still requires directory disclosure.
        let write_receipt = purpose == PublicationPurpose::WriteReceipt;
        if write_receipt && request.expected_path.is_none() {
            return Err(io.invalid("write receipt recovery requires its original path"));
        }
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
            super::super::access::require(&view.0, action.policy(), file.path(), io)?;
        } else {
            super::super::access::require(
                &view.0,
                acyclic_fs::access::Action::List,
                file.path(),
                io,
            )?;
        }
        if purpose == PublicationPurpose::Content {
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

pub(in crate::filesystem) fn encode_upload_receipt(
    file: &FileRef,
    expected_generation: Option<&GenerationRef>,
    action: PublicationAction,
    io: Option<&PreparationIo>,
) -> Result<acyclic_stream::OwnedBytes> {
    let receipt = UploadReceiptRef {
        version: 1,
        file,
        expected_generation,
        action,
    };
    match io {
        Some(io) => io.encode(&receipt, io.remaining()?.bytes_encoded, true),
        None => crate::contract::canonical_json_bytes(&receipt).map(Into::into),
    }
}

async fn verify_upload_metadata<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    original: &acyclic_fs::Generation<A, O>,
    recorded: &UploadReceipt,
    receipt: &acyclic_stream::OwnedBytes,
    io: &PreparationIo,
) -> Result<()> {
    let canonical = io.encode(recorded, UPLOAD_RECORD_BYTES, true)?;
    let hold = io.guard_bytes(&canonical)?;
    let canonical = (canonical, hold);
    if canonical.0.as_ref() != receipt.as_ref() {
        return Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Conflict,
            "original upload receipt is not canonical",
        ));
    }
    let metadata_path = content_metadata_path_io(recorded.file.path(), true, Some(io))?;
    let hold = io.guard_owned(metadata_path.capacity() as u64)?;
    let metadata_path = (metadata_path, hold);
    let metadata =
        read_generation(original, &metadata_path.0, UPLOAD_RECORD_BYTES, Some(io)).await?;
    let hold = io.guard_bytes(&metadata)?;
    let metadata = (metadata, hold);
    if metadata.0.as_ref() != canonical.0.as_ref() {
        return Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Conflict,
            "original upload metadata differs from its canonical receipt",
        ));
    }
    Ok(())
}

/// Decode the versioned publication record through the original funded codec.
pub(in crate::filesystem) fn decode_content_metadata<'a>(
    bytes: &[u8],
    io: &'a PreparationIo,
) -> Result<(FileRef, DecodeReservation<'a>)> {
    let (record, mut hold) = io.decode(
        PhantomData::<UploadReceipt>,
        bytes,
        crate::wire::ErrorCode::Storage,
    )?;
    record.validate(Some(io))?;
    let file = record.file;
    drop(record.expected_generation);
    hold.substitute(file.logical_owned_bytes()?)?;
    Ok((file, hold))
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    async fn recovered_upload<'a>(
        &self,
        original: &acyclic_fs::Generation<A, O>,
        mut recorded: (UploadReceipt, DecodeReservation<'a>),
        request: &PublicationLookup<'_>,
        io: &'a PreparationIo,
    ) -> Result<(FileRef, Option<acyclic_stream::OwnedBytes>)> {
        let version = bounded_io::hex_key(&original.id().to_bytes(), Some(io))?;
        let version_hold = io.guard_owned(version.capacity() as u64)?;
        recorded
            .0
            .file
            .pin_version(version, &mut Some(&mut io.history_io()))?;
        let bytes = self
            .read_retained_file(original, &recorded.0.file, request.maximum_bytes, Some(io))?
            .await?;
        let hold = io.guard_bytes(&bytes)?;
        let bytes = (bytes, hold);
        bounded_io::retain(original, Some(io)).await?;
        let version_bytes = version_hold.bytes();
        version_hold.into_retained()?;
        recorded.1.adopt(version_bytes)?;
        drop(recorded.0.expected_generation.take());
        recorded
            .1
            .substitute(recorded.0.file.logical_owned_bytes()?)?;
        recorded.1.into_retained()?;
        let payload = if request.purpose == PublicationPurpose::Content {
            if let Some(hold) = bytes.1 {
                hold.into_retained()?;
            }
            Some(bytes.0)
        } else {
            None
        };
        Ok((self.seal_publication(recorded.0.file, Some(io))?, payload))
    }
}
