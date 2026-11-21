//! Filesystem operations on the preparation context.
use super::*;
use crate::resources::ResourceRef;
use acyclic_fs_work::performance::WorkError;
use {acyclic_fs::Generation, acyclic_fs_format::object_reference::GenerationId};

impl acyclic_fs::facade::FsWork for &PreparationIo {
    type Output<T, E> = (
        std::result::Result<T, E>,
        std::result::Result<(), acyclic_fs::FsError>,
    );
    type OwnedOutput<T, E> = Self::Output<T, E>;
    fn finish_owned<T, E: From<acyclic_fs::FsError>>(
        &self,
        result: std::result::Result<(T, acyclic_fs::kernel::KernelOwnedAllocation), E>,
        accounting: &acyclic_fs::kernel::KernelAccounting<'_>,
    ) -> Self::OwnedOutput<T, E> {
        let result = result.and_then(|(value, allocation)| {
            #[cfg(target_arch = "wasm32")]
            let _ = allocation;
            #[cfg(not(target_arch = "wasm32"))]
            if let acyclic_fs::kernel::KernelOwnedAllocation::NativeOriginal(mut allocation) =
                allocation
            {
                let PreparationIo::Native { custody, .. } = self else {
                    drop(value);
                    return Err(E::from(acyclic_fs::FsError::NativeKernel(
                        acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                    )));
                };
                if let Err(cause) = custody
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .absorb(&mut allocation)
                {
                    drop(value);
                    return Err(E::from(acyclic_fs::FsError::from(cause)));
                }
            }
            Ok(value)
        });
        self.finish(result, accounting)
    }
    fn accounting<'a>(
        &'a self,
        cancellation: &'a CancellationToken,
    ) -> acyclic_fs::kernel::KernelAccounting<'a> {
        use acyclic_fs::kernel::KernelAccounting;
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            PreparationIo::Native { work, .. } => KernelAccounting::native(work, cancellation),
            PreparationIo::Owned { .. } => KernelAccounting::Standalone {
                allocations: Default::default(),
                work: Default::default(),
                budget: self
                    .remaining()
                    .unwrap_or_else(|_| acyclic_fs_work::performance::work_budget(0, 0)),
                cancellation,
                construction: Default::default(),
            },
        }
    }
    fn finish_with<T, E>(
        &self,
        result: std::result::Result<T, E>,
        accounting: &acyclic_fs::kernel::KernelAccounting<'_>,
        _: impl FnOnce(acyclic_fs::FsError) -> E,
    ) -> Self::Output<T, E> {
        use acyclic_fs::kernel::KernelAccounting;
        let work = match accounting {
            KernelAccounting::Standalone { work, .. } => *work,
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => return (result, Ok(())),
        };
        let recorded = self
            .spent(work)
            .and_then(|()| match (result.is_ok(), self) {
                (true, PreparationIo::Owned { budget, .. }) => self
                    .receipt()
                    .map_err(acyclic_fs::FsError::from)?
                    .verify(*budget)
                    .map_err(Into::into),
                _ => Ok(()),
            });
        (result, recorded)
    }
}

impl PreparationIo {
    /// Retain returned backing on the same preparation owner. Native bytes
    /// already carry their original allocation; standalone reads must retain
    /// capacity after their temporary filesystem accounting frame retires.
    pub(crate) fn retain_read_bytes(
        &self,
        bytes: acyclic_stream::OwnedBytes,
    ) -> Result<Charged<acyclic_stream::OwnedBytes>> {
        let capacity = if self.original_native_work().is_none() {
            u64::try_from(bytes.retained_capacity().ok_or_else(|| {
                crate::executor::stock_boundary(
                    Some(self),
                    crate::wire::ErrorCode::Unsupported,
                    "content producer does not expose its owned capacity",
                )
            })?)
            .map_err(|_| self.invalid("content capacity exceeds the ledger"))?
        } else {
            0
        };
        let charge = self.reserve_decode(capacity)?.take_custody()?;
        Ok(Charged::new(bytes, charge))
    }

    /// Interpret only the original missing-path result as optional absence.
    /// Missing authority and every other failure remain refusals, even when
    /// their public code is also NotFound. Original accounting still settles
    /// before an absent result can be returned.
    #[doc(hidden)]
    pub fn sdk_if_present<T>(
        &self,
        (result, recorded): (
            std::result::Result<T, WorkspaceError>,
            std::result::Result<(), acyclic_fs::FsError>,
        ),
    ) -> Result<Option<T>> {
        let result = match result {
            Ok(value) => Ok(Some(value)),
            Err(WorkspaceError::NotFound) => Ok(None),
            Err(error) => Err(error),
        };
        self.sdk((result, recorded))
    }

    /// Settles the incremental work of an owned filesystem cursor. Native
    /// cursors already charge the original owner directly; standalone cursors
    /// report cumulative receipts, so only their new work is added here.
    pub(crate) fn cursor_receipt<T, E: Into<Error>>(
        &self,
        result: acyclic_fs_work::performance::MeasuredResult<OperationReceipt<T>, E>,
        previous: &mut WorkCounters,
    ) -> Result<T> {
        let cumulative = match &result {
            Ok(receipt) => receipt.work,
            Err(failure) => **failure
                .work
                .as_ref()
                .map_err(|error| acyclic_fs::FsError::from(*error))?,
        };
        if matches!(self, Self::Owned { .. }) {
            let delta = cumulative
                .checked_sub_cumulative(previous)
                .map_err(acyclic_fs::FsError::from)?;
            self.spent(delta)?;
            self.remaining()?;
        }
        *previous = cumulative;
        result
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error.into())
    }

    #[doc(hidden)]
    pub fn sdk<T, E: Into<Error>>(
        &self,
        (result, recorded): (
            std::result::Result<T, E>,
            std::result::Result<(), acyclic_fs::FsError>,
        ),
    ) -> Result<T> {
        recorded?;
        result.map_err(Into::into)
    }
}

pub(crate) fn generation_ref(
    provider: &ProviderRef,
    generation: GenerationId,
    io: Option<&PreparationIo>,
) -> Result<crate::resources::GenerationRef> {
    crate::executor::with_history_io(io, |work| {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let provider = crate::core::copy_provider(provider, &mut quota)?;
        let key = crate::core::copy_copy_vector(&generation.to_bytes(), &mut quota)?;
        let resource = ResourceRef::new(
            crate::resources::ResourceKind::Generation,
            provider,
            key,
            None,
            quota.work(),
        )?;
        let reference = crate::resources::GenerationRef::from_resource(resource, quota.work())?;
        quota.transfer();
        Ok(reference)
    })
}

/// The resource format of a private namespace cut's `GenerationRef`.
pub(in crate::filesystem) const PRIVATE_CUT_FORMAT: &str = "lazy-v1";

/// FileRef version format v1 names the exact cut a file's bytes were read at,
/// as a lossless spelling of that cut's `GenerationRef` within the file's
/// provider. An authored generation is its canonical id in lowercase hex; a
/// private namespace cut is `lazy-v1:` followed by its key in lowercase hex.
/// Every cut has exactly one spelling, so equal references name equal bytes.
pub fn file_version(
    cut: &crate::resources::GenerationRef,
    io: Option<&PreparationIo>,
) -> Result<String> {
    let key = super::hex_key(cut.as_resource().key(), io)?;
    match cut.as_resource().version() {
        None => Ok(key),
        Some(format) => crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("{format}:{key}"), work)
        }),
    }
}

pub(in crate::filesystem) enum FileVersion<'a> {
    Authored(GenerationId),
    Private(&'a str),
}

fn invalid_file_version(io: Option<&PreparationIo>) -> Error {
    stock_boundary(
        io,
        crate::wire::ErrorCode::Invalid,
        "file version is not a canonical Filesystem generation",
    )
}

/// Validates the exact cut named by the FileRef version without allocating
/// the wire representation of the reference.
pub(in crate::filesystem) fn parse_file_version<'a>(
    version: &'a str,
    io: Option<&PreparationIo>,
) -> Result<FileVersion<'a>> {
    if let Some(io) = io {
        io.charge(WorkCounters {
            source_bytes_read: version.len() as u64,
            items_examined: version.len() as u64,
            ..Default::default()
        })?;
    }
    let invalid = || invalid_file_version(io);
    let (private, key) = match version.split_once(':') {
        None => (false, version),
        Some((PRIVATE_CUT_FORMAT, key)) => (true, key),
        Some(_) => return Err(invalid()),
    };
    if key.is_empty()
        || key.len() % 2 != 0
        || !key
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(invalid());
    }
    if private {
        return Ok(FileVersion::Private(key));
    }
    let mut bytes = [0; acyclic_fs_format::object_reference::ObjectId::ENCODED_LEN];
    let decoded = bytes.get_mut(..key.len() / 2).ok_or_else(invalid)?;
    hex::decode_to_slice(key, decoded).map_err(|_| invalid())?;
    let generation = GenerationId::from_bytes(decoded).ok_or_else(invalid)?;
    Ok(FileVersion::Authored(generation))
}

/// Converts a canonical FileRef version to a `GenerationRef` for the wire.
pub(in crate::filesystem) fn version_generation(
    provider: &ProviderRef,
    version: &str,
    io: Option<&PreparationIo>,
) -> Result<crate::resources::GenerationRef> {
    let key = match parse_file_version(version, io)? {
        FileVersion::Authored(generation) => return generation_ref(provider, generation, io),
        FileVersion::Private(key) => key,
    };
    crate::executor::with_history_io(io, |work| {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let provider = crate::core::copy_provider(provider, &mut quota)?;
        let mut decoded = crate::core::reserve_copy_vector(key.len() / 2, &mut quota)?;
        decoded.resize(key.len() / 2, 0);
        hex::decode_to_slice(key, &mut decoded).map_err(|_| invalid_file_version(io))?;
        let version = crate::core::copy_text(PRIVATE_CUT_FORMAT, &mut quota)?;
        let resource = ResourceRef::new(
            crate::resources::ResourceKind::Generation,
            provider,
            decoded,
            Some(version),
            quota.work(),
        )?;
        let reference = crate::resources::GenerationRef::from_resource(resource, quota.work())?;
        quota.transfer();
        Ok(reference)
    })
}

pub(in crate::filesystem) async fn retain<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &Generation<A, O>,
    io: Option<&PreparationIo>,
) -> std::result::Result<(), acyclic_fs::LazyWorkspaceError> {
    #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
    acyclic_differential::record(("retain", 0u64));
    use acyclic_fs::LazyWorkspaceError as LazyError;
    const PREFIX: &[u8] = b"harness-generation-";
    let mut storage =
        [0_u8; PREFIX.len() + 2 * acyclic_fs_format::object_reference::ObjectId::ENCODED_LEN];
    let buffer = &mut storage;
    let identity = move |work: &mut acyclic_fs::kernel::KernelAccounting<'_>| {
        let buffer = buffer;
        let reference = generation.id().to_bytes();
        let hex_bytes = reference.len() * 2;
        let label_bytes = PREFIX.len() + hex_bytes;
        work.charge(WorkCounters {
            bytes_encoded: hex_bytes as u64,
            bytes_copied: label_bytes as u64,
            ..Default::default()
        })?;
        let (prefix, tail) = buffer.split_at_mut(PREFIX.len());
        prefix.copy_from_slice(PREFIX);
        let key = tail.get_mut(..hex_bytes).ok_or(WorkError::Overflow)?;
        hex::encode_to_slice(reference.as_slice(), key)
            .map_err(|error| WorkspaceError::Path(error.to_string()))?;
        let label = buffer.get(..label_bytes).ok_or(WorkError::Overflow)?;
        std::str::from_utf8(label).map_err(|error| WorkspaceError::Path(error.to_string()))
    };
    let Some(io) = io else {
        return generation
            .retain_if_unpublished(identity, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|_| ())
            .map_err(|failure| LazyError::WorkspaceCause(failure.error));
    };
    let (result, recorded) = generation
        .retain_if_unpublished(identity, io, io.cancellation())
        .await;
    match result {
        Err(source) => Err(LazyError::WorkspaceCause(source)),
        Ok(_) => recorded.map_err(LazyError::Filesystem),
    }
}

pub(in crate::filesystem) fn operation_key(
    key: &IdempotencyKey,
    io: Option<&PreparationIo>,
) -> Result<acyclic_fs::IdempotencyKey> {
    let hashed = WorkCounters {
        bytes_hashed: key.as_str().len() as u64,
        ..Default::default()
    };
    if let Some(io) = io {
        io.preflight(hashed)?;
    }
    let key = crate::filesystem::filesystem_key(key);
    if let Some(io) = io {
        io.charge(hashed)?;
    }
    Ok(key)
}
/// Resolves the workspace reference that holds a volume's content.
pub fn content_workspace(
    provider: &ProviderRef,
    volume: &VolumeRef,
    io: Option<&PreparationIo>,
) -> Result<WorkspaceRef> {
    let name = crate::executor::with_history_io(io, |work| volume.storage_name(work))?;
    let provider = match io {
        Some(io) => provider.clone_with_io(io)?,
        None => provider.clone(),
    };
    WorkspaceRef::new(provider, name.into_bytes(), None)
}
