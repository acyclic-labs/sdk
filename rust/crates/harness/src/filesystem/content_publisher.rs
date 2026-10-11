use super::{FilesystemHost, PreparationIo, UploadRecord};
use super::fork_preparer::FilesystemRootBootstrapAllocation;
use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{ContentGrant, ContentPublisher, FileRef, VolumeOperation, VolumeRef},
    core::{AuthorityVerifier, Scope},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;

/// Owner-bound content staging through the existing Filesystem publication path.
pub struct FilesystemContentPublisher<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    grant: ContentGrant,
    maximum_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    _native_metadata: Option<acyclic_fs::NativeAllocation>,
}

impl<A, O> FilesystemContentPublisher<A, O> {
    /// Binds original write authority and a positive finite content limit.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        maximum_bytes: u64,
    ) -> Result<Self> {
        Self::new_with_io(host, volume, verifier, scope, maximum_bytes, None)
    }

    pub(crate) fn new_with_io(
        host: Arc<FilesystemHost<A, O>>, volume: VolumeRef,
        verifier: &AuthorityVerifier, scope: &Scope, maximum_bytes: u64,
        io: Option<&super::PreparationIo>,
    ) -> Result<Self> {
        Self::validate_limit(maximum_bytes, io)?;
        let grant = if let Some(io) = io {
            let mut observer = io.history_io();
            ContentGrant::verify_with_work(verifier, scope, &volume, VolumeOperation::Write,
                maximum_bytes, &mut Some(&mut observer))?
        } else { ContentGrant::verify(verifier, scope, &volume, VolumeOperation::Write)? };
        Ok(Self {
            host,
            volume,
            grant,
            maximum_bytes,
            #[cfg(not(target_arch = "wasm32"))]
            _native_metadata: None,
        })
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        self.volume.logical_owned_bytes()?.checked_add(self.grant.logical_owned_bytes()?)
            .ok_or_else(|| Error::Invalid("content publisher ownership overflow".into()))
    }

    fn validate_limit(maximum_bytes: u64, io: Option<&PreparationIo>) -> Result<()> {
        if maximum_bytes == 0 || maximum_bytes == u64::MAX {
            return Err(super::fork_preparer_io::invalid(io,
                "content publication limit must be positive and finite"));
        }
        Ok(())
    }

    /// Copies only newly owned configuration after the original write proof.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_with_native_work(
        host: Arc<FilesystemHost<A, O>>, volume: &VolumeRef,
        verifier: &AuthorityVerifier, scope: &Scope, maximum_bytes: u64,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let io = PreparationIo::from_native_work(work)?;
        Self::validate_limit(maximum_bytes, Some(&io))?;
        let grant = ContentGrant::verify_with_io(
            verifier, scope, volume, VolumeOperation::Write, &io,
        )?;
        let volume = volume.clone_with_io(&io)?;
        io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let retained = io.owned_bytes()?;
        let _native_metadata = Some(io.into_native_custody(retained)?);
        Ok(Self { host, volume, grant, maximum_bytes, _native_metadata })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Arc<Self>> {
        if !self._native_metadata.as_ref().is_some_and(|custody| work.shares_ledger(custody.work())) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native publisher sharing requires original metadata custody" });
        }
        let io = PreparationIo::from_native_work(work)?;
        io.allocation_attempt(std::mem::size_of::<Self>() as u64)?;
        Ok(Arc::new(self))
    }
}

impl<A, O> ContentPublisher for FilesystemContentPublisher<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    fn stage_once<'a>(
        &'a self,
        operation_id: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media_type: &'a str,
        display_name: &'a str,
    ) -> BoxProviderFuture<'a, Result<FileRef>> {
        Box::pin(async move {
            self.stage_once_with_io(operation_id, path, bytes, media_type,
                display_name, None, None, None).await
        })
    }

    fn observe_staged_once<'a>(
        &'a self, operation_id: OperationId, maximum_bytes: u64,
    ) -> BoxProviderFuture<'a, Result<Option<FileRef>>> {
        Box::pin(async move {
            self.observe_once_with_io(operation_id, maximum_bytes, None).await
        })
    }

    #[allow(clippy::too_many_arguments, reason = "complete once-publication identity and finite caller budget")]
    fn stage_once_measured<'a>(
        &'a self, operation_id: OperationId, path: &'a str, bytes: &'a [u8],
        media_type: &'a str, display_name: &'a str, budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<FileRef>, Error>> {
        Box::pin(async move {
            let io = PreparationIo::from_budget(budget, cancellation)
                .map_err(acyclic_fs::OperationFailure::before_work)?;
            let result = self.stage_once_with_io(operation_id, path, bytes, media_type, display_name, Some(&io), None, None).await;
            match result {
                Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
                Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
            }
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_stage(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        let io = PreparationIo::from_native_work(work)?;
        super::fork_preparer_io::item(Some(&io), 0)?;
        self.volume.validate_with_native_work(work)?;
        self.grant.require_with_io(&self.volume, VolumeOperation::Write, Some(&io))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn stage_once_with_native_work<'a>(
        &'a self, operation_id: OperationId, path: &'a str, bytes: &'a [u8],
        media_type: &'a str, display_name: &'a str, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> crate::Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let result = self.stage_once_with_io(operation_id, path, bytes, media_type, display_name, Some(&io), None, None).await;
            let file = result?;
            let retained = file.logical_owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((file, custody))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn stage_at_with_native_work<'a>(
        &'a self, operation_id: OperationId, source: &'a FileRef, bytes: &'a [u8],
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let file = self.stage_at_with_io(operation_id, source, bytes, Some(&io), None, None).await?;
            let custody = io.into_native_custody(file.logical_owned_bytes()?)?;
            Ok((file, custody))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn observe_staged_once_with_native_work<'a>(
        &'a self, operation_id: OperationId, maximum_bytes: u64,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<FileRef>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let file = self.observe_once_with_io(operation_id, maximum_bytes, Some(&io)).await?;
            let retained = file.as_ref().map(UploadRecord::logical_owned_bytes).transpose()?.unwrap_or(0);
            let custody = io.into_native_custody(retained)?;
            Ok((file, custody))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn observe_received_copy_with_native_work<'a>(
        &'a self, proof: &'a crate::runtime::ReceivedCopyReadWitness<'_>,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<FileRef>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            self.grant.require_with_io(&self.volume, VolumeOperation::Read, Some(&io))?;
            let file = self.host.received_copy_record_with_io(
                &self.volume, proof, self.maximum_bytes, &io,
            ).await?;
            let retained = file.as_ref().map(UploadRecord::logical_owned_bytes).transpose()?.unwrap_or(0);
            let custody = io.into_native_custody(retained)?;
            Ok((file, custody))
        })
    }

    fn observe_staged_once_measured<'a>(
        &'a self, operation_id: OperationId, maximum_bytes: u64,
        budget: acyclic_fs::WorkBudget, cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Option<FileRef>>, Error>> {
        Box::pin(async move {
            let io = PreparationIo::from_budget(budget, cancellation)
                .map_err(acyclic_fs::OperationFailure::before_work)?;
            let result = self.observe_once_with_io(operation_id, maximum_bytes, Some(&io)).await;
            match result {
                Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
                Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
            }
        })
    }

    fn stage_at<'a>(
        &'a self,
        operation_id: OperationId,
        source: &'a FileRef,
        bytes: &'a [u8],
    ) -> BoxProviderFuture<'a, Result<FileRef>> {
        Box::pin(async move {
            self.stage_at_with_io(operation_id, source, bytes, None, None, None).await
        })
    }
    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media_type: &'a str,
        display_name: &'a str,
    ) -> BoxProviderFuture<'a, Result<FileRef>> {
        Box::pin(async move {
            let retry = IdempotencyKey::new(format!(
                "filesystem-upload:{operation_id}:{}",
                blake3::hash(path.as_bytes()).to_hex()
            ))?;
            self.stage_with_retry(path, bytes, media_type, display_name, &retry)
                .await
        })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    FilesystemContentPublisher<A, O>
{
    /// Root-owned lease checks add no authority to this already bound writer.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments, reason = "original complete publication identity and borrowed lease")]
    pub(crate) fn stage_once_with_publication_lease<'a>(
        &'a self, operation_id: OperationId, path: &'a str, bytes: &'a [u8],
        media_type: &'a str, display_name: &'a str,
        lease: &'a dyn crate::resource_preparation::CreatedTaskLease,
        allocation: &'a FilesystemRootBootstrapAllocation<A, O>,
    ) -> BoxProviderFuture<'a, Result<FileRef>> {
        Box::pin(async move {
            self.stage_once_with_io(operation_id, path, bytes, media_type,
                display_name, None, Some(lease), Some(allocation)).await
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn stage_at_with_publication_lease<'a>(
        &'a self, operation_id: OperationId, source: &'a FileRef, bytes: &'a [u8],
        lease: &'a dyn crate::resource_preparation::CreatedTaskLease,
        allocation: &'a FilesystemRootBootstrapAllocation<A, O>,
    ) -> BoxProviderFuture<'a, Result<FileRef>> {
        Box::pin(async move {
            self.stage_at_with_io(operation_id, source, bytes, None, Some(lease), Some(allocation)).await
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments, reason = "original publication identity, finite budget and borrowed lease")]
    pub(crate) fn stage_once_with_publication_lease_measured<'a>(
        &'a self, operation_id: OperationId, path: &'a str, bytes: &'a [u8],
        media_type: &'a str, display_name: &'a str, budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
        lease: &'a dyn crate::resource_preparation::CreatedTaskLease,
        allocation: &'a FilesystemRootBootstrapAllocation<A, O>,
    ) -> BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<FileRef>, Error>> {
        Box::pin(async move {
            let io = PreparationIo::from_budget(budget, cancellation)
                .map_err(acyclic_fs::OperationFailure::before_work)?;
            let result = self.stage_once_with_io(operation_id, path, bytes,
                media_type, display_name, Some(&io), Some(lease), Some(allocation)).await;
            match result {
                Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
                Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
            }
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments, reason = "original publication identity, native ledger and borrowed lease")]
    pub(crate) fn stage_once_with_publication_lease_with_native_work<'a>(
        &'a self, operation_id: OperationId, path: &'a str, bytes: &'a [u8],
        media_type: &'a str, display_name: &'a str,
        work: &'a acyclic_fs::NativeMaterializationWork,
        lease: &'a dyn crate::resource_preparation::CreatedTaskLease,
        allocation: &'a FilesystemRootBootstrapAllocation<A, O>,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let file = self.stage_once_with_io(operation_id, path, bytes,
                media_type, display_name, Some(&io), Some(lease), Some(allocation)).await?;
            let custody = io.into_native_custody(file.logical_owned_bytes()?)?;
            Ok((file, custody))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn stage_at_with_publication_lease_with_native_work<'a>(
        &'a self, operation_id: OperationId, source: &'a FileRef, bytes: &'a [u8],
        work: &'a acyclic_fs::NativeMaterializationWork,
        lease: &'a dyn crate::resource_preparation::CreatedTaskLease,
        allocation: &'a FilesystemRootBootstrapAllocation<A, O>,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let file = self.stage_at_with_io(operation_id, source, bytes,
                Some(&io), Some(lease), Some(allocation)).await?;
            let custody = io.into_native_custody(file.logical_owned_bytes()?)?;
            Ok((file, custody))
        })
    }

    pub(super) async fn observe_once_with_io(
        &self, operation_id: OperationId, maximum_bytes: u64, io: Option<&PreparationIo>,
    ) -> Result<Option<FileRef>> {
        let retry = once_key(operation_id, io)?;
        let retry_hold = io.map(|io| io.guard_owned(retry.as_str().len() as u64)).transpose()?;
        let file = self.host.content_for_operation_impl(
            &self.volume, &self.grant, &retry, None, maximum_bytes.min(self.maximum_bytes), io,
        ).await?;
        drop(retry);
        drop(retry_hold);
        Ok(file)
    }

    pub(super) async fn stage_at_with_io(
        &self, operation_id: OperationId, source: &FileRef, bytes: &[u8],
        io: Option<&PreparationIo>,
        publication_lease: Option<&dyn crate::resource_preparation::CreatedTaskLease>,
        root_allocation: Option<&FilesystemRootBootstrapAllocation<A, O>>,
    ) -> Result<FileRef> {
        validate_publication(source.path(), bytes, source.descriptor().media_type(), source.display_name(), self.maximum_bytes, io)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            source.validate_with_native_work(work)?;
        } else {
            source.validate()?;
        }
        #[cfg(target_arch = "wasm32")]
        source.validate()?;
        if source.volume() != &self.volume {
            #[cfg(not(target_arch = "wasm32"))]
            if io.and_then(PreparationIo::original_native_work).is_some() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "source belongs to another writer volume" });
            }
            return Err(Error::Unauthorized("source belongs to another writer volume".into()));
        }
        let version = source.version();
        #[cfg(not(target_arch = "wasm32"))]
        if version.len() == 64
            && let Some(work) = io.and_then(PreparationIo::original_native_work) {
            crate::conversation::inspect_native_metadata(work, version.len())?;
        }
        if version.len() != 64 || !version.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(super::fork_preparer_io::invalid(io, "file version is not a Filesystem generation"));
        }
        if source.descriptor().byte_length() > 9_007_199_254_740_991 {
            return Err(super::fork_preparer_io::invalid(io, "file byte length exceeds cross-language precision"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let root_generation = root_allocation
            .map(|allocation| allocation.source_generation_for_file_with_io(source, io))
            .transpose()?.flatten();
        #[cfg(target_arch = "wasm32")]
        let root_generation = { let _ = root_allocation; None };
        let owned_generation = if root_generation.is_some() {
            None
        } else {
            Some(self.host.file_generation_with_io(source, io)?)
        };
        let generation_hold = owned_generation.as_ref().and_then(|generation|
            io.map(|io| io.guard_owned(generation.logical_owned_bytes()?))).transpose()?;
        let generation = root_generation.or(owned_generation.as_ref())
            .expect("Root loan basis or authenticated Filesystem generation");
        let retry = replacement_key(operation_id, source.path(), io)?;
        let retry_hold = io.map(|io| io.guard_owned(retry.as_str().len() as u64)).transpose()?;
        let file = self.host.put_content_impl(&self.volume, &self.grant, source.path(), bytes, source.descriptor().media_type(),
            source.display_name(), self.maximum_bytes, &retry, None, Some(generation), io,
            publication_lease, root_allocation.filter(|_| root_generation.is_some())).await?;
        drop(retry);
        drop(retry_hold);
        drop(owned_generation);
        drop(generation_hold);
        Ok(file)
    }

    #[allow(clippy::too_many_arguments, reason = "one canonical complete publication identity")]
    pub(super) async fn stage_once_with_io(
        &self, operation_id: OperationId, path: &str, bytes: &[u8],
        media_type: &str, display_name: &str, io: Option<&PreparationIo>,
        publication_lease: Option<&dyn crate::resource_preparation::CreatedTaskLease>,
        root_allocation: Option<&FilesystemRootBootstrapAllocation<A, O>>,
    ) -> Result<FileRef> {
        validate_publication(path, bytes, media_type, display_name, self.maximum_bytes, io)?;
        #[cfg(not(target_arch = "wasm32"))]
        let expected_generation = root_allocation
            .map(|allocation| allocation.source_generation_with_io(io))
            .transpose()?.flatten();
        #[cfg(target_arch = "wasm32")]
        let expected_generation = { let _ = root_allocation; None };
        let retry = once_key(operation_id, io)?;
        let retry_hold = io.map(|io| io.guard_owned(retry.as_str().len() as u64)).transpose()?;
        let file = self.host.put_content_impl(&self.volume, &self.grant, path, bytes,
            media_type, display_name, self.maximum_bytes, &retry, None, expected_generation, io,
            publication_lease, root_allocation.filter(|_| expected_generation.is_some())).await?;
        drop(retry);
        drop(retry_hold);
        Ok(file)
    }

    pub(crate) async fn stage_with_retry(
        &self,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        retry: &IdempotencyKey,
    ) -> Result<FileRef> {
        self.host
            .put_content(
                &self.volume,
                &self.grant,
                path,
                bytes,
                media_type,
                display_name,
                self.maximum_bytes,
                retry,
            )
            .await
    }
}

pub(super) fn once_key(operation: OperationId, io: Option<&PreparationIo>) -> Result<IdempotencyKey> {
    // OperationId's canonical UUID Display has exactly 36 ASCII bytes.
    let length = "filesystem-single-file:".len() + 36;
    if let Some(io) = io {
        io.claim(length as u64)?;
        io.preflight(acyclic_fs::WorkCounters { bytes_encoded: length as u64, bytes_copied: length as u64, items_examined: 1, ..Default::default() })?;
        io.allocation_attempt(length as u64)?;
    }
    use std::fmt::Write as _;
    let mut key = String::with_capacity(length);
    write!(&mut key, "filesystem-single-file:{operation}")
        .map_err(|_| super::fork_preparer_io::invalid(io, "original publication identity encoding failed"))?;
    if let Some(io) = io {
        io.charge(acyclic_fs::WorkCounters { bytes_encoded: key.len() as u64, bytes_copied: key.len() as u64, items_examined: 1, ..Default::default() })?;
    }
    IdempotencyKey::new(key)
}

fn replacement_key(operation: OperationId, path: &str, io: Option<&PreparationIo>) -> Result<IdempotencyKey> {
    let length = "filesystem-replacement:".len() + 36 + 1 + 64;
    if let Some(io) = io {
        io.claim(length as u64)?;
        io.preflight(acyclic_fs::WorkCounters {
            bytes_hashed: path.len() as u64, bytes_encoded: length as u64,
            bytes_copied: length as u64, items_examined: 1, ..Default::default()
        })?;
        io.allocation_attempt(length as u64)?;
    }
    use std::fmt::Write as _;
    let mut key = String::with_capacity(length);
    write!(&mut key, "filesystem-replacement:{operation}:{}", blake3::hash(path.as_bytes()).to_hex())
        .map_err(|_| super::fork_preparer_io::invalid(io, "original replacement identity encoding failed"))?;
    if let Some(io) = io {
        io.charge(acyclic_fs::WorkCounters {
            bytes_hashed: path.len() as u64, bytes_encoded: length as u64,
            bytes_copied: length as u64, items_examined: 1, ..Default::default()
        })?;
    }
    IdempotencyKey::new(key)
}

fn validate_publication(
    path: &str, bytes: &[u8], media_type: &str, display_name: &str,
    maximum_bytes: u64, io: Option<&PreparationIo>,
) -> Result<()> {
    super::fork_preparer_io::validate_content_path(path, io)?;
    if bytes.len() as u64 > maximum_bytes || bytes.len() as u64 > 9_007_199_254_740_991 {
        return Err(super::fork_preparer_io::invalid(io, "content size limit mismatch"));
    }
    if path == ".system" || path.starts_with(".system/")
        || path == "turns/.canonical" || path.starts_with("turns/.canonical/") {
        return Err(super::fork_preparer_io::invalid(io, "internal storage paths are reserved"));
    }
    if display_name.is_empty() || display_name.len() > crate::conversation::MAX_LABEL_BYTES
        || display_name.chars().any(char::is_control) {
        return Err(super::fork_preparer_io::invalid(io, "file display name is invalid"));
    }
    if !media_type.split_once('/').is_some_and(|(kind, subtype)| !kind.is_empty() && !subtype.is_empty())
        || media_type.chars().any(|character| !character.is_ascii_alphanumeric()
            && !matches!(character, '/' | '-' | '+' | '.' | '_')) {
        return Err(super::fork_preparer_io::invalid(io, "media type is invalid"));
    }
    Ok(())
}
