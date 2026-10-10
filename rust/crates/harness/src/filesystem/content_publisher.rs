use super::{FilesystemHost, PreparationIo, UploadRecord};
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
        if maximum_bytes == 0 || maximum_bytes == u64::MAX {
            return Err(Error::Invalid(
                "content publication limit must be positive and finite".into(),
            ));
        }
        let grant = ContentGrant::verify(verifier, scope, &volume, VolumeOperation::Write)?;
        Ok(Self {
            host,
            volume,
            grant,
            maximum_bytes,
        })
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
            let retry = once_key(operation_id, None)?;
            self.stage_with_retry(path, bytes, media_type, display_name, &retry)
                .await
        })
    }

    fn observe_staged_once<'a>(
        &'a self, operation_id: OperationId, maximum_bytes: u64,
    ) -> BoxProviderFuture<'a, Result<Option<FileRef>>> {
        Box::pin(async move {
            let retry = once_key(operation_id, None)?;
            self.host.observe_content_publication(&self.volume, &self.grant, &retry,
                maximum_bytes.min(self.maximum_bytes)).await
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
            let result = self.stage_once_with_io(operation_id, path, bytes, media_type, display_name, &io).await;
            match result {
                Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
                Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
            }
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn stage_once_with_native_work<'a>(
        &'a self, operation_id: OperationId, path: &'a str, bytes: &'a [u8],
        media_type: &'a str, display_name: &'a str, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> BoxProviderFuture<'a, crate::Result<(FileRef, acyclic_fs::NativeAllocation)>> {
        Box::pin(async move {
            let io = PreparationIo::from_native_work(work)?;
            let result = self.stage_once_with_io(operation_id, path, bytes, media_type, display_name, &io).await;
            let file = result?;
            let retained = file.logical_owned_bytes()?;
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
            let result = async {
                let retry = once_key(operation_id, Some(&io))?;
                let file = io.receive_content(self.host.original_content_for_operation_measured(
                    &self.volume, &self.grant, &retry, maximum_bytes.min(self.maximum_bytes),
                    io.remaining()?, cancellation).await)?;
                if let Some(file) = &file { io.retain_owned(file.logical_owned_bytes()?)?; }
                Ok(file)
            }.await;
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
            source.validate()?;
            if source.volume() != &self.volume {
                return Err(Error::Unauthorized(
                    "source belongs to another writer volume".into(),
                ));
            }
            let generation = self.host.file_generation(source)?;
            let retry = IdempotencyKey::new(format!(
                "filesystem-replacement:{operation_id}:{}",
                blake3::hash(source.path().as_bytes()).to_hex()
            ))?;
            self.host
                .put_content_at(
                    &self.volume,
                    &self.grant,
                    source.path(),
                    bytes,
                    source.descriptor().media_type(),
                    source.display_name(),
                    self.maximum_bytes,
                    &retry,
                    &generation,
                )
                .await
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
    #[allow(clippy::too_many_arguments, reason = "one canonical complete publication identity")]
    pub(super) async fn stage_once_with_io(
        &self, operation_id: OperationId, path: &str, bytes: &[u8],
        media_type: &str, display_name: &str, io: &PreparationIo,
    ) -> Result<FileRef> {
        let retry_hold;
        let retry = once_key(operation_id, Some(io))?;
        retry_hold = io.guard_owned(retry.as_str().len() as u64)?;
        let file = self.host.put_content_impl(&self.volume, &self.grant, path, bytes,
            media_type, display_name, self.maximum_bytes, &retry, None, None, Some(io)).await?;
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

fn once_key(operation: OperationId, io: Option<&PreparationIo>) -> Result<IdempotencyKey> {
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
        .map_err(|_| Error::Invalid("original publication identity encoding failed".into()))?;
    if let Some(io) = io {
        io.charge(acyclic_fs::WorkCounters { bytes_encoded: key.len() as u64, bytes_copied: key.len() as u64, items_examined: 1, ..Default::default() })?;
    }
    IdempotencyKey::new(key)
}
