use super::fork_preparer::FilesystemRootBootstrapAllocation;
use super::{FilesystemHost, PreparationIo, UploadRecord};
use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{
        BoundedContentFuture, ContentGrant, ContentHold, ContentPublisher, FileRef,
        VolumeOperation, VolumeRef,
    },
    core::{AuthorityVerifier, Scope},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
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
    /// Binds authenticated write authority and optional preparation accounting.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        volume: &VolumeRef,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Arc<Self>> {
        if maximum_bytes == 0 || maximum_bytes == u64::MAX {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "content publication limit must be positive and finite",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let (volume, grant) = crate::executor::with_history_io(io, |work| -> Result<_> {
            let grant = ContentGrant::verify(
                verifier,
                scope,
                volume,
                VolumeOperation::Write,
                io.map(|io| io.remaining().map(|remaining| remaining.bytes_encoded))
                    .transpose()?
                    .unwrap_or(u64::MAX),
                work,
            )?;
            Ok((volume.copy(work)?, grant))
        })?;
        if let Some(io) = io {
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
            io.allocation_attempt(std::mem::size_of::<Self>() as u64)?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        let _native_metadata = io
            .filter(|io| io.original_native_work().is_some())
            .map(|io| {
                io.take_native_custody(
                    io.owned_bytes()?
                        .checked_sub(before)
                        .ok_or_else(|| io.invalid("content publisher custody underflows"))?,
                )
            })
            .transpose()?;
        Ok(Arc::new(Self {
            host,
            volume,
            grant,
            maximum_bytes,
            #[cfg(not(target_arch = "wasm32"))]
            _native_metadata,
        }))
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    crate::distributed::SchedulerPayloadStore for FilesystemContentPublisher<A, O>
{
    fn stage<'a>(
        &'a self,
        operation: OperationId,
        idempotency_key: &'a str,
        bytes: &'a [u8],
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, FileRef>> {
        if self.volume.class() != crate::conversation::VolumeClass::AgentPrivate
            || self.volume.provider() != &self.host.provider
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "scheduler payload store requires a private volume and positive limit",
            ));
        }
        BoundedContentFuture::new(io, || async move {
            let hashing = acyclic_fs::WorkCounters {
                bytes_hashed: idempotency_key.len() as u64,
                items_examined: 1,
                ..Default::default()
            };
            if let Some(io) = io {
                io.preflight(hashing)?;
            }
            let key = blake3::hash(idempotency_key.as_bytes()).to_hex();
            if let Some(io) = io {
                io.charge(hashing)?;
            }
            let path_hold;
            let path = crate::executor::with_history_io(io, |work| {
                crate::contract::preparation_format(
                    format_args!("scheduler/results/{operation}/{key}.json"),
                    work,
                )
            })?;
            path_hold = io
                .map(|io| io.guard_owned(path.capacity() as u64))
                .transpose()?;
            let retry_hold;
            let retry = crate::executor::with_history_io(io, |work| {
                crate::contract::preparation_format(
                    format_args!("scheduler-result:{operation}:{key}"),
                    work,
                )
            })?;
            retry_hold = io
                .map(|io| io.guard_owned(retry.capacity() as u64))
                .transpose()?;
            let retry = IdempotencyKey::new(retry)?;
            let result = self
                .host
                .put_content(
                    &self.volume,
                    &self.grant,
                    &path,
                    bytes,
                    "application/json",
                    "result.json",
                    self.maximum_bytes,
                    &retry,
                    None,
                    io,
                )
                .await;
            drop(retry);
            drop(retry_hold);
            drop(path);
            drop(path_hold);
            result
        })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ContentPublisher
    for FilesystemContentPublisher<A, O>
{
    fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    fn supports_stage(&self, io: Option<&PreparationIo>) -> Result<()> {
        crate::model::charge_items(io, 1)?;
        crate::executor::with_history_io(io, |work| self.volume.validate(work))?;
        self.grant.require(&self.volume, VolumeOperation::Write, io)
    }

    fn stage_once<'a>(
        &'a self,
        operation: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media: &'a str,
        display: &'a str,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        BoundedContentFuture::new(io, || async move {
            let file = self
                .publish(operation, None, path, bytes, media, display, io, None, None)
                .await?;
            let hold = io
                .map(|io| io.guard_owned(file.logical_owned_bytes()?))
                .transpose()?;
            Ok((file, hold))
        })
    }

    fn stage_at<'a>(
        &'a self,
        operation: OperationId,
        source: &'a FileRef,
        bytes: &'a [u8],
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        BoundedContentFuture::new(io, || async move {
            let file = self
                .publish(
                    operation,
                    Some(source),
                    source.path(),
                    bytes,
                    source.descriptor().media_type(),
                    source.display_name(),
                    io,
                    None,
                    None,
                )
                .await?;
            let hold = io
                .map(|io| io.guard_owned(file.logical_owned_bytes()?))
                .transpose()?;
            Ok((file, hold))
        })
    }

    fn observe_staged_once<'a>(
        &'a self,
        operation: OperationId,
        maximum: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, (Option<FileRef>, Option<ContentHold<'a>>)>> {
        BoundedContentFuture::new(io, || async move {
            let file = self.observe_publication(operation, maximum, io).await?;
            let bytes = file
                .as_ref()
                .map(UploadRecord::logical_owned_bytes)
                .transpose()?
                .unwrap_or(0);
            let hold = io.map(|io| io.guard_owned(bytes)).transpose()?;
            Ok((file, hold))
        })
    }

    fn stage<'a>(
        &'a self,
        operation: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media: &'a str,
        display: &'a str,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        BoundedContentFuture::new(io, || async move {
            let hashing = acyclic_fs::WorkCounters {
                bytes_hashed: path.len() as u64,
                ..Default::default()
            };
            if let Some(io) = io {
                io.preflight(hashing)?;
            }
            let digest = blake3::hash(path.as_bytes());
            if let Some(io) = io {
                io.charge(hashing)?;
            }
            let retry = IdempotencyKey::new(crate::executor::with_history_io(io, |work| {
                crate::contract::preparation_format(
                    format_args!("filesystem-upload:{operation}:{}", digest.to_hex()),
                    work,
                )
            })?)?;
            let retry_hold = io
                .map(|io| io.guard_owned(retry.as_str().len() as u64))
                .transpose()?;
            let file = self
                .host
                .put_content(
                    &self.volume,
                    &self.grant,
                    path,
                    bytes,
                    media,
                    display,
                    self.maximum_bytes,
                    &retry,
                    None,
                    io,
                )
                .await?;
            drop(retry);
            drop(retry_hold);
            let hold = io
                .map(|io| io.guard_owned(file.logical_owned_bytes()?))
                .transpose()?;
            Ok((file, hold))
        })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    FilesystemContentPublisher<A, O>
{
    pub(super) async fn observe_publication(
        &self,
        operation: OperationId,
        maximum: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Option<FileRef>> {
        let retry = once_key(operation, io)?;
        let hold = io
            .map(|io| io.guard_owned(retry.as_str().len() as u64))
            .transpose()?;
        let file = self
            .host
            .content_for_operation_impl(
                &self.volume,
                &self.grant,
                &retry,
                None,
                maximum.min(self.maximum_bytes),
                io,
            )
            .await?;
        drop(retry);
        drop(hold);
        Ok(file)
    }

    pub(super) async fn publish(
        &self,
        operation: OperationId,
        source: Option<&FileRef>,
        path: &str,
        bytes: &[u8],
        media: &str,
        display: &str,
        io: Option<&PreparationIo>,
        lease: Option<&Arc<dyn crate::resource_preparation::CreatedTaskLease>>,
        root: Option<&FilesystemRootBootstrapAllocation<A, O>>,
    ) -> Result<FileRef> {
        if let Some(source) = source {
            crate::executor::with_history_io(io, |work| source.validate(work))?;
            if source.volume() != &self.volume {
                return Err(super::fork_preparer_io::failure(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "source belongs to another writer volume",
                ));
            }
            let version = source.version();
            #[cfg(not(target_arch = "wasm32"))]
            if version.len() == 64
                && let Some(work) = io.and_then(PreparationIo::original_native_work)
            {
                crate::conversation::inspect_native_metadata(work, version.len())?;
            }
            if version.len() != 64 || !version.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "file version is not a Filesystem generation",
                ));
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        let root_generation = root
            .map(|root| root.source_generation(source, io))
            .transpose()?
            .flatten();
        #[cfg(target_arch = "wasm32")]
        let root_generation = {
            let _ = root;
            None
        };
        let owned_generation = source
            .filter(|_| root_generation.is_none())
            .map(|source| self.host.file_generation(source, io))
            .transpose()?;
        let generation_hold = owned_generation
            .as_ref()
            .and_then(|generation| io.map(|io| io.guard_owned(generation.logical_owned_bytes()?)))
            .transpose()?;
        let generation = root_generation.or(owned_generation.as_ref());
        let (path, media, display) = source.map_or((path, media, display), |source| {
            (
                source.path(),
                source.descriptor().media_type(),
                source.display_name(),
            )
        });
        let retry = match source {
            Some(source) => replacement_key(operation, source.path(), io)?,
            None => once_key(operation, io)?,
        };
        let retry_hold = io
            .map(|io| io.guard_owned(retry.as_str().len() as u64))
            .transpose()?;
        let file = self
            .host
            .put_content_impl(
                &self.volume,
                &self.grant,
                path,
                bytes,
                media,
                display,
                self.maximum_bytes,
                &retry,
                None,
                generation,
                io,
                lease,
                root.filter(|_| root_generation.is_some()),
            )
            .await?;
        drop(retry);
        drop(retry_hold);
        drop(owned_generation);
        drop(generation_hold);
        Ok(file)
    }
}

pub(super) fn once_key(
    operation: OperationId,
    io: Option<&PreparationIo>,
) -> Result<IdempotencyKey> {
    let length = "filesystem-single-file:".len() + 36;
    if let Some(io) = io {
        io.retain_owned(length as u64)?;
        io.preflight(acyclic_fs::WorkCounters {
            bytes_encoded: length as u64,
            bytes_copied: length as u64,
            items_examined: 1,
            ..Default::default()
        })?;
        io.allocation_attempt(length as u64)?;
    }
    use std::fmt::Write as _;
    let mut key = String::with_capacity(length);
    write!(&mut key, "filesystem-single-file:{operation}").map_err(|_| {
        stock_boundary(
            io,
            crate::wire::ErrorCode::Invalid,
            "original publication identity encoding failed",
        )
    })?;
    if let Some(io) = io {
        io.charge(acyclic_fs::WorkCounters {
            bytes_encoded: key.len() as u64,
            bytes_copied: key.len() as u64,
            items_examined: 1,
            ..Default::default()
        })?;
    }
    IdempotencyKey::new(key)
}

fn replacement_key(
    operation: OperationId,
    path: &str,
    io: Option<&PreparationIo>,
) -> Result<IdempotencyKey> {
    let length = "filesystem-replacement:".len() + 36 + 1 + 64;
    if let Some(io) = io {
        io.retain_owned(length as u64)?;
        io.preflight(acyclic_fs::WorkCounters {
            bytes_hashed: path.len() as u64,
            bytes_encoded: length as u64,
            bytes_copied: length as u64,
            items_examined: 1,
            ..Default::default()
        })?;
        io.allocation_attempt(length as u64)?;
    }
    use std::fmt::Write as _;
    let mut key = String::with_capacity(length);
    write!(
        &mut key,
        "filesystem-replacement:{operation}:{}",
        blake3::hash(path.as_bytes()).to_hex()
    )
    .map_err(|_| {
        stock_boundary(
            io,
            crate::wire::ErrorCode::Invalid,
            "original replacement identity encoding failed",
        )
    })?;
    if let Some(io) = io {
        io.charge(acyclic_fs::WorkCounters {
            bytes_hashed: path.len() as u64,
            bytes_encoded: length as u64,
            bytes_copied: length as u64,
            items_examined: 1,
            ..Default::default()
        })?;
    }
    IdempotencyKey::new(key)
}
