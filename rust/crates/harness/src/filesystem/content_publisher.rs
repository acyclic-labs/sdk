use super::{FilesystemHost, PreparationIo, UploadRecord};
use super::fork_preparer::FilesystemRootBootstrapAllocation;
use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{BoundedContentFuture, ContentGrant, ContentHold, ContentPublisher, FileRef, VolumeOperation, VolumeRef},
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
    /// Binds write authority and copies configuration with optional accounting.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>, volume: &VolumeRef,
        verifier: &AuthorityVerifier, scope: &Scope, maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        Self::validate_limit(maximum_bytes, io)?;
        #[cfg(not(target_arch = "wasm32"))]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        let grant = ContentGrant::verify(verifier, scope, volume, VolumeOperation::Write, maximum_bytes, &mut work)?;
        let volume = volume.copy(&mut work)?;
        #[cfg(not(target_arch = "wasm32"))]
        let _native_metadata = match io.filter(|io| io.original_native_work().is_some()) {
            Some(io) => {
                io.retain_owned(std::mem::size_of::<Self>() as u64)?;
                Some(io.take_native_custody(io.owned_bytes()?.checked_sub(before)
                    .ok_or_else(|| super::fork_preparer_io::failure(Some(io), crate::wire::ErrorCode::Invalid, "content publisher custody underflows"))?)?)
            }
            None => None,
        };
        Ok(Self { host, volume, grant, maximum_bytes,
            #[cfg(not(target_arch = "wasm32"))] _native_metadata })
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        self.volume.logical_owned_bytes()?.checked_add(self.grant.logical_owned_bytes()?)
            .ok_or_else(|| Error::Invalid("content publisher ownership overflow".into()))
    }

    fn validate_limit(maximum_bytes: u64, io: Option<&PreparationIo>) -> Result<()> {
        if maximum_bytes == 0 || maximum_bytes == u64::MAX {
            return Err(super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid,
                "content publication limit must be positive and finite"));
        }
        Ok(())
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
where A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static,
{
    fn volume(&self) -> &VolumeRef { &self.volume }

    fn supports_stage(&self, io: Option<&PreparationIo>) -> Result<()> {
        super::fork_preparer_io::item(io, 0)?;
        let mut history = io.map(PreparationIo::history_io);
        self.volume.validate(&mut history.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo))?;
        self.grant.require(&self.volume, VolumeOperation::Write, io)
    }

    fn stage_once<'a>(&'a self, operation_id: OperationId, path: &'a str, bytes: &'a [u8],
        media_type: &'a str, display_name: &'a str, io: Option<&'a PreparationIo>)
        -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        BoundedContentFuture::new(io, || async move {
            let file = self.publish_once(operation_id, path, bytes, media_type, display_name, io, None, None).await?;
            let hold = io.map(|io| io.guard_owned(file.logical_owned_bytes()?)).transpose()?;
            Ok((file, hold))
        })
    }

    fn observe_staged_once<'a>(&'a self, operation_id: OperationId, maximum_bytes: u64,
        io: Option<&'a PreparationIo>)
        -> Result<crate::conversation::BoundedContentFuture<'a, (Option<FileRef>, Option<crate::conversation::ContentHold<'a>>)>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let file = self.observe_publication(operation_id, maximum_bytes, io).await?;
            let bytes = file.as_ref().map(UploadRecord::logical_owned_bytes).transpose()?.unwrap_or(0);
            let hold = io.map(|io| io.guard_owned(bytes)).transpose()?;
            Ok((file, hold))
        })
    }

    fn stage_at<'a>(&'a self, operation_id: OperationId, source: &'a FileRef, bytes: &'a [u8],
        io: Option<&'a PreparationIo>)
        -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        BoundedContentFuture::new(io, || async move {
            let file = self.replace(operation_id, source, bytes, io, None, None).await?;
            let hold = io.map(|io| io.guard_owned(file.logical_owned_bytes()?)).transpose()?;
            Ok((file, hold))
        })
    }

    fn stage<'a>(&'a self, operation_id: OperationId, path: &'a str, bytes: &'a [u8],
        media_type: &'a str, display_name: &'a str, io: Option<&'a PreparationIo>)
        -> Result<crate::conversation::BoundedContentFuture<'a, (FileRef, Option<crate::conversation::ContentHold<'a>>)>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let hashing = acyclic_fs::WorkCounters { bytes_hashed: path.len() as u64, ..Default::default() };
            if let Some(io) = io { io.preflight(hashing)?; }
            let digest = blake3::hash(path.as_bytes());
            if let Some(io) = io { io.charge(hashing)?; }
            let retry = IdempotencyKey::new(crate::contract::preparation_format(
                format_args!("filesystem-upload:{operation_id}:{}", digest.to_hex()), io)?)?;
            let retry_hold = io.map(|io| io.guard_owned(retry.as_str().len() as u64)).transpose()?;
            let file = self.host.put_content_impl(&self.volume, &self.grant, path, bytes,
                media_type, display_name, self.maximum_bytes, &retry, None, None, io, None, None).await?;
            drop(retry); drop(retry_hold);
            let hold = io.map(|io| io.guard_owned(file.logical_owned_bytes()?)).transpose()?;
            Ok((file, hold))
        })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    FilesystemContentPublisher<A, O>
{
    pub(super) async fn observe_publication(
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

    pub(super) async fn replace(
        &self, operation_id: OperationId, source: &FileRef, bytes: &[u8],
        io: Option<&PreparationIo>,
        publication_lease: Option<&dyn crate::resource_preparation::CreatedTaskLease>,
        root_allocation: Option<&FilesystemRootBootstrapAllocation<A, O>>,
    ) -> Result<FileRef> {
        validate_publication(source.path(), bytes, source.descriptor().media_type(), source.display_name(), self.maximum_bytes, io)?;
        let mut history = io.map(PreparationIo::history_io);
        source.validate(&mut history.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo))?;
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
            return Err(super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "file version is not a Filesystem generation"));
        }
        if source.descriptor().byte_length() > 9_007_199_254_740_991 {
            return Err(super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "file byte length exceeds cross-language precision"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let root_generation = root_allocation
            .map(|allocation| allocation.source_generation(Some(source), io))
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
    pub(super) async fn publish_once(
        &self, operation_id: OperationId, path: &str, bytes: &[u8],
        media_type: &str, display_name: &str, io: Option<&PreparationIo>,
        publication_lease: Option<&dyn crate::resource_preparation::CreatedTaskLease>,
        root_allocation: Option<&FilesystemRootBootstrapAllocation<A, O>>,
    ) -> Result<FileRef> {
        validate_publication(path, bytes, media_type, display_name, self.maximum_bytes, io)?;
        #[cfg(not(target_arch = "wasm32"))]
        let expected_generation = root_allocation
            .map(|allocation| allocation.source_generation(None, io))
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


}

pub(super) fn once_key(operation: OperationId, io: Option<&PreparationIo>) -> Result<IdempotencyKey> {
    let length = "filesystem-single-file:".len() + 36;
    if let Some(io) = io {
        io.claim(length as u64)?;
        io.preflight(acyclic_fs::WorkCounters { bytes_encoded: length as u64, bytes_copied: length as u64, items_examined: 1, ..Default::default() })?;
        io.allocation_attempt(length as u64)?;
    }
    use std::fmt::Write as _;
    let mut key = String::with_capacity(length);
    write!(&mut key, "filesystem-single-file:{operation}")
        .map_err(|_| super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "original publication identity encoding failed"))?;
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
        .map_err(|_| super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "original replacement identity encoding failed"))?;
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
        return Err(super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "content size limit mismatch"));
    }
    if path == ".system" || path.starts_with(".system/")
        || path == "turns/.canonical" || path.starts_with("turns/.canonical/") {
        return Err(super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "internal storage paths are reserved"));
    }
    if display_name.is_empty() || display_name.len() > crate::conversation::MAX_LABEL_BYTES
        || display_name.chars().any(char::is_control) {
        return Err(super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "file display name is invalid"));
    }
    if !media_type.split_once('/').is_some_and(|(kind, subtype)| !kind.is_empty() && !subtype.is_empty())
        || media_type.chars().any(|character| !character.is_ascii_alphanumeric()
            && !matches!(character, '/' | '-' | '+' | '.' | '_')) {
        return Err(super::fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "media type is invalid"));
    }
    Ok(())
}
