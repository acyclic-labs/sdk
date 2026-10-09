use super::FilesystemHost;
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
