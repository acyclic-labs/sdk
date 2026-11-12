//! Interrupt the real publisher after its receipt commits, before the caller
//! receives it. Resuming uses the stock tool and the original durable files.
use acyclic_harness_core::{OperationId, Result, conversation::*};
use std::sync::{Arc, Mutex};

pub(super) struct PublicationPause {
    manifest: bool,
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl PublicationPause {
    pub(super) fn new(manifest: bool) -> Self {
        Self {
            manifest,
            entered: Mutex::new(None),
        }
    }

    pub(super) fn arm(&self) -> tokio::sync::oneshot::Receiver<()> {
        let (send, receive) = tokio::sync::oneshot::channel();
        *self.entered.lock().unwrap() = Some(send);
        receive
    }

    async fn published(&self, manifest: bool) {
        if self.manifest == manifest {
            let entered = self.entered.lock().unwrap().take();
            if let Some(entered) = entered {
                entered.send(()).unwrap();
                std::future::pending::<()>().await;
            }
        }
    }
}

pub(super) struct PausedPublisher {
    pub(super) inner: Arc<dyn ContentPublisher>,
    pub(super) pause: Arc<PublicationPause>,
}

impl ContentPublisher for PausedPublisher {
    fn volume(&self) -> &VolumeRef {
        self.inner.volume()
    }

    fn observe_changes<'a, 'io: 'a>(
        &'a self,
        volume: &'a VolumeRef,
        operation: OperationId,
        maximum: u64,
        access: ContentAccess<'a, 'io>,
    ) -> Result<BoundedContentFuture<'a, (Option<ContentChanges>, Option<ContentHold<'io>>)>> {
        self.inner
            .observe_changes(volume, operation, maximum, access)
    }

    fn stage_changes<'a, 'io: 'a>(
        &'a self,
        volume: &'a VolumeRef,
        operation: OperationId,
        changes: &'a [ContentChange<'a>],
        access: ContentAccess<'a, 'io>,
    ) -> Result<BoundedContentFuture<'a, (ContentChanges, Option<ContentHold<'io>>)>> {
        BoundedContentFuture::new(access.io, || async move {
            let result = self
                .inner
                .stage_changes(volume, operation, changes, access)?
                .await?;
            self.pause.published(false).await;
            Ok(result)
        })
    }

    fn observe_staged_once<'a>(
        &'a self,
        operation: OperationId,
        replacement: Option<(&'a VolumeRef, &'a str)>,
        maximum: u64,
        access: ContentAccess<'a, 'a>,
    ) -> Result<BoundedContentFuture<'a, (Option<FileRef>, Option<ContentHold<'a>>)>> {
        self.inner
            .observe_staged_once(operation, replacement, maximum, access)
    }

    fn stage_once<'a>(
        &'a self,
        operation: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media_type: &'a str,
        display_name: &'a str,
        access: ContentAccess<'a, 'a>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        BoundedContentFuture::new(access.io, || async move {
            let result = self
                .inner
                .stage_once(operation, path, bytes, media_type, display_name, access)?
                .await?;
            if path.ends_with("/files/manifest.json") {
                self.pause.published(true).await;
            }
            Ok(result)
        })
    }
}
