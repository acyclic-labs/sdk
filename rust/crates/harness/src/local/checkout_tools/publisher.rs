//! File publication shares the selected checkout and its command lock.
use super::*;
use crate::conversation::{
    BoundedContentFuture, ContentHold, ContentIo, ContentPublisher, FileRef,
};

type Publisher = FilesystemContentPublisher<LocalAuthorityBackend, LocalObjectBackend>;

fn publication_operation(operation: OperationId, path: &str) -> acyclic_fs::OperationId {
    let mut hash = blake3::Hasher::new_derive_key("acyclic.local.checkout-publication/v2");
    hash.update(&operation.into_bytes());
    hash.update(path.as_bytes());
    let mut identity = [0; 16];
    identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    acyclic_fs::OperationId::from_bytes(identity)
}

pub(in crate::local) struct CheckoutPublisher {
    tools: Arc<CheckoutTools>,
    files: Arc<LocalFilesystem>,
    volume: VolumeRef,
    verifier: AuthorityVerifier,
    signed: Scope,
    maximum: u64,
}

enum Publication<'a> {
    Once {
        path: &'a str,
        media: &'a str,
        display: &'a str,
    },
    Path {
        path: &'a str,
        media: &'a str,
        display: &'a str,
    },
    Replace(&'a FileRef),
}

impl CheckoutPublisher {
    pub(in crate::local) fn new(
        tools: Arc<CheckoutTools>,
        files: Arc<LocalFilesystem>,
        volume: &VolumeRef,
        verifier: &AuthorityVerifier,
        signed: &Scope,
        maximum: u64,
    ) -> Self {
        Self {
            tools,
            files,
            volume: volume.clone(),
            verifier: verifier.clone(),
            signed: signed.clone(),
            maximum,
        }
    }

    fn writer(&self, view: &workspace::View) -> Result<Arc<Publisher>> {
        Publisher::new(
            self.files
                .for_workspace(&self.volume, view.workspace().clone())?,
            &self.volume,
            &self.verifier,
            &self.signed,
            self.maximum,
            None,
        )
    }

    async fn publish(
        &self,
        operation: OperationId,
        bytes: &[u8],
        publication: Publication<'_>,
    ) -> Result<FileRef> {
        serialized(&self.tools.state, async |mut state| {
            let view = state.active.as_ref().unwrap_or_else(|| self.tools.workspace.view());
            if reconciliation::recover(&self.tools.workspace, view, &|| async { Ok(()) }).await? {
                state.watcher = None;
            }
            let State { active, watcher, .. } = &mut *state;
            let view = active
                .as_ref()
                .unwrap_or_else(|| self.tools.workspace.view());
            let writer = self.writer(view)?;
            // Recover the original committed generation before reading the
            // checkout: capture must not import a missing physical write as a deletion.
            let original = match &publication {
                Publication::Once { .. } => writer.observe_staged_once(operation, self.maximum, None)?.await?.0,
                Publication::Path { path, .. } => writer.observe_staged_path(operation, path, self.maximum).await?,
                Publication::Replace(source) => writer.observe_staged_at(operation, source, self.maximum).await?,
            };
            if original.is_none() {
                self.tools.capture(view, watcher, operation, 0, PublicationPermit::Unrestricted, None).await?;
            }
            let file = match publication {
                Publication::Once {
                    path,
                    media,
                    display,
                } => {
                    writer
                        .stage_once(operation, path, bytes, media, display, None)?
                        .await?
                        .0
                }
                Publication::Path {
                    path,
                    media,
                    display,
                } => {
                    writer
                        .stage(operation, path, bytes, media, display, None)?
                        .await?
                        .0
                }
                Publication::Replace(source) => {
                    writer.stage_at(operation, source, bytes, None)?.await?.0
                }
            };
            self.materialize(view, operation, &file).await?;
            Ok(file)
        })
        .await
        .inspect_err(|error| tracing::error!(%operation, ?error, "checkout file publication did not complete"))
    }

    async fn materialize(
        &self,
        view: &workspace::View,
        operation: OperationId,
        file: &FileRef,
    ) -> Result<()> {
        let reference = self.files.file_generation(file, None)?;
        let digest = acyclic_fs::Digest::from_bytes(
            reference.as_resource().key().try_into().map_err(storage)?,
        );
        let cancellation = acyclic_fs::CancellationToken::new();
        let workspace = view.workspace();
        let to = workspace
            .generation(
                acyclic_fs::GenerationId::new(digest),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?
            .value;
        let parents = to
            .parents(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value;
        let [parent] = parents.as_slice() else {
            return Err(Error::Storage(
                "file publication has no unique parent generation".into(),
            ));
        };
        let from = workspace
            .generation(*parent, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value;
        crate::tools::git::publish_checkout_transition(
            &self.tools.workspace.path,
            self.tools.workspace.identity,
            &self.tools.workspace.store,
            publication_operation(operation, file.path()),
            from,
            to,
            &|| std::future::ready(Ok(())),
        )
        .await
    }
}

impl ContentPublisher for CheckoutPublisher {
    fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    fn supports_stage(&self, io: Option<&ContentIo>) -> Result<()> {
        if io.is_some() {
            return Err(Error::Unsupported(
                "checkout publication requires ordinary task execution".into(),
            ));
        }
        Ok(())
    }

    fn stage_once<'a>(
        &'a self,
        operation: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media: &'a str,
        display: &'a str,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        self.supports_stage(io)?;
        BoundedContentFuture::new(None, || async move {
            Ok((
                self.publish(
                    operation,
                    bytes,
                    Publication::Once {
                        path,
                        media,
                        display,
                    },
                )
                .await?,
                None,
            ))
        })
    }
    fn stage<'a>(
        &'a self,
        operation: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media: &'a str,
        display: &'a str,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        self.supports_stage(io)?;
        BoundedContentFuture::new(None, || async move {
            Ok((
                self.publish(
                    operation,
                    bytes,
                    Publication::Path {
                        path,
                        media,
                        display,
                    },
                )
                .await?,
                None,
            ))
        })
    }
    fn stage_at<'a>(
        &'a self,
        operation: OperationId,
        source: &'a FileRef,
        bytes: &'a [u8],
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        self.supports_stage(io)?;
        BoundedContentFuture::new(None, || async move {
            Ok((
                self.publish(operation, bytes, Publication::Replace(source))
                    .await?,
                None,
            ))
        })
    }
    fn observe_staged_once<'a>(
        &'a self,
        operation: OperationId,
        maximum: u64,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, (Option<FileRef>, Option<ContentHold<'a>>)>> {
        self.supports_stage(io)?;
        BoundedContentFuture::new(None, || async move {
            serialized(&self.tools.state, async |state| {
                let view = state
                    .active
                    .as_ref()
                    .unwrap_or_else(|| self.tools.workspace.view());
                let file = self
                    .writer(view)?
                    .observe_staged_once(operation, maximum, None)?
                    .await?
                    .0;
                if let Some(file) = &file {
                    self.materialize(view, operation, file).await?;
                }
                Ok((file, None))
            })
            .await
        })
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
