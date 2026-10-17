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

/// Capture identity for path resolution; phases 0 and 1 belong to shell
/// commands and publications under the same tool operation.
const READ_CAPTURE_PHASE: u8 = 2;

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

    fn writer_for(
        &self,
        volume: &VolumeRef,
        workspace: &acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
    ) -> Result<Arc<Publisher>> {
        Publisher::new(
            self.files.for_workspace(volume, workspace.clone())?,
            volume,
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
        let (volume, path) = match &publication {
            Publication::Once { path, .. } | Publication::Path { path, .. } => (None, *path),
            Publication::Replace(source) => (Some(source.volume()), source.path()),
        };
        let (mount, relative) = self.tools.workspace.mounts.file(volume, path)?;
        mount.check(
            acyclic_fs::access::Action::Write,
            &relative,
            self.tools.clock.now_unix_millis() / 1000,
        )?;
        serialized(&self.tools.state, async |mut state| {
            let view = state.active.as_ref().unwrap_or_else(|| self.tools.workspace.view());
            if reconciliation::recover(&self.tools.workspace, view, &|| async { Ok(()) }).await? {
                state.watcher = None;
            }
            let State { active, watcher, .. } = &mut *state;
            let view = active.as_ref().unwrap_or_else(|| self.tools.workspace.view());
            let native = match &mount.view.source {
                Some(source) => Some(source.native().await),
                None => None,
            };
            let native = native.as_ref().and_then(|guard| guard.as_ref());
            let workspace = mount.view.source.as_ref()
                .map_or_else(|| view.workspace(), |source| source.workspace.workspace());
            let writer = self.writer_for(&mount.view.volume, workspace)?;
            // Recover the original committed generation before capture: a
            // missing physical publication must not become a deletion.
            let original = match &publication {
                Publication::Once { .. } => writer.observe_staged_once(operation, self.maximum, None)?.await?.0,
                Publication::Path { .. } => writer.observe_staged_path(operation, &relative, self.maximum).await?,
                Publication::Replace(source) => {
                    writer.observe_replacement(operation, source.path(), self.maximum, None)?.await?.0
                }
            };
            if original.is_none() {
                if let Some(native) = native {
                    native.sync().await?;
                } else if mount.view.source.is_none() {
                    self.tools.capture((&self.tools.workspace, view), watcher, operation, 0, PublicationPermit::Unrestricted, None).await?;
                }
            }
            let file = match publication {
                Publication::Once { media, display, .. } => {
                    writer.stage_once(operation, &relative, bytes, media, display, None)?.await?.0
                }
                Publication::Path { media, display, .. } => {
                    writer.stage(operation, &relative, bytes, media, display, None)?.await?.0
                }
                Publication::Replace(source) => writer.stage_at(operation, source, bytes, None)?.await?.0,
            };
            if let Some(native) = native {
                native.advance_to_head().await?;
            } else if mount.view.source.is_none() {
                self.materialize(view, operation, &file).await?;
            }
            Ok(file)
        })
        .await
        .inspect_err(|error| tracing::error!(%operation, ?error, "checkout file publication did not complete"))
    }

    /// Resolves a path in the same view publication writes, after capturing
    /// pending checkout changes so reads observe what the task's shell sees.
    async fn current_file(
        &self,
        operation: OperationId,
        path: &str,
        maximum: u64,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        let (mount, relative) = self.tools.workspace.mounts.file(None, path)?;
        mount.check(
            acyclic_fs::access::Action::Read,
            &relative,
            self.tools.clock.now_unix_millis() / 1000,
        )?;
        serialized(&self.tools.state, async |mut state| {
            let view = state
                .active
                .as_ref()
                .unwrap_or_else(|| self.tools.workspace.view());
            if reconciliation::recover(&self.tools.workspace, view, &|| async { Ok(()) }).await? {
                state.watcher = None;
            }
            let State {
                active, watcher, ..
            } = &mut *state;
            let view = active
                .as_ref()
                .unwrap_or_else(|| self.tools.workspace.view());
            match &mount.view.source {
                Some(source) => {
                    if let Some(native) = source.native().await.as_ref() {
                        native.sync().await?;
                    }
                }
                None => {
                    self.tools
                        .capture(
                            (&self.tools.workspace, view),
                            watcher,
                            operation,
                            READ_CAPTURE_PHASE,
                            PublicationPermit::Unrestricted,
                            None,
                        )
                        .await?;
                }
            }
            let workspace = mount
                .view
                .source
                .as_ref()
                .map_or_else(|| view.workspace(), |source| source.workspace.workspace());
            let (file, bytes, _) = self
                .writer_for(&mount.view.volume, workspace)?
                .current(operation, &relative, maximum, None)?
                .await?;
            Ok((file, bytes))
        })
        .await
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

    fn destination<'a>(
        &'a self,
        volume: Option<&VolumeRef>,
        path: &'a str,
        io: Option<&ContentIo>,
    ) -> Result<(&'a VolumeRef, std::borrow::Cow<'a, str>)> {
        self.supports_stage(io)?;
        let (mount, relative) = self.tools.workspace.mounts.file(volume, path)?;
        mount.check(
            acyclic_fs::access::Action::Write,
            &relative,
            self.tools.clock.now_unix_millis() / 1000,
        )?;
        Ok((&mount.view.volume, std::borrow::Cow::Owned(relative)))
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
    fn current<'a>(
        &'a self,
        operation: OperationId,
        path: &'a str,
        maximum: u64,
        io: Option<&'a ContentIo>,
    ) -> Result<
        BoundedContentFuture<'a, (FileRef, acyclic_stream::OwnedBytes, Option<ContentHold<'a>>)>,
    > {
        self.supports_stage(io)?;
        BoundedContentFuture::new(None, || async move {
            let (file, bytes) = self.current_file(operation, path, maximum).await?;
            Ok((file, bytes, None))
        })
    }

    fn observe_replacement<'a>(
        &'a self,
        operation: OperationId,
        path: &'a str,
        maximum: u64,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, (Option<FileRef>, Option<ContentHold<'a>>)>> {
        self.supports_stage(io)?;
        BoundedContentFuture::new(None, || async move {
            let (mount, relative) = self.tools.workspace.mounts.file(None, path)?;
            serialized(&self.tools.state, async |state| {
                let view = state
                    .active
                    .as_ref()
                    .unwrap_or_else(|| self.tools.workspace.view());
                let workspace = mount
                    .view
                    .source
                    .as_ref()
                    .map_or_else(|| view.workspace(), |source| source.workspace.workspace());
                let file = self
                    .writer_for(&mount.view.volume, workspace)?
                    .observe_replacement(operation, &relative, maximum, None)?
                    .await?
                    .0;
                if let Some(file) = &file {
                    if let Some(source) = &mount.view.source {
                        if let Some(native) = source.native().await.as_ref() {
                            native.advance_to_head().await?;
                        }
                    } else {
                        self.materialize(view, operation, file).await?;
                    }
                }
                Ok((file, None))
            })
            .await
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
                for (_, mount) in self.tools.workspace.mounts.mounts() {
                    let workspace = mount
                        .view
                        .source
                        .as_ref()
                        .map_or_else(|| view.workspace(), |source| source.workspace.workspace());
                    let file = self
                        .writer_for(&mount.view.volume, workspace)?
                        .observe_staged_once(operation, maximum, None)?
                        .await?
                        .0;
                    if let Some(file) = file {
                        mount.check(
                            acyclic_fs::access::Action::Write,
                            file.path(),
                            self.tools.clock.now_unix_millis() / 1000,
                        )?;
                        if let Some(source) = &mount.view.source {
                            let native = source.native().await;
                            if let Some(native) = native.as_ref() {
                                native.advance_to_head().await?;
                            }
                        } else {
                            self.materialize(view, operation, &file).await?;
                        }
                        return Ok((Some(file), None));
                    }
                }
                Ok((None, None))
            })
            .await
        })
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
