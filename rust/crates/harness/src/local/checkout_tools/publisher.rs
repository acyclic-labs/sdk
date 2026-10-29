//! File publication shares the selected checkout and its command lock.
use super::*;
use crate::conversation::{
    BoundedContentFuture, ContentHold, ContentIo, ContentPublisher, FileRef,
};

mod observed;

type Publisher = FilesystemContentPublisher<LocalAuthorityBackend, LocalObjectBackend>;

fn publication_operation(operation: OperationId, path: &str) -> acyclic_fs::OperationId {
    let mut hash = blake3::Hasher::new_derive_key("acyclic.local.checkout-publication/v2");
    hash.update(&operation.into_bytes());
    hash.update(path.as_bytes());
    let mut identity = [0; 16];
    identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    acyclic_fs::OperationId::from_bytes(identity)
}

/// The operation that captures the physical write of `path` under
/// `operation`. Its committed generation is durably keyed by this identity,
/// so a replay finds it directly however far the head has moved since.
fn write_capture(operation: OperationId, path: &str) -> OperationId {
    OperationId::from_bytes(publication_operation(operation, path).into_bytes())
}

pub(in crate::local) struct CheckoutPublisher {
    tools: Arc<CheckoutTools>,
    files: Arc<LocalFilesystem>,
    volume: VolumeRef,
    verifier: AuthorityVerifier,
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
        maximum: u64,
    ) -> Self {
        Self {
            tools,
            files,
            volume: volume.clone(),
            verifier: verifier.clone(),
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
            &self.tools.signed,
            self.maximum,
            None,
        )
    }

    async fn publish(
        &self,
        operation: OperationId,
        bytes: &[u8],
        publication: Publication<'_>,
        ceiling: Option<&crate::Capabilities>,
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
            // Settle an earlier write's durable capture before staging moves its head.
            if mount.view.source.is_none() {
                self.finish_pending_capture(view, watcher).await?;
            }
            let native = match &mount.view.source {
                Some(source) => Some(source.native().await),
                None => None,
            };
            let native = native.as_ref().and_then(|guard| guard.as_ref());
            let workspace = mount.view.source.as_ref()
                .map_or_else(|| view.workspace(), |source| source.workspace.workspace());
            access::require_volume_access(workspace, &mount.view.volume, &self.tools.signed, ceiling.unwrap_or(self.tools.signed.capabilities()), "local checkout publication").await?;
            let writer = self.writer_for(&mount.view.volume, workspace)?;
            // Recover the original committed generation before capture: a
            // missing physical publication must not become a deletion.
            let original = match &publication {
                Publication::Once { .. } => writer.observe_staged_once(operation, self.maximum, crate::conversation::ContentAccess { ceiling, io: None })?.await?.0,
                Publication::Path { .. } => writer.observe_staged_path(operation, &relative, self.maximum, ceiling).await?,
                Publication::Replace(source) => writer.observe_staged_at(operation, source, self.maximum, ceiling).await?,
            };
            if original.is_none() {
                if let Some(native) = native {
                    native.sync().await?;
                } else if mount.view.source.is_none()
                    && matches!(&publication, Publication::Replace(_))
                {
                    self.tools.capture((&self.tools.workspace, view), watcher, operation, 0, PublicationPermit::Unrestricted)?.await?;
                }
            }
            let file = match publication {
                Publication::Once { media, display, .. } => {
                    writer.stage_once(operation, &relative, bytes, media, display, crate::conversation::ContentAccess { ceiling, io: None })?.await?.0
                }
                Publication::Path { media, display, .. } => {
                    writer.stage(operation, &relative, bytes, media, display, crate::conversation::ContentAccess { ceiling, io: None })?.await?.0
                }
                Publication::Replace(source) => writer.stage_at(operation, source, bytes, ceiling, None)?.await?.0,
            };
            if let Some(native) = native {
                native.advance_to_head().await?;
            } else if mount.view.source.is_none() {
                return self.settle(view, watcher, operation, file).await;
            }
            Ok(file)
        })
        .await
        .inspect_err(|error| tracing::error!(%operation, ?error, "checkout file publication did not complete"))
    }

    /// Settles a staged file in an unmounted checkout: materializes its exact
    /// bytes, finishes the capture of that physical write while it is still
    /// pending, and returns the reference to the generation that holds exactly
    /// those bytes. A first attempt and every replay, whether it enters
    /// through the write or through recovery, settle the same way.
    async fn settle(
        &self,
        view: &workspace::View,
        watcher: &mut Option<capture::Cursor>,
        operation: OperationId,
        file: FileRef,
    ) -> Result<FileRef> {
        // The physical write carries host metadata (mode, owner, times) that
        // the staged generation cannot predict, so it is owed a capture of
        // its own, under the write's operation, so the agent's own write is
        // not observed later as a foreign change to its file. The debt is
        // recorded durably before the write reaches disk. This write
        // finishes it under the same lock; after an interruption, the next
        // capture of this checkout finishes it first, whichever command or
        // replay that is, while the staged generation is still the head. One
        // operation may publish several paths, so the capture's identity
        // names the path too.
        let selected = (&*self.tools.workspace, view);
        let owed = (
            publication_operation(operation, file.path()),
            acyclic_fs::GitGenerationRef {
                workspace_id: view.workspace().id(),
                generation: self.staged_generation(&file)?,
            },
        );
        self.tools
            .owe_write_capture(selected, watcher, owed)
            .await?;
        self.materialize(view, operation, &file).await?;
        #[cfg(all(test, not(loom)))]
        if tests::KILL_BEFORE_CAPTURE.get() {
            return Err(Error::Storage("killed before the write's capture".into()));
        }
        self.finish_pending_capture(view, watcher).await?;
        self.observed(view, operation, file).await
    }

    // Every publication entry point settles the same durable capture debt.
    async fn finish_pending_capture(
        &self,
        view: &workspace::View,
        watcher: &mut Option<capture::Cursor>,
    ) -> Result<()> {
        #[cfg(all(test, not(loom)))]
        if tests::REPLAY_SKIPS_CAPTURE.get() {
            return Ok(());
        }
        self.tools
            .finish_owed_write(
                (&self.tools.workspace, view),
                watcher,
                PublicationPermit::Unrestricted,
            )?
            .await
    }

    fn staged_generation(&self, file: &FileRef) -> Result<acyclic_fs::GenerationId> {
        let staged = self.files.file_generation(file, None)?;
        Ok(acyclic_fs::GenerationId::new(
            acyclic_fs::Digest::from_bytes(staged.as_resource().key().try_into().map_err(storage)?),
        ))
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

    // A native tool call's IO bounds its own reads and encoding; the
    // checkout write stays bounded by this publisher's file bound and its
    // command lock, whichever execution path calls it.
    fn supports_stage(&self, _: Option<&ContentIo>) -> Result<()> {
        Ok(())
    }

    fn stage_once<'a>(
        &'a self,
        operation: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media: &'a str,
        display: &'a str,
        access: crate::conversation::ContentAccess<'a, 'a>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        let crate::conversation::ContentAccess { ceiling, io } = access;

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
                    ceiling,
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
        access: crate::conversation::ContentAccess<'a, 'a>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        let crate::conversation::ContentAccess { ceiling, io } = access;

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
                    ceiling,
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
        ceiling: Option<&'a crate::Capabilities>,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        self.supports_stage(io)?;
        BoundedContentFuture::new(None, || async move {
            Ok((
                self.publish(operation, bytes, Publication::Replace(source), ceiling)
                    .await?,
                None,
            ))
        })
    }
    fn observe_staged_once<'a>(
        &'a self,
        operation: OperationId,
        maximum: u64,
        access: crate::conversation::ContentAccess<'a, 'a>,
    ) -> Result<BoundedContentFuture<'a, (Option<FileRef>, Option<ContentHold<'a>>)>> {
        let crate::conversation::ContentAccess { ceiling, io } = access;

        self.supports_stage(io)?;
        BoundedContentFuture::new(None, || async move {
            serialized(&self.tools.state, async |mut state| {
                let State {
                    active, watcher, ..
                } = &mut *state;
                let view = active
                    .as_ref()
                    .unwrap_or_else(|| self.tools.workspace.view());
                for (_, mount) in self.tools.workspace.mounts.mounts() {
                    let workspace = mount
                        .view
                        .source
                        .as_ref()
                        .map_or_else(|| view.workspace(), |source| source.workspace.workspace());
                    access::require_volume_access(
                        workspace,
                        &mount.view.volume,
                        &self.tools.signed,
                        ceiling.unwrap_or(self.tools.signed.capabilities()),
                        "local checkout publication",
                    )
                    .await?;
                    let file = self
                        .writer_for(&mount.view.volume, workspace)?
                        .observe_staged_once(
                            operation,
                            maximum,
                            crate::conversation::ContentAccess { ceiling, io: None },
                        )?
                        .await?
                        .0;
                    if let Some(mut file) = file {
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
                            // A recovered publication names the same
                            // generation the original write returned.
                            file = self.settle(view, watcher, operation, file).await?;
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

#[cfg(all(test, not(loom)))]
mod mount_tests;
#[cfg(all(test, not(loom)))]
mod tests;
