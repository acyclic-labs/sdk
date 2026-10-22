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
                Publication::Replace(source) => writer.observe_staged_at(operation, source, self.maximum).await?,
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
        self.materialize(view, operation, &file).await?;
        #[cfg(test)]
        if tests::KILL_BEFORE_CAPTURE.get() {
            return Err(Error::Storage("killed before the write's capture".into()));
        }
        // The physical write carries host metadata (mode, owner, times) that
        // the staged generation cannot predict. Capture it now, under the
        // same lock, so the agent's own write is not observed later as a
        // foreign change to its file. The capture is pending exactly while
        // the staged generation is still the head: a replay after an
        // interrupted capture finishes it, and a replay after any later
        // commit never reuses this capture's identity for other content. One
        // operation may publish several paths, so the capture's identity
        // names the path too.
        let pending = self.head_is_staged(view, &file).await?;
        #[cfg(test)]
        let pending = pending && !tests::REPLAY_SKIPS_CAPTURE.get();
        if pending {
            let captured =
                OperationId::from_bytes(publication_operation(operation, file.path()).into_bytes());
            self.tools
                .capture(
                    (&self.tools.workspace, view),
                    watcher,
                    captured,
                    1,
                    PublicationPermit::Unrestricted,
                    None,
                )
                .await?;
        }
        self.observed(view, file).await
    }

    /// Rebinds a published file to the generation that captured its own
    /// physical write: the child of the staged generation on the head's
    /// first-parent chain, when the file's bytes there are exactly the
    /// published ones. A first attempt finds it at the head; a replay finds
    /// the same generation below later commits, so both return one reference.
    /// Any other change to the file leaves the staged reference, so an edit
    /// through it still conflicts with that change.
    async fn observed(&self, view: &workspace::View, file: FileRef) -> Result<FileRef> {
        /// Bounds the replay walk; a staged generation buried deeper keeps its
        /// own reference, which is always valid.
        const DEPTH: usize = 64;
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::WorkBudget::UNBOUNDED;
        let workspace = view.workspace();
        let staged = self.staged_generation(&file)?;
        let mut current = workspace
            .head(budget, &cancellation)
            .await
            .map_err(storage)?
            .value;
        let mut capture = None;
        for _ in 0..DEPTH {
            if current.id() == staged {
                break;
            }
            let parents = current
                .parents(budget, &cancellation)
                .await
                .map_err(storage)?
                .value;
            if parents == [staged] {
                capture = Some(current);
                break;
            }
            let Some(parent) = parents.first() else {
                break;
            };
            current = workspace
                .generation(*parent, budget, &cancellation)
                .await
                .map_err(storage)?
                .value;
        }
        let Some(capture) = capture else {
            return Ok(file);
        };
        let descriptor = file.descriptor();
        let bytes = match capture
            .read(
                &format!("/{}", file.path()),
                descriptor.byte_length().saturating_add(1),
                budget,
                &cancellation,
            )
            .await
        {
            Ok(read) => read.value,
            Err(failure) => match failure.error {
                acyclic_fs::WorkspaceError::NotFound
                | acyclic_fs::WorkspaceError::NotRegularFile
                | acyclic_fs::WorkspaceError::IsDirectory
                | acyclic_fs::WorkspaceError::ReadLimitExceeded => return Ok(file),
                error => return Err(storage(error)),
            },
        };
        let bytes: &[u8] = bytes.as_ref();
        if bytes.len() as u64 != descriptor.byte_length()
            || <sha2::Sha256 as sha2::Digest>::digest(bytes).as_slice() != descriptor.sha256()
        {
            return Ok(file);
        }
        // Retain it as publication retains the staged generation.
        crate::filesystem::retain_published_generation(&capture).await?;
        FileRef::new(
            file.volume().clone(),
            file.path().to_owned(),
            hex::encode(capture.id().digest().into_bytes()),
            descriptor.clone(),
            file.display_name().to_owned(),
            &mut None,
        )
    }

    fn staged_generation(&self, file: &FileRef) -> Result<acyclic_fs::GenerationId> {
        let staged = self.files.file_generation(file, None)?;
        Ok(acyclic_fs::GenerationId::new(
            acyclic_fs::Digest::from_bytes(staged.as_resource().key().try_into().map_err(storage)?),
        ))
    }

    /// Whether the head is still the generation that staged `file`, so the
    /// capture of its physical write has not committed.
    async fn head_is_staged(&self, view: &workspace::View, file: &FileRef) -> Result<bool> {
        let head = view
            .workspace()
            .head(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map_err(storage)?
            .value;
        Ok(head.id() == self.staged_generation(file)?)
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
    fn observe_staged_once<'a>(
        &'a self,
        operation: OperationId,
        maximum: u64,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, (Option<FileRef>, Option<ContentHold<'a>>)>> {
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
                    let file = self
                        .writer_for(&mount.view.volume, workspace)?
                        .observe_staged_once(operation, maximum, None)?
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

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod tests;
