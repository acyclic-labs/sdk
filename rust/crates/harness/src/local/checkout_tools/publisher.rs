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
    fn selected_workspace(&self, state: &State) -> Result<Arc<LocalWorkspace>> {
        #[cfg(all(test, not(loom)))]
        if tests::PUBLISH_TO_INITIAL_MOUNT.get() {
            return Ok(Arc::clone(&self.tools.workspace));
        }
        self.tools
            .workspace
            .cached_selected_workspace(state)
            .ok_or_else(|| {
                Error::Conflict("selected checkout has no admitted physical binding".into())
            })
    }

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
        serialized(&self.tools.state, async |mut state| {
            let physical = self.selected_workspace(&state)?;
            let (mount, relative) = physical.mounts.file(volume, path)?;
            mount.check(
                acyclic_fs::access::Action::Write,
                &relative,
                self.tools.clock.now_unix_millis() / 1000,
            )?;
            let view = state.active.as_ref().unwrap_or_else(|| self.tools.workspace.view());
            if reconciliation::recover(&physical, view, &|| async { Ok(()) }).await? {
                state.watcher = None;
            }
            let State { active, watcher, .. } = &mut *state;
            let view = active.as_ref().unwrap_or_else(|| self.tools.workspace.view());
            // Settle an earlier write's durable capture before staging moves its head.
            if mount.view.source.is_none() {
                self.finish_pending_capture(&physical, view, watcher).await?;
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
                } else if mount.view.source.is_none() {
                    if let Some(mounted) = &physical.mounted {
                        mounted.sync().await.map_err(storage)?;
                    } else if matches!(&publication, Publication::Replace(_)) {
                        self.tools.capture((&physical, view), watcher, operation, 0, PublicationPermit::Unrestricted)?.await?;
                    }
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
                return self.settle(&physical, view, watcher, operation, file).await;
            }
            Ok(file)
        })
        .await
        .inspect_err(|error| tracing::error!(%operation, ?error, "checkout file publication did not complete"))
    }

    /// Settles a staged file in its selected physical checkout: owes the write its
    /// physical publication and capture, finishes that debt, and returns the
    /// reference to the generation that holds exactly the written bytes. A
    /// first attempt and every replay, whether it enters through the write
    /// or through recovery, settle the same way.
    async fn settle(
        &self,
        physical: &LocalWorkspace,
        view: &workspace::View,
        watcher: &mut Option<capture::Cursor>,
        operation: OperationId,
        file: FileRef,
    ) -> Result<FileRef> {
        if physical.mounted.is_some() {
            // A native view already projects the committed generation. Advancing
            // that view replaces physical checkout publication and recapture.
            reconciliation::advance_mount(physical, &|| async { Ok(()) }).await?;
            return Ok(file);
        }
        // The physical write carries host metadata (mode, owner, times) that
        // the staged generation cannot predict, so it is owed a capture of
        // its own, under the write's operation, so the agent's own write is
        // not observed later as a foreign change to its file. The debt names
        // the write and its capture and is recorded durably before the write
        // reaches disk. This write finishes it under the same lock; after an
        // interruption, the next capture of this checkout finishes it first,
        // publishing the write if it never reached disk, whichever command or
        // replay that is, while the staged generation is still the head. One
        // operation may publish several paths, so the capture's identity
        // names the path too.
        let selected = (physical, view);
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
        #[cfg(all(test, not(loom)))]
        if tests::KILL_BEFORE_WRITE.get() {
            return Err(Error::Storage("killed before the write".into()));
        }
        self.finish_pending_capture(physical, view, watcher).await?;
        self.observed(view, operation, file).await
    }

    // Every publication entry point settles the same durable debt.
    async fn finish_pending_capture(
        &self,
        physical: &LocalWorkspace,
        view: &workspace::View,
        watcher: &mut Option<capture::Cursor>,
    ) -> Result<()> {
        #[cfg(all(test, not(loom)))]
        if tests::REPLAY_SKIPS_CAPTURE.get() {
            return Ok(());
        }
        self.tools
            .finish_owed_write((physical, view), watcher, PublicationPermit::Unrestricted)?
            .await
    }

    fn staged_generation(&self, file: &FileRef) -> Result<acyclic_fs::GenerationId> {
        let staged = self.files.file_generation(file, None)?;
        acyclic_fs::GenerationId::from_bytes(staged.as_resource().key())
            .ok_or_else(|| Error::Invalid("invalid canonical generation reference".into()))
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
                let physical = self.selected_workspace(&state)?;
                let State {
                    active, watcher, ..
                } = &mut *state;
                let view = active
                    .as_ref()
                    .unwrap_or_else(|| self.tools.workspace.view());
                // A publication is admitted only through a mount the caller
                // may write, so a mount outside its authority holds none of
                // its operations: observation skips it. A caller that may
                // reach no mount is refused.
                let mut refused = None;
                let mut admitted = false;
                for (_, mount) in physical.mounts.mounts() {
                    let workspace = mount
                        .view
                        .source
                        .as_ref()
                        .map_or_else(|| view.workspace(), |source| source.workspace.workspace());
                    match access::require_volume_access(
                        workspace,
                        &mount.view.volume,
                        &self.tools.signed,
                        ceiling.unwrap_or(self.tools.signed.capabilities()),
                        "local checkout publication",
                    )
                    .await
                    {
                        Ok(()) => admitted = true,
                        Err(
                            error @ (Error::FilesystemAccessConfiguration(_)
                            | Error::Unauthorized(_)),
                        ) => {
                            #[cfg(all(test, not(loom)))]
                            if tests::OBSERVE_EVERY_MOUNT.get() {
                                return Err(error);
                            }
                            refused.get_or_insert(error);
                            continue;
                        }
                        Err(error) => return Err(error),
                    }
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
                            file = self
                                .settle(&physical, view, watcher, operation, file)
                                .await?;
                        }
                        return Ok((Some(file), None));
                    }
                }
                match refused {
                    Some(error) if !admitted => Err(error),
                    _ => Ok((None, None)),
                }
            })
            .await
        })
    }
}

#[cfg(all(test, not(loom)))]
mod mount_tests;
#[cfg(all(test, not(loom)))]
mod tests;
