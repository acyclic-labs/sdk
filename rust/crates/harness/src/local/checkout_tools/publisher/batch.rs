use super::*;
use crate::conversation::{ContentAccess, ContentChange, ContentChanges};

impl CheckoutPublisher {
    pub(super) fn publish_changes<'a, 'io: 'a, Output: acyclic_stream::ProviderTask + 'a>(
        &'a self,
        volume: &'a VolumeRef,
        operation: OperationId,
        changes: Option<&'a [ContentChange<'a>]>,
        maximum: u64,
        access: ContentAccess<'a, 'io>,
        finish: fn(Option<ContentChanges>) -> Result<Output>,
    ) -> Result<BoundedContentFuture<'a, (Output, Option<ContentHold<'io>>)>> {
        BoundedContentFuture::new(access.io, || {
            serialized(&self.tools.state, async move |mut state| {
                let physical = self.selected_workspace(&state)?;
                let mount = physical.mounts.by_volume(volume, access.io)?;
                if let Some(changes) = changes {
                    for change in changes {
                        mount.check_with_work(
                            acyclic_fs::access::Action::Write,
                            change.path(),
                            self.tools.clock.now_unix_millis() / 1000,
                            access.io.and_then(ContentIo::original_native_work),
                        )?;
                    }
                }
                let view = state
                    .active
                    .as_ref()
                    .unwrap_or_else(|| self.tools.workspace.view());
                if reconciliation::recover(&physical, view, &|| async { Ok(()) }).await? {
                    state.watcher = None;
                }
                let State {
                    active, watcher, ..
                } = &mut *state;
                let view = active
                    .as_ref()
                    .unwrap_or_else(|| self.tools.workspace.view());
                if mount.view.source.is_none() {
                    self.finish_pending_capture(&physical, view, watcher)
                        .await?;
                }
                let native = match &mount.view.source {
                    Some(source) => Some(source.native().await),
                    None => None,
                };
                let native = native.as_ref().and_then(|guard| guard.as_ref());
                let workspace = mount
                    .view
                    .source
                    .as_ref()
                    .map_or_else(|| view.workspace(), |source| source.workspace.workspace());
                access::require_volume_access(
                    workspace,
                    volume,
                    &self.tools.signed,
                    access.ceiling.unwrap_or(self.tools.signed.capabilities()),
                    "local checkout publication",
                )
                .await?;
                let writer = self.writer_for(volume, workspace, access.io)?;
                let (original, original_hold) = writer
                    .observe_changes(volume, operation, maximum, access)?
                    .await?;
                let (receipt, hold) = if let Some(changes) = changes {
                    if original.is_none() {
                        if let Some(native) = native {
                            native.sync().await?;
                        } else if mount.view.source.is_none() {
                            if let Some(mounted) = &physical.mounted {
                                mounted.sync().await.map_err(storage)?;
                            } else {
                                self.tools
                                    .capture(
                                        (&physical, view),
                                        watcher,
                                        operation,
                                        0,
                                        PublicationPermit::Unrestricted,
                                    )?
                                    .await?;
                            }
                        }
                    }
                    // The original transaction validates the complete request even on replay.
                    drop(original);
                    drop(original_hold);
                    let (receipt, hold) = writer
                        .stage_changes(volume, operation, changes, access)?
                        .await?;
                    (Some(receipt), hold)
                } else {
                    (original, original_hold)
                };
                let Some(receipt) = receipt else {
                    return Ok((finish(None)?, hold));
                };
                for file in &receipt.files {
                    mount.check_with_work(
                        acyclic_fs::access::Action::Write,
                        file.path(),
                        self.tools.clock.now_unix_millis() / 1000,
                        access.io.and_then(ContentIo::original_native_work),
                    )?;
                }
                if let Some(native) = native {
                    native.advance_to_head().await?;
                } else if mount.view.source.is_none() {
                    if physical.mounted.is_some() {
                        reconciliation::advance_mount(&physical, &|| async { Ok(()) }).await?;
                    } else {
                        let generation = acyclic_fs::GenerationId::from_bytes(
                            receipt.generation.as_resource().key(),
                        )
                        .ok_or_else(|| {
                            Error::Invalid("invalid conditional publication generation".into())
                        })?;
                        self.tools
                            .owe_write_capture(
                                (&physical, view),
                                watcher,
                                (
                                    publication_operation(operation, ""),
                                    acyclic_fs::GitGenerationRef {
                                        workspace_id: view.workspace().id(),
                                        generation,
                                    },
                                ),
                            )
                            .await?;
                        self.finish_pending_capture(&physical, view, watcher)
                            .await?;
                    }
                }
                Ok((finish(Some(receipt))?, hold))
            })
        })
    }
}
