use super::*;
use crate::conversation::{ContentAccess, ContentChange, ContentChangeResult, ContentChanges};
use acyclic_fs::access::Action;

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
                            match change {
                                ContentChange::CreateAbsent { .. } => Action::Create,
                                ContentChange::UpdateExpected { .. } => Action::Write,
                                ContentChange::DeleteExpected { .. } => Action::Delete,
                            },
                            change.path(),
                            self.tools.clock.now_unix_millis() / 1000,
                            access
                                .io
                                .and_then(crate::store::HistoryIo::original_native_work),
                        )?;
                    }
                }
                let view = state
                    .active
                    .as_ref()
                    .unwrap_or_else(|| self.tools.workspace.view());
                let before_effect = || std::future::ready(Ok(()));
                let recovered = BoundedContentFuture::new(access.io, || {
                    reconciliation::recover(&physical, view, &before_effect)
                })?
                .await?;
                if recovered {
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
                BoundedContentFuture::new(access.io, || {
                    access::require_volume_access(
                        workspace,
                        volume,
                        &self.tools.signed,
                        access.ceiling.unwrap_or(self.tools.signed.capabilities()),
                        "local checkout publication",
                    )
                })?
                .await?;
                let writer = self.writer_for(volume, workspace, access.io)?;
                let (original, original_hold) = writer
                    .observe_changes(volume, operation, maximum, access)?
                    .await?;
                let (receipt, hold) = if let Some(changes) = changes {
                    if original.is_none() && (native.is_some() || mount.view.source.is_none()) {
                        if let Some(native) = native {
                            BoundedContentFuture::new(access.io, || async {
                                native.sync().await?;
                                Ok(())
                            })?
                            .await?;
                        } else {
                            BoundedContentFuture::new(access.io, || {
                                capture::capture(
                                    (&self.tools.capture_options, &self.tools.capture_policy),
                                    (&physical, view),
                                    watcher,
                                    (operation, 0),
                                    PublicationPermit::Unrestricted,
                                )
                            })?
                            .await?;
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
                for member in &receipt.members {
                    mount.check_with_work(
                        match member {
                            ContentChangeResult::Created(_) => Action::Create,
                            ContentChangeResult::Updated(_) => Action::Write,
                            ContentChangeResult::Deleted(_) => Action::Delete,
                        },
                        member.path(),
                        self.tools.clock.now_unix_millis() / 1000,
                        access
                            .io
                            .and_then(crate::store::HistoryIo::original_native_work),
                    )?;
                }
                if native.is_some() || mount.view.source.is_none() {
                    BoundedContentFuture::new(access.io, || async {
                        if let Some(native) = native {
                            native.advance_to_head().await?;
                        } else if mount.view.source.is_none() {
                            if physical.mounted.is_some() {
                                reconciliation::advance_mount(&physical, &|| async { Ok(()) })
                                    .await?;
                            } else {
                                let generation = acyclic_fs::GenerationId::from_bytes(
                                    receipt.generation.as_resource().key(),
                                )
                                .ok_or_else(|| {
                                    Error::Invalid(
                                        "invalid conditional publication generation".into(),
                                    )
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
                        Ok(())
                    })?
                    .await?;
                }
                Ok((finish(Some(receipt))?, hold))
            })
        })
    }
}
