use super::*;
use crate::conversation::{ContentAccess, ContentChange, ContentChangeResult, ContentChanges};
use acyclic_fs::access::Action;

impl CheckoutPublisher {
    /// Check every source path before choosing a publication binding. Aliases
    /// can grant disjoint roots; the first volume alias need not grant any of
    /// this batch, and a named read-only alias cannot lend write authority.
    fn changes_mount<'m>(
        &self,
        mounts: &'m workspace::Mounts,
        volume: &VolumeRef,
        changes: Option<&[ContentChange<'_>]>,
        access: ContentAccess<'_, '_>,
    ) -> Result<&'m crate::workspace::Mount<Option<Arc<workspace::MountedVolume>>>> {
        let mut selected = None;
        for change in changes.into_iter().flatten() {
            let mount = mounts.by_volume_path(
                volume,
                change.path(),
                change.action(),
                self.tools.clock.now_unix_millis() / 1000,
                access
                    .io
                    .as_ref()
                    .and_then(crate::store::HistoryIo::original_native_work),
            )?;
            selected.get_or_insert(mount);
        }
        selected.map_or_else(|| mounts.by_volume(volume, access.io), Ok)
    }

    pub(super) fn publish_changes<'a, 'io: 'a, Output: acyclic_stream::ProviderTask + 'a>(
        &'a self,
        volume: &'a VolumeRef,
        operation: OperationId,
        changes: Option<&'a [ContentChange<'a>]>,
        maximum: Option<u64>,
        access: ContentAccess<'a, 'io>,
        finish: fn(Option<ContentChanges>) -> Result<Output>,
    ) -> Result<BoundedContentFuture<'a, (Output, Option<ContentHold<'io>>)>> {
        BoundedContentFuture::new(access.io, || async move {
            if let Some(changes) = changes {
                let mount = self.changes_mount(
                    &self.tools.workspace.mounts,
                    volume,
                    Some(changes),
                    access,
                )?;
                let (selected, _) = self.tools.workspace.selected_view().await?;
                let selected = selected
                    .as_ref()
                    .unwrap_or_else(|| self.tools.workspace.view());
                let workspace = mount.view.source.as_ref().map_or_else(
                    || selected.workspace(),
                    |source| source.workspace.workspace(),
                );
                let writer = self.writer_for(volume, workspace)?;
                let (original, hold) = writer
                    .observe_changes(volume, operation, maximum, access)?
                    .await?;
                if original.is_none() {
                    if mount.view.source.is_none() && self.tools.workspace.mounted.is_none() {
                        writer.authorize_changes(changes, access)?.await?;
                    } else {
                        writer.check_change_destinations(changes, access)?.await?;
                    }
                    if mount.view.source.is_none() {
                        for change in changes {
                            if matches!(
                                change,
                                ContentChange::CreateAbsent { .. }
                                    | ContentChange::UpdateExpected { .. }
                            ) && self
                                .inherited_directory(selected, change.path(), access.io)
                                .await?
                            {
                                let (latest, latest_hold) = writer
                                    .observe_changes(volume, operation, maximum, access)?
                                    .await?;
                                let published = latest.is_some();
                                drop((latest, latest_hold));
                                if !published {
                                    if mount.view.source.is_none()
                                        && self.tools.workspace.mounted.is_none()
                                    {
                                        writer.authorize_changes(changes, access)?.await?;
                                    } else {
                                        writer.check_change_destinations(changes, access)?.await?;
                                    }
                                    return Err(Error::Invalid("path is a directory".into()));
                                }
                                break;
                            }
                        }
                    }
                }
                drop((original, hold));
            }
            serialized(&self.tools.state, async move |mut state| {
                let physical = self.selected_workspace(&state)?;
                let mount = self.changes_mount(&physical.mounts, volume, changes, access)?;
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
                    Some(source) => Some(source.publication().await?),
                    None => None,
                };
                let native = native.as_ref().and_then(|(guard, _)| guard.as_ref());
                let workspace = mount
                    .view
                    .source
                    .as_ref()
                    .map_or_else(|| view.workspace(), |source| source.workspace.workspace());
                // The shared writer checks the signed scope and caller ceiling
                // for each action and missing parent at the publication cut.
                let writer = self.writer_for(volume, workspace)?;
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
                    let check = |action, path: &str| {
                        physical
                            .mounts
                            .by_volume_path(
                                volume,
                                path,
                                action,
                                self.tools.clock.now_unix_millis() / 1000,
                                access
                                    .io
                                    .as_ref()
                                    .and_then(crate::store::HistoryIo::original_native_work),
                            )
                            .map(|_| ())
                    };
                    let (receipt, hold) = writer
                        .stage_changes_checked(volume, operation, changes, access, Some(&check))?
                        .await?;
                    (Some(receipt), hold)
                } else {
                    (original, original_hold)
                };
                let Some(receipt) = receipt else {
                    return Ok((finish(None)?, hold));
                };
                for member in &receipt.members {
                    physical.mounts.by_volume_path(
                        volume,
                        member.path(),
                        match member {
                            ContentChangeResult::Created(_) => Action::Create,
                            ContentChangeResult::Updated(_) => Action::Write,
                            ContentChangeResult::Deleted(_) => Action::Delete,
                        },
                        self.tools.clock.now_unix_millis() / 1000,
                        access
                            .io
                            .as_ref()
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
            .await
        })
    }
}
