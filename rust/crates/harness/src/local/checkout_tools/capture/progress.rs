//! Original native DATA receipts are measured before their tool terminal.
use super::*;
use crate::runtime::NativeCaptureEvidence;

impl CheckoutTools {
    pub(in crate::local) fn finish_native_capture<'a>(
        &'a self,
        selected: (&'a LocalWorkspace, &'a workspace::View),
        slot: &'a mut Option<Cursor>,
        context: &'a ToolContext,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(None, || async move {
            if !context.native_progress_enabled() {
                return self
                    .capture(
                        selected,
                        slot,
                        context.operation_id(),
                        1,
                        PublicationPermit::Unrestricted,
                    )?
                    .await;
            }
            if context.recover_native_progress().await? {
                return Ok(());
            }
            let (physical, view) = selected;
            let operation = capture_operation(context.operation_id(), 1);
            let cancellation = CancellationToken::new();
            let key = acyclic_fs::IdempotencyKey::from_bytes(operation.into_bytes());
            let mut original = view
                .workspace()
                .operation_generation(key, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(storage)?
                .value;
            if original.is_none() {
                if physical.mounted.is_some() {
                    self.capture(
                        selected,
                        slot,
                        context.operation_id(),
                        1,
                        PublicationPermit::Unrestricted,
                    )?
                    .await?;
                } else {
                    let verify = || async { Ok(()) };
                    finish_owed_write(
                        (&self.capture_options, &self.capture_policy),
                        selected,
                        slot,
                        PublicationPermit::Unrestricted,
                        &verify,
                    )
                    .await?;
                    // A clean command needs its own immutable receipt too.
                    // Replay must never capture a later edit under this call.
                    record_changes(
                        (&self.capture_options, &self.capture_policy),
                        selected,
                        slot,
                        (context.operation_id(), 1),
                        PublicationPermit::Unrestricted,
                        &verify,
                        Some(acyclic_fs::Digest::from_bytes(
                            *blake3::hash(b"acyclic.native-content-measurement/v1\0").as_bytes(),
                        )),
                    )
                    .await?;
                }
                original = view
                    .workspace()
                    .operation_generation(key, WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(storage)?
                    .value;
            }
            let Some(after) = original else {
                return context
                    .record_native_progress(NativeCaptureEvidence::Unavailable { operation }, None)
                    .await;
            };
            if physical.mounted.is_some() {
                let work = acyclic_fs::NativeMaterializationWork::new(
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
                let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                    work: &work,
                    cancellation: &cancellation,
                };
                let publication = match view.original_publication(operation, &mut accounting).await
                {
                    Ok(publication) => publication,
                    Err(
                        acyclic_fs::LazyWorkspaceError::OriginalSnapshotUnavailable
                        | acyclic_fs::LazyWorkspaceError::StaleSource,
                    ) => None,
                    Err(error) => return Err(storage(error)),
                };
                let cuts = match view
                    .original_publication_cuts(operation, &mut accounting)
                    .await
                {
                    Ok(cuts) => cuts,
                    Err(
                        acyclic_fs::LazyWorkspaceError::OriginalSnapshotUnavailable
                        | acyclic_fs::LazyWorkspaceError::StaleSource,
                    ) => None,
                    Err(error) => return Err(storage(error)),
                };
                let (Some(publication), Some((before, after))) = (publication, cuts) else {
                    return context
                        .record_native_progress(
                            NativeCaptureEvidence::Unavailable { operation },
                            None,
                        )
                        .await;
                };
                let changed = match view
                    .content_changed_at_snapshots(before, after, &mut accounting)
                    .await
                {
                    Ok(changed) => changed,
                    Err(
                        acyclic_fs::LazyWorkspaceError::OriginalSnapshotUnavailable
                        | acyclic_fs::LazyWorkspaceError::StaleSource,
                    ) => None,
                    Err(error) => return Err(storage(error)),
                };
                return context
                    .record_native_progress(
                        NativeCaptureEvidence::Lazy {
                            publication,
                            before,
                            after,
                        },
                        changed,
                    )
                    .await;
            }
            // A physical source import is complete only when its original
            // baseline receipt exists. A refused before-image is not absence.
            if physical.import_source
                && view
                    .workspace()
                    .operation_generation(
                        acyclic_fs::IdempotencyKey::from_bytes(
                            baseline_operation(view.workspace().id()).into_bytes(),
                        ),
                        WorkBudget::UNBOUNDED,
                        &cancellation,
                    )
                    .await
                    .map_err(storage)?
                    .value
                    .is_none()
            {
                return context
                    .record_native_progress(NativeCaptureEvidence::Unavailable { operation }, None)
                    .await;
            }
            let (parents, _allocation) = after
                .parents(WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(storage)?
                .value;
            let [before] = parents.as_slice() else {
                return Err(Error::Invalid(
                    "native DATA receipt has no unique predecessor".into(),
                ));
            };
            let before = view
                .workspace()
                .generation(*before, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(storage)?
                .value;
            let changed = after
                .content_changed_from(&before, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(storage)?
                .value;
            context
                .record_native_progress(
                    NativeCaptureEvidence::Authored {
                        operation,
                        workspace: view.workspace().id(),
                        before: before.id(),
                        after: after.id(),
                    },
                    Some(changed),
                )
                .await
        })
    }
}
