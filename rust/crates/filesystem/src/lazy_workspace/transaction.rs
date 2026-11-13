use super::*;
pub(super) fn exactify_evidence(
    overlay: LazyOverlayId,
    ctx: &mut KernelAccounting<'_>,
) -> Result<acyclic_stream::OwnedBytes, LazyWorkspaceError> {
    let mut bytes = Owned::allocate(32, ctx)?;
    let copied = WorkCounters {
        bytes_copied: 32,
        ..WorkCounters::UNCHARGED
    };
    let result = ctx.admit(copied).and_then(|()| {
        bytes.value.extend_from_slice(&overlay.into_bytes());
        ctx.record_completed(copied)
    });
    if let Err(error) = result {
        bytes.retire(ctx)?;
        return Err(error.into());
    }
    Ok(crate::kernel::owned_read_bytes(
        (bytes.value, bytes.allocation).into(),
    )?)
}
impl<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
> LazyWorkspace<A, O, D, S>
{
    /// Publish authored bytes and their source facts under the existing
    /// recoverable overlay decision. A concurrent source-only removal cannot
    /// be overwritten between preparation and the DATA commit.
    pub(super) async fn commit_overlay(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        state: LazyWorkspaceState,
        overlay: LazyOverlayId,
        key: IdempotencyKey,
        permit: crate::PublicationPermit,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        let evidence = exactify_evidence(overlay, ctx)?;
        let revision = state.revision;
        let next = LazyWorkspaceState {
            revision: revision.checked_add(1).ok_or(crate::WorkError::Overflow)?,
            overlay,
            pending_remove: Some(PendingLazyRemove {
                prior_overlay: state.overlay,
                prior_shadows: state.shadows,
                prepared_overlay: overlay,
                kind: PendingLazyRemoveKind::Exactify {
                    idempotency_key: key,
                },
            }),
            ..state
        };
        if !self
            .store
            .compare_and_swap_lazy_workspace(self.workspace.id(), revision, next, ctx)
            .await?
        {
            return Err(LazyWorkspaceError::Concurrent);
        }
        let result = self
            .commit_context(
                transaction,
                Some((
                    crate::Digest::from_bytes(overlay.into_bytes()),
                    evidence,
                    permit,
                )),
                permit,
                ctx,
            )
            .await?;
        self.recover_pending_remove(ctx).await?;
        Ok(result)
    }

    pub(super) async fn commit(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        permit: crate::PublicationPermit,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        self.commit_context(transaction, None, permit, ctx).await
    }

    pub(super) async fn commit_context(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        context: Option<(
            crate::Digest,
            acyclic_stream::OwnedBytes,
            crate::PublicationPermit,
        )>,
        permit: crate::PublicationPermit,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        let cancel = cancellation(ctx);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = ctx.native_work() {
            return transaction
                .commit_with_native_work(context, permit, work, cancel, &mut |_| async {
                    Ok::<(), LazyWorkspaceError>(())
                })
                .await;
        }
        let result = transaction
            .commit(context, permit, ctx.remaining()?, cancel, |_| async {
                Ok::<_, crate::OperationFailure<LazyWorkspaceError>>(OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                })
            })
            .await;
        accept_receipt(result, ctx)
    }
    pub(super) fn overlay_mode(
        &self,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<crate::demand::SourceOverlay, LazyWorkspaceError> {
        let namespace = self.namespace_path(path, ctx)?;
        let mode = self.source.overlay(namespace.value.components());
        namespace.retire(ctx)?;
        Ok(mode)
    }

    /// Removes only the requested identity and records the deleted source version.
    pub async fn remove(
        &self,
        path: &str,
        expected: Option<FileId>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let lookup = self.lookup(path, ctx).await?;
        let result = async {
            if let LazyLookup::Authored { record, .. } = &lookup.0 {
                if self.overlay_mode(path, ctx)? == crate::demand::SourceOverlay::Defaults {
                    if expected.is_some_and(|id| id != record.file_id) {
                        return Err(LazyWorkspaceError::StaleIdentity);
                    }
                    let mut transaction = accounted!(ctx, |work, cancel| self
                        .workspace
                        .begin_transaction(IdempotencyKey::new(), work, cancel))?;
                    accounted!(ctx, |work, cancel| transaction.remove_if(
                        path,
                        record.file_id,
                        work,
                        cancel
                    ))?;
                    return match self
                        .commit(
                            &mut transaction,
                            crate::PublicationPermit::Unrestricted,
                            ctx,
                        )
                        .await?
                    {
                        TransactionCommit::Committed(_)
                        | TransactionCommit::AlreadyCommitted(_) => Ok(()),
                        _ => Err(LazyWorkspaceError::Concurrent),
                    };
                }
            }
            let removed = match &lookup.0 {
                LazyLookup::Source { node, .. } => Some(node.version),
                _ => {
                    self.removed_source_version(&self.state(ctx).await?, path, ctx)
                        .await?
                }
            };
            self.remove_resolved(path, expected, &lookup.0, removed, ctx)
                .await
        }
        .await;
        self.retire_lookup(lookup, ctx)?;
        result
    }
    /// Hides a path without claiming a deleted source version.
    pub async fn hide_source_path(
        &self,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let lookup = self.lookup(path, ctx).await?;
        let result = self.remove_resolved(path, None, &lookup.0, None, ctx).await;
        self.retire_lookup(lookup, ctx)?;
        result
    }
    async fn remove_resolved(
        &self,
        path: &str,
        expected: Option<FileId>,
        lookup: &LazyLookup,
        removed: Option<SourceVersion>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let actual = self.stable_file_id_for_lookup(path, lookup, ctx).await?;
        if expected.is_some_and(|id| id != actual) {
            return Err(LazyWorkspaceError::StaleIdentity);
        }
        let kind = if matches!(lookup, LazyLookup::Authored { .. }) {
            PendingLazyRemoveKind::Authored {
                idempotency_key: IdempotencyKey::new(),
            }
        } else {
            PendingLazyRemoveKind::SourceOnly
        };
        {
            let state = self.state(ctx).await?;
            let shadows = if let LazyLookup::Authored {
                record, metadata, ..
            } = lookup
            {
                self.insert_shadow(state.shadows, record.file_id, *record, *metadata, ctx)
                    .await?
            } else {
                state.shadows
            };
            let overlay = self
                .insert_overlay(
                    state.overlay,
                    path,
                    LazyOverlayChange::Tombstone { removed },
                    ctx,
                )
                .await?;
            let revision = state.revision;
            let next = LazyWorkspaceState {
                revision: revision.checked_add(1).ok_or(crate::WorkError::Overflow)?,
                overlay,
                shadows,
                pending_remove: Some(PendingLazyRemove {
                    prior_overlay: state.overlay,
                    prior_shadows: state.shadows,
                    prepared_overlay: overlay,
                    kind,
                }),
                ..state
            };
            if !self
                .store
                .compare_and_swap_lazy_workspace(self.workspace.id(), revision, next, ctx)
                .await?
            {
                return Err(LazyWorkspaceError::Concurrent);
            }
        }
        if let PendingLazyRemoveKind::Authored { idempotency_key } = kind {
            let mut transaction = accounted!(ctx, |work, cancel| self
                .workspace
                .begin_transaction(idempotency_key, work, cancel))?;
            let staged = accounted!(ctx, |work, cancel| transaction
                .remove_if(path, actual, work, cancel));
            if let Err(error) = staged {
                self.recover_pending_remove(ctx).await?;
                return Err(error);
            }
            match self
                .commit(
                    &mut transaction,
                    crate::PublicationPermit::Unrestricted,
                    ctx,
                )
                .await?
            {
                TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_) => {}
                outcome => {
                    self.recover_pending_remove(ctx).await?;
                    return Err(
                        if matches!(outcome, TransactionCommit::IdempotencyConflict) {
                            LazyWorkspaceError::Concurrent
                        } else {
                            LazyWorkspaceError::StaleIdentity
                        },
                    );
                }
            }
        }
        self.recover_pending_remove(ctx).await
    }

    /// Prepares mounted removals and identity shadows for the same DATA decision.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn prepare_mount_removals(
        &self,
        paths: &[String],
        selected: &[String],
        records: &[(FileRecord, FileMetadata)],
        operation: IdempotencyKey,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let (mut state, source) = self
            .recorded_binding(|| LazyWorkspaceError::NotFound, ctx)
            .await?;
        if let Some(pending) = &state.pending_remove {
            if state.source != source {
                return Err(LazyWorkspaceError::StaleSource);
            }
            return if matches!(pending.kind,PendingLazyRemoveKind::Mounted { idempotency_key } if idempotency_key == operation)
                && state.overlay == pending.prepared_overlay
            {
                Ok(())
            } else {
                Err(LazyWorkspaceError::Concurrent)
            };
        }
        // This mutation, rather than a preceding read, owns the epoch update.
        state.source = source;
        let mut overlay = state.overlay;
        let mut authored = None;
        for path in paths.iter().chain(selected) {
            if self.overlay_mode(path, ctx)? == crate::demand::SourceOverlay::Defaults {
                if authored.is_none() {
                    authored = Some(
                        self.workspace
                            .checkout_accounted(
                                crate::model::GenerationSelector::Head,
                                crate::model::CheckoutMode::tracking_transaction(),
                                ctx,
                                cancellation(ctx),
                            )
                            .await?,
                    );
                }
                if self
                    .authored_record(
                        authored.as_mut().ok_or(LazyWorkspaceError::NotFound)?,
                        path,
                        ctx,
                    )
                    .await?
                    .is_some()
                {
                    // The DATA decision removes this authored override. Keep
                    // the prior source observation instead of hiding its default.
                    continue;
                }
            }
            let removed = self.removed_source_version(&state, path, ctx).await?;
            overlay = self
                .insert_overlay(overlay, path, LazyOverlayChange::Tombstone { removed }, ctx)
                .await?;
        }
        let mut shadows = state.shadows;
        for (record, metadata) in records {
            shadows = self
                .insert_shadow(
                    shadows,
                    record.file_id,
                    *record,
                    crate::WorkspaceMetadata::from_engine(*metadata),
                    ctx,
                )
                .await?;
        }
        let revision = state.revision;
        let next = LazyWorkspaceState {
            revision: revision.checked_add(1).ok_or(crate::WorkError::Overflow)?,
            overlay,
            shadows,
            pending_remove: Some(PendingLazyRemove {
                prior_overlay: state.overlay,
                prior_shadows: state.shadows,
                prepared_overlay: overlay,
                kind: PendingLazyRemoveKind::Mounted {
                    idempotency_key: operation,
                },
            }),
            ..state
        };
        if self
            .store
            .compare_and_swap_lazy_workspace(self.workspace.id(), revision, next, ctx)
            .await?
        {
            return Ok(());
        }
        Err(LazyWorkspaceError::Concurrent)
    }
    /// Reads the original mounted publication decision without updating the binding.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn mount_removal_publication(
        &self,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<bool>, LazyWorkspaceError> {
        let state = self
            .store
            .load_lazy_workspace(self.workspace.id(), ctx)
            .await?
            .ok_or(LazyWorkspaceError::NotFound)?;
        match state.pending_remove {
            Some(PendingLazyRemove {
                kind: PendingLazyRemoveKind::Mounted { idempotency_key },
                ..
            }) => Ok(Some(
                self.operation_generation(idempotency_key, ctx)
                    .await?
                    .is_some(),
            )),
            _ => Ok(None),
        }
    }
    /// Finishes the mounted overlay from its actual DATA result.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn finish_mount_removals(
        &self,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        let Some(committed) = self.mount_removal_publication(ctx).await? else {
            return Ok(false);
        };
        self.recover_pending_remove(ctx).await?;
        Ok(committed)
    }
}
