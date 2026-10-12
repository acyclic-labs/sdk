use super::*;
impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    pub(super) async fn commit(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        permit: crate::PublicationPermit,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        let cancel = cancellation(ctx);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = ctx.native_work() {
            return transaction
                .commit_with_native_work(None, permit, work, cancel, &mut |_| async {
                    Ok::<(), LazyWorkspaceError>(())
                })
                .await;
        }
        let result = transaction
            .commit(None, permit, ctx.remaining()?, cancel, |_| async {
                Ok::<_, crate::OperationFailure<LazyWorkspaceError>>(OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                })
            })
            .await;
        accept_receipt(result, ctx)
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
        let mut prepared = false;
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = self.state(ctx).await?;
            let shadows = if let LazyLookup::Authored { stat, .. } = lookup {
                let checkout = self
                    .checkout(crate::model::GenerationSelector::Head, ctx)
                    .await?;
                let record = checkout
                    .value
                    .lookup_file_record_by_id_with_accounting(stat.file_id, ctx, cancellation(ctx))
                    .await;
                checkout.retire(ctx)?;
                self.insert_shadow(
                    state.shadows,
                    stat.file_id,
                    record?.ok_or(LazyWorkspaceError::NotFound)?,
                    stat.metadata,
                    ctx,
                )
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
            if self
                .store
                .compare_and_swap_lazy_workspace(self.workspace.id(), revision, next, ctx)
                .await?
            {
                prepared = true;
                break;
            }
        }
        if !prepared {
            return Err(LazyWorkspaceError::Concurrent);
        }
        if let PendingLazyRemoveKind::Authored { idempotency_key } = kind {
            let checkout = self
                .workspace
                .checkout_accounted(
                    crate::model::GenerationSelector::Head,
                    crate::model::CheckoutMode::tracking_transaction(),
                    ctx,
                    cancellation(ctx),
                )
                .await?;
            let mut transaction = crate::Transaction::from_checkout(
                &self.workspace,
                checkout,
                idempotency_key,
                false,
            );
            let staged = accounted!(ctx, |work, cancel| transaction.mutate(
                path,
                |path| Ok::<_, WorkspaceError>(AuthoredMutation::Remove {
                    path,
                    expected_file_id: Some(actual)
                }),
                work,
                cancel
            ));
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
    #[cfg(feature = "native-mount")]
    pub(crate) async fn prepare_mount_removals(
        &self,
        paths: &[String],
        selected: &[String],
        records: &[(FileRecord, FileMetadata)],
        operation: IdempotencyKey,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = self
                .store
                .load_lazy_workspace(self.workspace.id(), ctx)
                .await?
                .ok_or(LazyWorkspaceError::NotFound)?;
            state.require_ready_fork()?;
            if state.schema_version != LAZY_STATE_SCHEMA
                || state.workspace_id != self.workspace.id()
                || state.source != self.source.reference()
            {
                return Err(LazyWorkspaceError::StaleSource);
            }
            if let Some(pending) = &state.pending_remove {
                return if matches!(pending.kind,PendingLazyRemoveKind::Mounted { idempotency_key } if idempotency_key == operation)
                    && state.overlay == pending.prepared_overlay
                {
                    Ok(())
                } else {
                    Err(LazyWorkspaceError::Concurrent)
                };
            }
            let mut overlay = state.overlay;
            for path in paths.iter().chain(selected) {
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
        }
        Err(LazyWorkspaceError::Concurrent)
    }
    /// Reads the original mounted publication decision without updating the binding.
    #[cfg(feature = "native-mount")]
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
    #[cfg(feature = "native-mount")]
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
