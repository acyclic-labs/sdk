use super::*;
use crate::kernel::KernelAccounting;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    pub(super) async fn commit(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        permit: crate::PublicationPermit,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        let cancellation = cancellation(accounting);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            return transaction
                .commit_with_native_work(None, permit, original, cancellation, &mut |_| async {
                    Ok::<(), LazyWorkspaceError>(())
                })
                .await;
        }
        accept_receipt(
            transaction
                .commit(
                    None,
                    permit,
                    accounting.remaining()?,
                    cancellation,
                    |_| async {
                        Ok::<_, crate::OperationFailure<LazyWorkspaceError>>(OperationReceipt {
                            value: (),
                            work: WorkCounters::UNCHARGED,
                        })
                    },
                )
                .await,
            accounting,
        )
    }

    /// Removes only the requested identity and records its source version.
    pub async fn remove(
        &self,
        path: &str,
        expected: Option<FileId>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let lookup = self.lookup(path, accounting).await?;
        let result = async {
            let state = self.state(accounting).await?;
            let removed = match &lookup.0 {
                LazyLookup::Source { node, .. } => Some(node.version),
                _ => {
                    self.removed_source_version(&state, path, accounting)
                        .await?
                }
            };
            self.remove_resolved(path, expected, &lookup.0, removed, accounting)
                .await
        }
        .await;
        self.retire_lookup(lookup, accounting)?;
        result
    }

    /// Hides one resolved path without recording a deleted source version.
    pub async fn hide_source_path(
        &self,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let lookup = self.lookup(path, accounting).await?;
        let result = self
            .remove_resolved(path, None, &lookup.0, None, accounting)
            .await;
        self.retire_lookup(lookup, accounting)?;
        result
    }

    async fn remove_resolved(
        &self,
        path: &str,
        expected: Option<FileId>,
        lookup: &LazyLookup,
        removed: Option<SourceVersion>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let actual = self
            .stable_file_id_for_lookup(path, lookup, accounting)
            .await?;
        if expected.is_some_and(|expected| expected != actual) {
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
            let state = self.state(accounting).await?;
            let shadows = if let LazyLookup::Authored { stat, .. } = lookup {
                let checkout = self
                    .checkout(crate::model::GenerationSelector::Head, accounting)
                    .await?;
                let record = checkout
                    .value
                    .lookup_file_record_by_id_with_accounting(
                        stat.file_id,
                        accounting,
                        cancellation(accounting),
                    )
                    .await;
                checkout.retire(accounting)?;
                self.insert_shadow(
                    state.shadows,
                    stat.file_id,
                    record?.ok_or(LazyWorkspaceError::NotFound)?,
                    stat.metadata,
                    accounting,
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
                    accounting,
                )
                .await?;
            let revision = state.revision;
            let replacement = LazyWorkspaceState {
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
                .compare_and_swap_lazy_workspace(
                    self.workspace.id(),
                    revision,
                    replacement,
                    accounting,
                )
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
                .checkout_with_accounting(
                    crate::model::GenerationSelector::Head,
                    crate::model::CheckoutMode::tracking_transaction(),
                    accounting,
                    cancellation(accounting),
                )
                .await?;
            let mut transaction = crate::Transaction::from_checkout(
                &self.workspace,
                checkout,
                idempotency_key,
                false,
            );
            if let Err(error) = transaction
                .apply_workspace_path_with_accounting(
                    path,
                    |path| AuthoredMutation::Remove {
                        path,
                        expected_file_id: Some(actual),
                    },
                    accounting,
                    cancellation(accounting),
                )
                .await
            {
                self.recover_pending_remove(accounting).await?;
                return Err(error.into());
            }
            match self
                .commit(
                    &mut transaction,
                    crate::PublicationPermit::Unrestricted,
                    accounting,
                )
                .await?
            {
                TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_) => {}
                TransactionCommit::Conflict { .. } | TransactionCommit::Fenced => {
                    self.recover_pending_remove(accounting).await?;
                    return Err(LazyWorkspaceError::StaleIdentity);
                }
                TransactionCommit::IdempotencyConflict => {
                    self.recover_pending_remove(accounting).await?;
                    return Err(LazyWorkspaceError::Concurrent);
                }
            }
        }
        self.recover_pending_remove(accounting).await
    }

    /// Prepares mounted removals and identity records for one DATA decision.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn prepare_mount_removals(
        &self,
        paths: &[String],
        selected_paths: &[String],
        records: &[(FileRecord, FileMetadata)],
        operation: IdempotencyKey,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = self
                .store
                .load_lazy_workspace(self.workspace.id(), accounting)
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
                return if matches!(pending.kind, PendingLazyRemoveKind::Mounted { idempotency_key } if idempotency_key == operation)
                    && state.overlay == pending.prepared_overlay
                {
                    Ok(())
                } else {
                    Err(LazyWorkspaceError::Concurrent)
                };
            }
            let mut overlay = state.overlay;
            for path in paths.iter().chain(selected_paths) {
                let removed = self
                    .removed_source_version(&state, path, accounting)
                    .await?;
                overlay = self
                    .insert_overlay(
                        overlay,
                        path,
                        LazyOverlayChange::Tombstone { removed },
                        accounting,
                    )
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
                        accounting,
                    )
                    .await?;
            }
            let revision = state.revision;
            let replacement = LazyWorkspaceState {
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
                .compare_and_swap_lazy_workspace(
                    self.workspace.id(),
                    revision,
                    replacement,
                    accounting,
                )
                .await?
            {
                return Ok(());
            }
        }
        Err(LazyWorkspaceError::Concurrent)
    }

    /// Observes the immutable publication decision without changing the binding.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn mount_removal_publication(
        &self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<bool>, LazyWorkspaceError> {
        let state = self
            .store
            .load_lazy_workspace(self.workspace.id(), accounting)
            .await?
            .ok_or(LazyWorkspaceError::NotFound)?;
        let Some(PendingLazyRemove {
            kind: PendingLazyRemoveKind::Mounted { idempotency_key },
            ..
        }) = state.pending_remove
        else {
            return Ok(None);
        };
        Ok(Some(
            self.operation_generation(idempotency_key, accounting)
                .await?
                .is_some(),
        ))
    }

    /// Finishes the mounted overlay using the original DATA decision.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn finish_mount_removals(
        &self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        let Some(committed) = self.mount_removal_publication(accounting).await? else {
            return Ok(false);
        };
        self.recover_pending_remove(accounting).await?;
        Ok(committed)
    }
}
