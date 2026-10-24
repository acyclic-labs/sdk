//! Derive immutable lazy cuts without publishing workspaces or observations.
use super::*;

mod store;
use store::ReadStore;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Reads a related view's current cut without recovery or epoch publication.
    pub async fn inspection_snapshot(
        &self,
        workspace: &Workspace<A, O>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<LazySnapshotRef, LazyWorkspaceError> {
        let state = self
            .store
            .for_inspection()
            .load_lazy_workspace(workspace.id(), ctx)
            .await?
            .ok_or(LazyWorkspaceError::NotFound)?;
        let source = self.source.reference();
        if state.schema_version != LAZY_STATE_SCHEMA
            || state.workspace_id != workspace.id()
            || state.source.identity != source.identity
            || state.source.epoch > source.epoch
        {
            return Err(LazyWorkspaceError::StaleSource);
        }
        state.require_ready_fork()?;
        if state.pending_remove.is_some() {
            return Err(LazyWorkspaceError::Concurrent);
        }
        let head = accounted!(ctx, |work, cancel| workspace.head(work, cancel))?;
        ctx.charge(WorkCounters {
            bytes_hashed: LAZY_SNAPSHOT_DOMAIN.len() as u64 + 136,
            ..WorkCounters::UNCHARGED
        })?;
        Ok(LazySnapshotRef::new(
            workspace.id(),
            head.id(),
            source,
            state.overlay,
            state.shadows,
        ))
    }

    pub(crate) async fn inspect_snapshot_on<R: AsyncAuthorityStore, T: AsyncObjectStore>(
        &self,
        workspace: Workspace<R, T>,
        cut: LazySnapshotRef,
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::Generation<R, T>, LazyWorkspaceError> {
        let inspection = LazyWorkspace {
            workspace,
            source: Arc::clone(&self.source),
            store: ReadStore(self.store.for_inspection()),
            _allocation: None,
        };
        inspection.derive_cut(cut, work, cancellation).await
    }

    async fn derive_cut(
        &self,
        cut: LazySnapshotRef,
        work: &crate::NativeMaterializationWork,
        cancel: &CancellationToken,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        use crate::facade::FsWork as _;
        let mut ctx = work.accounting(cancel);
        self.validate_snapshot(cut, &mut ctx).await?;
        let state = LazyWorkspaceState::new(cut.workspace_id, cut.source, cut.overlay, cut.shadows);
        let (mut paths, _) = self.enumerate_paths("/", cut, &state, &mut ctx).await?;
        paths.value.sort_unstable_by(|a, b| path_order(a, b));
        let result = async {
            let limits = self.workspace.limits();
            let maximum = (limits.maximum_checkout_dependencies as usize
                / (limits.maximum_path_depth as usize + 4))
                .min(limits.maximum_mutations_per_batch as usize / 4)
                .max(1);
            let mut generation = cut.authored_generation;
            for chunk in paths.value.chunks(maximum) {
                ctx.check()?;
                let checkout = self
                    .workspace
                    .checkout_accounted(
                        crate::model::GenerationSelector::Exact(generation),
                        crate::model::CheckoutMode::tracking_transaction(),
                        &mut ctx,
                        cancel,
                    )
                    .await?;
                let mut transaction = crate::Transaction::from_checkout(
                    &self.workspace,
                    checkout,
                    IdempotencyKey::from_bytes([0; 16]),
                    false,
                );
                for path in chunk {
                    let (lookup, _, guard) = self
                        .inspect_unauthored(
                            &mut transaction.checkout,
                            path,
                            Some(cut),
                            InspectBasis {
                                state: Some(&state),
                                retain_pinned_observation: true,
                                ..InspectBasis::default()
                            },
                            &mut ctx,
                        )
                        .await?;
                    if let Some(lookup) = lookup {
                        let promoted = self
                            .apply_resolved_promotion(
                                &mut transaction,
                                path,
                                &lookup,
                                u64::MAX,
                                &mut ctx,
                            )
                            .await;
                        self.retire_lookup((lookup, guard), &mut ctx)?;
                        promoted?;
                    } else {
                        crate::kernel::retire_allocation(guard, 0, &mut ctx)?;
                    }
                }
                // Only the disposable object backend receives a checkpoint.
                // There is no authority commit, workspace fork, or lazy CAS.
                generation = crate::GenerationId::new(
                    transaction
                        .checkout
                        .checkpoint_root(work, cancel)
                        .await?
                        .digest,
                );
            }
            Ok(crate::Generation {
                workspace: self.workspace.clone(),
                id: generation,
                publication_closure: None,
            })
        }
        .await;
        paths.retire(&mut ctx)?;
        result
    }
}

fn path_order(a: &str, b: &str) -> std::cmp::Ordering {
    a.bytes()
        .filter(|byte| *byte == b'/')
        .count()
        .cmp(&b.bytes().filter(|byte| *byte == b'/').count())
        .then_with(|| a.cmp(b))
}
