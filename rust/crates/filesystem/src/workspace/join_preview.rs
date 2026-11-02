//! One immutable merge projection and its demand-driven clean/conflict rows.

use super::*;
use crate::kernel::allocation::fixed_index::FixedIndex;
use crate::kernel::allocation::retained::{ReadOwner, Retained};
use crate::{OperationFailure, OperationReceipt, WorkCounters};

/// One unresolved identity, retaining any original binding-name allocation.
/// Describing a selected conflict does not require expanding its siblings.
pub struct JoinConflictEntry {
    row: Retained<MergeConflict>,
}

impl JoinConflictEntry {
    /// The exact file identity or directory/name pair from the merge producer.
    pub fn value(&self) -> &MergeConflict {
        &self.row.value
    }
}

/// A read-only projection of a join's pinned endpoints. Clean paths and raw
/// conflicts advance independently, without publishing either workspace.
/// Rows retain their original backing if they outlive the preview. A refused
/// or withdrawn clean-row call is terminal; dropping the preview retires its
/// frontier, generation pins and closure hold before the original read frame.
pub struct JoinPreview<A: 'static, O: 'static> {
    owner: ReadOwner<State<A, O>>,
}

struct State<A: 'static, O: 'static> {
    changes: super::changed_paths::State<A, O>,
    conflicts: Option<Retained<std::vec::IntoIter<MergeConflict>>>,
    excluded: FixedIndex<()>,
    plan: JoinPlan<A, O>,
    // Checkouts and traversal retire before the original projection closure.
    _hold: Option<crate::PublicationHold>,
    conflict_count: usize,
    clean_ready: bool,
    conflict_ready: bool,
    failed: bool,
}

impl<A, O> State<A, O> {
    fn new(plan: JoinPlan<A, O>) -> Self {
        Self {
            changes: Default::default(),
            conflicts: None,
            excluded: Default::default(),
            plan,
            _hold: None,
            conflict_count: 0,
            clean_ready: false,
            conflict_ready: false,
            failed: false,
        }
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> JoinPlan<A, O> {
    /// Retains the original merge's clean projection and conflicts for paging.
    /// This consumes one original read frame. The total budget and cancellation
    /// apply to preparation and every subsequent row, without a per-page reset.
    pub async fn preview_pages<W: crate::facade::FsWork>(
        self,
        options: ApplyOptions,
        work: W,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<JoinPreview<A, O>>, WorkspaceError> {
        let accounting = work.accounting(cancellation);
        let initial = accounting.spent();
        let mut owner = ReadOwner::new(accounting, State::new(self))
            .map_err(|error| OperationFailure::new(WorkspaceError::from(error), initial))?;
        let result = async {
            let (state, mut accounting) = owner.borrow();
            let plan = &state.plan;
            let cancellation = accounting.cancellation();
            let prepared = plan
                .preview_with_accounting(options, &mut accounting, cancellation)
                .await?;
            let JoinPreparation::Preview {
                generation_root,
                conflicts,
                hold,
            } = prepared
            else {
                return Err(WorkspaceError::InvalidMergeResolution);
            };
            state._hold = Some(hold);
            state.conflict_count = conflicts.value.len();
            // Moving the Vec into its iterator preserves its backing exactly.
            if state.conflict_count == 0 {
                conflicts.retire(&mut accounting)?;
            } else {
                let conflicts = Retained::from_owned(conflicts, &mut accounting)?;
                for conflict in &conflicts.value {
                    accounting.charge_items(1)?;
                    if let MergeConflict::File(id) = conflict {
                        state.excluded.get_or_try_insert_with(
                            id.into_bytes(),
                            &mut accounting,
                            |_| Ok(()),
                        )?;
                    }
                }
                state.conflicts = Some(Retained {
                    value: conflicts.value.into_iter(),
                    allocation: conflicts.allocation,
                });
            }
            let target = &plan.target_head.workspace;
            let before = target
                .open_checkout(
                    GenerationSelector::Exact(plan.target_head.id),
                    CheckoutMode::read_only_pinned(),
                    &mut accounting,
                    cancellation,
                )
                .await?;
            state.changes.set_checkouts([
                before,
                target
                    .volume
                    .open_join_preview(generation_root, &mut accounting, cancellation)
                    .await?,
            ]);
            state
                .changes
                .initialize(target.volume.config(), &mut accounting)
        }
        .await;
        let spent = owner.borrow().1.spent();
        match result {
            Ok(()) => Ok(OperationReceipt {
                value: JoinPreview { owner },
                work: spent,
            }),
            Err(error) => {
                drop(owner);
                Err(OperationFailure::new(error, spent))
            }
        }
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> JoinPreview<A, O> {
    /// Exact original conflict count; no path or content expansion is performed.
    pub fn conflict_count(&mut self) -> usize {
        self.owner.borrow().0.conflict_count
    }

    /// The original immutable source, target and base, retained for selection.
    pub fn plan(&mut self) -> &JoinPlan<A, O> {
        &self.owner.borrow().0.plan
    }

    /// Reads one selected side's exact record under this preview's original
    /// work and cancellation. A binding conflict selects that side's actual
    /// directory/name target; an absent target returns `None`. This loads no
    /// file body, expands no sibling conflict and does not advance either page.
    /// Keep the preview alive while following the record's object references.
    pub async fn conflict_record(
        &mut self,
        conflict: &MergeConflict,
        side: ConflictSide,
    ) -> crate::MeasuredResult<OperationReceipt<Option<crate::kernel::FileRecord>>, WorkspaceError>
    {
        let (state, mut accounting) = self.owner.borrow();
        let result = async {
            if state.failed {
                return Err(FsError::InvalidDiff.into());
            }
            state.failed = true;
            accounting.check()?;
            let plan = &state.plan;
            let (workspace, generation) = match side {
                ConflictSide::Base => (&plan.base_workspace, plan.base.id),
                ConflictSide::Ours => (&plan.target_head.workspace, plan.target_head.id),
                ConflictSide::Theirs => (&plan.source_head.workspace, plan.source_head.id),
            };
            let cancellation = accounting.cancellation();
            let record = workspace
                .volume
                .fs
                .workspace_merge_record_with_accounting(
                    &workspace.volume,
                    generation,
                    conflict,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            state.failed = false;
            Ok(record)
        }
        .await;
        let work = accounting.spent();
        result
            .map(|value| OperationReceipt { value, work })
            .map_err(|error| OperationFailure::new(error, work))
    }

    /// Advances the original changed-path frontier, excluding unresolved files.
    /// Work is proportional to actual changed rows and fixed-width identity
    /// decisions, independent of unrelated namespace size. Rows follow the same
    /// file/edge order as `ChangedPaths`; no second diff or prefix replay occurs.
    pub async fn next_clean(
        &mut self,
    ) -> crate::MeasuredResult<OperationReceipt<Option<ChangedPathEntry>>, WorkspaceError> {
        let (state, mut accounting) = self.owner.borrow();
        let result = state.prepare_clean(&mut accounting).await;
        let work = accounting.spent();
        result
            .map(|()| {
                state.clean_ready = false;
                OperationReceipt {
                    value: state.changes.pending.take(),
                    work,
                }
            })
            .map_err(|error| OperationFailure::new(error, work))
    }

    /// Borrows one clean row so an encoder can check its actual byte credit
    /// before taking it. Repeated borrows retain the same traversal position.
    pub async fn peek_clean(
        &mut self,
    ) -> crate::MeasuredResult<OperationReceipt<Option<&ChangedPath>>, WorkspaceError> {
        let (state, mut accounting) = self.owner.borrow();
        let result = state.prepare_clean(&mut accounting).await;
        let work = accounting.spent();
        result
            .map(|()| OperationReceipt {
                value: state.changes.pending.as_ref().map(ChangedPathEntry::value),
                work,
            })
            .map_err(|error| OperationFailure::new(error, work))
    }

    /// Takes one raw conflict without reading any other conflict's path/body.
    /// Only this binding name's existing allocation moves to the returned row,
    /// without fresh admission. Taking the final row retires the Vec buffer.
    pub fn next_conflict(
        &mut self,
    ) -> crate::MeasuredResult<OperationReceipt<Option<JoinConflictEntry>>, WorkspaceError> {
        let (state, mut accounting) = self.owner.borrow();
        let result = (|| -> Result<_, WorkspaceError> {
            state.prepare_conflict(&mut accounting)?;
            state.failed = true;
            let Some(conflicts) = state.conflicts.as_mut() else {
                state.failed = false;
                return Ok(None);
            };
            let row = conflicts
                .value
                .as_slice()
                .first()
                .ok_or(FsError::InvalidDiff)?;
            let bytes = match row {
                MergeConflict::Binding { name, .. } => {
                    crate::foundation::usize_to_u64(name.retained_bytes())
                }
                MergeConflict::File(_) => 0,
            };
            let allocation = conflicts.allocation.split_off(bytes)?;
            let row = conflicts.value.next().map(|value| JoinConflictEntry {
                row: Retained { value, allocation },
            });
            if conflicts.value.as_slice().is_empty() {
                drop(state.conflicts.take());
            }
            state.conflict_ready = false;
            state.failed = false;
            Ok(row)
        })();
        let work = accounting.spent();
        result
            .map(|value| OperationReceipt { value, work })
            .map_err(|error| OperationFailure::new(error, work))
    }

    /// Borrows the next exact identity so an encoder can admit its output
    /// before consuming it. Repeated borrows do no additional work or copying;
    /// the original cancellation token remains authoritative on every call.
    pub fn peek_conflict(
        &mut self,
    ) -> crate::MeasuredResult<OperationReceipt<Option<&MergeConflict>>, WorkspaceError> {
        let (state, mut accounting) = self.owner.borrow();
        let result = state.prepare_conflict(&mut accounting);
        let work = accounting.spent();
        result
            .map(|()| OperationReceipt {
                value: state
                    .conflicts
                    .as_ref()
                    .and_then(|conflicts| conflicts.value.as_slice().first()),
                work,
            })
            .map_err(|error| OperationFailure::new(error, work))
    }

    /// Cumulative work on the same original frame, including typed refusals.
    pub fn work(&mut self) -> WorkCounters {
        self.owner.borrow().1.spent()
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> State<A, O> {
    fn prepare_conflict(
        &mut self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), WorkspaceError> {
        if self.failed {
            return Err(FsError::InvalidDiff.into());
        }
        self.failed = true;
        if self.conflict_ready {
            accounting.check()?;
        } else {
            accounting.charge_items(1)?;
            self.conflict_ready = true;
        }
        self.failed = false;
        Ok(())
    }

    async fn prepare_clean(
        &mut self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), WorkspaceError> {
        if self.failed {
            return Err(FsError::InvalidDiff.into());
        }
        self.failed = true;
        if self.clean_ready {
            accounting.check()?;
            self.failed = false;
            return Ok(());
        }
        loop {
            self.changes.prepare_next(accounting).await?;
            let Some(row) = self.changes.pending.as_ref() else {
                self.clean_ready = true;
                self.failed = false;
                return Ok(());
            };
            let row = row.value();
            let before = row.before.map(|record| record.file_id);
            let after = row.after.map(|record| record.file_id);
            let mut excluded = false;
            for id in [before, after.filter(|id| Some(*id) != before)]
                .into_iter()
                .flatten()
            {
                excluded |= self.excluded.get(id.into_bytes(), accounting)?.is_some();
            }
            if !excluded {
                self.clean_ready = true;
                self.failed = false;
                return Ok(());
            }
            drop(self.changes.pending.take());
        }
    }
}
