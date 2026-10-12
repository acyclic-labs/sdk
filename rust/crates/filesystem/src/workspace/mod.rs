//! Customer workspace identities and immutable generation handles.

use acyclic_stream::ProviderTask;
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
use crate::foundation::{FileId, GenerationId, OperationId, VolumeId};
use crate::CancellationToken;
use crate::heap_future::in_heap;
use crate::kernel::{
    ExtentKind, FileKind, FileMetadata, FilePayload, LogicalName, MetadataField, NamespacePath,
};
use crate::model::{CheckoutMode, GenerationSelector};
use crate::path::PortablePath;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, Checkout, CheckoutCommitOutcome,
    ConflictKey, ConflictKind, ConflictSide, ConflictValue, FsError, GenerationDiff, MergeConflict,
    MergeDriverRegistry, MergePlan, MergePlanResolutionError, MergeResolution,
    MergeResolutionCache, UnpublishedMergeCandidate, Volume, resolve_merge_plan,
};
use crate::performance::WorkBudget;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use thiserror::Error;
use unicode_normalization::UnicodeNormalization;


mod accounting;
mod checkout;
mod commit;
mod directory;
mod errors;
mod extents;
mod fork;
mod generation;
mod handle;
mod identity;
mod materialize;
mod metadata;
mod mutate;
mod name;
mod path_apply;
mod paths;
mod publication;
mod reads;
mod retention;
mod transaction;
mod writes;

pub use commit::{TransactionCommit, TransactionRebase};
pub use errors::{WorkspaceError};
pub use extents::{WorkspaceExtentKind, WorkspaceExtentSpan, WorkspaceExtentPlan};
pub use fork::{ForkOptions};
pub use generation::{Generation};
pub use handle::{Workspace};
pub use identity::{WorkspaceId, IdempotencyKey};
pub use metadata::{WorkspaceStat, WorkspaceMetadata};
pub use name::{WorkspaceName, WorkspaceNameError};
pub use path_apply::{WorkspacePathConflict, WorkspacePathApply};
pub use retention::{WorkspaceDelete};
pub use transaction::{Transaction};

pub(crate) use accounting::{workspace_owned_strings_bytes, workspace_vec_bytes, workspace_remaining};
use accounting::{merge_workspace_backend, workspace_allocation_error};
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
use accounting::merge_workspace_work;
pub(crate) use paths::{customer_path, customer_path_with_accounting};
use paths::{logical_name_text, namespace_path_text, regular_file_bytes, restore_absolute_path, copy_workspace_label};
pub(crate) use errors::workspace_bounded_error;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Restores an exact generation under the supplied publication permit.
    pub async fn restore_generation(
        &self,
        generation: &Generation<A, O>,
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
    ) -> Result<WorkspaceRestore<Generation<A, O>>, WorkspaceError> {
        if generation.workspace.id != self.id {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let outcome = self
            .volume
            .fs
            .restore_workspace_generation(
                &self.volume,
                generation.id,
                if_current,
                idempotency_key.operation_id(),
                permit,
            )
            .await?;
        let restored = |id| Generation { workspace: self.clone(), id, publication_closure: None };
        Ok(match outcome {
            WorkspaceRestore::Restored(id) => WorkspaceRestore::Restored(restored(id)),
            WorkspaceRestore::AlreadyRestored(id) => {
                WorkspaceRestore::AlreadyRestored(restored(id))
            }
            WorkspaceRestore::Current(id) => WorkspaceRestore::Current(restored(id)),
            WorkspaceRestore::Stale(id) => WorkspaceRestore::Stale(restored(id)),
            WorkspaceRestore::Fenced => WorkspaceRestore::Fenced,
            WorkspaceRestore::IdempotencyConflict => WorkspaceRestore::IdempotencyConflict,
        })
    }

    /// Restores selected records through the original publication engine with
    pub async fn restore_paths_from<F, Fut>(
        &self,
        source: &Generation<A, O>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionCommit<A, O>>, WorkspaceError>
    where
        F: FnMut(crate::WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::FsResult<()>> + ProviderTask,
    {
        self.restore_path_records(
            source, paths, if_current, idempotency_key, permit, budget, cancellation,
            false, before_publication,
        ).await
    }

    /// Restores a caller-expanded selected subtree record union with one seal.
    pub async fn restore_selected_paths_from<F, Fut>(
        &self,
        source: &Generation<A, O>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionCommit<A, O>>, WorkspaceError>
    where
        F: FnMut(crate::WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::FsResult<()>> + ProviderTask,
    {
        self.restore_path_records(
            source, paths, if_current, idempotency_key, permit, budget, cancellation,
            true, before_publication,
        ).await
    }

    /// Restores the caller-expanded frozen selection at the original native seal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn restore_selected_paths_from_with_permit_guarded_with_native_work<'original, E, F, Fut>(
        &self, source: &Generation<A, O>, paths: &[String], if_current: GenerationId,
        idempotency_key: IdempotencyKey, permit: crate::PublicationPermit,
        original: &'original crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken, before_publication: &mut F,
    ) -> Result<TransactionCommit<A, O>, E>
    where
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        if !self.volume.fs.same_deployment(&source.workspace.volume.fs) {
            return Err(E::from(WorkspaceError::ForeignGeneration));
        }
        let current = self.head(original, cancellation).await.map_err(E::from)?;
        if current.id != if_current {
            return Ok(TransactionCommit::Conflict { actual: current });
        }
        let mut transaction = self.begin_pinned_transaction(
            &current, idempotency_key, original, cancellation,
        ).await.map_err(E::from)?;
        let mut accounting = KernelAccounting::native(original, cancellation);
        transaction.checkout.validate_native_work(original).map_err(FsError::from)?;
        let source_root = self.restore_source_root_with_accounting(source, &mut accounting, cancellation)
            .await.map_err(E::from)?;
        transaction.restore_paths_from_root_with_accounting(
            source, paths, true, source_root, &mut accounting, cancellation,
        ).await.map_err(E::from)?;
        transaction.commit_with_native_work(
            None, permit, original, cancellation, before_publication,
        ).await
    }

    async fn restore_path_records<F, Fut>(
        &self,
        source: &Generation<A, O>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        selected_subtrees: bool,
        mut before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionCommit<A, O>>, WorkspaceError>
    where
        F: FnMut(crate::WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::FsResult<()>> + ProviderTask,
    {
        crate::heap_future::in_heap_measured(move |budget| async move {
            let mut work = crate::WorkCounters::UNCHARGED;
            let mut allocations = crate::kernel::allocation::AllocationLedger::default();
            let result = async {
                if !self.volume.fs.same_deployment(&source.workspace.volume.fs) {
                    return Err(WorkspaceError::ForeignGeneration);
                }
                let current = merge_workspace_backend(
                    self.head(budget, cancellation).await, &mut work, 0,
                )?.value;
                if current.id != if_current {
                    return Ok(TransactionCommit::Conflict { actual: current });
                }
                let source_root = {
                    let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                        &mut allocations, &mut work, budget, cancellation,
                    );
                    self.restore_source_root_with_accounting(source, &mut accounting, cancellation).await?
                };
                let allowance = workspace_remaining(work, budget, allocations.live_bytes())?;
                let mut transaction = merge_workspace_backend(
                    self.begin_pinned_transaction(
                        &current, idempotency_key, allowance, cancellation,
                    ).await, &mut work, allocations.live_bytes(),
                )?.value;
                allocations.claim_bytes(transaction.checkout.logical_owned_bytes(), 0, &mut work, budget)
                    .map_err(workspace_allocation_error)?;
                {
                    let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                        &mut allocations, &mut work, budget, cancellation,
                    );
                    transaction.restore_paths_from_root_with_accounting(
                        source, paths, selected_subtrees, source_root, &mut accounting, cancellation,
                    ).await?;
                }
                let allowance = workspace_remaining(work, budget, allocations.live_bytes())?;
                let committed = merge_workspace_backend(
                    transaction.commit(None, permit, allowance, cancellation, |budget| {
                            let future = before_publication(budget);
                            async move { future.await.map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::UNCHARGED, WorkspaceError::from)) }
                        }).await, &mut work, allocations.live_bytes(),
                )?;
                Ok(committed.value)
            }.await;
            work.verify(budget)
                .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
            result.map(|value| crate::OperationReceipt { value, work })
                .map_err(|error| crate::OperationFailure::new(error, work))
        }, budget, cancellation, WorkspaceError::Cancelled(crate::CancellationError), None).await
    }

    async fn restore_source_root_with_accounting(
        &self, source: &Generation<A, O>, accounting: &mut KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::kernel::Owned<Checkout<A, O>>, WorkspaceError> {
        if !self.volume.fs.same_deployment(&source.workspace.volume.fs) {
            return Err(WorkspaceError::ForeignGeneration);
        }
        source.workspace.open_checkout(
            GenerationSelector::Exact(source.id), CheckoutMode::read_only_pinned(), accounting, cancellation,
        ).await
    }

    /// Computes one immutable bounded semantic change set between two exact
    pub async fn diff(
        &self,
        from: &Generation<A, O>,
        to: &Generation<A, O>,
        maximum_changes: u32,
    ) -> Result<ChangeSet<A, O>, WorkspaceError> {
        if from.workspace.id != self.id || to.workspace.id != self.id
            || !self.volume.fs.same_deployment(&from.workspace.volume.fs)
            || !self.volume.fs.same_deployment(&to.workspace.volume.fs)
        {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let receipt = self
            .volume
            .diff_generations(
                from.id,
                to.id,
                maximum_changes,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?;
        if receipt.value.truncated {
            return Err(WorkspaceError::JoinLimit);
        }
        Ok(ChangeSet {
            from: from.clone(),
            to: to.clone(),
            changes: receipt.value,
            work: receipt.work,
        })
    }

    /// Begins a side-effect-free join description from this workspace into a
    #[must_use]
    pub fn join_into(&self, target: &Workspace<A, O>) -> JoinBuilder<A, O> {
        JoinBuilder {
            source: self.clone(),
            target: target.clone(),
            history: JoinHistory::Merge,
            maximum_generations: 4_096,
            maximum_changes: self.volume.config().limits.maximum_paths_per_batch,
            maximum_conflicts: 1_024,
        }
    }

    /// Advances this fork to its source workspace's current generation while
    pub async fn live_rebase(
        &self,
        idempotency_key: IdempotencyKey,
        maximum_generations: u32,
        maximum_changes: u32,
        maximum_conflicts: u32,
    ) -> Result<WorkspaceRebase<A, O>, WorkspaceError> {
        let outcome = self
            .volume
            .fs
            .live_rebase_workspace(crate::facade::WorkspaceRebaseRequest {
                target: &self.volume,
                operation_id: idempotency_key.operation_id(),
                maximum_generations,
                maximum_changes,
                maximum_conflicts,
            })
            .await?;
        let generation = |id| Generation { workspace: self.clone(), id, publication_closure: None };
        Ok(match outcome {
            crate::facade::WorkspaceJoinOutcome::Applied(id) => {
                WorkspaceRebase::Rebased(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::AlreadyApplied(id) => {
                WorkspaceRebase::AlreadyRebased(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::Joined(_, _)
            | crate::facade::WorkspaceJoinOutcome::AlreadyJoined(_, _) => {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            crate::facade::WorkspaceJoinOutcome::NoChanges(id) => {
                WorkspaceRebase::Current(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::Stale(id) => {
                WorkspaceRebase::Stale(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::Conflicted(conflicts, truncated) => {
                WorkspaceRebase::Conflicted {
                    conflicts,
                    truncated,
                }
            }
            crate::facade::WorkspaceJoinOutcome::Fenced => WorkspaceRebase::Fenced,
            crate::facade::WorkspaceJoinOutcome::IdempotencyConflict => {
                WorkspaceRebase::IdempotencyConflict
            }
        })
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
    async fn restore_paths_from_root_with_accounting(
        &mut self,
        source: &Generation<A, O>,
        paths: &[String],
        selected_subtrees: bool,
        mut source_root: crate::kernel::Owned<Checkout<A, O>>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), WorkspaceError> {
        if !self.workspace.volume.fs.same_deployment(&source.workspace.volume.fs) {
            return Err(WorkspaceError::ForeignGeneration);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            self.checkout.validate_native_work(original)?;
        }
        let mut parsed = crate::kernel::Owned::allocate(paths.len(), accounting)?;
        for path in paths {
            cancellation.check()?;
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let absolute = restore_absolute_path(
                path.trim_start_matches('/'), accounting, cancellation,
            )?;
            let parsed_path = self.mutation_path(&absolute.value, accounting, cancellation)?;
            absolute.retire(accounting)?;
            Self::push_mutation(&mut parsed, parsed_path, accounting)?;
        }
        let source_records = match source_root.value.lookup_batch_no_follow_with_accounting(
            &parsed.value, selected_subtrees, accounting, cancellation,
        ).await {
            Ok((records, allocation)) => {
                let bytes = records.retained_allocation_bytes;
                crate::kernel::Owned::new(records, allocation, bytes)
            }
            Err(error) => {
                source_root.retire(accounting)?;
                return Err(error.into());
            }
        };
        let mut missing_count = 0;
        for entry in &source_records.value.entries {
            cancellation.check()?;
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            missing_count += usize::from(entry.record.is_none());
        }
        let mut missing_paths = crate::kernel::Owned::allocate(missing_count, accounting)?;
        for (path, entry) in parsed.value.iter().zip(&source_records.value.entries) {
            cancellation.check()?;
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            if entry.record.is_none() {
                let (path, allocation) = path.clone_with_accounting(accounting, cancellation)?;
                let bytes = path.retained_bytes();
                Self::push_mutation(
                    &mut missing_paths, crate::kernel::Owned::new(path, allocation, bytes), accounting,
                )?;
            }
        }
        let current_records = if missing_paths.value.is_empty() {
            None
        } else {
            let (records, allocation) = self.checkout.lookup_batch_no_follow_with_accounting(
                &missing_paths.value, false, accounting, cancellation,
            ).await?;
            let bytes = records.retained_allocation_bytes;
            Some(crate::kernel::Owned::new(records, allocation, bytes))
        };
        if selected_subtrees {
            let mut removal_count = 0;
            if let Some(records) = &current_records {
                for entry in &records.value.entries {
                    cancellation.check()?;
                    accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                    removal_count += usize::from(entry.record.is_some());
                }
            }
            let mut removals = crate::kernel::Owned::allocate(removal_count, accounting)?;
            if let Some(records) = &current_records {
                for (path, entry) in missing_paths.value.drain(..).zip(&records.value.entries) {
                    cancellation.check()?;
                    accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                    let bytes = path.retained_bytes();
                    let allocation = crate::kernel::split_allocation(&mut missing_paths.allocation, bytes)?;
                    let path = crate::kernel::Owned::new(path, allocation, bytes);
                    missing_paths.bytes = missing_paths.bytes.checked_sub(bytes).ok_or(crate::WorkError::Overflow)?;
                    if let Some(record) = entry.record {
                        Self::push_mutation(&mut removals, crate::kernel::Owned::new(
                            crate::kernel::Mutation::Remove {
                                path: path.value, expected_file_id: MetadataField::Value(record.file_id),
                            }, path.allocation, path.bytes,
                        ), accounting)?;
                    } else {
                        path.retire(accounting)?;
                    }
                }
            }
            if removals.value.is_empty() {
                removals.retire(accounting)?;
            } else {
                self.checkout.mutate_owned_with_accounting(removals, accounting, cancellation).await?;
            }
        }
        missing_paths.retire(accounting)?;
        let mut current_index = 0;
        let mut operations = crate::kernel::Owned::allocate(paths.len(), accounting)?;
        for ((original_path, path), entry) in paths.iter().zip(parsed.value.drain(..))
            .zip(&source_records.value.entries)
        {
            cancellation.check()?;
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let relative = original_path.trim_start_matches('/');
            let bytes = path.retained_bytes();
            let allocation = crate::kernel::split_allocation(&mut parsed.allocation, bytes)?;
            let path = crate::kernel::Owned::new(path, allocation, bytes);
            parsed.bytes = parsed.bytes.checked_sub(bytes).ok_or(crate::WorkError::Overflow)?;
            if let Some(record) = entry.record {
                if selected_subtrees && record.kind == FileKind::Directory {
                    let destination = self.checkout.lookup_no_follow_with_accounting(
                        &path.value, accounting, cancellation,
                    ).await?.0.record;
                    let absolute = restore_absolute_path(
                        relative, accounting, cancellation,
                    )?;
                    if destination.is_some_and(|record| record.kind != FileKind::Directory) {
                        self.remove_record(&absolute.value, accounting, cancellation).await?;
                    }
                    self.create_dirs(&absolute.value, accounting, cancellation).await?;
                    absolute.retire(accounting)?;
                }
                if record.kind != FileKind::Directory && let Some((parent, _)) = relative.rsplit_once('/') {
                    let absolute = restore_absolute_path(
                        parent, accounting, cancellation,
                    )?;
                    self.create_dirs(&absolute.value, accounting, cancellation).await?;
                    absolute.retire(accounting)?;
                }
                Self::push_mutation(&mut operations, crate::kernel::Owned::new(
                    crate::kernel::Mutation::Restore { path: path.value, record },
                    path.allocation, path.bytes,
                ), accounting)?;
            } else {
                let current_record = current_records.as_ref()
                    .and_then(|records| records.value.entries.get(current_index))
                    .ok_or(WorkspaceError::IncompatibleWorkspace)?;
                current_index += 1;
                if !selected_subtrees && let Some(record) = current_record.record {
                    Self::push_mutation(&mut operations, crate::kernel::Owned::new(
                        crate::kernel::Mutation::Remove {
                            path: path.value, expected_file_id: MetadataField::Value(record.file_id),
                        }, path.allocation, path.bytes,
                    ), accounting)?;
                } else {
                    path.retire(accounting)?;
                }
            }
        }
        parsed.retire(accounting)?;
        if let Some(records) = current_records {
            records.retire(accounting)?;
        }
        if operations.value.is_empty() {
            operations.retire(accounting)?;
        } else {
            self.checkout.mutate_owned_with_accounting(operations, accounting, cancellation).await?;
        }
        let records_result = source_records.retire(accounting);
        let root_result = source_root.retire(accounting);
        records_result.and(root_result).map_err(WorkspaceError::from)
    }

}

/// Terminal outcome of advancing one fork onto its source workspace.
pub enum WorkspaceRebase<A, O> {
    /// A new rebased generation became durable.
    Rebased(Generation<A, O>),
    /// The identical retry was already durable.
    AlreadyRebased(Generation<A, O>),
    /// The fork already includes the source's current generation.
    Current(Generation<A, O>),
    /// This workspace changed after rebase planning; retry from the new head.
    Stale(Generation<A, O>),
    /// Local and upstream semantic changes overlap.
    Conflicted {
        /// Stable path-independent conflict regions.
        conflicts: Vec<MergeConflict>,
        /// Additional conflicts exceeded the retained bound.
        truncated: bool,
    },
    /// Writer ownership changed before publication.
    Fenced,
    /// The retry identity was previously bound to another input.
    IdempotencyConflict,
}


/// Immutable semantic delta between two exact generations.
pub struct ChangeSet<A, O> {
    from: Generation<A, O>,
    to: Generation<A, O>,
    changes: GenerationDiff,
    work: crate::WorkCounters,
}

/// One changed exact path between two immutable generations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangedPath {
    /// Canonical exact path in either endpoint.
    pub path: NamespacePath,
    /// Complete path-independent record in the earlier generation, when present.
    pub before: Option<crate::kernel::FileRecord>,
    /// Complete path-independent record in the later generation, when present.
    pub after: Option<crate::kernel::FileRecord>,
}

impl<A, O> ChangeSet<A, O> {
    /// Exact immutable base endpoint.
    #[must_use]
    pub const fn from(&self) -> &Generation<A, O> {
        &self.from
    }

    /// Exact immutable resulting endpoint.
    #[must_use]
    pub const fn to(&self) -> &Generation<A, O> {
        &self.to
    }

    /// Stable path-independent records and namespace binding changes.
    #[must_use]
    pub const fn changes(&self) -> &GenerationDiff {
        &self.changes
    }

    /// Exact bounded work used to authenticate and compute this delta.
    #[must_use]
    pub const fn work(&self) -> crate::WorkCounters {
        self.work
    }

    /// Actual retained diff buffers, excluding shared immutable endpoints.
    pub fn retained_bytes(&self) -> u64 {
        workspace_vec_bytes(&self.changes.files)
            + workspace_vec_bytes(&self.changes.bindings)
            + self.changes.bindings.iter().map(|binding| {
                crate::foundation::usize_to_u64(binding.name.retained_bytes())
                    + binding.before.as_ref().map_or(0, |entry| {
                        crate::foundation::usize_to_u64(entry.name.retained_bytes())
                    })
                    + binding.after.as_ref().map_or(0, |entry| {
                        crate::foundation::usize_to_u64(entry.name.retained_bytes())
                    })
            }).sum::<u64>()
    }
}

fn bounded_search<T>(
    values: &[T], accounting: &mut KernelAccounting<'_>,
    mut compare: impl FnMut(&T, &mut KernelAccounting<'_>) -> Result<std::cmp::Ordering, FsError>,
) -> Result<(usize, bool), FsError> {
    let mut low = 0;
    let mut high = values.len();
    while low < high {
        accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        let middle = low + (high - low) / 2;
        match compare(&values[middle], accounting)? {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Ok((middle, true)),
        }
    }
    Ok((low, false))
}

fn sorted_changed_file_ids(
    file_ids: impl IntoIterator<Item = FileId>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Vec<FileId>, KernelOwnedAllocation), WorkspaceError> {
    let mut ids = (Vec::<FileId>::new(), accounting.claim_owned(0, 0)?);
    for file_id in file_ids {
        accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        let (low, present) = bounded_search(&ids.0, accounting, |id, _| Ok(id.cmp(&file_id)))?;
        if present { continue; }
        reverse_grow(&mut ids.0, &mut ids.1, accounting)?;
        let copied = crate::WorkCounters {
            bytes_copied: ((ids.0.len() - low + 1) * std::mem::size_of::<FileId>()) as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        ids.0.insert(low, file_id);
        accounting.record_completed(copied)?;
    }
    Ok(ids)
}

fn changed_path_position<'a>(
    paths: &[ChangedPath], components: impl Iterator<Item = &'a LogicalName> + Clone,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(usize, bool), WorkspaceError> {
    bounded_search(paths, accounting, |path, accounting| {
        let mut left = path.path.components().iter();
        let mut right = components.clone();
        loop {
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            match (left.next(), right.next()) {
                (Some(left), Some(right)) => match left.cmp(right) {
                    std::cmp::Ordering::Equal => {}
                    ordering => return Ok(ordering),
                },
                (Some(_), None) => return Ok(std::cmp::Ordering::Greater),
                (None, Some(_)) => return Ok(std::cmp::Ordering::Less),
                (None, None) => return Ok(std::cmp::Ordering::Equal),
            }
        }
    }).map_err(WorkspaceError::from)
}

fn set_changed_record(
    paths: &mut [ChangedPath], index: usize, record: Option<crate::kernel::FileRecord>,
    before: bool, accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    let copied = crate::WorkCounters {
        bytes_copied: std::mem::size_of::<Option<crate::kernel::FileRecord>>() as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    if before { paths[index].before = record; } else { paths[index].after = record; }
    accounting.record_completed(copied)?;
    Ok(())
}

fn insert_changed_record(
    paths: &mut (Vec<ChangedPath>, KernelOwnedAllocation), index: usize,
    mut path: (NamespacePath, KernelOwnedAllocation), record: Option<crate::kernel::FileRecord>,
    before: bool, accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    let copied = crate::WorkCounters {
        bytes_copied: ((paths.0.len() - index + 1) * std::mem::size_of::<ChangedPath>()) as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    paths.1.absorb(&mut path.1)?;
    paths.0.insert(index, ChangedPath {
        path: path.0, before: if before { record } else { None },
        after: if before { None } else { record },
    });
    accounting.record_completed(copied)?;
    Ok(())
}

fn merge_changed_record(
    paths: &mut (Vec<ChangedPath>, KernelOwnedAllocation), path: &NamespacePath,
    record: Option<crate::kernel::FileRecord>, before: bool,
    accounting: &mut KernelAccounting<'_>, cancellation: &crate::CancellationToken,
) -> Result<(), WorkspaceError> {
    let (index, present) = changed_path_position(&paths.0, path.components().iter(), accounting)?;
    if present { return set_changed_record(&mut paths.0, index, record, before, accounting); }
    reverse_grow(&mut paths.0, &mut paths.1, accounting)?;
    let (path, allocation) = path.clone_with_accounting(accounting, cancellation)?;
    insert_changed_record(paths, index, (path, allocation), record, before, accounting)
}

fn merge_binding_endpoint(
    paths: &mut (Vec<ChangedPath>, KernelOwnedAllocation), directories: &NamespaceRecords,
    change: &crate::DirectoryBindingChange, limits: crate::model::VolumeLimits, before: bool,
    accounting: &mut KernelAccounting<'_>, cancellation: &crate::CancellationToken,
) -> Result<(), WorkspaceError> {
    let Some(parents) = bounded_search(directories, accounting, |record, _| Ok(record.0.cmp(&change.directory_id))).map(|(index, present)| present.then(|| &directories[index].1))?
        else { return Ok(()); };
    let binding = if before { change.before.as_ref() } else { change.after.as_ref() };
    let record = match binding {
        Some(entry) => bounded_search(directories, accounting, |record, _| Ok(record.0.cmp(&entry.file_id))).map(|(index, present)| present.then(|| &directories[index].1))?
            .and_then(|records| records.first().map(|(_, record)| *record)),
        None => None,
    };
    for (directory, _) in parents {
        accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        let components = directory.components().iter().chain(std::iter::once(&change.name));
        let (index, present) = changed_path_position(&paths.0, components, accounting)?;
        if present {
            set_changed_record(&mut paths.0, index, record, before, accounting)?;
        } else {
            reverse_grow(&mut paths.0, &mut paths.1, accounting)?;
            let (path, allocation) = directory.child_with_accounting(
                &change.name, limits, accounting, cancellation,
            )?;
            insert_changed_record(paths, index, (path, allocation), record, before, accounting)?;
        }
    }
    Ok(())
}

// Query tuples drop their data before the allocation guard.
fn reverse_drop<T>(owned: (T, KernelOwnedAllocation), bytes: u64, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    let (value, mut allocation) = owned;
    drop(value);
    accounting.release_owned(&mut allocation, bytes)
}

fn reverse_grow<T>(values: &mut Vec<T>, allocation: &mut KernelOwnedAllocation, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    if values.len() < values.capacity() { return Ok(()); }
    let count = values.capacity().checked_mul(2).map(|count| count.max(1)).ok_or(crate::WorkError::Overflow)?;
    let copied = crate::WorkCounters {
        bytes_copied: values.len().checked_mul(std::mem::size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok()).ok_or(crate::WorkError::Overflow)?,
        ..crate::WorkCounters::UNCHARGED
    };
    let mut replacement = crate::kernel::Owned::<Vec<T>>::allocate(count, accounting)?;
    if let Err(error) = accounting.admit(copied) {
        replacement.retire(accounting)?;
        return Err(error);
    }
    if let Err(error) = allocation.absorb(&mut replacement.allocation) {
        replacement.retire(accounting)?;
        return Err(error);
    }
    let old_bytes = workspace_vec_bytes(values);
    replacement.value.append(values);
    let completed = accounting.record_completed(copied);
    let old = std::mem::replace(values, replacement.value);
    drop(old);
    accounting.release_owned(allocation, old_bytes)?;
    completed
}

fn reverse_insert(records: &mut NamespaceRecords, allocation: &mut KernelOwnedAllocation, path: (NamespacePath, KernelOwnedAllocation),
    record: crate::kernel::FileRecord, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    let (index, present) = bounded_search(records, accounting, |entry, _| Ok(entry.0.cmp(&record.file_id)))?;
    if !present {
        reverse_grow(records, allocation, accounting)?;
        accounting.charge(crate::WorkCounters { bytes_copied: ((records.len() - index)
            * std::mem::size_of::<(FileId, Vec<(NamespacePath, crate::kernel::FileRecord)>)>()) as u64,
            ..crate::WorkCounters::UNCHARGED })?;
        records.insert(index, (record.file_id, Vec::new()));
    }
    let entries = &mut records[index].1;
    reverse_grow(entries, allocation, accounting)?;
    let mut path = path;
    allocation.absorb(&mut path.1)?;
    entries.push((path.0, record));
    Ok(())
}

fn reverse_sort(records: &mut NamespaceRecords, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    for (_, entries) in records {
        for index in 1..entries.len() {
            let mut current = index;
            while current != 0 {
                accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                if entries[current - 1].0 <= entries[current].0 { break; }
                accounting.charge(crate::WorkCounters { bytes_copied:
                    (2 * std::mem::size_of::<(NamespacePath, crate::kernel::FileRecord)>()) as u64,
                    ..crate::WorkCounters::UNCHARGED })?;
                entries.swap(current - 1, current);
                current -= 1;
            }
        }
    }
    Ok(())
}

async fn namespace_records_to_file_root_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>, root: &crate::kernel::GenerationRoot,
    generation_root: crate::ObjectId, ids: &[FileId], maximum_entries: u32,
    accounting: &mut KernelAccounting<'_>, cancellation: &crate::CancellationToken,
) -> Result<(NamespaceRecordSearch, KernelOwnedAllocation), FsError> {
    accounting.check()?;
    let allocation = accounting.claim_owned(0, 0)?;
    let mut matches = (NamespaceRecordSearch { records: NamespaceRecords::default(), complete: ids.is_empty() }, allocation);
    if ids.is_empty() || maximum_entries == 0 { return Ok(matches); }
    let limits = volume.config.limits;
    let generation = GenerationId::new(generation_root.digest);
    let cache_key = crate::path_index::cache_key(
        generation, root.root_file_id, root.file_table, ids, accounting,
    )?;
    let (bytes, allocation) = volume.fs.path_index().load(cache_key, accounting)?;
    let bytes = (bytes, allocation);
    if let Some(payload) = bytes.0.as_ref() {
        let (cached, allocation) = crate::path_index::decode(
            payload, generation, ids, limits, accounting,
        )?;
        let mut cached = (cached, allocation);
        if let Some(paths) = cached.0.as_ref().filter(|paths| paths.len() <= maximum_entries as usize) {
            let allocation = accounting.claim_owned(0, 0)?;
            let mut copied = (Vec::new(), allocation);
            for (_, path) in paths {
                reverse_grow(&mut copied.0, &mut copied.1, accounting)?;
                let (path, allocation) = path.clone_with_accounting(accounting, cancellation)?;
                let mut path = (path, allocation);
                copied.1.absorb(&mut path.1)?;
                copied.0.push(path.0);
            }
            let (lookup, allocation) = crate::kernel::lookup_paths_async_with_accounting(
                volume.fs.objects(), root, &copied.0, volume.config(), accounting, cancellation, None,
            ).await?.into_parts();
            let lookup = (lookup, allocation);
            let mut valid = paths.len() == lookup.0.entries.len();
            for ((expected, _), entry) in paths.iter().zip(&lookup.0.entries) {
                accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                if entry.record.is_none_or(|record| record.file_id != *expected) { valid = false; break; }
            }
            let copied_bytes = workspace_vec_bytes(&copied.0)
                + copied.0.iter().map(NamespacePath::retained_bytes).sum::<u64>();
            reverse_drop(copied, copied_bytes, accounting)?;
            let lookup_bytes = lookup.0.retained_allocation_bytes;
            if valid {
                let paths = cached.0.take().expect("verified cache payload");
                let vector_bytes = workspace_vec_bytes(&paths);
                for ((_, path), entry) in paths.into_iter().zip(&lookup.0.entries) {
                    let allocation = crate::kernel::split_allocation(&mut cached.1, path.retained_bytes())?;
                    let path = (path, allocation);
                    let record = entry.record.ok_or(FsError::NotFound)?;
                    reverse_insert(&mut matches.0.records, &mut matches.1, path, record, accounting)?;
                }
                accounting.release_owned(&mut cached.1, vector_bytes)?;
                reverse_drop(lookup, lookup_bytes, accounting)?;
                matches.0.complete = true;
                let bytes_count = bytes.0.as_ref().map_or(0, workspace_vec_bytes);
                reverse_drop(bytes, bytes_count, accounting)?;
                return Ok(matches);
            }
            reverse_drop(lookup, lookup_bytes, accounting)?;
        }
        let cached_bytes = cached.0.as_ref().map_or(0, |paths| workspace_vec_bytes(paths)
            + paths.iter().map(|(_, path)| path.retained_bytes()).sum::<u64>());
        reverse_drop(cached, cached_bytes, accounting)?;
    }
    let bytes_count = bytes.0.as_ref().map_or(0, workspace_vec_bytes);
    reverse_drop(bytes, bytes_count, accounting)?;
    let namespace_root = NamespacePath::new(Vec::new(), limits).map_err(FsError::from)?;
    if let Some(record) = crate::kernel::lookup_path_async_with_accounting(
        volume.fs.objects(), root, &namespace_root, volume.config(), accounting, cancellation, None,
    ).await?.record {
        if bounded_search(ids, accounting, |id, _| Ok(id.cmp(&record.file_id)))?.1 {
            let allocation = accounting.claim_owned(0, 0)?;
            reverse_insert(&mut matches.0.records, &mut matches.1,
                (NamespacePath::new(Vec::new(), limits).map_err(FsError::from)?, allocation), record, accounting)?;
        }
    }
    let allocation = accounting.claim_owned(0, 0)?;
    let mut pending = (Vec::new(), allocation);
    reverse_grow(&mut pending.0, &mut pending.1, accounting)?;
    let allocation = accounting.claim_owned(0, 0)?;
    pending.0.push((namespace_root, allocation));
    let mut examined = 0_u32;
    while let Some(directory) = pending.0.pop() {
        let mut after: Option<(LogicalName, KernelOwnedAllocation)> = None;
        loop {
            let (page, allocation) = crate::facade::list_directory_records_to_file_root_with_accounting(
                volume, root, &directory.0, after.as_ref().map(|name| &name.0),
                maximum_entries.saturating_sub(examined).saturating_add(1).min(1_024),
                accounting, cancellation,
            ).await?;
            let mut page = (page, allocation);
            for entry in &page.0.entries {
                accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                if examined == maximum_entries {
                    reverse_sort(&mut matches.0.records, accounting)?;
                    let page_bytes = workspace_vec_bytes(&page.0.entries)
                        + page.0.entries.iter().map(|entry| entry.name.retained_bytes() as u64).sum::<u64>();
                    reverse_drop(page, page_bytes, accounting)?;
                    if let Some(name) = after.take() {
                        let bytes = name.0.retained_bytes() as u64;
                        reverse_drop(name, bytes, accounting)?;
                    }
                    let bytes = directory.0.retained_bytes();
                    reverse_drop(directory, bytes, accounting)?;
                    while let Some(path) = pending.0.pop() {
                        let bytes = path.0.retained_bytes();
                        reverse_drop(path, bytes, accounting)?;
                    }
                    let bytes = workspace_vec_bytes(&pending.0);
                    reverse_drop(pending, bytes, accounting)?;
                    return Ok(matches);
                }
                examined += 1;
                let found = bounded_search(ids, accounting, |id, _| Ok(id.cmp(&entry.record.file_id)))?.1;
                let is_directory = entry.record.kind == FileKind::Directory;
                if !found && !is_directory { continue; }
                let (path, allocation) = directory.0.child_with_accounting(
                    &entry.name, limits, accounting, cancellation,
                )?;
                let path = (path, allocation);
                if found && is_directory {
                    let (copy, allocation) = path.0.clone_with_accounting(accounting, cancellation)?;
                    reverse_insert(&mut matches.0.records, &mut matches.1,
                        (copy, allocation), entry.record, accounting)?;
                } else if found {
                    reverse_insert(&mut matches.0.records, &mut matches.1, path, entry.record, accounting)?;
                    continue;
                }
                reverse_grow(&mut pending.0, &mut pending.1, accounting)?;
                pending.0.push(path);
            }
            let next = if page.0.has_more {
                let name = page.0.entries.pop().ok_or(FsError::NotFound)?.name;
                let allocation = crate::kernel::split_allocation(&mut page.1, name.retained_bytes() as u64)?;
                Some((name, allocation))
            } else { None };
            let page_bytes = workspace_vec_bytes(&page.0.entries)
                + page.0.entries.iter().map(|entry| entry.name.retained_bytes() as u64).sum::<u64>();
            reverse_drop(page, page_bytes, accounting)?;
            if let Some(old) = after.take() {
                let bytes = old.0.retained_bytes() as u64;
                reverse_drop(old, bytes, accounting)?;
            }
            after = next;
            if after.is_none() { break; }
        }
        let bytes = directory.0.retained_bytes();
        reverse_drop(directory, bytes, accounting)?;
    }
    let pending_bytes = workspace_vec_bytes(&pending.0);
    reverse_drop(pending, pending_bytes, accounting)?;
    reverse_sort(&mut matches.0.records, accounting)?;
    let (bytes, allocation) = crate::path_index::encode_records(
        generation, ids, &matches.0.records, accounting,
    )?;
    let bytes = (bytes, allocation);
    if let Some(payload) = &bytes.0 {
        volume.fs.path_index().store(cache_key, payload, accounting)?;
    }
    let bytes_count = bytes.0.as_ref().map_or(0, workspace_vec_bytes);
    reverse_drop(bytes, bytes_count, accounting)?;
    matches.0.complete = true;
    Ok(matches)
}

/// Reverse aliases of the actual supplied root, retaining their original allocation.
pub(crate) async fn reverse_paths_to_file_root_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>, root: &crate::kernel::GenerationRoot, generation_root: crate::ObjectId,
    file_id: FileId, maximum_entries: u32, accounting: &mut KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<(Vec<NamespacePath>, KernelOwnedAllocation), WorkspaceError> {
    let mut search = namespace_records_to_file_root_with_accounting(
        volume, root, generation_root, &[file_id], maximum_entries, accounting, cancellation,
    ).await?;
    let allocation = accounting.claim_owned(0, 0)?;
    let mut paths = (Vec::new(), allocation);
    let entries_bytes = workspace_vec_bytes(&search.0.records);
    for (_, records) in std::mem::take(&mut search.0.records) {
        let bytes = workspace_vec_bytes(&records);
        for (path, _) in records {
            reverse_grow(&mut paths.0, &mut paths.1, accounting)?;
            paths.0.push(path);
        }
        accounting.release_owned(&mut search.1, bytes)?;
    }
    accounting.release_owned(&mut search.1, entries_bytes)?;
    paths.1.absorb(&mut search.1)?;
    Ok((paths.0, paths.1))
}

struct NamespaceRecordSearch {
    records: NamespaceRecords,
    complete: bool,
}

type NamespaceRecords = Vec<(FileId, Vec<(NamespacePath, crate::kernel::FileRecord)>)>;

fn namespace_records_bytes(records: &NamespaceRecords) -> u64 {
    workspace_vec_bytes(records) + records.iter().map(|(_, records)|
        workspace_vec_bytes(records) + records.iter().map(|(path, _)| path.retained_bytes()).sum::<u64>()
    ).sum::<u64>()
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> ChangeSet<A, O> {
    /// Resolves changed paths under one cumulative budget and cancellation token.
    #[allow(
        clippy::too_many_lines,
        reason = "one bounded resolution of changed paths shares a single cumulative work budget"
    )]
    pub async fn changed_paths(
        &self,
        maximum_entries: u32,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Vec<ChangedPath>>, WorkspaceError> {
        let mut work = self.work;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = async {
            work.verify(budget)?;
            // Only ordinary resolution continues the retained diff's original span.
            allocations.claim_bytes(self.retained_bytes(), 0, &mut work, budget)
                .map_err(workspace_allocation_error)?;
            let mut accounting = KernelAccounting::borrowed_standalone(&mut allocations, &mut work, budget, cancellation);
            self.changed_paths_with_accounting(maximum_entries, &mut accounting, cancellation).await.map(|owned| owned.0)
        }.await;
        result.map(|value| crate::OperationReceipt { value, work })
            .map_err(|error| crate::OperationFailure::new(error, work))
    }

    /// Borrows this exact delta while resolving paths on the original native invocation.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn changed_paths_with_native_work(
        &self, maximum_entries: u32, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(Vec<ChangedPath>, crate::NativeAllocation), WorkspaceError> {
        let mut accounting = KernelAccounting::native(original, cancellation);
        match self.changed_paths_with_accounting(maximum_entries, &mut accounting, cancellation).await? {
            (value, KernelOwnedAllocation::NativeOriginal(allocation)) => Ok((value, allocation)),
            _ => Err(WorkspaceError::from(crate::NativeTreeMaterializationError::InvalidLayout)),
        }
    }

    async fn changed_paths_with_accounting(
        &self, maximum_entries: u32, accounting: &mut KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(Vec<ChangedPath>, KernelOwnedAllocation), WorkspaceError> {
        accounting.check()?;
        if self.changes.truncated || (maximum_entries == 0
            && (!self.changes.files.is_empty() || !self.changes.bindings.is_empty())) {
            return Err(WorkspaceError::ChangedPathLimit);
        }
        let file_ids = sorted_changed_file_ids(
            self.changes.files.iter().map(|change| change.file_id)
                .chain(self.changes.bindings.iter().map(|change| change.directory_id))
                .chain(self.changes.bindings.iter().flat_map(|change| {
                    change.before.iter().chain(change.after.iter()).map(|entry| entry.file_id)
                })), accounting,
        )?;
        let before = self.from.namespace_records_with_accounting(&file_ids.0, maximum_entries, accounting, cancellation).await?;
        let after = self.to.namespace_records_with_accounting(&file_ids.0, maximum_entries, accounting, cancellation).await?;
        if !before.0.complete || !after.0.complete { return Err(WorkspaceError::ChangedPathLimit); }
        let mut paths = (Vec::new(), accounting.claim_owned(0, 0)?);
        for records in before.0.records.iter().map(|(_, records)| records) {
            for (path, record) in records {
                accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                merge_changed_record(&mut paths, path, Some(*record), true, accounting, cancellation)?;
            }
        }
        for records in after.0.records.iter().map(|(_, records)| records) {
            for (path, record) in records {
                accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                merge_changed_record(&mut paths, path, Some(*record), false, accounting, cancellation)?;
            }
        }
        for change in &self.changes.bindings {
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            merge_binding_endpoint(&mut paths, &before.0.records, change, self.from.workspace.volume.config.limits, true, accounting, cancellation)?;
            merge_binding_endpoint(&mut paths, &after.0.records, change, self.to.workspace.volume.config.limits, false, accounting, cancellation)?;
        }
        let before_bytes = namespace_records_bytes(&before.0.records);
        let after_bytes = namespace_records_bytes(&after.0.records);
        let id_bytes = workspace_vec_bytes(&file_ids.0);
        reverse_drop(before, before_bytes, accounting)?;
        reverse_drop(after, after_bytes, accounting)?;
        reverse_drop(file_ids, id_bytes, accounting)?;
        let mut retained = 0;
        let mut retired_paths = 0_u64;
        for index in 0..paths.0.len() {
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let changed = match (&paths.0[index].before, &paths.0[index].after) {
                (Some(before), Some(after)) => before.kind != after.kind || before.metadata != after.metadata
                    || (before.kind != FileKind::Directory && before.payload != after.payload),
                (None, None) => false,
                _ => true,
            };
            if changed {
                if retained != index {
                    let copied = crate::WorkCounters { bytes_copied: (2 * std::mem::size_of::<ChangedPath>()) as u64, ..crate::WorkCounters::UNCHARGED };
                    accounting.admit(copied)?;
                    paths.0.swap(retained, index);
                    accounting.record_completed(copied)?;
                }
                retained += 1;
            } else {
                retired_paths = retired_paths.checked_add(paths.0[index].path.retained_bytes()).ok_or(crate::WorkError::Overflow)?;
            }
        }
        paths.0.truncate(retained);
        accounting.release_owned(&mut paths.1, retired_paths)?;
        accounting.check()?;
        Ok(paths)
    }

    /// Composes contiguous immutable deltas into their exact net semantic
    pub async fn compose(
        &self,
        next: &ChangeSet<A, O>,
        maximum_changes: u32,
    ) -> Result<ChangeSet<A, O>, WorkspaceError> {
        if self.to.id != next.from.id
            || self.to.workspace.id != next.from.workspace.id
            || !self
                .to
                .workspace
                .volume
                .fs
                .same_deployment(&next.from.workspace.volume.fs)
        {
            return Err(WorkspaceError::ChangeSetContinuity);
        }
        self.from
            .workspace
            .diff(&self.from, &next.to, maximum_changes)
            .await
    }
}

/// How a successful join records immutable ancestry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinHistory {
    /// Preserve source and target as two parents.
    Merge,
    /// Replay source state onto the target as one target-parent generation.
    Rebase,
    /// Collapse the complete source delta into one target-parent generation.
    Squash,
    /// Apply the selected source delta as one target-parent generation.
    CherryPick,
}

/// Side-effect-free join builder.
pub struct JoinBuilder<A, O> {
    source: Workspace<A, O>,
    target: Workspace<A, O>,
    history: JoinHistory,
    maximum_generations: u32,
    maximum_changes: u32,
    maximum_conflicts: u32,
}

impl<A, O> JoinBuilder<A, O> {
    fn validate_bounds(&self) -> Result<(), WorkspaceError> {
        if self.maximum_generations == 0 || self.maximum_changes == 0 || self.maximum_conflicts == 0
        {
            Err(WorkspaceError::JoinLimit)
        } else {
            Ok(())
        }
    }

    fn validate_workspaces(&self) -> Result<(), WorkspaceError> {
        self.validate_bounds()?;
        if !self.source.volume.fs.same_deployment(&self.target.volume.fs) {
            return Err(WorkspaceError::ForeignGeneration);
        }
        if self.source.volume.config != self.target.volume.config {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        self.source.volume.config.validate().map_err(FsError::from)?;
        Ok(())
    }

    fn validate_endpoints(
        &self,
        source: &Generation<A, O>,
        target: &Generation<A, O>,
    ) -> Result<(), WorkspaceError> {
        self.validate_workspaces()?;
        if source.workspace.id != self.source.id || target.workspace.id != self.target.id
            || !self.source.volume.fs.same_deployment(&source.workspace.volume.fs)
            || !self.target.volume.fs.same_deployment(&target.workspace.volume.fs)
        {
            return Err(WorkspaceError::ForeignGeneration);
        }
        if source.workspace.volume.config != self.source.volume.config
            || target.workspace.volume.config != self.target.volume.config
        {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        Ok(())
    }

    /// Selects the immutable ancestry shape of a successful join.
    #[must_use]
    pub const fn history(mut self, history: JoinHistory) -> Self {
        self.history = history;
        self
    }

    /// Sets exact bounded lineage, diff, and conflict frontiers.
    #[must_use]
    pub const fn bounds(
        mut self,
        maximum_generations: u32,
        maximum_changes: u32,
        maximum_conflicts: u32,
    ) -> Self {
        self.maximum_generations = maximum_generations;
        self.maximum_changes = maximum_changes;
        self.maximum_conflicts = maximum_conflicts;
        self
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> JoinBuilder<A, O> {
    /// Authenticates endpoints and discovers one bounded common ancestor
    pub async fn plan(self) -> Result<JoinPlan<A, O>, WorkspaceError> {
        self.validate_workspaces()?;
        let cancellation = CancellationToken::new();
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED, &cancellation,
        );
        let source_head = self.source.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let (target_head_id, target_authority_head) = self.target.volume.fs
            .workspace_head_state(&self.target.volume, &mut accounting, &cancellation)
            .await?;
        let target_head = self.target.generation(target_head_id, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        self.plan_generations_with_accounting(
            source_head, target_head, target_authority_head, &mut accounting, &cancellation,
        ).await
    }

    /// Plans exact immutable endpoints under supplied work limits.
    pub async fn plan_at<W: crate::facade::FsWork>(
        self, source: &Generation<A, O>, target: &Generation<A, O>,
        work: W, cancellation: &CancellationToken,
    ) -> W::Output<JoinPlan<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            self.validate_endpoints(source, target)?;
            let (target_head_id, target_authority_head) = self.target.volume.fs
                .workspace_head_state(&self.target.volume, &mut accounting, cancellation).await?;
            if target.id != target_head_id { return Err(WorkspaceError::StaleTarget); }
            let copied = crate::WorkCounters {
                bytes_copied: (2 * std::mem::size_of::<Generation<A, O>>()) as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            let source = source.clone();
            let target = target.clone();
            accounting.record_completed(copied)?;
            let plan = self.plan_generations_with_accounting(
                source, target, target_authority_head, &mut accounting, cancellation,
            ).await?;
            Ok::<_, WorkspaceError>(plan)
        }.await;
        work.finish(result, &accounting)
    }

    /// Plans against caller-pinned immutable source and target generations.
    pub async fn plan_pinned(
        self,
        source_head: Generation<A, O>,
        target_head: Generation<A, O>,
    ) -> Result<JoinPlan<A, O>, WorkspaceError> {
        self.validate_endpoints(&source_head, &target_head)?;
        let cancellation = CancellationToken::new();
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED, &cancellation,
        );
        let (current_target, target_authority_head) = self.target.volume.fs
            .workspace_head_state(&self.target.volume, &mut accounting, &cancellation).await?;
        if current_target != target_head.id { return Err(WorkspaceError::StaleGeneration); }
        self.plan_generations_with_accounting(
            source_head, target_head, target_authority_head, &mut accounting, &cancellation,
        ).await
    }

    pub(crate) async fn plan_generations(
        self,
        source_head: Generation<A, O>,
        target_head: Generation<A, O>,
        target_authority_head: crate::Head,
    ) -> Result<JoinPlan<A, O>, WorkspaceError> {
        let cancellation = CancellationToken::new();
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED, &cancellation,
        );
        self.plan_generations_with_accounting(
            source_head, target_head, target_authority_head, &mut accounting, &cancellation,
        ).await
    }

    async fn plan_generations_with_accounting(
        self,
        source_head: Generation<A, O>,
        target_head: Generation<A, O>,
        target_authority_head: crate::Head,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<JoinPlan<A, O>, WorkspaceError> {
        self.validate_endpoints(&source_head, &target_head)?;
        let base = self.source.volume.fs.workspace_common_ancestor_with_accounting(
            &source_head, &target_head, self.maximum_generations, accounting, cancellation,
        ).await?;
        let copied = crate::WorkCounters {
            bytes_copied: std::mem::size_of::<JoinPlan<A, O>>() as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        let plan = JoinPlan {
            source: self.source,
            target: self.target,
            source_head,
            target_head,
            target_authority_head,
            base,
            history: self.history,
            maximum_generations: self.maximum_generations,
            maximum_changes: self.maximum_changes,
            maximum_conflicts: self.maximum_conflicts,
        };
        accounting.record_completed(copied)?;
        Ok(plan)
    }
}

/// Immutable side-effect-free join plan.
pub struct JoinPlan<A, O> {
    source: Workspace<A, O>,
    target: Workspace<A, O>,
    source_head: Generation<A, O>,
    target_head: Generation<A, O>,
    target_authority_head: crate::Head,
    base: crate::facade::WorkspaceCommonAncestor,
    history: JoinHistory,
    maximum_generations: u32,
    maximum_changes: u32,
    maximum_conflicts: u32,
}

impl<A, O> JoinPlan<A, O> {
    /// Source generation captured while planning.
    #[must_use]
    pub const fn source_head(&self) -> GenerationId {
        self.source_head.id
    }

    /// Target generation observed while planning.
    #[must_use]
    pub const fn target_head(&self) -> GenerationId {
        self.target_head.id
    }

    /// Exact discovered common ancestor.
    #[must_use]
    pub const fn common_ancestor(&self) -> GenerationId {
        self.base.id
    }

    /// Workspace volume that authenticated the exact common ancestor.
    #[must_use]
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) const fn common_ancestor_volume(&self) -> VolumeId {
        self.base.volume_id
    }

    /// Source workspace retained by the plan.
    #[must_use]
    pub const fn source(&self) -> &Workspace<A, O> {
        &self.source
    }

    /// Captures exact join inputs under supplied work limits.
    pub fn commit_witness<W: crate::facade::FsWork>(
        &self, application: &JoinApplication<A, O>, idempotency_key: IdempotencyKey,
        work: W, cancellation: &CancellationToken,
    ) -> W::Output<crate::JoinCommitWitness, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = (|| -> Result<_, WorkspaceError> {
            let examined = crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED };
            accounting.admit(examined)?;
            let incompatible = application.generation.workspace.id() != self.target.id()
                || application.generation.id() == self.target_head.id();
            accounting.record_completed(examined)?;
            if incompatible { return Err(WorkspaceError::IncompatibleWorkspace); }
            let copied = crate::WorkCounters {
                bytes_copied: std::mem::size_of::<crate::JoinCommitWitness>() as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            let witness = crate::JoinCommitWitness {
                source_workspace: self.source.id().volume_id(),
                source_generation: self.source_head.id,
                target_workspace: self.target.id().volume_id(),
                expected_target: self.target_head.id,
                base_workspace: self.base.volume_id,
                base_generation: self.base.id,
                history: self.history,
                maximum_generations: self.maximum_generations,
                maximum_changes: self.maximum_changes,
                maximum_conflicts: self.maximum_conflicts,
                resolutions_digest: application.resolutions_digest,
                expected_head: self.target_authority_head,
                operation_id: idempotency_key.operation_id(),
                result_generation: application.generation.id,
            };
            accounting.record_completed(copied)?;
            Ok(witness)
        })();
        work.finish(result, &accounting)
    }
}

/// Atomic join application preconditions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplyOptions {
    /// Exact target generation that must still be current.
    pub if_target: GenerationId,
    /// Stable identity reused only for an exact retry.
    pub idempotency_key: IdempotencyKey,
}

/// Terminal semantic join outcome.
pub enum JoinOutcome<A, O> {
    /// A new target generation became durable.
    Applied(JoinApplication<A, O>),
    /// The same exact application was already durable.
    AlreadyApplied(JoinApplication<A, O>),
    /// Source changes were already represented by the target.
    NoChanges(Generation<A, O>),
    /// Target changed after planning; no join was published.
    StaleTarget(Generation<A, O>),
    /// Exact conflicts prevented candidate publication.
    Conflicted {
        /// Stable path-independent conflict regions.
        conflicts: Vec<MergeConflict>,
        /// Additional conflicts exceeded the retained result bound.
        truncated: bool,
    },
    /// Target writer ownership changed before publication.
    Fenced,
    /// Retry identity was previously bound to another join input.
    IdempotencyConflict,
}

/// One durable join result together with its exact conflict-resolution input.
pub struct JoinApplication<A, O> {
    /// Immutable generation made durable by this join.
    pub generation: Generation<A, O>,
    /// Digest of the exact applied conflict resolutions.
    pub resolutions_digest: crate::Digest,
}

/// Terminal semantic outcome of an exact workspace-head restoration.
pub enum WorkspaceRestore<G> {
    /// The requested historical generation became current.
    Restored(G),
    /// The same restoration was already durable under this retry identity.
    AlreadyRestored(G),
    /// The requested generation was already current; no publication occurred.
    Current(G),
    /// The workspace advanced after the caller observed it.
    Stale(G),
    /// Workspace writer ownership changed before publication.
    Fenced,
    /// The retry identity was previously bound to another restoration.
    IdempotencyConflict,
}

/// Failure while resolving or validating a declarative real-workspace join.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum DrivenJoinError {
    /// Workspace planning, storage, or publication failed.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// Driver selection, execution, or cache reuse failed.
    #[error(transparent)]
    Resolution(#[from] MergePlanResolutionError),
    /// Candidate generations or conflict identities do not match this join plan.
    #[error("merge candidate does not match the immutable workspace join plan")]
    StaleCandidate,
    /// The current kernel cannot safely materialize this synthesized resolution.
    #[error("merge resolution for {0:?} is not representable by this join")]
    UnsupportedResolution(ConflictKey),
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> JoinPlan<A, O> {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn source_changes(
        &self,
    ) -> Result<crate::FsReceipt<GenerationDiff>, WorkspaceError> {
        self.source
            .volume
            .fs
            .workspace_join_changes(self.base.id, &self.source_head, self.maximum_changes, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| crate::FsReceipt { value: receipt.value.0, work: receipt.work }).map_err(|failure| failure.error)
    }

    /// Applies this immutable plan only while the supplied writer permit remains valid.
    pub async fn apply(
        &self,
        options: ApplyOptions,
        permit: crate::PublicationPermit,
    ) -> Result<JoinOutcome<A, O>, WorkspaceError> {
        self.apply_resolutions(options, BTreeMap::new(), permit)
            .await
    }

    /// Seals publication through the canonical native publisher before its CAS.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn apply_with_native_work<'original, F, Fut, E>(
        &self, options: ApplyOptions, permit: crate::PublicationPermit,
        original: &'original crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken, before_publication: &mut F,
    ) -> Result<(JoinOutcome<A, O>, crate::NativeAllocation), E>
    where
        F: for<'materialization> FnMut(&'materialization crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
    {
        self.apply_resolutions_guarded_with_native_work(
            options, &[], permit, original, cancellation, before_publication,
        ).await
    }

    /// Expands exact kernel conflicts into immutable, driver-ready values.
    pub async fn describe_conflicts(
        &self,
        conflicts: &[MergeConflict],
        truncated: bool,
    ) -> Result<MergePlan, WorkspaceError> {
        self.typed_merge_plan(conflicts.to_vec(), truncated).await
    }

    /// Resolves real join conflicts with registered immutable-input drivers.
    pub async fn apply_with_drivers<C: MergeResolutionCache>(
        &self, options: ApplyOptions, registry: &MergeDriverRegistry,
        cache: &mut C, replanning: bool,
    ) -> Result<JoinOutcome<A, O>, DrivenJoinError> {
        let initial = self.apply(options, crate::PublicationPermit::Unrestricted).await?;
        let JoinOutcome::Conflicted { conflicts, truncated } = initial else { return Ok(initial); };
        let plan = self.typed_merge_plan(conflicts, truncated).await?;
        let candidate = resolve_merge_plan(plan, registry, cache, replanning)?;
        self.apply_candidate(options, &candidate, crate::PublicationPermit::Unrestricted).await
    }

    /// Applies caller-selected immutable sides after validating that they
    pub async fn apply_sides(
        &self,
        options: ApplyOptions,
        selections: BTreeMap<MergeConflict, ConflictSide>,
    ) -> Result<JoinOutcome<A, O>, DrivenJoinError> {
        let initial = self.apply(options, crate::PublicationPermit::Unrestricted).await?;
        let JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } = initial
        else {
            return Ok(initial);
        };
        let cancellation = crate::CancellationToken::new();
        validate_join_side_selections(
            &conflicts, truncated, &selections,
            &mut crate::kernel::allocation::KernelAccounting::standalone(
                WorkBudget::UNBOUNDED, &cancellation,
            ),
        )?;
        let resolutions = selections
            .into_iter()
            .map(|(conflict, side)| {
                let side = match side {
                    ConflictSide::Base => crate::kernel::MergeConflictSide::Base,
                    ConflictSide::Ours => crate::kernel::MergeConflictSide::Ours,
                    ConflictSide::Theirs => crate::kernel::MergeConflictSide::Theirs,
                };
                (
                    conflict,
                    crate::kernel::MergeConflictResolution::Select(side),
                )
            })
            .collect();
        self.apply_resolutions(options, resolutions, crate::PublicationPermit::Unrestricted)
            .await
            .map_err(Into::into)
    }

    /// Validates the ordinary conflict set, then seals the same second application.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn apply_sides_with_permit_guarded_with_native_work<'original, F, Fut, E>(
        &self, options: ApplyOptions, selections: &BTreeMap<MergeConflict, ConflictSide>,
        permit: crate::PublicationPermit, original: &'original crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken, before_publication: &mut F,
    ) -> Result<(JoinOutcome<A, O>, crate::NativeAllocation), E>
    where
        F: for<'materialization> FnMut(&'materialization crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<FsError> + From<WorkspaceError> + From<DrivenJoinError> + Send + 'static,
    {
        let (initial, allocation) = self.apply_with_native_work(
            options, permit, original, cancellation, before_publication,
        ).await?;
        let JoinOutcome::Conflicted { conflicts, truncated } = initial else {
            return Ok((initial, allocation));
        };
        let bytes = allocation.bytes();
        let conflicts = crate::kernel::Owned::new(
            conflicts, crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation), bytes,
        );
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        let validation = validate_join_side_selections(&conflicts.value, truncated, selections, &mut accounting);
        conflicts.retire(&mut accounting).map_err(E::from)?;
        validation.map_err(E::from)?;
        let resolutions = native_join_side_resolutions(selections, &mut accounting, cancellation)
            .map_err(E::from)?;
        let applied = self.apply_resolutions_guarded_with_native_work(
            options, &resolutions.value, permit, original, cancellation, before_publication,
        ).await;
        resolutions.retire(&mut accounting).map_err(E::from)?;
        applied
    }

    /// Validates and publishes a declarative candidate under one writer permit.
    pub async fn apply_candidate(
        &self,
        options: ApplyOptions,
        candidate: &UnpublishedMergeCandidate,
        permit: crate::PublicationPermit,
    ) -> Result<JoinOutcome<A, O>, DrivenJoinError> {
        let conflict_keys = candidate
            .plan
            .conflicts
            .iter()
            .map(|conflict| &conflict.key)
            .collect::<BTreeSet<_>>();
        if candidate.plan.base != self.base.id
            || candidate.plan.ours != self.target_head.id
            || candidate.plan.theirs != self.source_head.id
            || candidate.plan.truncated
            || candidate.plan.conflicts.len() != candidate.resolutions.len()
            || conflict_keys.len() != candidate.plan.conflicts.len()
            || !candidate.resolutions.keys().eq(conflict_keys)
        {
            return Err(DrivenJoinError::StaleCandidate);
        }
        let mut resolutions = BTreeMap::new();
        for conflict in &candidate.plan.conflicts {
            let resolution = candidate
                .resolutions
                .get(&conflict.key)
                .ok_or(DrivenJoinError::StaleCandidate)?;
            if !resolution_matches_conflict(&conflict.key, resolution) {
                return Err(DrivenJoinError::UnsupportedResolution(conflict.key.clone()));
            }
            let resolution = match resolution {
                MergeResolution::Select(ConflictSide::Base) => {
                    crate::kernel::MergeConflictResolution::Select(
                        crate::kernel::MergeConflictSide::Base,
                    )
                }
                MergeResolution::Select(ConflictSide::Ours) => {
                    crate::kernel::MergeConflictResolution::Select(
                        crate::kernel::MergeConflictSide::Ours,
                    )
                }
                MergeResolution::Select(ConflictSide::Theirs) => {
                    crate::kernel::MergeConflictResolution::Select(
                        crate::kernel::MergeConflictSide::Theirs,
                    )
                }
                MergeResolution::Text(text) => crate::kernel::MergeConflictResolution::File(Some(
                    self.stage_regular_resolution(
                        &conflict.key,
                        Bytes::copy_from_slice(text.as_bytes()),
                    )
                    .await?,
                )),
                MergeResolution::Binary(bytes) => {
                    crate::kernel::MergeConflictResolution::File(Some(
                        self.stage_regular_resolution(&conflict.key, Bytes::copy_from_slice(bytes))
                            .await?,
                    ))
                }
                MergeResolution::Metadata(bytes) => {
                    let file_id = conflict_file_id(&conflict.key).ok_or_else(|| {
                        DrivenJoinError::UnsupportedResolution(conflict.key.clone())
                    })?;
                    let mut record = self
                        .target
                        .volume
                        .fs
                        .workspace_conflict_record(
                            &self.target.volume,
                            self.target_head.id,
                            file_id,
                        )
                        .await?
                        .ok_or(WorkspaceError::InvalidMergeResolution)?;
                    record.metadata = self
                        .target
                        .volume
                        .fs
                        .stage_merge_metadata(&self.target.volume, bytes)
                        .await?;
                    crate::kernel::MergeConflictResolution::File(Some(record))
                }
                MergeResolution::Binding(file_id) => {
                    crate::kernel::MergeConflictResolution::Binding(*file_id)
                }
                MergeResolution::Unresolved => {
                    return Err(DrivenJoinError::UnsupportedResolution(conflict.key.clone()));
                }
            };
            resolutions.insert(
                kernel_conflict(
                    &conflict.key,
                    self.target.volume.config.limits.maximum_component_bytes,
                )?,
                resolution,
            );
        }
        self.apply_resolutions(options, resolutions, permit)
            .await
            .map_err(Into::into)
    }

    async fn stage_regular_resolution(
        &self,
        key: &ConflictKey,
        bytes: Bytes,
    ) -> Result<crate::kernel::FileRecord, DrivenJoinError> {
        let file_id = conflict_file_id(key)
            .ok_or_else(|| DrivenJoinError::UnsupportedResolution(key.clone()))?;
        let record = self
            .target
            .volume
            .fs
            .workspace_conflict_record(&self.target.volume, self.target_head.id, file_id)
            .await?
            .ok_or(WorkspaceError::InvalidMergeResolution)?;
        self.target
            .volume
            .fs
            .stage_merge_regular_record(&self.target.volume, record, bytes)
            .await
            .map_err(Into::into)
    }

    async fn typed_merge_plan(
        &self,
        conflicts: Vec<MergeConflict>,
        truncated: bool,
    ) -> Result<MergePlan, WorkspaceError> {
        let mut typed = Vec::with_capacity(conflicts.len());
        for conflict in conflicts {
            typed.push(self.conflict_view(conflict).await?);
        }
        Ok(MergePlan {
            base: self.base.id,
            ours: self.target_head.id,
            theirs: self.source_head.id,
            conflicts: typed,
            truncated,
        })
    }

    async fn conflict_view(
        &self,
        conflict: MergeConflict,
    ) -> Result<crate::ConflictView, WorkspaceError> {
        let MergeConflict::File(file_id) = conflict else {
            let mut view = binding_conflict_view(self, conflict.clone());
            if let MergeConflict::Binding { directory_id, name } = conflict
                && let Some(directory) = self.conflict_path(directory_id).await?
                && let Ok(name) = logical_name_text(&name)
            {
                view.path = Some(if directory == "/" {
                    format!("/{name}")
                } else {
                    format!("{directory}/{name}")
                });
            }
            return Ok(view);
        };
        let path = self.conflict_path(file_id).await?;
        let fs = &self.target.volume.fs;
        let base = fs
            .workspace_conflict_record(&self.target.volume, self.base.id, file_id)
            .await?;
        let ours = fs
            .workspace_conflict_record(&self.target.volume, self.target_head.id, file_id)
            .await?;
        let theirs = fs
            .workspace_conflict_record(&self.target.volume, self.source_head.id, file_id)
            .await?;
        let fallback = |generation, record: Option<crate::kernel::FileRecord>| match record {
            Some(_) => ConflictValue::Record {
                generation,
                file_id,
            },
            None => ConflictValue::Absent,
        };
        let Some((base_record, ours_record, theirs_record)) = base
            .zip(ours)
            .zip(theirs)
            .map(|((base, ours), theirs)| (base, ours, theirs))
        else {
            return Ok(crate::ConflictView {
                key: ConflictKey::File(file_id),
                path,
                kind: ConflictKind::Record,
                base: fallback(self.base.id, base),
                ours: fallback(self.target_head.id, ours),
                theirs: fallback(self.source_head.id, theirs),
            });
        };
        if base_record.link_count != ours_record.link_count
            || base_record.link_count != theirs_record.link_count
        {
            return Ok(record_conflict_view(
                self,
                file_id,
                path,
                ConflictKind::HardLink,
            ));
        }
        if base_record.metadata != ours_record.metadata
            || base_record.metadata != theirs_record.metadata
        {
            let values = [base_record, ours_record, theirs_record];
            let mut metadata = Vec::with_capacity(3);
            for record in values {
                metadata.push(ConflictValue::Metadata(
                    fs.workspace_conflict_metadata(&self.target.volume, record)
                        .await?,
                ));
            }
            return Ok(crate::ConflictView {
                key: ConflictKey::Metadata(file_id),
                path,
                kind: ConflictKind::Metadata,
                base: metadata.remove(0),
                ours: metadata.remove(0),
                theirs: metadata.remove(0),
            });
        }
        if let Some(view) = self
            .regular_conflict_view(
                file_id,
                path.clone(),
                [base_record, ours_record, theirs_record],
            )
            .await?
        {
            return Ok(view);
        }
        let kind = match base_record.kind {
            FileKind::Directory => ConflictKind::Directory,
            FileKind::SymbolicLink => ConflictKind::SymbolicLink,
            _ => ConflictKind::Special,
        };
        Ok(record_conflict_view(self, file_id, path, kind))
    }

    async fn conflict_path(&self, file_id: FileId) -> Result<Option<String>, WorkspaceError> {
        let maximum = self.maximum_changes.max(1);
        let mut paths = self.target_head.paths_for_file_id(file_id, maximum, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        if paths.is_empty() {
            paths = self.source_head.paths_for_file_id(file_id, maximum, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        }
        Ok(paths.into_iter().next())
    }

    async fn regular_conflict_view(
        &self,
        file_id: FileId,
        path: Option<String>,
        records: [crate::kernel::FileRecord; 3],
    ) -> Result<Option<crate::ConflictView>, WorkspaceError> {
        if records
            .iter()
            .any(|record| record.kind != FileKind::Regular)
        {
            return Ok(None);
        }
        let mut bodies = Vec::with_capacity(3);
        for record in records {
            let Some(bytes) = self
                .target
                .volume
                .fs
                .workspace_conflict_bytes(&self.target.volume, record)
                .await?
            else {
                return Ok(Some(record_conflict_view(
                    self,
                    file_id,
                    path,
                    ConflictKind::Binary,
                )));
            };
            bodies.push(bytes);
        }
        let text = bodies
            .iter()
            .all(|bytes| std::str::from_utf8(bytes).is_ok());
        let mut values = bodies.into_iter().map(|bytes| {
            if text {
                ConflictValue::Text(String::from_utf8(bytes.to_vec()).unwrap_or_default())
            } else {
                ConflictValue::Binary(bytes.to_vec())
            }
        });
        Ok(Some(crate::ConflictView {
            key: ConflictKey::File(file_id),
            path,
            kind: if text {
                ConflictKind::Text
            } else {
                ConflictKind::Binary
            },
            base: values
                .next()
                .ok_or(WorkspaceError::InvalidMergeResolution)?,
            ours: values
                .next()
                .ok_or(WorkspaceError::InvalidMergeResolution)?,
            theirs: values
                .next()
                .ok_or(WorkspaceError::InvalidMergeResolution)?,
        }))
    }

    async fn apply_resolutions(
        &self,
        options: ApplyOptions,
        resolutions: BTreeMap<MergeConflict, crate::kernel::MergeConflictResolution>,
        permit: crate::PublicationPermit,
    ) -> Result<JoinOutcome<A, O>, WorkspaceError> {
        if options.if_target != self.target_head.id {
            return Ok(JoinOutcome::StaleTarget(self.target.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?));
        }
        let outcome = self
            .target
            .volume
            .fs
            .apply_workspace_join(crate::facade::WorkspaceJoinRequest {
                target: &self.target.volume,
                base: self.base,
                source: &self.source_head,
                expected_target: options.if_target,
                expected_head: self.target_authority_head,
                history: self.history,
                operation_id: options.idempotency_key.operation_id(),
                permit,
                maximum_generations: self.maximum_generations,
                maximum_changes: self.maximum_changes,
                maximum_conflicts: self.maximum_conflicts,
                resolutions,
            })
            .await?;
        self.map_join_outcome(outcome)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn apply_resolutions_guarded_with_native_work<'original, F, Fut, E>(
        &self, options: ApplyOptions,
        resolutions: &[(MergeConflict, crate::kernel::MergeConflictResolution)],
        permit: crate::PublicationPermit, original: &'original crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken, before_publication: &mut F,
    ) -> Result<(JoinOutcome<A, O>, crate::NativeAllocation), E>
    where
        F: for<'materialization> FnMut(&'materialization crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
    {
        if options.if_target != self.target_head.id {
            let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
            let (id, _) = self.target.volume.fs.workspace_head_state(
                &self.target.volume, &mut accounting, cancellation,
            ).await?;
            return Ok((JoinOutcome::StaleTarget(Generation {
                workspace: self.target.clone(), id, publication_closure: None,
            }), original.claim(0, 0, crate::WorkCounters::UNCHARGED).map_err(FsError::from)?));
        }
        let (outcome, allocation) = self.target.volume.fs.apply_workspace_join_guarded_with_native_work(
            crate::facade::WorkspaceJoinRequest {
                target: &self.target.volume, base: self.base, source: &self.source_head,
                expected_target: options.if_target, expected_head: self.target_authority_head,
                history: self.history, operation_id: options.idempotency_key.operation_id(),
                permit, maximum_generations: self.maximum_generations,
                maximum_changes: self.maximum_changes, maximum_conflicts: self.maximum_conflicts,
                resolutions,
            },
            original, cancellation, before_publication,
        ).await?;
        self.map_join_outcome(outcome).map(|outcome| (outcome, allocation)).map_err(E::from)
    }

    fn map_join_outcome(
        &self, outcome: crate::facade::WorkspaceJoinOutcome,
    ) -> Result<JoinOutcome<A, O>, WorkspaceError> {
        let generation = |id| Generation { workspace: self.target.clone(), id, publication_closure: None };
        Ok(match outcome {
            crate::facade::WorkspaceJoinOutcome::Joined(id, resolutions_digest) => {
                JoinOutcome::Applied(JoinApplication {
                    generation: generation(id),
                    resolutions_digest,
                })
            }
            crate::facade::WorkspaceJoinOutcome::AlreadyJoined(id, resolutions_digest) => {
                JoinOutcome::AlreadyApplied(JoinApplication {
                    generation: generation(id),
                    resolutions_digest,
                })
            }
            crate::facade::WorkspaceJoinOutcome::Applied(_)
            | crate::facade::WorkspaceJoinOutcome::AlreadyApplied(_) => {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            crate::facade::WorkspaceJoinOutcome::NoChanges(id) => {
                JoinOutcome::NoChanges(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::Stale(id) => {
                JoinOutcome::StaleTarget(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::Conflicted(conflicts, truncated) => {
                JoinOutcome::Conflicted {
                    conflicts,
                    truncated,
                }
            }
            crate::facade::WorkspaceJoinOutcome::Fenced => JoinOutcome::Fenced,
            crate::facade::WorkspaceJoinOutcome::IdempotencyConflict => {
                JoinOutcome::IdempotencyConflict
            }
        })
    }
}

impl From<FsError> for DrivenJoinError {
    fn from(error: FsError) -> Self {
        Self::Workspace(WorkspaceError::from(error))
    }
}

fn validate_join_side_selections(
    conflicts: &[MergeConflict], truncated: bool,
    selections: &BTreeMap<MergeConflict, ConflictSide>,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<(), DrivenJoinError> {
    if truncated || conflicts.len() != selections.len() {
        return Err(DrivenJoinError::StaleCandidate);
    }
    for conflict in conflicts {
        let mut found = false;
        for key in selections.keys() {
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })
                .map_err(WorkspaceError::from)?;
            match key.cmp(conflict) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => { found = true; break; }
                std::cmp::Ordering::Greater => break,
            }
        }
        if !found { return Err(DrivenJoinError::StaleCandidate); }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn native_join_side_resolutions(
    selections: &BTreeMap<MergeConflict, ConflictSide>,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<crate::kernel::Owned<Vec<(MergeConflict, crate::kernel::MergeConflictResolution)>>, FsError> {
    let mut resolutions = crate::kernel::Owned::allocate(selections.len(), accounting)?;
    for (conflict, side) in selections {
        let conflict = match conflict {
            MergeConflict::File(id) => MergeConflict::File(*id),
            MergeConflict::Binding { directory_id, name } => {
                let (name, allocation) = name.clone_with_accounting(accounting, cancellation)?;
                let bytes = name.retained_bytes() as u64;
                let mut name = crate::kernel::Owned::new(name, allocation, bytes);
                let retained = resolutions.bytes.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
                resolutions.allocation.absorb(&mut name.allocation)?;
                resolutions.bytes = retained;
                MergeConflict::Binding { directory_id: *directory_id, name: name.value }
            }
        };
        let side = match side {
            ConflictSide::Base => crate::kernel::MergeConflictSide::Base,
            ConflictSide::Ours => crate::kernel::MergeConflictSide::Ours,
            ConflictSide::Theirs => crate::kernel::MergeConflictSide::Theirs,
        };
        let copying = crate::WorkCounters {
            bytes_copied: std::mem::size_of::<(MergeConflict, crate::kernel::MergeConflictResolution)>() as u64,
            items_examined: 1,
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(copying)?;
        resolutions.value.push((conflict, crate::kernel::MergeConflictResolution::Select(side)));
        accounting.record_completed(copying)?;
    }
    Ok(resolutions)
}

fn binding_conflict_view<A, O>(
    plan: &JoinPlan<A, O>,
    conflict: MergeConflict,
) -> crate::ConflictView {
    match conflict {
        MergeConflict::File(file_id) => crate::ConflictView {
            key: ConflictKey::File(file_id),
            path: None,
            kind: ConflictKind::Record,
            base: ConflictValue::RecordReference {
                generation: plan.base.id,
                file_id,
            },
            ours: ConflictValue::RecordReference {
                generation: plan.target_head.id,
                file_id,
            },
            theirs: ConflictValue::RecordReference {
                generation: plan.source_head.id,
                file_id,
            },
        },
        MergeConflict::Binding { directory_id, name } => {
            let name = canonical_name_key(&name);
            let value = |generation| ConflictValue::BindingReference {
                generation,
                directory_id,
                name: name.clone(),
            };
            crate::ConflictView {
                key: ConflictKey::Binding {
                    directory_id,
                    name: name.clone(),
                },
                path: None,
                kind: ConflictKind::Binding,
                base: value(plan.base.id),
                ours: value(plan.target_head.id),
                theirs: value(plan.source_head.id),
            }
        }
    }
}

fn record_conflict_view<A, O>(
    plan: &JoinPlan<A, O>,
    file_id: FileId,
    path: Option<String>,
    kind: ConflictKind,
) -> crate::ConflictView {
    let value = |generation| ConflictValue::Record {
        generation,
        file_id,
    };
    crate::ConflictView {
        key: ConflictKey::File(file_id),
        path,
        kind,
        base: value(plan.base.id),
        ours: value(plan.target_head.id),
        theirs: value(plan.source_head.id),
    }
}

fn canonical_name_key(name: &LogicalName) -> Vec<u8> {
    let mut key = Vec::with_capacity(name.as_bytes().len().saturating_add(1));
    key.push(match name.encoding() {
        crate::kernel::NameEncoding::Utf8 => 1,
        crate::kernel::NameEncoding::PosixBytes => 2,
        crate::kernel::NameEncoding::WindowsUtf16Le => 3,
    });
    key.extend_from_slice(name.as_bytes());
    key
}

fn kernel_conflict(
    key: &ConflictKey,
    maximum_component_bytes: u32,
) -> Result<MergeConflict, DrivenJoinError> {
    match key {
        ConflictKey::File(file_id)
        | ConflictKey::Metadata(file_id)
        | ConflictKey::HardLinks(file_id)
        | ConflictKey::Directory(file_id)
        | ConflictKey::SpecialPayload(file_id)
        | ConflictKey::ContentRange { file_id, .. }
        | ConflictKey::Rename { file_id, .. } => Ok(MergeConflict::File(*file_id)),
        ConflictKey::Binding { directory_id, name } => {
            let (&tag, bytes) = name.split_first().ok_or(DrivenJoinError::StaleCandidate)?;
            let encoding = match tag {
                1 => crate::kernel::NameEncoding::Utf8,
                2 => crate::kernel::NameEncoding::PosixBytes,
                3 => crate::kernel::NameEncoding::WindowsUtf16Le,
                _ => return Err(DrivenJoinError::StaleCandidate),
            };
            let name = LogicalName::new(encoding, bytes.to_vec(), maximum_component_bytes)
                .map_err(|_| DrivenJoinError::StaleCandidate)?;
            Ok(MergeConflict::Binding {
                directory_id: *directory_id,
                name,
            })
        }
    }
}

fn conflict_file_id(key: &ConflictKey) -> Option<FileId> {
    match key {
        ConflictKey::File(file_id)
        | ConflictKey::Metadata(file_id)
        | ConflictKey::HardLinks(file_id)
        | ConflictKey::Directory(file_id)
        | ConflictKey::SpecialPayload(file_id)
        | ConflictKey::ContentRange { file_id, .. }
        | ConflictKey::Rename { file_id, .. } => Some(*file_id),
        ConflictKey::Binding { .. } => None,
    }
}

fn resolution_matches_conflict(key: &ConflictKey, resolution: &MergeResolution) -> bool {
    match resolution {
        MergeResolution::Select(_) | MergeResolution::Unresolved => true,
        MergeResolution::Text(_) | MergeResolution::Binary(_) => {
            matches!(key, ConflictKey::File(_) | ConflictKey::ContentRange { .. })
        }
        MergeResolution::Metadata(_) => matches!(key, ConflictKey::Metadata(_)),
        MergeResolution::Binding(_) => matches!(key, ConflictKey::Binding { .. }),
    }
}

#[cfg(test)]
mod merge_resolution_tests {
    use super::*;

    #[test]
    fn synthesized_resolutions_match_only_their_exact_conflict_shape() {
        let file_id = FileId::new();
        let file = ConflictKey::File(file_id);
        let metadata = ConflictKey::Metadata(file_id);
        let binding = ConflictKey::Binding {
            directory_id: file_id,
            name: vec![1, b'x'],
        };

        for resolution in [
            MergeResolution::Text("merged".into()),
            MergeResolution::Binary(vec![1]),
        ] {
            assert!(resolution_matches_conflict(&file, &resolution));
            assert!(!resolution_matches_conflict(&metadata, &resolution));
            assert!(!resolution_matches_conflict(&binding, &resolution));
        }
        let metadata_resolution = MergeResolution::Metadata(vec![1]);
        assert!(resolution_matches_conflict(&metadata, &metadata_resolution));
        assert!(!resolution_matches_conflict(&file, &metadata_resolution));
        let binding_resolution = MergeResolution::Binding(Some(file_id));
        assert!(resolution_matches_conflict(&binding, &binding_resolution));
        assert!(!resolution_matches_conflict(&file, &binding_resolution));
        assert!(resolution_matches_conflict(
            &file,
            &MergeResolution::Select(ConflictSide::Ours)
        ));
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Computes a semantic delta under explicit work and cancellation limits.
    pub async fn diff_to(
        &self,
        to: &Generation<A, O>,
        maximum_changes: u32,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<ChangeSet<A, O>, WorkspaceError> {
        let receipt = self
            .workspace
            .volume
            .fs
            .workspace_join_changes(self.id, to, maximum_changes, budget, cancellation)
            .await.map(|receipt| crate::FsReceipt { value: receipt.value.0, work: receipt.work })?;
        Ok(ChangeSet {
            from: self.clone(),
            to: to.clone(),
            changes: receipt.value,
            work: receipt.work,
        })
    }

    /// Computes the same semantic delta under the caller's original native work.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn diff_to_with_native_work(
        &self,
        to: &Generation<A, O>,
        maximum_changes: u32,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(ChangeSet<A, O>, crate::NativeAllocation), WorkspaceError> {
        let (changes, allocation) = self.workspace.volume.fs
            .workspace_join_changes(self.id, to, maximum_changes, original, cancellation)
            .await.and_then(|(diff, allocation)| match allocation {
                crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => Ok((diff, allocation)),
                crate::kernel::allocation::KernelOwnedAllocation::Standalone => Err(WorkspaceError::Filesystem(
                    FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout),
                )),
            })?;
        Ok((ChangeSet {
            from: self.clone(),
            to: to.clone(),
            changes,
            work: original.receipt(),
        }, allocation))
    }
    /// Finds paths for one stable file identity under supplied work limits.
    pub async fn paths_for_file_id(
        &self,
        file_id: FileId,
        maximum_entries: u32,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Vec<String>>, WorkspaceError> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let search = self.namespace_records_with_accounting(
                &[file_id], maximum_entries, &mut accounting, cancellation,
            ).await?;
            let mut paths = crate::kernel::Owned::allocate(0, &mut accounting)?;
            if let Some(records) = bounded_search(&search.0.records, &mut accounting, |record, _| Ok(record.0.cmp(&file_id))).map(|(index, present)| present.then(|| &search.0.records[index].1))? {
                for (path, _) in records {
                    paths.ensure_for_push(records.len(), &mut accounting)?;
                    let (text, mut allocation) = path.unicode_text_with_accounting(&mut accounting, cancellation)?;
                    paths.allocation.absorb(&mut allocation)?;
                    paths.bytes = paths.bytes.checked_add(text.capacity() as u64).ok_or(crate::WorkError::Overflow)?;
                    paths.value.push(text);
                }
            }
            Ok(paths.value)
        }.await;
        crate::facade::measured(result, &accounting)
    }

    /// Finds portable paths for multiple stable file identities in one namespace traversal.
    pub async fn paths_for_file_ids(
        &self,
        file_ids: impl IntoIterator<Item = FileId>,
        maximum_entries: u32,
    ) -> Result<BTreeMap<FileId, Vec<String>>, WorkspaceError> {
        self.namespace_records_for_file_ids(file_ids, maximum_entries).await?
            .records.into_iter().map(|(file_id, records)| {
                records.into_iter().map(|(path, _)| namespace_path_text(&path))
                    .collect::<Result<Vec<_>, _>>().map(|paths| (file_id, paths))
            }).collect()
    }

    async fn namespace_records_for_file_ids(
        &self,
        file_ids: impl IntoIterator<Item = FileId>,
        maximum_entries: u32,
    ) -> Result<NamespaceRecordSearch, WorkspaceError> {
        let cancellation = crate::CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
        let ids = sorted_changed_file_ids(file_ids, &mut accounting)?;
        self.namespace_records_with_accounting(
            &ids.0, maximum_entries, &mut accounting, &cancellation,
        ).await.map(|owned| owned.0)
    }

    async fn namespace_records_with_accounting(
        &self, ids: &[FileId], maximum_entries: u32,
        accounting: &mut KernelAccounting<'_>, cancellation: &crate::CancellationToken,
    ) -> Result<(NamespaceRecordSearch, KernelOwnedAllocation), WorkspaceError> {
        accounting.check()?;
        if ids.is_empty() || maximum_entries == 0 {
            return Ok((NamespaceRecordSearch { records: NamespaceRecords::default(), complete: ids.is_empty() },
                accounting.claim_owned(0, 0)?));
        }
        let checkout = self.workspace.open_checkout(
            GenerationSelector::Exact(self.id), CheckoutMode::read_only_pinned(), accounting, cancellation,
        ).await?;
        let generation_root = crate::ObjectId { kind: crate::ObjectKind::GenerationRoot, digest: self.id.digest() };
        let result = namespace_records_to_file_root_with_accounting(
            &self.workspace.volume, checkout.value.root(), generation_root,
            ids, maximum_entries, accounting, cancellation,
        ).await;
        checkout.retire(accounting)?;
        result.map_err(WorkspaceError::from)
    }

}

#[cfg(test)]
#[path = "../tests/workspace.rs"]
mod tests;
