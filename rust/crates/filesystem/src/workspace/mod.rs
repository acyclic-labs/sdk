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
use accounting::{merge_workspace_backend, workspace_allocation_error, workspace_allocation};
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
use accounting::merge_workspace_work;
pub(crate) use paths::{customer_path, customer_path_with_accounting};
use paths::{logical_name_text, namespace_path_text, regular_file_bytes, restore_absolute_path, copy_workspace_label};
pub(crate) use errors::workspace_bounded_error;

mod restore;
mod diff;
mod reverse_paths;
mod join_apply;
pub use restore::WorkspaceRestore;
pub use diff::{ChangeSet, ChangedPath};
pub use join_apply::DrivenJoinError;
pub(crate) use reverse_paths::reverse_paths_to_file_root_with_accounting;
use reverse_paths::{bounded_search, sorted_changed_file_ids, reverse_drop, reverse_grow, namespace_records_bytes, NamespaceRecordSearch, NamespaceRecords};

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
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

#[cfg(test)]
#[path = "../tests/workspace.rs"]
mod tests;
