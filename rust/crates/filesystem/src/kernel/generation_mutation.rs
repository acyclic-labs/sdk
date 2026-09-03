//! One-volume generation transaction composition over sparse persistent kernels.

use super::allocation::{AllocationError, AllocationLedger};
use super::regular_mutation::{
    RegularMutation, RegularMutationError, apply_regular_clone_async, apply_regular_mutation_async,
};
use super::{
    DecodeLimits, DirectoryReadError, FileKind, FileMutation, FilePayload, FileRecord,
    FileRecordReadError, FileTableMutation, FileTableMutationError, GenerationRoot, LogicalName,
    MetadataField, Mutation, MutationPlan, MutationPlanError, NamespacePath, PathBatchEntry,
    PathLookupError, TreeEntry, TreeMutation, TreeMutationError, apply_file_table_mutations_async,
    apply_tree_mutations_async, list_tree_entries_async, lookup_file_records_async,
    lookup_path_refs_async,
};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::foundation::{FileId, GenerationId};
use crate::model::{VolumeConfig, VolumeConfigError};
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::ObjectId;
use std::cell::Cell;
use std::mem::size_of;
use thiserror::Error;

/// Candidate generation produced by one atomic within-volume transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationMutationReceipt {
    /// Unpublished immutable generation root.
    pub root: GenerationRoot,
    /// Exact planning, lookup, path-copy, and immutable-write work.
    pub work: WorkCounters,
}

/// Generation transaction failure preserving spent work and harmless orphan objects.
pub type GenerationMutationFailure = OperationFailure<GenerationMutationError>;

/// Stable one-volume generation transaction failures.
#[derive(Debug, Error)]
pub enum GenerationMutationError {
    /// A path mutation planner rejected the batch.
    #[error(transparent)]
    Plan(#[from] MutationPlanError),
    /// Volume limits or semantic configuration are invalid.
    #[error(transparent)]
    Config(#[from] VolumeConfigError),
    /// Mutation endpoints exceed the admitted shared-path batch bound.
    #[error("generation mutation endpoint count exceeds the volume path limit")]
    TooManyPaths,
    /// A newly created record must begin with exactly one namespace binding.
    #[error("new file record must have link count one")]
    InvalidInitialLinkCount,
    /// The volume does not admit symbolic links.
    #[error("symbolic links are disabled for this volume")]
    UnsupportedSymbolicLink,
    /// The volume profile cannot represent the created kind exactly.
    #[error("file kind is unsupported by the volume profile")]
    UnsupportedFileKind,
    /// The requested operation can introduce sparse semantics in a dense volume.
    #[error("sparse mutation is disabled for this volume")]
    UnsupportedSparseMutation,
    /// Shared authenticated path resolution failed.
    #[error(transparent)]
    Path(#[from] PathLookupError),
    /// Created-identity freshness lookup failed.
    #[error(transparent)]
    FileRecord(#[from] FileRecordReadError),
    /// One operation requires a parent directory absent from the candidate view.
    #[error("namespace mutation parent directory is missing")]
    MissingParent,
    /// One operation requires an existing source binding.
    #[error("namespace mutation source is missing")]
    MissingSource,
    /// A create/link/rename destination expected absence.
    #[error("namespace mutation destination already exists")]
    AlreadyExists,
    /// A supplied exact file identity precondition failed.
    #[error("namespace mutation file identity precondition failed")]
    FileIdentityConflict,
    /// A create reused an identity already present in the base or batch.
    #[error("namespace mutation created file identity is not fresh")]
    FileIdentityAlreadyExists,
    /// A hard link targeted a directory or a volume without hard-link support.
    #[error("namespace mutation hard link is unsupported")]
    UnsupportedHardLink,
    /// A directory move would place the directory below itself.
    #[error("namespace mutation would create a directory cycle")]
    DirectoryCycle,
    /// A removed or replaced directory is not empty in the candidate view.
    #[error("namespace mutation directory is not empty")]
    DirectoryNotEmpty,
    /// Authenticated state and sparse planning disagree.
    #[error("namespace mutation authenticated state is inconsistent")]
    InconsistentState,
    /// Directory path-copy failed.
    #[error(transparent)]
    Tree(#[from] TreeMutationError),
    /// File-table path-copy failed.
    #[error(transparent)]
    FileTable(#[from] FileTableMutationError),
    /// Regular-file sparse mutation failed.
    #[error(transparent)]
    Regular(#[from] RegularMutationError),
    /// Bounded empty-directory proof failed.
    #[error(transparent)]
    Directory(#[from] DirectoryReadError),
    /// Bounded scratch storage could not be allocated.
    #[error("namespace mutation scratch allocation failed")]
    AllocationFailed,
    /// Exact work overflowed or exceeded the request budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Binding {
    file_id: FileId,
    kind: FileKind,
}

#[derive(Clone, Copy)]
struct PathState {
    operation: usize,
    endpoint: usize,
    binding: Option<Binding>,
    base_parent: Option<FileId>,
    base_parent_exact: bool,
    parent_state: Option<usize>,
}

#[derive(Clone, Copy)]
struct RecordSeed {
    record: FileRecord,
    created: bool,
}

#[derive(Clone, Copy)]
struct RecordState {
    base: Option<FileRecord>,
    working: FileRecord,
    present: bool,
}

struct DirectoryEdit {
    directory_id: FileId,
    order: u32,
    retained_name_bytes: u64,
    mutation: Option<TreeMutation>,
}

struct FreshnessContext<'a, S> {
    store: &'a S,
    generation: &'a GenerationRoot,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &'a CancellationToken,
}

struct InitialLookups {
    paths: Vec<PathBatchEntry>,
    path_bytes: u64,
    identities: Vec<Option<FileRecord>>,
    identity_bytes: u64,
}

/// Applies one ordered namespace, metadata, and content batch to a generation.
///
/// All path preconditions are evaluated against a sparse candidate view. The
/// executor performs one borrowed-path lookup, groups edits by stable parent
/// directory identity, rewrites every touched directory once, and then rewrites
/// the global file table once. It never publishes authority state; callers feed
/// the returned root into checkpoint and publication.
///
/// # Errors
///
/// Rejects malformed or contradictory candidate state before publication and
/// retains exact work plus safe orphan-write evidence on storage,
/// cancellation, allocation, or semantic failure.
pub async fn apply_generation_mutations_async<S: AsyncObjectStore>(
    store: &S,
    generation: &GenerationRoot,
    operations: Vec<Mutation>,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<GenerationMutationReceipt, GenerationMutationFailure> {
    let (receipt, _operations) = apply_generation_mutations_retaining_async(
        store,
        generation,
        operations,
        config,
        budget,
        cancellation,
    )
    .await?;
    Ok(receipt)
}

pub(crate) async fn apply_generation_mutations_retaining_async<S: AsyncObjectStore>(
    store: &S,
    generation: &GenerationRoot,
    operations: Vec<Mutation>,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(GenerationMutationReceipt, Vec<Mutation>), GenerationMutationFailure> {
    cancellation
        .check()
        .map_err(|_| OperationFailure::before_work(path_cancelled()))?;
    preflight_generation_mutations(&operations, config)?;
    let plan = MutationPlan::compile(operations, config.limits, budget)
        .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
    let mut work = plan.work();
    let mut allocations = AllocationLedger::default();
    allocations
        .claim_bytes(plan.retained_allocation_bytes(), 0, &mut work, budget)
        .map_err(|error| allocation_failure(error, work))?;

    let (created_ids, created_id_bytes) = verify_created_identities(
        FreshnessContext {
            store,
            generation,
            config,
            budget,
            cancellation,
        },
        &plan,
        &mut allocations,
        &mut work,
    )
    .await?;

    let lookups = load_initial_lookups(
        FreshnessContext {
            store,
            generation,
            config,
            budget,
            cancellation,
        },
        &plan,
        &created_ids,
        &mut allocations,
        &mut work,
    )
    .await?;
    drop(created_ids);
    allocations
        .release(created_id_bytes)
        .map_err(|error| allocation_failure(error, work))?;

    let mut state = TransactionState::new(&plan, lookups, config, allocations, work, budget)?;
    state.simulate(store, &plan, config, cancellation).await?;
    let plan_bytes = plan.retained_allocation_bytes();
    let operations = plan.into_operations();
    state
        .allocations
        .release(plan_bytes)
        .map_err(|error| allocation_failure(error, state.work))?;
    state
        .rewrite_directories(store, config, cancellation)
        .await?;
    state
        .verify_removed_directories(store, config, cancellation)
        .await?;
    let receipt = state
        .rewrite_file_table(store, generation, config, cancellation)
        .await?;
    Ok((receipt, operations))
}

/// Synchronous adapter over [`apply_generation_mutations_async`].
///
/// # Errors
///
/// Returns the same bounded failures, plus a typed rejection if a nominally
/// synchronous object store unexpectedly suspends.
pub fn apply_generation_mutations<S: crate::ImmediateObjectStore>(
    store: &S,
    generation: &GenerationRoot,
    operations: Vec<Mutation>,
    config: VolumeConfig,
    budget: WorkBudget,
) -> Result<GenerationMutationReceipt, GenerationMutationFailure> {
    crate::async_storage::poll_immediate(apply_generation_mutations_async(
        store,
        generation,
        operations,
        config,
        budget,
        &CancellationToken::new(),
    ))
}

async fn verify_created_identities<S: AsyncObjectStore>(
    context: FreshnessContext<'_, S>,
    plan: &MutationPlan,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
) -> Result<(Vec<FileId>, u64), GenerationMutationFailure> {
    let created_count = plan
        .operations()
        .iter()
        .filter(|operation| matches!(operation, Mutation::Create { .. }))
        .count();
    if created_count == 0 {
        return Ok((Vec::new(), 0));
    }
    let mut created_ids =
        reserve_exact::<FileId>(created_count, allocations, work, context.budget)?;
    created_ids.extend(plan.operations().iter().filter_map(|operation| {
        if let Mutation::Create { record, .. } = operation {
            Some(record.file_id)
        } else {
            None
        }
    }));
    let comparisons = Cell::new(0_u64);
    created_ids.sort_unstable_by(|left, right| {
        comparisons.set(comparisons.get().saturating_add(1));
        left.cmp(right)
    });
    charge_items(work, comparisons.get(), context.budget)?;
    let mut duplicate_examined = 0_u64;
    for pair in created_ids.windows(2) {
        duplicate_examined = duplicate_examined
            .checked_add(1)
            .ok_or_else(|| failed(WorkError::Overflow.into(), *work))?;
        if pair[0] == pair[1] {
            charge_items(work, duplicate_examined, context.budget)?;
            return Err(failed(
                GenerationMutationError::FileIdentityAlreadyExists,
                *work,
            ));
        }
    }
    charge_items(work, duplicate_examined, context.budget)?;
    let created_bytes = logical_vec_bytes(&created_ids)?;
    let freshness = lookup_file_records_async(
        context.store,
        context.generation.file_table,
        &created_ids,
        context.config.limits.maximum_mutations_per_batch,
        decode_limits(context.config),
        nested_budget(*work, context.budget, allocations.live_bytes())?,
        context.cancellation,
    )
    .await
    .map_err(|failure| {
        nested_failure(
            *work,
            *failure.work,
            allocations.live_bytes(),
            failure.error.into(),
        )
    })?;
    *work = merge_nested(
        *work,
        freshness.work,
        allocations.live_bytes(),
        context.budget,
    )?;
    if freshness.records.iter().any(Option::is_some) {
        return Err(failed(
            GenerationMutationError::FileIdentityAlreadyExists,
            *work,
        ));
    }
    drop(freshness);
    Ok((created_ids, created_bytes))
}

async fn load_initial_lookups<S: AsyncObjectStore>(
    context: FreshnessContext<'_, S>,
    plan: &MutationPlan,
    created_ids: &[FileId],
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
) -> Result<InitialLookups, GenerationMutationFailure> {
    let (paths, path_bytes) = load_path_lookups(&context, plan, allocations, work).await?;
    let (identities, identity_bytes) =
        load_identity_lookups(&context, plan, created_ids, allocations, work).await?;
    Ok(InitialLookups {
        paths,
        path_bytes,
        identities,
        identity_bytes,
    })
}

async fn load_path_lookups<S: AsyncObjectStore>(
    context: &FreshnessContext<'_, S>,
    plan: &MutationPlan,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
) -> Result<(Vec<PathBatchEntry>, u64), GenerationMutationFailure> {
    let path_count = plan.ordered_operation_paths().len();
    let mut path_refs =
        reserve_exact::<&NamespacePath>(path_count, allocations, work, context.budget)?;
    path_refs.extend(plan.ordered_paths());
    let path_ref_bytes = logical_vec_bytes(&path_refs)?;
    let (paths, path_bytes) = if path_refs.is_empty() {
        (Vec::new(), 0)
    } else {
        let lookup = lookup_path_refs_async(
            context.store,
            context.generation,
            &path_refs,
            context.config,
            nested_budget(*work, context.budget, allocations.live_bytes())?,
            context.cancellation,
        )
        .await
        .map_err(|failure| {
            nested_failure(
                *work,
                *failure.work,
                allocations.live_bytes(),
                failure.error.into(),
            )
        })?;
        *work = merge_nested(*work, lookup.work, allocations.live_bytes(), context.budget)?;
        (lookup.entries, lookup.retained_allocation_bytes)
    };
    drop(path_refs);
    allocations
        .release(path_ref_bytes)
        .map_err(|error| allocation_failure(error, *work))?;
    allocations
        .claim_bytes(path_bytes, 0, work, context.budget)
        .map_err(|error| allocation_failure(error, *work))?;
    Ok((paths, path_bytes))
}

async fn load_identity_lookups<S: AsyncObjectStore>(
    context: &FreshnessContext<'_, S>,
    plan: &MutationPlan,
    created_ids: &[FileId],
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
) -> Result<(Vec<Option<FileRecord>>, u64), GenerationMutationFailure> {
    let mut maximum_identity_uses = 0_usize;
    for operation in plan.operations() {
        maximum_identity_uses = maximum_identity_uses
            .checked_add(match operation {
                Mutation::File { .. } => 1,
                Mutation::CloneFileRange { .. } => 2,
                _ => 0,
            })
            .ok_or_else(|| failed(WorkError::Overflow.into(), *work))?;
    }
    if maximum_identity_uses == 0 {
        return Ok((Vec::new(), 0));
    }
    let mut identity_ids =
        reserve_exact::<FileId>(maximum_identity_uses, allocations, work, context.budget)?;
    for operation in plan.operations() {
        match operation {
            Mutation::File { file_id, .. } => identity_ids.push(*file_id),
            Mutation::CloneFileRange {
                source_file_id,
                destination_file_id,
                ..
            } => {
                identity_ids.push(*source_file_id);
                identity_ids.push(*destination_file_id);
            }
            _ => {}
        }
    }
    let comparisons = Cell::new(0_u64);
    identity_ids.sort_unstable_by(|left, right| {
        comparisons.set(comparisons.get().saturating_add(1));
        left.cmp(right)
    });
    charge_items(work, comparisons.get(), context.budget)?;
    // `dedup` examines every sorted identity once. Keep that deterministic
    // linear pass visible in the exact-work receipt instead of hiding it in
    // standard-library collection maintenance.
    charge_items(
        work,
        crate::foundation::usize_to_u64(identity_ids.len()),
        context.budget,
    )?;
    identity_ids.dedup();
    let identity_id_bytes = logical_vec_bytes(&identity_ids)?;
    let identities = if identity_ids.is_empty() {
        Vec::new()
    } else {
        let lookup = lookup_file_records_async(
            context.store,
            context.generation.file_table,
            &identity_ids,
            u32::try_from(identity_ids.len())
                .map_err(|_| failed(WorkError::Overflow.into(), *work))?,
            decode_limits(context.config),
            nested_budget(*work, context.budget, allocations.live_bytes())?,
            context.cancellation,
        )
        .await
        .map_err(|failure| {
            nested_failure(
                *work,
                *failure.work,
                allocations.live_bytes(),
                failure.error.into(),
            )
        })?;
        *work = merge_nested(*work, lookup.work, allocations.live_bytes(), context.budget)?;
        for (file_id, record) in identity_ids.iter().zip(&lookup.records) {
            if record.is_none() {
                let comparisons = Cell::new(0_u64);
                let created = created_ids.binary_search_by(|candidate| {
                    comparisons.set(comparisons.get().saturating_add(1));
                    candidate.cmp(file_id)
                });
                charge_items(work, comparisons.get(), context.budget)?;
                if created.is_err() {
                    return Err(failed(GenerationMutationError::MissingSource, *work));
                }
            }
        }
        lookup.records
    };
    let identity_bytes = logical_vec_bytes(&identities)?;
    allocations
        .claim_bytes(identity_bytes, 0, work, context.budget)
        .map_err(|error| allocation_failure(error, *work))?;
    drop(identity_ids);
    allocations
        .release(identity_id_bytes)
        .map_err(|error| allocation_failure(error, *work))?;
    Ok((identities, identity_bytes))
}

struct TransactionState {
    paths: Vec<PathState>,
    operation_paths: Vec<[usize; 2]>,
    records: Vec<RecordState>,
    directory_edits: Vec<DirectoryEdit>,
    removed_directories: Vec<FileId>,
    allocations: AllocationLedger,
    work: WorkCounters,
    budget: WorkBudget,
    config: VolumeConfig,
}

impl TransactionState {
    #[allow(clippy::too_many_lines)]
    fn new(
        plan: &MutationPlan,
        lookups: InitialLookups,
        config: VolumeConfig,
        mut allocations: AllocationLedger,
        mut work: WorkCounters,
        budget: WorkBudget,
    ) -> Result<Self, GenerationMutationFailure> {
        let InitialLookups {
            paths: lookup,
            path_bytes: lookup_retained_bytes,
            identities: identity_records,
            identity_bytes: identity_record_bytes,
        } = lookups;
        let operation_count = plan.operations().len();
        let mut operation_paths =
            reserve_exact::<[usize; 2]>(operation_count, &mut allocations, &mut work, budget)?;
        operation_paths.resize(operation_count, [usize::MAX; 2]);
        let mut paths =
            reserve_exact::<PathState>(lookup.len(), &mut allocations, &mut work, budget)?;
        if plan.ordered_operation_paths().len() != lookup.len() {
            return Err(failed(GenerationMutationError::InconsistentState, work));
        }
        let mut seeds = reserve_exact::<RecordSeed>(
            lookup
                .len()
                .checked_mul(2)
                .and_then(|value| value.checked_add(operation_count))
                .and_then(|value| value.checked_add(identity_records.len()))
                .ok_or_else(|| failed(WorkError::Overflow.into(), work))?,
            &mut allocations,
            &mut work,
            budget,
        )?;
        let mut previous: Option<&NamespacePath> = None;
        let mut current_state = usize::MAX;
        for (index, ((operation, endpoint, path), entry)) in plan
            .ordered_operation_paths()
            .zip(lookup.iter())
            .enumerate()
        {
            if previous != Some(path) {
                current_state = paths.len();
                let binding = entry.record.map(|record| Binding {
                    file_id: record.file_id,
                    kind: record.kind,
                });
                paths.push(PathState {
                    operation,
                    endpoint,
                    binding,
                    base_parent: entry.parent.map(|record| record.file_id),
                    base_parent_exact: (entry.record.is_some()
                        && usize::from(entry.resolved_components) == path.depth())
                        || (entry.record.is_none()
                            && usize::from(entry.resolved_components).saturating_add(1)
                                == path.depth()),
                    parent_state: None,
                });
                previous = Some(path);
            } else if lookup[index] != lookup[index - 1] {
                return Err(failed(GenerationMutationError::InconsistentState, work));
            }
            operation_paths[operation][endpoint] = current_state;
            if endpoint == 0 && operation_uses_one_path(&plan.operations()[operation]) {
                operation_paths[operation][1] = current_state;
            }
            if let Some(record) = entry.record {
                seeds.push(RecordSeed {
                    record,
                    created: false,
                });
            }
            if let Some(record) = entry.parent {
                seeds.push(RecordSeed {
                    record,
                    created: false,
                });
            }
        }
        for operation in plan.operations() {
            if let Mutation::Create { record, .. } = operation {
                seeds.push(RecordSeed {
                    record: *record,
                    created: true,
                });
            }
        }
        for record in identity_records.iter().flatten() {
            seeds.push(RecordSeed {
                record: *record,
                created: false,
            });
        }
        let comparisons = Cell::new(0_u64);
        seeds.sort_unstable_by(|left, right| {
            comparisons.set(comparisons.get().saturating_add(1));
            left.record.file_id.cmp(&right.record.file_id)
        });
        charge_items(&mut work, comparisons.get(), budget)?;
        let mut records =
            reserve_exact::<RecordState>(seeds.len(), &mut allocations, &mut work, budget)?;
        let mut cursor = 0;
        while cursor < seeds.len() {
            let file_id = seeds[cursor].record.file_id;
            let mut base = None;
            let mut created = None;
            let mut group_examined = 0_u64;
            while cursor < seeds.len() && seeds[cursor].record.file_id == file_id {
                group_examined = group_examined
                    .checked_add(1)
                    .ok_or_else(|| failed(WorkError::Overflow.into(), work))?;
                let seed = seeds[cursor];
                if seed.created {
                    if created.replace(seed.record).is_some() {
                        charge_items(&mut work, group_examined, budget)?;
                        return Err(failed(
                            GenerationMutationError::FileIdentityAlreadyExists,
                            work,
                        ));
                    }
                } else if base.is_some_and(|value| value != seed.record) {
                    charge_items(&mut work, group_examined, budget)?;
                    return Err(failed(
                        GenerationMutationError::FileIdentityAlreadyExists,
                        work,
                    ));
                } else {
                    base = Some(seed.record);
                }
                cursor += 1;
            }
            charge_items(&mut work, group_examined, budget)?;
            if base.is_some() && created.is_some() {
                return Err(failed(
                    GenerationMutationError::FileIdentityAlreadyExists,
                    work,
                ));
            }
            let working = base
                .or(created)
                .ok_or_else(|| failed(GenerationMutationError::InconsistentState, work))?;
            records.push(RecordState {
                base,
                working,
                present: base.is_some(),
            });
        }
        let path_comparisons = Cell::new(0_u64);
        for index in 0..paths.len() {
            let path = state_path(plan, &paths[index]);
            let Some((parent, _)) = path.split_last() else {
                return Err(failed(GenerationMutationError::InconsistentState, work));
            };
            paths[index].parent_state = find_path_state(plan, &paths, parent, &path_comparisons);
        }
        charge_items(&mut work, path_comparisons.get(), budget)?;
        let directory_edits = reserve_exact::<DirectoryEdit>(
            operation_count
                .checked_mul(2)
                .ok_or_else(|| failed(WorkError::Overflow.into(), work))?,
            &mut allocations,
            &mut work,
            budget,
        )?;
        let removed_directories =
            reserve_exact::<FileId>(operation_count, &mut allocations, &mut work, budget)?;
        let seed_bytes = logical_vec_bytes(&seeds)?;
        drop(seeds);
        allocations
            .release(seed_bytes)
            .map_err(|error| allocation_failure(error, work))?;
        drop(lookup);
        allocations
            .release(lookup_retained_bytes)
            .map_err(|error| allocation_failure(error, work))?;
        drop(identity_records);
        allocations
            .release(identity_record_bytes)
            .map_err(|error| allocation_failure(error, work))?;
        Ok(Self {
            paths,
            operation_paths,
            records,
            directory_edits,
            removed_directories,
            allocations,
            work,
            budget,
            config,
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn simulate<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        plan: &MutationPlan,
        config: VolumeConfig,
        cancellation: &CancellationToken,
    ) -> Result<(), GenerationMutationFailure> {
        for (ordinal, operation) in plan.operations().iter().enumerate() {
            let order = u32::try_from(ordinal)
                .map_err(|_| failed(GenerationMutationError::InconsistentState, self.work))?;
            match operation {
                Mutation::Create { record, .. } => self.create(plan, ordinal, *record, order)?,
                Mutation::Remove {
                    expected_file_id, ..
                } => {
                    let expected = match expected_file_id {
                        MetadataField::Unavailable => None,
                        MetadataField::Value(value) => Some(*value),
                    };
                    self.remove(plan, ordinal, expected, order)?;
                }
                Mutation::Rename {
                    source,
                    destination,
                    replace,
                } => self.rename(plan, ordinal, source, destination, *replace, order)?,
                Mutation::Link { .. } => self.link(plan, ordinal, order)?,
                Mutation::SetMetadata { metadata, .. } => {
                    let binding = self.source_binding(ordinal)?;
                    self.record_mut(binding.file_id)?.working.metadata = *metadata;
                }
                Mutation::Write {
                    offset,
                    length,
                    content,
                    content_offset,
                    ..
                } => {
                    self.mutate_regular(
                        store,
                        ordinal,
                        RegularMutation::Write {
                            offset: *offset,
                            length: *length,
                            content: *content,
                            content_offset: *content_offset,
                        },
                        config,
                        cancellation,
                    )
                    .await?;
                }
                Mutation::ValidateRegular { .. } => {
                    let binding = self.source_binding(ordinal)?;
                    if binding.kind != FileKind::Regular {
                        return Err(failed(
                            GenerationMutationError::InconsistentState,
                            self.work,
                        ));
                    }
                }
                Mutation::Resize { logical_bytes, .. } => {
                    self.mutate_regular(
                        store,
                        ordinal,
                        RegularMutation::Resize {
                            logical_bytes: *logical_bytes,
                        },
                        config,
                        cancellation,
                    )
                    .await?;
                }
                Mutation::ZeroRange {
                    offset,
                    length,
                    allocated,
                    extend,
                    ..
                } => {
                    self.mutate_regular(
                        store,
                        ordinal,
                        RegularMutation::ZeroRange {
                            offset: *offset,
                            length: *length,
                            allocated: *allocated,
                            extend: *extend,
                        },
                        config,
                        cancellation,
                    )
                    .await?;
                }
                Mutation::Preallocate {
                    offset,
                    length,
                    keep_size,
                    ..
                } => {
                    self.mutate_regular(
                        store,
                        ordinal,
                        RegularMutation::Preallocate {
                            offset: *offset,
                            length: *length,
                            keep_size: *keep_size,
                        },
                        config,
                        cancellation,
                    )
                    .await?;
                }
                Mutation::CloneRange {
                    source_offset,
                    destination_offset,
                    length,
                    ..
                } => {
                    self.clone_regular(
                        store,
                        ordinal,
                        *source_offset,
                        *destination_offset,
                        *length,
                        config,
                        cancellation,
                    )
                    .await?;
                }
                Mutation::File { file_id, mutation } => match mutation {
                    FileMutation::SetMetadata { metadata } => {
                        self.identity_binding(*file_id)?;
                        self.record_mut(*file_id)?.working.metadata = *metadata;
                    }
                    FileMutation::Write {
                        offset,
                        length,
                        content,
                        content_offset,
                    } => {
                        self.mutate_regular_id(
                            store,
                            *file_id,
                            RegularMutation::Write {
                                offset: *offset,
                                length: *length,
                                content: *content,
                                content_offset: *content_offset,
                            },
                            config,
                            cancellation,
                        )
                        .await?;
                    }
                    FileMutation::ValidateRegular => {
                        let binding = self.identity_binding(*file_id)?;
                        if binding.kind != FileKind::Regular {
                            return Err(failed(
                                GenerationMutationError::InconsistentState,
                                self.work,
                            ));
                        }
                    }
                    FileMutation::Resize { logical_bytes } => {
                        self.mutate_regular_id(
                            store,
                            *file_id,
                            RegularMutation::Resize {
                                logical_bytes: *logical_bytes,
                            },
                            config,
                            cancellation,
                        )
                        .await?;
                    }
                    FileMutation::ZeroRange {
                        offset,
                        length,
                        allocated,
                        extend,
                    } => {
                        self.mutate_regular_id(
                            store,
                            *file_id,
                            RegularMutation::ZeroRange {
                                offset: *offset,
                                length: *length,
                                allocated: *allocated,
                                extend: *extend,
                            },
                            config,
                            cancellation,
                        )
                        .await?;
                    }
                    FileMutation::Preallocate {
                        offset,
                        length,
                        keep_size,
                    } => {
                        self.mutate_regular_id(
                            store,
                            *file_id,
                            RegularMutation::Preallocate {
                                offset: *offset,
                                length: *length,
                                keep_size: *keep_size,
                            },
                            config,
                            cancellation,
                        )
                        .await?;
                    }
                },
                Mutation::CloneFileRange {
                    source_file_id,
                    source_offset,
                    destination_file_id,
                    destination_offset,
                    length,
                } => {
                    self.clone_regular_ids(
                        store,
                        *source_file_id,
                        *source_offset,
                        *destination_file_id,
                        *destination_offset,
                        *length,
                        config,
                        cancellation,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn mutate_regular<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        operation: usize,
        mutation: RegularMutation,
        config: VolumeConfig,
        cancellation: &CancellationToken,
    ) -> Result<(), GenerationMutationFailure> {
        let binding = self.source_binding(operation)?;
        self.mutate_regular_id(store, binding.file_id, mutation, config, cancellation)
            .await
    }

    async fn mutate_regular_id<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        file_id: FileId,
        mutation: RegularMutation,
        config: VolumeConfig,
        cancellation: &CancellationToken,
    ) -> Result<(), GenerationMutationFailure> {
        let binding = self.identity_binding(file_id)?;
        if binding.kind != FileKind::Regular {
            return Err(failed(
                GenerationMutationError::InconsistentState,
                self.work,
            ));
        }
        let payload = self.record(file_id)?.working.payload;
        let receipt = apply_regular_mutation_async(
            store,
            payload,
            mutation,
            config,
            nested_budget(self.work, self.budget, self.allocations.live_bytes())?,
            cancellation,
        )
        .await
        .map_err(|failure| {
            nested_failure(
                self.work,
                *failure.work,
                self.allocations.live_bytes(),
                failure.error.into(),
            )
        })?;
        self.work = merge_nested(
            self.work,
            receipt.work,
            self.allocations.live_bytes(),
            self.budget,
        )?;
        self.record_mut(file_id)?.working.payload = receipt.payload;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn clone_regular<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        operation: usize,
        source_offset: u64,
        destination_offset: u64,
        length: u64,
        config: VolumeConfig,
        cancellation: &CancellationToken,
    ) -> Result<(), GenerationMutationFailure> {
        let source = self.binding(operation, 0)?;
        let destination = self.binding(operation, 1)?;
        self.clone_regular_ids(
            store,
            source.file_id,
            source_offset,
            destination.file_id,
            destination_offset,
            length,
            config,
            cancellation,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn clone_regular_ids<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        source_file_id: FileId,
        source_offset: u64,
        destination_file_id: FileId,
        destination_offset: u64,
        length: u64,
        config: VolumeConfig,
        cancellation: &CancellationToken,
    ) -> Result<(), GenerationMutationFailure> {
        let source = self.identity_binding(source_file_id)?;
        let destination = self.identity_binding(destination_file_id)?;
        if source.kind != FileKind::Regular || destination.kind != FileKind::Regular {
            return Err(failed(
                GenerationMutationError::InconsistentState,
                self.work,
            ));
        }
        let source_payload = self.record(source_file_id)?.working.payload;
        let destination_payload = self.record(destination_file_id)?.working.payload;
        let receipt = apply_regular_clone_async(
            store,
            source_payload,
            source_offset,
            destination_payload,
            destination_offset,
            length,
            config,
            nested_budget(self.work, self.budget, self.allocations.live_bytes())?,
            cancellation,
        )
        .await
        .map_err(|failure| {
            nested_failure(
                self.work,
                *failure.work,
                self.allocations.live_bytes(),
                failure.error.into(),
            )
        })?;
        self.work = merge_nested(
            self.work,
            receipt.work,
            self.allocations.live_bytes(),
            self.budget,
        )?;
        self.record_mut(destination_file_id)?.working.payload = receipt.destination;
        Ok(())
    }

    fn create(
        &mut self,
        plan: &MutationPlan,
        operation: usize,
        record: FileRecord,
        order: u32,
    ) -> Result<(), GenerationMutationFailure> {
        let state = self.operation_paths[operation][0];
        if self.paths[state].binding.is_some() {
            return Err(failed(GenerationMutationError::AlreadyExists, self.work));
        }
        let directory_id = self.parent_directory(state)?;
        let record_state = self.record_mut(record.file_id)?;
        if record_state.base.is_some() || record_state.present {
            return Err(failed(
                GenerationMutationError::FileIdentityAlreadyExists,
                self.work,
            ));
        }
        record_state.working = record;
        record_state.present = true;
        self.paths[state].binding = Some(Binding {
            file_id: record.file_id,
            kind: record.kind,
        });
        let (name, retained_name_bytes) = self.clone_terminal_name(plan, operation, 0)?;
        self.push_edit(
            directory_id,
            order,
            retained_name_bytes,
            TreeMutation::Insert(TreeEntry {
                name,
                file_id: record.file_id,
                kind: record.kind,
            }),
        );
        Ok(())
    }

    fn remove(
        &mut self,
        plan: &MutationPlan,
        operation: usize,
        expected: Option<FileId>,
        order: u32,
    ) -> Result<(), GenerationMutationFailure> {
        let state = self.operation_paths[operation][0];
        let binding = self.paths[state]
            .binding
            .ok_or_else(|| failed(GenerationMutationError::MissingSource, self.work))?;
        if expected.is_some_and(|value| value != binding.file_id) {
            return Err(failed(
                GenerationMutationError::FileIdentityConflict,
                self.work,
            ));
        }
        let directory_id = self.parent_directory(state)?;
        let (name, retained_name_bytes) = self.clone_terminal_name(plan, operation, 0)?;
        self.push_edit(
            directory_id,
            order,
            retained_name_bytes,
            TreeMutation::Remove {
                name,
                expected_file_id: expected,
            },
        );
        self.paths[state].binding = None;
        self.drop_link(binding)
    }

    fn rename(
        &mut self,
        plan: &MutationPlan,
        operation: usize,
        source_path: &NamespacePath,
        destination_path: &NamespacePath,
        replace: bool,
        order: u32,
    ) -> Result<(), GenerationMutationFailure> {
        let source_state = self.operation_paths[operation][0];
        let destination_state = self.operation_paths[operation][1];
        let source = self.paths[source_state]
            .binding
            .ok_or_else(|| failed(GenerationMutationError::MissingSource, self.work))?;
        if source.kind == FileKind::Directory && destination_path.is_within(source_path) {
            return Err(failed(GenerationMutationError::DirectoryCycle, self.work));
        }
        let destination = self.paths[destination_state].binding;
        // POSIX and Windows both define renaming one hard-link alias over
        // another alias of the same file as a successful no-op. In
        // particular, it must not remove a name or decrement the shared link
        // count.
        if destination.is_some_and(|existing| existing.file_id == source.file_id) {
            return Ok(());
        }
        if destination.is_some() && !replace {
            return Err(failed(GenerationMutationError::AlreadyExists, self.work));
        }
        let source_parent = self.parent_directory(source_state)?;
        let destination_parent = self.parent_directory(destination_state)?;
        let (source_name, source_name_bytes) = self.clone_terminal_name(plan, operation, 0)?;
        self.push_edit(
            source_parent,
            order,
            source_name_bytes,
            TreeMutation::Remove {
                name: source_name,
                expected_file_id: Some(source.file_id),
            },
        );
        let (destination_name, destination_name_bytes) =
            self.clone_terminal_name(plan, operation, 1)?;
        let entry = TreeEntry {
            name: destination_name,
            file_id: source.file_id,
            kind: source.kind,
        };
        self.push_edit(
            destination_parent,
            order,
            destination_name_bytes,
            match destination {
                Some(existing) => TreeMutation::Replace {
                    entry,
                    expected_file_id: existing.file_id,
                },
                None => TreeMutation::Insert(entry),
            },
        );
        if let Some(existing) = destination {
            self.drop_link(existing)?;
        }
        self.paths[source_state].binding = None;
        self.paths[destination_state].binding = Some(source);
        Ok(())
    }

    fn link(
        &mut self,
        plan: &MutationPlan,
        operation: usize,
        order: u32,
    ) -> Result<(), GenerationMutationFailure> {
        let source = self.source_binding(operation)?;
        if source.kind == FileKind::Directory {
            return Err(failed(
                GenerationMutationError::UnsupportedHardLink,
                self.work,
            ));
        }
        let destination_state = self.operation_paths[operation][1];
        if self.paths[destination_state].binding.is_some() {
            return Err(failed(GenerationMutationError::AlreadyExists, self.work));
        }
        let destination_parent = self.parent_directory(destination_state)?;
        let current_links = self.record(source.file_id)?.working.link_count;
        let next_links = current_links
            .checked_add(1)
            .ok_or_else(|| failed(WorkError::Overflow.into(), self.work))?;
        self.record_mut(source.file_id)?.working.link_count = next_links;
        self.paths[destination_state].binding = Some(source);
        let (name, retained_name_bytes) = self.clone_terminal_name(plan, operation, 1)?;
        self.push_edit(
            destination_parent,
            order,
            retained_name_bytes,
            TreeMutation::Insert(TreeEntry {
                name,
                file_id: source.file_id,
                kind: source.kind,
            }),
        );
        Ok(())
    }

    fn source_binding(&self, operation: usize) -> Result<Binding, GenerationMutationFailure> {
        self.binding(operation, 0)
    }

    fn binding(
        &self,
        operation: usize,
        endpoint: usize,
    ) -> Result<Binding, GenerationMutationFailure> {
        self.paths[self.operation_paths[operation][endpoint]]
            .binding
            .ok_or_else(|| failed(GenerationMutationError::MissingSource, self.work))
    }

    fn parent_directory(&mut self, state: usize) -> Result<FileId, GenerationMutationFailure> {
        let file_id = match self.paths[state].parent_state {
            Some(parent_state) => {
                self.paths[parent_state]
                    .binding
                    .ok_or_else(|| failed(GenerationMutationError::MissingParent, self.work))?
                    .file_id
            }
            None if self.paths[state].base_parent_exact => self.paths[state]
                .base_parent
                .ok_or_else(|| failed(GenerationMutationError::MissingParent, self.work))?,
            None => return Err(failed(GenerationMutationError::MissingParent, self.work)),
        };
        let record = self.record(file_id)?;
        if record.working.kind != FileKind::Directory || !record.present {
            return Err(failed(GenerationMutationError::MissingParent, self.work));
        }
        Ok(file_id)
    }

    fn drop_link(&mut self, binding: Binding) -> Result<(), GenerationMutationFailure> {
        let current_links = self.record(binding.file_id)?.working.link_count;
        if current_links == 0 {
            return Err(failed(
                GenerationMutationError::InconsistentState,
                self.work,
            ));
        }
        let next_links = current_links - 1;
        let record = self.record_mut(binding.file_id)?;
        record.working.link_count = next_links;
        record.present = next_links != 0;
        if binding.kind == FileKind::Directory {
            self.removed_directories.push(binding.file_id);
        }
        Ok(())
    }

    fn clone_terminal_name(
        &mut self,
        plan: &MutationPlan,
        operation: usize,
        endpoint: usize,
    ) -> Result<(LogicalName, u64), GenerationMutationFailure> {
        let path = Self::operation_path(plan, operation, endpoint);
        let name = path
            .split_last()
            .map(|(_, name)| name)
            .ok_or_else(|| failed(GenerationMutationError::InconsistentState, self.work))?;
        let bytes = u64::try_from(name.as_bytes().len()).unwrap_or(u64::MAX);
        self.allocations
            .claim_bytes(bytes, 1, &mut self.work, self.budget)
            .map_err(|error| allocation_failure(error, self.work))?;
        let admitted = self.work;
        let mut copied = Vec::new();
        if copied.try_reserve_exact(name.as_bytes().len()).is_err() {
            self.allocations
                .release(bytes)
                .map_err(|error| allocation_failure(error, self.work))?;
            return Err(failed(GenerationMutationError::AllocationFailed, admitted));
        }
        let copied_work = self
            .work
            .checked_add(WorkCounters {
                bytes_copied: bytes,
                ..WorkCounters::default()
            })
            .map_err(|error| failed(error.into(), self.work))?;
        copied_work
            .verify(self.budget)
            .map_err(|error| failed(error.into(), self.work))?;
        copied.extend_from_slice(name.as_bytes());
        self.work = copied_work;
        let name = LogicalName::new(
            name.encoding(),
            copied,
            self.config.limits.maximum_component_bytes,
        )
        .map_err(|_| failed(GenerationMutationError::InconsistentState, self.work))?;
        Ok((name, bytes))
    }

    // Every caller obtains the operation from a planner-owned `PathState`,
    // which can only be constructed for a path-bearing mutation.
    #[allow(clippy::expect_used)]
    fn operation_path(plan: &MutationPlan, operation: usize, endpoint: usize) -> &NamespacePath {
        let paths = plan.operations()[operation]
            .paths()
            .expect("path operation must retain endpoints");
        paths[endpoint]
    }

    fn push_edit(
        &mut self,
        directory_id: FileId,
        order: u32,
        retained_name_bytes: u64,
        mutation: TreeMutation,
    ) {
        self.directory_edits.push(DirectoryEdit {
            directory_id,
            order,
            retained_name_bytes,
            mutation: Some(mutation),
        });
    }

    fn record(&mut self, file_id: FileId) -> Result<&RecordState, GenerationMutationFailure> {
        let index = self.record_index(file_id)?;
        Ok(&self.records[index])
    }

    fn identity_binding(&mut self, file_id: FileId) -> Result<Binding, GenerationMutationFailure> {
        let record = self.record(file_id)?;
        if !record.present || record.working.link_count == 0 {
            return Err(failed(GenerationMutationError::MissingSource, self.work));
        }
        Ok(Binding {
            file_id,
            kind: record.working.kind,
        })
    }

    fn record_mut(
        &mut self,
        file_id: FileId,
    ) -> Result<&mut RecordState, GenerationMutationFailure> {
        let index = self.record_index(file_id)?;
        Ok(&mut self.records[index])
    }

    fn record_index(&mut self, file_id: FileId) -> Result<usize, GenerationMutationFailure> {
        let comparisons = Cell::new(0_u64);
        let result = self.records.binary_search_by(|record| {
            comparisons.set(comparisons.get().saturating_add(1));
            record.working.file_id.cmp(&file_id)
        });
        charge_items(&mut self.work, comparisons.get(), self.budget)?;
        result.map_err(|_| failed(GenerationMutationError::InconsistentState, self.work))
    }

    async fn rewrite_directories<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        config: VolumeConfig,
        cancellation: &CancellationToken,
    ) -> Result<(), GenerationMutationFailure> {
        let comparisons = Cell::new(0_u64);
        self.directory_edits.sort_unstable_by(|left, right| {
            comparisons.set(comparisons.get().saturating_add(1));
            left.directory_id
                .cmp(&right.directory_id)
                .then_with(|| left.order.cmp(&right.order))
        });
        charge_items(&mut self.work, comparisons.get(), self.budget)?;
        let mut cursor = 0;
        while cursor < self.directory_edits.len() {
            cancellation
                .check()
                .map_err(|_| failed(path_cancelled(), self.work))?;
            let directory_id = self.directory_edits[cursor].directory_id;
            let start = cursor;
            cursor += 1;
            while cursor < self.directory_edits.len()
                && self.directory_edits[cursor].directory_id == directory_id
            {
                cursor += 1;
            }
            let mut mutations = reserve_exact::<TreeMutation>(
                cursor - start,
                &mut self.allocations,
                &mut self.work,
                self.budget,
            )?;
            let mutation_bytes = logical_vec_bytes(&mutations)?;
            let retained_name_bytes =
                self.directory_edits[start..cursor]
                    .iter()
                    .try_fold(0_u64, |total, edit| {
                        total
                            .checked_add(edit.retained_name_bytes)
                            .ok_or_else(|| failed(WorkError::Overflow.into(), self.work))
                    })?;
            for edit in &mut self.directory_edits[start..cursor] {
                mutations.push(edit.mutation.take().ok_or_else(|| {
                    failed(GenerationMutationError::InconsistentState, self.work)
                })?);
            }
            let directory = self.record(directory_id)?.working;
            let FilePayload::Directory { entries } = directory.payload else {
                return Err(failed(GenerationMutationError::MissingParent, self.work));
            };
            let receipt = apply_tree_mutations_async(
                store,
                entries,
                mutations,
                config.limits.maximum_mutations_per_batch,
                decode_limits(config),
                nested_budget(self.work, self.budget, self.allocations.live_bytes())?,
                cancellation,
            )
            .await
            .map_err(|failure| {
                nested_failure(
                    self.work,
                    *failure.work,
                    self.allocations.live_bytes(),
                    failure.error.into(),
                )
            })?;
            self.work = merge_nested(
                self.work,
                receipt.work,
                self.allocations.live_bytes(),
                self.budget,
            )?;
            self.allocations
                .release(
                    mutation_bytes
                        .checked_add(retained_name_bytes)
                        .ok_or_else(|| failed(WorkError::Overflow.into(), self.work))?,
                )
                .map_err(|error| allocation_failure(error, self.work))?;
            self.record_mut(directory_id)?.working.payload = FilePayload::Directory {
                entries: receipt.root,
            };
        }
        Ok(())
    }

    async fn verify_removed_directories<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        config: VolumeConfig,
        cancellation: &CancellationToken,
    ) -> Result<(), GenerationMutationFailure> {
        let comparisons = Cell::new(0_u64);
        self.removed_directories.sort_unstable_by(|left, right| {
            comparisons.set(comparisons.get().saturating_add(1));
            left.cmp(right)
        });
        charge_items(&mut self.work, comparisons.get(), self.budget)?;
        charge_items(
            &mut self.work,
            u64::try_from(self.removed_directories.len().saturating_sub(1)).unwrap_or(u64::MAX),
            self.budget,
        )?;
        self.removed_directories.dedup();
        for index in 0..self.removed_directories.len() {
            let file_id = self.removed_directories[index];
            let record = self.record(file_id)?.working;
            let FilePayload::Directory { entries } = record.payload else {
                return Err(failed(
                    GenerationMutationError::InconsistentState,
                    self.work,
                ));
            };
            let listing = list_tree_entries_async(
                store,
                entries,
                None,
                1,
                decode_limits(config),
                nested_budget(self.work, self.budget, self.allocations.live_bytes())?,
                cancellation,
            )
            .await
            .map_err(|failure| {
                nested_failure(
                    self.work,
                    *failure.work,
                    self.allocations.live_bytes(),
                    failure.error.into(),
                )
            })?;
            self.work = merge_nested(
                self.work,
                listing.work,
                self.allocations.live_bytes(),
                self.budget,
            )?;
            if !listing.entries.is_empty() {
                return Err(failed(
                    GenerationMutationError::DirectoryNotEmpty,
                    self.work,
                ));
            }
        }
        Ok(())
    }

    async fn rewrite_file_table<S: AsyncObjectStore>(
        mut self,
        store: &S,
        generation: &GenerationRoot,
        config: VolumeConfig,
        cancellation: &CancellationToken,
    ) -> Result<GenerationMutationReceipt, GenerationMutationFailure> {
        let mutation_count = self
            .records
            .iter()
            .filter(|record| {
                matches!(
                    (record.base, record.present),
                    (None, true) | (Some(_), false)
                ) || matches!(
                    (record.base, record.present),
                    (Some(expected), true) if expected != record.working
                )
            })
            .count();
        charge_items(
            &mut self.work,
            u64::try_from(self.records.len()).unwrap_or(u64::MAX),
            self.budget,
        )?;
        if mutation_count == 0 {
            let root = self.clone_generation_root(generation, generation.file_table)?;
            return Ok(GenerationMutationReceipt {
                root,
                work: self.work,
            });
        }
        let mut mutations = reserve_exact::<FileTableMutation>(
            mutation_count,
            &mut self.allocations,
            &mut self.work,
            self.budget,
        )?;
        let mutation_bytes = logical_vec_bytes(&mutations)?;
        for record in &self.records {
            match (record.base, record.present) {
                (None, true) => mutations.push(FileTableMutation::Insert(record.working)),
                (Some(expected), false) => mutations.push(FileTableMutation::Remove {
                    file_id: expected.file_id,
                    expected: Some(expected),
                }),
                (Some(expected), true) if expected != record.working => {
                    mutations.push(FileTableMutation::Replace {
                        expected,
                        replacement: record.working,
                    });
                }
                (None, false) | (Some(_), true) => {}
            }
        }
        let receipt = apply_file_table_mutations_async(
            store,
            generation.file_table,
            mutations,
            u32::try_from(mutation_count)
                .map_err(|_| failed(WorkError::Overflow.into(), self.work))?,
            decode_limits(config),
            nested_budget(self.work, self.budget, self.allocations.live_bytes())?,
            cancellation,
        )
        .await
        .map_err(|failure| {
            nested_failure(
                self.work,
                *failure.work,
                self.allocations.live_bytes(),
                failure.error.into(),
            )
        })?;
        self.work = merge_nested(
            self.work,
            receipt.work,
            self.allocations.live_bytes(),
            self.budget,
        )?;
        self.allocations
            .release(mutation_bytes)
            .map_err(|error| allocation_failure(error, self.work))?;
        let root = self.clone_generation_root(generation, receipt.root)?;
        Ok(GenerationMutationReceipt {
            root,
            work: self.work,
        })
    }

    fn clone_generation_root(
        &mut self,
        generation: &GenerationRoot,
        file_table: ObjectId,
    ) -> Result<GenerationRoot, GenerationMutationFailure> {
        let mut parents = reserve_exact::<GenerationId>(
            generation.parents.len(),
            &mut self.allocations,
            &mut self.work,
            self.budget,
        )?;
        let parent_bytes = logical_vec_bytes(&parents)?;
        let prospective = self
            .work
            .checked_add(WorkCounters {
                bytes_copied: parent_bytes,
                ..WorkCounters::default()
            })
            .map_err(|error| failed(error.into(), self.work))?;
        prospective
            .verify(self.budget)
            .map_err(|error| failed(error.into(), self.work))?;
        parents.extend_from_slice(&generation.parents);
        self.work = prospective;
        Ok(GenerationRoot {
            volume_id: generation.volume_id,
            root_file_id: generation.root_file_id,
            file_table,
            parents,
            required_features: generation.required_features,
        })
    }
}

fn operation_uses_one_path(mutation: &Mutation) -> bool {
    mutation
        .paths()
        .is_some_and(|[first, second]| first == second)
}

fn preflight_generation_mutations(
    operations: &[Mutation],
    config: VolumeConfig,
) -> Result<(), GenerationMutationFailure> {
    config
        .validate()
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    let mut endpoints = 0_u32;
    for operation in operations {
        if let Some([first, second]) = operation.paths() {
            endpoints = endpoints
                .checked_add(if first == second { 1 } else { 2 })
                .ok_or_else(|| {
                    OperationFailure::before_work(GenerationMutationError::TooManyPaths)
                })?;
        }
        match operation {
            Mutation::Create { record, .. } => {
                if !record.kind.is_supported_by_profile(config.profile) {
                    return Err(OperationFailure::before_work(
                        GenerationMutationError::UnsupportedFileKind,
                    ));
                }
                if record.link_count != 1 {
                    return Err(OperationFailure::before_work(
                        GenerationMutationError::InvalidInitialLinkCount,
                    ));
                }
                if record.kind == FileKind::SymbolicLink && !config.symbolic_links {
                    return Err(OperationFailure::before_work(
                        GenerationMutationError::UnsupportedSymbolicLink,
                    ));
                }
            }
            Mutation::Link { .. } if !config.hard_links => {
                return Err(OperationFailure::before_work(
                    GenerationMutationError::UnsupportedHardLink,
                ));
            }
            Mutation::ZeroRange { .. }
            | Mutation::Preallocate { .. }
            | Mutation::CloneRange { .. }
            | Mutation::File {
                mutation: FileMutation::ZeroRange { .. } | FileMutation::Preallocate { .. },
                ..
            }
            | Mutation::CloneFileRange { .. }
                if !config.sparse_files =>
            {
                return Err(OperationFailure::before_work(
                    GenerationMutationError::UnsupportedSparseMutation,
                ));
            }
            Mutation::Remove { .. }
            | Mutation::Rename { .. }
            | Mutation::Link { .. }
            | Mutation::SetMetadata { .. }
            | Mutation::Write { .. }
            | Mutation::ValidateRegular { .. }
            | Mutation::Resize { .. }
            | Mutation::ZeroRange { .. }
            | Mutation::Preallocate { .. }
            | Mutation::CloneRange { .. }
            | Mutation::File { .. }
            | Mutation::CloneFileRange { .. } => {}
        }
    }
    if endpoints > config.limits.maximum_paths_per_batch {
        return Err(OperationFailure::before_work(
            GenerationMutationError::TooManyPaths,
        ));
    }
    Ok(())
}

// `PathState` is private and exists only for planner-validated path uses.
#[allow(clippy::expect_used)]
fn state_path<'a>(plan: &'a MutationPlan, state: &PathState) -> &'a NamespacePath {
    plan.operations()[state.operation]
        .paths()
        .expect("path state must reference a path operation")[state.endpoint]
}

fn find_path_state(
    plan: &MutationPlan,
    states: &[PathState],
    components: &[LogicalName],
    comparisons: &Cell<u64>,
) -> Option<usize> {
    states
        .binary_search_by(|state| {
            comparisons.set(comparisons.get().saturating_add(1));
            state_path(plan, state).components().cmp(components)
        })
        .ok()
}

fn reserve_exact<T>(
    count: usize,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<Vec<T>, GenerationMutationFailure> {
    let bytes = count
        .checked_mul(size_of::<T>())
        .map(crate::foundation::usize_to_u64)
        .ok_or_else(|| failed(WorkError::Overflow.into(), *work))?;
    allocations
        .claim_bytes(bytes, u64::from(count != 0), work, budget)
        .map_err(|error| allocation_failure(error, *work))?;
    let admitted = *work;
    let mut values = Vec::new();
    if values.try_reserve_exact(count).is_err() {
        allocations
            .release(bytes)
            .map_err(|error| allocation_failure(error, *work))?;
        return Err(failed(GenerationMutationError::AllocationFailed, admitted));
    }
    Ok(values)
}

fn logical_vec_bytes<T>(values: &Vec<T>) -> Result<u64, GenerationMutationFailure> {
    values
        .capacity()
        .checked_mul(size_of::<T>())
        .map(crate::foundation::usize_to_u64)
        .ok_or_else(|| {
            OperationFailure::before_work(GenerationMutationError::Work(WorkError::Overflow))
        })
}

fn decode_limits(config: VolumeConfig) -> DecodeLimits {
    DecodeLimits {
        maximum_object_bytes: config.limits.maximum_object_bytes,
        maximum_name_bytes: config.limits.maximum_component_bytes,
        maximum_page_items: config.limits.maximum_directory_page_entries,
        maximum_page_bytes: u32::try_from(config.limits.maximum_object_bytes).unwrap_or(u32::MAX),
        maximum_page_height: config.limits.maximum_page_height,
        maximum_visited_pages: u32::try_from(config.limits.maximum_objects_per_generation)
            .unwrap_or(u32::MAX),
    }
}

fn nested_budget(
    work: WorkCounters,
    budget: WorkBudget,
    live_bytes: u64,
) -> Result<WorkBudget, GenerationMutationFailure> {
    let mut remaining = work
        .remaining(budget)
        .map_err(|error| failed(error.into(), work))?;
    remaining.peak_allocation_bytes = remaining
        .peak_allocation_bytes
        .checked_sub(live_bytes)
        .ok_or_else(|| failed(WorkError::Overflow.into(), work))?;
    Ok(remaining)
}

fn merge_nested(
    prior: WorkCounters,
    mut nested: WorkCounters,
    live_bytes: u64,
    budget: WorkBudget,
) -> Result<WorkCounters, GenerationMutationFailure> {
    let peak = live_bytes
        .checked_add(nested.peak_allocation_bytes)
        .ok_or_else(|| failed(WorkError::Overflow.into(), prior))?;
    nested.peak_allocation_bytes = 0;
    let mut work = prior
        .checked_add(nested)
        .map_err(|error| failed(error.into(), prior))?;
    work.peak_allocation_bytes = work.peak_allocation_bytes.max(peak);
    work.verify(budget)
        .map_err(|error| failed(error.into(), work))?;
    Ok(work)
}

fn nested_failure(
    prior: WorkCounters,
    mut nested: WorkCounters,
    live_bytes: u64,
    error: GenerationMutationError,
) -> GenerationMutationFailure {
    let Some(peak) = live_bytes.checked_add(nested.peak_allocation_bytes) else {
        return failed(WorkError::Overflow.into(), prior);
    };
    nested.peak_allocation_bytes = 0;
    let Ok(mut work) = prior.checked_add(nested) else {
        return failed(WorkError::Overflow.into(), prior);
    };
    work.peak_allocation_bytes = work.peak_allocation_bytes.max(peak);
    failed(error, work)
}

fn charge_items(
    work: &mut WorkCounters,
    count: u64,
    budget: WorkBudget,
) -> Result<(), GenerationMutationFailure> {
    let prospective = work
        .checked_add(WorkCounters {
            items_examined: count,
            ..WorkCounters::default()
        })
        .map_err(|error| failed(error.into(), *work))?;
    prospective
        .verify(budget)
        .map_err(|error| failed(error.into(), *work))?;
    *work = prospective;
    Ok(())
}

fn allocation_failure(error: AllocationError, work: WorkCounters) -> GenerationMutationFailure {
    match error {
        AllocationError::Work(error) => failed(error.into(), work),
        AllocationError::Overflow | AllocationError::ReleaseInvariant => {
            failed(WorkError::Overflow.into(), work)
        }
        AllocationError::InvalidCapacity
        | AllocationError::CapacityExceeded
        | AllocationError::AllocationFailed => {
            failed(GenerationMutationError::AllocationFailed, work)
        }
    }
}

fn path_cancelled() -> GenerationMutationError {
    GenerationMutationError::Path(PathLookupError::Tree(super::TreeReadError::Cancelled))
}

fn failed(error: GenerationMutationError, work: WorkCounters) -> GenerationMutationFailure {
    OperationFailure::new(error, work)
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/generation_mutation.rs"]
mod tests;
