//! One-volume generation transaction composition over sparse persistent kernels. Ordinary and original-native transactions share this sparse engine. Producers run outside Root loans and keep explicit preadmitted custody for actual data.

use super::allocation::{KernelAccounting, KernelOwnedAllocation};
#[cfg(all(test, feature = "memory"))]
use super::allocation::{AllocationError, AllocationLedger};
use super::regular_mutation::{RegularMutation, RegularMutationError};
use super::{
    DecodeLimits, DirectoryReadError, FileKind, FileMutation, FilePayload, FileRecord,
    FileRecordReadError, FileTableMutation, FileTableMutationError, GenerationRoot, LogicalName,
    MetadataField, Mutation, MutationPlan, MutationPlanError, NamespacePath, PathBatchEntry,
    PathLookupError, TreeEntry, TreeMutation, TreeMutationError,
};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::foundation::{FileId, GenerationId, usize_to_u64};
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
    replacement: bool,
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

struct InitialLookups {
    paths: Owned<Vec<PathBatchEntry>>,
    identities: Owned<Vec<Option<FileRecord>>>,
}

/// Applies one ordered namespace, metadata, and content batch to a generation.
pub async fn apply_generation_mutations<S: AsyncObjectStore>(
    store: &S,
    generation: &GenerationRoot,
    operations: Vec<Mutation>,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<GenerationMutationReceipt, GenerationMutationFailure> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    apply_generation_mutations_retaining_with_accounting(
        store, generation, operations, config, &mut accounting, cancellation,
    ).await.map(|(receipt, _, _)| receipt)
        .map_err(|error| OperationFailure::new(generation_error(error), accounting.spent()))
}

pub(crate) async fn apply_generation_mutations_retaining_with_accounting<S: AsyncObjectStore>(
    store: &S,
    generation: &GenerationRoot,
    operations: Vec<Mutation>,
    config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(GenerationMutationReceipt, Vec<Mutation>, KernelOwnedAllocation), crate::FsError> {
    accounting.check()?;
    preflight_generation_mutations(&operations, config)?;
    let (plan, plan_allocation) = MutationPlan::compile_with_accounting(operations, config.limits, accounting)?;
    let plan = Owned { value: plan, allocation: plan_allocation };
    let created_ids = verify_created_identities(store, generation, &plan.value, config, accounting, cancellation).await?;
    let lookups = load_initial_lookups(store, generation, &plan.value, &created_ids.value, config, accounting, cancellation).await?;
    let created_bytes = logical_vec_bytes(&created_ids.value)?;
    drop_owned(created_ids, created_bytes, accounting)?;
    let mut allocation = accounting.claim_owned(0, 0)?;
    let state = TransactionState::new(
        &plan.value, &lookups, config,
        &mut MutationLeaf { accounting, allocation: &mut allocation },
    )?;
    let path_bytes = logical_vec_bytes(&lookups.paths.value)?;
    let identity_bytes = logical_vec_bytes(&lookups.identities.value)?;
    drop_owned(lookups.paths, path_bytes, accounting)?;
    drop_owned(lookups.identities, identity_bytes, accounting)?;
    let mut state = Owned { value: state, allocation };
    state.simulate(store, &plan.value, config, accounting, cancellation).await?;
    let Owned { value: plan, allocation: plan_allocation } = plan;
    let plan_bytes = plan.retained_allocation_bytes();
    let operations = plan.into_operations();
    release_allocation(plan_allocation, plan_bytes, accounting)?;
    state.rewrite_directories(store, config, accounting, cancellation).await?;
    state.verify_removed_directories(store, config, accounting, cancellation).await?;
    let (root, allocation) = state.rewrite_file_table(store, generation, config, accounting, cancellation).await?;
    let state_bytes = state.value.retained_bytes()?;
    drop_owned(state, state_bytes, accounting)?;
    Ok((GenerationMutationReceipt { root, work: accounting.spent() }, operations, allocation))
}

struct Owned<T> {
    value: T,
    allocation: KernelOwnedAllocation,
}

fn accounting_error(error: crate::FsError) -> crate::FsError {
    match error {
        crate::FsError::Work(error) => GenerationMutationError::Work(error).into(),
        crate::FsError::PendingMutationAllocationFailed | crate::FsError::HandleAllocationFailed =>
            GenerationMutationError::AllocationFailed.into(),
        error => error,
    }
}

fn account_items(accounting: &mut KernelAccounting<'_>, count: u64) -> Result<(), crate::FsError> {
    accounting.charge(WorkCounters { items_examined: count, ..WorkCounters::default() })
        .map_err(accounting_error)
}

fn reserve_owned<T>(count: usize, accounting: &mut KernelAccounting<'_>) -> Result<Owned<Vec<T>>, crate::FsError> {
    let requested = count.checked_mul(size_of::<T>()).map(usize_to_u64).ok_or(WorkError::Overflow)?;
    let allocation = accounting.claim_owned(requested, u64::from(count != 0)).map_err(accounting_error)?;
    let mut owned = Owned { value: Vec::new(), allocation };
    if let Err(cause) = owned.value.try_reserve_exact(count) {
        drop_owned(owned, requested, accounting)?;
        #[cfg(not(target_arch = "wasm32"))]
        if is_original(accounting) {
            return Err(crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::AllocationFailed(cause)));
        }
        let _ = cause;
        return Err(GenerationMutationError::AllocationFailed.into());
    }
    let actual = logical_vec_bytes(&owned.value)?;
    if let Err(error) = accounting.reconcile_owned(&mut owned.allocation, requested, actual) {
        drop_owned(owned, actual, accounting)?;
        return Err(accounting_error(error));
    }
    Ok(owned)
}

fn drop_owned<T>(owned: Owned<T>, bytes: u64, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
    let Owned { value, allocation } = owned;
    drop(value);
    release_allocation(allocation, bytes, accounting)
}

fn split_allocation(allocation: &mut KernelOwnedAllocation, bytes: u64) -> Result<KernelOwnedAllocation, crate::FsError> {
    match allocation {
        KernelOwnedAllocation::Standalone => Ok(KernelOwnedAllocation::Standalone),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(allocation) => allocation.split_off(bytes)
            .map(KernelOwnedAllocation::NativeOriginal).map_err(Into::into),
    }
}

enum RegularAction {
    Mutate { id: FileId, payload: FilePayload, mutation: RegularMutation },
    Clone {
        destination: FileId, source_payload: FilePayload, source_offset: u64,
        destination_payload: FilePayload, destination_offset: u64, length: u64,
    },
}

impl Owned<TransactionState> {
    fn leaf<T>(
        &mut self, accounting: &mut KernelAccounting<'_>,
        operation: impl FnOnce(&mut TransactionState, &mut MutationLeaf<'_, '_>) -> Result<T, crate::FsError>,
    ) -> Result<T, crate::FsError> {
        accounting.check()?;
        let mut leaf = MutationLeaf { accounting, allocation: &mut self.allocation };
        let result = operation(&mut self.value, &mut leaf);
        if result.is_err() { self.value.clear(); }
        result
    }

    async fn simulate<S: AsyncObjectStore>(
        &mut self, store: &S, plan: &MutationPlan, config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(), crate::FsError> {
        for (ordinal, operation) in plan.operations().iter().enumerate() {
            let Some(action) = self.leaf(accounting, |state, leaf| state.prepare(leaf, plan, ordinal, operation))? else { continue; };
            let (id, payload) = match action {
                RegularAction::Mutate { id, payload, mutation } => {
                    let receipt = super::regular_mutation::apply_regular_mutation_with_accounting(
                        store, payload, mutation, config, accounting, cancellation,
                    ).await.map_err(|error| {
                        if is_original(accounting) { error } else {
                            GenerationMutationError::Regular(super::regular_mutation::project_regular_error(error)).into()
                        }
                    })?;
                    (id, receipt.payload)
                }
                RegularAction::Clone { destination, source_payload, source_offset, destination_payload, destination_offset, length } => {
                    let receipt = super::regular_mutation::apply_regular_clone_with_accounting(
                        store, source_payload, source_offset, destination_payload, destination_offset, length,
                        config, accounting, cancellation,
                    ).await.map_err(|error| {
                        if is_original(accounting) { error } else {
                            GenerationMutationError::Regular(super::regular_mutation::project_regular_error(error)).into()
                        }
                    })?;
                    (destination, receipt.destination)
                }
            };
            self.leaf(accounting, |state, leaf| { state.record_mut(leaf, id)?.working.payload = payload; Ok(()) })?;
        }
        Ok(())
    }

    async fn rewrite_directories<S: AsyncObjectStore>(
        &mut self, store: &S, config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(), crate::FsError> {
        self.leaf(accounting, TransactionState::sort_directory_edits)?;
        let mut cursor = 0;
        while cursor < self.value.directory_edits.len() {
            let (id, root, mutations, bytes, end) = self.leaf(accounting, |state, leaf| state.take_directory_edits(leaf, cursor))?;
            let allocation = split_allocation(&mut self.allocation, bytes)?;
            let mutations = Owned { value: mutations, allocation };
            let receipt = super::tree_mutation::apply_tree_mutations_with_accounting(
                store, root, &mutations.value, config.limits.maximum_mutations_per_batch,
                DecodeLimits::for_volume(config), accounting, cancellation,
            ).await.map_err(|error| {
                if is_original(accounting) { error } else {
                    GenerationMutationError::Tree(super::tree_mutation::project_tree_error(error)).into()
                }
            })?;
            drop_owned(mutations, bytes, accounting)?;
            self.leaf(accounting, |state, leaf| {
                state.record_mut(leaf, id)?.working.payload = FilePayload::Directory { entries: receipt.value }; Ok(())
            })?;
            cursor = end;
        }
        Ok(())
    }

    async fn verify_removed_directories<S: AsyncObjectStore>(
        &mut self, store: &S, config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(), crate::FsError> {
        self.leaf(accounting, TransactionState::sort_removed_directories)?;
        for index in 0..self.value.removed_directories.len() {
            let entries = self.leaf(accounting, |state, leaf| {
                let id = *state.removed_directories.get(index).ok_or(GenerationMutationError::InconsistentState)?;
                let FilePayload::Directory { entries } = state.record(leaf, id)?.working.payload else {
                    return Err(GenerationMutationError::InconsistentState.into());
                };
                Ok(entries)
            })?;
            let (listing, allocation) = super::list::list_tree_entries_async_with_accounting(
                store, entries, None, 1, DecodeLimits::for_volume(config), accounting, cancellation.clone(), None,
            ).await.map_err(|error| {
                if is_original(accounting) { error } else {
                    GenerationMutationError::Directory(super::persistent_pagination::project_failure(error)).into()
                }
            })?;
            let listing = Owned { value: listing, allocation };
            if !listing.value.entries.is_empty() { return Err(GenerationMutationError::DirectoryNotEmpty.into()); }
            let bytes = logical_vec_bytes(&listing.value.entries)?;
            drop_owned(listing, bytes, accounting)?;
        }
        Ok(())
    }

    async fn rewrite_file_table<S: AsyncObjectStore>(
        &mut self, store: &S, generation: &GenerationRoot, config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(GenerationRoot, KernelOwnedAllocation), crate::FsError> {
        let mutations = self.leaf(accounting, TransactionState::file_table_edits)?;
        let bytes = logical_vec_bytes(&mutations)?;
        let allocation = split_allocation(&mut self.allocation, bytes)?;
        let mutations = Owned { value: mutations, allocation };
        let root = if mutations.value.is_empty() { generation.file_table } else {
            super::file_table_mutation::apply_file_table_mutations_with_accounting(
                store, generation.file_table, &mutations.value,
                u32::try_from(mutations.value.len()).map_err(|_| WorkError::Overflow)?,
                DecodeLimits::for_volume(config), accounting, cancellation,
            ).await.map_err(|error| {
                if is_original(accounting) { error } else {
                    GenerationMutationError::FileTable(super::file_table_mutation::project_file_table_error(error)).into()
                }
            })?.value
        };
        drop_owned(mutations, bytes, accounting)?;
        let root = self.leaf(accounting, |state, leaf| state.clone_generation_root(leaf, generation, root))?;
        let bytes = logical_vec_bytes(&root.parents)?;
        let allocation = split_allocation(&mut self.allocation, bytes)?;
        Ok((root, allocation))
    }
}

fn is_original(accounting: &KernelAccounting<'_>) -> bool {
    #[cfg(not(target_arch = "wasm32"))]
    { accounting.native_work().is_some() }
    #[cfg(target_arch = "wasm32")]
    { let _ = accounting; false }
}

fn release_allocation(
    mut allocation: KernelOwnedAllocation, bytes: u64, accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    accounting.release_owned(&mut allocation, bytes).map_err(accounting_error)
}

fn generation_error(error: crate::FsError) -> GenerationMutationError {
    match error {
        crate::FsError::Mutation(error) => error,
        crate::FsError::Work(error) => error.into(),
        crate::FsError::Path(error) => error.into(),
        crate::FsError::FileRecord(error) => error.into(),
        crate::FsError::Directory(error) => error.into(),
        crate::FsError::Cancelled(_) => path_cancelled(),
        crate::FsError::Object(error) => GenerationMutationError::FileRecord(FileRecordReadError::Storage(error)),
        crate::FsError::Decode(error) => GenerationMutationError::FileRecord(FileRecordReadError::Decode(error)),
        crate::FsError::MergeTree(error) => error.into(),
        crate::FsError::MergeFileTable(error) => error.into(),
        crate::FsError::DetachedMutation(error) => error.into(),
        error @ (crate::FsError::BlobRead(_) | crate::FsError::FileRead(_)) =>
            GenerationMutationError::Regular(super::regular_mutation::project_regular_error(error)),
        crate::FsError::HandleAllocationFailed => GenerationMutationError::AllocationFailed,
        crate::FsError::Config(error) => error.into(),
        _ => unreachable!("standalone generation leaves return typed generation/read/work errors"),
    }
}

async fn verify_created_identities<S: AsyncObjectStore>(
    store: &S, generation: &GenerationRoot, plan: &MutationPlan, config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<Owned<Vec<FileId>>, crate::FsError> {
    let original = is_original(accounting);
    let count = plan.operations().iter().filter(|operation| matches!(operation, Mutation::Create { .. })).count();
    let mut ids = reserve_owned::<FileId>(count, accounting)?;
    ids.value.extend(plan.operations().iter().filter_map(|operation| {
        if let Mutation::Create { record, .. } = operation { Some(record.file_id) } else { None }
    }));
    let comparisons = Cell::new(0_u64);
    ids.value.sort_unstable_by(|left, right| {
        comparisons.set(comparisons.get().saturating_add(1));
        left.cmp(right)
    });
    account_items(accounting, comparisons.get())?;
    let mut examined = 0;
    for pair in ids.value.windows(2) {
        examined += 1;
        if let [left, right] = pair && left == right {
            account_items(accounting, examined)?;
            return Err(GenerationMutationError::FileIdentityAlreadyExists.into());
        }
    }
    account_items(accounting, examined)?;
    if !ids.value.is_empty() {
        let (records, allocation) = super::file_table::lookup_file_records_async_with_accounting(
            store, generation.file_table, &ids.value, config.limits.maximum_mutations_per_batch,
            DecodeLimits::for_volume(config), accounting, cancellation, None,
        ).await.map_err(|error| {
            if original { error } else {
                GenerationMutationError::FileRecord(super::file_table::map_batch_error(super::persistent_point::project_failure(error))).into()
            }
        })?;
        let records = Owned { value: records, allocation };
        if records.value.iter().any(Option::is_some) {
            return Err(GenerationMutationError::FileIdentityAlreadyExists.into());
        }
        let bytes = logical_vec_bytes(&records.value)?;
        drop_owned(records, bytes, accounting)?;
    }
    Ok(ids)
}

async fn load_initial_lookups<S: AsyncObjectStore>(
    store: &S, generation: &GenerationRoot, plan: &MutationPlan, created_ids: &[FileId],
    config: VolumeConfig, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<InitialLookups, crate::FsError> {
    let original = is_original(accounting);
    let mut refs = reserve_owned::<&NamespacePath>(plan.ordered_operation_paths().len(), accounting)?;
    refs.value.extend(plan.ordered_paths());
    let (paths, path_allocation) = if refs.value.is_empty() {
        (Vec::new(), accounting.claim_owned(0, 0)?)
    } else {
        let result = super::path_access::lookup_path_refs_async_with_accounting(
            store, generation, &refs.value, config, accounting, cancellation, None,
        ).await.map_err(|error| {
            if original { error } else { GenerationMutationError::Path(super::path_access::path_error(error)).into() }
        })?;
        let (lookup, allocation) = result.into_parts();
        (lookup.entries, allocation)
    };
    let paths = Owned { value: paths, allocation: path_allocation };
    let ref_bytes = logical_vec_bytes(&refs.value)?;
    drop_owned(refs, ref_bytes, accounting)?;
    let maximum = plan.operations().iter().try_fold(0_usize, |total, operation| {
        total.checked_add(match operation {
            Mutation::File { .. } | Mutation::Restore { .. } => 1,
            Mutation::CloneFileRange { .. } => 2,
            _ => 0,
        }).ok_or(WorkError::Overflow)
    })?;
    let mut ids = reserve_owned::<FileId>(maximum, accounting)?;
    for operation in plan.operations() {
        match operation {
            Mutation::File { file_id, .. } => ids.value.push(*file_id),
            Mutation::Restore { record, .. } => ids.value.push(record.file_id),
            Mutation::CloneFileRange { source_file_id, destination_file_id, .. } => {
                ids.value.push(*source_file_id); ids.value.push(*destination_file_id);
            }
            _ => {}
        }
    }
    let comparisons = Cell::new(0_u64);
    ids.value.sort_unstable_by(|left, right| {
        comparisons.set(comparisons.get().saturating_add(1)); left.cmp(right)
    });
    account_items(accounting, comparisons.get())?;
    account_items(accounting, usize_to_u64(ids.value.len()))?;
    ids.value.dedup();
    let (identities, allocation) = if ids.value.is_empty() {
        (Vec::new(), accounting.claim_owned(0, 0)?)
    } else {
        super::file_table::lookup_file_records_async_with_accounting(
            store, generation.file_table, &ids.value,
            u32::try_from(ids.value.len()).map_err(|_| WorkError::Overflow)?,
            DecodeLimits::for_volume(config), accounting, cancellation, None,
        ).await.map_err(|error| {
            if original { error } else {
                GenerationMutationError::FileRecord(super::file_table::map_batch_error(super::persistent_point::project_failure(error))).into()
            }
        })?
    };
    let identities = Owned { value: identities, allocation };
    for (file_id, record) in ids.value.iter().zip(&identities.value) {
        if record.is_none() {
            let comparisons = Cell::new(0_u64);
            let created = created_ids.binary_search_by(|candidate| {
                comparisons.set(comparisons.get().saturating_add(1)); candidate.cmp(file_id)
            });
            account_items(accounting, comparisons.get())?;
            let mut restored = false;
            let mut examined = 0;
            for operation in plan.operations() {
                examined += 1;
                if matches!(operation, Mutation::Restore { record, .. } if record.file_id == *file_id) {
                    restored = true; break;
                }
            }
            account_items(accounting, examined)?;
            if created.is_err() && !restored { return Err(GenerationMutationError::MissingSource.into()); }
        }
    }
    let id_bytes = logical_vec_bytes(&ids.value)?;
    drop_owned(ids, id_bytes, accounting)?;
    Ok(InitialLookups { paths, identities })
}

struct TransactionState {
    paths: Vec<PathState>,
    operation_paths: Vec<[usize; 2]>,
    records: Vec<RecordState>,
    directory_edits: Vec<DirectoryEdit>,
    removed_directories: Vec<FileId>,
    config: VolumeConfig,
}

struct MutationLeaf<'a, 'work> {
    accounting: &'a mut KernelAccounting<'work>,
    allocation: &'a mut KernelOwnedAllocation,
}

impl MutationLeaf<'_, '_> {
    fn reserve<T>(&mut self, count: usize) -> Result<Vec<T>, crate::FsError> {
        let Owned { value, mut allocation } = reserve_owned::<T>(count, self.accounting)?;
        if let Err(error) = self.allocation.absorb(&mut allocation) {
            drop(value);
            return Err(error);
        }
        Ok(value)
    }

    fn items(&mut self, count: u64) -> Result<(), crate::FsError> {
        account_items(self.accounting, count)
    }

    fn release(&mut self, bytes: u64) -> Result<(), crate::FsError> {
        let allocation = split_allocation(self.allocation, bytes)?;
        release_allocation(allocation, bytes, self.accounting)
    }
}

impl TransactionState {
    #[allow(
        clippy::too_many_lines,
        reason = "builds the whole transaction state from one validated plan"
    )]
    fn new(
        plan: &MutationPlan,
        lookups: &InitialLookups,
        config: VolumeConfig,
        leaf: &mut MutationLeaf<'_, '_>,
    ) -> Result<Self, crate::FsError> {
        let lookup = &lookups.paths.value;
        let identity_records = &lookups.identities.value;
        let operation_count = plan.operations().len();
        let mut operation_paths =
            leaf.reserve::<[usize; 2]>(operation_count)?;
        operation_paths.resize(operation_count, [usize::MAX; 2]);
        let mut paths =
            leaf.reserve::<PathState>(lookup.len())?;
        if plan.ordered_operation_paths().len() != lookup.len() {
            return Err(failed(GenerationMutationError::InconsistentState));
        }
        let mut seeds = leaf.reserve::<RecordSeed>(
            lookup
                .len()
                .checked_mul(2)
                .and_then(|value| value.checked_add(operation_count))
                .and_then(|value| value.checked_add(identity_records.len()))
                .ok_or_else(|| failed(WorkError::Overflow.into()))?,
        )?;
        let mut previous: Option<&NamespacePath> = None;
        let mut previous_entry: Option<PathBatchEntry> = None;
        let mut current_state = usize::MAX;
        for ((operation, endpoint, path), entry) in
            plan.ordered_operation_paths().zip(lookup.iter())
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
            } else if previous_entry != Some(*entry) {
                return Err(failed(GenerationMutationError::InconsistentState));
            }
            previous_entry = Some(*entry);
            #[allow(
                clippy::indexing_slicing,
                reason = "`operation` and `endpoint` come from `plan.ordered_operation_paths()`, \
                          whose (operation, endpoint) pairs are built in \
                          `MutationPlan::compile` from `operations.iter().enumerate()` (so \
                          `operation < plan.operations().len()`) with `endpoint` only ever 0 or \
                          1; `operation_paths` above is reserved and `resize`d to exactly \
                          `operation_count == plan.operations().len()` entries of `[usize; 2]`, \
                          so both index operations are in-bounds by construction"
            )]
            {
                operation_paths[operation][endpoint] = current_state;
                if endpoint == 0 && operation_uses_one_path(&plan.operations()[operation]) {
                    operation_paths[operation][1] = current_state;
                }
            }
            if let Some(record) = entry.record {
                seeds.push(RecordSeed {
                    record,
                    created: false,
                    replacement: false,
                });
            }
            if let Some(record) = entry.parent {
                seeds.push(RecordSeed {
                    record,
                    created: false,
                    replacement: false,
                });
            }
        }
        for operation in plan.operations() {
            if let Mutation::Create { record, .. } = operation {
                seeds.push(RecordSeed {
                    record: *record,
                    created: true,
                    replacement: false,
                });
            }
            if let Mutation::Restore { record, .. } = operation {
                seeds.push(RecordSeed {
                    record: *record,
                    created: false,
                    replacement: true,
                });
            }
        }
        for record in identity_records.iter().flatten() {
            seeds.push(RecordSeed {
                record: *record,
                created: false,
                replacement: false,
            });
        }
        let comparisons = Cell::new(0_u64);
        seeds.sort_unstable_by(|left, right| {
            comparisons.set(comparisons.get().saturating_add(1));
            left.record.file_id.cmp(&right.record.file_id)
        });
        leaf.items(comparisons.get())?;
        let mut records =
            leaf.reserve::<RecordState>(seeds.len())?;
        for group in seeds.chunk_by(|left, right| left.record.file_id == right.record.file_id) {
            let mut base = None;
            let mut created = None;
            let mut replacement = None;
            let mut group_examined = 0_u64;
            for seed in group {
                group_examined = group_examined
                    .checked_add(1)
                    .ok_or_else(|| failed(WorkError::Overflow.into()))?;
                if seed.created {
                    if created.replace(seed.record).is_some() {
                        leaf.items(group_examined)?;
                        return Err(failed(GenerationMutationError::FileIdentityAlreadyExists));
                    }
                } else if seed.replacement {
                    if replacement.is_some_and(|value| value != seed.record) {
                        leaf.items(group_examined)?;
                        return Err(failed(GenerationMutationError::InconsistentState));
                    }
                    replacement = Some(seed.record);
                } else if base.is_some_and(|value| value != seed.record) {
                    leaf.items(group_examined)?;
                    return Err(failed(GenerationMutationError::FileIdentityAlreadyExists));
                } else {
                    base = Some(seed.record);
                }
            }
            leaf.items(group_examined)?;
            if base.is_some() && created.is_some() {
                return Err(failed(GenerationMutationError::FileIdentityAlreadyExists));
            }
            let working = base
                .or(created)
                .or(replacement)
                .ok_or_else(|| failed(GenerationMutationError::InconsistentState))?;
            records.push(RecordState {
                base,
                working,
                present: base.is_some(),
            });
        }
        let path_comparisons = Cell::new(0_u64);
        #[allow(
            clippy::indexing_slicing,
            reason = "`index` is bounded by the `for index in 0..paths.len()` loop here; each \
                      iteration first reads `&paths[index]` and computes `find_path_state` from \
                      the whole (unmutated) `paths` slice, then writes back to that same \
                      in-bounds `paths[index]` — a manual split of read-all/write-one that \
                      `iter_mut` cannot express without an extra untracked allocation, which \
                      this budget-accounted kernel path avoids"
        )]
        for index in 0..paths.len() {
            let path = state_path(plan, &paths[index]);
            let Some((parent, _)) = path.split_last() else {
                return Err(failed(GenerationMutationError::InconsistentState));
            };
            paths[index].parent_state = find_path_state(plan, &paths, parent, &path_comparisons);
        }
        leaf.items(path_comparisons.get())?;
        let directory_edits = leaf.reserve::<DirectoryEdit>(
            operation_count
                .checked_mul(2)
                .ok_or_else(|| failed(WorkError::Overflow.into()))?,
        )?;
        let removed_directories =
            leaf.reserve::<FileId>(operation_count)?;
        let seed_bytes = logical_vec_bytes(&seeds)?;
        drop(seeds);
        leaf.release(seed_bytes)?;
        Ok(Self {
            paths,
            operation_paths,
            records,
            directory_edits,
            removed_directories,
            config,
        })
    }

    fn clear(&mut self) {
        drop(std::mem::take(&mut self.paths));
        drop(std::mem::take(&mut self.operation_paths));
        drop(std::mem::take(&mut self.records));
        drop(std::mem::take(&mut self.directory_edits));
        drop(std::mem::take(&mut self.removed_directories));
    }

    fn retained_bytes(&self) -> Result<u64, crate::FsError> {
        let vectors = [
            logical_vec_bytes(&self.paths)?, logical_vec_bytes(&self.operation_paths)?,
            logical_vec_bytes(&self.records)?, logical_vec_bytes(&self.directory_edits)?,
            logical_vec_bytes(&self.removed_directories)?,
        ];
        let base = vectors.into_iter().try_fold(0_u64, |sum, bytes| sum.checked_add(bytes).ok_or(WorkError::Overflow))?;
        self.directory_edits.iter().try_fold(base, |sum, edit| {
            sum.checked_add(edit.retained_name_bytes).ok_or_else(|| WorkError::Overflow.into())
        })
    }

    fn prepare(
        &mut self, leaf: &mut MutationLeaf<'_, '_>, plan: &MutationPlan, ordinal: usize, operation: &Mutation,
    ) -> Result<Option<RegularAction>, crate::FsError> {
        let order = u32::try_from(ordinal).map_err(|_| GenerationMutationError::InconsistentState)?;
        let path_id = |state: &Self| state.source_binding(ordinal).map(|binding| binding.file_id);
        match operation {
            Mutation::Create { record, .. } => self.create(leaf, plan, ordinal, *record, order)?,
            Mutation::Restore { record, .. } => self.restore(leaf, plan, ordinal, *record, order)?,
            Mutation::Remove { expected_file_id, .. } => {
                let expected = match expected_file_id {
                    MetadataField::Unavailable => None, MetadataField::Value(value) => Some(*value),
                };
                self.remove(leaf, plan, ordinal, expected, order)?;
            }
            Mutation::Rename { source, destination, replace } => self.rename(leaf, plan, ordinal, source, destination, *replace, order)?,
            Mutation::Link { .. } => self.link(leaf, plan, ordinal, order)?,
            Mutation::SetMetadata { metadata, .. } => {
                let id = path_id(self)?; self.record_mut(leaf, id)?.working.metadata = *metadata;
            }
            Mutation::ValidateRegular { .. } => { let id = path_id(self)?; self.regular_payload(leaf, id)?; }
            Mutation::Write { offset, length, content, content_offset, .. } => {
                let id = path_id(self)?;
                return self.regular_action(leaf, id, RegularMutation::Write {
                    offset: *offset, length: *length, content: *content, content_offset: *content_offset,
                }).map(Some);
            }
            Mutation::Resize { logical_bytes, .. } => {
                let id = path_id(self)?;
                return self.regular_action(leaf, id, RegularMutation::Resize { logical_bytes: *logical_bytes }).map(Some);
            }
            Mutation::ZeroRange { offset, length, allocated, extend, .. } => {
                let id = path_id(self)?;
                return self.regular_action(leaf, id, RegularMutation::ZeroRange {
                    offset: *offset, length: *length, allocated: *allocated, extend: *extend,
                }).map(Some);
            }
            Mutation::Preallocate { offset, length, keep_size, .. } => {
                let id = path_id(self)?;
                return self.regular_action(leaf, id, RegularMutation::Preallocate {
                    offset: *offset, length: *length, keep_size: *keep_size,
                }).map(Some);
            }
            Mutation::CloneRange { source_offset, destination_offset, length, .. } => {
                let source = self.binding(ordinal, 0)?.file_id;
                let destination = self.binding(ordinal, 1)?.file_id;
                return self.clone_action(leaf, source, *source_offset, destination, *destination_offset, *length).map(Some);
            }
            Mutation::File { file_id, mutation } => match mutation {
                FileMutation::ReplaceRecord { record } => {
                    let binding = self.identity_binding(leaf, *file_id)?;
                    if binding.kind != record.kind || binding.kind == FileKind::Directory {
                        return Err(GenerationMutationError::InconsistentState.into());
                    }
                    let link_count = self.record(leaf, *file_id)?.working.link_count;
                    self.record_mut(leaf, *file_id)?.working = FileRecord { link_count, ..*record };
                }
                FileMutation::SetMetadata { metadata } => {
                    self.identity_binding(leaf, *file_id)?; self.record_mut(leaf, *file_id)?.working.metadata = *metadata;
                }
                FileMutation::ValidateRegular => { self.regular_payload(leaf, *file_id)?; }
                FileMutation::Write { offset, length, content, content_offset } => {
                    return self.regular_action(leaf, *file_id, RegularMutation::Write {
                        offset: *offset, length: *length, content: *content, content_offset: *content_offset,
                    }).map(Some);
                }
                FileMutation::Resize { logical_bytes } => {
                    return self.regular_action(leaf, *file_id, RegularMutation::Resize { logical_bytes: *logical_bytes }).map(Some);
                }
                FileMutation::ZeroRange { offset, length, allocated, extend } => {
                    return self.regular_action(leaf, *file_id, RegularMutation::ZeroRange {
                        offset: *offset, length: *length, allocated: *allocated, extend: *extend,
                    }).map(Some);
                }
                FileMutation::Preallocate { offset, length, keep_size } => {
                    return self.regular_action(leaf, *file_id, RegularMutation::Preallocate {
                        offset: *offset, length: *length, keep_size: *keep_size,
                    }).map(Some);
                }
            },
            Mutation::CloneFileRange { source_file_id, source_offset, destination_file_id, destination_offset, length } => {
                return self.clone_action(leaf, *source_file_id, *source_offset, *destination_file_id, *destination_offset, *length).map(Some);
            }
        }
        Ok(None)
    }

    fn regular_payload(&mut self, leaf: &mut MutationLeaf<'_, '_>, id: FileId) -> Result<FilePayload, crate::FsError> {
        if self.identity_binding(leaf, id)?.kind != FileKind::Regular {
            return Err(GenerationMutationError::InconsistentState.into());
        }
        Ok(self.record(leaf, id)?.working.payload)
    }

    fn regular_action(&mut self, leaf: &mut MutationLeaf<'_, '_>, id: FileId, mutation: RegularMutation) -> Result<RegularAction, crate::FsError> {
        Ok(RegularAction::Mutate { id, payload: self.regular_payload(leaf, id)?, mutation })
    }

    fn clone_action(
        &mut self, leaf: &mut MutationLeaf<'_, '_>, source: FileId, source_offset: u64, destination: FileId, destination_offset: u64, length: u64,
    ) -> Result<RegularAction, crate::FsError> {
        Ok(RegularAction::Clone {
            destination, source_payload: self.regular_payload(leaf, source)?, source_offset,
            destination_payload: self.regular_payload(leaf, destination)?, destination_offset, length,
        })
    }

    #[allow(
        clippy::indexing_slicing,
        reason = "`operation` is always the enumerated ordinal from `simulate`'s \
                  `for (ordinal, operation) in plan.operations().iter().enumerate()`, so \
                  `operation < plan.operations().len() == self.operation_paths.len()`; this \
                  method is only reached for `Mutation::Create`, whose `paths()` always returns \
                  `Some`, so `TransactionState::new`'s construction loop already overwrote \
                  `operation_paths[operation]` away from its `[usize::MAX; 2]` resize sentinel \
                  with a valid index into `self.paths`"
    )]
    fn create(
        &mut self, leaf: &mut MutationLeaf<'_, '_>,
        plan: &MutationPlan,
        operation: usize,
        record: FileRecord,
        order: u32,
    ) -> Result<(), crate::FsError> {
        let state = self.operation_paths[operation][0];
        if self.paths[state].binding.is_some() {
            return Err(failed(GenerationMutationError::AlreadyExists));
        }
        let directory_id = self.parent_directory(leaf, state)?;
        let record_state = self.record_mut(leaf, record.file_id)?;
        if record_state.base.is_some() || record_state.present {
            return Err(failed(GenerationMutationError::FileIdentityAlreadyExists));
        }
        record_state.working = record;
        record_state.present = true;
        self.paths[state].binding = Some(Binding {
            file_id: record.file_id,
            kind: record.kind,
        });
        let (name, retained_name_bytes) = self.clone_terminal_name(leaf, plan, operation, 0)?;
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

    fn restore(
        &mut self, leaf: &mut MutationLeaf<'_, '_>,
        plan: &MutationPlan,
        operation: usize,
        source: FileRecord,
        order: u32,
    ) -> Result<(), crate::FsError> {
        #[allow(
            clippy::indexing_slicing,
            reason = "operation paths are preflighted and indexed by the enumerated mutation ordinal"
        )]
        let state = self.operation_paths[operation][0];
        #[allow(
            clippy::indexing_slicing,
            reason = "operation path state indices are constructed from the same path table"
        )]
        let current = self.paths[state].binding;
        if let Some(current) = current {
            if current.kind == FileKind::Directory && source.kind == FileKind::Directory {
                self.record_mut(leaf, current.file_id)?.working.metadata = source.metadata;
                return Ok(());
            }
            if current.file_id == source.file_id {
                let link_count = self.record(leaf, source.file_id)?.working.link_count;
                self.record_mut(leaf, source.file_id)?.working = FileRecord {
                    link_count,
                    ..source
                };
                return Ok(());
            }
        }

        let directory_id = self.parent_directory(leaf, state)?;
        let (name, retained_name_bytes) = self.clone_terminal_name(leaf, plan, operation, 0)?;
        let entry = TreeEntry {
            name,
            file_id: source.file_id,
            kind: source.kind,
        };
        self.push_edit(
            directory_id,
            order,
            retained_name_bytes,
            match current {
                Some(existing) => TreeMutation::Replace {
                    entry,
                    expected_file_id: existing.file_id,
                },
                None => TreeMutation::Insert(entry),
            },
        );
        if let Some(existing) = current {
            self.drop_link(leaf, existing)?;
        }
        
        let desired = self.record_mut(leaf, source.file_id)?;
        if desired.present {
            if source.kind == FileKind::Directory {
                return Err(failed(GenerationMutationError::UnsupportedHardLink));
            }
            desired.working = FileRecord {
                link_count: desired
                    .working
                    .link_count
                    .checked_add(1)
                    .ok_or_else(|| failed(WorkError::Overflow.into()))?,
                ..source
            };
        } else {
            desired.working = FileRecord {
                link_count: 1,
                ..source
            };
            desired.present = true;
        }
        #[allow(
            clippy::indexing_slicing,
            reason = "operation path state indices are constructed from the same path table"
        )]
        let path_state = &mut self.paths[state];
        path_state.binding = Some(Binding {
            file_id: source.file_id,
            kind: source.kind,
        });
        Ok(())
    }

    #[allow(
        clippy::indexing_slicing,
        reason = "`operation` is always the enumerated ordinal from `simulate`'s \
                  `for (ordinal, operation) in plan.operations().iter().enumerate()`, so \
                  `operation < plan.operations().len() == self.operation_paths.len()`; this \
                  method is only reached for `Mutation::Remove`, whose `paths()` always returns \
                  `Some`, so `TransactionState::new`'s construction loop already overwrote \
                  `operation_paths[operation]` away from its `[usize::MAX; 2]` resize sentinel \
                  with a valid index into `self.paths`"
    )]
    fn remove(
        &mut self, leaf: &mut MutationLeaf<'_, '_>,
        plan: &MutationPlan,
        operation: usize,
        expected: Option<FileId>,
        order: u32,
    ) -> Result<(), crate::FsError> {
        let state = self.operation_paths[operation][0];
        let binding = self.paths[state]
            .binding
            .ok_or_else(|| failed(GenerationMutationError::MissingSource))?;
        if expected.is_some_and(|value| value != binding.file_id) {
            return Err(failed(GenerationMutationError::FileIdentityConflict));
        }
        let directory_id = self.parent_directory(leaf, state)?;
        let (name, retained_name_bytes) = self.clone_terminal_name(leaf, plan, operation, 0)?;
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
        self.drop_link(leaf, binding)
    }

    #[allow(
        clippy::indexing_slicing,
        reason = "`operation` is always the enumerated ordinal from `simulate`'s \
                  `for (ordinal, operation) in plan.operations().iter().enumerate()`, so \
                  `operation < plan.operations().len() == self.operation_paths.len()`; this \
                  method is only reached for `Mutation::Rename`, whose `paths()` always returns \
                  `Some` for both endpoints, so `TransactionState::new`'s construction loop \
                  already overwrote `operation_paths[operation]` away from its \
                  `[usize::MAX; 2]` resize sentinel with valid indices into `self.paths`"
    )]
    fn rename(
        &mut self, leaf: &mut MutationLeaf<'_, '_>,
        plan: &MutationPlan,
        operation: usize,
        source_path: &NamespacePath,
        destination_path: &NamespacePath,
        replace: bool,
        order: u32,
    ) -> Result<(), crate::FsError> {
        let source_state = self.operation_paths[operation][0];
        let destination_state = self.operation_paths[operation][1];
        let source = self.paths[source_state]
            .binding
            .ok_or_else(|| failed(GenerationMutationError::MissingSource))?;
        if source.kind == FileKind::Directory && destination_path.is_within(source_path) {
            return Err(failed(GenerationMutationError::DirectoryCycle));
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
            return Err(failed(GenerationMutationError::AlreadyExists));
        }
        let source_parent = self.parent_directory(leaf, source_state)?;
        let destination_parent = self.parent_directory(leaf, destination_state)?;
        let (source_name, source_name_bytes) = self.clone_terminal_name(leaf, plan, operation, 0)?;
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
            self.clone_terminal_name(leaf, plan, operation, 1)?;
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
            self.drop_link(leaf, existing)?;
        }
        self.paths[source_state].binding = None;
        self.paths[destination_state].binding = Some(source);
        Ok(())
    }

    #[allow(
        clippy::indexing_slicing,
        reason = "`operation` is always the enumerated ordinal from `simulate`'s \
                  `for (ordinal, operation) in plan.operations().iter().enumerate()`, so \
                  `operation < plan.operations().len() == self.operation_paths.len()`; this \
                  method is only reached for `Mutation::Link`, whose `paths()` always returns \
                  `Some` for both endpoints, so `TransactionState::new`'s construction loop \
                  already overwrote `operation_paths[operation]` away from its \
                  `[usize::MAX; 2]` resize sentinel with valid indices into `self.paths`"
    )]
    fn link(
        &mut self, leaf: &mut MutationLeaf<'_, '_>,
        plan: &MutationPlan,
        operation: usize,
        order: u32,
    ) -> Result<(), crate::FsError> {
        let source = self.source_binding(operation)?;
        if source.kind == FileKind::Directory {
            return Err(failed(GenerationMutationError::UnsupportedHardLink));
        }
        let destination_state = self.operation_paths[operation][1];
        if self.paths[destination_state].binding.is_some() {
            return Err(failed(GenerationMutationError::AlreadyExists));
        }
        let destination_parent = self.parent_directory(leaf, destination_state)?;
        let current_links = self.record(leaf, source.file_id)?.working.link_count;
        let next_links = current_links
            .checked_add(1)
            .ok_or_else(|| failed(WorkError::Overflow.into()))?;
        self.record_mut(leaf, source.file_id)?.working.link_count = next_links;
        self.paths[destination_state].binding = Some(source);
        let (name, retained_name_bytes) = self.clone_terminal_name(leaf, plan, operation, 1)?;
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

    fn source_binding(&self, operation: usize) -> Result<Binding, crate::FsError> {
        self.binding(operation, 0)
    }

    #[allow(
        clippy::indexing_slicing,
        reason = "`operation` is always the enumerated ordinal from `simulate`'s \
                  `for (ordinal, operation) in plan.operations().iter().enumerate()`, so \
                  `operation < plan.operations().len() == self.operation_paths.len()`; every \
                  caller of `binding`/`source_binding` only does so for a `Mutation` variant \
                  whose `paths()` returns `Some` for the requested `endpoint` (0 or 1), so \
                  `TransactionState::new`'s construction loop already overwrote \
                  `operation_paths[operation][endpoint]` away from its `[usize::MAX; 2]` resize \
                  sentinel with a valid index into `self.paths`"
    )]
    fn binding(
        &self,
        operation: usize,
        endpoint: usize,
    ) -> Result<Binding, crate::FsError> {
        self.paths[self.operation_paths[operation][endpoint]]
            .binding
            .ok_or_else(|| failed(GenerationMutationError::MissingSource))
    }

    #[allow(
        clippy::indexing_slicing,
        reason = "`state` is always either an `operation_paths[operation][endpoint]` value \
                  (see `binding`'s invariant above: a valid index into `self.paths`) or a \
                  `PathState.parent_state`/`find_path_state` result, which is produced only by \
                  `states.binary_search_by(...).ok()` over the very same `self.paths` slice \
                  (`find_path_state`, below), so any `Some(parent_state)` it returns is also a \
                  valid index into `self.paths`"
    )]
    fn parent_directory(&mut self, leaf: &mut MutationLeaf<'_, '_>, state: usize) -> Result<FileId, crate::FsError> {
        let file_id = match self.paths[state].parent_state {
            Some(parent_state) => {
                self.paths[parent_state]
                    .binding
                    .ok_or_else(|| failed(GenerationMutationError::MissingParent))?
                    .file_id
            }
            None if self.paths[state].base_parent_exact => self.paths[state]
                .base_parent
                .ok_or_else(|| failed(GenerationMutationError::MissingParent))?,
            None => return Err(failed(GenerationMutationError::MissingParent)),
        };
        let record = self.record(leaf, file_id)?;
        if record.working.kind != FileKind::Directory || !record.present {
            return Err(failed(GenerationMutationError::MissingParent));
        }
        Ok(file_id)
    }

    fn drop_link(&mut self, leaf: &mut MutationLeaf<'_, '_>, binding: Binding) -> Result<(), crate::FsError> {
        let current_links = self.record(leaf, binding.file_id)?.working.link_count;
        if current_links == 0 {
            return Err(failed(GenerationMutationError::InconsistentState));
        }
        let next_links = current_links - 1;
        let record = self.record_mut(leaf, binding.file_id)?;
        record.working.link_count = next_links;
        record.present = next_links != 0;
        if binding.kind == FileKind::Directory {
            self.removed_directories.push(binding.file_id);
        }
        Ok(())
    }

    fn clone_terminal_name(
        &mut self, leaf: &mut MutationLeaf<'_, '_>,
        plan: &MutationPlan,
        operation: usize,
        endpoint: usize,
    ) -> Result<(LogicalName, u64), crate::FsError> {
        let path = Self::operation_path(plan, operation, endpoint);
        let name = path
            .split_last()
            .map(|(_, name)| name)
            .ok_or_else(|| failed(GenerationMutationError::InconsistentState))?;
        let mut copied = leaf.reserve::<u8>(name.as_bytes().len())?;
        let bytes = logical_vec_bytes(&copied)?;
        let delta = WorkCounters { bytes_copied: usize_to_u64(name.as_bytes().len()), ..WorkCounters::default() };
        leaf.accounting.admit(delta).map_err(accounting_error)?;
        copied.extend_from_slice(name.as_bytes());
        leaf.accounting.record_completed(delta).map_err(accounting_error)?;
        let name = LogicalName::new(name.encoding(), copied, self.config.limits.maximum_component_bytes)
            .map_err(|_| GenerationMutationError::InconsistentState)?;
        Ok((name, bytes))
    }

    #[allow(
        clippy::expect_used,
        reason = "every caller obtains the operation from a planner-owned PathState, which exists only for a path-bearing mutation"
    )]
    #[allow(
        clippy::indexing_slicing,
        reason = "every caller reaches this only via `clone_terminal_name`, whose `operation` \
                  is always the `simulate`-enumerated ordinal `< plan.operations().len()` and \
                  whose `endpoint` argument is always the literal `0` or `1` (see the \
                  `clone_terminal_name` call sites in `create`/`remove`/`rename`/`link`), and \
                  `plan.operations()[operation].paths()` is `Some` for exactly those callers, \
                  so both index operations are in-bounds by construction"
    )]
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

    #[allow(
        clippy::indexing_slicing,
        reason = "`record_index` returns `Ok(index)` only from a successful \
                  `self.records.binary_search_by(...)` over this same `self.records` slice \
                  (see `record_index` below), which guarantees `index < self.records.len()`, \
                  and nothing resizes `self.records` between that call and this indexing"
    )]
    fn record(&mut self, leaf: &mut MutationLeaf<'_, '_>, file_id: FileId) -> Result<&RecordState, crate::FsError> {
        let index = self.record_index(leaf, file_id)?;
        Ok(&self.records[index])
    }

    fn identity_binding(&mut self, leaf: &mut MutationLeaf<'_, '_>, file_id: FileId) -> Result<Binding, crate::FsError> {
        let record = self.record(leaf, file_id)?;
        if !record.present || record.working.link_count == 0 {
            return Err(failed(GenerationMutationError::MissingSource));
        }
        Ok(Binding {
            file_id,
            kind: record.working.kind,
        })
    }

    #[allow(
        clippy::indexing_slicing,
        reason = "`record_index` returns `Ok(index)` only from a successful \
                  `self.records.binary_search_by(...)` over this same `self.records` slice \
                  (see `record_index` below), which guarantees `index < self.records.len()`, \
                  and nothing resizes `self.records` between that call and this indexing"
    )]
    fn record_mut(
        &mut self, leaf: &mut MutationLeaf<'_, '_>,
        file_id: FileId,
    ) -> Result<&mut RecordState, crate::FsError> {
        let index = self.record_index(leaf, file_id)?;
        Ok(&mut self.records[index])
    }

    fn record_index(&mut self, leaf: &mut MutationLeaf<'_, '_>, file_id: FileId) -> Result<usize, crate::FsError> {
        let comparisons = Cell::new(0_u64);
        let result = self.records.binary_search_by(|record| {
            comparisons.set(comparisons.get().saturating_add(1));
            record.working.file_id.cmp(&file_id)
        });
        leaf.items(comparisons.get())?;
        result.map_err(|_| failed(GenerationMutationError::InconsistentState))
    }

    fn sort_directory_edits(&mut self, leaf: &mut MutationLeaf<'_, '_>) -> Result<(), crate::FsError> {
        let comparisons = Cell::new(0_u64);
        self.directory_edits.sort_unstable_by(|left, right| {
            comparisons.set(comparisons.get().saturating_add(1));
            left.directory_id.cmp(&right.directory_id).then_with(|| left.order.cmp(&right.order))
        });
        leaf.items(comparisons.get())
    }

    fn take_directory_edits(
        &mut self, leaf: &mut MutationLeaf<'_, '_>, start: usize,
    ) -> Result<(FileId, ObjectId, Vec<TreeMutation>, u64, usize), crate::FsError> {
        let directory_id = self.directory_edits.get(start).ok_or(GenerationMutationError::InconsistentState)?.directory_id;
        let mut end = start + 1;
        while self.directory_edits.get(end).is_some_and(|edit| edit.directory_id == directory_id) { end += 1; }
        let mut mutations = leaf.reserve::<TreeMutation>(end - start)?;
        let mut bytes = logical_vec_bytes(&mutations)?;
        for edit in self.directory_edits.get_mut(start..end).ok_or(GenerationMutationError::InconsistentState)? {
            bytes = bytes.checked_add(edit.retained_name_bytes).ok_or(WorkError::Overflow)?;
            mutations.push(edit.mutation.take().ok_or(GenerationMutationError::InconsistentState)?);
            edit.retained_name_bytes = 0;
        }
        let FilePayload::Directory { entries } = self.record(leaf, directory_id)?.working.payload else {
            return Err(GenerationMutationError::MissingParent.into());
        };
        Ok((directory_id, entries, mutations, bytes, end))
    }

    fn sort_removed_directories(&mut self, leaf: &mut MutationLeaf<'_, '_>) -> Result<(), crate::FsError> {
        let comparisons = Cell::new(0_u64);
        self.removed_directories.sort_unstable_by(|left, right| {
            comparisons.set(comparisons.get().saturating_add(1)); left.cmp(right)
        });
        leaf.items(comparisons.get())?;
        leaf.items(usize_to_u64(self.removed_directories.len().saturating_sub(1)))?;
        self.removed_directories.dedup();
        Ok(())
    }

    fn file_table_edits(&mut self, leaf: &mut MutationLeaf<'_, '_>) -> Result<Vec<FileTableMutation>, crate::FsError> {
        let count = self.records.iter().filter(|record| {
            matches!((record.base, record.present), (None, true) | (Some(_), false))
                || matches!((record.base, record.present), (Some(expected), true) if expected != record.working)
        }).count();
        leaf.items(usize_to_u64(self.records.len()))?;
        let mut mutations = leaf.reserve::<FileTableMutation>(count)?;
        for record in &self.records {
            match (record.base, record.present) {
                (None, true) => mutations.push(FileTableMutation::Insert(record.working)),
                (Some(expected), false) => mutations.push(FileTableMutation::Remove { file_id: expected.file_id, expected: Some(expected) }),
                (Some(expected), true) if expected != record.working => mutations.push(FileTableMutation::Replace { expected, replacement: record.working }),
                (None, false) | (Some(_), true) => {}
            }
        }
        Ok(mutations)
    }

    fn clone_generation_root(
        &mut self, leaf: &mut MutationLeaf<'_, '_>,
        generation: &GenerationRoot,
        file_table: ObjectId,
    ) -> Result<GenerationRoot, crate::FsError> {
        let (mut root, mut allocation) = clone_generation_root_with_accounting(generation, leaf.accounting)?;
        if let Err(error) = leaf.allocation.absorb(&mut allocation) {
            drop(root);
            return Err(error);
        }
        root.file_table = file_table;
        root.required_features = generation.continuation_features();
        Ok(root)
    }
}

pub(crate) fn clone_generation_root_with_accounting(
    root: &GenerationRoot, accounting: &mut KernelAccounting<'_>,
) -> Result<(GenerationRoot, KernelOwnedAllocation), crate::FsError> {
    let mut parents = reserve_owned::<GenerationId>(root.parents.len(), accounting)?;
    let delta = WorkCounters {
        bytes_copied: usize_to_u64(root.parents.len()).checked_mul(size_of::<GenerationId>() as u64).ok_or(WorkError::Overflow)?,
        ..WorkCounters::default()
    };
    accounting.admit(delta).map_err(accounting_error)?;
    parents.value.extend_from_slice(&root.parents);
    accounting.record_completed(delta).map_err(accounting_error)?;
    let Owned { value: parents, allocation } = parents;
    Ok((GenerationRoot {
        volume_id: root.volume_id, root_file_id: root.root_file_id, file_table: root.file_table,
        parents, required_features: root.required_features,
    }, allocation))
}

fn operation_uses_one_path(mutation: &Mutation) -> bool {
    mutation
        .paths()
        .is_some_and(|[first, second]| first == second)
}

fn preflight_generation_mutations(
    operations: &[Mutation],
    config: VolumeConfig,
) -> Result<(), crate::FsError> {
    config.validate().map_err(GenerationMutationError::from)?;
    let mut endpoints = 0_u32;
    for operation in operations {
        if let Some([first, second]) = operation.paths() {
            endpoints = endpoints
                .checked_add(if first == second { 1 } else { 2 })
                .ok_or_else(|| {
                    crate::FsError::Mutation(GenerationMutationError::TooManyPaths)
                })?;
        }
        match operation {
            Mutation::Create { record, .. } | Mutation::Restore { record, .. } => {
                if !record.kind.is_supported_by_profile(config.profile) {
                    return Err(crate::FsError::Mutation(GenerationMutationError::UnsupportedFileKind));
                }
                if matches!(operation, Mutation::Create { .. }) && record.link_count != 1 {
                    return Err(crate::FsError::Mutation(GenerationMutationError::InvalidInitialLinkCount));
                }
                if record.kind == FileKind::SymbolicLink && !config.symbolic_links {
                    return Err(crate::FsError::Mutation(GenerationMutationError::UnsupportedSymbolicLink));
                }
            }
            Mutation::Link { .. } if !config.hard_links => {
                return Err(crate::FsError::Mutation(GenerationMutationError::UnsupportedHardLink));
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
                return Err(crate::FsError::Mutation(GenerationMutationError::UnsupportedSparseMutation));
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
        return Err(crate::FsError::Mutation(GenerationMutationError::TooManyPaths));
    }
    Ok(())
}

#[allow(
    clippy::expect_used,
    reason = "PathState is private and exists only for planner-validated path uses"
)]
#[allow(
    clippy::indexing_slicing,
    reason = "every `PathState` is built in `TransactionState::new` from a \
              `plan.ordered_operation_paths()` tuple, whose `operation` and `endpoint` fields \
              are always `< plan.operations().len()` and always `0` or `1` respectively (see \
              `MutationPlan::ordered_operation_paths`/`compile` in mutation.rs), and only for \
              operations whose `paths()` returns `Some`, so both index operations here are \
              in-bounds by construction"
)]
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

fn logical_vec_bytes<T>(values: &Vec<T>) -> Result<u64, crate::FsError> {
    values
        .capacity()
        .checked_mul(size_of::<T>())
        .map(usize_to_u64)
        .ok_or_else(|| WorkError::Overflow.into())
}

#[cfg(all(test, feature = "memory"))]
fn allocation_failure(error: AllocationError) -> crate::FsError {
    error.into_work_or(GenerationMutationError::AllocationFailed).into()
}

fn path_cancelled() -> GenerationMutationError {
    GenerationMutationError::Path(PathLookupError::Tree(super::TreeReadError::Cancelled))
}

fn failed(error: GenerationMutationError) -> crate::FsError {
    error.into()
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/generation_mutation.rs"]
mod tests;
