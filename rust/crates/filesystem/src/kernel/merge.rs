//! Canonical bounded three-way generation merge.

use super::{
    CanonicalDecodeError, CheckpointError, CheckpointRequest, DecodeLimits, ExtentKind,
    ExtentMutation, ExtentMutationError, ExtentRangeRequest, ExtentReadError, ExtentSlice,
    FileMetadata, FilePayload, FileRecord, FileTableMutation, FileTableMutationError,
    GenerationRoot, LogicalName, MetadataField, TreeMutation,
    TreeMutationError, decode_file_metadata,
};
use super::allocation::{KernelAccounting, KernelOwnedAllocation};
use super::persistent_point::{Owned, absorb, retire_allocation};
use super::persistent_diff::{diff_file_records_with_accounting, diff_tree_entries_with_accounting};
use super::file_table_mutation::apply_file_table_mutations_with_accounting;
use super::tree_mutation::{TreeFormat, apply_tree_mutations_with_accounting};
use super::persistent_btree::Format;
use super::range::plan_extent_range_with_accounting;
use super::extent_mutation::{apply_extent_mutations_with_accounting, put_encoded_with_accounting};
use super::checkpoint::build_checkpoint_with_accounting;
use crate::FsError;
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::{CancellationError, CancellationToken};
use crate::foundation::{FileId, GenerationId};
use crate::performance::{
    MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError,
};
use crate::storage::{ByteRange, ObjectId, ObjectKind, ObjectStoreError};
use std::collections::BTreeMap;
use thiserror::Error;

/// One exact unresolved three-way merge region.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum MergeConflict {
    /// Competing changes to one path-independent record or directory metadata.
    File(FileId),
    /// Competing changes to one exact name in a stable directory.
    Binding {
        /// Stable parent directory.
        directory_id: FileId,
        /// Exact conflicting component.
        name: LogicalName,
    },
}

/// Exact immutable side selected for one otherwise conflicting region.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MergeConflictSide {
    /// Common-ancestor value.
    Base,
    /// Target-side value.
    Ours,
    /// Source-side value.
    Theirs,
}

/// Declarative resolution for one exact merge conflict.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MergeConflictResolution {
    /// Select one of the three immutable input sides.
    Select(MergeConflictSide),
    /// Install or remove a complete path-independent file record.
    File(Option<FileRecord>),
    /// Install or remove one exact directory binding.
    Binding(Option<FileId>),
}

/// Immutable inputs for one bounded generation merge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MergeGenerationRequest {
    /// Immutable common ancestor object.
    pub base_generation: ObjectId,
    /// Authenticated common ancestor root.
    pub base: GenerationRoot,
    /// Exact authenticated object for `ours`, when it is already immutable.
    pub ours_generation: Option<ObjectId>,
    /// Current sparse candidate rooted at `base_generation`.
    pub ours: GenerationRoot,
    /// Authenticated second-parent object.
    pub theirs_generation: ObjectId,
    /// Authenticated second-parent root.
    pub theirs: GenerationRoot,
    /// Whether the source generation is retained as a second parent.
    pub retain_theirs_parent: bool,
    /// Single total changed-record/name frontier bound.
    pub maximum_changes: u32,
    /// Maximum exact conflict regions retained in the terminal result.
    pub maximum_conflicts: u32,
    /// Caller-validated side selections for exact conflicting regions.
    pub resolutions: BTreeMap<MergeConflict, MergeConflictResolution>,
}

#[derive(Clone, Copy)]
pub(crate) struct MergeGenerationBorrowedRequest<'a> {
    pub base_generation: ObjectId,
    pub base: &'a GenerationRoot,
    pub ours_generation: Option<ObjectId>,
    pub ours: &'a GenerationRoot,
    pub theirs_generation: ObjectId,
    pub theirs: &'a GenerationRoot,
    pub retain_theirs_parent: bool,
    pub maximum_changes: u32,
    pub maximum_conflicts: u32,
}

#[derive(Clone, Copy)]
enum ResolutionView<'a> {
    Map(&'a BTreeMap<MergeConflict, MergeConflictResolution>),
    Slice(&'a [(MergeConflict, MergeConflictResolution)]),
}

impl<'a> ResolutionView<'a> {
    fn get_by(
        self, accounting: &mut KernelAccounting<'_>,
        mut compare: impl FnMut(&MergeConflict) -> std::cmp::Ordering,
    ) -> Result<Option<&'a MergeConflictResolution>, FsError> {
        match self {
            Self::Map(map) => {
                for (key, value) in map {
                    accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    match compare(key) {
                        std::cmp::Ordering::Less => {}
                        std::cmp::Ordering::Equal => return Ok(Some(value)),
                        std::cmp::Ordering::Greater => break,
                    }
                }
                Ok(None)
            }
            Self::Slice(slice) => {
                let index = super::search::binary_search_with_accounting(slice, accounting, |(key, _)| compare(key))?;
                Ok(index.ok().map(|index| &slice[index].1))
            }
        }
    }

    fn file(self, file: FileId, accounting: &mut KernelAccounting<'_>) -> Result<Option<&'a MergeConflictResolution>, FsError> {
        self.get_by(accounting, |key| key.cmp(&MergeConflict::File(file)))
    }

    fn binding(
        self, directory: FileId, name: &LogicalName, accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<&'a MergeConflictResolution>, FsError> {
        self.get_by(accounting, |key| match key {
            MergeConflict::File(_) => std::cmp::Ordering::Less,
            MergeConflict::Binding { directory_id, name: key } => directory_id.cmp(&directory).then_with(|| key.cmp(name)),
        })
    }
}

/// Terminal result of canonical merge preparation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MergeGenerationOutcome {
    /// Complete authenticated unpublished two-parent generation.
    Prepared {
        /// Candidate generation-root object.
        generation_root: ObjectId,
        /// Decoded candidate root without a redundant object-store reread.
        root: GenerationRoot,
        /// Candidate generation identity.
        generation_id: GenerationId,
    },
    /// No candidate was produced.
    Conflicted {
        /// Stable conflicting regions.
        conflicts: Vec<MergeConflict>,
        /// Additional conflicts exceeded the supplied bound.
        truncated: bool,
    },
}

/// Canonical merge failures.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum MergeGenerationError {
    /// Merge roots or persistent diff structure are contradictory.
    #[error("generation merge structure is invalid")]
    InvalidDiff,
    /// Caller supplied a zero or exceeded change frontier.
    #[error("generation merge exceeds its change bound")]
    ChangeLimit,
    /// Bounded diff allocation failed.
    #[error("generation merge diff allocation failed")]
    AllocationFailed,
    /// Immutable storage failed.
    #[error(transparent)]
    Object(#[from] ObjectStoreError),
    /// Canonical page decoding failed.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// Sparse file-table application failed.
    #[error(transparent)]
    FileTable(#[from] FileTableMutationError),
    /// Sparse directory-tree application failed.
    #[error(transparent)]
    Tree(#[from] TreeMutationError),
    /// Sparse extent planning failed.
    #[error(transparent)]
    ExtentRead(#[from] ExtentReadError),
    /// Sparse extent path-copy application failed.
    #[error(transparent)]
    ExtentMutation(#[from] ExtentMutationError),
    /// Checkpoint or two-parent construction failed.
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    /// Operation was cancelled before safe completion.
    #[error(transparent)]
    Cancelled(#[from] CancellationError),
    /// Exact work overflowed or exceeded its admitted budget.
    #[error(transparent)]
    Work(#[from] WorkError),
    /// A common accounting leaf retained an exact facade error cause.
    #[error(transparent)]
    Kernel(crate::FsError),
}

/// Receipt-bearing merge result.
pub type MergeGenerationResult =
    MeasuredResult<OperationReceipt<MergeGenerationOutcome>, MergeGenerationError>;

/// Performs one sparse bounded three-way merge and constructs its two-parent checkpoint.
#[allow(
    clippy::too_many_lines,
    reason = "one sparse bounded three-way merge and its two-parent checkpoint"
)]
pub async fn merge_generation_async<S: AsyncObjectStore>(
    store: &S, request: MergeGenerationRequest, limits: DecodeLimits,
    budget: WorkBudget, cancellation: &CancellationToken,
) -> MergeGenerationResult {
    let span = crate::obs::span!(DEBUG, "acyclic.fs.kernel.merge",
        outcome = crate::obs::Empty, error.kind = crate::obs::Empty,
        work.items = crate::obs::Empty, work.bytes = crate::obs::Empty,
        work.durability = crate::obs::Empty);
    let result = crate::obs::in_span(&span, async {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = merge_generation_with_accounting(store, request, limits, &mut accounting, cancellation).await;
        let work = accounting.spent();
        result.map(|(value, _)| OperationReceipt { value, work })
            .map_err(|error| OperationFailure::new(project_merge_error(error), work))
    }).await;
    crate::obs::measured_on(&span, result, |receipt| &receipt.work)
}

type RecordChange = (Option<FileRecord>, Option<FileRecord>);

struct MergeState {
    records: Owned<Vec<(FileId, RecordChange)>>,
    conflicts: Owned<Vec<MergeConflict>>,
    folds: Owned<Vec<(FileId, FileId)>>,
    file_folds: Owned<Vec<FileFold>>,
    truncated: bool,
    remaining_changes: u32,
}

impl MergeState {
    fn new(maximum_changes: u32, accounting: &mut KernelAccounting<'_>) -> Result<Self, FsError> {
        Ok(Self {
            records: Owned::allocate(0, accounting)?,
            conflicts: Owned::allocate(0, accounting)?,
            folds: Owned::allocate(0, accounting)?,
            file_folds: Owned::allocate(0, accounting)?,
            truncated: false,
            remaining_changes: maximum_changes,
        })
    }
    fn record(&self, file: FileId, accounting: &mut KernelAccounting<'_>) -> Result<RecordChange, FsError> {
        let index = super::search::binary_search_with_accounting(&self.records.value, accounting, |(id, _)| id.cmp(&file))?
            .map_err(|_| FsError::InvalidDiff)?;
        Ok(self.records.value.get(index).ok_or(FsError::InvalidDiff)?.1)
    }
    fn replace(&mut self, file: FileId, value: RecordChange, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        let index = super::search::binary_search_with_accounting(&self.records.value, accounting, |(id, _)| id.cmp(&file))?
            .map_err(|_| FsError::InvalidDiff)?;
        self.records.value.get_mut(index).ok_or(FsError::InvalidDiff)?.1 = value;
        Ok(())
    }
    fn consume(&mut self, count: usize) -> Result<(), FsError> {
        self.remaining_changes = self.remaining_changes.checked_sub(u32::try_from(count).map_err(|_| FsError::MergeChangeLimit)?)
            .ok_or(FsError::MergeChangeLimit)?;
        Ok(())
    }
    fn conflict(&mut self, conflict: MergeConflict, maximum: u32, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        if self.conflicts.value.len() < usize::try_from(maximum).unwrap_or(usize::MAX) {
            self.conflicts.ensure_for_push(usize::try_from(maximum).unwrap_or(usize::MAX), accounting)?;
            self.conflicts.value.push(conflict);
        } else { self.truncated = true; }
        Ok(())
    }
}

pub(crate) async fn merge_generation_with_accounting<S: AsyncObjectStore>(
    store: &S, request: MergeGenerationRequest, limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<(MergeGenerationOutcome, KernelOwnedAllocation), FsError> {
    let inputs = MergeGenerationBorrowedRequest {
        base_generation: request.base_generation, base: &request.base,
        ours_generation: request.ours_generation, ours: &request.ours,
        theirs_generation: request.theirs_generation, theirs: &request.theirs,
        retain_theirs_parent: request.retain_theirs_parent,
        maximum_changes: request.maximum_changes, maximum_conflicts: request.maximum_conflicts,
    };
    merge_generation_core(store, inputs, ResolutionView::Map(&request.resolutions), limits, accounting, cancellation).await
}

pub(crate) async fn merge_generation_with_borrowed_map_resolutions_with_accounting<S: AsyncObjectStore>(
    store: &S, request: &MergeGenerationBorrowedRequest<'_>,
    resolutions: &BTreeMap<MergeConflict, MergeConflictResolution>, limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<(MergeGenerationOutcome, KernelOwnedAllocation), FsError> {
    merge_generation_core(store, *request, ResolutionView::Map(resolutions), limits, accounting, cancellation).await
}

pub(crate) async fn merge_generation_with_borrowed_resolutions_with_accounting<S: AsyncObjectStore>(
    store: &S, request: MergeGenerationBorrowedRequest<'_>,
    resolutions: &[(MergeConflict, MergeConflictResolution)], limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<(MergeGenerationOutcome, KernelOwnedAllocation), FsError> {
    for pair in resolutions.windows(2) {
        accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if pair[0].0 >= pair[1].0 { return Err(FsError::InvalidDiff); }
    }
    merge_generation_core(store, request, ResolutionView::Slice(resolutions), limits, accounting, cancellation).await
}

fn merge_generation_core<'a, 'accounting, S: AsyncObjectStore>(
    store: &'a S, request: MergeGenerationBorrowedRequest<'a>, resolutions: ResolutionView<'a>, limits: DecodeLimits,
    accounting: &'a mut KernelAccounting<'accounting>, cancellation: &'a CancellationToken,
) -> acyclic_stream::BoxProviderFuture<'a, Result<(MergeGenerationOutcome, KernelOwnedAllocation), FsError>> {
    Box::pin(async move {
        accounting.check()?;
        if request.maximum_changes == 0 || request.maximum_conflicts == 0 { return Err(FsError::MergeChangeLimit); }
        if request.base.volume_id != request.ours.volume_id
            || request.base.volume_id != request.theirs.volume_id
            || request.base.root_file_id != request.ours.root_file_id
            || request.base.root_file_id != request.theirs.root_file_id { return Err(FsError::InvalidDiff); }
        let mut state = MergeState::new(request.maximum_changes, accounting)?;
        let result = async {
            let (ours, custody) = diff_file_records_with_accounting(store, Some(request.base.file_table), Some(request.ours.file_table),
                request.maximum_changes, limits, accounting, cancellation).await?;
            let bytes = ours.retained_allocation_bytes;
            let ours = Owned::new(ours, custody, bytes);
            let result = async {
                let (theirs, custody) = diff_file_records_with_accounting(store, Some(request.base.file_table), Some(request.theirs.file_table),
                    request.maximum_changes, limits, accounting, cancellation).await?;
                let bytes = theirs.retained_allocation_bytes;
                let theirs = Owned::new(theirs, custody, bytes);
                let result = async {
                    if ours.value.truncated || theirs.value.truncated { return Err(FsError::MergeChangeLimit); }
                    let mut frontier_indices = [0, 0];
                    let mut frontier = 0_usize;
                    while next_changes(&ours.value.changes, &theirs.value.changes, &mut frontier_indices).is_some() {
                        accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                        frontier = frontier.checked_add(1).ok_or(WorkError::Overflow)?;
                    }
                    state.consume(frontier)?;
                    let mut indices = [0, 0];
                    while let Some((left, right)) = next_changes(&ours.value.changes, &theirs.value.changes, &mut indices) {
                        accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                        let file = left.or(right).ok_or(FsError::InvalidDiff)?.key;
                        let base = left.or(right).and_then(|change| change.before);
                        let ours_value = left.map_or(base, |change| change.after);
                        let theirs_value = right.map_or(base, |change| change.after);
                        let mut resolved = resolve_optional(base, ours_value, theirs_value);
                        if matches!(resolved, OptionalResolution::Conflict) && base.is_none()
                            && let (Some(a), Some(b)) = (ours_value, theirs_value)
                            && a.kind == b.kind && a.payload == b.payload
                            && let Some(metadata) = converge_metadata_async(store, a.metadata, b.metadata, limits, accounting, cancellation).await? {
                            resolved = OptionalResolution::Resolved(Some(FileRecord { metadata, ..a }));
                        }
                        let base = if base.is_none() && matches!(resolved, OptionalResolution::Conflict)
                            && is_directory(ours_value) && is_directory(theirs_value) {
                            Some(FileRecord { payload: FilePayload::Directory { entries: empty_tree_async(store, limits, accounting, cancellation).await? },
                                ..ours_value.ok_or(FsError::InvalidDiff)? })
                        } else { base };
                        if matches!(resolved, OptionalResolution::Conflict) && is_directory(base)
                            && is_directory(ours_value) && is_directory(theirs_value) {
                            if state.remaining_changes == 0 { return Err(FsError::MergeChangeLimit); }
                            let record = merge_directory_record_with_resolutions_async(store, file,
                                base.ok_or(FsError::InvalidDiff)?, ours_value.ok_or(FsError::InvalidDiff)?, theirs_value.ok_or(FsError::InvalidDiff)?,
                                resolutions, request.maximum_conflicts, limits, &mut state, accounting, cancellation).await?;
                            if let Some(record) = record { resolved = OptionalResolution::Resolved(Some(record)); }
                            else {
                                state.records.ensure_for_push(usize::MAX, accounting)?;
                                state.records.value.push((file, (ours_value, ours_value)));
                                continue;
                            }
                        }
                        if matches!(resolved, OptionalResolution::Conflict)
                            && is_extent_regular(base) && is_extent_regular(ours_value) && is_extent_regular(theirs_value)
                            && let Some((record, examined)) = merge_regular_record_async(store,
                                base.ok_or(FsError::InvalidDiff)?, ours_value.ok_or(FsError::InvalidDiff)?, theirs_value.ok_or(FsError::InvalidDiff)?,
                                state.remaining_changes, limits, accounting, cancellation).await? {
                            state.consume(examined as usize)?;
                            resolved = OptionalResolution::Resolved(Some(record));
                        }
                        if matches!(resolved, OptionalResolution::Conflict) {
                            resolved = merge_records_async(store, [base, ours_value, theirs_value], limits, accounting, cancellation).await?
                                .map_or(OptionalResolution::Conflict, |record| OptionalResolution::Resolved(Some(record)));
                        }
                        let after = match resolved {
                            OptionalResolution::Resolved(record) => record,
                            OptionalResolution::Conflict => match resolutions.file(file, accounting)? {
                                Some(MergeConflictResolution::Select(side)) => select_optional(*side, base, ours_value, theirs_value),
                                Some(MergeConflictResolution::File(record)) => {
                                    if record.is_some_and(|record| record.file_id != file) { return Err(FsError::InvalidDiff); }
                                    *record
                                }
                                Some(MergeConflictResolution::Binding(_)) => return Err(FsError::InvalidDiff),
                                None => { state.conflict(MergeConflict::File(file), request.maximum_conflicts, accounting)?; ours_value }
                            },
                        };
                        state.records.ensure_for_push(usize::MAX, accounting)?;
                        state.records.value.push((file, (ours_value, after)));
                    }
                    Ok(())
                }.await;
                theirs.retire(accounting)?;
                result
            }.await;
            ours.retire(accounting)?;
            result?;
            fold_additions(store, resolutions, request.maximum_conflicts, limits, &mut state, accounting, cancellation).await?;
            if !state.conflicts.value.is_empty() || state.truncated { return Ok(None); }
            adjust_link_counts(store, limits, &mut state, accounting, cancellation).await?;
            let mut mutations = Owned::allocate(0, accounting)?;
            let result = async {
                for (file, (before, after)) in &state.records.value {
                    if before != after && let Some(mutation) = file_table_mutation(*file, *before, *after) {
                        mutations.ensure_for_push(usize::MAX, accounting)?;
                        mutations.value.push(mutation);
                    }
                }
                let merged_table = if mutations.value.is_empty() { request.ours.file_table } else {
                    apply_file_table_mutations_with_accounting(store, request.ours.file_table, &mutations.value,
                        request.maximum_changes, limits, accounting, cancellation).await?.root
                };
                let (parent, parent_id) = match request.ours_generation {
                    Some(object) if object.kind == ObjectKind::GenerationRoot => (object, GenerationId::new(object.digest)),
                    Some(_) => return Err(FsError::InvalidDiff),
                    None => {
                        let parent = build_checkpoint_with_accounting(store, CheckpointRequest { base: request.base_generation,
                            file_table: request.ours.file_table, merge_parent: None }, limits, accounting, cancellation).await?;
                        (parent.root, parent.generation_id)
                    }
                };
                let merged = build_checkpoint_with_accounting(store, CheckpointRequest { base: parent, file_table: merged_table,
                    merge_parent: request.retain_theirs_parent.then_some(request.theirs_generation) }, limits, accounting, cancellation).await?;
                let parent_count = if request.retain_theirs_parent { 2 } else { 1 };
                let mut parents = Owned::allocate(parent_count, accounting)?;
                let delta = WorkCounters {
                    bytes_copied: (parent_count * std::mem::size_of::<GenerationId>()) as u64,
                    ..WorkCounters::UNCHARGED
                };
                let copied = accounting.admit(delta).and_then(|()| {
                    parents.value.push(parent_id);
                    if request.retain_theirs_parent {
                        parents.value.push(GenerationId::new(request.theirs_generation.digest));
                    }
                    accounting.record_completed(delta)
                });
                if let Err(error) = copied { parents.retire(accounting)?; return Err(error); }
                let Owned { value: parents, allocation, .. } = parents;
                Ok((MergeGenerationOutcome::Prepared { generation_root: merged.root, generation_id: merged.generation_id,
                    root: GenerationRoot { volume_id: request.ours.volume_id, root_file_id: request.ours.root_file_id,
                        file_table: merged_table, parents, required_features: request.ours.continuation_features() } }, allocation))
            }.await;
            mutations.retire(accounting)?;
            result.map(Some)
        }.await;
        state.records.retire(accounting)?;
        state.folds.retire(accounting)?;
        state.file_folds.retire(accounting)?;
        match result {
            Ok(None) => {
                let Owned { value: conflicts, allocation, .. } = state.conflicts;
                Ok((MergeGenerationOutcome::Conflicted { conflicts, truncated: state.truncated }, allocation))
            }
            other => { state.conflicts.retire(accounting)?; other?.ok_or(FsError::InvalidDiff) }
        }
    })
}

fn next_changes<'a, K: Ord, V>(
    left: &'a [super::persistent_diff::ValueChange<K, V>],
    right: &'a [super::persistent_diff::ValueChange<K, V>], indices: &mut [usize; 2],
) -> Option<(Option<&'a super::persistent_diff::ValueChange<K, V>>, Option<&'a super::persistent_diff::ValueChange<K, V>>)> {
    let (a, b) = (left.get(indices[0]), right.get(indices[1]));
    let result = match (a, b) {
        (Some(a), Some(b)) => match a.key.cmp(&b.key) {
            std::cmp::Ordering::Less => (Some(a), None),
            std::cmp::Ordering::Greater => (None, Some(b)),
            std::cmp::Ordering::Equal => (Some(a), Some(b)),
        },
        (Some(a), None) => (Some(a), None), (None, Some(b)) => (None, Some(b)),
        (None, None) => return None,
    };
    indices[0] += usize::from(result.0.is_some());
    indices[1] += usize::from(result.1.is_some());
    Some(result)
}

fn is_extent_regular(record: Option<FileRecord>) -> bool {
    matches!(record.map(|record| record.payload), Some(FilePayload::Regular { .. }))
}

fn merge_regular_record_async<'a, 'accounting, S: AsyncObjectStore>(
    store: &'a S, base: FileRecord, ours: FileRecord, theirs: FileRecord, maximum_changes: u32,
    limits: DecodeLimits, accounting: &'a mut KernelAccounting<'accounting>, cancellation: &'a CancellationToken,
) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<(FileRecord, u32)>, FsError>>
{
    Box::pin(async move {
        let Some(kind) = resolve_three(&base.kind, &ours.kind, &theirs.kind) else { return Ok(None); };
        let Some(metadata) = merge_metadata_async(store, Some(base.metadata), ours.metadata, theirs.metadata, limits, accounting, cancellation).await?
            else { return Ok(None); };
        let (FilePayload::Regular { logical_bytes: base_bytes, extents: base_root },
            FilePayload::Regular { logical_bytes: ours_bytes, extents: ours_root },
            FilePayload::Regular { logical_bytes: theirs_bytes, extents: theirs_root }) = (base.payload, ours.payload, theirs.payload)
            else { return Ok(None); };
        let Some(logical_bytes) = resolve_three(&base_bytes, &ours_bytes, &theirs_bytes) else { return Ok(None); };
        if base_bytes != ours_bytes || base_bytes != theirs_bytes || logical_bytes == 0 { return Ok(None); }
        let request = move |root| ExtentRangeRequest { root, file_size: logical_bytes, range: ByteRange { offset: 0, length: logical_bytes },
            maximum_spans: maximum_changes, limits, budget: WorkBudget::UNBOUNDED };
        let (base_plan, allocation) = plan_extent_range_with_accounting(store, request(base_root), accounting, cancellation).await?;
        let bytes = base_plan.retained_allocation_bytes;
        let base_plan = Owned::new(base_plan, allocation, bytes);
        let result = {
            let base_plan = &base_plan;
            let accounting = &mut *accounting;
            async move {
                let (ours_plan, allocation) = plan_extent_range_with_accounting(store, request(ours_root), accounting, cancellation).await?;
                let bytes = ours_plan.retained_allocation_bytes;
                let ours_plan = Owned::new(ours_plan, allocation, bytes);
                let result = {
                    let ours_plan = &ours_plan;
                    let accounting = &mut *accounting;
                    async move {
                        let (theirs_plan, allocation) = plan_extent_range_with_accounting(store, request(theirs_root), accounting, cancellation).await?;
                        let bytes = theirs_plan.retained_allocation_bytes;
                        let theirs_plan = Owned::new(theirs_plan, allocation, bytes);
                        let mut mutations = Owned::allocate(0, accounting)?;
                        let result = {
                            let theirs_plan = &theirs_plan;
                            let mutations = &mut mutations;
                            let accounting = &mut *accounting;
                            async move {
                                let plans = [&base_plan.value.spans, &ours_plan.value.spans, &theirs_plan.value.spans];
                                let mut indices = [0_usize; 3];
                                let mut offset = 0;
                                while offset < logical_bytes {
                                    accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                                    let spans = [plans[0].get(indices[0]).ok_or(FsError::InvalidDiff)?,
                                        plans[1].get(indices[1]).ok_or(FsError::InvalidDiff)?, plans[2].get(indices[2]).ok_or(FsError::InvalidDiff)?];
                                    let end = spans.iter().map(|span| span.offset.saturating_add(span.length)).min().ok_or(FsError::InvalidDiff)?;
                                    if end <= offset { return Err(FsError::InvalidDiff); }
                                    let kinds = [sliced_extent_kind(spans[0], offset)?, sliced_extent_kind(spans[1], offset)?, sliced_extent_kind(spans[2], offset)?];
                                    let Some(resolved) = resolve_three(&kinds[0], &kinds[1], &kinds[2]) else { return Ok(None); };
                                    if resolved != kinds[1] {
                                        if mutations.value.len() >= maximum_changes as usize { return Err(FsError::MergeChangeLimit); }
                                        mutations.ensure_for_push(maximum_changes as usize, accounting)?;
                                        mutations.value.push(ExtentMutation::Replace { offset, length: end - offset, kind: resolved, extend: false });
                                    }
                                    offset = end;
                                    for (index, span) in indices.iter_mut().zip(spans) {
                                        if span.offset + span.length == end { *index += 1; }
                                    }
                                }
                                let root = if mutations.value.is_empty() { ours_root } else {
                                    apply_extent_mutations_with_accounting(store, ours_root, ours_bytes, &mutations.value,
                                        maximum_changes, limits, accounting, cancellation).await?.root
                                };
                                Ok(Some((root, mutations.value.len() as u32)))
                            }
                        }.await;
                        mutations.retire(accounting)?;
                        theirs_plan.retire(accounting)?;
                        result
                    }
                }.await;
                ours_plan.retire(accounting)?;
                result
            }
        }.await;
        base_plan.retire(accounting)?;
        result.map(|merged| merged.map(|(root, count)| (FileRecord {
            file_id: ours.file_id, kind, link_count: ours.link_count, metadata,
            payload: FilePayload::Regular { logical_bytes, extents: root },
        }, count)))
    })
}

fn sliced_extent_kind(span: &ExtentSlice, offset: u64) -> Result<ExtentKind, FsError> {
    let delta = offset.checked_sub(span.offset).ok_or(FsError::InvalidDiff)?;
    Ok(match span.kind {
        ExtentKind::Hole => ExtentKind::Hole, ExtentKind::AllocatedZero => ExtentKind::AllocatedZero,
        ExtentKind::Content { object, object_offset } => ExtentKind::Content {
            object, object_offset: object_offset.checked_add(delta).ok_or(FsError::InvalidDiff)? },
    })
}

#[cfg(test)]
pub(crate) struct DirectoryMergeResult {
    pub(crate) record: Option<FileRecord>,
    pub(crate) conflicts: Vec<MergeConflict>,
    pub(crate) truncated: bool,
    pub(crate) examined_changes: u32,
    pub(crate) folds: Vec<(FileId, FileId)>,
    pub(crate) file_folds: Vec<FileFold>,
}

pub(crate) struct FileFold {
    directory_id: FileId,
    name: LogicalName,
    ours: FileId,
    theirs: FileId,
}

#[cfg(test)]
pub(crate) async fn merge_directory_record_async<S: AsyncObjectStore>(
    store: &S, directory_id: FileId, base: FileRecord, ours: FileRecord, theirs: FileRecord,
    maximum_changes: u32, maximum_conflicts: u32, limits: DecodeLimits,
    budget: WorkBudget, cancellation: &CancellationToken,
) -> Result<OperationReceipt<DirectoryMergeResult>, OperationFailure<MergeGenerationError>> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    let result = async {
        let mut state = MergeState::new(maximum_changes, &mut accounting)?;
        let record = merge_directory_record_with_resolutions_async(store, directory_id, base, ours, theirs,
            ResolutionView::Slice(&[]), maximum_conflicts, limits, &mut state, &mut accounting, cancellation).await?;
        Ok(DirectoryMergeResult { record, conflicts: state.conflicts.value, truncated: state.truncated,
            examined_changes: maximum_changes - state.remaining_changes, folds: state.folds.value, file_folds: state.file_folds.value })
    }.await;
    let work = accounting.spent();
    result.map(|value| OperationReceipt { value, work })
        .map_err(|error| OperationFailure::new(project_merge_error(error), work))
}

async fn merge_directory_record_with_resolutions_async<S: AsyncObjectStore>(
    store: &S, directory_id: FileId, base: FileRecord, ours: FileRecord, theirs: FileRecord,
    requested: ResolutionView<'_>, maximum_conflicts: u32,
    limits: DecodeLimits, state: &mut MergeState, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<Option<FileRecord>, FsError> {
    let (Some(base_entries), Some(ours_entries), Some(theirs_entries)) =
        (directory_entries(Some(base)), directory_entries(Some(ours)), directory_entries(Some(theirs)))
        else { return Err(FsError::InvalidDiff); };
    let supplied = match requested.file(directory_id, accounting)? {
        Some(MergeConflictResolution::Select(side)) => Some(select_value(*side, &base.metadata, &ours.metadata, &theirs.metadata)),
        Some(MergeConflictResolution::File(Some(record))) if record.file_id == directory_id => Some(record.metadata),
        _ => None,
    };
    let metadata = if let Some(metadata) = supplied { Some(metadata) } else {
        merge_metadata_async(store, Some(base.metadata), ours.metadata, theirs.metadata, limits, accounting, cancellation).await?
    };
    let link_count = resolve_three(&base.link_count, &ours.link_count, &theirs.link_count);
    let (Some(metadata), Some(link_count)) = (metadata, link_count) else {
        state.conflict(MergeConflict::File(directory_id), maximum_conflicts, accounting)?;
        return Ok(None);
    };
    let maximum = state.remaining_changes.max(1);
    let (ours_diff, allocation) = diff_tree_entries_with_accounting(store, Some(base_entries), Some(ours_entries),
        maximum, limits, accounting, cancellation).await?;
    let bytes = ours_diff.retained_allocation_bytes;
    let ours_diff = Owned::new(ours_diff, allocation, bytes);
    let result = async {
        let (theirs_diff, allocation) = diff_tree_entries_with_accounting(store, Some(base_entries), Some(theirs_entries),
            maximum, limits, accounting, cancellation).await?;
        let bytes = theirs_diff.retained_allocation_bytes;
        let theirs_diff = Owned::new(theirs_diff, allocation, bytes);
        let mut mutations = Owned::allocate(0, accounting)?;
        let result = async {
            if ours_diff.value.truncated || theirs_diff.value.truncated { return Err(FsError::MergeChangeLimit); }
            let mut indices = [0, 0];
            let mut conflicted = false;
            while let Some((left, right)) = next_changes(&ours_diff.value.changes, &theirs_diff.value.changes, &mut indices) {
                accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                state.consume(1)?;
                let name = &left.or(right).ok_or(FsError::InvalidDiff)?.key;
                let base_value = left.or(right).and_then(|change| change.before.as_ref());
                let ours_value = left.map_or(base_value, |change| change.after.as_ref());
                let theirs_value = right.map_or(base_value, |change| change.after.as_ref());
                // Resolve borrowed entries; clone only actual retained outputs.
                let mut resolved = if ours_value == theirs_value || theirs_value == base_value { Some(ours_value) }
                    else if ours_value == base_value { Some(theirs_value) } else { None };
                if resolved.is_none() {
                    if let Some(resolution) = requested.binding(directory_id, name, accounting)? {
                        resolved = match resolution {
                            MergeConflictResolution::Select(side) => Some(match side {
                                MergeConflictSide::Base => base_value, MergeConflictSide::Ours => ours_value, MergeConflictSide::Theirs => theirs_value }),
                            MergeConflictResolution::Binding(id) => [base_value, ours_value, theirs_value].into_iter()
                                .find(|entry| entry.map(|entry| entry.file_id) == *id),
                            MergeConflictResolution::File(_) => None,
                        };
                    }
                }
                if resolved.is_none() && let (None, Some(a), Some(b)) = (base_value, ours_value, theirs_value)
                    && a.file_id != b.file_id {
                    if a.kind == super::FileKind::Directory && b.kind == super::FileKind::Directory {
                        state.folds.ensure_for_push(usize::MAX, accounting)?;
                        state.folds.value.push((a.file_id, b.file_id));
                        resolved = Some(ours_value);
                    } else if a.kind != super::FileKind::Directory && b.kind != super::FileKind::Directory {
                        state.file_folds.ensure_for_push(usize::MAX, accounting)?;
                        let (name, allocation) = super::persistent_io::clone_key_with_accounting::<TreeFormat>(name, limits.maximum_name_bytes, accounting)?;
                        let bytes = TreeFormat::key_owned_bytes(&name);
                        state.file_folds.value.push(FileFold { directory_id, name, ours: a.file_id, theirs: b.file_id });
                        absorb(&mut state.file_folds.allocation, allocation)?;
                        state.file_folds.bytes = state.file_folds.bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
                        resolved = Some(ours_value);
                    }
                }
                let Some(resolved) = resolved else {
                    conflicted = true;
                    if state.conflicts.value.len() < maximum_conflicts as usize {
                        state.conflicts.ensure_for_push(maximum_conflicts as usize, accounting)?;
                        let (name, allocation) = super::persistent_io::clone_key_with_accounting::<TreeFormat>(name, limits.maximum_name_bytes, accounting)?;
                        let bytes = TreeFormat::key_owned_bytes(&name);
                        state.conflicts.value.push(MergeConflict::Binding { directory_id, name });
                        absorb(&mut state.conflicts.allocation, allocation)?;
                        state.conflicts.bytes = state.conflicts.bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
                    } else { state.truncated = true; }
                    continue;
                };
                if resolved != ours_value {
                    mutations.ensure_for_push(usize::MAX, accounting)?;
                    let (mutation, allocation, bytes) = match resolved {
                        Some(entry) => {
                            let (entry, allocation) = super::persistent_io::clone_value_with_accounting::<TreeFormat>(entry, limits.maximum_name_bytes, accounting)?;
                            let bytes = TreeFormat::value_owned_bytes(&entry);
                            let mutation = if let Some(before) = ours_value { TreeMutation::Replace { entry, expected_file_id: before.file_id } }
                                else { TreeMutation::Insert(entry) };
                            (mutation, allocation, bytes)
                        }
                        None => {
                            let (name, allocation) = super::persistent_io::clone_key_with_accounting::<TreeFormat>(name, limits.maximum_name_bytes, accounting)?;
                            let bytes = TreeFormat::key_owned_bytes(&name);
                            (TreeMutation::Remove { name, expected_file_id: ours_value.map(|entry| entry.file_id) }, allocation, bytes)
                        }
                    };
                    mutations.value.push(mutation);
                    absorb(&mut mutations.allocation, allocation)?;
                    mutations.bytes = mutations.bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
                }
            }
            if conflicted { return Ok(None); }
            let entries = if mutations.value.is_empty() { ours_entries } else {
                apply_tree_mutations_with_accounting(store, ours_entries, &mutations.value, maximum,
                    limits, accounting, cancellation).await?.root
            };
            Ok(Some(FileRecord { file_id: directory_id, kind: super::FileKind::Directory, link_count, metadata,
                payload: FilePayload::Directory { entries } }))
        }.await;
        mutations.retire(accounting)?;
        theirs_diff.retire(accounting)?;
        result
    }.await;
    ours_diff.retire(accounting)?;
    result
}

async fn fold_additions<S: AsyncObjectStore>(
    store: &S, requested: ResolutionView<'_>, maximum_conflicts: u32,
    limits: DecodeLimits, state: &mut MergeState, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<(), FsError> {
    while let Some((ours_id, theirs_id)) = state.folds.value.pop() {
        let (ours_before, ours) = state.record(ours_id, accounting)?;
        let (theirs_before, theirs) = state.record(theirs_id, accounting)?;
        let (Some(ours), Some(theirs)) = (ours, theirs) else { return Err(FsError::InvalidDiff); };
        let empty = FileRecord { payload: FilePayload::Directory { entries: empty_tree_async(store, limits, accounting, cancellation).await? }, ..ours };
        let record = merge_directory_record_with_resolutions_async(store, ours_id, empty, ours, theirs,
            requested, maximum_conflicts, limits, state, accounting, cancellation).await?;
        if let Some(record) = record { state.replace(ours_id, (ours_before, Some(record)), accounting)?; }
        state.replace(theirs_id, (theirs_before, None), accounting)?;
    }
    state.file_folds.value.reverse();
    while let Some(fold) = state.file_folds.value.pop() {
        let (ours_before, ours) = state.record(fold.ours, accounting)?;
        let (theirs_before, theirs) = state.record(fold.theirs, accounting)?;
        let (Some(ours), Some(theirs)) = (ours, theirs) else { return Err(FsError::InvalidDiff); };
        let metadata = if ours.kind == theirs.kind && ours.payload == theirs.payload && ours.link_count == 1 && theirs.link_count == 1 {
            converge_metadata_async(store, ours.metadata, theirs.metadata, limits, accounting, cancellation).await?
        } else { None };
        let bytes = TreeFormat::key_owned_bytes(&fold.name);
        let allocation = super::persistent_point::split_allocation(&mut state.file_folds.allocation, bytes)?;
        state.file_folds.bytes = state.file_folds.bytes.checked_sub(bytes).ok_or(WorkError::Overflow)?;
        let name = Owned::new(fold.name, allocation, bytes);
        if let Some(metadata) = metadata {
            state.replace(fold.ours, (ours_before, Some(FileRecord { metadata, ..ours })), accounting)?;
            state.replace(fold.theirs, (theirs_before, None), accounting)?;
            name.retire(accounting)?;
        } else if state.conflicts.value.len() < maximum_conflicts as usize {
            state.conflicts.ensure_for_push(maximum_conflicts as usize, accounting)?;
            let Owned { value: name, allocation, bytes } = name;
            state.conflicts.value.push(MergeConflict::Binding { directory_id: fold.directory_id, name });
            absorb(&mut state.conflicts.allocation, allocation)?;
            state.conflicts.bytes = state.conflicts.bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
        } else { state.truncated = true; name.retire(accounting)?; }
    }
    Ok(())
}

struct DirectoryTransition {
    directory: FileId,
    before: Option<ObjectId>,
    after: Option<ObjectId>,
    contribution: Owned<Vec<(FileId, i64)>>,
}

fn add_delta(deltas: &mut Owned<Vec<(FileId, i64)>>, file: FileId, delta: i64, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    match super::search::binary_search_with_accounting(&deltas.value, accounting, |(id, _)| id.cmp(&file))? {
        Ok(index) => {
            let value = &mut deltas.value.get_mut(index).ok_or(FsError::InvalidDiff)?.1;
            *value = value.checked_add(delta).ok_or(WorkError::Overflow)?;
        }
        Err(index) => { deltas.ensure_for_push(usize::MAX, accounting)?; deltas.value.insert(index, (file, delta)); }
    }
    Ok(())
}

async fn adjust_link_counts<S: AsyncObjectStore>(
    store: &S, limits: DecodeLimits, state: &mut MergeState, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<(), FsError> {
    let mut transitions: Owned<Vec<DirectoryTransition>> = Owned::allocate(0, accounting)?;
    let result = async {
        loop {
            accounting.check()?;
            let mut deltas = Owned::allocate(0, accounting)?;
            let result = async {
                for (directory, (before, after)) in &state.records.value {
                    let before = directory_entries(*before);
                    let after = directory_entries(*after);
                    let cached = super::search::binary_search_with_accounting(&transitions.value, accounting, |entry| entry.directory.cmp(directory))?;
                    if before == after {
                        if let Ok(index) = cached { transitions.value.remove(index).contribution.retire(accounting)?; }
                        continue;
                    }
                    let unchanged = cached.ok().and_then(|index| transitions.value.get(index))
                        .is_some_and(|entry| entry.before == before && entry.after == after);
                    if !unchanged {
                        if state.remaining_changes == 0 { return Err(FsError::MergeChangeLimit); }
                        let (diff, allocation) = diff_tree_entries_with_accounting(store, before, after,
                            state.remaining_changes, limits, accounting, cancellation).await?;
                        let bytes = diff.retained_allocation_bytes;
                        let diff = Owned::new(diff, allocation, bytes);
                        let mut contribution = Owned::allocate(0, accounting)?;
                        let result = (|| {
                            if diff.value.truncated { return Err(FsError::MergeChangeLimit); }
                            for change in &diff.value.changes {
                                if let Some(before) = &change.before { add_delta(&mut contribution, before.file_id, -1, accounting)?; }
                                if let Some(after) = &change.after { add_delta(&mut contribution, after.file_id, 1, accounting)?; }
                            }
                            Ok(())
                        })();
                        let count = diff.value.changes.len();
                        diff.retire(accounting)?;
                        if let Err(error) = result { contribution.retire(accounting)?; return Err(error); }
                        state.remaining_changes = state.remaining_changes.checked_sub(count as u32).ok_or(FsError::MergeChangeLimit)?;
                        let entry = DirectoryTransition { directory: *directory, before, after, contribution };
                        match cached {
                            Ok(index) => {
                                let previous = std::mem::replace(transitions.value.get_mut(index).ok_or(FsError::InvalidDiff)?, entry);
                                previous.contribution.retire(accounting)?;
                            }
                            Err(index) => { transitions.ensure_for_push(usize::MAX, accounting)?; transitions.value.insert(index, entry); }
                        }
                    }
                    let index = super::search::binary_search_with_accounting(&transitions.value, accounting, |entry| entry.directory.cmp(directory))?
                        .map_err(|_| FsError::InvalidDiff)?;
                    for (file, delta) in &transitions.value.get(index).ok_or(FsError::InvalidDiff)?.contribution.value {
                        add_delta(&mut deltas, *file, *delta, accounting)?;
                    }
                }
                for (file, delta) in &deltas.value {
                    if *delta != 0 { state.record(*file, accounting)?; }
                }
                let mut pruned = false;
                for (file, (ours, resolved)) in &mut state.records.value {
                    let delta = super::search::binary_search_with_accounting(&deltas.value, accounting, |(id, _)| id.cmp(file))?
                        .ok().and_then(|index| deltas.value.get(index)).map_or(0, |(_, delta)| *delta);
                    let links = i128::from(ours.map_or(0, |record| record.link_count)).checked_add(i128::from(delta))
                        .and_then(|value| u64::try_from(value).ok()).ok_or(FsError::InvalidDiff)?;
                    if resolved.is_none() {
                        if links != 0 { return Err(FsError::InvalidDiff); }
                    } else if links == 0 {
                        pruned |= is_directory(*resolved);
                        *resolved = None;
                    } else if !pruned && let Some(record) = resolved { record.link_count = links; }
                }
                Ok(pruned)
            }.await;
            deltas.retire(accounting)?;
            if !result? { return Ok(()); }
        }
    }.await;
    for transition in transitions.value.drain(..) { transition.contribution.retire(accounting)?; }
    transitions.retire(accounting)?;
    result
}

enum OptionalResolution {
    Resolved(Option<FileRecord>),
    Conflict,
}

fn resolve_optional(
    base: Option<FileRecord>,
    ours: Option<FileRecord>,
    theirs: Option<FileRecord>,
) -> OptionalResolution {
    if ours == theirs || theirs == base {
        OptionalResolution::Resolved(ours)
    } else if ours == base {
        OptionalResolution::Resolved(theirs)
    } else {
        OptionalResolution::Conflict
    }
}

fn select_optional(
    side: MergeConflictSide,
    base: Option<FileRecord>,
    ours: Option<FileRecord>,
    theirs: Option<FileRecord>,
) -> Option<FileRecord> {
    match side {
        MergeConflictSide::Base => base,
        MergeConflictSide::Ours => ours,
        MergeConflictSide::Theirs => theirs,
    }
}

fn select_value<T: Clone>(side: MergeConflictSide, base: &T, ours: &T, theirs: &T) -> T {
    match side {
        MergeConflictSide::Base => base.clone(),
        MergeConflictSide::Ours => ours.clone(),
        MergeConflictSide::Theirs => theirs.clone(),
    }
}

pub(crate) fn resolve_three<T: Clone + Eq>(base: &T, ours: &T, theirs: &T) -> Option<T> {
    if ours == theirs || theirs == base {
        Some(ours.clone())
    } else if ours == base {
        Some(theirs.clone())
    } else {
        None
    }
}

#[cfg(test)]
pub(crate) fn merge_file_fields(
    base: FileRecord,
    ours: FileRecord,
    theirs: FileRecord,
) -> Option<FileRecord> {
    Some(FileRecord {
        file_id: ours.file_id,
        kind: resolve_three(&base.kind, &ours.kind, &theirs.kind)?,
        link_count: ours.link_count,
        metadata: resolve_three(&base.metadata, &ours.metadata, &theirs.metadata)?,
        payload: resolve_three(&base.payload, &ours.payload, &theirs.payload)?,
    })
}

async fn merge_file_fields_async<S: AsyncObjectStore>(
    store: &S, base: FileRecord, ours: FileRecord, theirs: FileRecord,
    limits: DecodeLimits, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<Option<FileRecord>, FsError> {
    let (Some(kind), Some(payload)) = (
        resolve_three(&base.kind, &ours.kind, &theirs.kind),
        resolve_three(&base.payload, &ours.payload, &theirs.payload),
    ) else { return Ok(None); };
    let metadata = merge_metadata_async(store, Some(base.metadata), ours.metadata, theirs.metadata, limits, accounting, cancellation).await?;
    Ok(metadata.map(|metadata| FileRecord { file_id: ours.file_id, kind, link_count: ours.link_count, metadata, payload }))
}

async fn merge_records_async<S: AsyncObjectStore>(
    store: &S, [base, ours, theirs]: [Option<FileRecord>; 3],
    limits: DecodeLimits, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<Option<FileRecord>, FsError> {
    let (Some(ours), Some(theirs)) = (ours, theirs) else { return Ok(None); };
    if let Some(base) = base {
        return merge_file_fields_async(store, base, ours, theirs, limits, accounting, cancellation).await;
    }
    if ours.kind != theirs.kind || ours.link_count != theirs.link_count || ours.payload != theirs.payload { return Ok(None); }
    let metadata = merge_metadata_async(store, None, ours.metadata, theirs.metadata, limits, accounting, cancellation).await?;
    Ok(metadata.map(|metadata| FileRecord { metadata, ..ours }))
}

async fn merge_metadata_async<S: AsyncObjectStore>(
    store: &S, base: Option<ObjectId>, ours: ObjectId, theirs: ObjectId,
    limits: DecodeLimits, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<Option<ObjectId>, FsError> {
    if ours == theirs { return Ok(Some(ours)); }
    if let Some(resolved) = base.and_then(|base| resolve_three(&base, &ours, &theirs)) { return Ok(Some(resolved)); }
    let ours_fields = read_metadata(store, ours, limits, accounting, cancellation).await?;
    let theirs_fields = read_metadata(store, theirs, limits, accounting, cancellation).await?;
    let (merged, base_fields) = if let Some(base) = base {
        let fields = read_metadata(store, base, limits, accounting, cancellation).await?;
        (merge_metadata_fields(fields, ours_fields, theirs_fields), Some(fields))
    } else { (merge_added_metadata_fields(ours_fields, theirs_fields), None) };
    let Some(merged) = merged else { return Ok(None); };
    if merged == ours_fields { return Ok(Some(ours)); }
    if merged == theirs_fields { return Ok(Some(theirs)); }
    if Some(merged) == base_fields { return Ok(base); }
    write_metadata(store, merged, accounting, cancellation).await.map(Some)
}

async fn write_metadata<S: AsyncObjectStore>(
    store: &S, metadata: FileMetadata, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<ObjectId, FsError> {
    let (encoded, allocation) = crate::kernel::codec::encode_owned(accounting, |observe, reserve| super::metadata::encode_file_metadata(metadata, observe, reserve))?;
    let bytes = crate::foundation::usize_to_u64(encoded.capacity());
    put_encoded_with_accounting(store, ObjectKind::Metadata, Owned::new(encoded, allocation, bytes), accounting, cancellation).await
}

async fn empty_tree_async<S: AsyncObjectStore>(
    store: &S, limits: DecodeLimits, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<ObjectId, FsError> {
    let (encoded, allocation) = crate::kernel::codec::encode_owned(accounting, |observe, _reserve| { cancellation.check()?; super::tree::encode_tree_page(&super::TreePage::Leaf(Vec::new()), limits.maximum_page_items, observe, crate::FsError::EncodedAllocationFailed) })?;
    let bytes = crate::foundation::usize_to_u64(encoded.capacity());
    put_encoded_with_accounting(store, ObjectKind::TreePage, Owned::new(encoded, allocation, bytes), accounting, cancellation).await
}

async fn read_metadata<S: AsyncObjectStore>(
    store: &S, object: ObjectId, limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<FileMetadata, FsError> {
    let (read, retained) = super::file_read::read_object_with_accounting(store, object, limits.maximum_object_bytes, accounting, cancellation).await?;
    let result = (|| {
        accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        decode_file_metadata(&read.bytes, limits, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(())).map_err(FsError::from)
    })();
    drop(read);
    // Native provider bytes own their original guard internally; only the
    // standalone provider retention remains on the caller's ledger.
    if retained != 0 { retire_allocation(KernelOwnedAllocation::Standalone, retained, accounting)?; }
    result
}

async fn converge_metadata_async<S: AsyncObjectStore>(
    store: &S, ours: ObjectId, theirs: ObjectId, limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<Option<ObjectId>, FsError> {
    // The added-record rule is exactly authored fields agree / later times.
    merge_metadata_async(store, None, ours, theirs, limits, accounting, cancellation).await
}

pub(crate) fn merge_metadata_fields(
    base: FileMetadata,
    ours: FileMetadata,
    theirs: FileMetadata,
) -> Option<FileMetadata> {
    fn later(
        base: &MetadataField<i64>,
        ours: &MetadataField<i64>,
        theirs: &MetadataField<i64>,
    ) -> MetadataField<i64> {
        resolve_three(base, ours, theirs).unwrap_or(match (ours, theirs) {
            (MetadataField::Value(ours), MetadataField::Value(theirs)) => {
                MetadataField::Value(*ours.max(theirs))
            }
            (MetadataField::Value(value), MetadataField::Unavailable)
            | (MetadataField::Unavailable, MetadataField::Value(value)) => {
                MetadataField::Value(*value)
            }
            (MetadataField::Unavailable, MetadataField::Unavailable) => MetadataField::Unavailable,
        })
    }
    Some(FileMetadata {
        posix_mode: resolve_three(&base.posix_mode, &ours.posix_mode, &theirs.posix_mode)?,
        posix_uid: resolve_three(&base.posix_uid, &ours.posix_uid, &theirs.posix_uid)?,
        posix_gid: resolve_three(&base.posix_gid, &ours.posix_gid, &theirs.posix_gid)?,
        posix_flags: resolve_three(&base.posix_flags, &ours.posix_flags, &theirs.posix_flags)?,
        windows_attributes: resolve_three(
            &base.windows_attributes,
            &ours.windows_attributes,
            &theirs.windows_attributes,
        )?,
        created_ns: later(&base.created_ns, &ours.created_ns, &theirs.created_ns),
        modified_ns: later(&base.modified_ns, &ours.modified_ns, &theirs.modified_ns),
        accessed_ns: later(&base.accessed_ns, &ours.accessed_ns, &theirs.accessed_ns),
        changed_ns: later(&base.changed_ns, &ours.changed_ns, &theirs.changed_ns),
        named_attributes: resolve_three(
            &base.named_attributes,
            &ours.named_attributes,
            &theirs.named_attributes,
        )?,
        acl: resolve_three(&base.acl, &ours.acl, &theirs.acl)?,
        security_descriptor: resolve_three(
            &base.security_descriptor,
            &ours.security_descriptor,
            &theirs.security_descriptor,
        )?,
    })
}

pub(crate) fn merge_added_metadata_fields(
    ours: FileMetadata,
    theirs: FileMetadata,
) -> Option<FileMetadata> {
    fn same<T: Copy + PartialEq>(ours: T, theirs: T) -> Option<T> {
        (ours == theirs).then_some(ours)
    }
    fn later(ours: MetadataField<i64>, theirs: MetadataField<i64>) -> MetadataField<i64> {
        match (ours, theirs) {
            (MetadataField::Value(ours), MetadataField::Value(theirs)) => {
                MetadataField::Value(ours.max(theirs))
            }
            (MetadataField::Value(value), MetadataField::Unavailable)
            | (MetadataField::Unavailable, MetadataField::Value(value)) => {
                MetadataField::Value(value)
            }
            (MetadataField::Unavailable, MetadataField::Unavailable) => MetadataField::Unavailable,
        }
    }
    Some(FileMetadata {
        posix_mode: same(ours.posix_mode, theirs.posix_mode)?,
        posix_uid: same(ours.posix_uid, theirs.posix_uid)?,
        posix_gid: same(ours.posix_gid, theirs.posix_gid)?,
        posix_flags: same(ours.posix_flags, theirs.posix_flags)?,
        windows_attributes: same(ours.windows_attributes, theirs.windows_attributes)?,
        created_ns: later(ours.created_ns, theirs.created_ns),
        modified_ns: later(ours.modified_ns, theirs.modified_ns),
        accessed_ns: later(ours.accessed_ns, theirs.accessed_ns),
        changed_ns: later(ours.changed_ns, theirs.changed_ns),
        named_attributes: same(ours.named_attributes, theirs.named_attributes)?,
        acl: same(ours.acl, theirs.acl)?,
        security_descriptor: same(ours.security_descriptor, theirs.security_descriptor)?,
    })
}

fn is_directory(record: Option<FileRecord>) -> bool {
    matches!(record, Some(record) if record.kind == super::FileKind::Directory)
}
fn directory_entries(record: Option<FileRecord>) -> Option<ObjectId> {
    match record?.payload {
        FilePayload::Directory { entries } => Some(entries),
        _ => None,
    }
}
pub(crate) fn file_table_mutation(
    file_id: FileId,
    before: Option<FileRecord>,
    after: Option<FileRecord>,
) -> Option<FileTableMutation> {
    match (before, after) {
        (None, Some(record)) => Some(FileTableMutation::Insert(record)),
        (Some(expected), None) => Some(FileTableMutation::Remove {
            file_id,
            expected: Some(expected),
        }),
        (Some(expected), Some(replacement)) => Some(FileTableMutation::Replace {
            expected,
            replacement,
        }),
        (None, None) => None,
    }
}
#[cfg(test)]
pub(crate) fn tree_mutation(
    name: LogicalName,
    before: Option<super::TreeEntry>,
    after: Option<super::TreeEntry>,
) -> Option<TreeMutation> {
    match (before, after) {
        (None, Some(entry)) => Some(TreeMutation::Insert(entry)),
        (Some(expected), None) => Some(TreeMutation::Remove {
            name,
            expected_file_id: Some(expected.file_id),
        }),
        (Some(expected), Some(entry)) => Some(TreeMutation::Replace {
            entry,
            expected_file_id: expected.file_id,
        }),
        (None, None) => None,
    }
}

fn project_merge_error(error: FsError) -> MergeGenerationError {
    match error {
        FsError::InvalidDiff => MergeGenerationError::InvalidDiff,
        FsError::MergeChangeLimit => MergeGenerationError::ChangeLimit,
        FsError::DiffAllocationFailed => MergeGenerationError::AllocationFailed,
        FsError::Object(error) => MergeGenerationError::Object(error),
        FsError::BlobRead(super::BlobReadError::Storage(error)) => MergeGenerationError::Object(error),
        FsError::Decode(error) => MergeGenerationError::Decode(error),
        FsError::MergeFileTable(error) => MergeGenerationError::FileTable(error),
        FsError::MergeTree(error) => MergeGenerationError::Tree(error),
        FsError::Checkpoint(error) => MergeGenerationError::Checkpoint(error),
        FsError::FileRead(super::FileRangeReadError::Extent(error)) => MergeGenerationError::ExtentRead(error),
        FsError::DetachedMutation(super::RegularMutationError::Extent(error)) => MergeGenerationError::ExtentMutation(error),
        FsError::Cancelled(error) => MergeGenerationError::Cancelled(error),
        FsError::Work(error) => MergeGenerationError::Work(error),
        error => MergeGenerationError::Kernel(error),
    }
}

#[cfg(test)]
#[path = "tests/merge.rs"]
mod tests;
