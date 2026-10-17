//! Canonical bounded three-way generation merge.

use super::allocation::{KernelAccounting, KernelOwnedAllocation};
use super::checkpoint::build_checkpoint_with_accounting;
use super::extent_mutation::{apply_extent_mutations_with_accounting, put_encoded_with_accounting};
use super::file_table_mutation::apply_file_table_mutations_with_accounting;
use super::persistent_btree::Format;
use super::persistent_diff::{
    diff_file_records_with_accounting, diff_tree_entries_with_accounting,
};
use super::persistent_point::{Owned, retire_allocation};
use super::range::plan_extent_range_with_accounting;
use super::tree_mutation::{TreeFormat, apply_tree_mutations_with_accounting};
use super::{
    CanonicalDecodeError, CheckpointError, CheckpointRequest, DecodeLimits, ExtentKind,
    ExtentMutation, ExtentMutationError, ExtentRangeRequest, ExtentReadError, ExtentSlice,
    FileMetadata, FilePayload, FileRecord, FileTableMutation, FileTableMutationError,
    GenerationRoot, LogicalName, MetadataField, TreeMutation, TreeMutationError,
    decode_file_metadata,
};
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
    #[cfg(feature = "native-mount")]
    DistinctAdditions,
    Map(&'a BTreeMap<MergeConflict, MergeConflictResolution>),
    #[cfg(not(target_arch = "wasm32"))]
    Slice(&'a [(MergeConflict, MergeConflictResolution)]),
}

impl<'a> ResolutionView<'a> {
    fn converges_additions(self) -> bool {
        match self {
            #[cfg(feature = "native-mount")]
            Self::DistinctAdditions => false,
            _ => true,
        }
    }

    fn get_by(
        self,
        accounting: &mut KernelAccounting<'_>,
        mut compare: impl FnMut(&MergeConflict) -> std::cmp::Ordering,
    ) -> Result<Option<&'a MergeConflictResolution>, FsError> {
        match self {
            #[cfg(feature = "native-mount")]
            Self::DistinctAdditions => Ok(None),
            Self::Map(map) => {
                for (key, value) in map {
                    accounting.charge(WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::UNCHARGED
                    })?;
                    match compare(key) {
                        std::cmp::Ordering::Less => {}
                        std::cmp::Ordering::Equal => return Ok(Some(value)),
                        std::cmp::Ordering::Greater => break,
                    }
                }
                Ok(None)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Slice(slice) => {
                let index =
                    super::search::binary_search_with_accounting(slice, accounting, |(key, _)| {
                        compare(key)
                    })?;
                Ok(index
                    .ok()
                    .and_then(|index| slice.get(index))
                    .map(|(_, value)| value))
            }
        }
    }

    fn file(
        self,
        file: FileId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<&'a MergeConflictResolution>, FsError> {
        self.get_by(accounting, |key| key.cmp(&MergeConflict::File(file)))
    }

    fn binding(
        self,
        directory: FileId,
        name: &LogicalName,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<&'a MergeConflictResolution>, FsError> {
        self.get_by(accounting, |key| match key {
            MergeConflict::File(_) => std::cmp::Ordering::Less,
            MergeConflict::Binding {
                directory_id,
                name: key,
            } => directory_id.cmp(&directory).then_with(|| key.cmp(name)),
        })
    }

    fn has_bindings(
        self,
        directory: FileId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, FsError> {
        Ok(self
            .get_by(accounting, |key| match key {
                MergeConflict::File(_) => std::cmp::Ordering::Less,
                MergeConflict::Binding { directory_id, .. } => directory_id.cmp(&directory),
            })?
            .is_some())
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
    store: &S,
    request: MergeGenerationRequest,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> MergeGenerationResult {
    let span = crate::obs::span!(
        DEBUG,
        "acyclic.fs.kernel.merge",
        outcome = crate::obs::Empty,
        error.kind = crate::obs::Empty,
        work.items = crate::obs::Empty,
        work.bytes = crate::obs::Empty,
        work.durability = crate::obs::Empty
    );
    let result = crate::obs::in_span(&span, async {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result =
            merge_generation_with_accounting(store, request, limits, &mut accounting, cancellation)
                .await;
        let work = accounting.spent();
        result
            .map(|(value, _)| OperationReceipt { value, work })
            .map_err(|error| OperationFailure::new(project_merge_error(error), work))
    })
    .await;
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
    fn record(
        &self,
        file: FileId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<RecordChange, FsError> {
        let index = super::search::binary_search_with_accounting(
            &self.records.value,
            accounting,
            |(id, _)| id.cmp(&file),
        )?
        .map_err(|_| FsError::InvalidDiff)?;
        Ok(self.records.value.get(index).ok_or(FsError::InvalidDiff)?.1)
    }
    fn replace(
        &mut self,
        file: FileId,
        value: RecordChange,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let index = super::search::binary_search_with_accounting(
            &self.records.value,
            accounting,
            |(id, _)| id.cmp(&file),
        )?
        .map_err(|_| FsError::InvalidDiff)?;
        self.records
            .value
            .get_mut(index)
            .ok_or(FsError::InvalidDiff)?
            .1 = value;
        Ok(())
    }
    fn consume(&mut self, count: usize) -> Result<(), FsError> {
        self.remaining_changes = self
            .remaining_changes
            .checked_sub(u32::try_from(count).map_err(|_| FsError::MergeChangeLimit)?)
            .ok_or(FsError::MergeChangeLimit)?;
        Ok(())
    }
    fn conflict(
        &mut self,
        conflict: MergeConflict,
        maximum: u32,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        if self.conflicts.value.len() < usize::try_from(maximum).unwrap_or(usize::MAX) {
            self.conflicts
                .ensure_for_push(usize::try_from(maximum).unwrap_or(usize::MAX), accounting)?;
            self.conflicts.value.push(conflict);
        } else {
            self.truncated = true;
        }
        Ok(())
    }
}

pub(crate) async fn merge_generation_with_accounting<S: AsyncObjectStore>(
    store: &S,
    request: MergeGenerationRequest,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(MergeGenerationOutcome, KernelOwnedAllocation), FsError> {
    let inputs = MergeGenerationBorrowedRequest {
        base_generation: request.base_generation,
        base: &request.base,
        ours_generation: request.ours_generation,
        ours: &request.ours,
        theirs_generation: request.theirs_generation,
        theirs: &request.theirs,
        retain_theirs_parent: request.retain_theirs_parent,
        maximum_changes: request.maximum_changes,
        maximum_conflicts: request.maximum_conflicts,
    };
    merge_generation_core(
        store,
        inputs,
        ResolutionView::Map(&request.resolutions),
        limits,
        accounting,
        cancellation,
    )
    .await
}

pub(crate) async fn merge_generation_with_borrowed_map_resolutions_with_accounting<
    S: AsyncObjectStore,
>(
    store: &S,
    request: &MergeGenerationBorrowedRequest<'_>,
    resolutions: &BTreeMap<MergeConflict, MergeConflictResolution>,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(MergeGenerationOutcome, KernelOwnedAllocation), FsError> {
    merge_generation_core(
        store,
        *request,
        ResolutionView::Map(resolutions),
        limits,
        accounting,
        cancellation,
    )
    .await
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn merge_generation_with_borrowed_resolutions_with_accounting<
    S: AsyncObjectStore,
>(
    store: &S,
    request: MergeGenerationBorrowedRequest<'_>,
    resolutions: &[(MergeConflict, MergeConflictResolution)],
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(MergeGenerationOutcome, KernelOwnedAllocation), FsError> {
    for [left, right] in resolutions.array_windows::<2>() {
        accounting.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
        if left.0 >= right.0 {
            return Err(FsError::InvalidDiff);
        }
    }
    merge_generation_core(
        store,
        request,
        ResolutionView::Slice(resolutions),
        limits,
        accounting,
        cancellation,
    )
    .await
}

include!("merge/tables.rs");
include!("merge/checkpoint.rs");
include!("merge/regular.rs");
include!("merge/directory.rs");
include!("merge/links.rs");
include!("merge/fields.rs");

fn project_merge_error(error: FsError) -> MergeGenerationError {
    match error {
        FsError::InvalidDiff => MergeGenerationError::InvalidDiff,
        FsError::MergeChangeLimit => MergeGenerationError::ChangeLimit,
        FsError::DiffAllocationFailed => MergeGenerationError::AllocationFailed,
        FsError::Object(error) | FsError::BlobRead(super::BlobReadError::Storage(error)) => {
            MergeGenerationError::Object(error)
        }
        FsError::Decode(error) => MergeGenerationError::Decode(error),
        FsError::MergeFileTable(error) => MergeGenerationError::FileTable(error),
        FsError::MergeTree(error) => MergeGenerationError::Tree(error),
        FsError::Checkpoint(error) => MergeGenerationError::Checkpoint(error),
        FsError::FileRead(super::FileRangeReadError::Extent(error)) => {
            MergeGenerationError::ExtentRead(error)
        }
        FsError::DetachedMutation(super::RegularMutationError::Extent(error)) => {
            MergeGenerationError::ExtentMutation(error)
        }
        FsError::Cancelled(error) => MergeGenerationError::Cancelled(error),
        FsError::Work(error) => MergeGenerationError::Work(error),
        error => MergeGenerationError::Kernel(error),
    }
}

#[cfg(test)]
#[path = "tests/merge/mod.rs"]
mod tests;
