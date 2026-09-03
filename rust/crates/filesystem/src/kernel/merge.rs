//! Canonical bounded three-way generation merge.

use super::{
    CanonicalDecodeError, CheckpointError, CheckpointRequest, DecodeLimits, ExtentKind,
    ExtentMutation, ExtentMutationError, ExtentMutationOptions, ExtentRangeRequest,
    ExtentReadError, ExtentSlice, FilePayload, FileRecord, FileTableMutation,
    FileTableMutationError, GenerationRoot, LogicalName, PersistentDiffError, TreeMutation,
    TreeMutationError, apply_extent_mutations_async, apply_file_table_mutations_async,
    apply_tree_mutations_async, build_checkpoint_async, diff_file_records_async,
    diff_tree_entries_async, plan_extent_range_async,
};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::{CancellationError, CancellationToken};
use crate::foundation::{FileId, GenerationId};
use crate::performance::{
    MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError,
};
use crate::storage::{ByteRange, ObjectId, ObjectStoreError};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

/// One exact unresolved three-way merge region.
#[derive(Clone, Debug, Eq, PartialEq)]
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

/// Immutable inputs for one bounded generation merge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MergeGenerationRequest {
    /// Immutable common ancestor object.
    pub base_generation: ObjectId,
    /// Authenticated common ancestor root.
    pub base: GenerationRoot,
    /// Exact authenticated object for `ours`, when it is already immutable.
    /// Omitting it creates the required parent checkpoint for a private candidate.
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
#[derive(Debug, Error)]
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
}

/// Receipt-bearing merge result.
pub type MergeGenerationResult =
    MeasuredResult<OperationReceipt<MergeGenerationOutcome>, MergeGenerationError>;

/// Performs one sparse bounded three-way merge and constructs its two-parent checkpoint.
///
/// Equal Merkle roots are skipped by the shared diff kernels. The complete
/// algorithm, including directory binding resolution and hard-link deltas, is
/// owned here so every facade and language binding has identical semantics and
/// physical work accounting.
///
/// # Errors
///
/// Rejects malformed or foreign roots, zero/truncated frontiers, cancellation,
/// storage corruption, sparse application, checkpoint, or work-budget failures.
#[allow(clippy::too_many_lines)]
pub async fn merge_generation_async<S: AsyncObjectStore>(
    store: &S,
    request: MergeGenerationRequest,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> MergeGenerationResult {
    cancellation
        .check()
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    if request.maximum_changes == 0 || request.maximum_conflicts == 0 {
        return Err(OperationFailure::before_work(
            MergeGenerationError::ChangeLimit,
        ));
    }
    if request.base.volume_id != request.ours.volume_id
        || request.base.volume_id != request.theirs.volume_id
        || request.base.root_file_id != request.ours.root_file_id
        || request.base.root_file_id != request.theirs.root_file_id
    {
        return Err(OperationFailure::before_work(
            MergeGenerationError::InvalidDiff,
        ));
    }
    let mut work = WorkCounters::default();
    let ours = diff_file_records_async(
        store,
        Some(request.base.file_table),
        Some(request.ours.file_table),
        request.maximum_changes,
        limits,
        remaining(work, budget)?,
        cancellation,
    )
    .await
    .map_err(|failure| map_diff_failure(failure, work))?;
    work = add(work, ours.work)?;
    let theirs = diff_file_records_async(
        store,
        Some(request.base.file_table),
        Some(request.theirs.file_table),
        request.maximum_changes,
        limits,
        remaining(work, budget)?,
        cancellation,
    )
    .await
    .map_err(|failure| map_diff_failure(failure, work))?;
    work = add(work, theirs.work)?;
    if ours.truncated || theirs.truncated {
        return Err(OperationFailure::new(
            MergeGenerationError::ChangeLimit,
            work,
        ));
    }
    let ours = changes_by_file(ours.changes);
    let theirs = changes_by_file(theirs.changes);
    let identities = ours
        .keys()
        .chain(theirs.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    if identities.len() > usize::try_from(request.maximum_changes).unwrap_or(usize::MAX) {
        return Err(OperationFailure::new(
            MergeGenerationError::ChangeLimit,
            work,
        ));
    }
    let mut remaining_changes = request
        .maximum_changes
        .checked_sub(u32::try_from(identities.len()).unwrap_or(u32::MAX))
        .ok_or_else(|| OperationFailure::new(MergeGenerationError::ChangeLimit, work))?;
    let mut resolutions = BTreeMap::new();
    let mut conflicts = Vec::new();
    let mut truncated = false;
    for file_id in identities {
        let base = ours
            .get(&file_id)
            .map(|change| change.0)
            .or_else(|| theirs.get(&file_id).map(|change| change.0))
            .flatten();
        let ours_value = ours.get(&file_id).map_or(base, |change| change.1);
        let theirs_value = theirs.get(&file_id).map_or(base, |change| change.1);
        let mut resolved = resolve_optional(base, ours_value, theirs_value);
        if matches!(resolved, OptionalResolution::Conflict)
            && is_directory(base)
            && is_directory(ours_value)
            && is_directory(theirs_value)
        {
            if remaining_changes == 0 {
                return Err(OperationFailure::new(
                    MergeGenerationError::ChangeLimit,
                    work,
                ));
            }
            let directory = merge_directory_record_async(
                store,
                file_id,
                base.ok_or_else(|| invalid(work))?,
                ours_value.ok_or_else(|| invalid(work))?,
                theirs_value.ok_or_else(|| invalid(work))?,
                remaining_changes,
                request
                    .maximum_conflicts
                    .saturating_sub(u32::try_from(conflicts.len()).unwrap_or(u32::MAX)),
                limits,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
            work = add(work, directory.work)?;
            remaining_changes = remaining_changes
                .checked_sub(directory.value.examined_changes)
                .ok_or_else(|| OperationFailure::new(MergeGenerationError::ChangeLimit, work))?;
            conflicts.extend(directory.value.conflicts);
            truncated |= directory.value.truncated;
            let Some(record) = directory.value.record else {
                continue;
            };
            resolved = OptionalResolution::Resolved(Some(record));
        }
        if matches!(resolved, OptionalResolution::Conflict)
            && base.is_some()
            && ours_value.is_some()
            && theirs_value.is_some()
            && is_extent_regular(base)
            && is_extent_regular(ours_value)
            && is_extent_regular(theirs_value)
        {
            let regular = merge_regular_record_async(
                store,
                base.ok_or_else(|| invalid(work))?,
                ours_value.ok_or_else(|| invalid(work))?,
                theirs_value.ok_or_else(|| invalid(work))?,
                remaining_changes,
                limits,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
            work = add(work, regular.work)?;
            if let Some((record, examined)) = regular.value {
                remaining_changes = remaining_changes.checked_sub(examined).ok_or_else(|| {
                    OperationFailure::new(MergeGenerationError::ChangeLimit, work)
                })?;
                resolved = OptionalResolution::Resolved(Some(record));
            }
        }
        if matches!(resolved, OptionalResolution::Conflict)
            && base.is_some()
            && ours_value.is_some()
            && theirs_value.is_some()
        {
            resolved = merge_file_fields(
                base.ok_or_else(|| invalid(work))?,
                ours_value.ok_or_else(|| invalid(work))?,
                theirs_value.ok_or_else(|| invalid(work))?,
            )
            .map_or(OptionalResolution::Conflict, |record| {
                OptionalResolution::Resolved(Some(record))
            });
        }
        let OptionalResolution::Resolved(resolved) = resolved else {
            if conflicts.len() < usize::try_from(request.maximum_conflicts).unwrap_or(usize::MAX) {
                conflicts.push(MergeConflict::File(file_id));
            } else {
                truncated = true;
            }
            continue;
        };
        resolutions.insert(file_id, (ours_value, resolved));
    }
    if !conflicts.is_empty() || truncated {
        return Ok(OperationReceipt {
            value: MergeGenerationOutcome::Conflicted {
                conflicts,
                truncated,
            },
            work,
        });
    }
    adjust_link_counts(
        store,
        &mut resolutions,
        &mut remaining_changes,
        limits,
        budget,
        cancellation,
        &mut work,
    )
    .await?;
    let mutations = resolutions
        .into_iter()
        .filter(|(_, (before, after))| before != after)
        .filter_map(|(file_id, (before, after))| file_table_mutation(file_id, before, after))
        .collect::<Vec<_>>();
    let merged_table = if mutations.is_empty() {
        request.ours.file_table
    } else {
        let applied = apply_file_table_mutations_async(
            store,
            request.ours.file_table,
            mutations,
            request.maximum_changes,
            limits,
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, MergeGenerationError::FileTable))?;
        work = add(work, applied.work)?;
        applied.root
    };
    let (ours_parent_root, ours_parent_generation) = match request.ours_generation {
        Some(object) if object.kind == crate::storage::ObjectKind::GenerationRoot => {
            (object, GenerationId::new(object.digest))
        }
        Some(_) => {
            return Err(OperationFailure::new(
                MergeGenerationError::InvalidDiff,
                work,
            ));
        }
        None => {
            let ours_parent = build_checkpoint_async(
                store,
                CheckpointRequest {
                    base: request.base_generation,
                    file_table: request.ours.file_table,
                    merge_parent: None,
                },
                limits,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| {
                failure.map_with_prior_work(work, MergeGenerationError::Checkpoint)
            })?;
            work = add(work, ours_parent.work)?;
            (ours_parent.root, ours_parent.generation_id)
        }
    };
    let merged = build_checkpoint_async(
        store,
        CheckpointRequest {
            base: ours_parent_root,
            file_table: merged_table,
            merge_parent: request
                .retain_theirs_parent
                .then_some(request.theirs_generation),
        },
        limits,
        remaining(work, budget)?,
        cancellation,
    )
    .await
    .map_err(|failure| failure.map_with_prior_work(work, MergeGenerationError::Checkpoint))?;
    work = add(work, merged.work)?;
    let root = GenerationRoot {
        volume_id: request.ours.volume_id,
        root_file_id: request.ours.root_file_id,
        file_table: merged_table,
        parents: if request.retain_theirs_parent {
            vec![
                ours_parent_generation,
                GenerationId::new(request.theirs_generation.digest),
            ]
        } else {
            vec![ours_parent_generation]
        },
        required_features: request.ours.required_features,
    };
    Ok(OperationReceipt {
        value: MergeGenerationOutcome::Prepared {
            generation_root: merged.root,
            root,
            generation_id: merged.generation_id,
        },
        work,
    })
}

fn is_extent_regular(record: Option<FileRecord>) -> bool {
    matches!(
        record.map(|value| value.payload),
        Some(FilePayload::Regular { .. })
    )
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn merge_regular_record_async<S: AsyncObjectStore>(
    store: &S,
    base: FileRecord,
    ours: FileRecord,
    theirs: FileRecord,
    maximum_changes: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<Option<(FileRecord, u32)>>, OperationFailure<MergeGenerationError>> {
    let Some(kind) = resolve_three(&base.kind, &ours.kind, &theirs.kind) else {
        return Ok(OperationReceipt {
            value: None,
            work: WorkCounters::default(),
        });
    };
    let Some(metadata) = resolve_three(&base.metadata, &ours.metadata, &theirs.metadata) else {
        return Ok(OperationReceipt {
            value: None,
            work: WorkCounters::default(),
        });
    };
    let (
        FilePayload::Regular {
            logical_bytes: base_bytes,
            extents: base_root,
        },
        FilePayload::Regular {
            logical_bytes: ours_bytes,
            extents: ours_root,
        },
        FilePayload::Regular {
            logical_bytes: theirs_bytes,
            extents: theirs_root,
        },
    ) = (base.payload, ours.payload, theirs.payload)
    else {
        return Ok(OperationReceipt {
            value: None,
            work: WorkCounters::default(),
        });
    };
    let Some(logical_bytes) = resolve_three(&base_bytes, &ours_bytes, &theirs_bytes) else {
        return Ok(OperationReceipt {
            value: None,
            work: WorkCounters::default(),
        });
    };
    if base_bytes != ours_bytes || base_bytes != theirs_bytes || logical_bytes == 0 {
        return Ok(OperationReceipt {
            value: None,
            work: WorkCounters::default(),
        });
    }
    let request = |root| ExtentRangeRequest {
        root,
        file_size: logical_bytes,
        range: ByteRange {
            offset: 0,
            length: logical_bytes,
        },
        maximum_spans: maximum_changes,
        limits,
        budget,
    };
    let mut work = WorkCounters::default();
    let base_plan = plan_extent_range_async(store, request(base_root), cancellation)
        .await
        .map_err(|failure| failure.map_with_prior_work(work, MergeGenerationError::ExtentRead))?;
    work = add(work, base_plan.work)?;
    let ours_plan = plan_extent_range_async(
        store,
        ExtentRangeRequest {
            budget: remaining(work, budget)?,
            ..request(ours_root)
        },
        cancellation,
    )
    .await
    .map_err(|failure| failure.map_with_prior_work(work, MergeGenerationError::ExtentRead))?;
    work = add(work, ours_plan.work)?;
    let theirs_plan = plan_extent_range_async(
        store,
        ExtentRangeRequest {
            budget: remaining(work, budget)?,
            ..request(theirs_root)
        },
        cancellation,
    )
    .await
    .map_err(|failure| failure.map_with_prior_work(work, MergeGenerationError::ExtentRead))?;
    work = add(work, theirs_plan.work)?;
    let mut indices = [0_usize; 3];
    let plans = [&base_plan.spans, &ours_plan.spans, &theirs_plan.spans];
    let mut offset = 0_u64;
    let mut mutations = Vec::new();
    while offset < logical_bytes {
        let spans = [
            plans[0].get(indices[0]).ok_or_else(|| invalid(work))?,
            plans[1].get(indices[1]).ok_or_else(|| invalid(work))?,
            plans[2].get(indices[2]).ok_or_else(|| invalid(work))?,
        ];
        let end = spans
            .iter()
            .map(|span| span.offset.saturating_add(span.length))
            .min()
            .ok_or_else(|| invalid(work))?;
        if end <= offset {
            return Err(invalid(work));
        }
        let kinds = [
            sliced_extent_kind(spans[0], offset)?,
            sliced_extent_kind(spans[1], offset)?,
            sliced_extent_kind(spans[2], offset)?,
        ];
        let Some(resolved) = resolve_three(&kinds[0], &kinds[1], &kinds[2]) else {
            return Ok(OperationReceipt { value: None, work });
        };
        if resolved != kinds[1] {
            mutations.push(ExtentMutation::Replace {
                offset,
                length: end - offset,
                kind: resolved,
                extend: false,
            });
            if mutations.len() > usize::try_from(maximum_changes).unwrap_or(usize::MAX) {
                return Err(OperationFailure::new(
                    MergeGenerationError::ChangeLimit,
                    work,
                ));
            }
        }
        offset = end;
        for index in 0..3 {
            if plans[index][indices[index]].offset + plans[index][indices[index]].length == end {
                indices[index] += 1;
            }
        }
    }
    let root = if mutations.is_empty() {
        ours_root
    } else {
        let applied = apply_extent_mutations_async(
            store,
            ours_root,
            ours_bytes,
            &mutations,
            ExtentMutationOptions {
                maximum_mutations: maximum_changes,
                limits,
                budget: remaining(work, budget)?,
            },
            cancellation,
        )
        .await
        .map_err(|failure| {
            failure.map_with_prior_work(work, MergeGenerationError::ExtentMutation)
        })?;
        work = add(work, applied.work)?;
        applied.root
    };
    Ok(OperationReceipt {
        value: Some((
            FileRecord {
                file_id: ours.file_id,
                kind,
                link_count: ours.link_count,
                metadata,
                payload: FilePayload::Regular {
                    logical_bytes,
                    extents: root,
                },
            },
            u32::try_from(mutations.len()).unwrap_or(u32::MAX),
        )),
        work,
    })
}

fn sliced_extent_kind(
    span: &ExtentSlice,
    offset: u64,
) -> Result<ExtentKind, OperationFailure<MergeGenerationError>> {
    let delta = offset
        .checked_sub(span.offset)
        .ok_or_else(|| OperationFailure::before_work(MergeGenerationError::InvalidDiff))?;
    Ok(match span.kind {
        ExtentKind::Hole => ExtentKind::Hole,
        ExtentKind::AllocatedZero => ExtentKind::AllocatedZero,
        ExtentKind::Content {
            object,
            object_offset,
        } => ExtentKind::Content {
            object,
            object_offset: object_offset
                .checked_add(delta)
                .ok_or_else(|| OperationFailure::before_work(MergeGenerationError::InvalidDiff))?,
        },
    })
}

type RecordChange = (Option<FileRecord>, Option<FileRecord>);

fn changes_by_file(
    changes: Vec<super::persistent_diff::ValueChange<FileId, FileRecord>>,
) -> BTreeMap<FileId, RecordChange> {
    changes
        .into_iter()
        .map(|change| (change.key, (change.before, change.after)))
        .collect()
}

async fn adjust_link_counts<S: AsyncObjectStore>(
    store: &S,
    resolutions: &mut BTreeMap<FileId, RecordChange>,
    remaining_changes: &mut u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    work: &mut WorkCounters,
) -> Result<(), OperationFailure<MergeGenerationError>> {
    let mut deltas = BTreeMap::<FileId, i64>::new();
    for (before, after) in resolutions.values() {
        let before_entries = directory_entries(*before);
        let after_entries = directory_entries(*after);
        if before_entries == after_entries {
            continue;
        }
        if *remaining_changes == 0 {
            return Err(OperationFailure::new(
                MergeGenerationError::ChangeLimit,
                *work,
            ));
        }
        let entries = diff_tree_entries_async(
            store,
            before_entries,
            after_entries,
            *remaining_changes,
            limits,
            remaining(*work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| map_diff_failure(failure, *work))?;
        *work = add(*work, entries.work)?;
        if entries.truncated {
            return Err(OperationFailure::new(
                MergeGenerationError::ChangeLimit,
                *work,
            ));
        }
        *remaining_changes = remaining_changes
            .checked_sub(u32::try_from(entries.changes.len()).unwrap_or(u32::MAX))
            .ok_or_else(|| OperationFailure::new(MergeGenerationError::ChangeLimit, *work))?;
        for binding in entries.changes {
            if let Some(before) = binding.before {
                *deltas.entry(before.file_id).or_default() -= 1;
            }
            if let Some(after) = binding.after {
                *deltas.entry(after.file_id).or_default() += 1;
            }
        }
    }
    for (file_id, delta) in deltas {
        if delta == 0 {
            continue;
        }
        let (ours, resolved) = resolutions
            .get_mut(&file_id)
            .ok_or_else(|| invalid(*work))?;
        let record = resolved.as_mut().ok_or_else(|| invalid(*work))?;
        record.link_count = i128::from(ours.map_or(0, |record| record.link_count))
            .checked_add(i128::from(delta))
            .and_then(|value| u64::try_from(value).ok())
            .filter(|value| *value != 0)
            .ok_or_else(|| invalid(*work))?;
    }
    Ok(())
}

pub(crate) struct DirectoryMergeResult {
    pub(crate) record: Option<FileRecord>,
    pub(crate) conflicts: Vec<MergeConflict>,
    pub(crate) truncated: bool,
    pub(crate) examined_changes: u32,
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) async fn merge_directory_record_async<S: AsyncObjectStore>(
    store: &S,
    directory_id: FileId,
    base: FileRecord,
    ours: FileRecord,
    theirs: FileRecord,
    maximum_changes: u32,
    maximum_conflicts: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<DirectoryMergeResult>, OperationFailure<MergeGenerationError>> {
    let mut work = WorkCounters::default();
    let conflict = || DirectoryMergeResult {
        record: None,
        conflicts: (maximum_conflicts != 0)
            .then_some(MergeConflict::File(directory_id))
            .into_iter()
            .collect(),
        truncated: maximum_conflicts == 0,
        examined_changes: 0,
    };
    let Some(metadata) = resolve_three(&base.metadata, &ours.metadata, &theirs.metadata) else {
        return Ok(OperationReceipt {
            value: conflict(),
            work,
        });
    };
    let Some(link_count) = resolve_three(&base.link_count, &ours.link_count, &theirs.link_count)
    else {
        return Ok(OperationReceipt {
            value: conflict(),
            work,
        });
    };
    let (Some(base_entries), Some(ours_entries), Some(theirs_entries)) = (
        directory_entries(Some(base)),
        directory_entries(Some(ours)),
        directory_entries(Some(theirs)),
    ) else {
        return Err(OperationFailure::before_work(
            MergeGenerationError::InvalidDiff,
        ));
    };
    let ours_diff = diff_tree_entries_async(
        store,
        Some(base_entries),
        Some(ours_entries),
        maximum_changes,
        limits,
        remaining(work, budget)?,
        cancellation,
    )
    .await
    .map_err(|failure| map_diff_failure(failure, work))?;
    work = add(work, ours_diff.work)?;
    let theirs_diff = diff_tree_entries_async(
        store,
        Some(base_entries),
        Some(theirs_entries),
        maximum_changes,
        limits,
        remaining(work, budget)?,
        cancellation,
    )
    .await
    .map_err(|failure| map_diff_failure(failure, work))?;
    work = add(work, theirs_diff.work)?;
    if ours_diff.truncated || theirs_diff.truncated {
        return Err(OperationFailure::new(
            MergeGenerationError::ChangeLimit,
            work,
        ));
    }
    let ours_changes = ours_diff
        .changes
        .into_iter()
        .map(|change| (change.key, (change.before, change.after)))
        .collect::<BTreeMap<_, _>>();
    let theirs_changes = theirs_diff
        .changes
        .into_iter()
        .map(|change| (change.key, (change.before, change.after)))
        .collect::<BTreeMap<_, _>>();
    let names = ours_changes
        .keys()
        .chain(theirs_changes.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    if names.len() > usize::try_from(maximum_changes).unwrap_or(usize::MAX) {
        return Err(OperationFailure::new(
            MergeGenerationError::ChangeLimit,
            work,
        ));
    }
    let examined_changes = u32::try_from(names.len()).unwrap_or(u32::MAX);
    let mut conflicts = Vec::new();
    let mut truncated = false;
    let mut mutations = Vec::new();
    for name in names {
        let base_value = ours_changes
            .get(&name)
            .map(|change| change.0.clone())
            .or_else(|| theirs_changes.get(&name).map(|change| change.0.clone()))
            .flatten();
        let ours_value = ours_changes
            .get(&name)
            .map_or_else(|| base_value.clone(), |change| change.1.clone());
        let theirs_value = theirs_changes
            .get(&name)
            .map_or_else(|| base_value.clone(), |change| change.1.clone());
        let Some(resolved) = resolve_three(&base_value, &ours_value, &theirs_value) else {
            if conflicts.len() < usize::try_from(maximum_conflicts).unwrap_or(usize::MAX) {
                conflicts.push(MergeConflict::Binding { directory_id, name });
            } else {
                truncated = true;
            }
            continue;
        };
        if resolved != ours_value
            && let Some(mutation) = tree_mutation(name, ours_value, resolved)
        {
            mutations.push(mutation);
        }
    }
    if !conflicts.is_empty() || truncated {
        return Ok(OperationReceipt {
            value: DirectoryMergeResult {
                record: None,
                conflicts,
                truncated,
                examined_changes,
            },
            work,
        });
    }
    let entries = if mutations.is_empty() {
        ours_entries
    } else {
        let applied = apply_tree_mutations_async(
            store,
            ours_entries,
            mutations,
            maximum_changes,
            limits,
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, MergeGenerationError::Tree))?;
        work = add(work, applied.work)?;
        applied.root
    };
    Ok(OperationReceipt {
        value: DirectoryMergeResult {
            record: Some(FileRecord {
                file_id: directory_id,
                kind: super::FileKind::Directory,
                link_count,
                metadata,
                payload: FilePayload::Directory { entries },
            }),
            conflicts,
            truncated,
            examined_changes,
        },
        work,
    })
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

pub(crate) fn resolve_three<T: Clone + Eq>(base: &T, ours: &T, theirs: &T) -> Option<T> {
    if ours == theirs || theirs == base {
        Some(ours.clone())
    } else if ours == base {
        Some(theirs.clone())
    } else {
        None
    }
}

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
fn invalid(work: WorkCounters) -> OperationFailure<MergeGenerationError> {
    OperationFailure::new(MergeGenerationError::InvalidDiff, work)
}
fn remaining(
    work: WorkCounters,
    budget: WorkBudget,
) -> Result<WorkBudget, OperationFailure<MergeGenerationError>> {
    work.remaining(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))
}
fn add(
    prior: WorkCounters,
    next: WorkCounters,
) -> Result<WorkCounters, OperationFailure<MergeGenerationError>> {
    prior
        .checked_add(next)
        .map_err(|error| OperationFailure::new(error.into(), prior))
}
fn map_diff_failure(
    failure: OperationFailure<PersistentDiffError>,
    prior: WorkCounters,
) -> OperationFailure<MergeGenerationError> {
    failure.map_with_prior_work(prior, |error| match error {
        PersistentDiffError::WrongRootKind | PersistentDiffError::InvalidLimit => {
            MergeGenerationError::InvalidDiff
        }
        PersistentDiffError::AllocationFailed => MergeGenerationError::AllocationFailed,
        PersistentDiffError::Storage(error) => MergeGenerationError::Object(error),
        PersistentDiffError::Decode(error) => MergeGenerationError::Decode(error),
        PersistentDiffError::Work(error) => MergeGenerationError::Work(error),
        PersistentDiffError::Cancelled => MergeGenerationError::Cancelled(CancellationError),
    })
}

#[cfg(test)]
#[path = "tests/merge.rs"]
mod tests;
