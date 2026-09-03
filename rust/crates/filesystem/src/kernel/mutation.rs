//! Bounded atomic mutation contracts and shared-prefix execution plans.

use super::{FileRecord, MetadataField, NamespacePath};
use crate::foundation::FileId;
use crate::model::VolumeLimits;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectKind};
use std::cell::Cell;
use std::mem::size_of;
use thiserror::Error;

/// One ordered mutation within a single-volume atomic batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mutation {
    /// Binds a new path-independent file record at an absent path.
    Create {
        /// New namespace path.
        path: NamespacePath,
        /// Complete immutable initial record.
        record: FileRecord,
    },
    /// Removes one namespace binding.
    Remove {
        /// Existing namespace path.
        path: NamespacePath,
        /// Optional exact file identity precondition.
        expected_file_id: MetadataField<FileId>,
    },
    /// Atomically moves one binding within the volume.
    Rename {
        /// Existing source path.
        source: NamespacePath,
        /// Destination path.
        destination: NamespacePath,
        /// Whether an existing destination may be replaced.
        replace: bool,
    },
    /// Creates another namespace binding to an existing non-directory file.
    Link {
        /// Existing source path.
        source: NamespacePath,
        /// New link path.
        destination: NamespacePath,
    },
    /// Replaces the complete canonical metadata object.
    SetMetadata {
        /// Existing path.
        path: NamespacePath,
        /// Authenticated metadata object.
        metadata: ObjectId,
    },
    /// Replaces one logical byte interval with authenticated content.
    Write {
        /// Existing regular-file path.
        path: NamespacePath,
        /// Inclusive destination offset.
        offset: u64,
        /// Positive byte count.
        length: u64,
        /// Authenticated blob containing source bytes.
        content: ObjectId,
        /// Inclusive source offset in `content`.
        content_offset: u64,
    },
    /// Validates that one existing path names a regular file without changing it.
    ValidateRegular {
        /// Existing regular-file path.
        path: NamespacePath,
    },
    /// Changes logical file length while preserving sparse semantics.
    Resize {
        /// Existing regular-file path.
        path: NamespacePath,
        /// New logical byte length.
        logical_bytes: u64,
    },
    /// Replaces a range with a hole or physically allocated zeros.
    ZeroRange {
        /// Existing regular-file path.
        path: NamespacePath,
        /// Inclusive logical offset.
        offset: u64,
        /// Positive byte count.
        length: u64,
        /// Preserve physical allocation instead of punching a hole.
        allocated: bool,
        /// Whether the replacement may extend logical file length.
        extend: bool,
    },
    /// Allocates sparse holes without replacing existing content.
    Preallocate {
        /// Existing regular-file path.
        path: NamespacePath,
        /// Inclusive logical offset.
        offset: u64,
        /// Positive byte count.
        length: u64,
        /// Preserve logical file length.
        keep_size: bool,
    },
    /// Clones one logical range without materializing unchanged content.
    CloneRange {
        /// Existing source regular file.
        source: NamespacePath,
        /// Inclusive source offset.
        source_offset: u64,
        /// Existing destination regular file.
        destination: NamespacePath,
        /// Inclusive destination offset.
        destination_offset: u64,
        /// Positive byte count.
        length: u64,
    },
    /// Mutates one existing file through its stable path-independent identity.
    File {
        /// Existing authenticated file-table identity.
        file_id: FileId,
        /// Ordered record mutation.
        mutation: FileMutation,
    },
    /// Clones one logical range between stable file identities.
    CloneFileRange {
        /// Existing source regular-file identity.
        source_file_id: FileId,
        /// Inclusive source offset.
        source_offset: u64,
        /// Existing destination regular-file identity.
        destination_file_id: FileId,
        /// Inclusive destination offset.
        destination_offset: u64,
        /// Positive byte count.
        length: u64,
    },
}

/// One ordered mutation of an existing path-independent file record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FileMutation {
    /// Replaces the complete canonical metadata object.
    SetMetadata {
        /// Authenticated metadata object.
        metadata: ObjectId,
    },
    /// Replaces one logical byte interval with authenticated content.
    Write {
        /// Inclusive destination offset.
        offset: u64,
        /// Positive byte count.
        length: u64,
        /// Authenticated blob containing source bytes.
        content: ObjectId,
        /// Inclusive source offset in `content`.
        content_offset: u64,
    },
    /// Validates that the existing identity is a regular file without changing it.
    ValidateRegular,
    /// Changes logical file length while preserving sparse semantics.
    Resize {
        /// New logical byte length.
        logical_bytes: u64,
    },
    /// Replaces a range with a hole or physically allocated zeros.
    ZeroRange {
        /// Inclusive logical offset.
        offset: u64,
        /// Positive byte count.
        length: u64,
        /// Preserve physical allocation instead of punching a hole.
        allocated: bool,
        /// Whether the replacement may extend logical file length.
        extend: bool,
    },
    /// Allocates sparse holes without replacing existing content.
    Preallocate {
        /// Inclusive logical offset.
        offset: u64,
        /// Positive byte count.
        length: u64,
        /// Preserve logical file length.
        keep_size: bool,
    },
}

impl Mutation {
    pub(crate) fn paths(&self) -> Option<[&NamespacePath; 2]> {
        let paths = match self {
            Self::Create { path, .. }
            | Self::Remove { path, .. }
            | Self::SetMetadata { path, .. }
            | Self::Write { path, .. }
            | Self::ValidateRegular { path }
            | Self::Resize { path, .. }
            | Self::ZeroRange { path, .. }
            | Self::Preallocate { path, .. } => [path, path],
            Self::Rename {
                source,
                destination,
                ..
            }
            | Self::Link {
                source,
                destination,
            }
            | Self::CloneRange {
                source,
                destination,
                ..
            } => [source, destination],
            Self::File { .. } | Self::CloneFileRange { .. } => return None,
        };
        Some(paths)
    }

    fn validate(&self) -> Result<(), MutationPlanError> {
        if let Some([first, second]) = self.paths()
            && (first.is_root() || second.is_root())
        {
            return Err(MutationPlanError::RootMutation);
        }
        match self {
            Self::Create { record, .. } if record.validate().is_err() => {
                Err(MutationPlanError::InvalidInitialRecord)
            }
            Self::Rename {
                source,
                destination,
                ..
            }
            | Self::Link {
                source,
                destination,
            } if source == destination => Err(MutationPlanError::SameSourceAndDestination),
            Self::Write {
                offset,
                length,
                content,
                content_offset,
                ..
            }
            | Self::File {
                mutation:
                    FileMutation::Write {
                        offset,
                        length,
                        content,
                        content_offset,
                    },
                ..
            } => {
                validate_range(*offset, *length)?;
                validate_range(*content_offset, *length)?;
                if content.kind != ObjectKind::Blob {
                    return Err(MutationPlanError::WrongContentKind);
                }
                Ok(())
            }
            Self::ZeroRange { offset, length, .. } | Self::Preallocate { offset, length, .. } => {
                validate_range(*offset, *length)
            }
            Self::CloneRange {
                source_offset,
                destination_offset,
                length,
                ..
            }
            | Self::CloneFileRange {
                source_offset,
                destination_offset,
                length,
                ..
            } => {
                validate_range(*source_offset, *length)?;
                validate_range(*destination_offset, *length)
            }
            Self::SetMetadata { metadata, .. } if metadata.kind != ObjectKind::Metadata => {
                Err(MutationPlanError::WrongMetadataKind)
            }
            Self::File {
                mutation: FileMutation::SetMetadata { metadata },
                ..
            } if metadata.kind != ObjectKind::Metadata => Err(MutationPlanError::WrongMetadataKind),
            Self::File {
                mutation:
                    FileMutation::ZeroRange { offset, length, .. }
                    | FileMutation::Preallocate { offset, length, .. },
                ..
            } => validate_range(*offset, *length),
            Self::Create { .. }
            | Self::Remove { .. }
            | Self::Rename { .. }
            | Self::Link { .. }
            | Self::SetMetadata { .. }
            | Self::ValidateRegular { .. }
            | Self::Resize { .. }
            | Self::File {
                mutation:
                    FileMutation::SetMetadata { .. }
                    | FileMutation::ValidateRegular
                    | FileMutation::Resize { .. },
                ..
            } => Ok(()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PathUse {
    operation: u32,
    endpoint: u8,
}

/// One normalized mutation batch and its shared-prefix execution plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationPlan {
    operations: Vec<Mutation>,
    ordered_paths: Vec<PathUse>,
    path_nodes: u64,
    retained_allocation_bytes: u64,
    work: WorkCounters,
}

impl MutationPlan {
    /// Compiles an ordered batch once before any immutable page is written.
    ///
    /// # Errors
    ///
    /// Rejects empty/excessive batches, invalid roots/object classes/ranges,
    /// index overflow, and work outside the admitted budget.
    pub fn compile(
        operations: Vec<Mutation>,
        limits: VolumeLimits,
        budget: WorkBudget,
    ) -> Result<Self, MutationPlanFailure> {
        if operations.is_empty() {
            return Err(OperationFailure::before_work(MutationPlanError::Empty));
        }
        if u32::try_from(operations.len()).unwrap_or(u32::MAX) > limits.maximum_mutations_per_batch
        {
            return Err(OperationFailure::before_work(
                MutationPlanError::TooManyMutations,
            ));
        }
        let mut work = WorkCounters::default();
        for operation in &operations {
            operation
                .validate()
                .map_err(|error| OperationFailure::new(error, work))?;
        }

        let maximum_uses = path_use_count(&operations)
            .ok_or_else(|| OperationFailure::new(MutationPlanError::IndexOverflow, work))?;
        let allocation_bytes = maximum_uses
            .checked_mul(size_of::<PathUse>())
            .map(crate::foundation::usize_to_u64)
            .ok_or_else(|| OperationFailure::new(MutationPlanError::IndexOverflow, work))?;
        let mut ordered_paths = Vec::new();
        if maximum_uses != 0 {
            let attempted = work
                .checked_add(WorkCounters {
                    allocation_operations: 1,
                    ..WorkCounters::default()
                })
                .map_err(|error| OperationFailure::new(error.into(), work))?;
            attempted
                .verify(budget)
                .map_err(|error| OperationFailure::new(error.into(), work))?;
            let mut allocated = attempted;
            allocated.peak_allocation_bytes = allocated.peak_allocation_bytes.max(allocation_bytes);
            allocated
                .verify(budget)
                .map_err(|error| OperationFailure::new(error.into(), work))?;
            ordered_paths.try_reserve_exact(maximum_uses).map_err(|_| {
                OperationFailure::new(MutationPlanError::AllocationFailed, attempted)
            })?;
            work = allocated;
        }
        for (index, operation) in operations.iter().enumerate() {
            let operation_index = u32::try_from(index)
                .map_err(|_| OperationFailure::new(MutationPlanError::IndexOverflow, work))?;
            let Some(paths) = operation.paths() else {
                continue;
            };
            ordered_paths.push(PathUse {
                operation: operation_index,
                endpoint: 0,
            });
            if paths[1] != paths[0] {
                ordered_paths.push(PathUse {
                    operation: operation_index,
                    endpoint: 1,
                });
            }
        }
        let comparisons = Cell::new(0_u64);
        ordered_paths.sort_unstable_by(|left, right| {
            comparisons.set(comparisons.get().saturating_add(1));
            path_for(&operations, *left)
                .cmp(path_for(&operations, *right))
                .then_with(|| left.operation.cmp(&right.operation))
                .then_with(|| left.endpoint.cmp(&right.endpoint))
        });
        let mut path_nodes = 1_u64;
        let mut prior: Option<&NamespacePath> = None;
        let mut component_visits = 0_u64;
        for path_use in &ordered_paths {
            let path = path_for(&operations, *path_use);
            component_visits = component_visits
                .checked_add(u64::try_from(path.depth()).unwrap_or(u64::MAX))
                .ok_or_else(|| OperationFailure::new(MutationPlanError::IndexOverflow, work))?;
            let shared = prior.map_or(0, |previous| shared_prefix(previous, path));
            path_nodes = path_nodes
                .checked_add(u64::try_from(path.depth() - shared).unwrap_or(u64::MAX))
                .ok_or_else(|| OperationFailure::new(MutationPlanError::IndexOverflow, work))?;
            prior = Some(path);
        }
        work = work
            .checked_add(WorkCounters {
                items_examined: component_visits
                    .checked_add(comparisons.get())
                    .ok_or_else(|| OperationFailure::new(MutationPlanError::IndexOverflow, work))?,
                ..WorkCounters::default()
            })
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        work.verify(budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        Ok(Self {
            operations,
            ordered_paths,
            path_nodes,
            retained_allocation_bytes: allocation_bytes,
            work,
        })
    }

    /// Ordered operations retained for atomic replay.
    #[must_use]
    pub fn operations(&self) -> &[Mutation] {
        &self.operations
    }

    /// Borrowed operation endpoints in canonical path order.
    ///
    /// Repeated paths are retained because operation ordering remains semantic;
    /// consumers can group adjacent equal paths without cloning names.
    #[must_use]
    pub fn ordered_paths(&self) -> impl ExactSizeIterator<Item = &NamespacePath> {
        self.ordered_paths
            .iter()
            .map(|path_use| path_for(&self.operations, *path_use))
    }

    /// Canonically ordered endpoints with their operation and endpoint indices.
    ///
    /// Endpoint zero is the sole or source path; endpoint one is a distinct
    /// destination. The executor uses these stable indices to restore semantic
    /// operation order after one shared borrowed-path lookup.
    #[must_use]
    pub fn ordered_operation_paths(
        &self,
    ) -> impl ExactSizeIterator<Item = (usize, usize, &NamespacePath)> {
        self.ordered_paths.iter().map(|path_use| {
            (
                usize::try_from(path_use.operation).unwrap_or(usize::MAX),
                usize::from(path_use.endpoint),
                path_for(&self.operations, *path_use),
            )
        })
    }

    /// Distinct path-prefix nodes; shared ancestors count once.
    #[must_use]
    pub const fn path_nodes(&self) -> u64 {
        self.path_nodes
    }

    /// Logical flat-plan capacity retained until generation execution finishes.
    #[must_use]
    pub const fn retained_allocation_bytes(&self) -> u64 {
        self.retained_allocation_bytes
    }

    /// Exact compilation work.
    #[must_use]
    pub const fn work(&self) -> WorkCounters {
        self.work
    }

    pub(crate) fn into_operations(self) -> Vec<Mutation> {
        self.operations
    }
}

// `PathUse` is private and is constructed only after `Mutation::paths`
// returns `Some`; retaining that proof here avoids cloning every path.
#[allow(clippy::expect_used)]
fn path_for(operations: &[Mutation], path_use: PathUse) -> &NamespacePath {
    let operation = &operations[usize::try_from(path_use.operation).unwrap_or(usize::MAX)];
    operation
        .paths()
        .expect("path use must reference a path mutation")[usize::from(path_use.endpoint)]
}

fn shared_prefix(left: &NamespacePath, right: &NamespacePath) -> usize {
    left.components()
        .iter()
        .zip(right.components())
        .take_while(|(left, right)| left == right)
        .count()
}

fn path_use_count(operations: &[Mutation]) -> Option<usize> {
    operations.iter().try_fold(0_usize, |count, operation| {
        let uses = operation
            .paths()
            .map_or(0, |[first, second]| if first == second { 1 } else { 2 });
        count.checked_add(uses)
    })
}

fn validate_range(offset: u64, length: u64) -> Result<(), MutationPlanError> {
    if length == 0 {
        return Err(MutationPlanError::EmptyRange);
    }
    offset
        .checked_add(length)
        .ok_or(MutationPlanError::RangeOverflow)?;
    Ok(())
}

/// Mutation-plan failure preserving exact compilation work.
pub type MutationPlanFailure = OperationFailure<MutationPlanError>;

/// Stable mutation-plan admission failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum MutationPlanError {
    /// Atomic batches contain at least one operation.
    #[error("mutation batch is empty")]
    Empty,
    /// Batch exceeds the volume's admitted operation bound.
    #[error("mutation batch exceeds the volume limit")]
    TooManyMutations,
    /// Initial create record violates canonical file-table invariants.
    #[error("created file record is invalid")]
    InvalidInitialRecord,
    /// The volume root cannot be created, removed, linked, renamed, or rewritten as a file.
    #[error("mutation cannot target the volume root")]
    RootMutation,
    /// Two-ended namespace operation used one identical path.
    #[error("mutation source and destination are identical")]
    SameSourceAndDestination,
    /// Byte range must be non-empty.
    #[error("mutation byte range is empty")]
    EmptyRange,
    /// Byte-range arithmetic overflowed.
    #[error("mutation byte range overflowed")]
    RangeOverflow,
    /// Write source is not an authenticated blob.
    #[error("mutation content object is not a blob")]
    WrongContentKind,
    /// Metadata replacement is not a canonical metadata object.
    #[error("mutation metadata object has the wrong kind")]
    WrongMetadataKind,
    /// Stable operation/path index cannot be represented.
    #[error("mutation-plan index overflowed")]
    IndexOverflow,
    /// Bounded flat-plan storage could not be allocated.
    #[error("mutation-plan allocation failed")]
    AllocationFailed,
    /// Exact compilation work exceeded or overflowed its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

#[cfg(test)]
#[path = "tests/mutation.rs"]
mod tests;
