//! Bounded atomic mutation contracts and shared-prefix execution plans.
//! Ordinary and original-native callers use the same flat planner; producers
//! execute outside Root loans with explicit custody for the paid path index.

use super::{FileRecord, MetadataField, NamespacePath};
use crate::foundation::{FileId, usize_to_u64};
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
    /// Restores one exact immutable record at a path while preserving any
    /// additional live hard-link bindings to the same identity. Unlike
    /// `Create`, the identity may already exist in the candidate generation.
    Restore {
        /// Namespace path to restore.
        path: NamespacePath,
        /// Exact source record. Its link count is adapted to the live binding
        /// topology; all other representable facts are restored verbatim.
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
    /// Replaces one authenticated file record without changing its live bindings.
    /// The current identity and link count remain authoritative.
    ReplaceRecord {
        /// Complete replacement record for the same file identity and kind.
        record: FileRecord,
    },
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
            | Self::Restore { path, .. }
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
            Self::Create { record, .. } | Self::Restore { record, .. }
                if record.validate().is_err() =>
            {
                Err(MutationPlanError::InvalidInitialRecord)
            }
            Self::File {
                file_id,
                mutation: FileMutation::ReplaceRecord { record },
            } if record.validate().is_err() || record.file_id != *file_id => {
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
            | Self::Restore { .. }
            | Self::Remove { .. }
            | Self::Rename { .. }
            | Self::Link { .. }
            | Self::SetMetadata { .. }
            | Self::ValidateRegular { .. }
            | Self::Resize { .. }
            | Self::File {
                mutation:
                    FileMutation::SetMetadata { .. }
                    | FileMutation::ReplaceRecord { .. }
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
        let cancellation = crate::CancellationToken::new();
        let mut accounting = super::allocation::KernelAccounting::standalone(budget, &cancellation);
        Self::compile_with_accounting(operations, limits, &mut accounting)
            .map(|(plan, _)| plan)
            .map_err(|error| OperationFailure::new(plan_error(error), accounting.spent()))
    }

    /// Compiles the same flat plan on the caller's original ledger. Operation
    /// storage remains in its incoming custody; only the new path index is owned.
    pub(crate) fn compile_with_accounting(
        operations: Vec<Mutation>,
        limits: VolumeLimits,
        accounting: &mut super::allocation::KernelAccounting<'_>,
    ) -> Result<(Self, super::allocation::KernelOwnedAllocation), crate::FsError> {
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        let original = accounting.native_work().is_some();
        #[cfg(target_arch = "wasm32")]
        let original = false;
        let error = |error: MutationPlanError| {
            crate::FsError::Mutation(super::GenerationMutationError::Plan(error))
        };
        if operations.is_empty() {
            return Err(error(MutationPlanError::Empty));
        }
        if u32::try_from(operations.len()).unwrap_or(u32::MAX) > limits.maximum_mutations_per_batch {
            return Err(error(MutationPlanError::TooManyMutations));
        }
        for operation in &operations {
            operation.validate().map_err(&error)?;
        }
        let maximum_uses = path_use_count(&operations)
            .ok_or_else(|| error(MutationPlanError::IndexOverflow))?;
        let requested = maximum_uses.checked_mul(size_of::<PathUse>())
            .map(usize_to_u64).ok_or_else(|| error(MutationPlanError::IndexOverflow))?;
        let mut allocation = accounting.claim_owned(requested, u64::from(maximum_uses != 0))?;
        let mut ordered_paths = Vec::new();
        if let Err(cause) = ordered_paths.try_reserve_exact(maximum_uses) {
            drop(ordered_paths);
            accounting.release_owned(&mut allocation, requested)?;
            #[cfg(not(target_arch = "wasm32"))]
            if original {
                return Err(crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::AllocationFailed(cause)));
            }
            let _ = (cause, original);
            return Err(error(MutationPlanError::AllocationFailed));
        }
        let allocation_bytes = ordered_paths.capacity().checked_mul(size_of::<PathUse>())
            .map(usize_to_u64).ok_or_else(|| error(MutationPlanError::IndexOverflow))?;
        if let Err(cause) = accounting.reconcile_owned(&mut allocation, requested, allocation_bytes) {
            drop(ordered_paths);
            accounting.release_owned(&mut allocation, allocation_bytes)?;
            return Err(cause);
        }
        for (index, operation) in operations.iter().enumerate() {
            let operation_index = u32::try_from(index)
                .map_err(|_| error(MutationPlanError::IndexOverflow))?;
            let Some(paths) = operation.paths() else {
                continue;
            };
            ordered_paths.push(PathUse { operation: operation_index, endpoint: 0 });
            if paths[1] != paths[0] {
                ordered_paths.push(PathUse { operation: operation_index, endpoint: 1 });
            }
        }
        let comparisons = Cell::new(0_u64);
        ordered_paths.sort_unstable_by(|left, right| {
            comparisons.set(comparisons.get().saturating_add(1));
            path_for(&operations, *left).cmp(path_for(&operations, *right))
                .then_with(|| left.operation.cmp(&right.operation))
                .then_with(|| left.endpoint.cmp(&right.endpoint))
        });
        let mut path_nodes = 1_u64;
        let mut prior: Option<&NamespacePath> = None;
        let mut component_visits = 0_u64;
        for path_use in &ordered_paths {
            let path = path_for(&operations, *path_use);
            component_visits = component_visits.checked_add(usize_to_u64(path.depth()))
                .ok_or_else(|| error(MutationPlanError::IndexOverflow))?;
            let shared = prior.map_or(0, |previous| shared_prefix(previous, path));
            path_nodes = path_nodes.checked_add(usize_to_u64(path.depth() - shared))
                .ok_or_else(|| error(MutationPlanError::IndexOverflow))?;
            prior = Some(path);
        }
        let delta = WorkCounters {
            items_examined: component_visits.checked_add(comparisons.get())
                .ok_or_else(|| error(MutationPlanError::IndexOverflow))?,
            ..WorkCounters::default()
        };
        if let Err(cause) = accounting.charge(delta) {
            drop(ordered_paths);
            accounting.release_owned(&mut allocation, allocation_bytes)?;
            return Err(cause);
        }
        Ok((Self { operations, ordered_paths, path_nodes, retained_allocation_bytes: allocation_bytes, work: accounting.spent() }, allocation))
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

#[allow(
    clippy::expect_used,
    reason = "PathUse is private and is constructed only after Mutation::paths returns Some; relying on that avoids cloning every path"
)]
#[allow(
    clippy::indexing_slicing,
    reason = "PathUse is private and is only ever constructed in MutationPlan::compile as `operation: u32::try_from(index)` for `index` in `operations.iter().enumerate()`; operations and ordered_paths are stored together in MutationPlan and neither is mutated afterward, so path_use.operation as usize is always < operations.len()"
)]
fn path_for(operations: &[Mutation], path_use: PathUse) -> &NamespacePath {
    let operation = &operations[usize::try_from(path_use.operation).unwrap_or(usize::MAX)];
    let [first, second] = operation
        .paths()
        .expect("path use must reference a path mutation");
    // PathUse::endpoint is only ever constructed as 0 or 1 (see `compile`),
    // selecting between the two fixed positions of `Mutation::paths`.
    match path_use.endpoint {
        0 => first,
        _ => second,
    }
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

fn plan_error(error: crate::FsError) -> MutationPlanError {
    match error {
        crate::FsError::Mutation(super::GenerationMutationError::Plan(error)) => error,
        crate::FsError::Work(error) => error.into(),
        crate::FsError::PendingMutationAllocationFailed => MutationPlanError::AllocationFailed,
        _ => unreachable!("standalone planner only returns plan or work errors"),
    }
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod original_controls {
    use super::*;
    use super::super::allocation::{KernelAccounting, KernelOwnedAllocation};
    use crate::kernel::{LogicalName, NameEncoding};

    struct Funded<T> {
        values: Vec<T>,
        allocation: KernelOwnedAllocation,
    }

    fn funded_vec<T>(count: usize, accounting: &mut KernelAccounting<'_>) -> Result<Funded<T>, crate::FsError> {
        let requested = usize_to_u64(count.checked_mul(size_of::<T>()).ok_or(WorkError::Overflow)?);
        let allocation = accounting.claim_owned(requested, u64::from(count != 0))?;
        let mut funded = Funded { values: Vec::new(), allocation };
        funded.values.try_reserve_exact(count).map_err(crate::NativeTreeMaterializationError::AllocationFailed)?;
        let actual = usize_to_u64(funded.values.capacity().checked_mul(size_of::<T>()).ok_or(WorkError::Overflow)?);
        accounting.reconcile_owned(&mut funded.allocation, requested, actual)?;
        Ok(funded)
    }

    fn funded_operation(
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Vec<Mutation>, KernelOwnedAllocation), crate::FsError> {
        let mut operations = funded_vec::<Mutation>(1, accounting)?;
        let mut components = funded_vec::<LogicalName>(1, accounting)?;
        if let Err(error) = operations.allocation.absorb(&mut components.allocation) {
            drop(components);
            return Err(error);
        }
        let mut bytes = funded_vec::<u8>(6, accounting)?;
        if let Err(error) = operations.allocation.absorb(&mut bytes.allocation) {
            drop(bytes);
            return Err(error);
        }
        let delta = WorkCounters { bytes_copied: 6, ..WorkCounters::default() };
        accounting.admit(delta)?;
        bytes.values.extend_from_slice(b"native");
        accounting.record_completed(delta)?;
        let limits = VolumeLimits::default();
        components.values.push(LogicalName::new(NameEncoding::Utf8, std::mem::take(&mut bytes.values), limits.maximum_component_bytes)
            .map_err(|error| crate::FsError::Path(super::super::PathLookupError::NamespacePath(
                super::super::NamespacePathError::Name(error),
            )))?);
        operations.values.push(Mutation::ValidateRegular { path: NamespacePath::new(std::mem::take(&mut components.values), limits)
            .map_err(|error| crate::FsError::Path(super::super::PathLookupError::NamespacePath(error)))? });
        Ok((operations.values, operations.allocation))
    }

    #[test]
    fn original_planner_refuses_index_before_allocating_on_another_ledger()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = crate::CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            WorkBudget { allocation_operations: 3, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let (operations, input_guard) = funded_operation(&mut accounting)?;
        let before = original.receipt();
        let error = MutationPlan::compile_with_accounting(operations, VolumeLimits::default(), &mut accounting)
            .err().ok_or("planner created an unaccounted path index")?;
        assert!(matches!(error, crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(WorkError::BudgetExceeded { .. }))));
        assert_eq!(original.receipt(), before);
        drop(error);
        drop(input_guard);
        assert_eq!(original.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn original_planner_index_retains_only_its_actual_paid_capacity()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = crate::CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let (operations, input_guard) = funded_operation(&mut accounting)?;
        let KernelOwnedAllocation::NativeOriginal(input_guard) = input_guard else { return Err("input guard is not original".into()); };
        let (plan, index_guard) = MutationPlan::compile_with_accounting(operations, VolumeLimits::default(), &mut accounting)?;
        let KernelOwnedAllocation::NativeOriginal(index_guard) = index_guard else { return Err("index guard is not original".into()); };
        assert!(index_guard.shares_ledger(&input_guard));
        assert_eq!(index_guard.bytes(), plan.retained_allocation_bytes());
        assert_eq!(index_guard.bytes(), usize_to_u64(plan.ordered_paths.capacity()) * size_of::<PathUse>() as u64);
        assert_eq!(plan.ordered_paths().count(), 1);
        assert_eq!(plan.path_nodes(), 2);
        assert_eq!(original.live_allocation_bytes(), baseline + input_guard.bytes() + index_guard.bytes());
        drop(plan);
        drop(index_guard);
        drop(input_guard);
        assert_eq!(original.live_allocation_bytes(), baseline);
        Ok(())
    }
}
