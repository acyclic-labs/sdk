//! Zero-copy logical range clone over immutable sparse extents.

use super::{
    DecodeLimits, ExtentMutation, ExtentMutationError, ExtentMutationOptions,
    apply_extent_mutations_async, plan_extent_range_async,
};
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ByteRange, ObjectId, ObjectKind};
use std::mem::size_of;
use thiserror::Error;

/// Candidate destination extent root and exact clone work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentCloneReceipt {
    /// Candidate immutable destination extent root.
    pub root: ObjectId,
    /// Destination logical size after extension, if any.
    pub logical_bytes: u64,
    /// Exact source planning, interval compilation, and destination path-copy work.
    pub work: WorkCounters,
}

/// Range clone failure retaining all completed work and safe orphan writes.
pub type ExtentCloneFailure = OperationFailure<ExtentCloneError>;

/// Complete bounded source and destination range-clone request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExtentCloneRequest {
    /// Authenticated source extent root.
    pub source_root: ObjectId,
    /// Exact source logical file size.
    pub source_logical_bytes: u64,
    /// Non-empty source range.
    pub source_range: ByteRange,
    /// Authenticated destination extent root.
    pub destination_root: ObjectId,
    /// Exact destination logical file size before clone.
    pub destination_logical_bytes: u64,
    /// Inclusive destination start offset.
    pub destination_offset: u64,
    /// Maximum source sparse spans admitted in the clone plan.
    pub maximum_spans: u32,
    /// Maximum destination interval mutations admitted.
    pub maximum_mutations: u32,
}

/// Clones one logical sparse range without reading or copying content bytes.
///
/// The source range is fully planned before any destination object is written,
/// so overlapping same-file clone has snapshot semantics. Hole, allocated-zero,
/// and content representations remain distinct. Content spans reuse immutable
/// blob identities and adjusted offsets.
///
/// # Errors
///
/// Rejects wrong object classes, empty/overflowing ranges, insufficient span or
/// mutation bounds, malformed source/destination extent trees, storage errors,
/// and work outside the admitted budget.
pub fn clone_extent_range<S: crate::ImmediateObjectStore>(
    store: &S,
    request: ExtentCloneRequest,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<ExtentCloneReceipt, ExtentCloneFailure> {
    let cancellation = CancellationToken::new();
    crate::async_storage::poll_immediate(clone_extent_range_async(
        store,
        request,
        limits,
        budget,
        &cancellation,
    ))
}

/// Asynchronously clones a sparse logical range through the canonical planner
/// and destination path-copy machine.
///
/// # Errors
///
/// Returns the same bounded failures as [`clone_extent_range`], including
/// cooperative cancellation before either source reads or destination writes.
pub async fn clone_extent_range_async<S: crate::AsyncObjectStore>(
    store: &S,
    request: ExtentCloneRequest,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<ExtentCloneReceipt, ExtentCloneFailure> {
    validate_request(request, limits)?;
    let plan = plan_extent_range_async(
        store,
        super::ExtentRangeRequest {
            root: request.source_root,
            file_size: request.source_logical_bytes,
            range: request.source_range,
            maximum_spans: request.maximum_spans,
            limits,
            budget,
        },
        cancellation,
    )
    .await
    .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
    let allocation_bytes = u64::try_from(plan.spans.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(size_of::<ExtentMutation>()).unwrap_or(u64::MAX));
    let simultaneous_plan_allocation = plan
        .retained_allocation_bytes
        .checked_add(allocation_bytes)
        .ok_or_else(|| {
            OperationFailure::new(ExtentCloneError::Work(WorkError::Overflow), plan.work)
        })?;
    let mut work = plan
        .work
        .checked_add(WorkCounters {
            allocation_operations: 1,
            items_examined: u64::try_from(plan.spans.len()).unwrap_or(u64::MAX),
            ..WorkCounters::default()
        })
        .map_err(|error| OperationFailure::new(error.into(), plan.work))?;
    work.peak_allocation_bytes = work.peak_allocation_bytes.max(simultaneous_plan_allocation);
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), plan.work))?;
    let mut mutations = Vec::new();
    if mutations.try_reserve_exact(plan.spans.len()).is_err() {
        return Err(OperationFailure::new(
            ExtentCloneError::AllocationFailed,
            work,
        ));
    }
    for span in plan.spans {
        let relative = span
            .offset
            .checked_sub(request.source_range.offset)
            .ok_or_else(|| OperationFailure::new(ExtentCloneError::RangeOverflow, work))?;
        let offset = request
            .destination_offset
            .checked_add(relative)
            .ok_or_else(|| OperationFailure::new(ExtentCloneError::RangeOverflow, work))?;
        mutations.push(ExtentMutation::Replace {
            offset,
            length: span.length,
            kind: span.kind,
            extend: true,
        });
    }
    let mut remaining = work
        .remaining(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    remaining.peak_allocation_bytes = budget
        .peak_allocation_bytes
        .checked_sub(allocation_bytes)
        .ok_or_else(|| OperationFailure::new(ExtentCloneError::Work(WorkError::Overflow), work))?;
    let receipt = apply_extent_mutations_async(
        store,
        request.destination_root,
        request.destination_logical_bytes,
        &mutations,
        ExtentMutationOptions {
            maximum_mutations: request.maximum_mutations,
            limits,
            budget: remaining,
        },
        cancellation,
    )
    .await
    .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
    let destination_peak = allocation_bytes
        .checked_add(receipt.work.peak_allocation_bytes)
        .ok_or_else(|| OperationFailure::new(ExtentCloneError::Work(WorkError::Overflow), work))?;
    let mut destination_work = receipt.work;
    destination_work.peak_allocation_bytes = 0;
    work = work
        .checked_add(destination_work)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    work.peak_allocation_bytes = work.peak_allocation_bytes.max(destination_peak);
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    Ok(ExtentCloneReceipt {
        root: receipt.root,
        logical_bytes: receipt.logical_bytes,
        work,
    })
}

fn validate_request(
    request: ExtentCloneRequest,
    limits: DecodeLimits,
) -> Result<(), ExtentCloneFailure> {
    let error = if request.source_root.kind != ObjectKind::ExtentPage
        || request.destination_root.kind != ObjectKind::ExtentPage
    {
        Some(ExtentCloneError::WrongRootKind)
    } else if request.source_range.length == 0 {
        Some(ExtentCloneError::EmptyRange)
    } else if request
        .source_range
        .offset
        .checked_add(request.source_range.length)
        .is_none()
        || request
            .destination_offset
            .checked_add(request.source_range.length)
            .is_none()
    {
        Some(ExtentCloneError::RangeOverflow)
    } else if request.maximum_spans == 0
        || request.maximum_mutations < request.maximum_spans
        || !limits.page_limits_valid(2)
    {
        Some(ExtentCloneError::InvalidLimits)
    } else {
        None
    };
    match error {
        Some(error) => Err(OperationFailure::before_work(error)),
        None => Ok(()),
    }
}

/// Sparse range-clone failures.
#[derive(Debug, Error)]
pub enum ExtentCloneError {
    /// Source or destination is not an extent-page object.
    #[error("range clone root has the wrong object kind")]
    WrongRootKind,
    /// Clone length must be positive.
    #[error("range clone is empty")]
    EmptyRange,
    /// Source or destination range arithmetic overflowed.
    #[error("range clone overflowed")]
    RangeOverflow,
    /// Span/mutation limits cannot admit the operation.
    #[error("range clone limits are invalid")]
    InvalidLimits,
    /// The admitted clone-plan scratch allocation was unavailable.
    #[error("range clone scratch allocation failed")]
    AllocationFailed,
    /// Source sparse-range planning failed.
    #[error(transparent)]
    Source(#[from] super::ExtentReadError),
    /// Destination sparse mutation failed.
    #[error(transparent)]
    Destination(#[from] ExtentMutationError),
    /// Exact work exceeded or overflowed its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/extent_clone.rs"]
mod tests;
