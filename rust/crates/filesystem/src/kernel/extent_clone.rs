//! Zero-copy logical range clone over immutable sparse extents.

use super::DecodeLimits;
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ByteRange, ObjectId, ObjectKind};
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
    let mut accounting = super::allocation::KernelAccounting::standalone(budget, cancellation);
    super::regular_mutation::clone_extent_range_with_accounting(
        store, request, limits, &mut accounting, cancellation,
    ).await.map_err(|error| OperationFailure::new(
        super::regular_mutation::project_clone_error(error), accounting.spent(),
    ))
}

pub(super) fn validate_request(
    request: ExtentCloneRequest,
    limits: DecodeLimits,
) -> Result<(), ExtentCloneError> {
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
        Some(error) => Err(error),
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
