//! Sparse extent planning that reads only page frontiers intersecting a request.

use super::allocation::{AllocationError, KernelAccounting, KernelOwnedAllocation, LogicalVecCapacity, VisitedObjectSet};
use super::file_read::{ReadOwned, release_read_allocation};
use super::{CanonicalDecodeError, DecodeLimits, ExtentKind, ExtentPage};
use crate::FsError;
use crate::async_storage::{AsyncObjectStore, DecodedCacheKey};
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError};
use crate::speculation::{ResidencyHint, ResidencyReason};
use crate::storage::{ByteRange, ObjectId, ObjectKind, ObjectReadRequest, ObjectStoreError};
use thiserror::Error;

/// One clipped logical span required to satisfy a range read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentSlice {
    /// Inclusive logical file offset.
    pub offset: u64,
    /// Positive bytes in this slice.
    pub length: u64,
    /// Exclusive end of the complete canonical extent containing this slice.
    pub source_end: u64,
    /// Physical representation, with content offset adjusted to the slice.
    pub kind: ExtentKind,
}

/// Bounded range plan and exact page-read evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentPlan {
    /// Ordered contiguous spans covering the requested range.
    pub spans: Vec<ExtentSlice>,
    /// Exact completed work in the supplied accounting context.
    pub work: WorkCounters,
    /// Logical owned capacity retained by `spans` in this returned plan.
    pub retained_allocation_bytes: u64,
    /// Exact authenticated forward page exposed by this traversal, if any.
    pub next_residency: Option<ResidencyHint>,
}

/// Native sparse seek class.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ExtentSeekTarget {
    /// First content or physically allocated-zero byte.
    Data,
    /// First unallocated hole byte, including logical end-of-file.
    Hole,
}

/// Complete admitted sparse seek request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExtentSeekRequest {
    /// Authenticated extent-tree root.
    pub root: ObjectId,
    /// Complete logical file size.
    pub file_size: u64,
    /// Inclusive logical starting offset.
    pub offset: u64,
    /// Sparse class to locate.
    pub target: ExtentSeekTarget,
    /// Canonical page and decode limits.
    pub limits: DecodeLimits,
    /// Exact admitted implementation work.
    pub budget: WorkBudget,
}

#[derive(Clone, Copy)]
enum TraversalMode {
    Plan,
    Seek(ExtentSeekTarget),
}

enum RangeOutput {
    Plan(ExtentPlan),
    Seek(Option<u64>),
}

struct PendingPage {
    page: ObjectId,
    first_offset: u64,
    end_offset: u64,
    height: u16,
}

#[derive(Clone, Copy)]
struct PlanInput {
    file_size: u64,
    range: ByteRange,
    maximum_spans: u32,
}

/// Complete admitted request shared by synchronous and asynchronous range planners.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExtentRangeRequest {
    /// Authenticated extent-tree root.
    pub root: ObjectId,
    /// Complete logical file size.
    pub file_size: u64,
    /// Exact logical range to cover.
    pub range: ByteRange,
    /// Maximum sparse spans returned.
    pub maximum_spans: u32,
    /// Canonical page and decode limits.
    pub limits: DecodeLimits,
    /// Exact admitted implementation work.
    pub budget: WorkBudget,
}

/// Plans one exact file range without reading unrelated extent pages or blobs.
pub async fn plan_extent_range<S: AsyncObjectStore>(
    store: &S, request: ExtentRangeRequest, cancellation: &CancellationToken,
) -> Result<ExtentPlan, ExtentReadFailure> {
    let mut accounting = KernelAccounting::standalone(request.budget, cancellation);
    plan_extent_range_with_accounting(store, request, &mut accounting, cancellation).await
        .map(|(plan, _)| plan)
        .map_err(|error| failed(range_error(error), accounting.spent()))
}

pub(crate) async fn plan_extent_range_with_accounting<S: AsyncObjectStore>(
    store: &S, request: ExtentRangeRequest, accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(ExtentPlan, KernelOwnedAllocation), FsError> {
    let result = ReadOwned::from(run_range_async(store, request, TraversalMode::Plan, accounting, cancellation).await?);
    match result.value {
        RangeOutput::Plan(plan) => Ok((plan, result.custody)),
        _ => Err(range_failure(ExtentReadError::TraversalState)),
    }
}

/// Finds a data or hole boundary without reading content objects.
pub async fn seek_extent<S: AsyncObjectStore>(
    store: &S, request: ExtentSeekRequest, cancellation: &CancellationToken,
) -> Result<OperationReceipt<Option<u64>>, ExtentReadFailure> {
    let mut accounting = KernelAccounting::standalone(request.budget, cancellation);
    let result = match seek_request(request) {
        Ok(range) => {
            run_range_async(store, range, TraversalMode::Seek(request.target), &mut accounting, cancellation).await
        }
        Err(error) => Err(error),
    };
    match result {
        Ok((RangeOutput::Seek(value), _)) => Ok(OperationReceipt { value, work: accounting.spent() }),
        Ok(_) => Err(failed(ExtentReadError::TraversalState, accounting.spent())),
        Err(error) => Err(failed(range_error(error), accounting.spent())),
    }
}

pub(crate) async fn seek_extent_with_accounting<S: AsyncObjectStore>(
    store: &S, request: ExtentSeekRequest, accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<u64>, FsError> {
    let result = ReadOwned::from(run_range_async(store, seek_request(request)?, TraversalMode::Seek(request.target), accounting, cancellation).await?);
    match result.value {
        RangeOutput::Seek(value) => Ok(value),
        _ => Err(range_failure(ExtentReadError::TraversalState)),
    }
}

fn seek_request(request: ExtentSeekRequest) -> Result<ExtentRangeRequest, FsError> {
    let length = request.file_size.checked_sub(request.offset)
        .ok_or_else(|| range_failure(ExtentReadError::InvalidRange))?;
    Ok(ExtentRangeRequest { root: request.root, file_size: request.file_size,
        range: ByteRange { offset: request.offset, length }, maximum_spans: 1,
        limits: request.limits, budget: request.budget })
}

struct RangeMachine {
    input: PlanInput,
    mode: TraversalMode,
    seek_result: Option<u64>,
    range_end: u64,
    limits: DecodeLimits,
    pending: Vec<PendingPage>,
    pending_capacity: LogicalVecCapacity,
    visited: Option<super::persistent_point::Owned<VisitedObjectSet>>,
    spans: Vec<ExtentSlice>,
    spans_capacity: LogicalVecCapacity,
    successor_page: Option<(u64, ResidencyHint)>,
    // Data fields precede their original physical allocation guards.
    pending_custody: KernelOwnedAllocation,
    spans_custody: KernelOwnedAllocation,
}

impl RangeMachine {
    fn new(request: ExtentRangeRequest, mode: TraversalMode, accounting: &mut KernelAccounting<'_>) -> Result<Self, FsError> {
        accounting.check()?;
        if request.root.kind != ObjectKind::ExtentPage { return Err(range_failure(ExtentReadError::WrongRootKind)); }
        let range_end = request.range.offset.checked_add(request.range.length)
            .filter(|end| *end <= request.file_size)
            .ok_or_else(|| range_failure(ExtentReadError::InvalidRange))?;
        if request.range.length != 0 && request.maximum_spans == 0 { return Err(range_failure(ExtentReadError::InvalidSpanLimit)); }
        if !request.limits.page_limits_valid(1) { return Err(range_failure(ExtentReadError::HeightExceeded)); }
        let maximum = usize::try_from(request.limits.maximum_visited_pages)
            .map_err(|_| range_failure(ExtentReadError::InvalidLimits))?;
        let visited = VisitedObjectSet::new_with_accounting(maximum, accounting)?;
        let mut machine = Self {
            input: PlanInput { file_size: request.file_size, range: request.range, maximum_spans: request.maximum_spans },
            mode, seek_result: (request.range.length == 0 && matches!(mode, TraversalMode::Seek(ExtentSeekTarget::Hole))).then_some(request.file_size),
            range_end, limits: request.limits, pending: Vec::new(), pending_capacity: LogicalVecCapacity::default(),
            visited: Some(visited), spans: Vec::new(), spans_capacity: LogicalVecCapacity::default(), successor_page: None,
            pending_custody: KernelOwnedAllocation::Standalone, spans_custody: KernelOwnedAllocation::Standalone,
        };
        if request.range.length != 0 {
            machine.push_page(PendingPage { page: request.root, first_offset: 0, end_offset: request.file_size, height: 1 }, accounting)?;
        }
        Ok(machine)
    }

    fn push_page(&mut self, page: PendingPage, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        self.pending_capacity.ensure_for_push_with_accounting(
            &mut self.pending,
            usize::try_from(self.limits.maximum_visited_pages).map_err(|_| range_failure(ExtentReadError::InvalidLimits))?,
            &mut self.pending_custody, accounting,
        )?;
        self.pending.push(page);
        Ok(())
    }

    fn prepare(&mut self, accounting: &mut KernelAccounting<'_>) -> Result<PendingPage, FsError> {
        let expected = self.pending.pop().ok_or_else(|| range_failure(ExtentReadError::TraversalState))?;
        if expected.height > self.limits.maximum_page_height { return Err(range_failure(ExtentReadError::HeightExceeded)); }
        let visited = self.visited.as_mut().ok_or_else(|| range_failure(ExtentReadError::TraversalState))?;
        let inserted = VisitedObjectSet::insert_with_accounting(visited, expected.page, accounting)?;
        if !inserted.inserted { return Err(range_failure(ExtentReadError::Cycle)); }
        accounting.charge(WorkCounters { page_reads: 1, ..WorkCounters::UNCHARGED })?;
        Ok(expected)
    }

    fn accept_decoded(&mut self, decoded: &ExtentPage, expected: &PendingPage, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        match decoded {
            ExtentPage::Leaf(extents) => {
                if extents.first().map(|extent| extent.offset) != Some(expected.first_offset)
                    || extents.last().and_then(|extent| extent.offset.checked_add(extent.length)) != Some(expected.end_offset) {
                    return Err(range_failure(ExtentReadError::ChildBoundsMismatch));
                }
                for extent in extents {
                    accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    let end = extent.offset.checked_add(extent.length).ok_or_else(|| range_failure(ExtentReadError::InvalidRange))?;
                    match self.mode {
                        TraversalMode::Seek(target) => {
                            if end > self.input.range.offset && match target {
                                ExtentSeekTarget::Data => {
                                        !matches!(extent.kind, ExtentKind::Hole)
                                    }
                                    ExtentSeekTarget::Hole => {
                                        matches!(extent.kind, ExtentKind::Hole)
                                    }
                                } {
                                self.seek_result = Some(extent.offset.max(self.input.range.offset));
                                self.pending.clear();
                                break;
                            }
                        }
                        TraversalMode::Plan => {
                            let start = extent.offset.max(self.input.range.offset);
                            let clipped_end = end.min(self.range_end);
                            if start < clipped_end {
                                if self.spans.len() >= usize::try_from(self.input.maximum_spans).unwrap_or(usize::MAX) {
                                    return Err(range_failure(ExtentReadError::TooManySpans));
                                }
                                let kind = clip_kind(extent.kind.clone(), start - extent.offset).map_err(range_failure)?;
                                self.spans_capacity.ensure_for_push_with_accounting(
                                    &mut self.spans,
                                    usize::try_from(self.input.maximum_spans).map_err(|_| {
                                        range_failure(ExtentReadError::InvalidSpanLimit)
                                    })?,
                                    &mut self.spans_custody, accounting,
                                )?;
                                accounting.admit(WorkCounters { items_returned: 1, ..WorkCounters::UNCHARGED })?;
                                self.spans.push(ExtentSlice { offset: start, length: clipped_end - start, source_end: end, kind });
                                accounting.record_completed(WorkCounters { items_returned: 1, ..WorkCounters::UNCHARGED })?;
                            }
                        }
                    }
                }
            }
            ExtentPage::Internal(children) => {
                if children.first().map(|child| child.first_offset) != Some(expected.first_offset)
                    || children.last().map(|child| child.end_offset) != Some(expected.end_offset) {
                    return Err(range_failure(ExtentReadError::ChildBoundsMismatch));
                }
                for child in children.iter().rev() {
                    accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    if child.first_offset < self.range_end && child.end_offset > self.input.range.offset {
                        self.push_page(PendingPage { page: child.page, first_offset: child.first_offset, end_offset: child.end_offset,
                            height: expected.height.checked_add(1).ok_or_else(|| {
                                    range_failure(ExtentReadError::HeightExceeded)
                                })? }, accounting)?;
                    } else if self.input.range.length != 0 && child.first_offset >= self.range_end
                        && self.successor_page.is_none_or(|(offset, _)| child.first_offset < offset) {
                        self.successor_page = Some((child.first_offset, ResidencyHint {
                            request: ObjectReadRequest { object_id: child.page, maximum_bytes: self.limits.maximum_page_object_bytes() },
                            reason: ResidencyReason::SequentialRange,
                        }));
                    }
                }
            }
        }
        Ok(())
    }

    fn finish(mut self, accounting: &mut KernelAccounting<'_>) -> Result<(RangeOutput, KernelOwnedAllocation), FsError> {
        let output = match self.mode {
            TraversalMode::Plan => {
                if self.input.range.length != 0 && (self.spans.first().map(|span| span.offset) != Some(self.input.range.offset)
                    || self.spans.last().and_then(|span| span.offset.checked_add(span.length)) != Some(self.range_end)
                    || self.spans.windows(2).any(|pair| match pair { [a, b] => a.offset.checked_add(a.length) != Some(b.offset), _ => false })) {
                    return Err(range_failure(ExtentReadError::IncompleteCoverage));
                }
                RangeOutput::Plan(ExtentPlan { spans: std::mem::take(&mut self.spans), work: accounting.spent(),
                    retained_allocation_bytes: self.spans_capacity.logical_bytes(), next_residency: self.successor_page.map(|(_, hint)| hint) })
            }
            TraversalMode::Seek(target) => RangeOutput::Seek(
                self.seek_result.or_else(|| (target == ExtentSeekTarget::Hole).then_some(self.input.file_size)),
            ),
        };
        let output = ReadOwned {
            value: output,
            custody: std::mem::replace(&mut self.spans_custody, KernelOwnedAllocation::Standalone),
        };
        let pending_bytes = self.pending_capacity.logical_bytes();
        drop(self.pending);
        release_read_allocation(pending_bytes, self.pending_custody, accounting)?;
        if let Some(visited) = self.visited {
            visited.retire(accounting)?;
        }
        Ok((output.value, output.custody))
    }
}

async fn run_range_async<S: AsyncObjectStore>(
    store: &S, request: ExtentRangeRequest, mode: TraversalMode, accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(RangeOutput, KernelOwnedAllocation), FsError> {
    let mut machine = RangeMachine::new(request, mode, accounting)?;
    while !machine.pending.is_empty() {
        let expected = machine.prepare(accounting)?;
        let key = DecodedCacheKey::new::<ExtentPage>(expected.page, request.limits);
        #[cfg(not(target_arch = "wasm32"))]
        let cached = if let Some(original) = accounting.native_work() {
            {
                let __decoded_args = (key, original);
                __decoded_args
                    .1
                    .remaining()
                    .map_err(crate::ObjectStoreError::from)
                    .and_then(|_| store.decoded_cache_get(__decoded_args.0))
            }
        } else { store.decoded_cache_get(key) };
        #[cfg(target_arch = "wasm32")]
        let cached = store.decoded_cache_get(key);
        if let Some(cached) = cached.map_err(crate::kernel::file_read::read_store_error)? {
            let page = cached.value.downcast_ref::<ExtentPage>().ok_or_else(|| range_failure(ObjectStoreError::Corrupt.into()))?;
            machine.accept_decoded(page, &expected, accounting)?;
            
            drop(cached);
            continue;
        }
        let (read, retained) = super::file_read::read_object_with_accounting(
            store, expected.page, request.limits.maximum_page_object_bytes(), accounting, cancellation,
        ).await?;
        let decoded = ReadOwned::decoded(super::extent::decode_extent_page_with_accounting(&read, request.limits, accounting)?);
        let (value, allocated) = super::file_read::cache_decoded_read(store, key, decoded, accounting)?;
        let page = value.value.downcast_ref::<ExtentPage>().ok_or_else(|| range_failure(ObjectStoreError::Corrupt.into()))?;
        machine.accept_decoded(page, &expected, accounting)?;
        
        drop(value);
        super::file_read::release_standalone_read_bytes(allocated, accounting)?;
        drop(read);
        super::file_read::release_standalone_read_bytes(retained, accounting)?;
    }
    machine.finish(accounting)
}

fn range_failure(error: ExtentReadError) -> FsError { super::FileRangeReadError::Extent(error).into() }
fn range_error(error: FsError) -> ExtentReadError {
    match error {
        FsError::FileRead(super::FileRangeReadError::Extent(error)) => error,
        FsError::FileRead(super::FileRangeReadError::AllocationFailed) => {
            ExtentReadError::AllocationFailed
        }
        FsError::Work(error) => error.into(),
        FsError::BlobRead(super::BlobReadError::Storage(error)) => error.into(),
        FsError::Cancelled(_) => ExtentReadError::Cancelled,
        error => ExtentReadError::Context(Box::new(error)),
    }
}

fn clip_kind(kind: ExtentKind, delta: u64) -> Result<ExtentKind, ExtentReadError> {
    match kind {
        ExtentKind::Content {
            object,
            object_offset,
        } => Ok(ExtentKind::Content {
            object,
            object_offset: object_offset
                .checked_add(delta)
                .ok_or(ExtentReadError::InvalidRange)?,
        }),
        other => Ok(other),
    }
}

/// Sparse extent range-planning failures.
#[derive(Debug, Error)]
pub enum ExtentReadError {
    /// Cooperative cancellation occurred before the next storage boundary.
    #[error("extent range planning was cancelled")]
    Cancelled,
    /// The private driver attempted an impossible transition.
    #[error("extent range transition state is invalid")]
    TraversalState,
    /// Root object is not an authenticated extent page.
    #[error("extent root is not an extent page")]
    WrongRootKind,
    /// Requested range exceeds the file or overflows.
    #[error("requested file range is invalid")]
    InvalidRange,
    /// Requested output-span bound must be positive for a non-empty range.
    #[error("extent span limit must be non-zero")]
    InvalidSpanLimit,
    /// Decode or traversal scratch could not be admitted or allocated.
    #[error("extent planning scratch allocation failed")]
    AllocationFailed,
    /// Decode or traversal limits are not representable on this target.
    #[error("extent planning limits are invalid")]
    InvalidLimits,
    /// Intersecting sparse spans exceed the caller's admitted output bound.
    #[error("extent plan exceeds its admitted span count")]
    TooManySpans,
    /// Child graph references an ancestor or repeated page.
    #[error("extent page graph contains a cycle or alias")]
    Cycle,
    /// Traversal exceeds its hard page-height bound.
    #[error("extent page height exceeds its admitted bound")]
    HeightExceeded,
    /// Parent child range does not match the decoded child content.
    #[error("extent child bounds do not match its page")]
    ChildBoundsMismatch,
    /// Planned spans do not cover the request exactly once.
    #[error("extent plan does not cover the requested range exactly")]
    IncompleteCoverage,
    /// Stored page exceeds its decode/work bound.
    #[error("extent page has {observed} bytes; maximum is {maximum}")]
    PageTooLarge {
        /// Observed canonical bytes.
        observed: u64,
        /// Admitted maximum.
        maximum: u64,
    },
    /// Immutable-object backend failed.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Canonical page failed decoding or validation.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// Exact work exceeded or overflowed its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
    /// Shared original-ledger read boundary failed.
    #[error(transparent)]
    Context(Box<FsError>),
}

impl From<AllocationError> for ExtentReadError {
    fn from(error: AllocationError) -> Self {
        error.into_work_or(Self::AllocationFailed)
    }
}

/// Sparse extent-plan failure retaining exact spent work.
pub type ExtentReadFailure = OperationFailure<ExtentReadError>;

fn failed(error: ExtentReadError, work: WorkCounters) -> ExtentReadFailure {
    OperationFailure::new(error, work)
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/range.rs"]
mod tests;
