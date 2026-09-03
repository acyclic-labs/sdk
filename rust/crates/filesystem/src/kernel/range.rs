//! Sparse extent planning that reads only page frontiers intersecting a request.

use super::allocation::{AllocationError, AllocationLedger, LogicalVecCapacity, VisitedObjectSet};
use super::codec::DecodedPageKind;
use super::extent::extent_page_decode_shape;
use super::frontier;
use super::frontier::Machine as _;
use super::{CanonicalDecodeError, DecodeLimits, ExtentKind, ExtentPage, decode_extent_page};
use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue,
};
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError};
use crate::speculation::{ResidencyHint, ResidencyReason};
use crate::storage::{
    ByteRange, ObjectId, ObjectKind, ObjectRead, ObjectReadRequest, ObjectReadRetention,
    ObjectReceipt, ObjectStore, ObjectStoreError,
};
use std::mem::size_of;
use std::sync::Arc;
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
    /// Exact kernel-visible work performed.
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
    Seek(OperationReceipt<Option<u64>>),
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

struct SpanCollector<'a> {
    input: PlanInput,
    range_end: u64,
    budget: WorkBudget,
    spans: &'a mut Vec<ExtentSlice>,
    work: &'a mut WorkCounters,
    allocations: &'a mut AllocationLedger,
    spans_capacity: &'a mut LogicalVecCapacity,
}

/// Complete admitted request shared by synchronous and asynchronous range
/// planners.
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
///
/// # Errors
///
/// Rejects invalid ranges, malformed page bounds, cycles, excessive height,
/// backend failures, or work beyond the admitted budget.
pub fn plan_extent_range<S: ObjectStore>(
    store: &S,
    request: ExtentRangeRequest,
) -> Result<ExtentPlan, ExtentReadFailure> {
    let input = PlanInput {
        file_size: request.file_size,
        range: request.range,
        maximum_spans: request.maximum_spans,
    };
    let mut machine = RangeMachine::new(
        request.root,
        input,
        TraversalMode::Plan,
        request.limits,
        request.budget,
    )?;
    match frontier::drive_sync(store, &mut machine)? {
        RangeOutput::Plan(plan) => Ok(plan),
        RangeOutput::Seek(_) => Err(failed(ExtentReadError::TraversalState, machine.work)),
    }
}

/// Asynchronously plans one exact sparse range through the same transition
/// machine as [`plan_extent_range`].
///
/// # Errors
///
/// Returns the same typed planning failures, including cooperative cancellation.
pub async fn plan_extent_range_async<S: AsyncObjectStore>(
    store: &S,
    request: ExtentRangeRequest,
    cancellation: &CancellationToken,
) -> Result<ExtentPlan, ExtentReadFailure> {
    if cancellation.is_cancelled() {
        return Err(failed(ExtentReadError::Cancelled, WorkCounters::default()));
    }
    let input = PlanInput {
        file_size: request.file_size,
        range: request.range,
        maximum_spans: request.maximum_spans,
    };
    let mut machine = RangeMachine::new(
        request.root,
        input,
        TraversalMode::Plan,
        request.limits,
        request.budget,
    )?;
    match drive_range_async(store, &mut machine, cancellation).await? {
        RangeOutput::Plan(plan) => Ok(plan),
        RangeOutput::Seek(_) => Err(failed(ExtentReadError::TraversalState, machine.work)),
    }
}

/// Finds one sparse data/hole boundary without restarting extent-tree
/// traversal or reading file-content objects.
///
/// # Errors
///
/// Rejects invalid offsets, malformed authenticated pages, cycles, excessive
/// height, cancellation, backend failures, or work beyond the admitted budget.
pub fn seek_extent<S: ObjectStore>(
    store: &S,
    request: ExtentSeekRequest,
) -> Result<OperationReceipt<Option<u64>>, ExtentReadFailure> {
    let mut machine = seek_machine(&request)?;
    match frontier::drive_sync(store, &mut machine)? {
        RangeOutput::Seek(receipt) => Ok(receipt),
        RangeOutput::Plan(_) => Err(failed(ExtentReadError::TraversalState, machine.work)),
    }
}

/// Asynchronous sparse seek through the same monotonic transition machine as
/// [`seek_extent`].
///
/// # Errors
///
/// Returns the same typed failures, including cooperative cancellation.
pub async fn seek_extent_async<S: AsyncObjectStore>(
    store: &S,
    request: ExtentSeekRequest,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<Option<u64>>, ExtentReadFailure> {
    if cancellation.is_cancelled() {
        return Err(failed(ExtentReadError::Cancelled, WorkCounters::default()));
    }
    let mut machine = seek_machine(&request)?;
    match drive_range_async(store, &mut machine, cancellation).await? {
        RangeOutput::Seek(receipt) => Ok(receipt),
        RangeOutput::Plan(_) => Err(failed(ExtentReadError::TraversalState, machine.work)),
    }
}

async fn drive_range_async<S: AsyncObjectStore>(
    store: &S,
    machine: &mut RangeMachine,
    cancellation: &CancellationToken,
) -> Result<RangeOutput, ExtentReadFailure> {
    loop {
        if cancellation.is_cancelled() {
            return Err(machine.cancelled());
        }
        if let Some(output) = machine.complete()? {
            return Ok(output);
        }
        let request = machine.prepare_read()?;
        let key = DecodedCacheKey::new::<ExtentPage>(request.page, machine.limits);
        let cached = store
            .decoded_cache_get(key)
            .map_err(|error| failed(error.into(), machine.work))?;
        if let Some(cached) = cached {
            machine.accept_cached_page(request.prospective, cached)?;
            continue;
        }
        let receipt = AsyncObjectStore::read(
            store,
            request.page,
            request.maximum_bytes,
            request.remaining,
            cancellation,
        )
        .await
        .map_err(|failure| machine.storage_failure(request.prospective, failure))?;
        machine.accept_page_with_cache(store, key, request.prospective, &receipt)?;
    }
}

fn seek_machine(request: &ExtentSeekRequest) -> Result<RangeMachine, ExtentReadFailure> {
    if request.offset > request.file_size {
        return Err(failed(
            ExtentReadError::InvalidRange,
            WorkCounters::default(),
        ));
    }
    RangeMachine::new(
        request.root,
        PlanInput {
            file_size: request.file_size,
            range: ByteRange {
                offset: request.offset,
                length: request.file_size - request.offset,
            },
            maximum_spans: 1,
        },
        TraversalMode::Seek(request.target),
        request.limits,
        request.budget,
    )
}

struct RangeMachine {
    input: PlanInput,
    mode: TraversalMode,
    seek_result: Option<u64>,
    range_end: u64,
    limits: DecodeLimits,
    budget: WorkBudget,
    pending: Vec<PendingPage>,
    pending_capacity: LogicalVecCapacity,
    awaiting: Option<PendingPage>,
    visited: VisitedObjectSet,
    spans: Vec<ExtentSlice>,
    spans_capacity: LogicalVecCapacity,
    work: WorkCounters,
    allocations: AllocationLedger,
    successor_page: Option<(u64, ResidencyHint)>,
}

impl RangeMachine {
    fn new(
        root: ObjectId,
        input: PlanInput,
        mode: TraversalMode,
        limits: DecodeLimits,
        budget: WorkBudget,
    ) -> Result<Self, ExtentReadFailure> {
        if root.kind != ObjectKind::ExtentPage {
            return Err(failed(
                ExtentReadError::WrongRootKind,
                WorkCounters::default(),
            ));
        }
        let range_end = input
            .range
            .offset
            .checked_add(input.range.length)
            .ok_or_else(|| failed(ExtentReadError::InvalidRange, WorkCounters::default()))?;
        if range_end > input.file_size {
            return Err(failed(
                ExtentReadError::InvalidRange,
                WorkCounters::default(),
            ));
        }
        if input.range.length != 0 && input.maximum_spans == 0 {
            return Err(failed(
                ExtentReadError::InvalidSpanLimit,
                WorkCounters::default(),
            ));
        }
        if !limits.page_limits_valid(1) {
            return Err(failed(
                ExtentReadError::HeightExceeded,
                WorkCounters::default(),
            ));
        }
        let maximum_visited = usize::try_from(limits.maximum_visited_pages)
            .map_err(|_| failed(ExtentReadError::InvalidLimits, WorkCounters::default()))?;
        let mut allocations = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let visited = VisitedObjectSet::new(maximum_visited, &mut allocations, &mut work, budget)
            .map_err(|error| failed(error.into(), work))?;
        let mut pending = Vec::new();
        let mut pending_capacity = LogicalVecCapacity::default();
        if input.range.length != 0 {
            pending_capacity
                .ensure_for_push(
                    &mut pending,
                    maximum_visited,
                    &mut allocations,
                    &mut work,
                    budget,
                )
                .map_err(|error| failed(error.into(), work))?;
            pending.push(PendingPage {
                page: root,
                first_offset: 0,
                end_offset: input.file_size,
                height: 1,
            });
        }
        Ok(Self {
            input,
            mode,
            seek_result: (input.range.length == 0
                && matches!(mode, TraversalMode::Seek(ExtentSeekTarget::Hole)))
            .then_some(input.file_size),
            range_end,
            limits,
            budget,
            pending,
            pending_capacity,
            awaiting: None,
            visited,
            spans: Vec::new(),
            spans_capacity: LogicalVecCapacity::default(),
            work,
            allocations,
            successor_page: None,
        })
    }

    fn finish(&mut self) -> Result<RangeOutput, ExtentReadFailure> {
        match self.mode {
            TraversalMode::Plan => {
                if self.input.range.length != 0
                    && (self.spans.first().map(|span| span.offset) != Some(self.input.range.offset)
                        || self
                            .spans
                            .last()
                            .and_then(|span| span.offset.checked_add(span.length))
                            != Some(self.range_end)
                        || self.spans.windows(2).any(|pair| {
                            pair[0].offset.checked_add(pair[0].length) != Some(pair[1].offset)
                        }))
                {
                    return Err(failed(ExtentReadError::IncompleteCoverage, self.work));
                }
                let retained_allocation_bytes = self.spans_capacity.logical_bytes();
                Ok(RangeOutput::Plan(ExtentPlan {
                    spans: std::mem::take(&mut self.spans),
                    work: self.work,
                    retained_allocation_bytes,
                    next_residency: self.successor_page.map(|(_, hint)| hint),
                }))
            }
            TraversalMode::Seek(target) => Ok(RangeOutput::Seek(OperationReceipt {
                value: self
                    .seek_result
                    .or_else(|| (target == ExtentSeekTarget::Hole).then_some(self.input.file_size)),
                work: self.work,
            })),
        }
    }

    fn prepare_read(&mut self) -> Result<frontier::ReadRequest, ExtentReadFailure> {
        let expected = self
            .pending
            .pop()
            .ok_or_else(|| failed(ExtentReadError::TraversalState, self.work))?;
        if expected.height > self.limits.maximum_page_height {
            return Err(failed(ExtentReadError::HeightExceeded, self.work));
        }
        let visited = self
            .visited
            .insert(
                expected.page,
                &mut self.allocations,
                &mut self.work,
                self.budget,
            )
            .map_err(|error| failed(error.into(), self.work))?;
        if !visited.inserted {
            return Err(failed(ExtentReadError::Cycle, self.work));
        }
        let prospective = self
            .work
            .checked_add(WorkCounters {
                page_reads: 1,
                ..WorkCounters::default()
            })
            .map_err(|error| failed(error.into(), self.work))?;
        let mut remaining = prospective
            .remaining(self.budget)
            .map_err(|error| failed(error.into(), self.work))?;
        remaining.peak_allocation_bytes = self
            .budget
            .peak_allocation_bytes
            .checked_sub(self.allocations.live_bytes())
            .ok_or_else(|| failed(ExtentReadError::Work(WorkError::Overflow), self.work))?;
        let page = expected.page;
        self.awaiting = Some(expected);
        Ok(frontier::ReadRequest {
            page,
            maximum_bytes: self.limits.maximum_page_object_bytes(),
            remaining,
            prospective,
        })
    }

    fn accept_page(
        &mut self,
        prospective: WorkCounters,
        receipt: &ObjectReceipt<ObjectRead>,
    ) -> Result<Option<RangeOutput>, ExtentReadFailure> {
        let expected = self
            .awaiting
            .take()
            .ok_or_else(|| failed(ExtentReadError::TraversalState, self.work))?;
        self.work = merge_backend_work(prospective, receipt.work, self.allocations.live_bytes())
            .map_err(|error| failed(error.into(), prospective))?;
        self.work
            .verify(self.budget)
            .map_err(|error| failed(error.into(), self.work))?;
        let (decoded, decoded_bytes, retained_bytes) = self.decode_page(receipt)?;
        let result = self.accept_decoded(&decoded, &expected);
        self.allocations
            .release(decoded_bytes)
            .map_err(|error| failed(error.into(), self.work))?;
        self.allocations
            .release(retained_bytes)
            .map_err(|error| failed(error.into(), self.work))?;
        result?;
        Ok(None)
    }

    fn decode_page(
        &mut self,
        receipt: &ObjectReceipt<ObjectRead>,
    ) -> Result<(ExtentPage, u64, u64), ExtentReadFailure> {
        let retained_bytes = match receipt.value.retention {
            ObjectReadRetention::Shared => 0,
            ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
        };
        self.allocations
            .claim_bytes(retained_bytes, 0, &mut self.work, self.budget)
            .map_err(|error| failed(error.into(), self.work))?;
        let shape = extent_page_decode_shape(&receipt.value, self.limits)
            .map_err(|error| failed(error.into(), self.work))?;
        self.add_work(WorkCounters {
            items_examined: u64::try_from(shape.items).unwrap_or(u64::MAX),
            ..WorkCounters::default()
        })?;
        let item_bytes = match shape.kind {
            DecodedPageKind::Leaf => size_of::<super::Extent>(),
            DecodedPageKind::Internal => size_of::<super::ExtentChild>(),
        };
        let decoded_bytes = shape
            .items
            .checked_mul(item_bytes)
            .map(crate::foundation::usize_to_u64)
            .ok_or_else(|| failed(ExtentReadError::AllocationFailed, self.work))?;
        self.allocations
            .claim_bytes(
                decoded_bytes,
                u64::from(decoded_bytes != 0),
                &mut self.work,
                self.budget,
            )
            .map_err(|error| failed(error.into(), self.work))?;
        let decoded = decode_extent_page(&receipt.value, self.limits)
            .map_err(|error| failed(error.into(), self.work))?;
        Ok((decoded, decoded_bytes, retained_bytes))
    }

    fn accept_cached_page(
        &mut self,
        prospective: WorkCounters,
        cached: DecodedCacheValue,
    ) -> Result<(), ExtentReadFailure> {
        let expected = self
            .awaiting
            .take()
            .ok_or_else(|| failed(ExtentReadError::TraversalState, self.work))?;
        self.work = prospective;
        let page = cached
            .value
            .downcast::<ExtentPage>()
            .map_err(|_| failed(ObjectStoreError::Corrupt.into(), self.work))?;
        self.accept_decoded(&page, &expected)
    }

    fn accept_page_with_cache<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        key: DecodedCacheKey,
        prospective: WorkCounters,
        receipt: &ObjectReceipt<ObjectRead>,
    ) -> Result<(), ExtentReadFailure> {
        let expected = self
            .awaiting
            .take()
            .ok_or_else(|| failed(ExtentReadError::TraversalState, self.work))?;
        self.work = merge_backend_work(prospective, receipt.work, self.allocations.live_bytes())
            .map_err(|error| failed(error.into(), prospective))?;
        self.work
            .verify(self.budget)
            .map_err(|error| failed(error.into(), self.work))?;
        let (decoded, decoded_bytes, retained_bytes) = self.decode_page(receipt)?;
        let admitted = store.decoded_cache_admit(
            key,
            DecodedCacheValue {
                value: Arc::new(decoded),
                logical_bytes: decoded_bytes,
            },
        );
        let result = match admitted {
            Ok(DecodedCacheAdmission::Shared(value)) => {
                self.allocations
                    .release(decoded_bytes)
                    .map_err(|error| failed(error.into(), self.work))?;
                let page = value
                    .value
                    .downcast::<ExtentPage>()
                    .map_err(|_| failed(ObjectStoreError::Corrupt.into(), self.work))?;
                self.accept_decoded(&page, &expected)
            }
            Ok(DecodedCacheAdmission::Uncached(value)) => {
                let page = value
                    .value
                    .downcast::<ExtentPage>()
                    .map_err(|_| failed(ObjectStoreError::Corrupt.into(), self.work))?;
                let result = self.accept_decoded(&page, &expected);
                self.allocations
                    .release(decoded_bytes)
                    .map_err(|error| failed(error.into(), self.work))?;
                result
            }
            Err(error) => {
                self.allocations
                    .release(decoded_bytes)
                    .map_err(|release| failed(release.into(), self.work))?;
                Err(failed(error.into(), self.work))
            }
        };
        self.allocations
            .release(retained_bytes)
            .map_err(|error| failed(error.into(), self.work))?;
        result
    }

    fn accept_decoded(
        &mut self,
        decoded: &ExtentPage,
        expected: &PendingPage,
    ) -> Result<(), ExtentReadFailure> {
        match decoded {
            ExtentPage::Leaf(extents) => match self.mode {
                TraversalMode::Plan => {
                    SpanCollector {
                        input: self.input,
                        range_end: self.range_end,
                        budget: self.budget,
                        spans: &mut self.spans,
                        work: &mut self.work,
                        allocations: &mut self.allocations,
                        spans_capacity: &mut self.spans_capacity,
                    }
                    .collect(extents, expected)
                    .map_err(|error| failed(error, self.work))?;
                }
                TraversalMode::Seek(target) => {
                    self.accept_seek_leaf(extents, expected, target)?;
                }
            },
            ExtentPage::Internal(children) => {
                if children.first().map(|child| child.first_offset) != Some(expected.first_offset)
                    || children.last().map(|child| child.end_offset) != Some(expected.end_offset)
                {
                    return Err(failed(ExtentReadError::ChildBoundsMismatch, self.work));
                }
                for child in children.iter().rev() {
                    self.work = self
                        .work
                        .checked_add(WorkCounters {
                            items_examined: 1,
                            ..WorkCounters::default()
                        })
                        .map_err(|error| failed(error.into(), self.work))?;
                    self.work
                        .verify(self.budget)
                        .map_err(|error| failed(error.into(), self.work))?;
                    if child.first_offset < self.range_end
                        && child.end_offset > self.input.range.offset
                    {
                        self.pending_capacity
                            .ensure_for_push(
                                &mut self.pending,
                                usize::try_from(self.limits.maximum_visited_pages)
                                    .unwrap_or(usize::MAX),
                                &mut self.allocations,
                                &mut self.work,
                                self.budget,
                            )
                            .map_err(|error| failed(error.into(), self.work))?;
                        self.pending.push(PendingPage {
                            page: child.page,
                            first_offset: child.first_offset,
                            end_offset: child.end_offset,
                            height: expected.height.checked_add(1).ok_or_else(|| {
                                failed(ExtentReadError::HeightExceeded, self.work)
                            })?,
                        });
                    } else if self.input.range.length != 0
                        && child.first_offset >= self.range_end
                        && self
                            .successor_page
                            .is_none_or(|(offset, _)| child.first_offset < offset)
                    {
                        self.successor_page = Some((
                            child.first_offset,
                            ResidencyHint {
                                request: ObjectReadRequest {
                                    object_id: child.page,
                                    maximum_bytes: self.limits.maximum_page_object_bytes(),
                                },
                                reason: ResidencyReason::SequentialRange,
                            },
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn accept_seek_leaf(
        &mut self,
        extents: &[super::Extent],
        expected: &PendingPage,
        target: ExtentSeekTarget,
    ) -> Result<(), ExtentReadFailure> {
        if extents.first().map(|extent| extent.offset) != Some(expected.first_offset)
            || extents
                .last()
                .and_then(|extent| extent.offset.checked_add(extent.length))
                != Some(expected.end_offset)
        {
            return Err(failed(ExtentReadError::ChildBoundsMismatch, self.work));
        }
        for extent in extents {
            self.add_work(WorkCounters {
                items_examined: 1,
                ..WorkCounters::default()
            })?;
            let end = extent
                .offset
                .checked_add(extent.length)
                .ok_or_else(|| failed(ExtentReadError::InvalidRange, self.work))?;
            if end <= self.input.range.offset {
                continue;
            }
            let matches = match target {
                ExtentSeekTarget::Data => !matches!(extent.kind, ExtentKind::Hole),
                ExtentSeekTarget::Hole => matches!(extent.kind, ExtentKind::Hole),
            };
            if matches {
                self.seek_result = Some(extent.offset.max(self.input.range.offset));
                self.pending.clear();
                return Ok(());
            }
        }
        Ok(())
    }

    fn add_work(&mut self, delta: WorkCounters) -> Result<(), ExtentReadFailure> {
        let prospective = self
            .work
            .checked_add(delta)
            .map_err(|error| failed(error.into(), self.work))?;
        prospective
            .verify(self.budget)
            .map_err(|error| failed(error.into(), self.work))?;
        self.work = prospective;
        Ok(())
    }
}

impl frontier::Machine for RangeMachine {
    type Output = RangeOutput;
    type Failure = ExtentReadFailure;

    fn complete(&mut self) -> Result<Option<Self::Output>, Self::Failure> {
        if self.pending.is_empty() && self.awaiting.is_none() {
            return self.finish().map(Some);
        }
        Ok(None)
    }

    fn prepare_read(&mut self) -> Result<frontier::ReadRequest, Self::Failure> {
        RangeMachine::prepare_read(self)
    }

    fn accept(
        &mut self,
        prospective: WorkCounters,
        receipt: &ObjectReceipt<ObjectRead>,
    ) -> Result<Option<Self::Output>, Self::Failure> {
        self.accept_page(prospective, receipt)
    }

    fn storage_failure(
        &self,
        prospective: WorkCounters,
        failure: crate::storage::ObjectFailure,
    ) -> Self::Failure {
        match prospective.checked_add(*failure.work) {
            Ok(combined) => failed(failure.error.into(), combined),
            Err(error) => failed(error.into(), prospective),
        }
    }

    fn cancelled(&self) -> Self::Failure {
        failed(ExtentReadError::Cancelled, self.work)
    }
}

impl SpanCollector<'_> {
    fn collect(
        &mut self,
        extents: &[super::Extent],
        expected: &PendingPage,
    ) -> Result<(), ExtentReadError> {
        if extents.first().map(|extent| extent.offset) != Some(expected.first_offset)
            || extents
                .last()
                .and_then(|extent| extent.offset.checked_add(extent.length))
                != Some(expected.end_offset)
        {
            return Err(ExtentReadError::ChildBoundsMismatch);
        }
        for extent in extents {
            *self.work = self.work.checked_add(WorkCounters {
                items_examined: 1,
                ..WorkCounters::default()
            })?;
            self.work.verify(self.budget)?;
            let extent_end = extent
                .offset
                .checked_add(extent.length)
                .ok_or(ExtentReadError::InvalidRange)?;
            let start = extent.offset.max(self.input.range.offset);
            let end = extent_end.min(self.range_end);
            if start < end {
                if u32::try_from(self.spans.len()).unwrap_or(u32::MAX) >= self.input.maximum_spans {
                    return Err(ExtentReadError::TooManySpans);
                }
                self.spans_capacity.ensure_for_push(
                    self.spans,
                    usize::try_from(self.input.maximum_spans)
                        .map_err(|_| ExtentReadError::InvalidSpanLimit)?,
                    self.allocations,
                    self.work,
                    self.budget,
                )?;
                self.spans.push(ExtentSlice {
                    offset: start,
                    length: end - start,
                    source_end: extent_end,
                    kind: clip_kind(extent.kind.clone(), start - extent.offset)?,
                });
                *self.work = self.work.checked_add(WorkCounters {
                    items_returned: 1,
                    ..WorkCounters::default()
                })?;
                self.work.verify(self.budget)?;
            }
        }
        Ok(())
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
}

impl From<AllocationError> for ExtentReadError {
    fn from(error: AllocationError) -> Self {
        match error {
            AllocationError::Work(error) => Self::Work(error),
            AllocationError::Overflow | AllocationError::ReleaseInvariant => {
                Self::Work(WorkError::Overflow)
            }
            AllocationError::InvalidCapacity
            | AllocationError::CapacityExceeded
            | AllocationError::AllocationFailed => Self::AllocationFailed,
        }
    }
}

fn merge_backend_work(
    prior: WorkCounters,
    mut backend: WorkCounters,
    live_bytes: u64,
) -> Result<WorkCounters, WorkError> {
    let simultaneous_peak = live_bytes
        .checked_add(backend.peak_allocation_bytes)
        .ok_or(WorkError::Overflow)?;
    backend.peak_allocation_bytes = 0;
    let mut merged = prior.checked_add(backend)?;
    merged.peak_allocation_bytes = merged.peak_allocation_bytes.max(simultaneous_peak);
    Ok(merged)
}

/// Sparse extent-plan failure retaining exact spent work.
pub type ExtentReadFailure = OperationFailure<ExtentReadError>;

fn failed(error: ExtentReadError, work: WorkCounters) -> ExtentReadFailure {
    OperationFailure::new(error, work)
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/range.rs"]
mod tests;
