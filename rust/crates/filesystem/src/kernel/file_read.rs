//! Composed bounded regular-file reads across inline and sparse representations.

use super::{
    BlobReadError, DecodeLimits, ExtentKind, ExtentRangeRequest, ExtentReadError, FileKind,
    FilePayload, FileRecord, plan_extent_range_async, read_blob_range_async,
};
use crate::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::ByteRange;
use bytes::Bytes;
use thiserror::Error;

/// Complete bounded request for one regular-file byte range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileRangeRequest {
    /// Authenticated file record selected by namespace lookup.
    pub record: FileRecord,
    /// Exact logical range returned.
    pub range: ByteRange,
    /// Maximum sparse spans admitted in the result plan.
    pub maximum_spans: u32,
    /// Canonical page and allocation limits.
    pub limits: DecodeLimits,
    /// Exact implementation-work ceiling.
    pub budget: WorkBudget,
}

/// Successful logical file range and exact physical work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileRangeRead {
    /// Exact requested bytes, with holes and allocated zeros represented as zero.
    pub bytes: Bytes,
    /// Complete page, blob, copy, allocation, and output work.
    pub work: WorkCounters,
}

/// Reads one regular-file range without materializing untouched extents.
///
/// # Errors
///
/// Rejects non-regular files, invalid or excessive ranges, malformed sparse
/// trees and blobs, cancellation, storage failures, and work beyond budget.
pub async fn read_file_range_async<S: AsyncObjectStore>(
    store: &S,
    request: FileRangeRequest,
    cancellation: &CancellationToken,
) -> Result<FileRangeRead, FileRangeReadFailure> {
    cancellation
        .check()
        .map_err(|_| failed(FileRangeReadError::Cancelled, WorkCounters::default()))?;
    if request.record.kind != FileKind::Regular {
        return Err(failed(
            FileRangeReadError::NotRegular,
            WorkCounters::default(),
        ));
    }
    match request.record.payload {
        FilePayload::InlineRegular(data) => read_inline(data.as_bytes(), &request),
        FilePayload::Regular {
            logical_bytes,
            extents,
        } => read_sparse(store, logical_bytes, extents, &request, cancellation).await,
        _ => Err(failed(
            FileRangeReadError::NotRegular,
            WorkCounters::default(),
        )),
    }
}

fn read_inline(
    bytes: &[u8],
    request: &FileRangeRequest,
) -> Result<FileRangeRead, FileRangeReadFailure> {
    let end = request
        .range
        .offset
        .checked_add(request.range.length)
        .ok_or_else(|| failed(FileRangeReadError::InvalidRange, WorkCounters::default()))?;
    if end > u64::try_from(bytes.len()).unwrap_or(u64::MAX) {
        return Err(failed(
            FileRangeReadError::InvalidRange,
            WorkCounters::default(),
        ));
    }
    let start = usize::try_from(request.range.offset)
        .map_err(|_| failed(FileRangeReadError::InvalidRange, WorkCounters::default()))?;
    let end = usize::try_from(end)
        .map_err(|_| failed(FileRangeReadError::InvalidRange, WorkCounters::default()))?;
    let length = u64::try_from(end - start).unwrap_or(u64::MAX);
    let work = WorkCounters {
        bytes_copied: length,
        output_bytes: length,
        items_returned: 1,
        allocation_operations: u64::from(length != 0),
        peak_allocation_bytes: length,
        ..WorkCounters::default()
    };
    work.verify(request.budget)
        .map_err(|error| failed(error.into(), WorkCounters::default()))?;
    Ok(FileRangeRead {
        bytes: Bytes::copy_from_slice(&bytes[start..end]),
        work,
    })
}

async fn read_sparse<S: AsyncObjectStore>(
    store: &S,
    logical_bytes: u64,
    extents: crate::ObjectId,
    request: &FileRangeRequest,
    cancellation: &CancellationToken,
) -> Result<FileRangeRead, FileRangeReadFailure> {
    let plan = plan_extent_range_async(
        store,
        ExtentRangeRequest {
            root: extents,
            file_size: logical_bytes,
            range: request.range,
            maximum_spans: request.maximum_spans,
            limits: request.limits,
            budget: request.budget,
        },
        cancellation,
    )
    .await
    .map_err(|failure| failure.map_with_prior_work(WorkCounters::default(), Into::into))?;
    let mut work = plan.work;
    let output_len = usize::try_from(request.range.length)
        .map_err(|_| failed(FileRangeReadError::InvalidRange, work))?;
    let live_output = request.range.length;
    let initial_peak = plan
        .retained_allocation_bytes
        .checked_add(live_output)
        .ok_or_else(|| failed(FileRangeReadError::Work(WorkError::Overflow), work))?;
    work = add(
        work,
        WorkCounters {
            allocation_operations: u64::from(output_len != 0),
            peak_allocation_bytes: initial_peak,
            ..WorkCounters::default()
        },
    )?;
    work.verify(request.budget)
        .map_err(|error| failed(error.into(), work))?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(output_len)
        .map_err(|_| failed(FileRangeReadError::AllocationFailed, work))?;
    output.resize(output_len, 0);
    for span in &plan.spans {
        if let ExtentKind::Content {
            object,
            object_offset,
        } = span.kind
        {
            let nested = read_blob_range_async(
                store,
                object,
                ByteRange {
                    offset: object_offset,
                    length: span.length,
                },
                request.limits,
                remaining(work, request.budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
            let simultaneous = initial_peak
                .checked_add(nested.work.peak_allocation_bytes)
                .ok_or_else(|| failed(FileRangeReadError::Work(WorkError::Overflow), work))?;
            let mut nested_work = nested.work;
            nested_work.peak_allocation_bytes = 0;
            nested_work.output_bytes = 0;
            work = add(work, nested_work)?;
            work.peak_allocation_bytes = work.peak_allocation_bytes.max(simultaneous);
            let destination = usize::try_from(span.offset - request.range.offset)
                .map_err(|_| failed(FileRangeReadError::InvalidRange, work))?;
            let end = destination
                .checked_add(nested.bytes.len())
                .ok_or_else(|| failed(FileRangeReadError::InvalidRange, work))?;
            output[destination..end].copy_from_slice(&nested.bytes);
            work = add(
                work,
                WorkCounters {
                    bytes_copied: span.length,
                    ..WorkCounters::default()
                },
            )?;
            work.verify(request.budget)
                .map_err(|error| failed(error.into(), work))?;
        }
    }
    work = add(
        work,
        WorkCounters {
            output_bytes: request.range.length,
            items_returned: 1,
            ..WorkCounters::default()
        },
    )?;
    work.verify(request.budget)
        .map_err(|error| failed(error.into(), work))?;
    Ok(FileRangeRead {
        bytes: Bytes::from(output),
        work,
    })
}

fn add(prior: WorkCounters, next: WorkCounters) -> Result<WorkCounters, FileRangeReadFailure> {
    prior
        .checked_add(next)
        .map_err(|error| failed(error.into(), prior))
}

fn remaining(work: WorkCounters, budget: WorkBudget) -> Result<WorkBudget, FileRangeReadFailure> {
    work.remaining(budget)
        .map_err(|error| failed(error.into(), work))
}

/// Measured composed file-read failure.
pub type FileRangeReadFailure = OperationFailure<FileRangeReadError>;

/// Fail-closed regular-file range errors.
#[derive(Debug, Error)]
pub enum FileRangeReadError {
    /// The selected file is not regular.
    #[error("file range read requires a regular file")]
    NotRegular,
    /// Logical range overflows or exceeds file size.
    #[error("file range is invalid")]
    InvalidRange,
    /// Cooperative cancellation occurred before the next boundary.
    #[error("file range read was cancelled")]
    Cancelled,
    /// Exact output allocation was unavailable.
    #[error("file range output allocation failed")]
    AllocationFailed,
    /// Sparse extent planning failed.
    #[error(transparent)]
    Extent(#[from] ExtentReadError),
    /// Authenticated blob reading failed.
    #[error(transparent)]
    Blob(#[from] BlobReadError),
    /// Exact work overflowed or exceeded budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

fn failed(error: FileRangeReadError, work: WorkCounters) -> FileRangeReadFailure {
    OperationFailure::new(error, work)
}

#[cfg(test)]
#[path = "tests/file_read.rs"]
mod tests;
