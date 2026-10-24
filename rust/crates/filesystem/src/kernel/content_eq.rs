//! Content equality independent of sparse extents and blob-index topology.

use super::allocation::KernelAccounting;
use super::file_read::{release_read_allocation, release_standalone_read_bytes};
use super::*;
use crate::{AsyncObjectStore, ByteRange, CancellationToken, FsError, WorkCounters};

#[derive(Clone, Copy)]
enum BytesAt {
    Zero,
    Inline,
    Chunk {
        object: crate::ObjectId,
        offset: u64,
        bytes: u64,
    },
}

#[derive(Clone, Copy)]
struct Span {
    length: u64,
    bytes: BytesAt,
}

async fn span_at<S: AsyncObjectStore>(
    store: &S,
    record: FileRecord,
    offset: u64,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Span, FsError> {
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = record.payload
    else {
        let FilePayload::InlineRegular(data) = record.payload else {
            return Err(FileRangeReadError::NotRegular.into());
        };
        return Ok(Span {
            length: data.as_bytes().len() as u64 - offset,
            bytes: BytesAt::Inline,
        });
    };
    // A one-byte plan finds one extent; source_end retains its complete bound.
    let (plan, custody) = range::plan_extent_range_with_accounting(
        store,
        ExtentRangeRequest {
            root: extents,
            file_size: logical_bytes,
            range: ByteRange { offset, length: 1 },
            maximum_spans: 1,
            limits,
            budget: accounting.remaining()?,
        },
        accounting,
        cancellation,
    )
    .await?;
    let span = plan.spans.first().cloned();
    let retained = plan.retained_allocation_bytes;
    drop(plan);
    release_read_allocation(retained, custody, accounting)?;
    let span = span.ok_or(FileRangeReadError::InvalidRange)?;
    let length = span.source_end.min(logical_bytes) - offset;
    match span.kind {
        ExtentKind::Hole | ExtentKind::AllocatedZero => Ok(Span {
            length,
            bytes: BytesAt::Zero,
        }),
        ExtentKind::Content {
            object,
            object_offset,
        } => {
            let chunk = blob::chunk_at(
                store,
                object,
                object_offset,
                limits,
                accounting,
                cancellation,
            )
            .await?;
            Ok(Span {
                length: length.min(chunk.end_offset - object_offset),
                bytes: BytesAt::Chunk {
                    object: chunk.chunk,
                    offset: object_offset - chunk.first_offset,
                    bytes: chunk.end_offset - chunk.first_offset,
                },
            })
        }
    }
}

/// Only complete chunk digests may establish inequality. Index/extent identities
/// may establish equality, but their inequality never decides content equality.
fn by_digest(left: Span, right: Span, length: u64) -> Option<bool> {
    match (left.bytes, right.bytes) {
        (BytesAt::Zero, BytesAt::Zero) => Some(true),
        (
            BytesAt::Chunk {
                object: a,
                offset: x,
                bytes: n,
            },
            BytesAt::Chunk {
                object: b,
                offset: y,
                bytes: m,
            },
        ) => {
            if a == b && x == y {
                Some(true)
            } else if x == 0 && y == 0 && n == length && m == length {
                Some(a == b)
            } else {
                None
            }
        }
        _ => None,
    }
}

pub(crate) async fn records_match_content<S: AsyncObjectStore>(
    store: &S,
    left: FileRecord,
    right: FileRecord,
    config: crate::model::VolumeConfig,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<bool, FsError> {
    let before = accounting
        .standalone_parts()
        .map(|(allocations, ..)| allocations.live_bytes());
    let result = compare(store, left, right, config, accounting, cancellation).await;
    // No body or plan escapes this operation, including when the second read
    // is denied. Native byte carriers release their own original-ledger guards.
    if let Some(before) = before
        && let Some((allocations, ..)) = accounting.standalone_parts()
    {
        let retired = allocations
            .live_bytes()
            .checked_sub(before)
            .ok_or(crate::WorkError::Overflow)?;
        allocations
            .release(retired)
            .map_err(|error| error.into_work_or(FsError::HandleAllocationFailed))?;
    }
    result
}

async fn compare<S: AsyncObjectStore>(
    store: &S,
    left: FileRecord,
    right: FileRecord,
    config: crate::model::VolumeConfig,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<bool, FsError> {
    accounting.check()?;
    if left.kind != FileKind::Regular || right.kind != FileKind::Regular {
        return Ok(false);
    }
    if left.payload == right.payload {
        return Ok(true);
    }
    let size = |record: FileRecord| match record.payload {
        FilePayload::Regular { logical_bytes, .. } => Ok(logical_bytes),
        FilePayload::InlineRegular(data) => Ok(data.as_bytes().len() as u64),
        _ => Err(FsError::FileRead(FileRangeReadError::NotRegular)),
    };
    let length = size(left)?;
    if length != size(right)? {
        return Ok(false);
    }
    let limits = DecodeLimits::for_volume(config);
    let mut offset = 0;
    while offset < length {
        accounting.check()?;
        let a = span_at(store, left, offset, limits, accounting, cancellation).await?;
        let b = span_at(store, right, offset, limits, accounting, cancellation).await?;
        let length = a.length.min(b.length);
        if let Some(equal) = by_digest(a, b, length) {
            if !equal {
                return Ok(false);
            }
            offset += length;
            continue;
        }
        // Partial chunk comparisons retain at most one admitted range per side,
        // rather than allocating or hashing a whole potentially enormous file.
        let length = length.min(config.limits.maximum_read_bytes);
        if length == 0 {
            return Err(FileRangeReadError::InvalidRange.into());
        }
        accounting.charge(WorkCounters {
            // Comparing consumes the two admitted byte sources. A span is one
            // item; its bytes use the byte budget, independently of item count.
            source_bytes_read: length.checked_mul(2).ok_or(crate::WorkError::Overflow)?,
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
        let range = ByteRange { offset, length };
        let mut read = async |record| {
            read_file_range_with_accounting(
                store,
                FileRangeRequest {
                    record,
                    range,
                    maximum_spans: limits.maximum_page_items,
                    limits,
                    budget: accounting.remaining()?,
                },
                accounting,
                cancellation,
            )
            .await
        };
        let a = read(left).await?;
        let b = read(right).await?;
        let equal = a.bytes == b.bytes;
        let retained = a
            .logical_owned_bytes
            .checked_add(b.logical_owned_bytes)
            .ok_or(crate::WorkError::Overflow)?;
        drop((a, b));
        release_standalone_read_bytes(retained, accounting)?;
        if !equal {
            return Ok(false);
        }
        offset += length;
    }
    Ok(true)
}
