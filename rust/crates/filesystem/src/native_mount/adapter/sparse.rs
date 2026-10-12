use super::*;

pub(super) fn sparse_changes<F>(
    offset: u64,
    range: &MountSparseRange,
    current_length: u64,
    work: &crate::NativeMaterializationWork,
) -> Result<impl Iterator<Item = ContentChange<F>>, MountSourceError> {
    let end = offset
        .checked_add(range.logical_bytes)
        .ok_or(MountSourceError::Work(WorkError::Overflow))?;
    let mut previous = 0;
    for span in &range.spans {
        work.charge(crate::WorkCounters {
            items_examined: 1,
            ..crate::WorkCounters::UNCHARGED
        })
        .map_err(MountSourceError::NativeWork)?;
        let span_end = span
            .relative_offset
            .checked_add(span.bytes.len() as u64)
            .ok_or(MountSourceError::Work(WorkError::Overflow))?;
        if span.relative_offset < previous || span_end > range.logical_bytes {
            return Err(MountSourceError::Invalid("invalid sparse range".into()));
        }
        previous = span_end;
    }
    Ok((end > current_length)
        .then_some(ContentChange::Resize { logical_bytes: end })
        .into_iter()
        .chain(std::iter::once(range_allocation(
            ByteRange {
                offset,
                length: range.logical_bytes,
            },
            MountRangeAllocation::PunchHole,
        )))
        .chain(range.spans.iter().map(move |span| ContentChange::Write {
            offset: offset + span.relative_offset,
            bytes: span.bytes.clone(),
        })))
}

pub(super) async fn read_sparse<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    file: &DetachedFile<A, O>,
    offset: u64,
    length: u32,
    work: &crate::NativeMaterializationWork,
    lifetime: &CancellationToken,
) -> Result<(MountSparseRange, crate::NativeAllocation), MountSourceError> {
    let end = offset
        .checked_add(u64::from(length))
        .ok_or(MountSourceError::Work(WorkError::Overflow))?;
    let mut carrier = NativeDemandCarrier::Native(work);
    let mut inputs = InputBuffers::new(&mut carrier)?;
    let mut spans = inputs.vec::<super::super::MountSparseSpan>(0)?;
    let mut position = offset;
    while position < end {
        inputs.carrier.check(work.cancellation(), Some(lifetime))?;
        inputs.items(1)?;
        let Some(data) = file
            .seek(position, ExtentSeekTarget::Data, work, work.cancellation())
            .await
            .map_err(fs_error)?
        else {
            break;
        };
        if data >= end {
            break;
        }
        let hole = file
            .seek(data, ExtentSeekTarget::Hole, work, work.cancellation())
            .await
            .map_err(fs_error)?
            .unwrap_or(end)
            .min(end);
        if data < position || hole <= data {
            return Err(MountSourceError::Stale);
        }
        let read = file
            .read_range(
                ByteRange {
                    offset: data,
                    length: hole - data,
                },
                work,
                work.cancellation(),
            )
            .await
            .map_err(fs_error)?;
        if spans.len() == spans.capacity() {
            let count = spans
                .len()
                .checked_add(1)
                .and_then(usize::checked_next_power_of_two)
                .ok_or(MountSourceError::Work(WorkError::Overflow))?;
            let mut next = inputs.vec(count)?;
            let copied = spans.len() * std::mem::size_of::<super::super::MountSparseSpan>();
            let retired = spans.capacity() * std::mem::size_of::<super::super::MountSparseSpan>();
            let delta = crate::WorkCounters {
                bytes_copied: copied as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            inputs.admit(delta)?;
            next.append(&mut spans);
            drop(spans);
            inputs.release(retired as u64)?;
            inputs.record(delta)?;
            spans = next;
        }
        inputs.charge(crate::WorkCounters {
            bytes_copied: std::mem::size_of::<super::super::MountSparseSpan>() as u64,
            ..crate::WorkCounters::UNCHARGED
        })?;
        spans.push(super::super::MountSparseSpan {
            relative_offset: data - offset,
            bytes: read.bytes,
        });
        position = hole;
    }
    inputs.carrier.check(work.cancellation(), Some(lifetime))?;
    Ok((
        MountSparseRange {
            logical_bytes: u64::from(length),
            spans,
        },
        inputs.retain().expect("native sparse spans"),
    ))
}

pub(super) fn validate_sparse_allocation(
    allocation: &crate::NativeAllocation,
    work: &crate::NativeMaterializationWork,
) -> Result<(), MountSourceError> {
    if !allocation.work().shares_ledger(work) || !allocation.work().shares_resource_allowance(work)
    {
        return Err(MountSourceError::NativeWork(
            crate::NativeTreeMaterializationError::InvalidLayout,
        ));
    }
    work.admit(crate::WorkCounters::UNCHARGED)
        .map_err(MountSourceError::NativeWork)
}
