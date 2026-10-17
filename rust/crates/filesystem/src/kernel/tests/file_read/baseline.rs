use super::*;

pub(super) async fn read<S: AsyncObjectStore>(
    store: &S,
    request: FileRangeRequest,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<FileRangeRead, FsError> {
    accounting.check()?;
    if request.record.kind != FileKind::Regular {
        return Err(FileRangeReadError::NotRegular.into());
    }
    let completed = WorkCounters {
        output_bytes: request.range.length,
        items_returned: 1,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(completed)?;
    let output = match request.record.payload {
        FilePayload::InlineRegular(data) => {
            let end = request
                .range
                .offset
                .checked_add(request.range.length)
                .ok_or(FileRangeReadError::InvalidRange)?;
            let start = usize::try_from(request.range.offset)
                .map_err(|_| FileRangeReadError::InvalidRange)?;
            let end = usize::try_from(end).map_err(|_| FileRangeReadError::InvalidRange)?;
            let region = data
                .as_bytes()
                .get(start..end)
                .ok_or(FileRangeReadError::InvalidRange)?;
            let copied = WorkCounters {
                bytes_copied: request.range.length,
                ..WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            let mut output = allocate_read_buffer(region.len(), accounting)?;
            output.value.extend_from_slice(region);
            accounting.record_completed(copied)?;
            output
        }
        FilePayload::Regular {
            logical_bytes,
            extents,
        } => {
            let plan = ReadOwned::from(
                crate::kernel::range::plan_extent_range_with_accounting(
                    store,
                    ExtentRangeRequest {
                        root: extents,
                        file_size: logical_bytes,
                        range: request.range,
                        maximum_spans: request.maximum_spans,
                        limits: request.limits,
                        budget: request.budget,
                    },
                    accounting,
                    cancellation,
                )
                .await?,
            );
            let length = usize::try_from(request.range.length)
                .map_err(|_| FileRangeReadError::InvalidRange)?;
            let zeroed = WorkCounters {
                bytes_copied: request.range.length,
                ..WorkCounters::UNCHARGED
            };
            accounting.admit(zeroed)?;
            let mut output = allocate_read_buffer(length, accounting)?;
            output.value.resize(length, 0);
            accounting.record_completed(zeroed)?;
            for span in &plan.value.spans {
                let ExtentKind::Content {
                    object,
                    object_offset,
                } = span.kind
                else {
                    continue;
                };
                let nested = crate::kernel::blob::baseline_range::read_blob_range_with_accounting(
                    store,
                    object,
                    ByteRange {
                        offset: object_offset,
                        length: span.length,
                    },
                    request.limits,
                    accounting,
                    cancellation,
                    false,
                )
                .await?;
                let start = usize::try_from(span.offset - request.range.offset)
                    .map_err(|_| FileRangeReadError::InvalidRange)?;
                let end = start
                    .checked_add(nested.bytes.len())
                    .ok_or(FileRangeReadError::InvalidRange)?;
                let destination = output
                    .value
                    .get_mut(start..end)
                    .ok_or(FileRangeReadError::InvalidRange)?;
                let copied = WorkCounters {
                    bytes_copied: span.length,
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                destination.copy_from_slice(&nested.bytes);
                accounting.record_completed(copied)?;
                let retained = nested.logical_owned_bytes;
                drop(nested);
                release_standalone_read_bytes(retained, accounting)?;
            }
            let retained = plan.value.retained_allocation_bytes;
            drop(plan.value);
            release_read_allocation(retained, plan.custody, accounting)?;
            output
        }
        _ => return Err(FileRangeReadError::NotRegular.into()),
    };
    let logical_owned_bytes = usize_to_u64(output.value.capacity());
    let bytes = owned_read_bytes(output)?;
    accounting.charge(completed)?;
    Ok(FileRangeRead {
        bytes,
        logical_owned_bytes,
        work: accounting.spent(),
    })
}
