use super::*;

/// The data ranges of a file marked sparse, settled against its content.
#[cfg(windows)]
pub(super) async fn settled_sparse_ranges(
    native: &std::sync::Arc<acyclic_native_runtime::NativeFile>,
    metadata: &HostStat,
    reported: Vec<HostDataRange>,
    maximum_ranges: u32,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<Vec<HostDataRange>, CaptureError> {
    use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_SPARSE_FILE;

    const BLOCK: u64 = 4096;
    const CHUNK: u64 = 1 << 20;
    if metadata.file_attributes() & FILE_ATTRIBUTE_SPARSE_FILE.0 == 0 {
        return Ok(reported);
    }

    let mut settled = Vec::new();
    for range in &reported {
        let mut reader = acyclic_native_runtime::AsyncRangeReader::new(
            std::sync::Arc::clone(native),
            range.offset,
            range.length,
        );
        let end = range
            .offset
            .checked_add(range.length)
            .ok_or_else(|| CaptureError::from(WorkError::Overflow))?;
        let mut position = range.offset;
        let mut report_stands = false;
        while position < end {
            let wanted = (CHUNK - position % CHUNK).min(end - position);
            let read = (async {
                cancellation.check()?;
                meter
                    .native
                    .admit(WorkCounters {
                        source_bytes_read: wanted,
                        ..WorkCounters::UNCHARGED
                    })
                    .map_err(CaptureError::from)?;
                let runtime = meter.native.runtime_meter();
                let value = reader
                    .read(wanted as usize, runtime, true)
                    .await
                    .map_err(|error| {
                        let cause = if let Some(cause) = meter.native.state.take_native_io_failure()
                        {
                            capture_demand_error(cause)
                        } else if error.kind() == std::io::ErrorKind::UnexpectedEof {
                            CaptureError::Kernel(crate::FsError::SourceSnapshotStale)
                        } else {
                            CaptureError::from(error)
                        };
                        cause
                    })?;
                Ok::<_, CaptureError>(value)
            })
            .await?;

            let chunk = read;
            let read_end = position + chunk.0.len() as u64;
            report_stands = {
                let scanned: Result<bool, CaptureError> = (|| {
                    let mut start = position;
                    while start < read_end {
                        meter.native.clone().check().map_err(capture_demand_error)?;
                        let block_end = start.saturating_add(BLOCK - start % BLOCK).min(read_end);
                        let block = chunk
                            .0
                            .get((start - position) as usize..(block_end - position) as usize)
                            .ok_or(CaptureError::InvalidOptions)?;
                        let mut nonzero = false;
                        for byte in block {
                            capture_charge(
                                meter,
                                WorkCounters {
                                    items_examined: 1,
                                    ..WorkCounters::UNCHARGED
                                },
                            )?;
                            if *byte != 0 {
                                nonzero = true;
                                break;
                            }
                        }
                        if nonzero
                            && !keep_sparse_range(
                                &mut settled,
                                maximum_ranges as usize,
                                start,
                                block_end - start,
                                meter,
                            )?
                        {
                            return Ok(true);
                        }
                        start = block_end;
                    }
                    Ok(false)
                })();
                // The original native guard remains behind the bytes through every scan and retires only after their physical drop.
                drop(chunk);
                scanned
            }?;
            position = read_end;
            if report_stands {
                break;
            }
        }
        if report_stands {
            {
                let bytes = settled.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64;
                drop(settled);
                meter
                    .native
                    .clone()
                    .release(bytes)
                    .map_err(capture_demand_error)
            }?;
            return Ok(reported);
        }
    }
    {
        let bytes = reported.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64;
        drop(reported);
        meter
            .native
            .clone()
            .release(bytes)
            .map_err(capture_demand_error)
    }?;
    Ok(settled)
}

#[cfg(windows)]
pub(super) fn keep_sparse_range(
    ranges: &mut Vec<HostDataRange>,
    maximum: usize,
    offset: u64,
    length: u64,
    meter: &CaptureWorkScope,
) -> Result<bool, CaptureError> {
    meter.native.clone().check().map_err(capture_demand_error)?;
    if let Some(last) = ranges.last_mut() {
        if last.offset.checked_add(last.length) == Some(offset) {
            last.length = last
                .length
                .checked_add(length)
                .ok_or(CaptureError::from(WorkError::Overflow))?;
            return Ok(true);
        }
    }
    if ranges.len() == maximum {
        return Ok(false);
    }
    if ranges.len() == ranges.capacity() {
        let size = std::mem::size_of::<HostDataRange>();
        let preferred = ranges.capacity().saturating_mul(2).max(1).min(maximum);
        let available = meter
            .native
            .with_original_leaf(|work| work.remaining())
            .map_err(capture_demand_error)?
            .peak_allocation_bytes
            / size as u64;
        let next = preferred
            .min(usize::try_from(available).unwrap_or(usize::MAX))
            .max(ranges.len() + 1);
        let copied = ranges.len() * size;
        let delta = WorkCounters {
            bytes_copied: copied as u64,
            ..WorkCounters::UNCHARGED
        };
        crate::native_host::NativeHostWork::admit(&mut meter.native.clone(), delta)
            .map_err(capture_demand_error)?;
        let mut replacement = capture_vec(next, meter)?;
        replacement.extend_from_slice(ranges);
        meter
            .native
            .clone()
            .record(delta)
            .map_err(capture_demand_error)?;
        let old_bytes = ranges.capacity() * size;
        let old = std::mem::replace(ranges, replacement);
        drop(old);
        meter
            .native
            .clone()
            .release(old_bytes as u64)
            .map_err(capture_demand_error)?;
    }
    ranges.push(HostDataRange { offset, length });
    Ok(true)
}
