use super::*;

/// Settles sparse allocation reports against bounded reads of their content.
#[cfg(windows)]
pub(super) async fn settled_sparse_ranges(
    native: &std::sync::Arc<acyclic_native_runtime::NativeFile>,
    metadata: &HostStat,
    reported: Vec<HostDataRange>,
    maximum_ranges: u32,
    meter: &NativeMaterializationWork,
) -> Result<Vec<HostDataRange>, CaptureError> {
    use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_SPARSE_FILE;
    const BLOCK: u64 = 4096;
    const CHUNK: u64 = 1 << 20;
    if metadata.raw.FileAttributes & FILE_ATTRIBUTE_SPARSE_FILE.0 == 0 {
        return Ok(reported);
    }
    let mut settled: Vec<HostDataRange> = Vec::new();
    for range in &reported {
        let mut reader = acyclic_native_runtime::AsyncRangeReader::new(
            std::sync::Arc::clone(native),
            range.offset,
            range.length,
        );
        let end = range
            .offset
            .checked_add(range.length)
            .ok_or(WorkError::Overflow)?;
        let mut position = range.offset;
        while position < end {
            meter.cancellation().check()?;
            let wanted = (CHUNK - position % CHUNK).min(end - position);
            meter.admit(WorkCounters {
                source_bytes_read: wanted,
                ..WorkCounters::UNCHARGED
            })?;
            let chunk = reader
                .read(wanted as usize, meter.runtime_meter(), true)
                .await
                .map_err(|error| {
                    if let Some(cause) = meter.state.take_native_io_failure() {
                        capture_demand_error(cause)
                    } else if error.kind() == std::io::ErrorKind::UnexpectedEof {
                        CaptureError::from(crate::FsError::SourceSnapshotStale)
                    } else {
                        error.into()
                    }
                })?;
            let read_end = position
                .checked_add(chunk.0.len() as u64)
                .ok_or(WorkError::Overflow)?;
            if read_end == position || read_end > end {
                return Err(CaptureError::from(crate::FsError::SourceSnapshotStale));
            }
            let mut start = position;
            while start < read_end {
                meter.clone().check().map_err(capture_demand_error)?;
                let block_end = start.saturating_add(BLOCK - start % BLOCK).min(read_end);
                let block = &chunk.0[(start - position) as usize..(block_end - position) as usize];
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
                if nonzero {
                    if let Some(last) = settled.last_mut()
                        && last.offset.checked_add(last.length) == Some(start)
                    {
                        last.length = last
                            .length
                            .checked_add(block_end - start)
                            .ok_or(WorkError::Overflow)?;
                    } else {
                        if settled.len() == maximum_ranges as usize {
                            let bytes = vector_bytes(&settled);
                            drop(settled);
                            meter.clone().release(bytes).map_err(capture_demand_error)?;
                            return Ok(reported);
                        }
                        capture_reserve(&mut settled, 1, meter)?;
                        settled.push(HostDataRange {
                            offset: start,
                            length: block_end - start,
                        });
                    }
                }
                start = block_end;
            }
            // The packet's allocation guard outlives its bytes and every scan.
            drop(chunk);
            position = read_end;
        }
    }
    let bytes = vector_bytes(&reported);
    drop(reported);
    meter.clone().release(bytes).map_err(capture_demand_error)?;
    Ok(settled)
}
