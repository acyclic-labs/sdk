use super::*;

pub(super) fn dense_attributes(attributes: u32) -> bool {
    use windows::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_ARCHIVE, FILE_ATTRIBUTE_HIDDEN, FILE_ATTRIBUTE_NORMAL,
        FILE_ATTRIBUTE_NOT_CONTENT_INDEXED, FILE_ATTRIBUTE_READONLY, FILE_ATTRIBUTE_SYSTEM,
        FILE_ATTRIBUTE_TEMPORARY,
    };
    // Unknown, sparse, compressed, reparse and recall attributes retain the native query.
    let ordinary = FILE_ATTRIBUTE_ARCHIVE.0
        | FILE_ATTRIBUTE_HIDDEN.0
        | FILE_ATTRIBUTE_NORMAL.0
        | FILE_ATTRIBUTE_NOT_CONTENT_INDEXED.0
        | FILE_ATTRIBUTE_READONLY.0
        | FILE_ATTRIBUTE_SYSTEM.0
        | FILE_ATTRIBUTE_TEMPORARY.0;
    attributes & !ordinary == 0
}

pub(super) fn regular_data_ranges(
    file: &std::fs::File,
    metadata: &HostStat,
    maximum_ranges: u32,
    meter: &NativeMaterializationWork,
) -> Result<Option<Vec<HostDataRange>>, CaptureError> {
    let dense = dense_attributes(metadata.raw.FileAttributes);
    #[cfg(all(test, feature = "memory"))]
    let dense = dense && !super::windows_dense_tests::force_query();
    if !dense || metadata.len() == 0 {
        return allocated_data_ranges(file, 0, metadata.len(), maximum_ranges, &mut meter.clone())
            .map(Some)
            .map_err(capture_demand_error);
    }
    meter.clone().check().map_err(capture_demand_error)?;
    if maximum_ranges == 0 || i64::try_from(metadata.len()).is_err() {
        return Err(capture_demand_error(DemandError::InvalidRequest));
    }
    // FSCTL_QUERY_ALLOCATED_RANGES returns precisely this range for ordinary files.
    // The caller still proves the held file and final path unchanged after reading.
    capture_charge(
        meter,
        WorkCounters {
            items_examined: 1,
            bytes_copied: std::mem::size_of::<HostDataRange>() as u64,
            ..WorkCounters::UNCHARGED
        },
    )?;
    // None lets staging keep the single full-file range on its stack.
    Ok(None)
}

/// Settles sparse allocation reports against bounded reads of their content.
pub(super) async fn settled_sparse_ranges(
    native: &acyclic_native_runtime::NativeFile,
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
        let mut reader =
            acyclic_native_runtime::AsyncRangeReader::new(native, range.offset, range.length);
        let end = range
            .offset
            .checked_add(range.length)
            .ok_or(WorkError::Overflow)?;
        let mut position = range.offset;
        while position < end {
            meter.cancellation().check()?;
            let wanted = (CHUNK - position % CHUNK).min(end - position);
            meter.admit(crate::materializer::NativeResourceCredit::ReadBytes.delta(wanted))?;
            let chunk = reader
                .read(
                    usize::try_from(wanted).map_err(|_| WorkError::Overflow)?,
                    meter.runtime_meter(),
                    true,
                )
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
                let first = usize::try_from(start - position).map_err(|_| WorkError::Overflow)?;
                let last =
                    usize::try_from(block_end - position).map_err(|_| WorkError::Overflow)?;
                let block = chunk.0.get(first..last).ok_or(WorkError::Overflow)?;
                let nonzero = sparse_scan::block_has_data(block, meter)?;
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
