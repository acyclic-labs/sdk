use super::*;

#[cfg(windows)]
pub(super) fn native_device_control<W: SourceDemandReadWork>(
    file: &File,
    code: u32,
    input: &[u8],
    output: &mut [u8],
    work: &mut W,
) -> Result<usize, DemandError> {
    work.with_native_read_meter(|meter| {
        acyclic_native_runtime::device_control_in_place(
            file,
            code,
            input,
            output,
            Some((meter, acyclic_native_runtime::NativeOperationKind::Read)),
        )
    })
    .map_err(|error| {
        work.with_original_leaf(|original| original.take_native_io_failure())
            .unwrap_or_else(|| crate::demand::native_demand_error(error))
    })
}

pub(crate) fn allocated_data_ranges<W: SourceDemandReadWork>(
    file: &File,
    offset: u64,
    length: u64,
    maximum_ranges: u32,
    work: &mut W,
) -> Result<Vec<HostDataRange>, DemandError> {
    work.check()?;
    if length == 0 {
        return Ok(Vec::new());
    }
    let logical_bytes = offset
        .checked_add(length)
        .ok_or(crate::WorkError::Overflow)?;
    if maximum_ranges == 0 {
        return Err(DemandError::InvalidRequest);
    }
    #[cfg(unix)]
    {
        let mut ranges = Vec::new();
        let mut offset = offset;
        let seek = |offset: i64, kind: i32, work: &mut W| {
            loop {
                work.started_io(READ)?;
                // SAFETY: the held regular-file descriptor remains live for the call; lseek changes only its offset, never its contents.
                let result = unsafe { libc::lseek(file.as_raw_fd(), offset, kind) };
                if result >= 0 {
                    return Ok(result);
                }
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(DemandError::Io(error));
                }
            }
        };
        while offset < logical_bytes {
            let start = i64::try_from(offset).map_err(|_| DemandError::InvalidRequest)?;
            let data_offset = match seek(start, libc::SEEK_DATA, work) {
                Err(DemandError::Io(error)) if error.raw_os_error() == Some(libc::ENXIO) => break,
                result => result?,
            };
            let data = u64::try_from(data_offset).map_err(|_| DemandError::InvalidRequest)?;
            if data >= logical_bytes {
                break;
            }
            let hole = seek(data_offset, libc::SEEK_HOLE, work)?;
            let hole = u64::try_from(hole)
                .map_err(|_| DemandError::InvalidRequest)?
                .min(logical_bytes);
            if hole <= data || data < offset {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "host sparse-range query made no progress",
                )
                .into());
            }
            if ranges.len() >= maximum_ranges as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "host file exceeds admitted sparse range count",
                )
                .into());
            }
            if ranges.len() == ranges.capacity() {
                let element = std::mem::size_of::<HostDataRange>();
                let old = ranges.capacity() * element;
                let wanted = ranges
                    .len()
                    .checked_add(1)
                    .ok_or(crate::WorkError::Overflow)?;
                let grown = ranges
                    .capacity()
                    .saturating_mul(2)
                    .max(4)
                    .min(maximum_ranges as usize)
                    .max(wanted);
                let capacity = match grown.checked_mul(element) {
                    Some(bytes) if bytes as u64 <= work.remaining()?.peak_allocation_bytes => grown,
                    _ => wanted,
                };
                let bytes = capacity
                    .checked_mul(element)
                    .ok_or(crate::WorkError::Overflow)?;
                work.claim(bytes as u64)?;
                let mut replacement = Vec::new();
                replacement
                    .try_reserve_exact(capacity)
                    .map_err(|_| DemandError::InvalidRequest)?;
                let copied = WorkCounters {
                    bytes_copied: (ranges.len() * element) as u64,
                    ..WorkCounters::UNCHARGED
                };
                work.admit(copied)?;
                work.record(copied)?;
                replacement.append(&mut ranges);
                let previous = std::mem::replace(&mut ranges, replacement);
                drop(previous);
                work.release(old as u64)?;
            }
            let copied = WorkCounters {
                bytes_copied: std::mem::size_of::<HostDataRange>() as u64,
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            };
            work.admit(copied)?;
            work.record(copied)?;
            ranges.push(HostDataRange {
                offset: data,
                length: hole - data,
            });
            offset = hole;
        }
        Ok(ranges)
    }
    #[cfg(windows)]
    {
        use windows::Win32::Foundation::{ERROR_INSUFFICIENT_BUFFER, ERROR_MORE_DATA};
        use windows::Win32::System::Ioctl::{
            FILE_ALLOCATED_RANGE_BUFFER, FSCTL_QUERY_ALLOCATED_RANGES,
        };
        let element = std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>();
        let maximum = usize::try_from(u64::from(maximum_ranges) + 1)
            .map_err(|_| DemandError::InvalidRequest)?
            .min(u32::MAX as usize / element);
        let mut count = maximum.min(8);
        work.claim((count * element) as u64)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(count)
            .map_err(|_| DemandError::InvalidRequest)?;
        output.resize(count, FILE_ALLOCATED_RANGE_BUFFER::default());
        let query = FILE_ALLOCATED_RANGE_BUFFER {
            FileOffset: i64::try_from(offset).map_err(|_| DemandError::InvalidRequest)?,
            Length: i64::try_from(length).map_err(|_| DemandError::InvalidRequest)?,
        };
        // SAFETY: FILE_ALLOCATED_RANGE_BUFFER is initialized plain native data.
        let input =
            unsafe { std::slice::from_raw_parts(std::ptr::from_ref(&query).cast::<u8>(), element) };
        let returned = loop {
            work.check()?;
            // SAFETY: the vector's initialized records stay alive, exclusive, and correctly aligned through synchronous ioctl completion.
            let bytes = unsafe {
                std::slice::from_raw_parts_mut(
                    output.as_mut_ptr().cast::<u8>(),
                    output.len() * element,
                )
            };
            match native_device_control(file, FSCTL_QUERY_ALLOCATED_RANGES, input, bytes, work) {
                Ok(returned) => break returned,
                Err(DemandError::Io(error))
                    if [ERROR_MORE_DATA, ERROR_INSUFFICIENT_BUFFER]
                        .iter()
                        .any(|code| error.raw_os_error() == i32::try_from(code.0).ok()) =>
                {
                    if count == maximum {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "host file exceeds admitted sparse range count",
                        )
                        .into());
                    }
                    let next = count.saturating_mul(2).min(maximum);
                    work.claim((next * element) as u64)?;
                    let mut replacement = Vec::new();
                    replacement
                        .try_reserve_exact(next)
                        .map_err(|_| DemandError::InvalidRequest)?;
                    replacement.resize(next, FILE_ALLOCATED_RANGE_BUFFER::default());
                    drop(output);
                    work.release((count * element) as u64)?;
                    output = replacement;
                    count = next;
                }
                Err(error) => return Err(error),
            }
        };
        if returned > output.len() * element || returned % element != 0 {
            return Err(
                io::Error::new(io::ErrorKind::InvalidData, "invalid range response").into(),
            );
        }
        let returned_count = returned / element;
        if returned_count > maximum_ranges as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "host file exceeds admitted sparse range count",
            )
            .into());
        }
        let bytes = returned_count
            .checked_mul(std::mem::size_of::<HostDataRange>())
            .ok_or(crate::WorkError::Overflow)?;
        work.claim(bytes as u64)?;
        let mut ranges = Vec::new();
        ranges
            .try_reserve_exact(returned_count)
            .map_err(|_| DemandError::InvalidRequest)?;
        let mut prior_end = offset;
        for range in output.iter().take(returned_count) {
            let copied = WorkCounters {
                items_examined: 1,
                bytes_copied: std::mem::size_of::<HostDataRange>() as u64,
                ..WorkCounters::UNCHARGED
            };
            work.admit(copied)?;
            work.record(copied)?;
            let native_start =
                u64::try_from(range.FileOffset).map_err(|_| DemandError::InvalidRequest)?;
            let start = native_start.max(offset);
            let length = u64::try_from(range.Length).map_err(|_| DemandError::InvalidRequest)?;
            let end = native_start
                .checked_add(length)
                .ok_or(crate::WorkError::Overflow)?
                .min(logical_bytes);
            if start < prior_end {
                return Err(DemandError::InvalidRequest);
            }
            if start < end {
                ranges.push(HostDataRange {
                    offset: start,
                    length: end - start,
                });
                prior_end = end;
            }
        }
        drop(output);
        work.release((count * element) as u64)?;
        Ok(ranges)
    }
}
