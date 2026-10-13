use super::*;

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
    if maximum_ranges == 0 {
        return Err(DemandError::InvalidRequest);
    }
    let end = offset
        .checked_add(length)
        .ok_or(crate::WorkError::Overflow)?;
    let element = std::mem::size_of::<HostDataRange>();
    #[cfg(unix)]
    let (mut ranges, mut allocation) = buffer::<HostDataRange, _>(0, work)?;
    #[cfg(windows)]
    let maximum = (maximum_ranges as usize)
        .saturating_add(1)
        .min(u32::MAX as usize / element);
    #[cfg(windows)]
    let (mut ranges, mut allocation) = buffer::<HostDataRange, _>(maximum.min(8), work)?;
    let result = (|| {
        #[cfg(unix)]
        {
            let seek = |offset: u64, kind: i32, work: &mut W| {
                let offset =
                    libc::off_t::try_from(offset).map_err(|_| DemandError::InvalidRequest)?;
                loop {
                    work.started_io(READ)?;
                    // SAFETY: the held descriptor is live; lseek changes only its cursor.
                    let position = unsafe { libc::lseek(file.as_raw_fd(), offset, kind) };
                    if position >= 0 {
                        return Ok(position as u64);
                    }
                    let error = io::Error::last_os_error();
                    if error.kind() != io::ErrorKind::Interrupted {
                        return Err(DemandError::Io(error));
                    }
                }
            };
            let mut offset = offset;
            while offset < end {
                let data = match seek(offset, libc::SEEK_DATA, work) {
                    Err(DemandError::Io(error)) if error.raw_os_error() == Some(libc::ENXIO) => {
                        break;
                    }
                    result => result?,
                };
                if data >= end {
                    break;
                }
                let hole = seek(data, libc::SEEK_HOLE, work)?.min(end);
                if hole <= data || data < offset || ranges.len() == maximum_ranges as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid or excessive sparse ranges",
                    )
                    .into());
                }
                if ranges.len() == ranges.capacity() {
                    let wanted = ranges.len() + 1;
                    let grown = ranges
                        .capacity()
                        .saturating_mul(2)
                        .max(4)
                        .min(maximum_ranges as usize)
                        .max(wanted);
                    let available = work.remaining()?.peak_allocation_bytes;
                    let capacity = if grown
                        .checked_mul(element)
                        .is_some_and(|bytes| bytes as u64 <= available)
                    {
                        grown
                    } else {
                        wanted
                    };
                    let (mut replacement, next) = buffer(capacity, work)?;
                    let copied = copy_work((ranges.len() * element) as u64);
                    if let Err(error) = work.started_io(copied) {
                        drop(replacement);
                        work.release(next)?;
                        return Err(error);
                    }
                    replacement.append(&mut ranges);
                    drop(std::mem::replace(&mut ranges, replacement));
                    work.release(std::mem::replace(&mut allocation, next))?;
                }
                work.started_io(WorkCounters {
                    items_examined: 1,
                    bytes_copied: element as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                ranges.push(HostDataRange {
                    offset: data,
                    length: hole - data,
                });
                offset = hole;
            }
        }
        #[cfg(windows)]
        {
            use windows::Win32::System::Ioctl::{
                FILE_ALLOCATED_RANGE_BUFFER, FSCTL_QUERY_ALLOCATED_RANGES,
            };
            let query = FILE_ALLOCATED_RANGE_BUFFER {
                FileOffset: i64::try_from(offset).map_err(|_| DemandError::InvalidRequest)?,
                Length: i64::try_from(length).map_err(|_| DemandError::InvalidRequest)?,
            };
            // SAFETY: the input is one initialized native range record.
            let input = unsafe {
                std::slice::from_raw_parts(std::ptr::from_ref(&query).cast::<u8>(), element)
            };
            let mut count = maximum.min(8);
            let returned = loop {
                ranges.resize(
                    count,
                    HostDataRange {
                        offset: 0,
                        length: 0,
                    },
                );
                // SAFETY: repr(C) HostDataRange has the same two-word layout as FILE_ALLOCATED_RANGE_BUFFER; every integer bit pattern is valid.
                let output = unsafe {
                    std::slice::from_raw_parts_mut(
                        ranges.as_mut_ptr().cast::<u8>(),
                        count * element,
                    )
                };
                let read = work
                    .with_native_read_meter(|meter| {
                        acyclic_native_runtime::device_control_in_place(
                            file,
                            FSCTL_QUERY_ALLOCATED_RANGES,
                            input,
                            output,
                            Some((meter, acyclic_native_runtime::NativeOperationKind::Read)),
                        )
                    })
                    .map_err(|error| {
                        work.with_original_leaf(|original| original.take_native_io_failure())
                            .unwrap_or_else(|| crate::demand::native_demand_error(error))
                    });
                match read {
                    Ok(returned) => break returned,
                    Err(DemandError::Io(error))
                        if matches!(error.raw_os_error(), Some(122 | 234)) && count < maximum =>
                    {
                        let next_count = count.saturating_mul(2).min(maximum);
                        let (replacement, next) = buffer(next_count, work)?;
                        drop(std::mem::replace(&mut ranges, replacement));
                        work.release(std::mem::replace(&mut allocation, next))?;
                        count = next_count;
                    }
                    Err(error) => return Err(error),
                }
            };
            if returned > count * element
                || !returned.is_multiple_of(element)
                || returned / element > maximum_ranges as usize
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid or excessive sparse ranges",
                )
                .into());
            }
            let mut used = 0;
            let mut prior_end = offset;
            for index in 0..returned / element {
                let range = ranges[index];
                // The native ABI uses signed integers; reject their negative bit patterns.
                i64::try_from(range.offset).map_err(|_| DemandError::InvalidRequest)?;
                i64::try_from(range.length).map_err(|_| DemandError::InvalidRequest)?;
                let start = range.offset.max(offset);
                let range_end = range
                    .offset
                    .checked_add(range.length)
                    .ok_or(crate::WorkError::Overflow)?
                    .min(end);
                if start < prior_end {
                    return Err(DemandError::InvalidRequest);
                }
                work.started_io(WorkCounters {
                    items_examined: 1,
                    bytes_copied: element as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                if start < range_end {
                    ranges[used] = HostDataRange {
                        offset: start,
                        length: range_end - start,
                    };
                    used += 1;
                    prior_end = range_end;
                }
            }
            ranges.truncate(used);
        }
        Ok(())
    })();
    if let Err(error) = result {
        drop(ranges);
        work.release(allocation)?;
        return Err(error);
    }
    Ok(ranges)
}
