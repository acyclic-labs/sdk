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
    #[cfg(windows)]
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
        fill_unix_ranges(
            file,
            (offset, end),
            maximum_ranges,
            &mut ranges,
            &mut allocation,
            work,
        )?;
        #[cfg(windows)]
        fill_windows_ranges(
            file,
            (offset, end, length),
            maximum_ranges,
            &mut ranges,
            &mut allocation,
            work,
        )?;
        Ok(())
    })();
    if let Err(error) = result {
        drop(ranges);
        work.release(allocation)?;
        return Err(error);
    }
    Ok(ranges)
}

#[cfg(windows)]
pub(super) fn source_control<W: SourceDemandReadWork>(
    file: &File,
    code: u32,
    input: &[u8],
    output: &mut [u8],
    kind: acyclic_native_runtime::NativeOperationKind,
    work: &mut W,
) -> Result<usize, DemandError> {
    work.with_native_read_meter(|meter| {
        acyclic_native_runtime::device_control_in_place(
            file,
            code,
            input,
            output,
            Some((meter, kind)),
        )
    })
    .map_err(|error| {
        work.with_original_leaf(|original| original.take_native_io_failure())
            .unwrap_or_else(|| crate::demand::native_demand_error(error))
    })
}

#[cfg(unix)]
fn fill_unix_ranges<W: SourceDemandReadWork>(
    file: &File,
    (offset, end): (u64, u64),
    maximum_ranges: u32,
    ranges: &mut Vec<HostDataRange>,
    allocation: &mut W::Allocation,
    work: &mut W,
) -> Result<(), DemandError> {
    let seek = |offset: u64, kind: i32, work: &mut W| {
        let offset = libc::off_t::try_from(offset).map_err(|_| DemandError::InvalidRequest)?;
        loop {
            work.started_io(READ)?;
            // SAFETY: the held descriptor is live; lseek changes only its cursor.
            let position = unsafe { libc::lseek(file.as_raw_fd(), offset, kind) };
            if position >= 0 {
                return Ok(position.cast_unsigned());
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(DemandError::Io(error));
            }
        }
    };
    fill_unix_ranges_with(
        (offset, end),
        maximum_ranges,
        ranges,
        allocation,
        work,
        seek,
    )
}

#[cfg(unix)]
fn fill_unix_ranges_with<W: SourceDemandReadWork>(
    (start, end): (u64, u64),
    maximum_ranges: u32,
    ranges: &mut Vec<HostDataRange>,
    allocation: &mut W::Allocation,
    work: &mut W,
    mut seek: impl FnMut(u64, i32, &mut W) -> Result<u64, DemandError>,
) -> Result<(), DemandError> {
    let unavailable = |error: &DemandError| {
        matches!(
            error,
            DemandError::Io(error) if matches!(
                error.raw_os_error(),
                Some(libc::ENOTTY | libc::ENOTSUP | libc::EINVAL)
            )
        )
    };
    let mut offset = start;
    while offset < end {
        let queried: Result<Option<(u64, u64)>, DemandError> = (|| {
            let data = match seek(offset, libc::SEEK_DATA, work) {
                Err(DemandError::Io(error)) if error.raw_os_error() == Some(libc::ENXIO) => {
                    return Ok(None);
                }
                result => result?,
            };
            if data >= end {
                return Ok(None);
            }
            Ok(Some((data, seek(data, libc::SEEK_HOLE, work)?.min(end))))
        })();
        let (data, hole) = match queried {
            Err(error) if unavailable(&error) => {
                // Sparse allocation is an optional source capability. When
                // unavailable, one conservative range admits every byte;
                // absence of a range report can never become missing data.
                ranges.clear();
                offset = start;
                (start, end)
            }
            result => match result? {
                Some(range) => range,
                None => break,
            },
        };
        if hole <= data || data < offset || ranges.len() == maximum_ranges as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid or excessive sparse ranges",
            )
            .into());
        }
        append_unix_range(
            ranges,
            allocation,
            HostDataRange {
                offset: data,
                length: hole - data,
            },
            maximum_ranges,
            work,
        )?;
        offset = hole;
    }

    Ok(())
}

#[cfg(unix)]
fn append_unix_range<W: SourceDemandReadWork>(
    ranges: &mut Vec<HostDataRange>,
    allocation: &mut W::Allocation,
    range: HostDataRange,
    maximum_ranges: u32,
    work: &mut W,
) -> Result<(), DemandError> {
    let element = std::mem::size_of::<HostDataRange>();
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
        replacement.append(ranges);
        drop(std::mem::replace(ranges, replacement));
        work.release(std::mem::replace(allocation, next))?;
    }
    work.started_io(WorkCounters {
        items_examined: 1,
        bytes_copied: element as u64,
        ..WorkCounters::UNCHARGED
    })?;
    ranges.push(range);
    Ok(())
}

#[cfg(windows)]
fn fill_windows_ranges<W: SourceDemandReadWork>(
    file: &File,
    (offset, end, length): (u64, u64, u64),
    maximum_ranges: u32,
    ranges: &mut Vec<HostDataRange>,
    allocation: &mut W::Allocation,
    work: &mut W,
) -> Result<(), DemandError> {
    let element = std::mem::size_of::<HostDataRange>();

    let maximum = (maximum_ranges as usize)
        .saturating_add(1)
        .min(u32::MAX as usize / element);
    use windows::Win32::System::Ioctl::{
        FILE_ALLOCATED_RANGE_BUFFER, FSCTL_QUERY_ALLOCATED_RANGES,
    };
    let query = FILE_ALLOCATED_RANGE_BUFFER {
        FileOffset: i64::try_from(offset).map_err(|_| DemandError::InvalidRequest)?,
        Length: i64::try_from(length).map_err(|_| DemandError::InvalidRequest)?,
    };
    // SAFETY: the input is one initialized native range record.
    let input =
        unsafe { std::slice::from_raw_parts(std::ptr::from_ref(&query).cast::<u8>(), element) };
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
            std::slice::from_raw_parts_mut(ranges.as_mut_ptr().cast::<u8>(), count * element)
        };
        let read = source_control(
            file,
            FSCTL_QUERY_ALLOCATED_RANGES,
            input,
            output,
            acyclic_native_runtime::NativeOperationKind::Read,
            work,
        );
        match read {
            Ok(returned) => break returned,
            Err(DemandError::Io(error))
                if matches!(error.raw_os_error(), Some(122 | 234)) && count < maximum =>
            {
                let next_count = count.saturating_mul(2).min(maximum);
                let (replacement, next) = buffer(next_count, work)?;
                drop(std::mem::replace(ranges, replacement));
                work.release(std::mem::replace(allocation, next))?;
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
        let range = *ranges.get(index).ok_or(DemandError::InvalidRequest)?;
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
            *ranges.get_mut(used).ok_or(DemandError::InvalidRequest)? = HostDataRange {
                offset: start,
                length: range_end - start,
            };
            used += 1;
            prior_end = range_end;
        }
    }
    ranges.truncate(used);

    Ok(())
}

#[cfg(all(test, unix))]
mod range_capability_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn unavailable_sparse_queries_never_omit_admitted_source_bytes(
            start in 0u64..4096, length in 1u64..4096, hole_query in any::<bool>(),
            error_index in 0usize..3,
        ) {
            let codes = [libc::ENOTTY, libc::ENOTSUP, libc::EINVAL];
            let cancellation = crate::CancellationToken::new();
            let mut work = crate::demand::DemandWork::new(crate::WorkBudget {
                allocation_operations: 1, peak_allocation_bytes: 4096, backend_read_operations: 8,
                items_examined: 16, bytes_copied: 4096,
                ..crate::WorkBudget::default()
            }, &cancellation);
            let (mut ranges, mut allocation) = buffer::<HostDataRange,_>(0, &mut work).unwrap();
            let mut calls = 0;
            fill_unix_ranges_with((start,start+length), 1, &mut ranges, &mut allocation, &mut work,
                |offset, kind, work| {
                    work.started_io(READ)?;
                    calls += 1;
                    if hole_query && kind == libc::SEEK_DATA { Ok(offset) }
                    else { Err(DemandError::Io(io::Error::from_raw_os_error(codes[error_index]))) }
                }).unwrap();
            prop_assert_eq!(calls, if hole_query {2} else {1});
            prop_assert_eq!(ranges.len(),1);
            prop_assert_eq!(ranges[0].offset,start);
            prop_assert_eq!(ranges[0].length,length);
            drop(ranges);
            work.release(allocation).unwrap();
            prop_assert_eq!(work.work.allocation_operations,1);
            prop_assert_eq!(work.live_allocation_bytes(),0);
        }

        #[test]
        fn losing_the_sparse_capability_discards_an_incomplete_prefix(
            start in 0u64..4096, prior_ranges in 1usize..4,
            error_index in 0usize..3, hole_query in any::<bool>(),
        ) {
            let length = prior_ranges as u64 * 2 + 8;
            let codes = [libc::ENOTTY, libc::ENOTSUP, libc::EINVAL];
            let cancellation = crate::CancellationToken::new();
            let mut work = crate::demand::DemandWork::new(crate::WorkBudget {
                allocation_operations: 1, peak_allocation_bytes: 4096, backend_read_operations: 16,
                items_examined: 64, bytes_copied: 4096,
                ..crate::WorkBudget::default()
            }, &cancellation);
            let (mut ranges, mut allocation) = buffer::<HostDataRange,_>(0, &mut work).unwrap();
            let mut calls = 0;
            fill_unix_ranges_with((start,start+length), 4, &mut ranges, &mut allocation, &mut work,
                |offset, kind, work| {
                    work.started_io(READ)?;
                    calls += 1;
                    if calls <= prior_ranges * 2 { Ok(offset + 1) }
                    else if hole_query && kind == libc::SEEK_DATA { Ok(offset) }
                    else { Err(DemandError::Io(io::Error::from_raw_os_error(codes[error_index]))) }
                }).unwrap();
            prop_assert_eq!(calls, prior_ranges * 2 + if hole_query {2} else {1});
            prop_assert_eq!(ranges.len(),1);
            prop_assert_eq!(ranges[0].offset,start);
            prop_assert_eq!(ranges[0].length,length);
            drop(ranges);
            work.release(allocation).unwrap();
            prop_assert_eq!(work.work.allocation_operations,1);
            prop_assert_eq!(work.live_allocation_bytes(),0);
        }
    }

    #[test]
    fn a_real_source_io_failure_never_becomes_a_dense_range() {
        let mut work = ordinary_work();
        let (mut ranges, mut allocation) = buffer::<HostDataRange, _>(0, &mut work).unwrap();
        let result = fill_unix_ranges_with(
            (0, 16),
            1,
            &mut ranges,
            &mut allocation,
            &mut work,
            |_, _, _| Err(DemandError::Io(io::Error::from_raw_os_error(libc::EIO))),
        );
        assert!(
            matches!(result,Err(DemandError::Io(error)) if error.raw_os_error()==Some(libc::EIO))
        );
        assert!(ranges.is_empty());
        drop(ranges);
        work.release(allocation).unwrap();
    }
}
