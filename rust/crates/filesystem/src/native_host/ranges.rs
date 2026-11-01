use super::*;

pub(crate) fn allocated_data_ranges<W: SourceDemandReadWork>(
    file: &File,
    offset: u64,
    length: u64,
    maximum_ranges: Option<u32>,
    work: &mut W,
) -> Result<Vec<HostDataRange>, DemandError> {
    work.check()?;
    if length == 0 {
        return Ok(Vec::new());
    }
    if maximum_ranges == Some(0) {
        return Err(DemandError::InvalidRequest);
    }
    let end = offset
        .checked_add(length)
        .ok_or(crate::WorkError::Overflow)?;
    let (mut ranges, mut allocation) = buffer::<HostDataRange, _>(0, work)?;
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
            (offset, end),
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
) -> Result<acyclic_native_runtime::NativeControlCompletion, DemandError> {
    work.with_native_read_meter(|meter| {
        acyclic_native_runtime::device_control_in_place(
            file,
            code,
            input,
            output,
            kind,
            Some(meter),
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
    maximum_ranges: Option<u32>,
    ranges: &mut Vec<HostDataRange>,
    allocation: &mut W::Allocation,
    work: &mut W,
) -> Result<(), DemandError> {
    let seek = |offset: u64, kind: i32, work: &mut W| {
        let offset = libc::off_t::try_from(offset).map_err(|_| DemandError::InvalidRequest)?;
        work.started_io(READ)?;
        // SAFETY: the held descriptor is live; lseek changes only its cursor.
        let position = unsafe { libc::lseek(file.as_raw_fd(), offset, kind) };
        if position >= 0 {
            Ok(position.cast_unsigned())
        } else {
            Err(DemandError::Io(io::Error::last_os_error()))
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
    maximum_ranges: Option<u32>,
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
        if hole <= data || data < offset {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid sparse ranges").into());
        }
        append_host_range(
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

fn append_host_range<W: SourceDemandReadWork>(
    ranges: &mut Vec<HostDataRange>,
    allocation: &mut W::Allocation,
    range: HostDataRange,
    maximum_ranges: Option<u32>,
    work: &mut W,
) -> Result<(), DemandError> {
    if maximum_ranges.is_some_and(|maximum| ranges.len() >= maximum as usize) {
        return Err(DemandError::InvalidRequest);
    }
    let element = std::mem::size_of::<HostDataRange>();
    if ranges.len() == ranges.capacity() {
        let wanted = ranges.len() + 1;
        let grown = ranges.capacity().saturating_mul(2).max(4);
        let grown = maximum_ranges
            .map_or(grown, |maximum| grown.min(maximum as usize))
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
    (offset, end): (u64, u64),
    maximum_ranges: Option<u32>,
    ranges: &mut Vec<HostDataRange>,
    allocation: &mut W::Allocation,
    work: &mut W,
) -> Result<(), DemandError> {
    use acyclic_native_runtime::{NativeControlCompletion, NativeOperationKind};
    use windows::Win32::System::Ioctl::{
        FILE_ALLOCATED_RANGE_BUFFER, FSCTL_QUERY_ALLOCATED_RANGES,
    };
    // Physical query pages never impose a limit on the file's extent count.
    let mut page = [HostDataRange {
        offset: 0,
        length: 0,
    }; 1024];
    let element = std::mem::size_of::<HostDataRange>();
    i64::try_from(end).map_err(|_| DemandError::InvalidRequest)?;
    let mut next_offset = offset;
    while next_offset < end {
        work.check()?;
        let query = FILE_ALLOCATED_RANGE_BUFFER {
            FileOffset: i64::try_from(next_offset).map_err(|_| DemandError::InvalidRequest)?,
            Length: i64::try_from(end - next_offset).map_err(|_| DemandError::InvalidRequest)?,
        };
        // SAFETY: this input is one initialized native range record.
        let input =
            unsafe { std::slice::from_raw_parts(std::ptr::from_ref(&query).cast::<u8>(), element) };
        // SAFETY: repr(C) HostDataRange matches the two native integer words;
        // every bit pattern is initialized and remains live through completion.
        let output = unsafe {
            std::slice::from_raw_parts_mut(
                page.as_mut_ptr().cast::<u8>(),
                std::mem::size_of_val(&page),
            )
        };
        let (returned, more) = match source_control(
            file,
            FSCTL_QUERY_ALLOCATED_RANGES,
            input,
            output,
            NativeOperationKind::Read,
            work,
        )? {
            NativeControlCompletion::Complete(bytes) => (bytes, false),
            NativeControlCompletion::MoreData(bytes) => (bytes, true),
        };
        if returned > std::mem::size_of_val(&page) || !returned.is_multiple_of(element) {
            return Err(DemandError::InvalidRequest);
        }
        let mut prior_end = next_offset;
        for range in &page[..returned / element] {
            i64::try_from(range.offset).map_err(|_| DemandError::InvalidRequest)?;
            i64::try_from(range.length).map_err(|_| DemandError::InvalidRequest)?;
            let range_end = range
                .offset
                .checked_add(range.length)
                .ok_or(crate::WorkError::Overflow)?
                .min(end);
            let start = range.offset.max(next_offset);
            if start < prior_end || start >= range_end {
                return Err(DemandError::InvalidRequest);
            }
            append_host_range(
                ranges,
                allocation,
                HostDataRange {
                    offset: start,
                    length: range_end - start,
                },
                maximum_ranges,
                work,
            )?;
            prior_end = range_end;
        }
        if !more {
            break;
        }
        if prior_end <= next_offset {
            // A malformed partial page must never turn into an infinite loop.
            return Err(DemandError::InvalidRequest);
        }
        next_offset = prior_end;
    }
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
            fill_unix_ranges_with((start,start+length), Some(1), &mut ranges, &mut allocation, &mut work,
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
            fill_unix_ranges_with((start,start+length), Some(4), &mut ranges, &mut allocation, &mut work,
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
    fn unlimited_sparse_plan_preserves_every_extent_and_original_funding() {
        let count = 65_537_u64;
        let budget = WorkBudget {
            allocation_operations: 64,
            peak_allocation_bytes: 4 * 1024 * 1024,
            backend_read_operations: 2 * count,
            items_examined: 4 * count,
            bytes_copied: 4 * 1024 * 1024,
            ..WorkBudget::UNBOUNDED
        };
        for maximum in [None, Some(65_536)] {
            let stop = crate::CancellationToken::new();
            let mut work = NativeMaterializationWork::new(budget, &stop)
                .map_err(|(error, _)| error)
                .unwrap();
            let baseline = work.live_allocation_bytes();
            let (mut ranges, mut allocation) = buffer::<HostDataRange, _>(0, &mut work).unwrap();
            let result = fill_unix_ranges_with(
                (0, 2 * count),
                maximum,
                &mut ranges,
                &mut allocation,
                &mut work,
                |offset, _, work| {
                    work.started_io(READ)?;
                    Ok(offset + 1)
                },
            );
            if maximum.is_some() {
                assert!(matches!(result, Err(DemandError::InvalidRequest)));
                assert_eq!(ranges.len(), 65_536);
            } else {
                result.unwrap();
                assert_eq!(ranges.len() as u64, count);
            }
            for (index, range) in ranges.iter().enumerate() {
                assert_eq!(range.offset, 2 * index as u64 + 1);
                assert_eq!(range.length, 1);
            }
            assert_eq!(work.receipt().backend_read_operations, 2 * count);
            drop(ranges);
            work.release(allocation).unwrap();
            assert_eq!(work.live_allocation_bytes(), baseline);
            work.receipt().verify(budget).unwrap();
        }
    }

    #[test]
    fn a_real_source_io_failure_never_becomes_a_dense_range() {
        let mut work = ordinary_work();
        let (mut ranges, mut allocation) = buffer::<HostDataRange, _>(0, &mut work).unwrap();
        let result = fill_unix_ranges_with(
            (0, 16),
            Some(1),
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
