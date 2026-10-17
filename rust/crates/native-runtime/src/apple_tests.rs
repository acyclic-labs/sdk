use super::faults::{Fault, Mutation, arm, disarm};
use super::*;
use acyclic_test_support::{differential, measure};
use proptest::prelude::*;
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt as _;

type Event = (&'static str, u64, u64);
type Outcome<T> = Result<T, (io::ErrorKind, Option<i32>)>;
type ReadCase<'a> = (&'a [(u64, usize)], Vec<Event>);
type FaultCase = (Option<Vectored>, Vec<Fault>, Vec<Event>, bool);
type WriteObservation = ((Outcome<()>, Vec<u8>), Vec<Event>);

const FILE: usize = 10_000;

fn content(offset: usize) -> u8 {
    offset.wrapping_mul(31).to_le_bytes()[0] ^ (offset >> 7).to_le_bytes()[0]
}

fn fixture() -> io::Result<(tempfile::TempDir, File)> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("positional");
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    file.write_all_at(&(0..FILE).map(content).collect::<Vec<_>>(), 0)?;
    Ok((directory, file))
}

fn outcome<T>(result: io::Result<T>) -> Outcome<T> {
    result.map_err(|error| (error.kind(), error.raw_os_error()))
}

fn reads(ranges: &[(u64, usize)]) -> Vec<OwnedRead> {
    ranges
        .iter()
        .map(|&(offset, length)| OwnedRead { offset, length })
        .collect()
}

/// The pre-change implementation: a zero-filled buffer and one `pread` per request.
fn baseline_read(file: &File, reads: &[OwnedRead]) -> io::Result<Vec<Bytes>> {
    for read in reads {
        validate_range(read.offset, read.length)?;
    }
    let mut results = Vec::new();
    results.try_reserve_exact(reads.len())?;
    for read in reads {
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(read.length)?;
        bytes.resize(read.length, 0);
        differential::record(("zeroed", read.offset, read.length as u64));
        differential::record(("pread", read.offset, read.length as u64));
        let count = file.read_at(&mut bytes, read.offset)?;
        bytes.truncate(count);
        results.push(Bytes::from(bytes));
    }
    Ok(results)
}

/// The pre-change implementation: one complete positional write per request.
fn baseline_write(file: &File, writes: &[OwnedWrite]) -> io::Result<()> {
    for write in writes {
        validate_range(write.offset, write.bytes.len())?;
    }
    for write in writes {
        differential::record(("pwrite", write.offset, write.bytes.len() as u64));
        file.write_all_at(&write.bytes, write.offset)?;
    }
    Ok(())
}

fn read_pair(
    ranges: &[(u64, usize)],
    vectored: Option<Vectored>,
) -> [(Outcome<Vec<Bytes>>, Vec<Event>); 2] {
    let requests = reads(ranges);
    differential::pair(
        || fixture().unwrap(),
        |(_directory, file), old| {
            outcome(if old {
                baseline_read(&file, &requests)
            } else {
                read_batch_with(&file, &requests, vectored)
            })
        },
    )
}

fn per_request(ranges: &[(u64, usize)], name: &'static str) -> Vec<Event> {
    ranges
        .iter()
        .map(|&(offset, length)| (name, offset, length as u64))
        .collect()
}

#[test]
fn reads_match_the_baseline_with_one_vectored_call_per_contiguous_run() {
    let preadv = vectored(Direction::Read);
    assert!(preadv.is_some(), "macOS 11+ resolves preadv");
    let limit = segment_limit();
    assert_eq!(limit, 1024, "macOS reports IOV_MAX through sysconf");
    let run: Vec<_> = (0..limit as u64 + 6).map(|index| (index * 9, 9)).collect();
    let cases: [ReadCase<'_>; 5] = [
        (
            &[(0, 4096), (4096, 4096), (8192, 4096)],
            vec![("preadv", 0, 12_288)],
        ),
        (
            &run,
            vec![
                ("preadv", 0, 9 * limit as u64),
                ("preadv", 9 * limit as u64, 54),
            ],
        ),
        (
            &[(5, 8), (1, 3), (13, 0), (13, 7), (9_990, 64), (20_000, 4)],
            vec![
                ("pread", 5, 8),
                ("pread", 1, 3),
                ("pread", 13, 7),
                ("pread", 9_990, 64),
                ("pread", 20_000, 4),
            ],
        ),
        (&[(0, 0), (40, 0)], vec![]),
        (&[(0, 1), (u64::MAX, 1)], vec![]),
    ];
    for (ranges, expected) in cases {
        let [(old, old_trace), (new, new_trace)] = read_pair(ranges, preadv);
        assert_eq!(new, old, "{ranges:?}");
        assert_eq!(new_trace, expected, "{ranges:?}");
        // Negative control: the baseline's per-request calls and zero-fill
        // fail the same exact trace whenever the batch reaches the kernel.
        if !expected.is_empty() {
            assert_ne!(old_trace, expected, "{ranges:?}");
            assert!(old_trace.iter().any(|event| event.0 == "zeroed"));
        }
        assert!(new_trace.iter().all(|event| event.0 != "zeroed"));
    }
}

#[test]
fn reads_without_preadv_issue_one_call_per_segment_and_stop_at_end_of_file() {
    let ranges = [(0, 4096), (4096, 4096), (8192, 4096), (12_288, 4096)];
    let [(old, old_trace), (new, new_trace)] = read_pair(&ranges, None);
    assert_eq!(new, old);
    assert_eq!(new.unwrap().get(2).map(Bytes::len), Some(FILE - 8192));
    // The short third transfer ends the run, so the fourth range costs no call.
    assert_eq!(new_trace, per_request(&ranges[..3], "pread"));
    assert_eq!(old_trace.len(), 2 * ranges.len());
}

#[test]
fn read_allocations_are_exactly_one_per_request_and_the_result_list() {
    let (_directory, file) = fixture().unwrap();
    let ranges: Vec<_> = (0..90).map(|index| (index * 100, 100)).collect();
    let requests = reads(&ranges);
    // A worker's first batch reserves its segment and buffer lists once.
    let limit = segment_limit();
    let first = std::thread::scope(|scope| {
        scope
            .spawn(|| measure(|| drop(read_batch_with(&file, &requests, None))))
            .join()
            .unwrap()
    });
    assert_eq!(first.count_total, requests.len() as u64 + 3);
    assert_eq!(
        first.bytes_total,
        (100 * requests.len()
            + size_of::<Bytes>() * requests.len()
            + (size_of::<libc::iovec>() + size_of::<Vec<u8>>()) * limit) as u64
    );
    // Every later batch on that worker allocates only its results.
    read_batch_with(&file, &requests, None).unwrap();
    for vectored in [vectored(Direction::Read), None] {
        let mut result = None;
        let allocation = measure(|| result = Some(read_batch_with(&file, &requests, vectored)));
        let result = result.unwrap().unwrap();
        assert_eq!(result.len(), requests.len());
        assert_eq!(allocation.count_total, requests.len() as u64 + 1);
        assert_eq!(
            allocation.bytes_total,
            (100 * requests.len() + size_of::<Bytes>() * requests.len()) as u64
        );
        drop(result);
    }
    // Negative control: a per-batch heap segment list fails the exact count.
    let mut extra = None;
    let allocation = measure(|| {
        let segments: Vec<libc::iovec> = Vec::with_capacity(limit);
        extra = Some((read_batch_with(&file, &requests, None), segments));
    });
    assert_ne!(allocation.count_total, requests.len() as u64 + 1);
}

fn write_pair(writes: &[OwnedWrite], vectored: Option<Vectored>) -> [WriteObservation; 2] {
    differential::pair(
        || fixture().unwrap(),
        |(_directory, file), old| {
            let result = outcome(if old {
                baseline_write(&file, writes)
            } else {
                write_batch_with(&file, writes, vectored)
            });
            (result, file_bytes(&file))
        },
    )
}

fn owned_writes(ranges: &[(u64, &'static [u8])]) -> Vec<OwnedWrite> {
    ranges
        .iter()
        .map(|&(offset, bytes)| OwnedWrite {
            offset,
            bytes: Bytes::from_static(bytes),
        })
        .collect()
}

#[test]
fn writes_match_the_baseline_with_one_vectored_call_per_contiguous_run() {
    let pwritev = vectored(Direction::Write);
    assert!(pwritev.is_some(), "macOS 11+ resolves pwritev");
    let cases: [(Vec<OwnedWrite>, Vec<Event>); 4] = [
        (
            owned_writes(&[
                (9_998, b"ab"),
                (10_000, b""),
                (10_000, b"cdef"),
                (10_004, b"g"),
            ]),
            vec![("pwritev", 9_998, 7)],
        ),
        (
            owned_writes(&[(0, b"one"), (100, b"two"), (103, b"three")]),
            vec![("pwrite", 0, 3), ("pwritev", 100, 8)],
        ),
        (owned_writes(&[(7, b""), (7, b"")]), vec![]),
        (owned_writes(&[(0, b"changed"), (u64::MAX, b"x")]), vec![]),
    ];
    for (writes, expected) in cases {
        let [(old, old_trace), (new, new_trace)] = write_pair(&writes, pwritev);
        assert_eq!(new, old, "{writes:?}");
        assert_eq!(new_trace, expected, "{writes:?}");
        if !old_trace.is_empty() {
            // Negative control: per-request writes fail the same exact trace.
            assert_ne!(old_trace, expected, "{writes:?}");
        }
    }
    let limit = segment_limit() as u64;
    let run: Vec<_> = (0..limit + 6)
        .map(|index| OwnedWrite {
            offset: index * 2,
            bytes: Bytes::from_static(b"xy"),
        })
        .collect();
    let [(old, _), (new, new_trace)] = write_pair(&run, pwritev);
    assert_eq!(new, old);
    assert_eq!(
        new_trace,
        [("pwritev", 0, 2 * limit), ("pwritev", 2 * limit, 12)]
    );
    let [(old, _), (new, new_trace)] = write_pair(run.get(..3).unwrap(), None);
    assert_eq!(new, old);
    assert_eq!(
        new_trace,
        [("pwrite", 0, 2), ("pwrite", 2, 2), ("pwrite", 4, 2)]
    );
}

#[test]
fn write_allocations_are_zero() {
    let (_directory, file) = fixture().unwrap();
    let writes = owned_writes(&[(0, b"abc"), (3, b"def"), (40, b"g")]);
    let first = std::thread::scope(|scope| {
        scope
            .spawn(|| measure(|| write_batch_with(&file, &writes, None).unwrap()))
            .join()
            .unwrap()
    });
    assert_eq!(
        first.count_total, 2,
        "the worker's segment and buffer lists"
    );
    write_batch_with(&file, &writes, None).unwrap();
    for vectored in [vectored(Direction::Write), None] {
        let mut result = None;
        let allocation = measure(|| result = Some(write_batch_with(&file, &writes, vectored)));
        result.unwrap().unwrap();
        assert_eq!(allocation.count_total, 0);
    }
}

#[test]
fn append_handle_preserves_positional_write_semantics() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("append-positional");
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .append(true)
        .open(&path)?;
    write_all_batch_owned(&file, owned_writes(&[(0, b"AB"), (2, b"CD")]))?;
    write_all_batch_owned(&file, owned_writes(&[(0, b"Z"), (1, b"Y")]))?;
    write_all_batch_owned(&file, owned_writes(&[(3, b"W")]))?;
    assert_eq!(std::fs::read(path)?, b"ZYCW");
    Ok(())
}

/// Runs the new path with `faults` injected on its raw syscalls under
/// `mutation`, beside the fault-free baseline on an identical fixture.
fn faulted<T>(
    faults: &[Fault],
    mutation: Mutation,
    old: impl FnOnce(&File) -> T,
    new: impl FnOnce(&File) -> T,
) -> ([(T, Vec<Event>); 2], usize) {
    let mut consumed = 0;
    let mut old = Some(old);
    let mut new = Some(new);
    let pair = differential::pair(
        || fixture().unwrap(),
        |(_directory, file), baseline| {
            if baseline {
                (old.take().unwrap())(&file)
            } else {
                arm(faults, mutation);
                let result = (new.take().unwrap())(&file);
                consumed = disarm(faults.len());
                result
            }
        },
    );
    (pair, consumed)
}

/// The complete file, at exactly its reported length.
fn file_bytes(file: &File) -> Vec<u8> {
    let length = usize::try_from(file.metadata().unwrap().len()).unwrap();
    let mut bytes = vec![0; length];
    file.read_exact_at(&mut bytes, 0).unwrap();
    bytes
}

fn first_difference(left: &[u8], right: &[u8]) -> Option<usize> {
    left.iter()
        .zip(right)
        .position(|(left, right)| left != right)
        .or_else(|| (left.len() != right.len()).then(|| left.len().min(right.len())))
}

/// The write oracle: a faulted batch must leave exactly the baseline bytes,
/// unless an injected zero-byte transfer ended it with `WriteZero`, in which
/// case every byte is untouched, a hole past the old end, or written there
/// by some request.
fn write_oracle(
    writes: &[OwnedWrite],
    vectored: Option<Vectored>,
    faults: &[Fault],
    mutation: Mutation,
) -> Result<(), String> {
    let ([((old, old_bytes), _), ((new, new_bytes), trace)], consumed) = faulted(
        faults,
        mutation,
        |file| (outcome(baseline_write(file, writes)), file_bytes(file)),
        |file| {
            (
                outcome(write_batch_with(file, writes, vectored)),
                file_bytes(file),
            )
        },
    );
    let zeroed = faults
        .get(..consumed)
        .unwrap_or_default()
        .iter()
        .any(|fault| matches!(fault, Fault::Zero | Fault::Short(0)));
    if consumed > trace.len() {
        return Err(format!("{consumed} faults but {} attempts", trace.len()));
    }
    match &new {
        Err((io::ErrorKind::WriteZero, None)) if zeroed && old.is_ok() => {
            // A partial batch can only end between the old end and the baseline end.
            if !(FILE..=old_bytes.len()).contains(&new_bytes.len()) {
                return Err(format!(
                    "WriteZero left {} bytes, outside {FILE}..={}",
                    new_bytes.len(),
                    old_bytes.len()
                ));
            }
            let original: Vec<u8> = (0..FILE).map(content).collect();
            // Overlapping writes may leave an earlier request's byte in place.
            let written = |index: usize, byte: u8| {
                writes.iter().any(|write| {
                    usize::try_from(write.offset)
                        .ok()
                        .and_then(|offset| index.checked_sub(offset))
                        .and_then(|at| write.bytes.get(at))
                        == Some(&byte)
                })
            };
            let partial = new_bytes.iter().enumerate().all(|(index, &byte)| {
                let hole = index >= FILE && byte == 0;
                original.get(index) == Some(&byte) || hole || written(index, byte)
            });
            partial
                .then_some(())
                .ok_or_else(|| "WriteZero left a byte no request wrote".to_owned())
        }
        _ if new == old && new_bytes == old_bytes => Ok(()),
        _ => Err(format!(
            "{new:?} vs baseline {old:?}; first byte difference: {:?}",
            first_difference(&new_bytes, &old_bytes)
        )),
    }
}

/// An independent model of a faulted batch read. Requests form runs of
/// contiguous ranges; each run issues attempts until one moves fewer bytes
/// than it asked for or the run is complete. An attempt asks for the rest of
/// the run when vectored, else the rest of the current request. The next
/// queued fault decides how many of the available bytes it moves: `Pass` all,
/// `Short(n)` at most n, `Zero` none, and `Interrupt` repeats the attempt.
/// Returns the exact length of every request and how many faults were consumed.
fn modelled_lengths(
    ranges: &[(u64, usize)],
    vectored: bool,
    faults: &[Fault],
    file_length: u64,
) -> (Vec<usize>, usize) {
    let mut queue = faults.iter().copied();
    let mut consumed = 0;
    let mut lengths = Vec::new();
    let mut start = 0;
    while start < ranges.len() {
        let mut end = start + 1;
        while end < ranges.len() && ranges[end].0 == ranges[end - 1].0 + ranges[end - 1].1 as u64 {
            end += 1;
        }
        let run = &ranges[start..end];
        let total: usize = run.iter().map(|range| range.1).sum();
        let mut moved = 0;
        while moved < total {
            let asked = if vectored {
                total - moved
            } else {
                let mut before = 0;
                run.iter()
                    .find_map(|range| {
                        before += range.1;
                        (before > moved).then(|| before - moved)
                    })
                    .unwrap_or(0)
            };
            let at = run[0].0 + moved as u64;
            let available = usize::try_from(file_length.saturating_sub(at)).unwrap_or(usize::MAX);
            let count = loop {
                let fault = queue.next();
                consumed += usize::from(fault.is_some());
                match fault {
                    Some(Fault::Interrupt) => {}
                    None | Some(Fault::Pass) => break asked.min(available),
                    Some(Fault::Short(limit)) => break asked.min(limit).min(available),
                    Some(Fault::Zero) => break 0,
                }
            };
            moved += count;
            if count < asked {
                break;
            }
        }
        let mut left = moved;
        for range in run {
            let take = left.min(range.1);
            lengths.push(take);
            left -= take;
        }
        start = end;
    }
    (lengths, consumed)
}

/// The read oracle: each buffer has exactly the modelled length for the
/// consumed fault schedule and holds the baseline bytes for that prefix.
fn read_oracle(
    ranges: &[(u64, usize)],
    vectored: Option<Vectored>,
    faults: &[Fault],
    mutation: Mutation,
) -> Result<(), String> {
    let requests = reads(ranges);
    let ([(old, _), (new, _)], consumed) = faulted(
        faults,
        mutation,
        |file| outcome(baseline_read(file, &requests)),
        |file| outcome(read_batch_with(file, &requests, vectored)),
    );
    let (old, new) = match (old, new) {
        (Ok(old), Ok(new)) => (old, new),
        (old, new) if old == new => return Ok(()),
        (old, new) => return Err(format!("{new:?} vs baseline {old:?}")),
    };
    let (lengths, modelled) = modelled_lengths(ranges, vectored.is_some(), faults, FILE as u64);
    if consumed != modelled {
        return Err(format!("consumed {consumed} faults, model {modelled}"));
    }
    let actual: Vec<usize> = new.iter().map(Bytes::len).collect();
    if actual != lengths {
        return Err(format!("lengths {actual:?}, model {lengths:?}"));
    }
    old.iter()
        .zip(&new)
        .zip(&lengths)
        .all(|((old, new), &length)| old.get(..length) == Some(&new[..]))
        .then_some(())
        .ok_or_else(|| "a buffer differs from the baseline bytes".to_owned())
}

fn arbitrary_fault() -> impl Strategy<Value = Fault> {
    prop_oneof![
        Just(Fault::Pass),
        Just(Fault::Interrupt),
        (0..700_usize).prop_map(Fault::Short),
        Just(Fault::Zero),
    ]
}

/// Requests that mix contiguous runs, gaps, empty ranges and end of file.
fn arbitrary_layout() -> impl Strategy<Value = Vec<(u64, usize)>> {
    (
        0..(FILE as u64 + 64),
        prop::collection::vec(
            (
                any::<bool>(),
                0..(FILE as u64),
                // Small exact buffers, and pooled buffers of one or two pages.
                prop_oneof![4 => 0..300_usize, 1 => POOL_PAGE..POOL_PAGE + 4000],
            ),
            1..12,
        ),
    )
        .prop_map(|(start, steps)| {
            let mut end = start;
            steps
                .into_iter()
                .map(|(contiguous, jump, length)| {
                    let offset = if contiguous { end } else { jump };
                    end = offset + length as u64;
                    (offset, length)
                })
                .collect()
        })
}

fn layout_writes(layout: &[(u64, usize)]) -> Vec<OwnedWrite> {
    layout
        .iter()
        .enumerate()
        .map(|(index, &(offset, length))| OwnedWrite {
            offset,
            bytes: Bytes::from(
                (0..length)
                    .map(|byte| (byte * 7 + index * 13).to_le_bytes()[0] | 1)
                    .collect::<Vec<u8>>(),
            ),
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn faulted_writes_land_exactly_where_the_baseline_puts_them(
        layout in arbitrary_layout(),
        faults in prop::collection::vec(arbitrary_fault(), 0..8),
        vectorize in any::<bool>(),
    ) {
        let pwritev = vectorize.then(|| vectored(Direction::Write)).flatten();
        let writes = layout_writes(&layout);
        prop_assert_eq!(write_oracle(&writes, pwritev, &faults, Mutation::None), Ok(()));
    }

    #[test]
    fn faulted_reads_return_initialized_prefixes_of_the_baseline(
        layout in arbitrary_layout(),
        faults in prop::collection::vec(arbitrary_fault(), 0..8),
        vectorize in any::<bool>(),
        uncached in any::<bool>(),
    ) {
        let preadv = vectorize.then(|| vectored(Direction::Read)).flatten();
        // Advice is cleared with the faults when the oracle disarms.
        super::faults::force_uncached(uncached);
        prop_assert_eq!(read_oracle(&layout, preadv, &faults, Mutation::None), Ok(()));
    }
}

#[test]
fn fault_oracles_reject_a_wrong_segment_resume_and_a_dropped_retry() {
    let writes = owned_writes(&[(0, b"abc"), (3, b"defg")]);
    for vectored in [vectored(Direction::Write), None] {
        let short = [Fault::Short(2)];
        assert_eq!(
            write_oracle(&writes, vectored, &short, Mutation::None),
            Ok(())
        );
        // The wrong resume skips the unwritten "c" and writes "defg" from
        // offset 2, so byte 2 is the first to differ.
        let wrong = write_oracle(&writes, vectored, &short, Mutation::WrongResume).unwrap_err();
        assert!(wrong.ends_with("first byte difference: Some(2)"), "{wrong}");
        // The first attempt is never mutated.
        assert_eq!(
            write_oracle(&writes, vectored, &[], Mutation::WrongResume),
            Ok(())
        );
        let interrupt = [Fault::Interrupt];
        assert_eq!(
            write_oracle(&writes, vectored, &interrupt, Mutation::None),
            Ok(())
        );
        assert!(write_oracle(&writes, vectored, &interrupt, Mutation::DropRetry).is_err());
    }
    let ranges = [(0, 4096), (4096, 4096), (9000, 100)];
    for vectored in [vectored(Direction::Read), None] {
        let interrupt = [Fault::Interrupt];
        assert_eq!(
            read_oracle(&ranges, vectored, &interrupt, Mutation::None),
            Ok(())
        );
        assert!(read_oracle(&ranges, vectored, &interrupt, Mutation::DropRetry).is_err());
        // A short read that forgets a transferred byte fails the exact model.
        let short = [Fault::Short(100)];
        assert_eq!(
            read_oracle(&ranges, vectored, &short, Mutation::None),
            Ok(())
        );
        let lost = read_oracle(&ranges, vectored, &short, Mutation::LoseByte).unwrap_err();
        assert!(lost.starts_with("lengths"), "{lost}");
    }
}

#[test]
fn the_read_model_matches_hand_computed_schedules() {
    let ranges = [(0, 4096), (4096, 4096), (9000, 100), (9950, 100)];
    // Vectored: one call for the run of two moves 5000, ending it; the
    // single-request runs read normally, the last stopping at end of file.
    assert_eq!(
        modelled_lengths(&ranges, true, &[Fault::Short(5_000)], FILE as u64),
        (vec![4096, 904, 100, 50], 1)
    );
    // Scalar: the first request completes, an interrupt repeats the second,
    // which then moves nothing and ends the run.
    assert_eq!(
        modelled_lengths(
            &ranges,
            false,
            &[Fault::Pass, Fault::Interrupt, Fault::Zero],
            FILE as u64
        ),
        (vec![4096, 0, 100, 50], 3)
    );
}

#[test]
fn a_scalar_zero_after_progress_is_write_zero() {
    let writes = owned_writes(&[(0, b"abc"), (3, b"defg")]);
    let faults = [Fault::Pass, Fault::Zero];
    let ([((old, _), _), ((new, new_bytes), trace)], consumed) = faulted(
        &faults,
        Mutation::None,
        |file| (outcome(baseline_write(file, &writes)), file_bytes(file)),
        |file| {
            (
                outcome(write_batch_with(file, &writes, None)),
                file_bytes(file),
            )
        },
    );
    assert_eq!(old, Ok(()));
    assert_eq!(new, Err((io::ErrorKind::WriteZero, None)));
    assert_eq!(consumed, 2);
    assert_eq!(trace, [("pwrite", 0, 3), ("pwrite", 3, 4)]);
    let mut expected: Vec<u8> = (0..FILE).map(content).collect();
    expected[..3].copy_from_slice(b"abc");
    assert_eq!(new_bytes, expected);
    assert_eq!(write_oracle(&writes, None, &faults, Mutation::None), Ok(()));
}

#[test]
fn every_raw_attempt_is_traced_including_interrupted_retries() {
    let requests = reads(&[(0, 4096), (4096, 4096), (8192, 4096)]);
    let cases = [
        (
            vectored(Direction::Read),
            vec![Fault::Interrupt],
            vec![("preadv", 0, 12_288), ("preadv", 0, 12_288)],
        ),
        (
            None,
            vec![Fault::Pass, Fault::Interrupt, Fault::Interrupt],
            vec![
                ("pread", 0, 4096),
                ("pread", 4096, 4096),
                ("pread", 4096, 4096),
                ("pread", 4096, 4096),
                ("pread", 8192, 4096),
            ],
        ),
    ];
    for (vectored, faults, expected) in cases {
        let ([(old, _), (new, trace)], consumed) = faulted(
            &faults,
            Mutation::None,
            |file| outcome(baseline_read(file, &requests)),
            |file| outcome(read_batch_with(file, &requests, vectored)),
        );
        assert_eq!(new, old);
        assert_eq!(consumed, faults.len());
        assert_eq!(trace, expected);
    }
}

#[test]
fn short_writes_resume_at_the_exact_byte_and_zero_progress_is_write_zero() {
    let writes = owned_writes(&[(0, b"abc"), (3, b"defg")]);
    let pwritev = vectored(Direction::Write);
    let cases: [FaultCase; 6] = [
        (
            pwritev,
            vec![Fault::Short(2)],
            vec![("pwritev", 0, 7), ("pwritev", 2, 5)],
            true,
        ),
        (
            pwritev,
            vec![Fault::Short(4)],
            vec![("pwritev", 0, 7), ("pwrite", 4, 3)],
            true,
        ),
        (
            pwritev,
            vec![Fault::Short(1), Fault::Interrupt, Fault::Short(3)],
            vec![
                ("pwritev", 0, 7),
                ("pwritev", 1, 6),
                ("pwritev", 1, 6),
                ("pwrite", 4, 3),
            ],
            true,
        ),
        (
            None,
            vec![Fault::Short(1)],
            vec![("pwrite", 0, 3), ("pwrite", 1, 2), ("pwrite", 3, 4)],
            true,
        ),
        (pwritev, vec![Fault::Zero], vec![("pwritev", 0, 7)], false),
        (None, vec![Fault::Zero], vec![("pwrite", 0, 3)], false),
    ];
    for (vectored, faults, expected, completes) in cases {
        let ([((old, old_bytes), _), ((new, new_bytes), trace)], _) = faulted(
            &faults,
            Mutation::None,
            |file| (outcome(baseline_write(file, &writes)), file_bytes(file)),
            |file| {
                (
                    outcome(write_batch_with(file, &writes, vectored)),
                    file_bytes(file),
                )
            },
        );
        if completes {
            assert_eq!(new, old, "{faults:?}");
            assert_eq!(new_bytes, old_bytes, "{faults:?}");
        } else {
            assert_eq!(new, Err((io::ErrorKind::WriteZero, None)), "{faults:?}");
        }
        assert_eq!(trace, expected, "{faults:?}");
    }
}

#[test]
fn a_short_vectored_read_ends_its_run_with_initialized_prefixes() {
    let requests = reads(&[(0, 4096), (4096, 4096), (8192, 4096)]);
    let ([(old, _), (new, trace)], _) = faulted(
        &[Fault::Short(5_000)],
        Mutation::None,
        |file| outcome(baseline_read(file, &requests)),
        |file| outcome(read_batch_with(file, &requests, vectored(Direction::Read))),
    );
    let (old, new) = (old.unwrap(), new.unwrap());
    assert_eq!(
        new.iter().map(Bytes::len).collect::<Vec<_>>(),
        [4096, 904, 0]
    );
    assert!(old.iter().zip(&new).all(|(old, new)| old.starts_with(new)));
    assert_eq!(trace, [("preadv", 0, 12_288)]);
}

fn pages() -> io::Result<(tempfile::TempDir, File, Vec<u8>)> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("pages");
    let bytes: Vec<u8> = (0..256 * 1024).map(content).collect();
    File::create_new(&path)?.write_all_at(&bytes, 0)?;
    let file = File::open(&path)?;
    Ok((directory, file, bytes))
}

fn assert_contents(results: &[Bytes], requests: &[OwnedRead], bytes: &[u8]) {
    for (result, read) in results.iter().zip(requests) {
        let start = usize::try_from(read.offset).unwrap();
        let end = (start + read.length).min(bytes.len());
        assert_eq!(result[..], bytes[start..end]);
    }
}

#[test]
fn page_sized_reads_reuse_their_released_buffers() {
    let (_directory, file, bytes) = pages().unwrap();
    // Three contiguous 64 KiB reads and a 40 KiB read whose buffer is three pages.
    let requests = reads(&[
        (0, 65_536),
        (65_536, 65_536),
        (131_072, 65_536),
        (200_000, 40_000),
    ]);
    let pooled_bytes = 3 * 65_536 + 3 * POOL_PAGE;
    let n = requests.len() as u64;
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let shard = Shard::isolate();
                // A small read first sets up this thread's scratch state, and
                // one lock sets up the platform mutex behind the shard.
                read_batch_with(&file, &reads(&[(0, 1)]), None).unwrap();
                drop(shard.idle());
                let mut first = None;
                let allocation = measure(|| first = Some(read_batch_with(&file, &requests, None)));
                let first = first.unwrap().unwrap();
                // Buffers, their owners and the result list.
                assert_eq!(allocation.count_total, 2 * n + 1);
                assert!(allocation.bytes_total >= pooled_bytes as u64);
                assert_contents(&first, &requests, &bytes);
                let mut storage: Vec<*const u8> =
                    first.iter().map(|bytes| bytes.as_ptr()).collect();
                storage.sort_unstable();
                drop(first);
                assert_eq!(shard.idle().bytes, pooled_bytes);
                for vectored in [vectored(Direction::Read), None] {
                    let mut again = None;
                    let allocation =
                        measure(|| again = Some(read_batch_with(&file, &requests, vectored)));
                    let again = again.unwrap().unwrap();
                    // Only the owners and the result list: no buffer is allocated.
                    assert_eq!(allocation.count_total, n + 1);
                    assert!(allocation.bytes_total < POOL_PAGE as u64);
                    let mut reused: Vec<*const u8> =
                        again.iter().map(|bytes| bytes.as_ptr()).collect();
                    reused.sort_unstable();
                    assert_eq!(reused, storage);
                    assert_contents(&again, &requests, &bytes);
                }
                // Negative control: with no idle buffers the same count fails.
                {
                    let mut idle = shard.idle();
                    idle.classes.clear();
                    idle.bytes = 0;
                }
                let mut fresh = None;
                let allocation = measure(|| fresh = Some(read_batch_with(&file, &requests, None)));
                assert_eq!(allocation.count_total, 2 * n + 1);
                assert_ne!(allocation.count_total, n + 1);
                drop(fresh);
            })
            .join()
            .unwrap();
    });
}

#[test]
fn a_reused_buffer_exposes_only_the_bytes_of_its_new_read() {
    let (_directory, file, bytes) = pages().unwrap();
    let full = reads(&[(0, 65_536)]);
    let tail = reads(&[(256 * 1024 - 100, 65_536)]);
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                Shard::isolate();
                let first = read_batch_with(&file, &full, None).unwrap();
                let storage = first[0].as_ptr();
                drop(first);
                // The same buffer comes back, filled only to end of file.
                let short = read_batch_with(&file, &tail, None).unwrap();
                assert_eq!(short[0].as_ptr(), storage);
                assert_contents(&short, &tail, &bytes);
                assert_eq!(short[0].len(), 100);
            })
            .join()
            .unwrap();
    });
}

#[test]
fn the_pool_keeps_idle_bytes_within_its_budget() {
    let shard = Shard::new();
    let share = POOL_BUDGET / POOL_SHARDS;
    let class = 1024 * 1024;
    for _ in 0..share / class {
        shard.put(Vec::with_capacity(class));
    }
    assert_eq!(shard.idle().bytes, share);
    // Over budget: freed instead of kept.
    shard.put(Vec::with_capacity(class));
    assert_eq!(shard.idle().bytes, share);
    // Taking hands back an empty buffer of exactly the class and frees budget.
    let buffer = shard.take(class).unwrap();
    assert!(buffer.is_empty());
    assert_eq!(buffer.capacity(), class);
    assert_eq!(shard.idle().bytes, share - class);
    // A class with nothing idle is allocated fresh.
    assert_eq!(shard.take(2 * class).unwrap().capacity(), 2 * class);
}

/// Reads `ranges` with every timed run forced uncached (or cached), beside
/// the baseline, and returns both outcomes and the new trace.
fn advised_read(
    ranges: &[(u64, usize)],
    vectored: Option<Vectored>,
    uncached: bool,
    mutation: Mutation,
) -> (Outcome<Vec<Bytes>>, Outcome<Vec<Bytes>>, Vec<Event>) {
    let requests = reads(ranges);
    let ([(old, _), (new, trace)], _) = faulted(
        &[],
        mutation,
        |file| outcome(baseline_read(file, &requests)),
        |file| {
            super::faults::force_uncached(uncached);
            outcome(read_batch_with(file, &requests, vectored))
        },
    );
    (old, new, trace)
}

#[test]
fn an_uncached_run_advises_every_later_run_once_and_cached_batches_never_do() {
    let scattered = [
        (0, 100),
        (100, 50),
        (5_000, 100),
        (9_000, 100),
        (9_950, 100),
    ];
    let preadv = vectored(Direction::Read);
    let (old, new, trace) = advised_read(&scattered, preadv, true, Mutation::None);
    assert_eq!(new, old);
    assert_eq!(
        trace,
        [
            ("preadv", 0, 150),
            ("rdadvise", 5_000, 100),
            ("rdadvise", 9_000, 100),
            ("rdadvise", 9_950, 100),
            ("pread", 5_000, 100),
            ("pread", 9_000, 100),
            ("pread", 9_950, 100),
        ]
    );
    let (old, new, trace) = advised_read(&scattered, preadv, false, Mutation::None);
    assert_eq!(new, old);
    let cached = [
        ("preadv", 0, 150),
        ("pread", 5_000, 100),
        ("pread", 9_000, 100),
        ("pread", 9_950, 100),
    ];
    assert_eq!(trace, cached);
    // Negative control: advising a cached batch fails the same exact trace.
    let (old, new, trace) = advised_read(&scattered, preadv, false, Mutation::AdviseAlways);
    assert_eq!(new, old);
    assert_ne!(trace, cached);
    // A single run has nothing later to advise, and empty runs are not advised.
    let (_, _, trace) = advised_read(&[(0, 4096), (4096, 4096)], preadv, true, Mutation::None);
    assert_eq!(trace, [("preadv", 0, 8192)]);
    let (_, _, trace) = advised_read(&[(0, 10), (50, 0), (60, 5)], None, true, Mutation::None);
    assert_eq!(
        trace,
        [("pread", 0, 10), ("rdadvise", 60, 5), ("pread", 60, 5)]
    );
}

#[test]
fn a_cached_batch_times_a_logarithmic_number_of_runs() {
    let (_directory, file) = fixture().unwrap();
    // 64 separate runs: runs 1, 2, 4, 8, 16 and 32 are timed; run 64 has
    // nothing after it to advise.
    let ranges: Vec<_> = (0..64).map(|index| (index * 150, 100)).collect();
    let requests = reads(&ranges);
    super::faults::timed_runs();
    read_batch_with(&file, &requests, vectored(Direction::Read)).unwrap();
    assert_eq!(super::faults::timed_runs(), 6);
    // Negative control: timing every run fails the same count.
    arm(&[], Mutation::TimeEveryRun);
    read_batch_with(&file, &requests, vectored(Direction::Read)).unwrap();
    disarm(0);
    assert_eq!(super::faults::timed_runs(), 63);
    // Once advised, nothing further is timed.
    super::faults::force_uncached(true);
    read_batch_with(&file, &requests, vectored(Direction::Read)).unwrap();
    super::faults::force_uncached(false);
    assert_eq!(super::faults::timed_runs(), 1);
}

#[test]
fn advising_allocates_nothing() {
    let (_directory, file) = fixture().unwrap();
    let ranges: Vec<_> = (0..40).map(|index| (index * 250, 100)).collect();
    let requests = reads(&ranges);
    read_batch_with(&file, &requests, None).unwrap();
    super::faults::force_uncached(true);
    let mut result = None;
    let allocation = measure(|| result = Some(read_batch_with(&file, &requests, None)));
    super::faults::force_uncached(false);
    assert_eq!(result.unwrap().unwrap().len(), requests.len());
    assert_eq!(allocation.count_total, requests.len() as u64 + 1);
}

#[test]
fn the_page_cache_verdict_is_a_threshold_on_elapsed_time() {
    let now = Instant::now();
    assert!(!exceeds(now, UNCACHED));
    let earlier = now.checked_sub(UNCACHED).unwrap();
    assert!(exceeds(earlier, UNCACHED));
}
