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
            let mut bytes = vec![0; FILE + 512];
            let count = file.read_at(&mut bytes, 0).unwrap();
            bytes.truncate(count);
            (result, bytes)
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

fn file_bytes(file: &File) -> Vec<u8> {
    let mut bytes = vec![0; FILE + 512];
    let count = file.read_at(&mut bytes, 0).unwrap();
    bytes.truncate(count);
    bytes
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
            "{new:?} vs baseline {old:?}; bytes equal: {}",
            new_bytes == old_bytes
        )),
    }
}

/// The read oracle: every buffer is an initialized prefix of the baseline
/// buffer for the same request, and without an injected short transfer the
/// whole result equals the baseline.
fn read_oracle(
    ranges: &[(u64, usize)],
    vectored: Option<Vectored>,
    faults: &[Fault],
    mutation: Mutation,
) -> Result<(), String> {
    let requests = reads(ranges);
    let ([(old, _), (new, trace)], consumed) = faulted(
        faults,
        mutation,
        |file| outcome(baseline_read(file, &requests)),
        |file| outcome(read_batch_with(file, &requests, vectored)),
    );
    if consumed > trace.len() {
        return Err(format!("{consumed} faults but {} attempts", trace.len()));
    }
    let shortened = faults
        .get(..consumed)
        .unwrap_or_default()
        .iter()
        .any(|fault| matches!(fault, Fault::Zero | Fault::Short(_)));
    match (&old, &new) {
        (Ok(old), Ok(new)) if old.len() == new.len() => {
            let prefixes = old.iter().zip(new).all(|(old, new)| old.starts_with(new));
            if !prefixes {
                return Err("a buffer is not a prefix of its request's bytes".to_owned());
            }
            (shortened || old == new)
                .then_some(())
                .ok_or_else(|| "unfaulted result differs from the baseline".to_owned())
        }
        _ if old == new => Ok(()),
        _ => Err(format!("{new:?} vs baseline {old:?}")),
    }
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
        prop::collection::vec((any::<bool>(), 0..(FILE as u64), 0..300_usize), 1..12),
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
    ) {
        let preadv = vectorize.then(|| vectored(Direction::Read)).flatten();
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
        assert!(write_oracle(&writes, vectored, &short, Mutation::WrongResume).is_err());
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
    }
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
