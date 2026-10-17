//! Object reads against an independent copy of the previous algorithm, with exact work.
use super::*;
use acyclic_native_runtime::{NativeOperationKind, NativeReadMeter};
use acyclic_test_support::differential;
use proptest::prelude::*;
use std::sync::Mutex;

type Work = (&'static str, u64, u64);
type Outcome = Result<Vec<u8>, Error>;

/// Counts the production path's physical reads. Buffer allocations are exact
/// in the work trace; the meter also sees runtime worker bookkeeping.
#[derive(Default)]
struct CountingMeter(Mutex<Counts>);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Counts {
    read_operations: u64,
    read_bytes: u64,
}

impl CountingMeter {
    fn counts(&self) -> Counts {
        *self.0.lock().unwrap()
    }
    fn update(&self, change: impl FnOnce(&mut Counts)) {
        change(&mut self.0.lock().unwrap());
    }
}

impl NativeReadMeter for CountingMeter {
    fn check(&self) -> std::io::Result<()> {
        Ok(())
    }
    fn reserve_operations(&self, _: NativeOperationKind, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn cancel_reserved_operations(&self, _: NativeOperationKind, _: u64) {}
    fn record_reserved_operation(&self, kind: NativeOperationKind) -> std::io::Result<()> {
        if kind == NativeOperationKind::Read {
            self.update(|counts| counts.read_operations += 1);
        }
        Ok(())
    }
    fn admit_write_bytes(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn reserve_bytes(&self, _: NativeOperationKind, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn cancel_reserved_bytes(&self, _: NativeOperationKind, _: u64) {}
    fn record_reserved_bytes(
        &self,
        kind: NativeOperationKind,
        _: u64,
        actual: u64,
    ) -> std::io::Result<()> {
        if kind == NativeOperationKind::Read {
            self.update(|counts| counts.read_bytes += actual);
        }
        Ok(())
    }
    fn allocate(&self, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn reserve_allocation(&self, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn cancel_reserved_allocation(&self, _: u64) {}
    fn record_reserved_allocation(&self, bytes: u64) -> std::io::Result<()> {
        self.allocate(bytes)
    }
    fn allocation_failed(&self, error: std::collections::TryReserveError) -> std::io::Error {
        std::io::Error::other(error)
    }
    fn allocate_refcount_header(&self) -> std::io::Result<()> {
        self.allocate(0)
    }
    fn adopt(&self, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
        (bytes, self.adopt(bytes))
    }
    fn admit_codec(&self, _: u64, _: u64, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn record_codec(&self, _: u64, _: u64, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn examine(&self, _: u64, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn admit_examination(&self, _: u64, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn record_examination(&self, _: u64, _: u64) -> std::io::Result<()> {
        Ok(())
    }
    fn release(&self, _: u64) {}
    fn admit_bytes(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn admit_read(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn record_read(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn admit_write(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn record_write(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn admit_durability(&self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The previous algorithm, written separately: open, stat, a zero-filled
/// 41 KB prefix, its own table decoder, then a zero-filled selected range.
fn baseline_read(
    directory: &Path,
    digest: &[u8; 32],
    length: usize,
    location: &LocalBodyLocation,
    (start, end): (usize, usize),
) -> Outcome {
    let unavailable = || Error::from(Unavailable);
    let read_exact = |file: &File, offset: u64, length: usize| {
        let mut bytes = vec![0_u8; length];
        let mut filled = 0;
        while filled < length {
            let count = read_at(file, offset + filled as u64, &mut bytes[filled..]);
            if count == 0 {
                return Err(unavailable());
            }
            filled += count;
        }
        Ok(bytes)
    };
    if start > end || end > length {
        return Err(unavailable());
    }
    match location {
        LocalBodyLocation::Journal { offset } => {
            if length > 64 * 1024 {
                return Err(unavailable());
            }
            let file = File::open(directory.join("mutations.log")).map_err(|_| unavailable())?;
            let body = read_exact(&file, *offset, length)?;
            if blake3::hash(&body).as_bytes() != digest {
                return Err(unavailable());
            }
            Ok(body[start..end].to_vec())
        }
        LocalBodyLocation::Segment { id, offset } => {
            let path = directory
                .join("segments")
                .join(format!("{}.segment", hex::encode(id)));
            let file = File::open(path).map_err(|_| unavailable())?;
            let file_length = file.metadata().map_err(|_| unavailable())?.len();
            let mut prefix = vec![0_u8; 28 + 1024 * 40];
            let mut filled = 0;
            loop {
                let count = read_at(&file, filled as u64, &mut prefix[filled..]);
                if count == 0 {
                    break;
                }
                filled += count;
            }
            prefix.truncate(filled);
            if prefix.get(..24) != Some(b"ACYCLIC-OBJECT-SEGMENT\0\x02".as_slice()) {
                return Err(unavailable());
            }
            let count = u32::from_le_bytes(prefix[24..28].try_into().unwrap()) as usize;
            if count == 0 || count > 1024 || prefix.len() < 28 + count * 40 {
                return Err(unavailable());
            }
            let mut position = (28 + count * 40) as u64;
            let mut found = false;
            for record in prefix[28..28 + count * 40].chunks(40) {
                let record_length = u64::from_le_bytes(record[32..40].try_into().unwrap());
                let record_end = position
                    .checked_add(record_length)
                    .ok_or_else(unavailable)?;
                if record_end > file_length {
                    return Err(unavailable());
                }
                if position == *offset
                    && record[..32] == digest[..]
                    && record_length == length as u64
                {
                    found = true;
                    break;
                }
                position = record_end;
            }
            if !found {
                return Err(unavailable());
            }
            read_exact(&file, offset + start as u64, end - start)
        }
    }
}

fn read_at(file: &File, offset: u64, bytes: &mut [u8]) -> usize {
    #[cfg(unix)]
    let count = std::os::unix::fs::FileExt::read_at(file, bytes, offset);
    #[cfg(windows)]
    let count = std::os::windows::fs::FileExt::seek_read(file, bytes, offset);
    count.unwrap()
}

fn service_error(error: NativeObjectsError) -> Error {
    match error.cause {
        NativeObjectsCause::Service(error) => error,
        cause => panic!("unexpected native cause {cause:?}"),
    }
}

/// One production read in each mode, its outputs, work trace and meter counts.
fn production_read(
    root: &Arc<LocalRoot>,
    digest: &[u8; 32],
    length: usize,
    location: &LocalBodyLocation,
    (start, end): (usize, usize),
) -> ((Outcome, Outcome), Vec<Work>, Counts) {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let meter = Arc::new(CountingMeter::default());
    let native: Arc<dyn NativeReadMeter> = meter.clone();
    let (outcomes, work) = differential::observe::<_, _, Work>(
        || (),
        |()| {
            let read = |mode| {
                futures::executor::block_on(read_body_at(
                    root, digest, length, location, start, end, mode,
                ))
            };
            let ordinary = read(crate::body::BodyReadMode::Ordinary)
                .map(|bytes| bytes.as_slice().to_vec())
                .map_err(service_error);
            let native = read(crate::body::BodyReadMode::Native(native))
                .map(|bytes| bytes.as_slice().to_vec())
                .map_err(service_error);
            (ordinary, native)
        },
    );
    (outcomes, work, meter.counts())
}

struct Fixture {
    directory: tempfile::TempDir,
    root: Arc<LocalRoot>,
    bodies: Vec<(Vec<u8>, [u8; 32], LocalBodyLocation)>,
}

fn fixture(sizes: &[usize], journal: usize) -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    fs::create_dir_all(directory.path().join("segments")).unwrap();
    let payloads: Vec<Vec<u8>> = sizes
        .iter()
        .enumerate()
        .map(|(index, size)| {
            (0..*size)
                .map(|byte| (byte * 31 + index * 7) as u8)
                .collect()
        })
        .collect();
    let entries: Vec<([u8; 32], acyclic_stream::OwnedBytes)> = payloads
        .iter()
        .map(|payload| {
            (
                *blake3::hash(payload).as_bytes(),
                acyclic_stream::OwnedBytes::copy_from_slice(payload),
            )
        })
        .collect();
    let (id, offsets) = persist_segment(
        directory.path(),
        &entries,
        LocalDurability::FullFlush,
        &mut Publication::new(None),
    )
    .unwrap();
    let mut bodies: Vec<_> = payloads
        .into_iter()
        .zip(entries)
        .zip(offsets)
        .map(|((payload, (digest, _)), offset)| {
            (payload, digest, LocalBodyLocation::Segment { id, offset })
        })
        .collect();
    let inline: Vec<u8> = (0..journal).map(|byte| (byte * 13 + 5) as u8).collect();
    let mut log = vec![0xa5; 3];
    log.extend_from_slice(&inline);
    fs::write(directory.path().join(JOURNAL_FILE), &log).unwrap();
    let digest = *blake3::hash(&inline).as_bytes();
    bodies.push((inline, digest, LocalBodyLocation::Journal { offset: 3 }));
    let root = Arc::new(LocalRoot::new(directory.path().to_path_buf()));
    Fixture {
        directory,
        root,
        bodies,
    }
}

fn segment_path_of(fixture: &Fixture) -> PathBuf {
    match &fixture.bodies[0].2 {
        LocalBodyLocation::Segment { id, .. } => segment_path(fixture.directory.path(), id),
        LocalBodyLocation::Journal { .. } => unreachable!(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    /// Ordinary and native reads return exactly the baseline's bytes or error,
    /// through valid, truncated and corrupted segments and journals.
    #[test]
    fn object_reads_match_independent_baseline(
        sizes in prop::collection::vec(prop_oneof![0_usize..80, 40_000_usize..48_000], 1..4),
        journal in 0_usize..300,
        selected in any::<prop::sample::Index>(),
        range in any::<(prop::sample::Index, prop::sample::Index)>(),
        fault in 0_u8..4,
        position in any::<prop::sample::Index>(),
    ) {
        let fixture = fixture(&sizes, journal);
        let (payload, digest, location) = &fixture.bodies[selected.index(fixture.bodies.len())];
        let (a, b) = (range.0.index(payload.len() + 1), range.1.index(payload.len() + 1));
        let span = (a.min(b), a.max(b));
        let segment = segment_path_of(&fixture);
        let segment_length = fs::metadata(&segment).unwrap().len() as usize;
        match fault {
            1 => OpenOptions::new().write(true).open(&segment).unwrap()
                .set_len(position.index(segment_length) as u64).unwrap(),
            2 => {
                let mut bytes = fs::read(&segment).unwrap();
                let table = 28 + sizes.len() * 40;
                bytes[position.index(table)] ^= 0x40;
                fs::write(&segment, bytes).unwrap();
            }
            3 => {
                let journal_path = fixture.directory.path().join(JOURNAL_FILE);
                let mut bytes = fs::read(&journal_path).unwrap();
                let index = position.index(bytes.len());
                bytes[index] ^= 1;
                fs::write(&journal_path, bytes).unwrap();
            }
            _ => {}
        }
        let expected = baseline_read(fixture.directory.path(), digest, payload.len(), location, span);
        let ((ordinary, native), _, _) =
            production_read(&fixture.root, digest, payload.len(), location, span);
        if fault == 0 {
            prop_assert_eq!(expected.as_deref(), Ok(&payload[span.0..span.1]));
        }
        prop_assert_eq!(&ordinary, &expected);
        prop_assert_eq!(&native, &expected);
    }
}

#[test]
fn segment_reads_do_exact_uninitialized_work() {
    // A large first body: the old path read 41 KB of it as "prefix".
    let fixture = fixture(&[100_000, 20], 64);
    let table = (28 + 2 * 40) as u64;
    let maximum = (28 + 1024 * 40) as u64;
    let cases: Vec<(usize, (usize, usize))> = vec![(0, (10, 4106)), (1, (0, 20)), (2, (1, 63))];
    for broken in [false, true] {
        for (index, span) in &cases {
            let (payload, digest, location) = &fixture.bodies[*index];
            let run = || production_read(&fixture.root, digest, payload.len(), location, *span);
            let ((ordinary, native), work, counts) = if broken {
                crate::body::work::restoring_old_work(run)
            } else {
                run()
            };
            let selected = (span.1 - span.0) as u64;
            let expected_bytes = &payload[span.0..span.1];
            assert_eq!(ordinary.as_deref(), Ok(expected_bytes));
            assert_eq!(native.as_deref(), Ok(expected_bytes));
            let (expected_work, expected_counts) = match location {
                LocalBodyLocation::Segment { offset, .. } => {
                    let prefix = (*offset).min(maximum);
                    assert_eq!(prefix, if *index == 0 { table } else { maximum });
                    let at = offset + span.0 as u64;
                    (
                        vec![
                            ("read", 0, prefix),
                            ("read", at, selected),
                            ("storage", 0, selected),
                            ("storage", 0, prefix),
                            ("read", 0, prefix),
                            ("read", at, selected),
                        ],
                        Counts {
                            read_operations: 2,
                            read_bytes: prefix + selected,
                        },
                    )
                }
                LocalBodyLocation::Journal { offset } => {
                    let length = payload.len() as u64;
                    (
                        vec![
                            ("journal", *offset, length),
                            ("storage", 0, length),
                            ("read", *offset, length),
                        ],
                        Counts {
                            read_operations: 1,
                            read_bytes: length,
                        },
                    )
                }
            };
            eprintln!("case {index} broken={broken}: {counts:?}");
            if broken {
                // The negative control restores the full prefix and zero-fill.
                assert_ne!(work, expected_work, "restored work was not detected");
                if *index == 0 {
                    assert_ne!(
                        counts, expected_counts,
                        "restored prefix bytes were not counted"
                    );
                }
            } else {
                assert_eq!(work, expected_work);
                assert_eq!(counts, expected_counts);
            }
        }
    }
}

/// Zero-fill cannot return to object reads: every native buffer comes from
/// one constructor of uninitialized capacity, and the old filled-storage API is gone.
#[test]
fn object_reads_have_one_uninitialized_storage_constructor() {
    let sources = [
        ("body.rs", include_str!("../body.rs")),
        ("physical/read.rs", include_str!("read.rs")),
    ];
    for (name, source) in sources {
        let production = source.split("#[cfg(all(test").next().unwrap();
        for forbidden in [
            "PreparedOwnedBytes",
            "read_prepared_bounded_async",
            "resize(",
            "try_reserve_exact(",
        ] {
            let allowed = usize::from(name == "body.rs" && forbidden == "try_reserve_exact(");
            assert_eq!(
                production.matches(forbidden).count(),
                allowed,
                "{name} uses {forbidden} outside the shared storage constructor"
            );
        }
    }
}

/// Paired measurement, not a gate: `cargo test --release -p acyclic-objects
/// --features local --lib physical::read_tests::measure -- --ignored --nocapture`.
#[test]
#[ignore = "measurement"]
fn measure_object_reads() {
    let fixture = fixture(&[1 << 20, 4096], 4096);
    for (name, index, span) in [
        ("segment 4 KiB of 1 MiB", 0, (8192, 12288)),
        ("segment 4 KiB body", 1, (0, 4096)),
        ("journal 4 KiB", 2, (0, 4096)),
    ] {
        let (payload, digest, location) = &fixture.bodies[index];
        let mut nanos = [0_u128; 2];
        for round in 0..2_000 {
            let old = round % 2 == 0;
            let started = std::time::Instant::now();
            let run = || production_read(&fixture.root, digest, payload.len(), location, span);
            let ((ordinary, native), _, _) = if old {
                crate::body::work::restoring_old_work(run)
            } else {
                run()
            };
            nanos[usize::from(!old)] += started.elapsed().as_nanos();
            assert!(ordinary.is_ok() && native.is_ok());
        }
        println!(
            "{name}: restored-old {} ns, new {} ns per ordinary+native pair",
            nanos[0] / 1_000,
            nanos[1] / 1_000
        );
    }
}
