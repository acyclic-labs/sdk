//! Sparse regular-file reads against an independent byte model: each output
//! byte is written once, straight from its chunk or as a hole's zero.
use super::super::read_work::{Work, restoring_old_work};
use super::*;
use acyclic_test_support::differential::observe;
use proptest::prelude::*;

#[path = "baseline.rs"]
mod baseline;

/// One extent of the model file: `None` is a hole, `Some(offset)` content
/// from that blob offset.
type Shape = (Option<u64>, u64);

struct Sparse {
    store: MemoryObjectStore,
    record: FileRecord,
    logical: Vec<u8>,
    blob: ObjectId,
    chunk_bytes: u64,
    shapes: Vec<Shape>,
}

fn sparse(
    content: &[u8],
    chunk_bytes: u32,
    shapes: &[Shape],
) -> Result<Sparse, Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let blob = futures::executor::block_on(build_blob(
        &store,
        &mut Cursor::new(content),
        BlobBuildOptions {
            chunk_bytes,
            page_items: 4,
            page_bytes: 1024,
            maximum_blob_bytes: 1 << 20,
        },
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))?;
    let mut extents = Vec::new();
    let mut logical = Vec::new();
    for (index, &(source, length)) in shapes.iter().enumerate() {
        let offset = logical.len() as u64;
        let kind = match source {
            Some(object_offset) => {
                let start = object_offset as usize;
                logical.extend_from_slice(&content[start..start + length as usize]);
                ExtentKind::Content {
                    object: blob.root,
                    object_offset,
                }
            }
            None => {
                logical.resize(logical.len() + length as usize, 0);
                if index % 2 == 0 {
                    ExtentKind::Hole
                } else {
                    ExtentKind::AllocatedZero
                }
            }
        };
        extents.push(Extent {
            offset,
            length,
            kind,
        });
    }
    let extents = ExtentPage::Leaf(extents);
    let root = extent_page_id(&extents, 16)?;
    let encoded = OwnedBytes::from(encode_extent_page(
        &extents,
        16,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?);
    let cancellation = CancellationToken::new();
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let put = futures::executor::block_on(AsyncObjectStore::put(&store, root, encoded, &mut work));
    work.receipt(put)?;
    let record = FileRecord {
        file_id: FileId::from_bytes([12; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([13; 32]),
        },
        payload: FilePayload::Regular {
            logical_bytes: logical.len() as u64,
            extents: root,
        },
    };
    Ok(Sparse {
        store,
        record,
        logical,
        blob: blob.root,
        chunk_bytes: u64::from(chunk_bytes),
        shapes: shapes.to_vec(),
    })
}

impl Sparse {
    fn run(&self, range: ByteRange, old: bool) -> Result<FileRangeRead, FileRangeReadFailure> {
        let cancellation = CancellationToken::new();
        let request = FileRangeRequest {
            record: self.record,
            range,
            maximum_spans: 16,
            limits: DecodeLimits::default(),
            budget: WorkBudget::UNBOUNDED,
        };
        if !old {
            return futures::executor::block_on(read_file_range_async(
                &self.store,
                request,
                &cancellation,
            ));
        }
        let mut accounting = KernelAccounting::standalone(request.budget, &cancellation);
        futures::executor::block_on(baseline::read(
            &self.store,
            request,
            &mut accounting,
            &cancellation,
        ))
        .map_err(|error| failed(file_error(error), accounting.spent()))
    }

    fn read(&self, range: ByteRange) -> (Result<FileRangeRead, FileRangeReadFailure>, Vec<Work>) {
        observe(
            || (),
            |()| {
                futures::executor::block_on(read_file_range_async(
                    &self.store,
                    FileRangeRequest {
                        record: self.record,
                        range,
                        maximum_spans: 16,
                        limits: DecodeLimits::default(),
                        budget: WorkBudget::UNBOUNDED,
                    },
                    &CancellationToken::new(),
                ))
            },
        )
    }

    /// The independent model: the output buffer, then for each extent in
    /// the range its zeros or its pieces of each content chunk, in order.
    fn expected_work(&self, range: ByteRange) -> Vec<Work> {
        let mut work = vec![("storage", 0, range.length)];
        let end = range.offset + range.length;
        let mut at = 0;
        for (offset, &(source, length)) in self.extent_offsets() {
            let start = offset.max(range.offset);
            let stop = (offset + length).min(end);
            if start >= stop {
                continue;
            }
            let Some(object_offset) = source else {
                work.push(("zero", at, stop - start));
                at += stop - start;
                continue;
            };
            let mut position = object_offset + start - offset;
            let base = position;
            let object_end = object_offset + stop - offset;
            while position < object_end {
                let chunk_end = (position / self.chunk_bytes + 1) * self.chunk_bytes;
                let piece = chunk_end.min(object_end) - position;
                work.push(("chunk", position - base, piece));
                at += piece;
                position += piece;
            }
        }
        work
    }

    /// Bytes copied by planning `range`, standalone reads of its content
    /// spans, and its holes' zeros.
    fn component_copies(&self, range: ByteRange) -> u64 {
        let end = range.offset + range.length;
        let mut copies = self.plan_copies(range);
        for (offset, &(source, length)) in self.extent_offsets() {
            let start = offset.max(range.offset);
            let stop = (offset + length).min(end);
            if start >= stop {
                continue;
            }
            let Some(object_offset) = source else {
                copies += stop - start;
                continue;
            };
            let blob = futures::executor::block_on(crate::kernel::read_blob_range(
                &self.store,
                self.blob,
                ByteRange {
                    offset: object_offset + start - offset,
                    length: stop - start,
                },
                DecodeLimits::default(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            ))
            .unwrap();
            copies += blob.work.bytes_copied;
        }
        copies
    }

    /// Bytes the independent extent planner copies for `range`.
    fn plan_copies(&self, range: ByteRange) -> u64 {
        let FilePayload::Regular { extents, .. } = self.record.payload else {
            unreachable!("sparse fixtures are regular files")
        };
        let plan = futures::executor::block_on(crate::kernel::range::plan_extent_range(
            &self.store,
            crate::kernel::range::ExtentRangeRequest {
                root: extents,
                file_size: self.logical.len() as u64,
                range,
                maximum_spans: 16,
                limits: DecodeLimits::default(),
                budget: WorkBudget::UNBOUNDED,
            },
            &CancellationToken::new(),
        ))
        .unwrap();
        plan.work.bytes_copied
    }

    fn extent_offsets(&self) -> impl Iterator<Item = (u64, &Shape)> {
        self.shapes.iter().scan(0, |offset, shape| {
            let start = *offset;
            *offset += shape.1;
            Some((start, shape))
        })
    }
}

fn output_work(trace: &[Work]) -> Vec<Work> {
    trace
        .iter()
        .copied()
        .filter(|(operation, ..)| *operation != "batch")
        .collect()
}

fn shapes(content: usize) -> impl Strategy<Value = Vec<Shape>> {
    prop::collection::vec(
        (any::<bool>(), 1_u64..=content as u64).prop_flat_map(move |(hole, length)| {
            (0..=content as u64 - length)
                .prop_map(move |offset| ((!hole).then_some(offset), length))
        }),
        1..=12,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn sparse_reads_match_independent_model_and_write_each_byte_once(
        (content, chunk_bytes, shapes, start, extra) in prop::collection::vec(any::<u8>(), 1..=48)
            .prop_flat_map(|content| {
                let length = content.len();
                (Just(content), 1_u32..=9, shapes(length), any::<prop::sample::Index>(), 0_u64..=3)
            })
    ) {
        let file = sparse(&content, chunk_bytes, &shapes).unwrap();
        let size = file.logical.len() as u64;
        let offset = start.index(size as usize + 1) as u64;
        let range = ByteRange { offset, length: size - offset + extra };
        let [(old, old_trace), (read, trace)] = acyclic_test_support::differential::pair(
            || sparse(&content, chunk_bytes, &shapes).unwrap(),
            |file, old| file.run(range, old),
        );
        let chunks = |trace: &[Work]| trace.iter().copied()
            .filter(|(op, ..)| matches!(*op, "chunk" | "batch")).collect::<Vec<_>>();
        prop_assert_eq!(chunks(&trace), chunks(&old_trace));
        if extra != 0 {
            let failure = read.expect_err("a range past the end must fail");
            let old = old.expect_err("the baseline must reject the same range");
            prop_assert_eq!(format!("{:?}", failure.error), format!("{:?}", old.error));
            prop_assert_eq!(failure.work.output_bytes, 0);
            prop_assert!(output_work(&trace).is_empty());
            return Ok(());
        }
        let read = read.unwrap();
        let old = old.unwrap();
        prop_assert_eq!(read.bytes.as_ref(), old.bytes.as_ref());
        prop_assert_eq!(read.work.backend_read_operations, old.work.backend_read_operations);
        prop_assert_eq!(read.work.object_bytes_read, old.work.object_bytes_read);
        prop_assert_eq!(read.work.bytes_hashed, old.work.bytes_hashed);
        prop_assert_eq!(read.work.page_reads, old.work.page_reads);
        prop_assert_eq!(read.work.items_examined, old.work.items_examined);
        let mut content_bytes = 0;
        let mut content_spans = 0;
        for (offset, &(source, length)) in file.extent_offsets() {
            let start = offset.max(range.offset);
            let end = (offset + length).min(range.offset + range.length);
            if source.is_some() && start < end {
                content_bytes += end - start;
                content_spans += 1;
            }
        }
        prop_assert_eq!(old.work.bytes_copied, read.work.bytes_copied + 2 * content_bytes);
        prop_assert_eq!(old.work.allocation_operations, read.work.allocation_operations + content_spans);
        prop_assert_eq!(read.bytes.as_ref(), &file.logical[offset as usize..]);
        prop_assert_eq!(output_work(&trace), file.expected_work(range));
        // The read copies what planning and a standalone read of each
        // content span copy, plus each hole's zeros: no second copy.
        prop_assert_eq!(read.work.bytes_copied, file.component_copies(range));
        prop_assert_eq!(read.work.output_bytes, range.length);
        prop_assert_eq!(read.logical_owned_bytes(), range.length);
    }
}

#[test]
fn restored_zero_fill_is_detected() -> Result<(), Box<dyn std::error::Error>> {
    let file = sparse(b"ABCDEFGHIJ", 2, &[(Some(0), 2), (None, 2), (Some(4), 4)])?;
    let range = ByteRange {
        offset: 1,
        length: 6,
    };
    let (read, trace) = file.read(range);
    assert_eq!(read?.bytes.as_ref(), b"B\0\0EFG");
    assert_eq!(output_work(&trace), file.expected_work(range));
    let (old, old_trace) = restoring_old_work(|| file.read(range));
    assert_eq!(old?.bytes.as_ref(), b"B\0\0EFG", "output is unchanged");
    assert_ne!(
        output_work(&old_trace),
        file.expected_work(range),
        "the trace exposes a whole-buffer zero fill"
    );
    Ok(())
}

#[test]
fn redundant_span_allocations_and_copies_fail_the_exact_work_contract()
-> Result<(), Box<dyn std::error::Error>> {
    let range = ByteRange {
        offset: 1,
        length: 6,
    };
    let [old, current] = [true, false].map(|old| {
        sparse(b"ABCDEFGHIJ", 2, &[(Some(0), 2), (None, 2), (Some(4), 4)])
            .and_then(|file| file.run(range, old).map_err(Into::into))
    });
    let old = old?;
    let current = current?;
    let expected = (
        old.work.bytes_copied - 8,
        old.work.allocation_operations - 2,
    );
    let work = |read: &FileRangeRead| (read.work.bytes_copied, read.work.allocation_operations);
    assert_eq!(work(&current), expected);
    assert_eq!(old.bytes, current.bytes);
    assert_ne!(
        work(&old),
        expected,
        "restoring the old path must fail the same exact contract"
    );
    Ok(())
}

/// Copy and zero sites in the read path: a full-range zero fill or a second
/// copy of chunk bytes would add one.
const READ_SOURCES: [(&str, &str); 2] = [
    ("file_read.rs", include_str!("../../file_read.rs")),
    ("blob/range.rs", include_str!("../../blob/range.rs")),
];

const ALLOWED: [(&str, &str, usize); 6] = [
    ("file_read.rs", "resize(", 1),
    ("file_read.rs", "copy_from_slice(", 0),
    ("file_read.rs", "extend_from_slice(", 1),
    ("file_read.rs", "read_blob_range_with_accounting(", 0),
    ("blob/range.rs", "extend_from_slice(", 1),
    ("blob/range.rs", "resize(", 0),
];

fn gate_violations(sources: &[(&str, String)]) -> Vec<String> {
    ALLOWED
        .iter()
        .filter_map(|&(file, pattern, allowed)| {
            let source = &sources.iter().find(|(name, _)| *name == file)?.1;
            let found = source.matches(pattern).count();
            (found != allowed)
                .then(|| format!("{file} has {found} {pattern} sites, allowed {allowed}"))
        })
        .collect()
}

#[test]
fn read_path_writes_output_bytes_at_one_site_each() {
    let sources: Vec<_> = READ_SOURCES
        .iter()
        .map(|&(name, source)| (name, source.to_owned()))
        .collect();
    assert_eq!(gate_violations(&sources), Vec::<String>::new());
    let mut zero_filled = sources.clone();
    zero_filled[0].1 = zero_filled[0].1.replacen(
        "            #[cfg(test)]\n            read_work::storage(",
        "            output.value.resize(length, 0);\n            output.value.clear();\n            #[cfg(test)]\n            read_work::storage(",
        1,
    );
    assert_eq!(
        gate_violations(&zero_filled),
        ["file_read.rs has 2 resize( sites, allowed 1"]
    );
}

#[test]
#[ignore = "measurement"]
fn measure_sparse_reads() -> Result<(), Box<dyn std::error::Error>> {
    let content: Vec<u8> = (0..1 << 20).map(|byte| byte as u8).collect();
    for (label, shapes) in [
        ("dense 1M", vec![(Some(0), 1 << 20)]),
        (
            "sparse 1M",
            vec![
                (Some(0), 256 << 10),
                (None, 512 << 10),
                (Some(512 << 10), 256 << 10),
            ],
        ),
    ] {
        for old in [true, false] {
            let file = sparse(&content, 64 << 10, &shapes)?;
            let range = ByteRange {
                offset: 0,
                length: file.logical.len() as u64,
            };
            // Warm each implementation on its own store.
            std::hint::black_box(file.run(range, old)?);
            let rounds = 64;
            let started = std::time::Instant::now();
            for _ in 0..rounds {
                std::hint::black_box(file.run(range, old)?);
            }
            println!(
                "{label} {}: {} ns/read",
                if old { "baseline" } else { "candidate" },
                started.elapsed().as_nanos() / rounds
            );
        }
    }
    Ok(())
}

/// Native accounting has an additional ownership-header allocation on the old
/// per-span path. Exercise the same production append path with original custody.
#[cfg(not(target_arch = "wasm32"))]
fn original_read(
    file: Sparse,
    range: ByteRange,
    old: bool,
) -> (Result<Vec<u8>, String>, WorkCounters) {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)
        .unwrap();
    let baseline = crate::NativeMaterializationWork::accounting_state_bytes();
    let request = FileRangeRequest {
        record: file.record,
        range,
        maximum_spans: 16,
        limits: DecodeLimits::default(),
        budget: WorkBudget::UNBOUNDED,
    };
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let result = if old {
        futures::executor::block_on(baseline::read(
            &file.store,
            request,
            &mut accounting,
            &cancellation,
        ))
    } else {
        futures::executor::block_on(read_file_range_with_accounting(
            &file.store,
            request,
            &mut accounting,
            &cancellation,
        ))
    };
    let spent = original.receipt();
    let bytes = result
        .map(|read| read.bytes.as_ref().to_vec())
        .map_err(|error| format!("{error:?}"));
    drop(file);
    assert_eq!(original.live_allocation_bytes(), baseline);
    (bytes, spent)
}

#[cfg(not(target_arch = "wasm32"))]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]
    #[test]
    fn original_reads_match_the_old_bytes_errors_and_exact_allocation_delta(
        (content, chunk_bytes, shapes, extra) in prop::collection::vec(any::<u8>(), 1..=32)
            .prop_flat_map(|content| {
                let length = content.len();
                (Just(content), 1_u32..=9, shapes(length), 0_u64..=1)
            })
    ) {
        let length: u64 = shapes.iter().map(|shape| shape.1).sum();
        let range = ByteRange { offset: 0, length: length + extra };
        let [((old, old_work), old_trace), ((current, current_work), current_trace)] =
            acyclic_test_support::differential::pair(
                || sparse(&content, chunk_bytes, &shapes).unwrap(),
                |file, old| original_read(file, range, old),
            );
        prop_assert_eq!(&current, &old);
        let chunks = |trace: &[Work]| trace.iter().copied()
            .filter(|(op, ..)| matches!(*op, "chunk" | "batch")).collect::<Vec<_>>();
        prop_assert_eq!(chunks(&current_trace), chunks(&old_trace));
        if extra == 0 {
            let content_spans = shapes.iter().filter(|shape| shape.0.is_some()).count() as u64;
            let content_bytes: u64 = shapes.iter().filter(|shape| shape.0.is_some()).map(|shape| shape.1).sum();
            prop_assert_eq!(old_work.allocation_operations, current_work.allocation_operations + 2 * content_spans);
            prop_assert_eq!(old_work.bytes_copied, current_work.bytes_copied + 2 * content_bytes);
            prop_assert_eq!(old_work.backend_read_operations, current_work.backend_read_operations);
            prop_assert_eq!(old_work.object_bytes_read, current_work.object_bytes_read);
            prop_assert_eq!(old_work.bytes_hashed, current_work.bytes_hashed);
            prop_assert_eq!(old_work.output_bytes, current_work.output_bytes);
            if content_spans > 0 {
                // The independently executed old path restores the redundant
                // allocation/copy and must fail the candidate's exact contract.
                prop_assert_ne!(old_work.allocation_operations,
                    current_work.allocation_operations);
            }
        }
    }
}
