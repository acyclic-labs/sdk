//! Shared byte-input oracle for the real canonical decoder accounting boundary.
use super::allocation::KernelAccounting;
use super::codec::{CanonicalInvariant, DecodedPageKind, DecodedPageShape, ReadAllocationStep};
use super::file_read::OriginalReadAccounting;
use super::*;
use crate::{CancellationToken, FsError, WorkBudget, WorkCounters};

/// Re-encodes an admitted value and requires the encoding to decode to it.
/// `PLANTED` flips one canonical byte: the negative control for this oracle.
fn canonical<T: PartialEq + std::fmt::Debug, const PLANTED: bool>(
    value: &T,
    encoded: Result<Vec<u8>, FsError>,
    decode: impl FnOnce(&[u8]) -> Result<T, FsError>,
) -> Vec<u8> {
    let mut canonical = encoded.expect("an admitted value has a canonical encoding");
    if PLANTED {
        *canonical.last_mut().expect("encodings carry a header") ^= 1;
    }
    let decoded = decode(&canonical).expect("a canonical encoding decodes");
    assert_eq!(
        &decoded, value,
        "canonical encoding changes the decoded value"
    );
    canonical
}

/// One persisted page or object kind per selector, in decoder-oracle order.
const KINDS: u8 = 6;

fn decode<const PLANTED: bool>(
    kind: u8,
    bytes: &[u8],
    limits: DecodeLimits,
    prefix: usize,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), FsError>,
) -> Result<Vec<u8>, FsError> {
    let allocation_error = |_| CanonicalDecodeError::AllocationFailed.into();
    let items = limits.maximum_page_items;
    Ok(match kind {
        0 => {
            let value = decode_file_metadata(bytes, limits, observe)?;
            canonical::<_, PLANTED>(
                &value,
                encode_file_metadata(value, |_| Ok::<_, FsError>(()), allocation_error),
                |bytes| decode_file_metadata(bytes, limits, &mut |_| Ok::<_, FsError>(())),
            )
        }
        1 => {
            let value = decode_generation_root(bytes, limits, observe)?;
            canonical::<_, PLANTED>(
                &value,
                encode_generation_root(&value, |_| Ok::<_, FsError>(()), allocation_error),
                |bytes| decode_generation_root(bytes, limits, |_| Ok::<_, FsError>(())),
            )
        }
        2 => {
            let value = decode_file_table_page(bytes, limits, observe)?;
            canonical::<_, PLANTED>(
                &value,
                encode_file_table_page(&value, items, |_| Ok::<_, FsError>(()), allocation_error),
                |bytes| decode_file_table_page(bytes, limits, &mut |_| Ok::<_, FsError>(())),
            )
        }
        3 => {
            let value = decode_tree_page(bytes, limits, observe)?;
            canonical::<_, PLANTED>(
                &value,
                encode_tree_page(&value, items, |_| Ok::<_, FsError>(()), allocation_error),
                |bytes| decode_tree_page(bytes, limits, &mut |_| Ok::<_, FsError>(())),
            )
        }
        4 => {
            // Shape admission is a separate production entry point. Exercise
            // arbitrary bytes even when full decoding or its admission fails.
            let shape = super::extent::decode_extent_page_shape(bytes, limits);
            let decoded = decode_extent_page(bytes, limits, observe);
            if let Err(FsError::Decode(error)) = &decoded
                && matches!(
                    error,
                    CanonicalDecodeError::WrongDomain
                        | CanonicalDecodeError::UnsupportedVersion { .. }
                        | CanonicalDecodeError::ObjectTooLarge { .. }
                        | CanonicalDecodeError::FieldTooLarge { .. }
                        | CanonicalDecodeError::UnknownTag { .. }
                        | CanonicalDecodeError::TrailingBytes
                        | CanonicalDecodeError::Invariant(CanonicalInvariant::Static(_))
                )
            {
                // Full-only extent semantics and injected Truncated refusals
                // do not establish a structural shape-decoder error.
                assert_eq!(
                    shape,
                    Err(error.clone()),
                    "extent shape refusal differs from the full decoder"
                );
            }
            let value = decoded?;
            let (kind, count) = match &value {
                ExtentPage::Leaf(items) => (DecodedPageKind::Leaf, items.len()),
                ExtentPage::Internal(items) => (DecodedPageKind::Internal, items.len()),
            };
            assert_eq!(
                shape,
                Ok(DecodedPageShape {
                    kind,
                    items: count,
                    nested_bytes: 0,
                }),
                "extent shape differs from the fully validated page"
            );
            assert!(
                super::extent::decode_extent_page_shape(&bytes[..prefix % bytes.len()], limits)
                    .is_err(),
                "extent shape admitted a truncated frame"
            );
            let mut trailing = bytes.to_vec();
            trailing.push(0);
            assert!(
                super::extent::decode_extent_page_shape(&trailing, limits).is_err(),
                "extent shape admitted trailing bytes"
            );
            let version = [0_u16, 2, u16::MAX][prefix % 3];
            let mut header = bytes[..10].to_vec();
            header[8..10].copy_from_slice(&version.to_le_bytes());
            assert_eq!(
                super::extent::decode_extent_page_shape(&header, limits),
                Err(CanonicalDecodeError::UnsupportedVersion {
                    found: version,
                    expected: 1,
                }),
                "extent shape did not retain version refusal"
            );
            canonical::<_, PLANTED>(
                &value,
                encode_extent_page(&value, items, |_| Ok::<_, FsError>(()), allocation_error),
                |bytes| decode_extent_page(bytes, limits, &mut |_| Ok::<_, FsError>(())),
            )
        }
        _ => {
            let value = bindings::decode_binding_page(bytes, limits, observe)?;
            let page = match &value {
                persistent_btree::Page::Leaf(entries) => persistent_btree::PageRef::Leaf(entries),
                persistent_btree::Page::Internal(children) => {
                    persistent_btree::PageRef::Internal(children)
                }
            };
            canonical::<_, PLANTED>(
                &value,
                bindings::encode_binding_page(
                    &page,
                    items,
                    |_| Ok::<_, FsError>(()),
                    allocation_error,
                ),
                |bytes| bindings::decode_binding_page(bytes, limits, &mut |_| Ok::<_, FsError>(())),
            )
        }
    })
}

/// Fuzz bytes, limits, cancellation and injected before-effect decoder failures.
pub fn check(input: &[u8]) {
    let _ = check_inner::<false>(input);
}

fn check_inner<const PLANTED: bool>(input: &[u8]) -> Result<(), FsError> {
    let Some((&kind, input)) = input.split_first() else {
        return Err(CanonicalDecodeError::Truncated.into());
    };
    let Some(controls) = input.get(..5) else {
        return Err(CanonicalDecodeError::Truncated.into());
    };
    let bytes = &input[5..];
    let maximum = u64::from(u16::from_le_bytes([controls[0], controls[1]]));
    let mode = controls[2] % 5;
    let budget = WorkBudget {
        bytes_copied: maximum,
        ..WorkBudget::UNBOUNDED
    };
    let cancellation = CancellationToken::new();
    #[cfg(not(target_arch = "wasm32"))]
    let original = (mode != 0).then(|| {
        crate::NativeMaterializationWork::new(
            if mode == 2 || mode == 4 {
                WorkBudget::UNBOUNDED
            } else {
                budget
            },
            &cancellation,
        )
        .unwrap()
    });
    #[cfg(not(target_arch = "wasm32"))]
    let context = original.as_ref().map(|original| {
        let work = if mode == 2 || mode == 4 {
            original.native_volume_work(budget).unwrap()
        } else {
            original.clone()
        };
        if mode >= 3 {
            work.recheck_admitted()
        } else {
            work
        }
    });
    let mut accounting = match mode {
        #[cfg(not(target_arch = "wasm32"))]
        1..=4 => KernelAccounting::native(context.as_ref().unwrap(), &cancellation),
        _ => KernelAccounting::standalone(budget, &cancellation),
    };
    let baseline = accounting.spent();
    let mut observed = OriginalReadAccounting::new(&mut accounting).unwrap();
    let mut actual = WorkCounters::UNCHARGED;
    let mut pending = (0, 0, 0);
    let mut admissions = 0_u8;
    let limits = DecodeLimits {
        maximum_object_bytes: 4096,
        maximum_page_bytes: 4096,
        maximum_page_items: 32,
        ..DecodeLimits::default()
    };
    let prefix = usize::from(u16::from_le_bytes([controls[3], controls[4]]));
    if kind % KINDS == 2 {
        super::file_table::check_fuzz::<true>(bytes, limits, prefix);
    }
    let decoded = decode::<PLANTED>(kind % KINDS, bytes, limits, prefix, &mut |step| {
        let before = matches!(
            step,
            ReadAllocationStep::BeforeCopy(_)
                | ReadAllocationStep::BeforeHash(_)
                | ReadAllocationStep::BeforeItem
                | ReadAllocationStep::Allocate { .. }
                | ReadAllocationStep::CheckCopy(_)
        );
        if before {
            if controls[3] & 1 != 0 && admissions == controls[4] {
                return Err(CanonicalDecodeError::Truncated.into());
            }
            admissions = admissions.wrapping_add(1);
        }
        // A completion is an actual effect even if its observer returns an
        // error. Track the effect independently of the production receipt.
        match &step {
            ReadAllocationStep::Copied(n) => {
                assert!(*n <= pending.0, "copy without a successful admission");
                pending.0 -= *n;
                actual.bytes_copied += *n;
            }
            ReadAllocationStep::Hashed(n) => {
                assert!(*n <= pending.1, "hash without a successful admission");
                pending.1 -= *n;
                actual.bytes_hashed += *n;
            }
            ReadAllocationStep::Item => {
                assert!(pending.2 > 0, "item without a successful admission");
                pending.2 -= 1;
                actual.items_examined += 1;
            }
            _ => {}
        }
        let reserved = match &step {
            ReadAllocationStep::BeforeCopy(n) => (*n, 0, 0),
            ReadAllocationStep::BeforeHash(n) => (0, *n, 0),
            ReadAllocationStep::BeforeItem => (0, 0, 1),
            _ => (0, 0, 0),
        };
        observed.observe(step)?;
        pending.0 += reserved.0;
        pending.1 += reserved.1;
        pending.2 += reserved.2;
        if before && controls[3] & 2 != 0 && admissions == controls[4] {
            cancellation.cancel();
        }
        Ok(())
    });
    if let Ok(canonical) = &decoded {
        assert_eq!(canonical, bytes, "canonical round trip changes wire bytes");
        assert_eq!(
            actual.bytes_copied,
            (canonical.len() - 8) as u64,
            "all canonical body bytes must have completion callbacks"
        );
    }
    let result = decoded.map(|_| ());
    observed.retire().unwrap();
    let receipt = accounting.spent();
    assert_eq!(
        receipt.bytes_copied - baseline.bytes_copied,
        actual.bytes_copied,
        "missing copy completion callback"
    );
    assert_eq!(
        receipt.bytes_hashed - baseline.bytes_hashed,
        actual.bytes_hashed
    );
    let recheck = cfg!(not(target_arch = "wasm32")) && mode >= 3;
    assert_eq!(
        receipt.items_examined - baseline.items_examined,
        if recheck { 0 } else { actual.items_examined }
    );
    assert!(
        actual.bytes_copied <= maximum,
        "denied copy effect executed"
    );
    result
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use crate::foundation::{Digest, FileId, GenerationId, VolumeId};
    use crate::storage::{ObjectId, ObjectKind};
    use proptest::prelude::*;

    fn fixture(kind: u8, payload: &[u8], number: u64) -> Vec<u8> {
        match kind {
            0 => encode_file_metadata(
                FileMetadata {
                    posix_uid: MetadataField::Value(number as u32),
                    created_ns: MetadataField::Value(number as i64),
                    ..FileMetadata::default()
                },
                |_| Ok::<_, FsError>(()),
                |_| CanonicalDecodeError::AllocationFailed.into(),
            )
            .unwrap(),
            1 => encode_generation_root(
                &GenerationRoot {
                    volume_id: VolumeId::from_bytes([1; 16]),
                    root_file_id: FileId::from_bytes([2; 16]),
                    file_table: ObjectId {
                        storage: None,
                        kind: ObjectKind::FileTablePage,
                        digest: Digest::from_bytes([3; 32]),
                    },
                    parents: (0..number % 3)
                        .map(|index| GenerationId::new(Digest::from_bytes([index as u8; 32])))
                        .collect(),
                    required_features: 0,
                },
                |_| Ok::<_, FsError>(()),
                |_| CanonicalDecodeError::AllocationFailed.into(),
            )
            .unwrap(),
            2 => encode_file_table_page(
                &FileTablePage::Leaf(vec![FileRecord {
                    bindings: None,
                    file_id: FileId::from_bytes([2; 16]),
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata: ObjectId {
                        storage: None,
                        kind: ObjectKind::Metadata,
                        digest: Digest::from_bytes([3; 32]),
                    },
                    payload: FilePayload::InlineRegular(InlineFileData::new(payload).unwrap()),
                }]),
                32,
                |_| Ok::<_, FsError>(()),
                |_| CanonicalDecodeError::AllocationFailed.into(),
            )
            .unwrap(),
            3 => encode_tree_page(
                &TreePage::Leaf(vec![TreeEntry {
                    name: LogicalName::new(
                        NameEncoding::Utf8,
                        format!("n{number:016x}").into_bytes(),
                    )
                    .unwrap(),
                    file_id: FileId::from_bytes([2; 16]),
                    kind: FileKind::Regular,
                }]),
                32,
                |_| Ok::<_, FsError>(()),
                |_| CanonicalDecodeError::AllocationFailed.into(),
            )
            .unwrap(),
            4 => encode_extent_page(
                &ExtentPage::Leaf(vec![Extent {
                    offset: 0,
                    length: number % 65 + 1,
                    kind: ExtentKind::Hole,
                }]),
                32,
                |_| Ok::<_, FsError>(()),
                |_| CanonicalDecodeError::AllocationFailed.into(),
            )
            .unwrap(),
            _ => {
                let binding = |parent: u8, name: String| bindings::Binding {
                    parent: FileId::from_bytes([parent; 16]),
                    name: LogicalName::new(NameEncoding::Utf8, name.into_bytes()).unwrap(),
                };
                let first = binding(2, format!("n{number:016x}"));
                let leaf = [first.clone(), binding(3, format!("p{}", payload.len()))];
                let children = [persistent_btree::Child {
                    first,
                    page: ObjectId {
                        storage: None,
                        kind: ObjectKind::BindingPage,
                        digest: Digest::from_bytes([4; 32]),
                    },
                }];
                let page = if number % 2 == 0 {
                    persistent_btree::PageRef::Leaf(&leaf)
                } else {
                    persistent_btree::PageRef::Internal(&children)
                };
                bindings::encode_binding_page(
                    &page,
                    32,
                    |_| Ok::<_, FsError>(()),
                    |_| CanonicalDecodeError::AllocationFailed.into(),
                )
                .unwrap()
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn arbitrary_lengths_truncation_and_before_effect_errors_are_bounded(
            bytes in prop::collection::vec(any::<u8>(), 0..512),
            controls in any::<[u8; 6]>(),
        ) {
            let mut input = controls.to_vec(); input.extend_from_slice(&bytes);
            check(&input);
        }

        #[test]
        fn canonical_round_trips_keep_exact_completed_work_and_denied_effects(
            kind in 0..KINDS,
            payload in prop::collection::vec(any::<u8>(), 0..65),
            number in any::<u64>(),
            maximum in 0_u16..192,
            fault in any::<u8>(), index in 0_u8..32,
        ) {
            let bytes = fixture(kind, &payload, number);
            for mode in [0,
                #[cfg(not(target_arch = "wasm32"))] 1,
                #[cfg(not(target_arch = "wasm32"))] 2,
                #[cfg(not(target_arch = "wasm32"))] 3,
                #[cfg(not(target_arch = "wasm32"))] 4,
            ] {
                let mut input = vec![kind, 255, 255, mode, 0, 0];
                input.extend_from_slice(&bytes);
                let result = check_inner::<false>(&input);
                prop_assert!(result.is_ok(), "{result:?}");
                input[1..3].copy_from_slice(&maximum.to_le_bytes());
                input[4] = fault; input[5] = index;
                check(&input);
                // Every truncation of a valid object reaches the real decoder,
                // with an independently witnessed exact completion receipt.
                input[1..3].fill(255); input[4] = 0;
                for length in 0..bytes.len() { check(&input[..6 + length]); }
            }
        }
    }

    #[test]
    fn canonical_writer_corpus_reaches_the_shared_differential_oracles() {
        for (index, bytes) in [
            include_bytes!("../../fuzz/corpus/decoder_work/file-table-empty-leaf").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/file-table-plain-internal").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/file-table-sealed-internal").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/file-table-plain-leaf").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/file-table-sealed-leaf").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-empty").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-populated").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-populated-sealed").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-internal-lower-bounds")
                .as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-internal-lower-bounds-sealed")
                .as_slice(),
        ]
        .into_iter()
        .enumerate()
        {
            let result = check_inner::<false>(bytes);
            assert!(
                result.is_ok(),
                "writer-derived seed {index} refused: {result:?}"
            );
        }
        // Preserve old producer bytes as rejected inputs after format changes.
        // Positive seeds above reuse the registered canonical writer captures.
        for bytes in [
            include_bytes!("../../fuzz/corpus/decoder_work/file-table-internal").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/file-table-single-fifo").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/inline-file-table").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/file-table-trailing").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/file-table-unordered").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/truncated-inline-payload").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-unknown-page-kind").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-unknown-extent-kind").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-wrong-blob-kind").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-internal").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-internal-sealed").as_slice(),
        ] {
            assert!(
                check_inner::<false>(bytes).is_err(),
                "malformed seed admitted"
            );
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn the_committed_corpus_replays_and_admits_its_canonical_binding_pages() {
        let corpus = acyclic_test_support::crate_path!("fuzz/corpus/decoder_work");
        for entry in std::fs::read_dir(corpus).unwrap() {
            let path = entry.unwrap().path();
            let input = std::fs::read(&path).unwrap();
            let admitted = check_inner::<false>(&input);
            let name = path.file_name().unwrap().to_string_lossy();
            if matches!(
                name.as_ref(),
                "binding-leaf" | "binding-internal" | "binding-sealed-internal"
            ) {
                assert!(admitted.is_ok(), "{name}: {admitted:?}");
            }
        }
    }

    #[test]
    fn a_planted_encoder_byte_is_detected_for_every_kind() {
        for kind in 0..KINDS {
            for number in [0, 1] {
                let mut input = vec![kind, 255, 255, 0, 0, 0];
                input.extend_from_slice(&fixture(kind, b"a", number));
                assert!(check_inner::<false>(&input).is_ok());
                let panic = std::panic::catch_unwind(|| check_inner::<true>(&input)).unwrap_err();
                let message = panic
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .unwrap_or("");
                assert!(
                    message.contains("a canonical encoding decodes")
                        || message.contains("canonical encoding changes the decoded value"),
                    "kind {kind}: {message}"
                );
            }
        }
    }
}
