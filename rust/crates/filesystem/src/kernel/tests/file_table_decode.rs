use super::*;
use proptest::prelude::*;

mod reference;

#[derive(Clone, Debug, PartialEq)]
enum Failure {
    Decode(CanonicalDecodeError),
    Denied,
}

impl From<CanonicalDecodeError> for Failure {
    fn from(error: CanonicalDecodeError) -> Self {
        Self::Decode(error)
    }
}

fn event(step: ReadAllocationStep) -> (u8, u64, u64) {
    match step {
        ReadAllocationStep::Allocate { bytes, operations } => (0, bytes, operations),
        ReadAllocationStep::ReserveFailed(_) => (1, 0, 0),
        ReadAllocationStep::Capacity {
            admitted_bytes,
            actual_bytes,
        } => (2, admitted_bytes, actual_bytes),
        ReadAllocationStep::CheckCopy(bytes) => (9, bytes, 0),
        ReadAllocationStep::BeforeCopy(bytes) => (3, bytes, 0),
        ReadAllocationStep::Copied(bytes) => (4, bytes, 0),
        ReadAllocationStep::BeforeHash(bytes) => (5, bytes, 0),
        ReadAllocationStep::Hashed(bytes) => (6, bytes, 0),
        ReadAllocationStep::BeforeItem => (7, 0, 0),
        ReadAllocationStep::Item => (8, 0, 0),
    }
}

fn observed<const ORDER: bool, const LEGACY: bool>(
    bytes: &[u8],
    limits: DecodeLimits,
    deny_at: usize,
) -> (Result<FileTablePage, Failure>, Vec<(u8, u64, u64)>) {
    let mut events = Vec::new();
    let mut observe = |step| {
        events.push(event(step));
        if events.len() > deny_at {
            Err(Failure::Denied)
        } else {
            Ok(())
        }
    };
    let result = if LEGACY {
        reference::decode(bytes, limits, &mut observe)
    } else if ORDER {
        decode_file_table_page(bytes, limits, &mut observe)
    } else {
        decode_page::<false, Failure>(bytes, limits, &mut observe)
    };
    (result, events)
}

fn compare<const ORDER: bool>(
    bytes: &[u8],
    limits: DecodeLimits,
) -> proptest::test_runner::TestCaseResult {
    // Independently derive every denied prefix from the complete legacy trace:
    // denial at event k must return Denied with exactly events [0..=k].
    let (complete, tape) = observed::<false, true>(bytes, limits, usize::MAX);
    for denied in 0..=tape.len() {
        let (actual, events) = observed::<ORDER, false>(bytes, limits, denied);
        let expected = if denied < tape.len() {
            Err(Failure::Denied)
        } else {
            complete.clone()
        };
        prop_assert_eq!(actual, expected);
        prop_assert_eq!(events.as_slice(), &tape[..(denied + 1).min(tape.len())]);
    }
    Ok(())
}

fn record(key: u8) -> FileRecord {
    let object = |kind| digest_object(kind, [key; 32]);
    let (kind, payload) = match key % 7 {
        0 => (
            FileKind::Regular,
            FilePayload::Regular {
                logical_bytes: 1,
                extents: object(ObjectKind::ExtentPage),
            },
        ),
        1 => (
            FileKind::Directory,
            FilePayload::Directory {
                entries: object(ObjectKind::TreePage),
            },
        ),
        2 => (
            FileKind::SymbolicLink,
            FilePayload::SymbolicLink {
                target_bytes: 1,
                target: object(ObjectKind::Blob),
            },
        ),
        3 => (
            FileKind::ReparsePoint,
            FilePayload::ReparsePoint {
                payload_bytes: 1,
                payload: object(ObjectKind::Blob),
            },
        ),
        4 => (
            FileKind::CharacterDevice,
            FilePayload::Device { major: 1, minor: 2 },
        ),
        5 => (
            FileKind::Regular,
            FilePayload::InlineRegular(InlineFileData::new(&[key; 16]).unwrap()),
        ),
        _ => (FileKind::Fifo, FilePayload::Empty),
    };
    FileRecord {
        file_id: FileId::from_bytes([key; 16]),
        kind,
        link_count: 1,
        metadata: object(ObjectKind::Metadata),
        payload,
    }
}

fn encode(page: &FileTablePage) -> Vec<u8> {
    encode_file_table_page(
        page,
        64,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )
    .unwrap()
}

fn pages() -> impl Strategy<Value = FileTablePage> {
    (
        prop::collection::btree_set(any::<u8>(), 0..33),
        any::<bool>(),
    )
        .prop_map(|(keys, leaf)| {
            if leaf || keys.is_empty() {
                FileTablePage::Leaf(keys.into_iter().map(record).collect())
            } else {
                FileTablePage::Internal(
                    keys.into_iter()
                        .map(|key| Child {
                            first: FileId::from_bytes([key; 16]),
                            page: digest_object(ObjectKind::FileTablePage, [key; 32]),
                        })
                        .collect(),
                )
            }
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn pages_round_trip_canonically(page in pages()) {
        let bytes = encode(&page);
        let decoded = decode_file_table_page(&bytes, DecodeLimits::default(),
                                             &mut |_| Ok::<_, CanonicalDecodeError>(()))
            .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(&decoded, &page);
        for mode in 0..5 {
            let mut input = vec![2, 255, 255, mode, 0, 0];
            input.extend_from_slice(&bytes);
            crate::kernel::decoder_fuzz::check(&input);
            input[1..3].fill(0);
            crate::kernel::decoder_fuzz::check(&input);
        }
        prop_assert_eq!(encode(&decoded), bytes);
    }

    #[test]
    fn fused_validation_preserves_results_and_all_admission_events(
        page in pages(), raw in prop::collection::vec(any::<u8>(), 0..512),
        mutations in prop::collection::vec((any::<usize>(), any::<u8>()), 0..8),
        maximum in 0_u32..65,
    ) {
        let mut bytes = encode(&page);
        for (index, value) in mutations {
            let length = bytes.len();
            bytes[index % length] = value;
        }
        let limits = DecodeLimits { maximum_page_items: maximum, ..DecodeLimits::default() };
        compare::<true>(&bytes, limits)?;
        compare::<true>(&raw, limits)?;
    }
}

#[test]
fn oracle_detects_missing_order_check_and_keeps_error_precedence() {
    let mut records = vec![record(1), record(2), record(3)];
    records.swap(0, 1);
    // Build an unordered wire page without weakening the public encoder.
    let mut encoder = Encoder::new(DOMAIN, VERSION);
    encoder.u8(1);
    encoder.u32(3);
    for record in records {
        encode_record(&mut encoder, record);
    }
    let bytes = encoder.finish();
    let limits = DecodeLimits::default();
    compare::<true>(&bytes, limits).unwrap();
    assert!(compare::<false>(&bytes, limits).is_err());
    for length in 0..bytes.len() {
        compare::<true>(&bytes[..length], limits).unwrap();
    }
    let mut invalid_record = record(3);
    invalid_record.link_count = 0;
    let mut malformed = Encoder::new(DOMAIN, VERSION);
    malformed.u8(1);
    malformed.u32(3);
    for record in [record(2), record(1), invalid_record] {
        encode_record(&mut malformed, record);
    }
    let malformed = malformed.finish();
    compare::<true>(&malformed, limits).unwrap();
    assert!(matches!(
        observed::<true, false>(&malformed, limits, usize::MAX).0,
        Err(Failure::Decode(CanonicalDecodeError::Invariant(_)))
    ));
    let mut trailing = bytes.clone();
    trailing.push(0);
    compare::<true>(&trailing, limits).unwrap();
    assert!(matches!(
        observed::<true, false>(&trailing, limits, usize::MAX).0,
        Err(Failure::Decode(CanonicalDecodeError::TrailingBytes))
    ));
}

#[test]
fn independent_reference_matches_crafted_malformed_pages_and_every_denied_prefix() {
    let bytes = encode(&FileTablePage::Leaf(vec![record(6), record(13)]));
    let mut cases = Vec::new();
    // Version, page tag, leaf kind, zero links and payload tag failures.
    for (offset, value) in [(8, 255), (10, 255), (31, 255), (72, 255), (31, 1)] {
        let mut malformed = bytes.clone();
        malformed[offset] = value;
        cases.push(malformed);
    }
    let mut no_links = bytes.clone();
    no_links[32..40].fill(0);
    cases.push(no_links);
    let mut excessive = bytes.clone();
    excessive[11..15].copy_from_slice(&u32::MAX.to_le_bytes());
    cases.push(excessive);
    let mut duplicate = bytes.clone();
    duplicate[73..89].copy_from_slice(&bytes[15..31]);
    cases.push(duplicate);
    let mut empty_internal = bytes[..15].to_vec();
    empty_internal[10] = 2;
    empty_internal[11..15].fill(0);
    cases.push(empty_internal);
    let inline = encode(&FileTablePage::Leaf(vec![record(5)]));
    let mut large_inline = inline.clone();
    large_inline[73] = 65;
    cases.push(large_inline);
    let internal = encode(&FileTablePage::Internal(vec![
        Child {
            first: FileId::from_bytes([1; 16]),
            page: digest_object(ObjectKind::FileTablePage, [1; 32]),
        },
        Child {
            first: FileId::from_bytes([2; 16]),
            page: digest_object(ObjectKind::FileTablePage, [2; 32]),
        },
    ]));
    let mut unordered_internal = internal.clone();
    unordered_internal[63..79].copy_from_slice(&internal[15..31]);
    cases.push(unordered_internal);
    cases.extend([bytes, inline, internal]);
    for bytes in cases {
        for length in 0..=bytes.len() {
            compare::<true>(&bytes[..length], DecodeLimits::default()).unwrap();
        }
        let mut trailing = bytes;
        trailing.push(0);
        compare::<true>(&trailing, DecodeLimits::default()).unwrap();
    }
}
