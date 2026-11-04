use super::*;
use proptest::prelude::*;

use super::fuzz::{Failure, compare, observed};

fn record(key: u8) -> FileRecord {
    let object = |kind| digest_object(kind, [key; 32]);
    let (kind, payload) = match key % 7 {
        0 => (
            FileKind::Regular,
            FilePayload::Regular {
                logical_bytes: u64::from(key) + 1,
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
        bindings: (key % 2 != 0).then(|| object(ObjectKind::BindingPage)),
        file_id: FileId::from_bytes([key; 16]),
        kind,
        link_count: u64::from(key) + 1,
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
        compare::<true>(&bytes, limits).map_err(proptest::test_runner::TestCaseError::fail)?;
        compare::<true>(&raw, limits).map_err(proptest::test_runner::TestCaseError::fail)?;
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
    let rejected = std::panic::catch_unwind(|| super::fuzz::check::<false>(&bytes, limits, 0));
    let message = rejected.unwrap_err();
    let message = message.downcast_ref::<String>().unwrap();
    assert!(message.contains("decoder result differs"), "{message}");
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
    let header_bytes = encode(&FileTablePage::Leaf(Vec::new())).len();
    let kind_offset = header_bytes + 16;
    let bindings_offset = kind_offset + 1 + 8 + record(6).metadata.encoded_len();
    let bytes = encode(&FileTablePage::Leaf(vec![record(6), record(13)]));
    let mut cases = Vec::new();
    // Version, page, kind, bindings and payload tags, plus mismatched kind.
    for (offset, value) in [
        (DOMAIN.len(), 255),
        (DOMAIN.len() + 2, 255),
        (kind_offset, 255),
        (bindings_offset, 255),
        (bindings_offset + 1, 255),
        (kind_offset, 1),
    ] {
        let mut malformed = bytes.clone();
        malformed[offset] = value;
        cases.push(malformed);
    }
    let mut no_links = bytes.clone();
    no_links[kind_offset + 1..kind_offset + 9].fill(0);
    cases.push(no_links);
    let mut excessive = bytes.clone();
    excessive[DOMAIN.len() + 3..header_bytes].copy_from_slice(&u32::MAX.to_le_bytes());
    cases.push(excessive);
    let mut duplicate = bytes.clone();
    let second_record = encode(&FileTablePage::Leaf(vec![record(6)])).len();
    duplicate[second_record..second_record + 16]
        .copy_from_slice(&bytes[header_bytes..header_bytes + 16]);
    assert_eq!(
        observed::<true, false>(&duplicate, DecodeLimits::default(), usize::MAX).0,
        Err(Failure::Decode(invariant(
            FileTableError::NotStrictlyOrdered
        )))
    );
    cases.push(duplicate);
    let mut empty_internal = bytes[..header_bytes].to_vec();
    empty_internal[DOMAIN.len() + 2] = 2;
    empty_internal[DOMAIN.len() + 3..header_bytes].fill(0);
    cases.push(empty_internal);
    let inline_record = record(5);
    let inline = encode(&FileTablePage::Leaf(vec![inline_record]));
    let mut large_inline = inline.clone();
    let inline_length = usize::try_from(inline_record.logical_file_bytes()).unwrap();
    let length_offset = inline.len() - inline_length - 1;
    large_inline[length_offset] = 65;
    assert_eq!(
        observed::<true, false>(&large_inline, DecodeLimits::default(), usize::MAX).0,
        Err(Failure::Decode(CanonicalDecodeError::FieldTooLarge {
            observed: 65,
            maximum: u32::try_from(MAXIMUM_INLINE_FILE_BYTES).unwrap(),
        }))
    );
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
    let second_child = encode(&FileTablePage::Internal(vec![Child {
        first: FileId::from_bytes([1; 16]),
        page: digest_object(ObjectKind::FileTablePage, [1; 32]),
    }]))
    .len();
    unordered_internal[second_child..second_child + 16]
        .copy_from_slice(&internal[header_bytes..header_bytes + 16]);
    assert_eq!(
        observed::<true, false>(&unordered_internal, DecodeLimits::default(), usize::MAX).0,
        Err(Failure::Decode(invariant(
            FileTableError::InvalidInternalPage
        )))
    );
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
