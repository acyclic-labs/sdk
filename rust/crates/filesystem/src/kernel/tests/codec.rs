use super::{CanonicalDecodeError, Decoder, Encoder};

#[test]
fn exact_capacity_accepts_only_the_complete_header_boundary() {
    let domain = b"domain";
    let minimum = domain.len() + 2;

    assert_eq!(
        Encoder::with_exact_capacity(domain, 7, minimum).map(Encoder::finish),
        Ok([domain.as_slice(), &7_u16.to_le_bytes()].concat())
    );

    assert!(matches!(
        Encoder::with_exact_capacity(domain, 7, minimum - 1),
        Err(CanonicalDecodeError::LengthOverflow)
    ));
}

#[test]
fn decoder_rejects_version_and_bounded_field_overclaims_before_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let domain = b"domain";
    let mut wrong_version = Encoder::new(domain, 8);
    wrong_version.u8(0);
    assert_eq!(
        Decoder::new(&wrong_version.finish(), domain, 7, u64::MAX).err(),
        Some(CanonicalDecodeError::UnsupportedVersion(8))
    );

    let mut oversized = Encoder::new(domain, 7);
    oversized.u32(5);
    oversized.fixed(b"value");
    let encoded = oversized.finish();
    let mut copying = Decoder::new(&encoded, domain, 7, u64::MAX)?;
    assert_eq!(
        copying.bounded_bytes(4),
        Err(CanonicalDecodeError::FieldTooLarge {
            observed: 5,
            maximum: 4,
        })
    );
    let mut skipping = Decoder::new(&encoded, domain, 7, u64::MAX)?;
    assert_eq!(
        skipping.skip_bounded_bytes(4),
        Err(CanonicalDecodeError::FieldTooLarge {
            observed: 5,
            maximum: 4,
        })
    );
    Ok(())
}

mod properties {
    use super::super::{CanonicalDecodeError, DecodeLimits, Decoder, Encoder};
    use crate::foundation::{Digest, FileId};
    use crate::kernel::{
        Extent, ExtentKind, ExtentPage, FileKind, FileMetadata, LogicalName, MetadataField,
        NameEncoding, TreeEntry, TreePage, decode_extent_page, decode_file_metadata,
        decode_tree_page, encode_extent_page, encode_file_metadata, encode_tree_page,
    };
    use crate::storage::{ObjectId, ObjectKind};
    use proptest::prelude::*;

    const DOMAIN: &[u8] = b"PROPTEST";

    #[derive(Clone, Debug, PartialEq)]
    enum Field {
        U8(u8),
        U32(u32),
        U64(u64),
        I64(i64),
        Bytes(Vec<u8>),
    }

    fn field() -> impl Strategy<Value = Field> {
        prop_oneof![
            any::<u8>().prop_map(Field::U8),
            any::<u32>().prop_map(Field::U32),
            any::<u64>().prop_map(Field::U64),
            any::<i64>().prop_map(Field::I64),
            prop::collection::vec(any::<u8>(), 0..12).prop_map(Field::Bytes),
        ]
    }

    fn encode(fields: &[Field]) -> Result<Vec<u8>, CanonicalDecodeError> {
        let mut encoder = Encoder::new(DOMAIN, 3);
        for field in fields {
            match field {
                Field::U8(value) => encoder.u8(*value),
                Field::U32(value) => encoder.u32(*value),
                Field::U64(value) => encoder.u64(*value),
                Field::I64(value) => encoder.i64(*value),
                Field::Bytes(value) => encoder.bounded_bytes(value)?,
            }
        }
        Ok(encoder.finish())
    }

    /// Decodes `bytes` with the shape of `schema`.
    fn decode(bytes: &[u8], schema: &[Field]) -> Result<Vec<Field>, CanonicalDecodeError> {
        let mut decoder = Decoder::new(bytes, DOMAIN, 3, 256)?;
        let mut fields = Vec::new();
        for field in schema {
            fields.push(match field {
                Field::U8(_) => Field::U8(decoder.u8()?),
                Field::U32(_) => Field::U32(decoder.u32()?),
                Field::U64(_) => Field::U64(decoder.u64()?),
                Field::I64(_) => Field::I64(decoder.i64()?),
                Field::Bytes(_) => Field::Bytes(decoder.bounded_bytes(12)?),
            });
        }
        decoder.finish()?;
        Ok(fields)
    }

    /// Bytes near a valid encoding: one flipped byte, a truncation, then junk.
    type Change = (usize, u8, usize, Vec<u8>);

    fn change() -> impl Strategy<Value = Change> {
        (
            any::<usize>(),
            any::<u8>(),
            0_usize..3,
            prop::collection::vec(any::<u8>(), 0..2),
        )
    }

    fn mutate(bytes: &[u8], (at, flip, cut, junk): &Change) -> Vec<u8> {
        let mut mutated = bytes.to_vec();
        if let Some(byte) = mutated.get_mut(at % bytes.len().max(1)) {
            *byte ^= flip;
        }
        mutated.truncate(mutated.len().saturating_sub(*cut));
        mutated.extend_from_slice(junk);
        mutated
    }

    /// `decode` inverts `encode`, and any bytes near the encoding that
    /// `decode` accepts are canonical: they re-encode to the same bytes.
    fn check_canonical<T: PartialEq + std::fmt::Debug, E: std::fmt::Debug>(
        value: &T,
        encode: impl Fn(&T) -> Result<Vec<u8>, E>,
        decode: impl Fn(&[u8]) -> Result<T, CanonicalDecodeError>,
        change: &Change,
    ) -> Result<(), TestCaseError> {
        let fail = |error: E| TestCaseError::fail(format!("{error:?}"));
        let encoded = encode(value).map_err(fail)?;
        let decoded = decode(&encoded);
        prop_assert_eq!(decoded.as_ref(), Ok(value));
        let mutated = mutate(&encoded, change);
        if let Ok(decoded) = decode(&mutated) {
            prop_assert_eq!(encode(&decoded).map_err(fail)?, mutated);
        }
        Ok(())
    }

    fn tree_entries() -> impl Strategy<Value = Vec<TreeEntry>> {
        let kind =
            prop::sample::select(vec![FileKind::Regular, FileKind::Directory, FileKind::Fifo]);
        prop::collection::btree_map("[a-e]{1,4}", (any::<u8>(), kind), 0..10).prop_map(|map| {
            map.into_iter()
                .filter_map(|(name, (id, kind))| {
                    Some(TreeEntry {
                        name: LogicalName::new(NameEncoding::Utf8, name.into_bytes(), 255).ok()?,
                        file_id: FileId::from_bytes([id; 16]),
                        kind,
                    })
                })
                .collect()
        })
    }

    fn extents() -> impl Strategy<Value = Vec<Extent>> {
        let kind = prop_oneof![
            Just(ExtentKind::Hole),
            Just(ExtentKind::AllocatedZero),
            (any::<u8>(), 0_u64..1_000).prop_map(|(byte, object_offset)| ExtentKind::Content {
                object: ObjectId {
                    kind: ObjectKind::Blob,
                    digest: Digest::from_bytes([byte; 32]),
                },
                object_offset,
            }),
        ];
        let spans = prop::collection::vec((1_u64..100, kind), 0..10);
        (0_u64..1_000, spans).prop_map(|(mut offset, spans)| {
            spans
                .into_iter()
                .map(|(length, kind)| {
                    let extent = Extent {
                        offset,
                        length,
                        kind,
                    };
                    offset += length;
                    extent
                })
                .collect()
        })
    }

    fn field_of<T: Clone + std::fmt::Debug>(
        value: impl Strategy<Value = T>,
    ) -> impl Strategy<Value = MetadataField<T>> {
        prop::option::of(value)
            .prop_map(|value| value.map_or(MetadataField::Unavailable, MetadataField::Value))
    }

    fn object_of(kind: ObjectKind) -> impl Strategy<Value = MetadataField<ObjectId>> {
        field_of(any::<u8>().prop_map(move |byte| ObjectId {
            kind,
            digest: Digest::from_bytes([byte; 32]),
        }))
    }

    fn metadata() -> impl Strategy<Value = FileMetadata> {
        let ids = (
            field_of(any::<u32>()),
            field_of(any::<u32>()),
            field_of(any::<u32>()),
        );
        let flags = (field_of(any::<u64>()), field_of(any::<u32>()));
        let times = prop::array::uniform4(field_of(any::<i64>()));
        let objects = (
            object_of(ObjectKind::AttributePage),
            object_of(ObjectKind::Blob),
            object_of(ObjectKind::Blob),
        );
        (ids, flags, times, objects).prop_map(
            |(
                (posix_mode, posix_uid, posix_gid),
                (posix_flags, windows_attributes),
                [created_ns, modified_ns, accessed_ns, changed_ns],
                (named_attributes, acl, security_descriptor),
            )| FileMetadata {
                posix_mode,
                posix_uid,
                posix_gid,
                posix_flags,
                windows_attributes,
                created_ns,
                modified_ns,
                accessed_ns,
                changed_ns,
                named_attributes,
                acl,
                security_descriptor,
            },
        )
    }

    fn fewer(count: usize) -> Option<u32> {
        u32::try_from(count).ok()?.checked_sub(1)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn primitive_fields_round_trip_and_reject_noncanonical_bytes(
            fields in prop::collection::vec(field(), 0..8),
            change in change(),
        ) {
            check_canonical(&fields, |fields| encode(fields), |bytes| decode(bytes, &fields), &change)?;
        }

        #[test]
        fn decoders_never_panic_on_arbitrary_bytes(
            bytes in prop::collection::vec(any::<u8>(), 0..96),
            schema in prop::collection::vec(field(), 0..8),
        ) {
            let limits = DecodeLimits::default();
            let _ = decode(&bytes, &schema);
            let _ = decode_tree_page(&bytes, limits);
            let _ = decode_extent_page(&bytes, limits);
            let _ = decode_file_metadata(&bytes, limits);
        }

        #[test]
        fn tree_pages_round_trip_canonically_within_limits(
            entries in tree_entries(),
            change in change(),
        ) {
            let limits = DecodeLimits::default();
            let page = TreePage::Leaf(entries.clone());
            let decode = |bytes: &[u8]| decode_tree_page(bytes, limits);
            check_canonical(&page, |page| encode_tree_page(page, 16), decode, &change)?;
            let encoded = encode_tree_page(&page, 16)
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
            if let Some(maximum_page_items) = fewer(entries.len()) {
                let tight = DecodeLimits { maximum_page_items, ..limits };
                prop_assert!(decode_tree_page(&encoded, tight).is_err());
            }
            let longest = entries.iter().map(|entry| entry.name.as_bytes().len()).max();
            if let Some(maximum_name_bytes) = longest.and_then(fewer) {
                let tight = DecodeLimits { maximum_name_bytes, ..limits };
                prop_assert!(decode_tree_page(&encoded, tight).is_err());
            }
        }

        #[test]
        fn extent_pages_round_trip_canonically_within_limits(
            extents in extents(),
            change in change(),
        ) {
            let limits = DecodeLimits::default();
            let page = ExtentPage::Leaf(extents.clone());
            let decode = |bytes: &[u8]| decode_extent_page(bytes, limits);
            check_canonical(&page, |page| encode_extent_page(page, 16), decode, &change)?;
            if let Some(maximum_page_items) = fewer(extents.len()) {
                let encoded = encode_extent_page(&page, 16)
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                let tight = DecodeLimits { maximum_page_items, ..limits };
                prop_assert!(decode_extent_page(&encoded, tight).is_err());
            }
        }

        #[test]
        fn metadata_round_trips_canonically(metadata in metadata(), change in change()) {
            let decode = |bytes: &[u8]| decode_file_metadata(bytes, DecodeLimits::default());
            check_canonical(&metadata, |metadata| encode_file_metadata(*metadata), decode, &change)?;
        }
    }
}
