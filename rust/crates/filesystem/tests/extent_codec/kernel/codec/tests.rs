#[path = "../../../support/codec_association.rs"]
mod codec_association;
#[path = "../../../support/codec_page_limit.rs"]
mod codec_page_limit;
#[path = "../../../support/codec_properties.rs"]
mod codec_properties;

mod properties {
    use super::codec_association::association;
    use super::codec_page_limit::fewer;
    use super::codec_properties::{change, check_canonical};
    use acyclic_fs::kernel::{
        CanonicalDecodeError, DecodeLimits, Extent, ExtentKind, ExtentPage, decode_extent_page,
        encode_extent_page,
    };
    use acyclic_fs_format::{
        identity::Digest,
        object_reference::{ObjectId, ObjectKind},
    };
    use proptest::prelude::*;

    fn extents() -> impl Strategy<Value = Vec<Extent>> {
        let kind = prop_oneof![
            Just(ExtentKind::Hole),
            Just(ExtentKind::AllocatedZero),
            (any::<u8>(), 0_u64..1_000).prop_map(|(byte, object_offset)| ExtentKind::Content {
                object: ObjectId {
                    storage: association(byte),
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

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn extent_pages_round_trip_canonically_within_limits(
            extents in extents(),
            change in change(),
        ) {
            let limits = DecodeLimits::default();
            let count = extents.len();
            let page = ExtentPage::Leaf(extents);
            let decode = |bytes: &[u8]| decode_extent_page(bytes, limits, &mut |_| Ok::<_, CanonicalDecodeError>(()));
            let encoded = check_canonical(&page, |page| encode_extent_page(page, 16, |_| Ok::<_, CanonicalDecodeError>(()), |_| CanonicalDecodeError::AllocationFailed), decode, &change)?;
            if let Some(maximum_page_items) = fewer(count) {

                let tight = DecodeLimits { maximum_page_items, ..limits };
                prop_assert!(decode_extent_page(&encoded, tight, &mut |_| Ok::<_, CanonicalDecodeError>(())).is_err());
            }
        }
    }
}
