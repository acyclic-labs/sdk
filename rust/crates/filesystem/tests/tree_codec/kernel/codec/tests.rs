#[path = "../../../support/codec_page_limit.rs"]
mod codec_page_limit;
#[path = "../../../support/codec_properties.rs"]
mod codec_properties;

mod properties {
    use super::codec_page_limit::fewer;
    use super::codec_properties::{change, check_canonical};
    use acyclic_fs::kernel::{
        CanonicalDecodeError, DecodeLimits, FileKind, LogicalName, NameEncoding, TreeEntry,
        TreePage, decode_tree_page, encode_tree_page,
    };
    use acyclic_fs_format::identity::FileId;
    use proptest::prelude::*;

    fn tree_entries() -> impl Strategy<Value = Vec<TreeEntry>> {
        let kind =
            prop::sample::select(vec![FileKind::Regular, FileKind::Directory, FileKind::Fifo]);
        prop::collection::btree_map("[a-e]{1,4}", (any::<u8>(), kind), 0..10).prop_map(|map| {
            map.into_iter()
                .filter_map(|(name, (id, kind))| {
                    Some(TreeEntry {
                        name: LogicalName::new(NameEncoding::Utf8, name.into_bytes()).ok()?,
                        file_id: FileId::from_bytes([id; 16]),
                        kind,
                    })
                })
                .collect()
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn tree_pages_round_trip_canonically_within_limits(
            entries in tree_entries(),
            change in change(),
        ) {
            let limits = DecodeLimits::default();
            let count = entries.len();
            let page = TreePage::Leaf(entries);
            let decode = |bytes: &[u8]| decode_tree_page(bytes, limits, &mut |_| Ok::<_, CanonicalDecodeError>(()));
            let encoded = check_canonical(&page, |page| encode_tree_page(page, 16, |_| Ok::<_, CanonicalDecodeError>(()), |_| CanonicalDecodeError::AllocationFailed), decode, &change)?;

            if let Some(maximum_page_items) = fewer(count) {
                let tight = DecodeLimits { maximum_page_items, ..limits };
                prop_assert!(decode_tree_page(&encoded, tight, &mut |_| Ok::<_, CanonicalDecodeError>(())).is_err(), "page bounds");
            }
        }
    }
}
