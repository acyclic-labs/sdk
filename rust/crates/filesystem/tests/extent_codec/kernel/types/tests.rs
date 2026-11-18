use acyclic_fs::kernel::{Extent, ExtentChild, ExtentKind, ExtentPage, ExtentPageError};
use acyclic_fs_format::{
    identity::Digest,
    object_reference::{ObjectId, ObjectKind},
};

#[test]
fn extent_page_validation_rejects_every_malformed_shape() {
    let page = |kind: ObjectKind| ObjectId {
        storage: None,
        kind,
        digest: Digest::from_bytes([1; 32]),
    };
    let extent = |offset, length, kind| Extent {
        offset,
        length,
        kind,
    };
    let cases = [
        (
            ExtentPage::Leaf(vec![extent(0, 0, ExtentKind::Hole)]),
            ExtentPageError::ZeroLength,
        ),
        (
            ExtentPage::Leaf(vec![extent(u64::MAX, 2, ExtentKind::Hole)]),
            ExtentPageError::RangeOverflow,
        ),
        (
            ExtentPage::Leaf(vec![
                extent(0, 1, ExtentKind::Hole),
                extent(2, 1, ExtentKind::AllocatedZero),
            ]),
            ExtentPageError::NonContiguous,
        ),
        (
            ExtentPage::Leaf(vec![extent(
                0,
                1,
                ExtentKind::Content {
                    object: page(ObjectKind::Metadata),
                    object_offset: 0,
                },
            )]),
            ExtentPageError::WrongContentKind,
        ),
        (
            ExtentPage::Leaf(vec![extent(
                0,
                2,
                ExtentKind::Content {
                    object: page(ObjectKind::Blob),
                    object_offset: u64::MAX,
                },
            )]),
            ExtentPageError::RangeOverflow,
        ),
        (
            ExtentPage::Internal(Vec::new()),
            ExtentPageError::EmptyInternalPage,
        ),
        (
            ExtentPage::Internal(vec![
                ExtentChild {
                    first: 1,
                    page: page(ObjectKind::ExtentPage),
                },
                ExtentChild {
                    first: 1,
                    page: page(ObjectKind::ExtentPage),
                },
            ]),
            ExtentPageError::InvalidChildRange,
        ),
        (
            ExtentPage::Internal(vec![ExtentChild {
                first: 0,
                page: page(ObjectKind::Blob),
            }]),
            ExtentPageError::WrongChildKind,
        ),
    ];
    for (page, error) in cases {
        assert_eq!(page.validate(8), Err(error));
    }
    assert_eq!(
        ExtentPage::Leaf(vec![extent(0, 1, ExtentKind::Hole)]).validate(0),
        Err(ExtentPageError::TooManyItems)
    );
}
