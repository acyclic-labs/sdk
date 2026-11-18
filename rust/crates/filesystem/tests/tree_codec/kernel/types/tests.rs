use acyclic_fs::kernel::{
    Child, FileKind, LogicalName, NameEncoding, TreeEntry, TreePage, TreePageError,
};
use acyclic_fs_format::{
    identity::{Digest, FileId},
    object_reference::{ObjectId, ObjectKind},
};

#[test]
fn tree_page_validation_rejects_every_malformed_shape() -> Result<(), Box<dyn std::error::Error>> {
    let object = |kind, byte| ObjectId {
        storage: None,
        kind,
        digest: Digest::from_bytes([byte; 32]),
    };
    let a = LogicalName::new(NameEncoding::Utf8, b"a".to_vec())?;
    let b = LogicalName::new(NameEncoding::Utf8, b"b".to_vec())?;
    let entry = |name| TreeEntry {
        name,
        file_id: FileId::from_bytes([1; 16]),
        kind: FileKind::Regular,
    };
    assert_eq!(TreePage::Leaf(Vec::new()).validate(0), Ok(()));
    assert_eq!(
        TreePage::Leaf(vec![entry(a.clone())]).validate(0),
        Err(TreePageError::TooManyItems)
    );
    assert_eq!(
        TreePage::Leaf(vec![entry(b.clone()), entry(a.clone())]).validate(2),
        Err(TreePageError::NamesNotStrictlyOrdered)
    );
    assert_eq!(
        TreePage::Internal(Vec::new()).validate(2),
        Err(TreePageError::EmptyInternalPage)
    );
    assert_eq!(
        TreePage::Internal(vec![Child {
            first: a.clone(),
            page: object(ObjectKind::Blob, 1),
        }])
        .validate(2),
        Err(TreePageError::WrongChildKind)
    );
    assert_eq!(
        TreePage::Internal(vec![
            Child {
                first: b,
                page: object(ObjectKind::TreePage, 1),
            },
            Child {
                first: a,
                page: object(ObjectKind::TreePage, 2),
            },
        ])
        .validate(2),
        Err(TreePageError::NamesNotStrictlyOrdered)
    );
    Ok(())
}
