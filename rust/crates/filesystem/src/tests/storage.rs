use super::*;

#[test]
fn object_kind_wire_tags_are_stable() {
    let kinds = [
        ObjectKind::Blob,
        ObjectKind::BlobChunk,
        ObjectKind::TreePage,
        ObjectKind::ExtentPage,
        ObjectKind::FileTablePage,
        ObjectKind::GenerationRoot,
        ObjectKind::Metadata,
        ObjectKind::AttributePage,
        ObjectKind::BindingPage,
    ];
    for (index, kind) in kinds.into_iter().enumerate() {
        let tag = u8::try_from(index + 1).unwrap_or(u8::MAX);
        assert_eq!(kind.canonical_tag(), tag);
        assert_eq!(ObjectKind::from_canonical_tag(tag), Ok(kind));
    }
    assert_eq!(
        ObjectKind::from_canonical_tag(0),
        Err(ObjectKindError::Unknown(0))
    );
    assert_eq!(
        ObjectKind::from_canonical_tag(10),
        Err(ObjectKindError::Unknown(10))
    );
}
