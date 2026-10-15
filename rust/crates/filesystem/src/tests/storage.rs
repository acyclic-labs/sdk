use super::*;
use acyclic_stream::OwnedBytes;

#[test]
fn object_kinds_digests_and_read_views_are_canonical() {
    let kinds = [
        ObjectKind::Blob,
        ObjectKind::BlobChunk,
        ObjectKind::TreePage,
        ObjectKind::ExtentPage,
        ObjectKind::FileTablePage,
        ObjectKind::GenerationRoot,
        ObjectKind::Metadata,
        ObjectKind::AttributePage,
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
        ObjectKind::from_canonical_tag(9),
        Err(ObjectKindError::Unknown(9))
    );
    assert_ne!(
        object_digest(ObjectKind::Blob, b"bytes"),
        object_digest(ObjectKind::BlobChunk, b"bytes")
    );
    assert_ne!(
        object_digest(ObjectKind::Blob, b"bytes"),
        object_digest(ObjectKind::Blob, b"other")
    );
    let read = ObjectRead {
        bytes: OwnedBytes::from_static(b"view"),
        retention: ObjectReadRetention::Owned { logical_bytes: 4 },
    };
    assert_eq!(&*read, b"view");
    assert_ne!(read.retention, ObjectReadRetention::Shared);
}
