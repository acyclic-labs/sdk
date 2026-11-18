use acyclic_fs::kernel::{
    AttributeClass, AttributeEntry, AttributeName, AttributePage, CanonicalDecodeError,
    DecodeLimits, decode_attribute_page, encode_attribute_page,
};
use acyclic_fs_format::{
    identity::Digest,
    object_reference::{ObjectId, ObjectKind},
};

#[test]
fn classes_and_raw_names_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let page = AttributePage::Leaf(vec![AttributeEntry {
        name: AttributeName::new(AttributeClass::PosixXattr, b"user.test".to_vec())?,
        value_bytes: 4,
        value: ObjectId {
            storage: None,
            kind: ObjectKind::Blob,
            digest: Digest::from_bytes([9; 32]),
        },
    }]);
    let bytes = encode_attribute_page(
        &page,
        8,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    assert_eq!(
        decode_attribute_page(&bytes, DecodeLimits::default(), &mut |_| Ok::<
            _,
            CanonicalDecodeError,
        >(()))?,
        page
    );
    Ok(())
}
