use acyclic_fs::kernel::{
    CanonicalDecodeError, DecodeLimits, decode_file_metadata, encode_file_metadata,
    file_metadata_id,
};
use acyclic_fs::object_digest;
use acyclic_fs_format::{
    identity::Digest,
    metadata::{FileMetadata, MetadataField},
    object_reference::{ObjectId, ObjectKind},
};

#[path = "../../../support/portable_metadata.rs"]
mod fixture;
use fixture::metadata;

#[test]
fn unavailable_zero_and_negative_remain_distinct() -> Result<(), Box<dyn std::error::Error>> {
    let value = metadata();
    let encoded = encode_file_metadata(
        value,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    assert_eq!(
        decode_file_metadata(&encoded, DecodeLimits::default(), &mut |_| Ok::<
            _,
            CanonicalDecodeError,
        >(()))?,
        value
    );
    assert_eq!(
        hex::encode(encoded),
        "41435946534d4554010001a481000001e803000001e803000000000001ffffffffffffffff01000000000000000000000101030303030303030303030303030303030303030303030303030303030303030300"
    );
    Ok(())
}

#[test]
fn wrong_reference_kind_fails_before_identity() {
    let mut value = metadata();
    value.named_attributes = MetadataField::Value(ObjectId {
        storage: None,
        kind: ObjectKind::Blob,
        digest: Digest::ZERO,
    });
    assert!(file_metadata_id(value).is_err());
}

#[test]
fn every_metadata_field_round_trips_and_identity_is_exact() -> Result<(), Box<dyn std::error::Error>>
{
    let blob = |byte| ObjectId {
        storage: None,
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([byte; 32]),
    };
    let value = FileMetadata {
        posix_mode: MetadataField::Value(u32::MAX),
        posix_uid: MetadataField::Value(0),
        posix_gid: MetadataField::Value(1),
        posix_flags: MetadataField::Value(u64::MAX),
        windows_attributes: MetadataField::Value(2),
        created_ns: MetadataField::Value(i64::MIN),
        modified_ns: MetadataField::Value(-1),
        accessed_ns: MetadataField::Value(0),
        changed_ns: MetadataField::Value(i64::MAX),
        named_attributes: MetadataField::Value(ObjectId {
            storage: None,
            kind: ObjectKind::AttributePage,
            digest: Digest::from_bytes([4; 32]),
        }),
        acl: MetadataField::Value(blob(5)),
        security_descriptor: MetadataField::Value(blob(6)),
    };
    let encoded = encode_file_metadata(
        value,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    assert_eq!(
        decode_file_metadata(&encoded, DecodeLimits::default(), &mut |_| Ok::<
            _,
            CanonicalDecodeError,
        >(()))?,
        value
    );
    assert_eq!(
        file_metadata_id(value)?,
        ObjectId {
            storage: None,
            kind: ObjectKind::Metadata,
            digest: object_digest(&encoded),
        }
    );
    Ok(())
}

#[test]
fn every_metadata_presence_tag_fails_closed_when_noncanonical()
-> Result<(), Box<dyn std::error::Error>> {
    let encoded = encode_file_metadata(
        FileMetadata {
            posix_mode: MetadataField::Value(1),
            posix_uid: MetadataField::Value(1),
            posix_gid: MetadataField::Value(1),
            posix_flags: MetadataField::Value(1),
            windows_attributes: MetadataField::Value(1),
            created_ns: MetadataField::Value(1),
            modified_ns: MetadataField::Value(1),
            accessed_ns: MetadataField::Value(1),
            changed_ns: MetadataField::Value(1),
            named_attributes: MetadataField::Value(ObjectId {
                storage: None,
                kind: ObjectKind::AttributePage,
                digest: Digest::ZERO,
            }),
            acl: MetadataField::Value(ObjectId {
                storage: None,
                kind: ObjectKind::Blob,
                digest: Digest::ZERO,
            }),
            security_descriptor: MetadataField::Value(ObjectId {
                storage: None,
                kind: ObjectKind::Blob,
                digest: Digest::ZERO,
            }),
        },
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    for (offset, field) in [
        (10, "posix_mode"),
        (15, "posix_uid"),
        (20, "posix_gid"),
        (25, "posix_flags"),
        (34, "windows_attributes"),
        (39, "created_ns"),
        (48, "modified_ns"),
        (57, "accessed_ns"),
        (66, "changed_ns"),
        (75, "named_attributes"),
        (109, "acl"),
        (143, "security_descriptor"),
    ] {
        let mut malformed = encoded.clone();
        *malformed
            .get_mut(offset)
            .ok_or("missing metadata presence tag")? = 2;
        assert!(matches!(
            decode_file_metadata(&malformed, DecodeLimits::default(), &mut |_| Ok::<_, CanonicalDecodeError>(())),
            Err(CanonicalDecodeError::UnknownTag { field: actual, tag: 2 }) if actual == field
        ));
    }
    Ok(())
}
