use super::*;
use crate::foundation::Digest;

fn metadata() -> FileMetadata {
    FileMetadata {
        posix_mode: MetadataField::Value(0o100_644),
        posix_uid: MetadataField::Value(1000),
        posix_gid: MetadataField::Value(1000),
        posix_flags: MetadataField::Unavailable,
        windows_attributes: MetadataField::Unavailable,
        created_ns: MetadataField::Unavailable,
        modified_ns: MetadataField::Value(-1),
        accessed_ns: MetadataField::Value(0),
        changed_ns: MetadataField::Unavailable,
        named_attributes: MetadataField::Unavailable,
        acl: MetadataField::Value(ObjectId {
            kind: ObjectKind::Blob,
            digest: Digest::from_bytes([3; 32]),
        }),
        security_descriptor: MetadataField::Unavailable,
    }
}

#[test]
fn unavailable_zero_and_negative_remain_distinct() -> Result<(), Box<dyn std::error::Error>> {
    let value = metadata();
    let encoded = encode_file_metadata(value)?;
    assert_eq!(
        decode_file_metadata(&encoded, DecodeLimits::default())?,
        value
    );
    assert_eq!(
        hex::encode(encoded),
        "41435946534d4554010001a481000001e803000001e803000000000001ffffffffffffffff010000000000000000000001030303030303030303030303030303030303030303030303030303030303030300"
    );
    Ok(())
}

#[test]
fn wrong_reference_kind_fails_before_identity() {
    let mut value = metadata();
    value.named_attributes = MetadataField::Value(ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::ZERO,
    });
    assert!(file_metadata_id(value).is_err());
}

#[test]
fn every_metadata_field_round_trips_and_identity_is_exact() -> Result<(), Box<dyn std::error::Error>>
{
    let blob = |byte| ObjectId {
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
            kind: ObjectKind::AttributePage,
            digest: Digest::from_bytes([4; 32]),
        }),
        acl: MetadataField::Value(blob(5)),
        security_descriptor: MetadataField::Value(blob(6)),
    };
    let encoded = encode_file_metadata(value)?;
    assert_eq!(
        decode_file_metadata(&encoded, DecodeLimits::default())?,
        value
    );
    assert_eq!(
        file_metadata_id(value)?,
        ObjectId {
            kind: ObjectKind::Metadata,
            digest: object_digest(ObjectKind::Metadata, &encoded),
        }
    );
    Ok(())
}

#[test]
fn every_metadata_presence_tag_fails_closed_when_noncanonical()
-> Result<(), Box<dyn std::error::Error>> {
    let encoded = encode_file_metadata(FileMetadata {
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
            kind: ObjectKind::AttributePage,
            digest: Digest::ZERO,
        }),
        acl: MetadataField::Value(ObjectId {
            kind: ObjectKind::Blob,
            digest: Digest::ZERO,
        }),
        security_descriptor: MetadataField::Value(ObjectId {
            kind: ObjectKind::Blob,
            digest: Digest::ZERO,
        }),
    })?;
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
        (108, "acl"),
        (141, "security_descriptor"),
    ] {
        let mut malformed = encoded.clone();
        malformed[offset] = 2;
        assert!(matches!(
            decode_file_metadata(&malformed, DecodeLimits::default()),
            Err(CanonicalDecodeError::UnknownTag { field: actual, tag: 2 }) if actual == field
        ));
    }
    Ok(())
}
