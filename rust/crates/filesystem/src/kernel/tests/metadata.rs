use super::*;
use crate::foundation::Digest;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn derived_stamp_updates_allow_equal_ticks_and_preserve_recorded_facts(
        accessed in any::<i64>(),
        changed in any::<i64>(),
        advance in 1_i64..i64::MAX,
    ) {
        let prior = FileMetadata {
            accessed_ns: MetadataField::Value(accessed),
            changed_ns: MetadataField::Value(changed),
            ..metadata()
        };
        prop_assert!(prior.only_advanced_derived_stamps(&prior));
        // In particular, ctime need not advance when atime does.
        let advanced = FileMetadata {
            accessed_ns: MetadataField::Value(accessed.saturating_add(advance)),
            ..prior
        };
        prop_assert!(advanced.only_advanced_derived_stamps(&prior));
        let advanced = FileMetadata {
            changed_ns: MetadataField::Value(changed.saturating_add(advance)),
            ..advanced
        };
        prop_assert!(advanced.only_advanced_derived_stamps(&prior));
        let recorded_change = FileMetadata {
            modified_ns: MetadataField::Unavailable,
            ..advanced
        };
        prop_assert!(!recorded_change.only_advanced_derived_stamps(&prior));
        for access_time in [false, true] {
            let mut unavailable = prior;
            let before = if access_time {
                unavailable.accessed_ns = MetadataField::Unavailable;
                accessed
            } else {
                unavailable.changed_ns = MetadataField::Unavailable;
                changed
            };
            prop_assert!(unavailable.only_advanced_derived_stamps(&unavailable));
            prop_assert!(!unavailable.only_advanced_derived_stamps(&prior));
            prop_assert!(!prior.only_advanced_derived_stamps(&unavailable));
            if let Some(rewound) = before.checked_sub(1) {
                let mut rewound_record = prior;
                if access_time {
                    rewound_record.accessed_ns = MetadataField::Value(rewound);
                } else {
                    rewound_record.changed_ns = MetadataField::Value(rewound);
                }
                prop_assert!(!rewound_record.only_advanced_derived_stamps(&prior));
            }
        }
    }
}

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
            storage: None,
            kind: ObjectKind::Blob,
            digest: Digest::from_bytes([3; 32]),
        }),
        security_descriptor: MetadataField::Unavailable,
    }
}

#[test]
fn unavailable_zero_and_negative_remain_distinct() -> Result<(), Box<dyn std::error::Error>> {
    let value = metadata();
    let encoded = encode_file_metadata(
        value,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    assert_eq!(
        decode_file_metadata(&encoded, DecodeLimits::default(), &mut |_| Ok::<
            _,
            crate::kernel::CanonicalDecodeError,
        >(()))?,
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
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    assert_eq!(
        decode_file_metadata(&encoded, DecodeLimits::default(), &mut |_| Ok::<
            _,
            crate::kernel::CanonicalDecodeError,
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
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
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
        (108, "acl"),
        (141, "security_descriptor"),
    ] {
        let mut malformed = encoded.clone();
        malformed[offset] = 2;
        assert!(matches!(
            decode_file_metadata(&malformed, DecodeLimits::default(), &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(())),
            Err(CanonicalDecodeError::UnknownTag { field: actual, tag: 2 }) if actual == field
        ));
    }
    Ok(())
}
