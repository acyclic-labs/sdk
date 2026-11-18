use acyclic_fs::kernel::{
    CanonicalDecodeError, RetentionCreated, RetentionCreatedError, RetentionKind,
    decode_retention_created, decode_workspace_deleted, encode_retention_created,
    encode_workspace_deleted, retention_authority_id,
};
use acyclic_fs::{Lifecycle, VolumeConfig};
use acyclic_fs_format::{
    identity::{Digest, VolumeId},
    object_reference::{ObjectId, ObjectKind},
};

const VERSION_OFFSET: usize = b"acyclic-fs-retention-created-v1\0".len();

#[test]
fn workspace_deletion_round_trip_is_canonical() -> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([8; 16]);
    let encoded = encode_workspace_deleted(
        volume_id,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    assert_eq!(decode_workspace_deleted(&encoded, 1024)?, volume_id);
    assert!(decode_workspace_deleted(&encoded, 1).is_err());
    let mut trailing = encoded;
    trailing.push(0);
    assert!(decode_workspace_deleted(&trailing, 1024).is_err());
    Ok(())
}

#[test]
fn retention_round_trip_and_identity_are_canonical() -> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([7; 16]);
    let value = RetentionCreated {
        volume_id,
        kind: RetentionKind::Checkpoint,
        label: "release".to_owned(),
        generation_root: ObjectId {
            storage: None,
            kind: ObjectKind::GenerationRoot,
            digest: Digest::from_bytes([9; 32]),
        },
        config: VolumeConfig::portable(Lifecycle::Durable),
    };
    let encoded = encode_retention_created(
        &value,
        |_| Ok::<_, RetentionCreatedError>(()),
        |_| RetentionCreatedError::Decode(CanonicalDecodeError::AllocationFailed),
    )?;
    assert_eq!(decode_retention_created(&encoded, u64::MAX)?, value);
    assert_ne!(
        retention_authority_id(volume_id, RetentionKind::Checkpoint, "release"),
        retention_authority_id(volume_id, RetentionKind::Pin, "release")
    );
    assert_ne!(
        retention_authority_id(volume_id, RetentionKind::Pin, "release"),
        retention_authority_id(volume_id, RetentionKind::ForkBase, "release")
    );
    // Old/unknown headers refuse before the removed configuration fields.
    for version in [0_u16, 1, u16::MAX] {
        let mut obsolete = encoded
            .get(..VERSION_OFFSET + size_of::<u16>())
            .ok_or("missing retention version")?
            .to_vec();
        obsolete
            .get_mut(VERSION_OFFSET..)
            .ok_or("missing retention version")?
            .copy_from_slice(&version.to_le_bytes());
        assert!(matches!(
            decode_retention_created(&obsolete, u64::MAX),
            Err(RetentionCreatedError::Decode(CanonicalDecodeError::UnsupportedVersion {
                found, expected: 2
            })) if found == version
        ));
    }
    Ok(())
}

#[test]
fn malformed_retention_fails_closed() {
    let value = RetentionCreated {
        volume_id: VolumeId::from_bytes([1; 16]),
        kind: RetentionKind::Checkpoint,
        label: "bad/name".to_owned(),
        generation_root: ObjectId {
            storage: None,
            kind: ObjectKind::GenerationRoot,
            digest: Digest::from_bytes([2; 32]),
        },
        config: VolumeConfig::portable(Lifecycle::Durable),
    };
    assert!(matches!(
        encode_retention_created(
            &value,
            |_| Ok::<_, RetentionCreatedError>(()),
            |_| RetentionCreatedError::Decode(CanonicalDecodeError::AllocationFailed)
        ),
        Err(RetentionCreatedError::InvalidLabel)
    ));
}
