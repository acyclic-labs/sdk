use super::*;
use crate::foundation::GenerationId;
use crate::model::Lifecycle;

#[test]
fn retention_round_trip_and_identity_are_canonical() -> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([7; 16]);
    let value = RetentionCreated {
        volume_id,
        kind: RetentionKind::Checkpoint,
        label: "release".to_owned(),
        generation_root: ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: Digest::from_bytes([9; 32]),
        },
        config: VolumeConfig::portable(Lifecycle::Durable),
    };
    let encoded = encode_retention_created(&value)?;
    assert_eq!(decode_retention_created(&encoded, u64::MAX)?, value);
    assert_eq!(
        retention_authority_id(volume_id, RetentionKind::Checkpoint, "release"),
        retention_authority_id(volume_id, RetentionKind::Checkpoint, "release")
    );
    assert_ne!(
        retention_authority_id(volume_id, RetentionKind::Checkpoint, "release"),
        retention_authority_id(volume_id, RetentionKind::Pin, "release")
    );
    assert_ne!(
        retention_authority_id(volume_id, RetentionKind::Pin, "release"),
        retention_authority_id(volume_id, RetentionKind::ForkBase, "release")
    );
    let _ = GenerationId::new(value.generation_root.digest);
    Ok(())
}

#[test]
fn malformed_retention_fails_closed() {
    let value = RetentionCreated {
        volume_id: VolumeId::from_bytes([1; 16]),
        kind: RetentionKind::Checkpoint,
        label: "bad/name".to_owned(),
        generation_root: ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: Digest::from_bytes([2; 32]),
        },
        config: VolumeConfig::portable(Lifecycle::Durable),
    };
    assert!(matches!(
        encode_retention_created(&value),
        Err(RetentionCreatedError::InvalidLabel)
    ));
}

#[test]
fn workspace_deletion_round_trip_is_canonical() -> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([8; 16]);
    let encoded = encode_workspace_deleted(volume_id);
    assert_eq!(decode_workspace_deleted(&encoded, 1024)?, volume_id);
    assert!(decode_workspace_deleted(&encoded, 1).is_err());
    let mut trailing = encoded;
    trailing.push(0);
    assert!(decode_workspace_deleted(&trailing, 1024).is_err());
    Ok(())
}
