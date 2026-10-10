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

#[test]
fn measured_retention_adopts_label_and_preserves_refused_prefix() -> Result<(), Box<dyn std::error::Error>> {
    let value = RetentionCreated {
        volume_id: VolumeId::from_bytes([21; 16]), kind: RetentionKind::Pin, label: "owned-pin".to_owned(),
        generation_root: ObjectId { kind: ObjectKind::GenerationRoot, digest: Digest::from_bytes([22; 32]) },
        config: VolumeConfig::portable(Lifecycle::Durable),
    };
    let cancellation = CancellationToken::new();
    let live = value.label.capacity() as u64;
    let canonical = encode_retention_created(&value)?;
    let mut legacy = Encoder::new(DOMAIN, VERSION);
    legacy.fixed(&value.volume_id.into_bytes());
    legacy.u8(value.kind.tag());
    legacy.bounded_bytes(value.label.as_bytes())?;
    legacy.u8(value.generation_root.kind.canonical_tag());
    legacy.fixed(value.generation_root.digest.as_bytes());
    encode_config(&mut legacy, value.config);
    assert_eq!(canonical, legacy.finish());
    let receipt = encode_retention_created_measured(&value, WorkBudget::UNBOUNDED, &cancellation, live)?;
    assert_eq!(receipt.value, canonical);
    assert_eq!(receipt.work.bytes_encoded, canonical.len() as u64);
    assert_eq!(receipt.work.bytes_copied, 48 + value.label.len() as u64);
    assert_eq!(receipt.work.allocation_operations, 1);
    assert_eq!(receipt.work.peak_allocation_bytes, live + receipt.value.capacity() as u64);
    for budget in [
        WorkBudget { bytes_encoded: 0, ..WorkBudget::UNBOUNDED },
        WorkBudget { bytes_copied: 0, ..WorkBudget::UNBOUNDED },
        WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED },
        WorkBudget { peak_allocation_bytes: live, ..WorkBudget::UNBOUNDED },
    ] {
        let failure = encode_retention_created_measured(&value, budget, &cancellation, live).expect_err("finite permit refused");
        assert!(matches!(failure.error, RetentionCreatedError::Work(_)));
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(failure.work.bytes_encoded, 0);
        assert_eq!(failure.work.bytes_copied, 0);
        assert_eq!(failure.work.peak_allocation_bytes, live);
    }
    cancellation.cancel();
    let failure = encode_retention_created_measured(&value, WorkBudget::UNBOUNDED, &cancellation, live).expect_err("cancelled");
    assert!(matches!(failure.error, RetentionCreatedError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}
