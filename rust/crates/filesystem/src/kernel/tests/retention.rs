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
fn measured_workspace_deletion_preserves_exact_schema_and_denied_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([0xe1; 16]);
    let cancellation = CancellationToken::new();
    let canonical = encode_workspace_deleted(volume_id);
    let receipt = encode_workspace_deleted_measured(volume_id, WorkBudget::UNBOUNDED, &cancellation)?;
    assert_eq!(receipt.value, canonical);
    assert_eq!(receipt.work.bytes_encoded, canonical.len() as u64);
    assert_eq!(receipt.work.bytes_copied, 16);
    assert_eq!(receipt.work.allocation_operations, 1);
    assert_eq!(receipt.work.peak_allocation_bytes, receipt.value.capacity() as u64);
    for budget in [
        WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED },
        WorkBudget { peak_allocation_bytes: 0, ..WorkBudget::UNBOUNDED },
        WorkBudget { bytes_encoded: 0, ..WorkBudget::UNBOUNDED },
        WorkBudget { bytes_copied: 0, ..WorkBudget::UNBOUNDED },
    ] {
        let failure = encode_workspace_deleted_measured(volume_id, budget, &cancellation)
            .expect_err("bounded deletion producer rejects before allocating");
        assert!(matches!(failure.error, RetentionCreatedError::Work(_)));
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(failure.work.bytes_encoded, 0);
        assert_eq!(failure.work.bytes_copied, 0);
        assert_eq!(failure.work.peak_allocation_bytes, 0);
    }
    cancellation.cancel();
    let failure = encode_workspace_deleted_measured(volume_id, WorkBudget::UNBOUNDED, &cancellation)
        .expect_err("cancelled deletion producer");
    assert!(matches!(failure.error, RetentionCreatedError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::UNCHARGED);
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

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn original_retention_writer_transfers_label_and_output_claims()
    -> Result<(), Box<dyn std::error::Error>>
{
    let value = RetentionCreated {
        volume_id: VolumeId::from_bytes([21; 16]), kind: RetentionKind::Pin,
        label: "owned-native-pin".to_owned(),
        generation_root: ObjectId {
            kind: ObjectKind::GenerationRoot, digest: Digest::from_bytes([22; 32]),
        },
        config: VolumeConfig::portable(Lifecycle::Durable),
    };
    let expected = encode_retention_created(&value)?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let label_bytes = value.label.capacity() as u64;
    let label_guard = original.retain_owned(label_bytes)?;
    let input = (value, label_guard);
    let input_live = original.with_allocations(|ledger, _, _, _| Ok::<_, crate::FsError>(ledger.live_bytes()))?;
    let before = original.receipt();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let owned = encode_retention_created_with_accounting(&input.0, &mut accounting)?;
    assert_eq!(owned.0, expected);
    let KernelOwnedAllocation::NativeOriginal(guard) = &owned.1 else {
        panic!("original retention returned standalone custody");
    };
    assert_eq!(guard.bytes(), owned.0.capacity() as u64);
    assert!(guard.work().shares_ledger(&original));
    let spent = original.receipt();
    assert_eq!(spent.allocation_operations, before.allocation_operations + 1);
    assert_eq!(spent.bytes_encoded, owned.0.len() as u64);
    assert_eq!(spent.bytes_copied, 48 + input.0.label.len() as u64);
    assert_eq!(spent.peak_allocation_bytes,
        input_live + owned.0.capacity() as u64);
    drop(owned);
    let live = original.with_allocations(|ledger, _, _, _| Ok::<_, crate::FsError>(ledger.live_bytes()))?;
    assert_eq!(live, input_live);
    drop(input);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn original_retention_refusal_never_produces_unclaimed_bytes()
    -> Result<(), Box<dyn std::error::Error>>
{
    let value = RetentionCreated {
        volume_id: VolumeId::from_bytes([23; 16]), kind: RetentionKind::Pin,
        label: "native-refusal".to_owned(),
        generation_root: ObjectId {
            kind: ObjectKind::GenerationRoot, digest: Digest::from_bytes([24; 32]),
        },
        config: VolumeConfig::portable(Lifecycle::Durable),
    };
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget { bytes_copied: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).map_err(|(error, _)| error)?;
    let label_guard = original.retain_owned(value.label.capacity() as u64)?;
    let input = (value, label_guard);
    let before = original.receipt();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let error = match encode_retention_created_with_accounting(&input.0, &mut accounting) {
        Err(error) => error,
        Ok(owned) => { drop(owned); panic!("retention copies were not denied"); }
    };
    assert!(matches!(error, crate::FsError::Work(_)));
    let denied = original.receipt();
    assert_eq!(denied.allocation_operations, before.allocation_operations);
    assert_eq!(denied.peak_allocation_bytes, before.peak_allocation_bytes);
    assert_eq!(denied.bytes_encoded, 0);
    assert_eq!(denied.bytes_copied, 0);
    assert_eq!(denied.items_examined, before.items_examined + 1);
    cancellation.cancel();
    assert!(encode_retention_created_with_accounting(&input.0, &mut accounting).is_err());
    assert_eq!(original.receipt(), denied);
    drop(input);
    Ok(())
}
