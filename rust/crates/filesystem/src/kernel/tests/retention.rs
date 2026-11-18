use super::*;
use crate::Digest;
use crate::kernel::allocation::KernelAccounting;
use crate::model::Lifecycle;
use crate::{CancellationToken, WorkBudget, WorkError};

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
        |_| Ok::<_, crate::kernel::RetentionCreatedError>(()),
        |_| {
            crate::kernel::RetentionCreatedError::Decode(
                crate::kernel::CanonicalDecodeError::AllocationFailed,
            )
        },
    )?;
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
    // Old/unknown headers refuse before the removed configuration fields.
    for version in [0_u16, 1, u16::MAX] {
        let mut obsolete = encoded[..DOMAIN.len() + 2].to_vec();
        obsolete[DOMAIN.len()..].copy_from_slice(&version.to_le_bytes());
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
            |_| Ok::<_, crate::kernel::RetentionCreatedError>(()),
            |_| crate::kernel::RetentionCreatedError::Decode(
                crate::kernel::CanonicalDecodeError::AllocationFailed
            )
        ),
        Err(RetentionCreatedError::InvalidLabel)
    ));
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn original_retention_refusal_never_produces_unclaimed_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    let value = RetentionCreated {
        volume_id: VolumeId::from_bytes([23; 16]),
        kind: RetentionKind::Pin,
        label: "native-refusal".to_owned(),
        generation_root: ObjectId {
            storage: None,
            kind: ObjectKind::GenerationRoot,
            digest: Digest::from_bytes([24; 32]),
        },
        config: VolumeConfig::portable(Lifecycle::Durable),
    };
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget {
            bytes_copied: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let label_guard = original.claim(
        value.label.capacity() as u64,
        0,
        crate::WorkCounters::UNCHARGED,
    )?;
    let input = (value, label_guard);
    let before_live = original.live_allocation_bytes().unwrap();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let error = match crate::kernel::codec::encode_owned(&mut accounting, |observe, reserve| {
        encode_retention_created(&input.0, observe, reserve)
    }) {
        Err(error) => error,
        Ok(owned) => {
            drop(owned);
            panic!("retention copies were not denied");
        }
    };
    assert!(matches!(
        error,
        crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(
            WorkError::BudgetExceeded {
                counter: "bytes_copied",
                observed: 1..,
                maximum: 0,
            }
        ))
    ));
    let denied = original.receipt().unwrap();
    assert_eq!(original.live_allocation_bytes().unwrap(), before_live);
    assert!(denied.bytes_encoded > 0);
    assert_eq!(denied.bytes_copied, 0);
    cancellation.cancel();
    assert!(
        crate::kernel::codec::encode_owned(&mut accounting, |observe, reserve| {
            encode_retention_created(&input.0, observe, reserve)
        })
        .is_err()
    );
    assert_eq!(original.receipt().unwrap(), denied);
    drop(input);
    Ok(())
}
