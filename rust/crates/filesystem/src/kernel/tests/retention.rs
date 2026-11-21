use super::*;
use crate::Digest;
use crate::kernel::allocation::KernelAccounting;
use crate::model::Lifecycle;
use crate::{CancellationToken, WorkBudget, WorkError};

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
    let original = crate::NativeMaterializationWork::root(
        WorkBudget {
            bytes_copied: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
        &crate::services::ServicesInitialized::for_kernel_tests(),
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
                counter: crate::WorkCounter::BytesCopied,
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
