use super::*;
use crate::storage::{ObjectId, ObjectKind};

fn object(byte: u8) -> ObjectId {
    ObjectId {
        kind: ObjectKind::Blob,
        digest: crate::foundation::Digest::from_bytes([byte; 32]),
    }
}

fn checkout_hold() -> Held {
    Held::Checkout {
        base: object(1),
        working: GenerationRoot {
            volume_id: crate::VolumeId::new(),
            root_file_id: crate::FileId::new(),
            file_table: ObjectId {
                kind: ObjectKind::FileTablePage,
                ..object(2)
            },
            parents: Vec::new(),
            required_features: 0,
        },
        config: crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral),
        control: crate::kernel::ProofDataClaim::default(),
    }
}

#[cfg(feature = "native-mount")]
#[test]
fn file_table_replacement_rejects_a_missing_checkout_registration()
-> Result<(), Box<dyn std::error::Error>> {
    let collection = Arc::new(Collection::default());
    let cancellation = crate::CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
    let Held::Checkout {
        base,
        working,
        config,
        ..
    } = checkout_hold()
    else {
        unreachable!()
    };
    let original = working.file_table;
    let mut roots = CheckoutRoots::new(
        Some(&collection),
        config,
        base,
        (working, crate::kernel::ProofDataClaim::default()),
        &mut accounting,
        None,
    )?;
    let id = roots.hold.as_ref().ok_or("missing registration")?.id;
    let saved = std::mem::replace(&mut collection.holds().slots[id], Slot::Free(None));
    let result = roots.set_file_table(object(9));
    collection.holds().slots[id] = saved;
    assert!(matches!(result, Err(crate::FsError::InvalidDiff)));
    assert_eq!(roots.file_table, original);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn original_root_factory_keeps_completed_copy_and_retires_buffers_on_unwind()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = crate::CancellationToken::new();
    let Held::Checkout { mut working, .. } = checkout_hold() else {
        unreachable!()
    };
    working.parents = vec![crate::GenerationId::new(object(7).digest)];
    let denied = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            bytes_copied: 0,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?;
    let baseline = denied.live_allocation_bytes();
    let before = denied.receipt();
    assert!(matches!(
        clone_generation_root_with_accounting(
            &working,
            &mut KernelAccounting::native(&denied, &cancellation)
        ),
        Err(crate::FsError::NativeKernel(
            crate::NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded {
                counter: "bytes_copied",
                ..
            })
        ))
    ));
    assert_eq!(denied.live_allocation_bytes(), baseline);
    assert_eq!(denied.receipt().bytes_copied, before.bytes_copied);

    let original =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
    let baseline = original.live_allocation_bytes();
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let owned = clone_generation_root_with_accounting(
            &working,
            &mut KernelAccounting::native(&original, &cancellation),
        )
        .expect("original-funded parent copy");
        assert_eq!(owned.0, working);
        assert!(original.live_allocation_bytes() > baseline);
        panic!("unwind after the completed physical parent copy");
    }));
    assert!(unwind.is_err());
    assert_eq!(original.live_allocation_bytes(), baseline);
    let cancelled = crate::CancellationToken::new();
    cancelled.cancel();
    let before = original.receipt();
    assert!(matches!(
        clone_generation_root_with_accounting(
            &working,
            &mut KernelAccounting::native(&original, &cancelled)
        ),
        Err(crate::FsError::Cancelled(_))
    ));
    assert_eq!(original.receipt(), before);
    Ok(())
}

#[test]
fn bounded_registration_preserves_roots_and_reuses_vacant_slots()
-> Result<(), Box<dyn std::error::Error>> {
    let collection = Arc::new(Collection::default());
    let cancellation = crate::CancellationToken::new();
    assert!(
        collection
            .register_with_accounting(
                checkout_hold(),
                &mut KernelAccounting::standalone(crate::WorkBudget::default(), &cancellation),
            )
            .is_err()
    );
    assert!(collection.held().is_empty());
    for _ in 0..2 {
        let held = collection.register_with_accounting(
            checkout_hold(),
            &mut KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation),
        )?;
        assert_eq!(collection.held().len(), 1);
        drop(held);
        assert!(collection.held().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn a_closure_proven_before_a_sweep_of_its_object_is_refused()
-> Result<(), Box<dyn std::error::Error>> {
    let collection = Arc::new(Collection::default());
    let proven_at = collection.sweeps();
    let collecting = collection.begin().await;
    let (gate, sweepable) = collecting.sweepable(vec![object(1), object(2)]).await;
    assert_eq!(sweepable, vec![object(1), object(2)]);
    for swept in sweepable {
        collecting.sweeping(swept);
    }
    drop(gate);
    assert!(
        collection
            .admit(&[object(1)], |_| false, proven_at)
            .await
            .is_err()
    );
    // A staged copy is stored again by the publication's drain.
    drop(collection.admit(&[object(1)], |_| true, proven_at).await?);
    // A proof made after the sweep read the store as it is.
    drop(
        collection
            .admit(&[object(1)], |_| false, collection.sweeps())
            .await?,
    );
    drop(collection.admit(&[object(3)], |_| false, proven_at).await?);
    Ok(())
}

#[tokio::test]
async fn a_closure_admitted_while_collecting_is_kept() -> Result<(), Box<dyn std::error::Error>> {
    let collection = Arc::new(Collection::default());
    let collecting = collection.begin().await;
    drop(
        collection
            .admit(&[object(1)], |_| false, collection.sweeps())
            .await?,
    );
    let (_gate, sweepable) = collecting.sweepable(vec![object(1), object(2)]).await;
    assert_eq!(sweepable, vec![object(2)]);
    Ok(())
}

#[tokio::test]
async fn a_collection_begins_after_every_admitted_record_is_written()
-> Result<(), Box<dyn std::error::Error>> {
    let collection = Arc::new(Collection::default());
    let hold = collection.admit(&[object(1)], |_| false, 0).await?;
    let begun = tokio::spawn({
        let collection = Arc::clone(&collection);
        async move { drop(collection.begin().await) }
    });
    tokio::task::yield_now().await;
    assert!(!begun.is_finished(), "the collection waits for the record");
    drop(hold);
    begun.await?;
    Ok(())
}

#[tokio::test]
async fn a_proof_older_than_every_remembered_collection_is_refused()
-> Result<(), Box<dyn std::error::Error>> {
    let collection = Arc::new(Collection::default());
    let proven_at = collection.sweeps();
    for round in 0..=REMEMBERED_COLLECTIONS {
        let collecting = collection.begin().await;
        let (_gate, _) = collecting.sweepable(Vec::new()).await;
        collecting.sweeping(object(u8::try_from(round)? + 10));
    }
    assert!(
        collection
            .admit(&[object(1)], |_| false, proven_at)
            .await
            .is_err()
    );
    Ok(())
}
