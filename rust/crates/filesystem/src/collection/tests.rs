use super::*;
use crate::storage::{ObjectId, ObjectKind};

fn object(byte: u8) -> ObjectId {
    ObjectId {
        storage: None,
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

#[test]
fn checkout_root_replacement_rejects_mismatched_hold_before_changing_state()
-> Result<(), Box<dyn std::error::Error>> {
    for registered in [false, true] {
        let collection = Arc::new(Collection::default());
        let cancellation = crate::CancellationToken::new();
        let mut accounting =
            KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancellation);
        let Held::Checkout {
            working, config, ..
        } = checkout_hold()
        else {
            unreachable!()
        };
        let initial = working.clone();
        let mut roots = CheckoutRoots::new(
            registered.then_some(&collection),
            config,
            object(1),
            (working, crate::kernel::ProofDataClaim::default()),
            &mut accounting,
            None,
        )?;
        let mut replacement = initial.clone();
        replacement.file_table = object(9);
        let invalid_hold = (!registered).then(|| {
            (
                replacement.clone(),
                crate::kernel::ProofDataClaim::default(),
            )
        });
        assert!(matches!(
            roots.set(
                object(7),
                (
                    replacement.clone(),
                    crate::kernel::ProofDataClaim::default(),
                ),
                invalid_hold,
            ),
            Err(crate::FsError::InvalidDiff)
        ));
        assert_eq!(roots.base, object(1));
        assert_eq!(roots.working, initial);
        assert_eq!(collection.held().len(), usize::from(registered));
        if let Some(Held::Checkout { base, working, .. }) = collection.held().first() {
            assert_eq!(*base, object(1));
            assert_eq!(*working, initial);
        }

        let held = registered.then(|| {
            (
                replacement.clone(),
                crate::kernel::ProofDataClaim::default(),
            )
        });
        roots.set(
            object(7),
            (
                replacement.clone(),
                crate::kernel::ProofDataClaim::default(),
            ),
            held,
        )?;
        assert_eq!(roots.base, object(7));
        assert_eq!(roots.working, replacement);
        if let Some(Held::Checkout { base, working, .. }) = collection.held().first() {
            assert_eq!(*base, object(7));
            assert_eq!(*working, replacement);
        }
    }
    Ok(())
}

#[tokio::test]
async fn prepared_generation_pin_hands_sweep_admission_to_the_root_registry()
-> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    for native in [false, true] {
        let collection = Arc::new(Collection::default());
        let cancellation = crate::CancellationToken::new();
        let mut accounting =
            KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancellation);
        #[cfg(not(target_arch = "wasm32"))]
        let original = crate::NativeMaterializationWork::new(
            crate::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .map_err(|(cause, _)| cause)?;
        #[cfg(not(target_arch = "wasm32"))]
        if native {
            accounting = KernelAccounting::native(&original, &cancellation);
        }
        #[cfg(target_arch = "wasm32")]
        if native {
            continue;
        }
        let Held::Checkout {
            working, config, ..
        } = checkout_hold()
        else {
            unreachable!()
        };
        let proof = crate::kernel::GenerationProof {
            root: working,
            generation_id: crate::GenerationId::new(object(7).digest),
            file_count: 1,
            logical_file_bytes: 0,
            objects: vec![object(1), object(2)],
            work: WorkCounters::UNCHARGED,
        };
        // Admit the owner's shared control before measuring per-hold credit.
        // The control remains live until the accounting owner retires.
        drop(
            crate::kernel::allocation::retained::RetainedAllocation::claim(0, 0, &mut accounting)?,
        );
        let baseline = accounting
            .standalone_parts()
            .map(|(ledger, _, _, _)| ledger.live_bytes());
        let collecting = collection.begin().await;
        let pin = collection
            .hold_generation_with_accounting(&proof, config, collection.sweeps(), &mut accounting)
            .await?;
        let held = collection.held();
        assert!(
            matches!(held.as_slice(), [Held::Generation { root, config: actual }]
            if root.digest == proof.generation_id.digest() && *actual == config)
        );
        // Registering an ordinary root must not retain the global sweep gate.
        let (gate, sweepable) = collecting
            .sweepable(vec![object(1).digest, object(2).digest, object(3).digest])
            .await;
        assert_eq!(
            sweepable,
            vec![object(3).digest],
            "new root pin lost its active-collection admission"
        );
        drop(gate);
        drop(collecting);
        drop(pin);
        assert!(collection.held().is_empty());
        assert_eq!(
            accounting
                .standalone_parts()
                .map(|(ledger, _, _, _)| ledger.live_bytes()),
            baseline,
            "root-pin retirement lost its original allocation credit"
        );
    }
    Ok(())
}

#[test]
fn collection_hold_retirement_preserves_the_original_accounting_credit()
-> Result<(), Box<dyn std::error::Error>> {
    let collection = Arc::new(Collection::default());
    let cancellation = crate::CancellationToken::new();
    let mut accounting =
        KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancellation);
    drop(crate::kernel::allocation::retained::RetainedAllocation::claim(0, 0, &mut accounting)?);
    let baseline = accounting
        .standalone_parts()
        .ok_or("missing ledger")?
        .0
        .live_bytes();
    let config = crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral);
    let record = crate::kernel::FileRecord {
        file_id: crate::FileId::new(),
        kind: crate::kernel::FileKind::Regular,
        link_count: 1,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            ..object(4)
        },
        payload: crate::kernel::FilePayload::InlineRegular(crate::kernel::InlineFileData::new(
            b"x",
        )?),
    };
    let holds = [
        collection.register_with_accounting(checkout_hold(), &mut accounting)?,
        collection.register_with_accounting(
            Held::Generation {
                root: object(9),
                config,
            },
            &mut accounting,
        )?,
        Collection::hold_record_with_accounting(
            Some(&collection),
            record,
            config,
            &mut accounting,
        )?
        .ok_or("missing record hold")?,
    ];
    assert_eq!(collection.held().len(), 3);
    drop(holds);
    assert!(collection.held().is_empty());
    assert_eq!(
        accounting
            .standalone_parts()
            .ok_or("missing ledger")?
            .0
            .live_bytes(),
        baseline,
        "retired collection holds retained their producer's credit"
    );
    Ok(())
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
    let (gate, sweepable) = collecting
        .sweepable(vec![object(1).digest, object(2).digest])
        .await;
    assert_eq!(sweepable, vec![object(1).digest, object(2).digest]);
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
    let (_gate, sweepable) = collecting
        .sweepable(vec![object(1).digest, object(2).digest])
        .await;
    assert_eq!(sweepable, vec![object(2).digest]);
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
        collecting.sweeping(object(u8::try_from(round)? + 10).digest);
    }
    assert!(
        collection
            .admit(&[object(1)], |_| false, proven_at)
            .await
            .is_err()
    );
    Ok(())
}
