use super::*;
use crate::storage::{ObjectId, ObjectKind, ObjectStoreError};

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
            .hold_generation_with_accounting(
                &crate::MemoryObjectStore::default(),
                &proof,
                config,
                collection.sweeps(),
                &mut accounting,
            )
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
        bindings: None,
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
    let objects = crate::MemoryObjectStore::default();
    let cancellation = crate::CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
    let proven_at = collection.sweeps();
    let collecting = collection.begin().await;
    let (gate, sweepable) = collecting
        .sweepable(vec![object(1).digest, object(2).digest])
        .await;
    assert_eq!(sweepable, vec![object(1).digest, object(2).digest]);
    for _ in sweepable {
        collecting.sweeping();
    }
    drop(gate);
    let scope = crate::PublicationScope {
        objects: &[object(1)],
        proven_at,
    };
    assert!(matches!(
        collection
            .admit(&objects, scope, |_| false, &mut accounting)
            .await,
        Err(crate::FsError::Object(ObjectStoreError::Missing))
    ));
    // A staged copy is stored again by the publication's drain.
    drop(
        collection
            .admit(&objects, scope, |_| true, &mut accounting)
            .await?,
    );
    // A proof made after the sweep read the store as it is; admission adds no read.
    let before = accounting.spent();
    drop(
        collection
            .admit(
                &objects,
                crate::PublicationScope {
                    objects: &[object(1)],
                    proven_at: collection.sweeps(),
                },
                |_| false,
                &mut accounting,
            )
            .await?,
    );
    assert_eq!(accounting.spent(), before);
    Ok(())
}

#[tokio::test]
async fn a_closure_admitted_while_collecting_is_kept() -> Result<(), Box<dyn std::error::Error>> {
    let collection = Arc::new(Collection::default());
    let objects = crate::MemoryObjectStore::default();
    let cancellation = crate::CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
    let collecting = collection.begin().await;
    drop(
        collection
            .admit(
                &objects,
                crate::PublicationScope {
                    objects: &[object(1)],
                    proven_at: collection.sweeps(),
                },
                |_| false,
                &mut accounting,
            )
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
    let cancellation = crate::CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
    let hold = collection
        .admit(
            &crate::MemoryObjectStore::default(),
            crate::PublicationScope {
                objects: &[object(1)],
                proven_at: 0,
            },
            |_| false,
            &mut accounting,
        )
        .await?;
    let (waiting, registered) = tokio::sync::oneshot::channel();
    let begun = tokio::spawn({
        let collection = Arc::clone(&collection);
        async move {
            let mut begin = std::pin::pin!(collection.begin());
            let mut waiting = Some(waiting);
            let collecting = std::future::poll_fn(|cx| {
                let result = std::future::Future::poll(begin.as_mut(), cx);
                if result.is_pending()
                    && let Some(waiting) = waiting.take()
                {
                    let _ = waiting.send(());
                }
                result
            })
            .await;
            drop(collecting);
        }
    });
    registered.await?;
    assert!(!begun.is_finished(), "the collection waits for the record");
    drop(hold);
    begun.await?;
    Ok(())
}

#[tokio::test]
async fn an_intact_old_closure_survives_unrelated_collections_with_original_work()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::AsyncObjectStore as _;
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let collection = Arc::new(Collection::default());
    let objects = crate::MemoryObjectStore::default();
    let cancellation = crate::CancellationToken::new();
    let object = crate::storage::HashedObject::new(
        ObjectKind::Blob,
        acyclic_stream::OwnedBytes::from_static(b"an intact old closure"),
    );
    let id = object.object_id();
    objects
        .put_hashed(
            object,
            &mut crate::AuthorityWork::new(crate::WorkBudget::UNBOUNDED, &cancellation),
        )
        .await?;
    let proven_at = collection.sweeps();
    for _ in 0..9 {
        let collecting = collection.begin().await;
        let (_gate, _) = collecting.sweepable(Vec::new()).await;
        collecting.sweeping();
    }
    let scope = crate::PublicationScope {
        objects: &[id],
        proven_at,
    };
    for native in [false, true] {
        let mut accounting =
            KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
        #[cfg(not(target_arch = "wasm32"))]
        let original =
            crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, _)| cause)?;
        #[cfg(not(target_arch = "wasm32"))]
        if native {
            accounting = KernelAccounting::native(&original, &cancellation);
        }
        #[cfg(target_arch = "wasm32")]
        if native {
            continue;
        }
        let before = accounting.spent();
        let collecting = collection.begin().await;
        drop(
            collection
                .admit(&objects, scope, |_| false, &mut accounting)
                .await?,
        );
        let after = accounting.spent();
        assert_eq!(
            after.backend_read_operations - before.backend_read_operations,
            1
        );
        assert_eq!(after.object_bytes_read - before.object_bytes_read, 21);
        assert_eq!(after.bytes_hashed - before.bytes_hashed, 21);
        let (gate, sweepable) = collecting.sweepable(vec![id.digest]).await;
        assert!(
            sweepable.is_empty(),
            "revalidated closure lost active-collection custody"
        );
        drop(gate);
        drop(collecting);

        let mut denied = KernelAccounting::standalone(
            crate::WorkBudget {
                object_bytes_read: 0,
                ..crate::WorkBudget::UNBOUNDED
            },
            &cancellation,
        );
        #[cfg(not(target_arch = "wasm32"))]
        let denied_original = crate::NativeMaterializationWork::new(
            crate::WorkBudget {
                object_bytes_read: 0,
                ..crate::WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .map_err(|(cause, _)| cause)?;
        #[cfg(not(target_arch = "wasm32"))]
        if native {
            denied = KernelAccounting::native(&denied_original, &cancellation);
        }
        let refusal = match collection
            .admit(&objects, scope, |_| false, &mut denied)
            .await
        {
            Ok(_) => return Err("finite work admitted an unfunded object read".into()),
            Err(error) => error,
        };
        if native {
            #[cfg(not(target_arch = "wasm32"))]
            assert!(matches!(
                refusal,
                crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(
                    crate::WorkError::BudgetExceeded {
                        counter: "object_bytes_read",
                        ..
                    }
                ))
            ));
        } else {
            assert!(matches!(
                refusal,
                crate::FsError::Work(crate::WorkError::BudgetExceeded {
                    counter: "object_bytes_read",
                    ..
                })
            ));
        }
        assert_eq!(denied.spent().object_bytes_read, 0);
        let cancelled = crate::CancellationToken::new();
        cancelled.cancel();
        let mut cancelled_work =
            KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancelled);
        #[cfg(not(target_arch = "wasm32"))]
        if native {
            cancelled_work = KernelAccounting::native(&original, &cancelled);
        }
        let before = cancelled_work.spent();
        assert!(matches!(
            collection
                .admit(&objects, scope, |_| false, &mut cancelled_work)
                .await,
            Err(crate::FsError::Cancelled(_))
        ));
        assert_eq!(cancelled_work.spent(), before);
    }
    Ok(())
}
