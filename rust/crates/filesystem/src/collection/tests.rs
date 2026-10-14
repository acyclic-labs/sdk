use super::*;
use crate::WorkBudget;
use crate::kernel::allocation::AllocationLedger;
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
        WorkBudget {
            bytes_copied: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?;
    let baseline = denied.live_allocation_bytes();
    let before = denied.receipt();
    let mut accounting = KernelAccounting::native(&denied, &cancellation);
    let error = clone_generation_root_with_accounting(&working, &mut accounting)
        .err()
        .ok_or("copy-denied root unexpectedly completed")?;
    assert!(matches!(
        error,
        crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(
            crate::WorkError::BudgetExceeded {
                counter: "bytes_copied",
                ..
            }
        ))
    ));
    assert_eq!(denied.live_allocation_bytes(), baseline);
    assert_eq!(denied.receipt().bytes_copied, before.bytes_copied);
    assert_eq!(
        denied.receipt().allocation_operations,
        before.allocation_operations + 1
    );

    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let baseline = original.live_allocation_bytes();
    let before = original.receipt();
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let owned = clone_generation_root_with_accounting(&working, &mut accounting)
            .expect("actual original-funded parent copy");
        assert_eq!(owned.0.parents, working.parents);
        assert_eq!(owned.0.volume_id, working.volume_id);
        assert_eq!(owned.0.file_table, working.file_table);
        assert_eq!(
            original.live_allocation_bytes(),
            baseline + root_parent_bytes(&owned.0)
        );
        panic!("unwind after the completed physical parent copy");
    }));
    assert!(unwind.is_err());
    assert_eq!(original.live_allocation_bytes(), baseline);
    assert_eq!(
        original.receipt().bytes_copied,
        before.bytes_copied
            + crate::foundation::usize_to_u64(std::mem::size_of::<crate::GenerationId>())
    );
    let cancelled = crate::CancellationToken::new();
    cancelled.cancel();
    let before = original.receipt();
    let mut accounting = KernelAccounting::native(&original, &cancelled);
    assert!(matches!(
        clone_generation_root_with_accounting(&working, &mut accounting),
        Err(crate::FsError::Cancelled(_))
    ));
    assert_eq!(original.live_allocation_bytes(), baseline);
    assert_eq!(original.receipt(), before);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_native_root_copy_and_registration_failures_retire_actual_buffers()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = crate::CancellationToken::new();
    let fs = crate::Fs::memory();
    let workspace = fs
        .create_workspace(
            "registered-collection-root-custody",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    workspace.write_text("/seed", "body").await?;
    let checkout = workspace
        .checkout(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode {
                access: crate::model::AccessMode::ReadWrite,
                consistency: crate::model::ConsistencyMode::Pinned,
                mutations: crate::model::MutationMode::PrivateOverlay,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let producer = checkout.private_candidate(
        &mut crate::kernel::allocation::KernelAccounting::native(&original, &cancellation),
        &cancellation,
    )?;
    let custody = producer.private_control_custody();
    let meter = (&original.state);
    let baseline = original.live_allocation_bytes();
    let physical = custody.physical_retained_bytes();
    let events = custody.native_control_events();
    let Held::Checkout {
        mut working,
        base,
        config,
        ..
    } = checkout_hold()
    else {
        unreachable!()
    };
    working.parents = vec![crate::GenerationId::new(object(7).digest)];
    let foreign_token = Arc::new(crate::kernel::ProofDataCustody::default());
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    assert!(
        clone_generation_root(&working, &mut accounting, Some(&foreign_token)).is_err(),
        "an unregistered token cannot adopt this original's root buffer"
    );
    assert_eq!(foreign_token.physical_retained_bytes(), 0);
    assert_eq!(original.live_allocation_bytes(), baseline);
    let collection = Arc::new(Collection::default());
    let source = CheckoutRoots::new(
        Some(&collection),
        config,
        base,
        (working, Default::default()),
        &mut KernelAccounting::borrowed_standalone(
            &mut AllocationLedger::default(),
            &mut WorkCounters::default(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ),
        None,
    )?;
    let cancelled = crate::CancellationToken::new();
    cancelled.cancel();
    let mut accounting = KernelAccounting::native(&original, &cancelled);
    let failed = source
        .replicate(&mut accounting, Some(&custody))
        .err()
        .ok_or("cancelled held registration installed a native replica")?;
    assert!(matches!(failed, crate::FsError::Cancelled(_)));
    assert_eq!(
        custody.native_control_events(),
        events,
        "cancelled preadmission owns no phantom buffers"
    );
    assert_eq!(custody.physical_retained_bytes(), physical);
    assert_eq!(original.live_allocation_bytes(), baseline);
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let mut paid = source.replicate(&mut accounting, Some(&custody))?;
    assert!(paid.same_original_scope(meter));
    let foreign = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    assert!(!paid.same_original_scope((&foreign.state)));
    let held_control = {
        let mut holds = collection
            .holds
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let held = holds
            .slots
            .get_mut(paid.hold.as_ref().expect("paid registration").id)
            .and_then(|slot| slot.held.as_mut())
            .expect("actual held vector");
        let Held::Checkout { control, .. } = held else {
            unreachable!()
        };
        std::mem::take(control)
    };
    assert!(
        !paid.same_original_scope(meter),
        "a paid working vector does not certify an untagged held copy"
    );
    {
        let mut holds = collection
            .holds
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let held = holds
            .slots
            .get_mut(paid.hold.as_ref().expect("paid registration").id)
            .and_then(|slot| slot.held.as_mut())
            .expect("actual held vector");
        let Held::Checkout { control, .. } = held else {
            unreachable!()
        };
        *control = held_control;
    }
    assert!(paid.same_original_scope(meter));
    let root_bytes = custody.physical_retained_bytes() - physical;
    assert!(root_bytes >= 2 * std::mem::size_of::<crate::GenerationId>() as u64);
    paid.set_working(source.working.clone());
    assert!(
        custody.is_native_original(meter),
        "token lineage is unchanged by an ordinary replacement"
    );
    assert!(
        !paid.same_original_scope(meter),
        "token lineage cannot certify the new unadmitted vectors"
    );
    assert_eq!(custody.physical_retained_bytes(), physical);
    drop(paid);
    assert_eq!(
        original.live_allocation_bytes(),
        baseline,
        "root buffers and exclusive slot quota retire but cached slots remain"
    );
    let Held::Checkout {
        working,
        base,
        config,
        ..
    } = checkout_hold()
    else {
        unreachable!()
    };
    let empty = CheckoutRoots::new(
        None,
        config,
        base,
        (working, Default::default()),
        &mut KernelAccounting::borrowed_standalone(
            &mut AllocationLedger::default(),
            &mut WorkCounters::default(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ),
        None,
    )?;
    assert!(
        empty.same_original_scope(meter),
        "actual zero-capacity buffers require no allocation tag"
    );
    Ok(())
}

#[test]
fn private_root_vector_tags_retire_actual_buffers_without_adopting_cached_slots()
-> Result<(), Box<dyn std::error::Error>> {
    let collection = Arc::new(Collection::default());
    let cancellation = crate::CancellationToken::new();
    let Held::Checkout {
        mut working,
        base,
        config,
        ..
    } = checkout_hold()
    else {
        unreachable!()
    };
    working.parents = vec![crate::GenerationId::new(object(7).digest)];
    let original = CheckoutRoots::new(
        Some(&collection),
        config,
        base,
        (working, Default::default()),
        &mut KernelAccounting::borrowed_standalone(
            &mut AllocationLedger::default(),
            &mut WorkCounters::default(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ),
        None,
    )?;
    let custody = Arc::new(crate::kernel::ProofDataCustody::default());
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let mut measured = original.replicate(
        &mut KernelAccounting::borrowed_standalone(
            &mut allocations,
            &mut work,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ),
        None,
    )?;
    measured.attach_control_custody(&custody);
    let (copied_held, held_bytes) = {
        let holds = collection
            .holds
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let held = holds
            .slots
            .get(measured.hold.as_ref().expect("registered clone").id)
            .and_then(|slot| slot.held.as_ref())
            .expect("live cloned roots");
        let Held::Checkout { working, .. } = held else {
            unreachable!()
        };
        (held.clone(), root_parent_bytes(working))
    };
    let bytes = root_parent_bytes(&measured.working) + held_bytes;
    assert_eq!(custody.physical_retained_bytes(), bytes);
    assert_eq!(custody.retained_bytes(), 0);
    assert!(work.peak_allocation_bytes >= bytes);
    drop(measured);
    assert_eq!(
        custody.physical_retained_bytes(),
        0,
        "snapshot clones never inherit original vector tags"
    );
    assert!(
        collection
            .holds
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .slots
            .capacity()
            > 0,
        "freed per-handle buffers do not destroy or adopt cached slot backing"
    );
    drop(copied_held);
    let mut measured = original.replicate(
        &mut KernelAccounting::borrowed_standalone(
            &mut allocations,
            &mut work,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ),
        None,
    )?;
    measured.attach_control_custody(&custody);
    assert!(custody.physical_retained_bytes() > 0);
    measured.set_working(original.working.clone());
    assert_eq!(
        custody.physical_retained_bytes(),
        0,
        "unmeasured replacements retire old buffers without phantom adoption"
    );
    drop(measured);
    drop(original);
    Ok(())
}

#[test]
fn bounded_registration_preserves_roots_and_reuses_vacant_slots()
-> Result<(), Box<dyn std::error::Error>> {
    let collection = Arc::new(Collection::default());
    let cancellation = crate::CancellationToken::new();
    let mut work = WorkCounters::default();
    let mut allocations = AllocationLedger::default();
    assert!(
        collection
            .register_with_accounting(
                checkout_hold(),
                &mut KernelAccounting::borrowed_standalone(
                    &mut allocations,
                    &mut work,
                    WorkBudget::default(),
                    &cancellation
                )
            )
            .is_err()
    );
    assert!(collection.held().is_empty());
    assert_eq!(work, WorkCounters::default());

    let mut work = WorkCounters::default();
    let mut allocations = AllocationLedger::default();
    let held = collection.register_with_accounting(
        checkout_hold(),
        &mut KernelAccounting::borrowed_standalone(
            &mut allocations,
            &mut work,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ),
    )?;
    assert_eq!(collection.held().len(), 1);
    assert_eq!(work.allocation_operations, 1);
    assert_eq!(
        held.logical_owned_bytes(),
        crate::foundation::usize_to_u64(std::mem::size_of::<Slot>())
    );

    let mut extra = Vec::new();
    loop {
        let full = {
            let holds = collection
                .holds
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            holds.slots.len() == holds.slots.capacity()
        };
        if full {
            break;
        }
        extra.push(collection.register_with_accounting(
            checkout_hold(),
            &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation),
        )?);
    }
    let retained_roots = collection.held().len();

    let mut limited = WorkBudget::UNBOUNDED;
    limited.bytes_copied = 0;
    let mut failed_work = WorkCounters::default();
    let mut allocations = AllocationLedger::default();
    assert!(
        collection
            .register_with_accounting(
                checkout_hold(),
                &mut KernelAccounting::borrowed_standalone(
                    &mut allocations,
                    &mut failed_work,
                    limited,
                    &cancellation
                )
            )
            .is_err()
    );
    assert_eq!(
        collection.held().len(),
        retained_roots,
        "failed growth does not unregister old roots"
    );
    assert_eq!(
        failed_work.allocation_operations, 1,
        "the attempted backing allocation remains measured"
    );
    drop(held);
    drop(extra);
    assert!(collection.held().is_empty());

    let mut reused_work = WorkCounters::default();
    let mut allocations = AllocationLedger::default();
    let mut reuse_budget = WorkBudget::UNBOUNDED;
    reuse_budget.allocation_operations = 0;
    let reused = collection.register_with_accounting(
        checkout_hold(),
        &mut KernelAccounting::borrowed_standalone(
            &mut allocations,
            &mut reused_work,
            reuse_budget,
            &cancellation,
        ),
    )?;
    assert_eq!(reused_work.allocation_operations, 0);
    assert_eq!(collection.held().len(), 1);
    drop(reused);
    assert!(collection.held().is_empty());
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
