use super::*;
use acyclic_stream::OwnedBytes;
use crate::GenerationExportManifestError;
use crate::async_storage::poll_ready;
use crate::kernel::{InlineFileData, LogicalName, NameEncoding, MAXIMUM_GENERATION_ROOT_BYTES, encode_tree_page};
use crate::memory::MemoryObjectStore as TestMemoryObjectStore;
use crate::model::{
    CaseSensitivity, ConcurrencyMode, FilesystemProfile, UnicodePolicy, VolumeLimits,
};
use crate::storage::CreateAuthorityOutcome;
use crate::storage::object_digest;
use crate::storage::{AuthorityFailure, AuthorityResult, AuthorityStore};
use crate::storage::{
    ObjectFailure, ObjectRead, ObjectReadRequest, ObjectResult, ObjectStore, ObjectWrite,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn measured_private_candidate_admits_only_independent_proof_and_root_storage()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("measured-private-candidate").await?;
    workspace.write_text("/file", "body").await?;
    let cancellation = CancellationToken::new();
    let checkout = workspace.checkout_measured(
        GenerationSelector::Head, CheckoutMode::read_only_pinned(),
        WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    assert!(checkout.work.allocation_operations >= 3, "initial proof owns three admitted Arc payloads");
    assert!(checkout.work.peak_allocation_bytes >= checkout.value.logical_owned_bytes());
    let checkout = checkout.value;
    let failed = match checkout.private_candidate_measured(WorkBudget::default(), &cancellation) {
        Err(failure) => failure,
        Ok(_) => return Err("zero allocation budget unexpectedly accepted a candidate".into()),
    };
    assert!(matches!(failed.error, FsError::Work(_)));
    assert_eq!(*failed.work, WorkCounters::default());
    let candidate = checkout.private_candidate_measured(WorkBudget::UNBOUNDED, &cancellation)?;
    assert!(candidate.work.allocation_operations > 0);
    assert!(candidate.work.peak_allocation_bytes >= candidate.value.logical_owned_bytes());
    assert!(!Arc::ptr_eq(&candidate.value.dependencies.proof, &checkout.dependencies.proof));
    assert!(Arc::ptr_eq(
        candidate.value.dependencies.proof().observed_paths.as_ref().expect("constructor map"),
        checkout.dependencies.proof().observed_paths.as_ref().expect("constructor map"),
    ));
    assert!(candidate.value.root == checkout.root);
    assert_eq!(candidate.value.base_root, checkout.base_root);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failed = match checkout.private_candidate_measured(WorkBudget::UNBOUNDED, &cancelled) {
        Err(failure) => failure,
        Ok(_) => return Err("cancelled candidate unexpectedly succeeded".into()),
    };
    assert!(matches!(failed.error, FsError::Cancelled(_)));
    assert_eq!(*failed.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn fresh_checkout_control_custody_uses_its_actual_constructor_and_last_proof_alias()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("fresh-checkout-control").await?;
    workspace.write_text("/file", "body").await?;
    let cancellation = CancellationToken::new();
    let receipt = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    let checkout = receipt.value;
    let custody = checkout.private_control_custody();
    let proof_bytes = size_of::<Mutex<Proof>>() as u64;
    let map_bytes = CheckoutDependencies::empty_logical_owned_bytes() as u64;
    let path_bytes = size_of::<ObservedPathMap>() as u64;
    let parents = root_parent_bytes(&checkout.base_root)?
        + root_parent_bytes(&checkout.root)? * (1 + u64::from(checkout.root.has_hold()));
    assert!(parents > 0);
    assert_eq!(custody.physical_retained_bytes(), proof_bytes + parents + map_bytes + path_bytes);
    assert_eq!(custody.retained_bytes(), 0);
    assert!(receipt.work.peak_allocation_bytes >= proof_bytes + parents + map_bytes + path_bytes + custody.logical_owned_bytes());
    let alias = Arc::clone(&checkout.dependencies.proof);
    let map_alias = checkout.dependencies.proof().dependencies.clone();
    let path_alias = Arc::clone(checkout.dependencies.proof().observed_paths.as_ref().expect("constructor map"));
    let mut borrowed = map_alias.clone();
    assert!(!borrowed.attach_fresh_control_custody(&custody),
        "fresh constructor attachment never copies or adopts a shared authority map");
    drop(borrowed);
    cancellation.cancel();
    drop(checkout);
    assert_eq!(custody.physical_retained_bytes(), proof_bytes + map_bytes + path_bytes);
    drop(alias);
    assert_eq!(custody.physical_retained_bytes(), map_bytes + path_bytes,
        "actual map payloads outlive the independent proof mutex");
    drop(map_alias);
    assert_eq!(custody.physical_retained_bytes(), path_bytes);
    drop(path_alias);
    assert_eq!(custody.physical_retained_bytes(), 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_private_ctor_retains_original_mutex_until_last_proof_alias_and_retires_failed_copies()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("native-private-ctor-custody").await?;
    workspace.write_text("/seed", "body").await?;
    let active = CancellationToken::new();
    let checkout = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &active,
    ).await?.value;
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &active)
        .map_err(|(cause, _)| cause)?;
    let candidate = checkout.private_candidate_with_native_work(&original, &active)?;
    let custody = candidate.private_control_custody();
    let proof_bytes = size_of::<Mutex<Proof>>() as u64;
    let parents = root_parent_bytes(&candidate.base_root)?
        + root_parent_bytes(&candidate.root)? * (1 + u64::from(candidate.root.has_hold()));
    assert!(parents > 0);
    assert_eq!(custody.native_control_events(), (u128::from(proof_bytes + parents), 0));
    assert_eq!(custody.non_native_retained_payload_bytes(), 0,
        "borrowed source map storage never becomes this original constructor's payload");
    let alias = Arc::clone(&candidate.dependencies.proof);
    let live = original.live_allocation_bytes();
    drop(candidate);
    assert_eq!(custody.physical_retained_bytes(), proof_bytes);
    assert_eq!(custody.native_control_events(),
        (u128::from(proof_bytes + parents), u128::from(parents)));
    assert_eq!(original.live_allocation_bytes(), live - parents);
    drop(alias);
    assert_eq!(custody.physical_retained_bytes(), 0);
    assert_eq!(custody.native_control_events(),
        (u128::from(proof_bytes + parents), u128::from(proof_bytes + parents)));
    assert_eq!(original.live_allocation_bytes(), live - parents - proof_bytes,
        "registry/token metadata is separate from actual retired primitive bytes");
    let bounded = crate::NativeMaterializationWork::new(
        WorkBudget { bytes_copied: size_of::<Proof>() as u64, ..WorkBudget::UNBOUNDED },
        &active,
    ).map_err(|(cause, _)| cause)?;
    let failed = checkout.private_candidate_with_native_work(&bounded, &active)
        .err().ok_or("copy-limited original ctor installed parent buffers")?;
    assert!(matches!(failed, FsError::Work(_)));
    let prefix = bounded.receipt();
    assert_eq!(prefix.bytes_copied, size_of::<Proof>() as u64);
    assert!(prefix.allocation_operations >= 3, "actual state, token and mutex constructors ran");
    assert!(bounded.live_allocation_bytes() + proof_bytes <= prefix.peak_allocation_bytes,
        "failed ctor retires its actual mutex/root copies, not its retained registry metadata");
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_proof_copy_admission_atomically_gates_original_and_resource_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let copy = WorkCounters { bytes_copied: 3, items_examined: 1, ..WorkCounters::UNCHARGED };
    for budget in [
        WorkBudget { peak_allocation_bytes: 2, ..WorkBudget::UNBOUNDED },
        WorkBudget { bytes_copied: 2, ..WorkBudget::UNBOUNDED },
        WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED },
    ] {
        let denied = original.native_volume_work(budget)?;
        let before = original.receipt();
        assert!(denied.claim_allocation_with_work(3, 1, copy).is_err());
        assert_eq!(original.receipt(), before,
            "a denied copy/allocation bundle records neither imaginary copies nor allocation attempts");
    }
    let scope = original.native_volume_work(WorkBudget {
        peak_allocation_bytes: 3, bytes_copied: 3, items_examined: 1,
        allocation_operations: 1, ..WorkBudget::UNBOUNDED
    })?;
    let before = original.receipt();
    let live = original.live_allocation_bytes();
    let allocation = scope.claim_allocation_with_work(3, 1, copy)?;
    // Cancellation cannot revoke an already granted infallible copy permit.
    cancellation.cancel();
    let payload = (b"abc".to_vec(), allocation);
    assert_eq!(payload.1.bytes(), payload.0.capacity() as u64);
    let after = original.receipt();
    assert_eq!(after.bytes_copied - before.bytes_copied, 3);
    assert_eq!(after.items_examined - before.items_examined, 1);
    assert_eq!(after.allocation_operations - before.allocation_operations, 1);
    assert_eq!(original.live_allocation_bytes(), live + 3);
    drop(payload);
    assert_eq!(original.live_allocation_bytes(), live);
    Ok(())
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
#[tokio::test]
async fn native_observer_frontdoors_share_semantic_proof_but_only_adopt_their_actual_constructors()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("native-observer-frontdoor-custody").await?;
    workspace.write_text("/seed", "body").await?;
    let active = CancellationToken::new();
    let checkout = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &active,
    ).await?.value;
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &active)
        .map_err(|(cause, _)| cause)?;
    let observer = checkout.observer_with_native_work(&original, &active)?;
    assert!(Arc::ptr_eq(&checkout.dependencies.proof, &observer.checkout.dependencies.proof),
        "a semantic observer aliases the existing actual proof, never clones its mutex");
    let custody = observer.checkout.private_control_custody();
    assert!(!Arc::ptr_eq(&custody, &checkout.dependencies.data_custody));
    let parents = root_parent_bytes(&observer.checkout.base_root)?
        + root_parent_bytes(&observer.checkout.root)? * (1 + u64::from(observer.checkout.root.has_hold()));
    assert!(parents > 0);
    assert_eq!(custody.physical_retained_bytes(), parents);
    assert_eq!(custody.retained_bytes(), 0);
    assert_eq!(custody.non_native_retained_payload_bytes(), 0);
    assert_eq!(custody.native_control_events(), (u128::from(parents), 0));
    let proof_alias = Arc::clone(&observer.checkout.dependencies.proof);
    let live = original.live_allocation_bytes();
    drop(observer);
    assert_eq!(custody.physical_retained_bytes(), 0,
        "copied observer roots retire even while the borrowed source proof remains live");
    assert_eq!(custody.native_control_events(), (u128::from(parents), u128::from(parents)));
    assert_eq!(original.live_allocation_bytes(), live - parents);
    assert!(Arc::ptr_eq(&proof_alias, &checkout.dependencies.proof));
    drop(proof_alias);

    let inspector = checkout.inspector_with_native_work(&original, &active)?;
    assert!(!Arc::ptr_eq(&checkout.dependencies.proof, &inspector.checkout.dependencies.proof));
    assert!(!inspector.checkout.dependencies.observing);
    let inspector_custody = inspector.checkout.private_control_custody();
    let proof_bytes = size_of::<Mutex<Proof>>() as u64;
    let alias = Arc::clone(&inspector.checkout.dependencies.proof);
    drop(inspector);
    assert_eq!(inspector_custody.physical_retained_bytes(), proof_bytes,
        "the independent inspector's actual mutex follows its last proof alias");
    drop(alias);
    assert_eq!(inspector_custody.physical_retained_bytes(), 0);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let before = original.receipt();
    let live = original.live_allocation_bytes();
    assert!(matches!(checkout.observer_with_native_work(&original, &cancelled),
        Err(FsError::Cancelled(_))));
    assert!(matches!(checkout.inspector_with_native_work(&original, &cancelled),
        Err(FsError::Cancelled(_))));
    assert_eq!(original.receipt(), before);
    assert_eq!(original.live_allocation_bytes(), live);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_observed_path_claims_original_data_once_until_last_proof_alias()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("native-observed-path-custody").await?;
    let cancellation = CancellationToken::new();
    let checkout = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let candidate = checkout.private_candidate_with_native_work(&original, &cancellation)?;
    let custody = candidate.private_control_custody();
    let observed = path("native-owned-path")?;
    let expected = path_clone_data(&observed).0;
    let before = original.receipt();
    let mut staged = original.claim(0)?;
    candidate.dependencies.observe_paths_with_native_work(
        candidate.base_root.file_table, [&observed], true, Vec::new(), &mut staged,
        candidate.volume.config.limits.maximum_checkout_dependencies, &original, &cancellation,
    )?;
    let after = original.receipt();
    assert_eq!(after.bytes_copied - before.bytes_copied, expected);
    assert_eq!(custody.retained_bytes(), expected);
    assert_eq!(custody.non_native_retained_payload_bytes(), 0);
    assert_eq!(custody.pending_data_work(), WorkCounters::UNCHARGED,
        "direct original claims cannot be imported again as child SDK work");
    assert_eq!(candidate.proof_data_snapshot_completed().total_bytes()?, expected);
    let alias = Arc::clone(&candidate.dependencies.proof);
    drop(candidate);
    assert_eq!(custody.retained_bytes(), expected, "the actual path still belongs to the proof");
    let live = original.live_allocation_bytes();
    drop(alias);
    assert_eq!(custody.retained_bytes(), 0);
    assert!(original.live_allocation_bytes() + expected <= live,
        "last physical proof drop retires its DATA and known map/mutex payloads");
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_copied_record_compiler_preserves_real_payload_and_authored_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-copied-record").await?;
    workspace.write_text("/source", "authenticated source bytes").await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let source = checkout.lookup_no_follow(
        &path("source")?, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record.ok_or("real source record absent")?;
    checkout.bind_authored_operation(OperationId::new());
    let expected = checkout.compile_copied_record_measured(
        path("copy")?, source, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let baseline = original.live_allocation_bytes();
    let requested = path("copy")?;
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    let (paid_path, allocation) = requested.clone_with_accounting(&mut accounting, &cancellation)?;
    let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) = allocation else {
        return Err("paid path lost its original custody".into());
    };
    let compiled = checkout.compile_copied_record_with_native_work(
        paid_path, source, allocation, &original, &cancellation,
    ).await?;
    let Mutation::Create { record, path: destination } = &compiled.0.0 else {
        return Err("regular source did not compile a canonical create".into());
    };
    assert_eq!(destination, &requested);
    assert_eq!(record.file_id, expected.1, "original and ordinary lineage derives the same ID");
    assert_ne!(record.file_id, source.file_id);
    assert_eq!(record.payload, source.payload, "no replacement or fake content root");
    assert_eq!(record.metadata, source.metadata);
    assert_eq!(record.link_count, 1);
    assert_eq!(compiled.1.bytes(), destination.logical_owned_bytes());
    assert!(compiled.1.work().shares_ledger(&original));
    assert_eq!(original.receipt().backend_read_operations, 0);
    assert_eq!(original.receipt().backend_write_operations, 0);
    drop(compiled);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_copied_directory_and_metadata_denial_precede_staged_effects()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-copied-denial").await?;
    workspace.write_text("/source", "existing nonempty directory contents").await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let root = NamespacePath::new(Vec::new(), workspace.limits())?;
    let source = checkout.lookup_no_follow(
        &root, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record.ok_or("authenticated directory absent")?;
    assert_eq!(source.kind, FileKind::Directory);
    let requested = path("copy")?;
    let original = crate::NativeMaterializationWork::new(
        WorkBudget { bytes_copied: 4, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).map_err(|(cause, _)| cause)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    let (paid_path, allocation) = requested.clone_with_accounting(&mut accounting, &cancellation)?;
    let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) = allocation else {
        return Err("paid path lost its original custody".into());
    };
    let failure = checkout.compile_copied_record_with_native_work(
        paid_path, source, allocation, &original, &cancellation,
    ).await.err().ok_or("copy denial staged a directory")?;
    assert!(matches!(failure, FsError::Work(_)));
    assert_eq!(original.receipt().bytes_copied, 4);
    assert_eq!(original.receipt().bytes_encoded, 0);
    assert_eq!(original.receipt().backend_write_operations, 0);
    assert_eq!(original.live_allocation_bytes(), baseline);
    let metadata = checkout.lookup_no_follow_with_metadata(
        &root, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.ok_or("authenticated metadata absent")?.metadata;
    let original = crate::NativeMaterializationWork::new(
        WorkBudget { bytes_encoded: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).map_err(|(cause, _)| cause)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    let failure = checkout.stage_metadata_with_accounting(
        metadata, &mut accounting, &cancellation,
    ).await.err().ok_or("metadata encoder denial reached object write")?;
    assert!(matches!(failure, FsError::Work(_)));
    assert_eq!(original.receipt().backend_write_operations, 0);
    assert_eq!(original.receipt().bytes_encoded, 0);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_identity_mutation_preserves_source_and_cancelled_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-identity-candidate").await?;
    workspace.write_text("/source", "actual source bytes").await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace.checkout_measured(
        GenerationSelector::Head, CheckoutMode::tracking_transaction(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let path = path("source")?;
    let source = checkout.lookup_no_follow(
        &path, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record.ok_or("actual source record absent")?;
    let original = crate::NativeMaterializationWork::new(
        WorkBudget::UNBOUNDED, &cancellation,
    ).map_err(|(cause, _)| cause)?;
    let mut candidate = checkout.private_candidate_with_native_work(
        &original, &cancellation,
    )?;
    let paid = |mutation| -> Result<(Vec<Mutation>, crate::NativeAllocation), FsError> {
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let mut operations = crate::kernel::Owned::<Vec<Mutation>>::allocate(1, &mut accounting)?;
        accounting.charge(WorkCounters {
            bytes_copied: size_of::<Mutation>() as u64, ..WorkCounters::UNCHARGED
        })?;
        operations.value.push(Mutation::File { file_id: source.file_id, mutation });
        let KernelOwnedAllocation::NativeOriginal(allocation) = operations.allocation else {
            unreachable!("native context returns original input custody")
        };
        Ok((operations.value, allocation))
    };
    let input = paid(FileMutation::Resize { logical_bytes: 0 })?;
    candidate.mutate_with_native_work(input.0, input.1, &original, &cancellation).await?;
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let current = crate::kernel::lookup_file_record_async_with_accounting(
        &fs.inner.objects, candidate.root.file_table, source.file_id,
        DecodeLimits::for_volume(candidate.volume.config), &mut accounting, &cancellation, None,
    ).await?;
    let current = current.0.ok_or("mutated identity disappeared")?;
    assert_eq!(current.file_id, source.file_id);
    assert_ne!(current.payload, source.payload);
    assert!(candidate.root.same_original_scope(original.native_read_meter()));
    assert_eq!(candidate.base_file_table, checkout.root.file_table);
    assert_eq!(checkout.lookup_no_follow(
        &path, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record, Some(source));
    let before = (
        candidate.root.file_table, candidate.base_file_table,
        candidate.authored_operation_id, candidate.live_operation_id,
    );
    let baseline = original.live_allocation_bytes();
    let input = paid(FileMutation::Resize { logical_bytes: 7 })?;
    let before_io = original.receipt();
    cancellation.cancel();
    let failure = candidate.mutate_with_native_work(
        input.0, input.1, &original, &cancellation,
    ).await.err().ok_or("cancelled mutation changed candidate")?;
    assert!(matches!(failure,
        FsError::Cancelled(_) |
        FsError::NativeKernel(crate::NativeTreeMaterializationError::Cancelled)));
    assert_eq!(before, (
        candidate.root.file_table, candidate.base_file_table,
        candidate.authored_operation_id, candidate.live_operation_id,
    ));
    assert_eq!(original.receipt().backend_read_operations, before_io.backend_read_operations);
    assert_eq!(original.receipt().backend_write_operations, before_io.backend_write_operations);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn private_control_custody_retires_with_actual_root_buffers_and_last_proof_alias()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("private-control-retirement").await?;
    workspace.write_text("/file", "body").await?;
    let cancellation = CancellationToken::new();
    let checkout = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let candidate = checkout.private_candidate_measured(WorkBudget::UNBOUNDED, &cancellation)?.value;
    let custody = candidate.private_control_custody();
    let proof_bytes = size_of::<Mutex<Proof>>() as u64;
    let parent_bytes = root_parent_bytes(&candidate.base_root)?
        + root_parent_bytes(&candidate.root)? * (1 + u64::from(candidate.root.has_hold()));
    assert!(parent_bytes > 0);
    assert_eq!(custody.physical_retained_bytes(), proof_bytes + parent_bytes);
    assert_eq!(custody.retained_bytes(), 0, "constructor control is not observer DATA");
    assert_eq!(custody.retained_payload_bytes(), proof_bytes + parent_bytes);
    let source_custody = checkout.private_control_custody();
    assert!(!Arc::ptr_eq(&source_custody, &custody),
        "a private copy does not adopt its source's constructor claims");
    assert_eq!(source_custody.physical_retained_bytes(),
        proof_bytes + CheckoutDependencies::empty_logical_owned_bytes() as u64 + size_of::<ObservedPathMap>() as u64
            + root_parent_bytes(&checkout.base_root)?
            + root_parent_bytes(&checkout.root)? * (1 + u64::from(checkout.root.has_hold())));
    let ordinary = candidate.private_candidate();
    assert_eq!(ordinary.private_control_custody().physical_retained_bytes(), 0,
        "a clone clears inherited primitive tags");
    assert!(Arc::ptr_eq(ordinary.dependencies.proof().observed_paths.as_ref().expect("constructor map"),
        candidate.dependencies.proof().observed_paths.as_ref().expect("constructor map")));
    drop(ordinary);
    let alias = Arc::clone(&candidate.dependencies.proof);
    let observed = Arc::clone(candidate.dependencies.proof().observed_paths.as_ref().expect("constructor map"));
    cancellation.cancel();
    drop(candidate);
    assert_eq!(custody.physical_retained_bytes(), proof_bytes,
        "the by-value Checkout dies before its aliased Mutex proof");
    drop(alias);
    assert_eq!(custody.physical_retained_bytes(), 0,
        "an immutable map alias does not retain independent control");
    drop(observed);
    let candidate = checkout.private_candidate_measured(
        WorkBudget::UNBOUNDED, &CancellationToken::new(),
    )?.value;
    let custody = candidate.private_control_custody();
    let alias = Arc::clone(&candidate.dependencies.proof);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _candidate = candidate;
        panic!("unwind the private producer's by-value owner");
    })).is_err());
    assert_eq!(custody.physical_retained_bytes(), proof_bytes);
    drop(alias);
    assert_eq!(custody.physical_retained_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn private_mutation_root_vectors_follow_their_actual_owner_across_replacements()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("private-mutation-root-custody").await?;
    workspace.write_text("/seed", "body").await?;
    let active = CancellationToken::new();
    let checkout = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &active,
    ).await?.value;
    let mut candidate = checkout.private_candidate_measured(WorkBudget::UNBOUNDED, &active)?.value;
    let custody = candidate.private_control_custody();
    let parents = candidate.root.parents.clone();
    assert!(!parents.is_empty());
    let base = candidate.base_root.clone();
    let generation = candidate.generation_id();
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let original_control = custody.physical_retained_bytes();
    let failed = candidate.create_file(
        path("cancelled")?, bytes::Bytes::from_static(b"body"), WorkBudget::UNBOUNDED, &cancelled,
    ).await.err().ok_or("cancelled mutation changed the working roots")?;
    assert!(matches!(failed.error, FsError::Cancelled(_) | FsError::Mutation(_)));
    assert_eq!(custody.physical_retained_bytes(), original_control);
    assert_eq!(candidate.base_root, base);
    for name in ["first", "second"] {
        let receipt = candidate.create_file(
            path(name)?, bytes::Bytes::from_static(b"body"), WorkBudget::UNBOUNDED, &active,
        ).await?;
        assert!(receipt.work.allocation_operations > 0);
        assert_eq!(candidate.generation_id(), generation);
        assert_eq!(candidate.base_root, base);
        assert_eq!(candidate.root.parents, parents);
    }
    // Moving the exact vector owner must not retire its control by dropping
    // Checkout or its proof. This also catches old-vector tags left behind by
    // either replacement, independently of dependency-map custody.
    let roots = std::mem::replace(&mut candidate.root, checkout.root.replicate());
    let retained_roots = root_parent_bytes(&roots)?
        * (1 + u64::from(roots.has_hold()));
    assert!(retained_roots > 0);
    drop(candidate);
    assert_eq!(custody.physical_retained_bytes(), retained_roots);
    assert_eq!(custody.live_payload_counts(), (retained_roots, 0),
        "new operation vectors await original receipt delivery, unlike ctor standing");
    drop(roots);
    assert_eq!(custody.physical_retained_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn private_post_seal_roots_preserve_custody_and_cancelled_preparation_retires_copies()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("private-seal-root-custody").await?;
    let active = CancellationToken::new();
    let mut checkout = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &active,
    ).await?.value;
    checkout.create_file(path("pending")?, bytes::Bytes::from_static(b"body"), WorkBudget::UNBOUNDED, &active).await?;
    let mut candidate = checkout.private_candidate_measured(WorkBudget::UNBOUNDED, &active)?.value;
    let custody = candidate.private_control_custody();
    let original_root = candidate.generation_id();
    let original_control = custody.physical_retained_bytes();
    let denied = candidate.prepare_root_replacement(
        &candidate.base_root, &candidate.root,
        WorkBudget { peak_allocation_bytes: 0, ..WorkBudget::UNBOUNDED }, &active, None,
    );
    if !candidate.base_root.parents.is_empty()
        || (candidate.root.has_hold() && !candidate.root.parents.is_empty())
    {
        let failure = denied.err().ok_or("root copy ignored its zero capacity budget")?;
        assert!(matches!(failure.error, FsError::Work(_)));
        assert_eq!(failure.work.allocation_operations, 0);
    } else {
        drop(denied?);
    }
    assert_eq!(custody.physical_retained_bytes(), original_control);
    let cancelled = CancellationToken::new();
    let operation = OperationId::from_bytes([241; 16]);
    let failed = candidate.commit_with_permit_guarded(
        operation, PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancelled,
        |_| {
            assert!(custody.physical_retained_bytes() > original_control,
                "the authoritative pre-seal root copies have real retained tags");
            cancelled.cancel();
            async { Ok(FsReceipt { value: (), work: WorkCounters::UNCHARGED }) }
        },
    ).await.err().ok_or("a cancelled publication seal appended")?;
    assert!(matches!(failed.error, FsError::Publication(_) | FsError::Cancelled(_)));
    assert!(failed.work.allocation_operations > 0);
    assert_eq!(candidate.generation_id(), original_root);
    assert!(candidate.has_pending_mutations());
    assert_eq!(custody.physical_retained_bytes(), original_control,
        "cancelled preparation frees actual copies, not the installed producer");
    let panicked = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
        candidate.commit_with_permit_guarded::<FsError, _, _>(
            operation, PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &active,
            |_| async {
                assert!(custody.physical_retained_bytes() > original_control);
                panic!("unwind after authoritative root copies, before the original CAS");
            },
        ),
    )).await;
    assert!(panicked.is_err());
    assert_eq!(candidate.generation_id(), original_root);
    assert_eq!(custody.physical_retained_bytes(), original_control,
        "panic retires actual prepared buffers without releasing installed control");
    let committed = candidate.commit_with_permit_guarded::<FsError, _, _>(
        operation, PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &active,
        |_| async { Ok(FsReceipt { value: (), work: WorkCounters::UNCHARGED }) },
    ).await?;
    assert!(matches!(committed.value, CheckoutCommitOutcome::Committed { .. }));
    assert!(!candidate.has_pending_mutations());
    assert_eq!(candidate.base_root, candidate.root.clone());
    assert_eq!(candidate.root.parents, vec![checkout.generation_id()]);
    let proof_bytes = size_of::<Mutex<Proof>>() as u64;
    let parent_bytes = root_parent_bytes(&candidate.base_root)?
        + root_parent_bytes(&candidate.root)? * (1 + u64::from(candidate.root.has_hold()));
    assert_eq!(custody.physical_retained_bytes(), proof_bytes + parent_bytes,
        "post-seal replacement keeps only the actual installed vector capacities");
    let alias = Arc::clone(&candidate.dependencies.proof);
    drop(candidate);
    assert_eq!(custody.physical_retained_bytes(), proof_bytes);
    drop(alias);
    assert_eq!(custody.physical_retained_bytes(), 0);
    Ok(())
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn installed_private_candidate_keeps_its_original_control_custody_until_actual_destruction()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("installed-private-control").await?;
    workspace.write_text("/file", "body").await?;
    let cancellation = CancellationToken::new();
    let mut installed = workspace.checkout_measured(
        GenerationSelector::Head, tracking(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let base = installed.private_candidate_measured(WorkBudget::UNBOUNDED, &cancellation)?.value;
    let mut candidate = base.private_candidate_measured(WorkBudget::UNBOUNDED, &cancellation)?.value;
    let custody = candidate.private_control_custody();
    let control = custody.physical_retained_bytes();
    candidate.lookup_no_follow(&path("retained-observation")?, WorkBudget::UNBOUNDED, &cancellation).await?;
    assert_eq!(custody.physical_retained_bytes(),
        control + CheckoutDependencies::empty_logical_owned_bytes() as u64);
    assert!(custody.retained_bytes() > 0, "name/path DATA stays a separate custody class");
    let prepared = installed.prepare_candidate_adoption_measured(
        &base, &candidate, WorkBudget::UNBOUNDED, &cancellation,
    )?.value.ok_or("compatible private candidate was rejected")?;
    assert!(installed.adopt_prepared_candidate(&base, candidate, prepared));
    assert!(Arc::ptr_eq(&custody, &installed.private_control_custody()),
        "installation retains the same producer, not a scalar replacement claim");
    drop(base);
    cancellation.cancel();
    assert_eq!(custody.physical_retained_bytes(),
        control + CheckoutDependencies::empty_logical_owned_bytes() as u64);
    let _completed = installed.proof_data_snapshot_completed();
    let alias = Arc::clone(&installed.dependencies.proof);
    drop(installed);
    assert_eq!(custody.physical_retained_bytes(),
        size_of::<Mutex<Proof>>() as u64 + CheckoutDependencies::empty_logical_owned_bytes() as u64);
    drop(alias);
    assert_eq!(custody.physical_retained_bytes(), 0);
    Ok(())
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn borrowed_seal_tags_only_new_root_buffers_to_the_current_producer()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("borrowed-seal-root-custody").await?;
    workspace.write_text("/file", "body").await?;
    let active = CancellationToken::new();
    let mut checkout = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &active,
    ).await?.value;
    checkout.create_file(path("pending")?, bytes::Bytes::from_static(b"pending"), WorkBudget::UNBOUNDED, &active).await?;
    let mut service = checkout.private_candidate();
    let foreign = service.private_control_custody();
    let foreign_control = foreign.physical_retained_bytes();
    assert_eq!(foreign_control, 0, "preexisting service control belongs to no current producer");
    let projection = service.private_candidate_measured(WorkBudget::UNBOUNDED, &active)?;
    assert!(projection.work.allocation_operations > 0,
        "the current producer token is a real admitted SDK constructor, not an empty marker");
    let projection = projection.value;
    let producer = projection.private_control_custody();
    let producer_control = producer.physical_retained_bytes();
    assert!(!Arc::ptr_eq(&foreign, &producer));
    let borrowed_proof_alias = Arc::clone(&service.dependencies.proof);
    let cancelled = CancellationToken::new();
    let operation = OperationId::from_bytes([242; 16]);
    let denied = service.commit_with_permit_even_if_clean_guarded(
        operation, PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancelled,
        &mut |_| {
            assert_eq!(foreign.physical_retained_bytes(), foreign_control);
            assert!(producer.physical_retained_bytes() > producer_control);
            cancelled.cancel();
            async { Ok::<_, OperationFailure<FsError>>(FsReceipt { value: (), work: WorkCounters::UNCHARGED }) }
        },
        Some(&producer),
    ).await.err().ok_or("cancelled borrowed seal published")?;
    assert!(denied.work.allocation_operations > 0);
    assert_eq!(foreign.physical_retained_bytes(), foreign_control);
    assert_eq!(producer.physical_retained_bytes(), producer_control,
        "cancelled new copies retire from only their current producer");
    let committed = service.commit_with_permit_even_if_clean_guarded(
        operation, PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &active,
        &mut |_| async { Ok::<_, OperationFailure<FsError>>(FsReceipt { value: (), work: WorkCounters::UNCHARGED }) },
        Some(&producer),
    ).await?;
    assert!(matches!(committed.value, CheckoutCommitOutcome::Committed { .. }));
    let new_vectors = root_parent_bytes(&service.base_root)?
        + root_parent_bytes(&service.root)? * (1 + u64::from(service.root.has_hold()));
    assert_eq!(foreign.physical_retained_bytes(), 0,
        "new buffers are tagged even when the borrowed service had no private producer tags");
    assert_eq!(producer.physical_retained_bytes(), producer_control + new_vectors);
    drop(projection);
    assert_eq!(producer.physical_retained_bytes(), new_vectors,
        "only genuine new seal buffers remain in the current producer");
    drop(service);
    assert_eq!(producer.physical_retained_bytes(), 0,
        "a borrowed proof alias never becomes current-producer control");
    assert_eq!(foreign.physical_retained_bytes(), 0);
    drop(borrowed_proof_alias);
    assert_eq!(foreign.physical_retained_bytes(), 0);
    Ok(())
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn borrowed_companion_seal_preserves_nonstatic_owner_cause_before_cas()
-> Result<(), Box<dyn std::error::Error>> {
    enum OwnerError<'a> { Fs(FsError), Lost(&'a str) }
    impl<'a> From<FsError> for OwnerError<'a> {
        fn from(error: FsError) -> Self { Self::Fs(error) }
    }
    let fs = Fs::memory();
    let workspace = fs.create_workspace("borrowed-companion-owner-cause").await?;
    let cancellation = CancellationToken::new();
    let mut service = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let original_head = service.authority_head;
    let original_root = service.generation_root;
    let custody = service.private_control_custody();
    let retained_before = custody.physical_retained_bytes();
    let reason = String::from("borrowed source owner changed");
    let mut guard = |_| std::future::ready(Err::<FsReceipt<()>, _>(
        OperationFailure::before_work(OwnerError::Lost(reason.as_str())),
    ));
    let failure = service.commit_with_permit_even_if_clean_guarded(
        OperationId::from_bytes([243; 16]), PublicationPermit::Unrestricted,
        WorkBudget::UNBOUNDED, &cancellation, &mut guard, None,
    ).await.err().ok_or("borrowed owner failure unexpectedly published")?;
    assert_eq!(failure.work.authority_records_appended, 0);
    assert!(failure.work.bytes_hashed > 0);
    match failure.error {
        OwnerError::Lost(actual) => assert!(std::ptr::eq(actual, reason.as_str())),
        OwnerError::Fs(error) => return Err(error.into()),
    }
    assert_eq!(service.authority_head, original_head);
    assert_eq!(service.generation_root, original_root);
    assert_eq!(custody.physical_retained_bytes(), retained_before);
    let reopened = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    assert_eq!(reopened.authority_head, original_head);
    assert_eq!(reopened.generation_root, original_root);
    Ok(())
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn measured_candidate_adoption_seals_actual_proof_and_preserves_failure_prefixes()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("measured-candidate-adoption").await?;
    let cancellation = CancellationToken::new();
    let mut live = workspace.checkout_measured(
        GenerationSelector::Head, tracking(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let base = live.private_candidate();
    let mut candidate = base.private_candidate();
    let candidate_path = path("candidate-observation")?;
    let live_path = path("reader-observation")?;
    candidate.lookup_no_follow(&candidate_path, WorkBudget::UNBOUNDED, &cancellation).await?;
    live.lookup_no_follow(&live_path, WorkBudget::UNBOUNDED, &cancellation).await?;
    let expected = candidate.dependencies.proof().dependencies
        .merged(&live.dependencies.proof().dependencies).ok_or("compatible proofs rejected")?;

    let denied = match live.prepare_candidate_adoption_measured(
        &base, &candidate,
        WorkBudget { peak_allocation_bytes: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
    ) {
        Err(failure) => failure,
        Ok(_) => return Err("zero DATA budget admitted proof copies".into()),
    };
    assert!(matches!(denied.error, FsError::Work(_)));
    assert_eq!(denied.work.bytes_copied, 0);
    assert_eq!(denied.work.allocation_operations, 0);
    assert!(denied.work.items_examined > 0);
    let denied = match live.prepare_candidate_adoption_measured(
        &base, &candidate,
        WorkBudget { allocation_operations: 1, ..WorkBudget::UNBOUNDED }, &cancellation,
    ) {
        Err(failure) => failure,
        Ok(_) => return Err("one allocation admitted two name copies".into()),
    };
    assert!(matches!(denied.error, FsError::Work(_)));
    assert_eq!(denied.work.allocation_operations, 1);
    assert_eq!(denied.work.bytes_copied, candidate_path.components()[0].as_bytes().len() as u64);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let denied = match live.prepare_candidate_adoption_measured(
        &base, &candidate, WorkBudget::UNBOUNDED, &cancelled,
    ) {
        Err(failure) => failure,
        Ok(_) => return Err("cancelled proof preparation succeeded".into()),
    };
    assert!(matches!(denied.error, FsError::Cancelled(_)));
    assert_eq!(*denied.work, WorkCounters::default());

    let prepared = live.prepare_candidate_adoption_measured(
        &base, &candidate, WorkBudget::UNBOUNDED, &cancellation,
    )?;
    let name_bytes = (candidate_path.components()[0].as_bytes().len()
        + live_path.components()[0].as_bytes().len()) as u64;
    assert_eq!(prepared.work.bytes_copied, name_bytes);
    assert_eq!(prepared.work.allocation_operations, 3);
    let prepared = prepared.value.ok_or("compatible candidate rejected")?;
    assert_eq!(prepared.retained_bytes(),
        name_bytes + CheckoutDependencies::empty_logical_owned_bytes() as u64);
    let mut ordinary = live.private_candidate();
    assert!(ordinary.admits_candidate(&base, &candidate));
    assert!(ordinary.adopt_candidate(&base, candidate.private_candidate()));
    assert_eq!(ordinary.dependencies.proof().dependencies, expected);
    assert!(live.adopt_prepared_candidate(&base, candidate, prepared));
    assert_eq!(live.dependencies.proof().dependencies, expected);
    assert!(live.dependencies.proof().observes_path(base.base_file_table, &candidate_path, true));
    Ok(())
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn prepared_candidate_adoption_rejects_new_observations_and_changed_base_state()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("candidate-adoption-seal").await?;
    let cancellation = CancellationToken::new();
    let mut live = workspace.checkout_measured(
        GenerationSelector::Head, tracking(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let base = live.private_candidate();
    let mut candidate = base.private_candidate();
    let prepared = live.prepare_candidate_adoption_measured(
        &base, &candidate, WorkBudget::UNBOUNDED, &cancellation,
    )?;
    assert_eq!(prepared.work.allocation_operations, 0, "identical proof is only shared");
    assert_eq!(prepared.value.as_ref().ok_or("shared proof rejected")?.retained_bytes(), 0);
    live.lookup_no_follow(&path("later-reader")?, WorkBudget::UNBOUNDED, &cancellation).await?;
    assert!(!live.adopt_prepared_candidate(
        &base, candidate.private_candidate(), prepared.value.ok_or("shared proof rejected")?,
    ));
    let prepared = live.prepare_candidate_adoption_measured(
        &base, &candidate, WorkBudget::UNBOUNDED, &cancellation,
    )?.value.ok_or("reader observations rejected")?;
    candidate.lookup_no_follow(&path("later-candidate")?, WorkBudget::UNBOUNDED, &cancellation).await?;
    assert!(!live.adopt_prepared_candidate(&base, candidate.private_candidate(), prepared));
    let prepared = live.prepare_candidate_adoption_measured(
        &base, &candidate, WorkBudget::UNBOUNDED, &cancellation,
    )?.value.ok_or("compatible candidate rejected")?;
    live.live_operation_id = Some(OperationId::from_bytes([209; 16]));
    assert!(!live.adopt_prepared_candidate(&base, candidate.private_candidate(), prepared));
    assert!(live.prepare_candidate_adoption_measured(
        &base, &candidate, WorkBudget::UNBOUNDED, &cancellation,
    )?.value.is_none());
    assert!(!live.admits_candidate(&base, &candidate));
    Ok(())
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn prepared_merge_custody_retires_precommit_copies_and_follows_actual_map_aliases()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("prepared-merge-custody").await?;
    let cancellation = CancellationToken::new();
    let mut installed = workspace.checkout_measured(
        GenerationSelector::Head, tracking(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let base = installed.private_candidate_measured(WorkBudget::UNBOUNDED, &cancellation)?.value;
    let mut candidate = base.private_candidate_measured(WorkBudget::UNBOUNDED, &cancellation)?.value;
    let own_path = path("producer-name")?;
    let reader_path = path("borrowed-reader-name")?;
    candidate.lookup_no_follow(&own_path, WorkBudget::UNBOUNDED, &cancellation).await?;
    installed.lookup_no_follow(&reader_path, WorkBudget::UNBOUNDED, &cancellation).await?;
    let custody = candidate.private_control_custody();
    let before_data = custody.retained_bytes();
    let before_control = custody.physical_retained_bytes();
    let names = (own_path.components()[0].as_bytes().len()
        + reader_path.components()[0].as_bytes().len()) as u64;
    let map_bytes = CheckoutDependencies::empty_logical_owned_bytes() as u64;

    let prepared = installed.prepare_candidate_adoption_measured(
        &base, &candidate, WorkBudget::UNBOUNDED, &cancellation,
    )?.value.ok_or("compatible candidate rejected")?;
    assert_eq!(custody.retained_bytes(), before_data + names);
    assert_eq!(custody.physical_retained_bytes(), before_control + map_bytes);
    drop(prepared); // e.g. an authority fence rejects before physical CAS.
    assert_eq!(custody.retained_bytes(), before_data);
    assert_eq!(custody.physical_retained_bytes(), before_control);

    let denied = match installed.prepare_candidate_adoption_measured(
        &base, &candidate,
        WorkBudget { allocation_operations: 1, ..WorkBudget::UNBOUNDED }, &cancellation,
    ) {
        Err(failure) => failure,
        Ok(_) => return Err("finite merge budget accepted two new name copies".into()),
    };
    assert_eq!(denied.work.allocation_operations, 1);
    assert_eq!(denied.work.bytes_copied, own_path.components()[0].as_bytes().len() as u64);
    assert_eq!(custody.retained_bytes(), before_data, "failed merge retires its actual first key");
    assert_eq!(custody.physical_retained_bytes(), before_control);

    let prepared = installed.prepare_candidate_adoption_measured(
        &base, &candidate, WorkBudget::UNBOUNDED, &cancellation,
    )?.value.ok_or("compatible candidate rejected")?;
    assert!(installed.adopt_prepared_candidate(&base, candidate, prepared));
    assert!(Arc::ptr_eq(&custody, &installed.private_control_custody()));
    assert_eq!(custody.physical_retained_bytes(), before_control,
        "installation retires the original candidate map before retaining only the new map");
    let proof_alias = Arc::clone(&installed.dependencies.proof);
    let map_alias = installed.dependencies.proof().dependencies.clone();
    drop(installed);
    assert_eq!(custody.physical_retained_bytes(), size_of::<Mutex<Proof>>() as u64 + map_bytes);
    drop(proof_alias);
    assert_eq!(custody.physical_retained_bytes(), map_bytes, "actual map Arc still owns its payload");
    assert_eq!(custody.retained_bytes(), names, "only actual merged keys remain");
    drop(map_alias);
    assert_eq!(custody.retained_payload_bytes(), 0);
    Ok(())
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
#[tokio::test]
async fn native_candidate_adoption_enforces_full_resource_copies_and_actual_map_retirement()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::allocation::KernelAccounting;
    use crate::kernel::Owned;
    let fs = Fs::memory();
    let workspace = fs.create_workspace("native-candidate-adoption-resource").await?;
    let cancellation = CancellationToken::new();
    let seed = workspace.checkout_measured(
        GenerationSelector::Head, tracking(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let mut installed = seed.private_candidate_with_native_work(&original, &cancellation)?;
    let base = installed.private_candidate_with_native_work(&original, &cancellation)?;
    let mut candidate = base.private_candidate_with_native_work(&original, &cancellation)?;
    let own_path = path("candidate-proof-key")?;
    let reader_path = path("reader-proof-key")?;
    candidate.lookup_no_follow_with_native_work(&own_path, &original, &cancellation).await?;
    installed.lookup_no_follow_with_native_work(&reader_path, &original, &cancellation).await?;
    let owner = candidate.private_control_custody();
    assert_eq!(owner.non_native_retained_payload_bytes(), 0);
    let before_payload = owner.retained_payload_bytes();
    let expected = candidate.dependencies.proof().dependencies
        .merged(&installed.dependencies.proof().dependencies).ok_or("compatible proofs rejected")?;
    let own_bytes = own_path.components()[0].as_bytes().len() as u64;
    let names = own_bytes + reader_path.components()[0].as_bytes().len() as u64;
    let map_bytes = CheckoutDependencies::empty_logical_owned_bytes() as u64;

    let denied_scope = original.native_volume_work(WorkBudget {
        peak_allocation_bytes: 0, ..WorkBudget::UNBOUNDED
    })?;
    let before = original.receipt();
    let live = original.live_allocation_bytes();
    let failed = installed.prepare_candidate_adoption_with_native_work(
        &base, &candidate, &denied_scope, &cancellation,
    ).err().ok_or("standalone Fs budget bypassed the actual native resource ceiling")?;
    assert!(matches!(failed, FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(
        WorkError::BudgetExceeded { counter: "peak_allocation_bytes", .. }
    ))));
    assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
    assert_eq!(original.receipt().bytes_copied, before.bytes_copied);
    assert_eq!(original.live_allocation_bytes(), live);
    assert_eq!(owner.retained_payload_bytes(), before_payload);

    let partial_scope = original.native_volume_work(WorkBudget {
        allocation_operations: 1, ..WorkBudget::UNBOUNDED
    })?;
    let before = original.receipt();
    let live = original.live_allocation_bytes();
    let events = owner.native_control_events();
    assert!(matches!(installed.prepare_candidate_adoption_with_native_work(
        &base, &candidate, &partial_scope, &cancellation,
    ), Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(
        WorkError::BudgetExceeded { counter: "allocation_operations", .. }
    )))));
    assert_eq!(original.receipt().allocation_operations - before.allocation_operations, 1);
    assert_eq!(original.receipt().bytes_copied - before.bytes_copied, own_bytes);
    assert_eq!(owner.native_control_events(),
        (events.0 + u128::from(own_bytes), events.1 + u128::from(own_bytes)));
    assert_eq!(original.live_allocation_bytes(), live, "failed merge destroyed its actual first copied key");
    assert_eq!(owner.retained_payload_bytes(), before_payload);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let before = original.receipt();
    assert!(matches!(installed.prepare_candidate_adoption_with_native_work(
        &base, &candidate, &original, &cancelled,
    ), Err(FsError::Cancelled(_))));
    assert_eq!(original.receipt(), before);
    let foreign = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    assert!(matches!(installed.prepare_candidate_adoption_with_native_work(
        &base, &candidate, &foreign, &cancellation,
    ), Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout))));
    assert_eq!(original.receipt(), before, "a foreign original cannot claim this producer's proof");

    let scope = original.native_volume_work(WorkBudget {
        peak_allocation_bytes: names + map_bytes, ..WorkBudget::UNBOUNDED
    })?;
    let before = original.receipt();
    let live = original.live_allocation_bytes();
    let prepared = installed.prepare_candidate_adoption_with_native_work(
        &base, &candidate, &scope, &cancellation,
    )?.ok_or("compatible native candidate rejected")?;
    assert_eq!(prepared.retained_bytes(), names + map_bytes);
    assert_eq!(original.receipt().allocation_operations - before.allocation_operations, 3);
    assert_eq!(original.receipt().bytes_copied - before.bytes_copied, names);
    assert_eq!(original.live_allocation_bytes(), live + names + map_bytes);
    assert_eq!(owner.retained_payload_bytes(), before_payload + names + map_bytes);
    assert_eq!(owner.non_native_retained_payload_bytes(), 0,
        "actual new buffers own native resource tickets, not imported scalar bytes");
    assert_eq!(owner.pending_data_work(), WorkCounters::UNCHARGED,
        "direct original work must never be merged as another child receipt");
    drop(prepared); // An original authority fence can reject before installation.
    assert_eq!(original.live_allocation_bytes(), live);
    assert_eq!(owner.retained_payload_bytes(), before_payload);

    let prepared = installed.prepare_candidate_adoption_with_native_work(
        &base, &candidate, &scope, &cancellation,
    )?.ok_or("compatible native candidate rejected")?;
    let completed = original.receipt();
    assert!(installed.adopt_prepared_candidate(&base, candidate, prepared));
    assert_eq!(original.receipt(), completed, "physical adoption does not allocate or recharge native work");
    assert_eq!(installed.dependencies.proof().dependencies, expected);
    let map_alias = installed.dependencies.proof().dependencies.clone();
    drop(installed);
    assert_eq!(owner.retained_bytes(), names);
    assert_eq!(owner.physical_retained_bytes(), map_bytes);
    let mut accounting = KernelAccounting::native(&scope, &cancellation);
    assert!(matches!(Owned::<Vec<u8>>::allocate(1, &mut accounting),
        Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(
            WorkError::BudgetExceeded { counter: "peak_allocation_bytes", .. }
        )))), "the actual map alias still consumes its original resource allowance");
    drop(map_alias);
    assert_eq!(owner.retained_payload_bytes(), 0);
    let bytes = Owned::<Vec<u8>>::allocate((names + map_bytes) as usize, &mut accounting)?;
    assert_eq!(bytes.bytes, names + map_bytes, "actual map destruction released the same resource allowance");
    drop(bytes);
    Ok(())
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn proof_payload_reconciliation_mirrors_constructor_and_preserves_pending_map_data_peak()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("proof-payload-reconciliation").await?;
    let cancellation = CancellationToken::new();
    let base = workspace.checkout_measured(
        GenerationSelector::Head, tracking(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let candidate = base.private_candidate_measured(WorkBudget::UNBOUNDED, &cancellation)?.value;
    let owner = candidate.private_control_custody();
    let constructor = owner.physical_retained_bytes();
    assert!(constructor > 0);
    assert_eq!(owner.live_payload_counts(), (constructor, constructor));
    let observed_path = path("new-owned-name")?;
    let name_bytes = observed_path.components()[0].as_bytes().len() as u64;
    let map_bytes = CheckoutDependencies::empty_logical_owned_bytes() as u64;
    let mut proof = candidate.dependencies.proof();
    let extension = proof.dependencies.prepare_observations(vec![Dependency {
        region: DependencyRegion::DirectoryName {
            directory_id: candidate.base_root.root_file_id,
            name: observed_path.components()[0].clone(),
        },
        expected: crate::kernel::DependencyState::Absent,
    }], candidate.volume.config.limits.maximum_checkout_dependencies)?;
    let mut accounting = candidate.dependencies.accounting.lock();
    let mut work = WorkCounters::default();
    {
        let mut admission = ProofDataAdmission {
            ledger: ProofDataAdmissionLedger::Local {
                accounting: &mut accounting, work: &mut work,
                budget: WorkBudget {
                    peak_allocation_bytes: map_bytes + name_bytes,
                    ..WorkBudget::UNBOUNDED
                },
            },
            owner: &owner,
            cancellation: &cancellation,
        };
        admission.admit(0, 0, 0, 0)?;
        assert_eq!(match &admission.ledger {
            ProofDataAdmissionLedger::Local { work, .. } => **work,
            #[cfg(not(target_arch = "wasm32"))]
            ProofDataAdmissionLedger::Original { .. } => unreachable!("local admission fixture"),
        }, WorkCounters::default(),
            "mirroring real constructor custody is not a new allocation or peak");
        proof.dependencies.commit_with_data_custody(
            extension, Some(&owner),
            |bytes, operations, copied, items| admission.admit(bytes, operations, copied, items),
        )?;
    }
    assert_eq!(work.allocation_operations, 1);
    assert_eq!(work.bytes_copied, 0, "the staged name was moved, not copied");
    assert_eq!(work.peak_allocation_bytes, map_bytes + name_bytes,
        "pending map and DATA remain simultaneous without charging constructor twice");
    assert_eq!(owner.live_payload_counts(), (constructor + map_bytes + name_bytes, constructor));
    let delivered = owner.take_completed_data_work();
    assert_eq!(delivered.peak_allocation_bytes, map_bytes + name_bytes);
    assert_eq!(owner.live_payload_counts(),
        (constructor + map_bytes + name_bytes, constructor + map_bytes + name_bytes));
    let mut next_work = WorkCounters::default();
    {
        let mut admission = ProofDataAdmission {
            ledger: ProofDataAdmissionLedger::Local {
                accounting: &mut accounting, work: &mut next_work, budget: WorkBudget::default(),
            },
            owner: &owner, cancellation: &cancellation,
        };
        admission.admit(0, 0, 0, 0)?;
        assert_eq!(match &admission.ledger {
            ProofDataAdmissionLedger::Local { work, .. } => **work,
            #[cfg(not(target_arch = "wasm32"))]
            ProofDataAdmissionLedger::Original { .. } => unreachable!("local admission fixture"),
        }, WorkCounters::default(), "delivered payload is standing");
        proof.dependencies.clear();
        admission.reconcile()?;
        assert_eq!(match &admission.ledger {
            ProofDataAdmissionLedger::Local { accounting, .. } => accounting.allocations.live_bytes(),
            #[cfg(not(target_arch = "wasm32"))]
            ProofDataAdmissionLedger::Original { .. } => unreachable!("local admission fixture"),
        }, constructor, "actual key/map destruction retires only those exact mirrored payloads");
    }
    assert_eq!(owner.live_payload_counts(), (constructor, constructor));
    Ok(())
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn concurrent_observers_attribute_only_their_completed_name_and_path_data()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("observer-local-proof-data").await?;
    let cancellation = CancellationToken::new();
    let checkout = workspace.checkout_measured(
        GenerationSelector::Head, tracking(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let mut first = checkout.observer_measured(WorkBudget::UNBOUNDED, &cancellation)?.value;
    let mut second = checkout.observer_measured(WorkBudget::UNBOUNDED, &cancellation)?.value;
    assert!(Arc::ptr_eq(&first.dependencies.proof, &second.dependencies.proof));
    let first_path = path("short")?;
    let second_path = path("a-much-longer-independent-name")?;
    let expected_first = path_clone_data(&first_path).0 + first_path.components()[0].as_bytes().len() as u64;
    let expected_second = path_clone_data(&second_path).0 + second_path.components()[0].as_bytes().len() as u64;
    let barrier = std::sync::Barrier::new(2);
    let (first_result, second_result) = std::thread::scope(|scope| {
        let first_thread = scope.spawn(|| {
            barrier.wait();
            let result = poll_ready(first.lookup_no_follow(&first_path, WorkBudget::UNBOUNDED, &cancellation))
                .expect("memory observation completes");
            (result, first.proof_data_completed())
        });
        let second_thread = scope.spawn(|| {
            barrier.wait();
            let result = poll_ready(second.lookup_no_follow(&second_path, WorkBudget::UNBOUNDED, &cancellation))
                .expect("memory observation completes");
            (result, second.proof_data_completed())
        });
        (first_thread.join().expect("first observer completes"), second_thread.join().expect("second observer completes"))
    });
    let first_receipt = first_result.0?;
    let second_receipt = second_result.0?;
    assert!(first_receipt.value.record.is_none());
    assert!(second_receipt.value.record.is_none());
    assert_eq!(first_result.1.retained_bytes, expected_first);
    assert_eq!(second_result.1.retained_bytes, expected_second);
    assert_eq!(first_result.1.work.bytes_copied, expected_first);
    assert_eq!(second_result.1.work.bytes_copied, expected_second);
    for (completed, receipt) in [
        (first_result.1, &first_receipt),
        (second_result.1, &second_receipt),
    ] {
        // Decoded cold-frontier frames depend on cache races, unlike each
        // observer's exact DATA buffers and the six always-reached frames.
        assert!(completed.work.allocation_operations >= 10);
        assert!(completed.work.allocation_operations <= receipt.work.allocation_operations);
    }
    assert_eq!(checkout.proof_data_snapshot_completed().total_bytes()?, expected_first + expected_second);
    cancellation.cancel();
    let failed = first.lookup_no_follow(&first_path, WorkBudget::UNBOUNDED, &cancellation).await;
    assert!(failed.is_err());
    assert_eq!(first.proof_data_completed().retained_bytes, expected_first);
    assert_eq!(first.proof_data_completed().work, first_result.1.work);
    let custody = first.proof_data_custody();
    drop(first);
    assert_eq!(custody.retained_bytes(), expected_first, "shared proof outlives the callback");
    drop(second);
    drop(checkout);
    assert_eq!(custody.retained_bytes(), 0, "actual key destruction retires DATA");
    Ok(())
}

#[test]
fn proof_data_failure_keeps_only_the_completed_moved_name_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let ledger = DependencyLedger::new(CheckoutDependencies::new([], [], 8)?);
    let observed = path("bounded-name")?;
    let name = observed.components()[0].clone();
    let name_bytes = name.retained_bytes() as u64;
    let dependencies = vec![Dependency {
        region: DependencyRegion::DirectoryName { directory_id: FileId::from_bytes([41; 16]), name },
        expected: DependencyState::Absent,
    }];
    let budget = WorkBudget { bytes_copied: path_clone_data(&observed).0 - 1, ..WorkBudget::UNBOUNDED };
    let cancellation = CancellationToken::new();
    let mut work = WorkCounters::default();
    let error = ledger.observe_paths(
        ObjectId { kind: ObjectKind::FileTablePage, digest: Digest::from_bytes([42; 32]) },
        [&observed], true, dependencies, 8, &mut work, budget, &cancellation,
    ).expect_err("path DATA copy is denied before construction");
    assert!(matches!(error, FsError::Work(_)));
    assert_eq!(ledger.proof().dependencies.len(), 1);
    assert!(ledger.proof().observed_paths.as_deref().expect("reset map").entries.is_empty());
    let completed = ledger.proof_data_completed();
    assert_eq!(completed.retained_bytes, name_bytes);
    assert_eq!(completed.work.bytes_copied, 0);
    assert_eq!(completed.work.allocation_operations, 1, "only the actual empty path-map Arc was allocated");
    cancellation.cancel();
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _held = ledger.proof();
        panic!("exercise cancellation-independent poisoned proof inspection");
    }));
    assert!(poisoned.is_err());
    assert_eq!(ledger.proof_data_completed().retained_bytes, name_bytes);
    assert_eq!(ledger.proof_data_completed().work, completed.work);
    ledger.proof().clear();
    assert_eq!(ledger.proof_data_completed().retained_bytes, 0);
    Ok(())
}

#[test]
fn proof_snapshot_allocation_generation_distinguishes_reused_addresses() {
    let prior = ProofDataSnapshot { dependencies: (7, 3, 19), paths: (9, 4, 23) };
    let reused = ProofDataSnapshot { dependencies: (7, 4, 11), paths: (9, 5, 13) };
    assert_eq!(reused.newly_retained_since(prior), Ok(24));
    let unchanged = ProofDataSnapshot { dependencies: (7, 3, 22), paths: (9, 4, 25) };
    assert_eq!(unchanged.newly_retained_since(prior), Ok(5));
}

#[test]
fn delivered_proof_data_is_not_recharged_and_unreported_peak_is_segment_local()
-> Result<(), Box<dyn std::error::Error>> {
    let ledger = DependencyLedger::new(CheckoutDependencies::new([], [], 8)?);
    let base = ObjectId { kind: ObjectKind::FileTablePage, digest: Digest::from_bytes([43; 32]) };
    let cancellation = CancellationToken::new();
    let first = path("a-longer-name-retained-by-the-first-query")?;
    let second = path("x")?;
    let dependency = |path: &NamespacePath| Dependency {
        region: DependencyRegion::DirectoryName {
            directory_id: FileId::from_bytes([44; 16]), name: path.components()[0].clone(),
        },
        expected: DependencyState::Absent,
    };
    let mut first_work = WorkCounters::default();
    ledger.observe_paths(
        base, [&first], true, vec![dependency(&first)], 8,
        &mut first_work, WorkBudget::UNBOUNDED, &cancellation,
    )?;
    let first_owned = ledger.data_custody.retained_bytes();
    let first_report = ledger.data_custody.take_completed_data_work();
    let map_bytes = size_of::<ObservedPathMap>() as u64;
    assert_eq!(first_report.peak_allocation_bytes, first_owned + map_bytes);
    assert_eq!(ledger.data_custody.live_payload_counts(), (first_owned + map_bytes, first_owned + map_bytes));
    assert_eq!(ledger.accounting.lock().allocations.live_bytes(), first_owned + map_bytes);
    let second_owned = path_clone_data(&second).0 + second.components()[0].retained_bytes() as u64;
    let mut second_work = WorkCounters::default();
    ledger.observe_paths(
        base, [&second], true, vec![dependency(&second)], 8, &mut second_work,
        WorkBudget { peak_allocation_bytes: second_owned, ..WorkBudget::UNBOUNDED },
        &cancellation,
    )?;
    assert_eq!(second_work.peak_allocation_bytes, second_owned);
    let second_report = ledger.data_custody.take_completed_data_work();
    assert_eq!(second_report.peak_allocation_bytes, second_owned);
    assert!(second_report.peak_allocation_bytes < first_report.peak_allocation_bytes);
    assert_eq!(ledger.proof_data_completed().retained_bytes, first_owned + second_owned);
    ledger.proof().clear();
    assert_eq!(ledger.data_custody.live_payload_counts(), (0, 0));
    let mut next_work = WorkCounters::default();
    ledger.observe_paths(
        base, [&second], true, vec![dependency(&second)], 8, &mut next_work,
        WorkBudget { peak_allocation_bytes: second_owned + map_bytes, ..WorkBudget::UNBOUNDED },
        &cancellation,
    )?;
    assert_eq!(ledger.data_custody.live_payload_counts(), (second_owned + map_bytes, 0),
        "new DATA and its actual map payload are not standing retired payloads");
    assert_eq!(next_work.peak_allocation_bytes, second_owned + map_bytes);
    assert_eq!(ledger.data_custody.take_completed_data_work().peak_allocation_bytes, second_owned + map_bytes);
    assert_eq!(ledger.data_custody.take_completed_data_work(), WorkCounters::default());
    assert!(ledger.proof_data_completed().work.bytes_copied > first_report.bytes_copied);
    Ok(())
}

#[test]
fn observed_path_map_replacement_and_clear_retire_only_their_actual_aliases()
-> Result<(), Box<dyn std::error::Error>> {
    let ledger = DependencyLedger::new(CheckoutDependencies::new([], [], 8)?);
    let owner = Arc::clone(&ledger.data_custody);
    let base = ObjectId { kind: ObjectKind::FileTablePage, digest: Digest::from_bytes([45; 32]) };
    let cancellation = CancellationToken::new();
    let first = path("first")?;
    let second = path("second")?;
    let map_bytes = size_of::<ObservedPathMap>() as u64;
    let mut work = WorkCounters::default();
    ledger.observe_paths(base, [&first], true, vec![], 1, &mut work, WorkBudget::UNBOUNDED, &cancellation)?;
    let old = Arc::clone(ledger.proof().observed_paths.as_ref().expect("first map"));
    let generation = ledger.proof().observed_data_generation;
    owner.take_completed_data_work();
    let mut next = WorkCounters::default();
    ledger.observe_paths(base, [&second], true, vec![], 1, &mut next, WorkBudget::UNBOUNDED, &cancellation)?;
    assert!(old.entries.contains_key(&first));
    assert!(!ledger.proof().observes_path(base, &first, true));
    assert!(ledger.proof().observes_path(base, &second, true));
    assert_eq!(ledger.proof().observed_data_generation, generation + 1);
    assert_eq!(owner.physical_retained_bytes(), 2 * map_bytes);
    assert_eq!(owner.retained_bytes(), path_clone_data(&first).0 + path_clone_data(&second).0);
    let current = Arc::clone(ledger.proof().observed_paths.as_ref().expect("replacement map"));
    cancellation.cancel();
    ledger.proof().clear();
    {
        let proof = ledger.proof();
        assert!(proof.observed_paths.is_none());
        assert!(proof.observed_base.is_none());
        assert_eq!(proof.observed_data_bytes, 0);
        assert_eq!(proof.observed_clone_data_bytes, 0);
        assert_eq!(proof.observed_data_generation, generation + 2);
    }
    assert_eq!(owner.physical_retained_bytes(), 2 * map_bytes, "clear does not destroy external map aliases");
    drop(old);
    assert_eq!(owner.physical_retained_bytes(), map_bytes);
    assert_eq!(owner.retained_bytes(), path_clone_data(&second).0);
    drop(current);
    assert_eq!(owner.retained_payload_bytes(), 0);
    Ok(())
}

#[test]
fn observed_path_true_cow_preclaims_arc_and_copy_and_clears_inherited_tags()
-> Result<(), Box<dyn std::error::Error>> {
    let ledger = DependencyLedger::new(CheckoutDependencies::new([], [], 8)?);
    let base = ObjectId { kind: ObjectKind::FileTablePage, digest: Digest::from_bytes([46; 32]) };
    let cancellation = CancellationToken::new();
    let observed = path("copied-name")?;
    let map_bytes = size_of::<ObservedPathMap>() as u64;
    let (data_bytes, data_operations) = path_clone_data(&observed);
    let mut work = WorkCounters::default();
    ledger.observe_paths(base, [&observed], false, vec![], 8, &mut work, WorkBudget::UNBOUNDED, &cancellation)?;
    let old = Arc::clone(ledger.proof().observed_paths.as_ref().expect("original map"));
    let original_generation = ledger.proof().observed_data_generation;
    ledger.data_custody.take_completed_data_work();
    let mut denied = WorkCounters::default();
    let error = ledger.observe_paths(
        base, [&observed], true, vec![], 8, &mut denied,
        WorkBudget { peak_allocation_bytes: map_bytes + data_bytes - 1, ..WorkBudget::UNBOUNDED },
        &cancellation,
    ).expect_err("the actual Arc and copied DATA must be funded together before cloning");
    assert!(matches!(error, FsError::Work(_)));
    assert_eq!(denied.allocation_operations, 0);
    assert_eq!(denied.bytes_copied, 0);
    assert!(Arc::ptr_eq(&old, ledger.proof().observed_paths.as_ref().expect("unchanged map")));
    assert_eq!(ledger.proof().observed_data_generation, original_generation);
    assert_eq!(ledger.data_custody.physical_retained_bytes(), map_bytes);
    let plain_clone = old.as_ref().clone();
    assert!(plain_clone == *old, "map equality compares only semantic entries");
    assert_eq!(ledger.data_custody.retained_payload_bytes(), map_bytes + data_bytes,
        "a genuine Clone clears every old physical and DATA tag");
    drop(plain_clone);
    let mut copied = WorkCounters::default();
    ledger.observe_paths(
        base, [&observed], true, vec![], 8, &mut copied,
        WorkBudget { peak_allocation_bytes: map_bytes + data_bytes, ..WorkBudget::UNBOUNDED },
        &cancellation,
    )?;
    assert_eq!(copied.allocation_operations, 1 + data_operations);
    assert_eq!(copied.bytes_copied, data_bytes);
    assert_eq!(copied.peak_allocation_bytes, map_bytes + data_bytes);
    assert!(!old.entries.get(&observed).expect("old entry").terminal);
    assert!(ledger.proof().observes_path(base, &observed, true));
    assert_eq!(ledger.proof().observed_data_bytes, data_bytes);
    assert_eq!(ledger.proof().observed_clone_data_bytes, data_bytes);
    assert_eq!(ledger.proof().observed_data_generation, original_generation + 1);
    assert_eq!(ledger.data_custody.retained_payload_bytes(), 2 * (map_bytes + data_bytes));
    drop(old);
    assert_eq!(ledger.data_custody.retained_payload_bytes(), map_bytes + data_bytes);
    ledger.proof().clear();
    assert_eq!(ledger.data_custody.retained_payload_bytes(), 0);
    Ok(())
}

#[test]
fn observed_path_weak_move_preserves_key_data_and_replaces_only_arc_control()
-> Result<(), Box<dyn std::error::Error>> {
    let ledger = DependencyLedger::new(CheckoutDependencies::new([], [], 8)?);
    let base = ObjectId { kind: ObjectKind::FileTablePage, digest: Digest::from_bytes([47; 32]) };
    let cancellation = CancellationToken::new();
    let observed = path("moved-name")?;
    let map_bytes = size_of::<ObservedPathMap>() as u64;
    let data_bytes = path_clone_data(&observed).0;
    let mut work = WorkCounters::default();
    ledger.observe_paths(base, [&observed], false, vec![], 8, &mut work, WorkBudget::UNBOUNDED, &cancellation)?;
    let (weak, old_address, key_address, generation) = {
        let proof = ledger.proof();
        let map = proof.observed_paths.as_ref().expect("observed map");
        let key = map.entries.keys().next().expect("observed key");
        (Arc::downgrade(map), Arc::as_ptr(map) as usize,
            key.components().as_ptr() as usize, proof.observed_data_generation)
    };
    ledger.data_custody.take_completed_data_work();
    let mut moved = WorkCounters::default();
    ledger.observe_paths(
        base, [&observed], true, vec![], 8, &mut moved,
        WorkBudget { peak_allocation_bytes: map_bytes, bytes_copied: 0, ..WorkBudget::UNBOUNDED },
        &cancellation,
    )?;
    assert!(weak.upgrade().is_none(), "make_mut dissociates the old weak-only Arc");
    assert_eq!(moved.allocation_operations, 1);
    assert_eq!(moved.bytes_copied, 0);
    assert_eq!(moved.peak_allocation_bytes, map_bytes);
    {
        let proof = ledger.proof();
        let map = proof.observed_paths.as_ref().expect("moved map");
        assert_ne!(Arc::as_ptr(map) as usize, old_address);
        assert_eq!(map.entries.keys().next().expect("moved key").components().as_ptr() as usize, key_address);
        assert_eq!(proof.observed_data_bytes, data_bytes);
        assert_eq!(proof.observed_clone_data_bytes, data_bytes);
        assert_eq!(proof.observed_data_generation, generation + 1);
    }
    assert_eq!(ledger.data_custody.retained_bytes(), data_bytes);
    assert_eq!(ledger.data_custody.physical_retained_bytes(), map_bytes);
    ledger.proof().clear();
    assert_eq!(ledger.data_custody.retained_payload_bytes(), 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_initial_path_map_denial_precedes_constructor_and_alias_owns_retirement()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("native-map-admission").await?;
    let cancellation = CancellationToken::new();
    let checkout = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let candidate = checkout.private_candidate_with_native_work(&original, &cancellation)?;
    let custody = candidate.private_control_custody();
    drop(candidate);
    let map_bytes = size_of::<ObservedPathMap>() as u64;
    let initial = custody.native_control_events();
    let denied = original.native_volume_work(WorkBudget {
        peak_allocation_bytes: map_bytes - 1, ..WorkBudget::UNBOUNDED
    })?;
    let before = original.receipt();
    assert!(denied.claim_allocation(map_bytes, 1).is_err(),
        "the real resource view rejects the known payload before its Arc constructor");
    assert_eq!(original.receipt(), before);
    assert_eq!(custody.native_control_events(), initial);
    drop(denied);
    let scope = original.native_volume_work(WorkBudget {
        peak_allocation_bytes: map_bytes, ..WorkBudget::UNBOUNDED
    })?;
    let witness = scope.checkout_control_sink(&custody)?;
    let mut allocation = scope.claim_allocation(map_bytes, 1)?;
    let control = allocation.transfer_checkout_control(&custody, &witness)?;
    let dependencies = checkout.dependencies.proof().dependencies.clone();
    let mut proof = Proof::new_with_control(dependencies, control);
    drop(allocation);
    drop(witness);
    let expected = (initial.0 + u128::from(map_bytes), initial.1);
    assert_eq!(custody.native_control_events(), expected);
    let alias = Arc::clone(proof.observed_paths.as_ref().expect("native initial map"));
    let live = original.live_allocation_bytes();
    cancellation.cancel();
    proof.clear();
    assert!(proof.observed_paths.is_none());
    assert_eq!(custody.native_control_events(), expected);
    assert_eq!(original.live_allocation_bytes(), live);
    drop(proof);
    drop(alias);
    assert_eq!(custody.native_control_events(),
        (expected.0, initial.1 + u128::from(map_bytes)));
    assert_eq!(original.live_allocation_bytes(), live - map_bytes);
    Ok(())
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
mod heterogeneous_mount {
    use super::*;

    // Consumer-owned dispatch implements existing provider contracts. Keeping
    // this adapter in the test avoids imposing a production backend taxonomy.
    enum Objects {
        Memory(TestMemoryObjectStore),
        Local(LocalFs),
    }

    macro_rules! delegate {
        ($self:ident, $method:ident($($argument:expr),*)) => {
            match $self {
                Objects::Memory(store) => AsyncObjectStore::$method(store, $($argument),*).await,
                Objects::Local(fs) => AsyncObjectStore::$method(&fs.inner.objects, $($argument),*).await,
            }
        };
    }

    impl AsyncObjectStore for Objects {
        async fn put(
            &self,
            id: ObjectId,
            bytes: OwnedBytes,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> ObjectResult<()> {
            delegate!(self, put(id, bytes, budget, cancellation))
        }
        async fn put_many(
            &self,
            writes: &[ObjectWrite],
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> ObjectResult<()> {
            delegate!(self, put_many(writes, budget, cancellation))
        }
        async fn read(
            &self,
            id: ObjectId,
            maximum: u64,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> ObjectResult<ObjectRead> {
            delegate!(self, read(id, maximum, budget, cancellation))
        }
        async fn read_many(
            &self,
            requests: &[ObjectReadRequest],
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> ObjectResult<Vec<ObjectRead>> {
            delegate!(self, read_many(requests, budget, cancellation))
        }
        async fn contains(
            &self,
            id: ObjectId,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> ObjectResult<bool> {
            delegate!(self, contains(id, budget, cancellation))
        }
        async fn flush_before_publish(
            &self,
            scope: crate::PublicationScope<'_>,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> ObjectResult<crate::PublicationHold> {
            delegate!(self, flush_before_publish(scope, budget, cancellation))
        }
        fn collection(&self) -> Option<&Arc<crate::Collection>> {
            match self {
                Self::Memory(_) => None,
                Self::Local(fs) => fs.inner.objects.collection(),
            }
        }
    }

    #[tokio::test]
    async fn mounted_view_routes_memory_and_local_objects_through_existing_traits()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let local = Fs::local(LocalOptions::new(directory.path())).await?;
        // Authority remains in memory in this consumer; only the object
        // backends differ. This does not claim a cross-provider transaction.
        let memory = Fs::new(
            crate::MemoryAuthorityStore::default(),
            Objects::Memory(TestMemoryObjectStore::default()),
            EmbeddedCapabilities::MEMORY,
        );
        let disk = Fs::new(
            crate::MemoryAuthorityStore::default(),
            Objects::Local(local),
            EmbeddedCapabilities::MEMORY,
        );
        let root = memory.create_workspace("root").await?;
        let scratch = disk.create_workspace("scratch").await?;
        root.write_text("/file", "memory").await?;
        scratch.write_text("/file", "disk").await?;
        let mut view = crate::MountedView::builder()
            .mount(
                "/",
                root.engine_checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned())
                    .await?,
            )?
            .mount(
                "/.scratch",
                scratch
                    .engine_checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned())
                    .await?,
            )?
            .build()?;
        let cancellation = CancellationToken::new();
        let root_path = crate::path::PortablePath::parse("/file", VolumeLimits::default())?;
        let scratch_path =
            crate::path::PortablePath::parse("/.scratch/file", VolumeLimits::default())?;
        for path in [&root_path, &scratch_path] {
            let route = view.route_mut(path)?;
            assert!(
                route
                    .checkout
                    .lookup_no_follow(&route.path, WorkBudget::UNBOUNDED, &cancellation)
                    .await?
                    .value
                    .record
                    .is_some()
            );
        }
        assert_eq!(view.snapshot().bindings.len(), 2);
        assert_eq!(
            view.validate_rename(&root_path, &scratch_path),
            Err(crate::MountError::CrossVolume)
        );
        assert_eq!(root.read("/file", 32).await?.as_ref(), b"memory");
        assert_eq!(scratch.read("/file", 32).await?.as_ref(), b"disk");
        Ok(())
    }
}

fn config() -> VolumeConfig {
    VolumeConfig {
        profile: FilesystemProfile::Portable,
        concurrency: ConcurrencyMode::Optimistic,
        lifecycle: Lifecycle::Ephemeral,
        case_sensitivity: CaseSensitivity::Sensitive,
        unicode: UnicodePolicy::Preserve,
        symbolic_links: true,
        hard_links: true,
        sparse_files: true,
        limits: VolumeLimits::default(),
    }
}

fn serialized_config() -> VolumeConfig {
    VolumeConfig {
        concurrency: ConcurrencyMode::SerializedAuthority,
        ..config()
    }
}

fn exclusive_config() -> VolumeConfig {
    VolumeConfig {
        concurrency: ConcurrencyMode::ExclusiveWriter,
        ..config()
    }
}

fn pinned() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadOnly,
        consistency: ConsistencyMode::Pinned,
        mutations: MutationMode::None,
    }
}

fn writable_pinned() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::Pinned,
        mutations: MutationMode::PrivateOverlay,
    }
}

fn manual() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadOnly,
        consistency: ConsistencyMode::Manual,
        mutations: MutationMode::None,
    }
}

fn tracking() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadOnly,
        consistency: ConsistencyMode::TrackingSafe,
        mutations: MutationMode::None,
    }
}

fn writable_manual() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::Manual,
        mutations: MutationMode::PrivateOverlay,
    }
}

fn writable_tracking() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::TrackingSafe,
        mutations: MutationMode::PrivateOverlay,
    }
}

#[tokio::test]
async fn workspace_fork_reads_the_verified_source_root_once()
-> Result<(), Box<dyn std::error::Error>> {
    let tracked = Arc::new(std::sync::Mutex::new(None));
    let reads = Arc::new(AtomicUsize::new(0));
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        CountingObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            tracked: Arc::clone(&tracked),
            reads: Arc::clone(&reads),
            unreadable: Arc::default(),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let source = fs.create_workspace("fork-read-bound").await?;
    source
        .write("/payload", bytes::Bytes::from(vec![7_u8; 128 * 1_024]))
        .await?;
    let generation = source.head().await?;
    *tracked.lock().map_err(|_| "tracking lock poisoned")? = Some(ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: generation.id().digest(),
    });
    reads.store(0, Ordering::Relaxed);

    source
        .fork(
            "fork-read-bound-child",
            crate::workspace::ForkOptions::from_generation(
                generation,
                crate::IdempotencyKey::from_bytes([0x7a; 16]),
            ),
        )
        .await?;

    assert_eq!(
        reads.load(Ordering::Relaxed),
        1,
        "fork must read the source root to construct the child, but must not prove its closure twice"
    );
    Ok(())
}

#[tokio::test]
async fn identical_live_retention_is_not_reproven_but_new_retention_fails_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let tracked = Arc::new(std::sync::Mutex::new(None));
    let reads = Arc::new(AtomicUsize::new(0));
    let unreadable = Arc::new(std::sync::Mutex::new(None));
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        CountingObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            tracked: Arc::clone(&tracked),
            reads: Arc::clone(&reads),
            unreadable: Arc::clone(&unreadable),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let workspace = fs.create_workspace("retention-proof").await?;
    let crate::TransactionCommit::Committed(generation) = workspace
        .write("/payload", bytes::Bytes::from(vec![7_u8; 128 * 1_024]))
        .await?
    else {
        return Err("retention fixture did not commit".into());
    };
    let cloned = generation.clone();
    let cold = workspace.head().await?;
    *tracked.lock().map_err(|_| "tracking lock poisoned")? = Some(ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: generation.id().digest(),
    });

    reads.store(0, Ordering::Relaxed);
    generation.pin("first").await?;
    cloned.pin("first").await?;
    assert_eq!(
        reads.load(Ordering::Relaxed),
        0,
        "the original committed generation and its clone share publication custody"
    );
    generation.pin("first").await?;
    assert_eq!(reads.load(Ordering::Relaxed), 0, "an identical live retention does not reprove");
    generation.pin("second").await?;
    assert_eq!(reads.load(Ordering::Relaxed), 1, "a different label proves its closure");
    generation.pin("first").await?;
    cloned.pin("second").await?;
    assert_eq!(reads.load(Ordering::Relaxed), 1, "repeat retentions do not reprove");

    cold.pin("cold").await?;
    assert_eq!(reads.load(Ordering::Relaxed), 2, "a cold head proves once without publication custody");
    cold.pin("cold").await?;
    assert_eq!(reads.load(Ordering::Relaxed), 2);

    // Negative control: once every content chunk is unreadable, recorded
    // retentions stay exact, but a retention not yet recorded still walks
    // the whole closure and fails closed on the missing chunk, every time.
    *unreadable.lock().map_err(|_| "unreadable lock poisoned")? = Some(ObjectKind::BlobChunk);
    generation.pin("first").await?;
    for expected_reads in [3, 4] {
        assert!(matches!(
            generation.pin("third").await,
            Err(crate::workspace::WorkspaceError::Filesystem(error))
                if error.to_string().contains("object is missing")
        ));
        assert_eq!(reads.load(Ordering::Relaxed), expected_reads);
    }
    Ok(())
}

fn writable_live() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::Live,
        mutations: MutationMode::DirectLive,
    }
}

fn path(value: &str) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    Ok(NamespacePath::new(
        vec![LogicalName::new(
            NameEncoding::Utf8,
            value.as_bytes().to_vec(),
            config().limits.maximum_component_bytes,
        )?],
        config().limits,
    )?)
}

fn path_parts(values: &[&str]) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    let components = values
        .iter()
        .map(|value| {
            LogicalName::new(
                NameEncoding::Utf8,
                value.as_bytes().to_vec(),
                config().limits.maximum_component_bytes,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(NamespacePath::new(components, config().limits)?)
}

fn root_path() -> Result<NamespacePath, Box<dyn std::error::Error>> {
    Ok(NamespacePath::new(Vec::new(), config().limits)?)
}

fn helper_record(file_id: FileId, payload: FilePayload, kind: FileKind) -> FileRecord {
    FileRecord {
        file_id,
        kind,
        link_count: 1,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([9; 32]),
        },
        payload,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CheckoutState {
    base_generation_root: ObjectId,
    generation_root: ObjectId,
    base_file_table: ObjectId,
    base_root: GenerationRoot,
    root: GenerationRoot,
    authority_head: Option<Head>,
    prepared_merge_parent: Option<ObjectId>,
    dependencies: CheckoutDependencies,
    mode: CheckoutMode,
}

fn checkout_state<A, O>(checkout: &Checkout<A, O>) -> CheckoutState {
    CheckoutState {
        base_generation_root: checkout.root.base(),
        generation_root: checkout.generation_root,
        base_file_table: checkout.base_file_table,
        base_root: checkout.base_root.clone(),
        root: checkout.root.clone(),
        authority_head: checkout.authority_head,
        prepared_merge_parent: checkout.prepared_merge_parent,
        dependencies: checkout.dependencies.proof().dependencies.clone(),
        mode: checkout.mode,
    }
}

struct FaultControl {
    operation: AtomicUsize,
    fail_at: AtomicUsize,
}

impl FaultControl {
    fn disabled() -> Self {
        Self {
            operation: AtomicUsize::new(0),
            fail_at: AtomicUsize::new(usize::MAX),
        }
    }

    fn arm(&self, fail_at: usize) {
        self.operation.store(0, Ordering::Relaxed);
        self.fail_at.store(fail_at, Ordering::Relaxed);
    }

    fn disable(&self) {
        self.operation.store(0, Ordering::Relaxed);
        self.fail_at.store(usize::MAX, Ordering::Relaxed);
    }

    fn should_fail(&self) -> bool {
        self.operation.fetch_add(1, Ordering::Relaxed) + 1 == self.fail_at.load(Ordering::Relaxed)
    }
}

struct FaultObjectStore {
    inner: crate::memory::MemoryObjectStore,
    control: Arc<FaultControl>,
}

struct FaultAuthorityStore {
    inner: crate::memory::MemoryAuthorityStore,
    control: Arc<FaultControl>,
}

struct PostPutObjectStore {
    inner: Arc<crate::memory::MemoryObjectStore>,
    control: Arc<FaultControl>,
}

struct PostAppendAuthorityStore {
    inner: Arc<crate::memory::MemoryAuthorityStore>,
    control: Arc<FaultControl>,
}

struct CountingObjectStore {
    inner: crate::memory::MemoryObjectStore,
    tracked: Arc<std::sync::Mutex<Option<ObjectId>>>,
    reads: Arc<AtomicUsize>,
    /// Every object of this kind reads as missing.
    unreadable: Arc<std::sync::Mutex<Option<ObjectKind>>>,
}

impl CountingObjectStore {
    fn record(&self, object_id: ObjectId) -> Result<(), ObjectFailure> {
        if self
            .tracked
            .lock()
            .is_ok_and(|tracked| tracked.as_ref() == Some(&object_id))
        {
            self.reads.fetch_add(1, Ordering::Relaxed);
        }
        if self
            .unreadable
            .lock()
            .is_ok_and(|kind| *kind == Some(object_id.kind))
        {
            return Err(ObjectFailure::before_work(ObjectStoreError::Missing));
        }
        Ok(())
    }
}

impl FaultAuthorityStore {
    fn failure(read: bool) -> AuthorityFailure {
        AuthorityFailure::new(
            AuthorityStoreError::Corrupt("deterministic test fault".to_owned()),
            WorkCounters {
                authority_records_read: u64::from(read),
                authority_records_appended: u64::from(!read),
                ..WorkCounters::default()
            },
        )
    }
}

impl AsyncAuthorityStore for FaultAuthorityStore {
    async fn fork_generation_authority(
        &self,
        source: crate::GenerationForkSource,
        destination_authority: crate::foundation::AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(false));
        }
        self.inner
            .fork_generation_authority(
                source,
                destination_authority,
                operation_id,
                budget,
                cancellation,
            )
            .await
    }

    async fn create_authority(
        &self,
        authority_id: crate::foundation::AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(false));
        }
        AuthorityStore::create_authority(&self.inner, authority_id, genesis_epoch, budget)
    }

    async fn head(
        &self,
        authority_id: crate::foundation::AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Head> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
        AuthorityStore::head(&self.inner, authority_id, budget)
    }

    async fn compare_and_append(
        &self,
        authority_id: crate::foundation::AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<AppendOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(false));
        }
        AuthorityStore::compare_and_append(
            &self.inner,
            authority_id,
            epoch,
            expected,
            commit,
            budget,
        )
    }

    async fn replay(
        &self,
        authority_id: crate::foundation::AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Vec<DurableCommit>> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
        AuthorityStore::replay(&self.inner, authority_id, after, limit, budget)
    }

    async fn fence(
        &self,
        authority_id: crate::foundation::AuthorityId,
        expected: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<FenceOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(false));
        }
        AuthorityStore::fence(&self.inner, authority_id, expected, budget)
    }

    async fn find_operation(
        &self,
        authority_id: crate::foundation::AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<DurableCommit>> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
        AuthorityStore::find_operation(&self.inner, authority_id, operation_id, budget)
    }
}

impl FaultObjectStore {
    fn failure(read: bool) -> ObjectFailure {
        ObjectFailure::new(
            ObjectStoreError::Corrupt,
            WorkCounters {
                backend_read_operations: u64::from(read),
                backend_write_operations: u64::from(!read),
                ..WorkCounters::default()
            },
        )
    }
}

impl AsyncObjectStore for FaultObjectStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(false));
        }
        ObjectStore::put(&self.inner, object_id, bytes, budget)
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(false));
        }
        ObjectStore::put_many(&self.inner, writes, budget)
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
        ObjectStore::read(&self.inner, object_id, maximum_bytes, budget)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
        ObjectStore::read_many(&self.inner, requests, budget)
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
        ObjectStore::contains(&self.inner, object_id, budget)
    }
}

impl AsyncObjectStore for CountingObjectStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        ObjectStore::put(&self.inner, object_id, bytes, budget)
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        ObjectStore::put_many(&self.inner, writes, budget)
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        self.record(object_id)?;
        ObjectStore::read(&self.inner, object_id, maximum_bytes, budget)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        for request in requests {
            self.record(request.object_id)?;
        }
        ObjectStore::read_many(&self.inner, requests, budget)
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        ObjectStore::contains(&self.inner, object_id, budget)
    }
}

impl AsyncObjectStore for PostPutObjectStore {
    #[cfg(not(target_arch = "wasm32"))]
    async fn put_with_native_work(
        &self, object_id: ObjectId, bytes: OwnedBytes,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        AsyncObjectStore::put_with_native_work(&*self.inner, object_id, bytes, original, cancellation).await?;
        if self.control.should_fail() { return Err(ObjectStoreError::Corrupt); }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_hashed_with_native_work(
        &self, object: HashedObject,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        AsyncObjectStore::put_hashed_with_native_work(&*self.inner, object, original, cancellation).await?;
        if self.control.should_fail() { return Err(ObjectStoreError::Corrupt); }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_many_with_native_work(
        &self, writes: &[ObjectWrite],
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        AsyncObjectStore::put_many_with_native_work(&*self.inner, writes, original, cancellation).await?;
        if self.control.should_fail() { return Err(ObjectStoreError::Corrupt); }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work(
        &self, object_id: ObjectId, maximum_bytes: u64,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<crate::async_storage::NativeObjectRead, ObjectStoreError> {
        AsyncObjectStore::read_with_native_work(&*self.inner, object_id, maximum_bytes, original, cancellation).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_many_with_native_work(
        &self, requests: &[ObjectReadRequest],
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<crate::async_storage::NativeObjectReadBatch, ObjectStoreError> {
        AsyncObjectStore::read_many_with_native_work(&*self.inner, requests, original, cancellation).await
    }

    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let receipt = ObjectStore::put(&*self.inner, object_id, bytes, budget)?;
        if self.control.should_fail() {
            return Err(ObjectFailure::new(ObjectStoreError::Corrupt, receipt.work));
        }
        Ok(receipt)
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let receipt = ObjectStore::put_many(&*self.inner, writes, budget)?;
        if self.control.should_fail() {
            return Err(ObjectFailure::new(ObjectStoreError::Corrupt, receipt.work));
        }
        Ok(receipt)
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        ObjectStore::read(&*self.inner, object_id, maximum_bytes, budget)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        ObjectStore::read_many(&*self.inner, requests, budget)
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        ObjectStore::contains(&*self.inner, object_id, budget)
    }
}

impl AsyncAuthorityStore for PostAppendAuthorityStore {
    async fn fork_generation_authority(
        &self,
        source: crate::GenerationForkSource,
        destination_authority: crate::foundation::AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        self.inner
            .fork_generation_authority(
                source,
                destination_authority,
                operation_id,
                budget,
                cancellation,
            )
            .await
    }

    async fn create_authority(
        &self,
        authority_id: crate::foundation::AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        AuthorityStore::create_authority(&*self.inner, authority_id, genesis_epoch, budget)
    }

    async fn head(
        &self,
        authority_id: crate::foundation::AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Head> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        AuthorityStore::head(&*self.inner, authority_id, budget)
    }

    async fn compare_and_append(
        &self,
        authority_id: crate::foundation::AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<AppendOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let receipt = AuthorityStore::compare_and_append(
            &*self.inner,
            authority_id,
            epoch,
            expected,
            commit,
            budget,
        )?;
        if matches!(receipt.value, AppendOutcome::Committed(_)) && self.control.should_fail() {
            return Err(AuthorityFailure::new(
                AuthorityStoreError::Indeterminate {
                    operation: "compare-and-append",
                    source: std::io::Error::other("deterministic acknowledgement loss"),
                },
                receipt.work,
            ));
        }
        Ok(receipt)
    }

    async fn replay(
        &self,
        authority_id: crate::foundation::AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Vec<DurableCommit>> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        AuthorityStore::replay(&*self.inner, authority_id, after, limit, budget)
    }

    async fn fence(
        &self,
        authority_id: crate::foundation::AuthorityId,
        expected: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<FenceOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        AuthorityStore::fence(&*self.inner, authority_id, expected, budget)
    }

    async fn find_operation(
        &self,
        authority_id: crate::foundation::AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<DurableCommit>> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        AuthorityStore::find_operation(&*self.inner, authority_id, operation_id, budget)
    }
}

#[test]
fn facade_helper_state_machines_are_total_and_preserve_typed_failures()
-> Result<(), Box<dyn std::error::Error>> {
    let file_id = FileId::from_bytes([1; 16]);
    let inline = helper_record(
        file_id,
        FilePayload::InlineRegular(InlineFileData::new(b"abcd")?),
        FileKind::Regular,
    );
    assert_eq!(regular_range_regions(None, 0, 1), [None, None]);
    assert_eq!(
        regular_range_regions(Some(inline), 1, 2),
        [
            Some(DependencyRegion::ContentRange {
                file_id,
                offset: 1,
                length: 2,
            }),
            None,
        ]
    );
    assert_eq!(
        regular_range_regions(Some(inline), 3, 4),
        [
            Some(DependencyRegion::ContentRange {
                file_id,
                offset: 3,
                length: 1,
            }),
            Some(DependencyRegion::FileLength(file_id)),
        ]
    );
    let directory = helper_record(
        file_id,
        FilePayload::Directory {
            entries: ObjectId {
                kind: ObjectKind::TreePage,
                digest: Digest::from_bytes([2; 32]),
            },
        },
        FileKind::Directory,
    );
    assert_eq!(
        regular_range_regions(Some(directory), 0, 1),
        [Some(DependencyRegion::FileRecord(file_id)), None]
    );

    assert!(
        validate_planned_file_range(
            4,
            ByteRange {
                offset: 1,
                length: 3
            },
            WorkCounters::default(),
        )
        .is_ok()
    );
    for range in [
        ByteRange {
            offset: 4,
            length: 1,
        },
        ByteRange {
            offset: u64::MAX,
            length: 1,
        },
    ] {
        assert!(matches!(
            validate_planned_file_range(4, range, WorkCounters::default()),
            Err(OperationFailure {
                error: FsError::FileRead(FileRangeReadError::InvalidRange),
                ..
            })
        ));
    }

    assert!(validate_volume_capabilities(config(), EmbeddedCapabilities::MEMORY).is_ok());
    let durable = VolumeConfig {
        lifecycle: Lifecycle::Durable,
        ..config()
    };
    assert!(matches!(
        validate_volume_capabilities(durable, EmbeddedCapabilities::MEMORY),
        Err(OperationFailure {
            error: FsError::UnsupportedDurability,
            ..
        })
    ));
    for admitted in [
        VolumeConfig {
            case_sensitivity: CaseSensitivity::ProfileFolded,
            ..config()
        },
        VolumeConfig {
            unicode: UnicodePolicy::RequireNfc,
            ..config()
        },
    ] {
        // `ProfileFolded`/`RequireNfc` are real, admitted policies (see the
        // `profile_folded_*`/`require_nfc_*` tests below for the folding and
        // normalization behavior itself); volume creation must not reject
        // them.
        assert!(validate_volume_capabilities(admitted, EmbeddedCapabilities::MEMORY).is_ok());
    }
    for mode in [
        pinned(),
        manual(),
        tracking(),
        writable_pinned(),
        writable_manual(),
        writable_tracking(),
    ] {
        assert!(validate_checkout(mode, config()).is_ok());
    }
    assert!(validate_checkout(writable_live(), serialized_config()).is_ok());
    assert!(matches!(
        validate_checkout(writable_live(), config()),
        Err(OperationFailure {
            error: FsError::LiveRequiresSerializedAuthority,
            ..
        })
    ));
    assert!(matches!(
        validate_checkout(
            CheckoutMode {
                access: AccessMode::ReadOnly,
                consistency: ConsistencyMode::Pinned,
                mutations: MutationMode::PrivateOverlay,
            },
            config(),
        ),
        Err(OperationFailure { .. })
    ));

    let replacement = helper_record(
        file_id,
        FilePayload::InlineRegular(InlineFileData::new(b"changed")?),
        FileKind::Regular,
    );
    assert!(matches!(
        file_table_mutation(file_id, None, Some(inline)),
        Some(FileTableMutation::Insert(_))
    ));
    assert!(matches!(
        file_table_mutation(file_id, Some(inline), None),
        Some(FileTableMutation::Remove { .. })
    ));
    assert!(matches!(
        file_table_mutation(file_id, Some(inline), Some(replacement)),
        Some(FileTableMutation::Replace { .. })
    ));
    assert_eq!(file_table_mutation(file_id, None, None), None);
    assert_eq!(directory_entries(None), None);
    assert_eq!(directory_entries(Some(inline)), None);
    assert!(directory_entries(Some(directory)).is_some());

    let logical_name = LogicalName::new(NameEncoding::Utf8, b"name".to_vec(), 255)?;
    let entry = crate::kernel::TreeEntry {
        name: logical_name.clone(),
        file_id,
        kind: FileKind::Regular,
    };
    assert!(matches!(
        tree_mutation(logical_name.clone(), None, Some(entry.clone())),
        Some(TreeMutation::Insert(_))
    ));
    assert!(matches!(
        tree_mutation(logical_name.clone(), Some(entry.clone()), None),
        Some(TreeMutation::Remove { .. })
    ));
    assert!(matches!(
        tree_mutation(logical_name.clone(), Some(entry.clone()), Some(entry)),
        Some(TreeMutation::Replace { .. })
    ));
    assert_eq!(tree_mutation(logical_name, None, None), None);

    assert_eq!(resolve_three(&1, &2, &2), Some(2));
    assert_eq!(resolve_three(&1, &2, &1), Some(2));
    assert_eq!(resolve_three(&1, &1, &2), Some(2));
    assert_eq!(resolve_three(&1, &2, &3), None);
    assert_eq!(
        merge_file_fields(inline, replacement, inline),
        Some(replacement)
    );
    let mut ours_conflicting = replacement;
    ours_conflicting.kind = FileKind::SymbolicLink;
    let mut theirs_conflicting = replacement;
    theirs_conflicting.kind = FileKind::Directory;
    assert_eq!(
        merge_file_fields(inline, ours_conflicting, theirs_conflicting),
        None
    );

    let prior = WorkCounters {
        bytes_copied: 2,
        peak_allocation_bytes: 3,
        ..WorkCounters::default()
    };
    let nested = WorkCounters {
        bytes_hashed: 4,
        peak_allocation_bytes: 5,
        ..WorkCounters::default()
    };
    let merged = merge_simultaneous_work(prior, nested, 7, WorkBudget::UNBOUNDED)?;
    assert_eq!(merged.bytes_copied, 2);
    assert_eq!(merged.bytes_hashed, 4);
    assert_eq!(merged.peak_allocation_bytes, 12);
    let failure = merge_simultaneous_failure(prior, nested, 7, FsError::NotFound);
    assert!(matches!(failure.error, FsError::NotFound));
    assert_eq!(failure.work.peak_allocation_bytes, 12);
    let overflow = merge_simultaneous_failure(
        prior,
        WorkCounters {
            peak_allocation_bytes: u64::MAX,
            ..WorkCounters::default()
        },
        1,
        FsError::NotFound,
    );
    assert!(matches!(overflow.error, FsError::Work(WorkError::Overflow)));
    Ok(())
}

#[test]
fn known_mutation_preconditions_capture_without_backend_rereads()
-> Result<(), Box<dyn std::error::Error>> {
    let store = TestMemoryObjectStore::default();
    let file_id = FileId::from_bytes([91; 16]);
    let record = helper_record(
        file_id,
        FilePayload::InlineRegular(InlineFileData::new(b"direct evidence")?),
        FileKind::Regular,
    );
    let captured = poll_ready(capture_known_dependencies_async(
        &store,
        config(),
        vec![
            (Some(record), DependencyRegion::Metadata(file_id)),
            (Some(record), DependencyRegion::FileLength(file_id)),
            (
                Some(record),
                DependencyRegion::ContentRange {
                    file_id,
                    offset: 0,
                    length: 6,
                },
            ),
        ],
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("known dependency capture blocked")??;
    assert_eq!(captured.value.len(), 3);
    assert_eq!(captured.work.backend_read_operations, 0);
    assert!(captured.work.bytes_hashed > 6);

    let mismatch = poll_ready(capture_known_dependencies_async(
        &store,
        config(),
        vec![(
            Some(record),
            DependencyRegion::Metadata(FileId::from_bytes([92; 16])),
        )],
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("mismatched dependency capture blocked")?
    .err()
    .ok_or("mismatched record identity unexpectedly succeeded")?;
    assert!(matches!(
        mismatch.error,
        FsError::Probe(AuthenticatedProbeError::RecordIdentityMismatch)
    ));
    assert_eq!(*mismatch.work, WorkCounters::default());
    Ok(())
}

#[test]
fn high_level_file_and_directory_operations_share_the_sparse_kernel()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("create blocked")??
        .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let original = bytes::Bytes::from(vec![b'a'; 100]);
    poll_ready(checkout.create_file(
        path("file")?,
        original,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("create file blocked")??;
    poll_ready(checkout.create_directory(path("directory")?, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("create directory blocked")??;
    poll_ready(checkout.write_file(
        path("file")?,
        40,
        bytes::Bytes::from_static(b"changed"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("write blocked")??;
    let read = poll_ready(checkout.read_file_range(
        &path("file")?,
        ByteRange {
            offset: 38,
            length: 12,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("read blocked")??;
    assert_eq!(&read.value.bytes[..], b"aachangedaaa");
    let page = poll_ready(checkout.list_directory(
        &root_path()?,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("list blocked")??;
    assert_eq!(page.value.entries.len(), 2);
    assert!(!page.value.has_more);
    assert!(checkout.has_pending_mutations());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
mod spans {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, PoisonError};
    use tracing::field::{Field, Visit};

    /// Each span or event by name (an event by target), with its fields.
    pub(super) type Captured = Arc<Mutex<Vec<(&'static str, Vec<(&'static str, String)>)>>>;

    pub(super) struct Capture(pub(super) Captured, pub(super) Mutex<HashMap<u64, usize>>);

    struct Fields<'a>(&'a mut Vec<(&'static str, String)>);

    impl Visit for Fields<'_> {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.push((field.name(), value.to_owned()));
        }
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0.push((field.name(), format!("{value:?}")));
        }
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
        fn on_new_span(
            &self,
            attributes: &tracing::span::Attributes<'_>,
            id: &tracing::span::Id,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut fields = Vec::new();
            attributes.record(&mut Fields(&mut fields));
            let mut captured = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            let mut spans = self.1.lock().unwrap_or_else(PoisonError::into_inner);
            spans.insert(id.into_u64(), captured.len());
            captured.push((attributes.metadata().name(), fields));
        }
        fn on_record(
            &self,
            id: &tracing::span::Id,
            values: &tracing::span::Record<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let spans = self.1.lock().unwrap_or_else(PoisonError::into_inner);
            let mut captured = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some((_, fields)) = spans
                .get(&id.into_u64())
                .and_then(|&index| captured.get_mut(index))
            {
                values.record(&mut Fields(fields));
            }
        }
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut fields = Vec::new();
            event.record(&mut Fields(&mut fields));
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((event.metadata().target(), fields));
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn commit_records_its_span_and_one_work_receipt() -> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("create blocked")??
        .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    poll_ready(checkout.create_file(
        path("traced")?,
        bytes::Bytes::from_static(b"secret contents"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("create file blocked")??;
    let mut reader = poll_ready(volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("reader checkout blocked")??
    .value;
    // With one live dispatcher, a callsite that a concurrent test reaches
    // first caches only that thread's (absent) interest; a second one makes
    // every callsite consult this test's subscriber too.
    let _second = tracing::Dispatch::new(tracing_subscriber::Registry::default());
    let captured = spans::Captured::default();
    let capture = spans::Capture(Arc::clone(&captured), std::sync::Mutex::default());
    let _default =
        tracing::subscriber::set_default(tracing_subscriber::Registry::default().with(capture));
    let operation = OperationId::from_bytes([31; 16]);
    let committed = poll_ready(checkout.commit(operation, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("commit blocked")??;
    let failed = poll_ready(reader.commit(operation, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("read-only commit blocked")?;
    assert!(failed.is_err());

    let captured = captured.lock().map_err(|_| "capture poisoned")?;
    let named = |name: &str| {
        captured
            .iter()
            .filter(|(found, _)| *found == name)
            .map(|(_, fields)| fields.clone())
            .collect::<Vec<_>>()
    };
    let has = |fields: &[(&str, String)], field: &str, value: &str| {
        fields.contains(&(field, value.to_owned()))
    };
    let commits = named("acyclic.fs.commit");
    assert_eq!(commits.len(), 2, "{captured:?}");
    let work = committed.work;
    let bytes = work.object_bytes_read + work.object_bytes_written + work.authority_bytes_written;
    for (field, value) in [
        ("outcome", "ok".to_owned()),
        ("work.items", work.items_examined.to_string()),
        ("work.bytes", bytes.to_string()),
        ("work.durability", work.durability_operations.to_string()),
    ] {
        assert!(has(&commits[0], field, &value), "{field}: {commits:?}");
    }
    assert!(
        commits[0]
            .iter()
            .any(|(field, value)| *field == "volume_id" && value.len() == 32)
    );
    assert!(has(&commits[1], "outcome", "err"));
    assert!(
        commits[1]
            .iter()
            .any(|(field, value)| *field == "error.kind" && !value.is_empty())
    );
    let publish = named("acyclic.fs.kernel.publish");
    assert!(publish.iter().any(|fields| {
        has(fields, "outcome", "ok")
            && fields
                .iter()
                .any(|(field, value)| *field == "generation" && value.len() == 16)
    }));
    // Nested spans summarise their own receipts; the operation emits the
    // one work event, whose counters are the receipt the caller received.
    let receipts = named("acyclic.work");
    assert_eq!(receipts.len(), 2, "{receipts:?}");
    assert!(has(&receipts[0], "op", "commit") && has(&receipts[0], "outcome", "ok"));
    for (field, value) in [
        ("items_examined", work.items_examined),
        (
            "authority_records_appended",
            work.authority_records_appended,
        ),
        ("peak_allocation_bytes", work.peak_allocation_bytes),
    ] {
        assert!(has(&receipts[0], field, &value.to_string()), "{field}");
    }
    assert!(has(&receipts[1], "outcome", "err"));
    for (_, fields) in captured.iter() {
        for (field, value) in fields {
            assert!(!["path", "token", "content", "body", "authorization"].contains(field));
            assert!(
                !value.contains("traced") && !value.contains("secret"),
                "{field}"
            );
        }
    }
    Ok(())
}

#[test]
fn every_object_backend_cut_preserves_facade_atomicity_and_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("fault fixture create blocked")??
        .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("fault fixture checkout blocked")??
    .value;

    macro_rules! through_every_object_cut {
        ($operation:expr) => {{
            let original_state = checkout_state(&checkout);
            let mut success = None;
            for cut in 1..=256 {
                control.arm(cut);
                let outcome =
                    poll_ready($operation).ok_or("fault-injected facade future blocked")?;
                match outcome {
                    Ok(receipt) => {
                        success = Some(receipt);
                        break;
                    }
                    Err(failure) => {
                        assert!(
                            failure.work.backend_read_operations
                                + failure.work.backend_write_operations
                                > 0
                        );
                        assert_eq!(checkout_state(&checkout), original_state);
                    }
                }
            }
            control.disable();
            success.ok_or_else(|| {
                format!(
                    "facade operation never passed cut sweep: {}",
                    stringify!($operation)
                )
            })?
        }};
    }

    let large = path("fault-large")?;
    let large_id = through_every_object_cut!(checkout.create_file(
        large.clone(),
        bytes::Bytes::from(vec![b'x'; 4_096]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let inline = path("fault-inline")?;
    let inline_id = through_every_object_cut!(checkout.create_file(
        inline.clone(),
        bytes::Bytes::from_static(b"inline"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    through_every_object_cut!(checkout.create_directory(
        path("fault-directory")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let authored = path("fault-authored-large")?;
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateFile {
            path: authored.clone(),
            bytes: bytes::Bytes::from(vec![b'a'; 4_096]),
            metadata: empty_metadata(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateDirectory {
            path: path("fault-authored-directory")?,
            metadata: empty_metadata(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateSymbolicLink {
            path: path("fault-authored-link")?,
            target: OwnedBytes::from_static(b"fault-authored-large"),
            metadata: empty_metadata(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::Write {
            path: authored.clone(),
            offset: 4_096,
            bytes: OwnedBytes::from_static(b"-fault-swept"),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::SetMetadata {
            path: authored.clone(),
            metadata: empty_metadata(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    control.disable();
    let mut staged_source = std::io::Cursor::new(OwnedBytes::from_static(b"staged-fault-content"));
    let staged = poll_ready(checkout.stage_content(
        &mut staged_source,
        64,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("fault fixture content staging blocked")??
    .value;
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateFileFromContent {
            path: path("fault-authored-content")?,
            content: staged,
            metadata: empty_metadata(),
            file_id: None,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));

    let range = ByteRange {
        offset: 1,
        length: 64,
    };
    let path_read = through_every_object_cut!(checkout.read_file_range(
        &large,
        range,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!((path_read.value.bytes).as_ref(), OwnedBytes::from(vec![b'x'; 64]).as_ref());
    let identity_read = through_every_object_cut!(checkout.read_file_range_by_id(
        large_id,
        range,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(identity_read.value.bytes, path_read.value.bytes);
    let path_plan = through_every_object_cut!(checkout.plan_file_extents(
        &large,
        range,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let identity_plan = through_every_object_cut!(checkout.plan_file_extents_by_id(
        large_id,
        range,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        path_plan.value.as_ref().map(|plan| &plan.spans),
        identity_plan.value.as_ref().map(|plan| &plan.spans)
    );
    assert_eq!(
        path_plan.value.as_ref().map(|plan| plan.spans.len()),
        Some(1)
    );
    let data_seek = through_every_object_cut!(checkout.seek_file_extent(
        &large,
        1,
        ExtentSeekTarget::Data,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(data_seek.value, Some(1));
    let hole_seek = through_every_object_cut!(checkout.seek_file_extent_by_id(
        large_id,
        1,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(hole_seek.value, Some(4_096));
    let path_metadata = through_every_object_cut!(checkout.read_metadata(
        &large,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let identity_metadata = through_every_object_cut!(checkout.read_metadata_by_id(
        large_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(identity_metadata.value, path_metadata.value);
    let identity_record = through_every_object_cut!(checkout.read_file_record_by_id(
        large_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(identity_record.value.file_id, large_id);
    let batch = through_every_object_cut!(checkout.lookup_batch_no_follow(
        &[large.clone(), inline.clone(), path("fault-missing")?],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(batch.value.entries.len(), 3);
    assert!(batch.value.entries[0].record.is_some());
    assert!(batch.value.entries[1].record.is_some());
    assert!(batch.value.entries[2].record.is_none());
    let directory_names = through_every_object_cut!(checkout.list_directory(
        &root_path()?,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(directory_names.value.entries.len(), 7);
    let directory = through_every_object_cut!(checkout.list_directory_records(
        &root_path()?,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(directory.value.entries.len(), 7);
    assert!(!directory.value.has_more);
    assert!(
        directory
            .value
            .entries
            .iter()
            .any(|entry| entry.record.file_id == large_id)
    );

    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.fault-cut".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    through_every_object_cut!(checkout.write_named_attribute(
        large.clone(),
        attribute.clone(),
        bytes::Bytes::from_static(b"fault-checked-value"),
        NamedAttributeWriteMode::Upsert,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let attribute_value = through_every_object_cut!(checkout.read_named_attribute(
        &large,
        &attribute,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        attribute_value.value,
        Some(OwnedBytes::from_static(b"fault-checked-value"))
    );
    let attributes = through_every_object_cut!(checkout.list_named_attributes(
        &large,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(attributes.value.entries.len(), 1);
    assert_eq!(attributes.value.entries[0].name, attribute);
    assert!(!attributes.value.has_more);
    let identity_attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.identity-fault-cut".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    through_every_object_cut!(checkout.write_named_attribute_by_id(
        large_id,
        identity_attribute.clone(),
        OwnedBytes::from_static(b"identity-fault-checked-value"),
        NamedAttributeWriteMode::Upsert,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        through_every_object_cut!(checkout.read_named_attribute_by_id(
            large_id,
            &identity_attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        Some(OwnedBytes::from_static(b"identity-fault-checked-value"))
    );
    let identity_attributes = through_every_object_cut!(checkout.list_named_attributes_by_id(
        large_id,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(identity_attributes.value.entries.len(), 2);
    through_every_object_cut!(checkout.remove_named_attribute_by_id(
        large_id,
        identity_attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.remove_named_attribute(
        large.clone(),
        attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.write_file_by_id(
        large_id,
        128,
        bytes::Bytes::from_static(b"atomic-retry"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.clone_file_range_by_id(
        large_id,
        0,
        inline_id,
        0,
        4,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let cloned = through_every_object_cut!(checkout.read_file_range_by_id(
        inline_id,
        ByteRange {
            offset: 0,
            length: 6,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(cloned.value.bytes.as_ref(), b"xxxxne");
    let mut changed_metadata = empty_metadata();
    changed_metadata.modified_ns = MetadataField::Value(101);
    through_every_object_cut!(checkout.set_metadata_by_id(
        large_id,
        changed_metadata,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    changed_metadata.modified_ns = MetadataField::Value(102);
    through_every_object_cut!(checkout.set_attributes_by_id(
        large_id,
        changed_metadata,
        Some(4_224),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.zero_file_range_by_id(
        large_id,
        ByteRange {
            offset: 4_096,
            length: 32,
        },
        true,
        true,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.preallocate_file_by_id(
        large_id,
        ByteRange {
            offset: 4_160,
            length: 32,
        },
        true,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.resize_file_by_id(
        large_id,
        4_192,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    changed_metadata.modified_ns = MetadataField::Value(103);
    through_every_object_cut!(checkout.set_metadata(
        large.clone(),
        changed_metadata,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    changed_metadata.modified_ns = MetadataField::Value(104);
    through_every_object_cut!(checkout.set_attributes(
        large.clone(),
        changed_metadata,
        Some(4_224),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.zero_file_range(
        large.clone(),
        ByteRange {
            offset: 4_096,
            length: 16,
        },
        false,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.preallocate_file(
        large.clone(),
        ByteRange {
            offset: 4_176,
            length: 16,
        },
        true,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.resize_file(
        large.clone(),
        4_192,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.clone_file_range(
        FileCloneRequest {
            source: large.clone(),
            source_offset: 0,
            destination: inline.clone(),
            destination_offset: 1,
            length: 3,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let mut detached = through_every_object_cut!(checkout.detach_regular_file(
        &large,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;

    macro_rules! through_every_detached_cut {
        ($operation:expr) => {{
            let original_record = detached.record;
            let mut success = None;
            for cut in 1..=256 {
                control.arm(cut);
                let outcome =
                    poll_ready($operation).ok_or("fault-injected detached future blocked")?;
                match outcome {
                    Ok(receipt) => {
                        success = Some(receipt);
                        break;
                    }
                    Err(failure) => {
                        assert!(
                            failure.work.backend_read_operations
                                + failure.work.backend_write_operations
                                > 0
                        );
                        assert_eq!(detached.record, original_record);
                    }
                }
            }
            control.disable();
            success.ok_or_else(|| {
                format!(
                    "detached operation never passed cut sweep: {}",
                    stringify!($operation)
                )
            })?
        }};
    }

    let detached_read = through_every_detached_cut!(detached.read_range(
        range,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!((detached_read.value.bytes).as_ref(), OwnedBytes::from(vec![b'x'; 64]).as_ref());
    let detached_seek = through_every_detached_cut!(detached.seek(
        1,
        ExtentSeekTarget::Data,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(detached_seek.value, Some(1));
    let mut detached_metadata =
        through_every_detached_cut!(detached.read_metadata(WorkBudget::UNBOUNDED, &cancellation,))
            .value;
    detached_metadata.modified_ns = MetadataField::Value(42);
    through_every_detached_cut!(detached.set_metadata(
        detached_metadata,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.write_named_attribute(
        attribute.clone(),
        OwnedBytes::from_static(b"detached fault value"),
        NamedAttributeWriteMode::Upsert,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let detached_attribute = through_every_detached_cut!(detached.read_named_attribute(
        &attribute,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        detached_attribute.value,
        Some(OwnedBytes::from_static(b"detached fault value"))
    );
    let detached_attributes = through_every_detached_cut!(detached.list_named_attributes(
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(detached_attributes.value.entries.len(), 1);
    assert_eq!(detached_attributes.value.entries[0].name, attribute);
    through_every_detached_cut!(detached.change_content(
        ContentChange::Write {
            offset: 256,
            bytes: OwnedBytes::from_static(b"detached atomic retry")
        },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.change_content(
        ContentChange::ZeroRange {
            range: ByteRange {
                offset: 512,
                length: 32,
            },
            allocated: true,
            extend: false
        },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.change_content(
        ContentChange::Preallocate {
            range: ByteRange {
                offset: 768,
                length: 32,
            },
            keep_size: false
        },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.change_content(
        ContentChange::Resize {
            logical_bytes: 2_048
        },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.remove_named_attribute(
        attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.set_attributes(
        detached_metadata,
        Some(1_024),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    Ok(())
}

#[test]
fn pinned_reader_batches_ranges_in_request_order() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("create blocked")??
        .value;
    let mut writer = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    poll_ready(writer.create_file(
        path("first")?,
        bytes::Bytes::from_static(b"abcdefgh"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first create blocked")??;
    poll_ready(writer.create_file(
        path("second")?,
        bytes::Bytes::from_static(b"01234567"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second create blocked")??;
    poll_ready(writer.create_symbolic_link(
        path("link")?,
        bytes::Bytes::from_static(b"first"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("link create blocked")??;
    let first_directory = path("first-directory")?;
    let second_directory = path("second-directory")?;
    poll_ready(writer.create_directory(
        first_directory.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first directory create blocked")??;
    poll_ready(writer.create_directory(
        second_directory.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second directory create blocked")??;
    for name in ["a", "b"] {
        poll_ready(writer.create_file(
            path_parts(&["first-directory", name])?,
            bytes::Bytes::from_static(b"x"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("directory child create blocked")??;
    }
    poll_ready(writer.create_file(
        path_parts(&["second-directory", "c"])?,
        bytes::Bytes::from_static(b"y"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second directory child create blocked")??;
    let reader = writer.pinned_reader()?;
    let request_paths = [path("second")?, path("first")?];
    let resolved_batch =
        poll_ready(reader.resolve_files(&request_paths, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("batch resolve blocked")??;
    let second_file = resolved_batch.value[0]
        .as_ref()
        .ok_or("second resolved file absent")?;
    let first_file = resolved_batch.value[1]
        .as_ref()
        .ok_or("first resolved file absent")?;
    let requests = [
        ResolvedFileRangeReadRequest {
            file: second_file,
            range: ByteRange {
                offset: 2,
                length: 3,
            },
        },
        ResolvedFileRangeReadRequest {
            file: first_file,
            range: ByteRange {
                offset: 1,
                length: 4,
            },
        },
    ];
    let batch =
        poll_ready(reader.read_resolved_ranges(&requests, 2, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("batch blocked")??;
    assert_eq!(&batch.value[0].bytes[..], b"234");
    assert_eq!(&batch.value[1].bytes[..], b"bcde");

    let descriptions = poll_ready(reader.describe_files(
        &[path("first")?, path("second")?, path("first")?],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("metadata batch blocked")??;
    assert_eq!(descriptions.value.len(), 3);
    assert_eq!(descriptions.value[0], descriptions.value[2]);
    assert!(descriptions.value.iter().all(Option::is_some));
    let resolved =
        poll_ready(reader.resolve_files(&[path("first")?], WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("resolve file blocked")??;
    let resolved_file = resolved.value[0].as_ref().ok_or("resolved file absent")?;
    let resolved_read = poll_ready(resolved_file.read_range(
        ByteRange {
            offset: 1,
            length: 4,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("resolved read blocked")??;
    assert_eq!(&resolved_read.value.bytes[..], b"bcde");
    assert_eq!(resolved_read.work.page_reads, 0);
    let other_volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("other volume create blocked")??
        .value;
    let mut other_writer = poll_ready(other_volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("other checkout blocked")??
    .value;
    poll_ready(other_writer.create_file(
        path("foreign")?,
        bytes::Bytes::from_static(b"WXYZ"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("other file create blocked")??;
    let other_reader = other_writer.pinned_reader()?;
    let other_resolved = poll_ready(other_reader.resolve_files(
        &[path("foreign")?],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("other resolve blocked")??;
    let other_file = other_resolved.value[0]
        .as_ref()
        .ok_or("other resolved file absent")?;
    let mixed_requests = [
        ResolvedFileRangeReadRequest {
            file: resolved_file,
            range: ByteRange {
                offset: 0,
                length: 2,
            },
        },
        ResolvedFileRangeReadRequest {
            file: other_file,
            range: ByteRange {
                offset: 1,
                length: 2,
            },
        },
    ];
    let mixed = poll_ready(reader.read_resolved_ranges(
        &mixed_requests,
        2,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("mixed resolved batch blocked")??;
    assert_eq!(&mixed.value[0].bytes[..], b"ab");
    assert_eq!(&mixed.value[1].bytes[..], b"XY");
    let bounded_mixed =
        poll_ready(reader.read_resolved_ranges(&mixed_requests, 2, mixed.work, &cancellation))
            .ok_or("bounded mixed resolved batch blocked")??;
    assert_eq!(bounded_mixed.value, mixed.value);
    let target =
        poll_ready(reader.read_symbolic_link(&path("link")?, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("symlink read blocked")??;
    assert_eq!(&target.value[..], b"first");
    assert!(matches!(
        poll_ready(reader.read_symbolic_link(
            &path("first")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("wrong-kind symlink read blocked")?,
        Err(OperationFailure {
            error: FsError::NotSymbolicLink,
            ..
        })
    ));
    let pages = poll_ready(reader.list_directory_pages(
        &[
            DirectoryPageRequest {
                path: second_directory,
                after: None,
                maximum_entries: 8,
            },
            DirectoryPageRequest {
                path: first_directory.clone(),
                after: Some(LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 255)?),
                maximum_entries: 1,
            },
        ],
        2,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("directory page batch blocked")??;
    let second_page = pages.value.first().ok_or("second directory page missing")?;
    let first_page = pages.value.get(1).ok_or("first directory page missing")?;
    assert_eq!(
        second_page
            .entries
            .first()
            .ok_or("second entry missing")?
            .name
            .as_bytes(),
        b"c"
    );
    assert_eq!(
        first_page
            .entries
            .first()
            .ok_or("first entry missing")?
            .name
            .as_bytes(),
        b"b"
    );
    assert!(!second_page.has_more);
    assert!(!first_page.has_more);
    let resolved_page = poll_ready(reader.resolve_directory_page(
        &first_directory,
        None,
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("resolved directory page blocked")??;
    assert_eq!(resolved_page.value.entries.len(), 1);
    assert!(resolved_page.value.has_more);
    let resolved_entry = resolved_page
        .value
        .entries
        .first()
        .ok_or("resolved directory entry missing")?;
    assert_eq!(resolved_entry.name.as_bytes(), b"a");
    assert_eq!(resolved_entry.file.description().kind, FileKind::Regular);
    let resolved_child = poll_ready(resolved_entry.file.read_range(
        ByteRange {
            offset: 0,
            length: 1,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("resolved child read blocked")??;
    assert_eq!(&resolved_child.value.bytes[..], b"x");
    assert_eq!(resolved_child.work.page_reads, 0);
    assert!(
        poll_ready(reader.list_directory_pages(&[], 0, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("zero-concurrency directory batch blocked")?
            .is_err()
    );

    let first = poll_ready(reader.read_file_range(
        &request_paths[0],
        requests[0].range,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("single read blocked")??;
    let second = poll_ready(reader.read_file_range(
        &request_paths[1],
        requests[1].range,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("single read blocked")??;
    let independent = first.work.checked_add(second.work)?;
    assert!(batch.work.object_probes < independent.object_probes);
    assert!(batch.work.backend_read_operations < independent.backend_read_operations);
    assert!(batch.work.page_reads < independent.page_reads);

    let Err(bounded_failure) =
        poll_ready(reader.read_resolved_ranges(&requests, 2, first.work, &cancellation))
            .ok_or("bounded batch blocked")?
    else {
        return Err("the second range received the first range's full budget".into());
    };
    bounded_failure.work.verify(first.work)?;

    let concurrent_failure_requests = [
        ResolvedFileRangeReadRequest {
            file: second_file,
            range: ByteRange {
                offset: 0,
                length: u64::MAX,
            },
        },
        ResolvedFileRangeReadRequest {
            file: first_file,
            range: requests[1].range,
        },
    ];
    let Err(concurrent_failure) = poll_ready(reader.read_resolved_ranges(
        &concurrent_failure_requests,
        2,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("concurrent failure batch blocked")?
    else {
        return Err("the invalid concurrent range unexpectedly succeeded".into());
    };
    assert_eq!(
        concurrent_failure.work.backend_read_operations,
        batch.work.backend_read_operations
    );
    assert_eq!(concurrent_failure.work.page_reads, batch.work.page_reads);
    assert_eq!(
        concurrent_failure.work.output_bytes,
        second.work.output_bytes
    );

    let tracking = poll_ready(volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("tracking checkout blocked")??
    .value;
    assert!(matches!(
        tracking.pinned_reader(),
        Err(FsError::MutationNotAllowed)
    ));
    Ok(())
}

#[test]
fn checkout_snapshot_reader_is_private_and_stable() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("create blocked")??
        .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let file_path = path("private")?;
    let file_id = poll_ready(checkout.create_file(
        file_path.clone(),
        bytes::Bytes::from_static(b"first"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("create file blocked")??
    .value;
    let snapshot = checkout.snapshot_reader();
    let resolved = poll_ready(snapshot.resolve_files(
        std::slice::from_ref(&file_path),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("snapshot resolve blocked")??
    .value
    .into_iter()
    .next()
    .flatten()
    .ok_or("private file missing from snapshot")?;
    assert_eq!(resolved.file_id(), file_id);

    poll_ready(checkout.write_file(
        file_path,
        0,
        bytes::Bytes::from_static(b"later"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("write blocked")??;
    let bytes = poll_ready(resolved.read_range(
        ByteRange {
            offset: 0,
            length: 5,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("snapshot read blocked")??
    .value
    .bytes;
    assert_eq!(&bytes[..], b"first");
    Ok(())
}

#[test]
fn every_authority_backend_cut_preserves_creation_checkout_and_commit_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        FaultAuthorityStore {
            inner: crate::memory::MemoryAuthorityStore::default(),
            control: Arc::clone(&control),
        },
        crate::memory::MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();

    macro_rules! through_every_authority_cut {
        ($operation:expr) => {{
            let mut success = None;
            for cut in 1..=64 {
                control.arm(cut);
                let outcome =
                    poll_ready($operation).ok_or("fault-injected authority future blocked")?;
                match outcome {
                    Ok(receipt) => {
                        success = Some(receipt);
                        break;
                    }
                    Err(failure) => assert!(
                        failure
                            .work
                            .authority_records_read
                            .saturating_add(failure.work.authority_records_appended)
                            > 0
                    ),
                }
            }
            control.disable();
            success.ok_or("authority operation never passed the complete cut-point sweep")?
        }};
    }

    let volume_id = VolumeId::from_bytes([31; 16]);
    let volume = through_every_authority_cut!(fs.create_volume_with_id(
        volume_id,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    assert_eq!(volume.id(), volume_id);
    let reopened = through_every_authority_cut!(fs.open_volume(
        volume_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut checkout = through_every_authority_cut!(reopened.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    control.disable();
    poll_ready(checkout.create_file(
        path("authority-cut-file")?,
        bytes::Bytes::from_static(b"authority cut candidate"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("authority cut candidate creation blocked")??;
    let candidate = checkout.generation_id();
    assert!(checkout.has_pending_mutations());
    let commit_operation = OperationId::from_bytes([33; 16]);
    let mut committed = None;
    for cut in 1..=64 {
        control.arm(cut);
        let outcome =
            poll_ready(checkout.commit(commit_operation, WorkBudget::UNBOUNDED, &cancellation))
                .ok_or("fault-injected commit blocked")?;
        match outcome {
            Ok(receipt) => {
                committed = Some(receipt);
                break;
            }
            Err(failure) => {
                assert!(
                    failure
                        .work
                        .authority_records_read
                        .saturating_add(failure.work.authority_records_appended)
                        > 0
                );
                assert_eq!(checkout.generation_id(), candidate);
                assert!(checkout.has_pending_mutations());
            }
        }
    }
    control.disable();
    let committed = committed.ok_or("commit never passed the complete authority cut sweep")?;
    let committed_generation = match committed.value {
        CheckoutCommitOutcome::Committed { generation_id, .. }
        | CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } => generation_id,
        CheckoutCommitOutcome::Conflict { .. }
        | CheckoutCommitOutcome::Fenced { .. }
        | CheckoutCommitOutcome::IdempotencyConflict { .. } => {
            return Err("exact retry returned a semantic authority conflict".into());
        }
    };
    assert_eq!(checkout.generation_id(), committed_generation);
    assert!(!checkout.has_pending_mutations());

    let live_id = VolumeId::from_bytes([32; 16]);
    let live_volume = through_every_authority_cut!(fs.create_volume_with_id(
        live_id,
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut live = through_every_authority_cut!(live_volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let live_before = live.generation_id();
    control.disable();
    poll_ready(live.create_file(
        path("live-authority-cut")?,
        bytes::Bytes::from_static(b"live"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("live candidate mutation blocked")??;
    assert!(live.has_pending_mutations());
    let live_operation = OperationId::from_bytes([34; 16]);
    let mut published = None;
    for cut in 1..=64 {
        control.arm(cut);
        let outcome = poll_ready(live.resume_live(
            live_operation,
            3,
            16,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("fault-injected live publication blocked")?;
        match outcome {
            Ok(receipt) => {
                published = Some(receipt);
                break;
            }
            Err(failure) => {
                assert!(
                    failure
                        .work
                        .authority_records_read
                        .saturating_add(failure.work.authority_records_appended)
                        > 0
                );
                assert_eq!(live.generation_id(), live_before);
                assert!(live.has_pending_mutations());
            }
        }
    }
    control.disable();
    let published = published.ok_or("live publication never passed the authority cut sweep")?;
    assert!(matches!(
        published.value,
        LiveMutationOutcome::Committed { .. } | LiveMutationOutcome::AlreadyCommitted { .. }
    ));
    assert_ne!(live.generation_id(), live_before);
    assert!(!live.has_pending_mutations());
    Ok(())
}

#[test]
fn every_post_put_creation_fault_retries_from_a_complete_authenticated_closure()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut reached_clean_completion = false;
    for cut in 1..=32 {
        let control = Arc::new(FaultControl::disabled());
        control.arm(cut);
        let objects = Arc::new(crate::memory::MemoryObjectStore::default());
        let fs = Fs::new(
            crate::memory::MemoryAuthorityStore::default(),
            PostPutObjectStore {
                inner: Arc::clone(&objects),
                control: Arc::clone(&control),
            },
            EmbeddedCapabilities::MEMORY,
        );
        let volume_id = VolumeId::from_bytes(
            [u8::try_from(cut).map_err(|_| "post-put cut does not fit identity")?; 16],
        );
        let first = poll_ready(fs.create_volume_with_id(
            volume_id,
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("post-put creation future blocked")?;
        match first {
            Ok(created) => {
                assert_eq!(created.value.id(), volume_id);
                reached_clean_completion = true;
                break;
            }
            Err(failure) => {
                assert!(
                    matches!(failure.error, FsError::Object(ObjectStoreError::Corrupt)),
                    "unexpected creation failure: {:#?}",
                    failure.error
                );
                assert_eq!(
                    failure.work.backend_write_operations,
                    u64::try_from(cut).map_err(|_| "post-put cut does not fit work counter")?
                );
                control.disable();
                let retried = poll_ready(fs.create_volume_with_id(
                    volume_id,
                    config(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                ))
                .ok_or("post-put creation retry blocked")??;
                assert_eq!(retried.value.id(), volume_id);
                let reopened =
                    poll_ready(fs.open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation))
                        .ok_or("post-put volume reopen blocked")??;
                let checkout = poll_ready(reopened.value.checkout(
                    GenerationSelector::Head,
                    pinned(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                ))
                .ok_or("post-put checkout blocked")??;
                assert_eq!(checkout.value.volume_id(), volume_id);
                assert!(!checkout.value.has_pending_mutations());
            }
        }
    }
    assert!(reached_clean_completion);
    Ok(())
}

#[test]
fn every_pre_object_fault_preserves_creation_and_checkout_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([50; 16]);
    let mut volume = None;
    for cut in 1..=64 {
        control.arm(cut);
        match poll_ready(fs.create_volume_with_id(
            volume_id,
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("pre-object creation future blocked")?
        {
            Ok(receipt) => {
                volume = Some(receipt.value);
                break;
            }
            Err(failure) => {
                assert!(
                    matches!(
                        failure.error,
                        FsError::Object(ObjectStoreError::Corrupt)
                            | FsError::Closure(ClosureError::Storage(ObjectStoreError::Corrupt))
                    ),
                    "unexpected creation failure: {:#?}",
                    failure.error
                );
                assert!(failure.work.backend_write_operations > 0);
            }
        }
    }
    control.disable();
    let volume = volume.ok_or("creation never passed the complete object cut sweep")?;
    assert_eq!(volume.id(), volume_id);

    let mut checkout = None;
    for cut in 1..=64 {
        control.arm(cut);
        match poll_ready(volume.checkout(
            GenerationSelector::Head,
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("pre-object checkout future blocked")?
        {
            Ok(receipt) => {
                checkout = Some(receipt.value);
                break;
            }
            Err(failure) => {
                assert!(
                    matches!(failure.error, FsError::Object(ObjectStoreError::Corrupt)),
                    "unexpected checkout failure: {:#?}",
                    failure.error
                );
                assert!(failure.work.backend_read_operations > 0);
            }
        }
    }
    control.disable();
    let checkout = checkout.ok_or("checkout never passed the complete object cut sweep")?;
    assert_eq!(checkout.volume_id(), volume_id);
    assert!(!checkout.has_pending_mutations());
    Ok(())
}

#[test]
fn every_object_cut_preserves_sparse_rebase_retry_and_candidate_state()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("rebase fixture creation blocked")??
        .value;
    let mut writer = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("writer checkout blocked")??
    .value;
    let mut rebasing = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("tracking checkout blocked")??
    .value;
    poll_ready(rebasing.create_file(
        path("local")?,
        bytes::Bytes::from_static(b"local sparse candidate"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("local mutation blocked")??;
    poll_ready(writer.create_file(
        path("remote")?,
        bytes::Bytes::from_static(b"remote sparse head"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote mutation blocked")??;
    poll_ready(writer.commit(
        OperationId::from_bytes([227; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote commit blocked")??;

    let original = checkout_state(&rebasing);
    let mut completed = None;
    for cut in 1..=512 {
        control.arm(cut);
        match poll_ready(rebasing.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("fault-injected rebase blocked")?
        {
            Ok(receipt) => {
                completed = Some(receipt);
                break;
            }
            Err(failure) => {
                assert!(failure.work.backend_read_operations > 0);
                assert_eq!(checkout_state(&rebasing), original);
            }
        }
    }
    control.disable();
    let completed = completed.ok_or("rebase never passed the complete object cut sweep")?;
    assert!(matches!(completed.value, RebaseDecision::Safe { .. }));
    assert!(rebasing.has_pending_mutations());
    assert!(
        poll_ready(rebasing.lookup_no_follow(
            &path("local")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("local lookup blocked")??
        .value
        .record
        .is_some()
    );
    assert!(
        poll_ready(rebasing.lookup_no_follow(
            &path("remote")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("remote lookup blocked")??
        .value
        .record
        .is_some()
    );
    Ok(())
}

#[test]
fn every_object_cut_preserves_diff_export_and_merge_preparation()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("merge fixture creation blocked")??
        .value;
    let base = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("base checkout blocked")??
    .value
    .generation_id();
    let mut ours = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("ours checkout blocked")??
    .value;
    let mut theirs = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("theirs checkout blocked")??
    .value;
    poll_ready(ours.create_file(
        path("ours")?,
        bytes::Bytes::from(vec![b'o'; 4_096]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("ours mutation blocked")??;
    poll_ready(theirs.create_file(
        path("theirs")?,
        bytes::Bytes::from(vec![b't'; 4_096]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("theirs mutation blocked")??;
    let ours_generation = poll_ready(ours.checkpoint(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("ours checkpoint blocked")??
        .value;
    let theirs_generation = poll_ready(theirs.checkpoint(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("theirs checkpoint blocked")??
        .value;
    poll_ready(theirs.commit(
        OperationId::from_bytes([228; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("theirs commit blocked")??;

    macro_rules! sweep_read_only {
        ($operation:expr) => {{
            let original = checkout_state(&ours);
            let mut completed = None;
            for cut in 1..=4_096 {
                control.arm(cut);
                match poll_ready($operation).ok_or("fault-injected operation blocked")? {
                    Ok(receipt) => {
                        completed = Some(receipt);
                        break;
                    }
                    Err(failure) => {
                        assert!(failure.work.backend_read_operations > 0);
                        assert_eq!(checkout_state(&ours), original);
                    }
                }
            }
            control.disable();
            completed.ok_or_else(|| {
                format!(
                    "operation never passed object cut sweep: {}",
                    stringify!($operation)
                )
            })?
        }};
    }

    let diff = sweep_read_only!(volume.diff_generations(
        base,
        ours_generation,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(diff.value.files.len() + diff.value.bindings.len(), 3);
    let manifest = sweep_read_only!(ours.export_manifest(WorkBudget::UNBOUNDED, &cancellation));
    assert_eq!(manifest.value.volume_id, volume.id());
    assert_eq!(
        manifest.value.generation_root,
        ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: ours_generation.digest(),
        }
    );
    let prepared = sweep_read_only!(ours.prepare_merge(
        theirs_generation,
        8,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(prepared.value, MergePreparation::Prepared { .. }));
    assert!(ours.has_pending_mutations());
    for expected in ["ours", "theirs"] {
        assert!(
            poll_ready(ours.lookup_no_follow(
                &path(expected)?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .ok_or("merged lookup blocked")??
            .value
            .record
            .is_some()
        );
    }
    Ok(())
}

#[test]
fn every_object_cut_preserves_authenticated_publication_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("publication fixture creation blocked")??
        .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("publication checkout blocked")??
    .value;
    poll_ready(checkout.create_file(
        path("publish-large")?,
        bytes::Bytes::from(vec![b'p'; 16_384]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("publication mutation blocked")??;
    poll_ready(checkout.write_named_attribute(
        path("publish-large")?,
        AttributeName::new(
            crate::kernel::AttributeClass::PosixXattr,
            b"user.publication".to_vec(),
            config().limits.maximum_component_bytes,
        )?,
        bytes::Bytes::from(vec![b'a'; 4_096]),
        NamedAttributeWriteMode::Create,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("publication attribute blocked")??;

    let original = checkout_state(&checkout);
    let operation_id = OperationId::from_bytes([229; 16]);
    let mut completed = None;
    for cut in 1..=4_096 {
        control.arm(cut);
        match poll_ready(checkout.commit(operation_id, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("fault-injected publication blocked")?
        {
            Ok(receipt) => {
                completed = Some(receipt);
                break;
            }
            Err(failure) => {
                assert!(
                    failure.work.backend_read_operations + failure.work.backend_write_operations
                        > 0
                );
                assert_eq!(checkout_state(&checkout), original);
            }
        }
    }
    control.disable();
    let completed = completed.ok_or("publication never passed the complete object cut sweep")?;
    assert!(matches!(
        completed.value,
        CheckoutCommitOutcome::Committed { .. }
    ));
    assert!(!checkout.has_pending_mutations());
    Ok(())
}

#[test]
fn indeterminate_post_append_retry_resolves_one_durable_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let authority = Arc::new(crate::memory::MemoryAuthorityStore::default());
    let fs = Fs::new(
        PostAppendAuthorityStore {
            inner: Arc::clone(&authority),
            control: Arc::clone(&control),
        },
        crate::memory::MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("indeterminate fixture create blocked")??
        .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("indeterminate fixture checkout blocked")??
    .value;
    poll_ready(checkout.create_file(
        path("indeterminate-file")?,
        bytes::Bytes::from_static(b"durable exactly once"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("indeterminate fixture mutation blocked")??;
    let before = checkout_state(&checkout);
    let operation_id = OperationId::from_bytes([49; 16]);
    control.arm(1);
    let failure = poll_ready(checkout.commit(operation_id, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("indeterminate commit blocked")?
        .err()
        .ok_or("post-append acknowledgement loss unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        FsError::Publication(PublicationError::Authority(
            AuthorityStoreError::Indeterminate { .. }
        ))
    ));
    assert_eq!(checkout_state(&checkout), before);
    assert!(
        AuthorityStore::find_operation(
            &*authority,
            volume_authority_id(volume.id()),
            operation_id,
            WorkBudget::UNBOUNDED,
        )?
        .value
        .is_some()
    );

    control.disable();
    let resolved = poll_ready(checkout.commit(operation_id, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("indeterminate commit retry blocked")??;
    assert!(matches!(
        resolved.value,
        CheckoutCommitOutcome::AlreadyCommitted { .. }
    ));
    assert!(!checkout.has_pending_mutations());
    let history = AuthorityStore::replay(
        &*authority,
        volume_authority_id(volume.id()),
        Sequence::GENESIS,
        ReplayLimit {
            records: 8,
            payload_bytes: 64 * 1024,
        },
        WorkBudget::UNBOUNDED,
    )?
    .value;
    assert_eq!(
        history
            .iter()
            .filter(|commit| commit.operation_id == operation_id)
            .count(),
        1
    );
    let mut reopened = poll_ready(volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("indeterminate committed checkout blocked")??;
    let bytes = poll_ready(reopened.value.read_file_range(
        &path("indeterminate-file")?,
        ByteRange {
            offset: 0,
            length: 20,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("indeterminate committed read blocked")??;
    assert_eq!(bytes.value.bytes.as_ref(), b"durable exactly once");
    Ok(())
}

#[test]
fn detached_open_file_remains_sparse_and_mutable_after_last_binding_removal()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("create blocked")??
        .value;
    let file = path("open-file")?;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    poll_ready(checkout.create_file(
        file.clone(),
        bytes::Bytes::from(vec![b'a'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("create file blocked")??;
    let detached =
        poll_ready(checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("detach blocked")??;
    let mut detached = detached.value;
    let file_id = detached.file_id();
    assert_eq!(
        poll_ready(detached.seek(
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("detached data seek blocked")??
        .value,
        Some(0)
    );
    assert_eq!(
        poll_ready(detached.seek(
            0,
            ExtentSeekTarget::Hole,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("detached hole seek blocked")??
        .value,
        Some(128)
    );
    for (offset, target, expected) in [
        (128, ExtentSeekTarget::Data, None),
        (128, ExtentSeekTarget::Hole, Some(128)),
        (129, ExtentSeekTarget::Data, None),
        (129, ExtentSeekTarget::Hole, None),
    ] {
        assert_eq!(
            poll_ready(detached.seek(offset, target, WorkBudget::UNBOUNDED, &cancellation,))
                .ok_or("detached boundary seek blocked")??
                .value,
            expected
        );
    }

    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.detached".to_vec(),
        128,
    )?;
    assert!(
        poll_ready(
            detached.read_named_attribute(&attribute, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("detached attribute read blocked")??
        .value
        .is_none()
    );
    assert!(
        poll_ready(detached.list_named_attributes(None, 8, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("detached attribute listing blocked")??
            .value
            .entries
            .is_empty()
    );
    let absent_remove = poll_ready(detached.remove_named_attribute(
        attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("attribute-free detached removal blocked")?
    .err()
    .ok_or("attribute-free detached removal unexpectedly succeeded")?;
    assert!(matches!(absent_remove.error, FsError::NotFound));
    let absent_replace = poll_ready(detached.write_named_attribute(
        attribute.clone(),
        bytes::Bytes::from_static(b"replacement"),
        NamedAttributeWriteMode::Replace,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("absent detached replacement blocked")?
    .err()
    .ok_or("absent detached replacement unexpectedly succeeded")?;
    assert!(matches!(absent_replace.error, FsError::CreationRejected));
    poll_ready(detached.write_named_attribute(
        attribute.clone(),
        bytes::Bytes::from_static(b"first"),
        NamedAttributeWriteMode::Create,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("detached attribute creation blocked")??;
    assert_eq!(
        poll_ready(
            detached.read_named_attribute(&attribute, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("detached attribute reread blocked")??
        .value
        .as_deref(),
        Some(b"first".as_slice())
    );
    assert_eq!(
        poll_ready(detached.list_named_attributes(None, 8, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("detached populated attribute listing blocked")??
            .value
            .entries
            .len(),
        1
    );
    assert!(
        poll_ready(detached.write_named_attribute(
            attribute.clone(),
            bytes::Bytes::from_static(b"duplicate"),
            NamedAttributeWriteMode::Create,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("detached duplicate attribute write blocked")?
        .is_err()
    );
    poll_ready(detached.write_named_attribute(
        attribute.clone(),
        bytes::Bytes::from_static(b"second"),
        NamedAttributeWriteMode::Replace,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("detached attribute replacement blocked")??;

    let mut metadata = poll_ready(detached.read_metadata(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("detached metadata read blocked")??
        .value;
    metadata.posix_mode = MetadataField::Value(0o100_640);
    poll_ready(detached.set_metadata(metadata, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("detached metadata write blocked")??;
    metadata.posix_mode = MetadataField::Value(0o100_600);
    poll_ready(detached.set_attributes(metadata, None, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("detached metadata-only attributes write blocked")??;
    assert_eq!(detached.logical_bytes(), 128);
    poll_ready(detached.set_attributes(metadata, Some(160), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("detached attributes write blocked")??;
    assert_eq!(detached.logical_bytes(), 160);
    poll_ready(checkout.remove(
        file.clone(),
        Some(file_id),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remove blocked")??;
    let absent = poll_ready(checkout.lookup_no_follow(&file, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("lookup blocked")??;
    assert!(absent.value.record.is_none());

    poll_ready(detached.change_content(
        ContentChange::Write { offset: 0, bytes: OwnedBytes::from_static(b"open") },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("detached write blocked")??;
    poll_ready(detached.change_content(
        ContentChange::Write { offset: 4, bytes: OwnedBytes::new() },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("detached empty write blocked")??;
    poll_ready(detached.change_content(
        ContentChange::Preallocate {
            range: ByteRange {
                offset: 128,
                length: 64,
            },
            keep_size: false,
        },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("detached preallocation blocked")??;
    poll_ready(detached.change_content(
        ContentChange::ZeroRange {
            range: ByteRange {
                offset: 64,
                length: 16,
            },
            allocated: false,
            extend: false,
        },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("detached hole blocked")??;
    let read = poll_ready(detached.read_range(
        ByteRange {
            offset: 0,
            length: 80,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("detached read blocked")??;
    assert_eq!(&read.value.bytes[..4], b"open");
    assert_eq!(&read.value.bytes[64..80], &[0; 16]);
    poll_ready(detached.change_content(
        ContentChange::Resize { logical_bytes: 192 },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("detached resize blocked")??;
    assert_eq!(detached.logical_bytes(), 192);
    poll_ready(detached.remove_named_attribute(
        attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("detached attribute removal blocked")??;
    assert!(
        poll_ready(detached.remove_named_attribute(
            attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("detached absent attribute removal blocked")?
        .is_err()
    );
    Ok(())
}

#[test]
fn tracking_reads_capture_only_their_exact_terminal_regions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([81; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let file = path("file")?;
    let mut seed = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed checkout blocked")??
    .value;
    poll_ready(seed.create_file(
        file.clone(),
        bytes::Bytes::from(vec![b'a'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed create blocked")??;
    poll_ready(seed.commit(
        OperationId::from_bytes([82; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed commit blocked")??;

    let mut content_reader = poll_ready(volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("content reader blocked")??
    .value;
    poll_ready(content_reader.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 8,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("content observation blocked")??;
    let mut metadata_writer = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("metadata writer blocked")??
    .value;
    let mut changed_metadata = empty_metadata();
    changed_metadata.modified_ns = MetadataField::Value(7);
    poll_ready(metadata_writer.set_metadata(
        file.clone(),
        changed_metadata,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("metadata mutation blocked")??;
    poll_ready(metadata_writer.commit(
        OperationId::from_bytes([83; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("metadata commit blocked")??;
    let content_rebase =
        poll_ready(content_reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("content rebase blocked")??;
    assert!(matches!(content_rebase.value, RebaseDecision::Safe { .. }));

    let mut metadata_reader = poll_ready(volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("metadata reader blocked")??
    .value;
    poll_ready(metadata_reader.read_metadata(&file, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("metadata observation blocked")??;
    let mut content_writer = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("content writer blocked")??
    .value;
    poll_ready(content_writer.write_file(
        file.clone(),
        64,
        bytes::Bytes::from_static(b"changed"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("content mutation blocked")??;
    poll_ready(content_writer.commit(
        OperationId::from_bytes([84; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("content commit blocked")??;
    let metadata_rebase =
        poll_ready(metadata_reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("metadata rebase blocked")??;
    assert!(matches!(metadata_rebase.value, RebaseDecision::Safe { .. }));

    let mut generic_reader = poll_ready(volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("generic reader blocked")??
    .value;
    poll_ready(generic_reader.lookup_no_follow(&file, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("generic observation blocked")??;
    let mut final_writer = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("final writer blocked")??
    .value;
    poll_ready(final_writer.write_file(
        file,
        96,
        bytes::Bytes::from_static(b"different"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("final mutation blocked")??;
    poll_ready(final_writer.commit(
        OperationId::from_bytes([85; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("final commit blocked")??;
    let generic_rebase =
        poll_ready(generic_reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("generic rebase blocked")??;
    assert!(matches!(
        generic_rebase.value,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::FileRecord(_)
            ))
    ));
    Ok(())
}

#[test]
fn sparse_seek_dependencies_track_base_semantics_and_exact_observed_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([101; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let file = path("sparse")?;
    let mut seed = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed checkout blocked")??
    .value;
    poll_ready(seed.create_file(
        file.clone(),
        bytes::Bytes::from(vec![b'a'; 256]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed create blocked")??;
    poll_ready(seed.commit(
        OperationId::from_bytes([102; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed commit blocked")??;

    let mut local = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("local checkout blocked")??
    .value;
    poll_ready(local.zero_file_range(
        file.clone(),
        ByteRange {
            offset: 128,
            length: 16,
        },
        false,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("local hole blocked")??;
    let local_observation = poll_ready(local.seek_file_extent(
        &file,
        128,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("local seek blocked")??;
    assert_eq!(local_observation.value, Some(128));

    let mut disjoint = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("disjoint checkout blocked")??
    .value;
    poll_ready(disjoint.zero_file_range(
        file.clone(),
        ByteRange {
            offset: 0,
            length: 64,
        },
        false,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("disjoint hole blocked")??;
    poll_ready(disjoint.commit(
        OperationId::from_bytes([103; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("disjoint commit blocked")??;
    let safe = poll_ready(local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("local rebase blocked")??;
    assert!(matches!(safe.value, RebaseDecision::Safe { .. }));
    let retained = poll_ready(local.seek_file_extent(
        &file,
        128,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("retained local seek blocked")??;
    assert_eq!(retained.value, Some(128));

    let mut reader = poll_ready(volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("reader checkout blocked")??
    .value;
    let observation = poll_ready(reader.seek_file_extent(
        &file,
        128,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("reader seek blocked")??;
    assert_eq!(observation.value, Some(256));

    let mut conflicting = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("conflicting checkout blocked")??
    .value;
    poll_ready(conflicting.zero_file_range(
        file,
        ByteRange {
            offset: 192,
            length: 16,
        },
        false,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("conflicting hole blocked")??;
    poll_ready(conflicting.commit(
        OperationId::from_bytes([104; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("conflicting commit blocked")??;
    let conflict = poll_ready(reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("conflicting rebase blocked")??;
    assert!(matches!(
        conflict.value,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::SparseSeek {
                    offset: 128,
                    target: ExtentSeekTarget::Hole,
                    ..
                }
            ))
    ));

    Ok(())
}

#[test]
fn clipped_identity_read_at_eof_tracks_file_length() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([190; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let file = path("file")?;
    let mut seed = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed checkout blocked")??
    .value;
    let file_id = poll_ready(seed.create_file(
        file.clone(),
        bytes::Bytes::from_static(b"abc"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed create blocked")??
    .value;
    poll_ready(seed.commit(
        OperationId::from_bytes([190; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed commit blocked")??;

    let mut reader = poll_ready(volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("reader checkout blocked")??
    .value;
    let at_eof = poll_ready(reader.read_file_up_to_by_id(
        file_id,
        3,
        16,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("EOF read blocked")??;
    assert!(at_eof.value.bytes.is_empty());

    let mut writer = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("writer checkout blocked")??
    .value;
    poll_ready(writer.write_file(
        file,
        3,
        bytes::Bytes::from_static(b"d"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("append blocked")??;
    poll_ready(writer.commit(
        OperationId::from_bytes([191; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("append commit blocked")??;
    let rebase = poll_ready(reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("reader rebase blocked")??;
    assert!(matches!(
        rebase.value,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::FileLength(actual) if actual == file_id
            ))
    ));
    Ok(())
}

#[test]
fn sparse_local_writes_rebase_across_disjoint_remote_ranges_and_conflict_on_overlap()
-> Result<(), Box<dyn std::error::Error>> {
    sparse_rebase_across_remote_ranges(false)
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn original_native_rebase_preserves_disjoint_ranges_and_is_atomic_on_denial_and_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    sparse_rebase_across_remote_ranges(true)
}

fn sparse_rebase_across_remote_ranges(native: bool)
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([86; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let file = path("file")?;
    let mut seed = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed checkout blocked")??
    .value;
    poll_ready(seed.create_file(
        file.clone(),
        bytes::Bytes::from(vec![b'a'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed create blocked")??;
    poll_ready(seed.commit(
        OperationId::from_bytes([87; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed commit blocked")??;

    let mut local = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("local checkout blocked")??
    .value;
    let mut remote = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote checkout blocked")??
    .value;
    poll_ready(local.write_file(
        file.clone(),
        0,
        bytes::Bytes::from_static(b"LOCAL"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("local write blocked")??;
    poll_ready(remote.write_file(
        file.clone(),
        64,
        bytes::Bytes::from_static(b"REMOTE"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote write blocked")??;
    poll_ready(remote.commit(
        OperationId::from_bytes([88; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote commit blocked")??;
    #[cfg(not(target_arch = "wasm32"))]
    if native {
        let before = (
            local.generation_id(), local.root.file_table, local.base_file_table,
            local.authority_head, local.authored_operation_id, local.live_operation_id,
            local.dependencies.proof().dependencies.clone(),
        );
        let denied = crate::NativeMaterializationWork::new(
            WorkBudget { allocation_operations: 1, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).map_err(|(cause, _)| cause)?;
        assert!(poll_ready(local.rebase_head_with_native_work(8, &denied, &cancellation))
            .ok_or("native denial rebase blocked")?.is_err());
        assert_eq!(before, (
            local.generation_id(), local.root.file_table, local.base_file_table,
            local.authority_head, local.authored_operation_id, local.live_operation_id,
            local.dependencies.proof().dependencies.clone(),
        ));
    }
    #[cfg(not(target_arch = "wasm32"))]
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    #[cfg(not(target_arch = "wasm32"))]
    let safe = if native {
        let result = poll_ready(local.rebase_head_with_native_work(8, &original, &cancellation))
            .ok_or("native disjoint rebase blocked")??;
        (result.0, Some(result.1))
    } else {
        (poll_ready(local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("disjoint rebase blocked")??.value, None)
    };
    #[cfg(target_arch = "wasm32")]
    let safe = (poll_ready(local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("disjoint rebase blocked")??.value, ());
    assert!(matches!(safe.0, RebaseDecision::Safe { .. }));
    let local_bytes = poll_ready(local.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 70,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("rebased read blocked")??;
    assert_eq!(&local_bytes.value.bytes[..5], b"LOCAL");
    assert_eq!(&local_bytes.value.bytes[64..70], b"REMOTE");

    let mut overlapping_local = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping local checkout blocked")??
    .value;
    let mut overlapping_remote = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping remote checkout blocked")??
    .value;
    poll_ready(overlapping_local.write_file(
        file.clone(),
        10,
        bytes::Bytes::from_static(b"local"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping local write blocked")??;
    poll_ready(overlapping_remote.write_file(
        file,
        12,
        bytes::Bytes::from_static(b"remote"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping remote write blocked")??;
    poll_ready(overlapping_remote.commit(
        OperationId::from_bytes([89; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping remote commit blocked")??;
    let unchanged = (
        overlapping_local.generation_id(), overlapping_local.root.file_table,
        overlapping_local.base_file_table, overlapping_local.authority_head,
        overlapping_local.authored_operation_id, overlapping_local.live_operation_id,
        overlapping_local.dependencies.proof().dependencies.clone(),
    );
    #[cfg(not(target_arch = "wasm32"))]
    let conflict = if native {
        let result = poll_ready(overlapping_local.rebase_head_with_native_work(8, &original, &cancellation))
            .ok_or("native overlapping rebase blocked")??;
        assert!(result.1.work().shares_ledger(&original));
        assert!(result.1.bytes() >= (std::mem::size_of::<crate::kernel::RebaseConflict>()) as u64);
        (result.0, Some(result.1))
    } else {
        (poll_ready(overlapping_local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("overlapping rebase blocked")??.value, None)
    };
    #[cfg(target_arch = "wasm32")]
    let conflict = (poll_ready(overlapping_local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("overlapping rebase blocked")??.value, ());
    assert_eq!(unchanged, (
        overlapping_local.generation_id(), overlapping_local.root.file_table,
        overlapping_local.base_file_table, overlapping_local.authority_head,
        overlapping_local.authored_operation_id, overlapping_local.live_operation_id,
        overlapping_local.dependencies.proof().dependencies.clone(),
    ));
    assert!(matches!(
        conflict.0,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::ContentRange { .. }
            ))
    ));
    Ok(())
}

#[test]
fn identity_writes_rebase_without_namespace_lookup_and_conflict_by_exact_range()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([111; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let file = path("identity")?;
    let mut seed = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed checkout blocked")??
    .value;
    let file_id = poll_ready(seed.create_file(
        file.clone(),
        bytes::Bytes::from(vec![b'a'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed create blocked")??
    .value;
    poll_ready(seed.commit(
        OperationId::from_bytes([112; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed commit blocked")??;

    let mut local = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("local checkout blocked")??
    .value;
    let mut remote = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote checkout blocked")??
    .value;
    poll_ready(local.write_file_by_id(
        file_id,
        0,
        bytes::Bytes::from_static(b"LOCAL"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity write blocked")??;
    poll_ready(remote.write_file(
        file.clone(),
        64,
        bytes::Bytes::from_static(b"REMOTE"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote write blocked")??;
    poll_ready(remote.commit(
        OperationId::from_bytes([113; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote commit blocked")??;
    let safe = poll_ready(local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("identity rebase blocked")??;
    assert!(matches!(safe.value, RebaseDecision::Safe { .. }));
    let bytes = poll_ready(local.read_file_range_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 70,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity read blocked")??;
    assert_eq!(&bytes.value.bytes[..5], b"LOCAL");
    assert_eq!(&bytes.value.bytes[64..70], b"REMOTE");
    let plan = poll_ready(local.plan_file_extents_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 70,
        },
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity extent plan blocked")??;
    assert!(plan.value.is_some());

    let mut overlapping_local = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping local checkout blocked")??
    .value;
    let mut overlapping_remote = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping remote checkout blocked")??
    .value;
    poll_ready(overlapping_local.write_file_by_id(
        file_id,
        10,
        bytes::Bytes::from_static(b"local"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping identity write blocked")??;
    poll_ready(overlapping_remote.write_file(
        file,
        12,
        bytes::Bytes::from_static(b"remote"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping remote write blocked")??;
    poll_ready(overlapping_remote.commit(
        OperationId::from_bytes([114; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlapping remote commit blocked")??;
    let conflict =
        poll_ready(overlapping_local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("overlapping identity rebase blocked")??;
    assert!(matches!(
        conflict.value,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::ContentRange { file_id: actual, .. } if actual == file_id
            ))
    ));

    let mut extending = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("extending identity checkout blocked")??
    .value;
    poll_ready(extending.write_file_by_id(
        file_id,
        128,
        bytes::Bytes::from_static(b"TAIL"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity append blocked")??;
    poll_ready(extending.clone_file_range_by_id(
        file_id,
        0,
        file_id,
        140,
        4,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity extending clone blocked")??;
    let extended = poll_ready(extending.read_file_range_by_id(
        file_id,
        ByteRange {
            offset: 128,
            length: 16,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity extended read blocked")??;
    assert_eq!(&extended.value.bytes[..4], b"TAIL");
    assert_eq!(&extended.value.bytes[4..12], &[0; 8]);
    assert_eq!(&extended.value.bytes[12..], b"aaaa");
    Ok(())
}

#[test]
fn inline_extent_plans_validate_ranges_and_capture_identity_dependencies()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([115; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let file = path("inline")?;
    let mut seed = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed checkout blocked")??
    .value;
    let file_id = poll_ready(seed.create_file(
        file.clone(),
        bytes::Bytes::from_static(b"abc"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed create blocked")??
    .value;
    poll_ready(seed.commit(
        OperationId::from_bytes([116; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed commit blocked")??;

    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("tracking checkout blocked")??
    .value;
    let plan = poll_ready(checkout.plan_file_extents_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 2,
        },
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inline identity plan blocked")??;
    assert!(plan.value.is_none());
    assert_eq!(checkout.dependencies.proof().len(), 1);

    let invalid = poll_ready(checkout.plan_file_extents_by_id(
        file_id,
        ByteRange {
            offset: 2,
            length: 2,
        },
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("invalid inline identity plan blocked")?
    .err()
    .ok_or("inline identity plan accepted a range beyond EOF")?;
    assert!(matches!(
        invalid.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(checkout.dependencies.proof().len(), 1);

    let path_plan = poll_ready(checkout.plan_file_extents(
        &file,
        ByteRange {
            offset: 1,
            length: 2,
        },
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inline path plan blocked")??;
    assert!(path_plan.value.is_none());
    let invalid_path = poll_ready(checkout.plan_file_extents(
        &file,
        ByteRange {
            offset: 3,
            length: 1,
        },
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("invalid inline path plan blocked")?
    .err()
    .ok_or("inline path plan accepted a range beyond EOF")?;
    assert!(matches!(
        invalid_path.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_guarded_live_retry_preserves_both_real_identities()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = fs.create_volume_with_id(
        VolumeId::from_bytes([209; 16]), serialized_config(),
        WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let mut seed = volume.checkout(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let left = seed.create_file(
        path("left")?, bytes::Bytes::from_static(b"left real source"),
        WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let right = seed.create_file(
        path("right")?, bytes::Bytes::from_static(b"distinct right real source"),
        WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    seed.commit(OperationId::from_bytes([210; 16]), WorkBudget::UNBOUNDED, &cancellation).await?;
    let original = crate::NativeMaterializationWork::new(
        WorkBudget::UNBOUNDED, &cancellation,
    ).map_err(|(cause, _)| cause)?;
    let mut local = volume.checkout_with_native_work(
        GenerationSelector::Head, writable_live(), &original, &cancellation,
    ).await?;
    let mut remote = volume.checkout(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let mut operations = crate::kernel::Owned::<Vec<Mutation>>::allocate(1, &mut accounting)?;
    accounting.charge(WorkCounters {
        bytes_copied: size_of::<Mutation>() as u64, ..WorkCounters::UNCHARGED
    })?;
    operations.value.push(Mutation::File {
        file_id: left, mutation: FileMutation::Resize { logical_bytes: 0 },
    });
    let KernelOwnedAllocation::NativeOriginal(allocation) = operations.allocation else {
        unreachable!("native input retains original custody")
    };
    local.mutate_with_native_work(
        operations.value, allocation, &original, &cancellation,
    ).await?;
    remote.mutate(vec![Mutation::File {
        file_id: right, mutation: FileMutation::Resize { logical_bytes: 0 },
    }], WorkBudget::UNBOUNDED, &cancellation).await?;
    remote.commit(
        OperationId::from_bytes([211; 16]), WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    let callbacks = Arc::new(AtomicUsize::new(0));
    let callback_calls = Arc::clone(&callbacks);
    let callback_original = original.clone();
    let mut before_publication = move |meter: &crate::NativeMaterializationWork| {
        assert!(meter.shares_ledger(&callback_original));
        callback_calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok::<(), FsError>(()))
    };
    let outcome = local.resume_live_guarded_with_native_work(
        OperationId::from_bytes([212; 16]), 8, 16, PublicationPermit::Unrestricted,
        &original, &cancellation, &mut before_publication, None, None, None, None,
    ).await?;
    assert!(matches!(outcome.0, LiveMutationOutcome::Committed { .. }));
    assert_eq!(outcome.1.bytes(), 0);
    assert!(outcome.1.work().shares_ledger(&original));
    assert!(callbacks.load(Ordering::SeqCst) >= 2);
    assert!(!local.has_pending_mutations());
    assert_eq!(local.live_operation_id, None);
    assert!(local.root.same_original_scope(original.native_read_meter()));
    let left_record = crate::kernel::lookup_file_record_async_with_accounting(
        &fs.inner.objects, local.root.file_table, left, DecodeLimits::for_volume(volume.config),
        &mut accounting, &cancellation, None,
    ).await?.0.ok_or("local identity disappeared during safe retry")?;
    let right_record = crate::kernel::lookup_file_record_async_with_accounting(
        &fs.inner.objects, local.root.file_table, right, DecodeLimits::for_volume(volume.config),
        &mut accounting, &cancellation, None,
    ).await?.0.ok_or("concurrent destination identity disappeared during safe retry")?;
    assert_eq!(left_record.file_id, left);
    assert_eq!(right_record.file_id, right);
    assert_eq!(left_record.payload, right_record.payload);
    Ok(())
}

#[test]
fn direct_live_mutations_retry_only_across_exactly_safe_regions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let optimistic = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([90; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("optimistic volume blocked")??
    .value;
    let rejected = poll_ready(optimistic.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("live rejection blocked")?
    .err()
    .ok_or("optimistic volume admitted direct live")?;
    assert!(matches!(
        rejected.error,
        FsError::LiveRequiresSerializedAuthority
    ));

    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([91; 16]),
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("serialized volume blocked")??
    .value;
    let file = path("file")?;
    let mut seed = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed checkout blocked")??
    .value;
    poll_ready(seed.create_file(
        file.clone(),
        bytes::Bytes::from(vec![b'a'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed create blocked")??;
    poll_ready(seed.commit(
        OperationId::from_bytes([92; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed commit blocked")??;

    let mut first = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first live checkout blocked")??
    .value;
    let mut second = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second live checkout blocked")??
    .value;
    let first_blob = poll_ready(first.stage_blob(
        bytes::Bytes::from_static(b"FIRST"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first blob blocked")??;
    let second_blob = poll_ready(second.stage_blob(
        bytes::Bytes::from_static(b"SECOND"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second blob blocked")??;
    let first_outcome = poll_ready(first.mutate_live(
        vec![Mutation::Write {
            path: file.clone(),
            offset: 0,
            length: first_blob.value.logical_bytes,
            content: first_blob.value.root,
            content_offset: 0,
        }],
        OperationId::from_bytes([93; 16]),
        2,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first live mutation blocked")??;
    assert!(matches!(
        first_outcome.value,
        LiveMutationOutcome::Committed { .. }
    ));
    let second_outcome = poll_ready(second.mutate_live(
        vec![Mutation::Write {
            path: file.clone(),
            offset: 64,
            length: second_blob.value.logical_bytes,
            content: second_blob.value.root,
            content_offset: 0,
        }],
        OperationId::from_bytes([94; 16]),
        2,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second live mutation blocked")??;
    assert!(matches!(
        second_outcome.value,
        LiveMutationOutcome::Committed { .. }
    ));

    let mut overlapping = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlap checkout blocked")??
    .value;
    let mut racing = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("racing checkout blocked")??
    .value;
    let overlap_blob = poll_ready(overlapping.stage_blob(
        bytes::Bytes::from_static(b"OVERLAP"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlap blob blocked")??;
    let racing_blob = poll_ready(racing.stage_blob(
        bytes::Bytes::from_static(b"RACING"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("racing blob blocked")??;
    poll_ready(racing.mutate_live(
        vec![Mutation::Write {
            path: file.clone(),
            offset: 10,
            length: racing_blob.value.logical_bytes,
            content: racing_blob.value.root,
            content_offset: 0,
        }],
        OperationId::from_bytes([95; 16]),
        2,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("racing mutation blocked")??;
    let conflict = poll_ready(overlapping.mutate_live(
        vec![Mutation::Write {
            path: file,
            offset: 12,
            length: overlap_blob.value.logical_bytes,
            content: overlap_blob.value.root,
            content_offset: 0,
        }],
        OperationId::from_bytes([96; 16]),
        2,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("overlap mutation blocked")??;
    assert!(matches!(
        conflict.value,
        LiveMutationOutcome::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::ContentRange { .. }
            ))
    ));
    Ok(())
}

#[test]
fn explicit_live_refresh_advances_only_across_unobserved_regions()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_ready($future).ok_or("filesystem future blocked")??
        };
    }

    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = ready!(fs.create_volume_with_id(
        VolumeId::from_bytes([97; 16]),
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let file = path("refresh")?;
    let mut seed = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(seed.create_file(
        file.clone(),
        bytes::Bytes::from(vec![b'a'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(seed.commit(
        OperationId::from_bytes([98; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));

    let mut live = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    assert_eq!(
        ready!(live.read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 4,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .bytes
        .as_ref(),
        b"aaaa"
    );

    let mut disjoint = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(disjoint.write_file(
        file.clone(),
        64,
        bytes::Bytes::from_static(b"SAFE"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let disjoint_generation = match ready!(disjoint.commit(
        OperationId::from_bytes([99; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected disjoint commit: {outcome:?}").into()),
    };
    assert_eq!(
        ready!(live.refresh_live(WorkBudget::UNBOUNDED, &cancellation)).value,
        disjoint_generation
    );
    assert_eq!(live.generation_id(), disjoint_generation);

    assert_eq!(
        ready!(live.read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 4,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .bytes
        .as_ref(),
        b"aaaa"
    );
    let mut overlapping = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(overlapping.write_file(
        file,
        0,
        bytes::Bytes::from_static(b"RACE"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(overlapping.commit(
        OperationId::from_bytes([100; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let before_conflict = live.generation_id();
    let conflict = poll_ready(live.refresh_live(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("conflicting live refresh blocked")?
        .err()
        .ok_or("conflicting live refresh unexpectedly advanced")?;
    assert!(matches!(
        conflict.error,
        FsError::LiveConflict { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::ContentRange { .. }
            ))
    ));
    assert_eq!(live.generation_id(), before_conflict);
    #[cfg(not(target_arch = "wasm32"))]
    {
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let native_conflict = poll_ready(live.synchronize_live_with_native_work(&original, &cancellation))
            .ok_or("native conflicting live sync blocked")?
            .err().ok_or("native conflicting live sync unexpectedly advanced")?;
        assert!(matches!(&native_conflict,
            FsError::NativeLiveConflict { conflicts, allocation, .. }
                if allocation.work().shares_ledger(&original)
                    && allocation.bytes() >= size_of::<RebaseConflict>() as u64
                    && conflicts.iter().any(|conflict| matches!(
                        conflict.region, DependencyRegion::ContentRange { .. }
                    ))
        ));
        assert_eq!(live.generation_id(), before_conflict);
    }
    Ok(())
}

#[test]
fn direct_live_resume_is_idempotent_retry_bounded_and_epoch_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_ready($future).ok_or("filesystem future blocked")??
        };
    }

    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([117; 16]);
    let volume = ready!(fs.create_volume_with_id(
        volume_id,
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;

    let mut winner = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut retrying = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(winner.create_file(
        path("winner")?,
        bytes::Bytes::from_static(b"winner"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(winner.resume_live(
        OperationId::from_bytes([118; 16]),
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));

    ready!(retrying.create_file(
        path("retrying")?,
        bytes::Bytes::from_static(b"retrying"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let retry_operation = OperationId::from_bytes([119; 16]);
    let limited =
        ready!(retrying.resume_live(retry_operation, 1, 8, WorkBudget::UNBOUNDED, &cancellation,));
    assert!(matches!(
        limited.value,
        LiveMutationOutcome::RetryLimit { .. }
    ));
    assert!(retrying.has_pending_mutations());
    let mutation_while_unresolved = poll_ready(retrying.create_file(
        path("must-remain-fenced")?,
        bytes::Bytes::from_static(b"blocked"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("unresolved live mutation guard blocked")?
    .err()
    .ok_or("unresolved live mutation accepted another mutation")?;
    assert!(matches!(
        mutation_while_unresolved.error,
        FsError::PendingLiveMutation
    ));
    let mismatched_resume = poll_ready(retrying.resume_live(
        OperationId::from_bytes([120; 16]),
        2,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("mismatched live resume blocked")?
    .err()
    .ok_or("mismatched live resume unexpectedly succeeded")?;
    assert!(matches!(
        mismatched_resume.error,
        FsError::PendingLiveMutation
    ));
    let rebase_while_unresolved =
        poll_ready(retrying.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("unresolved live rebase guard blocked")?
            .err()
            .ok_or("unresolved live operation accepted public rebase")?;
    assert!(matches!(
        rebase_while_unresolved.error,
        FsError::PendingLiveMutation
    ));
    let resumed =
        ready!(retrying.resume_live(retry_operation, 2, 8, WorkBudget::UNBOUNDED, &cancellation,));
    assert!(matches!(
        resumed.value,
        LiveMutationOutcome::Committed { .. }
    ));
    assert!(!retrying.has_pending_mutations());
    let clean_resume = poll_ready(retrying.resume_live(
        retry_operation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("clean live resume blocked")?
    .err()
    .ok_or("clean live resume unexpectedly succeeded")?;
    assert!(matches!(clean_resume.error, FsError::NoPendingMutations));
    assert_eq!(*clean_resume.work, WorkCounters::default());
    for failure in [
        poll_ready(retrying.resume_live(
            retry_operation,
            0,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("zero-limit live resume blocked")?
        .err()
        .ok_or("zero-limit live resume unexpectedly succeeded")?,
        poll_ready(retrying.mutate_live(
            Vec::new(),
            OperationId::from_bytes([124; 16]),
            0,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("zero-limit live mutation blocked")?
        .err()
        .ok_or("zero-limit live mutation unexpectedly succeeded")?,
    ] {
        assert!(matches!(failure.error, FsError::ZeroLiveRetryLimit));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let mut pending_guard = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(pending_guard.create_file(
        path("pending-guard")?,
        bytes::Bytes::from_static(b"pending"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let pending_failure = poll_ready(pending_guard.mutate_live(
        Vec::new(),
        OperationId::from_bytes([125; 16]),
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("pending live mutation guard blocked")?
    .err()
    .ok_or("second pending live mutation unexpectedly succeeded")?;
    assert!(matches!(
        pending_failure.error,
        FsError::PendingLiveMutation
    ));
    assert_eq!(*pending_failure.work, WorkCounters::default());
    let authored_pending = poll_ready(pending_guard.apply_authored_live(
        Vec::new(),
        OperationId::from_bytes([126; 16]),
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("pending authored-live guard blocked")?
    .err()
    .ok_or("pending authored-live operation unexpectedly succeeded")?;
    assert!(matches!(
        authored_pending.error,
        FsError::PendingLiveMutation
    ));
    assert_eq!(*authored_pending.work, WorkCounters::default());

    let mut first_retry = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut exact_retry = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let root = root_path()?;
    let metadata =
        ready!(first_retry.lookup_no_follow(&root, WorkBudget::UNBOUNDED, &cancellation,))
            .value
            .record
            .ok_or("root record absent")?
            .metadata;
    let identical = Mutation::Create {
        path: path("idempotent")?,
        record: FileRecord {
            file_id: FileId::from_bytes([120; 16]),
            kind: FileKind::Regular,
            link_count: 1,
            metadata,
            payload: FilePayload::InlineRegular(InlineFileData::new(b"same")?),
        },
    };
    ready!(first_retry.mutate(
        vec![identical.clone()],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(exact_retry.mutate(vec![identical], WorkBudget::UNBOUNDED, &cancellation,));
    let idempotent_operation = OperationId::from_bytes([121; 16]);
    let committed = ready!(first_retry.resume_live(
        idempotent_operation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        committed.value,
        LiveMutationOutcome::Committed { .. }
    ));
    let repeated = ready!(exact_retry.resume_live(
        idempotent_operation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        repeated.value,
        LiveMutationOutcome::AlreadyCommitted { .. }
    ));

    let mut conflicting_retry = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(conflicting_retry.create_file(
        path("different-operation")?,
        bytes::Bytes::from_static(b"different"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let conflicted = ready!(conflicting_retry.resume_live(
        idempotent_operation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        conflicted.value,
        LiveMutationOutcome::IdempotencyConflict { .. }
    ));
    assert!(conflicting_retry.has_pending_mutations());

    let mut fenced = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(fenced.create_file(
        path("fenced")?,
        bytes::Bytes::from_static(b"fenced"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let fence_expected = ready!(AsyncAuthorityStore::head(
        &fs.inner.authority,
        volume_authority_id(volume_id),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let fence = ready!(AsyncAuthorityStore::fence(
        &fs.inner.authority,
        volume_authority_id(volume_id),
        fence_expected,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let FenceOutcome::Advanced(fenced_head) = fence.value else {
        return Err("fresh facade fence conflicted".into());
    };
    let fenced_outcome = ready!(fenced.resume_live(
        OperationId::from_bytes([122; 16]),
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        fenced_outcome.value,
        LiveMutationOutcome::Fenced { actual_epoch } if actual_epoch == fenced_head.epoch
    ));

    let mut non_live = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    assert!(
        poll_ready(non_live.refresh_live(WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("non-live refresh blocked")?
            .is_err()
    );
    assert!(
        poll_ready(non_live.mutate_live(
            Vec::new(),
            OperationId::from_bytes([123; 16]),
            1,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("non-live mutation blocked")?
        .is_err()
    );
    Ok(())
}

#[test]
fn high_level_operations_stage_one_explicit_direct_live_transaction()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([111; 16]),
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("serialized volume blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("live checkout blocked")??
    .value;
    let file = path("live.txt")?;
    poll_ready(checkout.create_file(
        file.clone(),
        bytes::Bytes::from_static(b"initial"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("high-level live create blocked")??;
    poll_ready(checkout.write_file(
        file.clone(),
        7,
        bytes::Bytes::from_static(b"-published"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("high-level live write blocked")??;
    assert!(checkout.has_pending_mutations());

    let published = poll_ready(checkout.resume_live(
        OperationId::from_bytes([112; 16]),
        3,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("live publication blocked")??;
    assert!(matches!(
        published.value,
        LiveMutationOutcome::Committed { .. }
    ));
    assert!(!checkout.has_pending_mutations());
    let bytes = poll_ready(checkout.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 17,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("published read blocked")??;
    assert_eq!(bytes.value.bytes.as_ref(), b"initial-published");
    Ok(())
}

#[test]
fn authored_live_transaction_preserves_create_ids_and_publishes_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([117; 16]),
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("serialized volume blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("live checkout blocked")??
    .value;
    let file = path("authored-live.txt")?;
    let receipt = poll_ready(checkout.apply_authored_live(
        vec![
            AuthoredMutation::CreateFile {
                path: file.clone(),
                bytes: bytes::Bytes::from_static(b"initial"),
                metadata: empty_metadata(),
            },
            AuthoredMutation::Write {
                path: file.clone(),
                offset: 7,
                bytes: bytes::Bytes::from_static(b"-published"),
            },
        ],
        OperationId::from_bytes([118; 16]),
        3,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("authored live transaction blocked")??;
    assert!(matches!(
        receipt.value.outcome,
        LiveMutationOutcome::Committed { .. }
    ));
    assert_eq!(receipt.value.created_file_ids.len(), 2);
    assert!(receipt.value.created_file_ids[0].is_some());
    assert!(receipt.value.created_file_ids[1].is_none());
    assert!(!checkout.has_pending_mutations());
    let read = poll_ready(checkout.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 17,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("published authored read blocked")??;
    assert_eq!(read.value.bytes.as_ref(), b"initial-published");
    Ok(())
}

#[test]
fn authored_transaction_is_atomic_and_preserves_result_positions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([113; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let directory = path("workspace")?;
    let file = path("file.txt")?;
    let transaction = poll_ready(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateDirectory {
                path: directory,
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateFile {
                path: file.clone(),
                bytes: bytes::Bytes::from_static(b"initial"),
                metadata: empty_metadata(),
            },
            AuthoredMutation::Write {
                path: file.clone(),
                offset: 7,
                bytes: OwnedBytes::from_static(b"-atomic"),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("authored transaction blocked")??;
    assert_eq!(transaction.value.created_file_ids.len(), 3);
    assert!(transaction.value.created_file_ids[0].is_some());
    assert!(transaction.value.created_file_ids[1].is_some());
    assert!(transaction.value.created_file_ids[2].is_none());
    let read = poll_ready(checkout.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 14,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("transaction read blocked")??;
    assert_eq!(read.value.bytes.as_ref(), b"initial-atomic");

    let failed = poll_ready(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateFile {
                path: path("orphan")?,
                bytes: OwnedBytes::from_static(b"never-visible"),
                metadata: empty_metadata(),
            },
            AuthoredMutation::Remove {
                path: path("absent")?,
                expected_file_id: None,
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("failing transaction blocked")?;
    assert!(failed.is_err());
    let absent = poll_ready(checkout.lookup_no_follow(
        &path("orphan")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("atomicity lookup blocked")??;
    assert!(absent.value.record.is_none());
    Ok(())
}

#[test]
fn authored_bulk_transaction_is_bounded_and_atomic() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_mutations_per_batch = 2;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([244; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let created = (0..5)
        .map(|index| {
            Ok(AuthoredMutation::CreateFile { path: path(&format!("bulk{index}"))?, bytes: OwnedBytes::from_static(b"ok"), metadata: empty_metadata() })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    poll_ready(checkout.apply_authored_bulk_transaction(
        created,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("bulk transaction blocked")??;
    for index in 0..5 {
        assert!(
            poll_ready(checkout.lookup_no_follow(
                &path(&format!("bulk{index}"))?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .ok_or("bulk lookup blocked")??
            .value
            .record
            .is_some()
        );
    }
    let failed = poll_ready(checkout.apply_authored_bulk_transaction(
        vec![
            AuthoredMutation::CreateFile {
                path: path("should-not-appear")?,
                bytes: OwnedBytes::from_static(b"no"),
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateFile {
                path: path("should-not-appear-either")?,
                bytes: OwnedBytes::from_static(b"no"),
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateFile {
                path: path("bulk0")?,
                bytes: bytes::Bytes::from_static(b"duplicate"),
                metadata: empty_metadata(),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("failing bulk transaction blocked")?;
    assert!(failed.is_err());
    assert!(
        poll_ready(checkout.lookup_no_follow(
            &path("should-not-appear")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("failed bulk lookup blocked")??
        .value
        .record
        .is_none()
    );
    Ok(())
}

#[test]
fn encoded_metadata_denials_preserve_the_actual_producer_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let encoded = crate::kernel::encode_file_metadata_measured(
        empty_metadata(), WorkBudget::UNBOUNDED, &cancellation,
    )?;
    let producer_work = encoded.work;
    assert_eq!(producer_work.allocation_operations, 1);
    assert_eq!(producer_work.bytes_encoded, encoded.value.len() as u64);
    let failure = poll_ready(fs.put_encoded(
        ObjectKind::Metadata, encoded, WorkCounters::UNCHARGED,
        WorkBudget { bytes_hashed: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
    )).ok_or("bounded metadata put blocked")?
        .err().ok_or("metadata hashing unexpectedly admitted")?;
    assert!(matches!(failure.error, FsError::Work(_)));
    assert_eq!(*failure.work, producer_work);

    let mut encoded = crate::kernel::encode_file_metadata_measured(
        empty_metadata(), WorkBudget::UNBOUNDED, &cancellation,
    )?;
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    allocations.claim_bytes(
        encoded.value.capacity() as u64, 0, &mut encoded.work, WorkBudget::UNBOUNDED,
    ).map_err(crate::collection::handle_allocation_error)?;
    let spare_capacity = encoded.value.capacity() + 1;
    admit_pending_vec(
        &mut encoded.value, spare_capacity, &mut allocations, &mut encoded.work,
        WorkBudget::UNBOUNDED, &cancellation,
    )?;
    assert!(encoded.value.capacity() > encoded.value.len());
    let producer_work = encoded.work;
    let failure = poll_ready(fs.put_encoded(
        ObjectKind::Metadata, encoded, WorkCounters::UNCHARGED,
        WorkBudget {
            allocation_operations: producer_work.allocation_operations,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )).ok_or("bounded bytes owner allocation blocked")?
        .err().ok_or("bytes owner allocation unexpectedly admitted")?;
    assert!(matches!(failure.error, FsError::Work(_)));
    assert_eq!(*failure.work, producer_work);

    let encoded = crate::kernel::encode_file_metadata_measured(
        empty_metadata(), WorkBudget::UNBOUNDED, &cancellation,
    )?;
    let producer_work = encoded.work;
    cancellation.cancel();
    let failure = poll_ready(fs.put_encoded(
        ObjectKind::Metadata, encoded, WorkCounters::UNCHARGED,
        WorkBudget::UNBOUNDED, &cancellation,
    )).ok_or("cancelled metadata put blocked")?
        .err().ok_or("cancelled metadata put unexpectedly succeeded")?;
    assert!(matches!(failure.error, FsError::Cancelled(_)));
    assert_eq!(*failure.work, producer_work);
    Ok(())
}

#[test]
fn authored_buffer_failures_retain_admitted_capacity_and_leave_checkout_unchanged()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([243; 16]), config(), WorkBudget::UNBOUNDED, &cancellation,
    )).ok_or("volume creation blocked")??.value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    )).ok_or("checkout blocked")??.value;
    let original_root = checkout.root.clone();
    let mutation = || -> Result<AuthoredMutation, Box<dyn std::error::Error>> {
        Ok(AuthoredMutation::CreateFile { path: path("not-created")?, bytes: OwnedBytes::from_static(b"payload"), metadata: empty_metadata() })
    };
    let one_allocation = WorkBudget {
        allocation_operations: 1,
        ..WorkBudget::UNBOUNDED
    };
    let failed = poll_ready(checkout.apply_authored_transaction(
        vec![mutation()?], one_allocation, &cancellation,
    )).ok_or("bounded transaction blocked")?
        .err().ok_or("second buffer allocation unexpectedly admitted")?;
    assert!(matches!(failed.error, FsError::Work(_)));
    assert_eq!(failed.work.allocation_operations, 1);
    assert!(failed.work.peak_allocation_bytes >= size_of::<Mutation>() as u64);
    assert_eq!(failed.work.bytes_copied, 0);
    assert!(*checkout.root == original_root);

    // A real released candidate provides a vacant registration slot, so the
    // next candidate does not need to copy a growing global root table.
    let spare = checkout.private_candidate_measured(WorkBudget::UNBOUNDED, &cancellation)?;
    drop(spare);
    let candidate_copy_only = WorkBudget {
        bytes_copied: size_of::<Proof>() as u64,
        ..WorkBudget::UNBOUNDED
    };
    let failed = poll_ready(checkout.apply_authored_bulk_transaction(
        vec![mutation()?], candidate_copy_only, &cancellation,
    )).ok_or("bounded bulk transaction blocked")?
        .err().ok_or("authored batch copy unexpectedly admitted")?;
    assert!(matches!(failed.error, FsError::Work(_)));
    assert!(failed.work.allocation_operations >= 2);
    assert_eq!(failed.work.bytes_copied, size_of::<Proof>() as u64);
    assert!(*checkout.root == original_root);
    assert!(poll_ready(checkout.lookup_no_follow(
        &path("not-created")?, WorkBudget::UNBOUNDED, &cancellation,
    )).ok_or("unchanged checkout lookup blocked")??.value.record.is_none());
    Ok(())
}

#[test]
fn authored_transaction_reuses_a_staged_file_metadata_object()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([119; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let file = path("memo.txt")?;
    let metadata = empty_metadata();
    let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
        WorkBudget::UNBOUNDED, &cancellation,
    );
    let mut operations = crate::kernel::Owned::<Vec<Mutation>>::allocate(4, &mut accounting)?;
    let mut staged_metadata = AuthoredMetadataCache::new();
    let mut writes_after_create = 0;
    for (index, authored) in [
        AuthoredMutation::CreateFile { path: file.clone(), bytes: bytes::Bytes::new(), metadata, },
        AuthoredMutation::CreateFile { path: path("other.txt")?, bytes: bytes::Bytes::new(), metadata, },
        AuthoredMutation::SetMetadata { path: file.clone(), metadata },
    ].into_iter().enumerate() {
        let bytes = authored.logical_owned_bytes();
        let allocation = accounting.claim_owned(bytes, 0)?;
        poll_ready(checkout.compile_authored_mutation_with_accounting(
            crate::kernel::Owned::new(authored, allocation, bytes), &mut operations,
            &mut staged_metadata, &mut accounting, &cancellation,
        )).ok_or("authored compilation blocked")??;
        if index == 0 {
            writes_after_create = accounting.spent().backend_write_operations;
            assert!(writes_after_create > 0);
        } else {
            assert_eq!(accounting.spent().backend_write_operations, writes_after_create);
        }
    }
    let created_metadata = match &operations.value[0] {
        Mutation::Create { record, .. } => record.metadata,
        _ => return Err("expected create".into()),
    };
    assert!(matches!(
        &operations.value[2],
        Mutation::SetMetadata { metadata, .. } if *metadata == created_metadata
    ));

    let mut changed = metadata;
    changed.modified_ns = MetadataField::Value(42);
    let authored = AuthoredMutation::SetMetadata { path: file, metadata: changed };
    let bytes = authored.logical_owned_bytes();
    let allocation = accounting.claim_owned(bytes, 0)?;
    poll_ready(checkout.compile_authored_mutation_with_accounting(
        crate::kernel::Owned::new(authored, allocation, bytes), &mut operations,
        &mut staged_metadata, &mut accounting, &cancellation,
    )).ok_or("changed metadata compilation blocked")??;
    assert_eq!(accounting.spent().backend_write_operations, writes_after_create + 1);
    operations.retire(&mut accounting)?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn original_authored_creates(
    requests: &[(&NamespacePath, OwnedBytes)],
    metadata: FileMetadata,
    original: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<(Vec<AuthoredMutation>, crate::NativeAllocation), FsError> {
    use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
    let mut accounting = KernelAccounting::native(original, cancellation);
    let mut input = crate::kernel::Owned::<Vec<AuthoredMutation>>::allocate(requests.len(), &mut accounting)?;
    for (requested, content) in requests {
        let mut path = clone_authored_path(requested, &mut accounting)?;
        accounting.charge(WorkCounters {
            bytes_copied: size_of::<AuthoredMutation>() as u64, ..WorkCounters::UNCHARGED
        })?;
        let bytes = input.bytes.checked_add(path.bytes).ok_or(WorkError::Overflow)?;
        absorb_authored_allocation(&mut input.allocation, &mut path.allocation)?;
        input.value.push(AuthoredMutation::CreateFile { path: path.value, bytes: content.clone().into(), metadata, });
        input.bytes = bytes;
    }
    let KernelOwnedAllocation::NativeOriginal(allocation) = input.allocation else {
        unreachable!("original producer keeps its original guard")
    };
    Ok((input.value, allocation))
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_authored_compiler_admits_metadata_and_inline_copy_before_effects()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-authored-compiler").await?;
    workspace.write_text("/source", "authenticated compiler source").await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let metadata = checkout.lookup_no_follow_with_metadata(
        &path("source")?, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.ok_or("authenticated source metadata absent")?.metadata;
    checkout.bind_authored_operation(OperationId::new());
    let requested = path("compiler-copy")?;
    let body = OwnedBytes::from_static(b"authenticated compiler source");
    let expected = checkout.authored_file_id(FileKind::Regular, &requested);
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let mut operations = crate::kernel::Owned::<Vec<Mutation>>::allocate(1, &mut accounting)?;
    let paid_path = clone_authored_path(&requested, &mut accounting)?;
    let authored = crate::kernel::Owned::new(
        AuthoredMutation::CreateFile { path: paid_path.value, bytes: body.clone(), metadata },
        paid_path.allocation, paid_path.bytes,
    );
    let mut cache = AuthoredMetadataCache::new();
    let created = checkout.compile_authored_mutation_with_accounting(
        authored, &mut operations, &mut cache, &mut accounting, &cancellation,
    ).await?;
    assert_eq!(created, Some(expected));
    let Mutation::Create { record, path: actual_path } = &operations.value[0] else {
        return Err("inline source did not produce an actual create".into());
    };
    assert_eq!(actual_path, &requested);
    assert_eq!(record.file_id, expected);
    assert_eq!(record.payload, FilePayload::InlineRegular(InlineFileData::new(&body)?));
    assert!(original.receipt().backend_write_operations > 0);
    assert_eq!(original.receipt().source_bytes_read, 0);
    let KernelOwnedAllocation::NativeOriginal(guard) = &operations.allocation else {
        return Err("compiler output lost original custody".into());
    };
    assert!(guard.work().shares_ledger(&original));
    assert_eq!(guard.bytes(), operations.bytes);
    operations.retire(&mut accounting)?;
    assert_eq!(original.live_allocation_bytes(), baseline);

    // Cached real metadata removes staging from the copy-denial control.
    let metadata_object = cache.get(metadata, &mut accounting)?.ok_or("real metadata was not cached")?;
    for deny_metadata in [true, false] {
        let budget = if deny_metadata {
            WorkBudget { bytes_encoded: 0, ..WorkBudget::UNBOUNDED }
        } else {
            WorkBudget { bytes_copied: requested.components().iter()
                .map(|name| name.as_bytes().len() as u64).sum(), ..WorkBudget::UNBOUNDED }
        };
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let mut operations = crate::kernel::Owned::<Vec<Mutation>>::allocate(1, &mut accounting)?;
        let paid_path = clone_authored_path(&requested, &mut accounting)?;
        let authored = crate::kernel::Owned::new(
            AuthoredMutation::CreateFile { path: paid_path.value, bytes: body.clone(), metadata },
            paid_path.allocation, paid_path.bytes,
        );
        let mut cache = AuthoredMetadataCache::new();
        if !deny_metadata { cache.remember(metadata, metadata_object); }
        let failure = checkout.compile_authored_mutation_with_accounting(
            authored, &mut operations, &mut cache, &mut accounting, &cancellation,
        ).await.err().ok_or("finite compiler denial unexpectedly succeeded")?;
        assert!(matches!(failure, FsError::Work(_) | FsError::NativeKernel(_)));
        assert!(operations.value.is_empty());
        assert_eq!(original.receipt().backend_write_operations, 0);
        assert_eq!(original.receipt().source_bytes_read, 0);
        assert_eq!(original.receipt().bytes_encoded, 0);
        operations.retire(&mut accounting)?;
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn borrowed_compiler_source_body(
    body: &[u8],
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<OwnedBytes, FsError> {
    let mut data = crate::kernel::Owned::<Vec<u8>>::allocate(body.len(), accounting)?;
    accounting.charge(WorkCounters {
        bytes_copied: body.len() as u64, ..WorkCounters::UNCHARGED
    })?;
    data.value.extend_from_slice(body);
    let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(custody) = &data.allocation else {
        data.retire(accounting)?;
        return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
    };
    let original = custody.work().clone();
    OwnedBytes::from_vec_with_guard(data.value, data.allocation, original.runtime_meter_owner())
        .map_err(|failure| match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => original.runtime_error(error).into(),
            acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
            | acyclic_stream::PreparedOwnedBytesError::NotUnique => {
                crate::NativeTreeMaterializationError::InvalidLayout.into()
            }
        })
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn borrowed_authored_compiler_preserves_all_descriptors_and_external_source_custody()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    for profile in [FilesystemProfile::Posix, FilesystemProfile::Windows] {
        let mut volume_config = config();
        volume_config.profile = profile;
        let volume = fs.create_volume(
            volume_config, WorkBudget::UNBOUNDED, &cancellation,
        ).await?.value;
        let mut checkout = volume.checkout(
            GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
        ).await?.value;
        let metadata = empty_metadata();
        checkout.apply_authored_transaction(
            vec![AuthoredMutation::CreateFile {
                path: path("source")?, bytes: OwnedBytes::from_static(b"existing source"), metadata,
            }], WorkBudget::UNBOUNDED, &cancellation,
        ).await?;
        let source_id = checkout.lookup_no_follow(
            &path("source")?, WorkBudget::UNBOUNDED, &cancellation,
        ).await?.value.record.ok_or("compiler source missing")?.file_id;
        checkout.bind_authored_operation(OperationId::new());
        let mut cursor = std::io::Cursor::new(b"authenticated staged source");
        let staged = checkout.stage_content(
            &mut cursor, 64, WorkBudget::UNBOUNDED, &cancellation,
        ).await?.value;

        // These are real producer allocations on another ledger. Neither source
        // paths nor the opaque OwnedBytes owner may be adopted by the SDK's original.
        let producer = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let producer_baseline = producer.live_allocation_bytes();
        let mut source_accounting = KernelAccounting::native(&producer, &cancellation);
        let body = borrowed_compiler_source_body(b"opaque\0source target", &mut source_accounting)?;
        let body_pointer = body.as_ptr();
        let empty_body = body.slice(..0);
        let mut requests = vec![
            AuthoredMutation::Reidentify { path: path("source")?, file_id: source_id },
            AuthoredMutation::CreateFile {
                path: path("new-file")?, bytes: body.clone(), metadata,
            },
            AuthoredMutation::CreateFileFromContent {
                path: path("streamed-file")?, content: staged, metadata, file_id: Some(source_id),
            },
            AuthoredMutation::CreateDirectory { path: path("directory")?, metadata },
            AuthoredMutation::CreateSymbolicLink {
                path: path("symlink")?, target: body.clone(), metadata,
            },
            AuthoredMutation::CreateEmptySpecial {
                path: path("boundary")?, kind: FileKind::MountBoundary, metadata,
            },
            AuthoredMutation::Remove { path: path("source")?, expected_file_id: Some(source_id) },
            AuthoredMutation::Rename {
                source: path("source")?, destination: path("renamed")?, replace: true,
            },
            AuthoredMutation::HardLink { source: path("source")?, destination: path("alias")? },
            AuthoredMutation::Write {
                path: path("source")?, offset: 7, bytes: body.clone(),
            },
            AuthoredMutation::WriteFromContent { path: path("source")?, offset: 11, content: staged },
            AuthoredMutation::SetMetadata { path: path("source")?, metadata },
            AuthoredMutation::Resize { path: path("source")?, logical_bytes: 91 },
            AuthoredMutation::ZeroRange {
                path: path("source")?, range: ByteRange { offset: 13, length: 17 },
                allocated: true, extend: true,
            },
            AuthoredMutation::Preallocate {
                path: path("source")?, range: ByteRange { offset: 19, length: 23 }, keep_size: true,
            },
            AuthoredMutation::CloneRange(FileCloneRequest {
                source: path("source")?, source_offset: 29,
                destination: path("destination")?, destination_offset: 31, length: 37,
            }),
        ];
        if profile == FilesystemProfile::Posix {
            requests.push(AuthoredMutation::CreateEmptySpecial {
                path: path("fifo")?, kind: FileKind::Fifo, metadata,
            });
            requests.push(AuthoredMutation::CreateEmptySpecial {
                path: path("socket")?, kind: FileKind::Socket, metadata,
            });
            for (name, kind) in [
                ("character-device", FileKind::CharacterDevice),
                ("block-device", FileKind::BlockDevice),
            ] {
                requests.push(AuthoredMutation::CreateDevice {
                    path: path(name)?, kind, major: 41, minor: 43, metadata,
                });
            }
        } else {
            requests.push(AuthoredMutation::CreateReparsePoint {
                path: path("reparse")?, payload: body.clone(), metadata,
            });
        }
        for request in requests {
            source_accounting.charge(WorkCounters {
                bytes_copied: size_of::<AuthoredMutation>() as u64, ..WorkCounters::UNCHARGED
            })?;
            let source = clone_borrowed_authored_descriptor(&request, &mut source_accounting)?;
            let source_live = producer.live_allocation_bytes();
            let source_receipt = producer.receipt();
            let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, _)| cause)?;
            let baseline = original.live_allocation_bytes();
            let mut accounting = KernelAccounting::native(&original, &cancellation);
            let mut operations = crate::kernel::Owned::<Vec<Mutation>>::allocate(2, &mut accounting)?;
            let mut cache = AuthoredMetadataCache::new();
            let created = checkout.compile_borrowed_authored_mutation_with_accounting(
                &source.value, &mut operations, &mut cache, &mut accounting, &cancellation,
            ).await?;
            assert_eq!(source.value, request);
            assert_eq!(producer.live_allocation_bytes(), source_live);
            assert_eq!(producer.receipt(), source_receipt);
            assert_eq!(body.as_ptr(), body_pointer);
            let KernelOwnedAllocation::NativeOriginal(source_guard) = &source.allocation else {
                return Err("source descriptor lost producer custody".into());
            };
            assert!(source_guard.work().shares_ledger(&producer));
            assert!(!source_guard.work().shares_ledger(&original));
            assert_eq!(source_guard.bytes(), source.bytes);
            let KernelOwnedAllocation::NativeOriginal(output_guard) = &operations.allocation else {
                return Err("borrowed compiler output lost original custody".into());
            };
            assert!(output_guard.work().shares_ledger(&original));
            assert_eq!(output_guard.bytes(), operations.bytes);

            let mut ordinary = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
            let mut expected = crate::kernel::Owned::<Vec<Mutation>>::allocate(2, &mut ordinary)?;
            let input = request.clone();
            let bytes = input.logical_owned_bytes();
            let allocation = ordinary.claim_owned(bytes, 0)?;
            let ordinary_created = checkout.compile_authored_mutation_with_accounting(
                crate::kernel::Owned::new(input, allocation, bytes),
                &mut expected, &mut AuthoredMetadataCache::new(), &mut ordinary, &cancellation,
            ).await?;
            assert_eq!(created, ordinary_created);
            assert_eq!(operations.value, expected.value);
            expected.retire(&mut ordinary)?;
            operations.retire(&mut accounting)?;
            assert_eq!(original.live_allocation_bytes(), baseline);
            // Destroy every descriptor reference before retiring its producer guard.
            drop(request);
            source.retire(&mut source_accounting)?;
        }
        drop(body);
        assert!(producer.live_allocation_bytes() > producer_baseline);
        drop(empty_body);
        assert_eq!(producer.live_allocation_bytes(), producer_baseline);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn borrowed_authored_compiler_denies_copy_metadata_and_partial_path_before_effects()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::allocation::KernelAccounting;
    let fs = Fs::memory();
    let workspace = fs.create_workspace("borrowed-compiler-denials").await?;
    let cancellation = CancellationToken::new();
    let checkout = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let producer = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let producer_baseline = producer.live_allocation_bytes();
    let mut source_accounting = KernelAccounting::native(&producer, &cancellation);
    let body = borrowed_compiler_source_body(b"paid source body", &mut source_accounting)?;
    let request = AuthoredMutation::CreateFile { path: path("not-staged")?, bytes: body.clone(), metadata: empty_metadata() };
    let source = clone_borrowed_authored_descriptor(&request, &mut source_accounting)?;
    for budget in [
        WorkBudget { bytes_copied: 0, ..WorkBudget::UNBOUNDED },
        WorkBudget { bytes_encoded: 0, ..WorkBudget::UNBOUNDED },
    ] {
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let baseline = original.live_allocation_bytes();
        let source_live = producer.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let mut operations = crate::kernel::Owned::<Vec<Mutation>>::allocate(1, &mut accounting)?;
        let failure = checkout.compile_borrowed_authored_mutation_with_accounting(
            &source.value, &mut operations, &mut AuthoredMetadataCache::new(),
            &mut accounting, &cancellation,
        ).await.err().ok_or("borrowed compiler denial unexpectedly succeeded")?;
        assert!(matches!(failure, FsError::Work(_) | FsError::NativeKernel(_)));
        assert!(operations.value.is_empty());
        assert_eq!(original.receipt().backend_write_operations, 0);
        assert_eq!(original.receipt().source_bytes_read, 0);
        assert_eq!(original.receipt().bytes_encoded, 0);
        operations.retire(&mut accounting)?;
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert_eq!(producer.live_allocation_bytes(), source_live);
    }
    let cancelled = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancelled)
        .map_err(|(cause, _)| cause)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancelled);
    let mut operations = crate::kernel::Owned::<Vec<Mutation>>::allocate(1, &mut accounting)?;
    let before = original.receipt();
    let source_live = producer.live_allocation_bytes();
    cancelled.cancel();
    assert!(checkout.compile_borrowed_authored_mutation_with_accounting(
        &source.value, &mut operations, &mut AuthoredMetadataCache::new(),
        &mut accounting, &cancelled,
    ).await.is_err());
    assert!(operations.value.is_empty());
    assert_eq!(original.receipt(), before);
    assert_eq!(producer.live_allocation_bytes(), source_live);
    operations.retire(&mut accounting)?;
    assert_eq!(original.live_allocation_bytes(), baseline);
    drop(request);
    source.retire(&mut source_accounting)?;

    let request = AuthoredMutation::HardLink {
        source: path("first-paid-path")?, destination: path("second-denied-path")?,
    };
    let source = clone_borrowed_authored_descriptor(&request, &mut source_accounting)?;
    let AuthoredMutation::HardLink { source: first, .. } = &source.value else { unreachable!() };
    let mut probe = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let paid_first = clone_authored_path(first, &mut probe)?;
    let first_copy_bytes = probe.spent().bytes_copied;
    paid_first.retire(&mut probe)?;
    let original = crate::NativeMaterializationWork::new(WorkBudget {
        bytes_copied: size_of::<AuthoredMutation>() as u64 + first_copy_bytes,
        ..WorkBudget::UNBOUNDED
    }, &cancellation).map_err(|(cause, _)| cause)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let mut operations = crate::kernel::Owned::<Vec<Mutation>>::allocate(1, &mut accounting)?;
    assert!(checkout.compile_borrowed_authored_mutation_with_accounting(
        &source.value, &mut operations, &mut AuthoredMetadataCache::new(),
        &mut accounting, &cancellation,
    ).await.is_err());
    assert!(operations.value.is_empty());
    assert_eq!(original.receipt().bytes_copied,
        size_of::<AuthoredMutation>() as u64 + first_copy_bytes);
    assert_eq!(original.receipt().backend_write_operations, 0);
    assert_eq!(original.live_allocation_bytes(), baseline + operations.bytes);
    operations.retire(&mut accounting)?;
    assert_eq!(original.live_allocation_bytes(), baseline);
    drop(request);
    source.retire(&mut source_accounting)?;
    drop(body);
    assert_eq!(producer.live_allocation_bytes(), producer_baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_authored_transaction_keeps_true_ids_and_original_result_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-authored-transaction").await?;
    workspace.write_text("/source", "transaction source").await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let metadata = checkout.lookup_no_follow_with_metadata(
        &path("source")?, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.ok_or("real source metadata absent")?.metadata;
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let mut candidate = checkout.private_candidate_with_native_work(&original, &cancellation)?;
    candidate.bind_authored_operation(OperationId::new());
    let first = path("transaction-first")?;
    let second = path("transaction-second")?;
    let expected = [
        Some(candidate.authored_file_id(FileKind::Regular, &first)),
        Some(candidate.authored_file_id(FileKind::Regular, &second)),
    ];
    let input = original_authored_creates(
        &[(&first, OwnedBytes::from_static(b"first actual source")),
          (&second, OwnedBytes::from_static(b"second actual source"))],
        metadata, &original, &cancellation,
    )?;
    let result = candidate.apply_authored_transaction_with_native_work(
        input.0, input.1, &original, &cancellation,
    ).await?;
    assert_eq!(result.0.created_file_ids, expected);
    assert!(result.1.work().shares_ledger(&original));
    assert_eq!(result.1.bytes(),
        (result.0.created_file_ids.capacity() * size_of::<Option<FileId>>()) as u64);
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    for (requested, expected) in [(&first, expected[0]), (&second, expected[1])] {
        let actual = crate::kernel::lookup_path_async_with_accounting(
            &fs.inner.objects, &candidate.root, requested, candidate.volume.config,
            &mut accounting, &cancellation, None,
        ).await?.record.ok_or("created real record absent")?;
        assert_eq!(Some(actual.file_id), expected);
    }
    let live = original.live_allocation_bytes();
    let result_bytes = result.1.bytes();
    drop(result);
    assert_eq!(original.live_allocation_bytes(), live - result_bytes);
    assert!(checkout.lookup_no_follow(
        &first, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record.is_none(), "private transaction cannot alter its source checkout");
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn original_borrowed_path_changes(
    source: &NamespacePath,
    destination: &NamespacePath,
    removed: &NamespacePath,
    metadata: FileMetadata,
    original: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<(Vec<AuthoredMutation>, crate::NativeAllocation, crate::NativeAllocation), FsError> {
    use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
    let mut accounting = KernelAccounting::native(original, cancellation);
    let input = crate::kernel::Owned::<Vec<AuthoredMutation>>::allocate(4, &mut accounting)?;
    let mut source_allocation = accounting.claim_owned(0, 0)?;
    let mut copy_path = |requested: &NamespacePath| -> Result<NamespacePath, FsError> {
        let mut path = clone_authored_path(requested, &mut accounting)?;
        absorb_authored_allocation(&mut source_allocation, &mut path.allocation)?;
        Ok(path.value)
    };
    let metadata_path = copy_path(source)?;
    let resize_path = copy_path(source)?;
    let rename_source = copy_path(source)?;
    let rename_destination = copy_path(destination)?;
    let remove_path = copy_path(removed)?;
    accounting.charge(WorkCounters {
        bytes_copied: (4 * size_of::<AuthoredMutation>()) as u64,
        ..WorkCounters::UNCHARGED
    })?;
    let crate::kernel::Owned { mut value, allocation, .. } = input;
    value.push(AuthoredMutation::SetMetadata { path: metadata_path, metadata });
    value.push(AuthoredMutation::Resize { path: resize_path, logical_bytes: 4 });
    value.push(AuthoredMutation::Rename {
        source: rename_source, destination: rename_destination, replace: true,
    });
    value.push(AuthoredMutation::Remove { path: remove_path, expected_file_id: None });
    let KernelOwnedAllocation::NativeOriginal(slots) = allocation else {
        unreachable!("native producer retains original slots")
    };
    let KernelOwnedAllocation::NativeOriginal(source) = source_allocation else {
        unreachable!("native producer retains original paths")
    };
    Ok((value, slots, source))
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn borrowed_authored_path_changes_match_owned_and_keep_source_for_finish()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("borrowed-authored-paths").await?;
    workspace.write_text("/source", "source body").await?;
    workspace.write_text("/removed", "removed body").await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let source = path("source")?;
    let destination = path("destination")?;
    let removed = path("removed")?;
    let mut metadata = checkout.lookup_no_follow_with_metadata(
        &source, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.ok_or("real source missing")?.metadata;
    metadata.modified_ns = MetadataField::Value(42);
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let mut native = checkout.private_candidate_with_native_work(&original, &cancellation)?;
    let mut ordinary = checkout.private_candidate();
    let operation = OperationId::new();
    native.bind_authored_operation(operation);
    ordinary.bind_authored_operation(operation);
    let input = original_borrowed_path_changes(
        &source, &destination, &removed, metadata, &original, &cancellation,
    )?;
    let slots = input.1.bytes();
    let source_bytes = input.2.bytes();
    assert_eq!(slots, (input.0.capacity() * size_of::<AuthoredMutation>()) as u64);
    let result = native.apply_borrowed_authored_transaction_with_native_work(
        &input.0, &input.1, &original, &cancellation,
    ).await?;
    let ordinary_result = ordinary.apply_authored_transaction(
        vec![
            AuthoredMutation::SetMetadata { path: source.clone(), metadata },
            AuthoredMutation::Resize { path: source.clone(), logical_bytes: 4 },
            AuthoredMutation::Rename {
                source: source.clone(), destination: destination.clone(), replace: true,
            },
            AuthoredMutation::Remove { path: removed.clone(), expected_file_id: None },
        ], WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    assert_eq!(result.0.created_file_ids, ordinary_result.created_file_ids);
    assert_eq!(result.0.created_file_ids, [None; 4]);
    assert_eq!(original.receipt().source_bytes_read, 0);
    assert_eq!(native.root.file_table, ordinary.root.file_table);
    assert!(result.1.work().shares_ledger(&original));
    assert_eq!(result.1.bytes(),
        (result.0.created_file_ids.capacity() * size_of::<Option<FileId>>()) as u64);
    assert_eq!(input.1.bytes(), slots);
    assert_eq!(input.2.bytes(), source_bytes);
    // Source's real finish phase can still inspect the original paths after await.
    assert!(matches!(&input.0[2], AuthoredMutation::Rename {
        source: old_source, destination: old_destination, ..
    } if old_source == &source && old_destination == &destination));
    let actual = native.lookup_no_follow_with_metadata_with_native_work(
        &destination, &original, &cancellation,
    ).await?.ok_or("renamed source absent")?;
    assert_eq!(actual.metadata, metadata);
    match actual.record.payload {
        FilePayload::InlineRegular(data) => assert_eq!(data.as_bytes(), b"sour"),
        FilePayload::Regular { logical_bytes, .. } => assert_eq!(logical_bytes, 4),
        _ => return Err("metadata/resize changed regular file kind".into()),
    }
    for requested in [&source, &removed] {
        assert!(native.lookup_no_follow(requested, WorkBudget::UNBOUNDED, &cancellation)
            .await?.value.record.is_none());
        assert!(checkout.lookup_no_follow(requested, WorkBudget::UNBOUNDED, &cancellation)
            .await?.value.record.is_some());
    }
    drop(result);
    drop(native);
    let live = original.live_allocation_bytes();
    drop(input);
    assert_eq!(original.live_allocation_bytes(), live - slots - source_bytes);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn borrowed_authored_slots_and_native_denial_precede_copies_and_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("borrowed-authored-slots").await?;
    workspace.write_text("/source", "source body").await?;
    workspace.write_text("/removed", "removed body").await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let source = path("source")?;
    let destination = path("destination")?;
    let removed = path("removed")?;
    let metadata = checkout.lookup_no_follow_with_metadata(
        &source, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.ok_or("real source missing")?.metadata;
    let mut producer_allocations = 0;
    for deny in [false, true] {
        let original = crate::NativeMaterializationWork::new(
            WorkBudget {
                allocation_operations: if deny { producer_allocations } else { u64::MAX },
                ..WorkBudget::UNBOUNDED
            }, &cancellation,
        ).map_err(|(cause, _)| cause)?;
        let mut candidate = checkout.private_candidate_with_native_work(&original, &cancellation)?;
        let input = original_borrowed_path_changes(
            &source, &destination, &removed, metadata, &original, &cancellation,
        )?;
        producer_allocations = original.receipt().allocation_operations;
        let short = original.claim(0)?;
        let foreign = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let foreign_slots = foreign.claim(input.1.bytes())?;
        let before = original.receipt();
        let live = original.live_allocation_bytes();
        let state = checkout_state(&candidate);
        for witness in [&short, &foreign_slots] {
            let failure = candidate.apply_borrowed_authored_transaction_with_native_work(
                &input.0, witness, &original, &cancellation,
            ).await.err().ok_or("invalid slots accepted")?;
            assert!(matches!(failure, FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout)));
            assert_eq!(original.receipt(), before);
            assert_eq!(checkout_state(&candidate), state);
        }
        let failure = candidate.apply_borrowed_authored_transaction_with_native_work(
            &input.0, &input.1, &foreign, &cancellation,
        ).await.err().ok_or("foreign original accepted")?;
        assert!(matches!(failure, FsError::NativeKernel(_)));
        assert_eq!(original.receipt(), before);
        if deny {
            let failure = candidate.apply_borrowed_authored_transaction_with_native_work(
                &input.0, &input.1, &original, &cancellation,
            ).await.err().ok_or("exhausted allocation budget accepted")?;
            assert!(matches!(failure, FsError::Work(_) | FsError::NativeKernel(_)));
            assert_eq!(original.receipt().bytes_copied, before.bytes_copied);
            assert_eq!(original.receipt().backend_write_operations, before.backend_write_operations);
            assert_eq!(original.live_allocation_bytes(), live);
            assert_eq!(checkout_state(&candidate), state);
            assert_eq!(input.0.len(), 4);
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn borrowed_authored_second_staged_write_failure_keeps_candidate_source_and_destination()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let control = Arc::new(FaultControl::disabled());
    let objects = Arc::new(crate::memory::MemoryObjectStore::default());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        PostPutObjectStore { inner: Arc::clone(&objects), control: Arc::clone(&control) },
        EmbeddedCapabilities::MEMORY,
    );
    let volume = fs.create_volume_with_id(
        VolumeId::from_bytes([122; 16]), config(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let mut checkout = volume.checkout(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let source = path("source")?;
    let destination = path("destination")?;
    let removed = path("removed")?;
    for (requested, body) in [
        (&source, b"source body".as_slice()),
        (&destination, b"destination body".as_slice()),
        (&removed, b"removed body".as_slice()),
    ] {
        checkout.create_file(requested.clone(), bytes::Bytes::copy_from_slice(body), WorkBudget::UNBOUNDED, &cancellation).await?;
    }
    let mut metadata = checkout.lookup_no_follow_with_metadata(
        &source, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.ok_or("real source missing")?.metadata;
    metadata.modified_ns = MetadataField::Value(99);
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let mut candidate = checkout.private_candidate_with_native_work(&original, &cancellation)?;
    let input = original_borrowed_path_changes(
        &source, &destination, &removed, metadata, &original, &cancellation,
    )?;
    let state = checkout_state(&candidate);
    let installed = checkout_state(&checkout);
    let live = original.live_allocation_bytes();
    control.arm(2);
    let failure = candidate.apply_borrowed_authored_transaction_with_native_work(
        &input.0, &input.1, &original, &cancellation,
    ).await.err().ok_or("second staged write unexpectedly succeeded")?;
    assert!(matches!(failure, FsError::Object(ObjectStoreError::Corrupt)));
    assert_eq!(control.operation.load(Ordering::Relaxed), 2);
    assert_eq!(original.receipt().backend_write_operations, 2);
    assert_eq!(checkout_state(&candidate), state);
    assert_eq!(checkout_state(&checkout), installed);
    assert_eq!(original.live_allocation_bytes(), live);
    assert!(matches!(&input.0[2], AuthoredMutation::Rename {
        source: old_source, destination: old_destination, ..
    } if old_source == &source && old_destination == &destination));
    control.disable();
    for requested in [&source, &destination, &removed] {
        let retained = candidate.lookup_no_follow(requested, WorkBudget::UNBOUNDED, &cancellation)
            .await?.value.record.ok_or("failed candidate lost existing record")?;
        let installed = checkout.lookup_no_follow(requested, WorkBudget::UNBOUNDED, &cancellation)
            .await?.value.record.ok_or("installed source record disappeared")?;
        assert_eq!(retained, installed);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_authored_bulk_source_denial_preserves_the_installed_root()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limits = config();
    limits.limits.maximum_mutations_per_batch = 2;
    let volume = fs.create_volume_with_id(
        VolumeId::from_bytes([121; 16]), limits, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let mut checkout = volume.checkout(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let root = NamespacePath::new(Vec::new(), limits.limits)?;
    let metadata = checkout.lookup_no_follow_with_metadata(
        &root, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.ok_or("authenticated root metadata absent")?.metadata;
    let original = crate::NativeMaterializationWork::new(
        WorkBudget { source_bytes_read: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).map_err(|(cause, _)| cause)?;
    let baseline = original.live_allocation_bytes();
    let first = path("bulk-first")?;
    let second = path("bulk-source-denied")?;
    let input = original_authored_creates(
        &[(&first, OwnedBytes::from_static(b"first staged batch")),
          (&second, OwnedBytes::from_static(&[b'b'; 1024]))],
        metadata, &original, &cancellation,
    )?;
    let before = (checkout.root.file_table, checkout.base_file_table,
        checkout.authored_operation_id, checkout.live_operation_id);
    let failure = checkout.apply_authored_bulk_transaction_with_native_work(
        input.0, input.1, &original, &cancellation,
    ).await.err().ok_or("denied actual bulk source installed a candidate")?;
    assert!(matches!(failure, FsError::Work(_) | FsError::NativeKernel(_)));
    assert!(original.receipt().backend_write_operations > 0, "immutable first-batch prefix was staged");
    assert_eq!(original.receipt().source_bytes_read, 0);
    assert_eq!(before, (checkout.root.file_table, checkout.base_file_table,
        checkout.authored_operation_id, checkout.live_operation_id));
    assert_eq!(original.live_allocation_bytes(), baseline);
    for requested in [&first, &second] {
        assert!(checkout.lookup_no_follow(
            requested, WorkBudget::UNBOUNDED, &cancellation,
        ).await?.value.record.is_none());
    }
    Ok(())
}

#[test]
fn public_authored_transaction_reuses_metadata_across_paths()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let first = path("first")?;
    let second = path("second")?;
    let shared = empty_metadata();
    let mut distinct = shared;
    distinct.modified_ns = MetadataField::Value(42);
    let mut writes = Vec::new();

    for second_metadata in [shared, distinct] {
        let fs = Fs::memory();
        let volume = poll_ready(fs.create_volume_with_id(
            VolumeId::from_bytes([120; 16]),
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("volume creation blocked")??
        .value;
        let mut checkout = poll_ready(volume.checkout(
            GenerationSelector::Head,
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("checkout blocked")??
        .value;
        let result = poll_ready(checkout.apply_authored_transaction(
            vec![
                AuthoredMutation::CreateFile {
                    path: first.clone(),
                    bytes: bytes::Bytes::new(),
                    metadata: shared,
                },
                AuthoredMutation::CreateFile {
                    path: second.clone(),
                    bytes: bytes::Bytes::new(),
                    metadata: second_metadata,
                },
                AuthoredMutation::SetMetadata {
                    path: first.clone(),
                    metadata: shared,
                },
            ],
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("authored transaction blocked")??;
        let first_record =
            poll_ready(checkout.lookup_no_follow(&first, WorkBudget::UNBOUNDED, &cancellation))
                .ok_or("first lookup blocked")??
                .value
                .record
                .ok_or("first record missing")?;
        let second_record =
            poll_ready(checkout.lookup_no_follow(&second, WorkBudget::UNBOUNDED, &cancellation))
                .ok_or("second lookup blocked")??
                .value
                .record
                .ok_or("second record missing")?;
        assert_eq!(
            first_record.metadata == second_record.metadata,
            second_metadata == shared
        );
        writes.push(result.work.backend_write_operations);
    }
    assert_eq!(writes[1], writes[0] + 1);
    Ok(())
}

#[test]
fn inverse_private_mutations_restore_the_base_and_clear_replay_state()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([136; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inverse volume creation blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inverse checkout blocked")??
    .value;
    let base = checkout.generation_id();
    let transient = path("transient")?;
    poll_ready(checkout.create_file(
        transient.clone(),
        bytes::Bytes::from_static(b"temporary"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inverse creation blocked")??;
    assert!(checkout.has_pending_mutations());
    poll_ready(checkout.remove(transient, None, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("inverse removal blocked")??;
    assert!(!checkout.has_pending_mutations());
    assert_eq!(checkout.generation_id(), base);
    Ok(())
}

#[test]
fn authored_transactions_preflight_expansion_noops_and_byte_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();

    let mut operation_limited = config();
    operation_limited.limits.maximum_mutations_per_batch = 1;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([124; 16]),
        operation_limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("operation-limited volume blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("operation-limited checkout blocked")??
    .value;
    let missing_noop = poll_ready(checkout.apply_authored_transaction(
        vec![AuthoredMutation::Write {
            path: path("absent")?,
            offset: 0,
            bytes: bytes::Bytes::new(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("authored missing noop blocked")?
    .err()
    .ok_or("authored missing noop succeeded")?;
    assert!(matches!(
        missing_noop.error,
        FsError::Mutation(GenerationMutationError::MissingSource)
    ));
    assert!(missing_noop.work.page_reads > 0);

    let noop_fs = Fs::memory();
    let noop_volume = poll_ready(noop_fs.create_volume_with_id(
        VolumeId::from_bytes([125; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("noop volume creation blocked")??
    .value;
    let mut noop_checkout = poll_ready(noop_volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("noop checkout blocked")??
    .value;
    let ordered = poll_ready(noop_checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::Write {
                path: path("ordered")?,
                offset: 0,
                bytes: bytes::Bytes::new(),
            },
            AuthoredMutation::CreateFile {
                path: path("ordered")?,
                bytes: bytes::Bytes::new(),
                metadata: empty_metadata(),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("ordered noop transaction blocked")?
    .err()
    .ok_or("ordered noop bypassed its missing source")?;
    assert!(matches!(
        ordered.error,
        FsError::Mutation(GenerationMutationError::MissingSource)
    ));
    assert!(
        poll_ready(noop_checkout.lookup_no_follow(
            &path("ordered")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("ordered path lookup blocked")??
        .value
        .record
        .is_none()
    );

    let direct_missing = poll_ready(noop_checkout.write_file(
        path("missing")?,
        0,
        bytes::Bytes::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("direct missing noop blocked")?
    .err()
    .ok_or("direct missing noop succeeded")?;
    assert!(matches!(
        direct_missing.error,
        FsError::Mutation(GenerationMutationError::MissingSource)
    ));

    let created = poll_ready(noop_checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateFile {
            path: path("present")?,
            bytes: bytes::Bytes::new(),
            metadata: empty_metadata(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("authored fixture creation blocked")??;
    let file_id = created.value.created_file_ids[0].ok_or("created file id missing")?;
    let generation_before_noop = noop_checkout.generation_id();
    let empty = poll_ready(noop_checkout.apply_authored_transaction(
        Vec::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("empty authored transaction blocked")??;
    assert!(empty.value.created_file_ids.is_empty());
    assert_eq!(empty.work, WorkCounters::default());
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let noop = poll_ready(noop_checkout.apply_authored_transaction(
        vec![AuthoredMutation::Write {
            path: path("present")?,
            offset: 0,
            bytes: bytes::Bytes::new(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("authored validated noop blocked")??;
    assert_eq!(noop.value.created_file_ids, vec![None]);
    assert!(noop.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let mut empty_source = std::io::Cursor::new(OwnedBytes::new());
    let empty_content = poll_ready(noop_checkout.stage_content(
        &mut empty_source,
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("empty content staging blocked")??
    .value;
    assert_eq!(empty_content.logical_bytes(), 0);
    let content_noop = poll_ready(noop_checkout.apply_authored_transaction(
        vec![AuthoredMutation::WriteFromContent {
            path: path("present")?,
            offset: u64::MAX,
            content: empty_content,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("empty staged-content write blocked")??;
    assert_eq!(content_noop.value.created_file_ids, vec![None]);
    assert!(content_noop.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let direct = poll_ready(noop_checkout.write_file(
        path("present")?,
        u64::MAX,
        bytes::Bytes::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("direct validated noop blocked")??;
    assert!(direct.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let identity = poll_ready(noop_checkout.write_file_by_id(
        file_id,
        u64::MAX,
        bytes::Bytes::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity validated noop blocked")??;
    assert!(identity.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);

    let expanded = poll_ready(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateFile {
            path: path("large")?,
            bytes: OwnedBytes::from(vec![1; crate::kernel::MAXIMUM_INLINE_FILE_BYTES + 1]),
            metadata: empty_metadata(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("expanded authored transaction blocked")?
    .err()
    .ok_or("expanded authored transaction unexpectedly succeeded")?;
    assert!(matches!(expanded.error, FsError::TooManyPendingMutations));
    assert_eq!(*expanded.work, WorkCounters::default());
    assert!(
        poll_ready(checkout.lookup_no_follow(
            &path("large")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("expanded path lookup blocked")??
        .value
        .record
        .is_none()
    );

    let mut byte_limited = config();
    byte_limited.limits.maximum_read_bytes = 1;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([125; 16]),
        byte_limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("byte-limited volume blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("byte-limited checkout blocked")??
    .value;
    for mutation in [
        AuthoredMutation::CreateFile { path: path("created-file")?, bytes: bytes::Bytes::from_static(b"xx"), metadata: empty_metadata() },
        AuthoredMutation::Write { path: path("file")?, offset: 0, bytes: OwnedBytes::from_static(b"xx") },
        AuthoredMutation::CreateSymbolicLink { path: path("link")?, target: bytes::Bytes::from_static(b"xx"), metadata: empty_metadata() },
    ] {
        let failure = poll_ready(checkout.apply_authored_transaction(
            vec![mutation],
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("byte-limited authored transaction blocked")?
        .err()
        .ok_or("byte-limited authored transaction unexpectedly succeeded")?;
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }
    let direct = poll_ready(checkout.create_symbolic_link(
        path("direct-link")?,
        bytes::Bytes::from_static(b"xx"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("byte-limited direct symlink blocked")?
    .err()
    .ok_or("byte-limited direct symlink unexpectedly succeeded")?;
    assert!(matches!(
        direct.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*direct.work, WorkCounters::default());
    Ok(())
}

#[test]
fn opaque_payload_reads_enforce_the_volume_output_bound() -> Result<(), Box<dyn std::error::Error>>
{
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.profile = FilesystemProfile::Windows;
    limited.limits.maximum_read_bytes = 1;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([126; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("limited Windows volume blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("limited Windows checkout blocked")??
    .value;
    let mut source = std::io::Cursor::new(OwnedBytes::from_static(b"xx"));
    let staged =
        poll_ready(checkout.stage_content(&mut source, 2, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("oversized opaque staging blocked")??
            .value;
    let metadata =
        poll_ready(checkout.stage_metadata(empty_metadata(), WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("opaque metadata staging blocked")??
            .value;
    let symbolic = path("symbolic")?;
    let reparse = path("reparse")?;
    poll_ready(checkout.mutate(
        vec![
            Mutation::Create {
                path: symbolic.clone(),
                record: FileRecord {
                    file_id: FileId::from_bytes([127; 16]),
                    kind: FileKind::SymbolicLink,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::SymbolicLink {
                        target_bytes: staged.logical_bytes(),
                        target: staged.root(),
                    },
                },
            },
            Mutation::Create {
                path: reparse.clone(),
                record: FileRecord {
                    file_id: FileId::from_bytes([128; 16]),
                    kind: FileKind::ReparsePoint,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::ReparsePoint {
                        payload_bytes: staged.logical_bytes(),
                        payload: staged.root(),
                    },
                },
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("opaque raw mutation blocked")??;
    for failure in [
        poll_ready(checkout.read_symbolic_link(&symbolic, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("bounded symlink read blocked")?
            .err()
            .ok_or("oversized symlink read unexpectedly succeeded")?,
        poll_ready(checkout.read_reparse_point(&reparse, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("bounded reparse read blocked")?
            .err()
            .ok_or("oversized reparse read unexpectedly succeeded")?,
    ] {
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert!(failure.work.object_probes > 0);
    }
    Ok(())
}

#[test]
fn authored_transactions_cover_every_portable_operation_without_hidden_paths()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([114; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;

    let mut create_source = std::io::Cursor::new(OwnedBytes::from_static(b"streamed"));
    let staged_create = poll_ready(checkout.stage_content(
        &mut create_source,
        64,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("create staging blocked")??
    .value;
    let mut write_source = std::io::Cursor::new(OwnedBytes::from_static(b"TAIL"));
    let staged_write = poll_ready(checkout.stage_content(
        &mut write_source,
        64,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("write staging blocked")??
    .value;
    assert_eq!(staged_create.logical_bytes(), 8);
    assert_eq!(staged_create.root().kind, ObjectKind::Blob);
    assert_eq!(staged_write.logical_bytes(), 4);
    assert_eq!(staged_write.root().kind, ObjectKind::Blob);

    let source = path("source")?;
    let destination = path("destination")?;
    let streamed = path("streamed")?;
    let streamed_identity = FileId::from_bytes([199; 16]);
    let hard_link = path("hard-link")?;
    let renamed = path("renamed")?;
    let mut replacement_metadata = empty_metadata();
    replacement_metadata.modified_ns = MetadataField::Value(42);
    let large = OwnedBytes::from(vec![0x5a; crate::kernel::MAXIMUM_INLINE_FILE_BYTES + 1]);
    let transaction = poll_ready(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateDirectory {
                path: path("directory")?,
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateFile {
                path: source.clone(),
                bytes: bytes::Bytes::from_static(b"abcdefgh"),
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateFile {
                path: destination.clone(),
                bytes: OwnedBytes::from_static(b"................"),
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateFile {
                path: path("large")?,
                bytes: large.clone(),
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateFileFromContent {
                path: streamed.clone(),
                content: staged_create,
                metadata: empty_metadata(),
                file_id: Some(streamed_identity),
            },
            AuthoredMutation::CreateSymbolicLink {
                path: path("symbolic")?,
                target: OwnedBytes::from_static(b"../source"),
                metadata: empty_metadata(),
            },
            AuthoredMutation::Write {
                path: source.clone(),
                offset: 0,
                bytes: bytes::Bytes::new(),
            },
            AuthoredMutation::Write {
                path: source.clone(),
                offset: 6,
                bytes: OwnedBytes::from_static(b"XY"),
            },
            AuthoredMutation::WriteFromContent {
                path: streamed.clone(),
                offset: 8,
                content: staged_write,
            },
            AuthoredMutation::SetMetadata {
                path: source.clone(),
                metadata: replacement_metadata,
            },
            AuthoredMutation::Resize {
                path: destination.clone(),
                logical_bytes: 24,
            },
            AuthoredMutation::ZeroRange {
                path: source.clone(),
                range: ByteRange {
                    offset: 2,
                    length: 2,
                },
                allocated: false,
                extend: false,
            },
            AuthoredMutation::Preallocate {
                path: destination.clone(),
                range: ByteRange {
                    offset: 16,
                    length: 8,
                },
                keep_size: false,
            },
            AuthoredMutation::CloneRange(FileCloneRequest {
                source: source.clone(),
                source_offset: 0,
                destination: destination.clone(),
                destination_offset: 4,
                length: 8,
            }),
            AuthoredMutation::HardLink {
                source: source.clone(),
                destination: hard_link.clone(),
            },
            AuthoredMutation::Rename {
                source: hard_link,
                destination: renamed.clone(),
                replace: false,
            },
            AuthoredMutation::Remove {
                path: renamed,
                expected_file_id: None,
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("complete authored transaction blocked")??;
    assert_eq!(transaction.value.created_file_ids.len(), 17);
    assert_eq!(
        transaction.value.created_file_ids.get(4),
        Some(&Some(streamed_identity))
    );
    assert_eq!(
        transaction
            .value
            .created_file_ids
            .iter()
            .filter(|created| created.is_some())
            .count(),
        6
    );
    assert_eq!(
        poll_ready(checkout.read_file_range(
            &streamed,
            ByteRange {
                offset: 0,
                length: 12,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("streamed read blocked")??
        .value
        .bytes
        .as_ref(),
        b"streamedTAIL"
    );
    assert!(
        poll_ready(checkout.apply_authored_transaction(
            vec![AuthoredMutation::CreateFileFromContent {
                path: path("duplicate-source-identity")?,
                content: staged_create,
                metadata: empty_metadata(),
                file_id: Some(streamed_identity),
            }],
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("duplicate identity transaction blocked")?
        .is_err()
    );
    assert_eq!(
        poll_ready(checkout.read_file_range(
            &path("large")?,
            ByteRange {
                offset: 0,
                length: u64::try_from(large.len())?,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("large read blocked")??
        .value
        .bytes.as_ref(),
        large.as_ref()
    );
    assert_eq!(
        poll_ready(checkout.read_symbolic_link(
            &path("symbolic")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("symbolic-link read blocked")??
        .value
        .as_ref(),
        b"../source"
    );
    assert_eq!(
        poll_ready(checkout.read_metadata(&source, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("metadata read blocked")??
            .value
            .modified_ns,
        MetadataField::Value(42)
    );
    Ok(())
}

#[test]
fn path_sdk_exposes_every_sparse_operation_with_one_authenticated_state()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([117; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    assert_eq!(volume.id(), VolumeId::from_bytes([117; 16]));
    assert_eq!(volume.config(), config());
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let source = path("source-path")?;
    let destination = path("destination-path")?;
    let source_id = poll_ready(checkout.create_file(
        source.clone(),
        bytes::Bytes::from(vec![b's'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("source creation blocked")??
    .value;
    poll_ready(checkout.create_file(
        destination.clone(),
        bytes::Bytes::from(vec![b'd'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("destination creation blocked")??;
    let mut metadata = empty_metadata();
    metadata.modified_ns = MetadataField::Value(99);
    poll_ready(checkout.set_metadata(
        source.clone(),
        metadata,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path metadata write blocked")??;
    poll_ready(checkout.resize_file(
        destination.clone(),
        160,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path resize blocked")??;
    poll_ready(checkout.zero_file_range(
        source.clone(),
        ByteRange {
            offset: 64,
            length: 16,
        },
        false,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path zero blocked")??;
    poll_ready(checkout.preallocate_file(
        destination.clone(),
        ByteRange {
            offset: 128,
            length: 32,
        },
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path preallocation blocked")??;
    poll_ready(checkout.clone_file_range(
        FileCloneRequest {
            source: source.clone(),
            source_offset: 0,
            destination: destination.clone(),
            destination_offset: 32,
            length: 96,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path clone blocked")??;
    let alias = path("source-alias")?;
    let renamed = path("source-renamed")?;
    poll_ready(checkout.hard_link(
        source.clone(),
        alias.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path hard link blocked")??;
    poll_ready(checkout.rename(
        alias,
        renamed.clone(),
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path rename blocked")??;
    let lookup = poll_ready(checkout.lookup_no_follow_with_metadata(
        &renamed,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("metadata lookup blocked")??
    .value
    .ok_or("renamed link is absent")?;
    assert_eq!(lookup.record.file_id, source_id);
    assert_eq!(lookup.metadata.modified_ns, MetadataField::Value(99));
    let batch = poll_ready(checkout.lookup_batch_no_follow(
        &[source.clone(), renamed.clone(), path("absent-path")?],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path batch lookup blocked")??;
    assert_eq!(batch.value.entries.len(), 3);
    assert_eq!(
        batch.value.entries[0].record.map(|record| record.file_id),
        Some(source_id)
    );
    assert_eq!(
        batch.value.entries[1].record.map(|record| record.file_id),
        Some(source_id)
    );
    assert!(batch.value.entries[2].record.is_none());
    let records = poll_ready(checkout.list_directory_records(
        &root_path()?,
        None,
        16,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("record listing blocked")??;
    assert_eq!(records.value.entries.len(), 3);
    let plan = poll_ready(checkout.plan_file_extents(
        &source,
        ByteRange {
            offset: 0,
            length: 128,
        },
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path extent plan blocked")??;
    assert!(plan.value.is_some());
    assert_eq!(
        poll_ready(checkout.seek_file_extent(
            &source,
            64,
            ExtentSeekTarget::Hole,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("path hole seek blocked")??
        .value,
        Some(64)
    );
    assert_eq!(
        poll_ready(checkout.read_metadata(&source, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("path metadata read blocked")??
            .value,
        metadata
    );
    poll_ready(checkout.remove(
        renamed,
        Some(source_id),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("path removal blocked")??;

    let mut empty = std::io::Cursor::new(OwnedBytes::new());
    assert!(
        poll_ready(checkout.stage_content(&mut empty, 0, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("zero staging bound blocked")?
            .is_err()
    );
    assert!(
        poll_ready(checkout.stage_content(
            &mut empty,
            config().limits.maximum_generation_bytes + 1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("excessive staging bound blocked")?
        .is_err()
    );
    #[cfg(not(target_arch = "wasm32"))]
    {
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
        let body = [b'n'; 256];
        let mut source = std::io::Cursor::new(body.as_slice());
        let staged = poll_ready(checkout.stage_content_with_accounting(
            &mut source, body.len() as u64, &mut accounting, &cancellation,
        )).ok_or("original content staging blocked")??;
        assert_eq!(staged.logical_bytes(), body.len() as u64);
        assert_eq!(original.receipt().source_bytes_read, body.len() as u64);
        assert!(original.receipt().backend_write_operations > 0);

        let denied = crate::NativeMaterializationWork::new(
            WorkBudget { source_bytes_read: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).map_err(|(cause, _)| cause)?;
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&denied, &cancellation);
        let mut source = std::io::Cursor::new(body.as_slice());
        assert!(poll_ready(checkout.stage_content_with_accounting(
            &mut source, body.len() as u64, &mut accounting, &cancellation,
        )).ok_or("original source denial blocked")?.is_err());
        assert_eq!(source.position(), 0);
        assert_eq!(denied.receipt().backend_write_operations, 0);

        struct PrefixFailure(bool);
        impl crate::kernel::AsyncBlobSource for PrefixFailure {
            async fn read<'a>(
                &'a mut self, destination: &'a mut [u8], _budget: WorkBudget,
                _cancellation: &'a CancellationToken,
            ) -> std::io::Result<usize> {
                if self.0 { return Err(std::io::Error::other("content source failed")); }
                self.0 = true;
                let count = destination.len().min(4);
                destination[..count].fill(b'p');
                Ok(count)
            }
            async fn read_with_native_work<'a>(
                &'a mut self, destination: &'a mut [u8], original: &'a crate::NativeMaterializationWork,
                cancellation: &'a CancellationToken,
            ) -> Result<usize, FsError> {
                cancellation.check()?;
                let delta = WorkCounters {
                    source_bytes_read: destination.len().min(4) as u64,
                    bytes_copied: destination.len().min(4) as u64,
                    ..WorkCounters::UNCHARGED
                };
                original.admit(delta)?;
                let count = self.read(destination, original.remaining()?, cancellation).await
                    .map_err(crate::kernel::BlobBuildError::Source)?;
                original.spent(delta)?;
                Ok(count)
            }
        }
        let failed = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&failed, &cancellation);
        let mut source = PrefixFailure(false);
        assert!(poll_ready(checkout.stage_content_with_accounting(
            &mut source, 64, &mut accounting, &cancellation,
        )).ok_or("original source failure blocked")?.is_err());
        assert_eq!(failed.receipt().source_bytes_read, 4);

        let cancelled = CancellationToken::new();
        let stopped = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancelled)
            .map_err(|(cause, _)| cause)?;
        cancelled.cancel();
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&stopped, &cancelled);
        let mut source = std::io::Cursor::new(body.as_slice());
        assert!(poll_ready(checkout.stage_content_with_accounting(
            &mut source, body.len() as u64, &mut accounting, &cancelled,
        )).ok_or("original cancellation blocked")?.is_err());
        assert_eq!(source.position(), 0);
        assert_eq!(stopped.receipt().backend_write_operations, 0);
    }
    Ok(())
}

#[test]
fn public_query_and_identity_failures_preserve_the_checkout_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([118; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let file = path("regular")?;
    let directory = path("directory")?;
    let missing = path("missing")?;
    let file_id = poll_ready(checkout.create_file(
        file.clone(),
        bytes::Bytes::from_static(b"data"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("file creation blocked")??
    .value;
    let directory_id = poll_ready(checkout.create_directory(
        directory.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("directory creation blocked")??
    .value;
    let empty_directory = poll_ready(checkout.list_directory_records(
        &directory,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("empty directory record listing blocked")??;
    assert!(empty_directory.value.entries.is_empty());
    assert!(!empty_directory.value.has_more);
    let unknown_id = FileId::from_bytes([250; 16]);
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.missing".to_vec(),
        128,
    )?;

    let oversized_identity_read = poll_ready(checkout.read_file_range_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: config().limits.maximum_read_bytes + 1,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("oversized identity read blocked")?
    .err()
    .ok_or("oversized identity read unexpectedly succeeded")?;
    assert!(matches!(
        oversized_identity_read.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*oversized_identity_read.work, WorkCounters::default());
    let zero_span_plan = poll_ready(checkout.plan_file_extents_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 1,
        },
        0,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("zero-span identity plan blocked")?
    .err()
    .ok_or("zero-span identity plan unexpectedly succeeded")?;
    assert!(matches!(
        zero_span_plan.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*zero_span_plan.work, WorkCounters::default());
    assert_eq!(
        poll_ready(checkout.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("inline identity data seek blocked")??
        .value,
        Some(0)
    );
    assert_eq!(
        poll_ready(checkout.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Hole,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("inline identity hole seek blocked")??
        .value,
        Some(4)
    );
    for (offset, target, expected) in [
        (4, ExtentSeekTarget::Data, None),
        (4, ExtentSeekTarget::Hole, Some(4)),
        (5, ExtentSeekTarget::Data, None),
        (5, ExtentSeekTarget::Hole, None),
    ] {
        assert_eq!(
            poll_ready(checkout.seek_file_extent_by_id(
                file_id,
                offset,
                target,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .ok_or("inline identity boundary seek blocked")??
            .value,
            expected
        );
        assert_eq!(
            poll_ready(checkout.seek_file_extent(
                &file,
                offset,
                target,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .ok_or("inline path boundary seek blocked")??
            .value,
            expected
        );
    }
    let wrong_kind_seek = poll_ready(checkout.seek_file_extent_by_id(
        directory_id,
        0,
        ExtentSeekTarget::Data,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("wrong-kind identity seek blocked")?
    .err()
    .ok_or("wrong-kind identity seek unexpectedly succeeded")?;
    assert!(matches!(
        wrong_kind_seek.error,
        FsError::FileRead(FileRangeReadError::NotRegular)
    ));
    assert!(
        poll_ready(checkout.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("attribute-free identity read blocked")??
        .value
        .is_none()
    );
    assert!(
        poll_ready(checkout.list_named_attributes_by_id(
            file_id,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("attribute-free identity listing blocked")??
        .value
        .entries
        .is_empty()
    );
    let missing_remove = poll_ready(checkout.remove_named_attribute_by_id(
        file_id,
        attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("attribute-free identity removal blocked")?
    .err()
    .ok_or("attribute-free identity removal unexpectedly succeeded")?;
    assert!(matches!(missing_remove.error, FsError::NotFound));
    let missing_replace = poll_ready(checkout.write_named_attribute_by_id(
        file_id,
        attribute.clone(),
        bytes::Bytes::from_static(b"first"),
        NamedAttributeWriteMode::Replace,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("missing identity attribute replacement blocked")?
    .err()
    .ok_or("missing identity attribute replacement unexpectedly succeeded")?;
    assert!(matches!(missing_replace.error, FsError::CreationRejected));
    poll_ready(checkout.write_named_attribute_by_id(
        file_id,
        attribute.clone(),
        bytes::Bytes::from_static(b"first"),
        NamedAttributeWriteMode::Create,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity attribute creation blocked")??;
    let duplicate_create = poll_ready(checkout.write_named_attribute_by_id(
        file_id,
        attribute.clone(),
        bytes::Bytes::from_static(b"second"),
        NamedAttributeWriteMode::Create,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("duplicate identity attribute creation blocked")?
    .err()
    .ok_or("duplicate identity attribute creation unexpectedly succeeded")?;
    assert!(matches!(duplicate_create.error, FsError::CreationRejected));
    assert_eq!(
        poll_ready(checkout.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("identity attribute read blocked")??
        .value
        .as_deref(),
        Some(b"first".as_slice())
    );
    poll_ready(checkout.remove_named_attribute_by_id(
        file_id,
        attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity attribute removal blocked")??;
    let repeated_remove = poll_ready(checkout.remove_named_attribute_by_id(
        file_id,
        attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("repeated identity attribute removal blocked")?
    .err()
    .ok_or("repeated identity attribute removal unexpectedly succeeded")?;
    assert!(matches!(repeated_remove.error, FsError::NotFound));

    #[cfg(not(target_arch = "wasm32"))]
    {
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let mut candidate = checkout.private_candidate_with_native_work(&original, &cancellation)?;
        let source = OwnedBytes::from_static(b"actual original-accounted attribute source");
        poll_ready(candidate.write_named_attribute_by_id_with_native_work(
            file_id, &attribute, &source, NamedAttributeWriteMode::Create, &original, &cancellation,
        )).ok_or("native identity attribute creation blocked")??;
        let created = poll_ready(candidate.read_metadata_by_id_with_native_work(
            file_id, &original, &cancellation,
        )).ok_or("native identity metadata read blocked")??;
        let MetadataField::Value(root) = created.named_attributes else {
            return Err("native attribute metadata root absent".into());
        };
        let reader = candidate.pinned_reader_with_native_work(&original, &cancellation)?;
        let (values, allocation) = poll_ready(reader.read_named_attributes_with_native_work(
            root, &original, &cancellation,
        )).ok_or("native attribute source read blocked")??;
        assert_eq!(values.len(), 1);
        assert_eq!(&values[0].0, &attribute);
        assert_eq!(values[0].1, source);
        drop(values);
        drop(allocation);
        let duplicate = poll_ready(candidate.write_named_attribute_by_id_with_native_work(
            file_id, &attribute, &source, NamedAttributeWriteMode::Create, &original, &cancellation,
        )).ok_or("native duplicate attribute creation blocked")?
            .err().ok_or("native duplicate attribute unexpectedly accepted")?;
        assert!(matches!(duplicate, FsError::CreationRejected));
        let mut updated = created;
        updated.modified_ns = MetadataField::Value(913);
        poll_ready(candidate.set_attributes_by_id_with_native_work(
            file_id, updated, Some(7), &original, &cancellation,
        )).ok_or("native metadata resize blocked")??;
        assert_eq!(poll_ready(candidate.read_metadata_by_id_with_native_work(
            file_id, &original, &cancellation,
        )).ok_or("native changed metadata read blocked")??, updated);
        let resized = poll_ready(candidate.read_file_record_by_id_with_native_work(
            file_id, &original, &cancellation,
        )).ok_or("native resized record read blocked")??;
        let resized_bytes = match resized.payload {
            FilePayload::InlineRegular(data) => data.as_bytes().len() as u64,
            FilePayload::Regular { logical_bytes, .. } => logical_bytes,
            _ => return Err("native resize changed the regular file kind".into()),
        };
        assert_eq!(resized_bytes, 7);
        poll_ready(candidate.remove_named_attribute_by_id_with_native_work(
            file_id, &attribute, &original, &cancellation,
        )).ok_or("native identity attribute removal blocked")??;
        assert!(original.receipt().source_bytes_read >= source.len() as u64);
        assert!(original.receipt().bytes_encoded > 0);
        assert!(original.receipt().allocation_operations > 0);

        let denied = crate::NativeMaterializationWork::new(
            WorkBudget { source_bytes_read: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).map_err(|(cause, _)| cause)?;
        let mut denied_candidate = checkout.private_candidate_with_native_work(&denied, &cancellation)?;
        let before = denied_candidate.root.file_table;
        let failure = poll_ready(denied_candidate.write_named_attribute_by_id_with_native_work(
            file_id, &attribute, &source, NamedAttributeWriteMode::Upsert, &denied, &cancellation,
        )).ok_or("native denied attribute source blocked")?
            .err().ok_or("native attribute source denial ignored")?;
        assert!(matches!(failure, FsError::Work(_)));
        assert_eq!(denied_candidate.root.file_table, before);
        assert_eq!(denied.receipt().source_bytes_read, 0);

        let denied_metadata = crate::NativeMaterializationWork::new(
            WorkBudget { bytes_encoded: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).map_err(|(cause, _)| cause)?;
        let mut denied_candidate = checkout.private_candidate_with_native_work(&denied_metadata, &cancellation)?;
        let before = denied_candidate.root.file_table;
        let failure = poll_ready(denied_candidate.set_attributes_by_id_with_native_work(
            file_id, updated, Some(3), &denied_metadata, &cancellation,
        )).ok_or("native denied metadata resize blocked")?
            .err().ok_or("native metadata encoding denial ignored")?;
        assert!(matches!(failure, FsError::Work(_)));
        assert_eq!(denied_candidate.root.file_table, before);
        assert_eq!(denied_metadata.receipt().bytes_encoded, 0);
    }

    assert!(
        poll_ready(checkout.list_directory(&file, None, 8, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("wrong-kind directory listing blocked")?
            .is_err()
    );
    assert!(
        poll_ready(checkout.list_directory_records(
            &file,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("wrong-kind record listing blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.read_file_range(
            &directory,
            ByteRange {
                offset: 0,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("wrong-kind path read blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.read_file_range(
            &file,
            ByteRange {
                offset: 5,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("invalid path range blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.plan_file_extents(
            &directory,
            ByteRange {
                offset: 0,
                length: 1,
            },
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("wrong-kind path plan blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.seek_file_extent(
            &directory,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("wrong-kind path seek blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.read_symbolic_link(&file, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("wrong-kind symbolic read blocked")?
            .is_err()
    );
    assert!(
        poll_ready(checkout.read_reparse_point(&file, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("wrong-kind reparse read blocked")?
            .is_err()
    );
    assert!(
        poll_ready(checkout.read_metadata(&missing, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("missing metadata read blocked")?
            .is_err()
    );
    assert!(
        poll_ready(checkout.read_named_attribute(
            &missing,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("missing attribute read blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.list_named_attributes(
            &missing,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("missing attribute listing blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.remove_named_attribute(
            missing.clone(),
            attribute.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("missing attribute removal blocked")?
        .is_err()
    );

    assert!(
        poll_ready(checkout.read_file_record_by_id(
            unknown_id,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("unknown identity read blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.read_file_range_by_id(
            directory_id,
            ByteRange {
                offset: 0,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("wrong-kind identity read blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 5,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("invalid identity range blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.plan_file_extents_by_id(
            directory_id,
            ByteRange {
                offset: 0,
                length: 1,
            },
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("wrong-kind identity plan blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.seek_file_extent_by_id(
            directory_id,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("wrong-kind identity seek blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.read_metadata_by_id(unknown_id, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("unknown identity metadata blocked")?
            .is_err()
    );
    assert!(
        poll_ready(checkout.read_named_attribute_by_id(
            unknown_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("unknown identity attribute blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.list_named_attributes_by_id(
            unknown_id,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("unknown identity attribute listing blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.write_file_by_id(
            directory_id,
            0,
            bytes::Bytes::from_static(b"x"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("wrong-kind identity write blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.remove_named_attribute_by_id(
            unknown_id,
            attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("unknown identity attribute removal blocked")?
        .is_err()
    );
    assert!(checkout.has_pending_mutations());
    assert_eq!(
        poll_ready(checkout.read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 4,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("surviving file read blocked")??
        .value
        .bytes
        .as_ref(),
        b"data"
    );
    Ok(())
}

#[test]
fn public_byte_boundaries_reject_before_allocation_or_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 4;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([189; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let file = path("bounded")?;
    let failure = poll_ready(checkout.create_file(
        file.clone(),
        bytes::Bytes::from_static(b"12345"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("oversized create blocked")?
    .err()
    .ok_or("oversized create succeeded")?;
    assert!(matches!(
        failure.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*failure.work, WorkCounters::default());

    let file_id = poll_ready(checkout.create_file(
        file.clone(),
        bytes::Bytes::from_static(b"1234"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("bounded create blocked")??
    .value;
    for failure in [
        poll_ready(checkout.write_file(
            file.clone(),
            0,
            bytes::Bytes::from_static(b"12345"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("oversized path write blocked")?
        .err()
        .ok_or("oversized path write succeeded")?,
        poll_ready(checkout.write_file_by_id(
            file_id,
            0,
            bytes::Bytes::from_static(b"12345"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("oversized identity write blocked")?
        .err()
        .ok_or("oversized identity write succeeded")?,
    ] {
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    poll_ready(checkout.write_file_by_id(
        file_id,
        4,
        bytes::Bytes::from_static(b"5"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("bounded extension blocked")??;

    for failure in [
        poll_ready(checkout.read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 5,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("oversized path read blocked")?
        .err()
        .ok_or("oversized path read succeeded")?,
        poll_ready(checkout.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 0,
                length: 5,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("oversized identity read blocked")?
        .err()
        .ok_or("oversized identity read succeeded")?,
    ] {
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let excessive_range = ByteRange {
        offset: 0,
        length: limited.limits.maximum_generation_bytes + 1,
    };
    for failure in [
        poll_ready(checkout.plan_file_extents(
            &file,
            excessive_range,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("oversized path plan blocked")?
        .err()
        .ok_or("oversized path plan succeeded")?,
        poll_ready(checkout.plan_file_extents_by_id(
            file_id,
            excessive_range,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("oversized identity plan blocked")?
        .err()
        .ok_or("oversized identity plan succeeded")?,
    ] {
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let symbolic = poll_ready(checkout.create_symbolic_link(
        path("link")?,
        bytes::Bytes::from_static(b"12345"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("oversized symbolic link blocked")?
    .err()
    .ok_or("oversized symbolic link succeeded")?;
    assert!(matches!(
        symbolic.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*symbolic.work, WorkCounters::default());

    let mut detached =
        poll_ready(checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("file detachment blocked")??
            .value;
    let detached_read_failure = poll_ready(detached.read_range(
        ByteRange {
            offset: 0,
            length: 5,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("oversized detached read blocked")?
    .err()
    .ok_or("oversized detached read succeeded")?;
    assert!(matches!(
        detached_read_failure.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*detached_read_failure.work, WorkCounters::default());
    let detached_failure = poll_ready(detached.change_content(
        ContentChange::Write { offset: 0, bytes: OwnedBytes::from_static(b"12345") },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("oversized detached write blocked")?
    .err()
    .ok_or("oversized detached write succeeded")?;
    assert!(matches!(
        detached_failure.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*detached_failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn checkout_generation_and_mode_guards_reject_without_candidate_damage()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let first = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([119; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first volume blocked")??
    .value;
    let second = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([120; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second volume blocked")??
    .value;
    let first_head = poll_ready(first.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first head blocked")??
    .value
    .generation_id();
    let second_head = poll_ready(second.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second head blocked")??
    .value
    .generation_id();
    assert!(
        poll_ready(first.checkout(
            GenerationSelector::Exact(first_head),
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("writable exact checkout blocked")?
        .is_err()
    );
    assert!(
        poll_ready(first.checkout(
            GenerationSelector::Exact(second_head),
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("foreign exact checkout blocked")?
        .is_err()
    );
    assert!(
        poll_ready(first.diff_generations(
            first_head,
            second_head,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("foreign generation diff blocked")?
        .is_err()
    );

    let mut read_only = poll_ready(first.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("read-only checkout blocked")??
    .value;
    assert!(
        poll_ready(read_only.mutate(Vec::new(), WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("read-only mutation blocked")?
            .is_err()
    );
    assert!(
        poll_ready(read_only.commit(OperationId::new(), WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("read-only commit blocked")?
            .is_err()
    );
    assert!(
        poll_ready(read_only.discard(WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("read-only discard blocked")?
            .is_err()
    );
    assert!(
        poll_ready(read_only.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("pinned refresh blocked")?
            .is_err()
    );
    assert!(
        poll_ready(read_only.refresh_live(WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("non-live refresh blocked")?
            .is_err()
    );
    assert!(
        poll_ready(read_only.mutate_live(
            Vec::new(),
            OperationId::new(),
            1,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("non-live mutation blocked")?
        .is_err()
    );
    assert!(
        poll_ready(read_only.resume_live(
            OperationId::new(),
            1,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("non-live resume blocked")?
        .is_err()
    );

    let mut writable = poll_ready(first.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("writable checkout blocked")??
    .value;
    let excessive_capacity = Vec::<Mutation>::with_capacity(
        usize::try_from(config().limits.maximum_mutations_per_batch)? + 1,
    );
    let failure =
        poll_ready(writable.mutate(excessive_capacity, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("capacity-guarded mutation blocked")?
            .err()
            .ok_or("excessive mutation capacity unexpectedly succeeded")?;
    assert!(matches!(failure.error, FsError::ExcessiveMutationCapacity));
    assert_eq!(*failure.work, WorkCounters::default());

    let mut clean = poll_ready(first.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("clean writable checkout blocked")??
    .value;
    assert!(
        poll_ready(clean.commit(OperationId::new(), WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("clean commit blocked")?
            .is_err()
    );
    assert!(
        poll_ready(clean.resume_live(
            OperationId::new(),
            1,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("clean non-live resume blocked")?
        .is_err()
    );

    let mut manual_checkout = poll_ready(first.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("manual checkout blocked")??
    .value;
    poll_ready(manual_checkout.create_file(
        path("dirty")?,
        bytes::Bytes::from_static(b"dirty"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("manual mutation blocked")??;
    assert!(
        poll_ready(manual_checkout.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("dirty manual refresh blocked")?
            .is_err()
    );
    assert!(manual_checkout.has_pending_mutations());
    poll_ready(manual_checkout.discard(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("manual discard blocked")??;
    assert!(!manual_checkout.has_pending_mutations());
    assert_eq!(
        poll_ready(manual_checkout.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("clean manual refresh blocked")??
            .value,
        first_head
    );
    Ok(())
}

#[test]
fn authored_special_operations_are_profile_exact_and_fail_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut posix = config();
    posix.profile = FilesystemProfile::Posix;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([115; 16]),
        posix,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("POSIX volume blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("POSIX checkout blocked")??
    .value;
    let receipt = poll_ready(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateEmptySpecial {
                path: path("fifo")?,
                kind: FileKind::Fifo,
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateEmptySpecial {
                path: path("socket")?,
                kind: FileKind::Socket,
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateEmptySpecial {
                path: path("mount")?,
                kind: FileKind::MountBoundary,
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateDevice {
                path: path("character")?,
                kind: FileKind::CharacterDevice,
                major: 1,
                minor: 3,
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateDevice {
                path: path("block")?,
                kind: FileKind::BlockDevice,
                major: 8,
                minor: 0,
                metadata: empty_metadata(),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("POSIX authored transaction blocked")??;
    assert!(receipt.value.created_file_ids.iter().all(Option::is_some));

    for (mutation, sentinel) in [
        (
            AuthoredMutation::CreateEmptySpecial {
                path: path("invalid-special")?,
                kind: FileKind::Regular,
                metadata: empty_metadata(),
            },
            path("invalid-special")?,
        ),
        (
            AuthoredMutation::CreateDevice {
                path: path("invalid-device")?,
                kind: FileKind::Regular,
                major: 0,
                minor: 0,
                metadata: empty_metadata(),
            },
            path("invalid-device")?,
        ),
    ] {
        let failure = poll_ready(checkout.apply_authored_transaction(
            vec![mutation],
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("invalid authored transaction blocked")?;
        assert!(failure.is_err());
        assert!(
            poll_ready(checkout.lookup_no_follow(&sentinel, WorkBudget::UNBOUNDED, &cancellation,))
                .ok_or("invalid-path lookup blocked")??
                .value
                .record
                .is_none()
        );
    }

    let mut windows = config();
    windows.profile = FilesystemProfile::Windows;
    let windows_volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([116; 16]),
        windows,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("Windows volume blocked")??
    .value;
    let mut windows_checkout = poll_ready(windows_volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("Windows checkout blocked")??
    .value;
    let reparse = path("reparse")?;
    let reparse_payload = OwnedBytes::from_static(b"opaque-reparse-data");
    let receipt = poll_ready(windows_checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateReparsePoint {
            path: reparse.clone(),
            payload: reparse_payload.clone(),
            metadata: empty_metadata(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("Windows authored transaction blocked")??;
    assert!(receipt.value.created_file_ids[0].is_some());
    assert_eq!(
        poll_ready(windows_checkout.read_reparse_point(
            &reparse,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("reparse read blocked")??
        .value.as_ref(),
        reparse_payload.as_ref()
    );
    Ok(())
}

#[test]
fn manifest_transfer_restores_only_after_the_complete_closure_authenticates()
-> Result<(), Box<dyn std::error::Error>> {
    let source = Fs::memory();
    let destination = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(source.create_volume_with_id(
        VolumeId::from_bytes([97; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("source volume blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("source checkout blocked")??
    .value;
    poll_ready(checkout.create_file(
        path("file")?,
        bytes::Bytes::from(vec![42; 4096]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("source file blocked")??;
    let first_checkpoint = poll_ready(checkout.checkpoint(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("first checkpoint blocked")??;
    let repeated_checkpoint = poll_ready(checkout.checkpoint(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("repeated checkpoint blocked")??;
    assert_eq!(first_checkpoint.value, repeated_checkpoint.value);
    let manifest = poll_ready(checkout.export_manifest(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("manifest blocked")??
        .value;
    assert_eq!(manifest.generation_id, first_checkpoint.value);
    assert!(manifest.objects.contains(&manifest.generation_root));

    let mut cursor = TransferCursor::START;
    loop {
        let exported = poll_ready(source.export_generation_batch(
            &manifest,
            cursor,
            2,
            config().limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("object export batch blocked")??;
        let bodies = exported
            .value
            .objects
            .iter()
            .map(|object| object.bytes.clone())
            .collect::<Vec<_>>();
        let imported = poll_ready(destination.import_generation_batch(
            &manifest,
            cursor,
            &bodies,
            2,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("object import batch blocked")??;
        assert_eq!(
            imported.value.next_object(),
            cursor.next_object() + u64::try_from(bodies.len())?
        );
        let Some(next) = exported.value.next else {
            assert_eq!(
                imported.value.next_object(),
                u64::try_from(manifest.objects.len())?
            );
            break;
        };
        assert_eq!(imported.value, next);
        cursor = next;
    }
    let mut wrong_volume = manifest.clone();
    wrong_volume.volume_id = VolumeId::from_bytes([96; 16]);
    let mut wrong_generation = manifest.clone();
    wrong_generation.generation_id = GenerationId::new(Digest::from_bytes([0xee; 32]));
    let mut wrong_objects = manifest.clone();
    wrong_objects
        .objects
        .pop()
        .ok_or("manifest closure is empty")?;
    let mut wrong_file_count = manifest.clone();
    wrong_file_count.file_count = wrong_file_count
        .file_count
        .checked_add(1)
        .ok_or("fixture file count overflow")?;
    for (label, invalid, expected_backend_reads) in [
        ("volume", wrong_volume, true),
        ("generation", wrong_generation, false),
        ("objects", wrong_objects, true),
        ("file-count", wrong_file_count, true),
    ] {
        let failure = poll_ready(destination.restore_volume(
            &invalid,
            OperationId::from_bytes([31; 16]),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("invalid restore blocked")?
        .err()
        .ok_or("invalid manifest unexpectedly restored")?;
        assert!(matches!(failure.error, FsError::InvalidExportManifest));
        assert_eq!(
            failure.work.backend_read_operations > 0,
            expected_backend_reads,
            "unexpected invalid-manifest physical plan for {label}"
        );
        assert_eq!(failure.work.authority_records_appended, 0);
    }
    let restore_operation = OperationId::from_bytes([32; 16]);
    let restored = poll_ready(destination.restore_volume(
        &manifest,
        restore_operation,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("restore blocked")??
    .value;
    let retried = poll_ready(destination.restore_volume(
        &manifest,
        restore_operation,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("restore retry blocked")??
    .value;
    assert_eq!(retried.id(), restored.id());
    let changed_operation = poll_ready(destination.restore_volume(
        &manifest,
        OperationId::from_bytes([33; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("changed-operation restore blocked")?
    .err()
    .ok_or("changed-operation restore unexpectedly succeeded")?;
    assert!(matches!(changed_operation.error, FsError::CreationRejected));
    let mut restored_checkout = poll_ready(restored.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("restored checkout blocked")??
    .value;
    let bytes = poll_ready(restored_checkout.read_file_range(
        &path("file")?,
        ByteRange {
            offset: 0,
            length: 4096,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("restored read blocked")??;
    assert_eq!(bytes.value.bytes.as_ref(), vec![42; 4096]);
    Ok(())
}

#[test]
fn windows_reparse_payload_round_trips_without_interpretation()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut windows = config();
    windows.profile = FilesystemProfile::Windows;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([98; 16]),
        windows,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("Windows volume blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("Windows checkout blocked")??
    .value;
    let payload = OwnedBytes::from_static(b"opaque-reparse\0payload");
    poll_ready(checkout.create_reparse_point(
        path("junction")?,
        payload.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("reparse create blocked")??;
    let read = poll_ready(checkout.read_reparse_point(
        &path("junction")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("reparse read blocked")??;
    assert_eq!(read.value.as_ref(), payload.as_ref());

    let empty_reparse = path("empty-reparse")?;
    poll_ready(checkout.create_reparse_point(
        empty_reparse.clone(),
        bytes::Bytes::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("empty reparse create blocked")??;
    let empty_reparse_read = poll_ready(checkout.read_reparse_point(
        &empty_reparse,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("empty reparse read blocked")??;
    assert!(empty_reparse_read.value.is_empty());

    let empty_link = path("empty-link")?;
    poll_ready(checkout.create_symbolic_link(
        empty_link.clone(),
        bytes::Bytes::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("empty symbolic-link create blocked")??;
    let empty_link_read =
        poll_ready(checkout.read_symbolic_link(&empty_link, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("empty symbolic-link read blocked")??;
    assert!(empty_link_read.value.is_empty());
    Ok(())
}

#[test]
fn encoded_object_and_backend_buffers_share_one_exact_peak()
-> Result<(), Box<dyn std::error::Error>> {
    let prior = WorkCounters {
        bytes_encoded: 64,
        peak_allocation_bytes: 64,
        ..WorkCounters::default()
    };
    let nested = WorkCounters {
        backend_write_operations: 1,
        peak_allocation_bytes: 32,
        ..WorkCounters::default()
    };
    let mut budget = WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = 96;
    let merged =
        merge_simultaneous_work(prior, nested, 64, budget).map_err(|failure| failure.error)?;
    assert_eq!(merged.peak_allocation_bytes, 96);
    assert_eq!(merged.backend_write_operations, 1);

    budget.peak_allocation_bytes = 95;
    let rejected = merge_simultaneous_work(prior, nested, 64, budget)
        .err()
        .ok_or("simultaneous peak unexpectedly admitted")?;
    assert!(matches!(
        rejected.error,
        FsError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed: 96,
            maximum: 95,
        })
    ));

    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let encoded = vec![7_u8; 16];
    let encoded_capacity = u64::try_from(encoded.capacity())?;
    let stored = poll_ready(fs.put_encoded(
        ObjectKind::Metadata,
        OperationReceipt { value: encoded.clone(), work: WorkCounters::default() },
        WorkCounters::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("encoded object put blocked")??;
    assert_eq!(stored.1.peak_allocation_bytes, encoded_capacity);
    assert_eq!(stored.1.backend_write_operations, 1);

    let mut one_byte_short = WorkBudget::UNBOUNDED;
    one_byte_short.peak_allocation_bytes = encoded_capacity - 1;
    let failure = poll_ready(fs.put_encoded(
        ObjectKind::Metadata,
        OperationReceipt { value: encoded, work: WorkCounters::default() },
        WorkCounters::default(),
        one_byte_short,
        &cancellation,
    ))
    .ok_or("peak-limited encoded put blocked")?
    .err()
    .ok_or("one-byte-short encoded put unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        FsError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed,
            maximum,
        }) if observed == encoded_capacity && maximum == encoded_capacity - 1
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn memory_create_open_and_head_checkout_reconstruct_from_two_stores()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([41; 16]);
    let created = poll_ready(fs.create_volume_with_id(
        volume_id,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory create blocked")??;
    let opened = poll_ready(fs.open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("memory open blocked")??;
    assert_eq!(opened.value.config(), config());
    let mut checkout = poll_ready(opened.value.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory checkout blocked")??;
    assert_eq!(checkout.value.volume_id(), volume_id);
    assert_eq!(checkout.value.root().parents, Vec::new());
    assert_eq!(checkout.work.object_probes, 1);
    assert_eq!(checkout.work.page_reads, 0);
    assert!(checkout.work.object_bytes_read <= MAXIMUM_GENERATION_ROOT_BYTES);
    let root_path = NamespacePath::new(Vec::new(), config().limits)?;
    let root = poll_ready(checkout.value.lookup_no_follow(
        &root_path,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory root lookup blocked")??;
    assert_eq!(
        root.value.record.map(|record| record.kind),
        Some(FileKind::Directory)
    );
    let roots = poll_ready(checkout.value.lookup_batch_no_follow(
        &[root_path.clone(), root_path],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory root batch blocked")??;
    assert_eq!(roots.value.entries.len(), 2);
    assert_eq!(roots.value.entries[0], roots.value.entries[1]);
    assert!(created.work.backend_write_operations >= 4);
    Ok(())
}

#[test]
fn create_retry_is_idempotent_and_exact_selector_matches_head()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([42; 16]);
    let first = poll_ready(fs.create_volume_with_id(
        volume_id,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first create blocked")??;
    let retry = poll_ready(fs.create_volume_with_id(
        volume_id,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("retry create blocked")??;
    let head = poll_ready(first.value.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("head checkout blocked")??;
    let exact = poll_ready(retry.value.checkout(
        GenerationSelector::Exact(head.value.generation_id()),
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("exact checkout blocked")??;
    assert_eq!(exact.value.generation_id(), head.value.generation_id());
    Ok(())
}

#[test]
fn unsupported_semantics_reject_before_backend_work() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    let result = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([43; 16]),
        durable,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory create blocked")?;
    let Err(failure) = result else {
        return Err("durable memory volume succeeded".into());
    };
    assert!(matches!(failure.error, FsError::UnsupportedDurability));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn conflicting_creation_config_is_rejected_without_changing_opened_config()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([44; 16]);
    poll_ready(fs.create_volume_with_id(volume_id, config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("first create blocked")??;
    let mut conflicting = config();
    conflicting.sparse_files = false;
    let retry = poll_ready(fs.create_volume_with_id(
        volume_id,
        conflicting,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("conflicting create blocked")?;
    let Err(failure) = retry else {
        return Err("conflicting creation succeeded".into());
    };
    assert!(matches!(failure.error, FsError::CreationRejected));
    let opened = poll_ready(fs.open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("open blocked")??;
    assert_eq!(opened.value.config(), config());
    Ok(())
}

#[test]
fn exact_generation_cannot_cross_volume_identity() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let first = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([45; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first volume create blocked")??;
    let second = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([46; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second volume create blocked")??;
    let second_head = poll_ready(second.value.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second head blocked")??;
    let crossing = poll_ready(first.value.checkout(
        GenerationSelector::Exact(second_head.value.generation_id()),
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cross-volume exact checkout blocked")?;
    let Err(failure) = crossing else {
        return Err("cross-volume exact checkout succeeded".into());
    };
    assert!(matches!(failure.error, FsError::VolumeMismatch));
    Ok(())
}

#[test]
fn pre_cancelled_facade_operations_perform_zero_work() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let result = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([47; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cancelled create blocked")?;
    let Err(failure) = result else {
        return Err("cancelled create succeeded".into());
    };
    assert!(matches!(failure.error, FsError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn pre_cancelled_existing_volume_surfaces_fail_before_visible_work()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! cancelled {
        ($future:expr, $label:literal) => {{
            let result = poll_ready($future).ok_or(concat!($label, " blocked"))?;
            let failure = result
                .err()
                .ok_or(concat!($label, " unexpectedly succeeded"))?;
            assert!(
                format!("{:?}", failure.error).contains("Cancelled"),
                "{}: {:?}",
                $label,
                failure.error
            );
            assert_eq!(*failure.work, WorkCounters::default(), "{}", $label);
        }};
    }

    let fs = Fs::memory();
    let active = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([137; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &active,
    ))
    .ok_or("cancellation fixture volume blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &active,
    ))
    .ok_or("cancellation fixture checkout blocked")??
    .value;
    let file = path("cancelled-file")?;
    let file_id = poll_ready(checkout.create_file(
        file.clone(),
        bytes::Bytes::from_static(b"abcdefgh"),
        WorkBudget::UNBOUNDED,
        &active,
    ))
    .ok_or("cancellation fixture file blocked")??
    .value;
    let mut detached =
        poll_ready(checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &active))
            .ok_or("cancellation fixture detach blocked")??
            .value;
    let cancelled_token = CancellationToken::new();
    cancelled_token.cancel();
    let range = ByteRange {
        offset: 0,
        length: 1,
    };
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.cancelled".to_vec(),
        128,
    )?;

    cancelled!(
        volume.checkout(
            GenerationSelector::Head,
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled checkout"
    );
    cancelled!(
        checkout.lookup_no_follow(&file, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled lookup"
    );
    cancelled!(
        checkout.lookup_batch_no_follow(
            std::slice::from_ref(&file),
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled batch lookup"
    );
    cancelled!(
        checkout.list_directory(
            &root_path()?,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled directory listing"
    );
    cancelled!(
        checkout.read_metadata(&file, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled metadata"
    );
    cancelled!(
        checkout.read_file_range(&file, range, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled path read"
    );
    cancelled!(
        checkout.plan_file_extents(&file, range, 8, WorkBudget::UNBOUNDED, &cancelled_token,),
        "cancelled path plan"
    );
    cancelled!(
        checkout.seek_file_extent(
            &file,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled path seek"
    );
    cancelled!(
        checkout.read_file_record_by_id(file_id, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled identity record"
    );
    cancelled!(
        checkout.read_file_range_by_id(file_id, range, WorkBudget::UNBOUNDED, &cancelled_token,),
        "cancelled identity read"
    );
    cancelled!(
        checkout.read_named_attribute(&file, &attribute, WorkBudget::UNBOUNDED, &cancelled_token,),
        "cancelled path attribute"
    );
    cancelled!(
        checkout.write_file(
            file.clone(),
            0,
            bytes::Bytes::from_static(b"x"),
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled path write"
    );
    cancelled!(
        checkout.create_file(
            path("cancelled-new-file")?,
            bytes::Bytes::from_static(b"must-not-be-encoded"),
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled file creation"
    );
    let mut empty = std::io::Cursor::new(OwnedBytes::new());
    cancelled!(
        checkout.stage_content(&mut empty, 1, WorkBudget::UNBOUNDED, &cancelled_token,),
        "cancelled content staging"
    );
    cancelled!(
        checkout.commit(
            OperationId::from_bytes([138; 16]),
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled commit"
    );
    cancelled!(
        detached.read_range(range, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled detached read"
    );
    cancelled!(
        detached.change_content(
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::from_static(b"x")
            },
            None,
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled detached write"
    );
    Ok(())
}

#[test]
fn private_overlay_commit_retry_and_conflict_are_generation_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([49; 16]);
    let volume = poll_ready(fs.create_volume_with_id(
        volume_id,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory create blocked")??
    .value;
    let mut first = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first writable checkout blocked")??
    .value;
    let mut retry = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("retry writable checkout blocked")??
    .value;
    let mut stale = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("stale writable checkout blocked")??
    .value;
    let root_path = NamespacePath::new(Vec::new(), config().limits)?;
    let metadata =
        poll_ready(first.lookup_no_follow(&root_path, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("root lookup blocked")??
            .value
            .record
            .ok_or("root record absent")?
            .metadata;
    let record = FileRecord {
        file_id: FileId::from_bytes([7; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata,
        payload: FilePayload::InlineRegular(InlineFileData::new(b"hello")?),
    };
    for checkout in [&mut first, &mut retry] {
        poll_ready(checkout.mutate(
            vec![Mutation::Create {
                path: path("a")?,
                record,
            }],
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("overlay mutation blocked")??;
        assert_eq!(
            poll_ready(checkout.lookup_no_follow(
                &path("a")?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .ok_or("candidate lookup blocked")??
            .value
            .record
            .map(|value| value.payload),
            Some(record.payload)
        );
    }
    assert!(
        poll_ready(stale.lookup_no_follow(&path("a")?, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("base lookup blocked")??
            .value
            .record
            .is_none()
    );
    poll_ready(stale.mutate(
        vec![Mutation::Create {
            path: path("b")?,
            record: FileRecord {
                file_id: FileId::from_bytes([8; 16]),
                ..record
            },
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("stale overlay mutation blocked")??;

    let operation_id = OperationId::from_bytes([9; 16]);
    let committed = poll_ready(first.commit(operation_id, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("first commit blocked")??;
    assert!(matches!(
        committed.value,
        CheckoutCommitOutcome::Committed { .. }
    ));
    assert!(!first.has_pending_mutations());
    let repeated = poll_ready(retry.commit(operation_id, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("idempotent commit blocked")??;
    assert!(matches!(
        repeated.value,
        CheckoutCommitOutcome::AlreadyCommitted { .. }
    ));
    assert!(!retry.has_pending_mutations());
    let conflict = poll_ready(stale.commit(
        OperationId::from_bytes([10; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("stale commit blocked")??;
    assert!(matches!(
        conflict.value,
        CheckoutCommitOutcome::Conflict { .. }
    ));
    assert!(stale.has_pending_mutations());

    let mut head = poll_ready(volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("published head checkout blocked")??;
    assert!(
        poll_ready(
            head.value
                .lookup_no_follow(&path("a")?, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("published file lookup blocked")??
        .value
        .record
        .is_some()
    );
    let clean_failure = poll_ready(first.commit(
        OperationId::from_bytes([11; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("clean commit blocked")?
    .err()
    .ok_or("clean checkout commit unexpectedly succeeded")?;
    assert!(matches!(clean_failure.error, FsError::NoPendingMutations));
    assert_eq!(*clean_failure.work, WorkCounters::default());

    let mut inverse = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inverse writable checkout blocked")??
    .value;
    let inverse_record = FileRecord {
        file_id: FileId::from_bytes([15; 16]),
        ..record
    };
    poll_ready(inverse.mutate(
        vec![Mutation::Create {
            path: path("inverse")?,
            record: inverse_record,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inverse create blocked")??;
    assert!(inverse.has_pending_mutations());
    poll_ready(inverse.mutate(
        vec![Mutation::Remove {
            path: path("inverse")?,
            expected_file_id: MetadataField::Value(inverse_record.file_id),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inverse removal blocked")??;
    assert!(!inverse.has_pending_mutations());
    assert!(inverse.dependencies.proof().is_empty());
    let inverse_commit = poll_ready(inverse.commit(
        OperationId::from_bytes([16; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inverse clean commit blocked")?
    .err()
    .ok_or("inverse clean commit unexpectedly succeeded")?;
    assert!(matches!(inverse_commit.error, FsError::NoPendingMutations));
    assert_eq!(
        *inverse_commit.work,
        WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::default()
        }
    );
    Ok(())
}

#[test]
fn exclusive_writer_checkout_atomically_fences_every_prior_writer()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([0xe1; 16]),
        exclusive_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("exclusive volume creation blocked")??
    .value;
    let mut displaced = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first exclusive checkout blocked")??
    .value;
    let mut owner = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("replacement exclusive checkout blocked")??
    .value;
    poll_ready(displaced.create_file(
        path("displaced")?,
        bytes::Bytes::from_static(b"stale"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("displaced mutation blocked")??;
    let rejected = poll_ready(displaced.commit(
        OperationId::from_bytes([0xe2; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("displaced publication blocked")??;
    assert!(matches!(
        rejected.value,
        CheckoutCommitOutcome::Fenced { actual_epoch } if actual_epoch.get() == 3
    ));
    poll_ready(owner.create_file(
        path("owner")?,
        bytes::Bytes::from_static(b"current"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("owner mutation blocked")??;
    let committed = poll_ready(owner.commit(
        OperationId::from_bytes([0xe3; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("owner publication blocked")??;
    assert!(matches!(
        committed.value,
        CheckoutCommitOutcome::Committed { head, .. } if head.epoch.get() == 3
    ));
    Ok(())
}

#[test]
fn manual_refresh_is_explicit_bounded_and_never_discards_mutations()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([50; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory create blocked")??
    .value;
    let mut clean = poll_ready(volume.checkout(
        GenerationSelector::Head,
        manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("clean manual checkout blocked")??
    .value;
    let mut clean_tracking = poll_ready(volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("clean tracking checkout blocked")??
    .value;
    let mut dirty = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("dirty manual checkout blocked")??
    .value;
    let mut conflicted = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("conflicted manual checkout blocked")??
    .value;
    let mut writer = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("writer checkout blocked")??
    .value;
    let mut pinned_checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("pinned checkout blocked")??
    .value;
    let root_path = NamespacePath::new(Vec::new(), config().limits)?;
    let metadata =
        poll_ready(writer.lookup_no_follow(&root_path, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("root lookup blocked")??
            .value
            .record
            .ok_or("root record absent")?
            .metadata;
    let record = FileRecord {
        file_id: FileId::from_bytes([12; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata,
        payload: FilePayload::InlineRegular(InlineFileData::new(b"new-head")?),
    };
    assert!(
        poll_ready(conflicted.lookup_no_follow(
            &path("published")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("negative dependency lookup blocked")??
        .value
        .record
        .is_none()
    );
    poll_ready(conflicted.mutate(
        vec![Mutation::Create {
            path: path("conflicted-local")?,
            record: FileRecord {
                file_id: FileId::from_bytes([16; 16]),
                ..record
            },
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("conflicted local mutation blocked")??;
    poll_ready(dirty.mutate(
        vec![Mutation::Create {
            path: path("private")?,
            record,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("dirty mutation blocked")??;
    poll_ready(writer.mutate(
        vec![Mutation::Create {
            path: path("published")?,
            record: FileRecord {
                file_id: FileId::from_bytes([13; 16]),
                ..record
            },
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("writer mutation blocked")??;
    poll_ready(writer.commit(
        OperationId::from_bytes([14; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("writer commit blocked")??;
    let conflict = poll_ready(conflicted.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("conflicting rebase blocked")??;
    assert!(matches!(
        conflict.value,
        RebaseDecision::Conflicted {
            ref conflicts,
            truncated: false,
        } if conflicts.iter().any(|value| matches!(
            value.region,
            DependencyRegion::DirectoryName { .. }
        ))
    ));
    assert!(conflicted.has_pending_mutations());

    let pinned_failure =
        poll_ready(pinned_checkout.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("pinned refresh blocked")?
            .err()
            .ok_or("pinned refresh unexpectedly succeeded")?;
    assert!(matches!(pinned_failure.error, FsError::RefreshNotAllowed));
    assert_eq!(*pinned_failure.work, WorkCounters::default());
    let pinned_rebase =
        poll_ready(pinned_checkout.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("pinned rebase guard blocked")?
            .err()
            .ok_or("pinned rebase unexpectedly succeeded")?;
    assert!(matches!(pinned_rebase.error, FsError::RefreshNotAllowed));
    assert_eq!(*pinned_rebase.work, WorkCounters::default());

    let dirty_failure = poll_ready(dirty.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("dirty refresh blocked")?
        .err()
        .ok_or("dirty refresh unexpectedly succeeded")?;
    assert!(matches!(
        dirty_failure.error,
        FsError::PendingMutationsRequireRebase
    ));
    assert_eq!(*dirty_failure.work, WorkCounters::default());
    assert!(dirty.has_pending_mutations());
    assert!(
        poll_ready(
            dirty.lookup_no_follow(&path("private")?, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("private lookup blocked")??
        .value
        .record
        .is_some()
    );
    let rebased = poll_ready(dirty.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("safe dirty rebase blocked")??;
    assert!(matches!(rebased.value, RebaseDecision::Safe { .. }));
    assert!(dirty.has_pending_mutations());
    assert!(
        poll_ready(dirty.lookup_no_follow(
            &path("published")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("rebased published lookup blocked")??
        .value
        .record
        .is_some()
    );
    assert!(
        poll_ready(
            dirty.lookup_no_follow(&path("private")?, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("rebased private lookup blocked")??
        .value
        .record
        .is_some()
    );

    let advanced = poll_ready(clean.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("manual refresh blocked")??;
    assert_eq!(advanced.value, writer.generation_id());
    assert!(advanced.work.object_probes > 0);
    let tracking_advanced =
        poll_ready(clean_tracking.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("tracking refresh blocked")??;
    assert_eq!(tracking_advanced.value, writer.generation_id());
    assert!(
        poll_ready(clean.lookup_no_follow(
            &path("published")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("published lookup blocked")??
        .value
        .record
        .is_some()
    );
    let equal = poll_ready(clean.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("equal refresh blocked")??;
    assert_eq!(equal.value, advanced.value);
    assert_eq!(equal.work.object_probes, 0);
    assert_eq!(equal.work.backend_read_operations, 3);

    assert!(!dirty.dependencies.proof().is_empty());
    poll_ready(dirty.discard(WorkBudget::UNBOUNDED, &cancellation)).ok_or("discard blocked")??;
    assert!(!dirty.has_pending_mutations());
    assert!(dirty.dependencies.proof().is_empty());
    assert!(
        poll_ready(
            dirty.lookup_no_follow(&path("private")?, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("discarded lookup blocked")??
        .value
        .record
        .is_none()
    );
    assert!(
        poll_ready(dirty.lookup_no_follow(
            &path("published")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("discarded published lookup blocked")??
        .value
        .record
        .is_some()
    );
    let after_discard = poll_ready(dirty.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("post-discard refresh blocked")??;
    assert_eq!(after_discard.value, writer.generation_id());
    Ok(())
}

#[test]
fn private_candidate_rebases_more_changes_than_one_mutation_batch()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_mutations_per_batch = 8;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([241; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let mut local = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("local checkout blocked")??
    .value;
    let mut remote = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote checkout blocked")??
    .value;
    for index in 0..20 {
        poll_ready(local.create_file(
            path(&format!("local{index}"))?,
            bytes::Bytes::from_static(b"local"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("local mutation blocked")??;
    }
    poll_ready(remote.create_file(
        path("remote")?,
        bytes::Bytes::from_static(b"remote"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote mutation blocked")??;
    poll_ready(remote.commit(
        OperationId::from_bytes([242; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote commit blocked")??;
    assert!(matches!(
        poll_ready(local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("rebase blocked")??
            .value,
        RebaseDecision::Safe { .. }
    ));
    assert!(matches!(
        poll_ready(local.commit(
            OperationId::from_bytes([243; 16]),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("local commit blocked")??
        .value,
        CheckoutCommitOutcome::Committed { .. }
    ));
    for index in 0..20 {
        assert!(
            poll_ready(local.lookup_no_follow(
                &path(&format!("local{index}"))?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .ok_or("local lookup blocked")??
            .value
            .record
            .is_some()
        );
    }
    assert!(
        poll_ready(local.lookup_no_follow(&path("remote")?, WorkBudget::UNBOUNDED, &cancellation,))
            .ok_or("remote lookup blocked")??
            .value
            .record
            .is_some()
    );
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_facade_reopens_durable_volume_and_exact_generation()
-> Result<(), Box<dyn std::error::Error>> {
    use fs2::FileExt as _;

    let directory = tempfile::tempdir()?;
    let volume_id = VolumeId::from_bytes([48; 16]);
    let cancellation = CancellationToken::new();
    let generation_id;
    let root_released;
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    {
        let fs = Fs::local(LocalOptions::new(directory.path())).await?;
        root_released = fs
            .local_root_release_barrier()
            .ok_or("local root has no release barrier")?;
        let created = fs
            .create_volume_with_id(volume_id, durable, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        let checkout = created
            .value
            .checkout(
                GenerationSelector::Head,
                writable_pinned(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        generation_id = checkout.value.generation_id();
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !root_released.is_released() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    for relative in ["stream/stream.lock", "objects/owner.lock"] {
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.path().join(relative))?;
        lock.try_lock_exclusive()?;
        lock.unlock()?;
    }
    let objects_journal = directory.path().join("objects").join("mutations.log");
    let journal_bytes_before_reopen = std::fs::metadata(&objects_journal)?.len();
    let reopened = Fs::local(LocalOptions::new(directory.path())).await?;
    assert_eq!(
        std::fs::metadata(objects_journal)?.len(),
        journal_bytes_before_reopen,
        "read-only reopen must not append an idempotent bucket mutation"
    );
    let volume = reopened
        .open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    let exact = volume
        .value
        .checkout(
            GenerationSelector::Exact(generation_id),
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(exact.value.generation_id(), generation_id);
    assert_eq!(exact.value.volume_id(), volume_id);
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[test]
fn local_root_lifecycle_outlives_provider_destruction() -> Result<(), Box<dyn std::error::Error>> {
    struct BlockingProviderDrop {
        entered: std::sync::mpsc::SyncSender<()>,
        release: std::sync::mpsc::Receiver<()>,
    }

    impl Drop for BlockingProviderDrop {
        fn drop(&mut self) {
            let _ = self.entered.send(());
            let _ = self.release.recv();
        }
    }

    let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
    let ownership = Arc::clone(&lifecycle).try_lock_owned()?;
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let inner = FsInner {
        authority: BlockingProviderDrop {
            entered: entered_tx,
            release: release_rx,
        },
        objects: (),
        capabilities: EmbeddedCapabilities::MEMORY,
        workspace_namespace: [0; 16],
        path_index: Arc::new(crate::path_index::MemoryGenerationPathIndex::default()),
        _local_root_lifecycle: Some(acyclic_native_runtime::OwnershipAnchor::new(ownership)),
    };
    let dropping = std::thread::spawn(move || drop(inner));

    entered_rx.recv()?;
    assert!(
        Arc::clone(&lifecycle).try_lock_owned().is_err(),
        "replacement root ownership must remain fenced while providers are dropping"
    );
    release_tx.send(())?;
    dropping
        .join()
        .map_err(|_| "provider drop thread panicked")?;
    Arc::clone(&lifecycle).try_lock_owned()?;
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detached_provider_handles_retain_local_root_ownership()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
    let ownership = Arc::clone(&lifecycle).lock_owned().await;
    let fs = Fs::open_local_unshared(
        LocalOptions::new(directory.path()),
        Some(acyclic_native_runtime::OwnershipAnchor::new(ownership)),
    )
    .await?;
    let stream = fs.inner.authority.provider();
    let objects = Arc::clone(fs.inner.objects.inner().inner().provider());
    drop(fs);

    assert!(
        Arc::clone(&lifecycle).try_lock_owned().is_err(),
        "a detached Stream or Objects handle must retain the canonical root gate"
    );
    drop(stream);
    assert!(
        Arc::clone(&lifecycle).try_lock_owned().is_err(),
        "the root gate must remain held until every detached provider is gone"
    );
    drop(objects);
    Arc::clone(&lifecycle).try_lock_owned()?;
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test]
async fn cancelled_local_open_keeps_the_canonical_lifecycle_gate()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let options = LocalOptions::new(directory.path());
    let first = Fs::local(options.clone()).await?;
    let retained = first
        .inner
        ._local_root_lifecycle
        .as_ref()
        .ok_or_else(|| std::io::Error::other("local root ownership"))?
        .clone();
    drop(first);

    let retry_options = options.clone();
    let opening = tokio::spawn(async move { Fs::local(retry_options).await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!opening.is_finished());
    let cancelled_options = options.clone();
    let cancelled = tokio::spawn(async move { Fs::local(cancelled_options).await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!cancelled.is_finished());
    cancelled.abort();
    assert!(cancelled.await.is_err_and(|error| error.is_cancelled()));

    drop(retained);
    let reopened = tokio::time::timeout(std::time::Duration::from_secs(2), opening).await???;
    let coalesced = Fs::local(options).await?;
    assert!(reopened.same_deployment(&coalesced));
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test]
async fn cancelled_local_initialization_keeps_root_owned_until_startup_stops()
-> Result<(), Box<dyn std::error::Error>> {
    let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
    let ownership = Arc::clone(&lifecycle).lock_owned().await;
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let opening = tokio::spawn(run_local_initialization(
        ownership,
        crate::obs::span!(DEBUG, "test.local_initialization"),
        move |ownership| async move {
            let _ownership = ownership;
            let _ = started_tx.send(());
            let _ = release_rx.await;
            drop(_ownership);
        },
    ));

    started_rx.await?;
    opening.abort();
    let cancelled = opening.await;
    assert!(
        cancelled.as_ref().is_err_and(|error| error.is_cancelled()),
        "opening caller must be cancelled: {cancelled:?}"
    );
    assert!(
        Arc::clone(&lifecycle).try_lock_owned().is_err(),
        "caller cancellation must not release ownership from an active startup worker"
    );
    let _ = release_tx.send(());
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        Arc::clone(&lifecycle).lock_owned(),
    )
    .await?;
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_facade_shares_bounded_object_acceleration_across_handles()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    let fs = Fs::local(LocalOptions::new(directory.path())).await?;
    let volume = fs
        .create_volume(durable, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let checkout = volume
        .checkout(
            GenerationSelector::Head,
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let root = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: checkout.generation_id().digest(),
    };

    fs.clear_object_cache()?;
    let baseline = fs.object_cache_stats()?;
    assert_eq!(baseline.resident_entries, 0);
    let first = fs
        .export_object(
            root,
            durable.limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let cloned = fs.clone();
    let warm = cloned
        .export_object(
            root,
            durable.limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;

    assert_eq!(first.value.bytes, warm.value.bytes);
    assert_eq!(first.work.backend_read_operations, 1);
    assert_eq!(warm.work.backend_read_operations, 0);
    let stats = fs.object_cache_stats()?;
    assert_eq!(stats.misses - baseline.misses, 1);
    assert_eq!(stats.hits - baseline.hits, 1);
    assert_eq!(stats.resident_entries, 1);
    assert!(stats.resident_bytes > 0);
    Ok(())
}

#[test]
fn public_identity_and_posix_special_surfaces_share_one_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_ready($future).ok_or("filesystem future blocked")??
        };
    }

    let mut volume_config = config();
    volume_config.profile = FilesystemProfile::Posix;
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume(volume_config, WorkBudget::UNBOUNDED, &cancellation,));
    let mut checkout = ready!(volume.value.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));

    let file = path("identity.bin")?;
    let clone = path("clone.bin")?;
    let file_id = ready!(checkout.value.create_file(
        file.clone(),
        bytes::Bytes::from_static(b"abcdefgh"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let clone_id = ready!(checkout.value.create_file(
        clone.clone(),
        bytes::Bytes::from_static(b"........"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;

    let record = ready!(checkout.value.read_file_record_by_id(
        file_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(record.value.file_id, file_id);
    assert_eq!(record.value.kind, FileKind::Regular);
    assert_eq!(
        ready!(checkout.value.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 2,
                length: 3,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .bytes
        .as_ref(),
        b"cde"
    );
    assert!(
        ready!(checkout.value.plan_file_extents_by_id(
            file_id,
            ByteRange {
                offset: 0,
                length: 8,
            },
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .is_none()
    );
    assert_eq!(
        ready!(checkout.value.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        Some(0)
    );
    assert_eq!(
        ready!(checkout.value.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Hole,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        Some(8)
    );
    assert_eq!(
        ready!(checkout.value.seek_file_extent_by_id(
            file_id,
            9,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        None
    );
    assert_eq!(
        ready!(checkout.value.seek_file_extent(
            &file,
            9,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        None
    );

    let mut path_metadata = ready!(checkout.value.read_metadata(
        &file,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    path_metadata.posix_mode = MetadataField::Value(0o100_660);
    ready!(checkout.value.set_attributes(
        file.clone(),
        path_metadata,
        Some(10),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        ready!(checkout.value.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 8,
                length: 2,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .bytes
        .as_ref(),
        &[0, 0]
    );

    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.identity".to_vec(),
        128,
    )?;
    assert!(
        ready!(checkout.value.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .is_none()
    );
    assert!(
        ready!(checkout.value.list_named_attributes_by_id(
            file_id,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .entries
        .is_empty()
    );
    ready!(checkout.value.write_named_attribute_by_id(
        file_id,
        attribute.clone(),
        bytes::Bytes::from_static(b"first"),
        NamedAttributeWriteMode::Create,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        ready!(checkout.value.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .as_deref(),
        Some(b"first".as_slice())
    );
    assert_eq!(
        ready!(checkout.value.list_named_attributes_by_id(
            file_id,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .entries
        .len(),
        1
    );
    ready!(checkout.value.write_named_attribute_by_id(
        file_id,
        attribute.clone(),
        bytes::Bytes::from_static(b"second"),
        NamedAttributeWriteMode::Replace,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));

    let mut metadata = ready!(checkout.value.read_metadata_by_id(
        file_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    metadata.posix_mode = MetadataField::Value(0o100_640);
    ready!(checkout.value.set_metadata_by_id(
        file_id,
        metadata,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    metadata.posix_mode = MetadataField::Value(0o100_600);
    ready!(checkout.value.set_attributes_by_id(
        file_id,
        metadata,
        Some(12),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.write_file_by_id(
        file_id,
        8,
        bytes::Bytes::from_static(b"tail"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.zero_file_range_by_id(
        file_id,
        ByteRange {
            offset: 4,
            length: 2,
        },
        false,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.preallocate_file_by_id(
        file_id,
        ByteRange {
            offset: 12,
            length: 4,
        },
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(
        checkout
            .value
            .resize_file_by_id(clone_id, 16, WorkBudget::UNBOUNDED, &cancellation,)
    );
    ready!(checkout.value.clone_file_range_by_id(
        file_id,
        0,
        clone_id,
        4,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.remove_named_attribute_by_id(
        file_id,
        attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(
        ready!(checkout.value.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .is_none()
    );

    let target = OwnedBytes::from_static(b"../identity.bin");
    ready!(checkout.value.create_symbolic_link(
        path("link")?,
        target.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        ready!(checkout.value.read_symbolic_link(
            &path("link")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        target
    );
    ready!(checkout.value.create_empty_special(
        path("fifo")?,
        FileKind::Fifo,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.create_empty_special(
        path("socket")?,
        FileKind::Socket,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.create_device(
        path("character")?,
        FileKind::CharacterDevice,
        1,
        3,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.create_device(
        path("block")?,
        FileKind::BlockDevice,
        8,
        0,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(
        poll_ready(checkout.value.create_empty_special(
            path("invalid")?,
            FileKind::Regular,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("invalid special blocked")?
        .is_err()
    );
    assert!(
        poll_ready(checkout.value.create_device(
            path("invalid-device")?,
            FileKind::Regular,
            0,
            0,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("invalid device blocked")?
        .is_err()
    );

    ready!(checkout.value.hard_link(
        file.clone(),
        path("hard-link")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.rename(
        path("hard-link")?,
        path("renamed-link")?,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.remove(
        path("renamed-link")?,
        Some(file_id),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let listing = ready!(checkout.value.list_directory_records(
        &root_path()?,
        None,
        32,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(listing.value.entries.len(), 7);
    Ok(())
}

/// A rejected open must not leave a provider opening behind it: once the live
/// engine drops, its providers belong to the next opener at once.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejected_open_leaves_no_provider_opening_behind_it()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let options = LocalOptions::new(directory.path());
    // An orphaned opener shows on the first quick reopen it races; the
    // iterations only vary that race's timing.
    for iteration in 0..24_u32 {
        let fs = Fs::local(options.clone()).await?;
        assert!(
            Fs::open_local_unshared(options.clone(), None)
                .await
                .is_err(),
            "a live engine excludes a second owner"
        );
        drop(fs);
        drop(
            acyclic_objects::v1::local::LocalObjects::open(
                directory.path().join("objects"),
                acyclic_objects::LocalObjectsLimits::default(),
            )
            .await
            .map_err(|error| format!("objects still owned after iteration {iteration}: {error}"))?,
        );
        drop(
            acyclic_stream::LocalStream::open(
                directory.path().join("stream"),
                acyclic_stream::LocalStreamLimits::default(),
            )
            .await
            .map_err(|error| format!("stream still owned after iteration {iteration}: {error}"))?,
        );
    }
    Ok(())
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_garbage_collection_keeps_live_state_while_the_root_is_open()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let options = LocalOptions::new(directory.path());
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([49; 16]);
    let file = path("retained.txt")?;
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    let fs = Fs::local(options.clone()).await?;
    let volume = fs
        .create_volume_with_id(volume_id, durable, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let mut checkout = volume
        .checkout(
            GenerationSelector::Head,
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    checkout
        .create_file(
            file.clone(),
            bytes::Bytes::from_static(b"retained"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    checkout
        .commit(
            OperationId::from_bytes([49; 16]),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    // An open checkout's unpublished work survives a collection, and so
    // does everything its publication then names.
    let pending = path("pending.txt")?;
    checkout
        .create_file(
            pending.clone(),
            bytes::Bytes::from(vec![b'p'; 256 * 1_024]),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    fs.collect_local_garbage(None, &cancellation).await?;
    checkout
        .commit(
            OperationId::from_bytes([50; 16]),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    fs.collect_local_garbage(None, &cancellation).await?;
    drop(checkout);
    drop(volume);
    drop(fs);

    let orphan_bytes = OwnedBytes::from(vec![b'o'; 64 * 1_024 + 1]);
    let orphan = ObjectId {
        kind: ObjectKind::Blob,
        digest: object_digest(ObjectKind::Blob, &orphan_bytes),
    };
    let objects = acyclic_objects::v1::local::LocalObjects::open(
        directory.path().join("objects"),
        acyclic_objects::LocalObjectsLimits::default(),
    )
    .await?;
    let bucket = acyclic_objects::v1::wire::BucketRef {
        name: "filesystem-objects".to_owned(),
    };
    acyclic_objects::v1::ObjectsProvider::head_bucket(
        &objects,
        acyclic_objects::v1::wire::HeadBucketRequest {
            bucket: Some(bucket.clone()),
        },
    )
    .await?;
    acyclic_objects::v1::ObjectsProvider::put(
        &objects,
        acyclic_objects::v1::wire::PutObjectHeader {
            bucket: Some(bucket),
            object_key: crate::distributed::object_key(orphan),
            metadata: Some(acyclic_objects::v1::wire::ObjectMetadata::default()),
            preconditions: Some(acyclic_objects::v1::wire::Preconditions {
                condition: Some(
                    acyclic_objects::v1::wire::preconditions::Condition::IfAbsent(true),
                ),
            }),
            mutation: None,
        },
        orphan_bytes,
    )
    .await?;
    drop(objects);
    let reopened = Fs::local(options).await?;
    let collected = reopened.collect_local_garbage(None, &cancellation).await?;
    assert!(collected.removed >= 1);
    assert!(collected.segments_removed >= 1);

    let volume = reopened
        .open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    let mut checkout = volume
        .value
        .checkout(
            GenerationSelector::Head,
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let retained = checkout
        .value
        .read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 8,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(retained.value.bytes.as_ref(), b"retained");
    let pending = checkout
        .value
        .read_file_range(
            &pending,
            ByteRange {
                offset: 0,
                length: 256 * 1_024,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(
        pending.value.bytes.as_ref(),
        vec![b'p'; 256 * 1_024].as_slice()
    );
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_facade_centralizes_same_process_owners_and_rejects_option_drift()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let options = LocalOptions::new(directory.path());
    let first = Fs::local(options.clone()).await?;
    let second = Fs::local(options.clone()).await?;
    assert!(first.same_deployment(&second));

    let mut conflicting = options;
    conflicting.object_cache.maximum_entries =
        conflicting.object_cache.maximum_entries.saturating_add(1);
    assert!(matches!(
        Fs::local(conflicting).await,
        Err(FsError::LocalOptionsConflict)
    ));
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_facade_waits_per_root_without_serializing_independent_roots()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let first_root = directory.path().join("first");
    let second_root = directory.path().join("second");
    let first = Fs::local(LocalOptions::new(&first_root)).await?;
    let retained = first
        .inner
        ._local_root_lifecycle
        .as_ref()
        .ok_or_else(|| std::io::Error::other("local root ownership"))?
        .clone();
    drop(first);

    let same_root = first_root.clone();
    let reopening = tokio::spawn(async move { Fs::local(LocalOptions::new(first_root)).await });
    let concurrent = tokio::spawn(async move { Fs::local(LocalOptions::new(same_root)).await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !reopening.is_finished(),
        "retained native owner must fence reopening"
    );
    assert!(!concurrent.is_finished());

    let second = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        Fs::local(LocalOptions::new(second_root)),
    )
    .await??;
    drop(second);
    drop(retained);
    let reopened = tokio::time::timeout(std::time::Duration::from_secs(2), reopening).await???;
    let shared = tokio::time::timeout(std::time::Duration::from_secs(2), concurrent).await???;
    assert!(reopened.same_deployment(&shared));
    drop(shared);
    drop(reopened);
    Ok(())
}

#[test]
fn named_attributes_are_sparse_bounded_and_atomic_with_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_ready($future).ok_or("filesystem future blocked")??
        };
    }
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation,));
    let mut checkout = ready!(volume.value.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let path = path("attributes.bin")?;
    ready!(checkout.value.create_file(
        path.clone(),
        bytes::Bytes::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let name = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.acyclic".to_vec(),
        128,
    )?;
    assert!(
        ready!(checkout.value.read_named_attribute(
            &path,
            &name,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .is_none()
    );
    let empty_listing = ready!(checkout.value.list_named_attributes(
        &path,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(empty_listing.value.entries.is_empty());
    assert!(!empty_listing.value.has_more);
    let absent_remove = poll_ready(checkout.value.remove_named_attribute(
        path.clone(),
        name.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("attribute-free removal blocked")?
    .err()
    .ok_or("attribute-free removal unexpectedly succeeded")?;
    assert!(matches!(absent_remove.error, FsError::NotFound));
    let absent_replace = poll_ready(checkout.value.write_named_attribute(
        path.clone(),
        name.clone(),
        bytes::Bytes::from_static(b"replacement"),
        NamedAttributeWriteMode::Replace,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("absent attribute replacement blocked")?
    .err()
    .ok_or("absent attribute replacement unexpectedly succeeded")?;
    assert!(matches!(absent_replace.error, FsError::CreationRejected));
    ready!(checkout.value.write_named_attribute(
        path.clone(),
        name.clone(),
        bytes::Bytes::from_static(b"first"),
        NamedAttributeWriteMode::Create,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let duplicate_create = poll_ready(checkout.value.write_named_attribute(
        path.clone(),
        name.clone(),
        bytes::Bytes::from_static(b"duplicate"),
        NamedAttributeWriteMode::Create,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("duplicate attribute creation blocked")?
    .err()
    .ok_or("duplicate attribute creation unexpectedly succeeded")?;
    assert!(matches!(duplicate_create.error, FsError::CreationRejected));
    assert_eq!(
        ready!(checkout.value.read_named_attribute(
            &path,
            &name,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .as_deref(),
        Some(b"first".as_slice())
    );
    let listing = ready!(checkout.value.list_named_attributes(
        &path,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(listing.value.entries.len(), 1);
    assert_eq!(listing.value.entries[0].name, name);
    assert!(!listing.value.has_more);
    ready!(checkout.value.write_named_attribute(
        path.clone(),
        name.clone(),
        bytes::Bytes::from_static(b"second"),
        NamedAttributeWriteMode::Replace,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.remove_named_attribute(
        path.clone(),
        name.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(
        ready!(checkout.value.read_named_attribute(
            &path,
            &name,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .is_none()
    );
    let removed_listing = ready!(checkout.value.list_named_attributes(
        &path,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(removed_listing.value.entries.is_empty());
    assert!(!removed_listing.value.has_more);
    Ok(())
}

#[test]
fn generation_diff_is_semantic_bounded_and_equal_root_constant_work()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_ready($future).ok_or("filesystem future blocked")??
        };
    }
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume_with_id(
        VolumeId::from_bytes([73; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut checkout = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let initial = checkout.generation_id();
    ready!(checkout.create_file(
        path("changed")?,
        bytes::Bytes::from_static(b"one"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let first =
        match ready!(checkout.commit(OperationId::new(), WorkBudget::UNBOUNDED, &cancellation,))
            .value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
            outcome => return Err(format!("unexpected first commit: {outcome:?}").into()),
        };
    let invalid = poll_ready(volume.diff_generations(
        initial,
        first,
        0,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("zero-bound generation diff blocked")?
    .err()
    .ok_or("zero-bound generation diff unexpectedly succeeded")?;
    assert!(matches!(invalid.error, FsError::InvalidDiff));
    assert_eq!(*invalid.work, WorkCounters::default());
    ready!(checkout.write_file(
        path("changed")?,
        0,
        bytes::Bytes::from_static(b"two"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let second =
        match ready!(checkout.commit(OperationId::new(), WorkBudget::UNBOUNDED, &cancellation,))
            .value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
            outcome => return Err(format!("unexpected second commit: {outcome:?}").into()),
        };

    let created =
        ready!(volume.diff_generations(initial, first, 16, WorkBudget::UNBOUNDED, &cancellation,));
    assert!(!created.value.truncated);
    assert!(created.value.files.len() >= 2);
    assert_eq!(created.value.bindings.len(), 1);
    let bounded =
        ready!(volume.diff_generations(initial, first, 1, WorkBudget::UNBOUNDED, &cancellation,));
    assert!(bounded.value.truncated);
    assert_eq!(bounded.value.files.len(), 1);
    assert!(bounded.value.bindings.is_empty());
    let modified =
        ready!(volume.diff_generations(first, second, 16, WorkBudget::UNBOUNDED, &cancellation,));
    assert_eq!(modified.value.files.len(), 1);
    assert!(modified.value.bindings.is_empty());
    let equal =
        ready!(volume.diff_generations(second, second, 16, WorkBudget::UNBOUNDED, &cancellation,));
    assert!(equal.value.files.is_empty());
    assert_eq!(equal.work, WorkCounters::default());

    let root_metadata =
        ready!(checkout.lookup_no_follow(&root_path()?, WorkBudget::UNBOUNDED, &cancellation,))
            .value
            .record
            .ok_or("diff fixture root is absent")?
            .metadata;
    ready!(checkout.mutate(
        vec![Mutation::Create {
            path: path("ordered-last")?,
            record: FileRecord {
                file_id: FileId::from_bytes([0xff; 16]),
                kind: FileKind::Regular,
                link_count: 1,
                metadata: root_metadata,
                payload: FilePayload::InlineRegular(InlineFileData::new(b"last")?),
            },
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let third =
        match ready!(checkout.commit(OperationId::new(), WorkBudget::UNBOUNDED, &cancellation,))
            .value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
            outcome => return Err(format!("unexpected ordered diff commit: {outcome:?}").into()),
        };
    for maximum in [1_u32, 2] {
        let bounded = ready!(volume.diff_generations(
            second,
            third,
            maximum,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ));
        assert!(bounded.value.truncated);
        assert_eq!(
            bounded.value.files.len() + bounded.value.bindings.len(),
            usize::try_from(maximum)?
        );
    }
    // Extend the same fixture with an identity-preserving rename, a hard link,
    // and directory metadata: none may become a remove/create approximation.
    let changed_id = ready!(checkout.lookup_no_follow(
        &path("changed")?, WorkBudget::UNBOUNDED, &cancellation,
    )).value.record.ok_or("changed record absent")?.file_id;
    ready!(checkout.hard_link(
        path("changed")?, path("alias")?, WorkBudget::UNBOUNDED, &cancellation,
    ));
    ready!(checkout.rename(
        path("changed")?, path("renamed")?, false, WorkBudget::UNBOUNDED, &cancellation,
    ));
    let mut metadata = ready!(checkout.lookup_no_follow_with_metadata(
        &root_path()?, WorkBudget::UNBOUNDED, &cancellation,
    )).value.ok_or("root metadata absent")?.metadata;
    metadata.modified_ns = MetadataField::Value(123);
    ready!(checkout.set_metadata(
        root_path()?, metadata, WorkBudget::UNBOUNDED, &cancellation,
    ));
    let fourth = match ready!(checkout.commit(
        OperationId::new(), WorkBudget::UNBOUNDED, &cancellation,
    )).value {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected semantic diff commit: {outcome:?}").into()),
    };
    let semantic = ready!(volume.diff_generations(
        third, fourth, 32, WorkBudget::UNBOUNDED, &cancellation,
    )).value;
    let file = semantic.files.iter().find(|change| change.file_id == changed_id)
        .ok_or("hard-link identity change absent")?;
    assert_eq!(file.before.ok_or("original record absent")?.file_id, changed_id);
    assert_eq!(file.after.ok_or("linked record absent")?.link_count, 2);
    assert!(semantic.files.iter().any(|change|
        change.after.is_some_and(|record| record.kind == FileKind::Directory && record.metadata == metadata)));
    assert_eq!(semantic.bindings.len(), 3);
    assert!(semantic.bindings.iter().any(|change|
        change.name.as_bytes() == b"changed" && change.before.is_some() && change.after.is_none()));
    for name in [b"alias".as_slice(), b"renamed".as_slice()] {
        assert!(semantic.bindings.iter().any(|change|
            change.name.as_bytes() == name && change.before.is_none()
                && change.after.as_ref().is_some_and(|entry| entry.file_id == changed_id)));
    }
    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    for (before, after) in [(initial, first), (first, second), (second, third), (third, fourth), (fourth, fourth)] {
        let before = ready!(volume.checkout(
            GenerationSelector::Exact(before), pinned(), WorkBudget::UNBOUNDED, &cancellation,
        )).value;
        let after = ready!(volume.checkout(
            GenerationSelector::Exact(after), pinned(), WorkBudget::UNBOUNDED, &cancellation,
        )).value;
        for maximum in [1, 2, 32] {
            let ordinary = ready!(before.candidate_diff(
                &after, maximum, WorkBudget::UNBOUNDED, &cancellation,
            )).value;
            let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, _)| cause)?;
            let baseline = original.live_allocation_bytes();
            let (native, allocation) = ready!(before.candidate_diff_with_native_work(
                &after, maximum, &original, &cancellation,
            ));
            assert_eq!(ordinary, native);
            let actual = (native.files.capacity() * size_of::<FileRecordChange>()
                + native.bindings.capacity() * size_of::<DirectoryBindingChange>()) as u64
                + native.bindings.iter().map(|change| {
                    change.name.retained_bytes() as u64
                        + change.before.as_ref().map_or(0, |entry| entry.name.retained_bytes() as u64)
                        + change.after.as_ref().map_or(0, |entry| entry.name.retained_bytes() as u64)
                }).sum::<u64>();
            assert_eq!(original.live_allocation_bytes() - baseline, actual);
            drop(native);
            drop(allocation);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        if before.root.file_table != after.root.file_table {
            for budget in [
                WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED },
                WorkBudget { bytes_copied: 0, ..WorkBudget::UNBOUNDED },
            ] {
                let denied = crate::NativeMaterializationWork::new(budget, &cancellation)
                    .map_err(|(cause, _)| cause)?;
                let baseline = denied.live_allocation_bytes();
                let failure = poll_ready(before.candidate_diff_with_native_work(
                    &after, 32, &denied, &cancellation,
                )).ok_or("native diff denial blocked")?.err().ok_or("native diff denial succeeded")?;
                assert!(matches!(failure, FsError::Work(_) | FsError::NativeKernel(_)));
                assert_eq!(denied.receipt().bytes_copied, 0);
                if budget.allocation_operations == 0 {
                    assert_eq!(denied.receipt().backend_read_operations, 0);
                }
                drop(failure);
                assert_eq!(denied.live_allocation_bytes(), baseline);
            }
        }
    }
    Ok(())
}

#[test]
fn three_way_merge_combines_independent_directory_bindings_and_publishes()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_ready($future).ok_or("filesystem future blocked")??
        };
    }
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume_with_id(
        VolumeId::from_bytes([74; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut ours = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut theirs = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(ours.create_file(
        path("ours")?,
        bytes::Bytes::from_static(b"ours"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(theirs.create_file(
        path("theirs")?,
        bytes::Bytes::from_static(b"theirs"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let theirs_generation = match ready!(theirs.commit(
        OperationId::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected branch commit: {outcome:?}").into()),
    };
    let prepared = ready!(ours.prepare_merge(
        theirs_generation,
        32,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(prepared.value, MergePreparation::Prepared { .. }));
    let mutation_during_merge =
        poll_ready(ours.mutate(Vec::new(), WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("prepared merge mutation guard blocked")?
            .err()
            .ok_or("prepared merge accepted a mutation")?;
    assert!(matches!(
        mutation_during_merge.error,
        FsError::PreparedMergePending
    ));
    assert!(
        poll_ready(ours.prepare_merge(
            theirs_generation,
            32,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("duplicate merge preparation blocked")?
        .is_err()
    );
    assert!(
        poll_ready(ours.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("prepared merge rebase blocked")?
            .is_err()
    );
    let merged_generation = match ready!(ours.commit(
        OperationId::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected merge commit: {outcome:?}").into()),
    };
    let mut merged = ready!(volume.checkout(
        GenerationSelector::Exact(merged_generation),
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    assert!(
        ready!(merged.lookup_no_follow(&path("ours")?, WorkBudget::UNBOUNDED, &cancellation,))
            .value
            .record
            .is_some()
    );
    assert!(
        ready!(merged.lookup_no_follow(&path("theirs")?, WorkBudget::UNBOUNDED, &cancellation,))
            .value
            .record
            .is_some()
    );
    assert_eq!(merged.root().parents.len(), 2);

    let mut left = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut right = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(left.hard_link(
        path("ours")?,
        path("left-link")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(right.hard_link(
        path("ours")?,
        path("right-link")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let right_generation =
        match ready!(right.commit(OperationId::new(), WorkBudget::UNBOUNDED, &cancellation,)).value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
            outcome => return Err(format!("unexpected hard-link commit: {outcome:?}").into()),
        };
    assert!(matches!(
        ready!(left.prepare_merge(
            right_generation,
            32,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        MergePreparation::Prepared { .. }
    ));
    let links_generation = match ready!(left.commit(
        OperationId::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected hard-link merge: {outcome:?}").into()),
    };
    let mut links = ready!(volume.checkout(
        GenerationSelector::Exact(links_generation),
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let source =
        ready!(links.lookup_no_follow(&path("ours")?, WorkBudget::UNBOUNDED, &cancellation,))
            .value
            .record
            .ok_or("merged hard-link source missing")?;
    assert_eq!(source.link_count, 3);
    for linked in ["left-link", "right-link"] {
        let record =
            ready!(links.lookup_no_follow(&path(linked)?, WorkBudget::UNBOUNDED, &cancellation,))
                .value
                .record
                .ok_or("merged hard link missing")?;
        assert_eq!(record.file_id, source.file_id);
    }
    Ok(())
}

#[test]
fn three_way_merge_rejects_guards_and_reports_exact_file_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_ready($future).ok_or("filesystem future blocked")??
        };
    }
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume_with_id(
        VolumeId::from_bytes([124; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut seed = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let file = path("conflict.txt")?;
    let file_id = ready!(seed.create_file(
        file.clone(),
        bytes::Bytes::from_static(b"base"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let second_file = path("second-conflict.txt")?;
    let second_file_id = ready!(seed.create_file(
        second_file.clone(),
        bytes::Bytes::from_static(b"base-two"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let base_generation = match ready!(seed.commit(
        OperationId::from_bytes([125; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected seed commit: {outcome:?}").into()),
    };
    let mut ours = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut theirs = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut read_only = ready!(volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    assert!(
        poll_ready(read_only.prepare_merge(
            base_generation,
            8,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("read-only merge preparation blocked")?
        .is_err()
    );
    assert!(
        poll_ready(
            ours.prepare_merge(base_generation, 0, 8, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("zero-change merge preparation blocked")?
        .is_err()
    );
    assert!(
        poll_ready(
            ours.prepare_merge(base_generation, 8, 0, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("zero-conflict merge preparation blocked")?
        .is_err()
    );
    ready!(ours.write_file(
        file.clone(),
        0,
        bytes::Bytes::from_static(b"ours"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(ours.write_file(
        second_file.clone(),
        0,
        bytes::Bytes::from_static(b"ours-two"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(theirs.write_file(
        file,
        0,
        bytes::Bytes::from_static(b"them"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(theirs.write_file(
        second_file,
        0,
        bytes::Bytes::from_static(b"them-two"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let theirs_generation = match ready!(theirs.commit(
        OperationId::from_bytes([126; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected competing commit: {outcome:?}").into()),
    };
    assert!(
        poll_ready(
            ours.prepare_merge(base_generation, 8, 8, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("stale merge parent check blocked")?
        .is_err()
    );
    let change_limited = poll_ready(ours.prepare_merge(
        theirs_generation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("change-limited merge preparation blocked")?
    .err()
    .ok_or("truncated merge inputs unexpectedly prepared")?;
    assert!(matches!(change_limited.error, FsError::MergeChangeLimit));
    assert!(change_limited.work.backend_read_operations > 0);
    let bounded_conflicts = ready!(ours.prepare_merge(
        theirs_generation,
        8,
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        bounded_conflicts.value,
        MergePreparation::Conflicted {
            ref conflicts,
            truncated: true,
        } if conflicts.len() == 1
            && matches!(conflicts[0], MergeConflict::File(id) if id == file_id || id == second_file_id)
    ));
    let conflicted = ready!(ours.prepare_merge(
        theirs_generation,
        8,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        conflicted.value,
        MergePreparation::Conflicted {
            ref conflicts,
            truncated: false,
        } if conflicts.len() == 2
            && conflicts.contains(&MergeConflict::File(file_id))
            && conflicts.contains(&MergeConflict::File(second_file_id))
    ));
    assert!(ours.has_pending_mutations());
    Ok(())
}

#[test]
fn directory_merge_helper_covers_scalar_binding_limit_and_invalid_diff_matrix()
-> Result<(), Box<dyn std::error::Error>> {
    let store = crate::memory::MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let limits = DecodeLimits::default();
    let directory_id = FileId::from_bytes([80; 16]);
    // Real stored metadata differing in authored state (the mode): the merge
    // reads metadata to tell activity timestamps from authored changes.
    let metadata = |byte: u8| -> ObjectId {
        let record = crate::kernel::FileMetadata {
            posix_mode: crate::kernel::MetadataField::Value(u32::from(byte)),
            ..crate::kernel::FileMetadata::default()
        };
        let bytes = OwnedBytes::from(crate::kernel::encode_file_metadata(record).expect("metadata"));
        let object_id = ObjectId {
            kind: ObjectKind::Metadata,
            digest: object_digest(ObjectKind::Metadata, &bytes),
        };
        ObjectStore::put(&store, object_id, bytes, WorkBudget::UNBOUNDED).expect("store metadata");
        object_id
    };
    let put_tree = |page: TreePage| -> Result<ObjectId, Box<dyn std::error::Error>> {
        let bytes = OwnedBytes::from(encode_tree_page(&page, 16)?);
        let object_id = ObjectId {
            kind: ObjectKind::TreePage,
            digest: object_digest(ObjectKind::TreePage, &bytes),
        };
        ObjectStore::put(&store, object_id, bytes, WorkBudget::UNBOUNDED)?;
        Ok(object_id)
    };
    let empty = put_tree(TreePage::Leaf(Vec::new()))?;
    let name = LogicalName::new(NameEncoding::Utf8, b"entry".to_vec(), 128)?;
    let entry = |byte| crate::kernel::TreeEntry {
        name: name.clone(),
        file_id: FileId::from_bytes([byte; 16]),
        kind: FileKind::Regular,
    };
    let base_entries = put_tree(TreePage::Leaf(vec![entry(81)]))?;
    let ours_entries = put_tree(TreePage::Leaf(vec![entry(82)]))?;
    let theirs_entries = put_tree(TreePage::Leaf(vec![entry(83)]))?;
    let directory = |entries, metadata, link_count| FileRecord {
        file_id: directory_id,
        kind: FileKind::Directory,
        link_count,
        metadata,
        payload: FilePayload::Directory { entries },
    };
    let base = directory(base_entries, metadata(84), 1);

    let scalar = poll_ready(merge_directory_record(
        &store,
        directory_id,
        base,
        directory(base_entries, metadata(85), 1),
        directory(base_entries, metadata(86), 1),
        8,
        1,
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("scalar directory merge blocked")??;
    assert_eq!(
        scalar.value.conflicts,
        vec![MergeConflict::File(directory_id)]
    );
    assert!(scalar.value.record.is_none());
    assert!(!scalar.value.truncated);

    let truncated_scalar = poll_ready(merge_directory_record(
        &store,
        directory_id,
        base,
        directory(base_entries, metadata(84), 2),
        directory(base_entries, metadata(84), 3),
        8,
        0,
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("truncated scalar directory merge blocked")??;
    assert!(truncated_scalar.value.conflicts.is_empty());
    assert!(truncated_scalar.value.record.is_none());
    assert!(truncated_scalar.value.truncated);

    let invalid = poll_ready(merge_directory_record(
        &store,
        directory_id,
        base,
        helper_record(directory_id, FilePayload::Empty, FileKind::Fifo),
        base,
        8,
        1,
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("invalid directory merge blocked")?
    .err()
    .ok_or("invalid directory payload merged")?;
    assert!(matches!(invalid.error, MergeGenerationError::InvalidDiff));

    let binding_conflict = poll_ready(merge_directory_record(
        &store,
        directory_id,
        base,
        directory(ours_entries, metadata(84), 1),
        directory(theirs_entries, metadata(84), 1),
        8,
        1,
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("binding-conflict directory merge blocked")??;
    assert_eq!(
        binding_conflict.value.conflicts,
        vec![MergeConflict::Binding {
            directory_id,
            name: name.clone(),
        }]
    );
    assert!(binding_conflict.value.record.is_none());
    assert!(!binding_conflict.value.truncated);

    let binding_truncated = poll_ready(merge_directory_record(
        &store,
        directory_id,
        base,
        directory(ours_entries, metadata(84), 1),
        directory(theirs_entries, metadata(84), 1),
        8,
        0,
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("truncated binding directory merge blocked")??;
    assert!(binding_truncated.value.conflicts.is_empty());
    assert!(binding_truncated.value.truncated);

    let unchanged = poll_ready(merge_directory_record(
        &store,
        directory_id,
        directory(empty, metadata(84), 1),
        directory(empty, metadata(84), 1),
        directory(empty, metadata(84), 1),
        8,
        1,
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("unchanged directory merge blocked")??;
    assert_eq!(
        unchanged
            .value
            .record
            .and_then(|record| directory_entries(Some(record))),
        Some(empty)
    );
    assert!(unchanged.value.conflicts.is_empty());

    let prior = WorkCounters {
        object_probes: 1,
        ..WorkCounters::default()
    };
    let invalid_root = poll_ready(diff_generation_file_tables(
        &store, empty, empty, 1, config(), prior, WorkBudget::UNBOUNDED, &cancellation,
    )).ok_or("invalid-root diff blocked")?.expect_err("a tree is not a file-table root");
    assert!(matches!(invalid_root.error, FsError::InvalidDiff));
    assert_eq!(invalid_root.work, prior, "pre-I/O rejection preserves the actual caller prefix");
    let table = ObjectId { kind: ObjectKind::FileTablePage, digest: empty.digest };
    let invalid_bound = poll_ready(diff_generation_file_tables(
        &store, table, table, 0, config(), prior, WorkBudget::UNBOUNDED, &cancellation,
    )).ok_or("zero-bound diff blocked")?.expect_err("the canonical engine rejects a zero bound");
    assert!(matches!(invalid_bound.error, FsError::InvalidDiff));
    assert_eq!(invalid_bound.work, prior);
    Ok(())
}

#[test]
fn merge_and_transfer_error_adapters_preserve_every_typed_class() {
    let prior = WorkCounters {
        object_probes: 1,
        ..WorkCounters::default()
    };
    let nested = WorkCounters {
        items_examined: 1,
        ..WorkCounters::default()
    };
    let mapped = [
        map_merge_failure(
            OperationFailure::new(MergeGenerationError::InvalidDiff, nested),
            prior,
        ),
        map_merge_failure(
            OperationFailure::new(MergeGenerationError::ChangeLimit, nested),
            prior,
        ),
        map_merge_failure(
            OperationFailure::new(MergeGenerationError::AllocationFailed, nested),
            prior,
        ),
        map_merge_failure(
            OperationFailure::new(
                MergeGenerationError::Object(ObjectStoreError::Corrupt),
                nested,
            ),
            prior,
        ),
        map_merge_failure(
            OperationFailure::new(
                MergeGenerationError::Decode(CanonicalDecodeError::TrailingBytes),
                nested,
            ),
            prior,
        ),
        map_merge_failure(
            OperationFailure::new(
                MergeGenerationError::FileTable(FileTableMutationError::Missing),
                nested,
            ),
            prior,
        ),
        map_merge_failure(
            OperationFailure::new(
                MergeGenerationError::Tree(TreeMutationError::Missing),
                nested,
            ),
            prior,
        ),
        map_merge_failure(
            OperationFailure::new(
                MergeGenerationError::Checkpoint(CheckpointError::DuplicateParent),
                nested,
            ),
            prior,
        ),
        map_merge_failure(
            OperationFailure::new(MergeGenerationError::Cancelled(CancellationError), nested),
            prior,
        ),
        map_merge_failure(
            OperationFailure::new(MergeGenerationError::Work(WorkError::Overflow), nested),
            prior,
        ),
    ];
    assert!(matches!(mapped[0].error, FsError::InvalidDiff));
    assert!(matches!(mapped[1].error, FsError::MergeChangeLimit));
    assert!(matches!(mapped[2].error, FsError::DiffAllocationFailed));
    assert!(matches!(mapped[3].error, FsError::Object(_)));
    assert!(matches!(mapped[4].error, FsError::Decode(_)));
    assert!(matches!(mapped[5].error, FsError::MergeFileTable(_)));
    assert!(matches!(mapped[6].error, FsError::MergeTree(_)));
    assert!(matches!(mapped[7].error, FsError::Checkpoint(_)));
    assert!(matches!(mapped[8].error, FsError::Cancelled(_)));
    assert!(matches!(mapped[9].error, FsError::Work(_)));
    assert!(
        mapped
            .iter()
            .all(|failure| failure.work.object_probes == 1 && failure.work.items_examined == 1)
    );
}

#[test]
fn transfer_error_adapter_preserves_every_typed_class() {
    let transfer = [
        map_transfer_error(GenerationTransferError::Cancelled(CancellationError)),
        map_transfer_error(GenerationTransferError::Manifest(
            GenerationExportManifestError::WrongRootKind,
        )),
        map_transfer_error(GenerationTransferError::ManifestMismatch),
        map_transfer_error(GenerationTransferError::Closure(
            ClosureError::WrongObjectKind,
        )),
        map_transfer_error(GenerationTransferError::Object(ObjectStoreError::Corrupt)),
        map_transfer_error(GenerationTransferError::Work(WorkError::Overflow)),
        map_transfer_error(GenerationTransferError::InvalidCursor),
        map_transfer_error(GenerationTransferError::EmptyBatch),
        map_transfer_error(GenerationTransferError::TooManyObjects),
        map_transfer_error(GenerationTransferError::AllocationFailed),
    ];
    assert!(matches!(transfer[0], FsError::Cancelled(_)));
    assert!(matches!(transfer[1], FsError::InvalidExportManifest));
    assert!(matches!(transfer[2], FsError::InvalidExportManifest));
    assert!(matches!(transfer[3], FsError::Closure(_)));
    assert!(matches!(transfer[4], FsError::Object(_)));
    assert!(matches!(transfer[5], FsError::Work(_)));
    assert!(
        transfer[6..]
            .iter()
            .all(|error| matches!(error, FsError::Transfer(_)))
    );
}

#[test]
fn memory_facade_exposes_shared_cache_control_without_alternate_storage_truth()
-> Result<(), Box<dyn std::error::Error>> {
    let objects = crate::CachedObjectStore::new(
        TestMemoryObjectStore::default(),
        crate::ObjectCacheOptions::default(),
    )?;
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        objects,
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("cached memory volume creation blocked")??
        .value;
    let checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cached memory checkout blocked")??
    .value;
    let root = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: checkout.generation_id().digest(),
    };

    fs.clear_object_cache()?;
    assert_eq!(fs.object_cache_stats()?.resident_entries, 0);
    let first = poll_ready(fs.export_object(
        root,
        config().limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cold cached export blocked")??;
    let warm = poll_ready(fs.clone().export_object(
        root,
        config().limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("warm cached export blocked")??;
    assert_eq!(first.value.bytes, warm.value.bytes);
    assert_eq!(first.work.backend_read_operations, 1);
    assert_eq!(warm.work.backend_read_operations, 0);
    let populated = fs.object_cache_stats()?;
    assert_eq!(populated.resident_entries, 1);
    assert!(populated.hits >= 1);
    fs.clear_object_cache()?;
    assert_eq!(fs.object_cache_stats()?.resident_entries, 0);
    Ok(())
}

#[test]
fn detached_inline_file_seek_and_attribute_absence_are_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("inline detached volume creation blocked")??
        .value;
    let file = path("inline-detached")?;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inline detached checkout blocked")??
    .value;
    poll_ready(checkout.create_file(
        file.clone(),
        bytes::Bytes::from_static(b"abc"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inline file creation blocked")??;
    let mut detached =
        poll_ready(checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("inline file detachment blocked")??
            .value;
    assert_eq!(detached.logical_bytes(), 3);
    for (offset, target, expected) in [
        (0, ExtentSeekTarget::Data, Some(0)),
        (3, ExtentSeekTarget::Data, None),
        (0, ExtentSeekTarget::Hole, Some(3)),
        (3, ExtentSeekTarget::Hole, Some(3)),
        (4, ExtentSeekTarget::Hole, None),
    ] {
        let receipt =
            poll_ready(detached.seek(offset, target, WorkBudget::UNBOUNDED, &cancellation))
                .ok_or("inline detached seek blocked")??;
        assert_eq!(receipt.value, expected);
        assert_eq!(receipt.work, WorkCounters::default());
    }

    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.inline".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    poll_ready(detached.write_named_attribute(
        attribute.clone(),
        OwnedBytes::from_static(b"value"),
        NamedAttributeWriteMode::Create,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inline detached attribute creation blocked")??;
    poll_ready(detached.remove_named_attribute(
        attribute.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("inline detached attribute removal blocked")??;
    let absent =
        poll_ready(detached.read_named_attribute(&attribute, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("inline detached absent attribute read blocked")??;
    assert_eq!(absent.value, None);

    detached.record.kind = FileKind::Fifo;
    detached.record.payload = FilePayload::Empty;
    assert_eq!(detached.logical_bytes(), 0);
    let rejected = poll_ready(detached.seek(
        0,
        ExtentSeekTarget::Data,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("non-regular detached seek blocked")?
    .err()
    .ok_or("non-regular detached seek succeeded")?;
    assert!(matches!(
        rejected.error,
        FsError::FileRead(FileRangeReadError::NotRegular)
    ));
    assert_eq!(*rejected.work, WorkCounters::default());
    Ok(())
}

#[test]
fn remaining_facade_guards_are_fast_typed_and_non_mutating()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(
        validate_checkout(
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::Live,
                mutations: MutationMode::PrivateOverlay,
            },
            serialized_config(),
        )
        .is_ok()
    );

    let prior = WorkCounters {
        bytes_copied: u64::MAX,
        peak_allocation_bytes: 1,
        ..WorkCounters::default()
    };
    let overflow = merge_simultaneous_failure(
        prior,
        WorkCounters {
            bytes_copied: 1,
            peak_allocation_bytes: 1,
            ..WorkCounters::default()
        },
        0,
        FsError::NotFound,
    );
    assert!(matches!(overflow.error, FsError::Work(WorkError::Overflow)));
    assert_eq!(*overflow.work, prior);

    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 1;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([211; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;

    let baseline = WorkCounters {
        object_probes: 1,
        ..WorkCounters::default()
    };
    assert_eq!(
        poll_ready(checkout.observe_base_metadata_ids(
            &[],
            baseline,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("empty metadata observation blocked")??,
        baseline
    );
    assert!(matches!(
        checkout.validate_profile_kind(FileKind::ReparsePoint, baseline),
        Err(OperationFailure {
            error: FsError::UnsupportedFileKind,
            ..
        })
    ));
    assert!(matches!(
        poll_ready(checkout.stage_blob(
            bytes::Bytes::from_static(b"xx"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("oversized blob guard blocked")?,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::InvalidRange),
            ..
        })
    ));

    poll_ready(checkout.create_directory(path("directory")?, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("directory creation blocked")??;
    assert!(matches!(
        poll_ready(checkout.detach_regular_file(
            &path("directory")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("directory detach guard blocked")?,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::NotRegular),
            ..
        })
    ));
    Ok(())
}

fn assert_authority_volume_identity_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let generation_root = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: Digest::from_bytes([221; 32]),
    };
    let expected_volume = VolumeId::from_bytes([222; 16]);
    let wrong_volume = VolumeId::from_bytes([223; 16]);
    let creation = DurableCommit {
        settled_at_micros: None,
        epoch: Epoch::new(1)?,
        sequence: Sequence::new(1),
        operation_id: OperationId::from_bytes([224; 16]),
        fingerprint: Digest::from_bytes([225; 32]),
        previous_digest: Digest::ZERO,
        digest: Digest::from_bytes([226; 32]),
        payload: bytes::Bytes::from(encode_volume_created(VolumeCreated {
            volume_id: wrong_volume,
            config: config(),
            initial_generation_root: generation_root,
        })?),
    };
    assert!(matches!(
        generation_from_record(&creation, expected_volume, WorkCounters::default()),
        Err(OperationFailure {
            error: FsError::VolumeMismatch,
            ..
        })
    ));
    let publication = DurableCommit {
        sequence: Sequence::new(2),
        payload: {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(b"acyclic-fs-publish-generation-v1\0");
            bytes.extend_from_slice(&1_u16.to_le_bytes());
            bytes.extend_from_slice(&wrong_volume.into_bytes());
            bytes.push(generation_root.kind.canonical_tag());
            bytes.extend_from_slice(generation_root.digest.as_bytes());
            bytes::Bytes::from(bytes)
        },
        ..creation
    };
    assert!(matches!(
        generation_from_record(&publication, expected_volume, WorkCounters::default()),
        Err(OperationFailure {
            error: FsError::VolumeMismatch,
            ..
        })
    ));

    let authority = Arc::new(crate::memory::MemoryAuthorityStore::default());
    let fs = Fs::new(
        Arc::clone(&authority),
        crate::memory::MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let expected_authority = volume_authority_id(expected_volume);
    let active = AuthorityStore::create_authority(
        &*authority,
        expected_authority,
        Epoch::GENESIS,
        WorkBudget::UNBOUNDED,
    )?;
    let head = match active.value {
        CreateAuthorityOutcome::Created(head) | CreateAuthorityOutcome::Existing(head) => head,
    };
    let encoded = crate::kernel::encode_volume_created_measured(VolumeCreated {
        volume_id: wrong_volume,
        config: config(),
        initial_generation_root: generation_root,
    }, WorkBudget::UNBOUNDED, &CancellationToken::new(), 0)?;
    let (proposal, _) = creation_commit(
        OperationId::from_bytes([227; 16]), encoded,
        WorkBudget::UNBOUNDED, &CancellationToken::new(),
    )?.value;
    AuthorityStore::compare_and_append(
        &*authority,
        expected_authority,
        head.epoch,
        Head::genesis(head.epoch),
        proposal,
        WorkBudget::UNBOUNDED,
    )?;
    let cancellation = CancellationToken::new();
    assert!(matches!(
        poll_ready(fs.read_creation(
            expected_volume,
            WorkCounters::default(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("authority identity replay blocked")?,
        Err(OperationFailure {
            error: FsError::VolumeMismatch,
            ..
        })
    ));

    let empty_volume = VolumeId::from_bytes([228; 16]);
    AuthorityStore::create_authority(
        &*authority,
        volume_authority_id(empty_volume),
        Epoch::GENESIS,
        WorkBudget::UNBOUNDED,
    )?;
    let empty = Volume {
        fs,
        id: empty_volume,
        config: config(),
    };
    assert!(matches!(
        poll_ready(empty.resolve_head_generation(WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("empty authority head blocked")?,
        Err(OperationFailure {
            error: FsError::EmptyAuthority,
            ..
        })
    ));
    Ok(())
}

#[test]
fn detached_payload_bounds_and_authority_volume_identity_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("volume creation blocked")??
        .value;
    let file = path("bounded-detached")?;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    poll_ready(checkout.create_file(
        file.clone(),
        bytes::Bytes::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("file creation blocked")??;
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.bounded".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    poll_ready(checkout.write_named_attribute(
        file.clone(),
        attribute.clone(),
        bytes::Bytes::from_static(b"xx"),
        NamedAttributeWriteMode::Create,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("attribute write blocked")??;
    let mut detached =
        poll_ready(checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("file detachment blocked")??
            .value;
    detached.volume.config.limits.maximum_read_bytes = 1;
    assert!(matches!(
        poll_ready(
            detached.read_named_attribute(&attribute, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .ok_or("bounded attribute read blocked")?,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::InvalidRange),
            ..
        })
    ));
    assert!(matches!(
        poll_ready(detached.stage_blob(
            bytes::Bytes::from_static(b"xx"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("bounded detached staging blocked")?,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::InvalidRange),
            ..
        })
    ));

    assert_authority_volume_identity_fails_closed()
}

// --- Case-folding / Unicode-normalization ---
//
// `CaseSensitivity::ProfileFolded` rejects same-directory siblings whose
// names collide once case-folded (`FsError::NameCollision`) and resolves
// lookups case-insensitively; `UnicodePolicy::RequireNfc` rejects any new
// name that is not canonical NFC (`FsError::NonNormalizedName`). Both are
// admitted at volume creation (pinned above).

fn folded_config() -> VolumeConfig {
    VolumeConfig {
        case_sensitivity: CaseSensitivity::ProfileFolded,
        ..config()
    }
}

fn nfc_config() -> VolumeConfig {
    VolumeConfig {
        unicode: UnicodePolicy::RequireNfc,
        ..config()
    }
}

#[test]
fn profile_folded_volumes_can_be_created_and_checked_out() -> Result<(), Box<dyn std::error::Error>>
{
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        poll_ready(fs.create_volume(folded_config(), WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("create blocked")??
            .value;
    poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??;
    Ok(())
}

#[test]
fn require_nfc_volumes_can_be_created_and_checked_out() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(nfc_config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("create blocked")??
        .value;
    poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??;
    Ok(())
}

#[test]
fn profile_folded_volume_rejects_case_only_collisions() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        poll_ready(fs.create_volume(folded_config(), WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("create blocked")??
            .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    poll_ready(checkout.create_file(
        path("Foo")?,
        bytes::Bytes::from_static(b"first"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first create blocked")??;
    let collision = poll_ready(checkout.create_file(
        path("foo")?,
        bytes::Bytes::from_static(b"second"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second create blocked")?;
    assert!(
        collision.is_err(),
        "creating \"foo\" must collide with the existing \"Foo\" under ProfileFolded"
    );
    Ok(())
}

#[test]
fn profile_folded_admission_covers_directories_links_and_renames()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        poll_ready(fs.create_volume(folded_config(), WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("create blocked")??
            .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;

    poll_ready(checkout.create_directory(path("Directory")?, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("directory create blocked")??;
    let directory_collision = poll_ready(checkout.create_directory(
        path("directory")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("directory collision blocked")?
    .err()
    .ok_or("directory collision unexpectedly succeeded")?;
    assert!(matches!(directory_collision.error, FsError::NameCollision));

    poll_ready(checkout.create_symbolic_link(
        path("Link")?,
        bytes::Bytes::from_static(b"target"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("link create blocked")??;
    let link_collision = poll_ready(checkout.create_symbolic_link(
        path("link")?,
        bytes::Bytes::from_static(b"other"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("link collision blocked")?
    .err()
    .ok_or("link collision unexpectedly succeeded")?;
    assert!(matches!(link_collision.error, FsError::NameCollision));

    let source = poll_ready(checkout.create_file(
        path("Source")?,
        bytes::Bytes::from_static(b"body"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("source create blocked")??
    .value;
    poll_ready(checkout.rename(
        path("Source")?,
        path("SOURCE")?,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("case-only rename blocked")??;
    let renamed = poll_ready(checkout.lookup_no_follow(
        &path("source")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("renamed lookup blocked")??
    .value
    .record
    .ok_or("renamed source absent")?;
    assert_eq!(renamed.file_id, source);

    poll_ready(checkout.create_file(
        path("Other")?,
        bytes::Bytes::from_static(b"other"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("other create blocked")??;
    let rename_collision = poll_ready(checkout.rename(
        path("SOURCE")?,
        path("other")?,
        false,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("rename collision blocked")?
    .err()
    .ok_or("rename collision unexpectedly succeeded")?;
    assert!(matches!(rename_collision.error, FsError::NameCollision));

    let link_destination_collision = poll_ready(checkout.hard_link(
        path("SOURCE")?,
        path("source")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("same-fold link blocked")?
    .err()
    .ok_or("same-fold link unexpectedly succeeded")?;
    assert!(matches!(
        link_destination_collision.error,
        FsError::NameCollision
    ));

    poll_ready(checkout.create_file(
        path("Removed")?,
        bytes::Bytes::from_static(b"removed"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("removed create blocked")??;
    poll_ready(checkout.remove(path("Removed")?, None, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("removed delete blocked")??;
    poll_ready(checkout.create_file(
        path("removed")?,
        bytes::Bytes::from_static(b"reused"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("reused folded name blocked")??;
    Ok(())
}

#[test]
fn profile_folded_authored_batches_reject_intra_batch_collisions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        poll_ready(fs.create_volume(folded_config(), WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("create blocked")??
            .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let failure = poll_ready(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateDirectory {
                path: path("Batch")?,
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateFile {
                path: path("batch")?,
                bytes: bytes::Bytes::from_static(b"body"),
                metadata: empty_metadata(),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("authored collision blocked")?
    .err()
    .ok_or("authored collision unexpectedly succeeded")?;
    assert!(matches!(failure.error, FsError::NameCollision));
    assert!(!checkout.has_pending_mutations());

    let ordered_aliases = poll_ready(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateFile {
                path: path("Source")?,
                bytes: OwnedBytes::from_static(b"source"),
                metadata: empty_metadata(),
            },
            AuthoredMutation::Rename {
                source: path("source")?,
                destination: path("Renamed")?,
                replace: false,
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("folded authored alias batch blocked")??;
    let created_id = ordered_aliases.value.created_file_ids[0].ok_or("created id missing")?;
    let renamed = poll_ready(checkout.lookup_no_follow(
        &path("renamed")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("folded authored alias lookup blocked")??;
    assert_eq!(
        renamed
            .value
            .record
            .ok_or("renamed record missing")?
            .file_id,
        created_id
    );

    let nested = poll_ready(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateDirectory {
                path: path("Nested")?,
                metadata: empty_metadata(),
            },
            AuthoredMutation::CreateFile {
                path: path_parts(&["nested", "Child"])?,
                bytes: OwnedBytes::from_static(b"nested"),
                metadata: empty_metadata(),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("nested folded authored batch blocked")??;
    assert_eq!(nested.value.created_file_ids.len(), 2);
    let nested_read = poll_ready(checkout.read_file_range(
        &path_parts(&["NESTED", "child"])?,
        ByteRange {
            offset: 0,
            length: 6,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("nested folded lookup blocked")??;
    assert_eq!(nested_read.value.bytes.as_ref(), b"nested");
    Ok(())
}

#[test]
fn sensitive_volume_still_permits_case_distinct_siblings() -> Result<(), Box<dyn std::error::Error>>
{
    // Control: default `Sensitive` behavior must not regress while the
    // ProfileFolded tests above are red.
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("create blocked")??
        .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    poll_ready(checkout.create_file(
        path("Foo")?,
        bytes::Bytes::from_static(b"first"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first create blocked")??;
    poll_ready(checkout.create_file(
        path("foo")?,
        bytes::Bytes::from_static(b"second"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second create blocked")??;
    Ok(())
}

#[test]
fn profile_folded_volume_resolves_case_insensitive_lookups()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        poll_ready(fs.create_volume(folded_config(), WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("create blocked")??
            .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let created = poll_ready(checkout.create_file(
        path("foo")?,
        bytes::Bytes::from_static(b"body"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("create blocked")??
    .value;
    let lookup =
        poll_ready(checkout.lookup_no_follow(&path("FOO")?, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("lookup blocked")??
            .value;
    assert_eq!(
        lookup.record.map(|record| record.file_id),
        Some(created),
        "a folded lookup of \"FOO\" must resolve the entry created as \"foo\""
    );
    Ok(())
}

#[test]
fn require_nfc_volume_rejects_non_normalized_names() -> Result<(), Box<dyn std::error::Error>> {
    // "Ã©" spelled two ways: precomposed NFC (U+00E9) and decomposed NFD
    // ('e' U+0065 followed by combining acute accent U+0301). Both encode to
    // the same visible grapheme; only the NFC form is canonical.
    let precomposed = "\u{00e9}";
    let decomposed = "e\u{0301}";
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume(nfc_config(), WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("create blocked")??
        .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let rejected = poll_ready(checkout.create_file(
        path(decomposed)?,
        bytes::Bytes::from_static(b"body"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("nfd create blocked")?;
    assert!(
        rejected.is_err(),
        "a non-normalized (NFD) name must be rejected under RequireNfc"
    );
    poll_ready(checkout.create_file(
        path(precomposed)?,
        bytes::Bytes::from_static(b"body"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("nfc create blocked")??;
    Ok(())
}

#[test]
fn stamped_content_change_equals_the_change_then_its_stamp_in_one_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([131; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let timed = path("timed")?;
    let untimed = path("untimed")?;
    let mut seed = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed checkout blocked")??
    .value;
    let file_id = poll_ready(seed.create_file(
        timed.clone(),
        bytes::Bytes::from(vec![b'a'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed create blocked")??
    .value;
    poll_ready(seed.create_file(
        untimed.clone(),
        bytes::Bytes::from(vec![b'b'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("untimed create blocked")??;
    let times = FileMetadata {
        modified_ns: MetadataField::Value(1),
        accessed_ns: MetadataField::Value(2),
        changed_ns: MetadataField::Value(3),
        ..FileMetadata::default()
    };
    poll_ready(seed.set_metadata_by_id(file_id, times, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("seed metadata blocked")??;
    poll_ready(seed.commit(
        OperationId::from_bytes([132; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed commit blocked")??;

    let mut stamped = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("stamped checkout blocked")??
    .value;
    let mut separate = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("separate checkout blocked")??
    .value;
    poll_ready(stamped.change_content_by_id(
        file_id,
        ContentChange::Write { offset: 4, bytes: OwnedBytes::from_static(b"STAMP") },
        ContentTimes::Stamp(99),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("stamped write blocked")??;
    poll_ready(separate.write_file_by_id(
        file_id,
        4,
        bytes::Bytes::from_static(b"STAMP"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("separate write blocked")??;
    let mut expected =
        poll_ready(separate.read_metadata_by_id(file_id, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("separate metadata read blocked")??
            .value;
    assert_eq!(expected, times, "a preserving write leaves metadata intact");
    assert!(expected.stamp_content_change(99));
    poll_ready(separate.set_metadata_by_id(
        file_id,
        expected,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("separate stamp blocked")??;
    assert_eq!(stamped.root(), separate.root());
    let metadata =
        poll_ready(stamped.read_metadata_by_id(file_id, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("stamped metadata read blocked")??
            .value;
    assert_eq!(metadata.modified_ns, MetadataField::Value(99));
    assert_eq!(metadata.changed_ns, MetadataField::Value(99));
    assert_eq!(metadata.accessed_ns, MetadataField::Value(2));

    // The change and its stamp are one mutation: a change that fails stamps nothing.
    let before = stamped.root().clone();
    assert!(
        poll_ready(stamped.change_content_by_id(
            file_id,
            ContentChange::CloneFrom {
                source: FileId::from_bytes([200; 16]),
                source_offset: 0,
                offset: 0,
                length: 1,
            },
            ContentTimes::Stamp(100),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("failing stamped clone blocked")?
        .is_err()
    );
    assert_eq!(stamped.root(), &before);

    // A profile that represents no content times keeps its metadata object.
    let untimed_before =
        poll_ready(stamped.read_metadata(&untimed, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("untimed metadata read blocked")??
            .value;
    poll_ready(stamped.change_content(
        untimed.clone(),
        ContentChange::Resize { logical_bytes: 7 },
        ContentTimes::Stamp(101),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("untimed resize blocked")??;
    let untimed_after =
        poll_ready(stamped.read_metadata(&untimed, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("untimed metadata reread blocked")??
            .value;
    assert_eq!(untimed_after, untimed_before);
    Ok(())
}

#[test]
fn grouped_changes_land_together_and_each_keeps_its_own_result()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([141; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let existing = poll_ready(checkout.create_file(
        path("existing")?,
        bytes::Bytes::from(vec![b'a'; 128]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed create blocked")??
    .value;
    let content = |bytes: &'static [u8]| GroupedChange::Content {
        file_id: existing,
        change: ContentChange::Write { offset: 0, bytes: OwnedBytes::from_static(bytes) },
        times: ContentTimes::Stamp(7),
    };
    let create = |name: &str| -> Result<GroupedChange, Box<dyn std::error::Error>> {
        Ok(GroupedChange::CreateFile {
            path: path(name)?,
            metadata: Box::new(FileMetadata::default()),
            file_id: FileId::new(),
            bytes: bytes::Bytes::new(),
        })
    };

    // Every change applies: one mutation, one result each.
    let applied = poll_ready(checkout.apply_group(
        vec![create("first")?, content(b"ONE"), create("second")?],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("group blocked")??
    .value;
    let [
        Ok(GroupedOutcome::Created(first)),
        Ok(GroupedOutcome::Changed),
        Ok(GroupedOutcome::Created(second)),
    ] = applied.as_slice()
    else {
        return Err(format!("unexpected grouped outcomes {applied:?}").into());
    };
    assert_ne!(first, second);

    // A failing change fails alone; its neighbours still apply in order.
    let mixed = poll_ready(checkout.apply_group(
        vec![
            create("third")?,
            create("first")?,
            GroupedChange::Content {
                file_id: FileId::from_bytes([142; 16]),
                change: ContentChange::Resize { logical_bytes: 1 },
                times: ContentTimes::Stamp(8),
            },
            content(b"TWO"),
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("mixed group blocked")??
    .value;
    assert!(matches!(mixed[0], Ok(GroupedOutcome::Created(_))));
    assert!(
        mixed[1].is_err(),
        "an existing name cannot be created again"
    );
    assert!(mixed[2].is_err(), "an absent identity cannot change");
    assert!(matches!(mixed[3], Ok(GroupedOutcome::Changed)));

    for name in ["existing", "first", "second", "third"] {
        assert!(
            poll_ready(checkout.lookup_no_follow(
                &path(name)?,
                WorkBudget::UNBOUNDED,
                &cancellation
            ))
            .ok_or("lookup blocked")??
            .value
            .record
            .is_some(),
            "{name} is bound"
        );
    }
    let read = poll_ready(checkout.read_file_range_by_id(
        existing,
        ByteRange {
            offset: 0,
            length: 4,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("read blocked")??
    .value;
    assert_eq!(read.bytes.as_ref(), b"TWOa");
    // A real streamed source still completes before ordered retry, even when
    // its neighbour makes the combined transaction fail.
    let streamed_id = FileId::new();
    let streamed = poll_ready(checkout.apply_group(
        vec![
            GroupedChange::CreateFile {
                path: path("streamed")?,
                metadata: Box::new(FileMetadata::default()),
                file_id: streamed_id,
                bytes: OwnedBytes::from(vec![b's'; 4096]),
            },
            create("first")?,
            content(b"END"),
            content(b"LAST"),
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).ok_or("streamed mixed group blocked")??.value;
    assert!(matches!(streamed[0], Ok(GroupedOutcome::Created(id)) if id == streamed_id));
    assert!(streamed[1].is_err());
    assert!(matches!(streamed[2], Ok(GroupedOutcome::Changed)));
    assert!(matches!(streamed[3], Ok(GroupedOutcome::Changed)));
    let streamed_read = poll_ready(checkout.read_file_range_by_id(
        streamed_id, ByteRange { offset: 0, length: 4096 },
        WorkBudget::UNBOUNDED, &cancellation,
    )).ok_or("streamed read blocked")??.value;
    assert_eq!(streamed_read.bytes.as_ref(), vec![b's'; 4096].as_slice());
    let ordered_read = poll_ready(checkout.read_file_range_by_id(
        existing, ByteRange { offset: 0, length: 4 },
        WorkBudget::UNBOUNDED, &cancellation,
    )).ok_or("ordered read blocked")??.value;
    assert_eq!(ordered_read.bytes.as_ref(), b"LAST");

    let denied = poll_ready(checkout.apply_group(
        vec![create("denied-group")?],
        WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED },
        &cancellation,
    )).ok_or("denied group blocked")?;
    assert!(denied.is_err(), "output admission must precede effects");
    assert!(poll_ready(checkout.lookup_no_follow(
        &path("denied-group")?, WorkBudget::UNBOUNDED, &cancellation,
    )).ok_or("denied lookup blocked")??.value.record.is_none());
    Ok(())
}

#[test]
fn repeated_lookups_observe_the_base_once_and_still_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([151; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let mut local = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("local checkout blocked")??
    .value;
    let mut remote = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote checkout blocked")??
    .value;
    poll_ready(local.create_file(
        path("authored")?,
        bytes::Bytes::from_static(b"local"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("local create blocked")??;

    let observed = path("observed")?;
    let first = poll_ready(local.lookup_no_follow_with_metadata(
        &observed,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first lookup blocked")??;
    assert!(first.value.is_none());
    let proof = local.dependencies.proof().dependencies.clone();
    let second = poll_ready(local.lookup_no_follow_with_metadata(
        &observed,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second lookup blocked")??;
    assert!(second.value.is_none());
    assert_eq!(
        local.dependencies.proof().dependencies,
        proof,
        "repeating an observation leaves the proof unchanged"
    );
    assert!(
        second.work.page_reads < first.work.page_reads,
        "the repeated lookup walks only the candidate"
    );

    // The observation absorbed once still fences a racing binding.
    poll_ready(remote.create_file(
        observed,
        bytes::Bytes::from_static(b"remote"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote create blocked")??;
    poll_ready(remote.commit(
        OperationId::from_bytes([152; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("remote commit blocked")??;
    let rebase = poll_ready(local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
        .ok_or("rebase blocked")??;
    assert!(matches!(rebase.value, RebaseDecision::Conflicted { .. }));
    Ok(())
}

#[test]
fn batch_lookups_observe_every_path_in_one_walk_and_still_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    // Once with an unchanged candidate, whose one walk both answers and
    // observes, and once with a private change, which observes the base.
    for (seed, authored) in [(171_u8, false), (181, true)] {
        let fs = Fs::memory();
        let cancellation = CancellationToken::new();
        let volume = poll_ready(fs.create_volume_with_id(
            VolumeId::from_bytes([seed; 16]),
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("volume creation blocked")??
        .value;
        let mut local = poll_ready(volume.checkout(
            GenerationSelector::Head,
            writable_tracking(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("local checkout blocked")??
        .value;
        let mut remote = poll_ready(volume.checkout(
            GenerationSelector::Head,
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("remote checkout blocked")??
        .value;
        if authored {
            poll_ready(local.create_file(
                path("authored")?,
                bytes::Bytes::from_static(b"local"),
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .ok_or("local create blocked")??;
        }
        let paths = [path("first")?, path("second")?];
        let batch =
            poll_ready(local.lookup_batch_no_follow(&paths, WorkBudget::UNBOUNDED, &cancellation))
                .ok_or("batch lookup blocked")??;
        assert!(
            batch
                .value
                .entries
                .iter()
                .all(|entry| entry.record.is_none())
        );
        let proof = local.dependencies.proof().dependencies.clone();
        poll_ready(local.lookup_batch_no_follow(&paths, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("repeated batch lookup blocked")??;
        assert_eq!(local.dependencies.proof().dependencies, proof);

        poll_ready(remote.create_file(
            path("second")?,
            bytes::Bytes::from_static(b"remote"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("remote create blocked")??;
        poll_ready(remote.commit(
            OperationId::from_bytes([seed.wrapping_add(1); 16]),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("remote commit blocked")??;
        let rebase = poll_ready(local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("rebase blocked")??;
        assert!(matches!(rebase.value, RebaseDecision::Conflicted { .. }));
    }
    Ok(())
}

#[test]
fn one_reader_answers_a_batch_of_identities() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([191; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let mut writer = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let mut identities = Vec::new();
    for name in ["a", "b"] {
        poll_ready(writer.create_file(
            path(name)?,
            bytes::Bytes::from_static(b"x"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("create blocked")??;
        let record =
            poll_ready(writer.lookup_no_follow(&path(name)?, WorkBudget::UNBOUNDED, &cancellation))
                .ok_or("lookup blocked")??
                .value
                .record
                .ok_or("created file missing")?;
        identities.push(record);
    }
    let absent = FileId::from_bytes([192; 16]);
    let reader = writer.pinned_reader()?;
    assert_eq!(reader.generation_id(), writer.generation_id());
    let records = poll_ready(reader.file_records_by_id(
        &[identities[1].file_id, absent, identities[0].file_id],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("identity batch blocked")??;
    assert_eq!(
        records.value,
        vec![Some(identities[1]), None, Some(identities[0])]
    );
    Ok(())
}

#[test]
fn each_grouped_change_has_its_own_budget() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([161; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let mut checkout = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value;
    let mut files = Vec::new();
    for (index, name) in ["one", "two", "three", "four"].into_iter().enumerate() {
        let file_id = poll_ready(checkout.create_file(
            path(name)?,
            bytes::Bytes::from(vec![u8::try_from(index)?; 16]),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("seed create blocked")??
        .value;
        files.push(file_id);
    }
    let write = |file_id: FileId, fill: u8| GroupedChange::Content {
        file_id,
        change: ContentChange::Write { offset: 0, bytes: OwnedBytes::from(vec![fill; 4_096]) },
        times: ContentTimes::Preserve,
    };
    let single = poll_ready(checkout.apply_group(
        vec![write(files[0], 1)],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("single group blocked")??;
    assert!(single.value.iter().all(Result::is_ok));
    // Enough for any one change, far from enough for three sharing it.
    let mut budget = WorkBudget::UNBOUNDED;
    budget.bytes_hashed = single.work.bytes_hashed * 3 / 2;
    let group = poll_ready(checkout.apply_group(
        vec![write(files[1], 2), write(files[2], 3), write(files[3], 4)],
        budget,
        &cancellation,
    ))
    .ok_or("group blocked")??;
    assert!(
        group.value.iter().all(Result::is_ok),
        "a change failed on its neighbours' work: {:?}",
        group.value
    );
    assert!(group.work.bytes_hashed > budget.bytes_hashed);
    Ok(())
}

/// Rewriting an existing file whole (truncate, extend, write from staged content, the shape
/// native capture gives every changed regular file) depends on the file's complete record
/// through its `Resize`. Its content-range dependencies on the old bytes add nothing to that
/// proof, and when the file is larger than `maximum_read_bytes` they cannot be captured at all:
/// every later publication of a workspace holding a file over 16 MiB failed with
/// `ContentRangeTooLarge`.
#[test]
fn a_whole_file_rewrite_larger_than_the_read_bound_is_admitted()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 8;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([231; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let file = path("large")?;
    let stage = |checkout: &Checkout<_, _>, body: &'static [u8]| {
        let mut source = std::io::Cursor::new(OwnedBytes::from_static(body));
        poll_ready(checkout.stage_content(&mut source, 64, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("staging blocked")?
            .map(|staged| staged.value)
            .map_err(|failure| format!("staging failed: {:?}", failure.error))
    };

    let mut first = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first checkout blocked")??
    .value;
    let original = stage(&first, b"0123456789abcdef0123456789abcdef")?;
    poll_ready(first.apply_authored_transaction(
        vec![AuthoredMutation::CreateFileFromContent {
            path: file.clone(),
            content: original,
            metadata: empty_metadata(),
            file_id: None,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("create blocked")??;
    poll_ready(first.commit(
        OperationId::from_bytes([231; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first commit blocked")??;

    let mut second = poll_ready(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second checkout blocked")??
    .value;
    let replacement = stage(&second, b"ZYXWVUTSRQPONMLKzyxwvutsrqponmlk")?;
    poll_ready(second.apply_authored_transaction(
        vec![
            AuthoredMutation::Resize {
                path: file.clone(),
                logical_bytes: 0,
            },
            AuthoredMutation::Resize {
                path: file.clone(),
                logical_bytes: 32,
            },
            AuthoredMutation::WriteFromContent {
                path: file.clone(),
                offset: 0,
                content: replacement,
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("rewrite blocked")?
    .map_err(|failure| format!("whole-file rewrite refused: {:?}", failure.error))?;
    poll_ready(second.commit(
        OperationId::from_bytes([232; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second commit blocked")??;
    let head = poll_ready(second.read_file_range(
        &file,
        ByteRange {
            offset: 24,
            length: 8,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("read blocked")??
    .value;
    assert_eq!(head.bytes.as_ref(), b"rqponmlk");
    Ok(())
}

/// Dropping a rewritten file's byte-range dependencies keeps its record dependency, so a whole
/// rewrite planned against a stale base still conflicts with a rewrite another checkout
/// committed first, rather than silently replacing it.
#[test]
fn a_stale_whole_file_rewrite_still_conflicts() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 8;
    let volume = poll_ready(fs.create_volume_with_id(
        VolumeId::from_bytes([233; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    let file = path("large")?;
    let checkout = || {
        poll_ready(volume.checkout(
            GenerationSelector::Head,
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("checkout blocked")?
        .map(|checkout| checkout.value)
        .map_err(|failure| format!("checkout failed: {:?}", failure.error))
    };
    let stage = |checkout: &Checkout<_, _>, body: &'static [u8]| {
        let mut source = std::io::Cursor::new(OwnedBytes::from_static(body));
        poll_ready(checkout.stage_content(&mut source, 64, WorkBudget::UNBOUNDED, &cancellation))
            .ok_or("staging blocked")?
            .map(|staged| staged.value)
            .map_err(|failure| format!("staging failed: {:?}", failure.error))
    };
    let rewrite = |checkout: &mut Checkout<_, _>, body: &'static [u8]| {
        let content = stage(checkout, body)?;
        poll_ready(checkout.apply_authored_transaction(
            vec![
                AuthoredMutation::Resize {
                    path: file.clone(),
                    logical_bytes: 0,
                },
                AuthoredMutation::Resize {
                    path: file.clone(),
                    logical_bytes: 32,
                },
                AuthoredMutation::WriteFromContent {
                    path: file.clone(),
                    offset: 0,
                    content,
                },
            ],
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("rewrite blocked")?
        .map(|_| ())
        .map_err(|failure| format!("rewrite refused: {:?}", failure.error))
    };

    let mut seed = checkout()?;
    let original = stage(&seed, b"0123456789abcdef0123456789abcdef")?;
    poll_ready(seed.apply_authored_transaction(
        vec![AuthoredMutation::CreateFileFromContent {
            path: file.clone(),
            content: original,
            metadata: empty_metadata(),
            file_id: None,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("create blocked")??;
    poll_ready(seed.commit(
        OperationId::from_bytes([233; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("seed commit blocked")??;

    let mut first = checkout()?;
    let mut stale = checkout()?;
    rewrite(&mut first, b"FIRSTFIRSTFIRSTFIRSTFIRSTFIRST12")?;
    let committed = poll_ready(first.commit(
        OperationId::from_bytes([234; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("first commit blocked")??
    .value;
    assert!(
        matches!(committed, CheckoutCommitOutcome::Committed { .. }),
        "{committed:?}"
    );
    rewrite(&mut stale, b"STALESTALESTALESTALESTALESTALE12")?;
    let refused = poll_ready(stale.commit(
        OperationId::from_bytes([235; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("stale commit blocked")??
    .value;
    assert!(
        matches!(refused, CheckoutCommitOutcome::Conflict { .. }),
        "{refused:?}"
    );
    Ok(())
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test]
async fn local_open_records_its_owned_outcome_even_with_filtered_children()
-> Result<(), Box<dyn std::error::Error>> {
    use tracing::{Instrument as _, instrument::WithSubscriber as _};
    use tracing_subscriber::{Layer as _, layer::SubscriberExt as _};
    let _second = tracing::Dispatch::new(tracing_subscriber::registry());
    let directory = tempfile::tempdir()?;
    let invalid = directory.path().join("private-root-sentinel");
    std::fs::write(&invalid, b"private-body-sentinel")?;
    for filtered in [false, true] {
        let captured = spans::Captured::default();
        let capture = spans::Capture(Arc::clone(&captured), std::sync::Mutex::default());
        let filter = tracing_subscriber::filter::filter_fn(move |metadata| {
            metadata.name() == "local_parent"
                || (!filtered && metadata.name() == "acyclic.fs.local")
        });
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::Registry::default().with(capture.with_filter(filter)),
        );
        async {
            let parent = tracing::info_span!("local_parent", outcome = "unchanged");
            async {
                let fs = Fs::local(LocalOptions::new(directory.path().join("valid"))).await?;
                let alias = Fs::local(LocalOptions::new(directory.path().join("valid"))).await?;
                assert!(fs.same_deployment(&alias));
                let failed = Fs::local(LocalOptions::new(&invalid)).await;
                assert!(matches!(failed, Err(FsError::LocalRoot(_))));
                Ok::<_, Box<dyn std::error::Error>>(())
            }
            .instrument(parent)
            .await
        }
        .with_subscriber(dispatch)
        .await?;
        let captured = captured.lock().unwrap();
        let parent = captured
            .iter()
            .find(|(name, _)| *name == "local_parent")
            .unwrap();
        assert_eq!(
            parent
                .1
                .iter()
                .filter(|(field, _)| *field == "outcome")
                .count(),
            1,
            "{captured:?}"
        );
        let operations: Vec<_> = captured
            .iter()
            .filter(|(name, _)| *name == "acyclic.fs.local")
            .collect();
        assert_eq!(operations.len(), if filtered { 0 } else { 3 });
        if !filtered {
            assert!(operations[0].1.contains(&("outcome", "ok".into())));
            assert!(operations[1].1.contains(&("outcome", "ok".into())));
            assert!(operations[2].1.contains(&("outcome", "err".into())));
            assert!(
                operations[2]
                    .1
                    .contains(&("error.kind", "LocalRoot".into()))
            );
        }
        assert!(
            captured
                .iter()
                .all(|(_, fields)| fields.iter().all(|(_, value)| !value.contains("private-")))
        );
    }
    Ok(())
}

#[tokio::test]
async fn readonly_directory_records_share_the_finite_and_unbounded_engine()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("readonly-directory-finite-engine").await?;
    workspace.write_text("/first", "one").await?;
    workspace.write_text("/second", "two").await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace.checkout_measured(
        GenerationSelector::Head, CheckoutMode::read_only_pinned(),
        WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let root = root_path()?;
    // Warm authenticated immutable-page caches before comparing exact receipts.
    checkout.list_directory_records(
        &root, None, 8, WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    let ordinary = checkout.list_directory_records(
        &root, None, 8, WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    let finite = checkout.list_directory_records(
        &root, None, 8,
        WorkBudget {
            items_returned: ordinary.work.items_returned + 8 - ordinary.value.entries.len() as u64,
            ..ordinary.work
        },
        &cancellation,
    ).await?;
    assert_eq!(finite.value, ordinary.value);
    assert_eq!(finite.work, ordinary.work);
    assert!(ordinary.work.allocation_operations > 0);
    // Every allocation cutoff includes the named-record, request, and final
    // output admissions, not merely the outer heap-future allocation.
    for maximum in 0..ordinary.work.allocation_operations {
        let failure = checkout.list_directory_records(
            &root, None, 8,
            WorkBudget { allocation_operations: maximum, ..WorkBudget::UNBOUNDED },
            &cancellation,
        ).await.err().ok_or("finite list bypassed a vector admission")?;
        assert!(failure.work.allocation_operations <= maximum);
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = checkout.list_directory_records(
        &root, None, 8, WorkBudget::UNBOUNDED, &cancelled,
    ).await.err().ok_or("cancelled list unexpectedly succeeded")?;
    assert!(matches!(failure.error, FsError::Cancelled(_)));
    let after_failure = checkout.list_directory_records(
        &root, None, 8, WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    assert_eq!(after_failure.value, ordinary.value);
    Ok(())
}

#[tokio::test]
async fn selected_fork_invalid_native_paths_preflight_before_allocation_and_reads()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::workspace::WorkspaceError;
    let fs = Fs::memory();
    let workspace = fs.create_workspace("selected-fork-invalid-paths").await?;
    workspace.write_text("/file", "body").await?;
    let source = workspace.head().await?;
    let cancellation = CancellationToken::new();
    let checkout = workspace.checkout_measured(
        GenerationSelector::Exact(source.id()), CheckoutMode::read_only_pinned(),
        WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let limits = source.workspace.volume.config().limits;
    for invalid in [
        "/../file".to_owned(),
        "/bad\0name".to_owned(),
        format!("/{}", "x".repeat(limits.maximum_component_bytes as usize + 1)),
        format!("/{}", "a/".repeat(limits.maximum_path_depth as usize + 1)),
        format!("/{}", "x".repeat(limits.maximum_path_bytes as usize + 1)),
    ] {
        let paths = vec![invalid];
        let owned = (paths.capacity() * std::mem::size_of::<String>()
            + paths[0].capacity()) as u64;
        let failure = fs.select_fork_paths(
            &source, checkout.root(), paths,
            WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).await.err().ok_or("invalid selected path unexpectedly succeeded")?;
        assert!(matches!(failure.error, WorkspaceError::NamespacePath(_)));
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(failure.work.backend_read_operations, 0);
        assert_eq!(failure.work.page_reads, 0);
        assert_eq!(failure.work.bytes_copied, 0);
        assert_eq!(failure.work.peak_allocation_bytes, owned);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn selected_fork_original_native_matches_frozen_source_and_keeps_failed_work()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::allocation::KernelAccounting;
    let fs = Fs::memory();
    let workspace = fs.create_workspace("selected-fork-original-frozen").await?;
    let mut transaction = workspace.begin_transaction(crate::IdempotencyKey::new()).await?;
    transaction.create_dir_all("/tree/nested").await?;
    transaction.write_text("/tree/nested/file", "frozen body").await?;
    transaction.hard_link("/tree/nested/file", "/tree/nested/alias").await?;
    transaction.hard_link("/tree/nested/file", "/outside-alias").await?;
    transaction.write_text("/omitted", "old omitted body").await?;
    transaction.commit().await?;
    let source = workspace.head().await?;
    let cancellation = CancellationToken::new();
    let checkout = workspace.checkout_measured(
        GenerationSelector::Exact(source.id()), CheckoutMode::read_only_pinned(),
        WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let source_table = checkout.root().file_table;
    let source_id = source.id();
    // Selection must authenticate the supplied frozen generation, not current head.
    workspace.write_text("/tree/nested/file", "replacement at current head").await?;
    workspace.write_text("/new-at-head", "not in the frozen source").await?;
    for paths in [
        vec!["/tree".to_owned(), "/tree/nested/file".to_owned(), "/tree".to_owned()],
        vec!["/tree/nested/file".to_owned()],
        vec!["/outside-alias".to_owned(), "/tree/nested/alias".to_owned()],
        Vec::new(),
        vec!["/tree".to_owned(), "/".to_owned()],
    ] {
        let expected = fs.select_fork_paths(
            &source, checkout.root(), paths.clone(), WorkBudget::UNBOUNDED, &cancellation,
        ).await?.0;
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let actual = fs.select_fork_paths_with_accounting(
            &source, checkout.root(), paths, &mut accounting, &cancellation,
        ).await?;
        assert_eq!(actual, expected, "the original operation uses the same canonical closure");
        assert_eq!(source.id(), source_id);
        assert_eq!(checkout.root().file_table, source_table);
    }
    assert_eq!((source.read("/tree/nested/file", 64).await?).as_ref(), b"frozen body");
    assert!(source.read("/new-at-head", 64).await.is_err());

    let denied = crate::NativeMaterializationWork::new(
        WorkBudget { bytes_encoded: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).map_err(|(cause, _)| cause)?;
    let mut accounting = KernelAccounting::native(&denied, &cancellation);
    let failed = fs.select_fork_paths_with_accounting(
        &source, checkout.root(), vec!["/tree/nested/file".to_owned()],
        &mut accounting, &cancellation,
    ).await.err().ok_or("native selection ignored its original encoding ceiling")?;
    assert!(matches!(failed, crate::workspace::WorkspaceError::Work(_)));
    let spent = denied.receipt();
    assert!(spent.allocation_operations > 0);
    assert!(spent.backend_read_operations > 0);
    assert_eq!(spent.bytes_encoded, 0);
    assert_eq!(spent.backend_write_operations, 0);
    assert_eq!(checkout.root().file_table, source_table);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn selected_fork_original_root_transfer_adopts_capacity_once_without_new_work()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::allocation::KernelAccounting;
    let fs = Fs::memory();
    let workspace = fs.create_workspace("selected-fork-original-root-transfer").await?;
    let source = workspace.head().await?;
    let cancellation = CancellationToken::new();
    let checkout = workspace.checkout_measured(
        GenerationSelector::Exact(source.id()), CheckoutMode::read_only_pinned(),
        WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let original = crate::NativeMaterializationWork::new(
        WorkBudget { allocation_operations: 0, backend_read_operations: 0,
            bytes_copied: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).map_err(|(cause, _)| cause)?;
    let baseline = original.live_allocation_bytes();
    let mut paths = Vec::with_capacity(17);
    let mut root = String::with_capacity(97);
    root.push('/');
    paths.push(root);
    let owned = crate::workspace::workspace_owned_strings_bytes(&paths)?;
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let selected = fs.select_fork_paths_with_accounting(
        &source, checkout.root(), paths, &mut accounting, &cancellation,
    ).await?;
    assert_eq!(selected, checkout.root().file_table);
    let spent = original.receipt();
    assert_eq!(spent.allocation_operations, 0);
    assert_eq!(spent.backend_read_operations, 0);
    assert_eq!(spent.bytes_copied, 0);
    assert_eq!(spent.peak_allocation_bytes, baseline + owned);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn selected_fork_nested_hardlink_closure_replays_finite_work_and_root_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::workspace::{ForkOptions, WorkspaceError};
    let fs = Fs::memory();
    let workspace = fs.create_workspace("selected-fork-closure-controls").await?;
    let mut transaction = workspace.begin_transaction(crate::IdempotencyKey::new()).await?;
    transaction.create_dir_all("/tree/nested").await?;
    transaction.write_text("/tree/nested/file", "linked body").await?;
    transaction.hard_link("/tree/nested/file", "/tree/nested/alias").await?;
    transaction.hard_link("/tree/nested/file", "/outside-alias").await?;
    transaction.write_text("/omitted", "not inherited").await?;
    transaction.commit().await?;
    let source = workspace.head().await?;
    let paths = vec!["/tree".to_owned(), "/tree/nested/file".to_owned(), "/tree".to_owned()];
    let child = workspace.fork(
        "selected-fork-closure-child",
        ForkOptions::from_generation(source.clone(), crate::IdempotencyKey::new())
            .inherit_paths(paths.clone()),
    ).await?;
    let initial = child.head().await?;
    assert_eq!((child.read("/tree/nested/alias", 32).await?).as_ref(), b"linked body");
    assert!(child.read("/outside-alias", 32).await.is_err());
    assert!(child.read("/omitted", 32).await.is_err());
    let file = initial.stat("/tree/nested/file").await?;
    let alias = initial.stat("/tree/nested/alias").await?;
    assert_eq!(file.file_id, alias.file_id);
    assert_eq!(file.link_count, 2);
    assert!(initial.matches_fork_selection(&source, Some(paths.clone())).await?);
    assert!(initial.matches_fork_selection(&source, Some(vec!["/tree".to_owned()])).await?);
    let cancellation = CancellationToken::new();
    let baseline = initial.matches_fork_selection_measured(
        &source, Some(paths.clone()), WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    assert!(baseline.value);
    assert!(baseline.work.allocation_operations > 0);
    assert!(baseline.work.backend_read_operations > 0);
    let finite = WorkBudget {
        authority_records_read: 1_000_000, authority_records_appended: 1_000_000,
        authority_bytes_read: 1_000_000, authority_bytes_written: 1_000_000,
        object_probes: 1_000_000, backend_read_operations: 1_000_000,
        backend_write_operations: 1_000_000, durability_operations: 1_000_000,
        page_reads: 1_000_000, page_writes: 1_000_000,
        object_bytes_read: 1_000_000, object_bytes_written: 1_000_000,
        bytes_hashed: 1_000_000, bytes_copied: 1_000_000, bytes_encoded: 1_000_000,
        source_bytes_read: 1_000_000, source_path_components: 1_000_000,
        source_entries_visited: 1_000_000, output_bytes: 1_000_000,
        items_examined: 1_000_000, items_returned: 1_000_000,
        allocation_operations: 1_000_000, peak_allocation_bytes: 64 * 1024 * 1024,
        materializations: 1_000_000,
    };
    let replay = initial.matches_fork_selection_measured(
        &source, Some(paths.clone()), finite, &cancellation,
    ).await?;
    assert_eq!(replay.value, baseline.value);
    assert_eq!(replay.work, baseline.work);
    let denied = initial.matches_fork_selection_measured(
        &source, Some(paths),
        WorkBudget { peak_allocation_bytes: baseline.work.peak_allocation_bytes - 1, ..finite },
        &cancellation,
    ).await.err().ok_or("tight peak unexpectedly admitted the entire selection")?;
    assert!(matches!(denied.error, WorkspaceError::Work(_)));
    assert!(denied.work.peak_allocation_bytes > 0);
    assert!(denied.work.backend_read_operations > 0);
    assert!(denied.work.allocation_operations > 0);
    let all = workspace.fork(
        "selected-fork-root-child",
        ForkOptions::from_generation(source.clone(), crate::IdempotencyKey::new())
            .inherit_paths(vec!["/tree".to_owned(), "/".to_owned(), "/".to_owned()]),
    ).await?;
    let all = all.head().await?;
    assert!(all.matches_fork_selection(&source, None).await?);
    assert!(all.matches_fork_selection(&source, Some(vec!["/".to_owned()])).await?);
    assert!(!initial.matches_fork_selection(&source, Some(vec!["/".to_owned()])).await?);
    Ok(())
}

#[tokio::test]
async fn selected_fork_verification_zero_allocation_and_cancellation_keep_failed_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::workspace::{ForkOptions, WorkspaceError};
    let tracked = Arc::new(std::sync::Mutex::new(None));
    let reads = Arc::new(AtomicUsize::new(0));
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        CountingObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            tracked: Arc::clone(&tracked),
            reads: Arc::clone(&reads),
            unreadable: Arc::default(),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let workspace = fs.create_workspace("selected-fork-verification-gates").await?;
    workspace.write_text("/file", "body").await?;
    let source = workspace.head().await?;
    let child = workspace.fork(
        "selected-fork-verification-child",
        ForkOptions::from_generation(source.clone(), crate::IdempotencyKey::new())
            .inherit_paths(vec!["/file".to_owned()]),
    ).await?;
    let initial = child.head().await?;
    *tracked.lock().map_err(|_| "tracking lock poisoned")? = Some(ObjectId {
        kind: ObjectKind::GenerationRoot, digest: source.id().digest(),
    });
    reads.store(0, Ordering::Relaxed);
    let cancellation = CancellationToken::new();
    for budget in [
        WorkBudget::default(),
        WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED },
    ] {
        let failed = initial.matches_fork_selection_measured(
            &source, Some(vec!["/file".to_owned()]), budget, &cancellation,
        ).await.err().ok_or("verification ignored its allocation gate")?;
        assert!(matches!(failed.error, WorkspaceError::Work(_)));
        assert_eq!(failed.work.allocation_operations, 0);
        assert_eq!(failed.work.backend_read_operations, 0);
        assert_eq!(reads.load(Ordering::Relaxed), 0);
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failed = initial.matches_fork_selection_measured(
        &source, Some(vec!["/file".to_owned()]), WorkBudget::UNBOUNDED, &cancelled,
    ).await.err().ok_or("verification ignored cancellation")?;
    assert!(matches!(failed.error, WorkspaceError::Cancelled(_)));
    assert_eq!(failed.work.allocation_operations, 0);
    assert_eq!(failed.work.backend_read_operations, 0);
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_content_stream_and_clone_keep_real_identity_and_atomic_stamps()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-content-stream").await?;
    workspace.write_text("/source", "authenticated source bytes").await?;
    workspace.write_text("/destination", "old destination").await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let source = checkout.lookup_no_follow(
        &path("source")?, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record.ok_or("actual source absent")?;
    let destination = checkout.lookup_no_follow(
        &path("destination")?, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record.ok_or("actual destination absent")?;
    let metadata = FileMetadata {
        modified_ns: MetadataField::Value(1), changed_ns: MetadataField::Value(2),
        accessed_ns: MetadataField::Value(3), ..FileMetadata::default()
    };
    checkout.set_metadata_by_id(
        destination.file_id, metadata, WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let reader = checkout.pinned_reader()?;
    let bytes = reader.read_record_range_with_native_work(
        source, ByteRange { offset: 0, length: 26 }, &original, &cancellation,
    ).await?.bytes;
    assert_eq!(bytes.as_ref(), b"authenticated source bytes");
    let mut candidate = checkout.private_candidate_with_native_work(&original, &cancellation)?;
    candidate.change_content_by_id_with_native_work(
        destination.file_id, ContentChange::Write { offset: 0, bytes },
        ContentTimes::Stamp(40), &original, &cancellation,
    ).await?;
    candidate.change_content_by_id_with_native_work(
        destination.file_id,
        ContentChange::CloneFrom { source: source.file_id, source_offset: 14, offset: 0, length: 6 },
        ContentTimes::Stamp(41), &original, &cancellation,
    ).await?;
    let actual = candidate.read_file_record_by_id_with_native_work(
        destination.file_id, &original, &cancellation,
    ).await?;
    assert_eq!(actual.file_id, destination.file_id);
    let content = reader.read_record_range_with_native_work(
        actual, ByteRange { offset: 0, length: 26 }, &original, &cancellation,
    ).await?;
    assert_eq!(content.bytes.as_ref(), b"sourceticated source bytes");
    let stamped = candidate.read_metadata_by_id_with_native_work(
        destination.file_id, &original, &cancellation,
    ).await?;
    assert_eq!(stamped.modified_ns, MetadataField::Value(41));
    assert_eq!(stamped.changed_ns, MetadataField::Value(41));
    assert_eq!(stamped.accessed_ns, MetadataField::Value(3));
    assert!(original.receipt().source_bytes_read >= 26);
    assert!(original.receipt().bytes_encoded > 0);
    assert_eq!(checkout.read_metadata_by_id(
        destination.file_id, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value, metadata, "private native content cannot stamp its source checkout");
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    let destination_path = path("destination")?;
    let source_path = path("source")?;
    let mut destination_owned = clone_authored_path(&destination_path, &mut accounting)?;
    let mut source_owned = clone_authored_path(&source_path, &mut accounting)?;
    let input_bytes = destination_owned.bytes.checked_add(source_owned.bytes).ok_or(WorkError::Overflow)?;
    destination_owned.allocation.absorb(&mut source_owned.allocation)?;
    let compiled = candidate.compile_content_with_accounting(
        crate::kernel::Owned::new(
            (destination_owned.value, ContentChange::CloneFrom {
                source: source_owned.value, source_offset: 0, offset: 0, length: 6,
            }),
            destination_owned.allocation, input_bytes,
        ),
        ContentTimes::Stamp(42), &mut accounting, &cancellation,
    ).await?;
    assert!(matches!(&compiled.value[0], Mutation::CloneRange { source, destination, .. }
        if source == &source_path && destination == &destination_path));
    assert_eq!(compiled.bytes,
        compiled.value.capacity() as u64 * size_of::<Mutation>() as u64
        + source_path.retained_bytes() + 2 * destination_path.retained_bytes());
    compiled.retire(&mut accounting)?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_content_copy_and_stamp_denials_leave_real_records_unchanged()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::allocation::KernelAccounting;
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-content-denials").await?;
    workspace.write_text("/source", "actual clone source").await?;
    workspace.write_text("/destination", "actual old destination").await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let source = checkout.lookup_no_follow(
        &path("source")?, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record.ok_or("actual source absent")?;
    let destination = checkout.lookup_no_follow(
        &path("destination")?, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record.ok_or("actual destination absent")?;
    checkout.set_metadata_by_id(destination.file_id, FileMetadata {
        modified_ns: MetadataField::Value(1), changed_ns: MetadataField::Value(2),
        ..FileMetadata::default()
    }, WorkBudget::UNBOUNDED, &cancellation).await?;
    let before = checkout.root.file_table;
    for deny_stamp in [false, true] {
        let budget = if deny_stamp {
            WorkBudget { bytes_encoded: 0, ..WorkBudget::UNBOUNDED }
        } else {
            WorkBudget { bytes_copied: 0, ..WorkBudget::UNBOUNDED }
        };
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let allocation = accounting.claim_owned(0, 0)?;
        let change = ContentChange::CloneFrom {
            source: source.file_id, source_offset: 0, offset: 0, length: 6,
        };
        let failed = checkout.compile_content_with_accounting(
            crate::kernel::Owned::new((destination.file_id, change), allocation, 0),
            if deny_stamp { ContentTimes::Stamp(50) } else { ContentTimes::Preserve },
            &mut accounting, &cancellation,
        ).await.err().ok_or("original content ignored its copy/stamp gate")?;
        assert!(matches!(failed, FsError::Work(_) | FsError::NativeKernel(_)));
        assert_eq!(checkout.root.file_table, before);
        assert_eq!(original.receipt().bytes_encoded, 0);
        assert_eq!(original.receipt().backend_write_operations, 0);
        if !deny_stamp { assert_eq!(original.live_allocation_bytes(), baseline); }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_detached_content_stream_keeps_atomic_metadata_transition()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-detached-content").await?;
    workspace.write_text("/source", "real detached stream").await?;
    workspace.write_text("/destination", "before").await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace.checkout_measured(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let source = checkout.lookup_no_follow(
        &path("source")?, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record.ok_or("real detached source absent")?;
    let destination = checkout.lookup_no_follow(
        &path("destination")?, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record.ok_or("real detached destination absent")?;
    let reader = checkout.pinned_reader()?;
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let source = reader.read_record_range_with_native_work(
        source, ByteRange { offset: 0, length: 20 }, &original, &cancellation,
    ).await?;
    let metadata = FileMetadata {
        modified_ns: MetadataField::Value(90), changed_ns: MetadataField::Value(90),
        ..FileMetadata::default()
    };
    let mut detached = DetachedFile::from_record(checkout.volume.clone(), destination);
    detached.change_content_with_native_work(
        ContentChange::Write { offset: 0, bytes: source.bytes }, Some(metadata),
        &original, &cancellation,
    ).await?;
    assert_eq!(detached.record.file_id, destination.file_id);
    assert_eq!(reader.read_record_range_with_native_work(
        detached.record, ByteRange { offset: 0, length: 20 }, &original, &cancellation,
    ).await?.bytes.as_ref(), b"real detached stream");
    let before = detached.record;
    let denied = crate::NativeMaterializationWork::new(
        WorkBudget { bytes_encoded: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).map_err(|(cause, _)| cause)?;
    let failure = detached.change_content_with_native_work(
        ContentChange::Write { offset: 0, bytes: OwnedBytes::new() },
        Some(FileMetadata { modified_ns: MetadataField::Value(91), ..metadata }),
        &denied, &cancellation,
    ).await.err().ok_or("detached metadata encoding ignored original denial")?;
    assert!(matches!(failure, FsError::Work(_) | FsError::NativeKernel(_)));
    assert_eq!(detached.record, before);
    assert_eq!(denied.receipt().bytes_encoded, 0);
    assert_eq!(denied.receipt().backend_write_operations, 0);
    Ok(())
}
