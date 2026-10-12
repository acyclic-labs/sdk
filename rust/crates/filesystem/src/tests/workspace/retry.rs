use super::*;

#[tokio::test]
async fn original_fork_retry_after_both_heads_advance_needs_no_publication_seal()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let parent = fs.create_workspace("original-fork-parent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    parent.write_text("/file", "original").await?;
    let frozen = parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let key = IdempotencyKey::new();
    let cancellation = CancellationToken::new();
    let budget = selected_transaction_budget();
    let first = parent.fork(
        "original-fork-child", ForkOptions::from_generation(frozen.clone(), key),
        crate::PublicationPermit::Unrestricted, budget, &cancellation,
        |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })),
    ).await?;
    let child = first.value;
    let original_child = child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    parent.write_text("/file", "later parent").await?;
    child.write_text("/file", "later child").await?;
    let advanced_child = child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let retry = parent.fork(
        "original-fork-child", ForkOptions::from_generation(frozen, key),
        crate::PublicationPermit::Unrestricted, budget, &cancellation,
        |_| std::future::ready(Err(crate::OperationFailure::before_work(crate::FsError::CreationRejected))),
    ).await?;
    retry.work.verify(budget)?;
    assert_eq!(retry.work.authority_records_appended, 0);
    assert_eq!(retry.value.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), advanced_child.id());
    assert_eq!(retry.value.read("/file", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"later child");
    let creation = fs.workspace_for_creation_operation(
        "original-fork-child", child.volume.config, key.operation_id(), budget, &cancellation,
    ).await?.value.ok_or("original creation missing")?;
    assert_eq!(creation.id(), original_child.id());
    assert_eq!(creation.read("/file", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"original");
    Ok(())
}

#[tokio::test]
async fn guarded_stale_exact_retry_resolves_committed_without_owner_reseal()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("resolved-owner-seal", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let base = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let key = IdempotencyKey::new();
    let mut first = workspace.begin_transaction(key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    first.write("/file", bytes::Bytes::from_static(b"same input"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let TransactionCommit::Committed(committed) = first.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)? else {
        return Err("initial original commit did not publish".into());
    };
    let mut retry = workspace.begin_transaction_at(&base, key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    retry.write("/file", bytes::Bytes::from_static(b"same input"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let budget = selected_transaction_budget();
    let mut owner_calls = 0;
    let resolved = retry.commit(None, crate::PublicationPermit::Unrestricted, budget, &cancellation, |_| {
            owner_calls += 1;
            std::future::ready(Err(crate::OperationFailure::before_work(
                OrdinaryOwnerError::Denied { task: [119; 16], revision: 914, expired: true },
            )))
        }).await?;
    resolved.work.verify(budget)?;
    assert!(matches!(&resolved.value, TransactionCommit::AlreadyCommitted(generation) if generation.id() == committed.id()));
    assert_eq!(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), committed.id());
    assert_eq!(owner_calls, 0, "an already committed operation performs no new CAS");
    Ok(())
}

#[tokio::test]
async fn ordinary_owner_denial_preserves_whole_error_work_and_candidate()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("whole-ordinary-owner", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let base = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let mut candidate = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    candidate.write("/file", bytes::Bytes::from_static(b"candidate"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let callback_work = crate::WorkCounters {
        backend_read_operations: 3,
        ..crate::WorkCounters::UNCHARGED
    };
    let failure = candidate.commit(None, crate::PublicationPermit::Unrestricted, selected_transaction_budget(), &cancellation, |_| std::future::ready(Err(crate::OperationFailure::new(
            OrdinaryOwnerError::Denied { task: [117; 16], revision: 912, expired: true },
            callback_work,
        )))).await.err().ok_or("expired owner published")?;
    assert!(matches!(failure.error,
        OrdinaryOwnerError::Denied { task, revision: 912, expired: true } if task == [117; 16]));
    assert!(failure.work.backend_read_operations >= callback_work.backend_read_operations);
    assert!(failure.work.bytes_hashed > 0);
    assert_eq!(failure.work.authority_records_appended, 0);
    assert_eq!(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), base.id());
    let failure = async { let mut guard = || std::future::ready(Err(OrdinaryOwnerError::Denied {
            task: [118; 16], revision: 913, expired: false,
        })); candidate.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &cancellation, |_| { let future = guard(); async move { future.await.map(|value| crate::OperationReceipt { value, work: crate::WorkCounters::default() }).map_err(crate::OperationFailure::before_work) } }).await.map(|receipt| receipt.value).map_err(|failure| failure.error) }.await.err().ok_or("denied ordinary owner published")?;
    assert!(matches!(failure,
        OrdinaryOwnerError::Denied { task, revision: 913, expired: false } if task == [118; 16]));
    assert_eq!(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), base.id());
    assert!(matches!(candidate.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, TransactionCommit::Committed(_)));
    assert_eq!(workspace.read("/file", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"candidate");
    let mut cancelled_candidate = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    cancelled_candidate.write("/cancelled", bytes::Bytes::from_static(b"not published"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    cancellation.cancel();
    let sdk_failure = async { let mut guard = || std::future::ready(Ok::<(), OrdinaryOwnerError>(())); cancelled_candidate.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &cancellation, |_| { let future = guard(); async move { future.await.map(|value| crate::OperationReceipt { value, work: crate::WorkCounters::default() }).map_err(crate::OperationFailure::before_work) } }).await.map(|receipt| receipt.value).map_err(|failure| failure.error) }.await.err().ok_or("cancelled SDK work published")?;
    assert!(matches!(sdk_failure, OrdinaryOwnerError::Sdk(WorkspaceError::Cancelled(_))));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_guarded_creation_preserves_original_callback_error_and_prepared_work()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        selected_transaction_budget(), &cancellation,
    ).map_err(|(error, _)| error)?;
    let before = original.receipt();
    let seals = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_seals = Arc::clone(&seals);
    let guard_original = original.clone();
    let mut deny = move |observed: &crate::NativeMaterializationWork| {
        assert!(observed.shares_ledger(&guard_original));
        observed_seals.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::future::ready(Err(NativeCreationGuardTestError::Denied(23)))
    };
    let failure = fs.create_workspace_with_operation_guarded_with_native_work(
        "native-guarded-creation-denied", VolumeConfig::portable(Lifecycle::Durable),
        IdempotencyKey::new().operation_id(), &original, &cancellation, &mut deny,
    ).await.err().ok_or("the owner denial unexpectedly created a workspace")?;
    assert!(matches!(failure, NativeCreationGuardTestError::Denied(23)));
    assert!(seals.load(std::sync::atomic::Ordering::Relaxed) > 0);
    assert!(original.receipt().bytes_hashed > before.bytes_hashed);
    assert_eq!(original.receipt().authority_records_appended, before.authority_records_appended);
    assert!(fs.open_workspace("native-guarded-creation-denied", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
    original.receipt().verify(selected_transaction_budget())?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_guarded_fork_denial_keeps_prepared_work_without_destination_authority()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("native-guarded-fork-source", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write("/selected", bytes::Bytes::from_static(b"original")).await?;
    let frozen = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        selected_transaction_budget(), &cancellation,
    ).map_err(|(error, _)| error)?;
    let before = original.receipt();
    let seals = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_seals = Arc::clone(&seals);
    let guard_original = original.clone();
    let mut deny = move |observed: &crate::NativeMaterializationWork| {
        assert!(observed.shares_ledger(&guard_original));
        observed_seals.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::future::ready(Err(WorkspaceError::from(crate::FsError::MutationNotAllowed)))
    };
    let failure = workspace.fork_with_native_work(
        "native-guarded-fork-denied", ForkOptions::from_generation(frozen, IdempotencyKey::new())
            .inherit_paths(vec!["/selected".to_owned()]),
        crate::PublicationPermit::Unrestricted, &original, &cancellation, &mut deny,
    ).await.err().ok_or("the original owner denial unexpectedly published a destination")?;
    assert!(matches!(failure, WorkspaceError::Filesystem(crate::FsError::MutationNotAllowed)));
    assert!(seals.load(std::sync::atomic::Ordering::Relaxed) > 0);
    assert!(original.receipt().bytes_hashed > before.bytes_hashed);
    assert!(fs.open_workspace("native-guarded-fork-denied", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
    original.receipt().verify(selected_transaction_budget())?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_guarded_fork_rejects_an_unrelated_source_reservation_without_rebinding_it()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("native-fork-reserved-source", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write("/value", bytes::Bytes::from_static(b"frozen")).await?;
    let frozen = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        selected_transaction_budget(), &cancellation,
    ).map_err(|(error, _)| error)?;
    let reservation = workspace.reserve_publication(
        IdempotencyKey::new().operation_id(), &original, &cancellation,
    ).await?;
    let before = original.receipt().authority_records_appended;
    let permit = crate::PublicationPermit::Reservation {
        operation_id: reservation.operation_id.into_bytes(),
        gate_tail: reservation.gate_tail,
        expected: reservation.expected,
    };
    let mut allow = |_: &crate::NativeMaterializationWork| std::future::ready(Ok::<(), WorkspaceError>(()));
    let failed = workspace.fork_with_native_work(
        "native-fork-reserved-destination", ForkOptions::from_generation(frozen, IdempotencyKey::new()),
        permit, &original, &cancellation, &mut allow,
    ).await.err().ok_or("a source reservation was incorrectly rebound to a new destination")?;
    assert!(matches!(failed,
        WorkspaceError::Filesystem(crate::FsError::Authority(
            crate::AuthorityStoreError::RejectedStatic(_)))));
    assert_eq!(original.receipt().authority_records_appended, before);
    assert!(fs.open_workspace("native-fork-reserved-destination", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
    workspace.release_publication(reservation, &original, &cancellation)
        .await?;
    Ok(())
}

#[tokio::test]
async fn contextual_operation_record_retains_original_witness_after_head_advances()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-context-record", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let key = IdempotencyKey::new();
    let original_evidence: &'static [u8] = b"opaque original SDK operation witness";
    let context = crate::Digest::from_bytes([0xc7; 32]);
    let mut transaction = workspace.begin_transaction(key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.write("/original", bytes::Bytes::from_static(b"original generation"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let committed = transaction.commit(Some((context, acyclic_stream::OwnedBytes::from_static(original_evidence), crate::PublicationPermit::Unrestricted)), crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| std::future::ready(Ok::<_, crate::OperationFailure<WorkspaceError>>(
            crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED },
        ))).await?;
    let original_generation = match committed.value {
        TransactionCommit::Committed(generation) | TransactionCommit::AlreadyCommitted(generation) => generation,
        _ => return Err("the original contextual operation did not publish".into()),
    };
    workspace.write("/later", bytes::Bytes::from_static(b"later unrelated generation")).await?;
    let current = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_ne!(current.id(), original_generation.id());
    let observed = workspace.operation_generation_record(
        key, WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    let (generation, record) = observed.value.0.ok_or("original operation disappeared")?;
    assert_eq!(generation.id(), original_generation.id());
    assert_eq!(record.operation_id, key.operation_id());
    let mut decode_work = crate::WorkCounters::UNCHARGED;
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
        &mut allocations, &mut decode_work, WorkBudget::UNBOUNDED, &cancellation,
    );
    let evidence = crate::kernel::decode_publication_evidence_with_accounting(
        &record.payload, 4 * 1024, &mut accounting,
    )?;
    assert_eq!(evidence.publication.generation_root.digest, original_generation.id().digest());
    assert_eq!(evidence.original_evidence, Some(original_evidence));
    assert!(evidence.evidence_digest.is_some());
    drop(accounting);
    assert!(decode_work.bytes_hashed >= original_evidence.len() as u64);
    assert_eq!(workspace.operation_generation(
        key, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.ok_or("generation-only projection lost the operation")?.id(), original_generation.id());
    assert!(workspace.operation_generation_record(
        IdempotencyKey::new(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.0.is_none());
    Ok(())
}

#[tokio::test]
async fn original_fingerprint_survives_current_reservation_rotation_and_rejects_changed_data()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-fingerprint-current-reservation", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let key = IdempotencyKey::new();
    let context = crate::Digest::from_bytes([0xc8; 32]);
    let evidence = acyclic_stream::OwnedBytes::from_static(b"the original frozen SDK witness");
    let historical_permit = crate::PublicationPermit::Unrestricted;
    let mut transaction = workspace.begin_transaction(key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.write("/original", bytes::Bytes::from_static(b"original content"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let first = workspace.reserve_publication(IdempotencyKey::new().operation_id(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let current_permit = crate::PublicationPermit::Reservation {
        operation_id: first.operation_id.into_bytes(), gate_tail: first.gate_tail, expected: first.expected,
    };
    let mut allow = |_| std::future::ready(Ok::<_, crate::OperationFailure<WorkspaceError>>(
        crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED },
    ));
    let committed = transaction.commit(Some((context, evidence.clone(), historical_permit)), current_permit, WorkBudget::UNBOUNDED, &cancellation, &mut allow).await?;
    let generation = match committed.value {
        TransactionCommit::Committed(generation) | TransactionCommit::AlreadyCommitted(generation) => generation,
        _ => return Err("the current genuine reservation did not authorize publication".into()),
    };
    let (_, record) = workspace.operation_generation_record(
        key, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.0.ok_or("the original publication disappeared")?;
    let mut work = crate::WorkCounters::UNCHARGED;
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
        &mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation,
    );
    let publication = crate::kernel::decode_publication_evidence_with_accounting(
        &record.payload, 4 * 1024, &mut accounting,
    )?;
    let request = crate::kernel::PublishGenerationRequest {
        authority_id: workspace.authority_id(), volume_id: workspace.volume.id,
        epoch: record.epoch,
        expected: crate::Head {
            epoch: record.epoch,
            sequence: crate::Sequence::new(record.sequence.get().checked_sub(1).ok_or("missing original predecessor")?),
            digest: record.previous_digest,
        },
        operation_id: key.operation_id(),
        generation_root: publication.publication.generation_root,
    };
    let historical_fingerprint = crate::kernel::contextual_publication_fingerprint_from_payload_with_accounting(
        request, context, &record.payload, &mut accounting,
    )?;
    assert_eq!(record.fingerprint, historical_fingerprint);
    drop(accounting);
    drop(record);
    workspace.release_publication(first, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let rotated = workspace.reserve_publication(IdempotencyKey::new().operation_id(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let rotated_permit = crate::PublicationPermit::Reservation {
        operation_id: rotated.operation_id.into_bytes(), gate_tail: rotated.gate_tail, expected: rotated.expected,
    };
    assert_ne!(rotated_permit, current_permit);
    let recovered = transaction.checkout.retry_stale_commit(key.operation_id(), rotated_permit, WorkBudget::UNBOUNDED, &cancellation, &mut allow, None, Some(context), Some(evidence.clone()), Some(historical_permit)).await?;
    assert!(matches!(recovered.value, Some(crate::CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. })
        if generation_id == generation.id()));
    let changed_evidence = transaction.checkout.retry_stale_commit(key.operation_id(), rotated_permit, WorkBudget::UNBOUNDED, &cancellation, &mut allow, None, Some(context), Some(acyclic_stream::OwnedBytes::from_static(b"a changed SDK witness")), Some(historical_permit)).await?;
    assert!(matches!(changed_evidence.value, Some(crate::CheckoutCommitOutcome::IdempotencyConflict { .. })));
    transaction.write("/original", bytes::Bytes::from_static(b"changed candidate content"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let changed_payload = transaction.checkout.retry_stale_commit(key.operation_id(), rotated_permit, WorkBudget::UNBOUNDED, &cancellation, &mut allow, None, Some(context), Some(evidence), Some(historical_permit)).await?;
    assert!(matches!(changed_payload.value, Some(crate::CheckoutCommitOutcome::IdempotencyConflict { .. })));
    assert_eq!(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), generation.id());
    workspace.release_publication(rotated, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    Ok(())
}

#[tokio::test]
async fn original_fingerprint_metadata_never_authorizes_a_foreign_current_reservation()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-fingerprint-current-target", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let foreign = fs.create_workspace("original-fingerprint-foreign-current-grant", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let original_head = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let key = IdempotencyKey::new();
    let mut transaction = workspace.begin_transaction(key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.write("/value", bytes::Bytes::from_static(b"the genuine candidate"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let target_reservation = workspace.reserve_publication(IdempotencyKey::new().operation_id(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let foreign_reservation = foreign.reserve_publication(IdempotencyKey::new().operation_id(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let foreign_permit = crate::PublicationPermit::Reservation {
        operation_id: foreign_reservation.operation_id.into_bytes(),
        gate_tail: foreign_reservation.gate_tail, expected: foreign_reservation.expected,
    };
    let rejected = transaction.commit(Some((crate::Digest::from_bytes([0xc9; 32]), acyclic_stream::OwnedBytes::from_static(b"valid original opaque SDK witness"), crate::PublicationPermit::Unrestricted)), foreign_permit, WorkBudget::UNBOUNDED, &cancellation, |_| std::future::ready(Ok::<_, crate::OperationFailure<WorkspaceError>>(
            crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED },
        ))).await.err().ok_or("historical fingerprint metadata authorized a foreign current grant")?;
    assert!(matches!(rejected.error,
        WorkspaceError::Filesystem(crate::FsError::Authority(crate::AuthorityStoreError::RejectedStatic(_)))));
    assert!(rejected.work.bytes_hashed > 0);
    assert_eq!(rejected.work.authority_records_appended, 0);
    assert_eq!(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), original_head.id());
    assert!(workspace.operation_generation_record(
        key, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.0.is_none());
    workspace.release_publication(target_reservation, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    foreign.release_publication(foreign_reservation, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    Ok(())
}
