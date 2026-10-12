use super::*;

#[tokio::test]
async fn original_fork_retry_after_both_heads_advance_needs_no_publication_seal()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let parent = fs
        .create_workspace("original-fork-parent", B, &cancel)
        .await?
        .value;
    parent.write_text("/file", "original").await?;
    let frozen = parent.head(B, &cancel).await?.value;
    let key = IdempotencyKey::new();
    let cancellation = CancellationToken::new();
    let budget = selected_transaction_budget();
    let first = parent
        .fork::<_, _, crate::WorkspaceError>(
            "original-fork-child",
            ForkOptions::from_generation(frozen.clone(), key),
            crate::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            &mut |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?;
    let child = first.value;
    let original_child = child.head(B, &cancel).await?.value;
    parent.write_text("/file", "later parent").await?;
    child.write_text("/file", "later child").await?;
    let advanced_child = child.head(B, &cancel).await?.value;
    let retry = parent
        .fork::<_, _, crate::WorkspaceError>(
            "original-fork-child",
            ForkOptions::from_generation(frozen, key),
            crate::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            &mut |_| {
                std::future::ready(Err(crate::OperationFailure::before_work(
                    crate::FsError::CreationRejected,
                )))
            },
        )
        .await?;
    retry.work.verify(budget)?;
    assert_eq!(retry.work.authority_records_appended, 0);
    assert_eq!(
        retry.value.head(B, &cancel).await?.value.id(),
        advanced_child.id()
    );
    assert_eq!(
        retry
            .value
            .read("/file", 32, B, &cancel)
            .await?
            .value
            .as_ref(),
        b"later child"
    );
    let creation = fs
        .workspace_for_creation_operation(
            "original-fork-child",
            child.volume.config,
            key.operation_id(),
            budget,
            &cancellation,
        )
        .await?
        .value
        .ok_or("original creation missing")?;
    assert_eq!(creation.id(), original_child.id());
    assert_eq!(
        creation.read("/file", 32, B, &cancel).await?.value.as_ref(),
        b"original"
    );
    Ok(())
}

#[tokio::test]
async fn guarded_stale_exact_retry_resolves_committed_without_owner_reseal()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("resolved-owner-seal", B, &cancel)
        .await?
        .value;
    let base = workspace.head(B, &cancel).await?.value;
    let key = IdempotencyKey::new();
    let mut first = workspace.begin_transaction(key, B, &cancel).await?.value;
    first
        .write(
            "/file",
            bytes::Bytes::from_static(b"same input"),
            B,
            &cancel,
        )
        .await?
        .value;
    let TransactionCommit::Committed(committed) = first
        .commit(
            None,
            crate::PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<WorkspaceError>,
        )
        .await?
        .value
    else {
        return Err("initial original commit did not publish".into());
    };
    let mut retry = workspace
        .begin_transaction_at(&base, key, B, &cancel)
        .await?
        .value;
    retry
        .write(
            "/file",
            bytes::Bytes::from_static(b"same input"),
            B,
            &cancel,
        )
        .await?
        .value;
    let cancellation = CancellationToken::new();
    let budget = selected_transaction_budget();
    let mut owner_calls = 0;
    let resolved = retry
        .commit(
            None,
            crate::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            |_| {
                owner_calls += 1;
                std::future::ready(Err(crate::OperationFailure::before_work(
                    OrdinaryOwnerError::Denied {
                        task: [119; 16],
                        revision: 914,
                        expired: true,
                    },
                )))
            },
        )
        .await?;
    resolved.work.verify(budget)?;
    assert!(
        matches!(&resolved.value, TransactionCommit::AlreadyCommitted(generation) if generation.id() == committed.id())
    );
    assert_eq!(workspace.head(B, &cancel).await?.value.id(), committed.id());
    assert_eq!(
        owner_calls, 0,
        "an already committed operation performs no new CAS"
    );
    Ok(())
}
#[tokio::test]
async fn ordinary_owner_denial_preserves_whole_error_work_and_candidate()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("whole-ordinary-owner", B, &cancel)
        .await?
        .value;
    let base = workspace.head(B, &cancel).await?.value;
    let cancellation = CancellationToken::new();
    let mut candidate = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    candidate
        .write("/file", bytes::Bytes::from_static(b"candidate"), B, &cancel)
        .await?;
    let callback_work = WorkCounters {
        backend_read_operations: 3,
        ..WorkCounters::UNCHARGED
    };
    let failure = candidate
        .commit(
            None,
            PublicationPermit::Unrestricted,
            selected_transaction_budget(),
            &cancellation,
            |_| {
                std::future::ready(Err(OperationFailure::new(
                    OrdinaryOwnerError::Denied {
                        task: [117; 16],
                        revision: 912,
                        expired: true,
                    },
                    callback_work,
                )))
            },
        )
        .await
        .err()
        .ok_or("expired owner published")?;
    assert!(
        matches!(failure.error, OrdinaryOwnerError::Denied { task, revision: 912, expired: true } if task == [117; 16])
    );
    assert!(failure.work.backend_read_operations >= callback_work.backend_read_operations);
    assert!(failure.work.bytes_hashed > 0);
    assert_eq!(failure.work.authority_records_appended, 0);
    assert_eq!(workspace.head(B, &cancel).await?.value.id(), base.id());
    let failure = candidate
        .commit(
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancellation,
            |_| {
                std::future::ready(Err(OperationFailure::before_work(
                    OrdinaryOwnerError::Denied {
                        task: [118; 16],
                        revision: 913,
                        expired: false,
                    },
                )))
            },
        )
        .await
        .err()
        .ok_or("denied ordinary owner published")?;
    assert!(
        matches!(failure.error, OrdinaryOwnerError::Denied { task, revision: 913, expired: false } if task == [118; 16])
    );
    assert_eq!(workspace.head(B, &cancel).await?.value.id(), base.id());
    assert!(matches!(
        candidate
            .commit(
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<WorkspaceError>,
            )
            .await?
            .value,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        workspace
            .read("/file", 32, B, &cancel)
            .await?
            .value
            .as_ref(),
        b"candidate"
    );
    let mut cancelled_candidate = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    cancelled_candidate
        .write(
            "/cancelled",
            bytes::Bytes::from_static(b"not published"),
            B,
            &cancel,
        )
        .await?;
    cancellation.cancel();
    let failure = cancelled_candidate
        .commit(
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancellation,
            allow::<OrdinaryOwnerError>,
        )
        .await
        .err()
        .ok_or("cancelled SDK work published")?;
    assert!(matches!(
        failure.error,
        OrdinaryOwnerError::Sdk(WorkspaceError::Cancelled(_))
    ));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_guarded_creation_preserves_original_callback_error_and_prepared_work()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(selected_transaction_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
    let before = original.receipt();
    let seals = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_seals = Arc::clone(&seals);
    let guard_original = original.clone();
    let mut deny = move |observed: &crate::NativeMaterializationWork| {
        assert!(observed.shares_ledger(&guard_original));
        observed_seals.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::future::ready(Err(NativeCreationGuardTestError::Denied(23)))
    };
    let mut owner = crate::NativeAuthorityGuard(deny);
    let failure = fs
        .create_workspace_with_operation::<_, _, NativeCreationGuardTestError>(
            "native-guarded-creation-denied",
            VolumeConfig::portable(Lifecycle::Durable),
            IdempotencyKey::new().operation_id(),
            &original,
            &cancellation,
            Some(&mut owner),
        )
        .await
        .err()
        .ok_or("the owner denial unexpectedly created a workspace")?;
    assert!(matches!(failure, NativeCreationGuardTestError::Denied(23)));
    assert!(seals.load(std::sync::atomic::Ordering::Relaxed) > 0);
    assert!(original.receipt().bytes_hashed > before.bytes_hashed);
    assert_eq!(
        original.receipt().authority_records_appended,
        before.authority_records_appended
    );
    assert!(
        fs.open_workspace("native-guarded-creation-denied", B, &cancel)
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_err()
    );
    original.receipt().verify(selected_transaction_budget())?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_guarded_fork_denial_keeps_prepared_work_without_destination_authority()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("native-guarded-fork-source", B, &cancel)
        .await?
        .value;
    workspace
        .write("/selected", bytes::Bytes::from_static(b"original"))
        .await?;
    let frozen = workspace.head(B, &cancel).await?.value;
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(selected_transaction_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
    let before = original.receipt();
    let seals = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_seals = Arc::clone(&seals);
    let guard_original = original.clone();
    let mut deny = move |observed: &crate::NativeMaterializationWork| {
        assert!(observed.shares_ledger(&guard_original));
        observed_seals.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::future::ready(Err(WorkspaceError::from(
            crate::FsError::MutationNotAllowed,
        )))
    };
    let failure = workspace
        .fork::<_, _, crate::WorkspaceError>(
            "native-guarded-fork-denied",
            ForkOptions::from_generation(frozen, IdempotencyKey::new())
                .inherit_paths(vec!["/selected".to_owned()]),
            crate::PublicationPermit::Unrestricted,
            &original,
            &cancellation,
            crate::NativeAuthorityGuard(deny.clone()),
        )
        .await
        .err()
        .ok_or("the original owner denial unexpectedly published a destination")?;
    assert!(matches!(
        failure,
        WorkspaceError::Filesystem(crate::FsError::MutationNotAllowed)
    ));
    assert!(seals.load(std::sync::atomic::Ordering::Relaxed) > 0);
    assert!(original.receipt().bytes_hashed > before.bytes_hashed);
    assert!(
        fs.open_workspace("native-guarded-fork-denied", B, &cancel)
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_err()
    );
    original.receipt().verify(selected_transaction_budget())?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_guarded_fork_rejects_an_unrelated_source_reservation_without_rebinding_it()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("native-fork-reserved-source", B, &cancel)
        .await?
        .value;
    workspace
        .write("/value", bytes::Bytes::from_static(b"frozen"))
        .await?;
    let frozen = workspace.head(B, &cancel).await?.value;
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(selected_transaction_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
    let reservation = workspace
        .reserve_publication(
            IdempotencyKey::new().operation_id(),
            &original,
            &cancellation,
        )
        .await?;
    let before = original.receipt().authority_records_appended;
    let permit = crate::PublicationPermit::Reservation {
        operation_id: reservation.operation_id.into_bytes(),
        gate_tail: reservation.gate_tail,
        expected: reservation.expected,
    };
    let mut allow =
        |_: &crate::NativeMaterializationWork| std::future::ready(Ok::<(), WorkspaceError>(()));
    let failed = workspace
        .fork::<_, _, crate::WorkspaceError>(
            "native-fork-reserved-destination",
            ForkOptions::from_generation(frozen, IdempotencyKey::new()),
            permit,
            &original,
            &cancellation,
            crate::NativeAuthorityGuard(allow.clone()),
        )
        .await
        .err()
        .ok_or("a source reservation was incorrectly rebound to a new destination")?;
    assert!(matches!(
        failed,
        WorkspaceError::Filesystem(crate::FsError::Authority(
            crate::AuthorityStoreError::RejectedStatic(_)
        ))
    ));
    assert_eq!(original.receipt().authority_records_appended, before);
    assert!(
        fs.open_workspace("native-fork-reserved-destination", B, &cancel)
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_err()
    );
    workspace
        .release_publication(reservation, &original, &cancellation)
        .await?;
    Ok(())
}

#[tokio::test]
async fn contextual_operation_record_retains_original_witness_after_head_advances()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("original-context-record", B, &cancel)
        .await?
        .value;
    let cancellation = CancellationToken::new();
    let key = IdempotencyKey::new();
    let original_evidence: &'static [u8] = b"opaque original SDK operation witness";
    let context = crate::Digest::from_bytes([0xc7; 32]);
    let mut transaction = workspace.begin_transaction(key, B, &cancel).await?.value;
    transaction
        .write(
            "/original",
            bytes::Bytes::from_static(b"original generation"),
            B,
            &cancel,
        )
        .await?
        .value;
    let committed = transaction
        .commit(
            Some((
                context,
                acyclic_stream::OwnedBytes::from_static(original_evidence),
                crate::PublicationPermit::Unrestricted,
            )),
            crate::PublicationPermit::Unrestricted,
            B,
            &cancellation,
            allow::<WorkspaceError>,
        )
        .await?;
    let original_generation = match committed.value {
        TransactionCommit::Committed(generation)
        | TransactionCommit::AlreadyCommitted(generation) => generation,
        _ => return Err("the original contextual operation did not publish".into()),
    };
    workspace
        .write(
            "/later",
            bytes::Bytes::from_static(b"later unrelated generation"),
        )
        .await?;
    let current = workspace.head(B, &cancel).await?.value;
    assert_ne!(current.id(), original_generation.id());
    let observed = workspace
        .volume.fs.workspace_operation_generation(&workspace.volume, (key).operation_id(), &mut crate::kernel::allocation::KernelAccounting::standalone(B, &cancellation), &cancellation).await?;
    let (generation, record) = observed.0.ok_or("original operation disappeared")?;
    assert_eq!(generation, original_generation.id());
    assert_eq!(record.operation_id, key.operation_id());
    let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(B, &cancellation);
    let evidence = crate::kernel::decode_publication_evidence_with_accounting(
        &record.payload,
        4 * 1024,
        &mut accounting,
    )?;
    assert_eq!(
        evidence.publication.generation_root.digest,
        original_generation.id().digest()
    );
    assert_eq!(evidence.original_evidence, Some(original_evidence));
    assert!(evidence.evidence_digest.is_some());
    assert!(accounting.spent().bytes_hashed >= original_evidence.len() as u64);
    assert_eq!(
        workspace
            .operation_generation(key, B, &cancellation,)
            .await?
            .value
            .ok_or("generation-only projection lost the operation")?
            .id(),
        original_generation.id()
    );
    assert!(
        workspace
            .volume.fs.workspace_operation_generation(&workspace.volume, (IdempotencyKey::new()).operation_id(), &mut crate::kernel::allocation::KernelAccounting::standalone(B, &cancellation), &cancellation).await?
            .0
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn original_fingerprint_survives_current_reservation_rotation_and_rejects_changed_data()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("original-fingerprint-current-reservation", B, &cancel)
        .await?
        .value;
    let cancellation = CancellationToken::new();
    let key = IdempotencyKey::new();
    let context = crate::Digest::from_bytes([0xc8; 32]);
    let evidence = acyclic_stream::OwnedBytes::from_static(b"the original frozen SDK witness");
    let historical_permit = crate::PublicationPermit::Unrestricted;
    let mut transaction = workspace.begin_transaction(key, B, &cancel).await?.value;
    transaction
        .write(
            "/original",
            bytes::Bytes::from_static(b"original content"),
            B,
            &cancel,
        )
        .await?
        .value;
    let first = workspace
        .reserve_publication(IdempotencyKey::new().operation_id(), B, &cancel)
        .await?
        .value;
    let current_permit = crate::PublicationPermit::Reservation {
        operation_id: first.operation_id.into_bytes(),
        gate_tail: first.gate_tail,
        expected: first.expected,
    };
    let mut allow = allow::<WorkspaceError>;
    let committed = transaction
        .commit(
            Some((context, evidence.clone(), historical_permit)),
            current_permit,
            B,
            &cancellation,
            &mut allow,
        )
        .await?;
    let generation = match committed.value {
        TransactionCommit::Committed(generation)
        | TransactionCommit::AlreadyCommitted(generation) => generation,
        _ => return Err("the current genuine reservation did not authorize publication".into()),
    };
    workspace
        .release_publication(first, B, &cancel)
        .await?
        .value;
    let rotated = workspace
        .reserve_publication(IdempotencyKey::new().operation_id(), B, &cancel)
        .await?
        .value;
    let rotated_permit = crate::PublicationPermit::Reservation {
        operation_id: rotated.operation_id.into_bytes(),
        gate_tail: rotated.gate_tail,
        expected: rotated.expected,
    };
    assert_ne!(rotated_permit, current_permit);
    let recovered = transaction
        .checkout
        .retry_stale_commit(
            key.operation_id(),
            rotated_permit,
            B,
            &cancellation,
            &mut allow,
            None,
            Some(context),
            Some(evidence.clone()),
            Some(historical_permit),
        )
        .await?;
    assert!(
        matches!(recovered.value, Some(crate::LivePublicationObservation::AlreadyCommitted { generation_id, .. })
        if generation_id == generation.id())
    );
    let changed_evidence = transaction
        .checkout
        .retry_stale_commit(
            key.operation_id(),
            rotated_permit,
            B,
            &cancellation,
            &mut allow,
            None,
            Some(context),
            Some(acyclic_stream::OwnedBytes::from_static(
                b"a changed SDK witness",
            )),
            Some(historical_permit),
        )
        .await?;
    assert!(matches!(
        changed_evidence.value,
        Some(crate::LivePublicationObservation::IdempotencyConflict { .. })
    ));
    transaction
        .write(
            "/original",
            bytes::Bytes::from_static(b"changed candidate content"),
            B,
            &cancel,
        )
        .await?
        .value;
    let changed_payload = transaction
        .checkout
        .retry_stale_commit(
            key.operation_id(),
            rotated_permit,
            B,
            &cancellation,
            &mut allow,
            None,
            Some(context),
            Some(evidence),
            Some(historical_permit),
        )
        .await?;
    assert!(matches!(
        changed_payload.value,
        Some(crate::LivePublicationObservation::IdempotencyConflict { .. })
    ));
    assert_eq!(
        workspace.head(B, &cancel).await?.value.id(),
        generation.id()
    );
    workspace
        .release_publication(rotated, B, &cancel)
        .await?
        .value;
    Ok(())
}

#[tokio::test]
async fn original_fingerprint_metadata_never_authorizes_a_foreign_current_reservation()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("original-fingerprint-current-target", B, &cancel)
        .await?
        .value;
    let foreign = fs
        .create_workspace("original-fingerprint-foreign-current-grant", B, &cancel)
        .await?
        .value;
    let cancellation = CancellationToken::new();
    let original_head = workspace.head(B, &cancel).await?.value;
    let key = IdempotencyKey::new();
    let mut transaction = workspace.begin_transaction(key, B, &cancel).await?.value;
    transaction
        .write(
            "/value",
            bytes::Bytes::from_static(b"the genuine candidate"),
            B,
            &cancel,
        )
        .await?
        .value;
    let target_reservation = workspace
        .reserve_publication(IdempotencyKey::new().operation_id(), B, &cancel)
        .await?
        .value;
    let foreign_reservation = foreign
        .reserve_publication(IdempotencyKey::new().operation_id(), B, &cancel)
        .await?
        .value;
    let foreign_permit = crate::PublicationPermit::Reservation {
        operation_id: foreign_reservation.operation_id.into_bytes(),
        gate_tail: foreign_reservation.gate_tail,
        expected: foreign_reservation.expected,
    };
    let rejected = transaction
        .commit(
            Some((
                crate::Digest::from_bytes([0xc9; 32]),
                acyclic_stream::OwnedBytes::from_static(b"valid original opaque SDK witness"),
                crate::PublicationPermit::Unrestricted,
            )),
            foreign_permit,
            B,
            &cancellation,
            allow::<WorkspaceError>,
        )
        .await
        .err()
        .ok_or("historical fingerprint metadata authorized a foreign current grant")?;
    assert!(matches!(
        rejected.error,
        WorkspaceError::Filesystem(crate::FsError::Authority(
            crate::AuthorityStoreError::RejectedStatic(_)
        ))
    ));
    assert!(rejected.work.bytes_hashed > 0);
    assert_eq!(rejected.work.authority_records_appended, 0);
    assert_eq!(
        workspace.head(B, &cancel).await?.value.id(),
        original_head.id()
    );
    assert!(
        workspace
            .volume.fs.workspace_operation_generation(&workspace.volume, (key).operation_id(), &mut crate::kernel::allocation::KernelAccounting::standalone(B, &cancellation), &cancellation).await?
            .0
            .is_none()
    );
    workspace
        .release_publication(target_reservation, B, &cancel)
        .await?
        .value;
    foreign
        .release_publication(foreign_reservation, B, &cancel)
        .await?
        .value;
    Ok(())
}
