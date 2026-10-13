use super::*;

#[tokio::test]
async fn original_fork_retry_after_both_heads_advance_needs_no_publication_seal()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let parent = unbounded!(fs, create_workspace("original-fork-parent"))?;
    parent.write_text("/file", "original").await?;
    let frozen = unbounded!(parent, head())?;
    let key = IdempotencyKey::new();
    let cancellation = CancellationToken::new();
    let budget = selected_transaction_budget();
    let first = parent
        .fork(
            "original-fork-child",
            ForkOptions::from_generation(frozen.clone(), key),
            crate::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            allow,
        )
        .await?;
    let child = first.value;
    let original_child = unbounded!(child, head())?;
    parent.write_text("/file", "later parent").await?;
    child.write_text("/file", "later child").await?;
    let advanced_child = unbounded!(child, head())?;
    let retry = parent
        .fork(
            "original-fork-child",
            ForkOptions::from_generation(frozen, key),
            crate::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            |_| {
                std::future::ready(Err(crate::OperationFailure::before_work(
                    crate::FsError::CreationRejected,
                )))
            },
        )
        .await?;
    retry.work.verify(budget)?;

    assert_eq!(unbounded!(retry.value, head())?.id(), advanced_child.id());
    assert_eq!(
        unbounded!(retry.value, read("/file", 32))?.as_ref(),
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
        unbounded!(creation, read("/file", 32))?.as_ref(),
        b"original"
    );
    Ok(())
}

#[tokio::test]
async fn contextual_operation_record_retains_original_witness_after_head_advances()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("original-context-record"))?;
    let cancellation = CancellationToken::new();
    let key = IdempotencyKey::new();
    let original_evidence: &'static [u8] = b"opaque original SDK operation witness";
    let context = crate::Digest::from_bytes([0xc7; 32]);
    let mut transaction = unbounded!(workspace, begin_transaction(key))?;
    unbounded!(
        transaction,
        write(
            "/original",
            bytes::Bytes::from_static(b"original generation")
        )
    )?;
    let committed = transaction
        .commit(
            Some((
                context,
                acyclic_stream::OwnedBytes::from_static(original_evidence),
                crate::PublicationPermit::Unrestricted,
            )),
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancellation,
            |_| {
                std::future::ready(Ok::<_, crate::OperationFailure<WorkspaceError>>(
                    crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    },
                ))
            },
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
    let current = unbounded!(workspace, head())?;
    assert_ne!(current.id(), original_generation.id());
    let observed = workspace
        .operation_generation_record(key, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    let (generation, record) = observed.value.0.ok_or("original operation disappeared")?;
    assert_eq!(generation.id(), original_generation.id());
    assert_eq!(record.operation_id, key.operation_id());
    let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
        WorkBudget::UNBOUNDED,
        &cancellation,
    );
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
    assert_eq!(
        workspace
            .operation_generation(key, WorkBudget::UNBOUNDED, &cancellation,)
            .await?
            .value
            .ok_or("generation-only projection lost the operation")?
            .id(),
        original_generation.id()
    );
    assert!(
        workspace
            .operation_generation_record(
                IdempotencyKey::new(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value
            .0
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn original_fingerprint_survives_current_reservation_rotation_and_rejects_changed_data()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(
        fs,
        create_workspace("original-fingerprint-current-reservation")
    )?;
    let cancellation = CancellationToken::new();
    let key = IdempotencyKey::new();
    let context = crate::Digest::from_bytes([0xc8; 32]);
    let evidence = acyclic_stream::OwnedBytes::from_static(b"the original frozen SDK witness");
    let historical_permit = crate::PublicationPermit::Unrestricted;
    let mut transaction = unbounded!(workspace, begin_transaction(key))?;
    unbounded!(
        transaction,
        write("/original", bytes::Bytes::from_static(b"original content"))
    )?;
    let first = unbounded!(
        workspace,
        reserve_publication(IdempotencyKey::new().operation_id())
    )?;
    let current_permit = crate::PublicationPermit::Reservation {
        operation_id: first.operation_id.into_bytes(),
        gate_tail: first.gate_tail,
        expected: first.expected,
    };
    let committed = transaction
        .commit(
            Some((context, evidence.clone(), historical_permit)),
            current_permit,
            WorkBudget::UNBOUNDED,
            &cancellation,
            allow::<WorkspaceError>,
        )
        .await?;
    let generation = match committed.value {
        TransactionCommit::Committed(generation)
        | TransactionCommit::AlreadyCommitted(generation) => generation,
        _ => return Err("the current genuine reservation did not authorize publication".into()),
    };
    unbounded!(workspace, release_publication(first))?;
    let rotated = unbounded!(
        workspace,
        reserve_publication(IdempotencyKey::new().operation_id())
    )?;
    let rotated_permit = crate::PublicationPermit::Reservation {
        operation_id: rotated.operation_id.into_bytes(),
        gate_tail: rotated.gate_tail,
        expected: rotated.expected,
    };
    assert_ne!(rotated_permit, current_permit);
    let recovered = transaction
        .commit::<WorkspaceError, _, _>(
            Some((context, evidence.clone(), historical_permit)),
            rotated_permit,
            WorkBudget::UNBOUNDED,
            &cancellation,
            allow,
        )
        .await?;
    assert!(
        matches!(recovered.value, TransactionCommit::AlreadyCommitted(ref recovered) if recovered.id() == generation.id())
    );
    let changed_evidence = transaction
        .commit::<WorkspaceError, _, _>(
            Some((
                context,
                OwnedBytes::from_static(b"a changed SDK witness"),
                historical_permit,
            )),
            rotated_permit,
            WorkBudget::UNBOUNDED,
            &cancellation,
            allow,
        )
        .await?;
    assert!(matches!(
        changed_evidence.value,
        TransactionCommit::IdempotencyConflict
    ));
    unbounded!(
        transaction,
        write(
            "/original",
            bytes::Bytes::from_static(b"changed candidate content")
        )
    )?;
    let changed_payload = transaction
        .commit::<WorkspaceError, _, _>(
            Some((context, evidence, historical_permit)),
            rotated_permit,
            WorkBudget::UNBOUNDED,
            &cancellation,
            allow,
        )
        .await?;
    assert!(matches!(
        changed_payload.value,
        TransactionCommit::IdempotencyConflict
    ));
    assert_eq!(unbounded!(workspace, head())?.id(), generation.id());
    unbounded!(workspace, release_publication(rotated))?;
    Ok(())
}

#[tokio::test]
async fn original_fingerprint_metadata_never_authorizes_a_foreign_current_reservation()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("original-fingerprint-current-target"))?;
    let foreign = unbounded!(
        fs,
        create_workspace("original-fingerprint-foreign-current-grant")
    )?;
    let cancellation = CancellationToken::new();
    let original_head = unbounded!(workspace, head())?;
    let key = IdempotencyKey::new();
    let mut transaction = unbounded!(workspace, begin_transaction(key))?;
    unbounded!(
        transaction,
        write(
            "/value",
            bytes::Bytes::from_static(b"the genuine candidate")
        )
    )?;
    let target_reservation = unbounded!(
        workspace,
        reserve_publication(IdempotencyKey::new().operation_id())
    )?;
    let foreign_reservation = unbounded!(
        foreign,
        reserve_publication(IdempotencyKey::new().operation_id())
    )?;
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
            WorkBudget::UNBOUNDED,
            &cancellation,
            |_| {
                std::future::ready(Ok::<_, crate::OperationFailure<WorkspaceError>>(
                    crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    },
                ))
            },
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

    assert_eq!(unbounded!(workspace, head())?.id(), original_head.id());
    assert!(
        workspace
            .operation_generation_record(key, WorkBudget::UNBOUNDED, &cancellation,)
            .await?
            .value
            .0
            .is_none()
    );
    unbounded!(workspace, release_publication(target_reservation))?;
    unbounded!(foreign, release_publication(foreign_reservation))?;
    Ok(())
}
