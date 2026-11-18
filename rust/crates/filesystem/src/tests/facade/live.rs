use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_guarded_live_conflict_requires_explicit_rebase() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(serialized_config(), &cancel).await?;
    let left = create(
        &mut seed,
        "left",
        OwnedBytes::from_static(b"left real source"),
        &cancel,
    )
    .await?;
    let right = create(
        &mut seed,
        "right",
        OwnedBytes::from_static(b"distinct right real source"),
        &cancel,
    )
    .await?;
    commit(&mut seed, &cancel).await?;
    let original = crate::NativeMaterializationWork::new(B, &cancel).map_err(|(cause, _)| cause)?;
    let mut local = volume
        .checkout(
            GenerationSelector::Head,
            writable_live(),
            &original,
            &cancel,
        )
        .await?;
    let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
    local
        .change_content(
            left,
            ContentChange::Resize { logical_bytes: 0 },
            ContentTimes::Preserve,
            &original,
            &cancel,
        )
        .await?;
    remote
        .change_content(
            right,
            ContentChange::Resize { logical_bytes: 0 },
            ContentTimes::Preserve,
            B,
            &cancel,
        )
        .await?;
    commit(&mut remote, &cancel).await?;
    let callbacks = Arc::new(AtomicUsize::new(0));
    let calls = callbacks.clone();
    let shared = original.clone();
    let mut guard = move |meter: &crate::NativeMaterializationWork| {
        assert!(meter.shares_ledger(&shared));
        calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok::<(), FsError>(()))
    };
    let conflict = local
        .resume_live_with_native_work(
            OperationId::from_bytes([212; 16]),
            PublicationPermit::Unrestricted,
            &original,
            &cancel,
            &mut guard,
            None,
            None,
            None,
            None,
        )
        .await?;
    assert!(matches!(conflict.0, LiveMutationOutcome::Conflict { .. }));
    assert!(conflict.1.work().shares_ledger(&original));
    let checks_at_conflict = callbacks.load(Ordering::SeqCst);
    assert!(checks_at_conflict <= 1);
    assert!(local.has_pending_mutations());
    drop(conflict);
    let (decision, allocation) = local.rebase_head(Some(16), &original, &cancel).await?;
    assert!(matches!(decision, RebaseDecision::Safe { .. }));
    drop(allocation);
    let outcome = local
        .resume_live_with_native_work(
            OperationId::from_bytes([212; 16]),
            PublicationPermit::Unrestricted,
            &original,
            &cancel,
            &mut guard,
            None,
            None,
            None,
            None,
        )
        .await?;
    assert!(matches!(outcome.0, LiveMutationOutcome::Committed { .. }));
    assert!(outcome.1.work().shares_ledger(&original));
    assert_eq!(callbacks.load(Ordering::SeqCst), checks_at_conflict + 1);
    assert!(!local.has_pending_mutations());
    let left_record = local
        .read_file_record_by_id(left, &original, &cancel)
        .await?;
    let right_record = local
        .read_file_record_by_id(right, &original, &cancel)
        .await?;
    assert_eq!(left_record.file_id, left);
    assert_eq!(right_record.file_id, right);
    assert_eq!(left_record.payload, right_record.payload);
    Ok(())
}

#[tokio::test]
async fn direct_live_resume_preserves_original_identity_and_epoch_fence() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, _) = fixture(serialized_config(), &cancel).await?;
    let mut winner = open_checkout(&volume, writable_live(), &cancel).await?;
    let mut retrying = open_checkout(&volume, writable_live(), &cancel).await?;
    create(
        &mut winner,
        "winner",
        OwnedBytes::from_static(b"winner"),
        &cancel,
    )
    .await?;
    winner.resume_live(OperationId::new(), B, &cancel).await?;
    create(
        &mut retrying,
        "retrying",
        OwnedBytes::from_static(b"retrying"),
        &cancel,
    )
    .await?;
    let retry = OperationId::from_bytes([119; 16]);
    assert!(matches!(
        retrying.resume_live(retry, B, &cancel).await?.value,
        LiveMutationOutcome::Conflict { .. }
    ));
    assert!(retrying.has_pending_mutations());
    assert!(matches!(
        retrying.rebase_head(Some(8), B, &cancel).await?.value,
        RebaseDecision::Safe { .. }
    ));
    assert!(matches!(
        retrying.resume_live(retry, B, &cancel).await?.value,
        LiveMutationOutcome::Committed { .. }
    ));
    assert!(!retrying.has_pending_mutations());
    assert!(matches!(
        retrying
            .resume_live(retry, B, &cancel)
            .await
            .unwrap_err()
            .error,
        FsError::NoPendingMutations
    ));
    let mut pending = open_checkout(&volume, writable_live(), &cancel).await?;
    create(
        &mut pending,
        "pending-guard",
        OwnedBytes::from_static(b"pending"),
        &cancel,
    )
    .await?;
    assert!(matches!(
        pending
            .mutate_live(Vec::new(), OperationId::new(), B, &cancel)
            .await
            .unwrap_err()
            .error,
        FsError::PendingLiveMutation
    ));
    assert!(matches!(
        pending
            .apply_authored_live(Vec::new(), OperationId::new(), B, &cancel)
            .await
            .unwrap_err()
            .error,
        FsError::PendingLiveMutation
    ));
    let mut first = open_checkout(&volume, writable_live(), &cancel).await?;
    let mut repeated = open_checkout(&volume, writable_live(), &cancel).await?;
    let metadata = first
        .lookup_no_follow(&root_path()?, B, &cancel)
        .await?
        .value
        .record
        .ok_or("root absent")?
        .metadata;
    let mutation = Mutation::Create {
        path: path("idempotent")?,
        record: FileRecord {
            bindings: None,
            file_id: FileId::from_bytes([120; 16]),
            kind: FileKind::Regular,
            link_count: 1,
            metadata,
            payload: FilePayload::InlineRegular(InlineFileData::new(b"same")?),
        },
    };
    first.mutate(vec![mutation.clone()], B, &cancel).await?;
    repeated.mutate(vec![mutation], B, &cancel).await?;
    let operation = OperationId::from_bytes([121; 16]);
    assert!(matches!(
        first.resume_live(operation, B, &cancel).await?.value,
        LiveMutationOutcome::Committed { .. }
    ));
    assert!(matches!(
        repeated.resume_live(operation, B, &cancel).await?.value,
        LiveMutationOutcome::AlreadyCommitted { .. }
    ));
    let mut different = open_checkout(&volume, writable_live(), &cancel).await?;
    create(
        &mut different,
        "different-operation",
        OwnedBytes::from_static(b"different"),
        &cancel,
    )
    .await?;
    assert!(matches!(
        different.resume_live(operation, B, &cancel).await?.value,
        LiveMutationOutcome::IdempotencyConflict { .. }
    ));
    assert!(different.has_pending_mutations());
    let mut fenced = open_checkout(&volume, writable_live(), &cancel).await?;
    create(
        &mut fenced,
        "fenced",
        OwnedBytes::from_static(b"fenced"),
        &cancel,
    )
    .await?;
    let authority = &volume.fs.inner.authority;
    let authority_id = volume_authority_id(volume.id);
    let mut work = crate::AuthorityWork::new(B, &cancel);
    let expected = authority.head(authority_id, &mut work).await?;
    let FenceOutcome::Advanced(head) = authority.fence(authority_id, expected, &mut work).await?
    else {
        return Err("fresh fence conflicted".into());
    };
    assert!(
        matches!(fenced.resume_live(OperationId::new(), B, &cancel).await?.value,
        LiveMutationOutcome::Fenced { actual_epoch } if actual_epoch == head.epoch)
    );
    let mut manual = open_checkout(&volume, writable_manual(), &cancel).await?;
    assert!(manual.refresh_live(B, &cancel).await.is_err());
    assert!(
        manual
            .mutate_live(Vec::new(), OperationId::new(), B, &cancel)
            .await
            .is_err()
    );
    Ok(())
}
