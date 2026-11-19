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
