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
    let (decision, allocation) = local.rebase_head(16, &original, &cancel).await?;
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
async fn direct_live_conflicts_return_before_explicit_rebase() -> TestResult {
    let cancel = CancellationToken::new();
    let (optimistic, _) = fixture(config(), &cancel).await?;
    let rejected = optimistic
        .checkout(GenerationSelector::Head, writable_live(), B, &cancel)
        .await
        .err()
        .ok_or("optimistic live admitted")?;
    assert!(matches!(
        rejected.error,
        FsError::LiveRequiresSerializedAuthority
    ));
    let (volume, mut seed) = fixture(serialized_config(), &cancel).await?;
    create(
        &mut seed,
        "file",
        OwnedBytes::from(vec![b'a'; 128]),
        &cancel,
    )
    .await?;
    commit(&mut seed, &cancel).await?;
    for overlap in [false, true] {
        let mut first = open_checkout(&volume, writable_live(), &cancel).await?;
        let mut second = open_checkout(&volume, writable_live(), &cancel).await?;
        let writes = if overlap {
            [(10, b"RACING".as_slice()), (12, b"OVERLAP".as_slice())]
        } else {
            [(0, b"FIRST".as_slice()), (64, b"SECOND".as_slice())]
        };
        let mut blobs = Vec::new();
        for (checkout, (_, bytes)) in [(&mut first, writes[0]), (&mut second, writes[1])] {
            blobs.push(
                checkout
                    .stage_content(
                        &mut std::io::Cursor::new(bytes),
                        bytes.len() as u64,
                        B,
                        &cancel,
                    )
                    .await?
                    .value,
            );
        }
        for (index, checkout) in [&mut first, &mut second].into_iter().enumerate() {
            let blob = &blobs[index];
            checkout
                .mutate(
                    vec![Mutation::Write {
                        path: path("file")?,
                        offset: writes[index].0,
                        length: blob.logical_bytes,
                        content: blob.root,
                        content_offset: 0,
                    }],
                    B,
                    &cancel,
                )
                .await?;
        }
        assert!(matches!(
            first
                .resume_live(OperationId::new(), B, &cancel)
                .await?
                .value,
            LiveMutationOutcome::Committed { .. }
        ));
        let operation = OperationId::new();
        let before = second.generation_id();
        let conflict = second.resume_live(operation, B, &cancel).await?;
        let LiveMutationOutcome::Conflict { actual } = conflict.value else {
            return Err("stale publication did not return its original conflict".into());
        };
        assert_eq!(second.generation_id(), before);
        assert!(second.has_pending_mutations());
        assert_eq!(
            second.resume_live(operation, B, &cancel).await?.value,
            LiveMutationOutcome::Conflict { actual }
        );
        let rebase = second.rebase_head(8, B, &cancel).await?.value;
        if overlap {
            assert!(
                matches!(rebase, RebaseDecision::Conflicted { conflicts, .. }
                if conflicts.iter().any(|conflict| matches!(conflict.region, DependencyRegion::ContentRange { .. })))
            );
            assert_eq!(second.generation_id(), before);
        } else {
            assert!(matches!(rebase, RebaseDecision::Safe { .. }));
            assert!(matches!(
                second.resume_live(operation, B, &cancel).await?.value,
                LiveMutationOutcome::Committed { .. }
            ));
        }
    }
    Ok(())
}

#[tokio::test]
async fn explicit_live_refresh_advances_only_across_unobserved_regions() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(serialized_config(), &cancel).await?;
    let id = create(
        &mut seed,
        "refresh",
        OwnedBytes::from(vec![b'a'; 128]),
        &cancel,
    )
    .await?;
    commit(&mut seed, &cancel).await?;
    let mut live = open_checkout(&volume, writable_live(), &cancel).await?;
    for (offset, bytes) in [(64, b"SAFE"), (0, b"RACE")] {
        assert_eq!(read(&mut live, id, 0, 4, &cancel).await?.as_ref(), b"aaaa");
        let before = live.generation_id();
        let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
        remote
            .change_content(
                path("refresh")?,
                ContentChange::Write {
                    offset,
                    bytes: OwnedBytes::from_static(bytes),
                },
                ContentTimes::Preserve,
                B,
                &cancel,
            )
            .await?;
        commit(&mut remote, &cancel).await?;
        if offset == 0 {
            let conflict = live.refresh_live(B, &cancel).await.unwrap_err();
            assert!(
                matches!(conflict.error, FsError::LiveConflict { conflicts, .. }
                if conflicts.iter().any(|conflict| matches!(conflict.region, DependencyRegion::ContentRange { .. })))
            );
            assert_eq!(live.generation_id(), before);
        } else {
            assert_eq!(
                live.refresh_live(B, &cancel).await?.value,
                remote.generation_id()
            );
            assert_eq!(live.generation_id(), remote.generation_id());
        }
    }
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
        retrying.rebase_head(8, B, &cancel).await?.value,
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

#[tokio::test]
async fn authored_live_transaction_preserves_create_ids_and_publishes_atomically() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, _) = fixture(serialized_config(), &cancel).await?;
    let mut checkout = open_checkout(&volume, writable_live(), &cancel).await?;
    let file = path("authored-live.txt")?;
    let applied = checkout
        .apply_authored_live(
            vec![
                AuthoredMutation::CreateFile {
                    path: file.clone(),
                    bytes: OwnedBytes::from_static(b"initial"),
                    metadata: FileMetadata::default(),
                },
                AuthoredMutation::Write {
                    path: file.clone(),
                    offset: 7,
                    bytes: OwnedBytes::from_static(b"-published"),
                },
            ],
            OperationId::from_bytes([118; 16]),
            B,
            &cancel,
        )
        .await?
        .value;
    assert!(matches!(
        applied.outcome,
        LiveMutationOutcome::Committed { .. }
    ));
    assert_eq!(applied.created_file_ids.len(), 2);
    assert!(applied.created_file_ids[0].is_some());
    assert!(applied.created_file_ids[1].is_none());
    assert!(!checkout.has_pending_mutations());
    assert_eq!(
        checkout
            .read_file_range(
                &file,
                ByteRange {
                    offset: 0,
                    length: 17
                },
                B,
                &cancel
            )
            .await?
            .value
            .bytes
            .as_ref(),
        b"initial-published"
    );
    Ok(())
}
