use super::*;

#[tokio::test]
async fn complete_generation_publishes_and_retries_exactly() -> TestResult {
    let objects = std::sync::Arc::new(MemoryObjectStore::default());
    let authority = std::sync::Arc::new(MemoryAuthorityStore::default());
    let volume_id = VolumeId::from_bytes([2; 16]);
    let authority_id = volume_authority_id(volume_id);
    let head = initialized(&objects, &authority, volume_id).await?;
    let request = PublishGenerationRequest {
        authority_id,
        volume_id,
        epoch: Epoch::GENESIS,
        expected: head,
        operation_id: OperationId::from_bytes([4; 16]),
        generation_root: fixture(&objects, volume_id).await?,
    };
    let cancellation = CancellationToken::new();
    let publish = |request| {
        publish_generation(
            crate::Fs::new(
                authority.clone(),
                objects.clone(),
                crate::EmbeddedCapabilities::MEMORY,
            ),
            request,
            PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
            limits(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
    };
    let first = publish(request).await?;
    assert!(matches!(&first.outcome, AppendOutcome::Committed(_)));
    let retry = publish(request).await?;
    assert!(matches!(retry.outcome, AppendOutcome::AlreadyCommitted(_)));
    assert!(retry.work.authority_records_read > 0);
    let conflicting_expected_epoch = publish(PublishGenerationRequest {
        expected: Head {
            epoch: Epoch::new(2)?,
            ..head
        },
        ..request
    })
    .await?;
    assert!(matches!(
        conflicting_expected_epoch.outcome,
        AppendOutcome::IdempotencyConflict { .. }
    ));
    let committed_head = match first.outcome {
        AppendOutcome::Committed(commit) => Head {
            epoch: commit.epoch,
            sequence: commit.sequence,
            digest: commit.digest,
        },
        _ => return Err("first publication was not committed".into()),
    };
    assert!(matches!(
        fenced(&authority, authority_id, committed_head).await?,
        FenceOutcome::Advanced(_)
    ));
    let post_fence_retry = publish(request).await?;
    assert!(matches!(
        post_fence_retry.outcome,
        AppendOutcome::AlreadyCommitted(_)
    ));
    let post_fence_altered = publish(PublishGenerationRequest {
        expected: Head {
            epoch: Epoch::new(2)?,
            ..head
        },
        ..request
    })
    .await?;
    assert!(matches!(
        post_fence_altered.outcome,
        AppendOutcome::IdempotencyConflict { .. }
    ));
    let post_fence_fresh = publish(PublishGenerationRequest {
        operation_id: OperationId::from_bytes([5; 16]),
        ..request
    })
    .await?;
    assert!(matches!(
        post_fence_fresh.outcome,
        AppendOutcome::Fenced { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn invalid_and_foreign_closures_never_mutate_authority() -> TestResult {
    for (volume, root_volume, operation) in [(6, None, 7), (51, Some(52), 53)] {
        let objects = std::sync::Arc::new(MemoryObjectStore::default());
        let authority = std::sync::Arc::new(MemoryAuthorityStore::default());
        let volume_id = VolumeId::from_bytes([volume; 16]);
        let authority_id = volume_authority_id(volume_id);
        let expected = created(&authority, authority_id).await?;
        let generation_root = match root_volume {
            Some(actual) => fixture(&objects, VolumeId::from_bytes([actual; 16])).await?,
            None => ObjectId {
                kind: ObjectKind::GenerationRoot,
                digest: Digest::from_bytes([8; 32]),
            },
        };
        let failure = publish_generation(
            crate::Fs::new(
                authority.clone(),
                objects.clone(),
                crate::EmbeddedCapabilities::MEMORY,
            ),
            PublishGenerationRequest {
                authority_id,
                volume_id,
                epoch: Epoch::GENESIS,
                expected,
                operation_id: OperationId::from_bytes([operation; 16]),
                generation_root,
            },
            PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
            limits(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("invalid closure published")?;
        match root_volume {
            None => assert!(matches!(
                failure.error,
                PublicationError::Closure(ClosureError::Storage(_))
            )),
            Some(actual_volume) => {
                assert!(
                    matches!(failure.error, PublicationError::VolumeMismatch { expected, actual }
                    if expected == volume_id && actual == VolumeId::from_bytes([actual_volume; 16]))
                );
                assert!(failure.work.backend_read_operations > 0);
            }
        }
        assert_eq!(failure.work.authority_records_appended, 0);
        assert_eq!(read_head(&authority, authority_id).await?, expected);
    }
    Ok(())
}

#[tokio::test]
async fn mismatched_authority_and_precancellation_are_inert() -> TestResult {
    for cancelled in [false, true] {
        let objects = std::sync::Arc::new(MemoryObjectStore::default());
        let authority = std::sync::Arc::new(MemoryAuthorityStore::default());
        let volume_id = VolumeId::from_bytes([if cancelled { 22 } else { 41 }; 16]);
        let authority_id = if cancelled {
            volume_authority_id(volume_id)
        } else {
            AuthorityId::from_bytes([42; 16])
        };
        let expected = if cancelled {
            created(&authority, authority_id).await?
        } else {
            Head::genesis(Epoch::GENESIS)
        };
        let generation_root = if cancelled {
            fixture(&objects, volume_id).await?
        } else {
            ObjectId {
                kind: ObjectKind::GenerationRoot,
                digest: Digest::from_bytes([44; 32]),
            }
        };
        let cancellation = CancellationToken::new();
        if cancelled {
            cancellation.cancel();
        }
        let failure = publish_generation(
            crate::Fs::new(
                authority.clone(),
                objects.clone(),
                crate::EmbeddedCapabilities::MEMORY,
            ),
            PublishGenerationRequest {
                authority_id,
                volume_id,
                epoch: Epoch::GENESIS,
                expected,
                operation_id: OperationId::from_bytes([if cancelled { 24 } else { 43 }; 16]),
                generation_root,
            },
            PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
            limits(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .err()
        .ok_or("inert publication ran")?;
        if cancelled {
            assert!(matches!(
                failure.error,
                PublicationError::Authority(crate::AuthorityStoreError::Cancelled)
            ));
            assert_eq!(read_head(&authority, authority_id).await?, expected);
        } else {
            assert!(matches!(
                failure.error,
                PublicationError::AuthorityMismatch { .. }
            ));
        }
        assert_eq!(*failure.work, WorkCounters::UNCHARGED);
    }
    Ok(())
}

#[tokio::test]
async fn publication_encoding_copy_and_hash_are_admitted_before_authority_mutation() -> TestResult {
    let objects = std::sync::Arc::new(MemoryObjectStore::default());
    let authority = std::sync::Arc::new(MemoryAuthorityStore::default());
    let volume_id = VolumeId::from_bytes([45; 16]);
    let authority_id = volume_authority_id(volume_id);
    let head = created(&authority, authority_id).await?;
    let generation_root = fixture(&objects, volume_id).await?;
    let closure = prove_generation_closure(
        &objects,
        generation_root,
        limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    let request = PublishGenerationRequest {
        authority_id,
        volume_id,
        epoch: Epoch::GENESIS,
        expected: head,
        operation_id: OperationId::from_bytes([46; 16]),
        generation_root,
    };
    for counter in [
        "bytes_copied",
        "bytes_hashed",
        "bytes_encoded",
        "allocation_operations",
        "peak_allocation_bytes",
    ] {
        let mut budget = WorkBudget::UNBOUNDED;
        match counter {
            "bytes_copied" => budget.bytes_copied = closure.work.bytes_copied,
            "bytes_hashed" => budget.bytes_hashed = closure.work.bytes_hashed,
            "bytes_encoded" => budget.bytes_encoded = closure.work.bytes_encoded,
            "allocation_operations" => {
                budget.allocation_operations = closure.work.allocation_operations;
            }
            "peak_allocation_bytes" => {
                budget.peak_allocation_bytes = closure.work.peak_allocation_bytes.saturating_sub(1);
            }
            _ => return Err("unknown test counter".into()),
        }
        let failure = publish_generation(
            crate::Fs::new(
                authority.clone(),
                objects.clone(),
                crate::EmbeddedCapabilities::MEMORY,
            ),
            request,
            PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
            limits(),
            budget,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("unadmitted publication succeeded")?;
        assert!(matches!(
            failure.error,
            PublicationError::Work(_) | PublicationError::Closure(ClosureError::Work(_))
        ));
        assert_eq!(read_head(&authority, authority_id).await?, head);
    }
    Ok(())
}

#[tokio::test]
async fn missing_creation_record_returns_change_validation_without_appending() -> TestResult {
    let objects = std::sync::Arc::new(MemoryObjectStore::default());
    let authority = std::sync::Arc::new(MemoryAuthorityStore::default());
    let volume_id = VolumeId::from_bytes([94; 16]);
    let authority_id = volume_authority_id(volume_id);
    let expected = created(&authority, authority_id).await?;
    let failure = publish_generation(
        crate::Fs::new(
            authority.clone(),
            objects.clone(),
            crate::EmbeddedCapabilities::MEMORY,
        ),
        PublishGenerationRequest {
            authority_id,
            volume_id,
            expected,
            epoch: Epoch::GENESIS,
            operation_id: OperationId::from_bytes([95; 16]),
            generation_root: fixture(&objects, volume_id).await?,
        },
        PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
        limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .expect_err("a bare authority has no authenticated volume configuration");
    assert!(
        matches!(failure.error, PublicationError::ChangeValidation(error) if matches!(*error, crate::FsError::InvalidDiff))
    );
    assert_eq!(read_head(&authority, authority_id).await?, expected);
    Ok(())
}
