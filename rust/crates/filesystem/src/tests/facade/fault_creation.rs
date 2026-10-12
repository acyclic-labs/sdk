use super::*;

#[tokio::test]
async fn every_authority_backend_cut_preserves_creation_checkout_and_commit_retry()
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
                    ($operation).await;
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
    (checkout.create(crate::AuthoredMutation::CreateFile { path: path("authority-cut-file")?, bytes: (bytes::Bytes::from_static(b"authority cut candidate")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let candidate = checkout.generation_id();
    assert!(checkout.has_pending_mutations());
    let commit_operation = OperationId::from_bytes([33; 16]);
    let mut committed = None;
    for cut in 1..=64 {
        control.arm(cut);
        let outcome =
            (checkout.commit(commit_operation, None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await;
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
    (live.create(crate::AuthoredMutation::CreateFile { path: path("live-authority-cut")?, bytes: (bytes::Bytes::from_static(b"live")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(live.has_pending_mutations());
    let live_operation = OperationId::from_bytes([34; 16]);
    let mut published = None;
    for cut in 1..=64 {
        control.arm(cut);
        let outcome = (live.resume_live(
            live_operation,
            3,
            16,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await;
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

#[tokio::test]
async fn every_post_put_creation_fault_retries_from_a_complete_authenticated_closure()
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
        let first = (fs.create_volume_with_id(
            volume_id,
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await;
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
                let retried = (fs.create_volume_with_id(
                    volume_id,
                    config(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )).await?;
                assert_eq!(retried.value.id(), volume_id);
                let reopened =
                    (fs.open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation)).await?;
                let checkout = (reopened.value.checkout(
                    GenerationSelector::Head,
                    pinned(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )).await?;
                assert_eq!(checkout.value.volume_id(), volume_id);
                assert!(!checkout.value.has_pending_mutations());
            }
        }
    }
    assert!(reached_clean_completion);
    Ok(())
}

#[tokio::test]
async fn every_pre_object_fault_preserves_creation_and_checkout_retry()
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
        match (fs.create_volume_with_id(
            volume_id,
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
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
        match (volume.checkout(
            GenerationSelector::Head,
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
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
