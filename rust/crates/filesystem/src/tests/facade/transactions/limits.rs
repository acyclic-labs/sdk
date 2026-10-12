use super::*;

#[tokio::test]
async fn public_byte_boundaries_reject_before_allocation_or_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 4;
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([189; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let file = path("bounded")?;
    let failure = (checkout.create(
        crate::AuthoredMutation::CreateFile {
            path: file.clone(),
            bytes: (bytes::Bytes::from_static(b"12345")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("oversized create succeeded")?;
    assert!(matches!(
        failure.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*failure.work, WorkCounters::default());

    let file_id = (checkout.create(
        crate::AuthoredMutation::CreateFile {
            path: file.clone(),
            bytes: (bytes::Bytes::from_static(b"1234")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    for failure in [
        (checkout.change_content(
            file.clone(),
            crate::ContentChange::Write {
                offset: 0,
                bytes: (bytes::Bytes::from_static(b"12345")).into(),
            },
            crate::ContentTimes::Preserve,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
        .err()
        .ok_or("oversized path write succeeded")?,
        (checkout.change_content(
            file_id,
            crate::ContentChange::Write {
                offset: 0,
                bytes: (bytes::Bytes::from_static(b"12345")).into(),
            },
            crate::ContentTimes::Preserve,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
        .err()
        .ok_or("oversized identity write succeeded")?,
    ] {
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    (checkout.change_content(
        file_id,
        crate::ContentChange::Write {
            offset: 4,
            bytes: (bytes::Bytes::from_static(b"5")).into(),
        },
        crate::ContentTimes::Preserve,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;

    for failure in [
        (checkout.read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 5,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
        .err()
        .ok_or("oversized path read succeeded")?,
        (checkout.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 0,
                length: 5,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
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
        (checkout.plan_file_extents(
            &file,
            excessive_range,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
        .err()
        .ok_or("oversized path plan succeeded")?,
        (checkout.plan_file_extents_by_id(
            file_id,
            excessive_range,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
        .err()
        .ok_or("oversized identity plan succeeded")?,
    ] {
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let symbolic = (checkout.create(
        crate::AuthoredMutation::CreateSymbolicLink {
            path: path("link")?,
            target: (bytes::Bytes::from_static(b"12345")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("oversized symbolic link succeeded")?;
    assert!(matches!(
        symbolic.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*symbolic.work, WorkCounters::default());

    let mut detached = (checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &cancellation))
        .await?
        .value;
    let detached_read_failure = (detached.read_range(
        ByteRange {
            offset: 0,
            length: 5,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("oversized detached read succeeded")?;
    assert!(matches!(
        detached_read_failure.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*detached_read_failure.work, WorkCounters::default());
    let detached_failure = (detached.change_content(
        ContentChange::Write {
            offset: 0,
            bytes: OwnedBytes::from_static(b"12345"),
        },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("oversized detached write succeeded")?;
    assert!(matches!(
        detached_failure.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*detached_failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn checkout_generation_and_mode_guards_reject_without_candidate_damage()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let first = (fs.create_volume_with_id(
        VolumeId::from_bytes([119; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let second = (fs.create_volume_with_id(
        VolumeId::from_bytes([120; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let first_head = (first.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value
    .generation_id();
    let second_head = (second.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value
    .generation_id();
    assert!(
        (first.checkout(
            GenerationSelector::Exact(first_head),
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
        .is_err()
    );
    assert!(matches!(
        (first.checkout(
            GenerationSelector::Exact(second_head),
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await,
        Err(OperationFailure {
            error: FsError::VolumeMismatch,
            ..
        })
    ));
    assert!(
        (first.diff_generations(
            first_head,
            second_head,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
        .is_err()
    );

    let mut read_only = (first.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    assert!(
        (read_only.mutate(Vec::new(), WorkBudget::UNBOUNDED, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (read_only.commit(
            OperationId::new(),
            None,
            None,
            None,
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancellation,
            |_| async {
                crate::FsResult::Ok(crate::FsReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                })
            }
        ))
        .await
        .is_err()
    );
    assert!(
        (read_only.discard(WorkBudget::UNBOUNDED, &cancellation))
            .await
            .is_err()
    );
    assert!(
        (read_only.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
            .await
            .is_err()
    );
    assert!(
        (read_only.refresh_live(WorkBudget::UNBOUNDED, &cancellation))
            .await
            .is_err()
    );
    assert!(
        (read_only.mutate_live(
            Vec::new(),
            OperationId::new(),
            1,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
        .is_err()
    );
    assert!(
        (read_only.resume_live(
            OperationId::new(),
            1,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
        .is_err()
    );

    let mut writable = (first.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let excessive_capacity = Vec::<Mutation>::with_capacity(
        usize::try_from(config().limits.maximum_mutations_per_batch)? + 1,
    );
    let failure = (writable.mutate(excessive_capacity, WorkBudget::UNBOUNDED, &cancellation))
        .await
        .err()
        .ok_or("excessive mutation capacity unexpectedly succeeded")?;
    assert!(matches!(failure.error, FsError::ExcessiveMutationCapacity));
    assert_eq!(*failure.work, WorkCounters::default());

    let mut clean = (first.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    assert!(
        (clean.commit(
            OperationId::new(),
            None,
            None,
            None,
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancellation,
            |_| async {
                crate::FsResult::Ok(crate::FsReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                })
            }
        ))
        .await
        .is_err()
    );
    assert!(
        (clean.resume_live(
            OperationId::new(),
            1,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
        .is_err()
    );

    let mut manual_checkout = (first.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    (manual_checkout.create(
        crate::AuthoredMutation::CreateFile {
            path: path("dirty")?,
            bytes: (bytes::Bytes::from_static(b"dirty")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(
        (manual_checkout.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
            .await
            .is_err()
    );
    assert!(manual_checkout.has_pending_mutations());
    (manual_checkout.discard(WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(!manual_checkout.has_pending_mutations());
    assert_eq!(
        (manual_checkout.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
            .await?
            .value,
        first_head
    );
    Ok(())
}
