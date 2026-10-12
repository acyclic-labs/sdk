use super::*;

#[tokio::test]
async fn generation_diff_is_semantic_bounded_and_equal_root_constant_work()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            ($future).await?
        };
    }
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume_with_id(
        VolumeId::from_bytes([73; 16]),
        config(),
        B,
        &cancellation,
    ))
    .value;
    let mut checkout = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .value;
    let initial = checkout.generation_id();
    ready!(checkout.create(
        AuthoredMutation::CreateFile {
            path: path("changed")?,
            bytes: OwnedBytes::from_static(b"one"),
            metadata: FileMetadata::default()
        },
        B,
        &cancellation
    ));
    let first = match ready!(checkout.commit(
        OperationId::new(),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>
    ))
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected first commit: {outcome:?}").into()),
    };
    let invalid = (volume.diff_generations(initial, first, 0, B, &cancellation))
        .await
        .err()
        .ok_or("zero-bound generation diff unexpectedly succeeded")?;
    assert!(matches!(invalid.error, FsError::InvalidDiff));
    assert_eq!(*invalid.work, WorkCounters::default());
    ready!(checkout.change_content(
        path("changed")?,
        ContentChange::Write {
            offset: 0,
            bytes: OwnedBytes::from_static(b"two")
        },
        ContentTimes::Preserve,
        B,
        &cancellation
    ));
    let second = match ready!(checkout.commit(
        OperationId::new(),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>
    ))
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected second commit: {outcome:?}").into()),
    };

    let created = ready!(volume.diff_generations(initial, first, 16, B, &cancellation,));
    assert!(!created.value.truncated);
    assert!(created.value.files.len() >= 2);
    assert_eq!(created.value.bindings.len(), 1);
    let bounded = ready!(volume.diff_generations(initial, first, 1, B, &cancellation,));
    assert!(bounded.value.truncated);
    assert_eq!(bounded.value.files.len(), 1);
    assert!(bounded.value.bindings.is_empty());
    let modified = ready!(volume.diff_generations(first, second, 16, B, &cancellation,));
    assert_eq!(modified.value.files.len(), 1);
    assert!(modified.value.bindings.is_empty());
    let equal = ready!(volume.diff_generations(second, second, 16, B, &cancellation,));
    assert!(equal.value.files.is_empty());
    assert_eq!(equal.work, WorkCounters::default());

    let root_metadata = ready!(checkout.lookup_no_follow(&root_path()?, B, &cancellation,))
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
        B,
        &cancellation,
    ));
    let third = match ready!(checkout.commit(
        OperationId::new(),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>
    ))
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected ordered diff commit: {outcome:?}").into()),
    };
    for maximum in [1_u32, 2] {
        let bounded = ready!(volume.diff_generations(second, third, maximum, B, &cancellation,));
        assert!(bounded.value.truncated);
        assert_eq!(
            bounded.value.files.len() + bounded.value.bindings.len(),
            usize::try_from(maximum)?
        );
    }
    Ok(())
}

#[tokio::test]
async fn three_way_merge_combines_independent_directory_bindings_and_publishes()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            ($future).await?
        };
    }
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume_with_id(
        VolumeId::from_bytes([74; 16]),
        config(),
        B,
        &cancellation,
    ))
    .value;
    let mut ours = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .value;
    let mut theirs = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .value;
    ready!(ours.create(
        AuthoredMutation::CreateFile {
            path: path("ours")?,
            bytes: OwnedBytes::from_static(b"ours"),
            metadata: FileMetadata::default()
        },
        B,
        &cancellation
    ));
    ready!(theirs.create(
        AuthoredMutation::CreateFile {
            path: path("theirs")?,
            bytes: OwnedBytes::from_static(b"theirs"),
            metadata: FileMetadata::default()
        },
        B,
        &cancellation
    ));
    let theirs_generation = match ready!(theirs.commit(
        OperationId::new(),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>
    ))
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected branch commit: {outcome:?}").into()),
    };
    let prepared = ready!(ours.prepare_merge(theirs_generation, 32, 8, B, &cancellation,));
    assert!(matches!(prepared.value, MergePreparation::Prepared { .. }));
    let mutation_during_merge = (ours.mutate(Vec::new(), B, &cancellation))
        .await
        .err()
        .ok_or("prepared merge accepted a mutation")?;
    assert!(matches!(
        mutation_during_merge.error,
        FsError::PreparedMergePending
    ));
    assert!(
        (ours.prepare_merge(theirs_generation, 32, 8, B, &cancellation,))
            .await
            .is_err()
    );
    assert!((ours.rebase_head(8, B, &cancellation)).await.is_err());
    let merged_generation = match ready!(ours.commit(
        OperationId::new(),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>
    ))
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected merge commit: {outcome:?}").into()),
    };
    let mut merged = ready!(volume.checkout(
        GenerationSelector::Exact(merged_generation),
        pinned(),
        B,
        &cancellation,
    ))
    .value;
    assert!(
        ready!(merged.lookup_no_follow(&path("ours")?, B, &cancellation,))
            .value
            .record
            .is_some()
    );
    assert!(
        ready!(merged.lookup_no_follow(&path("theirs")?, B, &cancellation,))
            .value
            .record
            .is_some()
    );
    assert_eq!(merged.root().parents.len(), 2);

    let mut left = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .value;
    let mut right = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .value;
    ready!(left.mutate(
        vec![Mutation::Link {
            source: path("ours")?,
            destination: path("left-link")?
        }],
        B,
        &cancellation
    ));
    ready!(right.mutate(
        vec![Mutation::Link {
            source: path("ours")?,
            destination: path("right-link")?
        }],
        B,
        &cancellation
    ));
    let right_generation = match ready!(right.commit(
        OperationId::new(),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>
    ))
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected hard-link commit: {outcome:?}").into()),
    };
    assert!(matches!(
        ready!(left.prepare_merge(right_generation, 32, 8, B, &cancellation,)).value,
        MergePreparation::Prepared { .. }
    ));
    let links_generation = match ready!(left.commit(
        OperationId::new(),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>
    ))
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected hard-link merge: {outcome:?}").into()),
    };
    let mut links = ready!(volume.checkout(
        GenerationSelector::Exact(links_generation),
        pinned(),
        B,
        &cancellation,
    ))
    .value;
    let source = ready!(links.lookup_no_follow(&path("ours")?, B, &cancellation,))
        .value
        .record
        .ok_or("merged hard-link source missing")?;
    assert_eq!(source.link_count, 3);
    for linked in ["left-link", "right-link"] {
        let record = ready!(links.lookup_no_follow(&path(linked)?, B, &cancellation,))
            .value
            .record
            .ok_or("merged hard link missing")?;
        assert_eq!(record.file_id, source.file_id);
    }
    Ok(())
}

#[tokio::test]
async fn three_way_merge_rejects_guards_and_reports_exact_file_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            ($future).await?
        };
    }
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume_with_id(
        VolumeId::from_bytes([124; 16]),
        config(),
        B,
        &cancellation,
    ))
    .value;
    let mut seed = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .value;
    let file = path("conflict.txt")?;
    let file_id = ready!(seed.create(
        AuthoredMutation::CreateFile {
            path: file.clone(),
            bytes: OwnedBytes::from_static(b"base"),
            metadata: FileMetadata::default()
        },
        B,
        &cancellation
    ))
    .value;
    let second_file = path("second-conflict.txt")?;
    let second_file_id = ready!(seed.create(
        AuthoredMutation::CreateFile {
            path: second_file.clone(),
            bytes: OwnedBytes::from_static(b"base-two"),
            metadata: FileMetadata::default()
        },
        B,
        &cancellation
    ))
    .value;
    let base_generation = match ready!(seed.commit(
        OperationId::from_bytes([125; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>
    ))
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected seed commit: {outcome:?}").into()),
    };
    let mut ours = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .value;
    let mut theirs = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .value;
    let mut read_only =
        ready!(volume.checkout(GenerationSelector::Head, pinned(), B, &cancellation,)).value;
    assert!(
        (read_only.prepare_merge(base_generation, 8, 8, B, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (ours.prepare_merge(base_generation, 0, 8, B, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (ours.prepare_merge(base_generation, 8, 0, B, &cancellation,))
            .await
            .is_err()
    );
    ready!(ours.change_content(
        file.clone(),
        ContentChange::Write {
            offset: 0,
            bytes: OwnedBytes::from_static(b"ours")
        },
        ContentTimes::Preserve,
        B,
        &cancellation
    ));
    ready!(ours.change_content(
        second_file.clone(),
        ContentChange::Write {
            offset: 0,
            bytes: OwnedBytes::from_static(b"ours-two")
        },
        ContentTimes::Preserve,
        B,
        &cancellation
    ));
    ready!(theirs.change_content(
        file,
        ContentChange::Write {
            offset: 0,
            bytes: OwnedBytes::from_static(b"them")
        },
        ContentTimes::Preserve,
        B,
        &cancellation
    ));
    ready!(theirs.change_content(
        second_file,
        ContentChange::Write {
            offset: 0,
            bytes: OwnedBytes::from_static(b"them-two")
        },
        ContentTimes::Preserve,
        B,
        &cancellation
    ));
    let theirs_generation = match ready!(theirs.commit(
        OperationId::from_bytes([126; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>
    ))
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected competing commit: {outcome:?}").into()),
    };
    assert!(
        (ours.prepare_merge(base_generation, 8, 8, B, &cancellation,))
            .await
            .is_err()
    );
    let change_limited = (ours.prepare_merge(theirs_generation, 1, 8, B, &cancellation))
        .await
        .err()
        .ok_or("truncated merge inputs unexpectedly prepared")?;
    assert!(matches!(change_limited.error, FsError::MergeChangeLimit));
    assert!(change_limited.work.backend_read_operations > 0);
    let bounded_conflicts = ready!(ours.prepare_merge(theirs_generation, 8, 1, B, &cancellation,));
    assert!(matches!(
        bounded_conflicts.value,
        MergePreparation::Conflicted {
            ref conflicts,
            truncated: true,
        } if conflicts.len() == 1
            && matches!(conflicts[0], MergeConflict::File(id) if id == file_id || id == second_file_id)
    ));
    let conflicted = ready!(ours.prepare_merge(theirs_generation, 8, 8, B, &cancellation,));
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
