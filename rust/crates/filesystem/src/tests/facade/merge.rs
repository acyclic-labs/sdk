use super::*;

#[tokio::test]
async fn generation_diff_is_semantic_bounded_and_equal_root_constant_work()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([73; 16]),
        config(),
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
    let initial = checkout.generation_id();
    (checkout.create(
        crate::AuthoredMutation::CreateFile {
            path: path("changed")?,
            bytes: (bytes::Bytes::from_static(b"one")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let first = match (checkout.commit(
        OperationId::new(),
        None,
        None,
        None,
        crate::PublicationPermit::Unrestricted,
        WorkBudget::UNBOUNDED,
        &cancellation,
        |_| async {
            crate::FsResult::Ok(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::UNCHARGED,
            })
        },
    ))
    .await?
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected first commit: {outcome:?}").into()),
    };
    let invalid =
        (volume.diff_generations(initial, first, 0, WorkBudget::UNBOUNDED, &cancellation))
            .await
            .err()
            .ok_or("zero-bound generation diff unexpectedly succeeded")?;
    assert!(matches!(invalid.error, FsError::InvalidDiff));
    assert_eq!(*invalid.work, WorkCounters::default());
    (checkout.change_content(
        path("changed")?,
        crate::ContentChange::Write {
            offset: 0,
            bytes: (bytes::Bytes::from_static(b"two")).into(),
        },
        crate::ContentTimes::Preserve,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let second = match (checkout.commit(
        OperationId::new(),
        None,
        None,
        None,
        crate::PublicationPermit::Unrestricted,
        WorkBudget::UNBOUNDED,
        &cancellation,
        |_| async {
            crate::FsResult::Ok(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::UNCHARGED,
            })
        },
    ))
    .await?
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected second commit: {outcome:?}").into()),
    };

    let created =
        (volume.diff_generations(initial, first, 16, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(!created.value.truncated);
    assert!(created.value.files.len() >= 2);
    assert_eq!(created.value.bindings.len(), 1);
    let bounded =
        (volume.diff_generations(initial, first, 1, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(bounded.value.truncated);
    assert_eq!(bounded.value.files.len(), 1);
    assert!(bounded.value.bindings.is_empty());
    let modified =
        (volume.diff_generations(first, second, 16, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(modified.value.files.len(), 1);
    assert!(modified.value.bindings.is_empty());
    let equal =
        (volume.diff_generations(second, second, 16, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(equal.value.files.is_empty());
    assert_eq!(equal.work, WorkCounters::default());

    let root_metadata =
        (checkout.lookup_no_follow(&root_path()?, WorkBudget::UNBOUNDED, &cancellation))
            .await?
            .value
            .record
            .ok_or("diff fixture root is absent")?
            .metadata;
    (checkout.mutate(
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
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let third = match (checkout.commit(
        OperationId::new(),
        None,
        None,
        None,
        crate::PublicationPermit::Unrestricted,
        WorkBudget::UNBOUNDED,
        &cancellation,
        |_| async {
            crate::FsResult::Ok(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::UNCHARGED,
            })
        },
    ))
    .await?
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected ordered diff commit: {outcome:?}").into()),
    };
    for maximum in [1_u32, 2] {
        let bounded =
            (volume.diff_generations(second, third, maximum, WorkBudget::UNBOUNDED, &cancellation))
                .await?;
        assert!(bounded.value.truncated);
        assert_eq!(
            bounded.value.files.len() + bounded.value.bindings.len(),
            usize::try_from(maximum)?
        );
    }
    let changed_id =
        (checkout.lookup_no_follow(&path("changed")?, WorkBudget::UNBOUNDED, &cancellation))
            .await?
            .value
            .record
            .ok_or("changed record absent")?
            .file_id;
    (checkout.mutate(
        vec![crate::kernel::Mutation::Link {
            source: path("changed")?,
            destination: path("alias")?,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    (checkout.mutate(
        vec![crate::kernel::Mutation::Rename {
            source: path("changed")?,
            destination: path("renamed")?,
            replace: false,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let mut metadata = (checkout.lookup_no_follow_with_metadata(
        &root_path()?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value
    .ok_or("root metadata absent")?
    .metadata;
    metadata.modified_ns = MetadataField::Value(123);
    (checkout.apply_authored_transaction(
        vec![crate::AuthoredMutation::SetMetadata {
            path: root_path()?,
            metadata: metadata,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let fourth = match (checkout.commit(
        OperationId::new(),
        None,
        None,
        None,
        crate::PublicationPermit::Unrestricted,
        WorkBudget::UNBOUNDED,
        &cancellation,
        |_| async {
            crate::FsResult::Ok(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::UNCHARGED,
            })
        },
    ))
    .await?
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected semantic diff commit: {outcome:?}").into()),
    };
    let semantic =
        (volume.diff_generations(third, fourth, 32, WorkBudget::UNBOUNDED, &cancellation))
            .await?
            .value;
    let file = semantic
        .files
        .iter()
        .find(|change| change.file_id == changed_id)
        .ok_or("hard-link identity change absent")?;
    assert_eq!(
        file.before.ok_or("original record absent")?.file_id,
        changed_id
    );
    assert_eq!(file.after.ok_or("linked record absent")?.link_count, 2);
    let root = (checkout.lookup_no_follow_with_metadata(
        &root_path()?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value
    .ok_or("updated root metadata absent")?;
    assert_eq!(root.metadata, metadata);
    assert!(
        semantic.files.iter().any(|change| change
            .after
            .is_some_and(|record| record.kind == FileKind::Directory
                && record.metadata == root.record.metadata))
    );
    assert_eq!(semantic.bindings.len(), 3);
    assert!(
        semantic
            .bindings
            .iter()
            .any(|change| change.name.as_bytes() == b"changed"
                && change.before.is_some()
                && change.after.is_none())
    );
    for name in [b"alias".as_slice(), b"renamed".as_slice()] {
        assert!(semantic.bindings.iter().any(|change| {
            change.name.as_bytes() == name
                && change.before.is_none()
                && change
                    .after
                    .as_ref()
                    .is_some_and(|entry| entry.file_id == changed_id)
        }));
    }
    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    for (before, after) in [
        (initial, first),
        (first, second),
        (second, third),
        (third, fourth),
        (fourth, fourth),
    ] {
        let before = (volume.checkout(
            GenerationSelector::Exact(before),
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await?
        .value;
        let after = (volume.checkout(
            GenerationSelector::Exact(after),
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await?
        .value;
        for maximum in [1, 2, 32] {
            let ordinary =
                (before.candidate_diff(&after, maximum, WorkBudget::UNBOUNDED, &cancellation))
                    .await?
                    .value
                    .0;
            let original =
                crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                    .map_err(|(cause, _)| cause)?;
            let baseline = original.live_allocation_bytes();
            let (native, allocation) =
                (before.candidate_diff(&after, maximum, &original, &cancellation)).await?;
            assert_eq!(ordinary, native);
            let actual = (native.files.capacity() * size_of::<FileRecordChange>()
                + native.bindings.capacity() * size_of::<DirectoryBindingChange>())
                as u64
                + native
                    .bindings
                    .iter()
                    .map(|change| {
                        change.name.retained_bytes() as u64
                            + change
                                .before
                                .as_ref()
                                .map_or(0, |entry| entry.name.retained_bytes() as u64)
                            + change
                                .after
                                .as_ref()
                                .map_or(0, |entry| entry.name.retained_bytes() as u64)
                    })
                    .sum::<u64>();
            assert_eq!(original.live_allocation_bytes() - baseline, actual);
            drop(native);
            drop(allocation);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        if before.root.file_table != after.root.file_table {
            for budget in [
                WorkBudget {
                    allocation_operations: 0,
                    ..WorkBudget::UNBOUNDED
                },
                WorkBudget {
                    bytes_copied: 0,
                    ..WorkBudget::UNBOUNDED
                },
            ] {
                let denied = crate::NativeMaterializationWork::new(budget, &cancellation)
                    .map_err(|(cause, _)| cause)?;
                let baseline = denied.live_allocation_bytes();
                let failure = (before.candidate_diff(&after, 32, &denied, &cancellation))
                    .await
                    .err()
                    .ok_or("native diff denial succeeded")?;
                assert!(matches!(
                    failure,
                    FsError::Work(_) | FsError::NativeKernel(_)
                ));
                assert_eq!(denied.receipt().bytes_copied, 0);
                if budget.allocation_operations == 0 {
                    assert_eq!(denied.receipt().backend_read_operations, 0);
                }
                drop(failure);
                assert_eq!(denied.live_allocation_bytes(), baseline);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn three_way_merge_combines_independent_directory_bindings_and_publishes()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([74; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let mut ours = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let mut theirs = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    (ours.create(
        crate::AuthoredMutation::CreateFile {
            path: path("ours")?,
            bytes: (bytes::Bytes::from_static(b"ours")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    (theirs.create(
        crate::AuthoredMutation::CreateFile {
            path: path("theirs")?,
            bytes: (bytes::Bytes::from_static(b"theirs")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let theirs_generation = match (theirs.commit(
        OperationId::new(),
        None,
        None,
        None,
        crate::PublicationPermit::Unrestricted,
        WorkBudget::UNBOUNDED,
        &cancellation,
        |_| async {
            crate::FsResult::Ok(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::UNCHARGED,
            })
        },
    ))
    .await?
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected branch commit: {outcome:?}").into()),
    };
    let prepared = (ours.prepare_merge(
        theirs_generation,
        32,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(matches!(prepared.value, MergePreparation::Prepared { .. }));
    let mutation_during_merge = (ours.mutate(Vec::new(), WorkBudget::UNBOUNDED, &cancellation))
        .await
        .err()
        .ok_or("prepared merge accepted a mutation")?;
    assert!(matches!(
        mutation_during_merge.error,
        FsError::PreparedMergePending
    ));
    assert!(
        (ours.prepare_merge(
            theirs_generation,
            32,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
        .is_err()
    );
    assert!(
        (ours.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .await
            .is_err()
    );
    let merged_generation = match (ours.commit(
        OperationId::new(),
        None,
        None,
        None,
        crate::PublicationPermit::Unrestricted,
        WorkBudget::UNBOUNDED,
        &cancellation,
        |_| async {
            crate::FsResult::Ok(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::UNCHARGED,
            })
        },
    ))
    .await?
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected merge commit: {outcome:?}").into()),
    };
    let mut merged = (volume.checkout(
        GenerationSelector::Exact(merged_generation),
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    assert!(
        (merged.lookup_no_follow(&path("ours")?, WorkBudget::UNBOUNDED, &cancellation,))
            .await?
            .value
            .record
            .is_some()
    );
    assert!(
        (merged.lookup_no_follow(&path("theirs")?, WorkBudget::UNBOUNDED, &cancellation,))
            .await?
            .value
            .record
            .is_some()
    );
    assert_eq!(merged.root().parents.len(), 2);

    let mut left = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let mut right = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    (left.mutate(
        vec![crate::kernel::Mutation::Link {
            source: path("ours")?,
            destination: path("left-link")?,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    (right.mutate(
        vec![crate::kernel::Mutation::Link {
            source: path("ours")?,
            destination: path("right-link")?,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let right_generation = match (right.commit(
        OperationId::new(),
        None,
        None,
        None,
        crate::PublicationPermit::Unrestricted,
        WorkBudget::UNBOUNDED,
        &cancellation,
        |_| async {
            crate::FsResult::Ok(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::UNCHARGED,
            })
        },
    ))
    .await?
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected hard-link commit: {outcome:?}").into()),
    };
    assert!(matches!(
        (left.prepare_merge(
            right_generation,
            32,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await?
        .value,
        MergePreparation::Prepared { .. }
    ));
    let links_generation = match (left.commit(
        OperationId::new(),
        None,
        None,
        None,
        crate::PublicationPermit::Unrestricted,
        WorkBudget::UNBOUNDED,
        &cancellation,
        |_| async {
            crate::FsResult::Ok(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::UNCHARGED,
            })
        },
    ))
    .await?
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected hard-link merge: {outcome:?}").into()),
    };
    let mut links = (volume.checkout(
        GenerationSelector::Exact(links_generation),
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let source = (links.lookup_no_follow(&path("ours")?, WorkBudget::UNBOUNDED, &cancellation))
        .await?
        .value
        .record
        .ok_or("merged hard-link source missing")?;
    assert_eq!(source.link_count, 3);
    for linked in ["left-link", "right-link"] {
        let record = (links.lookup_no_follow(&path(linked)?, WorkBudget::UNBOUNDED, &cancellation))
            .await?
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
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([124; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let mut seed = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let file = path("conflict.txt")?;
    let file_id = (seed.create(
        crate::AuthoredMutation::CreateFile {
            path: file.clone(),
            bytes: (bytes::Bytes::from_static(b"base")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let second_file = path("second-conflict.txt")?;
    let second_file_id = (seed.create(
        crate::AuthoredMutation::CreateFile {
            path: second_file.clone(),
            bytes: (bytes::Bytes::from_static(b"base-two")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let base_generation = match (seed.commit(
        OperationId::from_bytes([125; 16]),
        None,
        None,
        None,
        crate::PublicationPermit::Unrestricted,
        WorkBudget::UNBOUNDED,
        &cancellation,
        |_| async {
            crate::FsResult::Ok(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::UNCHARGED,
            })
        },
    ))
    .await?
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected seed commit: {outcome:?}").into()),
    };
    let mut ours = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let mut theirs = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let mut read_only = (volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    assert!(
        (read_only.prepare_merge(base_generation, 8, 8, WorkBudget::UNBOUNDED, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (ours.prepare_merge(base_generation, 0, 8, WorkBudget::UNBOUNDED, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (ours.prepare_merge(base_generation, 8, 0, WorkBudget::UNBOUNDED, &cancellation,))
            .await
            .is_err()
    );
    (ours.change_content(
        file.clone(),
        crate::ContentChange::Write {
            offset: 0,
            bytes: (bytes::Bytes::from_static(b"ours")).into(),
        },
        crate::ContentTimes::Preserve,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    (ours.change_content(
        second_file.clone(),
        crate::ContentChange::Write {
            offset: 0,
            bytes: (bytes::Bytes::from_static(b"ours-two")).into(),
        },
        crate::ContentTimes::Preserve,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    (theirs.change_content(
        file,
        crate::ContentChange::Write {
            offset: 0,
            bytes: (bytes::Bytes::from_static(b"them")).into(),
        },
        crate::ContentTimes::Preserve,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    (theirs.change_content(
        second_file,
        crate::ContentChange::Write {
            offset: 0,
            bytes: (bytes::Bytes::from_static(b"them-two")).into(),
        },
        crate::ContentTimes::Preserve,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let theirs_generation = match (theirs.commit(
        OperationId::from_bytes([126; 16]),
        None,
        None,
        None,
        crate::PublicationPermit::Unrestricted,
        WorkBudget::UNBOUNDED,
        &cancellation,
        |_| async {
            crate::FsResult::Ok(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::UNCHARGED,
            })
        },
    ))
    .await?
    .value
    {
        LivePublicationObservation::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected competing commit: {outcome:?}").into()),
    };
    assert!(
        (ours.prepare_merge(base_generation, 8, 8, WorkBudget::UNBOUNDED, &cancellation,))
            .await
            .is_err()
    );
    let change_limited = (ours.prepare_merge(
        theirs_generation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("truncated merge inputs unexpectedly prepared")?;
    assert!(matches!(change_limited.error, FsError::MergeChangeLimit));
    assert!(change_limited.work.backend_read_operations > 0);
    let bounded_conflicts = (ours.prepare_merge(
        theirs_generation,
        8,
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(matches!(
        bounded_conflicts.value,
        MergePreparation::Conflicted {
            ref conflicts,
            truncated: true,
        } if conflicts.len() == 1
            && matches!(conflicts[0], MergeConflict::File(id) if id == file_id || id == second_file_id)
    ));
    let conflicted = (ours.prepare_merge(
        theirs_generation,
        8,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
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
