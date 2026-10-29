use super::*;

#[tokio::test]
async fn only_serialized_non_pinned_overlays_compose_disjoint_publications() -> TestResult {
    for simultaneous in [false, true] {
        for (config, mode, composed) in [
            (config(), writable_manual(), false),
            (config(), writable_pinned(), false),
            (serialized_config(), writable_manual(), true),
            (serialized_config(), writable_pinned(), false),
        ] {
            let cancel = CancellationToken::new();
            let (volume, _) = fixture(config, &cancel).await?;
            let mut first = open_checkout(&volume, mode, &cancel).await?;
            let mut second = open_checkout(&volume, mode, &cancel).await?;
            create(&mut first, "left", OwnedBytes::from_static(b"L"), &cancel).await?;
            create(&mut second, "right", OwnedBytes::from_static(b"R"), &cancel).await?;
            let publish_first = first.commit(
                OperationId::new(),
                None,
                None,
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<FsError>,
            );
            let publish_second = second.commit(
                OperationId::new(),
                None,
                None,
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<FsError>,
            );
            let (left, right) = if simultaneous {
                futures::future::join(publish_first, publish_second).await
            } else {
                (publish_first.await, publish_second.await)
            };
            let mut committed = 0;
            for outcome in [left?.value, right?.value] {
                match outcome {
                    LivePublicationObservation::Committed { .. } => committed += 1,
                    LivePublicationObservation::Conflict { .. } if !composed => {}
                    other => {
                        return Err(format!("unexpected publication outcome: {other:?}").into());
                    }
                }
            }
            assert_eq!(committed, if composed { 2 } else { 1 });
            assert_eq!(
                usize::from(first.has_pending_mutations())
                    + usize::from(second.has_pending_mutations()),
                2 - committed
            );
            let mut current = open_checkout(&volume, pinned(), &cancel).await?;
            let mut published = 0;
            for name in ["left", "right"] {
                published += usize::from(
                    current
                        .lookup_no_follow(&path(name)?, B, &cancel)
                        .await?
                        .value
                        .record
                        .is_some(),
                );
            }
            assert_eq!(published, committed);
        }
    }
    Ok(())
}

#[tokio::test]
async fn private_overlay_commit_retry_and_conflict_are_generation_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([49; 16]);
    let volume = (fs.create_volume_with_id(volume_id, config(), B, &cancellation))
        .await?
        .value;
    let mut first = open_checkout(&volume, writable_pinned(), &cancellation).await?;
    let mut retry = open_checkout(&volume, writable_pinned(), &cancellation).await?;
    let mut stale = open_checkout(&volume, writable_pinned(), &cancellation).await?;
    let root_path = NamespacePath::new(Vec::new(), config().limits)?;
    let metadata = (first.lookup_no_follow(&root_path, B, &cancellation))
        .await?
        .value
        .record
        .ok_or("root record absent")?
        .metadata;
    let record = FileRecord {
        file_id: FileId::from_bytes([7; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata,
        payload: FilePayload::InlineRegular(InlineFileData::new(b"hello")?),
    };
    for checkout in [&mut first, &mut retry] {
        (checkout.mutate(
            vec![Mutation::Create {
                path: path("a")?,
                record,
            }],
            B,
            &cancellation,
        ))
        .await?;
        assert_eq!(
            (checkout.lookup_no_follow(&path("a")?, B, &cancellation,))
                .await?
                .value
                .record
                .map(|value| value.payload),
            Some(record.payload)
        );
    }
    assert!(
        (stale.lookup_no_follow(&path("a")?, B, &cancellation,))
            .await?
            .value
            .record
            .is_none()
    );
    (stale.mutate(
        vec![Mutation::Create {
            path: path("b")?,
            record: FileRecord {
                file_id: FileId::from_bytes([8; 16]),
                ..record
            },
        }],
        B,
        &cancellation,
    ))
    .await?;

    let operation_id = OperationId::from_bytes([9; 16]);
    let committed = (first.commit(
        operation_id,
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    assert!(matches!(
        committed.value,
        LivePublicationObservation::Committed { .. }
    ));
    assert!(!first.has_pending_mutations());
    let repeated = (retry.commit(
        operation_id,
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    assert!(matches!(
        repeated.value,
        LivePublicationObservation::AlreadyCommitted { .. }
    ));
    assert!(!retry.has_pending_mutations());
    let conflict = (stale.commit(
        OperationId::from_bytes([10; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    assert!(matches!(
        conflict.value,
        LivePublicationObservation::Conflict { .. }
    ));
    assert!(stale.has_pending_mutations());

    let mut head = (volume.checkout(GenerationSelector::Head, pinned(), B, &cancellation)).await?;
    assert!(
        (head.value.lookup_no_follow(&path("a")?, B, &cancellation,))
            .await?
            .value
            .record
            .is_some()
    );
    let clean_failure = (first.commit(
        OperationId::from_bytes([11; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await
    .err()
    .ok_or("clean checkout commit unexpectedly succeeded")?;
    assert!(matches!(clean_failure.error, FsError::NoPendingMutations));

    let mut inverse = open_checkout(&volume, writable_pinned(), &cancellation).await?;
    let inverse_record = FileRecord {
        file_id: FileId::from_bytes([15; 16]),
        ..record
    };
    (inverse.mutate(
        vec![Mutation::Create {
            path: path("inverse")?,
            record: inverse_record,
        }],
        B,
        &cancellation,
    ))
    .await?;
    assert!(inverse.has_pending_mutations());
    (inverse.mutate(
        vec![Mutation::Remove {
            path: path("inverse")?,
            expected_file_id: MetadataField::Value(inverse_record.file_id),
        }],
        B,
        &cancellation,
    ))
    .await?;
    assert!(!inverse.has_pending_mutations());
    let inverse_commit = (inverse.commit(
        OperationId::from_bytes([16; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await
    .err()
    .ok_or("inverse clean commit unexpectedly succeeded")?;
    assert!(matches!(inverse_commit.error, FsError::NoPendingMutations));

    Ok(())
}

#[tokio::test]
async fn exclusive_writer_checkout_atomically_fences_every_prior_writer()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let volume = Fs::memory()
        .create_volume(exclusive_config(), B, &cancellation)
        .await?
        .value;
    let mut displaced = open_checkout(&volume, writable_pinned(), &cancellation).await?;
    let mut owner = open_checkout(&volume, writable_pinned(), &cancellation).await?;
    (displaced.create(
        AuthoredMutation::CreateFile {
            path: path("displaced")?,
            bytes: OwnedBytes::from_static(b"stale"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let rejected = (displaced.commit(
        OperationId::from_bytes([0xe2; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    assert!(matches!(
        rejected.value,
        LivePublicationObservation::Fenced { actual_epoch } if actual_epoch.get() == 3
    ));
    (owner.create(
        AuthoredMutation::CreateFile {
            path: path("owner")?,
            bytes: OwnedBytes::from_static(b"current"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let committed = (owner.commit(
        OperationId::from_bytes([0xe3; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    assert!(matches!(
        committed.value,
        LivePublicationObservation::Committed { head, .. } if head.epoch.get() == 3
    ));
    Ok(())
}

#[tokio::test]
async fn manual_refresh_is_explicit_bounded_and_never_discards_mutations()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let (volume, _) = fixture(config(), &cancellation).await?;
    let mut clean = open_checkout(&volume, manual(), &cancellation).await?;
    let mut clean_tracking = open_checkout(&volume, tracking(), &cancellation).await?;
    let mut dirty = open_checkout(&volume, writable_manual(), &cancellation).await?;
    let mut conflicted = open_checkout(&volume, writable_manual(), &cancellation).await?;
    let mut writer = open_checkout(&volume, writable_pinned(), &cancellation).await?;
    let mut pinned_checkout = open_checkout(&volume, pinned(), &cancellation).await?;
    let root_path = NamespacePath::new(Vec::new(), config().limits)?;
    let metadata = (writer.lookup_no_follow(&root_path, B, &cancellation))
        .await?
        .value
        .record
        .ok_or("root record absent")?
        .metadata;
    let record = FileRecord {
        file_id: FileId::from_bytes([12; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata,
        payload: FilePayload::InlineRegular(InlineFileData::new(b"new-head")?),
    };
    assert!(
        (conflicted.lookup_no_follow(&path("published")?, B, &cancellation,))
            .await?
            .value
            .record
            .is_none()
    );
    (conflicted.mutate(
        vec![Mutation::Create {
            path: path("conflicted-local")?,
            record: FileRecord {
                file_id: FileId::from_bytes([16; 16]),
                ..record
            },
        }],
        B,
        &cancellation,
    ))
    .await?;
    (dirty.mutate(
        vec![Mutation::Create {
            path: path("private")?,
            record,
        }],
        B,
        &cancellation,
    ))
    .await?;
    (writer.mutate(
        vec![Mutation::Create {
            path: path("published")?,
            record: FileRecord {
                file_id: FileId::from_bytes([13; 16]),
                ..record
            },
        }],
        B,
        &cancellation,
    ))
    .await?;
    (writer.commit(
        OperationId::from_bytes([14; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    let conflict = (conflicted.rebase_head(8, B, &cancellation)).await?;
    assert!(matches!(
        conflict.value,
        RebaseDecision::Conflicted {
            ref conflicts,
            truncated: false,
        } if conflicts.iter().any(|value| matches!(
            value.region,
            DependencyRegion::DirectoryName { .. }
        ))
    ));
    assert!(conflicted.has_pending_mutations());

    let pinned_failure = (pinned_checkout.refresh_head(B, &cancellation))
        .await
        .err()
        .ok_or("pinned refresh unexpectedly succeeded")?;
    assert!(matches!(pinned_failure.error, FsError::RefreshNotAllowed));

    let pinned_rebase = (pinned_checkout.rebase_head(8, B, &cancellation))
        .await
        .err()
        .ok_or("pinned rebase unexpectedly succeeded")?;
    assert!(matches!(pinned_rebase.error, FsError::RefreshNotAllowed));

    let dirty_failure = (dirty.refresh_head(B, &cancellation))
        .await
        .err()
        .ok_or("dirty refresh unexpectedly succeeded")?;
    assert!(matches!(
        dirty_failure.error,
        FsError::PendingMutationsRequireRebase
    ));

    assert!(dirty.has_pending_mutations());
    assert!(
        (dirty.lookup_no_follow(&path("private")?, B, &cancellation,))
            .await?
            .value
            .record
            .is_some()
    );
    let rebased = (dirty.rebase_head(8, B, &cancellation)).await?;
    assert!(matches!(rebased.value, RebaseDecision::Safe { .. }));
    assert!(dirty.has_pending_mutations());
    assert!(
        (dirty.lookup_no_follow(&path("published")?, B, &cancellation,))
            .await?
            .value
            .record
            .is_some()
    );
    assert!(
        (dirty.lookup_no_follow(&path("private")?, B, &cancellation,))
            .await?
            .value
            .record
            .is_some()
    );

    let advanced = (clean.refresh_head(B, &cancellation)).await?;
    assert_eq!(advanced.value, writer.generation_id());
    assert!(advanced.work.object_probes > 0);
    let tracking_advanced = (clean_tracking.refresh_head(B, &cancellation)).await?;
    assert_eq!(tracking_advanced.value, writer.generation_id());
    assert!(
        (clean.lookup_no_follow(&path("published")?, B, &cancellation,))
            .await?
            .value
            .record
            .is_some()
    );
    let equal = (clean.refresh_head(B, &cancellation)).await?;
    assert_eq!(equal.value, advanced.value);
    (dirty.discard(B, &cancellation)).await?;
    assert!(!dirty.has_pending_mutations());
    assert!(
        (dirty.lookup_no_follow(&path("private")?, B, &cancellation,))
            .await?
            .value
            .record
            .is_none()
    );
    assert!(
        (dirty.lookup_no_follow(&path("published")?, B, &cancellation,))
            .await?
            .value
            .record
            .is_some()
    );
    let after_discard = (dirty.refresh_head(B, &cancellation)).await?;
    assert_eq!(after_discard.value, writer.generation_id());
    Ok(())
}

#[tokio::test]
async fn private_candidate_rebases_more_changes_than_one_mutation_batch()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_mutations_per_batch = 8;
    let (volume, _) = fixture(limited, &cancellation).await?;
    let mut local = open_checkout(&volume, writable_manual(), &cancellation).await?;
    let mut remote = open_checkout(&volume, writable_pinned(), &cancellation).await?;
    for index in 0..20 {
        (local.create(
            AuthoredMutation::CreateFile {
                path: path(&format!("local{index}"))?,
                bytes: OwnedBytes::from_static(b"local"),
                metadata: FileMetadata::default(),
            },
            B,
            &cancellation,
        ))
        .await?;
    }
    (remote.create(
        AuthoredMutation::CreateFile {
            path: path("remote")?,
            bytes: OwnedBytes::from_static(b"remote"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    (remote.commit(
        OperationId::from_bytes([242; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    assert!(matches!(
        (local.rebase_head(8, B, &cancellation)).await?.value,
        RebaseDecision::Safe { .. }
    ));
    assert!(matches!(
        (local.commit(
            OperationId::from_bytes([243; 16]),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancellation,
            allow::<FsError>
        ))
        .await?
        .value,
        LivePublicationObservation::Committed { .. }
    ));
    for index in 0..20 {
        assert!(
            (local.lookup_no_follow(&path(&format!("local{index}"))?, B, &cancellation,))
                .await?
                .value
                .record
                .is_some()
        );
    }
    assert!(
        (local.lookup_no_follow(&path("remote")?, B, &cancellation,))
            .await?
            .value
            .record
            .is_some()
    );
    Ok(())
}
