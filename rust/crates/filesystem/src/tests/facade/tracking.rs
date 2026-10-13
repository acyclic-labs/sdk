use super::*;

#[tokio::test]
async fn tracking_reads_capture_only_their_terminal_regions() -> TestResult {
    for observation in 0..3 {
        let cancel = CancellationToken::new();
        let (volume, mut seed) = fixture(config(), &cancel).await?;
        create(
            &mut seed,
            "file",
            OwnedBytes::from(vec![b'a'; 128]),
            &cancel,
        )
        .await?;
        commit(&mut seed, &cancel).await?;
        let file = path("file")?;
        let mut reader = open_checkout(&volume, tracking(), &cancel).await?;
        match observation {
            0 => {
                reader
                    .read_file_range(
                        &file,
                        ByteRange {
                            offset: 0,
                            length: 8,
                        },
                        B,
                        &cancel,
                    )
                    .await?;
            }
            1 => {
                reader.read_metadata(&file, B, &cancel).await?;
            }
            _ => {
                reader.lookup_no_follow(&file, B, &cancel).await?;
            }
        }
        let mut writer = open_checkout(&volume, writable_pinned(), &cancel).await?;
        if observation == 0 {
            writer
                .set_attributes(
                    file,
                    FileMetadata {
                        modified_ns: MetadataField::Value(7),
                        ..FileMetadata::default()
                    },
                    None,
                    B,
                    &cancel,
                )
                .await?;
        } else {
            let (offset, bytes) = if observation == 1 {
                (64, b"changed".as_slice())
            } else {
                (96, b"different".as_slice())
            };
            writer
                .change_content(
                    file,
                    ContentChange::Write {
                        offset,
                        bytes: OwnedBytes::copy_from_slice(bytes),
                    },
                    ContentTimes::Preserve,
                    B,
                    &cancel,
                )
                .await?;
        }
        commit(&mut writer, &cancel).await?;
        let rebase = reader.rebase_head(8, B, &cancel).await?.value;
        if observation == 2 {
            assert!(
                matches!(rebase, RebaseDecision::Conflicted { conflicts, .. }
                if conflicts.iter().any(|conflict| matches!(conflict.region, DependencyRegion::FileRecord(_))))
            );
        } else {
            assert!(matches!(rebase, RebaseDecision::Safe { .. }));
        }
    }
    Ok(())
}

#[tokio::test]
async fn sparse_seek_tracks_local_holes_and_remote_boundary_changes() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    create(
        &mut seed,
        "sparse",
        OwnedBytes::from(vec![b'a'; 256]),
        &cancel,
    )
    .await?;
    commit(&mut seed, &cancel).await?;
    let file = path("sparse")?;
    let mut local = open_checkout(&volume, writable_tracking(), &cancel).await?;
    local
        .change_content(
            file.clone(),
            ContentChange::ZeroRange {
                range: ByteRange {
                    offset: 128,
                    length: 16,
                },
                allocated: false,
                extend: false,
            },
            ContentTimes::Preserve,
            B,
            &cancel,
        )
        .await?;
    assert_eq!(
        local
            .seek_file_extent(&file, 128, ExtentSeekTarget::Hole, B, &cancel)
            .await?
            .value,
        Some(128)
    );
    let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
    remote
        .change_content(
            file.clone(),
            ContentChange::ZeroRange {
                range: ByteRange {
                    offset: 0,
                    length: 64,
                },
                allocated: false,
                extend: false,
            },
            ContentTimes::Preserve,
            B,
            &cancel,
        )
        .await?;
    commit(&mut remote, &cancel).await?;
    assert!(matches!(
        local.rebase_head(8, B, &cancel).await?.value,
        RebaseDecision::Safe { .. }
    ));
    assert_eq!(
        local
            .seek_file_extent(&file, 128, ExtentSeekTarget::Hole, B, &cancel)
            .await?
            .value,
        Some(128)
    );
    let mut reader = open_checkout(&volume, tracking(), &cancel).await?;
    assert_eq!(
        reader
            .seek_file_extent(&file, 128, ExtentSeekTarget::Hole, B, &cancel)
            .await?
            .value,
        Some(256)
    );
    let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
    remote
        .change_content(
            file,
            ContentChange::ZeroRange {
                range: ByteRange {
                    offset: 192,
                    length: 16,
                },
                allocated: false,
                extend: false,
            },
            ContentTimes::Preserve,
            B,
            &cancel,
        )
        .await?;
    commit(&mut remote, &cancel).await?;
    assert!(
        matches!(reader.rebase_head(8, B, &cancel).await?.value, RebaseDecision::Conflicted { conflicts, .. }
        if conflicts.iter().any(|conflict| matches!(conflict.region,
            DependencyRegion::SparseSeek { offset: 128, target: ExtentSeekTarget::Hole, .. })))
    );
    Ok(())
}

#[tokio::test]
async fn clipped_identity_read_at_eof_tracks_file_length() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    let id = create(&mut seed, "file", OwnedBytes::from_static(b"abc"), &cancel).await?;
    commit(&mut seed, &cancel).await?;
    let mut reader = open_checkout(&volume, tracking(), &cancel).await?;
    assert!(
        reader
            .read_file_up_to(id, 3, 16, B, &cancel)
            .await?
            .value
            .bytes
            .is_empty()
    );
    let mut writer = open_checkout(&volume, writable_pinned(), &cancel).await?;
    writer
        .change_content(
            path("file")?,
            ContentChange::Write {
                offset: 3,
                bytes: OwnedBytes::from_static(b"d"),
            },
            ContentTimes::Preserve,
            B,
            &cancel,
        )
        .await?;
    commit(&mut writer, &cancel).await?;
    assert!(
        matches!(reader.rebase_head(8, B, &cancel).await?.value, RebaseDecision::Conflicted { conflicts, .. }
        if conflicts.iter().any(|conflict| matches!(conflict.region, DependencyRegion::FileLength(actual) if actual == id)))
    );
    Ok(())
}

#[tokio::test]
async fn sparse_local_writes_rebase_across_disjoint_remote_ranges_and_conflict_on_overlap()
-> TestResult {
    sparse_rebase_across_remote_ranges(false).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_native_rebase_preserves_disjoint_ranges_and_is_atomic_on_denial_and_conflict()
-> TestResult {
    sparse_rebase_across_remote_ranges(true).await
}

#[tokio::test]
async fn repeated_single_and_batch_lookups_preserve_binding_conflicts() -> TestResult {
    for (batch, authored) in [(false, true), (true, false), (true, true)] {
        let cancel = CancellationToken::new();
        let (volume, _) = fixture(config(), &cancel).await?;
        let mut local = open_checkout(&volume, writable_tracking(), &cancel).await?;
        if authored {
            create(
                &mut local,
                "authored",
                OwnedBytes::from_static(b"local"),
                &cancel,
            )
            .await?;
        }
        let paths = [path("first")?, path("second")?];
        for _ in 0..2 {
            if batch {
                assert!(
                    local
                        .lookup_batch_no_follow(&paths, B, &cancel)
                        .await?
                        .value
                        .entries
                        .iter()
                        .all(|entry| entry.record.is_none())
                );
            } else {
                assert!(
                    local
                        .lookup_no_follow_with_metadata(&paths[1], B, &cancel)
                        .await?
                        .value
                        .is_none()
                );
            }
        }
        let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
        create(
            &mut remote,
            "second",
            OwnedBytes::from_static(b"remote"),
            &cancel,
        )
        .await?;
        commit(&mut remote, &cancel).await?;
        assert!(matches!(
            local.rebase_head(8, B, &cancel).await?.value,
            RebaseDecision::Conflicted { .. }
        ));
    }
    Ok(())
}
