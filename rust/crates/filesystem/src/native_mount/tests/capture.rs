use super::*;

async fn assert_contents<A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore>(
    checkout: &mut crate::Checkout<A, O>,
    path: &NamespacePath,
    expected: &[u8],
    cancellation: &CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let read = checkout
        .read_file_range(
            path,
            ByteRange {
                offset: 0,
                length: expected.len() as u64,
            },
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await?;
    assert_eq!(read.value.bytes.as_ref(), expected);
    Ok(())
}

#[tokio::test]
async fn baseline_path_limit_stops_unbounded_traversal() -> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let volume = memory_volume(
        VolumeId::from_bytes([94; 16]),
        VolumeConfig::portable(Lifecycle::Ephemeral),
        &cancellation,
    )
    .await?;
    let mut checkout = private_checkout(&volume, &cancellation).await?;
    let host = tempfile::tempdir()?;
    for index in 0..3 {
        std::fs::write(host.path().join(format!("file-{index}")), b"x")?;
    }
    let result = capture_baseline(
        &mut checkout,
        &CaptureOptions {
            expected_root_identity: capture_root_identity(host.path())?,
            source_root: host.path().to_path_buf(),
            maximum_paths: 2,
            maximum_extent_spans: 16,
        },
        &crate::CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await;
    let Err(error) = result else {
        return Err("path cap unexpectedly permitted traversal".into());
    };
    assert!(matches!(error.error, CaptureError::InvalidOptions));
    Ok(())
}

#[tokio::test]
async fn coalesced_parent_child_removals_capture_deepest_first()
-> Result<(), Box<dyn std::error::Error>> {
    check_coalesced_removals(false).await
}

#[tokio::test]
async fn coalesced_removed_child_below_replaced_directory_requires_rescan()
-> Result<(), Box<dyn std::error::Error>> {
    check_coalesced_removals(true).await
}

async fn check_coalesced_removals(replace_parent: bool) -> Result<(), Box<dyn std::error::Error>> {
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let limits = config.limits;
    let cancellation = CancellationToken::new();
    let volume = memory_volume(VolumeId::new(), config, &cancellation).await?;
    let mut checkout = private_checkout(&volume, &cancellation).await?;
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("capture");
    std::fs::create_dir(&root)?;
    let options = CaptureOptions {
        expected_root_identity: capture_root_identity(&root)?,
        source_root: root.clone(),
        maximum_paths: 8,
        maximum_extent_spans: 8,
    };
    capture_baseline(
        &mut checkout,
        &options,
        &crate::CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    let parent = portable_path(limits, &[b"parent"])?;
    let child = portable_path(limits, &[b"parent", b"child.txt"])?;
    std::fs::create_dir(root.join("parent"))?;
    std::fs::write(root.join("parent").join("child.txt"), b"child")?;
    capture_watch_batch(
        &mut checkout,
        WatchBatch::Changes {
            epoch: 1,
            first_sequence: 0,
            next_sequence: 1,
            changes: vec![WatchChange::Created(parent.clone())],
            original: Default::default(),
        },
        &options,
        &crate::CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert!(
        checkout
            .lookup_no_follow(&child, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .is_some()
    );
    std::fs::remove_file(root.join("parent").join("child.txt"))?;
    std::fs::remove_dir(root.join("parent"))?;
    if replace_parent {
        std::fs::write(root.join("parent"), b"replacement file")?;
    }
    let answer = capture_watch_batch(
        &mut checkout,
        WatchBatch::Changes {
            epoch: 1,
            first_sequence: 1,
            next_sequence: 3,
            changes: vec![
                WatchChange::Removed(parent.clone()),
                WatchChange::Removed(child.clone()),
            ],
            original: Default::default(),
        },
        &options,
        &crate::CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await;
    if replace_parent {
        let Err(error) = answer else {
            return Err("a replaced ancestor was accepted as absent".into());
        };
        assert!(matches!(error.error, CaptureError::RescanRequired { .. }));
        return Ok(());
    }
    answer?;
    assert!(
        checkout
            .lookup_no_follow(&parent, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .is_none()
    );
    Ok(())
}
#[tokio::test]
async fn stale_created_hints_for_existing_directory_capture_exactly()
-> Result<(), Box<dyn std::error::Error>> {
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let limits = config.limits;
    let cancellation = CancellationToken::new();
    let volume = memory_volume(VolumeId::new(), config, &cancellation).await?;
    let mut checkout = private_checkout(&volume, &cancellation).await?;
    let nested = portable_path(limits, &[b"nested"])?;
    let child = portable_path(limits, &[b"nested", b"child.txt"])?;
    let created = portable_path(limits, &[b"watch.txt"])?;

    let temporary = tempfile::tempdir()?;
    std::fs::create_dir(temporary.path().join("nested"))?;
    std::fs::write(temporary.path().join("nested/child.txt"), b"child")?;
    let options = CaptureOptions {
        source_root: temporary.path().to_path_buf(),
        expected_root_identity: capture_root_identity(temporary.path())?,
        maximum_paths: 16,
        maximum_extent_spans: 16,
    };
    capture_baseline(
        &mut checkout,
        &options,
        &crate::CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    let directory_before = checkout
        .lookup_no_follow(&nested, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .record
        .ok_or("captured directory disappeared")?;

    std::fs::write(temporary.path().join("watch.txt"), b"created")?;
    capture_watch_batch(
        &mut checkout,
        WatchBatch::Changes {
            epoch: 1,
            first_sequence: 0,
            next_sequence: 6,
            changes: vec![
                WatchChange::Created(child.clone()),
                WatchChange::MetadataChanged(child.clone()),
                WatchChange::Created(nested.clone()),
                WatchChange::MetadataChanged(nested.clone()),
                WatchChange::Created(created.clone()),
                WatchChange::Modified(created.clone()),
            ],
            original: Default::default(),
        },
        &options,
        &crate::CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    let directory_after = checkout
        .lookup_no_follow(&nested, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .record
        .ok_or("directory disappeared after stale hints")?;
    assert_eq!(directory_after.file_id, directory_before.file_id);
    for (path, expected) in [(&child, b"child".as_slice()), (&created, b"created")] {
        assert_contents(&mut checkout, path, expected, &cancellation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn windows_created_and_modified_hint_captures_one_new_file()
-> Result<(), Box<dyn std::error::Error>> {
    let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
    config.profile = FilesystemProfile::Windows;
    let limits = config.limits;
    let cancellation = CancellationToken::new();
    let volume = memory_volume(VolumeId::new(), config, &cancellation).await?;
    let mut checkout = private_checkout(&volume, &cancellation).await?;
    let name = "watch.txt"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect();
    let path = NamespacePath::new(
        vec![LogicalName::new(
            NameEncoding::WindowsUtf16Le,
            name,
            limits.maximum_component_bytes,
        )?],
        limits,
    )?;
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("watch.txt"), b"created")?;
    capture_watch_batch(
        &mut checkout,
        WatchBatch::Changes {
            epoch: 1,
            first_sequence: 0,
            next_sequence: 2,
            changes: vec![
                WatchChange::Created(path.clone()),
                WatchChange::Modified(path.clone()),
            ],
            original: Default::default(),
        },
        &CaptureOptions {
            source_root: temporary.path().to_path_buf(),
            expected_root_identity: capture_root_identity(temporary.path())?,
            maximum_paths: 16,
            maximum_extent_spans: 16,
        },
        &crate::CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert_contents(&mut checkout, &path, b"created", &cancellation).await?;
    Ok(())
}
