use super::*;

#[tokio::test]
async fn capture_host_hard_links_preserves_shared_file_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let limits = VolumeLimits::default();
    let config = VolumeConfig {
        limits,
        ..VolumeConfig::portable(Lifecycle::Ephemeral)
    };
    let cancellation = CancellationToken::new();
    let volume = memory_volume(VolumeId::from_bytes([92; 16]), config, &cancellation).await?;
    let mut checkout = private_checkout(&volume, &cancellation).await?;
    let host = tempfile::tempdir()?;
    std::fs::write(host.path().join("a"), b"shared")?;
    std::fs::hard_link(host.path().join("a"), host.path().join("b"))?;
    let path = |name: &str| portable_path(limits, &[name.as_bytes()]);
    let options = CaptureOptions {
        expected_root_identity: capture_root_identity(host.path())?,
        source_root: host.path().to_path_buf(),
        maximum_paths: 2,
        maximum_extent_spans: 16,
    };
    let paths = [path("a")?, path("b")?];
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    crate::native_capture::capture_paths_batched(
        &mut checkout,
        &paths,
        &options,
        1,
        &cancellation,
        None,
        &original,
    )
    .await?;
    let first = checkout
        .lookup_no_follow(&paths[0], WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .record
        .ok_or("first captured link missing")?;
    let second = checkout
        .lookup_no_follow(&paths[1], WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .record
        .ok_or("second captured link missing")?;
    assert_eq!(first.file_id, second.file_id);
    for name in ["z", "0"] {
        std::fs::hard_link(host.path().join("a"), host.path().join(name))?;
        let additional = path(name)?;
        capture_paths(
            &mut checkout,
            &[paths[0].clone(), additional.clone()],
            &options,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let additional_id = checkout
            .lookup_no_follow(&additional, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("additional link missing")?
            .file_id;
        assert_eq!(additional_id, first.file_id);
    }
    std::fs::hard_link(host.path().join("a"), host.path().join("c"))?;
    let third = path("c")?;
    let watch = capture_watch_batch(
        &mut checkout,
        WatchBatch::Changes {
            epoch: 1,
            first_sequence: 1,
            next_sequence: 2,
            changes: vec![WatchChange::Created(third.clone())],
            original: Default::default(),
        },
        &CaptureOptions {
            maximum_paths: 1,
            ..options.clone()
        },
        &crate::CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await;
    assert!(matches!(
        watch,
        Err(crate::OperationFailure {
            error: CaptureError::RescanRequired { .. },
            ..
        })
    ));
    assert!(
        checkout
            .lookup_no_follow(&third, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .is_none()
    );
    std::fs::write(host.path().join("existing"), b"old")?;
    let existing = path("existing")?;
    capture_paths(
        &mut checkout,
        std::slice::from_ref(&existing),
        &CaptureOptions {
            maximum_paths: 1,
            ..options.clone()
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    let old_id = checkout
        .lookup_no_follow(&existing, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .record
        .ok_or("existing path missing")?
        .file_id;
    std::fs::remove_file(host.path().join("existing"))?;
    std::fs::hard_link(host.path().join("a"), host.path().join("existing"))?;
    let watch = capture_watch_batch(
        &mut checkout,
        WatchBatch::Changes {
            epoch: 1,
            first_sequence: 2,
            next_sequence: 3,
            changes: vec![WatchChange::Created(existing.clone())],
            original: Default::default(),
        },
        &CaptureOptions {
            maximum_paths: 1,
            ..options
        },
        &crate::CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await;
    assert!(matches!(
        watch,
        Err(crate::OperationFailure {
            error: CaptureError::RescanRequired { .. },
            ..
        })
    ));
    assert_eq!(
        checkout
            .lookup_no_follow(&existing, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("replacement changed checkout")?
            .file_id,
        old_id
    );
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
    assert!(error.work.items_examined <= 2);
    Ok(())
}

#[tokio::test]
async fn explicit_materialization_and_capture_round_trip_sparse_checkout()
-> Result<(), Box<dyn std::error::Error>> {
    let limits = VolumeLimits::default();
    let config = VolumeConfig {
        limits,
        ..VolumeConfig::portable(Lifecycle::Ephemeral)
    };
    let cancellation = CancellationToken::new();
    let volume = memory_volume(VolumeId::from_bytes([91; 16]), config, &cancellation).await?;
    let mut checkout = private_checkout(&volume, &cancellation).await?;
    let file = portable_path(limits, &[b"sparse.bin"])?;
    checkout
        .create(
            crate::AuthoredMutation::CreateFile {
                path: file.clone(),
                bytes: (Bytes::from_static(b"head")).into(),
                metadata: crate::kernel::FileMetadata::default(),
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    checkout
        .change_content(
            file.clone(),
            crate::ContentChange::Resize {
                logical_bytes: 1024 * 1024,
            },
            crate::ContentTimes::Preserve,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    checkout
        .change_content(
            file.clone(),
            crate::ContentChange::Write {
                offset: 1024 * 1024 - 4,
                bytes: (Bytes::from_static(b"tail")).into(),
            },
            crate::ContentTimes::Preserve,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;

    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("view");
    mutate_as_a_user_would(|| std::fs::create_dir(&destination))?;
    let materialized = materialize_checkout(
        &mut checkout,
        &MaterializeOptions {
            destination: destination.clone(),
            expected_root_identity: None,
            maximum_directory_entries: 16,
            maximum_extent_spans: 16,
            transfer_bytes: 64 * 1024,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await
    .map_err(|error| std::io::Error::other(format!("sparse materialization: {error}")))?;
    assert_eq!(materialized.value.files, 1);
    let host_file = destination.join("sparse.bin");
    assert_eq!(
        std::fs::metadata(&host_file)
            .map_err(|error| std::io::Error::other(format!("materialized metadata: {error}")))?
            .len(),
        1024 * 1024
    );
    let body = std::fs::read(&host_file)
        .map_err(|error| std::io::Error::other(format!("materialized read: {error}")))?;
    assert_eq!(&body[..4], b"head");
    assert_eq!(&body[body.len() - 4..], b"tail");

    let capture_options = CaptureOptions {
        expected_root_identity: capture_root_identity(&destination)?,
        source_root: destination.clone(),
        maximum_paths: 1,
        maximum_extent_spans: 16,
    };
    let sparse_capture = capture_paths(
        &mut checkout,
        std::slice::from_ref(&file),
        &capture_options,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await
    .map_err(|error| std::io::Error::other(format!("sparse recapture: {error}")))?;
    assert!(sparse_capture.value.staged_file_bytes < 1024 * 1024);
    let sparse_plan = checkout
        .plan_file_extents(
            &file,
            ByteRange {
                offset: 0,
                length: 1024 * 1024,
            },
            16,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value
        .ok_or("captured sparse file returned an inline plan")?;
    assert!(
        sparse_plan
            .spans
            .iter()
            .any(|span| matches!(span.kind, crate::kernel::ExtentKind::Hole))
    );

    mutate_as_a_user_would(|| std::fs::write(&host_file, b"captured"))
        .map_err(|error| std::io::Error::other(format!("host rewrite: {error}")))?;
    let captured = capture_paths(
        &mut checkout,
        std::slice::from_ref(&file),
        &capture_options,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert_eq!(captured.value.changed_paths, 1);
    let read = checkout
        .read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 8,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(read.value.bytes.as_ref(), b"captured");

    let before_rename = checkout
        .lookup_no_follow(&file, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .record
        .ok_or("captured file disappeared")?;
    let renamed = portable_path(limits, &[b"renamed.bin"])?;
    mutate_as_a_user_would(|| std::fs::rename(&host_file, destination.join("renamed.bin")))
        .map_err(|error| std::io::Error::other(format!("host rename: {error}")))?;
    let watch = capture_watch_batch(
        &mut checkout,
        WatchBatch::Changes {
            epoch: 7,
            first_sequence: 11,
            next_sequence: 12,
            changes: vec![WatchChange::Renamed {
                from: file.clone(),
                to: renamed.clone(),
            }],
            original: Default::default(),
        },
        &capture_options,
        &crate::CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert_eq!(watch.value.next_sequence, 12);
    let old = checkout
        .lookup_no_follow(&file, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    assert!(old.value.record.is_none());
    let new = checkout
        .lookup_no_follow(&renamed, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .record
        .ok_or("renamed file disappeared")?;
    assert_eq!(new.file_id, before_rename.file_id);

    let moved = portable_path(limits, &[b"moved.bin"])?;
    mutate_as_a_user_would(|| {
        std::fs::rename(
            destination.join("renamed.bin"),
            destination.join("moved.bin"),
        )
    })
    .map_err(|error| std::io::Error::other(format!("second host rename: {error}")))?;
    mutate_as_a_user_would(|| std::fs::write(destination.join("renamed.bin"), b"replacement"))
        .map_err(|error| std::io::Error::other(format!("host replacement create: {error}")))?;
    capture_watch_batch(
        &mut checkout,
        WatchBatch::Changes {
            epoch: 7,
            first_sequence: 12,
            next_sequence: 14,
            changes: vec![
                WatchChange::Renamed {
                    from: renamed.clone(),
                    to: moved.clone(),
                },
                WatchChange::Created(renamed.clone()),
            ],
            original: Default::default(),
        },
        &CaptureOptions {
            maximum_paths: 2,
            ..capture_options.clone()
        },
        &crate::CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    let moved_record = checkout
        .lookup_no_follow(&moved, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .record
        .ok_or("moved file disappeared")?;
    let replacement_record = checkout
        .lookup_no_follow(&renamed, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .record
        .ok_or("replacement file disappeared")?;
    assert_eq!(moved_record.file_id, before_rename.file_id);
    assert_ne!(replacement_record.file_id, before_rename.file_id);
    let replacement = checkout
        .read_file_range(
            &renamed,
            ByteRange {
                offset: 0,
                length: 11,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(replacement.value.bytes.as_ref(), b"replacement");

    let mut previous_id = replacement_record.file_id;
    let mut first_sequence = 14;
    for (payload, changes) in [
        (
            b"replaced-again".as_slice(),
            vec![WatchChange::Created(renamed.clone())],
        ),
        (
            b"compound-replacement".as_slice(),
            vec![
                WatchChange::Created(renamed.clone()),
                WatchChange::Modified(renamed.clone()),
            ],
        ),
    ] {
        mutate_as_a_user_would(|| std::fs::write(destination.join("renamed.bin"), payload))?;
        let maximum_paths = changes.len() as u32;
        let next_sequence = first_sequence + u64::from(maximum_paths);
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                epoch: 7,
                first_sequence,
                next_sequence,
                changes,
                original: Default::default(),
            },
            &CaptureOptions {
                maximum_paths,
                ..capture_options.clone()
            },
            &crate::CapturePolicy::default(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let replacement = checkout
            .lookup_no_follow(&renamed, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("replacement disappeared")?;
        assert_ne!(replacement.file_id, previous_id);
        previous_id = replacement.file_id;
        first_sequence = next_sequence;
    }

    let dirty_baseline = capture_baseline(
        &mut checkout,
        &CaptureOptions {
            maximum_paths: 16,
            ..capture_options.clone()
        },
        &crate::CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await;
    let Err(dirty_baseline) = dirty_baseline else {
        return Err("dirty baseline replaced private mutations".into());
    };
    assert!(matches!(dirty_baseline.error, CaptureError::DirtyCheckout));

    seal_checkout(
        &mut checkout,
        crate::OperationId::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;

    mutate_as_a_user_would(|| std::fs::remove_file(destination.join("renamed.bin")))
        .map_err(|error| std::io::Error::other(format!("host replacement removal: {error}")))?;
    mutate_as_a_user_would(|| std::fs::remove_file(destination.join("moved.bin")))
        .map_err(|error| std::io::Error::other(format!("host moved removal: {error}")))?;
    mutate_as_a_user_would(|| std::fs::create_dir(destination.join("nested")))
        .map_err(|error| std::io::Error::other(format!("host nested directory: {error}")))?;
    mutate_as_a_user_would(|| {
        std::fs::write(destination.join("nested").join("new.txt"), b"baseline")
    })
    .map_err(|error| std::io::Error::other(format!("host baseline write: {error}")))?;
    let baseline = capture_baseline(
        &mut checkout,
        &CaptureOptions {
            maximum_paths: 16,
            ..capture_options.clone()
        },
        &crate::CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert_eq!(baseline.value.examined_paths, 4);
    let removed = checkout
        .lookup_no_follow(&renamed, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    assert!(removed.value.record.is_none());
    let removed_moved = checkout
        .lookup_no_follow(&moved, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    assert!(removed_moved.value.record.is_none());
    let nested = PortablePath::parse("/nested/new.txt", limits)?;
    let nested = NamespacePath::from_portable(&nested, limits)?;
    let nested_read = checkout
        .read_file_range(
            &nested,
            ByteRange {
                offset: 0,
                length: 8,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(nested_read.value.bytes.as_ref(), b"baseline");

    let missing =
        NamespacePath::from_portable(&PortablePath::parse("/missing.tmp", limits)?, limits)?;
    let arrived =
        NamespacePath::from_portable(&PortablePath::parse("/arrived.txt", limits)?, limits)?;
    std::fs::write(destination.join("arrived.txt"), b"arrival")?;
    capture_watch_batch(
        &mut checkout,
        WatchBatch::Changes {
            epoch: 8,
            first_sequence: 0,
            next_sequence: 1,
            changes: vec![WatchChange::Renamed {
                from: missing,
                to: arrived.clone(),
            }],
            original: Default::default(),
        },
        &capture_options,
        &crate::CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert_eq!(
        checkout
            .read_file_range(
                &arrived,
                ByteRange {
                    offset: 0,
                    length: 7,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value
            .bytes
            .as_ref(),
        b"arrival"
    );

    let prior_arrival = checkout
        .lookup_no_follow(&arrived, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .record
        .ok_or("arrival disappeared")?;
    let temporary_name =
        NamespacePath::from_portable(&PortablePath::parse("/arrival.tmp", limits)?, limits)?;
    std::fs::write(destination.join("arrival.tmp"), b"replacement-arrival")?;
    std::fs::remove_file(destination.join("arrived.txt"))?;
    std::fs::rename(
        destination.join("arrival.tmp"),
        destination.join("arrived.txt"),
    )?;
    capture_watch_batch(
        &mut checkout,
        WatchBatch::Changes {
            epoch: 8,
            first_sequence: 1,
            next_sequence: 3,
            changes: vec![
                WatchChange::Created(temporary_name.clone()),
                WatchChange::Renamed {
                    from: temporary_name,
                    to: arrived.clone(),
                },
            ],
            original: Default::default(),
        },
        &CaptureOptions {
            maximum_paths: 2,
            ..capture_options.clone()
        },
        &crate::CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    let replaced_arrival = checkout
        .lookup_no_follow(&arrived, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .record
        .ok_or("replacement arrival disappeared")?;
    assert_ne!(replaced_arrival.file_id, prior_arrival.file_id);

    let created_and_modified = NamespacePath::from_portable(
        &PortablePath::parse("/created-and-modified.txt", limits)?,
        limits,
    )?;
    std::fs::write(
        destination.join("created-and-modified.txt"),
        b"created-and-modified",
    )?;
    capture_watch_batch(
        &mut checkout,
        WatchBatch::Changes {
            epoch: 8,
            first_sequence: 3,
            next_sequence: 5,
            changes: vec![
                WatchChange::Created(created_and_modified.clone()),
                WatchChange::Modified(created_and_modified.clone()),
            ],
            original: Default::default(),
        },
        &CaptureOptions {
            maximum_paths: 2,
            ..capture_options.clone()
        },
        &crate::CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert_eq!(
        checkout
            .read_file_range(
                &created_and_modified,
                ByteRange {
                    offset: 0,
                    length: 20,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value
            .bytes
            .as_ref(),
        b"created-and-modified"
    );

    let expected_root_identity = capture_root_identity(&destination)?;
    let displaced = temporary.path().join("displaced-view");
    std::fs::rename(&destination, &displaced)?;
    std::fs::create_dir(&destination)?;
    std::fs::write(destination.join("arrived.txt"), b"untrusted-root")?;
    let swapped = capture_paths(
        &mut checkout,
        std::slice::from_ref(&arrived),
        &CaptureOptions {
            source_root: destination,
            expected_root_identity,
            maximum_paths: 1,
            maximum_extent_spans: 16,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await;
    let Err(swapped) = swapped else {
        return Err("capture accepted a replacement root".into());
    };
    assert!(matches!(swapped.error, CaptureError::RootChanged));
    Ok(())
}

#[tokio::test]
async fn coalesced_parent_child_removals_capture_deepest_first()
-> Result<(), Box<dyn std::error::Error>> {
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
    capture_watch_batch(
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
    .await?;
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

// Stale creation hints must preserve existing directories and their children.
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
        let read = checkout
            .read_file_range(
                path,
                ByteRange {
                    offset: 0,
                    length: (expected.len() as u64),
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        assert_eq!(read.value.bytes.as_ref(), expected);
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
    let read = checkout
        .read_file_range(
            &path,
            ByteRange {
                offset: 0,
                length: 7,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(read.value.bytes.as_ref(), b"created");
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn posix_fifo_and_socket_materialize_and_capture_exactly()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::FileTypeExt;

    let limits = VolumeLimits::default();
    let config = VolumeConfig {
        profile: FilesystemProfile::Posix,
        limits,
        ..VolumeConfig::portable(Lifecycle::Ephemeral)
    };
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let source = fs
        .create_volume_with_id(
            VolumeId::from_bytes([92; 16]),
            config,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let mut source_checkout = private_checkout(&source, &cancellation).await?;
    let fifo = NamespacePath::new(
        vec![LogicalName::new(
            NameEncoding::PosixBytes,
            b"pipe".to_vec(),
            limits.maximum_component_bytes,
        )?],
        limits,
    )?;
    let socket = NamespacePath::new(
        vec![LogicalName::new(
            NameEncoding::PosixBytes,
            b"socket".to_vec(),
            limits.maximum_component_bytes,
        )?],
        limits,
    )?;
    source_checkout
        .create(
            crate::AuthoredMutation::CreateEmptySpecial {
                path: fifo.clone(),
                kind: crate::kernel::FileKind::Fifo,
                metadata: crate::kernel::FileMetadata::default(),
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    source_checkout
        .create(
            crate::AuthoredMutation::CreateEmptySpecial {
                path: socket.clone(),
                kind: crate::kernel::FileKind::Socket,
                metadata: crate::kernel::FileMetadata::default(),
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;

    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("special-view");
    std::fs::create_dir(&destination)?;
    let materialized = materialize_checkout(
        &mut source_checkout,
        &MaterializeOptions {
            destination: destination.clone(),
            expected_root_identity: None,
            maximum_directory_entries: 16,
            maximum_extent_spans: 16,
            transfer_bytes: 64 * 1024,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert_eq!(materialized.value.special_files, 2);
    assert!(
        std::fs::symlink_metadata(destination.join("pipe"))?
            .file_type()
            .is_fifo()
    );
    assert!(
        std::fs::symlink_metadata(destination.join("socket"))?
            .file_type()
            .is_socket()
    );

    let captured = fs
        .create_volume_with_id(
            VolumeId::from_bytes([93; 16]),
            config,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let mut captured_checkout = private_checkout(&captured, &cancellation).await?;
    let receipt = capture_baseline(
        &mut captured_checkout,
        &CaptureOptions {
            expected_root_identity: capture_root_identity(&destination)?,
            source_root: destination,
            maximum_paths: 2,
            maximum_extent_spans: 16,
        },
        &crate::CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert_eq!(receipt.value.changed_paths, 2);
    assert_eq!(
        captured_checkout
            .lookup_no_follow(&fifo, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("captured FIFO is absent")?
            .kind,
        crate::kernel::FileKind::Fifo
    );
    assert_eq!(
        captured_checkout
            .lookup_no_follow(&socket, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("captured socket is absent")?
            .kind,
        crate::kernel::FileKind::Socket
    );
    Ok(())
}
