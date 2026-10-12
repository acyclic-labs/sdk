use super::*;

#[tokio::test]
async fn tracking_reads_capture_only_their_exact_terminal_regions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([81; 16]), config(), B, &cancellation))
            .await?
            .value;
    let file = path("file")?;
    let mut seed = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (seed.create(
        AuthoredMutation::CreateFile {
            path: file.clone(),
            bytes: OwnedBytes::from(vec![b'a'; 128]),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    (seed.commit(
        OperationId::from_bytes([82; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;

    let mut content_reader =
        (volume.checkout(GenerationSelector::Head, tracking(), B, &cancellation))
            .await?
            .value;
    (content_reader.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 8,
        },
        B,
        &cancellation,
    ))
    .await?;
    let mut metadata_writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let mut changed_metadata = empty_metadata();
    changed_metadata.modified_ns = MetadataField::Value(7);
    (metadata_writer.set_attributes(file.clone(), changed_metadata, None, B, &cancellation))
        .await?;
    (metadata_writer.commit(
        OperationId::from_bytes([83; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    let content_rebase = (content_reader.rebase_head(8, B, &cancellation)).await?;
    assert!(matches!(content_rebase.value, RebaseDecision::Safe { .. }));

    let mut metadata_reader =
        (volume.checkout(GenerationSelector::Head, tracking(), B, &cancellation))
            .await?
            .value;
    (metadata_reader.read_metadata(&file, B, &cancellation)).await?;
    let mut content_writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (content_writer.change_content(
        file.clone(),
        ContentChange::Write {
            offset: 64,
            bytes: OwnedBytes::from_static(b"changed"),
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    (content_writer.commit(
        OperationId::from_bytes([84; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    let metadata_rebase = (metadata_reader.rebase_head(8, B, &cancellation)).await?;
    assert!(matches!(metadata_rebase.value, RebaseDecision::Safe { .. }));

    let mut generic_reader =
        (volume.checkout(GenerationSelector::Head, tracking(), B, &cancellation))
            .await?
            .value;
    (generic_reader.lookup_no_follow(&file, B, &cancellation)).await?;
    let mut final_writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (final_writer.change_content(
        file,
        ContentChange::Write {
            offset: 96,
            bytes: OwnedBytes::from_static(b"different"),
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    (final_writer.commit(
        OperationId::from_bytes([85; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    let generic_rebase = (generic_reader.rebase_head(8, B, &cancellation)).await?;
    assert!(matches!(
        generic_rebase.value,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::FileRecord(_)
            ))
    ));
    Ok(())
}

#[tokio::test]
async fn sparse_seek_dependencies_track_base_semantics_and_exact_observed_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([101; 16]), config(), B, &cancellation))
            .await?
            .value;
    let file = path("sparse")?;
    let mut seed = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (seed.create(
        AuthoredMutation::CreateFile {
            path: file.clone(),
            bytes: OwnedBytes::from(vec![b'a'; 256]),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    (seed.commit(
        OperationId::from_bytes([102; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;

    let mut local = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (local.change_content(
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
        &cancellation,
    ))
    .await?;
    let local_observation =
        (local.seek_file_extent(&file, 128, ExtentSeekTarget::Hole, B, &cancellation)).await?;
    assert_eq!(local_observation.value, Some(128));

    let mut disjoint = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (disjoint.change_content(
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
        &cancellation,
    ))
    .await?;
    (disjoint.commit(
        OperationId::from_bytes([103; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    let safe = (local.rebase_head(8, B, &cancellation)).await?;
    assert!(matches!(safe.value, RebaseDecision::Safe { .. }));
    let retained =
        (local.seek_file_extent(&file, 128, ExtentSeekTarget::Hole, B, &cancellation)).await?;
    assert_eq!(retained.value, Some(128));

    let mut reader = (volume.checkout(GenerationSelector::Head, tracking(), B, &cancellation))
        .await?
        .value;
    let observation =
        (reader.seek_file_extent(&file, 128, ExtentSeekTarget::Hole, B, &cancellation)).await?;
    assert_eq!(observation.value, Some(256));

    let mut conflicting = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (conflicting.change_content(
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
        &cancellation,
    ))
    .await?;
    (conflicting.commit(
        OperationId::from_bytes([104; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    let conflict = (reader.rebase_head(8, B, &cancellation)).await?;
    assert!(matches!(
        conflict.value,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::SparseSeek {
                    offset: 128,
                    target: ExtentSeekTarget::Hole,
                    ..
                }
            ))
    ));

    Ok(())
}

#[tokio::test]
async fn clipped_identity_read_at_eof_tracks_file_length() -> Result<(), Box<dyn std::error::Error>>
{
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([190; 16]), config(), B, &cancellation))
            .await?
            .value;
    let file = path("file")?;
    let mut seed = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let file_id = (seed.create(
        AuthoredMutation::CreateFile {
            path: file.clone(),
            bytes: OwnedBytes::from_static(b"abc"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?
    .value;
    (seed.commit(
        OperationId::from_bytes([190; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;

    let mut reader = (volume.checkout(GenerationSelector::Head, tracking(), B, &cancellation))
        .await?
        .value;
    let at_eof = (reader.read_file_up_to_by_id(file_id, 3, 16, B, &cancellation)).await?;
    assert!(at_eof.value.bytes.is_empty());

    let mut writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (writer.change_content(
        file,
        ContentChange::Write {
            offset: 3,
            bytes: OwnedBytes::from_static(b"d"),
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    (writer.commit(
        OperationId::from_bytes([191; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    let rebase = (reader.rebase_head(8, B, &cancellation)).await?;
    assert!(matches!(
        rebase.value,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::FileLength(actual) if actual == file_id
            ))
    ));
    Ok(())
}

#[tokio::test]
async fn sparse_local_writes_rebase_across_disjoint_remote_ranges_and_conflict_on_overlap()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([86; 16]), config(), B, &cancellation))
            .await?
            .value;
    let file = path("file")?;
    let mut seed = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (seed.create(
        AuthoredMutation::CreateFile {
            path: file.clone(),
            bytes: OwnedBytes::from(vec![b'a'; 128]),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    (seed.commit(
        OperationId::from_bytes([87; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;

    let mut local = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let mut remote = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (local.change_content(
        file.clone(),
        ContentChange::Write {
            offset: 0,
            bytes: OwnedBytes::from_static(b"LOCAL"),
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    (remote.change_content(
        file.clone(),
        ContentChange::Write {
            offset: 64,
            bytes: OwnedBytes::from_static(b"REMOTE"),
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    (remote.commit(
        OperationId::from_bytes([88; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    let safe = (local.rebase_head(8, B, &cancellation)).await?;
    assert!(matches!(safe.value, RebaseDecision::Safe { .. }));
    let local_bytes = (local.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 70,
        },
        B,
        &cancellation,
    ))
    .await?;
    assert_eq!(&local_bytes.value.bytes[..5], b"LOCAL");
    assert_eq!(&local_bytes.value.bytes[64..70], b"REMOTE");

    let mut overlapping_local = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let mut overlapping_remote = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (overlapping_local.change_content(
        file.clone(),
        ContentChange::Write {
            offset: 10,
            bytes: OwnedBytes::from_static(b"local"),
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    (overlapping_remote.change_content(
        file,
        ContentChange::Write {
            offset: 12,
            bytes: OwnedBytes::from_static(b"remote"),
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    (overlapping_remote.commit(
        OperationId::from_bytes([89; 16]),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancellation,
        allow::<FsError>,
    ))
    .await?;
    let conflict = (overlapping_local.rebase_head(8, B, &cancellation)).await?;
    assert!(matches!(
        conflict.value,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::ContentRange { .. }
            ))
    ));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_native_rebase_preserves_disjoint_ranges_and_is_atomic_on_denial_and_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    sparse_rebase_across_remote_ranges(true).await
}

#[tokio::test]
async fn repeated_metadata_lookups_preserve_binding_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([151; 16]), config(), B, &cancellation))
            .await?
            .value;
    let mut local = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let mut remote = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (local.create(
        crate::AuthoredMutation::CreateFile {
            path: path("authored")?,
            bytes: (bytes::Bytes::from_static(b"local")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;

    let observed = path("observed")?;
    let first = (local.lookup_no_follow_with_metadata(&observed, B, &cancellation)).await?;
    assert!(first.value.is_none());
    let second = (local.lookup_no_follow_with_metadata(&observed, B, &cancellation)).await?;
    assert!(second.value.is_none());

    (remote.create(
        crate::AuthoredMutation::CreateFile {
            path: observed,
            bytes: (bytes::Bytes::from_static(b"remote")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    (remote.commit(
        OperationId::from_bytes([152; 16]),
        None,
        None,
        None,
        crate::PublicationPermit::Unrestricted,
        B,
        &cancellation,
        |_| async {
            crate::FsResult::Ok(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::UNCHARGED,
            })
        },
    ))
    .await?;
    let rebase = (local.rebase_head(8, B, &cancellation)).await?;
    assert!(matches!(rebase.value, RebaseDecision::Conflicted { .. }));
    Ok(())
}

#[tokio::test]
async fn batch_lookups_preserve_each_binding_conflict() -> Result<(), Box<dyn std::error::Error>> {
    for (seed, authored) in [(171_u8, false), (181, true)] {
        let fs = Fs::memory();
        let cancellation = CancellationToken::new();
        let volume = (fs.create_volume_with_id(
            VolumeId::from_bytes([seed; 16]),
            config(),
            B,
            &cancellation,
        ))
        .await?
        .value;
        let mut local = (volume.checkout(
            GenerationSelector::Head,
            writable_tracking(),
            B,
            &cancellation,
        ))
        .await?
        .value;
        let mut remote = (volume.checkout(
            GenerationSelector::Head,
            writable_pinned(),
            B,
            &cancellation,
        ))
        .await?
        .value;
        if authored {
            (local.create(
                crate::AuthoredMutation::CreateFile {
                    path: path("authored")?,
                    bytes: (bytes::Bytes::from_static(b"local")).into(),
                    metadata: crate::kernel::FileMetadata::default(),
                },
                B,
                &cancellation,
            ))
            .await?;
        }
        let paths = [path("first")?, path("second")?];
        let batch = (local.lookup_batch_no_follow(&paths, B, &cancellation)).await?;
        assert!(
            batch
                .value
                .entries
                .iter()
                .all(|entry| entry.record.is_none())
        );
        (local.lookup_batch_no_follow(&paths, B, &cancellation)).await?;

        (remote.create(
            crate::AuthoredMutation::CreateFile {
                path: path("second")?,
                bytes: (bytes::Bytes::from_static(b"remote")).into(),
                metadata: crate::kernel::FileMetadata::default(),
            },
            B,
            &cancellation,
        ))
        .await?;
        (remote.commit(
            OperationId::from_bytes([seed.wrapping_add(1); 16]),
            None,
            None,
            None,
            crate::PublicationPermit::Unrestricted,
            B,
            &cancellation,
            |_| async {
                crate::FsResult::Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                })
            },
        ))
        .await?;
        let rebase = (local.rebase_head(8, B, &cancellation)).await?;
        assert!(matches!(rebase.value, RebaseDecision::Conflicted { .. }));
    }
    Ok(())
}
