use super::*;

#[tokio::test]
async fn tracking_reads_capture_only_their_exact_terminal_regions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([81; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file = path("file")?;
    let mut seed = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (seed.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from(vec![b'a'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (seed.commit(OperationId::from_bytes([82; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;

    let mut content_reader = (volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (content_reader.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 8,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    let mut metadata_writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut changed_metadata = FileMetadata::default();
    changed_metadata.modified_ns = MetadataField::Value(7);
    (metadata_writer.apply_authored_transaction(vec![crate::AuthoredMutation::SetMetadata { path: file.clone(), metadata: changed_metadata }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    (metadata_writer.commit(OperationId::from_bytes([83; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let content_rebase =
        (content_reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(matches!(content_rebase.value, RebaseDecision::Safe { .. }));

    let mut metadata_reader = (volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (metadata_reader.read_metadata(&file, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let mut content_writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (content_writer.change_content(file.clone(), crate::ContentChange::Write { offset: 64, bytes: (bytes::Bytes::from_static(b"changed")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (content_writer.commit(OperationId::from_bytes([84; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let metadata_rebase =
        (metadata_reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(matches!(metadata_rebase.value, RebaseDecision::Safe { .. }));

    let mut generic_reader = (volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (generic_reader.lookup_no_follow(&file, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let mut final_writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (final_writer.change_content(file, crate::ContentChange::Write { offset: 96, bytes: (bytes::Bytes::from_static(b"different")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (final_writer.commit(OperationId::from_bytes([85; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let generic_rebase =
        (generic_reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
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
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([101; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file = path("sparse")?;
    let mut seed = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (seed.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from(vec![b'a'; 256])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (seed.commit(OperationId::from_bytes([102; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;

    let mut local = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (local.change_content(file.clone(), crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 128,
            length: 16,
        }, allocated: false, extend: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let local_observation = (local.seek_file_extent(
        &file,
        128,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(local_observation.value, Some(128));

    let mut disjoint = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (disjoint.change_content(file.clone(), crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 0,
            length: 64,
        }, allocated: false, extend: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (disjoint.commit(OperationId::from_bytes([103; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let safe = (local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(matches!(safe.value, RebaseDecision::Safe { .. }));
    let retained = (local.seek_file_extent(
        &file,
        128,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(retained.value, Some(128));

    let mut reader = (volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let observation = (reader.seek_file_extent(
        &file,
        128,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(observation.value, Some(256));

    let mut conflicting = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (conflicting.change_content(file, crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 192,
            length: 16,
        }, allocated: false, extend: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (conflicting.commit(OperationId::from_bytes([104; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let conflict = (reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
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
async fn clipped_identity_read_at_eof_tracks_file_length() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([190; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file = path("file")?;
    let mut seed = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file_id = (seed.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"abc")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
    .value;
    (seed.commit(OperationId::from_bytes([190; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;

    let mut reader = (volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let at_eof = (reader.read_file_up_to_by_id(
        file_id,
        3,
        16,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(at_eof.value.bytes.is_empty());

    let mut writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (writer.change_content(file, crate::ContentChange::Write { offset: 3, bytes: (bytes::Bytes::from_static(b"d")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (writer.commit(OperationId::from_bytes([191; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let rebase = (reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
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
    sparse_rebase_across_remote_ranges(false).await
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
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([151; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut local = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut remote = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (local.create(crate::AuthoredMutation::CreateFile { path: path("authored")?, bytes: (bytes::Bytes::from_static(b"local")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;

    let observed = path("observed")?;
    let first = (local.lookup_no_follow_with_metadata(
        &observed,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(first.value.is_none());
    let second = (local.lookup_no_follow_with_metadata(
        &observed,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(second.value.is_none());

    (remote.create(crate::AuthoredMutation::CreateFile { path: observed, bytes: (bytes::Bytes::from_static(b"remote")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (remote.commit(OperationId::from_bytes([152; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let rebase = (local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(matches!(rebase.value, RebaseDecision::Conflicted { .. }));
    Ok(())
}

#[tokio::test]
async fn batch_lookups_preserve_each_binding_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    for (seed, authored) in [(171_u8, false), (181, true)] {
        let fs = Fs::memory();
        let cancellation = CancellationToken::new();
        let volume = (fs.create_volume_with_id(
            VolumeId::from_bytes([seed; 16]),
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value;
        let mut local = (volume.checkout(
            GenerationSelector::Head,
            writable_tracking(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value;
        let mut remote = (volume.checkout(
            GenerationSelector::Head,
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value;
        if authored {
            (local.create(crate::AuthoredMutation::CreateFile { path: path("authored")?, bytes: (bytes::Bytes::from_static(b"local")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
        }
        let paths = [path("first")?, path("second")?];
        let batch =
        (local.lookup_batch_no_follow(&paths, WorkBudget::UNBOUNDED, &cancellation)).await?;
        assert!(
            batch
                .value
                .entries
                .iter()
                .all(|entry| entry.record.is_none())
        );
        (local.lookup_batch_no_follow(&paths, WorkBudget::UNBOUNDED, &cancellation)).await?;

        (remote.create(crate::AuthoredMutation::CreateFile { path: path("second")?, bytes: (bytes::Bytes::from_static(b"remote")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
        (remote.commit(OperationId::from_bytes([seed.wrapping_add(1); 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
        let rebase = (local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
        assert!(matches!(rebase.value, RebaseDecision::Conflicted { .. }));
    }
    Ok(())
}
