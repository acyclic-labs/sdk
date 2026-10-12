use super::*;

#[tokio::test]
async fn manifest_transfer_restores_only_after_the_complete_closure_authenticates()
-> Result<(), Box<dyn std::error::Error>> {
    let source = Fs::memory();
    let destination = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (source.create_volume_with_id(
        VolumeId::from_bytes([97; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (checkout.create(crate::AuthoredMutation::CreateFile { path: path("file")?, bytes: (bytes::Bytes::from(vec![42; 4096])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let first_checkpoint = (checkout.checkpoint(WorkBudget::UNBOUNDED, &cancellation)).await?;
    let repeated_checkpoint = (checkout.checkpoint(WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(first_checkpoint.value, repeated_checkpoint.value);
    let manifest = (checkout.export_manifest(WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    assert_eq!(manifest.generation_id, first_checkpoint.value);
    assert!(manifest.objects.contains(&manifest.generation_root));

    let mut cursor = TransferCursor::START;
    loop {
        let exported = (source.export_generation_batch(
            &manifest,
            cursor,
            2,
            config().limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?;
        let bodies = exported
            .value
            .objects
            .iter()
            .map(|object| object.bytes.clone())
            .collect::<Vec<_>>();
        let imported = (destination.import_generation_batch(
            &manifest,
            cursor,
            &bodies,
            2,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?;
        assert_eq!(
            imported.value.next_object(),
            cursor.next_object() + u64::try_from(bodies.len())?
        );
        let Some(next) = exported.value.next else {
            assert_eq!(
                imported.value.next_object(),
                u64::try_from(manifest.objects.len())?
            );
            break;
        };
        assert_eq!(imported.value, next);
        cursor = next;
    }
    let mut wrong_volume = manifest.clone();
    wrong_volume.volume_id = VolumeId::from_bytes([96; 16]);
    let mut wrong_generation = manifest.clone();
    wrong_generation.generation_id = GenerationId::new(Digest::from_bytes([0xee; 32]));
    let mut wrong_objects = manifest.clone();
    wrong_objects
        .objects
        .pop()
        .ok_or("manifest closure is empty")?;
    let mut wrong_file_count = manifest.clone();
    wrong_file_count.file_count = wrong_file_count
        .file_count
        .checked_add(1)
        .ok_or("fixture file count overflow")?;
    for (label, invalid, expected_backend_reads) in [
        ("volume", wrong_volume, true),
        ("generation", wrong_generation, false),
        ("objects", wrong_objects, true),
        ("file-count", wrong_file_count, true),
    ] {
        let failure = (destination.restore_volume(
            &invalid,
            OperationId::from_bytes([31; 16]),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .err()
        .ok_or("invalid manifest unexpectedly restored")?;
        assert!(matches!(failure.error, FsError::InvalidExportManifest));
        assert_eq!(
            failure.work.backend_read_operations > 0,
            expected_backend_reads,
            "unexpected invalid-manifest physical plan for {label}"
        );
        assert_eq!(failure.work.authority_records_appended, 0);
    }
    let restore_operation = OperationId::from_bytes([32; 16]);
    let restored = (destination.restore_volume(
        &manifest,
        restore_operation,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let retried = (destination.restore_volume(
        &manifest,
        restore_operation,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    assert_eq!(retried.id(), restored.id());
    let changed_operation = (destination.restore_volume(
        &manifest,
        OperationId::from_bytes([33; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("changed-operation restore unexpectedly succeeded")?;
    assert!(matches!(changed_operation.error, FsError::CreationRejected));
    let mut restored_checkout = (restored.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let bytes = (restored_checkout.read_file_range(
        &path("file")?,
        ByteRange {
            offset: 0,
            length: 4096,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(bytes.value.bytes.as_ref(), vec![42; 4096]);
    Ok(())
}
