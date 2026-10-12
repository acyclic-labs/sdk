use super::*;

#[tokio::test]
async fn path_sdk_exposes_every_sparse_operation_with_one_authenticated_state()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([117; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    assert_eq!(volume.id(), VolumeId::from_bytes([117; 16]));
    assert_eq!(volume.config(), config());
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let source = path("source-path")?;
    let destination = path("destination-path")?;
    let source_id = (checkout.create(crate::AuthoredMutation::CreateFile { path: source.clone(), bytes: (bytes::Bytes::from(vec![b's'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
    .value;
    (checkout.create(crate::AuthoredMutation::CreateFile { path: destination.clone(), bytes: (bytes::Bytes::from(vec![b'd'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let mut metadata = FileMetadata::default();
    metadata.modified_ns = MetadataField::Value(99);
    (checkout.apply_authored_transaction(vec![crate::AuthoredMutation::SetMetadata { path: source.clone(), metadata: metadata }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.change_content(destination.clone(), crate::ContentChange::Resize { logical_bytes: 160 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.change_content(source.clone(), crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 64,
            length: 16,
        }, allocated: false, extend: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.change_content(destination.clone(), crate::ContentChange::Preallocate { range: ByteRange {
            offset: 128,
            length: 32,
        }, keep_size: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.change_content(destination.clone(), ContentChange::CloneFrom { source: source.clone(), source_offset: 0, offset: 32, length: 96 }, ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let alias = path("source-alias")?;
    let renamed = path("source-renamed")?;
    (checkout.mutate(vec![crate::kernel::Mutation::Link { source: source.clone(), destination: alias.clone() }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.mutate(vec![crate::kernel::Mutation::Rename { source: alias, destination: renamed.clone(), replace: false }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    let lookup = (checkout.lookup_no_follow_with_metadata(
        &renamed,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value
    .ok_or("renamed link is absent")?;
    assert_eq!(lookup.record.file_id, source_id);
    assert_eq!(lookup.metadata.modified_ns, MetadataField::Value(99));
    let batch = (checkout.lookup_batch_no_follow(
        &[source.clone(), renamed.clone(), path("absent-path")?],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(batch.value.entries.len(), 3);
    assert_eq!(
        batch.value.entries[0].record.map(|record| record.file_id),
        Some(source_id)
    );
    assert_eq!(
        batch.value.entries[1].record.map(|record| record.file_id),
        Some(source_id)
    );
    assert!(batch.value.entries[2].record.is_none());
    let records = (checkout.list_directory_records(
        &root_path()?,
        std::ops::Bound::Unbounded,
        16,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(records.value.entries.len(), 3);
    let plan = (checkout.plan_file_extents(
        &source,
        ByteRange {
            offset: 0,
            length: 128,
        },
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(plan.value.is_some());
    assert_eq!(
        (checkout.seek_file_extent(
            &source,
            64,
            ExtentSeekTarget::Hole,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value,
        Some(64)
    );
    assert_eq!(
        (checkout.read_metadata(&source, WorkBudget::UNBOUNDED, &cancellation,)).await?
            .value,
        metadata
    );
    (checkout.mutate(vec![crate::kernel::Mutation::Remove { path: renamed, expected_file_id: (Some(source_id)).map_or(crate::kernel::MetadataField::Unavailable, crate::kernel::MetadataField::Value) }], WorkBudget::UNBOUNDED, &cancellation)).await?;

    let mut empty = std::io::Cursor::new(OwnedBytes::new());
    assert!(
        (checkout.stage_content(&mut empty, 0, WorkBudget::UNBOUNDED, &cancellation,)).await
            .is_err()
    );
    assert!(
        (checkout.stage_content(
            &mut empty,
            config().limits.maximum_generation_bytes + 1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    #[cfg(not(target_arch = "wasm32"))]
    {
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
        let body = [b'n'; 256];
        let mut source = std::io::Cursor::new(body.as_slice());
        let staged = (checkout.stage_content_with_accounting(
            &mut source, body.len() as u64, &mut accounting, &cancellation,
        )).await?;
        assert_eq!(staged.logical_bytes(), body.len() as u64);
        assert_eq!(original.receipt().source_bytes_read, body.len() as u64);
        assert!(original.receipt().backend_write_operations > 0);

        let denied = crate::NativeMaterializationWork::new(
            WorkBudget { source_bytes_read: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).map_err(|(cause, _)| cause)?;
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&denied, &cancellation);
        let mut source = std::io::Cursor::new(body.as_slice());
        assert!((checkout.stage_content_with_accounting(
            &mut source, body.len() as u64, &mut accounting, &cancellation,
        )).await.is_err());
        assert_eq!(source.position(), 0);
        assert_eq!(denied.receipt().backend_write_operations, 0);

        struct PrefixFailure(bool);
        impl crate::kernel::AsyncBlobSource for PrefixFailure {
            async fn read<'a>(
                &'a mut self, destination: &'a mut [u8], _budget: WorkBudget,
                _cancellation: &'a CancellationToken,
            ) -> std::io::Result<usize> {
                if self.0 { return Err(std::io::Error::other("content source failed")); }
                self.0 = true;
                let count = destination.len().min(4);
                destination[..count].fill(b'p');
                Ok(count)
            }
            async fn read_with_native_work<'a>(
                &'a mut self, destination: &'a mut [u8], original: &'a crate::NativeMaterializationWork,
                cancellation: &'a CancellationToken,
            ) -> Result<usize, FsError> {
                cancellation.check()?;
                let delta = WorkCounters {
                    source_bytes_read: destination.len().min(4) as u64,
                    bytes_copied: destination.len().min(4) as u64,
                    ..WorkCounters::UNCHARGED
                };
                original.admit(delta)?;
                let count = self.read(destination, original.remaining()?, cancellation).await
                    .map_err(crate::kernel::BlobBuildError::Source)?;
                original.spent(delta)?;
                Ok(count)
            }
        }
        let failed = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&failed, &cancellation);
        let mut source = PrefixFailure(false);
        assert!((checkout.stage_content_with_accounting(
            &mut source, 64, &mut accounting, &cancellation,
        )).await.is_err());
        assert_eq!(failed.receipt().source_bytes_read, 4);

        let cancelled = CancellationToken::new();
        let stopped = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancelled)
            .map_err(|(cause, _)| cause)?;
        cancelled.cancel();
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&stopped, &cancelled);
        let mut source = std::io::Cursor::new(body.as_slice());
        assert!((checkout.stage_content_with_accounting(
            &mut source, body.len() as u64, &mut accounting, &cancelled,
        )).await.is_err());
        assert_eq!(source.position(), 0);
        assert_eq!(stopped.receipt().backend_write_operations, 0);
    }
    Ok(())
}
