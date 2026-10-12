use super::*;

#[tokio::test]
async fn path_sdk_exposes_every_sparse_operation_with_one_authenticated_state()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume =
        (fs.create_volume_with_id(VolumeId::from_bytes([117; 16]), config(), B, &cancellation))
            .await?
            .value;
    assert_eq!(volume.id(), VolumeId::from_bytes([117; 16]));
    assert_eq!(volume.config(), config());
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let source = path("source-path")?;
    let destination = path("destination-path")?;
    let source_id = (checkout.create(
        AuthoredMutation::CreateFile {
            path: source.clone(),
            bytes: OwnedBytes::from(vec![b's'; 128]),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?
    .value;
    (checkout.create(
        AuthoredMutation::CreateFile {
            path: destination.clone(),
            bytes: OwnedBytes::from(vec![b'd'; 128]),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let mut metadata = FileMetadata::default();
    metadata.modified_ns = MetadataField::Value(99);
    (checkout.set_attributes(source.clone(), metadata, None, B, &cancellation)).await?;
    (checkout.change_content(
        destination.clone(),
        ContentChange::Resize { logical_bytes: 160 },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    (checkout.change_content(
        source.clone(),
        ContentChange::ZeroRange {
            range: ByteRange {
                offset: 64,
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
    (checkout.change_content(
        destination.clone(),
        ContentChange::Preallocate {
            range: ByteRange {
                offset: 128,
                length: 32,
            },
            keep_size: false,
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    (checkout.change_content(
        FileCloneRequest {
            source: source.clone(),
            source_offset: 0,
            destination: destination.clone(),
            destination_offset: 32,
            length: 96,
        }
        .destination,
        ContentChange::CloneFrom {
            source: FileCloneRequest {
                source: source.clone(),
                source_offset: 0,
                destination: destination.clone(),
                destination_offset: 32,
                length: 96,
            }
            .source,
            source_offset: FileCloneRequest {
                source: source.clone(),
                source_offset: 0,
                destination: destination.clone(),
                destination_offset: 32,
                length: 96,
            }
            .source_offset,
            offset: FileCloneRequest {
                source: source.clone(),
                source_offset: 0,
                destination: destination.clone(),
                destination_offset: 32,
                length: 96,
            }
            .destination_offset,
            length: FileCloneRequest {
                source: source.clone(),
                source_offset: 0,
                destination: destination.clone(),
                destination_offset: 32,
                length: 96,
            }
            .length,
        },
        ContentTimes::Preserve,
        B,
        &cancellation,
    ))
    .await?;
    let alias = path("source-alias")?;
    let renamed = path("source-renamed")?;
    (checkout.mutate(
        vec![Mutation::Link {
            source: source.clone(),
            destination: alias.clone(),
        }],
        B,
        &cancellation,
    ))
    .await?;
    (checkout.mutate(
        vec![Mutation::Rename {
            source: alias,
            destination: renamed.clone(),
            replace: false,
        }],
        B,
        &cancellation,
    ))
    .await?;
    let lookup = (checkout.lookup_no_follow_with_metadata(&renamed, B, &cancellation))
        .await?
        .value
        .ok_or("renamed link is absent")?;
    assert_eq!(lookup.record.file_id, source_id);
    assert_eq!(lookup.metadata.modified_ns, MetadataField::Value(99));
    let batch = (checkout.lookup_batch_no_follow(
        &[source.clone(), renamed.clone(), path("absent-path")?],
        B,
        &cancellation,
    ))
    .await?;
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
        B,
        &cancellation,
    ))
    .await?;
    assert_eq!(records.value.entries.len(), 3);
    let plan = (checkout.plan_file_extents(
        &source,
        ByteRange {
            offset: 0,
            length: 128,
        },
        8,
        B,
        &cancellation,
    ))
    .await?;
    assert!(plan.value.is_some());
    assert_eq!(
        (checkout.seek_file_extent(&source, 64, ExtentSeekTarget::Hole, B, &cancellation,))
            .await?
            .value,
        Some(64)
    );
    assert_eq!(
        (checkout.read_metadata(&source, B, &cancellation,))
            .await?
            .value,
        metadata
    );
    (checkout.mutate(
        vec![Mutation::Remove {
            path: renamed,
            expected_file_id: Some(source_id)
                .map_or(MetadataField::Unavailable, MetadataField::Value),
        }],
        B,
        &cancellation,
    ))
    .await?;

    let mut empty = std::io::Cursor::new(OwnedBytes::new());
    assert!(
        (checkout.stage_content(&mut empty, 0, B, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (checkout.stage_content(
            &mut empty,
            config().limits.maximum_generation_bytes + 1,
            B,
            &cancellation,
        ))
        .await
        .is_err()
    );
    Ok(())
}
