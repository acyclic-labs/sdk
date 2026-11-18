use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_content_stream_and_clone_keep_real_identity_and_atomic_stamps()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("original-content-stream", B, &cancel)
        .await?
        .value;
    workspace
        .write_text("/source", "authenticated source bytes")
        .await?;
    workspace
        .write_text("/destination", "old destination")
        .await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            writable_pinned(),
            B,
            &cancellation,
        )
        .await?
        .value;
    let source = checkout
        .lookup_no_follow(&path("source")?, B, &cancellation)
        .await?
        .value
        .record
        .ok_or("actual source absent")?;
    let destination = checkout
        .lookup_no_follow(&path("destination")?, B, &cancellation)
        .await?
        .value
        .record
        .ok_or("actual destination absent")?;
    let metadata = FileMetadata {
        modified_ns: MetadataField::Value(1),
        changed_ns: MetadataField::Value(2),
        accessed_ns: MetadataField::Value(3),
        ..FileMetadata::default()
    };
    checkout
        .set_attributes(destination.file_id, metadata, None, B, &cancellation)
        .await?;
    let original =
        crate::NativeMaterializationWork::new(B, &cancellation).map_err(|(cause, _)| cause)?;
    let reader = checkout
        .private_candidate(
            &mut crate::kernel::KernelAccounting::native(&original, &cancellation),
            &cancellation,
        )?
        .into_pinned_reader_with_native_work(&original, &cancellation)?;
    let bytes = reader
        .read_file_range(
            &path("source")?,
            ByteRange {
                offset: 0,
                length: 26,
            },
            &original,
            &cancellation,
        )
        .await?
        .bytes;
    assert_eq!(bytes.as_ref(), b"authenticated source bytes");
    let mut candidate = checkout.private_candidate(
        &mut crate::kernel::allocation::KernelAccounting::native(&original, &cancellation),
        &cancellation,
    )?;
    candidate
        .change_content(
            destination.file_id,
            ContentChange::Write { offset: 0, bytes },
            ContentTimes::Stamp(40),
            &original,
            &cancellation,
        )
        .await?;
    candidate
        .change_content(
            destination.file_id,
            ContentChange::CloneFrom {
                source: source.file_id,
                source_offset: 14,
                offset: 0,
                length: 6,
            },
            ContentTimes::Stamp(41),
            &original,
            &cancellation,
        )
        .await?;
    let actual = candidate
        .read_file_record_by_id(destination.file_id, &original, &cancellation)
        .await?;
    assert_eq!(actual.file_id, destination.file_id);
    let content = candidate
        .read_file_range(
            destination.file_id,
            ByteRange {
                offset: 0,
                length: 26,
            },
            &original,
            &cancellation,
        )
        .await?;
    assert_eq!(content.bytes.as_ref(), b"sourceticated source bytes");
    let stamped = candidate
        .read_metadata(destination.file_id, &original, &cancellation)
        .await?;
    assert_eq!(stamped.modified_ns, MetadataField::Value(41));
    assert_eq!(stamped.changed_ns, MetadataField::Value(41));
    assert_eq!(stamped.accessed_ns, MetadataField::Value(3));
    assert!(original.receipt().unwrap().source_bytes_read >= 26);
    assert!(original.receipt().unwrap().bytes_encoded > 0);
    assert_eq!(
        checkout
            .read_metadata(destination.file_id, B, &cancellation,)
            .await?
            .value,
        metadata,
        "private native content cannot stamp its source checkout"
    );
    Ok(())
}
