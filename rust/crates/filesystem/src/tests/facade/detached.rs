use super::*;

#[tokio::test]
async fn metadata_reads_preserve_the_original_decoder_work() -> TestResult {
    let metadata = FileMetadata {
        posix_mode: MetadataField::Value(0o100_640),
        posix_uid: MetadataField::Value(41),
        posix_gid: MetadataField::Value(42),
        posix_flags: MetadataField::Value(43),
        windows_attributes: MetadataField::Value(44),
        created_ns: MetadataField::Value(45),
        modified_ns: MetadataField::Value(46),
        accessed_ns: MetadataField::Value(47),
        changed_ns: MetadataField::Value(48),
        ..FileMetadata::default()
    };
    let minimal = FileMetadata {
        posix_uid: metadata.posix_uid,
        ..FileMetadata::default()
    };
    let (minimal_decode, minimal_closure) = metadata_work_trace(minimal).await?;
    let (complete_decode, complete_closure) = metadata_work_trace(metadata).await?;
    // Both real generations have the same topology and object classes. Only
    // the file metadata's scalar payload differs; its original decoder trace
    // must account for the entire change in closure copy work.
    assert_eq!(
        complete_closure - minimal_closure,
        complete_decode - minimal_decode,
        "metadata closure discarded its original decoder work"
    );
    Ok(())
}

async fn metadata_work_trace(metadata: FileMetadata) -> TestResult<(u64, u64)> {
    use crate::kernel::{DecodeLimits, ReadAllocationStep, decode_file_metadata};

    let cancel = CancellationToken::new();
    let (volume, mut checkout) = fixture(config(), &cancel).await?;
    let file = path("metadata")?;
    checkout
        .create(
            AuthoredMutation::CreateFile {
                file_id: None,
                path: file.clone(),
                bytes: OwnedBytes::from_static(b"original"),
                metadata,
            },
            B,
            &cancel,
        )
        .await?;
    let detached = checkout.detach_regular_file(&file, B, &cancel).await?.value;
    let record = checkout
        .lookup_no_follow(&file, B, &cancel)
        .await?
        .value
        .record
        .ok_or("created file is missing")?;
    // Capture the actual stored object and its physical read work. The same
    // production decoder supplies the independent completion trace; no codec
    // sizes or adapter counters are restated in this oracle.
    let mut physical = crate::AuthorityWork::new(B, &cancel);
    let limits = DecodeLimits::for_volume(config());
    let read = volume
        .fs
        .objects()
        .read(record.metadata, limits.maximum_object_bytes, &mut physical)
        .await?;
    let mut copied = physical.spent().unwrap().bytes_copied;
    let mut examined = physical.spent().unwrap().items_examined;
    assert_eq!(
        decode_file_metadata(&read.bytes, limits, &mut |step| {
            match step {
                ReadAllocationStep::Copied(bytes) => copied += bytes,
                ReadAllocationStep::Item => examined += 1,
                _ => {}
            }
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?,
        metadata
    );
    for maximum in 0..=copied {
        let result = detached
            .read_metadata(
                WorkBudget {
                    bytes_copied: maximum,
                    ..B
                },
                &cancel,
            )
            .await;
        if maximum == copied {
            let receipt = result?;
            assert_eq!(receipt.value, metadata);
            assert_eq!(receipt.work.bytes_copied, copied);
            assert_eq!(receipt.work.items_examined, examined);
        } else {
            let failure =
                result.expect_err("metadata decoder bypassed its original copy admission");
            assert!(
                matches!(
                    failure.error,
                    FsError::Work(WorkError::BudgetExceeded {
                        counter: "bytes_copied",
                        ..
                    })
                ),
                "{failure:?}"
            );
            assert!(failure.work.as_deref().unwrap().bytes_copied <= maximum);
        }
    }
    commit(&mut checkout, &cancel).await?;
    let proof = crate::kernel::prove_generation_closure(
        volume.fs.objects(),
        checkout.generation_root,
        crate::facade::closure_limits(config()),
        B,
        &cancel,
    )
    .await?;
    Ok((copied, proof.work.bytes_copied))
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_detached_content_stream_keeps_atomic_metadata_transition() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut checkout) = fixture(config(), &cancel).await?;
    create(
        &mut checkout,
        "source",
        OwnedBytes::from_static(b"real detached stream"),
        &cancel,
    )
    .await?;
    create(
        &mut checkout,
        "destination",
        OwnedBytes::from_static(b"before"),
        &cancel,
    )
    .await?;
    commit(&mut checkout, &cancel).await?;
    let original = crate::NativeMaterializationWork::new(B, &cancel).map_err(|(cause, _)| cause)?;
    let reader = checkout
        .private_candidate(
            &mut crate::kernel::KernelAccounting::native(&original, &cancel),
            &cancel,
        )?
        .into_pinned_reader_with_native_work(&original, &cancel)?;
    let bytes = reader
        .read_file_range(
            &path("source")?,
            ByteRange {
                offset: 0,
                length: 20,
            },
            &original,
            &cancel,
        )
        .await?
        .bytes;
    let metadata = FileMetadata {
        modified_ns: MetadataField::Value(90),
        changed_ns: MetadataField::Value(90),
        ..FileMetadata::default()
    };
    let mut detached = checkout
        .detach_regular_file(&path("destination")?, B, &cancel)
        .await?
        .value;
    let id = detached.file_id();
    detached
        .change_content(
            ContentChange::Write { offset: 0, bytes },
            Some(metadata),
            &original,
            &cancel,
        )
        .await?;
    assert_eq!(detached.file_id(), id);
    assert_eq!(
        detached
            .read_range(
                ByteRange {
                    offset: 0,
                    length: 20
                },
                &original,
                &cancel
            )
            .await?
            .bytes
            .as_ref(),
        b"real detached stream"
    );
    let before = detached.read_metadata(B, &cancel).await?.value;
    let denied = crate::NativeMaterializationWork::new(
        WorkBudget {
            bytes_encoded: 0,
            ..B
        },
        &cancel,
    )
    .map_err(|(cause, _)| cause)?;
    let failure = detached
        .change_content(
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::new(),
            },
            Some(FileMetadata {
                modified_ns: MetadataField::Value(91),
                ..metadata
            }),
            &denied,
            &cancel,
        )
        .await
        .err()
        .ok_or("metadata encoding ignored original denial")?;
    assert!(matches!(
        failure,
        FsError::Work(_) | FsError::NativeKernel(_)
    ));
    assert_eq!(detached.read_metadata(B, &cancel).await?.value, before);
    assert_eq!(
        detached
            .read_range(
                ByteRange {
                    offset: 0,
                    length: 20
                },
                &original,
                &cancel
            )
            .await?
            .bytes
            .as_ref(),
        b"real detached stream"
    );
    assert_eq!(denied.receipt().unwrap().bytes_encoded, 0);
    assert_eq!(denied.receipt().unwrap().backend_write_operations, 0);
    Ok(())
}
