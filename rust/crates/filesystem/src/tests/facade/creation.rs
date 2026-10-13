use super::*;

fn cancelled<T>(result: Result<OperationReceipt<T>, OperationFailure<FsError>>) {
    let failure = result.err().expect("cancelled operation succeeded");
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&failure.error);
    let mut cancellation = matches!(
        failure.error,
        FsError::Cancelled(_) | FsError::FileRead(FileRangeReadError::Cancelled)
    );
    while let Some(error) = source {
        cancellation |= error.is::<CancellationError>();
        source = error.source();
    }
    assert!(cancellation, "unexpected error: {:?}", failure.error);
    assert_eq!(*failure.work, WorkCounters::default());
}

#[tokio::test]
async fn create_retry_is_idempotent_and_exact_selector_matches_head() -> TestResult {
    let fs = Fs::memory();
    let cancel = CancellationToken::new();
    let id = VolumeId::from_bytes([42; 16]);
    let first = fs
        .create_volume_with_id(id, config(), B, &cancel)
        .await?
        .value;
    let retry = fs
        .create_volume_with_id(id, config(), B, &cancel)
        .await?
        .value;
    let head = first
        .checkout(GenerationSelector::Head, pinned(), B, &cancel)
        .await?
        .value;
    let exact = retry
        .checkout(
            GenerationSelector::Exact(head.generation_id()),
            pinned(),
            B,
            &cancel,
        )
        .await?
        .value;
    assert_eq!(exact.generation_id(), head.generation_id());
    Ok(())
}

#[tokio::test]
async fn unsupported_semantics_reject_before_backend_work() -> TestResult {
    let cancel = CancellationToken::new();
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    let failure = Fs::memory()
        .create_volume_with_id(VolumeId::from_bytes([43; 16]), durable, B, &cancel)
        .await
        .err()
        .ok_or("durable memory volume succeeded")?;
    assert!(matches!(failure.error, FsError::UnsupportedDurability));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn conflicting_creation_config_is_rejected_without_changing_opened_config() -> TestResult {
    let fs = Fs::memory();
    let cancel = CancellationToken::new();
    let id = VolumeId::from_bytes([44; 16]);
    fs.create_volume_with_id(id, config(), B, &cancel).await?;
    let mut conflicting = config();
    conflicting.sparse_files = false;
    assert!(matches!(
        fs.create_volume_with_id(id, conflicting, B, &cancel).await,
        Err(OperationFailure {
            error: FsError::CreationRejected,
            ..
        })
    ));
    assert_eq!(
        fs.open_volume(id, B, &cancel).await?.value.config(),
        config()
    );
    Ok(())
}

#[tokio::test]
async fn pre_cancelled_facade_operations_perform_zero_work() -> TestResult {
    let cancel = CancellationToken::new();
    cancel.cancel();
    cancelled(
        Fs::memory()
            .create_volume_with_id(VolumeId::from_bytes([47; 16]), config(), B, &cancel)
            .await,
    );
    Ok(())
}

#[tokio::test]
async fn pre_cancelled_existing_volume_surfaces_fail_before_visible_work() -> TestResult {
    let active = CancellationToken::new();
    let (volume, _) = fixture(config(), &active).await?;
    let mut checkout = volume
        .checkout(GenerationSelector::Head, writable_tracking(), B, &active)
        .await?
        .value;
    let file = path("cancelled-file")?;
    let id = create(
        &mut checkout,
        "cancelled-file",
        OwnedBytes::from_static(b"abcdefgh"),
        &active,
    )
    .await?;
    let mut detached = checkout.detach_regular_file(&file, B, &active).await?.value;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let range = ByteRange {
        offset: 0,
        length: 1,
    };
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.cancelled".to_vec(),
        128,
    )?;
    cancelled(
        volume
            .checkout(GenerationSelector::Head, pinned(), B, &cancel)
            .await,
    );
    cancelled(checkout.lookup_no_follow(&file, B, &cancel).await);
    cancelled(
        checkout
            .lookup_batch_no_follow(std::slice::from_ref(&file), B, &cancel)
            .await,
    );
    cancelled(
        checkout
            .list_directory(&root_path()?, None, 8, B, &cancel)
            .await,
    );
    cancelled(checkout.read_metadata(&file, B, &cancel).await);
    cancelled(checkout.read_file_range(&file, range, B, &cancel).await);
    cancelled(
        checkout
            .plan_file_extents(&file, range, 8, B, &cancel)
            .await,
    );
    cancelled(
        checkout
            .seek_file_extent(&file, 0, ExtentSeekTarget::Data, B, &cancel)
            .await,
    );
    cancelled(checkout.read_file_record_by_id(id, B, &cancel).await);
    cancelled(checkout.read_file_range_by_id(id, range, B, &cancel).await);
    cancelled(
        checkout
            .read_named_attribute(&file, &attribute, B, &cancel)
            .await,
    );
    cancelled(
        checkout
            .change_content(
                file,
                ContentChange::Write {
                    offset: 0,
                    bytes: OwnedBytes::from_static(b"x"),
                },
                ContentTimes::Preserve,
                B,
                &cancel,
            )
            .await,
    );
    cancelled(
        checkout
            .create(
                AuthoredMutation::CreateFile {
                    path: path("cancelled-new-file")?,
                    bytes: OwnedBytes::from_static(b"must-not-be-encoded"),
                    metadata: FileMetadata::default(),
                },
                B,
                &cancel,
            )
            .await,
    );
    let mut empty = std::io::Cursor::new(OwnedBytes::new());
    cancelled(checkout.stage_content(&mut empty, 1, B, &cancel).await);
    cancelled(
        checkout
            .commit(
                OperationId::from_bytes([138; 16]),
                None,
                None,
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<FsError>,
            )
            .await,
    );
    cancelled(detached.read_range(range, B, &cancel).await);
    cancelled(
        detached
            .change_content(
                ContentChange::Write {
                    offset: 0,
                    bytes: OwnedBytes::from_static(b"x"),
                },
                None,
                B,
                &cancel,
            )
            .await,
    );
    Ok(())
}
