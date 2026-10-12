use super::*;

#[tokio::test]
async fn create_retry_is_idempotent_and_exact_selector_matches_head()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([42; 16]);
    let first = (fs.create_volume_with_id(volume_id, config(), B, &cancellation)).await?;
    let retry = (fs.create_volume_with_id(volume_id, config(), B, &cancellation)).await?;
    let head = (first
        .value
        .checkout(GenerationSelector::Head, pinned(), B, &cancellation))
    .await?;
    let exact = (retry.value.checkout(
        GenerationSelector::Exact(head.value.generation_id()),
        pinned(),
        B,
        &cancellation,
    ))
    .await?;
    assert_eq!(exact.value.generation_id(), head.value.generation_id());
    Ok(())
}

#[tokio::test]
async fn unsupported_semantics_reject_before_backend_work() -> Result<(), Box<dyn std::error::Error>>
{
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    let result =
        (fs.create_volume_with_id(VolumeId::from_bytes([43; 16]), durable, B, &cancellation)).await;
    let Err(failure) = result else {
        return Err("durable memory volume succeeded".into());
    };
    assert!(matches!(failure.error, FsError::UnsupportedDurability));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn conflicting_creation_config_is_rejected_without_changing_opened_config()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([44; 16]);
    (fs.create_volume_with_id(volume_id, config(), B, &cancellation)).await?;
    let mut conflicting = config();
    conflicting.sparse_files = false;
    let retry = (fs.create_volume_with_id(volume_id, conflicting, B, &cancellation)).await;
    let Err(failure) = retry else {
        return Err("conflicting creation succeeded".into());
    };
    assert!(matches!(failure.error, FsError::CreationRejected));
    let opened = (fs.open_volume(volume_id, B, &cancellation)).await?;
    assert_eq!(opened.value.config(), config());
    Ok(())
}

#[tokio::test]
async fn pre_cancelled_facade_operations_perform_zero_work()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let result =
        (fs.create_volume_with_id(VolumeId::from_bytes([47; 16]), config(), B, &cancellation))
            .await;
    let Err(failure) = result else {
        return Err("cancelled create succeeded".into());
    };
    assert!(matches!(failure.error, FsError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn pre_cancelled_existing_volume_surfaces_fail_before_visible_work()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! cancelled {
        ($future:expr, $label:literal) => {{
            let result = ($future).await;
            let failure = result
                .err()
                .ok_or(concat!($label, " unexpectedly succeeded"))?;
            assert!(
                format!("{:?}", failure.error).contains("Cancelled"),
                "{}: {:?}",
                $label,
                failure.error
            );
            assert_eq!(*failure.work, WorkCounters::default(), "{}", $label);
        }};
    }

    let fs = Fs::memory();
    let active = CancellationToken::new();
    let volume = (fs.create_volume_with_id(VolumeId::from_bytes([137; 16]), config(), B, &active))
        .await?
        .value;
    let mut checkout = (volume.checkout(GenerationSelector::Head, writable_tracking(), B, &active))
        .await?
        .value;
    let file = path("cancelled-file")?;
    let file_id = (checkout.create(
        AuthoredMutation::CreateFile {
            path: file.clone(),
            bytes: OwnedBytes::from_static(b"abcdefgh"),
            metadata: FileMetadata::default(),
        },
        B,
        &active,
    ))
    .await?
    .value;
    let mut detached = (checkout.detach_regular_file(&file, B, &active))
        .await?
        .value;
    let cancelled_token = CancellationToken::new();
    cancelled_token.cancel();
    let range = ByteRange {
        offset: 0,
        length: 1,
    };
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.cancelled".to_vec(),
        128,
    )?;

    cancelled!(
        volume.checkout(GenerationSelector::Head, pinned(), B, &cancelled_token,),
        "cancelled checkout"
    );
    cancelled!(
        checkout.lookup_no_follow(&file, B, &cancelled_token),
        "cancelled lookup"
    );
    cancelled!(
        checkout.lookup_batch_no_follow(std::slice::from_ref(&file), B, &cancelled_token,),
        "cancelled batch lookup"
    );
    cancelled!(
        checkout.list_directory(
            &root_path()?,
            std::ops::Bound::Unbounded,
            8,
            B,
            &cancelled_token
        ),
        "cancelled directory listing"
    );
    cancelled!(
        checkout.read_metadata(&file, B, &cancelled_token),
        "cancelled metadata"
    );
    cancelled!(
        checkout.read_file_range(&file, range, B, &cancelled_token),
        "cancelled path read"
    );
    cancelled!(
        checkout.plan_file_extents(&file, range, 8, B, &cancelled_token,),
        "cancelled path plan"
    );
    cancelled!(
        checkout.seek_file_extent(&file, 0, ExtentSeekTarget::Data, B, &cancelled_token,),
        "cancelled path seek"
    );
    cancelled!(
        checkout.read_file_record_by_id(file_id, B, &cancelled_token),
        "cancelled identity record"
    );
    cancelled!(
        checkout.read_file_range_by_id(file_id, range, B, &cancelled_token,),
        "cancelled identity read"
    );
    cancelled!(
        checkout.read_named_attribute(&file, &attribute, B, &cancelled_token,),
        "cancelled path attribute"
    );
    cancelled!(
        checkout.change_content(
            file.clone(),
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::from_static(b"x")
            },
            ContentTimes::Preserve,
            B,
            &cancelled_token
        ),
        "cancelled path write"
    );
    cancelled!(
        checkout.create(
            AuthoredMutation::CreateFile {
                path: path("cancelled-new-file")?,
                bytes: OwnedBytes::from_static(b"must-not-be-encoded"),
                metadata: FileMetadata::default()
            },
            B,
            &cancelled_token
        ),
        "cancelled file creation"
    );
    let mut empty = std::io::Cursor::new(OwnedBytes::new());
    cancelled!(
        checkout.stage_content(&mut empty, 1, B, &cancelled_token,),
        "cancelled content staging"
    );
    cancelled!(
        checkout.commit(
            OperationId::from_bytes([138; 16]),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancelled_token,
            allow::<FsError>
        ),
        "cancelled commit"
    );
    cancelled!(
        detached.read_range(range, B, &cancelled_token),
        "cancelled detached read"
    );
    cancelled!(
        detached.change_content(
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::from_static(b"x")
            },
            None,
            B,
            &cancelled_token,
        ),
        "cancelled detached write"
    );
    Ok(())
}
