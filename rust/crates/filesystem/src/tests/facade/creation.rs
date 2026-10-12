use super::*;

#[tokio::test]
async fn create_retry_is_idempotent_and_exact_selector_matches_head()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([42; 16]);
    (fs.create_volume_with_id(volume_id, config(), WorkBudget::UNBOUNDED, &cancellation)).await?;
    let retry =
        (fs.create_volume_with_id(volume_id, config(), WorkBudget::UNBOUNDED, &cancellation))
            .await?;
    let opened = fs
        .open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    assert_eq!(opened.config(), config());
    let mut head = (opened.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let exact = (retry.value.checkout(
        GenerationSelector::Exact(head.value.generation_id()),
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(exact.value.generation_id(), head.value.generation_id());
    let authority = volume_authority_id(volume_id);
    let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let header =
        AsyncAuthorityStore::head(&fs.inner.authority, authority, &mut authority_work).await?;
    AsyncAuthorityStore::replay(
        &fs.inner.authority,
        authority,
        Sequence::new(header.sequence.get() - 1),
        ReplayLimit {
            records: 1,
            payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES,
        },
        &mut authority_work,
    )
    .await?;
    let without_record_decode = exact.work.checked_add(authority_work.spent())?;
    let denied = opened
        .checkout(
            GenerationSelector::Head,
            pinned(),
            WorkBudget {
                items_examined: without_record_decode.items_examined,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .await
        .err()
        .ok_or("head record decoding escaped the checkout item budget")?;
    assert!(matches!(
        denied.error,
        FsError::Work(WorkError::BudgetExceeded {
            counter: "items_examined",
            ..
        })
    ));
    assert_eq!(head.value.volume_id(), volume_id);
    assert!(head.value.root().parents.is_empty());
    assert!(head.work.object_bytes_read <= MAXIMUM_GENERATION_ROOT_BYTES);
    let roots = head
        .value
        .lookup_batch_no_follow(
            &[root_path()?, root_path()?],
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    assert_eq!(roots.entries.len(), 2);
    assert_eq!(roots.entries[0], roots.entries[1]);
    assert_eq!(
        roots.entries[0].record.map(|record| record.kind),
        Some(FileKind::Directory)
    );
    Ok(())
}

#[tokio::test]
async fn unsupported_semantics_reject_before_backend_work() -> Result<(), Box<dyn std::error::Error>>
{
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    let result = (fs.create_volume_with_id(
        VolumeId::from_bytes([43; 16]),
        durable,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await;
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
    (fs.create_volume_with_id(volume_id, config(), WorkBudget::UNBOUNDED, &cancellation)).await?;
    let mut conflicting = config();
    conflicting.sparse_files = false;
    let retry =
        (fs.create_volume_with_id(volume_id, conflicting, WorkBudget::UNBOUNDED, &cancellation))
            .await;
    let Err(failure) = retry else {
        return Err("conflicting creation succeeded".into());
    };
    assert!(matches!(failure.error, FsError::CreationRejected));
    let opened = (fs.open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(opened.value.config(), config());
    Ok(())
}

#[tokio::test]
async fn pre_cancelled_facade_operations_perform_zero_work()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let result = (fs.create_volume_with_id(
        VolumeId::from_bytes([47; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
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
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([137; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &active,
    ))
    .await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &active,
    ))
    .await?
    .value;
    let file = path("cancelled-file")?;
    let file_id = (checkout.create(
        crate::AuthoredMutation::CreateFile {
            path: file.clone(),
            bytes: (bytes::Bytes::from_static(b"abcdefgh")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &active,
    ))
    .await?
    .value;
    let mut detached = (checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &active))
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
        volume.checkout(
            GenerationSelector::Head,
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled checkout"
    );
    cancelled!(
        checkout.lookup_no_follow(&file, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled lookup"
    );
    cancelled!(
        checkout.lookup_batch_no_follow(
            std::slice::from_ref(&file),
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled batch lookup"
    );
    cancelled!(
        checkout.list_directory(
            &root_path()?,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled directory listing"
    );
    cancelled!(
        checkout.read_metadata(&file, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled metadata"
    );
    cancelled!(
        checkout.read_file_range(&file, range, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled path read"
    );
    cancelled!(
        checkout.plan_file_extents(&file, range, 8, WorkBudget::UNBOUNDED, &cancelled_token,),
        "cancelled path plan"
    );
    cancelled!(
        checkout.seek_file_extent(
            &file,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled path seek"
    );
    cancelled!(
        checkout.read_file_record_by_id(file_id, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled identity record"
    );
    cancelled!(
        checkout.read_file_range_by_id(file_id, range, WorkBudget::UNBOUNDED, &cancelled_token,),
        "cancelled identity read"
    );
    cancelled!(
        checkout.read_named_attribute(&file, &attribute, WorkBudget::UNBOUNDED, &cancelled_token,),
        "cancelled path attribute"
    );
    cancelled!(
        checkout.change_content(
            file.clone(),
            crate::ContentChange::Write {
                offset: 0,
                bytes: (bytes::Bytes::from_static(b"x")).into()
            },
            crate::ContentTimes::Preserve,
            WorkBudget::UNBOUNDED,
            &cancelled_token
        ),
        "cancelled path write"
    );
    cancelled!(
        checkout.create(
            crate::AuthoredMutation::CreateFile {
                path: path("cancelled-new-file")?,
                bytes: (bytes::Bytes::from_static(b"must-not-be-encoded")).into(),
                metadata: crate::kernel::FileMetadata::default()
            },
            WorkBudget::UNBOUNDED,
            &cancelled_token
        ),
        "cancelled file creation"
    );
    let mut empty = std::io::Cursor::new(OwnedBytes::new());
    cancelled!(
        checkout.stage_content(&mut empty, 1, WorkBudget::UNBOUNDED, &cancelled_token,),
        "cancelled content staging"
    );
    cancelled!(
        checkout.commit(
            OperationId::from_bytes([138; 16]),
            None,
            None,
            None,
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancelled_token,
            |_| async {
                crate::FsResult::Ok(crate::FsReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                })
            }
        ),
        "cancelled commit"
    );
    cancelled!(
        detached.read_range(range, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled detached read"
    );
    cancelled!(
        detached.change_content(
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::from_static(b"x")
            },
            None,
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled detached write"
    );
    Ok(())
}
