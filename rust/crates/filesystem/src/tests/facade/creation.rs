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
    let head = open_checkout(&first, pinned(), &cancel).await?;
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
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::initialize_test_native_io();
        for deny_hash in [true, false] {
            let caller = CancellationToken::new();
            let original =
                crate::NativeMaterializationWork::new(B, &caller).map_err(|(cause, _)| cause)?;
            original.charge(WorkCounters {
                items_examined: 7,
                ..WorkCounters::UNCHARGED
            })?;
            let initial_live = original.live_allocation_bytes();
            let held = crate::kernel::allocation::retained::Retained::<Vec<u8>>::allocate(
                4096,
                &mut crate::kernel::KernelAccounting::native(&original, &cancel),
            )?;
            let restricted = original.native_volume_work(WorkBudget {
                bytes_hashed: if deny_hash { 0 } else { B.bytes_hashed },
                ..B
            })?;
            let baseline = original.live_allocation_bytes();
            let id = VolumeId::from_bytes([45; 16]);
            let result = fs
                .create_volume_with_id(id, config(), &restricted, &cancel)
                .await;
            if deny_hash {
                assert!(matches!(
                    result,
                    Err(FsError::Object(crate::ObjectStoreError::Native(
                        crate::NativeTreeMaterializationError::Work(
                            crate::WorkError::BudgetExceeded {
                                counter: "bytes_hashed",
                                ..
                            }
                        )
                    )))
                ));
                let spent = original.receipt();
                assert_eq!(spent.bytes_hashed, 0);
                assert_eq!(spent.backend_write_operations, 0);
                assert_eq!(spent.authority_records_appended, 0);
            } else {
                let first = result?;
                let head = open_checkout(&first, pinned(), &cancel).await?;
                let published = original.receipt();
                assert_eq!(published.authority_records_appended, 1);
                assert!(published.backend_write_operations > 0);
                let retry = fs
                    .create_volume_with_id(id, config(), &restricted, &cancel)
                    .await?;
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
                assert_eq!(original.receipt().authority_records_appended, 1);
                assert_eq!(
                    fs.create_volume(config(), &restricted, &cancel)
                        .await?
                        .config(),
                    config()
                );
                assert_eq!(original.receipt().authority_records_appended, 2);
            }
            assert!(original.receipt().items_examined >= 7);
            assert_eq!(original.live_allocation_bytes(), baseline);
            let spent = original.receipt();
            caller.cancel();
            assert!(matches!(
                fs.create_volume_with_id(id, config(), &restricted, &cancel)
                    .await,
                Err(FsError::NativeKernel(
                    crate::NativeTreeMaterializationError::Cancelled
                ))
            ));
            assert!(matches!(
                fs.create_volume(config(), &restricted, &cancel).await,
                Err(FsError::NativeKernel(
                    crate::NativeTreeMaterializationError::Cancelled
                ))
            ));
            assert_eq!(original.receipt(), spent);
            assert_eq!(original.live_allocation_bytes(), baseline);
            drop(restricted);
            drop(held);
            assert_eq!(original.live_allocation_bytes(), initial_live);
        }
    }
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
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::initialize_test_native_io();
        for admitted in [0, 2] {
            let caller = CancellationToken::new();
            let original =
                crate::NativeMaterializationWork::new(B, &caller).map_err(|(cause, _)| cause)?;
            original.charge(WorkCounters {
                items_examined: 7,
                ..WorkCounters::UNCHARGED
            })?;
            let initial_live = original.live_allocation_bytes();
            let held = crate::kernel::allocation::retained::Retained::<Vec<u8>>::allocate(
                4096,
                &mut crate::kernel::KernelAccounting::native(&original, &cancel),
            )?;
            let restricted = original.native_volume_work(WorkBudget {
                backend_read_operations: admitted,
                ..B
            })?;
            let baseline = original.live_allocation_bytes();
            let result = fs.open_volume(id, &restricted, &cancel).await;
            if admitted == 0 {
                assert!(
                    matches!(
                        result,
                        Err(FsError::Authority(
                            crate::AuthorityStoreError::NativeOriginal(
                                crate::NativeTreeMaterializationError::Work(
                                    crate::WorkError::BudgetExceeded {
                                        counter: "backend_read_operations",
                                        ..
                                    }
                                )
                            )
                        ))
                    ),
                    "opening lost original read admission or its typed refusal"
                );
            } else {
                assert_eq!(result?.config(), config());
            }
            let spent = original.receipt();
            assert_eq!(spent.authority_records_read, u64::from(admitted != 0));
            assert!(spent.items_examined >= 7);
            assert_eq!(spent.authority_records_appended, 0);
            assert_eq!(spent.backend_read_operations, admitted);
            assert_eq!(spent.backend_write_operations, 0);
            assert_eq!(original.live_allocation_bytes(), baseline);

            caller.cancel();
            assert!(matches!(
                fs.open_volume(id, &restricted, &cancel).await,
                Err(FsError::NativeKernel(
                    crate::NativeTreeMaterializationError::Cancelled
                ))
            ));
            assert_eq!(original.receipt(), spent);
            assert_eq!(original.live_allocation_bytes(), baseline);
            drop(restricted);
            drop(held);
            assert_eq!(original.live_allocation_bytes(), initial_live);
        }
    }
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
    let mut checkout = open_checkout(&volume, writable_tracking(), &active).await?;
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
    cancelled(checkout.read_file_range(id, range, B, &cancel).await);
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
