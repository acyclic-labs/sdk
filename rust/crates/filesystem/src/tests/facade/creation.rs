use super::*;

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
            let fs = Fs::memory();
            let caller = CancellationToken::new();
            let original =
                crate::NativeMaterializationWork::new(B, &caller).map_err(|(cause, _)| cause)?;
            original.charge(WorkCounters {
                items_examined: 7,
                ..WorkCounters::UNCHARGED
            })?;
            let initial_live = original.live_allocation_bytes().unwrap();
            let held = crate::kernel::allocation::retained::Retained::<Vec<u8>>::allocate(
                4096,
                &mut crate::kernel::KernelAccounting::native(&original, &cancel),
            )?;
            let restricted = original.native_volume_work(WorkBudget {
                bytes_hashed: if deny_hash { 0 } else { B.bytes_hashed },
                ..B
            })?;
            let baseline = original.live_allocation_bytes().unwrap();
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
                let spent = original.receipt().unwrap();
                assert_eq!(spent.bytes_hashed, 0);
                assert_eq!(spent.backend_write_operations, 0);
                assert_eq!(spent.authority_records_appended, 0);
            } else {
                let first = result?;
                let head = open_checkout(&first, pinned(), &cancel).await?;
                let published_head = head
                    .base_authority_head()
                    .ok_or("head checkout lost its authority observation")?;
                let published = original.receipt().unwrap();
                assert!(published.authority_records_appended > 0);
                assert!(published.backend_write_operations > 0);
                let retry = fs
                    .create_volume_with_id(id, config(), &restricted, &cancel)
                    .await?;
                let retried = original.receipt().unwrap();
                let retry_head = open_checkout(&retry, pinned(), &cancel).await?;
                assert_eq!(retry_head.generation_id(), head.generation_id());
                assert_eq!(retry_head.base_authority_head(), Some(published_head));
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
                // A committed idempotent Stream request accounts for its full
                // physical record set even though the authority head is unchanged.
                assert_eq!(
                    retried.authority_records_appended,
                    2 * published.authority_records_appended
                );
                assert_eq!(
                    fs.create_volume(config(), &restricted, &cancel)
                        .await?
                        .config(),
                    config()
                );
                assert_eq!(
                    original.receipt().unwrap().authority_records_appended,
                    retried.authority_records_appended + published.authority_records_appended
                );
            }
            assert!(original.receipt().unwrap().items_examined >= 7);
            let stored_live = original.live_allocation_bytes().unwrap();
            if deny_hash {
                assert_eq!(stored_live, baseline);
            } else {
                assert!(
                    stored_live > baseline,
                    "stored bytes lost their original funding"
                );
            }
            let spent = original.receipt().unwrap();
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
            assert_eq!(original.receipt().unwrap(), spent);
            assert_eq!(original.live_allocation_bytes().unwrap(), stored_live);
            drop(fs);
            assert_eq!(original.live_allocation_bytes().unwrap(), baseline);
            drop(restricted);
            drop(held);
            assert_eq!(original.live_allocation_bytes().unwrap(), initial_live);
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
    assert_eq!(*failure.work().unwrap(), WorkCounters::default());
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
            let initial_live = original.live_allocation_bytes().unwrap();
            let held = crate::kernel::allocation::retained::Retained::<Vec<u8>>::allocate(
                4096,
                &mut crate::kernel::KernelAccounting::native(&original, &cancel),
            )?;
            let restricted = original.native_volume_work(WorkBudget {
                backend_read_operations: admitted,
                ..B
            })?;
            let baseline = original.live_allocation_bytes().unwrap();
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
            let spent = original.receipt().unwrap();
            assert_eq!(spent.authority_records_read, u64::from(admitted != 0));
            assert!(spent.items_examined >= 7);
            assert_eq!(spent.authority_records_appended, 0);
            assert_eq!(spent.backend_read_operations, admitted);
            assert_eq!(spent.backend_write_operations, 0);
            assert_eq!(original.live_allocation_bytes().unwrap(), baseline);

            caller.cancel();
            assert!(matches!(
                fs.open_volume(id, &restricted, &cancel).await,
                Err(FsError::NativeKernel(
                    crate::NativeTreeMaterializationError::Cancelled
                ))
            ));
            assert_eq!(original.receipt().unwrap(), spent);
            assert_eq!(original.live_allocation_bytes().unwrap(), baseline);
            drop(restricted);
            drop(held);
            assert_eq!(original.live_allocation_bytes().unwrap(), initial_live);
        }
    }
    Ok(())
}
