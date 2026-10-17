use super::*;

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    // Recovery authenticates the publication operation, not the later head or
    // a placeholder version; the requested path must still match its receipt.
    #[test]
    fn publication_recovery_preserves_operation_identity(
        key in "[a-z0-9]{1,32}",
        body in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..1024),
    ) {
        let result = futures::executor::block_on(async {
            let storage = MemoryHarnessStorage::new(AgentId::new(), 1024).await?;
            let write = ContentGrant::verify(
                &storage.verifier(), &storage.scope, &storage.volume,
                VolumeOperation::Write, 1024, &mut None,
            )?;
            let key = IdempotencyKey::new(key)?;
            let original = storage.host.put_content(
                &storage.volume, &write, "original", &body, "application/octet-stream",
                "original", 1024, &key, None, None, None,
            ).await?;
            storage.host.put_content(
                &storage.volume, &write, "original", b"replacement", "text/plain",
                "replacement", 1024, &IdempotencyKey::new("later-head/edit")?,
                None, None, None,
            ).await?;
            assert_eq!(storage.host.content_for_operation(
                &storage.volume, &write, &key, "original", 1024, None, None,
            ).await?, Some(original));
            assert!(storage.host.content_for_operation(
                &storage.volume, &write, &key, "different", 1024, None, None,
            ).await.is_err());
            Ok::<_, Error>(())
        });
        proptest::prop_assert!(result.is_ok(), "{result:?}");
    }
}

#[tokio::test]
async fn stock_publisher_finite_once_observes_original_metadata_after_head_changes() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 1024).await?;
    let operation = OperationId::new();
    let cancellation = acyclic_fs::CancellationToken::new();
    let committed_io =
        crate::filesystem::PreparationIo::from_budget(FINITE_CONTENT_BUDGET, &cancellation)?;
    let (committed, _committed_hold) = storage
        .publisher
        .stage_once(
            operation,
            "once.json",
            b"original",
            "application/json",
            "original.json",
            None,
            Some(&committed_io),
        )?
        .await?;
    assert!(committed_io.receipt().backend_write_operations > 0);
    storage
        .publisher
        .stage_once(
            OperationId::new(),
            "once.json",
            b"later",
            "text/plain",
            "later.txt",
            None,
            None,
        )?
        .await?;
    let observed_io =
        crate::filesystem::PreparationIo::from_budget(FINITE_CONTENT_BUDGET, &cancellation)?;
    let (recovered, _recovered_hold) = storage
        .publisher
        .observe_staged_once(operation, 1024, None, Some(&observed_io))?
        .await?;
    assert_eq!(recovered.as_ref(), Some(&committed));
    let read_io =
        crate::filesystem::PreparationIo::from_budget(FINITE_CONTENT_BUDGET, &cancellation)?;
    assert_eq!(
        storage
            .content_verifier
            .read(&committed, 1024, None, Some(&read_io))?
            .await?
            .as_ref(),
        b"original"
    );
    assert!(
        storage
            .publisher
            .stage_once(
                operation,
                "once.json",
                b"original",
                "text/plain",
                "changed.txt",
                None,
                Some(&committed_io)
            )?
            .await
            .is_err()
    );
    let cancelled = acyclic_fs::CancellationToken::new();
    let cancelled_io =
        crate::filesystem::PreparationIo::from_budget(FINITE_CONTENT_BUDGET, &cancelled)?;
    cancelled.cancel();
    assert!(
        storage
            .publisher
            .stage_once(
                OperationId::new(),
                "cancelled",
                b"never",
                "text/plain",
                "never",
                None,
                Some(&cancelled_io)
            )
            .is_err()
    );
    assert_eq!(cancelled_io.receipt().backend_write_operations, 0);
    let denied_operation = OperationId::new();
    let mut denied = FINITE_CONTENT_BUDGET;
    denied.peak_allocation_bytes = 0;
    let denied_io = crate::filesystem::PreparationIo::from_budget(denied, &cancellation)?;
    assert!(
        storage
            .publisher
            .stage_once(
                denied_operation,
                "denied",
                b"never",
                "text/plain",
                "never",
                None,
                Some(&denied_io)
            )
            .is_err()
    );
    assert!(
        storage
            .publisher
            .observe_staged_once(denied_operation, 1024, None, Some(&observed_io))?
            .await?
            .0
            .is_none()
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn stock_native_content_keeps_original_ledger_and_output_custody() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 1024).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(FINITE_CONTENT_BUDGET, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    storage.publisher.supports_stage(Some(&io))?;
    assert_eq!(work.receipt().backend_write_operations, 0);
    struct OrdinaryOnlyPublisher {
        volume: VolumeRef,
    }
    impl ContentPublisher for OrdinaryOnlyPublisher {
        fn volume(&self) -> &VolumeRef {
            &self.volume
        }
    }
    let ordinary = OrdinaryOnlyPublisher {
        volume: storage.volume.clone(),
    };
    assert!(matches!(
        ordinary.supports_stage(Some(&io)),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            ..
        })
    ));
    assert!(matches!(
        ordinary.stage_once(
            OperationId::new(),
            "unsupported",
            b"never",
            "text/plain",
            "never",
            None,
            Some(&io)
        ),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            ..
        })
    ));
    let mut frame_budget = FINITE_CONTENT_BUDGET;
    frame_budget.allocation_operations = work.receipt().allocation_operations;
    let frame_work = acyclic_fs::NativeMaterializationWork::new(
        frame_budget,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let frame_io = crate::filesystem::PreparationIo::from_native_work(&frame_work)?;
    assert!(
        storage
            .publisher
            .stage_once(
                OperationId::new(),
                "frame-denied",
                b"never",
                "text/plain",
                "never",
                None,
                Some(&frame_io)
            )
            .is_err()
    );
    assert_eq!(frame_work.receipt().backend_write_operations, 0);
    let operation = OperationId::new();
    let (file, file_hold) = storage
        .publisher
        .stage_once(
            operation,
            "native.json",
            b"original",
            "application/json",
            "original.json",
            None,
            Some(&io),
        )?
        .await?;
    assert!(file_hold.as_ref().is_some_and(|hold| hold.bytes() > 0));
    assert!(work.receipt().backend_write_operations > 0);
    let before_read = work.live_allocation_bytes();
    let bytes = storage
        .content_verifier
        .read(&file, 1024, None, Some(&io))?
        .await?;
    assert_eq!(bytes.as_ref(), b"original");
    assert!(bytes.matches_meter(&work.runtime_meter()));
    assert!(work.live_allocation_bytes() > before_read);
    let with_output = work.live_allocation_bytes();
    drop(bytes);
    assert!(work.live_allocation_bytes() < with_output);
    assert!(
        storage
            .publisher
            .stage_once(
                operation,
                "native.json",
                b"original",
                "text/plain",
                "changed",
                None,
                Some(&io)
            )?
            .await
            .is_err()
    );
    drop(file);
    drop(file_hold);
    cancellation.cancel();
    assert!(
        storage
            .publisher
            .stage_once(
                OperationId::new(),
                "cancelled",
                b"never",
                "text/plain",
                "never",
                None,
                Some(&io)
            )
            .is_err()
    );
    let mut denied = FINITE_CONTENT_BUDGET;
    denied.backend_write_operations = 0;
    let denied_work =
        acyclic_fs::NativeMaterializationWork::new(denied, &acyclic_fs::CancellationToken::new())
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let denied_io = crate::filesystem::PreparationIo::from_native_work(&denied_work)?;
    assert!(
        storage
            .publisher
            .stage_once(
                OperationId::new(),
                "denied",
                b"never",
                "text/plain",
                "never",
                None,
                Some(&denied_io)
            )?
            .await
            .is_err()
    );
    assert_eq!(denied_work.receipt().backend_write_operations, 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn scheduler_native_stage_preserves_original_key_receipt_and_finite_hash_guard() -> Result<()>
{
    let storage = MemoryHarnessStorage::new(AgentId::new(), 1024).await?;
    let scheduler = crate::filesystem::FilesystemContentPublisher::new(
        storage.host.clone(),
        &(storage.volume),
        &storage.issuer.verifier(&mut None).expect("verifier copy"),
        &storage.scope,
        1024,
        None,
    )?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(FINITE_CONTENT_BUDGET, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let operation = OperationId::new();
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let file = crate::distributed::SchedulerPayloadStore::stage(
        scheduler.as_ref(),
        operation,
        "canonical-original-key",
        br#"{"input":1}"#,
        Some(&io),
    )?
    .await?;
    let hold = io.take_native_custody(file.logical_owned_bytes()?)?;
    assert_eq!(hold.bytes(), file.logical_owned_bytes()?);
    let retry = crate::distributed::SchedulerPayloadStore::stage(
        scheduler.as_ref(),
        operation,
        "canonical-original-key",
        br#"{"input":1}"#,
        Some(&io),
    )?
    .await?;
    let retry_hold = io.take_native_custody(retry.logical_owned_bytes()?)?;
    assert_eq!(retry, file);
    assert_eq!(
        crate::distributed::SchedulerPayloadStore::stage(
            scheduler.as_ref(),
            operation,
            "canonical-original-key",
            br#"{"input":1}"#,
            None
        )?
        .await?,
        file
    );
    assert!(
        crate::distributed::SchedulerPayloadStore::stage(
            scheduler.as_ref(),
            operation,
            "canonical-original-key",
            br#"{"input":2}"#,
            Some(&io)
        )?
        .await
        .is_err()
    );
    drop(retry);
    drop(retry_hold);
    drop(file);
    drop(hold);
    let mut denied = FINITE_CONTENT_BUDGET;
    denied.bytes_hashed = "canonical-original-key".len() as u64 - 1;
    let denied_work = acyclic_fs::NativeMaterializationWork::new(denied, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let denied_io = crate::filesystem::PreparationIo::from_native_work(&denied_work)?;
    let denied = crate::distributed::SchedulerPayloadStore::stage(
        scheduler.as_ref(),
        OperationId::new(),
        "canonical-original-key",
        b"never",
        Some(&denied_io),
    )?
    .await
    .err()
    .ok_or_else(|| Error::Invalid("staging ignored the original hash allowance".into()))?;
    let cause = match &denied {
        Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(cause),
            ..
        } => Some(cause),
        Error::NativeFilesystem { source, .. } => match source.as_ref() {
            acyclic_fs::FsError::Work(cause)
            | acyclic_fs::FsError::NativeKernel(
                acyclic_fs::NativeTreeMaterializationError::Work(cause),
            ) => Some(cause),
            _ => None,
        },
        _ => None,
    };
    assert!(
        matches!(
            cause,
            Some(acyclic_fs::WorkError::BudgetExceeded {
                counter: "bytes_hashed",
                ..
            })
        ),
        "unexpected hash rejection: {denied:?}"
    );
    assert_eq!(denied_work.receipt().bytes_hashed, 0);
    assert_eq!(denied_work.receipt().backend_write_operations, 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_host_original_key_recovers_immutable_upload_after_head_edit() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 1024).await?;
    let write = ContentGrant::verify(
        &storage.verifier(),
        &storage.scope,
        &storage.volume,
        VolumeOperation::Write,
        1024,
        &mut None,
    )?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(FINITE_CONTENT_BUDGET, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let key = IdempotencyKey::new("acyclic.pi/input/root-task/original-operation/label")?;
    let mut path_budget = FINITE_CONTENT_BUDGET;
    path_budget.allocation_operations = work.receipt().allocation_operations;
    let path_work = acyclic_fs::NativeMaterializationWork::new(path_budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let path_io = crate::filesystem::PreparationIo::from_native_work(&path_work)?;
    let path_prefix = path_work.receipt();
    assert!(matches!(
        storage
            .host
            .put_content(
                &storage.volume,
                &write,
                "../outside",
                b"never",
                "text/plain",
                "never",
                1024,
                &key,
                None,
                None,
                Some(&path_io)
            )
            .await,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            ..
        })
    ));
    assert!(matches!(
        storage
            .host
            .content_for_operation(
                &storage.volume,
                &write,
                &key,
                ".system/private",
                1024,
                None,
                Some(&path_io)
            )
            .await,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            ..
        })
    ));
    let rejected = path_work.receipt();
    assert_eq!(
        rejected,
        acyclic_fs::WorkCounters {
            items_examined: rejected.items_examined,
            ..path_prefix
        }
    );
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let staged = storage
        .host
        .put_content(
            &storage.volume,
            &write,
            "inputs/original.json",
            b"original",
            "application/json",
            "original.json",
            1024,
            &key,
            None,
            None,
            Some(&io),
        )
        .await?;
    let staged_hold = io.take_native_custody(staged.logical_owned_bytes()?)?;
    storage
        .host
        .put_content(
            &storage.volume,
            &write,
            "inputs/original.json",
            b"later",
            "text/plain",
            "later.txt",
            1024,
            &IdempotencyKey::new("head-edit")?,
            None,
            None,
            None,
        )
        .await?;
    let recovered = storage
        .host
        .content_for_operation(
            &storage.volume,
            &write,
            &key,
            "inputs/original.json",
            1024,
            None,
            Some(&io),
        )
        .await?
        .ok_or_else(|| Error::NotFound("original native operation receipt".into()))?;
    let recovered_hold = io.take_native_custody(recovered.logical_owned_bytes()?)?;
    assert_eq!(recovered, staged);
    let content = storage
        .content_verifier
        .read(&recovered, 1024, None, Some(&io))?
        .await?;
    assert_eq!(content.as_ref(), b"original");
    drop(content);
    let writes = work.receipt().backend_write_operations;
    assert!(
        storage
            .host
            .put_content(
                &storage.volume,
                &write,
                "inputs/original.json",
                b"original",
                "text/plain",
                "changed.txt",
                1024,
                &key,
                None,
                None,
                Some(&io)
            )
            .await
            .is_err()
    );
    assert_eq!(work.receipt().backend_write_operations, writes);
    assert!(
        storage
            .host
            .content_for_operation(
                &storage.volume,
                &write,
                &key,
                "inputs/changed.json",
                1024,
                None,
                Some(&io)
            )
            .await
            .is_err()
    );
    assert!(
        storage
            .host
            .content_for_operation(
                &storage.volume,
                &write,
                &IdempotencyKey::new("proven-absent-operation")?,
                "inputs/original.json",
                1024,
                None,
                Some(&io)
            )
            .await?
            .is_none()
    );
    drop(recovered);
    drop(recovered_hold);
    drop(staged);
    drop(staged_hold);
    drop(io);
    let cancelled = acyclic_fs::CancellationToken::new();
    let cancelled_work =
        acyclic_fs::NativeMaterializationWork::new(FINITE_CONTENT_BUDGET, &cancelled)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::filesystem::PreparationIo::from_native_work(&cancelled_work)?;
    let prefix = cancelled_work.receipt();
    cancelled.cancel();
    assert!(matches!(
        storage
            .host
            .content_for_operation(
                &storage.volume,
                &write,
                &key,
                "inputs/original.json",
                1024,
                None,
                Some(&io)
            )
            .await,
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    assert_eq!(cancelled_work.receipt(), prefix);
    drop(io);
    let with_storage = work.live_allocation_bytes();
    // Checkout registrations remain on the original ledger until that ledger drops.
    drop(storage);
    assert!(work.live_allocation_bytes() < with_storage);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn exhausted_native_custody_transfer_keeps_existing_guard_until_value_drops() -> Result<()> {
    let work = acyclic_fs::NativeMaterializationWork::new(
        FINITE_CONTENT_BUDGET,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let bytes = io.encode(&serde_json::Value::Null, 1024, true)?;
    let held = io.owned_bytes()?;
    let live_held = work.live_allocation_bytes() - baseline;
    let Err(error) = io.into_native_custody(held + work.remaining()?.peak_allocation_bytes + 1)
    else {
        panic!("custody beyond the original remaining quota must fail");
    };
    let failure = (bytes, error);
    assert!(matches!(
        &failure.1,
        Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(_),
            custody: Some(_),
        }
    ));
    assert_eq!(work.live_allocation_bytes(), baseline + live_held);
    let empty_alias = failure.0.slice(0..0);
    drop(failure);
    assert!(work.live_allocation_bytes() > baseline);
    drop(empty_alias);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn rejected_foreign_native_custody_preserves_original_owned_data_guard() -> Result<()> {
    let token = acyclic_fs::CancellationToken::new();
    let receiving = acyclic_fs::NativeMaterializationWork::new(FINITE_CONTENT_BUDGET, &token)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let producing = acyclic_fs::NativeMaterializationWork::new(FINITE_CONTENT_BUDGET, &token)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let receiver_io = crate::filesystem::PreparationIo::from_native_work(&receiving)?;
    let receiver_baseline = receiving.live_allocation_bytes();
    let producer_baseline = producing.live_allocation_bytes();
    let producer_io = crate::filesystem::PreparationIo::from_native_work(&producing)?;
    let bytes = producer_io.encode(&serde_json::Value::Null, 1024, true)?;
    drop(producer_io);
    let (metadata, incoming) = producing
        .display("source carrier metadata")
        .map_err(Error::from)?;
    let held = producing.live_allocation_bytes() - producer_baseline;
    let Err(error) = receiver_io.absorb_native_custody(incoming) else {
        panic!("distinct original allocation ledgers must not merge");
    };
    let failure = (bytes, metadata, error);
    assert!(matches!(
        &failure.2,
        Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            custody: Some(_),
        }
    ));
    assert_eq!(receiving.live_allocation_bytes(), receiver_baseline);
    assert_eq!(producing.live_allocation_bytes(), producer_baseline + held);
    drop(failure);
    assert_eq!(producing.live_allocation_bytes(), producer_baseline);
    Ok(())
}
