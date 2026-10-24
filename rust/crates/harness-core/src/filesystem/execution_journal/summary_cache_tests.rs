use super::*;

fn seed(cache: &mut SummaryCache, operation: OperationId, io: &PreparationIo) -> Result<()> {
    cache.bind(operation, Some(io))?;
    let mut edit = cache.edit();
    let (summary, retained_io) = edit.parts();
    summary.accept_verified(
        (1, [1; 32], [2; 32]),
        &ExecutionEvent::Started {
            request_digest: [3; 32],
        },
        None,
        Limits::default(),
        retained_io,
    )?;
    edit.finish();
    Ok(())
}

#[test]
fn cache_storage_keeps_its_original_ledger_and_releases_only_its_own_pool() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let io = PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(1_000_000, 1_000_000),
        &cancellation,
    )?;
    let observer = io.scoped()?;
    let mut cache = SummaryCache::default();
    seed(&mut cache, OperationId::new(), &io)?;
    assert_eq!(io.owned_bytes()?, 0);
    assert!(ledger_live(&io)? > 0);
    observer.retain_owned(177)?;
    drop(io);
    let retained = cache
        .io
        .as_ref()
        .ok_or_else(|| Error::Invalid("cache lost its ledger".into()))?;
    assert!(retained.shares_ledger(&observer));
    assert!(retained.owned_bytes()? > 0);
    assert_eq!(observer.owned_bytes()?, 177);
    let original_live = ledger_live(&observer)?;
    assert!(original_live > 177);
    drop(cache);
    let original_live = ledger_live(&observer)?;
    assert_eq!(original_live, 177);
    drop(observer);
    Ok(())
}

fn ledger_live(io: &PreparationIo) -> Result<u64> {
    match io {
        PreparationIo::Owned { work, .. } => {
            let state = work
                .lock()
                .map_err(|_| Error::Invalid("poisoned test ledger".into()))?;
            state
                .1
                .as_ref()
                .map(|ledger| ledger.live_bytes())
                .map_err(|cause| acyclic_fs::FsError::from(*cause).into())
        }
        #[cfg(not(target_arch = "wasm32"))]
        _ => Err(Error::Invalid("expected finite test ledger".into())),
    }
}

#[test]
fn incomplete_refresh_and_ledger_changes_discard_the_authenticated_prefix() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
    let first = PreparationIo::from_budget(budget, &cancellation)?;
    let second = PreparationIo::from_budget(budget, &cancellation)?;
    let operation = OperationId::new();
    let mut cache = SummaryCache::default();
    seed(&mut cache, operation, &first)?;
    cache.bind(operation, Some(&first))?;
    assert_eq!(cache.summary().tail, 1);
    cache.bind(operation, Some(&second))?;
    assert_eq!(cache.summary().tail, 0);
    assert_eq!(first.owned_bytes()?, 0);
    seed(&mut cache, operation, &second)?;
    drop(cache.edit());
    assert!(cache.summary().operation.is_none());
    assert_eq!(second.owned_bytes()?, 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_cache_custody_survives_temporary_io_and_returns_to_the_original_ledger() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 1_000_000),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    let mut cache = SummaryCache::default();
    let operation = OperationId::new();
    {
        let io = PreparationIo::from_native_work(&work)?;
        seed(&mut cache, operation, &io)?;
    }
    assert!(work.live_allocation_bytes() > baseline);
    let io = PreparationIo::from_native_work(&work)?;
    cache.bind(operation, Some(&io))?;
    assert_eq!(cache.summary().tail, 1);
    assert_eq!(io.owned_bytes()?, 0);
    drop(cache);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn a_fresh_invocation_reuses_storage_with_current_cancellation_and_work() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 1_000_000),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let (first, first_cancel) = work.invocation_view()?;
    let first = first.invocation_window()?;
    let first_io = PreparationIo::from_native_work(&first)?;
    let mut cache = SummaryCache::default();
    let operation = OperationId::new();
    seed(&mut cache, operation, &first_io)?;
    let retained_bytes = cache
        .io
        .as_ref()
        .ok_or_else(|| Error::Invalid("cache lost its ledger".into()))?
        .owned_bytes()?;
    first_cancel.cancel();
    assert!(
        cache
            .io
            .as_ref()
            .ok_or_else(|| Error::Invalid("cache lost its ledger".into()))?
            .check_cancellation()
            .is_err(),
        "the old invocation must fail the current-context oracle"
    );
    let (second, second_cancel) = work.invocation_view()?;
    let second = second.invocation_window()?;
    let second_io = PreparationIo::from_native_work(&second)?;
    cache.bind(operation, Some(&second_io))?;
    assert_eq!(cache.summary().tail, 1);
    let retained = cache
        .io
        .as_ref()
        .ok_or_else(|| Error::Invalid("cache lost its ledger".into()))?;
    assert_eq!(retained.owned_bytes()?, retained_bytes);
    retained.check_cancellation()?;
    let outside_window = work.remaining()?.bytes_hashed;
    let before_second = second.remaining()?.bytes_hashed;
    retained.charge(acyclic_fs::WorkCounters {
        bytes_hashed: 17,
        ..Default::default()
    })?;
    assert_eq!(work.remaining()?.bytes_hashed, outside_window);
    assert_eq!(second.remaining()?.bytes_hashed, before_second - 17);
    // Growth must claim new bytes on the current window and retain custody on
    // the same original ledger even though the first invocation is cancelled.
    retained.retain_owned(31)?;
    assert_eq!(retained.owned_bytes()?, retained_bytes + 31);
    retained.release(31)?;
    second_cancel.cancel();
    assert!(retained.check_cancellation().is_err());
    drop(cache);
    Ok(())
}

#[test]
fn replay_proofs_bind_operation_sequence_retry_and_complete_event() -> Result<()> {
    let operation = OperationId::new();
    let event = ExecutionEvent::Started {
        request_digest: [7; 32],
    };
    let mut cache = SummaryCache::default();
    cache.bind(operation, None)?;
    {
        let mut edit = cache.edit();
        let (summary, io) = edit.parts();
        summary.accept(
            &ExecutionRecord {
                operation_id: operation,
                sequence: 1,
                idempotency_key: "original".into(),
                event: event.clone(),
            },
            None,
            Limits::default(),
            io,
        )?;
        edit.finish();
    }
    assert!(cache.authenticates(operation, 1, "original", &event, None)?);
    assert!(!cache.authenticates(OperationId::new(), 1, "original", &event, None)?);
    assert!(!cache.authenticates(operation, 2, "original", &event, None)?);
    for (sequence, retry, changed) in [
        (0, "original", event.clone()),
        (1, "substituted", event),
        (
            1,
            "original",
            ExecutionEvent::Started {
                request_digest: [8; 32],
            },
        ),
    ] {
        let error = cache
            .authenticates(operation, sequence, retry, &changed, None)
            .expect_err("changed authenticated metadata was accepted");
        assert_eq!(error.code(), crate::wire::ErrorCode::Conflict);
    }
    Ok(())
}
