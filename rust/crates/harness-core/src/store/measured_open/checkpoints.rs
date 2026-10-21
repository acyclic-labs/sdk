use super::*;

#[cfg(not(target_arch = "wasm32"))]
async fn open_native_history(
    provider: &Arc<Provider>,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(StreamAggregate<Provider>, acyclic_fs::NativeAllocation)> {
    let mut io = HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?);
    let aggregate = StreamAggregate::open(
        provider,
        (authority(), issuer().verifier(&mut None)?, schemas()),
        None,
        default_projection_read_limits(),
        DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut Some(&mut io),
    )
    .await?;
    io.return_staged_output()?;
    Ok((aggregate, io.into_custody()?))
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_checkpoint_open_preserves_actual_output_and_failed_source_prefixes() -> Result<()> {
    use acyclic_fs::{NativeMaterializationWork, NativeTreeMaterializationError, WorkError};
    let provider = projection_fixture(69).await?;
    let expected = with_content(&provider).await?.into_reducer();
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(budget(), &cancellation).expect("finite test budget");
    let accounting_bytes = work.receipt().peak_allocation_bytes;
    provider.reset_io();
    let (aggregate, custody) = open_native_history(&provider, &work).await?;
    assert_eq!(aggregate.reducer(), &expected);
    assert!(work.shares_ledger(custody.work()));
    assert_eq!(
        work.receipt().authority_bytes_read,
        provider.io_snapshot().read_bytes
    );
    drop(custody);
    let all_payload_allowance = budget().peak_allocation_bytes - accounting_bytes;
    assert!(work.admit_retained(all_payload_allowance).is_err());
    drop(aggregate);
    assert!(work.admit_retained(all_payload_allowance).is_ok());

    let denied_copy = NativeMaterializationWork::new(
        WorkBudget {
            bytes_copied: 0,
            ..budget()
        },
        &cancellation,
    )
    .expect("finite test budget");
    assert!(matches!(
        open_native_history(&provider, &denied_copy).await,
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "bytes_copied",
                ..
            }),
            custody: None,
        })
    ));
    for narrow in [
        WorkBudget {
            output_bytes: 0,
            ..budget()
        },
        WorkBudget {
            authority_bytes_read: 0,
            ..budget()
        },
    ] {
        let denied =
            NativeMaterializationWork::new(narrow, &cancellation).expect("finite test budget");
        provider.reset_io();
        assert!(open_native_history(&provider, &denied).await.is_err());
        assert_eq!(
            denied.receipt().authority_bytes_read,
            provider.io_snapshot().read_bytes
        );
        assert!(denied.receipt().authority_bytes_read > 0);
        assert_eq!(denied.receipt().output_bytes, 0);
    }
    let cancelled = CancellationToken::new();
    let work = NativeMaterializationWork::new(budget(), &cancelled).expect("finite test budget");
    cancelled.cancel();
    assert!(matches!(
        open_native_history(&provider, &work).await,
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn measured_checkpoint_cancellation_stops_before_next_sdk_call() -> Result<()> {
    let provider = projection_fixture(69).await?;
    let cancellation = CancellationToken::new();
    *provider
        .history_cancel_after_tail
        .lock()
        .map_err(|error| Error::Storage(error.to_string()))? = Some(cancellation.clone());
    provider.reset_io();
    let failure = open_history(
        &provider,
        authority(),
        None,
        default_projection_read_limits(),
        budget(),
        &cancellation,
    )
    .await
    .err()
    .expect("cancelled checkpoint read");
    assert_eq!(failure.work.authority_records_read, 0);
    assert_eq!(failure.work.output_bytes, 0);
    assert_eq!(provider.io_snapshot().reads, 0);
    Ok(())
}
