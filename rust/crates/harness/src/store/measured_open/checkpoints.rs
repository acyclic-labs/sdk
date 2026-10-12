use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_checkpoint_open_preserves_actual_output_and_failed_source_prefixes() -> Result<()> {
    use acyclic_fs::{NativeMaterializationWork, NativeTreeMaterializationError};
    let provider = fixture().await?;
    let cancellation = CancellationToken::new();
    let expected = StreamAggregate::open(
        &provider,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .reducer()
    .snapshot(usize::MAX, &mut None)?
    .0;
    let measured = open_history(
        &provider,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        None,
        crate::store::default_projection_read_limits(),
        budget(),
        &cancellation,
    )
    .await
    .map_err(|failure| failure.error)?;
    let expected_output = measured.work.output_bytes;
    let expected_items = measured.work.items_returned;
    drop(measured);
    let work = NativeMaterializationWork::new(budget(), &cancellation)
        .expect("original finite open ledger");
    let accounting_bytes = work.receipt().peak_allocation_bytes;
    provider.reset_io();
    let (aggregate, custody) = async {
        let mut _history_io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&work)?,
        );
        let _history_value = StreamAggregate::open(
            &provider,
            authority(),
            issuer().verifier(&mut None).expect("verifier copy"),
            schemas(),
            None,
            crate::store::default_projection_read_limits(),
            crate::store::DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut Some(&mut _history_io),
        )
        .await?;
        _history_io.return_staged_output()?;
        Ok::<_, crate::Error>((_history_value, _history_io.into_custody()?))
    }
    .await?;
    assert_eq!(
        aggregate.reducer().snapshot(usize::MAX, &mut None)?.0,
        expected
    );
    assert!(work.shares_ledger(custody.work()));
    let receipt = work.receipt();
    let sdk = provider.io_snapshot();

    assert_eq!(receipt.authority_bytes_read, sdk.read_bytes);
    assert_eq!(receipt.output_bytes, expected_output);
    assert_eq!(receipt.items_returned, expected_items);
    assert_eq!(
        sdk.writes + sdk.inspections + sdk.commit_reads + sdk.other_calls,
        0
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
    .expect("original clone-denied ledger");
    provider.reset_io();
    assert!(matches!(
        async {
            let mut io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&denied_copy)?,
            );
            let value = StreamAggregate::open(
                &provider,
                authority(),
                issuer().verifier(&mut None).expect("verifier copy"),
                schemas(),
                None,
                default_projection_read_limits(),
                DEFAULT_TERMINAL_EFFECTS,
                None,
                &mut Some(&mut io),
            )
            .await?;
            io.return_staged_output()?;
            Ok::<_, Error>((value, io.into_custody()?))
        }
        .await,
        Err(Error::NativeWork { custody: None, .. })
    ));
    let sdk = provider.io_snapshot();
    assert_eq!(sdk.reads + sdk.tails as u64, 0);
    assert_eq!(denied_copy.receipt().bytes_copied, 0);
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
        let denied = NativeMaterializationWork::new(narrow, &cancellation)
            .expect("original denied open ledger");
        provider.reset_io();
        assert!(
            async {
                let mut io = crate::store::HistoryWork::new(
                    crate::filesystem::PreparationIo::from_native_work(&denied)?,
                );
                let value = StreamAggregate::open(
                    &provider,
                    authority(),
                    issuer().verifier(&mut None).expect("verifier copy"),
                    schemas(),
                    None,
                    default_projection_read_limits(),
                    DEFAULT_TERMINAL_EFFECTS,
                    None,
                    &mut Some(&mut io),
                )
                .await?;
                io.return_staged_output()?;
                Ok::<_, Error>((value, io.into_custody()?))
            }
            .await
            .is_err()
        );
        assert_eq!(
            denied.receipt().authority_bytes_read,
            provider.io_snapshot().read_bytes
        );
        assert!(denied.receipt().authority_bytes_read > 0);
        assert_eq!(denied.receipt().output_bytes, 0);
        assert_eq!(denied.receipt().items_returned, 0);
    }
    let wrong_issuer = AuthorityIssuer::new("test", [8; 32], authority());
    let auth_work = NativeMaterializationWork::new(budget(), &cancellation)
        .expect("original authentication ledger");
    provider.reset_io();
    assert!(
        async {
            let mut io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&auth_work)?,
            );
            let value = StreamAggregate::open(
                &provider,
                authority(),
                wrong_issuer.verifier(&mut None).expect("verifier copy"),
                schemas(),
                None,
                default_projection_read_limits(),
                DEFAULT_TERMINAL_EFFECTS,
                None,
                &mut Some(&mut io),
            )
            .await?;
            io.return_staged_output()?;
            Ok::<_, Error>((value, io.into_custody()?))
        }
        .await
        .is_err()
    );
    assert_eq!(
        auth_work.receipt().authority_bytes_read,
        provider.io_snapshot().read_bytes
    );
    assert!(auth_work.receipt().authority_bytes_read > 0);
    assert_eq!(auth_work.receipt().output_bytes, 0);
    let cancelled = CancellationToken::new();
    let cancelled_work = NativeMaterializationWork::new(budget(), &cancelled)
        .expect("original cancelled open ledger");
    cancelled.cancel();
    provider.reset_io();
    assert!(matches!(
        async {
            let mut io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&cancelled_work)?,
            );
            let value = StreamAggregate::open(
                &provider,
                authority(),
                issuer().verifier(&mut None).expect("verifier copy"),
                schemas(),
                None,
                default_projection_read_limits(),
                DEFAULT_TERMINAL_EFFECTS,
                None,
                &mut Some(&mut io),
            )
            .await?;
            io.return_staged_output()?;
            Ok::<_, Error>((value, io.into_custody()?))
        }
        .await,
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Cancelled,
            custody: None
        })
    ));
    let sdk = provider.io_snapshot();
    assert_eq!(sdk.reads + sdk.tails as u64, 0);
    assert_eq!(cancelled_work.receipt().output_bytes, 0);
    Ok(())
}

#[tokio::test]
async fn measured_checkpoint_cancellation_stops_before_next_sdk_call() -> Result<()> {
    let provider = fixture().await?;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let uncancelled = CancellationToken::new();
    for (work_budget, token) in [
        (budget(), &cancelled),
        (WorkBudget::UNBOUNDED, &uncancelled),
    ] {
        provider.reset_io();
        let failure = open_history(
            &provider,
            authority(),
            issuer().verifier(&mut None).expect("verifier copy"),
            schemas(),
            None,
            crate::store::default_projection_read_limits(),
            work_budget,
            token,
        )
        .await
        .err()
        .expect("invalid preflight must not touch provider");
        assert_eq!(*failure.work, WorkCounters::default());
        assert_eq!(
            provider.io_snapshot().reads + provider.io_snapshot().tails as u64,
            0
        );
    }
    let cancellation = CancellationToken::new();
    *provider
        .history_cancel_after_tail
        .lock()
        .map_err(|_| Error::Storage("cancellation control poisoned".into()))? =
        Some(cancellation.clone());
    provider.reset_io();
    let failure = open_history(
        &provider,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        None,
        crate::store::default_projection_read_limits(),
        budget(),
        &cancellation,
    )
    .await
    .err()
    .expect("cancellation after cut must precede checkpoint read");
    assert_eq!(failure.work.authority_records_read, 0);
    assert_eq!(failure.work.output_bytes, 0);
    assert_eq!(provider.io_snapshot().reads, 0);
    provider.reset_io();
    let other = Authority {
        id: "other".into(),
        ..authority()
    };
    let failure = open_history(
        &provider,
        other,
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        None,
        crate::store::default_projection_read_limits(),
        budget(),
        &CancellationToken::new(),
    )
    .await
    .err()
    .expect("wrong audience must fail before provider IO");
    assert!(matches!(failure.error, Error::Unauthorized(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    let empty_provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let empty = empty_provider.clone();
    let receipt = open_history(
        &empty,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        None,
        crate::store::default_projection_read_limits(),
        budget(),
        &CancellationToken::new(),
    )
    .await
    .map_err(|failure| failure.error)?;
    assert_eq!(receipt.value.reducer().revision(), 0);
    assert_eq!(receipt.work.authority_bytes_read, 0);
    assert_eq!(receipt.work.items_returned, 0);
    assert_eq!(empty_provider.io_snapshot().writes, 0);
    Ok(())
}

#[tokio::test]
async fn measured_snapshot_authentication_observes_original_encoder_and_mac_failures() -> Result<()>
{
    let provider = fixture().await?;
    let snapshot = crate::store::checkpoints::load_with_work(
        &provider,
        &authority(),
        &issuer().verifier(&mut None).expect("verifier copy"),
        default_projection_read_limits(),
        None,
        &mut None,
    )
    .await?
    .snapshot
    .expect("durable checkpoint");
    let expected = Reducer::restore(
        snapshot.copy(&mut None)?,
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
    )?;
    let cancellation = CancellationToken::new();
    let mut work = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
        budget(),
        &cancellation,
    )?);
    work.preflight()?;
    provider.reset_io();
    let result = Reducer::restore(
        snapshot.copy(&mut None)?,
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        &mut Some(&mut work),
    );
    let receipt = work.finish(result).map_err(|failure| failure.error)?;
    assert_eq!(receipt.value, expected);
    assert!(receipt.work.bytes_encoded > 0);
    assert!(receipt.work.bytes_hashed > 0);
    assert!(receipt.work.peak_allocation_bytes > 0);
    assert_eq!(receipt.work.authority_bytes_read, 0);
    assert_eq!(receipt.work.backend_read_operations, 0);
    assert_eq!(receipt.work.source_bytes_read, 0);
    for narrow in [
        WorkBudget {
            bytes_encoded: 0,
            ..budget()
        },
        WorkBudget {
            bytes_hashed: 0,
            ..budget()
        },
        WorkBudget {
            peak_allocation_bytes: 0,
            ..budget()
        },
    ] {
        let mut work = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_budget(narrow, &cancellation)?,
        );
        work.preflight()?;
        let result = Reducer::restore(
            snapshot.copy(&mut None)?,
            issuer().verifier(&mut None).expect("verifier copy"),
            schemas(),
            &mut Some(&mut work),
        );
        let failure = work
            .finish(result)
            .err()
            .expect("original snapshot crypto shares the cumulative work allowance");
        if narrow.bytes_encoded == 0 {
            assert_eq!(failure.work.bytes_encoded, 0);
        }
        if narrow.peak_allocation_bytes == 0 {
            assert_eq!(failure.work.peak_allocation_bytes, 0);
        }
        if narrow.bytes_hashed == 0 {
            assert!(failure.work.bytes_encoded > 0);
            assert!(failure.work.peak_allocation_bytes > 0);
            assert_eq!(failure.work.bytes_hashed, 0);
        }
        assert_eq!(failure.work.authority_bytes_read, 0);
        assert_eq!(failure.work.backend_read_operations, 0);
        assert_eq!(failure.work.output_bytes, 0);
    }
    let mut forged = snapshot.copy(&mut None)?;
    forged.attestation[0] ^= 1;
    assert!(matches!(
        Reducer::restore(
            forged.copy(&mut None)?,
            issuer().verifier(&mut None).expect("verifier copy"),
            schemas()
        ),
        Err(Error::Unauthorized(_))
    ));
    let mut work = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
        budget(),
        &cancellation,
    )?);
    let result = Reducer::restore(
        forged,
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        &mut Some(&mut work),
    );
    let failure = work
        .finish(result)
        .err()
        .expect("original snapshot MAC must reject even after its state digest verifies");
    assert!(matches!(failure.error, Error::Unauthorized(_)));
    assert!(failure.work.bytes_encoded > 0);
    assert!(failure.work.bytes_hashed > 0);
    assert_eq!(failure.work.output_bytes, 0);
    assert_eq!(failure.work.items_returned, 0);
    let mut forged = snapshot;
    forged.state_digest[0] ^= 1;
    let mut work = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
        budget(),
        &cancellation,
    )?);
    let result = Reducer::restore(
        forged,
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        &mut Some(&mut work),
    );
    let failure = work
        .finish(result)
        .err()
        .expect("original complete projection digest must reject");
    assert!(matches!(failure.error, Error::Invalid(_)));
    assert!(failure.work.bytes_encoded > 0);
    assert!(failure.work.bytes_hashed > 0);
    assert_eq!(failure.work.output_bytes, 0);
    assert_eq!(
        provider.io_snapshot().reads + provider.io_snapshot().tails as u64,
        0
    );
    Ok(())
}
