use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_reader_constructor_rejects_before_copy_and_transfers_original_custody() -> Result<()> {
    use acyclic_fs::{NativeMaterializationWork, NativeTreeMaterializationError, WorkError};
    let provider = Arc::new(Provider::default());
    let cancellation = CancellationToken::new();
    let mut denied = budget();
    denied.bytes_copied = 0;
    let native =
        NativeMaterializationWork::new(denied, &cancellation).expect("original accounting state");
    let prefix = native.receipt();
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native).expect("empty custody"),
    );
    assert!(matches!(
        HistoryReader::new(
            &provider,
            &authority(),
            &issuer().verifier(&mut None).expect("verifier copy"),
            &mut Some(&mut io)
        ),
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "bytes_copied",
                ..
            }),
            custody: None,
        })
    ));
    assert_eq!(native.receipt(), prefix);
    drop(io);
    let native =
        NativeMaterializationWork::new(budget(), &cancellation).expect("original accounting state");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native).expect("empty custody"),
    );
    let reader = HistoryReader::new(
        &provider,
        &authority(),
        &issuer().verifier(&mut None).expect("verifier copy"),
        &mut Some(&mut io),
    )?;
    let custody = io.into_custody()?;
    assert!(custody.bytes() >= authority().stream_path(&mut None)?.len() as u64);
    assert!(native.receipt().bytes_copied >= authority().stream_path(&mut None)?.len() as u64);
    let sdk = provider.io_snapshot();
    assert_eq!(
        sdk.reads
            + sdk.tails as u64
            + sdk.writes
            + sdk.inspections
            + sdk.commit_reads
            + sdk.other_calls,
        0
    );
    drop(reader);
    drop(custody);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_history_pages_preserve_original_provider_prefixes_and_custody() -> Result<()> {
    use acyclic_fs::{NativeMaterializationWork, NativeTreeMaterializationError};
    let provider = fixture().await?;
    let cancellation = CancellationToken::new();
    let native =
        NativeMaterializationWork::new(budget(), &cancellation).expect("original finite ledger");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)
            .expect("original reader custody"),
    );
    let reader = HistoryReader::new(
        &provider,
        &authority(),
        &issuer().verifier(&mut None).expect("verifier copy"),
        &mut Some(&mut io),
    )?;
    let reader_custody = io.into_custody()?;
    provider.reset_io();
    let (cursor, cursor_custody) = async {
        let mut _history_io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&native)?,
        );
        let _history_value = reader.pin(64, Some(&mut _history_io)).await?;
        Ok::<_, crate::Error>((_history_value, _history_io.into_custody()?))
    }
    .await?;
    assert_eq!(cursor.through_revision, 69);
    assert!(cursor_custody.bytes() >= cursor.authority.id.len() as u64);
    assert!(native.shares_ledger(cursor_custody.work()));
    let prefix = native.receipt();
    provider.reset_io();
    let (page, page_custody) = async {
        let mut _history_io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&native)?,
        );
        let _history_value = reader
            .read_page(
                &cursor,
                default_projection_read_limits(),
                Some(&mut _history_io),
            )
            .await?;
        _history_io.return_staged_output()?;
        Ok::<_, crate::Error>((_history_value, _history_io.into_custody()?))
    }
    .await?;
    let receipt = native.receipt();
    let sdk = provider.io_snapshot();
    assert_eq!(page.events.len(), 5);
    assert_eq!(page.cursor.after_revision, 69);
    assert!(native.shares_ledger(page_custody.work()));
    assert!(page_custody.bytes() >= page.cursor.authority.id.len() as u64);

    assert_eq!(
        receipt.authority_bytes_read - prefix.authority_bytes_read,
        sdk.read_bytes
    );
    assert_eq!(receipt.output_bytes - prefix.output_bytes, sdk.read_bytes);
    assert_eq!(receipt.items_returned - prefix.items_returned, 5);
    assert_eq!(
        sdk.writes + sdk.inspections + sdk.commit_reads + sdk.other_calls,
        0
    );
    let prefix = native.receipt();
    provider.reset_io();
    let (archived, archived_custody) = async {
        let mut _history_io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&native)?,
        );
        let _history_value = reader
            .operation_event(
                page.events[0].operation_id,
                budget().authority_bytes_read,
                &mut Some(&mut _history_io),
            )
            .await?
            .0;
        _history_io.return_staged_output()?;
        Ok::<_, crate::Error>((_history_value, _history_io.into_custody()?))
    }
    .await?;
    assert_eq!(archived.as_ref(), Some(&page.events[0]));
    assert!(native.shares_ledger(archived_custody.work()));
    assert_eq!(native.receipt().items_returned - prefix.items_returned, 1);
    assert_eq!(
        native.receipt().authority_bytes_read - prefix.authority_bytes_read,
        provider.io_snapshot().read_bytes
    );
    drop(archived);
    drop(archived_custody);
    drop(page);
    drop(page_custody);

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
            .expect("original finite denied ledger");
        provider.reset_io();
        assert!(matches!(
            async {
                let mut io = crate::store::HistoryWork::new(
                    crate::filesystem::PreparationIo::from_native_work(&denied)?,
                );
                let value = reader
                    .read_page(&cursor, default_projection_read_limits(), Some(&mut io))
                    .await?;
                io.return_staged_output()?;
                Ok::<_, Error>((value, io.into_custody()?))
            }
            .await,
            Err(Error::NativeWork { custody: None, .. })
        ));
        let receipt = denied.receipt();
        assert_eq!(
            receipt.authority_bytes_read,
            provider.io_snapshot().read_bytes
        );
        assert!(receipt.authority_bytes_read > 0);
        assert_eq!(receipt.output_bytes, 0);
        assert_eq!(receipt.items_returned, 0);
    }

    let fault_work = NativeMaterializationWork::new(budget(), &cancellation)
        .expect("original failed source ledger");
    provider.reset_io();
    provider.history_read_fault.store(4, Ordering::SeqCst);
    assert!(
        async {
            let mut io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&fault_work)?,
            );
            let value = reader
                .read_page(&cursor, default_projection_read_limits(), Some(&mut io))
                .await?;
            io.return_staged_output()?;
            Ok::<_, Error>((value, io.into_custody()?))
        }
        .await
        .is_err()
    );
    assert_eq!(fault_work.receipt().authority_bytes_read, 7);
    assert_eq!(
        fault_work.receipt().authority_bytes_read,
        provider.io_snapshot().read_bytes
    );
    assert_eq!(fault_work.receipt().output_bytes, 0);
    assert_eq!(fault_work.receipt().items_returned, 0);

    let wrong_issuer = AuthorityIssuer::new("test", [8; 32], authority());
    let auth_work = NativeMaterializationWork::new(budget(), &cancellation)
        .expect("original authentication ledger");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&auth_work)
            .expect("original reader custody"),
    );
    let wrong_reader = HistoryReader::new(
        &provider,
        &authority(),
        &wrong_issuer.verifier(&mut None).expect("verifier copy"),
        &mut Some(&mut io),
    )?;
    let wrong_custody = io.into_custody()?;
    provider.reset_io();
    assert!(
        async {
            let mut io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&auth_work)?,
            );
            let value = wrong_reader
                .read_page(&cursor, default_projection_read_limits(), Some(&mut io))
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
    drop(wrong_reader);
    drop(wrong_custody);

    let cancelled = CancellationToken::new();
    let cancelled_work =
        NativeMaterializationWork::new(budget(), &cancelled).expect("original cancellation ledger");
    cancelled.cancel();
    provider.reset_io();
    assert!(matches!(
        async {
            let mut io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&cancelled_work)?,
            );
            let value = reader.pin(64, Some(&mut io)).await?;
            Ok::<_, Error>((value, io.into_custody()?))
        }
        .await,
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Cancelled,
            custody: None
        })
    ));
    assert!(matches!(
        async {
            let mut io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&cancelled_work)?,
            );
            let value = reader
                .read_page(&cursor, default_projection_read_limits(), Some(&mut io))
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
    drop(cursor);
    drop(cursor_custody);
    drop(reader);
    drop(reader_custody);
    Ok(())
}
