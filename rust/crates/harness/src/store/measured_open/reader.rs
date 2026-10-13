use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_history_pages_preserve_original_provider_prefixes_and_custody() -> Result<()> {
    use acyclic_fs::NativeMaterializationWork;
    let provider = projection_fixture(69).await?;
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
    assert_eq!(
        fault_work.receipt().authority_bytes_read,
        provider.io_snapshot().read_bytes
    );
    assert_eq!(fault_work.receipt().output_bytes, 0);
    assert_eq!(fault_work.receipt().items_returned, 0);

    drop(cursor);
    drop(cursor_custody);
    drop(reader);
    drop(reader_custody);
    Ok(())
}
