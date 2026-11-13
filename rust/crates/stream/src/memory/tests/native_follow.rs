use super::*;
use crate::test_meter::Meter;
use futures::{FutureExt as _, StreamExt as _};

#[tokio::test]
async fn anchor_validation_uses_original_read_credit_before_opening()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = MemoryStream::default();
    let path = StreamPath::new("native/anchor")?;
    let AppendOutcome::Committed(receipt) = provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"aa").into()],
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?
    else {
        return Err("append refused".into());
    };
    let state = provider.state.read().await;
    let selected = state.paths.get(&path).ok_or("missing path")?;
    for read_limit in [0, 2] {
        let meter = Arc::new(Meter {
            read_limit,
            ..Meter::default()
        });
        let work = crate::native::Work::new(meter.original())?;
        let cursor = HistoryCursor::initial(
            Some(selected),
            1,
            Some(receipt.commit_id),
            crate::WhenAbsent::NotFound,
            Some(work),
        )
        .await;
        assert_eq!(cursor.is_ok(), read_limit == 2);
        assert_eq!(meter.read.load(Ordering::SeqCst), read_limit);
        drop(cursor);
        meter.assert_refunded();
    }
    Ok(())
}

#[tokio::test]
async fn refused_record_accounting_preserves_the_cursor_position()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = MemoryStream::default();
    let path = StreamPath::new("native/refused-record")?;
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"aa").into()],
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?;
    let state = provider.state.read().await;
    let selected = state.paths.get(&path).ok_or("missing path")?;
    let mut cursor = FiniteRead::new(
        selected,
        ReadRequest {
            key: None,
            path: path.clone(),
            from: 0,
            limit: 1,
            at_least: Default::default(),
        },
    );
    assert!(
        cursor
            .peek(|_| Ok::<_, StreamError>(()), || StreamError::Unavailable)?
            .is_some()
    );
    let meter = Arc::new(Meter {
        check_on_record: true,
        cancel_on_copy: true.into(),
        ..Meter::default()
    });
    assert!(cursor.take_native_record(&meter.original(), 2).is_err());
    assert_eq!(cursor.next, 0, "refused delivery consumed the record");
    meter.assert_refunded();
    Ok(())
}

#[tokio::test]
async fn stop_retires_the_pending_read_without_releasing_the_state_lock()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = MemoryStream::default();
    let path = StreamPath::new("native/pending")?;
    let meter = Arc::new(Meter::default());
    let mut request = crate::FollowRequest::new(path.clone(), 0);
    request.when_absent = crate::WhenAbsent::Wait;
    let mut output = provider.native_follow(request, meter.original()).await??;
    let held = provider.state.write().await;
    assert!(output.value_mut().next().now_or_never().is_none());
    assert!(meter.held.load(Ordering::SeqCst) > 0);
    drop(output);
    // No detached snapshot producer may wait for this lock after the cursor
    // is destroyed. The production watch's owning-future stop is tested at
    // the real computed UI boundary, using its reservation-capable ledger.
    meter.assert_refunded();
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    assert!(held.signals.get(&path)?.is_none());
    drop(held);
    assert!(provider.state.try_write().is_ok());
    Ok(())
}
