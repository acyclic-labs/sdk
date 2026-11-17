use super::*;
use crate::test_meter::Meter;
use std::sync::atomic::Ordering;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

fn request(path: &StreamPath, tail: u64) -> AppendRequest {
    AppendRequest {
        path: path.clone(),
        records: vec![Bytes::from_static(b"value").into()],
        if_tail: Some(tail),
        idempotency_key: crate::IdempotencyKey::unique(),
    }
}

// Without the move, every write's payload stays on its writer's ledger for as
// long as the store keeps it, so each writer's `held` stays above zero here.
#[tokio::test]
async fn a_landed_write_leaves_nothing_on_its_writer_and_charges_the_store() -> TestResult<()> {
    let provider = MemoryStream::default();
    let capacity = Arc::clone(&provider.capacity);
    let path = path("custody/kept")?;
    let mut kept = vec![capacity.held()];
    let writer = Arc::new(Meter::default());
    for tail in 0..256 {
        let output = provider
            .native_append(request(&path, tail), writer.original())
            .await??;
        drop(output);
        assert_eq!(writer.held.load(Ordering::SeqCst), 0, "write {tail}");
        writer.assert_refunded();
        kept.push(capacity.held());
    }
    assert!(
        kept.windows(2).all(|pair| pair[0] < pair[1]),
        "the store is charged for each write it keeps: {kept:?}"
    );
    drop(provider);
    assert_eq!(
        capacity.held(),
        0,
        "the store returns what it no longer keeps"
    );
    Ok(())
}

#[tokio::test]
async fn a_write_past_store_capacity_is_refused_before_publication() -> TestResult<()> {
    let path = path("custody/bounded")?;
    let provider = MemoryStream::new(MemoryLimits {
        retained_bytes: Some(0),
        ..MemoryLimits::default()
    });
    let snapshot = provider.encode_state().await;
    let writer = Arc::new(Meter::default());
    let refused = provider
        .native_append(request(&path, 0), writer.original())
        .await?;
    assert!(matches!(refused, Err(StreamError::Capacity)));
    assert_eq!(provider.encode_state().await, snapshot);
    assert_eq!(provider.capacity.held(), 0);
    assert_eq!(writer.held.load(Ordering::SeqCst), 0);
    writer.assert_refunded();
    Ok(())
}

// Without the copies, the store keeps sharing the caller's buffers, and the
// caller's ledger stays charged for as long as the store keeps the write.
#[tokio::test]
async fn kept_request_bytes_funded_by_their_caller_leave_the_callers_ledger() -> TestResult<()> {
    let provider = MemoryStream::default();
    let caller = Arc::new(Meter::default());
    let body = Bytes::copy_from_slice(b"value").with_guard((), caller.original())?;
    let key = Bytes::copy_from_slice(b"custody-key").with_guard((), caller.original())?;
    let writer = Arc::new(Meter::default());
    let mut write = request(
        &StreamPath::new_native("custody/body", &caller.original())??,
        0,
    );
    write.records = vec![body.into()];
    write.idempotency_key = crate::IdempotencyKey(key);
    assert!(caller.held.load(Ordering::SeqCst) > 0);
    drop(provider.native_append(write, writer.original()).await??);
    assert_eq!(caller.held.load(Ordering::SeqCst), 0);
    assert_eq!(writer.held.load(Ordering::SeqCst), 0);
    let records = provider
        .read(ReadRequest {
            key: None,
            path: path("custody/body")?,
            from: 0,
            limit: 1,
            at_least: Default::default(),
        })
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0].value.as_slice(), b"value");
    assert_eq!(records[0].origin.as_str(), "custody/body");
    Ok(())
}
