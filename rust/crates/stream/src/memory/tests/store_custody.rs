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
    for tail in 0..64 {
        let writer = Arc::new(Meter::default());
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
