use super::*;
use crate::{OwnedBytes, test_meter::Meter};
use futures::StreamExt as _;
type MemoryResult<T> = Result<T, Box<dyn std::error::Error>>;

async fn seeded() -> MemoryResult<MemoryStream> {
    let provider = MemoryStream::default();
    provider
        .append(AppendRequest {
            path: path("native/read")?,
            records: vec![
                (OwnedBytes::from_static(b"aa")).into(),
                (OwnedBytes::from_static(b"bb")).into(),
                OwnedBytes::from_static(b"cc").into(),
            ],
            if_tail: Some(0),
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?;
    Ok(provider)
}

fn read_request() -> Result<ReadRequest, StreamError> {
    Ok(ReadRequest {
        path: path("native/read")?,
        from: 0,
        limit: 3,
        at_least: Default::default(),
    })
}

#[tokio::test]
async fn partial_budget_and_cancellation_keep_the_exact_ordered_prefix() -> MemoryResult<()> {
    let provider = seeded().await?;
    for cancellation in [false, true] {
        let meter = Arc::new(Meter {
            read_limit: if cancellation { u64::MAX } else { 2 },
            cancel_after_read: if cancellation { 2 } else { u64::MAX },
            ..Meter::default()
        });
        let mut output = provider
            .native_read(read_request()?, meter.original())
            .await??;
        let first = output
            .value_mut()
            .next()
            .await
            .ok_or(StreamError::Unavailable)???;
        assert_eq!(first.value().sequence, 0);
        assert_eq!(first.value().value, OwnedBytes::from_static(b"aa"));
        let alias = first.value().value.clone();
        drop(first);
        let error = output
            .value_mut()
            .next()
            .await
            .ok_or(StreamError::Unavailable)?;
        assert!(
            matches!(error, Err(error) if error.kind() == if cancellation {
            std::io::ErrorKind::Interrupted
        } else { std::io::ErrorKind::OutOfMemory })
        );
        assert!(output.value_mut().next().await.is_none());
        assert_eq!(meter.read.load(Ordering::SeqCst), 2);
        assert_eq!(meter.pending_read.load(Ordering::SeqCst), 0);
        assert!(meter.held.load(Ordering::SeqCst) > 0);
        drop(output);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        assert_eq!(alias, OwnedBytes::from_static(b"aa"));
    }
    Ok(())
}

#[tokio::test]
async fn tail_read_fits_its_budget_without_hydrating_older_batches() -> MemoryResult<()> {
    let provider = MemoryStream::default();
    for sequence in 0..1000 {
        provider
            .append(AppendRequest {
                path: path("native/read")?,
                records: vec![(OwnedBytes::from_static(b"aa")).into()],
                if_tail: Some(sequence),
                idempotency_key: crate::IdempotencyKey::unique(),
            })
            .await?;
    }
    let meter = Arc::new(Meter {
        memory_limit: 4096,
        ..Meter::default()
    });
    let mut output = provider
        .native_read(
            ReadRequest {
                path: path("native/read")?,
                from: 999,
                limit: 1,
                at_least: Default::default(),
            },
            meter.original(),
        )
        .await??;
    let record = output
        .value_mut()
        .next()
        .await
        .ok_or(StreamError::Unavailable)???;
    assert_eq!(record.value().sequence, 999);
    assert_eq!(record.value().value.as_ref(), b"aa");
    assert!(output.value_mut().next().await.is_none());
    drop(record);
    drop(output);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn large_snapshot_read_reuses_one_page_and_preserves_original_refusals() -> MemoryResult<()> {
    let provider = MemoryStream::default();
    let path = path("native/read/pages")?;
    for from in (0..10_000).step_by(128) {
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: (from..(from + 128).min(10_000))
                    .map(|_| OwnedBytes::from_static(b"aa").into())
                    .collect(),
                if_tail: Some(from),
                idempotency_key: crate::IdempotencyKey::unique(),
            })
            .await?;
    }
    for failure in [None, Some(false), Some(true)] {
        let meter = Arc::new(Meter {
            memory_limit: 40 * 1024,
            read_limit: if failure == Some(false) {
                2048
            } else {
                u64::MAX
            },
            cancel_after_read: if failure == Some(true) {
                2048
            } else {
                u64::MAX
            },
            ..Meter::default()
        });
        let mut output = provider
            .native_read(
                ReadRequest {
                    path: path.clone(),
                    from: 0,
                    limit: u32::MAX,
                    at_least: Default::default(),
                },
                meter.original(),
            )
            .await??;
        if failure.is_none() {
            // Appending after opening must not extend the captured finite cut.
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![OwnedBytes::from_static(b"late").into()],
                    if_tail: Some(10_000),
                    idempotency_key: crate::IdempotencyKey::unique(),
                })
                .await?;
        }
        let expected = if failure.is_none() { 10_000 } else { 1024 };
        for sequence in 0..expected {
            let record = output
                .value_mut()
                .next()
                .await
                .ok_or(StreamError::Unavailable)???;
            assert_eq!(record.value().sequence, sequence);
            assert_eq!(record.value().value.as_ref(), b"aa");
        }
        if let Some(cancellation) = failure {
            let error = output
                .value_mut()
                .next()
                .await
                .ok_or(StreamError::Unavailable)?;
            assert!(
                matches!(error, Err(error) if error.kind() == if cancellation {
                    std::io::ErrorKind::Interrupted
                } else {
                    std::io::ErrorKind::OutOfMemory
                })
            );
            assert_eq!(meter.read.load(Ordering::SeqCst), 2048);
        }
        assert!(output.value_mut().next().await.is_none());
        drop(output);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        assert_eq!(meter.pending_read.load(Ordering::SeqCst), 0);
        assert_eq!(meter.pending_examination.load(Ordering::SeqCst), 0);
    }
    Ok(())
}
