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
        key: None,
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
                key: None,
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
async fn lazy_native_key_read_pins_inherited_cut_and_exact_key_bytes() -> MemoryResult<()> {
    let provider = MemoryStream::default();
    let source = path("native/key-source")?;
    let destination = path("native/key-fork")?;
    let keyed = |key: &'static [u8], value: &'static [u8]| AppendRecord {
        value: OwnedBytes::from_static(value),
        headers: crate::RecordHeaders::new(vec![OwnedBytes::from_static(key)], Vec::new()).unwrap(),
    };
    provider
        .append(AppendRequest {
            path: source.clone(),
            records: vec![
                keyed(b"a", b"{ invalid JSON \0\xff"),
                keyed(b"a\0", b"different key"),
                keyed(b"a", b"beyond inherited cut"),
            ],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    provider
        .fork(ForkRequest {
            source,
            destination: destination.clone(),
            at_tail: Some(2),
            exclude: Vec::new(),
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    provider
        .append(AppendRequest {
            path: destination.clone(),
            records: vec![keyed(b"a", b"private suffix")],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    let request = ReadRequest {
        path: destination.clone(),
        from: 0,
        limit: 10,
        at_least: Default::default(),
        key: Some(crate::ReadKeySeek {
            key: OwnedBytes::from_static(b"a"),
            before: u64::MAX,
        }),
    };
    let expected =
        futures::TryStreamExt::try_collect::<Vec<_>>(provider.read(request.clone()).await?).await?;
    let meter = Arc::new(Meter::local());
    let mut output = provider.native_read(request, meter.original()).await??;
    provider
        .append(AppendRequest {
            path: destination,
            records: vec![keyed(b"a", b"after pin")],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    let mut actual = Vec::new();
    while let Some(row) = output.value_mut().next().await {
        actual.push(row??.value().clone());
    }
    assert_eq!(actual, expected);
    assert_eq!(
        actual
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        [0, 2]
    );
    drop(output);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}

#[tokio::test]
async fn lazy_native_read_keeps_original_late_examination_refusal() -> MemoryResult<()> {
    let provider = seeded().await?;
    let meter = Arc::new(Meter::local());
    let mut output = provider
        .native_read(read_request()?, meter.original())
        .await??;
    let opened = meter.examined.load(Ordering::SeqCst);
    meter.examination_limit.store(opened, Ordering::SeqCst);
    let error = output
        .value_mut()
        .next()
        .await
        .ok_or("expected original refusal")?
        .err()
        .ok_or("read escaped admission")?;
    assert!(error.get_ref().is_some_and(|cause| {
        cause
            .downcast_ref::<crate::test_meter::NativeDenial>()
            .is_some_and(|cause| cause.0 == "examination")
    }));
    assert!(output.value_mut().next().await.is_none());
    assert_eq!(meter.read.load(Ordering::SeqCst), 0);
    assert_eq!(meter.copied.load(Ordering::SeqCst), 0);
    drop(output);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}
