use super::*;

fn request(path: &StreamPath) -> Result<CommitRequest, StreamError> {
    Ok(CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![crate::CommitCondition::Tail {
            path: path.clone(),
            expected: 1,
        }],
        mutations: vec![crate::CommitMutation::Append {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"started").into()],
        }],
        idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"dispatch-retry"))?,
    })
}

async fn publish(
    stream: &LocalStream,
    meter: &Arc<Meter>,
    mode: u8,
) -> std::io::Result<Result<(), NativeGuardFailure>> {
    // Inputs are fixed valid fixture values; construction stays outside the
    // provider's original native result and is checked by the caller below.
    let path = StreamPath::new("retry").map_err(std::io::Error::other)?;
    match mode {
        0 => stream
            .native_append(
                keyed_append("retry", 1).map_err(std::io::Error::other)?,
                meter.original(),
            )
            .await
            .map(|output| output.map(drop).map_err(NativeGuardFailure::from)),
        1 => stream
            .native_commit(
                request(&path).map_err(std::io::Error::other)?,
                None,
                meter.original(),
            )
            .await
            .map(|output| output.map(drop).map_err(NativeGuardFailure::from)),
        _ => stream
            .native_commit_guarded::<NativeGuardFailure, _, _>(
                request(&path).map_err(std::io::Error::other)?,
                None,
                meter.original(),
                || async { Err(NativeGuardFailure::Owner(42)) },
            )
            .await
            .map(|output| output.map(drop)),
    }
}

#[tokio::test]
async fn native_retries_seal_a_deferred_original_without_repeating_its_frame()
-> Result<(), Box<dyn std::error::Error>> {
    for mode in 0..3 {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits::default();
        let stream = LocalStream::open(directory.path(), limits).await?;
        let meter = Arc::new(Meter::local());
        let path = StreamPath::new("retry")?;
        stream.append(keyed_append("retry", 0)?).await?;
        if mode == 0 {
            drop(
                deferring_durability(
                    stream.native_append(keyed_append("retry", 1)?, meter.original()),
                )
                .await??,
            );
        } else {
            drop(
                deferring_durability(stream.native_commit(request(&path)?, None, meter.original()))
                    .await??,
            );
        }
        let before = stream.watermarks(path.clone()).await?;
        assert_eq!(before.accepted, 2);
        assert!(
            before.durable < before.accepted,
            "negative control already durable"
        );
        meter.deny_durability.store(true, Ordering::SeqCst);
        let error = publish(&stream, &meter, mode)
            .await
            .err()
            .ok_or("unfunded retry acknowledged an undurable claim")?;
        assert_original_denial(&error, "durability");
        assert_eq!(stream.watermarks(path.clone()).await?, before);
        meter.deny_durability.store(false, Ordering::SeqCst);
        publish(&stream, &meter, mode).await??;
        assert_eq!(stream.watermarks(path.clone()).await?.durable, 2);
        meter.assert_refunded();
        drop(stream);
        let reopened = LocalStream::open(directory.path(), limits).await?;
        assert_eq!(reopened.tail(path).await?, 2);
    }
    Ok(())
}
