use futures::TryFutureExt as _;
use super::*;

#[tokio::test]
async fn dispatch_publication_admits_durability_before_accepting_a_start()
-> Result<(), Box<dyn std::error::Error>> {
    for guarded in [false, true] {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let meter = Arc::new(Meter::local());
        let path = StreamPath::new("dispatch")?;
        provider.append(keyed_append("dispatch", 0)?).await?;
        meter.deny_durability.store(true, Ordering::SeqCst);
        for denied in [true, false] {
            meter.deny_durability.store(denied, Ordering::SeqCst);
            let key = IdempotencyKey::new(OwnedBytes::from_static(b"dispatch"))?;
            let append = keyed_append("dispatch", 1)?;
            let result = deferring_durability(requiring_durability(async {
                if guarded {
                    provider
                        .native_commit_guarded::<NativeGuardFailure, _, _>(
                            CommitRequest {
                                conditions: vec![crate::CommitCondition::Tail {
                                    path: path.clone(),
                                    expected: 1,
                                }],
                                mutations: vec![crate::CommitMutation::Append {
                                    path: path.clone(),
                                    records: vec![OwnedBytes::from_static(b"started").into()],
                                }],
                                idempotency_key: key,
                            },
                            None,
                            meter.original(),
                            || async { Ok(()) },
                        )
                        .await
                        .map(|output| output.map(drop))
                } else {
                    provider
                        .native_append(append, meter.original())
                        .await
                        .map(|output| output.map(drop).map_err(NativeGuardFailure::from))
                }
            }))
            .await;
            if denied {
                let error = result.err().ok_or("unfunded dispatch was accepted")?;
                assert_original_denial(&error, "durability");
                assert_eq!(provider.tail(path.clone()).await?, 1);
            } else {
                result??;
                assert_eq!(provider.watermarks(path.clone()).await?.durable, 2);
            }
            meter.assert_refunded();
        }
    }
    Ok(())
}

#[tokio::test]
async fn phase_barrier_preserves_live_delivery_and_reopens_the_accepted_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    for defer in [false, true] {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits::default();
        let provider = LocalStream::open(directory.path(), limits).await?;
        assert!(provider.supports_deferred_durability());
        let path = StreamPath::new("phase")?;
        for ordinal in 0..16 {
            let request = keyed_append("phase", ordinal)?;
            let result = if defer {
                deferring_durability(provider.append(request)).await
            } else {
                // Negative control: the previous per-append durability path.
                provider.append(request).await
            };
            assert!(matches!(result?, AppendOutcome::Committed(_)));
            let watermarks = provider.watermarks(path.clone()).await?;
            assert_eq!(watermarks.accepted, ordinal + 1);
            assert_eq!(watermarks.durable, if defer { 0 } else { ordinal + 1 });
            let mut follow = provider
                .follow(crate::FollowRequest { path: path.clone(), from: ordinal, preceding: None, filter: crate::FollowFilter::default(), progress: false }).map_ok(crate::records_only)
                .await?;
            assert_eq!(
                follow
                    .next()
                    .await
                    .ok_or("accepted delta missing")??
                    .sequence,
                ordinal
            );
        }
        provider.durability_barrier().await?;
        assert_eq!(provider.watermarks(path.clone()).await?.durable, 16);
        drop(provider);
        let reopened = LocalStream::open(directory.path(), limits).await?;
        assert_eq!(reopened.tail(path).await?, 16);
        for ordinal in 0..16 {
            let request = keyed_append("phase", ordinal)?;
            assert!(matches!(
                reopened.append(request).await?,
                AppendOutcome::Committed(_)
            ));
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_phase_barrier_admits_before_io_and_flushes_append_and_guarded_commit()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let meter = Arc::new(Meter::local());
    let path = StreamPath::new("native-phase")?;
    meter.deny_durability.store(true, Ordering::SeqCst);
    drop(
        provider
            .native_durability_barrier(meter.original())
            .await??,
    );
    assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
    meter.deny_durability.store(false, Ordering::SeqCst);
    deferring_durability(async {
        let output = provider
            .native_append(keyed_append("native-phase", 0)?, meter.original())
            .await??;
        drop(output);
        let request = CommitRequest {
            conditions: vec![crate::CommitCondition::Tail {
                path: path.clone(),
                expected: 1,
            }],
            mutations: vec![crate::CommitMutation::Append {
                path: path.clone(),
                records: vec![OwnedBytes::from_static(b"committed").into()],
            }],
            idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"native-phase-commit"))?,
        };
        let output = provider
            .native_commit_guarded::<NativeGuardFailure, _, _>(
                request,
                None,
                meter.original(),
                || async { Ok(()) },
            )
            .await??;
        drop(output);
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await?;
    assert_eq!(provider.watermarks(path.clone()).await?.durable, 0);
    assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
    meter.deny_durability.store(true, Ordering::SeqCst);
    let error = provider
        .native_durability_barrier(meter.original())
        .await
        .err()
        .ok_or("unfunded durability barrier succeeded")?;
    assert_original_denial(&error, "durability");
    assert_eq!(provider.watermarks(path.clone()).await?.durable, 0);
    meter.deny_durability.store(false, Ordering::SeqCst);
    drop(
        provider
            .native_durability_barrier(meter.original())
            .await??,
    );
    assert_eq!(provider.watermarks(path).await?.durable, 2);
    assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 1);
    meter.assert_refunded();
    Ok(())
}
