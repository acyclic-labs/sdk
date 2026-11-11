use super::*;
use futures::{FutureExt as _, StreamExt as _};

#[tokio::test]
async fn live_native_follows_keep_original_credit_across_creation_and_appends()
-> Result<(), Box<dyn std::error::Error>> {
    for local in [false, true] {
        let directory = tempfile::tempdir()?;
        let provider: Arc<dyn crate::NativeStreamProvider> = if local {
            Arc::new(LocalStream::open(directory.path(), LocalStreamLimits::default()).await?)
        } else {
            Arc::new(MemoryStream::default())
        };
        let path = StreamPath::new("native/live")?;
        let meter = Arc::new(Meter {
            read_limit: 4,
            ..Meter::default()
        });
        let mut request = crate::FollowRequest::new(path.clone(), 0);
        request.when_absent = crate::WhenAbsent::Wait;
        let mut output = provider.native_follow(request, meter.original()).await??;
        assert!(output.value_mut().next().now_or_never().is_none());
        for sequence in 0..3 {
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![OwnedBytes::from_static(b"aa").into()],
                    if_tail: Some(sequence),
                    idempotency_key: IdempotencyKey::unique(),
                })
                .await?;
            let next = output
                .value_mut()
                .next()
                .await
                .ok_or("missing native delivery")?;
            if sequence < 2 {
                let record = next??;
                assert_eq!(record.value().sequence, sequence);
                assert_eq!(record.value().value.as_ref(), b"aa");
                drop(record);
                assert!(output.value_mut().next().now_or_never().is_none());
            } else {
                assert!(
                    matches!(next, Err(error) if error.kind() == std::io::ErrorKind::OutOfMemory)
                );
                assert!(output.value_mut().next().await.is_none());
            }
        }
        assert_eq!(meter.read.load(Ordering::SeqCst), 4);
        drop(output);
        meter.assert_refunded();
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
async fn native_follow_stop_destroys_pending_acquisition_before_returning()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let path = StreamPath::new("native/pending")?;
    let meter = Arc::new(Meter::default());
    let mut request = crate::FollowRequest::new(path, 0);
    request.when_absent = crate::WhenAbsent::Wait;
    let output = provider.native_follow(request, meter.original()).await??;
    let held = provider.inner.publication.write().await;
    let owner = acyclic_native_runtime::retain_future(
        async move {
            let mut output = output;
            let _ = output.value_mut().next().await;
        },
        &meter.original(),
    )?;
    owner.stop()?;
    assert!(owner.is_stopped());
    drop(owner);
    meter.assert_refunded();
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    drop(held);
    assert!(provider.inner.publication.try_write().is_ok());
    Ok(())
}
