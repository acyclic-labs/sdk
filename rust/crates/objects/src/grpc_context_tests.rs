//! Real HTTP/2 controls for the original caller budget; no readiness sleeps.
use super::*;
use acyclic_auth::operation::{Context, Stopped};
use futures::{
    FutureExt,
    channel::oneshot,
    future::{pending, ready},
};
use std::sync::atomic::{AtomicUsize, Ordering};

fn future_deadline() -> u64 {
    (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        + Duration::from_secs(120))
    .as_millis()
    .try_into()
    .unwrap()
}

#[tokio::test]
async fn original_context_stops_refresh_and_remains_stopped_across_clones() -> TestResult {
    for completion in ["deadline", "cancel", "producer-loss"] {
        let replica = Replica::new().await?;
        let mut objects = client(&replica);
        let (entered, started) = oneshot::channel();
        let entered = Arc::new(Mutex::new(Some(entered)));
        let (release, released) = oneshot::channel::<()>();
        let released = Arc::new(Mutex::new(Some(released)));
        objects.credential = CredentialProvider::new(move || {
            let entered = entered.lock().unwrap().take().unwrap();
            let released = released.lock().unwrap().take().unwrap();
            async move {
                entered.send(()).unwrap();
                let _ = released.await;
                Ok("fixture".into())
            }
        });
        let (expire, expired) = oneshot::channel();
        let (cancel, cancelled) = oneshot::channel();
        let bound = objects.clone().with_context(Context::new(
            future_deadline(),
            async {
                let _ = expired.await;
            },
            async {
                let _ = cancelled.await;
            },
        ));
        let mut put = Box::pin(bound.put(b"held".as_slice().into(), Vec::new()));
        futures::select! {
            result = put.as_mut().fuse() => panic!("refresh still held: {result:?}"),
            _ = started.fuse() => {},
        }
        let expected = match completion {
            "deadline" => {
                expire.send(()).unwrap();
                Stopped::Deadline
            }
            "cancel" => {
                cancel.send(()).unwrap();
                Stopped::Cancelled
            }
            _ => {
                drop(cancel);
                Stopped::Cancelled
            }
        };
        assert_eq!(
            put.now_or_never()
                .expect("original stop must settle immediately")
                .unwrap_err()
                .stopped,
            Some(expected)
        );
        assert!(release.is_canceled(), "stopping drops the refresh interest");
        assert_eq!(
            bound.clone().usage().await.unwrap_err().stopped,
            Some(expected)
        );
        assert!(
            replica.pending.lock().unwrap().is_empty(),
            "zero RPCs before refresh"
        );
        // A new operation on the same channel retains ordinary progress.
        objects.credential = CredentialProvider::new(|| ready(Ok("fixture".into())));
        objects.put(b"ready".as_slice().into(), Vec::new()).await?;
        assert_eq!(replica.pending.lock().unwrap().len(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn original_budget_covers_rpc_wait_and_has_no_hidden_relative_ceiling() -> TestResult {
    let replica = Replica::new().await?;
    let mut objects = client(&replica);
    let (digest, committed) = written(&objects, b"not applied").await?;
    let (entered, started) = oneshot::channel();
    let entered = Arc::new(Mutex::new(Some(entered)));
    let resolve: AccountResolver = {
        let replica = Arc::clone(&replica);
        Arc::new(move |metadata| {
            let remaining =
                grpc_timeout(metadata.get("grpc-timeout").unwrap().to_str().unwrap()).unwrap();
            assert!(
                remaining > Duration::from_secs(60),
                "the 30-second default cannot cap an explicit budget"
            );
            entered.lock().unwrap().take().unwrap().send(()).unwrap();
            Ok(Arc::clone(&replica) as Arc<dyn Blobs>)
        })
    };
    objects.channel = Endpoint::from_static("http://fixture.invalid").connect_with_connector_lazy(
        tower::service_fn(move |_| {
            let resolve = Arc::clone(&resolve);
            async move {
                let (client, server) = tokio::io::duplex(64 * 1024);
                tokio::spawn(async move {
                    let incoming = futures::stream::once(async { Ok::<_, std::io::Error>(server) });
                    tonic::transport::Server::builder()
                        .add_service(BlobsServer::new(resolve).into_service())
                        .serve_with_incoming(incoming)
                        .await
                        .unwrap();
                });
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(client))
            }
        }),
    );
    let (cancel, cancelled) = oneshot::channel::<()>();
    let bound = objects
        .clone()
        .with_context(Context::new(future_deadline(), pending(), async {
            let _ = cancelled.await;
        }));
    let mut read = Box::pin(bound.get(digest, ByteRange::FULL, 64));
    futures::select! {
        result = read.as_mut().fuse() => panic!("replica has not applied: {result:?}"),
        _ = started.fuse() => {},
    }
    assert!(read.as_mut().now_or_never().is_none());
    drop(cancel);
    assert_eq!(
        read.now_or_never()
            .expect("producer loss must settle the RPC wait")
            .unwrap_err()
            .stopped,
        Some(Stopped::Cancelled)
    );
    replica.catch_up().await?;
    assert!(replica.applied.current().index >= committed.index);
    assert_eq!(
        client(&replica)
            .get(digest, ByteRange::FULL, 64)
            .await?
            .body()
            .as_ref(),
        b"not applied"
    );
    Ok(())
}

#[tokio::test]
async fn stopped_context_admits_zero_callbacks_for_any_concurrent_cohort() -> TestResult {
    let replica = Replica::new().await?;
    let mut objects = client(&replica);
    let callbacks = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&callbacks);
    objects.credential = CredentialProvider::new(move || {
        counted.fetch_add(1, Ordering::SeqCst);
        ready(Ok("fixture".into()))
    });
    let stopped = objects
        .clone()
        .with_context(Context::new(1, ready(()), pending()));
    for count in [1, 100, 1000] {
        let results = futures::future::join_all((0..count).map(|_| stopped.usage())).await;
        assert!(
            results
                .iter()
                .all(|result| result.as_ref().unwrap_err().stopped == Some(Stopped::Deadline))
        );
        assert_eq!(callbacks.load(Ordering::SeqCst), 0);
    }
    #[cfg(feature = "test-support")]
    {
        let body: OwnedBytes = b"stopped-before-sealing".as_slice().into();
        let before = acyclic_stream::hashed_on_this_thread();
        assert_eq!(
            stopped
                .put(body.clone(), Vec::new())
                .await
                .unwrap_err()
                .stopped,
            Some(Stopped::Deadline)
        );
        assert_eq!(acyclic_stream::hashed_on_this_thread(), before);
        let sealed = crate::Sealed::new(body);
        assert!(acyclic_stream::hashed_on_this_thread() > before);
        let before = acyclic_stream::hashed_on_this_thread();
        assert_eq!(
            stopped
                .put_sealed(sealed, Vec::new())
                .await
                .unwrap_err()
                .stopped,
            Some(Stopped::Deadline)
        );
        assert_eq!(acyclic_stream::hashed_on_this_thread(), before);
        assert_eq!(callbacks.load(Ordering::SeqCst), 0);
    }
    // A deadline beyond the wire's eight-digit-hours range refuses before
    // callbacks rather than panicking in metadata encoding or silently capping.
    assert_eq!(
        objects
            .clone()
            .with_context(Context::new(u64::MAX, pending(), pending()))
            .usage()
            .await
            .unwrap_err()
            .code,
        wire::ErrorCode::InvalidArgument
    );
    assert_eq!(callbacks.load(Ordering::SeqCst), 0);
    objects.usage().await?;
    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
    Ok(())
}
