//! Real HTTP/2 controls for the original caller budget; no readiness sleeps.
use super::*;
use acyclic_auth::operation::{Context, Stopped};
use futures::{
    FutureExt,
    channel::oneshot,
    future::{pending, ready},
};
use std::sync::atomic::{AtomicUsize, Ordering};

struct OriginalClock(Mutex<Result<u64, Stopped>>);

impl acyclic_auth::operation::Clock for OriginalClock {
    fn now_unix_millis(&self) -> Result<u64, Stopped> {
        *self.0.lock().unwrap()
    }
    fn expires_at(&self, _: u64) -> Result<acyclic_auth::operation::StopFuture, Stopped> {
        panic!("transport projection must not register a new timer")
    }
}

fn hold_credentials(objects: &mut GrpcObjects) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (entered, started) = oneshot::channel();
    let (release, released) = oneshot::channel::<()>();
    let held = Arc::new(Mutex::new(Some((entered, released))));
    objects.credential = CredentialProvider::new(move || {
        let (entered, released) = held.lock().unwrap().take().unwrap();
        async move {
            entered.send(()).unwrap();
            let _ = released.await;
            Ok("fixture".into())
        }
    });
    (started, release)
}
#[tokio::test]
async fn original_context_stops_refresh_and_remains_stopped_across_clones() -> TestResult {
    for completion in ["deadline", "cancel", "producer-loss"] {
        let replica = Replica::new().await?;
        let mut objects = client(&replica);
        let (started, release) = hold_credentials(&mut objects);
        let (expire, expired) = oneshot::channel();
        let (cancel, cancelled) = oneshot::channel();
        let bound = objects.clone().with_context(Context::new(
            u64::MAX,
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
    let mut objects = client(&replica).with_transfer_timeout(Duration::from_secs(30))?;
    let (digest, committed) = written(&objects, b"not applied").await?;
    let requested = replica.next_request();
    let (started, release) = hold_credentials(&mut objects);
    let clock = Arc::new(OriginalClock(Mutex::new(Ok(1_000))));
    let (cancel, cancelled) = oneshot::channel::<()>();
    let bound = objects.clone().with_context(Context::from_clock(
        121_000,
        async {
            let _ = cancelled.await;
            Stopped::Cancelled
        },
        clock.clone(),
    ));
    let mut read = Box::pin(bound.get(digest, ByteRange::FULL, 64));
    futures::select! {
        result = read.as_mut().fuse() => panic!("credentials still held: {result:?}"),
        _ = started.fuse() => {},
    }
    *clock.0.lock().unwrap() = Ok(11_000);
    release.send(()).unwrap();
    let metadata = futures::select! {
        result = read.as_mut().fuse() => panic!("replica has not applied: {result:?}"),
        metadata = requested.fuse() => metadata?,
    };
    let remaining = grpc_timeout(metadata.get("grpc-timeout").unwrap().to_str().unwrap()).unwrap();
    assert_eq!(
        remaining,
        Duration::from_secs(110),
        "use the original clock after credentials, without the relative transfer ceiling"
    );
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
async fn original_clock_refuses_expired_lost_and_unrepresentable_dispatch() -> TestResult {
    for mode in ["expired", "lost", "wire-range"] {
        let replica = Replica::new().await?;
        let mut objects = client(&replica);
        let requested = replica.next_request();
        let (started, release) = hold_credentials(&mut objects);
        let clock = Arc::new(OriginalClock(Mutex::new(Ok(1_000))));
        let deadline = if mode == "wire-range" {
            u64::MAX
        } else {
            121_000
        };
        let bound = objects.with_context(Context::from_clock(deadline, pending(), clock.clone()));
        let mut put = Box::pin(bound.put(b"not dispatched".as_slice().into(), Vec::new()));
        futures::select! {
            result = put.as_mut().fuse() => panic!("credentials still held: {result:?}"),
            _ = started.fuse() => {},
        }
        *clock.0.lock().unwrap() = match mode {
            "expired" => Ok(deadline),
            "lost" => Err(Stopped::Cancelled),
            _ => Ok(1_000),
        };
        release.send(()).unwrap();
        let error = put.await.unwrap_err();
        match mode {
            "expired" => assert_eq!(error.stopped, Some(Stopped::Deadline)),
            "lost" => assert_eq!(error.stopped, Some(Stopped::Cancelled)),
            _ => assert_eq!(error.code, wire::ErrorCode::InvalidArgument),
        }
        assert!(
            requested.now_or_never().is_none(),
            "no server request before original-clock admission"
        );
        assert!(
            replica.pending.lock().unwrap().is_empty(),
            "refused dispatch published no mutation"
        );
    }
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
    let requested = replica.next_request();
    objects
        .with_context(Context::new(u64::MAX, pending(), pending()))
        .usage()
        .await?;
    assert!(!requested.await?.contains_key("grpc-timeout"));
    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
    Ok(())
}
