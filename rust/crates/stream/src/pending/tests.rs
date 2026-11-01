use super::*;
use crate::BoxProviderFuture;
use crate::{CommitCondition, CommitMutation, IdempotencyKey, MemoryStream, Stream};
use futures::FutureExt as _;
use std::sync::Mutex;

#[derive(Default)]
struct Executor(Mutex<Vec<BoxProviderFuture<'static, ()>>>);
impl crate::runtime::Executor for Executor {
    fn spawn(&self, future: BoxProviderFuture<'static, ()>) {
        self.0.lock().unwrap().push(future);
    }
    fn sleep(&self, _: u64) -> BoxProviderFuture<'static, Result<(), crate::runtime::SleepError>> {
        Box::pin(futures::future::pending())
    }
}

async fn cancellation_keeps_exact_submission_and_shared_outcome() -> Result<(), StreamError> {
    let provider = Arc::new(MemoryStream::default());
    let stream = Stream::new(Arc::clone(&provider), "pending")?;
    let path = stream.path().clone();
    let request = CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Absent {
            path: stream.path().clone(),
        }],
        mutations: vec![CommitMutation::Append {
            path: stream.path().clone(),
            records: vec!["original".into()],
        }],
        idempotency_key: IdempotencyKey::unique(),
    };
    let executor = Executor::default();
    let pending = Pending::submit(Arc::clone(&provider), request.clone(), &executor);
    let retained = pending.clone();
    assert!(pending.wait().now_or_never().is_none());
    drop(pending);
    drop(stream);
    assert_eq!(provider.head(path).await, Err(StreamError::NotFound));
    let owned = executor.0.lock().unwrap().pop().unwrap();
    owned.await;
    let outcome = retained.wait().await?;
    assert!(matches!(outcome, CommitOutcome::Committed(_)));
    assert_eq!(retained.clone().wait().await?, outcome);
    assert_eq!(provider.commit(request.clone()).await?, outcome);
    let mut changed = request;
    let CommitMutation::Append { records, .. } = &mut changed.mutations[0] else {
        unreachable!()
    };
    *records = vec!["changed".into()];
    assert_eq!(
        provider.commit(changed).await,
        Err(StreamError::IdempotencyMismatch)
    );

    // Even losing every waiter cannot revoke custody already submitted.
    let request = CommitRequest {
        conditions: Vec::new(),
        idempotency_key: IdempotencyKey::unique(),
        ..retained.request().clone()
    };
    drop(Pending::submit(
        Arc::clone(&provider),
        request.clone(),
        &executor,
    ));
    let owned = executor.0.lock().unwrap().pop().unwrap();
    owned.await;
    assert!(
        provider
            .inspect_idempotency(request.idempotency_key)
            .await?
            .is_some()
    );
    Ok(())
}

#[tokio::test]
async fn commit_custody_keeps_exact_results_and_terminates_on_producer_loss()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::{NativePending, StreamPath, test_meter::Meter};
    use std::{future::Future, sync::atomic::Ordering, task::Context};

    let provider = Arc::new(MemoryStream::default());
    let path = StreamPath::new("pending/native")?;
    let request = CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Absent { path: path.clone() }],
        mutations: vec![CommitMutation::Append {
            path: path.clone(),
            records: vec!["original native result".into()],
        }],
        idempotency_key: IdempotencyKey::unique(),
    };
    let meter = Arc::new(Meter::default());
    let funded = request.try_clone_native(&meter.original())?;
    let before = meter.held.load(Ordering::SeqCst);
    let allocations = meter.allocations.load(Ordering::SeqCst);
    let mut prepared = None;
    let physical = acyclic_test_support::measure(|| {
        let (request, storage) = funded.into_parts();
        prepared = Some(NativePending::prepare_native(
            Arc::clone(&provider),
            request,
            storage,
            meter.original(),
        ));
    });
    let (pending, producer) = prepared.unwrap()?;
    let pending = match pending.try_take() {
        Err(pending) => pending,
        Ok(_) => panic!("a live producer lost its original request"),
    };
    assert_eq!(pending.request(), &request);
    assert_eq!(
        physical.bytes_current,
        (meter.held.load(Ordering::SeqCst) - before) as i64
    );
    assert_eq!(
        physical.count_current,
        (meter.allocations.load(Ordering::SeqCst) - allocations) as i64
    );
    assert!(pending.result().now_or_never().is_none());
    assert_eq!(
        provider.head(path.clone()).await,
        Err(StreamError::NotFound)
    );
    producer.await??;
    let retained = pending.result().await?;
    let outcome = retained.as_ref().unwrap().as_ref().unwrap().value();
    assert!(matches!(outcome, CommitOutcome::Committed(_)));
    let cloned = pending.clone();
    assert!(std::ptr::eq(retained, cloned.result().await?));
    assert_eq!(&provider.commit(request.clone()).await?, outcome);
    let pending = match pending.try_take() {
        Err(pending) => pending,
        Ok(_) => panic!("another observer lost its original result"),
    };
    drop(cloned);
    let (original, result, storage) = match pending.try_take() {
        Ok(owned) => owned,
        Err(_) => panic!("the sole completed owner could not take its result"),
    };
    assert_eq!(original, request);
    assert!(matches!(result, Some(Ok(Ok(_)))));
    drop((original, result, storage));
    // Committed history still physically belongs to the provider. Its native
    // storage must stay funded until that original owner retires as well.
    assert!(meter.held.load(Ordering::SeqCst) > 0);
    drop(provider);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();

    // Producer loss before first poll must terminate both registered waiters.
    // Cancelling the selected waiter must pass its event to the next waiter.
    let meter = Arc::new(Meter::default());
    let (retained, storage) = request.try_clone_native(&meter.original())?.into_parts();
    let (pending, producer) = NativePending::prepare_native(
        Arc::new(MemoryStream::default()),
        retained,
        storage,
        meter.original(),
    )?;
    let mut first = Box::pin(pending.result());
    let mut second = Box::pin(pending.result());
    struct Wake(std::sync::atomic::AtomicUsize);
    impl futures::task::ArcWake for Wake {
        fn wake_by_ref(this: &Arc<Self>) {
            this.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let first_wake = Arc::new(Wake(std::sync::atomic::AtomicUsize::new(0)));
    let second_wake = Arc::new(Wake(std::sync::atomic::AtomicUsize::new(0)));
    let first_waker = futures::task::waker(first_wake.clone());
    let second_waker = futures::task::waker(second_wake.clone());
    let mut first_context = Context::from_waker(&first_waker);
    let mut second_context = Context::from_waker(&second_waker);
    assert!(first.as_mut().poll(&mut first_context).is_pending());
    assert!(second.as_mut().poll(&mut second_context).is_pending());
    drop(producer);
    assert_eq!(first_wake.0.load(Ordering::SeqCst), 1);
    assert_eq!(second_wake.0.load(Ordering::SeqCst), 0);
    drop(first);
    assert_eq!(second_wake.0.load(Ordering::SeqCst), 1);
    assert!(matches!(
        second.as_mut().poll(&mut second_context),
        std::task::Poll::Ready(Err(StreamError::Unavailable))
    ));
    drop(second);
    assert!(matches!(
        pending.result().now_or_never(),
        Some(Err(StreamError::Unavailable))
    ));
    drop(pending);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();

    for allowed in 0..physical.count_current as u64 {
        let meter = Arc::new(Meter::default());
        let funded = request.try_clone_native(&meter.original())?;
        let (retained, storage) = funded.into_parts();
        meter.limit.store(
            meter.allocations.load(Ordering::SeqCst) + allowed,
            Ordering::SeqCst,
        );
        assert!(
            NativePending::prepare_native(
                Arc::new(MemoryStream::default()),
                retained,
                storage,
                meter.original(),
            )
            .is_err()
        );
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
    }

    // Original native failure is retained, not normalized to a portable error
    // or reconstructed by each observer. Admission succeeds before cancellation.
    let meter = Arc::new(Meter::default());
    let (retained, storage) = request.try_clone_native(&meter.original())?.into_parts();
    let (pending, producer) = NativePending::prepare_native(
        Arc::new(MemoryStream::default()),
        retained,
        storage,
        meter.original(),
    )?;
    meter.cancelled.store(true, Ordering::SeqCst);
    producer.await??;
    let result = pending.result().await?;
    let error = result.as_ref().err().expect("native cancellation lost");
    assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
    assert!(error.get_ref().is_some_and(|cause| {
        cause
            .downcast_ref::<crate::test_meter::NativeDenial>()
            .is_some_and(|cause| cause.0 == "cancelled")
    }));
    let observer = pending.clone();
    assert!(std::ptr::eq(result, observer.result().await?));
    drop(observer);
    drop(pending);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    cancellation_keeps_exact_submission_and_shared_outcome().await?;
    Ok(())
}
