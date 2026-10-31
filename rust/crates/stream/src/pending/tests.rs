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
    fn sleep(&self, _: u32) -> BoxProviderFuture<'static, ()> {
        Box::pin(futures::future::pending())
    }
}

#[tokio::test]
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
async fn native_custody_keeps_exact_results_and_terminates_on_producer_loss()
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
        prepared = Some(NativePending::prepare_native(
            Arc::clone(&provider),
            funded,
            meter.original(),
        ));
    });
    let (pending, producer) = prepared.unwrap()?;
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
    drop(cloned);
    drop(pending);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();

    // Producer loss before first poll must terminate both registered waiters.
    // Cancelling the selected waiter must pass its event to the next waiter.
    let meter = Arc::new(Meter::default());
    let (pending, producer) = NativePending::prepare_native(
        provider,
        request.try_clone_native(&meter.original())?,
        meter.original(),
    )?;
    let mut first = Box::pin(pending.result());
    let mut second = Box::pin(pending.result());
    let mut context = Context::from_waker(std::task::Waker::noop());
    assert!(first.as_mut().poll(&mut context).is_pending());
    assert!(second.as_mut().poll(&mut context).is_pending());
    drop(producer);
    drop(first);
    assert!(matches!(
        second.as_mut().poll(&mut context),
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
    Ok(())
}
