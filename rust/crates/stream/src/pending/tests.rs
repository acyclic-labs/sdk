use super::*;
use crate::{
    BoxProviderFuture, CommitCondition, CommitMutation, IdempotencyKey, MemoryStream, Stream,
};
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
    assert_eq!(retained.clone().await?, outcome);
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
