use super::*;
use crate::guest::{CancellationToken, GuestError};
use std::sync::atomic::AtomicUsize;

struct RefusedRecord {
    package: Package,
    error: GuestError,
    failures: AtomicUsize,
}
impl Handler for RefusedRecord {
    fn manifest(&self) -> &Manifest {
        self.package.manifest()
    }
    fn on_record<'a>(
        &'a self,
        _: Delivery,
        _: Option<&'a Checkpoint>,
        _: CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async { Err(self.error.clone()) })
    }
    fn on_failure<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        _: String,
    ) -> GuestFuture<'a, Option<Transition>> {
        self.failures.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            self.package
                .on_record(delivery, checkpoint, CancellationToken::new())
                .await
                .map(Some)
        })
    }
}

#[tokio::test]
async fn handler_failure_transition_consumes_only_decided_work() -> TestResult {
    for pending in [false, true] {
        let provider = Arc::new(MemoryStream::default());
        let store = Store::new(Arc::clone(&provider), root()?);
        let address = spawn_with(&store, "handler-failure", &[b"emit"]).await?;
        let error = (if pending {
            GuestError::Pending("unknown external acknowledgement".into())
        } else {
            GuestError::Handler("definitive handler failure".into())
        })
        .with_context("handler delivery");
        let handler = Arc::new(RefusedRecord {
            package: Package::new()?,
            error: error.clone(),
            failures: AtomicUsize::new(0),
        });
        let runtime = Runtime::new(
            crate::guest::Requests::default(),
            store,
            handler.clone(),
            Arc::new(SystemClock),
        );
        runtime.track(address.clone())?;
        let result = runtime.drain_actor(&address).await;
        let snapshot = runtime.store().load(&address).await?;
        let state = snapshot.state().ok_or("missing actor state")?;
        if pending {
            assert!(matches!(result, Err(Error::Handler(ref source)) if source == &error));
            assert_eq!(state.cursor("inbox"), Some(0));
            assert_eq!(handler.failures.load(Ordering::SeqCst), 0);
            assert!(records(&*provider, "out/handler-failure").await?.is_empty());
        } else {
            result?;
            assert_eq!(state.cursor("inbox"), Some(1));
            assert_eq!(handler.failures.load(Ordering::SeqCst), 1);
            assert_eq!(
                records(&*provider, "out/handler-failure").await?,
                vec![Bytes::from_static(b"r0")]
            );
        }
    }
    Ok(())
}
