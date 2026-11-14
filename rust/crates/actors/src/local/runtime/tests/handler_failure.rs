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
async fn closed_admission_reports_unavailability_without_consuming_the_original_input() -> TestResult
{
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = spawn_with(&store, "closed-admission", &[b"emit"]).await?;
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        store,
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    )
    .with_limits(RuntimeLimits::new(1, 1)?);
    runtime.admission[crate::guest::AdmissionClass::Work.index()]
        .as_ref()
        .ok_or("configured admission missing")?
        .close();
    runtime.track(address.clone())?;
    let mut unavailable = runtime.unavailable(&address)?;
    assert!(matches!(
        runtime.drain_actor(&address).await,
        Err(Error::AdmissionClosed(_))
    ));
    assert!(
        matches!(futures::poll!(unavailable.as_mut()), std::task::Poll::Ready(cause)
            if cause.to_string().contains("runtime admission is closed")),
        "closed admission did not publish its original unavailability"
    );
    assert_eq!(
        runtime
            .store()
            .load(&address)
            .await?
            .state()
            .ok_or("missing actor state")?
            .cursor("inbox"),
        Some(0)
    );
    assert!(
        records(&*provider, "out/closed-admission")
            .await?
            .is_empty()
    );
    drop(unavailable);
    drop(runtime);

    // A replacement admission owner can still execute that exact input once.
    let recovered = Runtime::new(
        crate::guest::Requests::default(),
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    )
    .with_limits(RuntimeLimits::new(1, 1)?);
    recovered.track(address.clone())?;
    recovered.drain_actor(&address).await?;
    assert_eq!(
        records(&*provider, "out/closed-admission").await?,
        vec![Bytes::from_static(b"r0")]
    );
    Ok(())
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
        let mut unavailable = runtime.unavailable(&address)?;
        let result = runtime.drain_actor(&address).await;
        // Retained (pending) work is retried by the actor's next event, and
        // settled work published its failure: neither leaves it unavailable.
        assert!(
            futures::poll!(unavailable.as_mut()).is_pending(),
            "a decided or retryable failure made its actor unavailable"
        );
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
