use super::*;
use crate::guest::{CancellationToken, GuestError};
use std::sync::atomic::AtomicUsize;

/// Fails its first `refusals` attempts with `error`, then delivers normally.
struct RefusedRecord {
    package: Package,
    error: GuestError,
    refusals: usize,
    attempts: AtomicUsize,
    failures: AtomicUsize,
}
impl Handler for RefusedRecord {
    fn manifest(&self) -> &Manifest {
        self.package.manifest()
    }
    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) < self.refusals {
            return Box::pin(async { Err(self.error.clone()) });
        }
        self.package.on_record(delivery, checkpoint, cancellation)
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

/// Every failure settles its input within the one delivery: a decided one
/// through the failure transition, an undecided one by redelivering at once,
/// and one undecided again on redelivery through the failure transition.
/// No failure leaves the actor idle with its input waiting for an event that
/// nothing owes it (GC-263: a turn idled with a prepared step).
#[tokio::test]
async fn handler_failure_never_idles_with_its_input() -> TestResult {
    let pending = GuestError::Pending("unknown external acknowledgement".into())
        .with_context("handler delivery");
    let decided =
        GuestError::Handler("definitive handler failure".into()).with_context("handler delivery");
    // (error, refusals, attempts, failure transitions, work steps per batch);
    // a one-step work budget yields between the attempt and its redelivery.
    for (error, refusals, attempts, failures, work) in [
        (decided, usize::MAX, 1, 1, 64),
        (pending.clone(), 1, 2, 0, 64),
        (pending.clone(), usize::MAX, 2, 1, 64),
        (pending, usize::MAX, 2, 1, 1),
    ] {
        let provider = Arc::new(MemoryStream::default());
        let store = Store::new(Arc::clone(&provider), root()?);
        let address = spawn_with(&store, "handler-failure", &[b"emit"]).await?;
        let handler = Arc::new(RefusedRecord {
            package: Package::new()?,
            error,
            refusals,
            attempts: AtomicUsize::new(0),
            failures: AtomicUsize::new(0),
        });
        let runtime = Runtime::new(
            crate::guest::Requests::default(),
            store,
            handler.clone(),
            Arc::new(SystemClock),
        )
        .with_limits(RuntimeLimits::new(1, work)?);
        runtime.track(address.clone())?;
        let mut unavailable = runtime.unavailable(&address)?;
        runtime.drain_actor(&address).await?;
        assert!(
            futures::poll!(unavailable.as_mut()).is_pending(),
            "a settled failure made its actor unavailable"
        );
        let snapshot = runtime.store().load(&address).await?;
        let state = snapshot.state().ok_or("missing actor state")?;
        assert_eq!(state.cursor("inbox"), Some(1));
        assert_eq!(handler.attempts.load(Ordering::SeqCst), attempts);
        assert_eq!(handler.failures.load(Ordering::SeqCst), failures);
        assert_eq!(
            records(&*provider, "out/handler-failure").await?,
            vec![Bytes::from_static(b"r0")]
        );
    }
    Ok(())
}
