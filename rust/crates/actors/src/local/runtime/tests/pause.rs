use super::*;
use acyclic_stream::{
    CommitId, CommittedEnvelope, FollowRequest, FollowStream, Heads, HeadsRequest,
    IdempotencyObservation, ProviderContext, ReadRequest, WriteOutcome, WriteRequest,
};
use futures::channel::oneshot;
use std::sync::Mutex;

#[derive(Clone, Copy)]
enum Fault {
    Load,
    Refuse,
    Unknown,
}

struct Cut {
    fault: Fault,
    entered: oneshot::Sender<()>,
    released: oneshot::Receiver<()>,
}

#[derive(Default)]
struct Provider {
    inner: MemoryStream,
    cut: Mutex<Option<Cut>>,
}

impl Provider {
    fn arm(&self, fault: Fault) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, observing) = oneshot::channel();
        let (release, released) = oneshot::channel();
        *self.cut.lock().expect("cut lock") = Some(Cut {
            fault,
            entered,
            released,
        });
        (observing, release)
    }

    fn take(&self, load: bool) -> Option<Cut> {
        let mut cut = self.cut.lock().expect("cut lock");
        if cut
            .as_ref()
            .is_some_and(|cut| matches!(cut.fault, Fault::Load) == load)
        {
            cut.take()
        } else {
            None
        }
    }
}

#[async_trait::async_trait]
impl StreamProvider for Provider {
    fn context(&self) -> ProviderContext<'_> {
        self.inner.context()
    }

    async fn heads(&self, request: HeadsRequest) -> Result<Heads, StreamError> {
        if let Some(cut) = self.take(true) {
            let _ = cut.entered.send(());
            cut.released.await.map_err(|_| StreamError::Unavailable)?;
            return Err(StreamError::AccessDenied);
        }
        self.inner.heads(request).await
    }

    fn write(
        &self,
        request: WriteRequest,
    ) -> BoxProviderFuture<'_, Result<WriteOutcome, StreamError>> {
        Box::pin(async move {
            if let Some(cut) = self.take(false) {
                let _ = cut.entered.send(());
                cut.released.await.map_err(|_| StreamError::Unavailable)?;
                return match cut.fault {
                    Fault::Unknown => {
                        self.inner.write(request).await?;
                        Err(StreamError::Unavailable)
                    }
                    Fault::Refuse => Err(StreamError::AccessDenied),
                    Fault::Load => unreachable!("load cut taken by write"),
                };
            }
            self.inner.write(request).await
        })
    }

    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.inner.read(request).await
    }
    async fn follow(&self, request: FollowRequest) -> Result<FollowStream, StreamError> {
        self.inner.follow(request).await
    }
    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }
}

/// Exercise the original control/delivery locks, real MemoryStream decisions,
/// and resource completion channels. Every held wait has its producer here.
#[tokio::test]
async fn pause_retains_decision_cleanup_and_unknown_after_waiter_loss() -> TestResult {
    active_cleanup_and_queued_controls().await?;
    completed_unread_failure().await?;
    replacement_and_producer_loss().await?;
    for fault in [Fault::Load, Fault::Refuse, Fault::Unknown] {
        for abandon in [false, true] {
            publication_cut(fault, abandon).await?;
        }
    }
    Ok(())
}

async fn completed_unread_failure() -> TestResult {
    let provider = Arc::new(Provider::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = spawn_with(&store, "pause-unread", &[b"emit"]).await?;
    let runtime = Runtime::new(store, Arc::new(Package::new()?), Arc::new(SystemClock));
    runtime.track(address.clone())?;
    let (entered, release) = provider.arm(Fault::Unknown);
    release.send(()).map_err(|_| "lost original provider cut")?;
    // The real MemoryStream decision and failed reply are ready in the
    // executor's inline poll. Its completion is queued before this future is
    // polled, reproducing disconnect after send but before receive.
    let completion = super::super::pause::start(
        Store::new(Arc::clone(&provider), root()?),
        address.clone(),
        runtime.pin(&address)?.ok_or("missing original placement")?,
        true,
        std::future::ready(()),
    );
    entered.await?;
    let slot = runtime.slot(&address).ok_or("missing original placement")?;
    assert!(
        slot.inner.lock().await.control_failure.is_some(),
        "unread completed pause lost its original Unknown"
    );
    assert!(!runtime.release(&address));
    drop(completion);
    let Err(Error::Unknown {
        publication,
        source: StreamError::Unavailable,
    }) = runtime.pause(&address, false).await
    else {
        return Err("unread completed pause lost its original Unknown".into());
    };
    let before = runtime.store.load(&address).await?;
    runtime.store.submit(*publication).await?;
    assert_eq!(runtime.store.load(&address).await?.anchor, before.anchor);
    runtime.pause(&address, false).await?;
    runtime.drain_actor(&address).await?;
    assert_eq!(records(&*provider, "out/pause-unread").await?.len(), 1);
    Ok(())
}

async fn active_cleanup_and_queued_controls() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = spawn_with(&store, "pause-owner", &[b"emit"]).await?;
    let unrelated = spawn_with(&store, "pause-unrelated", &[b"emit"]).await?;
    let handler = Arc::new(Gated {
        package: Package::new()?,
        active: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
        gate: tokio::sync::Semaphore::new(0),
    });
    let runtime = Runtime::new(store, handler.clone(), Arc::new(SystemClock));
    runtime.track(address.clone())?;
    runtime.track(unrelated.clone())?;
    let mut delivery = Box::pin(runtime.drain_actor(&address));
    assert!(futures::poll!(delivery.as_mut()).is_pending());
    assert_eq!(handler.active.load(Ordering::SeqCst), 1);
    let (entered, parked) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let transition = async move {
        let _ = entered.send(());
        released.await
    };
    let mut pause = Box::pin(runtime.pause_with(&address, true, transition));
    assert!(futures::poll!(pause.as_mut()).is_pending());
    drop(pause); // Original bug: this discarded the only control owner.
    assert!(!runtime.release(&address));
    handler.gate.add_permits(1);
    let (delivered, entered) = futures::join!(delivery, parked);
    // The cancelled handler may fail or return its original transition; pause
    // must await its actual cleanup in either case.
    let _delivery_outcome = delivered;
    entered.map_err(|_| "pause owner was cancelled with its waiter")?;
    assert_eq!(
        runtime
            .store
            .load(&address)
            .await?
            .state()
            .map(State::paused),
        Some(true)
    );
    assert_eq!(handler.active.load(Ordering::SeqCst), 0);

    // A queued resume also outlives its observer, but cannot pass parking.
    let resumed = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&resumed);
    let mut resume = Box::pin(runtime.pause_with(&address, false, async move {
        observed.fetch_add(1, Ordering::SeqCst);
    }));
    assert!(futures::poll!(resume.as_mut()).is_pending());
    drop(resume);
    let mut late = Box::pin(runtime.pause(&address, false));
    assert!(futures::poll!(late.as_mut()).is_pending());
    assert_eq!(resumed.load(Ordering::SeqCst), 0);
    handler.gate.add_permits(1);
    runtime.drain_actor(&unrelated).await?;
    assert_eq!(records(&*provider, "out/pause-unrelated").await?.len(), 1);
    release
        .send(())
        .map_err(|_| "resource transition lost its owner")?;
    late.await?;
    assert_eq!(resumed.load(Ordering::SeqCst), 1);
    assert_eq!(
        runtime
            .store
            .load(&address)
            .await?
            .state()
            .map(State::paused),
        Some(false)
    );
    handler.gate.add_permits(1);
    runtime.drain_actor(&address).await?;
    assert_eq!(records(&*provider, "out/pause-owner").await?.len(), 1);
    assert_eq!(
        runtime
            .store
            .load(&address)
            .await?
            .state()
            .and_then(|state| state.cursor("inbox")),
        Some(1)
    );
    let reopened = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
    reopened.track(address.clone())?;
    reopened.drain_actor(&address).await?;
    assert_eq!(records(&*provider, "out/pause-owner").await?.len(), 1);
    Ok(())
}

async fn publication_cut(fault: Fault, abandon: bool) -> TestResult {
    let provider = Arc::new(Provider::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = spawn_with(&store, "pause-cut", &[b"emit"]).await?;
    let runtime = Runtime::new(store, Arc::new(Package::new()?), Arc::new(SystemClock));
    let (entered, release) = provider.arm(fault);
    let transitions = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&transitions);
    let mut pause = Box::pin(runtime.pause_with(&address, true, async move {
        calls.fetch_add(1, Ordering::SeqCst);
    }));
    assert!(futures::poll!(pause.as_mut()).is_pending());
    entered.await?;
    assert!(!runtime.release(&address));
    let outcome = if abandon {
        drop(pause);
        release.send(()).map_err(|_| "lost owned provider cut")?;
        // Same original control lock joins the task's completion. Its retained
        // error is returned before a later caller can perform a new transition.
        let slot = runtime.slot(&address).ok_or("lost placement")?;
        let control = slot.control.lock().await;
        assert!(
            slot.inner.lock().await.control_failure.is_some(),
            "abandoned error lost its original outcome"
        );
        drop(control);
        assert!(
            !runtime.release(&address),
            "unobserved outcome lost on hibernation"
        );
        if !matches!(fault, Fault::Unknown) {
            runtime.wake(&address);
            runtime.drain_actor(&address).await?;
            assert_eq!(
                records(&*provider, "out/pause-cut").await?.len(),
                1,
                "precommit failure must reopen delivery before a late control observes it"
            );
        }
        runtime.pause(&address, true).await
    } else {
        release.send(()).map_err(|_| "lost owned provider cut")?;
        pause.await
    };
    assert_eq!(transitions.load(Ordering::SeqCst), 0);
    match fault {
        Fault::Load | Fault::Refuse => {
            assert!(matches!(
                outcome,
                Err(Error::Stream(StreamError::AccessDenied))
            ));
            assert_eq!(
                runtime
                    .store
                    .load(&address)
                    .await?
                    .state()
                    .map(State::paused),
                Some(false)
            );
            runtime.wake(&address);
            runtime.drain_actor(&address).await?;
            assert_eq!(
                records(&*provider, "out/pause-cut").await?.len(),
                1,
                "a failed pause must progress without an external resume"
            );
        }
        Fault::Unknown => {
            let Err(Error::Unknown {
                publication,
                source: StreamError::Unavailable,
            }) = outcome
            else {
                return Err("lost original Unknown publication".into());
            };
            let before = runtime.store.load(&address).await?;
            assert_eq!(before.state().map(State::paused), Some(true));
            runtime.store.submit(*publication).await?;
            assert_eq!(
                runtime.store.load(&address).await?.anchor,
                before.anchor,
                "the original keyed decision must replay the same commit"
            );
            runtime.drain_actor(&address).await?;
            assert!(records(&*provider, "out/pause-cut").await?.is_empty());
            runtime.pause(&address, false).await?;
            runtime.drain_actor(&address).await?;
            assert_eq!(records(&*provider, "out/pause-cut").await?.len(), 1);
        }
    }
    let reopened = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
    reopened.track(address.clone())?;
    reopened.drain_actor(&address).await?;
    assert_eq!(records(&*provider, "out/pause-cut").await?.len(), 1);
    Ok(())
}

async fn replacement_and_producer_loss() -> TestResult {
    let provider = Arc::new(Provider::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = spawn_with(&store, "pause-replacement", &[b"emit"]).await?;
    let first = store
        .claim(&store.load(&address).await?, 1, key("pause/claim-1")?)
        .await?;
    let runtime = Runtime::new(store, Arc::new(Package::new()?), Arc::new(SystemClock));
    runtime.track(address.clone())?;
    let slot = runtime.slot(&address).ok_or("missing slot")?;
    let mut delivery = slot.inner.lock().await;
    delivery.epoch = 1;
    let mut pause = Box::pin(runtime.pause(&address, true));
    assert!(futures::poll!(pause.as_mut()).is_pending());
    let replacement = runtime
        .store
        .claim(&first, 2, key("pause/claim-2")?)
        .await?;
    drop(delivery);
    assert!(matches!(pause.await, Err(Error::Conflict)));
    assert_eq!(
        runtime.store.load(&address).await?.anchor,
        replacement.anchor
    );
    assert!(runtime.release(&address));

    let (entered, release) = provider.arm(Fault::Load);
    let mut pause = Box::pin(runtime.pause(&address, true));
    assert!(futures::poll!(pause.as_mut()).is_pending());
    entered.await?;
    drop(release); // Original provider producer disappears; no timeout involved.
    assert!(matches!(
        pause.await,
        Err(Error::Stream(StreamError::Unavailable))
    ));
    runtime.wake(&address);
    runtime.drain_actor(&address).await?;
    assert_eq!(records(&*provider, "out/pause-replacement").await?.len(), 1);
    Ok(())
}
