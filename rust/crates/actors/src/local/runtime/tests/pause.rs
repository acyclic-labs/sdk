use super::*;
use acyclic_stream::{
    CommitId, CommittedEnvelope, FollowRequest, FollowStream, Heads, HeadsRequest,
    IdempotencyObservation, ProviderContext, ReadRequest, WriteOutcome, WriteRequest,
};
use futures::channel::oneshot;
use std::sync::Mutex;

#[derive(Clone, Copy, Debug)]
enum Fault {
    Load,
    Refuse,
    Unknown,
    Commit,
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
    cuts: AtomicUsize,
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
            self.cuts.fetch_add(1, Ordering::SeqCst);
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
                    Fault::Commit => self.inner.write(request).await,
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
    for abandon_at_transition in [false, true] {
        for resource_producer_lost in [false, true] {
            active_cleanup_and_queued_controls(abandon_at_transition, resource_producer_lost)
                .await?;
        }
    }
    completed_unread_failure().await?;
    failed_pause_reports_before_held_delivery().await?;
    replacement_and_producer_loss().await?;
    let mut covered = 0u16;
    for (index, fault) in [Fault::Load, Fault::Refuse, Fault::Unknown, Fault::Commit]
        .into_iter()
        .enumerate()
    {
        for abandon in [false, true] {
            for producer_lost in [false, true] {
                publication_cut(fault, abandon, producer_lost).await?;
                covered |= 1 << (index * 4 + usize::from(abandon) * 2 + usize::from(producer_lost));
            }
        }
    }
    assert_eq!(covered, u16::MAX, "missing original dependency schedule");
    eprintln!(
        "pause dependency schedules: publication=16/16, cleanup/resource=4/4; publication cut visits=1, transitions=0..1"
    );
    Ok(())
}

async fn failed_pause_reports_before_held_delivery() -> TestResult {
    let provider = Arc::new(Provider::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = spawn_with(&store, "pause-error-delivery", &[b"emit"]).await?;
    let handler = Arc::new(Gated {
        package: Package::new()?,
        active: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
        gate: tokio::sync::Semaphore::new(0),
    });
    let runtime = Runtime::new(store, handler.clone(), Arc::new(SystemClock));
    runtime.track(address.clone())?;
    let (entered, release) = provider.arm(Fault::Load);
    let mut failed = Box::pin(runtime.pause(&address, true));
    assert!(futures::poll!(failed.as_mut()).is_pending());
    entered.await?;
    // Queue the real delivery before the original failed load completes. The
    // mutex's own wake drives it; its handler holds the original gate permit.
    let mut delivery = Box::pin(runtime.drain_actor(&address));
    assert!(futures::poll!(delivery.as_mut()).is_pending());
    release.send(()).map_err(|_| "lost failed load producer")?;
    futures::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(delivery.as_mut(), cx).is_pending());
        if handler.active.load(Ordering::SeqCst) == 1 {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    })
    .await;
    let slot = runtime.slot(&address).ok_or("missing original placement")?;
    assert!(
        slot.delivery_control
            .lock()
            .expect("control lock")
            .failure
            .is_some(),
        "failed pause lost error custody to a queued delivery"
    );
    assert!(matches!(
        failed.await,
        Err(Error::Stream(StreamError::AccessDenied))
    ));
    assert_eq!(handler.active.load(Ordering::SeqCst), 1);
    // Taking the error cannot wait for this held handler. A later pause can
    // now cancel it and retain cleanup, rather than queue behind an error read.
    let mut late = Box::pin(runtime.pause(&address, true));
    assert!(futures::poll!(late.as_mut()).is_pending());
    drop(late);
    handler.gate.add_permits(1);
    let (_delivery_outcome, resumed) = futures::join!(delivery, runtime.pause(&address, false));
    resumed?;
    handler.gate.add_permits(1);
    runtime.drain_actor(&address).await?;
    assert_eq!(
        records(&*provider, "out/pause-error-delivery").await?.len(),
        1
    );
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
        slot.delivery_control
            .lock()
            .expect("control lock")
            .failure
            .is_some(),
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

async fn active_cleanup_and_queued_controls(
    abandon_at_transition: bool,
    resource_producer_lost: bool,
) -> TestResult {
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
    let resource_outcome = Arc::new(AtomicUsize::new(0));
    let settled = Arc::clone(&resource_outcome);
    let transition = async move {
        let _ = entered.send(());
        let outcome = released.await;
        settled.store(if outcome.is_ok() { 1 } else { 2 }, Ordering::SeqCst);
        outcome
    };
    let mut pause = Box::pin(runtime.pause_with(&address, true, transition));
    assert!(futures::poll!(pause.as_mut()).is_pending());
    let mut pause = Some(pause);
    if !abandon_at_transition {
        drop(pause.take()); // Caller loss while original cleanup is active.
    }
    assert!(!runtime.release(&address));
    handler.gate.add_permits(1);
    let (delivered, entered) = futures::join!(delivery, parked);
    // The cancelled handler may fail or return its original transition; pause
    // must await its actual cleanup in either case.
    let _delivery_outcome = delivered;
    entered.map_err(|_| "pause owner was cancelled with its waiter")?;
    drop(pause); // Alternatively lose the caller after the durable decision.
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
    if resource_producer_lost {
        drop(release);
    } else {
        release
            .send(())
            .map_err(|_| "resource transition lost its owner")?;
    }
    late.await?;
    assert_eq!(
        resource_outcome.load(Ordering::SeqCst),
        if resource_producer_lost { 2 } else { 1 },
        "original resource completion must settle before a queued control"
    );
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

async fn publication_cut(fault: Fault, abandon: bool, producer_lost: bool) -> TestResult {
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
    let complete = || -> TestResult {
        if producer_lost {
            drop(release);
        } else {
            release.send(()).map_err(|_| "lost owned provider cut")?;
        }
        Ok(())
    };
    let committed = !producer_lost && matches!(fault, Fault::Unknown | Fault::Commit);
    let succeeded = !producer_lost && matches!(fault, Fault::Commit);
    let known_failure =
        matches!(fault, Fault::Load) || (!producer_lost && matches!(fault, Fault::Refuse));
    let outcome = if abandon {
        drop(pause);
        complete()?;
        // Same original control lock joins the task's completion. Its retained
        // error is returned before a later caller can perform a new transition.
        let slot = runtime.slot(&address).ok_or("lost placement")?;
        let control = slot.control.lock().await;
        assert_eq!(
            slot.delivery_control
                .lock()
                .expect("control lock")
                .failure
                .is_some(),
            !succeeded,
            "abandoned operation lost its original outcome: {fault:?}, producer_lost={producer_lost}"
        );
        drop(control);
        if !succeeded {
            assert!(
                !runtime.release(&address),
                "unobserved outcome lost on hibernation"
            );
        }
        if known_failure {
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
        complete()?;
        pause.await
    };
    assert_eq!(provider.cuts.load(Ordering::SeqCst), 1);
    assert_eq!(transitions.load(Ordering::SeqCst), usize::from(succeeded));
    match (known_failure, succeeded) {
        (true, _) => {
            let expected = if producer_lost {
                StreamError::Unavailable
            } else {
                StreamError::AccessDenied
            };
            assert!(matches!(outcome, Err(Error::Stream(error)) if error == expected));
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
        }
        (false, false) => {
            let Err(Error::Unknown {
                publication,
                source: StreamError::Unavailable,
            }) = outcome
            else {
                return Err("lost original Unknown publication".into());
            };
            let before = runtime.store.load(&address).await?;
            assert_eq!(before.state().map(State::paused), Some(committed));
            // A lost write producer leaves an uncertain, unapplied request;
            // a lost reply leaves that same keyed request already committed.
            // Submit the retained original in either case, then require an
            // exact second replay without a new durable decision.
            let replay = publication.request().clone();
            runtime.store.submit(*publication).await?;
            let settled = runtime.store.load(&address).await?;
            assert_eq!(settled.state().map(State::paused), Some(true));
            if committed {
                assert_eq!(settled.anchor, before.anchor);
            } else {
                assert_ne!(settled.anchor, before.anchor);
            }
            PreparedPublish::settle(&provider.commit(replay).await?)?;
            assert_eq!(
                runtime.store.load(&address).await?.anchor,
                settled.anchor,
                "the original keyed decision must replay the same commit"
            );
        }
        (false, true) => {
            outcome?;
            assert_eq!(
                runtime
                    .store
                    .load(&address)
                    .await?
                    .state()
                    .map(State::paused),
                Some(true),
                "accepted publication must finish after observer loss"
            );
        }
    }
    if !known_failure {
        runtime.drain_actor(&address).await?;
        assert!(records(&*provider, "out/pause-cut").await?.is_empty());
        runtime.pause(&address, false).await?;
    }
    runtime.drain_actor(&address).await?;
    assert_eq!(
        records(&*provider, "out/pause-cut").await?.len(),
        1,
        "known failure must progress without resume; settled pause must resume exactly once"
    );
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
    assert!(matches!(
        runtime.drain_actor(&address).await,
        Err(Error::Conflict)
    ));

    // A fresh host can claim the replacement. The old slot's error receipt
    // may still be retiring on its original executor; that is not permission
    // for the stale epoch to deliver, nor an immediate hibernation guarantee.
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
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
