//! `local::Runtime`: the single-node Actors host over any Stream provider.
//!
//! One dispatcher owns one follow of the state root's activation log, one
//! ready index and one timer. A dormant actor owns no task, timer, follow or
//! socket: only its registration and one bounded state cut. The activation log
//! is also the wake set: a producer that writes an actor's subscription appends
//! [`Store::activation`] for that actor in the same commit, and the dispatcher's
//! one follow wakes it. A delivery reads its inputs with bounded finite reads.
use super::ready::{ReadySet, Registration, Work};
use super::{Error, PreparedPublish, Snapshot, State, Store};
use acyclic_stream::StreamProviderExt as _;
mod dispatcher;
mod interrupt;
mod limits;
use crate::addressing::{AccountId, ActorReference, NamespaceId};
use crate::guest::{Delivery, Handler, Trigger};
use acyclic_stream::query_sync::SyncMutex;
use acyclic_stream::runtime::{Executor as _, PlatformExecutor};
use acyclic_stream::{
    AppendRequest, BoxProviderFuture, IdempotencyKey, ProviderPlatform, Record, RecordStream,
    StreamError, StreamPath, StreamProvider, UnixMillisClock as _,
};
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
pub use limits::RuntimeLimits;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, hash_map::RandomState},
    convert::Infallible,
    future::poll_fn,
    hash::BuildHasher,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

/// The host clock. The dispatcher owns the only timer, armed at the earliest
/// committed, undelivered alarm.
pub trait Clock: ProviderPlatform + 'static {
    /// Current Unix milliseconds.
    fn now_unix_ms(&self) -> u64;
    /// Resolve no earlier than `deadline_unix_ms`.
    fn sleep_until(&self, deadline_unix_ms: u64) -> BoxProviderFuture<'static, ()>;
}

/// Wall clock over the platform timer: Tokio natively, the event loop in a browser.
pub struct SystemClock;
impl Clock for SystemClock {
    fn now_unix_ms(&self) -> u64 {
        acyclic_stream::SystemUnixMillisClock.now_unix_millis()
    }
    fn sleep_until(&self, deadline_unix_ms: u64) -> BoxProviderFuture<'static, ()> {
        sleep(deadline_unix_ms.saturating_sub(self.now_unix_ms()))
    }
}

/// A platform sleep of `milliseconds`, in steps the platform timer can express.
fn sleep(milliseconds: u64) -> BoxProviderFuture<'static, ()> {
    Box::pin(async move {
        let mut remaining = milliseconds;
        while remaining > 0 {
            let step = u32::try_from(remaining).unwrap_or(u32::MAX);
            PlatformExecutor.sleep(step).await;
            remaining -= u64::from(step);
        }
    })
}

struct Slot {
    registration: Registration<ActorReference>,
    waker: Waker,
    inner: tokio::sync::Mutex<Inner>,
    refresh: AtomicBool,
    event_deliveries: AtomicUsize,
}

/// An event retains its placement through cooperative batch boundaries.
/// Admission and the actor lock can pass to another actor meanwhile.
struct EventDelivery(Arc<Slot>);
impl Drop for EventDelivery {
    fn drop(&mut self) {
        self.0.event_deliveries.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Invalidate cancelled delivery or control publication without taking its lock.
/// Control changes also wake the dispatcher after publication finishes.
struct RefreshOnDrop(Option<Arc<Slot>>, bool);
impl Drop for RefreshOnDrop {
    fn drop(&mut self) {
        if let Some(slot) = &self.0 {
            slot.refresh.store(true, Ordering::Release);
            if self.1 {
                slot.waker.wake_by_ref();
            }
        }
    }
}

#[derive(Default)]
struct Inner {
    /// A removed slot must not begin another publication through an older Arc.
    released: bool,
    /// Placement this runtime owns; zero until the first claim.
    epoch: u64,
    /// One receipt-backed cut per resident actor; never advances speculatively.
    observed: Option<Arc<Snapshot>>,
    follows: BTreeMap<String, RecordStream>,
    /// Last selected input; scheduling position only, never a consumed cursor.
    last_input: Option<String>,
    /// The exact request and participant custody survive an unknown acknowledgement.
    pending: Option<(Arc<Snapshot>, PreparedPublish)>,
}

enum Event {
    Created(Record),
    Dispatched(Result<Snapshot, Error>),
    Work(Work<ActorReference>),
    Done(Result<(), (ActorReference, Error)>),
    Rearm(Option<u64>),
    Due,
    Failed(StreamError),
}

/// Executes one handler package for every actor whose state lives under the
/// store's root and that it claims, including actors created by other
/// parties' commits.
pub struct Runtime<P: StreamProvider + ?Sized> {
    store: Store<P>,
    /// The one account and namespace whose actors this runtime claims, when
    /// several hosts share the root; `None` claims every actor.
    claims: Option<(AccountId, NamespaceId)>,
    handler: Arc<dyn Handler>,
    clock: Arc<dyn Clock>,
    ready: ReadySet<ActorReference>,
    actors: [SyncMutex<HashMap<ActorReference, Arc<Slot>>>; 64],
    shards: RandomState,
    /// Identity of this runtime's placement attempts: two hosts claiming the
    /// same cut never share an idempotency key, so only one claim commits.
    instance: uuid::Uuid,
    limits: RuntimeLimits,
    admission: tokio::sync::Semaphore,
}

impl<P: StreamProvider + ?Sized> Runtime<P> {
    /// One runtime per state root and handler package.
    pub fn new(store: Store<P>, handler: Arc<dyn Handler>, clock: Arc<dyn Clock>) -> Self {
        Self {
            store,
            claims: None,
            handler,
            clock,
            ready: ReadySet::default(),
            actors: std::array::from_fn(|_| SyncMutex::new(HashMap::new())),
            shards: RandomState::new(),
            instance: uuid::Uuid::new_v4(),
            limits: RuntimeLimits::default(),
            admission: tokio::sync::Semaphore::new(RuntimeLimits::default().active),
        }
    }

    /// Configure admission before sharing or running this host. Event-owned
    /// deliveries and continuous dispatch use the same admission bound.
    #[must_use]
    pub fn with_limits(mut self, limits: RuntimeLimits) -> Self {
        self.admission = tokio::sync::Semaphore::new(limits.active);
        self.limits = limits;
        self
    }

    /// Claim only the actors of `within`'s account and namespace: discovery
    /// skips every other actor and [`Self::track`] refuses it, so hosts that
    /// share one state root never claim each other's actors.
    #[must_use]
    pub fn claiming(mut self, within: &ActorReference) -> Self {
        self.claims = Some((within.account().clone(), within.namespace().clone()));
        self
    }

    /// Whether this runtime claims `actor`.
    pub fn claims(&self, actor: &ActorReference) -> bool {
        self.claims.as_ref().is_none_or(|(account, namespace)| {
            actor.account() == account && actor.namespace() == namespace
        })
    }

    /// The shared store.
    pub const fn store(&self) -> &Store<P> {
        &self.store
    }

    /// Exact resident registration and ready counts.
    pub fn counts(&self) -> (usize, usize) {
        self.ready.counts()
    }

    /// Start tracking an actor. Idempotent; the first delivery claims a new
    /// epoch. An actor this runtime does not [claim](Self::claims) is refused.
    pub fn track(&self, actor: ActorReference) -> Result<(), Error> {
        if !self.claims(&actor) {
            return Err(Error::Invalid("actor is outside this runtime's claims"));
        }
        let mut actors = self
            .shard(&actor)
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(slot) = actors.get(&actor) {
            // An activation may follow another producer's state commit. Wake
            // against that new cut even if the resident had no input or alarm.
            slot.refresh.store(true, Ordering::Release);
            slot.waker.wake_by_ref();
            return Ok(());
        }
        let registration = self.ready.register(actor.clone())?;
        let waker = registration.waker();
        actors.insert(
            actor,
            Arc::new(Slot {
                registration,
                waker,
                inner: tokio::sync::Mutex::new(Inner::default()),
                refresh: AtomicBool::new(false),
                event_deliveries: AtomicUsize::new(0),
            }),
        );
        Ok(())
    }

    /// Stop hosting an idle actor (hibernate). Returns false while a delivery
    /// or an unknown publication still owns participant custody; wake it to
    /// settle before releasing. Its durable state is untouched.
    pub fn release(&self, actor: &ActorReference) -> bool {
        let mut actors = self
            .shard(actor)
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(slot) = actors.get(actor) else {
            return true;
        };
        if slot.event_deliveries.load(Ordering::Acquire) != 0 {
            return false;
        }
        let Ok(mut inner) = slot.inner.try_lock() else {
            return false;
        };
        if inner.pending.is_some() {
            return false;
        }
        inner.released = true;
        drop(inner);
        actors.remove(actor);
        true
    }

    /// Request a delivery attempt, for example after a transient failure.
    pub fn wake(&self, actor: &ActorReference) {
        if let Some(slot) = self.slot(actor) {
            slot.waker.wake_by_ref();
        }
    }

    /// Durably pause or resume. Pause fences publication; resume restores placement.
    /// A changed state cut returns [`Error::Conflict`] without another attempt.
    pub async fn pause(&self, actor: &ActorReference, paused: bool) -> Result<(), Error> {
        acyclic_observability::async_operation!(
            "acyclic.actors.pause",
            Error::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let _refresh = RefreshOnDrop(self.slot(actor), true);
                let snapshot = self.store.load(actor).await?;
                let state = snapshot
                    .state()
                    .ok_or(Error::Invalid("actor state was never created"))?;
                if state.paused() != paused {
                    let operation = if paused { "pause" } else { "resume" };
                    self.store
                        .set_paused(&snapshot, state.epoch(), paused, key(&snapshot, operation)?)
                        .await?;
                }
                if !paused {
                    self.track(actor.clone())?;
                }
                Ok(())
            }
        )
    }

    /// Run every currently ready or due actor to quiescence, then return the
    /// failures. Independent actors share the same active admission as `run`
    /// and event delivery; no task is created for a dormant actor.
    pub async fn drain(&self) -> Vec<(ActorReference, Error)> {
        let now = self.clock.now_unix_ms();
        while self.ready.wake_due(now, self.limits.work) == self.limits.work {
            limits::yield_now().await;
        }
        let mut failures = Vec::new();
        let mut running = FuturesUnordered::new();
        let mut steps = 0;
        loop {
            while running.len() < self.limits.active {
                let Some(work) = self.ready.take() else { break };
                running.push(self.execute(work));
                steps += 1;
                if steps == self.limits.work {
                    limits::yield_now().await;
                    steps = 0;
                }
            }
            match running.next().await {
                Some(Err(failure)) => failures.push(failure),
                Some(Ok(())) => {}
                None => break,
            }
        }
        failures
    }

    /// Drain one tracked actor using finite reads of its committed input.
    /// Only declared interrupts retain a follow while their handler is
    /// running; no idle follow is retained.
    pub async fn drain_actor(&self, actor: &ActorReference) -> Result<(), Error> {
        acyclic_observability::async_operation!(
            "acyclic.actors.drain_actor",
            Error::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let delivery = {
                    // Pin the event under the same table lock as hibernation, before
                    // waiting for admission. Cancellation drops this guard as well.
                    let actors = self
                        .shard(actor)
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let slot = actors
                        .get(actor)
                        .ok_or(Error::Invalid("actor is not tracked"))?;
                    slot.event_deliveries
                        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                            count.checked_add(1)
                        })
                        .map_err(|_| Error::Invalid("event delivery count overflow"))?;
                    EventDelivery(Arc::clone(slot))
                };
                let result = loop {
                    match self.deliver(actor, &delivery.0).await {
                        Ok(true) => break Ok(()),
                        Ok(false) => {
                            // Drop both locks before queueing the next finite batch.
                            limits::yield_now().await;
                        }
                        Err(error) => break Err(error),
                    }
                };
                drop(delivery);
                if matches!(result, Err(Error::Conflict)) {
                    self.release(actor);
                }
                result
            }
        )
    }

    /// Event-owned delivery followed by the committed alarm/hibernation handoff.
    /// The owner serializes its delivery events, retains their lifetime and
    /// persists the returned deadline before completing the native event.
    /// A failed/unknown publication retains its slot and participant custody.
    pub async fn drain_event(&self, actor: &ActorReference) -> Result<Option<u64>, Error> {
        acyclic_observability::async_operation!(
            "acyclic.actors.drain_event",
            Error::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.track(actor.clone())?;
                self.drain_actor(actor).await?;
                let snapshot = self.store.load(actor).await?;
                let alarm = snapshot
                    .state()
                    .filter(|state| !state.paused())
                    .and_then(|state| state.alarm().pending());
                if !self.release(actor) {
                    return Err(Error::Invalid("actor still owns an unsettled delivery"));
                }
                Ok(alarm)
            }
        )
    }

    /// Host forever: follow the root's activation log to discover actors
    /// created by any commit, deliver concurrently across actors, and fire
    /// alarms from one timer. Per-actor failures go to `report`; the input
    /// stays queued.
    pub async fn run(&self, report: impl Fn(&ActorReference, &Error)) -> Result<Infallible, Error> {
        let root = self.store.root().clone();
        let provider = self.store.provider();
        // Streams has no empty create: open the log with one non-address record
        // at sequence 0. Any existing log (an earlier opening or a creation)
        // refuses it, which is equally fine.
        provider
            .append(AppendRequest {
                path: root.clone(),
                records: vec![b"open".to_vec().into()],
                if_tail: Some(0),
                idempotency_key: key(
                    &Snapshot {
                        path: root.clone(),
                        tail: 0,
                        anchor: None,
                        state: None,
                    },
                    "open",
                )?,
            })
            .await?;
        let (dispatch_store, dispatch_actor) = self.dispatcher_store()?;
        let dispatcher = self.dispatcher(&dispatch_store, &dispatch_actor).await?;
        // A cursor alone would forget all previously discovered subscriptions
        // and alarms. Rebuild residents from current state names, without
        // replaying their creation history. New creations race safely with the
        // enumeration: the follow starts at the checkpoint read BEFORE it.
        self.restore().await?;
        let from = dispatcher
            .state()
            .and_then(|state| state.cursor("activation"))
            .ok_or(Error::Invalid("dispatcher has no activation cursor"))?;
        let mut created = acyclic_stream::records_only(
            provider
                .follow(acyclic_stream::FollowRequest::new(root, from))
                .await?,
        );
        let mut dispatcher = Some(dispatcher);
        let mut dispatching: Option<BoxProviderFuture<'_, std::result::Result<Snapshot, Error>>> =
            None;
        // The activation log is also every producer's wake entry: the cursor
        // only bounds restart replay, so consecutive entries coalesce into one
        // checkpoint in flight at the latest seen end.
        let mut seen = from;
        let mut running = FuturesUnordered::new();
        let mut timer: Option<(u64, BoxProviderFuture<'static, ()>)> = None;
        let mut next_class = 0;
        let mut steps = 0;
        loop {
            if steps == self.limits.work {
                limits::yield_now().await;
                steps = 0;
            }
            let event = poll_fn(|context: &mut Context<'_>| {
                // Alarm changes must wake us even when active admission is
                // full and the ready queue cannot currently be consumed.
                self.ready.listen(context);
                for _ in 0..4 {
                    let class = next_class;
                    next_class = (next_class + 1) % 4;
                    match class {
                        0 => {
                            // Provider I/O owned by an active delivery must keep
                            // progressing during dispatcher checkpoint writes.
                            if let Poll::Ready(Some(result)) = running.poll_next_unpin(context) {
                                return Poll::Ready(Event::Done(result));
                            }
                        }
                        1 => {
                            if let Some(checkpoint) = dispatching.as_mut()
                                && let Poll::Ready(result) = checkpoint.poll_unpin(context)
                            {
                                return Poll::Ready(Event::Dispatched(result));
                            }
                            match created.poll_next_unpin(context) {
                                Poll::Ready(Some(Ok(record))) => {
                                    return Poll::Ready(Event::Created(record));
                                }
                                Poll::Ready(Some(Err(error))) => {
                                    return Poll::Ready(Event::Failed(error));
                                }
                                Poll::Ready(None) => {
                                    return Poll::Ready(Event::Failed(StreamError::Unavailable));
                                }
                                Poll::Pending => {}
                            }
                        }
                        2 => {
                            if running.len() < self.limits.active
                                && let Poll::Ready(work) = self.ready.poll_ready(context)
                            {
                                return Poll::Ready(Event::Work(work));
                            }
                        }
                        _ => {
                            // Read the deadline after installing the waiter: an earlier
                            // alarm installed concurrently wakes us to rearm.
                            let next = self.ready.next_deadline();
                            if next != timer.as_ref().map(|(deadline, _)| *deadline) {
                                return Poll::Ready(Event::Rearm(next));
                            }
                            if let Some((_, sleep)) = timer.as_mut()
                                && sleep.poll_unpin(context).is_ready()
                            {
                                return Poll::Ready(Event::Due);
                            }
                        }
                    }
                }
                Poll::Pending
            })
            .await;
            steps += 1;
            match event {
                Event::Created(record) => {
                    // The opening record is not an address; skip it. Another
                    // host's actors are not discovered here. Tracking wakes an
                    // already resident actor: this is its producer's wake.
                    if let Ok(actor) = ActorReference::decode(record.value.as_ref())
                        && self.claims(&actor)
                    {
                        self.track(actor)?;
                    }
                    seen = record
                        .sequence
                        .checked_add(1)
                        .ok_or(Error::Invalid("activation cursor overflow"))?;
                }
                Event::Dispatched(result) => {
                    dispatching = None;
                    dispatcher = Some(result?);
                }
                Event::Work(work) => {
                    let execution: BoxProviderFuture<'_, _> = Box::pin(self.execute(work));
                    running.push(execution);
                }
                Event::Done(Err((actor, error))) => report(&actor, &error),
                Event::Done(Ok(())) => {}
                Event::Rearm(next) => {
                    timer = next.map(|deadline| (deadline, self.clock.sleep_until(deadline)));
                }
                Event::Due => {
                    timer = None;
                    self.ready
                        .wake_due(self.clock.now_unix_ms(), self.limits.work);
                }
                Event::Failed(error) => return Err(error.into()),
            }
            if dispatching.is_none()
                && let Some(previous) = dispatcher.take_if(|previous| {
                    previous
                        .state()
                        .and_then(|state| state.cursor("activation"))
                        .is_some_and(|cursor| cursor < seen)
                })
            {
                let store = &dispatch_store;
                dispatching = Some(Box::pin(async move {
                    self.advance_dispatcher(store, previous, seen).await
                }));
            }
        }
    }

    fn slot(&self, actor: &ActorReference) -> Option<Arc<Slot>> {
        self.shard(actor)
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(actor)
            .cloned()
    }

    fn shard(&self, actor: &ActorReference) -> &SyncMutex<HashMap<ActorReference, Arc<Slot>>> {
        &self.actors[(self.shards.hash_one(actor) as usize) % self.actors.len()]
    }

    async fn execute(&self, work: Work<ActorReference>) -> Result<(), (ActorReference, Error)> {
        let actor = work.key().clone();
        let Some(slot) = self.slot(&actor) else {
            return Ok(());
        };
        let result = self.deliver(&actor, &slot).await;
        if let Err(Error::Conflict) = result {
            // Another placement owns the actor now; stop hosting it here.
            self.release(&actor);
        }
        drop(work);
        result.map(|_| ()).map_err(|error| (actor, error))
    }

    /// Deliver one bounded batch. False retains a wake for the next batch.
    async fn deliver(&self, actor: &ActorReference, slot: &Arc<Slot>) -> Result<bool, Error> {
        acyclic_observability::async_operation!(
            "acyclic.actors.deliver",
            Error::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.handler.manifest().validate_interrupts()?;
                // Duplicate events for one actor wait outside global admission. Only
                // an actor that can make progress may occupy an active delivery slot.
                let inner = slot.inner.lock().await;
                let _admission = self
                    .admission
                    .acquire()
                    .await
                    .map_err(|_| Error::Invalid("runtime admission is closed"))?;
                let mut refresh = RefreshOnDrop(Some(Arc::clone(slot)), false);
                let result = self.deliver_retained(actor, slot, inner).await;
                // Cancellation can interrupt a committed publication before its receipt
                // is retained. A completed attempt handles its outcome in the loop.
                refresh.0 = None;
                result
            }
        )
    }

    async fn deliver_retained(
        &self,
        actor: &ActorReference,
        slot: &Slot,
        mut inner: tokio::sync::MutexGuard<'_, Inner>,
    ) -> Result<bool, Error> {
        if inner.released {
            return Ok(true);
        }
        let mut steps = 0;
        loop {
            // A pending follow may own asynchronous provider I/O or a fair
            // mutex ticket. Cancel it before loading state through that same
            // provider; the committed cursor is the source of truth below.
            inner.follows.clear();
            if steps == self.limits.work {
                slot.waker.wake_by_ref();
                return Ok(false);
            }
            steps += 1;
            if let Some((snapshot, publication)) = inner.pending.take() {
                match self.store.submit_retained(publication).await {
                    Ok(observed) => {
                        inner.observed = Some(Arc::new(
                            observed.ok_or(Error::Invalid("actor receipt has no state"))?,
                        ));
                        continue;
                    }
                    Err(Error::Unknown {
                        publication,
                        source,
                    }) => {
                        inner.pending = Some((snapshot, *publication));
                        return Err(Error::Stream(source));
                    }
                    Err(error) => {
                        inner.observed = None;
                        return Err(error);
                    }
                }
            }
            if slot.refresh.swap(false, Ordering::AcqRel) {
                inner.observed = None;
            }
            let snapshot = match &inner.observed {
                Some(observed) => Arc::clone(observed),
                None => {
                    let observed = Arc::new(self.store.load(actor).await?);
                    inner.observed = Some(Arc::clone(&observed));
                    observed
                }
            };
            let Some(state) = snapshot.state() else {
                inner.observed = None;
                return Ok(true);
            };
            // Unclaimed here (epoch 0 locally) always claims, including a state
            // created at epoch 0 by a foreign commit.
            if inner.epoch == 0 || state.epoch() != inner.epoch {
                if inner.epoch != 0 {
                    return Err(Error::Conflict);
                }
                let epoch = state
                    .epoch()
                    .checked_add(1)
                    .ok_or(Error::Invalid("epoch overflow"))?;
                match self
                    .store
                    .claim(
                        &snapshot,
                        epoch,
                        key(&snapshot, &format!("claim-{epoch}-{}", self.instance))?,
                    )
                    .await
                {
                    Ok(claimed) => {
                        inner.epoch = epoch;
                        inner.observed = Some(Arc::new(claimed));
                    }
                    Err(error) => {
                        inner.observed = None;
                        return Err(error);
                    }
                }
                continue;
            }
            if state.paused() {
                // The alarm stays committed; only its active wake is withdrawn.
                // A later wake must observe an external resume as well.
                inner.observed = None;
                slot.registration.suspend_alarm();
                return Ok(true);
            }
            slot.registration.update_alarm(state.alarm())?;
            let now = self.clock.now_unix_ms();
            let (trigger, record) = match state.alarm().pending() {
                Some(deadline) if deadline <= now => (
                    Trigger::Alarm {
                        generation: state.alarm().generation(),
                        now_unix_ms: now,
                    },
                    None,
                ),
                _ => match self.next_record(state, inner.last_input.as_deref()).await? {
                    Some((subscription, record)) => {
                        inner.last_input = Some(subscription.clone());
                        (
                            Trigger::Record {
                                subscription,
                                sequence: record.sequence,
                            },
                            Some(record),
                        )
                    }
                    None => return Ok(true),
                },
            };
            // A retained publication cut cannot admit external handler work:
            // another host may already have paused or replaced this placement.
            // Until canonical control invalidation is available, observe the
            // real provider before every handler attempt, including alarms.
            if self.store.observe(&snapshot).await?.is_some() {
                inner.observed = None;
                return Err(Error::Conflict);
            }
            let epoch = inner.epoch;
            let limits = &self.handler.manifest().limits;
            let cancellation = crate::guest::CancellationToken::new();
            self.interrupt_follows(&mut inner, state, &trigger).await?;
            // A handler past its manifest timeout (wall time, not the actor
            // clock) is abandoned with its input unconsumed; the actor lock is
            // released for the next attempt.
            let handled = self.handler.on_record(
                Delivery {
                    actor: actor.clone(),
                    epoch,
                    trigger: trigger.clone(),
                    now_unix_ms: now,
                    record: record.clone(),
                    subscriptions: crate::guest::SubscriptionCuts::new(
                        snapshot.path.clone(),
                        snapshot
                            .anchor
                            .ok_or(Error::Invalid("actor state has no committed anchor"))?,
                        state.subscriptions().clone(),
                    ),
                },
                state.checkpoint(),
                cancellation.clone(),
            );
            let result = {
                let interrupted = self.interrupt(&mut inner, cancellation.clone());
                futures::pin_mut!(interrupted);
                match futures::future::select(
                    handled,
                    futures::future::select(sleep(limits.handler_timeout_millis()), interrupted),
                )
                .await
                {
                    futures::future::Either::Left((transition, _)) => {
                        transition.map_err(Error::from)
                    }
                    futures::future::Either::Right((futures::future::Either::Left(_), _)) => {
                        Err(Error::Invalid("handler exceeded its manifest timeout"))
                    }
                    futures::future::Either::Right((
                        futures::future::Either::Right((error, _)),
                        _,
                    )) => Err(error),
                }
            };
            // Release any pending provider I/O before checkpoint publication.
            inner.follows.clear();
            let publication = match result {
                Ok(transition) => {
                    if u64::try_from(transition.checkpoint.bytes.len()).unwrap_or(u64::MAX)
                        > limits.checkpoint_bytes()
                    {
                        return Err(Error::Invalid("checkpoint exceeds the manifest limit"));
                    }
                    self.store.prepare_publish(
                        &snapshot,
                        epoch,
                        &trigger,
                        record.as_ref(),
                        transition,
                        key(&snapshot, "publish")?,
                    )?
                }
                Err(Error::Handler(failure)) if !cancellation.is_cancelled() => {
                    let Some(publication) = self.store.prepare_invocation_failure(
                        &snapshot,
                        epoch,
                        &trigger,
                        record.as_ref(),
                        &failure,
                        key(&snapshot, "publish")?,
                    )?
                    else {
                        return Err(Error::Handler(failure));
                    };
                    publication
                }
                Err(error) => return Err(error),
            };
            match self.store.submit_retained(publication).await {
                Ok(observed) => {
                    inner.observed = Some(Arc::new(
                        observed.ok_or(Error::Invalid("actor receipt has no state"))?,
                    ));
                }
                Err(Error::Unknown {
                    publication,
                    source,
                }) => {
                    inner.pending = Some((snapshot, *publication));
                    return Err(Error::Stream(source));
                }
                Err(error) => {
                    // Settlement can fail after a successful commit. Reopen the
                    // actual state next time instead of rerunning its handler.
                    inner.observed = None;
                    return Err(error);
                }
            }
        }
    }

    /// Open the follow for one subscription. A stream that does not exist yet
    /// is no input: its creator makes it in the actor's creating commit, or the
    /// actor's next wake (alarm, other input, state change) reopens it.
    async fn open(
        &self,
        inner: &mut Inner,
        id: &str,
        path: &StreamPath,
        cursor: u64,
    ) -> Result<bool, Error> {
        match self
            .store
            .provider()
            .follow(acyclic_stream::FollowRequest::new(path.clone(), cursor))
            .await
        {
            Ok(follow) => {
                inner
                    .follows
                    .insert(id.to_owned(), acyclic_stream::records_only(follow));
                Ok(true)
            }
            Err(StreamError::NotFound) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// The next unconsumed record of any subscription, by one bounded finite
    /// read per subscription. No follow is retained: the producer's same-commit
    /// activation entry wakes this actor when new input commits.
    async fn next_record(
        &self,
        state: &State,
        after: Option<&str>,
    ) -> Result<Option<(String, Record)>, Error> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let subscriptions = state.subscriptions();
        let later = subscriptions.range::<str, _>((after.map_or(Unbounded, Excluded), Unbounded));
        let earlier = after
            .into_iter()
            .flat_map(|last| subscriptions.range::<str, _>((Unbounded, Included(last))));
        for (id, subscription) in later.chain(earlier) {
            let records = self
                .store
                .provider()
                .read(acyclic_stream::ReadRequest {
                    key: None,
                    path: subscription.path.clone(),
                    from: subscription.cursor,
                    limit: 1,
                    at_least: Default::default(),
                })
                .await;
            let mut records = match records {
                Ok(records) => records,
                Err(StreamError::NotFound) => continue,
                Err(error) => return Err(error.into()),
            };
            if let Some(record) = records.next().await.transpose()? {
                if record.sequence != subscription.cursor {
                    return Err(Error::Invalid("subscription skipped a record"));
                }
                return Ok(Some((id.clone(), record)));
            }
        }
        Ok(None)
    }
}

/// One stable key per intent at one exact cut: an uncertain acknowledgement
/// retries the identical request, and distinct cuts never share a key.
fn key(snapshot: &Snapshot, operation: &str) -> Result<IdempotencyKey, Error> {
    // Hashed so any valid root and path fit the bounded key; parts are
    // length-prefixed so distinct (path, tail, operation) never collide.
    let mut hash = Sha256::new();
    for part in [
        snapshot.path.as_str().as_bytes(),
        &snapshot.tail.to_le_bytes(),
        operation.as_bytes(),
    ] {
        hash.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_le_bytes());
        hash.update(part);
    }
    Ok(IdempotencyKey::new(
        [b"actors/v1/".as_slice(), hash.finalize().as_slice()].concat(),
    )?)
}

#[cfg(test)]
mod tests;
