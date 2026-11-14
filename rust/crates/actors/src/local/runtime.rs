//! `local::Runtime`: the single-node Actors host over any Stream provider.
//!
//! Each assigned scheduling shard owns one activation follow and checkpoint.
//! The host owns one ready index and timer. A dormant actor owns no task, timer, follow or
//! socket for its manifest inputs: only its registration and one bounded state cut. Guest-added
//! interests share one follow per distinct stream and create no task or timer. The activation log
//! is also the wake set: a producer that writes an actor's subscription appends
//! [`Store::activation`] for that actor in the same commit, and the dispatcher's
//! assigned shard follow wakes it. A delivery reads its inputs with bounded finite reads.
use super::ready::{Flight, ReadySet, Registration, Work};
use super::{Error, PreparedPublish, Snapshot, State, Store, scheduling};
use acyclic_stream::StreamProviderExt as _;
mod clock;
mod completion;
mod dispatcher;
mod external;
mod interrupt;
mod limits;
mod pause;
mod placement;
mod subscriptions;
use crate::addressing::{AccountId, ActorReference, NamespaceId};
use crate::guest::{Delivery, Handler, Transition, Trigger};
#[cfg(test)]
use acyclic_stream::AppendRequest;
use acyclic_stream::query_sync::SyncMutex;
use acyclic_stream::runtime::PlatformExecutor;
use acyclic_stream::{
    BoxProviderFuture, IdempotencyKey, Record, RecordStream, StreamError, StreamPath,
    StreamProvider,
};
use clock::sleep;
pub use clock::{Clock, ClockControl, ClockError, ControlledClock, SystemClock};
use completion::{Completion, Completions};
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
pub use limits::RuntimeLimits;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, hash_map::RandomState},
    convert::Infallible,
    future::poll_fn,
    hash::BuildHasher,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

struct Slot {
    registration: Registration<ActorReference>,
    waker: Waker,
    inner: tokio::sync::Mutex<Inner>,
    refresh: AtomicBool,
    /// Just past its latest activation entry this dispatcher observed or
    /// carried; zero for none. A failed delivery's checkpoint bound.
    entry: AtomicU64,
    evict: AtomicBool,
    event_deliveries: AtomicUsize,
    /// Control requests serialize separately from the delivery they stop.
    control: tokio::sync::Mutex<()>,
    delivery_control: SyncMutex<DeliveryControl>,
}

#[derive(Default)]
struct DeliveryControl {
    paused: bool,
    /// An unread control result retains its original uncertain publication.
    failure: Option<Box<Error>>,
}

/// An event retains its placement through cooperative batch boundaries.
/// Admission and the actor lock can pass to another actor meanwhile.
struct EventDelivery(Arc<Slot>);
impl Drop for EventDelivery {
    fn drop(&mut self) {
        if self.0.event_deliveries.fetch_sub(1, Ordering::AcqRel) == 1
            && self.0.evict.load(Ordering::Acquire)
        {
            self.0.waker.wake_by_ref();
        }
    }
}

/// One delivery's single settlement, on every way out, and the only place
/// any input settles as delivered: only a delivery that owns its slot and
/// ran to an outcome clears `failed`. An error, a panic, a cancelled await
/// or a released slot settles it failed. A failed delivery invalidates its
/// slot's state without taking its lock (a cancelled publication may have
/// committed), and its carried wake, not a resident token, retains the
/// alarm; its flight then keeps the actor's failure mark. The work it was
/// taken for settles with it.
struct Settlement<'a> {
    slot: &'a Slot,
    flight: Option<Flight<ActorReference>>,
    work: Option<Work<ActorReference>>,
    failed: bool,
}
impl Drop for Settlement<'_> {
    fn drop(&mut self) {
        let Some(flight) = self.flight.take() else {
            return;
        };
        if self.failed {
            self.slot.refresh.store(true, Ordering::Release);
            self.slot.registration.suspend_alarm();
        }
        flight.finish(self.failed);
        if let Some(work) = self.work.take() {
            work.settle();
        }
    }
}

#[derive(Default)]
struct Inner {
    /// A removed slot must not begin another publication through an older Arc.
    released: bool,
    /// Epoch admitted by an explicit placement command, never by delivery.
    epoch: u64,
    /// One receipt-backed cut per resident actor; never advances speculatively.
    observed: Option<Arc<Snapshot>>,
    follows: BTreeMap<String, RecordStream>,
    /// Last completed attempt; scheduling position only, never a consumed cursor.
    last_input: Option<String>,
    /// The exact request and participant custody survive an unknown acknowledgement.
    pending: Option<(Arc<Snapshot>, PreparedPublish, Option<String>)>,
    /// Conflicts of the last composed-guard refusal since a publication.
    refused: Option<Vec<acyclic_stream::CommitConflict>>,
    /// This resident actor's interest marker is durable (or not needed).
    interests_marked: bool,
}

impl Inner {
    /// Every attempt touches storage, so a guard refusal is retried only on
    /// evidence of an external change: the identical refusal again means
    /// nothing changed, and the retry loop would never append or wait.
    fn refused(&mut self, conflicts: Vec<acyclic_stream::CommitConflict>) -> Result<(), Error> {
        // Preparation changes carry no comparable source observation.
        if conflicts.is_empty() {
            self.refused = None;
            return Ok(());
        }
        if self.refused.take().is_some_and(|last| last == conflicts) {
            return Err(Error::PreconditionUnchanged);
        }
        self.refused = Some(conflicts);
        Ok(())
    }
}

/// Only a decided refusal can be replaced with a failure transition. Capacity
/// is backpressure; an unknown acknowledgement retains its original publication.
fn terminal(error: &Error) -> bool {
    match error {
        Error::Stream(error) => {
            !error.uncertain() && !matches!(error.category(), StreamError::Capacity)
        }
        Error::PreconditionUnchanged => true,
        _ => false,
    }
}

enum Event {
    ExternalRegistered(external::Watch),
    External(external::Outcome),
    Created(u16, Record),
    Dispatched(u16, Result<Snapshot, Error>),
    Work(usize, Work<ActorReference>),
    Done(Result<(), (ActorReference, Error)>),
    Rearm(Option<u64>),
    Due(Result<(), ClockError>),
    Armed(Vec<(u64, ActorReference)>),
    /// Every consumer's deliveries settled while a checkpoint waited.
    Quiet,
    Opened(Result<Opened, Error>),
    /// One page of a shard's interest holders, and where the next starts.
    Restored(Result<(u16, Vec<ActorReference>, Option<StreamPath>), Error>),
    Failed(StreamError),
    Completion(Completion),
    CompletionCancelled,
    Idle,
    Subscription(Result<(), (Vec<ActorReference>, StreamError)>),
}

/// One page of a due deadline window's actors, read off the dispatcher's
/// event loop so in-flight deliveries keep progressing while it is read.
struct Opened {
    shard: u16,
    window: u64,
    actors: Vec<ActorReference>,
    next: Next,
}

/// What follows an [`Opened`] page.
enum Next {
    /// The window's next entry.
    Page(u64),
    /// The page was the window's last: the shard's next deadline window.
    Window(Option<u64>),
}

enum DrainEvent {
    Work(usize, Work<ActorReference>),
    Done(Result<(), (ActorReference, Error)>),
    Subscription(Result<(), (Vec<ActorReference>, StreamError)>),
    Idle,
}

/// Executes one handler package for every actor whose state lives under the
/// store's root and that it claims, including actors created by other
/// parties' commits.
pub struct Runtime<P: StreamProvider + ?Sized> {
    store: Store<P>,
    /// Discovery scope only. Per-actor placement, not this filter, grants
    /// ownership when several hosts share the same state root.
    scope: Option<(AccountId, NamespaceId)>,
    scheduling_shards: BTreeSet<u16>,
    handler: Arc<dyn Handler>,
    clock: Arc<dyn Clock>,
    ready: [ReadySet<ActorReference>; 2],
    actors: [SyncMutex<HashMap<ActorReference, Arc<Slot>>>; 64],
    shards: RandomState,
    /// Host node incarnation retained in explicit placement commands.
    instance: uuid::Uuid,
    /// Whether this runtime is its store's local placement owner.
    places: bool,
    limits: RuntimeLimits,
    admission: [Option<tokio::sync::Semaphore>; 2],
    completions: OnceLock<tokio::sync::mpsc::Sender<Completion>>,
    subscriptions: SyncMutex<subscriptions::Subscriptions>,
    requests: crate::guest::Requests,
    /// Set once this host has paged every interest marker of its shards.
    restored: tokio::sync::watch::Sender<bool>,
    external: external::Registry,
}

impl<P: StreamProvider + ?Sized> Runtime<P> {
    /// One runtime per state root and handler package.
    pub fn new(store: Store<P>, handler: Arc<dyn Handler>, clock: Arc<dyn Clock>) -> Self {
        let shards = store.scheduling_shards;
        Self {
            scheduling_shards: (0..store.scheduling_shards()).collect(),
            // Each shard's deadline watermark is held only by its own alarms.
            ready: std::array::from_fn(|_| {
                ReadySet::grouped(move |actor| scheduling::shard(shards, actor))
            }),
            store,
            scope: None,
            handler,
            clock,
            actors: std::array::from_fn(|_| SyncMutex::new(HashMap::new())),
            shards: RandomState::new(),
            instance: crate::identity::fresh_uuid(),
            places: true,
            limits: RuntimeLimits::default(),
            admission: [None, None],
            completions: OnceLock::new(),
            subscriptions: SyncMutex::new(subscriptions::Subscriptions::default()),
            requests: crate::guest::Requests::default(),
            restored: tokio::sync::watch::Sender::new(false),
            external: external::Registry::default(),
        }
    }

    /// Wait until the running host has woken every dormant interest holder
    /// of its shards. Serving never waits on this: restoration pages in the
    /// background through ordinary admission, and its woken deliveries are
    /// queued work that [`Self::complete`] then covers.
    pub async fn restored(&self) -> Result<(), Error> {
        self.restored
            .subscribe()
            .wait_for(|restored| *restored)
            .await
            .map(|_| ())
            .map_err(|_| Error::CompletionStopped)
    }

    /// The live `request -> (actor, epoch)` bindings of this runtime's handler
    /// attempts: the sole authority of a host capability call.
    pub const fn requests(&self) -> &crate::guest::Requests {
        &self.requests
    }

    /// Configure admission before sharing or running this host. Event-owned
    /// deliveries and continuous dispatch use the same admission bound.
    #[must_use]
    pub fn with_limits(mut self, limits: RuntimeLimits) -> Self {
        self.admission = limits
            .active
            .map(|capacity| capacity.map(tokio::sync::Semaphore::new));
        self.limits = limits;
        self
    }

    /// Restrict discovery to an account and namespace. This grants no ownership:
    /// only an explicit per-actor placement command admits execution.
    #[must_use]
    pub fn within(mut self, within: &ActorReference) -> Self {
        self.scope = Some((within.account().clone(), within.namespace().clone()));
        self
    }

    /// Accept ownership only from explicit [Self::place] commands, as a node
    /// among several sharing one state root. By default a runtime is the
    /// single local placement owner of its store, which holds the store's
    /// exclusive claim, and issues each resident actor's next command itself.
    #[must_use]
    pub const fn with_external_placement(mut self) -> Self {
        self.places = false;
        self
    }

    /// Select the stable scheduling partitions assigned by placement before
    /// starting this host. Local single-node hosts own all partitions by
    /// default. This routing assignment never grants a per-actor epoch. A
    /// partition outside the store's count would follow a log no producer
    /// writes, so it is refused before the dispatcher starts.
    pub fn with_scheduling_shards(
        mut self,
        shards: impl IntoIterator<Item = u16>,
    ) -> Result<Self, Error> {
        let count = self.store.scheduling_shards();
        self.scheduling_shards = shards.into_iter().collect();
        if self.scheduling_shards.iter().any(|&shard| shard >= count) {
            return Err(Error::Invalid(
                "scheduling shard outside the store's partitions",
            ));
        }
        Ok(self)
    }

    /// Whether an actor belongs to this runtime's discovery scope.
    pub fn includes(&self, actor: &ActorReference) -> bool {
        self.scheduling_shards
            .contains(&self.store.scheduling_shard(actor))
            && self.scope.as_ref().is_none_or(|(account, namespace)| {
                actor.account() == account && actor.namespace() == namespace
            })
    }

    /// The shared store.
    pub const fn store(&self) -> &Store<P> {
        &self.store
    }

    /// Exact resident registration and ready counts.
    pub fn counts(&self) -> (usize, usize) {
        self.ready
            .iter()
            .map(ReadySet::counts)
            .fold((0, 0), |total, next| (total.0 + next.0, total.1 + next.1))
    }

    fn wake_due(&self, now: u64) -> usize {
        self.ready
            .iter()
            .map(|ready| ready.wake_due(now, self.limits.work))
            .sum()
    }

    /// Register a dormant slot. Registration queues no work and grants no ownership.
    /// Unassigned actors cannot claim themselves when input arrives.
    pub fn track(&self, actor: ActorReference) -> Result<(), Error> {
        if !self.includes(&actor) {
            return Err(Error::Invalid("actor is outside this runtime's scope"));
        }
        let mut actors = self
            .shard(&actor)
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if actors.contains_key(&actor) {
            return Ok(());
        }
        let registration =
            self.ready[self.handler.admission_class(&actor).index()].register(actor.clone())?;
        let waker = registration.waker();
        actors.insert(
            actor,
            Arc::new(Slot {
                registration,
                waker,
                inner: tokio::sync::Mutex::new(Inner::default()),
                refresh: AtomicBool::new(false),
                entry: AtomicU64::new(0),
                evict: AtomicBool::new(false),
                event_deliveries: AtomicUsize::new(0),
                control: tokio::sync::Mutex::new(()),
                delivery_control: SyncMutex::new(DeliveryControl::default()),
            }),
        );
        Ok(())
    }

    /// Register an actor and request its first delivery attempt, as after
    /// reopening a store whose committed input this host must now serve.
    pub fn host(&self, actor: ActorReference) -> Result<(), Error> {
        self.track(actor.clone())?;
        self.wake(&actor);
        Ok(())
    }

    /// Stop hosting an idle actor (hibernate). Returns false while a delivery
    /// control or an unknown publication still owns participant custody; wake it to
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
        if inner.pending.is_some()
            || slot
                .delivery_control
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .failure
                .is_some()
        {
            return false;
        }
        inner.released = true;
        drop(inner);
        actors.remove(actor);
        self.subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .release(actor);
        if !self.handler.external_subscriptions().is_empty() {
            self.external.remove(actor);
        }
        true
    }

    /// Request a delivery attempt, for example after a transient failure.
    pub fn wake(&self, actor: &ActorReference) {
        if let Some(slot) = self.slot(actor) {
            slot.refresh.store(true, Ordering::Release);
            slot.waker.wake_by_ref();
        }
    }

    /// Durably pause or resume. Pause closes admission, lets the active delivery
    /// finish and settles its retained publication before parking resources.
    /// Once admitted, the operation outlives its waiter. An uncertain publication
    /// remains with the same slot. Resume reopens admission and wakes it.
    /// A changed state cut returns [`Error::Conflict`] without another attempt.
    pub async fn pause(&self, actor: &ActorReference, paused: bool) -> Result<(), Error> {
        self.pause_with(actor, paused, std::future::ready(())).await
    }

    /// Apply pause/resume and finish the original owner's resource transition
    /// under the same delivery/control fence. A concurrent resume cannot pass
    /// a resource parking operation that has not yet completed. The future
    /// must own its resources: losing the waiter does not cancel the decision
    /// or transition. An abandoned error, including its exact Unknown request,
    /// is retained in the placement and returned to the next control caller.
    pub async fn pause_with<T>(
        &self,
        actor: &ActorReference,
        paused: bool,
        transition: impl std::future::Future<Output = T> + acyclic_stream::ProviderTask + 'static,
    ) -> Result<T, Error>
    where
        T: acyclic_stream::ProviderTask + 'static,
    {
        acyclic_observability::async_operation!(
            "acyclic.actors.pause",
            Error::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.track(actor.clone())?;
                let placement = self.pin(actor)?.ok_or(Error::Invalid(
                    "actor placement retired during pause admission",
                ))?;
                pause::start(
                    self.store.clone(),
                    actor.clone(),
                    placement,
                    self.places.then_some(placement::Owner {
                        node: self.instance,
                        places: true,
                    }),
                    paused,
                    transition,
                )
                .await
            }
        )
    }

    // Pin under the same table lock as hibernation before awaiting delivery
    // or control. The original event counter prevents a second live slot.
    fn pin(&self, actor: &ActorReference) -> Result<Option<EventDelivery>, Error> {
        let actors = self
            .shard(actor)
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(slot) = actors.get(actor) else {
            return Ok(None);
        };
        slot.event_deliveries
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .map_err(|_| Error::Invalid("event delivery count overflow"))?;
        Ok(Some(EventDelivery(Arc::clone(slot))))
    }

    /// Run every currently ready or due actor to quiescence, then return the
    /// failures. Independent actors share the same active admission as `run`
    /// and event delivery; no task is created for a dormant actor.
    pub async fn drain(&self) -> Vec<(ActorReference, Error)> {
        let now = self.clock.now_unix_ms();
        while self.wake_due(now) >= self.limits.work {
            limits::yield_now().await;
        }
        let mut failures = Vec::new();
        let mut running: [_; 2] = std::array::from_fn(|_| FuturesUnordered::new());
        let mut next_done = 0;
        let mut next_work = 0;
        let mut steps = 0;
        loop {
            for step in 0..self.limits.work {
                let mut admitted = false;
                for _ in 0..2 {
                    let class = next_work;
                    next_work = (next_work + 1) % 2;
                    if self.limits.allows(class, running[class].len())
                        && let Some(work) = self.ready[class].take()
                    {
                        running[class].push(self.execute(work));
                        admitted = true;
                        break;
                    }
                }
                if !admitted {
                    break;
                }
                if step + 1 == self.limits.work {
                    limits::yield_now().await;
                }
            }
            let result = poll_fn(|context| {
                for _ in 0..3 {
                    let class = next_done;
                    next_done = (next_done + 1) % 3;
                    if class == 2 {
                        if let Poll::Ready(result) = self.poll_subscriptions(context) {
                            return Poll::Ready(DrainEvent::Subscription(result));
                        }
                        continue;
                    }
                    if let Poll::Ready(Some(result)) = running[class].poll_next_unpin(context) {
                        return Poll::Ready(DrainEvent::Done(result));
                    }
                }
                for _ in 0..2 {
                    let class = next_work;
                    next_work = (next_work + 1) % 2;
                    if self.limits.allows(class, running[class].len())
                        && let Poll::Ready(work) = self.ready[class].poll_ready(context)
                    {
                        return Poll::Ready(DrainEvent::Work(class, work));
                    }
                }
                if running.iter().all(FuturesUnordered::is_empty) {
                    Poll::Ready(DrainEvent::Idle)
                } else {
                    Poll::Pending
                }
            })
            .await;
            match result {
                DrainEvent::Done(Err(failure)) => failures.push(failure),
                DrainEvent::Done(Ok(())) => {}
                DrainEvent::Subscription(Ok(())) => {}
                DrainEvent::Subscription(Err((actors, error))) => {
                    failures.extend(
                        actors
                            .into_iter()
                            .map(|actor| (actor, Error::Stream(error.clone()))),
                    );
                }
                DrainEvent::Work(class, work) => running[class].push(self.execute(work)),
                DrainEvent::Idle => break,
            }
            steps += 1;
            if steps == self.limits.work {
                limits::yield_now().await;
                steps = 0;
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
                let delivery = self
                    .pin(actor)?
                    .ok_or(Error::Invalid("actor is not tracked"))?;
                let result = loop {
                    match self.deliver(actor, &delivery.0, None).await {
                        Ok(true) => break Ok(()),
                        Ok(false) => {
                            // Drop both locks before queueing the next finite batch.
                            limits::yield_now().await;
                        }
                        Err(error) => break Err(error),
                    }
                };
                let evict = delivery.0.evict.load(Ordering::Acquire);
                drop(delivery);
                if matches!(result, Err(Error::Conflict)) || (result.is_ok() && evict) {
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
                if self.slot(actor).is_some() && !self.release(actor) {
                    return Err(Error::Invalid("actor still owns an unsettled delivery"));
                }
                Ok(alarm)
            }
        )
    }

    /// Complete activation and due-alarm work through the original [`Self::run`]
    /// dispatcher. Future alarms are not advanced. This neither executes a
    /// second scheduler nor places actors on the caller's behalf.
    ///
    /// At an idle boundary the dispatcher captures the activation tail, consumes
    /// that prefix and its deliveries, then confirms the resulting tail. Each
    /// changed cut waits on the original activation follow; there is no polling.
    /// Concurrent producers may append after the final observation. Cancellation
    /// or loss of the running host returns [`Error::CompletionStopped`], as does
    /// calling before [`Self::run`] creates its dispatcher future. A failed delivery
    /// refuses completion until the host is restarted.
    /// One handoff credit backpressures requests; cancellation retires the
    /// original request without waiting for a delivery or an idle cut.
    pub async fn complete(&self) -> Result<(), Error> {
        let completions = self.completions.get().ok_or(Error::CompletionStopped)?;
        let (send, receive) = tokio::sync::oneshot::channel();
        completions
            .send(send)
            .await
            .map_err(|_| Error::CompletionStopped)?;
        receive
            .await
            .map_err(|_| Error::CompletionStopped)?
            .map_err(Error::CompletionFailed)
    }

    /// Host forever: follow assigned scheduling shards to wake admitted actors
    /// created by any commit, deliver concurrently across actors, and fire
    /// alarms from one timer. Per-actor failures go to `report`; the input
    /// stays queued. Creating this future claims its completion producer;
    /// dropping it, including before the first poll, refuses pending and late
    /// completion requests with [`Error::CompletionStopped`].
    pub fn run(
        &self,
        report: impl Fn(&ActorReference, &Error),
    ) -> impl std::future::Future<Output = Result<Infallible, Error>> {
        // Transfer acknowledgement custody when the future is CREATED: dropping
        // it must refuse observers even if the executor never polls it.
        let mut completions = Err(Error::CompletionStopped);
        self.completions.get_or_init(|| {
            // One handoff credit: a backpressured caller retains its request,
            // so cancelling it cannot leave unbounded queued acknowledgements.
            let (send, receive) = tokio::sync::mpsc::channel(1);
            completions = Ok(receive);
            send
        });
        self.run_owned(report, completions)
    }

    async fn run_owned(
        &self,
        report: impl Fn(&ActorReference, &Error),
        completions: Result<tokio::sync::mpsc::Receiver<Completion>, Error>,
    ) -> Result<Infallible, Error> {
        let mut completions = completions?;
        let _external = self.external.running();
        let mut completing = Completions::default();
        let mut completion_tail = BTreeMap::new();
        let mut failure = None;
        let provider = self.store.provider();
        // Placement admits resident work explicitly. Never rebuild ownership
        // by enumerating every dormant actor under the state root.
        if self.scheduling_shards.is_empty() {
            return Err(Error::Invalid(
                "dispatcher has no assigned scheduling shards",
            ));
        }
        let mut seen = BTreeMap::new();
        let mut stores = BTreeMap::new();
        let mut dispatchers = BTreeMap::new();
        let mut created = futures::stream::SelectAll::new();
        // Every shard checkpoint loads in one snapshot request.
        let mut assigned = Vec::with_capacity(self.scheduling_shards.len());
        for &shard in &self.scheduling_shards {
            assigned.push(self.dispatcher_store(shard)?);
        }
        let actors: Vec<_> = assigned.iter().map(|(_, actor)| actor.clone()).collect();
        let loaded = assigned[0].0.load_all(&actors).await?;
        // Each shard's earliest pending deadline window past its consumed
        // watermark: one bounded children page per assigned shard.
        let mut consumed = BTreeMap::new();
        let mut windows = BTreeSet::new();
        // From here on a hibernating resident parks its alarm token in the
        // ready index, which holds the deadline watermark at its window.
        for ready in &self.ready {
            ready.dispatch();
        }
        let earliest =
            futures::future::try_join_all(self.scheduling_shards.iter().zip(&loaded).map(
                |(&shard, dispatcher)| {
                    self.store
                        .next_window(shard, dispatcher::deadline_cursor(dispatcher))
                },
            ))
            .await?;
        for ((&shard, dispatcher), next) in self.scheduling_shards.iter().zip(&loaded).zip(earliest)
        {
            consumed.insert(shard, dispatcher::deadline_cursor(dispatcher));
            windows.extend(next.map(|window| (window, shard)));
        }
        for ((&shard, (store, _)), dispatcher) in
            self.scheduling_shards.iter().zip(assigned).zip(loaded)
        {
            let from = dispatcher::activation_cursor(&dispatcher);
            stores.insert(shard, store);
            dispatchers.insert(shard, Some(dispatcher));
            seen.insert(shard, from);
            completion_tail.insert(shard, from);
            let mut request =
                acyclic_stream::FollowRequest::new(self.store.scheduling_path(shard)?, from);
            request.when_absent = acyclic_stream::WhenAbsent::Wait;
            let records = acyclic_stream::records_only(provider.follow(request).await?);
            created.push(records.map(move |record| (shard, record)));
        }
        let mut dispatching: FuturesUnordered<
            BoxProviderFuture<'_, (u16, Result<Snapshot, Error>)>,
        > = FuturesUnordered::new();
        let mut opening: FuturesUnordered<BoxProviderFuture<'_, Result<Opened, Error>>> =
            FuturesUnordered::new();
        // A placing host restores dormant interest holders on their own
        // credit: one page read in flight, whatever delivery work is queued.
        // Their wakes join the same ready index and admission as every
        // delivery, so a backlogged actor never stalls the remaining pages.
        let mut restoring: Option<
            BoxProviderFuture<'_, Result<(u16, Vec<ActorReference>, Option<StreamPath>), Error>>,
        > = None;
        let mut restore: Vec<(u16, Option<StreamPath>)> = if self.places {
            self.scheduling_shards
                .iter()
                .map(|&shard| (shard, None))
                .collect()
        } else {
            Vec::new()
        };
        // Windows whose reads are in flight, as `(shard, window)`: a shard's
        // watermark never passes one of its windows still being read.
        let mut reading = BTreeSet::new();
        // The activation log is also every producer's wake entry: the cursor
        // only bounds restart replay, so consecutive entries coalesce into one
        // checkpoint in flight at the latest seen end.
        let mut running: [_; 2] = std::array::from_fn(|_| FuturesUnordered::new());
        let mut next_done = 0;
        let mut next_work = 0;
        let mut sources = FuturesUnordered::new();
        let mut held = false;
        let mut timer: Option<(u64, BoxProviderFuture<'static, Result<(), ClockError>>)> = None;
        let mut next_class = 0;
        let mut steps = 0;
        loop {
            if steps == self.limits.work {
                limits::yield_now().await;
                steps = 0;
            }
            if restoring.is_none() {
                if restore.is_empty() {
                    // The last page's wakes are already queued work.
                    self.restored
                        .send_if_modified(|restored| !std::mem::replace(restored, true));
                } else if let Some((shard, after)) = restore.pop() {
                    restoring = Some(Box::pin(async move {
                        let (actors, next) = self.store.interest_holders(shard, after).await?;
                        Ok((shard, actors, next))
                    }));
                }
            }
            let event = poll_fn(|context: &mut Context<'_>| {
                // Alarm changes must wake us even when active admission is
                // full and the ready queue cannot currently be consumed.
                for ready in &self.ready {
                    ready.listen(context);
                }
                if held
                    && running.iter().all(FuturesUnordered::is_empty)
                    && !self.ready.iter().any(ReadySet::busy)
                {
                    return Poll::Ready(Event::Quiet);
                }
                for _ in 0..7 {
                    let class = next_class;
                    next_class = (next_class + 1) % 7;
                    match class {
                        0 => {
                            // Provider I/O owned by an active delivery must keep
                            // progressing during dispatcher checkpoint writes.
                            for _ in 0..2 {
                                let class = next_done;
                                next_done = (next_done + 1) % 2;
                                if let Poll::Ready(Some(result)) =
                                    running[class].poll_next_unpin(context)
                                {
                                    return Poll::Ready(Event::Done(result));
                                }
                            }
                        }
                        1 => {
                            if let Poll::Ready(Some((shard, result))) =
                                dispatching.poll_next_unpin(context)
                            {
                                return Poll::Ready(Event::Dispatched(shard, result));
                            }
                            if let Poll::Ready(Some(opened)) = opening.poll_next_unpin(context) {
                                return Poll::Ready(Event::Opened(opened));
                            }
                            if let Some(page) = restoring.as_mut()
                                && let Poll::Ready(page) = page.poll_unpin(context)
                            {
                                restoring = None;
                                return Poll::Ready(Event::Restored(page));
                            }
                            match created.poll_next_unpin(context) {
                                Poll::Ready(Some((shard, Ok(record)))) => {
                                    return Poll::Ready(Event::Created(shard, record));
                                }
                                Poll::Ready(Some((_, Err(error)))) => {
                                    return Poll::Ready(Event::Failed(error));
                                }
                                Poll::Ready(None) => {
                                    return Poll::Ready(Event::Failed(StreamError::Unavailable));
                                }
                                Poll::Pending => {}
                            }
                        }
                        2 => {
                            for _ in 0..2 {
                                let class = next_work;
                                next_work = (next_work + 1) % 2;
                                if self.limits.allows(class, running[class].len())
                                    && let Poll::Ready(work) = self.ready[class].poll_ready(context)
                                {
                                    return Poll::Ready(Event::Work(class, work));
                                }
                            }
                        }
                        3 => {
                            // Read the deadline after installing the waiter: an earlier
                            // alarm installed concurrently wakes us to rearm.
                            let armed = self
                                .ready
                                .iter()
                                .flat_map(ReadySet::take_armed)
                                .collect::<Vec<_>>();
                            if !armed.is_empty() {
                                return Poll::Ready(Event::Armed(armed));
                            }
                            // Only a placing host can host a parked actor again.
                            let opens = windows
                                .first()
                                .map(|&(window, _)| scheduling::window_start(window))
                                .into_iter()
                                .chain(
                                    self.ready
                                        .iter()
                                        .filter_map(ReadySet::next_parked_wake)
                                        .min()
                                        .filter(|_| self.places),
                                )
                                .min();
                            let next = match (
                                self.ready.iter().filter_map(ReadySet::next_deadline).min(),
                                opens,
                            ) {
                                (Some(resident), Some(opens)) => Some(resident.min(opens)),
                                (resident, opens) => resident.or(opens),
                            };
                            if next != timer.as_ref().map(|(deadline, _)| *deadline) {
                                return Poll::Ready(Event::Rearm(next));
                            }
                            if let Some((_, sleep)) = timer.as_mut()
                                && let Poll::Ready(result) = sleep.poll_unpin(context)
                            {
                                return Poll::Ready(Event::Due(result));
                            }
                        }
                        4 => {
                            if completing.poll_cancelled(context).is_ready() {
                                return Poll::Ready(Event::CompletionCancelled);
                            }
                            if let Poll::Ready(Some(completion)) = completions.poll_recv(context) {
                                return Poll::Ready(Event::Completion(completion));
                            }
                        }
                        5 => {
                            if let Poll::Ready(watch) = self.external.poll_registered(context) {
                                return Poll::Ready(Event::ExternalRegistered(watch));
                            }
                            if let Poll::Ready(Some(outcome)) = sources.poll_next_unpin(context) {
                                return Poll::Ready(Event::External(outcome));
                            }
                        }
                        _ => {
                            if let Poll::Ready(result) = self.poll_subscriptions(context) {
                                return Poll::Ready(Event::Subscription(result));
                            }
                        }
                    }
                }
                if !completing.is_empty()
                    && running.iter().all(FuturesUnordered::is_empty)
                    && !self.ready.iter().any(ReadySet::busy)
                    && dispatching.is_empty()
                    && opening.is_empty()
                    && completion_tail
                        .iter()
                        .all(|(shard, tail)| seen.get(shard).is_some_and(|cursor| cursor >= tail))
                {
                    return Poll::Ready(Event::Idle);
                }
                Poll::Pending
            })
            .await;
            steps += 1;
            match event {
                Event::ExternalRegistered(watch) => {
                    sources.push(watch.next(provider));
                }
                Event::External(outcome) => {
                    outcome.result?;
                    if let Some(watch) = outcome.watch {
                        sources.push(watch.next(provider));
                    }
                }
                Event::Created(shard, record) => {
                    // Activation is a wake hint, not a placement command. The
                    // local placement owner hosts its shard's actors on demand;
                    // under external placement only admitted residents wake.
                    let cursor = record
                        .sequence
                        .checked_add(1)
                        .ok_or(Error::Invalid("activation cursor overflow"))?;
                    if let Ok(actor) = ActorReference::decode(record.value.as_ref())
                        && self.store.scheduling_shard(&actor) == shard
                        && self.includes(&actor)
                    {
                        if self.places {
                            self.track(actor.clone())?;
                        }
                        if let Some(slot) = self.slot(&actor) {
                            slot.entry.fetch_max(cursor, Ordering::AcqRel);
                        }
                        self.wake(&actor);
                    }
                    seen.insert(shard, cursor);
                }
                Event::Dispatched(shard, result) => {
                    dispatchers.insert(shard, Some(result?));
                }
                Event::Work(class, work) => {
                    let execution: BoxProviderFuture<'_, _> = Box::pin(self.execute(work));
                    running[class].push(execution);
                }
                Event::Done(Err((actor, error))) => {
                    report(&actor, &error);
                    failure.get_or_insert_with(|| Arc::new(error));
                }
                Event::Done(Ok(())) | Event::Quiet => {}
                Event::Armed(armed) => {
                    // A newly armed alarm's window is learned as it is armed,
                    // so its opening advances the watermark even when startup
                    // found no window.
                    for (deadline, actor) in armed {
                        let shard = self.store.scheduling_shard(&actor);
                        if let Some(&from) = consumed.get(&shard) {
                            scheduling::learn(
                                &mut windows,
                                from,
                                shard,
                                scheduling::window(deadline),
                            );
                        }
                    }
                }
                Event::Rearm(next) => {
                    timer = next.map(|deadline| (deadline, self.clock.sleep_until(deadline)));
                }
                Event::Due(result) => {
                    result?;
                    timer = None;
                    let now = self.clock.now_unix_ms();
                    // Hosting a parked actor again moves its token into its
                    // registration, which this same pass then wakes.
                    if self.places {
                        for actor in self
                            .ready
                            .iter()
                            .flat_map(|ready| ready.parked_due(now, self.limits.work))
                        {
                            self.track(actor.clone())?;
                            if let Some(slot) = self.slot(&actor) {
                                slot.registration.adopt_parked();
                            }
                            self.wake(&actor);
                        }
                    }
                    self.wake_due(now);
                    // An opened window wakes its dormant actors, page by page;
                    // each delivery installs its exact committed deadline.
                    for (shard, window) in
                        scheduling::open(&mut windows, &mut reading, &consumed, now)
                    {
                        opening.push(Box::pin(self.open_window(shard, window, 0)));
                    }
                }
                Event::Opened(opened) => {
                    let Opened {
                        shard,
                        window,
                        actors,
                        next,
                    } = opened?;
                    for actor in actors {
                        if self.store.scheduling_shard(&actor) == shard && self.includes(&actor) {
                            if self.places {
                                self.track(actor.clone())?;
                            }
                            // The resident takes the filed alarm over before
                            // the window can be passed.
                            if let Some(slot) = self.slot(&actor) {
                                slot.registration
                                    .claim_alarm(scheduling::window_start(window));
                            }
                            self.wake(&actor);
                        }
                    }
                    match next {
                        Next::Page(from) => {
                            opening.push(Box::pin(self.open_window(shard, window, from)))
                        }
                        Next::Window(next) => scheduling::read(
                            &mut reading,
                            &mut consumed,
                            &mut windows,
                            (shard, window),
                            next,
                        )?,
                    }
                }
                Event::Restored(page) => {
                    let (shard, actors, next) = page?;
                    for actor in actors {
                        if self.store.scheduling_shard(&actor) == shard && self.includes(&actor) {
                            self.track(actor.clone())?;
                            self.wake(&actor);
                        }
                    }
                    if next.is_some() {
                        restore.push((shard, next));
                    }
                }
                Event::Failed(error) => return Err(error.into()),
                Event::Completion(completion) => completing.insert(completion),
                Event::CompletionCancelled => {}
                Event::Subscription(Ok(())) => {}
                Event::Subscription(Err((actors, error))) => {
                    let error = Error::Stream(error);
                    for actor in actors {
                        report(&actor, &error);
                    }
                    failure.get_or_insert_with(|| Arc::new(error));
                }
                Event::Idle => {
                    // No delivery can publish while this owner captures the cut.
                    // A newly observed prefix resumes the SAME activation follow.
                    // One snapshot of every assigned shard log: a single
                    // control request regardless of the shard count.
                    let request = acyclic_stream::HeadsRequest {
                        retained: Default::default(),
                        durable_through: None,
                        projection: acyclic_stream::HeadProjection::Tail,
                        paths: self
                            .scheduling_shards
                            .iter()
                            .map(|&shard| self.store.scheduling_path(shard))
                            .collect::<Result<_, _>>()?,
                        children: None,
                        limit: u32::try_from(self.scheduling_shards.len())
                            .map_err(|_| Error::Invalid("too many scheduling shards"))?,
                        at_least: Default::default(),
                    };
                    let view = provider.heads(request.clone()).await?;
                    view.validate_for(&request)?;
                    for (&shard, head) in self.scheduling_shards.iter().zip(view.heads) {
                        completion_tail.insert(shard, head.map_or(0, |head| head.tail));
                    }
                    if completion_tail
                        .iter()
                        .all(|(shard, tail)| seen.get(shard).is_some_and(|cursor| cursor >= tail))
                        && self.counts().1 == 0
                        && timer
                            .as_ref()
                            .map(|(deadline, _)| *deadline)
                            .is_none_or(|deadline| deadline > self.clock.now_unix_ms())
                    {
                        completing.finish(failure.as_ref());
                    }
                }
            }
            // A checkpoint may pass an activation entry only after its wake
            // finished: every seen entry was delivered to quiescence, so a
            // restart replays exactly the entries that may still own work.
            // A failed actor keeps its own latest entry, or the checkpoint
            // commits a fresh one for it (`scheduling::checkpoint`); either
            // way only that actor's wake is retained, never its whole shard.
            // A pending alarm lives in its deadline window, so the activation
            // cursor never waits for it. The window watermark passes only
            // windows whose wakes were delivered, never the current window
            // (an alarm armed now with a future deadline lands at or after
            // it), and never the original window of a resident alarm still
            // unconsumed, however late an authority deferred its wake; it
            // moves back only for a resident alarm armed already past due.
            // Every alarm token, resident or parked, holds it at its window.
            // In-flight and failed deliveries are counted once, in the shared
            // ready index, for every consumer (this loop, `drain`, direct
            // deliveries), so no local view can let a checkpoint pass one.
            held = self.ready.iter().any(ReadySet::busy);
            if held {
                continue;
            }
            let now = self.clock.now_unix_ms();
            for (&shard, previous) in &mut dispatchers {
                let Some(checkpoint) = previous.as_ref() else {
                    continue;
                };
                let from = dispatcher::activation_cursor(checkpoint);
                let failed = self
                    .ready
                    .iter()
                    .flat_map(|ready| ready.failed(shard))
                    .collect::<Vec<_>>();
                let holds: Vec<_> = failed
                    .iter()
                    .map(|actor| {
                        self.slot(actor)
                            .map(|slot| slot.entry.load(Ordering::Acquire))
                            .filter(|&entry| entry > from)
                            .map(|entry| entry - 1)
                    })
                    .collect();
                let (cursor, carry) = scheduling::checkpoint(seen[&shard], &holds);
                let deadline = consumed[&shard].min(
                    self.ready
                        .iter()
                        .map(|ready| ready.watermark(shard, now))
                        .min()
                        .unwrap_or(now),
                );
                if let Some(previous) = previous.take_if(|previous| {
                    from < cursor || dispatcher::deadline_cursor(previous) != deadline
                }) {
                    let carried = if carry { failed } else { Vec::new() };
                    for actor in &carried {
                        // Its fresh entry lands at or past the cursor.
                        match self.slot(actor) {
                            Some(slot) => {
                                slot.entry
                                    .fetch_max(cursor.saturating_add(1), Ordering::AcqRel);
                            }
                            // Released: this one entry is its handoff, which
                            // the next placer's wake owns; the mark ends here.
                            None => self.ready[self.handler.admission_class(actor).index()]
                                .forget(actor),
                        }
                    }
                    let store = &stores[&shard];
                    dispatching.push(Box::pin(async move {
                        (
                            shard,
                            self.advance_dispatcher(
                                store, previous, shard, cursor, deadline, carried,
                            )
                            .await,
                        )
                    }));
                }
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

    /// Read one page of a due window from entry `from`; after its last page,
    /// the shard's next window past it.
    async fn open_window(&self, shard: u16, window: u64, from: u64) -> Result<Opened, Error> {
        let (actors, entry) = self.store.window_actors(shard, window, from).await?;
        let next = match entry {
            Some(entry) => Next::Page(entry),
            None => Next::Window(
                self.store
                    .next_window(
                        shard,
                        window
                            .checked_add(1)
                            .ok_or(Error::Invalid("alarm window overflow"))?,
                    )
                    .await?,
            ),
        };
        Ok(Opened {
            shard,
            window,
            actors,
            next,
        })
    }

    async fn execute(&self, work: Work<ActorReference>) -> Result<(), (ActorReference, Error)> {
        let actor = work.key().clone();
        // Released before it ran: the work drops unsettled, so its input
        // keeps the failure mark and the one carried entry it hands off.
        let Some(slot) = self.slot(&actor) else {
            return Ok(());
        };
        let result = self.deliver(&actor, &slot, Some(work)).await;
        if let Err(Error::Conflict) = result {
            // Another placement owns the actor now; stop hosting it here.
            self.release(&actor);
        }
        result.map(|_| ()).map_err(|error| (actor, error))
    }

    /// Deliver one bounded batch. False retains a wake for the next batch.
    async fn deliver(
        &self,
        actor: &ActorReference,
        slot: &Arc<Slot>,
        work: Option<Work<ActorReference>>,
    ) -> Result<bool, Error> {
        acyclic_observability::async_operation!(
            "acyclic.actors.deliver",
            Error::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                // Duplicate events for one actor wait outside global admission. Only
                // an actor that can make progress may occupy an active delivery slot.
                let mut inner = slot.inner.lock().await;
                // The delivery starts once it owns the actor: a waiter owns
                // nothing and settles nothing. From here it counts in flight
                // in the shared ready index, whichever consumer started it,
                // until it settles; a failed or unfinished one keeps its
                // actor's wake past every later checkpoint.
                let mut settlement = Settlement {
                    slot,
                    flight: Some(
                        self.ready[self.handler.admission_class(actor).index()].flight(actor),
                    ),
                    work,
                    failed: true,
                };
                let result = match &self.admission[self.handler.admission_class(actor).index()] {
                    Some(admission) => admission
                        .acquire()
                        .await
                        .map(Some)
                        .map_err(|_| Error::Invalid("runtime admission is closed")),
                    None => Ok(None),
                };
                let result = match result.and_then(|admission| {
                    self.handler.manifest().validate_interrupts()?;
                    self.handler
                        .manifest()
                        .validate_external(self.handler.external_subscriptions())?;
                    self.handler
                        .manifest()
                        .validate_external(self.handler.ordered_subscriptions())?;
                    Ok(admission)
                }) {
                    Ok(_admission) => self.deliver_retained(actor, slot, &mut inner).await,
                    Err(error) => Err(error),
                };
                // Every failure keeps a wake, a conflict's included: one that
                // releases the actor leaves its single carried entry behind.
                // Settled under the actor's delivery lock, so an earlier
                // delivery's success never clears a later one's failure.
                // A released slot delivered nothing: its input belongs to
                // the next placer.
                settlement.failed = result.is_err() || inner.released;
                drop(settlement);
                drop(inner);
                if result.is_ok() && slot.evict.load(Ordering::Acquire) && self.release(actor) {
                    slot.evict.store(false, Ordering::Release);
                }
                result
            }
        )
    }

    /// The one entry point of every delivery, activation replays included.
    /// An incarnation that has not admitted placement settles custody on
    /// every way out: admission recovers it after its claim, and a replay
    /// that returns without admission (no input, no due alarm, paused)
    /// recovers the cut it read. A dead owner's participant whose
    /// publication consumed the final input therefore always settles.
    async fn deliver_retained(
        &self,
        actor: &ActorReference,
        slot: &Slot,
        inner: &mut Inner,
    ) -> Result<bool, Error> {
        let mut replayed = None;
        let done = self
            .deliver_admitted(actor, slot, inner, &mut replayed)
            .await?;
        if done
            && inner.epoch == 0
            && let Some(snapshot) = replayed
        {
            self.store.recover_custody(&snapshot).await?;
        }
        Ok(done)
    }

    async fn deliver_admitted(
        &self,
        actor: &ActorReference,
        slot: &Slot,
        inner: &mut Inner,
        replayed: &mut Option<Arc<Snapshot>>,
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
            if settle_pending(&self.store, inner).await? {
                continue;
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
            if inner.epoch == 0 {
                *replayed = Some(Arc::clone(&snapshot));
            }
            let Some(state) = snapshot.state() else {
                // No actor: nothing is left to deliver or wake.
                self.external.remove(actor);
                inner.observed = None;
                self.subscriptions
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .release(actor);
                slot.registration.suspend_alarm();
                return Ok(true);
            };
            if state.deleted() {
                self.external.remove(actor);
                // Deletion is terminal: nothing is left to deliver or wake.
                inner.observed = None;
                self.subscriptions
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .release(actor);
                slot.registration.suspend_alarm();
                return Ok(true);
            }
            self.external
                .refresh(actor, state, &*self.handler, &slot.waker)?;
            if inner.epoch != 0 && state.epoch() != inner.epoch {
                // Another placement owns the actor, and with it the alarm.
                slot.registration.suspend_alarm();
                return Err(Error::Conflict);
            }
            slot.evict.store(
                state.paused() && !self.handler.retain_paused(actor, state.checkpoint()),
                Ordering::Release,
            );
            if state.paused() {
                // The alarm stays committed; only its active wake is withdrawn.
                // A later wake must observe an external resume as well.
                inner.observed = None;
                slot.registration.suspend_alarm();
                self.subscriptions
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .release(actor);
                return Ok(true);
            }
            if slot
                .delivery_control
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .paused
            {
                self.subscriptions
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .release(actor);
                return Ok(true);
            }
            let unwatched = self
                .subscriptions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .unwatched(self.handler.manifest(), state);
            let mut tails = BTreeMap::new();
            for path in unwatched {
                // Read before this delivery looks for input: it owns every
                // earlier record, so the new follow reports only later ones.
                let tail = match self.store.provider().tail(path.clone()).await {
                    Ok(tail) => tail,
                    Err(error) if error.category() == StreamError::NotFound => 0,
                    Err(error) => return Err(error.into()),
                };
                tails.insert(path, tail);
            }
            self.subscriptions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .bind(
                    self.store.provider(),
                    self.handler.manifest(),
                    actor,
                    state,
                    &tails,
                )?;
            let now = self.clock.now_unix_ms();
            let due = slot.registration.observe_delivery(state.alarm(), now)?;
            let (trigger, record, subscription) = match state.alarm().pending() {
                Some(_) if due => (
                    Trigger::Alarm {
                        generation: state.alarm().generation(),
                        now_unix_ms: now,
                    },
                    None,
                    None,
                ),
                _ => match self.next_record(state, inner.last_input.as_deref()).await? {
                    Some((trigger, record, after)) => (trigger, Some(record), Some(after)),
                    None => {
                        // An idle actor's activation is checkpointed without a
                        // publication: its interests must be restorable first.
                        if !inner.interests_marked {
                            self.store
                                .persist_interests(actor, state, self.handler.as_ref())
                                .await?;
                            inner.interests_marked = true;
                        }
                        return Ok(true);
                    }
                },
            };
            if inner.epoch == 0 {
                // Only work admits placement: one command per node incarnation,
                // never per delivery. The next pass delivers at the admitted
                // cut; admission is not a delivery step of the work budget.
                self.admit(actor, inner).await?;
                steps -= 1;
                continue;
            }
            // A retained publication cut cannot admit external handler work:
            // another host may already have paused or replaced this placement.
            // Until canonical control invalidation is available, observe the
            // real provider before every handler attempt, including alarms.
            if self.store.observe(&snapshot).await?.is_some() {
                // Decide from the new cut: a moved epoch is a conflict above,
                // a deletion or pause is finished work.
                inner.observed = None;
                continue;
            }
            let epoch = inner.epoch;
            let cancellation = crate::guest::CancellationToken::new();
            self.interrupt_follows(inner, state, &trigger).await?;
            {
                let control = slot
                    .delivery_control
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if control.paused {
                    return Ok(true);
                }
            }
            // A handler past its manifest timeout (wall time, not the actor
            // clock) is abandoned with its input unconsumed; the actor lock is
            // released for the next attempt.
            let subscriptions = crate::guest::SubscriptionCuts::new(
                snapshot.path.clone(),
                snapshot
                    .anchor
                    .ok_or(Error::Invalid("actor state has no committed anchor"))?,
                state.subscriptions().clone(),
            );
            let delivery = |request| Delivery {
                request,
                actor: actor.clone(),
                epoch,
                trigger: trigger.clone(),
                now_unix_ms: now,
                record: record.clone(),
                subscriptions: subscriptions.clone(),
            };
            let invoked = |delivery| {
                self.handler
                    .on_record(delivery, state.checkpoint(), cancellation.clone())
            };
            let result = match self
                .handled(
                    inner,
                    actor,
                    epoch,
                    cancellation.clone(),
                    delivery,
                    state.checkpoint(),
                    invoked,
                )
                .await
            {
                Ok(mut transition) => self
                    .store
                    .resolve_starts(&mut transition)
                    .await
                    .map(|()| transition),
                failure => failure,
            };
            // Release any pending provider I/O before checkpoint publication.
            inner.follows.clear();
            // Preparation failures and submission failures finish the same attempt.
            // Keep its scheduling position pinned only while a guard is retried
            // or an uncertain publication retains its original input.
            let publication = (|| -> Result<PreparedPublish, Error> {
                let mut publication = match result {
                    Ok(transition) => self.prepare_transition(
                        &snapshot,
                        epoch,
                        &trigger,
                        record.as_ref(),
                        transition,
                        key(&snapshot, "publish")?,
                    )?,
                    Err(Error::Handler(failure)) if !cancellation.is_cancelled() => {
                        let Some(input) = record.as_ref() else {
                            return Err(Error::Handler(failure));
                        };
                        let publication = self.store.prepare_failed_input(
                            &snapshot,
                            epoch,
                            &trigger,
                            input,
                            &failure,
                            key(&snapshot, "publish")?,
                        )?;
                        if publication.call.is_none() {
                            return Err(Error::Handler(failure));
                        }
                        publication
                    }
                    Err(error) => return Err(error),
                };
                self.finish_publication(&mut publication, actor)?;
                Ok(publication)
            })();
            let mut result = match publication {
                Ok(publication) => {
                    inner.pending =
                        Some((Arc::clone(&snapshot), publication, subscription.clone()));
                    settle_pending(&self.store, inner).await
                }
                Err(Error::PreconditionChanged(conflicts)) => {
                    inner.refused(conflicts).map(|()| true)
                }
                Err(error) => Err(error),
            };
            if let Err(error) = &result {
                if inner.pending.is_none() && terminal(error) && !cancellation.is_cancelled() {
                    self.interrupt_follows(inner, state, &trigger).await?;
                    let failure = error.to_string();
                    let delivery = |request| Delivery {
                        request,
                        actor: actor.clone(),
                        epoch,
                        trigger: trigger.clone(),
                        now_unix_ms: now,
                        record: record.clone(),
                        subscriptions,
                    };
                    let invoked = |delivery| {
                        self.handler
                            .on_failure(delivery, state.checkpoint(), failure)
                    };
                    let failure = match self
                        .handled(
                            inner,
                            actor,
                            epoch,
                            cancellation.clone(),
                            delivery,
                            state.checkpoint(),
                            invoked,
                        )
                        .await
                    {
                        Ok(Some(mut transition)) => self
                            .store
                            .resolve_starts(&mut transition)
                            .await
                            .map(|()| Some(transition)),
                        failure => failure,
                    };
                    inner.follows.clear();
                    let publication = failure.and_then(|transition| {
                        transition
                            .map(|transition| {
                                let mut publication = self.prepare_transition(
                                    &snapshot,
                                    epoch,
                                    &trigger,
                                    record.as_ref(),
                                    transition,
                                    key(&snapshot, "failure")?,
                                )?;
                                self.finish_publication(&mut publication, actor)?;
                                Ok(publication)
                            })
                            .transpose()
                    });
                    match publication {
                        Ok(Some(publication)) => {
                            inner.pending =
                                Some((Arc::clone(&snapshot), publication, subscription.clone()));
                            result = settle_pending(&self.store, inner).await;
                        }
                        Ok(None) => {}
                        Err(error) => result = Err(error),
                    }
                }
            }
            if let (Err(Error::AlarmNotDue { authority_unix_ms }), Some(deadline)) =
                (&result, state.alarm().pending())
            {
                // The authority decides alarm time. Wait out its remaining
                // time on this host's clock; nothing was published.
                let remaining = deadline.saturating_sub(*authority_unix_ms).max(1);
                slot.registration
                    .defer_alarm(state.alarm(), now.saturating_add(remaining))?;
                inner.observed = None;
                return Ok(true);
            }
            if let Err(error) = result {
                if inner.pending.is_none() {
                    if subscription.is_some() {
                        inner.last_input = subscription;
                    }
                    inner.observed = None;
                }
                return Err(error);
            }
        }
    }

    /// Every handler entry uses the same manifest deadline and control fence,
    /// including recovery from a definitive publication refusal. Each
    /// invocation gets its own capability binding, revoked when it returns,
    /// times out or is cancelled, so a task it leaves behind holds nothing.
    async fn handled<'f, T>(
        &self,
        inner: &mut Inner,
        actor: &ActorReference,
        epoch: u64,
        cancellation: crate::guest::CancellationToken,
        delivery: impl FnOnce(crate::guest::RequestId) -> Delivery,
        checkpoint: Option<&crate::guest::Checkpoint>,
        invoke: impl FnOnce(Delivery) -> crate::guest::GuestFuture<'f, T>,
    ) -> Result<T, Error> {
        let request = self.requests.open(actor, epoch);
        let delivery = delivery(request.id());
        let handled = invoke(delivery.clone());
        let delivery = &delivery;
        let interrupted = self.interrupt(inner, cancellation, |subscription, record| {
            Box::pin(async move {
                self.handler
                    .accepts_interrupt(delivery, checkpoint, &subscription, &record)
                    .await
            })
        });
        let interrupted = core::pin::pin!(interrupted);
        let timeout: BoxProviderFuture<'_, ()> = Box::pin(async {
            match self.handler.manifest().limits.handler_timeout_millis() {
                None => futures::future::pending::<()>().await,
                Some(milliseconds) => sleep(milliseconds).await,
            }
        });
        match futures::future::select(handled, futures::future::select(timeout, interrupted)).await
        {
            futures::future::Either::Left((transition, _)) => transition.map_err(Error::from),
            futures::future::Either::Right((futures::future::Either::Left(_), _)) => {
                Err(Error::Invalid("handler exceeded its manifest timeout"))
            }
            futures::future::Either::Right((futures::future::Either::Right((error, _)), _)) => {
                Err(error)
            }
        }
    }

    /// Every handler publication (a transition, a failed input, an
    /// `on_failure` transition) carries the handler's conditions and, when its
    /// state holds guest-added interests, the marker that restores them.
    fn finish_publication(
        &self,
        publication: &mut PreparedPublish,
        actor: &ActorReference,
    ) -> Result<(), Error> {
        publication.require(self.handler.publication_conditions(actor))?;
        self.store
            .mark_interests(publication, actor, self.handler.as_ref())
    }

    fn prepare_transition(
        &self,
        snapshot: &Snapshot,
        epoch: u64,
        trigger: &Trigger,
        record: Option<&Record>,
        transition: Transition,
        identity: IdempotencyKey,
    ) -> Result<PreparedPublish, Error> {
        if self
            .handler
            .manifest()
            .limits
            .checkpoint_bytes()
            .is_some_and(|limit| transition.checkpoint.bytes.len() as u128 > u128::from(limit))
        {
            return Err(Error::Invalid("checkpoint exceeds the manifest limit"));
        }
        let mut publication = self
            .store
            .prepare_publish(snapshot, epoch, trigger, record, transition, identity)?;
        if self
            .handler
            .revalidate_foreign_conflicts(&self.store.actor(snapshot.path())?)
        {
            publication.revalidate_foreign_conflicts()?;
        }
        Ok(publication)
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
        key: Option<bytes::Bytes>,
    ) -> Result<bool, Error> {
        let mut request = acyclic_stream::FollowRequest::new(path.clone(), cursor);
        if let Some(key) = key {
            request.filter = acyclic_stream::FollowFilter::exact_key(key)?;
        }
        match self.store.provider().follow(request).await {
            Ok(follow) => {
                inner
                    .follows
                    .insert(id.to_owned(), acyclic_stream::records_only(follow));
                Ok(true)
            }
            Err(error) if error.category() == StreamError::NotFound => Ok(false),
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
    ) -> Result<Option<(Trigger, Record, String)>, Error> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let subscriptions = state.subscriptions();
        let later = subscriptions.range::<str, _>((after.map_or(Unbounded, Excluded), Unbounded));
        let earlier = after
            .into_iter()
            .flat_map(|last| subscriptions.range::<str, _>((Unbounded, Included(last))));
        let ordered = self.handler.ordered_subscriptions();
        for (id, subscription) in later.chain(earlier) {
            if !ordered.contains(id) {
                if let Some((trigger, record)) =
                    self.next_subscription(state, id, subscription).await?
                {
                    return Ok(Some((trigger, record, id.clone())));
                }
                continue;
            }
            // One stable representative rotates this source against unrelated
            // inputs. Other members participate only in its source-order cut.
            if ordered.iter().any(|other| {
                other < id
                    && subscriptions
                        .get(other)
                        .is_some_and(|other| other.path == subscription.path)
            }) {
                continue;
            }
            let mut selected: Option<(Trigger, Record)> = None;
            for member in ordered {
                let Some(bound) = subscriptions
                    .get(member)
                    .filter(|bound| bound.path == subscription.path)
                else {
                    continue;
                };
                if let Some((trigger, record)) =
                    self.next_subscription(state, member, bound).await?
                    && selected
                        .as_ref()
                        .is_none_or(|(_, previous)| record.sequence < previous.sequence)
                {
                    selected = Some((trigger, record));
                }
            }
            if let Some((trigger, record)) = selected {
                return Ok(Some((trigger, record, id.clone())));
            }
        }
        Ok(None)
    }

    async fn next_subscription(
        &self,
        state: &State,
        id: &str,
        subscription: &crate::guest::Subscription,
    ) -> Result<Option<(Trigger, Record)>, Error> {
        let key = self.handler.subscription_key(id, state.checkpoint())?;
        let records = self
            .store
            .provider()
            .read(acyclic_stream::ReadRequest {
                key: key
                    .clone()
                    .map(|key| acyclic_stream::ReadKeySeek::exact(key, u64::MAX)),
                path: subscription.path.clone(),
                from: subscription.cursor,
                limit: 1,
                at_least: Default::default(),
            })
            .await;
        let mut records = match records {
            Ok(records) => records,
            Err(error) if error.category() == StreamError::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let Some(record) = records.next().await.transpose()? else {
            return Ok(None);
        };
        if record.sequence < subscription.cursor
            || (key.is_none() && record.sequence != subscription.cursor)
        {
            return Err(Error::Invalid("subscription skipped a record"));
        }
        let trigger = match key {
            Some(key) => Trigger::KeyedRecord {
                subscription: id.to_owned(),
                from: subscription.cursor,
                sequence: record.sequence,
                key,
            },
            None => Trigger::Record {
                subscription: id.to_owned(),
                sequence: record.sequence,
            },
        };
        Ok(Some((trigger, record)))
    }
}

/// Delivery and control settle the same retained request before changing its
/// actor cut. An unavailable producer leaves its original publication in place.
async fn settle_pending<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    inner: &mut Inner,
) -> Result<bool, Error> {
    let Some((snapshot, publication, subscription)) = inner.pending.take() else {
        return Ok(false);
    };
    match submit_delivery(store, &snapshot, publication).await {
        Ok(observed) => {
            if subscription.is_some() {
                inner.last_input = subscription;
            }
            inner.refused = None;
            inner.observed = Some(Arc::new(
                observed.ok_or(Error::Invalid("actor receipt has no state"))?,
            ));
            Ok(true)
        }
        Err(Error::Unknown {
            publication,
            source,
        }) => {
            inner.pending = Some((snapshot, *publication, subscription));
            Err(Error::Stream(source))
        }
        Err(Error::PreconditionChanged(conflicts)) => {
            inner.refused(conflicts)?;
            Ok(true)
        }
        Err(Error::Conflict) => {
            // A same-placement metadata/control write is not a new owner.
            // The definite refusal published none of this delivery's effects;
            // admit again from the actually changed cut, without claiming that
            // the refused publication applied or advancing its source cursor.
            if let Some(observed) = refresh_owned(store, &snapshot).await? {
                inner.refused = None;
                inner.observed = Some(Arc::new(observed));
                Ok(true)
            } else {
                inner.observed = None;
                Err(Error::Conflict)
            }
        }
        Err(error) => {
            if subscription.is_some() {
                inner.last_input = subscription;
            }
            inner.observed = None;
            Err(error)
        }
    }
}

/// A conditional refusal is the wake. Revalidate only a genuinely changed
/// original actor cut in the same placement; unchanged/foreign cuts still fail.
async fn refresh_owned<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    previous: &Snapshot,
) -> Result<Option<Snapshot>, Error> {
    let epoch = previous.state().ok_or(Error::Conflict)?.epoch();
    if !previous.owns_epoch(epoch) {
        return Ok(None);
    }
    Ok(store
        .observe(previous)
        .await?
        .filter(|observed| observed.owns_epoch(epoch)))
}

/// The original and unknown-acknowledgement paths share cancellation
/// retirement, so neither can leave a settled inline input blocking progress.
async fn submit_delivery<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    snapshot: &Snapshot,
    publication: PreparedPublish,
) -> Result<Option<Snapshot>, Error> {
    match store.submit_retained(publication).await {
        Err(Error::CallSettled { call, trigger }) => {
            let epoch = snapshot.state().ok_or(Error::Conflict)?.epoch();
            let cancelled = store.prepare_settled_input(
                snapshot,
                epoch,
                &trigger,
                &call,
                key(snapshot, "settled-input")?,
            )?;
            store.submit_retained(cancelled).await
        }
        outcome => outcome,
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
