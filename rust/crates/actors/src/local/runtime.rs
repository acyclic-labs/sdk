//! `local::Runtime`: the single-node Actors host over any Stream provider.
//!
//! One dispatcher owns one follow of the state root's activation log, one
//! ready index and one timer. A dormant actor owns no task, timer or socket: only its
//! registration and a passive follow cursor per subscription that the
//! dispatcher polls with that registration's waker.
use super::ready::{ReadySet, Registration, Work};
use super::{Error, PreparedPublish, Snapshot, State, Store};
use crate::addressing::ActorReference;
use crate::guest::{Delivery, Handler, Trigger};
use acyclic_stream::query_sync::SyncMutex;
use acyclic_stream::runtime::{Executor as _, PlatformExecutor};
use acyclic_stream::{
    AppendRequest, BoxProviderFuture, FollowFilter, IdempotencyKey, ProviderPlatform, Record,
    RecordStream, StreamError, StreamPath, StreamProvider, UnixMillisClock as _,
};
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    convert::Infallible,
    future::poll_fn,
    sync::Arc,
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
}

#[derive(Default)]
struct Inner {
    /// A removed slot must not begin another publication through an older Arc.
    released: bool,
    /// Placement this runtime owns; zero until the first claim.
    epoch: u64,
    follows: BTreeMap<String, RecordStream>,
    /// A record read from a follow but not yet consumed by a commit.
    held: BTreeMap<String, Record>,
    /// The exact request and participant custody survive an unknown acknowledgement.
    pending: Option<(Snapshot, PreparedPublish)>,
}

enum Event {
    Created(Record),
    Work(Work<ActorReference>),
    Done(Result<(), (ActorReference, Error)>),
    Rearm(Option<u64>),
    Due,
    Failed(StreamError),
}

/// Executes one handler package for every actor whose state lives under the
/// store's root, including actors created by other parties' commits.
pub struct Runtime<P: StreamProvider + ?Sized> {
    store: Store<P>,
    handler: Arc<dyn Handler>,
    clock: Arc<dyn Clock>,
    ready: ReadySet<ActorReference>,
    actors: SyncMutex<HashMap<ActorReference, Arc<Slot>>>,
    /// Identity of this runtime's placement attempts: two hosts claiming the
    /// same cut never share an idempotency key, so only one claim commits.
    instance: uuid::Uuid,
}

impl<P: StreamProvider + ?Sized> Runtime<P> {
    /// One runtime per state root and handler package.
    pub fn new(store: Store<P>, handler: Arc<dyn Handler>, clock: Arc<dyn Clock>) -> Self {
        Self {
            store,
            handler,
            clock,
            ready: ReadySet::default(),
            actors: SyncMutex::new(HashMap::new()),
            instance: uuid::Uuid::new_v4(),
        }
    }

    /// The shared store.
    pub const fn store(&self) -> &Store<P> {
        &self.store
    }

    /// Exact resident registration and ready counts.
    pub fn counts(&self) -> (usize, usize) {
        self.ready.counts()
    }

    /// Start tracking an actor. Idempotent; the first delivery claims a new epoch.
    pub fn track(&self, actor: ActorReference) -> Result<(), Error> {
        let mut actors = self
            .actors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(slot) = actors.get(&actor) {
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
            }),
        );
        Ok(())
    }

    /// Stop hosting an idle actor (hibernate). Returns false while a delivery
    /// or an unknown publication still owns participant custody; wake it to
    /// settle before releasing. Its durable state is untouched.
    pub fn release(&self, actor: &ActorReference) -> bool {
        let mut actors = self
            .actors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(slot) = actors.get(actor) else {
            return true;
        };
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

    /// Durably pause or resume. Pause fences in-flight publication; resume wakes.
    pub async fn pause(&self, actor: &ActorReference, paused: bool) -> Result<(), Error> {
        loop {
            let snapshot = self.store.load(actor).await?;
            let state = snapshot
                .state()
                .ok_or(Error::Invalid("actor state was never created"))?;
            if state.paused() == paused {
                break;
            }
            let operation = if paused { "pause" } else { "resume" };
            match self
                .store
                .set_paused(&snapshot, state.epoch(), paused, key(&snapshot, operation)?)
                .await
            {
                Err(Error::Conflict) => {}
                result => {
                    result?;
                    break;
                }
            }
        }
        if !paused {
            self.wake(actor);
        }
        Ok(())
    }

    /// Run every currently ready or due actor to quiescence, then return the
    /// failures. Deterministic hosts and tests drive the runtime with this.
    pub async fn drain(&self) -> Vec<(ActorReference, Error)> {
        self.ready.wake_due(self.clock.now_unix_ms(), usize::MAX);
        let mut failures = Vec::new();
        while let Some(work) = self.ready.take() {
            if let Err(failure) = self.execute(work).await {
                failures.push(failure);
            }
        }
        failures
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
                idempotency_key: None,
            })
            .await?;
        let mut created = provider.follow(root, 0, FollowFilter::default()).await?;
        let mut running = FuturesUnordered::new();
        let mut timer: Option<(u64, BoxProviderFuture<'static, ()>)> = None;
        loop {
            let event = poll_fn(|context: &mut Context<'_>| {
                match created.poll_next_unpin(context) {
                    Poll::Ready(Some(Ok(record))) => return Poll::Ready(Event::Created(record)),
                    Poll::Ready(Some(Err(error))) => return Poll::Ready(Event::Failed(error)),
                    Poll::Ready(None) => {
                        return Poll::Ready(Event::Failed(StreamError::Unavailable));
                    }
                    Poll::Pending => {}
                }
                if let Poll::Ready(Some(result)) = running.poll_next_unpin(context) {
                    return Poll::Ready(Event::Done(result));
                }
                if let Poll::Ready(work) = self.ready.poll_ready(context) {
                    return Poll::Ready(Event::Work(work));
                }
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
                Poll::Pending
            })
            .await;
            match event {
                Event::Created(record) => {
                    // The opening record is not an address; skip it.
                    if let Ok(actor) = ActorReference::decode(record.value.as_ref()) {
                        self.track(actor)?;
                    }
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
                    self.ready.wake_due(self.clock.now_unix_ms(), usize::MAX);
                }
                Event::Failed(error) => return Err(error.into()),
            }
        }
    }

    fn slot(&self, actor: &ActorReference) -> Option<Arc<Slot>> {
        self.actors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(actor)
            .cloned()
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
        result.map_err(|error| (actor, error))
    }

    /// Deliver until nothing is ready: one committed transition per input.
    async fn deliver(&self, actor: &ActorReference, slot: &Slot) -> Result<(), Error> {
        let mut inner = slot.inner.lock().await;
        if inner.released {
            return Ok(());
        }
        loop {
            if let Some((snapshot, publication)) = inner.pending.take() {
                match self.store.submit(publication).await {
                    Ok(()) => continue,
                    Err(Error::Unknown {
                        publication,
                        source,
                    }) => {
                        inner.pending = Some((snapshot, *publication));
                        return Err(Error::Stream(source));
                    }
                    Err(Error::Conflict) if self.store.load(actor).await?.tail != snapshot.tail => {
                        continue;
                    }
                    Err(Error::Conflict) => {
                        return Err(Error::Invalid("transition refused at an unchanged cut"));
                    }
                    Err(error) => return Err(error),
                }
            }
            let snapshot = self.store.load(actor).await?;
            let Some(state) = snapshot.state() else {
                return Ok(());
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
                    Ok(()) => inner.epoch = epoch,
                    Err(Error::Conflict) => {}
                    Err(error) => return Err(error),
                }
                continue;
            }
            if state.paused() {
                // The alarm stays committed; only its active wake is withdrawn.
                slot.registration.suspend_alarm();
                return Ok(());
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
                _ => match self.next_record(slot, &mut inner, state).await? {
                    Some((subscription, record)) => (
                        Trigger::Record {
                            subscription,
                            sequence: record.sequence,
                        },
                        Some(record),
                    ),
                    None => return Ok(()),
                },
            };
            let epoch = inner.epoch;
            let limits = &self.handler.manifest().limits;
            // A handler past its manifest timeout (wall time, not the actor
            // clock) is abandoned with its input unconsumed; the actor lock is
            // released for the next attempt.
            let handled = self.handler.on_record(
                Delivery {
                    actor: actor.clone(),
                    epoch,
                    trigger: trigger.clone(),
                    now_unix_ms: now,
                    record,
                },
                state.checkpoint(),
            );
            let transition = match futures::future::select(
                handled,
                sleep(limits.handler_timeout_millis()),
            )
            .await
            {
                futures::future::Either::Left((transition, _)) => transition?,
                futures::future::Either::Right(_) => {
                    return Err(Error::Invalid("handler exceeded its manifest timeout"));
                }
            };
            if u64::try_from(transition.checkpoint.bytes.len()).unwrap_or(u64::MAX)
                > limits.checkpoint_bytes()
            {
                return Err(Error::Invalid("checkpoint exceeds the manifest limit"));
            }
            match self
                .store
                .publish(
                    &snapshot,
                    epoch,
                    &trigger,
                    transition,
                    key(&snapshot, "publish")?,
                )
                .await
            {
                Ok(()) => {}
                // A moved cut is re-read; a lost placement surfaces above.
                Err(Error::Conflict) if self.store.load(actor).await?.tail != snapshot.tail => {}
                // The same cut refused the transition (its effect conditions or a
                // stale trigger): retrying cannot succeed, so surface it.
                Err(Error::Conflict) => {
                    return Err(Error::Invalid("transition refused at an unchanged cut"));
                }
                Err(Error::Unknown {
                    publication,
                    source,
                }) => {
                    inner.pending = Some((snapshot, *publication));
                    return Err(Error::Stream(source));
                }
                Err(error) => return Err(error),
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
            .follow(path.clone(), cursor, FollowFilter::default())
            .await
        {
            Ok(follow) => {
                inner.follows.insert(id.to_owned(), follow);
                Ok(true)
            }
            Err(StreamError::NotFound) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// The next unconsumed record of any subscription, without blocking.
    /// A pending follow keeps this actor's waker, so an append wakes it.
    async fn next_record(
        &self,
        slot: &Slot,
        inner: &mut Inner,
        state: &State,
    ) -> Result<Option<(String, Record)>, Error> {
        for (id, subscription) in state.subscriptions() {
            if let Some(held) = inner.held.get(id) {
                if held.sequence == subscription.cursor {
                    return Ok(Some((id.clone(), held.clone())));
                }
                inner.held.remove(id);
            }
            if !inner.follows.contains_key(id)
                && !self
                    .open(inner, id, &subscription.path, subscription.cursor)
                    .await?
            {
                continue;
            }
            let Some(follow) = inner.follows.get_mut(id) else {
                continue;
            };
            let mut context = Context::from_waker(&slot.waker);
            loop {
                match follow.poll_next_unpin(&mut context) {
                    Poll::Ready(Some(Ok(record))) if record.sequence < subscription.cursor => {}
                    Poll::Ready(Some(Ok(record))) if record.sequence == subscription.cursor => {
                        inner.held.insert(id.clone(), record.clone());
                        return Ok(Some((id.clone(), record)));
                    }
                    Poll::Ready(Some(Ok(_))) => {
                        inner.follows.remove(id);
                        return Err(Error::Invalid("subscription skipped a record"));
                    }
                    Poll::Ready(Some(Err(error))) => {
                        inner.follows.remove(id);
                        return Err(error.into());
                    }
                    Poll::Ready(None) => {
                        // Reopen from the committed cursor on the next attempt.
                        inner.follows.remove(id);
                        slot.waker.wake_by_ref();
                        break;
                    }
                    Poll::Pending => break,
                }
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
