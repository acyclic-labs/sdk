//! Shared, coalescing wake registration. Only running work owns a future.
use acyclic_stream::query_sync::SyncMutex;
mod registry;
use registry::{Id, Registry};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Weak},
    task::{Context, Poll, Wake, Waker},
};

struct Entry<K> {
    key: K,
    phase: Phase,
    alarm: super::Alarm,
    token: Option<Token>,
}

/// This host's sole claim on one pending durable alarm. It is not `Clone`:
/// every transition moves it, between a resident registration and the parked
/// set, and only an observed consumption, a durable suspension or a host with
/// no dispatcher burns it. While it exists its original deadline bounds the
/// deadline watermark, whatever its wake time.
#[must_use]
struct Token {
    serial: u64,
    /// The watermark group (scheduling shard) its deadline bounds.
    group: u16,
    deadline: u64,
    /// Next wake; None once a wake was delivered to its registration.
    wake: Option<u64>,
}
enum Phase {
    Queued {
        previous: Option<Id>,
        next: Option<Id>,
    },
    Running {
        notified: bool,
    },
    Idle,
}
struct Index<K> {
    next: u64,
    entries: Registry<K>,
    first: Option<Id>,
    last: Option<Id>,
    ready_count: usize,
    /// Deliveries in flight, for every consumer alike: each taken [`Work`]
    /// until it drops, and each [`Flight`] of a direct delivery.
    in_flight: usize,
    /// Keys whose last delivery failed, whichever consumer ran it: each one's
    /// input stays unconsumed, so a checkpoint carries its wake forward.
    /// Bounded by failed keys, never by residents.
    failed: BTreeSet<K>,
    waiter: Option<Waker>,
    /// Resident wakes, as `(wake_at, registration, serial)`.
    due: BTreeSet<(u64, Id, u64)>,
    /// Every live token, as `(group, deadline, serial)`, wherever it is held.
    deadlines: BTreeSet<(u16, u64, u64)>,
    /// A key's watermark group: one group's tokens never hold another's.
    group: Box<dyn Fn(&K) -> u16 + Send + Sync>,
    serials: u64,
    /// Tokens of retired registrations, until their key registers again.
    /// Bounded by parked alarms, never by residents.
    parked: BTreeMap<K, Token>,
    parked_wakes: BTreeSet<(u64, K)>,
    /// Deadlines of tokens newly minted from committed alarms, as
    /// `(deadline, key)`, until the dispatcher takes them to learn their
    /// windows.
    armed: Vec<(u64, K)>,
    /// Set once a dispatcher exists; until then nothing watermarks a window,
    /// so a retiring registration's token is burned and no armed deadline is
    /// kept, never accumulated.
    dispatching: bool,
}
impl<K: 'static> Default for Index<K> {
    fn default() -> Self {
        Self {
            next: 0,
            entries: Registry::default(),
            first: None,
            last: None,
            ready_count: 0,
            in_flight: 0,
            failed: BTreeSet::new(),
            waiter: None,
            due: BTreeSet::new(),
            deadlines: BTreeSet::new(),
            group: Box::new(|_| 0),
            serials: 0,
            parked: BTreeMap::new(),
            parked_wakes: BTreeSet::new(),
            armed: Vec::new(),
            dispatching: false,
        }
    }
}
impl<K> Index<K> {
    fn notify(&mut self, id: Id) -> Option<Waker> {
        match self.entries.get_mut(&id).map(|entry| &mut entry.phase) {
            Some(Phase::Running { notified }) => {
                *notified = true;
                None
            }
            Some(Phase::Idle) => self.enqueue(id),
            Some(Phase::Queued { .. }) | None => None,
        }
    }
    fn enqueue(&mut self, id: Id) -> Option<Waker> {
        let previous = self.last;
        if let Some(entry) = self.entries.get_mut(&id) {
            entry.phase = Phase::Queued {
                previous,
                next: None,
            };
        }
        if let Some(last) = previous {
            if let Some(Entry {
                phase: Phase::Queued { next, .. },
                ..
            }) = self.entries.get_mut(&last)
            {
                *next = Some(id);
            }
        } else {
            self.first = Some(id);
        }
        self.last = Some(id);
        self.ready_count += 1;
        self.waiter.take()
    }
    fn unlink(&mut self, previous: Option<Id>, next: Option<Id>) {
        if let Some(id) = previous {
            if let Some(Entry {
                phase: Phase::Queued { next: link, .. },
                ..
            }) = self.entries.get_mut(&id)
            {
                *link = next;
            }
        } else {
            self.first = next;
        }
        if let Some(id) = next {
            if let Some(Entry {
                phase: Phase::Queued { previous: link, .. },
                ..
            }) = self.entries.get_mut(&id)
            {
                *link = previous;
            }
        } else {
            self.last = previous;
        }
        self.ready_count -= 1;
    }
    /// One delivery settled. The last wakes the dispatcher, whose checkpoint
    /// waits for every consumer's deliveries.
    fn settle(&mut self) -> Option<Waker> {
        self.in_flight -= 1;
        if self.in_flight == 0 {
            self.waiter.take()
        } else {
            None
        }
    }
}
impl<K: Ord + Clone> Index<K> {
    fn mint(&mut self, key: &K, deadline: u64, wake: Option<u64>) -> Token {
        self.serials = self.serials.wrapping_add(1);
        let group = (self.group)(key);
        self.deadlines.insert((group, deadline, self.serials));
        Token {
            serial: self.serials,
            group,
            deadline,
            wake,
        }
    }

    /// The durable alarm no longer needs this host: consumed or suspended.
    fn burn(&mut self, token: Token) {
        self.deadlines
            .remove(&(token.group, token.deadline, token.serial));
    }

    /// Two claims on one actor's alarm keep the earlier deadline and wake.
    fn merge(&mut self, first: Token, second: Token) -> Token {
        let wake = first.wake.min(second.wake);
        let (mut kept, spare) = if first.deadline <= second.deadline {
            (first, second)
        } else {
            (second, first)
        };
        self.burn(spare);
        kept.wake = wake;
        kept
    }

    fn release(&mut self, id: Id) -> Option<Token> {
        let token = self.entries.get_mut(&id)?.token.take()?;
        if let Some(wake) = token.wake {
            self.due.remove(&(wake, id, token.serial));
        }
        Some(token)
    }

    /// Give `token` to registration `id`, or back when it is retired.
    fn hold(&mut self, id: Id, token: Token) -> Result<(), Token> {
        if self.entries.get(&id).is_none() {
            return Err(token);
        }
        let token = match self.release(id) {
            Some(held) => self.merge(held, token),
            None => token,
        };
        let (wake, serial) = (token.wake, token.serial);
        let Some(entry) = self.entries.get_mut(&id) else {
            return Err(token);
        };
        entry.token = Some(token);
        if let Some(wake) = wake {
            self.due.insert((wake, id, serial));
        }
        Ok(())
    }

    fn park(&mut self, key: K, token: Token) {
        let token = match self.unpark(&key) {
            Some(parked) => self.merge(parked, token),
            None => token,
        };
        self.parked_wakes
            .insert((token.wake.unwrap_or(token.deadline), key.clone()));
        self.parked.insert(key, token);
    }

    fn unpark(&mut self, key: &K) -> Option<Token> {
        let token = self.parked.remove(key)?;
        self.parked_wakes
            .remove(&(token.wake.unwrap_or(token.deadline), key.clone()));
        Some(token)
    }

    fn take(&mut self) -> Option<(Id, K)> {
        let id = self.first?;
        let next = match self.entries.get(&id)?.phase {
            Phase::Queued { next, .. } => next,
            Phase::Running { .. } | Phase::Idle => return None,
        };
        self.unlink(None, next);
        self.in_flight += 1;
        let entry = self.entries.get_mut(&id)?;
        entry.phase = Phase::Running { notified: false };
        Some((id, entry.key.clone()))
    }
}
struct Core<K> {
    index: SyncMutex<Index<K>>,
}

/// One process-wide ready index. Direct-address queue bookkeeping is O(1);
/// slot growth is amortized O(1). Alarm insertion/removal separately pays
/// O(log A) for A armed deadlines. Idle entries own no task, future or timer.
pub struct ReadySet<K> {
    core: Arc<Core<K>>,
}
impl<K: 'static> Default for ReadySet<K> {
    fn default() -> Self {
        Self {
            core: Arc::new(Core {
                index: SyncMutex::new(Index::default()),
            }),
        }
    }
}
impl<K: 'static> ReadySet<K> {
    /// A ready index whose deadline watermarks are kept per `group` of key,
    /// so an alarm held in one scheduling shard never holds another's.
    pub fn grouped(group: impl Fn(&K) -> u16 + Send + Sync + 'static) -> Self {
        let mut index = Index::default();
        index.group = Box::new(group);
        Self {
            core: Arc::new(Core {
                index: SyncMutex::new(index),
            }),
        }
    }
}

/// Exclusive lifetime of one subscription registration. Old wakers cannot revive it.
pub struct Registration<K: Ord + Clone> {
    core: Arc<Core<K>>,
    id: Id,
}

/// One delivery of one actor: it counts in flight, so no checkpoint passes
/// what it may still commit. It settles only by dropping, and a drop before
/// [`Flight::finish`] (an error, a panic, a cancelled await) settles failed.
#[must_use]
pub struct Flight<K: Ord + Clone> {
    core: Arc<Core<K>>,
    key: K,
    failed: bool,
}

/// Running work retains notifications until its final durable-tail recheck
/// completes. A drop before [`Work::settle`] (an error, a panic, a cancelled
/// await) marks its key failed, so the input keeps a carried wake.
pub struct Work<K: Ord + Clone> {
    core: Arc<Core<K>>,
    id: Id,
    key: K,
    settled: bool,
}

impl<K: Ord + Clone> ReadySet<K> {
    /// Register a dormant actor. Only an explicit input, placement or due-alarm
    /// wake queues work; registration never scans or replays dormant state.
    pub fn register(&self, key: K) -> Result<Registration<K>, super::Error> {
        let mut index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = index
            .next
            .checked_add(1)
            .ok_or(super::Error::Invalid("wake registration exhausted"))?;
        index.next = id;
        let parked = index.unpark(&key);
        let fallback = parked.as_ref().map(|_| key.clone());
        let id = index.entries.insert(
            id,
            Entry {
                key,
                phase: Phase::Idle,
                alarm: super::Alarm::default(),
                token: None,
            },
        );
        // A parked alarm moves into its key's new registration.
        let waiter = match parked.zip(fallback) {
            Some((token, key)) => {
                if let Err(token) = index.hold(id, token) {
                    index.park(key, token);
                }
                index.waiter.take()
            }
            None => None,
        };
        drop(index);
        if let Some(waiter) = waiter {
            waiter.wake();
        }
        Ok(Registration {
            core: Arc::clone(&self.core),
            id,
        })
    }

    /// Take one coalesced actor; no dormant-population scan.
    pub fn take(&self) -> Option<Work<K>> {
        let mut index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (id, key) = index.take()?;
        Some(Work {
            core: Arc::clone(&self.core),
            id,
            key,
            settled: false,
        })
    }

    /// One shared dispatcher waiter, installed under the same lock as the empty check.
    /// The runtime owns exactly one pending call, independent of actor population.
    pub fn poll_ready(&self, context: &mut Context<'_>) -> Poll<Work<K>> {
        // Clone and retire arbitrary executor callbacks outside the index lock.
        let waker = context.waker().clone();
        let mut index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((id, key)) = index.take() {
            drop(index);
            drop(waker);
            Poll::Ready(Work {
                core: Arc::clone(&self.core),
                id,
                key,
                settled: false,
            })
        } else {
            let previous = index.waiter.replace(waker);
            drop(index);
            drop(previous);
            Poll::Pending
        }
    }

    /// Retain the dispatcher's alarm-change wake while admission is full.
    /// Install before reading the minimum deadline, under the same index lock.
    pub fn listen(&self, context: &Context<'_>) {
        let waker = context.waker().clone();
        let previous = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .waiter
            .replace(waker);
        drop(previous);
    }

    /// A dispatcher exists: from now on a retiring registration parks its
    /// alarm token instead of burning it.
    pub fn dispatch(&self) {
        self.core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .dispatching = true;
    }

    /// Take the deadlines newly armed since the last call, as
    /// `(deadline, key)`, so their windows are known before they open.
    pub fn take_armed(&self) -> Vec<(u64, K)> {
        std::mem::take(
            &mut self
                .core
                .index
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .armed,
        )
    }

    /// Earliest wake of a parked alarm, which only re-registering its key
    /// delivers.
    pub fn next_parked_wake(&self) -> Option<u64> {
        let index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        index.parked_wakes.first().map(|&(at, _)| at)
    }

    /// At most `limit` keys whose parked alarm wakes by `now_unix_ms`. Their
    /// tokens stay parked until each key registers again.
    pub fn parked_due(&self, now_unix_ms: u64, limit: usize) -> Vec<K> {
        let index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        index
            .parked_wakes
            .iter()
            .take_while(|&&(at, _)| at <= now_unix_ms)
            .take(limit)
            .map(|(_, key)| key.clone())
            .collect()
    }

    /// Count a delivery of `key` in flight until the returned guard drops.
    pub fn flight(&self, key: &K) -> Flight<K> {
        self.core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .in_flight += 1;
        Flight {
            core: Arc::clone(&self.core),
            key: key.clone(),
            failed: true,
        }
    }

    /// Whether any work is queued or any delivery is in flight, whichever
    /// consumer took it: the one bound every checkpoint waits on.
    pub fn busy(&self) -> bool {
        let index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        index.ready_count != 0 || index.in_flight != 0
    }

    /// The keys of `group` whose last delivery failed: each checkpoint of
    /// the group keeps or carries forward a wake entry for every one.
    pub fn failed(&self, group: u16) -> Vec<K> {
        let index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        index
            .failed
            .iter()
            .filter(|key| (index.group)(key) == group)
            .cloned()
            .collect()
    }

    /// End `key`'s failure mark once a checkpoint committed its last carried
    /// wake: a released key is no longer this host's to carry.
    pub fn forget(&self, key: &K) {
        self.core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .failed
            .remove(key);
    }

    /// Exact retained registration and ready counts, independent of actor heaps.
    pub fn counts(&self) -> (usize, usize) {
        let index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (index.entries.len(), index.ready_count)
    }

    /// Earliest committed, unconsumed alarm. The dispatcher owns the sole timer.
    /// Register its waiter with `poll_ready` before reading this deadline, so an
    /// earlier deadline racing timer installation retains a wake.
    pub fn next_deadline(&self) -> Option<u64> {
        let index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        index.due.first().map(|&(deadline, _, _)| deadline)
    }

    /// The deadline window `group`'s watermark may not pass at `now_unix_ms`:
    /// the current window, or the earliest original deadline's window of any
    /// of the group's alarm tokens, resident or parked, however late its wake
    /// was deferred.
    pub fn watermark(&self, group: u16, now_unix_ms: u64) -> u64 {
        let index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        super::scheduling::window(
            index
                .deadlines
                .range((group, 0, 0)..=(group, u64::MAX, u64::MAX))
                .next()
                .map_or(now_unix_ms, |&(_, deadline, _)| deadline.min(now_unix_ms)),
        )
    }

    /// Number of retained alarm nodes; replacement and cancellation leave none behind.
    pub fn alarm_count(&self) -> usize {
        self.core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .due
            .len()
    }

    /// Wake at most `limit` due actors, without visiting any dormant registration.
    /// The runtime rechecks the committed generation before delivering an alarm.
    pub fn wake_due(&self, now_unix_ms: u64, limit: usize) -> usize {
        let mut index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut count = 0;
        let mut waiter = None;
        while count < limit {
            let Some(due @ (wake, id, _)) = index.due.first().copied() else {
                break;
            };
            if wake > now_unix_ms {
                break;
            }
            // The wake is delivered; the token stays with its registration.
            index.due.remove(&due);
            if let Some(token) = index
                .entries
                .get_mut(&id)
                .and_then(|entry| entry.token.as_mut())
            {
                token.wake = None;
            }
            if let Some(wake) = index.notify(id) {
                waiter = Some(wake);
            }
            count += 1;
        }
        drop(index);
        if let Some(waiter) = waiter {
            waiter.wake();
        }
        count
    }
}

impl<K> Core<K> {
    fn notify(&self, id: Id) {
        let mut index = self
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let waiter = index.notify(id);
        drop(index);
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }

    fn finish(&self, id: Id, failed: Option<&K>)
    where
        K: Ord + Clone,
    {
        let mut index = self
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(key) = failed {
            index.failed.insert(key.clone());
        }
        let waiter = if let Some(entry) = index.entries.get_mut(&id) {
            let notified = matches!(entry.phase, Phase::Running { notified: true });
            entry.phase = Phase::Idle;
            if notified { index.enqueue(id) } else { None }
        } else {
            None
        };
        let settled = index.settle();
        let waiter = waiter.or(settled);
        drop(index);
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }
}

struct Notifier<K> {
    core: Weak<Core<K>>,
    id: Id,
}
impl<K: Send + Sync + 'static> Wake for Notifier<K> {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(core) = self.core.upgrade() {
            core.notify(self.id);
        }
    }
}
impl<K: Ord + Clone + Send + Sync + 'static> Registration<K> {
    /// Lightweight callback for a shared Streams session; it owns no pending future.
    pub fn waker(&self) -> Waker {
        Waker::from(Arc::new(Notifier {
            core: Arc::downgrade(&self.core),
            id: self.id,
        }))
    }
}
impl<K: Ord + Clone> Registration<K> {
    /// Install a durably observed alarm into the shared due index. A later
    /// observation cannot move generation or consumption backwards. Reinstalling
    /// an unconsumed alarm permits retry after an unsuccessful physical attempt.
    pub fn update_alarm(&self, alarm: &super::Alarm) -> Result<(), super::Error> {
        self.observe_alarm(alarm, alarm.pending())
    }

    /// Observe the committed alarm before a delivery at `now_unix_ms`. True
    /// when this delivery is that due alarm: the delivery is its wake, so it
    /// leaves the due index and a failed attempt is retried by the actor's
    /// next event, never by its own elapsed deadline. An undue alarm stays
    /// installed as the actor's future wake.
    pub fn observe_delivery(
        &self,
        alarm: &super::Alarm,
        now_unix_ms: u64,
    ) -> Result<bool, super::Error> {
        let due = alarm
            .pending()
            .is_some_and(|deadline| deadline <= now_unix_ms);
        self.observe_alarm(alarm, alarm.pending().filter(|_| !due))?;
        Ok(due)
    }

    /// Reinstall the committed, still pending alarm to wake at host time
    /// t_unix_ms instead of its deadline: the authority refused its
    /// consumption because its own clock had not yet reached the deadline.
    pub fn defer_alarm(&self, alarm: &super::Alarm, at_unix_ms: u64) -> Result<(), super::Error> {
        self.observe_alarm(alarm, alarm.pending().map(|_| at_unix_ms))
    }

    fn observe_alarm(
        &self,
        alarm: &super::Alarm,
        deadline: Option<u64>,
    ) -> Result<(), super::Error> {
        if alarm.deadline_unix_ms == Some(0)
            || (alarm.deadline_unix_ms.is_some() && alarm.generation == 0)
            || alarm
                .delivered_generation
                .is_some_and(|generation| generation != alarm.generation)
        {
            return Err(super::Error::Invalid("invalid committed alarm"));
        }
        let mut index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = index
            .entries
            .get_mut(&self.id)
            .ok_or(super::Error::Invalid("retired wake registration"))?;
        if alarm.generation < entry.alarm.generation
            || (alarm.generation == entry.alarm.generation
                && (alarm.deadline_unix_ms != entry.alarm.deadline_unix_ms
                    || (entry.alarm.delivered_generation.is_some()
                        && alarm.delivered_generation != entry.alarm.delivered_generation)))
        {
            return Err(super::Error::Conflict);
        }
        let pending = alarm.pending();
        if &entry.alarm == alarm
            && entry
                .token
                .as_ref()
                .map(|token| (token.deadline, token.wake))
                == pending.map(|pending| (pending, deadline))
        {
            return Ok(());
        }
        entry.alarm = alarm.clone();
        let key = entry.key.clone();
        // The committed alarm replaces whatever this host claimed for it.
        let previous = index.release(self.id);
        let known = previous.as_ref().map(|previous| previous.deadline);
        if let Some(previous) = previous {
            index.burn(previous);
        }
        if let Some(pending) = pending {
            // A new deadline is an event: the dispatcher learns its window.
            if index.dispatching && known != Some(pending) {
                index.armed.push((pending, key.clone()));
            }
            let token = index.mint(&key, pending, deadline);
            if let Err(token) = index.hold(self.id, token) {
                index.park(key, token);
            }
        }
        // An earlier deadline must wake the single dispatcher so it can rearm.
        let waiter = index.waiter.take();
        drop(index);
        if let Some(waiter) = waiter {
            waiter.wake();
        }
        Ok(())
    }
}
impl<K: Ord + Clone> Registration<K> {
    /// Burn the alarm token while its actor is paused or deleted: the durable
    /// state owns the alarm (a resume files it again). The observed alarm is
    /// retained, so reinstalling the same committed alarm on resume is a
    /// permitted `update_alarm`.
    pub fn suspend_alarm(&self) {
        let mut index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(token) = index.release(self.id) {
            index.burn(token);
        }
    }

    /// Take over a token parked for this key, as when a predecessor
    /// registration retired after this one registered.
    pub fn adopt_parked(&self) {
        let mut index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(key) = index.entries.get(&self.id).map(|entry| entry.key.clone()) else {
            return;
        };
        if let Some(token) = index.unpark(&key)
            && let Err(token) = index.hold(self.id, token)
        {
            index.park(key, token);
        }
    }

    /// Take over the alarm filed in an opened window starting at
    /// `window_start_unix_ms` before the window may be passed. The actor's
    /// delivery replaces the claim with its committed alarm.
    pub fn claim_alarm(&self, window_start_unix_ms: u64) {
        let mut index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(entry) = index.entries.get(&self.id) else {
            return;
        };
        if entry.token.is_none() {
            let key = entry.key.clone();
            let token = index.mint(&key, window_start_unix_ms, None);
            if let Err(token) = index.hold(self.id, token) {
                index.park(key, token);
            }
        }
    }
}
impl<K: Ord + Clone> Drop for Registration<K> {
    fn drop(&mut self) {
        let mut index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let retired = index.entries.remove(&self.id);
        let mut waiter = None;
        if let Some(entry) = retired {
            if let Phase::Queued { previous, next } = entry.phase {
                index.unlink(previous, next);
            }
            if let Some(mut token) = entry.token {
                if let Some(wake) = token.wake {
                    index.due.remove(&(wake, self.id, token.serial));
                }
                // Retiring never drops an alarm token, armed, woken or
                // deferred: it is parked until its key registers again.
                if index.dispatching {
                    token.wake = Some(token.wake.unwrap_or(token.deadline));
                    index.park(entry.key, token);
                    waiter = index.waiter.take();
                } else {
                    index.burn(token);
                }
            }
        }
        drop(index);
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }
}
impl<K: Ord + Clone> Work<K> {
    /// Identity to replay using its durable cursor before parking this work.
    pub const fn key(&self) -> &K {
        &self.key
    }

    /// Its delivery ran to an outcome, which that delivery's [`Flight`]
    /// recorded; or there was none to run.
    pub fn settle(mut self) {
        self.settled = true;
    }
}
impl<K: Ord + Clone> Flight<K> {
    /// The delivery ran to an outcome: a failed one leaves its key's input
    /// for a carried wake, a completed one clears that.
    pub fn finish(mut self, failed: bool) {
        self.failed = failed;
    }
}

/// The one settlement of every delivery, however it ends.
impl<K: Ord + Clone> Drop for Flight<K> {
    fn drop(&mut self) {
        let mut index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.failed {
            index.failed.insert(self.key.clone());
        } else {
            index.failed.remove(&self.key);
        }
        let waiter = index.settle();
        drop(index);
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }
}
/// The one settlement of all taken work, however it ends.
impl<K: Ord + Clone> Drop for Work<K> {
    fn drop(&mut self) {
        self.core
            .finish(self.id, (!self.settled).then_some(&self.key));
    }
}

#[cfg(test)]
mod tests;
