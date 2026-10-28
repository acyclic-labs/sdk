//! Shared, coalescing wake registration. Only running work owns a future.
use acyclic_stream::query_sync::SyncMutex;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Weak},
    task::{Context, Poll, Wake, Waker},
};

struct Entry<K> {
    key: K,
    phase: Phase,
    alarm: super::Alarm,
    due: Option<(u64, u64, u64)>,
}
enum Phase {
    Queued {
        previous: Option<u64>,
        next: Option<u64>,
    },
    Running {
        notified: bool,
    },
    Idle,
}
struct Index<K> {
    next: u64,
    entries: BTreeMap<u64, Entry<K>>,
    first: Option<u64>,
    last: Option<u64>,
    ready_count: usize,
    waiter: Option<Waker>,
    due: BTreeSet<(u64, u64, u64)>,
}
impl<K> Default for Index<K> {
    fn default() -> Self {
        Self {
            next: 0,
            entries: BTreeMap::new(),
            first: None,
            last: None,
            ready_count: 0,
            waiter: None,
            due: BTreeSet::new(),
        }
    }
}
impl<K> Index<K> {
    fn notify(&mut self, id: u64) -> Option<Waker> {
        match self.entries.get_mut(&id).map(|entry| &mut entry.phase) {
            Some(Phase::Running { notified }) => {
                *notified = true;
                None
            }
            Some(Phase::Idle) => self.enqueue(id),
            Some(Phase::Queued { .. }) | None => None,
        }
    }
    fn enqueue(&mut self, id: u64) -> Option<Waker> {
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
    fn unlink(&mut self, previous: Option<u64>, next: Option<u64>) {
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
}
impl<K: Clone> Index<K> {
    fn take(&mut self) -> Option<(u64, K)> {
        let id = self.first?;
        let next = match self.entries.get(&id)?.phase {
            Phase::Queued { next, .. } => next,
            Phase::Running { .. } | Phase::Idle => return None,
        };
        self.unlink(None, next);
        let entry = self.entries.get_mut(&id)?;
        entry.phase = Phase::Running { notified: false };
        Some((id, entry.key.clone()))
    }
}
struct Core<K> {
    index: SyncMutex<Index<K>>,
}

/// One process-wide ready index. Idle entries own no task, future or timer.
pub struct ReadySet<K> {
    core: Arc<Core<K>>,
}
impl<K> Default for ReadySet<K> {
    fn default() -> Self {
        Self {
            core: Arc::new(Core {
                index: SyncMutex::new(Index::default()),
            }),
        }
    }
}

/// Exclusive lifetime of one subscription registration. Old wakers cannot revive it.
pub struct Registration<K> {
    core: Arc<Core<K>>,
    id: u64,
}

/// Running work retains notifications until its final durable-tail recheck completes.
pub struct Work<K> {
    core: Arc<Core<K>>,
    id: u64,
    key: K,
}

impl<K: Clone> ReadySet<K> {
    /// Register before reading the durable cursor/tail. Initial replay is always ready.
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
        index.entries.insert(
            id,
            Entry {
                key,
                phase: Phase::Idle,
                alarm: super::Alarm::default(),
                due: None,
            },
        );
        let waiter = index.enqueue(id);
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
        })
    }

    /// One shared dispatcher waiter, installed under the same lock as the empty check.
    /// The runtime owns exactly one pending call, independent of actor population.
    pub fn poll_ready(&self, context: &mut Context<'_>) -> Poll<Work<K>> {
        let mut index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((id, key)) = index.take() {
            Poll::Ready(Work {
                core: Arc::clone(&self.core),
                id,
                key,
            })
        } else {
            index.waiter = Some(context.waker().clone());
            Poll::Pending
        }
    }

    /// Retain the dispatcher's alarm-change wake while admission is full.
    /// Install before reading the minimum deadline, under the same index lock.
    pub fn listen(&self, context: &Context<'_>) {
        self.core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .waiter = Some(context.waker().clone());
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
            let Some(due @ (deadline, id, _)) = index.due.first().copied() else {
                break;
            };
            if deadline > now_unix_ms {
                break;
            }
            index.due.remove(&due);
            if let Some(entry) = index.entries.get_mut(&id) {
                entry.due = None;
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
    fn notify(&self, id: u64) {
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

    fn finish(&self, id: u64) {
        let mut index = self
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let waiter = if let Some(entry) = index.entries.get_mut(&id) {
            let notified = matches!(entry.phase, Phase::Running { notified: true });
            entry.phase = Phase::Idle;
            if notified { index.enqueue(id) } else { None }
        } else {
            None
        };
        drop(index);
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }
}

struct Notifier<K> {
    core: Weak<Core<K>>,
    id: u64,
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
impl<K: Send + Sync + 'static> Registration<K> {
    /// Lightweight callback for a shared Streams session; it owns no pending future.
    pub fn waker(&self) -> Waker {
        Waker::from(Arc::new(Notifier {
            core: Arc::downgrade(&self.core),
            id: self.id,
        }))
    }
}
impl<K> Registration<K> {
    /// Install a durably observed alarm into the shared due index. A later
    /// observation cannot move generation or consumption backwards. Reinstalling
    /// an unconsumed alarm permits retry after an unsuccessful physical attempt.
    pub fn update_alarm(&self, alarm: &super::Alarm) -> Result<(), super::Error> {
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
        let due = alarm
            .deadline_unix_ms
            .filter(|_| alarm.delivered_generation != Some(alarm.generation))
            .map(|deadline| (deadline, self.id, alarm.generation));
        if &entry.alarm == alarm && entry.due == due {
            return Ok(());
        }
        let previous = entry.due.take();
        entry.alarm = alarm.clone();
        entry.due = due;
        if let Some(previous) = previous {
            index.due.remove(&previous);
        }
        if let Some(due) = due {
            index.due.insert(due);
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
impl<K> Registration<K> {
    /// Withdraw the alarm from the due index while its actor is paused. The
    /// observed alarm is retained, so reinstalling the same committed alarm
    /// on resume is a permitted `update_alarm`.
    pub fn suspend_alarm(&self) {
        let mut index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let due = index
            .entries
            .get_mut(&self.id)
            .and_then(|entry| entry.due.take());
        if let Some(due) = due {
            index.due.remove(&due);
        }
    }
}
impl<K> Drop for Registration<K> {
    fn drop(&mut self) {
        let mut index = self
            .core
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = index.entries.remove(&self.id) {
            if let Some(due) = entry.due {
                index.due.remove(&due);
            }
            if let Phase::Queued { previous, next } = entry.phase {
                index.unlink(previous, next);
            }
        }
    }
}
impl<K> Work<K> {
    /// Identity to replay using its durable cursor before parking this work.
    pub const fn key(&self) -> &K {
        &self.key
    }
}
impl<K> Drop for Work<K> {
    fn drop(&mut self) {
        self.core.finish(self.id);
    }
}

#[cfg(test)]
mod tests;
