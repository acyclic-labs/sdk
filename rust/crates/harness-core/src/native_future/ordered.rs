//! Ordered, notified futures on their original allocation ledger.

use crate::{Error, Result, executor::PreparationIo};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError, Weak},
    task::{Context, Poll, Wake, Waker},
};

/// Identity of one queued future. Recycled slots cannot accept an old key.
#[derive(Clone)]
pub struct OrderedFutureKey {
    owner: Arc<Ready>,
    index: usize,
    generation: u64,
}

/// Concurrent polling with issue-ordered delivery and funded wake ownership.
///
/// Only pushed or notified futures are polled. Push, wake, cancellation and
/// delivery use constant-time links and slot access. Capacity growth is
/// amortized: geometric slot growth admits and charges the actual relocation
/// through the shared vector reservation boundary. Slots follow peak concurrent
/// demand and are reused, so completed history adds no storage or lookup work.
/// Dropping the owner necessarily drops its remaining futures once each.
///
/// Future payloads retain their own allocation custody (for example through
/// [`super::NativeFutureBundle`]); this owner funds slots and wake nodes. A
/// retained child waker keeps both its node and shared ready event funded.
pub struct NativeOrdered<F: Future + Unpin> {
    slots: Vec<Slot<F>>,
    // Slots and their payloads die before their backing capacity is released.
    slot_io: Option<PreparationIo>,
    ready: Arc<Ready>,
    work: Option<NativeMaterializationWork>,
    cancellation: Option<acyclic_fs::Cancelled<'static>>,
    free: Option<usize>,
    first: Option<usize>,
    last: Option<usize>,
    length: usize,
    terminal: bool,
}

struct Slot<F: Future> {
    generation: u64,
    previous: Option<usize>,
    next: Option<usize>,
    wake: Option<Arc<ChildWake>>,
    state: SlotState<F>,
}

enum SlotState<F: Future> {
    Free(Option<usize>),
    Running(F),
    Completed(F::Output),
}

struct Ready {
    queue: Mutex<ReadyQueue>,
    // Last: queue links and the parent waker die under their original custody.
    _custody: Option<NativeAllocation>,
}

#[derive(Default)]
struct ReadyQueue {
    first: Option<Arc<ChildWake>>,
    last: Option<Arc<ChildWake>>,
    length: usize,
    parent: Option<Waker>,
    closed: bool,
}

struct ChildWake {
    index: usize,
    generation: u64,
    ready: Arc<Ready>,
    // All access to these links and flags is serialized by ready.queue.
    links: Mutex<WakeLinks>,
    _custody: Option<NativeAllocation>,
}

#[derive(Default)]
struct WakeLinks {
    previous: Option<Weak<ChildWake>>,
    next: Option<Arc<ChildWake>>,
    queued: bool,
    inactive: bool,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn claim_arc<T>(work: Option<&NativeMaterializationWork>) -> Result<Option<NativeAllocation>> {
    work.map(|work| {
        work.claim(
            acyclic_native_runtime::arc_allocation_bytes::<T>() as u64,
            1,
            acyclic_fs::WorkCounters::UNCHARGED,
        )
        .map_err(Error::from)
    })
    .transpose()
}

fn invalid_slot() -> Error {
    Error::NativeBoundary {
        code: crate::wire::ErrorCode::Storage,
        reason: "ordered future issue links are inconsistent",
    }
}

impl Ready {
    fn enqueue(node: &Arc<ChildWake>) {
        let parent = {
            let mut queue = lock(&node.ready.queue);
            let mut links = lock(&node.links);
            if queue.closed || links.inactive || links.queued {
                return;
            }
            links.queued = true;
            links.previous = queue.last.as_ref().map(Arc::downgrade);
            drop(links);
            if let Some(last) = &queue.last {
                lock(&last.links).next = Some(Arc::clone(node));
            } else {
                queue.first = Some(Arc::clone(node));
            }
            queue.last = Some(Arc::clone(node));
            queue.length += 1;
            queue.parent.take()
        };
        if let Some(parent) = parent {
            parent.wake();
        }
    }

    fn pop(&self) -> Option<Arc<ChildWake>> {
        let mut queue = lock(&self.queue);
        let node = Arc::clone(queue.first.as_ref()?);
        Self::unlink(&mut queue, &node);
        Some(node)
    }

    fn deactivate(node: &Arc<ChildWake>) {
        let mut queue = lock(&node.ready.queue);
        lock(&node.links).inactive = true;
        Self::unlink(&mut queue, node);
    }

    fn unlink(queue: &mut ReadyQueue, node: &Arc<ChildWake>) {
        let mut links = lock(&node.links);
        if !links.queued {
            return;
        }
        let previous = links.previous.take().and_then(|node| node.upgrade());
        let next = links.next.take();
        if let Some(previous) = &previous {
            lock(&previous.links).next = next.clone();
        } else {
            queue.first = next.clone();
        }
        if let Some(next) = next {
            lock(&next.links).previous = previous.as_ref().map(Arc::downgrade);
        } else {
            queue.last = previous;
        }
        links.queued = false;
        queue.length -= 1;
    }

    fn register(&self, context: &Context<'_>) -> usize {
        // Register under the same lock used by enqueue, before observing the
        // ready count. No waker callback or destructor runs under that lock.
        let next = context.waker().clone();
        let (previous, count) = {
            let mut queue = lock(&self.queue);
            (queue.parent.replace(next), queue.length)
        };
        drop(previous);
        count
    }

    fn notify_remaining(&self) {
        let parent = {
            let mut queue = lock(&self.queue);
            if queue.length == 0 {
                return;
            }
            queue.parent.take()
        };
        if let Some(parent) = parent {
            parent.wake();
        }
    }

    fn close(&self) {
        let (mut next, parent) = {
            let mut queue = lock(&self.queue);
            queue.closed = true;
            queue.last = None;
            queue.length = 0;
            (queue.first.take(), queue.parent.take())
        };
        // Break the ready-list ownership cycle before dropping any future.
        // Later retained wakers see closed and cannot recreate it.
        while let Some(node) = next {
            let mut links = lock(&node.links);
            links.inactive = true;
            links.queued = false;
            links.previous = None;
            next = links.next.take();
        }
        drop(parent);
    }
}

impl Wake for ChildWake {
    fn wake(self: Arc<Self>) {
        Ready::enqueue(&self);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        Ready::enqueue(self);
    }
}

impl<F: Future + Unpin> NativeOrdered<F> {
    /// Retains the original ledger and its funded cancellation registration.
    /// `None` runs the same queue without native accounting.
    pub fn new(work: Option<&NativeMaterializationWork>) -> Result<Self> {
        let slot_io = work.map(PreparationIo::from_native_work).transpose()?;
        let custody = claim_arc::<Ready>(work)?;
        let ready = Arc::new(Ready {
            queue: Mutex::new(ReadyQueue::default()),
            _custody: custody,
        });
        let cancellation = work
            .map(NativeMaterializationWork::cancellation_wait)
            .transpose()?;
        Ok(Self {
            slots: Vec::new(),
            slot_io,
            ready,
            work: work.cloned(),
            cancellation,
            free: None,
            first: None,
            last: None,
            length: 0,
            terminal: false,
        })
    }

    /// Number of undelivered, uncancelled futures, including completed ones.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.length
    }

    /// Whether no future remains to deliver.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Admits one slot and wake node before publishing it to either queue.
    pub fn push_back(&mut self, future: F) -> Result<OrderedFutureKey> {
        if self.terminal {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "ordered future owner has terminated",
            });
        }
        if let Some(work) = &self.work {
            work.remaining()?;
        }
        let (index, generation) = match self.free {
            Some(index) => (
                index,
                self.slot(index)?
                    .generation
                    .checked_add(1)
                    .ok_or_else(invalid_slot)?,
            ),
            None => {
                self.reserve_slot()?;
                (self.slots.len(), 0)
            }
        };
        let custody = claim_arc::<ChildWake>(self.work.as_ref())?;
        let wake = Arc::new(ChildWake {
            index,
            generation,
            ready: Arc::clone(&self.ready),
            links: Mutex::new(WakeLinks::default()),
            _custody: custody,
        });
        let slot = Slot {
            generation,
            previous: self.last,
            next: None,
            wake: Some(Arc::clone(&wake)),
            state: SlotState::Running(future),
        };
        if let Some(index) = self.free {
            let SlotState::Free(next) = &self.slot(index)?.state else {
                return Err(invalid_slot());
            };
            self.free = *next;
            *self.slot_mut(index)? = slot;
        } else {
            self.slots.push(slot);
        }
        if let Some(last) = self.last {
            self.slot_mut(last)?.next = Some(index);
        } else {
            self.first = Some(index);
        }
        self.last = Some(index);
        self.length += 1;
        Ready::enqueue(&wake);
        Ok(OrderedFutureKey {
            owner: Arc::clone(&self.ready),
            index,
            generation,
        })
    }

    fn reserve_slot(&mut self) -> Result<()> {
        if self.slots.len() < self.slots.capacity() {
            return Ok(());
        }
        let before = self.slots.capacity();
        let capacity = before.max(1).checked_mul(2).ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "ordered future slot capacity overflows",
        })?;
        crate::executor::stock_vec_reserve(&mut self.slots, capacity, self.slot_io.as_ref())
    }

    fn slot(&self, index: usize) -> Result<&Slot<F>> {
        self.slots.get(index).ok_or_else(invalid_slot)
    }

    fn slot_mut(&mut self, index: usize) -> Result<&mut Slot<F>> {
        self.slots.get_mut(index).ok_or_else(invalid_slot)
    }

    fn remove(&mut self, index: usize) -> Result<SlotState<F>> {
        let slot = self.slot(index)?;
        let previous = slot.previous;
        let next = slot.next;
        if let Some(wake) = &slot.wake {
            Ready::deactivate(wake);
        }
        if let Some(previous) = previous {
            self.slot_mut(previous)?.next = next;
        } else {
            self.first = next;
        }
        if let Some(next) = next {
            self.slot_mut(next)?.previous = previous;
        } else {
            self.last = previous;
        }
        let free = self.free;
        let slot = self.slot_mut(index)?;
        slot.wake = None;
        slot.previous = None;
        slot.next = None;
        let state = std::mem::replace(&mut slot.state, SlotState::Free(free));
        // A physically exhausted generation is retired, never wrapped.
        if slot.generation != u64::MAX {
            self.free = Some(index);
        }
        self.length -= 1;
        Ok(state)
    }

    /// Removes one exact live entry. Stale or foreign keys change nothing.
    /// Its future is dropped once; retained wakers cannot poll a reused slot.
    pub fn cancel(&mut self, key: &OrderedFutureKey) -> Result<bool> {
        if !Arc::ptr_eq(&key.owner, &self.ready) {
            return Ok(false);
        }
        let Some(slot) = self.slots.get(key.index) else {
            return Ok(false);
        };
        if slot.generation != key.generation || slot.wake.is_none() {
            return Ok(false);
        }
        drop(self.remove(key.index)?);
        Ok(true)
    }

    /// Polls ready work and delivers the next completed issue in order.
    /// A native cancellation wakes this wait and returns its original error.
    pub fn poll_next(&mut self, context: &mut Context<'_>) -> Poll<Result<Option<F::Output>>> {
        if self.terminal {
            return Poll::Ready(Ok(None));
        }
        if let Some(cancelled) = &mut self.cancellation {
            let _ = Pin::new(cancelled).poll(context);
        }
        if let Some(work) = &self.work
            && let Err(error) = work.remaining()
        {
            self.terminal = true;
            self.ready.close();
            return Poll::Ready(Err(error.into()));
        }
        if self.is_empty() {
            return Poll::Ready(Ok(None));
        }
        // One poll consumes the notifications present at registration. A
        // self-waking future cannot make this poll spin without returning.
        let count = self.ready.register(context);
        for _ in 0..count {
            if self.front_completed() {
                break;
            }
            let Some(wake) = self.ready.pop() else {
                break;
            };
            let Some(slot) = self.slots.get_mut(wake.index) else {
                continue;
            };
            if slot.generation != wake.generation || slot.wake.is_none() {
                continue;
            }
            let SlotState::Running(future) = &mut slot.state else {
                continue;
            };
            let waker = Waker::from(Arc::clone(&wake));
            let mut child_context = Context::from_waker(&waker);
            if let Poll::Ready(output) = Pin::new(future).poll(&mut child_context) {
                Ready::deactivate(&wake);
                slot.state = SlotState::Completed(output);
            }
        }
        if self.front_completed() {
            let Some(index) = self.first else {
                return Poll::Ready(Err(invalid_slot()));
            };
            let SlotState::Completed(output) = self.remove(index)? else {
                return Poll::Ready(Err(invalid_slot()));
            };
            Poll::Ready(Ok(Some(output)))
        } else {
            self.ready.notify_remaining();
            Poll::Pending
        }
    }

    fn front_completed(&self) -> bool {
        self.first
            .and_then(|index| self.slots.get(index))
            .is_some_and(|slot| matches!(slot.state, SlotState::Completed(_)))
    }

    /// Waits on the same shared readiness event used by every child wake.
    pub async fn next(&mut self) -> Result<Option<F::Output>> {
        std::future::poll_fn(|context| self.poll_next(context)).await
    }
}

impl<F: Future + Unpin> Drop for NativeOrdered<F> {
    fn drop(&mut self) {
        self.ready.close();
    }
}

#[cfg(test)]
mod tests;
