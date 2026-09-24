use super::{MountSourceError, MountViewLease};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::ThreadId;
use tokio::sync::Notify;

#[derive(Default)]
struct ViewState {
    readers: usize,
    readers_by_owner: HashMap<ViewReaderId, usize>,
    writer: bool,
    waiting_writers: usize,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
enum ViewReaderId {
    Callback(ThreadId),
    #[cfg(test)]
    Independent(u64),
}

/// Pins a coherent mount view across a native callback or authored mutation.
pub(super) struct ViewGate {
    state: Mutex<ViewState>,
    changed: Notify,
    generation: AtomicU64,
    #[cfg(test)]
    next_reader: AtomicU64,
}

pub(super) struct ViewReadLease {
    pub(super) generation: u64,
    gate: Arc<ViewGate>,
    owner: ViewReaderId,
}

pub(super) struct ViewWriteLease {
    pub(super) generation: u64,
    gate: Arc<ViewGate>,
}

struct WaitingWriter {
    gate: Arc<ViewGate>,
    registered: bool,
}

impl MountViewLease for ViewReadLease {}

impl Drop for ViewReadLease {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.readers = state.readers.saturating_sub(1);
        if let Some(readers) = state.readers_by_owner.get_mut(&self.owner) {
            *readers = readers.saturating_sub(1);
            if *readers == 0 {
                state.readers_by_owner.remove(&self.owner);
            }
        }
        if state.readers == 0 {
            self.gate.changed.notify_waiters();
        }
    }
}

impl WaitingWriter {
    fn register(gate: &Arc<ViewGate>) -> Self {
        let mut state = gate.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.waiting_writers = state.waiting_writers.saturating_add(1);
        drop(state);
        Self {
            gate: Arc::clone(gate),
            registered: true,
        }
    }

    fn admit(&mut self, state: &mut ViewState) {
        state.waiting_writers = state.waiting_writers.saturating_sub(1);
        self.registered = false;
    }
}

impl Drop for WaitingWriter {
    fn drop(&mut self) {
        if self.registered {
            let mut state = self
                .gate
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            state.waiting_writers = state.waiting_writers.saturating_sub(1);
            self.gate.changed.notify_waiters();
        }
    }
}

impl Drop for ViewWriteLease {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.writer = false;
        self.gate.changed.notify_waiters();
    }
}

impl ViewGate {
    pub(super) const INITIAL_GENERATION: u64 = 2;

    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(ViewState::default()),
            changed: Notify::new(),
            generation: AtomicU64::new(Self::INITIAL_GENERATION),
            #[cfg(test)]
            next_reader: AtomicU64::new(1),
        }
    }

    #[cfg(test)]
    pub(super) async fn read(
        self: &Arc<Self>,
        expected: Option<u64>,
    ) -> Result<ViewReadLease, MountSourceError> {
        let owner = ViewReaderId::Independent(self.next_reader.fetch_add(1, Ordering::Relaxed));
        self.read_as(owner, expected).await
    }

    pub(super) fn callback_owner() -> ThreadId {
        std::thread::current().id()
    }

    pub(super) async fn read_for_callback(
        self: &Arc<Self>,
        owner: ThreadId,
        expected: Option<u64>,
    ) -> Result<ViewReadLease, MountSourceError> {
        self.read_as(ViewReaderId::Callback(owner), expected).await
    }

    async fn read_as(
        self: &Arc<Self>,
        owner: ViewReaderId,
        expected: Option<u64>,
    ) -> Result<ViewReadLease, MountSourceError> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                if !state.writer
                    && (state.waiting_writers == 0 || state.readers_by_owner.contains_key(&owner))
                {
                    let generation = self.generation.load(Ordering::Acquire);
                    if !generation.is_multiple_of(2)
                        || expected.is_some_and(|expected| expected != generation)
                    {
                        return Err(MountSourceError::Stale);
                    }
                    state.readers = state.readers.saturating_add(1);
                    *state.readers_by_owner.entry(owner).or_default() += 1;
                    return Ok(ViewReadLease {
                        generation,
                        gate: Arc::clone(self),
                        owner,
                    });
                }
            }
            notified.await;
        }
    }

    pub(super) async fn write(self: &Arc<Self>) -> ViewWriteLease {
        self.acquire_writer().await
    }

    pub(super) async fn write_stable(
        self: &Arc<Self>,
        expected: Option<u64>,
    ) -> Result<ViewWriteLease, MountSourceError> {
        let lease = self.acquire_writer().await;
        if !lease.generation.is_multiple_of(2)
            || expected.is_some_and(|expected| expected != lease.generation)
        {
            return Err(MountSourceError::Stale);
        }
        Ok(lease)
    }

    async fn acquire_writer(self: &Arc<Self>) -> ViewWriteLease {
        let mut waiter = WaitingWriter::register(self);
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                if !state.writer && state.readers == 0 {
                    let generation = self.generation.load(Ordering::Acquire);
                    waiter.admit(&mut state);
                    state.writer = true;
                    return ViewWriteLease {
                        generation,
                        gate: Arc::clone(self),
                    };
                }
            }
            notified.await;
        }
    }

    pub(super) fn begin_transition(&self) {
        let generation = self.generation.load(Ordering::Acquire);
        if generation.is_multiple_of(2) {
            self.generation.store(generation + 1, Ordering::Release);
        }
    }

    pub(super) fn finish_transition(&self) {
        let generation = self.generation.load(Ordering::Acquire);
        debug_assert_eq!(generation % 2, 1);
        self.generation.store(generation + 1, Ordering::Release);
    }

    pub(super) fn is_stable(&self) -> bool {
        self.generation.load(Ordering::Acquire).is_multiple_of(2)
    }

    pub(super) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
}
