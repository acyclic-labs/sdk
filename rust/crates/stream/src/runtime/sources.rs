//! One persistent wake and queue node per provider source.
use super::signal;
use super::sync::{AtomicU64, Ordering};
use crate::{StreamError, wire};
use futures::{Stream, task::ArcWake};
use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
mod tests;
pub(crate) const OPENING: usize = 0;
pub(crate) const ALL: usize = 1;
pub(crate) const LATEST: usize = 2;
pub(crate) const EITHER: usize = 3;

pub(crate) struct Wake {
    pub(crate) identity: wire::SessionChannel,
    pub(crate) queued: AtomicU64,
    pub(crate) signal: Arc<signal::Signal<wire::SessionChannel>>,
    #[cfg(loom)]
    pub(crate) omit_signal: bool,
    #[cfg(loom)]
    pub(crate) clear_after_recheck: bool,
    #[cfg(loom)]
    pub(crate) release_only: bool,
}
impl Wake {
    fn recheck<T>(&self, provider: impl FnOnce() -> T) -> T {
        #[cfg(loom)]
        let late = self.clear_after_recheck;
        #[cfg(not(loom))]
        let late = false;
        // A wake racing the provider recheck must enqueue a fresh hint. Clearing
        // afterward can erase a wake that was deduplicated against the old hint.
        // The RMW also acquires publications carried by coalesced wakes.
        let reset = || {
            #[cfg(loom)]
            if self.release_only {
                self.queued.store(0, Ordering::Release);
                return;
            }
            self.queued.swap(0, Ordering::AcqRel);
        };
        if !late {
            reset();
        }
        let result = provider();
        if late {
            reset();
        }
        result
    }
}
impl Wake {
    // Return whether this publication joined an already queued hint. The model
    // uses that observation without adding a synchronizing lock or handshake.
    pub(crate) fn notify(&self) -> bool {
        let coalesced = self.queued.swap(1, Ordering::AcqRel) != 0;
        if !coalesced {
            #[cfg(loom)]
            if self.omit_signal {
                return false;
            }
            let _ = self.signal.send(self.identity);
        }
        coalesced
    }
}
impl ArcWake for Wake {
    fn wake_by_ref(wake: &Arc<Self>) {
        wake.notify();
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Links {
    pub(crate) class: usize,
    pub(crate) before: Option<u64>,
    pub(crate) after: Option<u64>,
}
#[derive(Default)]
pub(crate) struct Queue {
    pub(crate) first: Option<u64>,
    pub(crate) last: Option<u64>,
}
pub(crate) struct Entry<S> {
    pub(crate) stream: S,
    pub(crate) wake: Arc<Wake>,
    pub(crate) opening: bool,
    pub(crate) mode: Option<usize>,
    pub(crate) ready: bool,
    pub(crate) links: Option<Links>,
}

/// Fair, wake-driven provider streams sharing persistent queue nodes.
///
/// The owner polls only runnable sources. Receiving an item moves that source
/// behind its ready peers without allocating another task. Removing a source
/// drops its stream immediately, independently of delivery credit. An exhausted
/// source is removed; callers that require a typed closure must emit that item.
pub struct ReadySources<S> {
    // Streams, links and modes belong exclusively to the session event loop.
    // A provider waker touches only its atomic and the existing Signal lock;
    // it never polls a provider or acquires another lock while signaling.
    pub(crate) entries: BTreeMap<u64, Entry<S>>,
    pub(crate) queues: [Queue; 4],
    pub(crate) signal: Arc<signal::Signal<wire::SessionChannel>>,
    preferred: usize,
    #[cfg(test)]
    pub(crate) queue_edits: usize,
}
impl<S: Stream + Unpin> ReadySources<S> {
    /// Creates an empty owner with no task or timer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            queues: Default::default(),
            signal: Arc::new(signal::Signal::new()),
            preferred: 0,
            #[cfg(test)]
            queue_edits: 0,
        }
    }
    /// Whether any source is retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn insert(
        &mut self,
        identity: wire::SessionChannel,
        stream: S,
        mode: Option<usize>,
    ) {
        self.insert_source(identity, stream, mode, true);
    }
    fn insert_source(
        &mut self,
        identity: wire::SessionChannel,
        stream: S,
        mode: Option<usize>,
        opening: bool,
    ) {
        self.remove(identity.channel_id);
        self.entries.insert(
            identity.channel_id,
            Entry {
                stream,
                wake: Arc::new(Wake {
                    identity,
                    queued: AtomicU64::new(0),
                    signal: self.signal.clone(),
                    #[cfg(loom)]
                    omit_signal: false,
                    #[cfg(loom)]
                    clear_after_recheck: false,
                    #[cfg(loom)]
                    release_only: false,
                }),
                opening,
                mode,
                ready: true,
                links: None,
            },
        );
        self.enqueue(identity.channel_id);
    }
    pub(crate) fn mode(&mut self, id: u64, mode: Option<usize>) {
        if self.entries.get(&id).is_none_or(|entry| entry.mode == mode) {
            return;
        }
        self.unlink(id);
        if let Some(entry) = self.entries.get_mut(&id) {
            entry.mode = mode;
        }
        self.enqueue(id);
    }
    /// Cancels and drops a source, even when delivery has no credit.
    pub fn remove(&mut self, id: u64) {
        self.unlink(id);
        self.entries.remove(&id);
    }
    pub(crate) fn unlink(&mut self, id: u64) {
        let Some(links) = self.entries.get_mut(&id).and_then(|e| e.links.take()) else {
            return;
        };
        #[cfg(test)]
        {
            self.queue_edits += 1;
        }
        // Queued neighbours always hold links; a class always names a queue.
        if let Some(before) = links.before {
            if let Some(neighbour) = self.links(before) {
                neighbour.after = links.after;
            }
        } else if let Some(queue) = self.queues.get_mut(links.class) {
            queue.first = links.after;
        }
        if let Some(after) = links.after {
            if let Some(neighbour) = self.links(after) {
                neighbour.before = links.before;
            }
        } else if let Some(queue) = self.queues.get_mut(links.class) {
            queue.last = links.before;
        }
    }
    fn links(&mut self, id: u64) -> Option<&mut Links> {
        self.entries.get_mut(&id)?.links.as_mut()
    }
    pub(crate) fn enqueue(&mut self, id: u64) {
        let Some(entry) = self.entries.get_mut(&id) else {
            return;
        };
        if !entry.ready || entry.links.is_some() {
            return;
        }
        let Some(class) = (if entry.opening {
            Some(OPENING)
        } else {
            entry.mode
        }) else {
            return;
        };
        let Some(before) = self.queues.get(class).map(|queue| queue.last) else {
            return;
        };
        entry.links = Some(Links {
            class,
            before,
            after: None,
        });
        #[cfg(test)]
        {
            self.queue_edits += 1;
        }
        if let Some(neighbour) = before.and_then(|before| self.links(before)) {
            neighbour.after = Some(id);
        }
        if let Some(queue) = self.queues.get_mut(class) {
            if before.is_none() {
                queue.first = Some(id);
            }
            queue.last = Some(id);
        }
    }
    pub(crate) fn wake(&mut self, identity: wire::SessionChannel) {
        if let Some(entry) = self.entries.get_mut(&identity.channel_id)
            && entry.wake.identity == identity
        {
            entry.ready = true;
            self.enqueue(identity.channel_id);
        }
    }
    fn enabled(room: (bool, bool, bool)) -> [bool; 4] {
        let all = room.1 && room.2;
        [true, all, room.0, all || room.0]
    }
    fn pop(&mut self, room: (bool, bool, bool)) -> Option<u64> {
        let enabled = Self::enabled(room);
        for offset in 0..self.queues.len() {
            let class = (self.preferred + offset) % self.queues.len();
            if enabled.get(class) == Some(&true)
                && let Some(id) = self.queues.get(class).and_then(|queue| queue.first)
            {
                self.unlink(id);
                self.preferred = (class + 1) % self.queues.len();
                return Some(id);
            }
        }
        None
    }
    pub(crate) fn next_with_control(
        &mut self,
        control: &mut Self,
        room: (bool, bool, bool),
    ) -> impl std::future::Future<Output = Result<Option<S::Item>, StreamError>> + Unpin {
        // Terminal entries stay in OPENING until their sole result. Reuse the
        // same bounded, wake-driven queues, independently of all data credit.
        std::future::poll_fn(move |cx| {
            if !control.is_empty() {
                match control.poll((false, false, false), cx) {
                    Poll::Ready(Ok(Some(event))) => return Poll::Ready(Ok(Some(event))),
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    _ => {}
                }
            }
            if self.is_empty() && !control.is_empty() {
                Poll::Pending
            } else {
                self.poll(room, cx)
            }
        })
    }
    pub(crate) fn poll(
        &mut self,
        room: (bool, bool, bool),
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<S::Item>, StreamError>> {
        let mut polled = 0;
        loop {
            while let Some(identity) = self.signal.try_next()? {
                self.wake(identity);
            }
            if let Some(id) = self.pop(room) {
                // A queued id always names a live entry.
                let Some(entry) = self.entries.get_mut(&id) else {
                    continue;
                };
                entry.ready = false;
                let waker = futures::task::waker_ref(&entry.wake);
                match entry.wake.recheck(|| {
                    Pin::new(&mut entry.stream).poll_next(&mut Context::from_waker(&waker))
                }) {
                    Poll::Ready(Some(event)) => {
                        entry.opening = false;
                        entry.ready = true;
                        self.enqueue(id);
                        return Poll::Ready(Ok(Some(event)));
                    }
                    Poll::Ready(None) => self.remove(id),
                    Poll::Pending => {}
                }
                polled += 1;
                if polled >= self.entries.len().max(1) {
                    // Register the parent before yielding a provider's Pending.
                    // Otherwise a last source can lose its producer while the
                    // only parent wake still names an earlier cooperative turn.
                    match self.signal.poll(cx) {
                        Poll::Ready(Ok(identity)) => self.wake(identity),
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                        Poll::Pending => {}
                    }
                    if self
                        .queues
                        .iter()
                        .zip(Self::enabled(room))
                        .any(|(queue, enabled)| enabled && queue.first.is_some())
                    {
                        cx.waker().wake_by_ref();
                    }
                    return Poll::Pending;
                }
            } else if self.entries.is_empty() {
                return Poll::Ready(Ok(None));
            } else {
                match self.signal.poll(cx) {
                    Poll::Ready(Ok(identity)) => self.wake(identity),
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
    }
}

impl<S: Stream + Unpin> Default for ReadySources<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Stream + Unpin> ReadySources<S> {
    /// Registers a stream for unrestricted delivery. The caller's identity and
    /// generation fence stale provider wakes; they confer no access authority.
    pub fn register(&mut self, identity: wire::SessionChannel, stream: S) {
        // The provider has already admitted this stream. Session protocol
        // openings retain their separate class through the internal insert.
        self.insert_source(identity, stream, Some(ALL), false);
    }

    /// Number of retained sources.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Borrows one source's cursor without polling its provider.
    pub fn get(&self, id: u64) -> Option<&S> {
        self.entries.get(&id).map(|entry| &entry.stream)
    }

    /// Removes a source while transferring its stream to the caller.
    pub fn take(&mut self, id: u64) -> Option<S> {
        self.unlink(id);
        self.entries.remove(&id).map(|entry| entry.stream)
    }

    /// Polls one ready source, with round-robin service among ready peers.
    /// Pending sources remain asleep until their provider's registered wake.
    pub fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<S::Item>, StreamError>> {
        self.poll((true, true, true), cx)
    }
}
