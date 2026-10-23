//! One owner polls only provider sources made runnable by push or credit.
use super::sync::{AtomicU64, Ordering};
use super::*;
use futures::task::ArcWake;
use std::task::{Context, Poll};

#[cfg(acyclic_stream_loom)]
pub(super) mod model;
#[cfg(test)]
#[cfg(not(acyclic_stream_loom))]
mod tests;

const OPENING: usize = 0;
pub(super) const ALL: usize = 1;
pub(super) const LATEST: usize = 2;
const EITHER: usize = 3;

struct Wake {
    identity: wire::SessionChannel,
    queued: AtomicU64,
    signal: Arc<signal::Signal<wire::SessionChannel>>,
    #[cfg(acyclic_stream_loom)]
    omit_signal: bool,
    #[cfg(acyclic_stream_loom)]
    clear_after_recheck: bool,
    #[cfg(acyclic_stream_loom)]
    release_only: bool,
}
impl Wake {
    fn recheck<T>(&self, provider: impl FnOnce() -> T) -> T {
        #[cfg(acyclic_stream_loom)]
        let late = self.clear_after_recheck;
        #[cfg(not(acyclic_stream_loom))]
        let late = false;
        // A wake racing the provider recheck must enqueue a fresh hint. Clearing
        // afterward can erase a wake that was deduplicated against the old hint.
        // The RMW also acquires publications carried by coalesced wakes.
        let reset = || {
            #[cfg(acyclic_stream_loom)]
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
    fn notify(&self) -> bool {
        let coalesced = self.queued.swap(1, Ordering::AcqRel) != 0;
        if !coalesced {
            #[cfg(acyclic_stream_loom)]
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
struct Links {
    class: usize,
    before: Option<u64>,
    after: Option<u64>,
}
#[derive(Default)]
struct Queue {
    first: Option<u64>,
    last: Option<u64>,
}
struct Entry {
    stream: Source,
    wake: Arc<Wake>,
    opening: bool,
    mode: Option<usize>,
    ready: bool,
    links: Option<Links>,
}

pub(super) struct Sources {
    // Streams, links and modes belong exclusively to the session event loop.
    // A provider waker touches only its atomic and the existing Signal lock;
    // it never polls a provider or acquires another lock while signaling.
    entries: BTreeMap<u64, Entry>,
    queues: [Queue; 4],
    signal: Arc<signal::Signal<wire::SessionChannel>>,
    preferred: usize,
    #[cfg(test)]
    queue_edits: usize,
}
impl Sources {
    pub(super) fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            queues: Default::default(),
            signal: Arc::new(signal::Signal::new()),
            preferred: 0,
            #[cfg(test)]
            queue_edits: 0,
        }
    }
    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(super) fn insert(
        &mut self,
        identity: wire::SessionChannel,
        stream: Source,
        mode: Option<usize>,
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
                    #[cfg(acyclic_stream_loom)]
                    omit_signal: false,
                    #[cfg(acyclic_stream_loom)]
                    clear_after_recheck: false,
                    #[cfg(acyclic_stream_loom)]
                    release_only: false,
                }),
                opening: true,
                mode,
                ready: true,
                links: None,
            },
        );
        self.enqueue(identity.channel_id);
    }
    pub(super) fn mode(&mut self, id: u64, mode: Option<usize>) {
        if self.entries.get(&id).is_none_or(|entry| entry.mode == mode) {
            return;
        }
        self.unlink(id);
        if let Some(entry) = self.entries.get_mut(&id) {
            entry.mode = mode;
        }
        self.enqueue(id);
    }
    pub(super) fn remove(&mut self, id: u64) {
        self.unlink(id);
        self.entries.remove(&id);
    }
    fn unlink(&mut self, id: u64) {
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
    fn enqueue(&mut self, id: u64) {
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
    fn wake(&mut self, identity: wire::SessionChannel) {
        if let Some(entry) = self.entries.get_mut(&identity.channel_id)
            && entry.wake.identity == identity
        {
            entry.ready = true;
            self.enqueue(identity.channel_id);
        }
    }
    fn pop(&mut self, room: (bool, bool, bool)) -> Option<u64> {
        let all = room.1 && room.2;
        let enabled = [true, all, room.0, all || room.0];
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
    pub(super) fn next_with_control(
        &mut self,
        control: &mut Self,
        room: (bool, bool, bool),
    ) -> impl std::future::Future<Output = Result<Option<SourceItem>, StreamError>> + Unpin {
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
    #[cfg(test)]
    pub(super) fn next(
        &mut self,
        room: (bool, bool, bool),
    ) -> impl std::future::Future<Output = Result<Option<SourceItem>, StreamError>> + Unpin {
        std::future::poll_fn(move |cx| self.poll(room, cx))
    }
    fn poll(
        &mut self,
        room: (bool, bool, bool),
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<SourceItem>, StreamError>> {
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
                    entry
                        .stream
                        .as_mut()
                        .poll_next(&mut Context::from_waker(&waker))
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
                    cx.waker().wake_by_ref();
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

impl Channel {
    pub(super) fn source_mode(&self) -> Option<usize> {
        if self.ending.is_some() || self.progress.is_some() {
            None
        } else {
            let all = self.pending.is_empty() && self.credit.frames > 0 && self.credit.bytes > 0;
            // One bounded lookahead permits metadata progress without data
            // credit. A matching record fills the queue and stops this path.
            let latest = (self.latest && self.pending.len() < 3)
                || (self.pending.is_empty() && self.binding.as_ref().is_some_and(|b| !b.children));
            match (all, latest) {
                (true, true) => Some(EITHER),
                (true, false) => Some(ALL),
                (false, true) => Some(LATEST),
                (false, false) => None,
            }
        }
    }
}
impl<P: StreamProvider + ?Sized> Session<P> {
    pub(super) fn refresh_source(&mut self, id: u64) {
        let mode = self.channels.get(&id).and_then(Channel::source_mode);
        if let Some(channel) = self.channels.get_mut(&id) {
            if let Some(length) = channel.indexed.take() {
                self.runnable.remove(&(length, id));
            }
            if channel.credit.frames > 0
                && let Some(head) = channel.pending.front()
                && head.frame.encoded_len() as u64 <= channel.credit.bytes
            {
                let length = head.frame.encoded_len();
                channel.indexed = Some(length);
                self.runnable.insert((length, id));
            }
        }
        self.sources.mode(id, mode);
    }
}
