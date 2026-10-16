//! One owner polls only provider sources made runnable by push or credit.
use super::sync::{AtomicU64, Ordering};
use super::*;
use futures::task::ArcWake;
use std::task::{Context, Poll};

#[cfg(acyclic_stream_loom)]
pub(super) mod model;
#[cfg(all(test, not(acyclic_stream_loom)))]
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
}
impl ArcWake for Wake {
    fn wake_by_ref(wake: &Arc<Self>) {
        if wake.queued.swap(1, Ordering::AcqRel) == 0 {
            #[cfg(acyclic_stream_loom)]
            if wake.omit_signal {
                return;
            }
            let _ = wake.signal.send(wake.identity);
        }
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
}
impl Sources {
    pub(super) fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            queues: Default::default(),
            signal: Arc::new(signal::Signal::new()),
            preferred: 0,
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
        if let Some(before) = links.before {
            self.entries
                .get_mut(&before)
                .unwrap()
                .links
                .as_mut()
                .unwrap()
                .after = links.after;
        } else {
            self.queues[links.class].first = links.after;
        }
        if let Some(after) = links.after {
            self.entries
                .get_mut(&after)
                .unwrap()
                .links
                .as_mut()
                .unwrap()
                .before = links.before;
        } else {
            self.queues[links.class].last = links.before;
        }
    }
    fn enqueue(&mut self, id: u64) {
        let Some(entry) = self.entries.get(&id) else {
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
        let before = self.queues[class].last;
        if let Some(before) = before {
            self.entries
                .get_mut(&before)
                .unwrap()
                .links
                .as_mut()
                .unwrap()
                .after = Some(id);
        } else {
            self.queues[class].first = Some(id);
        }
        self.queues[class].last = Some(id);
        self.entries.get_mut(&id).unwrap().links = Some(Links {
            class,
            before,
            after: None,
        });
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
            if enabled[class]
                && let Some(id) = self.queues[class].first
            {
                self.unlink(id);
                self.preferred = (class + 1) % self.queues.len();
                return Some(id);
            }
        }
        None
    }
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
                let entry = self.entries.get_mut(&id).unwrap();
                entry.ready = false;
                entry.wake.queued.store(0, Ordering::Release);
                let waker = futures::task::waker_ref(&entry.wake);
                match entry
                    .stream
                    .as_mut()
                    .poll_next(&mut Context::from_waker(&waker))
                {
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
        if self.ending.is_some() {
            None
        } else {
            let all = self.pending == 0 && self.credit.frames > 0 && self.credit.bytes > 0;
            let latest = self.latest && self.pending < 3;
            match (all, latest) {
                (true, true) => Some(EITHER),
                (true, false) => Some(ALL),
                (false, true) => Some(LATEST),
                (false, false) => None,
            }
        }
    }
}
impl<P: StreamProvider> Session<P> {
    pub(super) fn refresh_source(&mut self, id: u64) {
        let mode = self.channels.get(&id).and_then(Channel::source_mode);
        if self
            .channels
            .get(&id)
            .is_some_and(|c| c.pending > 0 && c.credit.frames > 0 && c.credit.bytes > 0)
        {
            self.runnable.insert(id);
        } else {
            self.runnable.remove(&id);
        }
        self.sources.mode(id, mode);
    }
}
