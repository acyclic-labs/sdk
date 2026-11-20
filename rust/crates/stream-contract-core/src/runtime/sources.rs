//! One persistent wake and queue node per provider source.
use super::signal;
use super::sync::{AtomicU64, Ordering};
use crate::StreamError;
use acyclic_collections::{FixedIndex, IndexWork};
use acyclic_stream_wire::wire;
use futures::{
    Stream,
    task::{ArcWake, AtomicWaker},
};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

// Source registration, like its existing stream and wake owners, is constructed
// by the event-loop owner without a separate allocation ledger. Reads and
// retirement use the same fixed-width index as the other primitive indexes.
#[derive(Default)]
struct SourceIndexWork {
    #[cfg(test)]
    examined: u64,
}
impl IndexWork for SourceIndexWork {
    type Error = std::convert::Infallible;
    type Allocation = ();

    fn check(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn charge_items(&mut self, _items: u64) -> Result<(), Self::Error> {
        #[cfg(test)]
        {
            self.examined += _items;
        }
        Ok(())
    }
    fn claim(&mut self, _: u64) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn infallible<T>(result: Result<T, std::convert::Infallible>) -> T {
    match result {
        Ok(value) => value,
        Err(impossible) => match impossible {},
    }
}

/// Fixed-width index from channel id to source entry.
pub struct SourceIndex<S, N: SourceWake = ()> {
    entries: FixedIndex<Entry<S, N>, (), 8>,
    length: usize,
}
impl<S, N: SourceWake> SourceIndex<S, N> {
    fn new() -> Self {
        Self {
            entries: FixedIndex::default(),
            length: 0,
        }
    }
    /// The entry for `id`, if it is registered.
    ///
    /// # Panics
    ///
    /// In tests, when the lookup examines more than the fixed key width.
    pub fn get(&self, id: &u64) -> Option<&Entry<S, N>> {
        let mut work = SourceIndexWork::default();
        let entry = infallible(self.entries.get(id.to_be_bytes(), &mut work));
        #[cfg(test)]
        assert!(
            work.examined <= 73,
            "source lookup exceeded the fixed key width"
        );
        entry
    }
    /// The entry for `id`, if it is registered.
    ///
    /// # Panics
    ///
    /// In tests, when the lookup examines more than the fixed key width.
    pub fn get_mut(&mut self, id: &u64) -> Option<&mut Entry<S, N>> {
        let mut work = SourceIndexWork::default();
        let entry = infallible(self.entries.get_mut(id.to_be_bytes(), &mut work));
        #[cfg(test)]
        assert!(
            work.examined <= 73,
            "source lookup exceeded the fixed key width"
        );
        entry
    }
    fn insert(&mut self, id: u64, entry: Entry<S, N>) {
        // The owning insertion path has already removed this exact channel.
        infallible(self.entries.get_or_try_insert_with(
            id.to_be_bytes(),
            &mut SourceIndexWork::default(),
            |_| Ok(entry),
        ));
        self.length += 1;
    }
    fn remove(&mut self, id: &u64) -> Option<Entry<S, N>> {
        let removed = self.entries.remove_if(id.to_be_bytes(), |_| true)?;
        self.length -= 1;
        Some(removed.into_parts().1)
    }
    fn len(&self) -> usize {
        self.length
    }
    fn is_empty(&self) -> bool {
        self.length == 0
    }
}
/// Queue of sources that have not delivered their first item.
pub const OPENING: usize = 0;
/// Queue of sources ready for any item.
pub const ALL: usize = 1;
#[cfg(any(
    not(target_arch = "wasm32"),
    all(feature = "wasm", target_arch = "wasm32")
))]
/// Queue of sources serving the latest record first.
pub const LATEST: usize = 2;
#[cfg(any(
    not(target_arch = "wasm32"),
    all(feature = "wasm", target_arch = "wasm32")
))]
/// Queue of sources serving either direction.
pub const EITHER: usize = 3;

/// Optional notification accompanying a source's shared ready hint.
/// The default has no storage or wake work.
pub trait SourceWake: Default + Send + Sync + 'static {
    /// Whether external interest changes can accompany independent reads.
    const NOTIFY_INTEREST: bool;
    /// Notify an independently waiting reader.
    fn wake(&self);
}

impl SourceWake for () {
    const NOTIFY_INTEREST: bool = false;
    fn wake(&self) {}
}

/// Wake registration for an independently polled source.
#[derive(Default)]
pub struct SourceReader(AtomicWaker);

impl SourceWake for SourceReader {
    const NOTIFY_INTEREST: bool = true;
    fn wake(&self) {
        self.0.wake();
    }
}

/// One source's shared ready hint and reader wake.
pub struct Wake<N: SourceWake = ()> {
    /// The channel this source was registered under.
    pub identity: wire::SessionChannel,
    /// Whether a hint is already queued for this source.
    pub queued: AtomicU64,
    /// The owner's signal that a source became ready.
    pub signal: Arc<signal::Signal<wire::SessionChannel>>,
    /// The provider's own wake, if it has one.
    pub reader: N,
    #[cfg(loom)]
    pub omit_signal: bool,
    #[cfg(loom)]
    pub clear_after_recheck: bool,
    #[cfg(loom)]
    pub release_only: bool,
}
impl<N: SourceWake> Wake<N> {
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
impl<N: SourceWake> Wake<N> {
    // Return whether this publication joined an already queued hint. The model
    // uses that observation without adding a synchronizing lock or handshake.
    /// Publishes a ready hint, returning whether one was already queued.
    pub fn notify(&self) -> bool {
        let coalesced = self.queued.swap(1, Ordering::AcqRel) != 0;
        if !coalesced {
            #[cfg(loom)]
            if self.omit_signal {
                return false;
            }
            let _ = self.signal.send(self.identity);
        }
        self.reader.wake();
        coalesced
    }
}
impl<N: SourceWake> ArcWake for Wake<N> {
    fn wake_by_ref(wake: &Arc<Self>) {
        wake.notify();
    }
}

#[derive(Clone, Copy)]
/// A queued source's neighbours in its class queue.
pub struct Links {
    /// The queue this source is linked into.
    pub class: usize,
    /// The source ahead of this one.
    pub before: Option<u64>,
    /// The source behind this one.
    pub after: Option<u64>,
}
#[derive(Default)]
/// One class queue's ends.
pub struct Queue {
    /// The source at the head of the queue.
    pub first: Option<u64>,
    /// The source at the tail of the queue.
    pub last: Option<u64>,
}
/// One registered source and its queue state.
pub struct Entry<S, N: SourceWake = ()> {
    /// The provider stream this entry owns.
    pub stream: S,
    /// The shared ready hint the provider waker touches.
    pub wake: Arc<Wake<N>>,
    /// Whether the source has yet to deliver its first item.
    pub opening: bool,
    /// The queue class the source serves, if it has one.
    pub mode: Option<usize>,
    /// Whether the source has an item to deliver.
    pub ready: bool,
    /// The source's queue neighbours while it is queued.
    pub links: Option<Links>,
}

/// Fair, wake-driven provider streams sharing persistent queue nodes.
///
/// The owner polls only runnable sources. Receiving an item moves that source
/// behind its ready peers without allocating another task. Removing a source
/// drops its stream immediately, independently of delivery credit. An exhausted
/// source is removed; callers that require a typed closure must emit that item.
pub struct ReadySources<S, N: SourceWake = ()> {
    // Streams, links and modes belong exclusively to the session event loop.
    // A provider waker touches only its atomic and the existing Signal lock;
    // it never polls a provider or acquires another lock while signaling.
    /// Every registered source, by channel id.
    pub entries: SourceIndex<S, N>,
    /// The four class queues' ends.
    pub queues: [Queue; 4],
    /// The signal providers publish ready hints through.
    pub signal: Arc<signal::Signal<wire::SessionChannel>>,
    preferred: usize,
    #[cfg(any(test, feature = "test-support"))]
    /// Queue link edits, counted by the fairness tests.
    pub queue_edits: usize,
}
impl<S: Stream + Unpin> ReadySources<S> {
    /// Creates an empty owner with no task or timer.
    #[must_use]
    pub fn new() -> Self {
        Self::empty()
    }
}
impl<S: Stream + Unpin, N: SourceWake> ReadySources<S, N> {
    fn empty() -> Self {
        Self {
            entries: SourceIndex::new(),
            queues: Default::default(),
            signal: Arc::new(signal::Signal::new()),
            preferred: 0,
            #[cfg(any(test, feature = "test-support"))]
            queue_edits: 0,
        }
    }
    /// Whether any source is retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[cfg(any(
        not(target_arch = "wasm32"),
        all(feature = "wasm", target_arch = "wasm32")
    ))]
    /// Registers a source that has not delivered its first item.
    pub fn insert(&mut self, identity: wire::SessionChannel, stream: S, mode: Option<usize>) {
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
                    reader: N::default(),
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
        if N::NOTIFY_INTEREST {
            let _ = self.signal.wake();
        }
    }
    #[cfg(any(
        test,
        not(target_arch = "wasm32"),
        all(feature = "wasm", target_arch = "wasm32")
    ))]
    /// Moves a source to another queue class.
    pub fn mode(&mut self, id: u64, mode: Option<usize>) {
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
        if let Some(entry) = self.entries.remove(&id) {
            entry.wake.reader.wake();
            if N::NOTIFY_INTEREST {
                let _ = self.signal.wake();
            }
        }
    }
    /// Removes a source from its queue without dropping it.
    pub fn unlink(&mut self, id: u64) {
        let Some(links) = self.entries.get_mut(&id).and_then(|e| e.links.take()) else {
            return;
        };
        #[cfg(any(test, feature = "test-support"))]
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
    /// Links a ready source at the tail of its class queue.
    pub fn enqueue(&mut self, id: u64) {
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
        #[cfg(any(test, feature = "test-support"))]
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
    /// Marks the source ready and queues it.
    pub fn wake(&mut self, identity: wire::SessionChannel) {
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
    #[cfg(any(
        not(target_arch = "wasm32"),
        all(feature = "wasm", target_arch = "wasm32")
    ))]
    /// The next item from either this owner or its control owner.
    pub fn next_with_control(
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
    /// Polls the runnable sources in fair order.
    pub fn poll(
        &mut self,
        room: (bool, bool, bool),
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<S::Item>, StreamError>> {
        if let Some(identity) = self.signal.try_next()? {
            self.wake(identity);
        }
        if let Some(id) = self.pop(room) {
            match self.poll_entry(id) {
                Poll::Ready(Ok(Some(event))) => return Poll::Ready(Ok(Some(event))),
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(None)) | Poll::Pending => {}
            }
        }
        if self.is_empty() {
            return Poll::Ready(Ok(None));
        }
        // A provider can publish while being polled, including through an old
        // cancelled callback. Consume one hint and retain the parent under the
        // same signal lock, without draining the other sources' publications.
        let (identity, buffered) = self.signal.take_and_listen(cx)?;
        if let Some(identity) = identity {
            self.wake(identity);
        }
        if buffered
            || self
                .queues
                .iter()
                .zip(Self::enabled(room))
                .any(|(queue, enabled)| enabled && queue.first.is_some())
        {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
    // One targeted recheck shares the original provider wake and queue node.
    fn poll_entry(&mut self, id: u64) -> Poll<Result<Option<S::Item>, StreamError>> {
        let Some(entry) = self.entries.get_mut(&id) else {
            return Poll::Ready(Err(StreamError::Unavailable));
        };
        entry.ready = false;
        let waker = futures::task::waker_ref(&entry.wake);
        let result = entry
            .wake
            .recheck(|| Pin::new(&mut entry.stream).poll_next(&mut Context::from_waker(&waker)));
        match &result {
            Poll::Ready(Some(_)) => {
                entry.opening = false;
                entry.ready = true;
                self.enqueue(id);
            }
            Poll::Ready(None) => self.remove(id),
            Poll::Pending => {}
        }
        result.map(Ok)
    }
    // Keep keyed turns within the same bounded hint work as combined delivery.
    fn notified(&mut self) -> Result<(), StreamError> {
        if let Some(identity) = self.signal.try_next()? {
            self.wake(identity);
        }
        Ok(())
    }
}

impl<S: Stream + Unpin, N: SourceWake> Default for ReadySources<S, N> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<S: Stream + Unpin, N: SourceWake> ReadySources<S, N> {
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
        self.entries.remove(&id).map(|entry| {
            entry.wake.reader.wake();
            if N::NOTIFY_INTEREST {
                let _ = self.signal.wake();
            }
            entry.stream
        })
    }

    /// Polls one ready source, with round-robin service among ready peers.
    /// Pending sources remain asleep until their provider's registered wake.
    /// Each call consumes at most two wake hints and polls at most one provider.
    /// Buffered hints and runnable peers fund a subsequent cooperative turn.
    pub fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<S::Item>, StreamError>> {
        self.poll((true, true, true), cx)
    }
}

impl<S: Stream + Unpin> ReadySources<S, SourceReader> {
    /// Creates an owner supporting any-ready and independent keyed reads.
    #[must_use]
    pub fn with_readers() -> Self {
        Self::empty()
    }

    /// Polls one original registration using the same provider wake and queue node.
    /// A removed or replaced generation returns None without polling its successor.
    /// Each key retains its own pending reader; another key cannot replace it.
    /// Removing the source wakes its keyed reader, whose next poll returns None.
    /// Each turn consumes at most two hints and rechecks only the selected provider.
    pub fn poll_one(
        &mut self,
        identity: wire::SessionChannel,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<S::Item>, StreamError>> {
        // Direct readers retire the same hints as combined delivery. Leaving
        // them queued would retain one obsolete hint per historical wake.
        self.notified()?;
        let id = identity.channel_id;
        let Some(entry) = self
            .entries
            .get(&id)
            .filter(|entry| entry.wake.identity == identity)
        else {
            return Poll::Ready(Ok(None));
        };
        entry.wake.reader.0.register(cx.waker());
        self.unlink(id);
        let result = self.poll_entry(id);
        self.notified()?;
        if result.is_ready()
            && let Some(entry) = self.entries.get(&id)
        {
            entry.wake.reader.0.take();
        }
        result
    }
}
