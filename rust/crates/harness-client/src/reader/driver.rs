//! The one platform driver: performs the reader's ops against any
//! [`StreamProvider`] and keeps its follows open on that provider. A session
//! provider multiplexes every follow onto its one physical session, whose
//! receive credit is the backpressure: the driver stops polling follows once
//! the frame's delivery budget is spent. Native and WASM share this code.
//!
//! A follow that resumes after cached records is checked (`follow_checked`)
//! against the commit of the last one, so a provisional suffix the server
//! replaced is detected and dropped instead of joined to new history.

use super::{Failure, Fetched, HeadObservation, ListedChild, Op, Outcome, Reader};
use acyclic_stream::{
    ChildrenPageRequest, PathHead, ReadRequest, Record, RecordStream, StreamError, StreamPath,
    StreamProvider,
};
use acyclic_stream::{StreamProviderExt as _, runtime::ReadySources};
use futures::Stream;
use futures::future::{AbortHandle, Abortable, LocalBoxFuture};
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

type Opened = (
    StreamPath,
    u64,
    Option<(u64, u64)>,
    Result<(PathHead, RecordStream), StreamError>,
);
type Opening = LocalBoxFuture<'static, Option<Opened>>;

struct FollowSource {
    path: StreamPath,
    position: u64,
    accepted: u64,
    records: RecordStream,
}

struct Waiting {
    from: u64,
    before: Option<StreamPath>,
    after: Option<StreamPath>,
}

impl Stream for FollowSource {
    type Item = (StreamPath, u64, Result<Record, StreamError>);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let source = self.get_mut();
        match source.records.poll_next_unpin(cx) {
            Poll::Ready(record) => {
                let record = record.unwrap_or(Err(StreamError::Unavailable));
                if let Ok(record) = &record {
                    source.position = record.sequence + 1;
                    source.accepted = source.accepted.max(source.position);
                }
                Poll::Ready(Some((source.path.clone(), source.accepted, record)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
/// Executes one reader's ops and follows on one provider.
pub struct Driver<P: ?Sized> {
    identity: Arc<()>,
    reader: Option<Arc<()>>,
    provider: Arc<P>,
    ops: FuturesUnordered<LocalBoxFuture<'static, Option<(u64, Outcome)>>>,
    aborts: BTreeMap<u64, AbortHandle>,
    opening: FuturesUnordered<Opening>,
    /// Opens in flight, each cancelled once its stream or start is unwanted.
    pending: BTreeMap<StreamPath, (u64, AbortHandle)>,
    follows: BTreeMap<StreamPath, u64>,
    waiting: BTreeMap<StreamPath, Waiting>,
    first_waiting: Option<StreamPath>,
    last_waiting: Option<StreamPath>,
    ready: ReadySources<FollowSource>,
    next_follow: u64,
    #[cfg(test)]
    reconciled: usize,
}

impl<P: StreamProvider + ?Sized> Driver<P> {
    /// A driver over one provider session.
    pub fn new(provider: Arc<P>) -> Self {
        Self {
            identity: Arc::new(()),
            reader: None,
            provider,
            ops: FuturesUnordered::new(),
            aborts: BTreeMap::new(),
            opening: FuturesUnordered::new(),
            pending: BTreeMap::new(),
            follows: BTreeMap::new(),
            waiting: BTreeMap::new(),
            first_waiting: None,
            last_waiting: None,
            ready: ReadySources::new(),
            next_follow: 0,
            #[cfg(test)]
            reconciled: 0,
        }
    }

    /// Live follows currently open.
    #[must_use]
    pub fn open_follows(&self) -> usize {
        self.follows.len() + self.pending.len()
    }

    /// Register this provider's source before restoring or accessing retained
    /// state. Repeated attachment of the same pair does no work. Replacing
    /// either member retires outstanding observations and cached authority.
    pub fn attach(&mut self, reader: &mut Reader, now: u64) {
        let attached = self
            .reader
            .as_ref()
            .is_some_and(|identity| Arc::ptr_eq(identity, &reader.identity))
            && reader
                .driver
                .as_ref()
                .is_some_and(|driver| Arc::ptr_eq(driver, &self.identity));
        if !attached {
            reader.configure_source(self.provider.as_ref(), now);
            reader.driver = Some(self.identity.clone());
            self.reader = Some(reader.identity.clone());
            self.clear();
        }
    }

    fn clear(&mut self) {
        self.ops = FuturesUnordered::new();
        self.aborts.clear();
        self.opening = FuturesUnordered::new();
        self.pending.clear();
        self.follows.clear();
        self.waiting.clear();
        self.first_waiting = None;
        self.last_waiting = None;
        self.ready = ReadySources::new();
        self.next_follow = 0;
    }

    /// Advance once: start admitted reads, fold completions, reconcile and
    /// drain follows within the frame budget. Wakes `cx` on progress.
    pub fn step(&mut self, reader: &mut Reader, now: u64, cx: &mut Context<'_>) {
        self.attach(reader, now);
        let changed = reader.advance_credentials(now, cx);
        if changed {
            self.clear();
        }
        if !reader.readable() {
            if !changed {
                self.clear();
            }
            return;
        }
        for id in reader.take_cancelled() {
            if let Some(abort) = self.aborts.remove(&id) {
                abort.abort();
            }
        }
        for op in reader.poll(now) {
            self.start(op, reader.cache.access.clone());
        }
        let mut completed = false;
        while let Poll::Ready(Some(done)) = self.ops.poll_next_unpin(cx) {
            if let Some((id, outcome)) = done {
                self.aborts.remove(&id);
                reader.complete(id, outcome, now);
                completed = true;
            }
        }
        // Freed request credit serves the remaining plan even before a UI
        // frame. Transport failures still wait for the reader's deadline.
        if completed && reader.ready(now) && !reader.demand.is_empty() {
            cx.waker().wake_by_ref();
        }
        let parked = self.reconcile(reader, now);
        let mut completed_open = false;
        while let Poll::Ready(Some(done)) = self.opening.poll_next_unpin(cx) {
            // Cancelled opens never complete, so this is the current one.
            let Some((stream, from, observed, opened)) = done else {
                continue;
            };
            self.pending.remove(&stream);
            completed_open = true;
            if reader.follows.get(&stream).map(|intent| intent.from) != Some(from) {
                continue;
            }
            match opened {
                Ok((head, records)) => {
                    let accepted = head.tail.max(reader.cache().tail(&stream).unwrap_or(0));
                    match reader.observed_head(&stream, head, observed) {
                        HeadObservation::Invalid => {
                            reader.follow_failed(Failure::Refused, now);
                            self.queue(&stream, reader);
                            continue;
                        }
                        HeadObservation::Superseded => {
                            self.queue(&stream, reader);
                            cx.waker().wake_by_ref();
                            continue;
                        }
                        HeadObservation::Applied => {}
                    }
                    // A changed origin invalidated this opening and its old
                    // cursor. The next frame opens the newly learned history.
                    if !reader.follows.contains_key(&stream) {
                        continue;
                    }
                    reader.follow_opened(&stream, from);
                    if let Err(error) = self.opened(stream.clone(), from, accepted, records) {
                        reader.follow_failed(Failure::of(&error), now);
                        self.queue(&stream, reader);
                    }
                }
                Err(error) => {
                    failed(reader, &stream, &error, now);
                    self.queue(&stream, reader);
                }
            }
        }
        self.drain(reader, now, cx);
        if parked && completed_open && reader.ready(now) && reader.accepting() {
            cx.waker().wake_by_ref();
        }
    }

    fn opened(
        &mut self,
        path: StreamPath,
        position: u64,
        accepted: u64,
        records: RecordStream,
    ) -> Result<(), StreamError> {
        let id = self.next_follow;
        self.next_follow = id.checked_add(1).ok_or(StreamError::Capacity)?;
        self.ready.register(
            acyclic_stream::wire::SessionChannel {
                channel_id: id,
                generation: 0,
            },
            FollowSource {
                path: path.clone(),
                position,
                accepted,
                records,
            },
        );
        if let Some(previous) = self.follows.insert(path, id) {
            self.ready.remove(previous);
        }
        Ok(())
    }
    fn start(&mut self, op: Op, access: super::Access) {
        let provider = Arc::clone(&self.provider);
        let id = op.id();
        let (abort, registration) = AbortHandle::new_pair();
        self.aborts.insert(id, abort);
        let work = async move {
            if !access.check().await {
                return Some((id, Outcome::Failed(Failure::Unavailable)));
            }
            let outcome = execute(provider.as_ref(), op).await;
            Some((
                id,
                if access.check().await {
                    outcome
                } else {
                    Outcome::Failed(Failure::Unavailable)
                },
            ))
        };
        self.ops.push(Box::pin(async move {
            Abortable::new(work, registration).await.ok().flatten()
        }));
    }

    fn unqueue(&mut self, path: &StreamPath) -> Option<u64> {
        let waiting = self.waiting.remove(path)?;
        if let Some(before) = &waiting.before {
            if let Some(previous) = self.waiting.get_mut(before) {
                previous.after = waiting.after.clone();
            }
        } else {
            self.first_waiting = waiting.after.clone();
        }
        if let Some(after) = &waiting.after {
            if let Some(next) = self.waiting.get_mut(after) {
                next.before = waiting.before;
            }
        } else {
            self.last_waiting = waiting.before;
        }
        Some(waiting.from)
    }

    fn queue(&mut self, path: &StreamPath, reader: &Reader) {
        let Some(from) = reader.follows.get(path).map(|intent| intent.from) else {
            let _ = self.unqueue(path);
            return;
        };
        if self.follows.contains_key(path) || self.pending.contains_key(path) {
            let _ = self.unqueue(path);
        } else if let Some(waiting) = self.waiting.get_mut(path) {
            waiting.from = from;
        } else {
            // One removable queue node per retained intent. New requests stay
            // behind work already waiting for opening credit.
            if let Some(last) = self.last_waiting.as_ref() {
                if let Some(previous) = self.waiting.get_mut(last) {
                    previous.after = Some(path.clone());
                }
            } else {
                self.first_waiting = Some(path.clone());
            }
            self.waiting.insert(
                path.clone(),
                Waiting {
                    from,
                    before: self.last_waiting.replace(path.clone()),
                    after: None,
                },
            );
        }
    }

    fn reconcile(&mut self, reader: &mut Reader, now: u64) -> bool {
        // Cursor delivery does not change subscription intent. Only explicit
        // selection, lag jumps and invalidation reach this index.
        while let Some(path) = reader.follow_changes.pop_first() {
            #[cfg(test)]
            {
                self.reconciled += 1;
            }
            let from = reader.follows.get(&path).map(|intent| intent.from);
            if let Some(id) = self.follows.get(&path).copied()
                && !from.is_some_and(|from| {
                    self.ready
                        .get(id)
                        .is_some_and(|source| from <= source.position)
                })
            {
                self.follows.remove(&path);
                self.ready.remove(id);
            }
            if let Some((pending_from, _)) = self.pending.get(&path)
                && from != Some(*pending_from)
                && let Some((_, abort)) = self.pending.remove(&path)
            {
                abort.abort();
            }
            self.queue(&path, reader);
        }
        if !reader.ready(now) || !reader.accepting() {
            return false;
        }
        while self.pending.len() < reader.policy.follow_requests {
            let Some(path) = self.first_waiting.clone() else {
                break;
            };
            let Some(from) = self.unqueue(&path) else {
                break;
            };
            let (abort, registration) = AbortHandle::new_pair();
            self.pending.insert(path.clone(), (from, abort));
            let anchor = reader.follow_anchor(&path, from);
            let observed = reader.cache().observation(&path);
            let provider = Arc::clone(&self.provider);
            let access = reader.cache.access.clone();
            let open = async move {
                let opened = async {
                    if !access.check().await {
                        return Err(StreamError::Unavailable);
                    }
                    let head = provider.head(path.clone()).await;
                    if !access.check().await {
                        return Err(StreamError::Unavailable);
                    }
                    let head = head?;
                    let records = provider
                        .follow(acyclic_stream::FollowRequest {
                            preceding: anchor,
                            ..acyclic_stream::FollowRequest::new(path.clone(), from)
                        })
                        .await
                        .map(acyclic_stream::records_only);
                    if !access.check().await {
                        return Err(StreamError::Unavailable);
                    }
                    records.map(|records| (head, records))
                }
                .await;
                Some((path, from, observed, opened))
            };
            self.opening.push(Box::pin(async move {
                Abortable::new(open, registration).await.ok().flatten()
            }));
        }
        !self.waiting.is_empty()
    }

    fn drain(&mut self, reader: &mut Reader, now: u64, cx: &mut Context<'_>) {
        while reader.accepting() {
            let (path, accepted, record) = match self.ready.poll_next(cx) {
                Poll::Ready(Ok(Some(record))) => record,
                Poll::Ready(Err(error)) => {
                    reader.follow_failed(Failure::of(&error), now);
                    for (path, _) in std::mem::take(&mut self.follows) {
                        self.queue(&path, reader);
                    }
                    self.ready = ReadySources::new();
                    break;
                }
                Poll::Ready(Ok(None)) | Poll::Pending => break,
            };
            match record {
                Ok(record) => reader.deliver(&path, fetched(record), accepted),
                Err(error) => {
                    failed(reader, &path, &error, now);
                    if let Some(id) = self.follows.remove(&path) {
                        self.ready.remove(id);
                    }
                    self.queue(&path, reader);
                }
            }
        }
    }
}

/// A changed anchor replaced the provisional suffix; anything else backs off.
fn failed(reader: &mut Reader, stream: &StreamPath, error: &StreamError, now: u64) {
    if matches!(error, StreamError::PrefixNotRetained) {
        reader.replaced(stream);
    } else if matches!(error, StreamError::AccessDenied) {
        reader.denied(stream);
    } else {
        reader.follow_failed(Failure::of(error), now);
    }
}

/// Preserve the primitive allocation owner; the cache decides whether a
/// transport slice needs detaching to establish a bounded retained charge.
pub(super) fn fetched(record: Record) -> Fetched {
    Fetched {
        sequence: record.sequence,
        value: record.value,
        commit: record.commit_id,
    }
}

async fn execute<P: StreamProvider + ?Sized>(provider: &P, op: Op) -> Outcome {
    let result = match op {
        Op::Read {
            stream,
            from,
            limit,
            ..
        } => read(provider, stream, from, limit).await,
        Op::Head { stream, .. } => provider.head(stream).await.map(Outcome::Head),
        Op::Children {
            parent,
            after,
            limit,
            tail,
            version,
            ..
        } => provider
            .children_page(ChildrenPageRequest {
                tail_stream: tail,
                parent: Some(parent),
                after,
                hierarchy_version: version,
                limit,
            })
            .await
            .map(|page| Outcome::Children {
                next: page.next_after,
                version: page.hierarchy_version,
                children: page
                    .children
                    .into_iter()
                    .map(|child| ListedChild {
                        path: child.path,
                        origin: child.origin,
                        tail: child.tail_record.map(fetched),
                    })
                    .collect(),
            }),
    };
    result.unwrap_or_else(|error| Outcome::Failed(Failure::of(&error)))
}

async fn read<P: StreamProvider + ?Sized>(
    provider: &P,
    path: StreamPath,
    from: u64,
    limit: u32,
) -> Result<Outcome, StreamError> {
    let mut records = provider
        .read(ReadRequest {
            key: None,
            path,
            from,
            limit,
            at_least: Default::default(),
        })
        .await?;
    let mut out = Vec::new();
    while let Some(record) = records.next().await {
        let record = record?;
        out.push(fetched(record));
        if out.len() >= limit as usize {
            break;
        }
    }
    Ok(Outcome::Records(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_stream::{AppendRequest, ForkRequest, IdempotencyKey, MemoryStream};

    #[test]
    fn every_busy_follow_progresses_across_delivery_frames() {
        use super::super::ReaderPolicy;
        futures::executor::block_on(async {
            for (streams, deliveries, frames) in [(2, 1, 4), (513, 256, 3)] {
                let provider = Arc::new(MemoryStream::default());
                let mut driver = Driver::new(Arc::clone(&provider));
                let mut reader = Reader::new(ReaderPolicy {
                    deliveries,
                    ..ReaderPolicy::default()
                })
                .unwrap();
                for index in 0..streams {
                    let path = StreamPath::new(format!("fair/{index:04}")).unwrap();
                    provider
                        .append(AppendRequest {
                            path: path.clone(),
                            records: vec![b"record".as_slice().into(); 4],
                            if_tail: Some(0),
                            idempotency_key: IdempotencyKey::new(path.to_string()).unwrap(),
                        })
                        .await
                        .unwrap();
                    let records = provider
                        .follow(acyclic_stream::FollowRequest::new(path.clone(), 0))
                        .await
                        .unwrap();
                    reader.follows.insert(
                        path.clone(),
                        super::super::FollowIntent {
                            from: 0,
                            selected: false,
                        },
                    );
                    driver
                        .opened(path, 0, 4, acyclic_stream::records_only(records))
                        .unwrap();
                }
                let waker = futures::task::noop_waker();
                let mut cx = Context::from_waker(&waker);
                for frame in 0..frames {
                    reader.begin_frame(frame);
                    driver.drain(&mut reader, frame, &mut cx);
                    assert!(!reader.accepting());
                }
                assert_eq!(driver.open_follows(), streams);
                let minimum = if streams == 2 { 2 } else { 1 };
                let progressed = reader
                    .follows()
                    .filter(|(_, position)| *position >= minimum)
                    .count();
                assert_eq!(progressed, streams);
                eprintln!(
                    "follow progress: streams={streams} credit={deliveries} frames={frames} progressed={progressed}"
                );
            }
        });
    }

    #[test]
    fn dormant_follows_are_not_repolled_and_cancel_without_delivery_credit() {
        use super::super::ReaderPolicy;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct Counted {
            records: RecordStream,
            polls: Arc<AtomicUsize>,
            dropped: Arc<AtomicUsize>,
        }
        impl Stream for Counted {
            type Item = Result<Record, StreamError>;
            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                self.polls.fetch_add(1, Ordering::SeqCst);
                self.records.poll_next_unpin(cx)
            }
        }
        impl Drop for Counted {
            fn drop(&mut self) {
                self.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }

        futures::executor::block_on(async {
            let provider = Arc::new(MemoryStream::default());
            let mut driver = Driver::new(Arc::clone(&provider));
            let mut reader = Reader::new(ReaderPolicy {
                deliveries: 1,
                ..ReaderPolicy::default()
            })
            .unwrap();
            driver.attach(&mut reader, 0);
            let dropped = Arc::new(AtomicUsize::new(0));
            let mut counters = Vec::new();
            for index in 0..513 {
                let path = StreamPath::new(format!("quiet/{index:04}")).unwrap();
                provider
                    .append(AppendRequest {
                        path: path.clone(),
                        records: vec![b"initial".as_slice().into()],
                        if_tail: Some(0),
                        idempotency_key: IdempotencyKey::new(path.to_string()).unwrap(),
                    })
                    .await
                    .unwrap();
                let records = provider
                    .follow(acyclic_stream::FollowRequest::new(path.clone(), 1))
                    .await
                    .unwrap();
                let polls = Arc::new(AtomicUsize::new(0));
                counters.push(polls.clone());
                reader.follows.insert(
                    path.clone(),
                    super::super::FollowIntent {
                        from: 1,
                        selected: false,
                    },
                );
                driver
                    .opened(
                        path,
                        1,
                        1,
                        Box::pin(Counted {
                            records: acyclic_stream::records_only(records),
                            polls,
                            dropped: dropped.clone(),
                        }),
                    )
                    .unwrap();
            }
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            driver.drain(&mut reader, 0, &mut cx);
            assert!(
                counters
                    .iter()
                    .all(|polls| polls.swap(0, Ordering::SeqCst) == 1)
            );
            let path = StreamPath::new("quiet/0512").unwrap();
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![b"changed".as_slice().into()],
                    if_tail: Some(1),
                    idempotency_key: IdempotencyKey::new(b"changed".to_vec()).unwrap(),
                })
                .await
                .unwrap();
            driver.step(&mut reader, 0, &mut cx);
            assert_eq!(reader.follows.get(&path).map(|intent| intent.from), Some(2));
            assert_eq!(driver.reconciled, 0);
            assert_eq!(counters[512].load(Ordering::SeqCst), 1);
            let dormant_polls: usize = counters[..512]
                .iter()
                .map(|polls| polls.load(Ordering::SeqCst))
                .sum();
            assert_eq!(dormant_polls, 0);
            eprintln!(
                "follow source polls: dormant=512 dormant_polls={dormant_polls} changed_polls={}",
                counters[512].load(Ordering::SeqCst)
            );
            assert!(!reader.accepting());
            reader.set_follows([]);
            driver.step(&mut reader, 0, &mut cx);
            assert_eq!(dropped.load(Ordering::SeqCst), 513);
            assert_eq!(driver.open_follows(), 0);
            assert_eq!(driver.reconciled, 513);
            assert!(driver.waiting.is_empty());
        });
    }

    #[test]
    fn opening_credit_retains_and_services_every_follow_intent() {
        use super::super::ReaderPolicy;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::task::{Wake, Waker};
        struct Woken(AtomicBool);
        impl Wake for Woken {
            fn wake(self: Arc<Self>) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        futures::executor::block_on(async {
            let provider = Arc::new(MemoryStream::default());
            let mut driver = Driver::new(Arc::clone(&provider));
            let mut reader = Reader::new(ReaderPolicy {
                follow_requests: 1,
                ..ReaderPolicy::default()
            })
            .unwrap();
            driver.attach(&mut reader, 0);
            let mut paths = Vec::new();
            for index in 0..5 {
                let path = StreamPath::new(format!("opening/z{index}")).unwrap();
                provider
                    .append(AppendRequest {
                        path: path.clone(),
                        records: vec![b"initial".as_slice().into()],
                        if_tail: Some(0),
                        idempotency_key: IdempotencyKey::new(path.to_string()).unwrap(),
                    })
                    .await
                    .unwrap();
                reader.observed_head(&path, provider.head(path.clone()).await.unwrap(), None);
                paths.push(path);
            }
            reader.set_follows(paths.clone());
            assert_eq!(reader.follows().count(), 5);
            let inserted = reader.follow_insertions;
            reader.set_follows(paths.iter().rev().chain(&paths).cloned());
            assert_eq!(
                reader.follow_insertions, inserted,
                "unchanged selection rebuilt retained follow entries"
            );
            eprintln!("follow membership: selected=5 duplicate_inputs=10 new_entries=0");
            let woken = Arc::new(Woken(AtomicBool::new(true)));
            let waker = Waker::from(woken.clone());
            let mut cx = Context::from_waker(&waker);
            for count in 1..=5 {
                assert!(woken.0.swap(false, Ordering::SeqCst));
                driver.step(&mut reader, 0, &mut cx);
                assert_eq!(driver.follows.len(), count);
                assert!(driver.pending.is_empty());
                assert_eq!(driver.reconciled, count + 4);
                assert!(driver.follows.contains_key(&paths[count - 1]));
                if count < 5 {
                    // Each newcomer sorts before every original request. It
                    // cannot overtake requests already waiting for credit.
                    let path = StreamPath::new(format!("opening/a{count}")).unwrap();
                    provider
                        .append(AppendRequest {
                            path: path.clone(),
                            records: vec![b"new".as_slice().into()],
                            if_tail: Some(0),
                            idempotency_key: IdempotencyKey::new(path.to_string()).unwrap(),
                        })
                        .await
                        .unwrap();
                    reader.observed_head(&path, provider.head(path.clone()).await.unwrap(), None);
                    paths.push(path);
                    reader.set_follows(paths.clone());
                    assert_eq!(reader.follow_insertions, paths.len());
                }
            }
            assert_eq!(driver.waiting.len(), 4);
            for count in 6..=9 {
                assert!(woken.0.swap(false, Ordering::SeqCst));
                driver.step(&mut reader, 0, &mut cx);
                assert_eq!(driver.follows.len(), count);
                assert_eq!(driver.reconciled, 9);
            }
            assert!(driver.waiting.is_empty());
            assert!(driver.first_waiting.is_none());
            assert!(driver.last_waiting.is_none());
            driver.step(&mut reader, 0, &mut cx);
            assert_eq!(driver.reconciled, 9);
            woken.0.store(false, Ordering::SeqCst);
            provider
                .append(AppendRequest {
                    path: paths[8].clone(),
                    records: vec![b"changed".as_slice().into()],
                    if_tail: Some(1),
                    idempotency_key: IdempotencyKey::new(b"changed".to_vec()).unwrap(),
                })
                .await
                .unwrap();
            assert!(woken.0.swap(false, Ordering::SeqCst));
            driver.step(&mut reader, 0, &mut cx);
            assert_eq!(driver.reconciled, 9);
            assert_eq!(
                reader.follows.get(&paths[8]).map(|intent| intent.from),
                Some(2)
            );
            eprintln!("follow reconciliation: unchanged=9 changed_intents=0 record_deliveries=1");
            reader.set_follows([]);
            driver.step(&mut reader, 0, &mut cx);
            assert_eq!(driver.open_follows(), 0);
            // Cancellation unlinks first, middle and last queued intents even
            // while exhausted delivery credit prevents new opens.
            reader.delivered = reader.policy.deliveries;
            reader.set_follows(paths[..4].iter().cloned());
            driver.step(&mut reader, 0, &mut cx);
            assert_eq!(driver.waiting.len(), 4);
            reader.set_follows([paths[1].clone()]);
            driver.step(&mut reader, 0, &mut cx);
            assert_eq!(driver.waiting.len(), 1);
            assert_eq!(driver.first_waiting.as_ref(), Some(&paths[1]));
            assert_eq!(driver.last_waiting.as_ref(), Some(&paths[1]));
            reader.delivered = 0;
            driver.step(&mut reader, 0, &mut cx);
            assert_eq!(driver.open_follows(), 1);
            assert!(driver.follows.contains_key(&paths[1]));
            assert!(driver.waiting.is_empty());
            assert!(driver.first_waiting.is_none());
            assert!(driver.last_waiting.is_none());
            assert_eq!(reader.issued(), 0);
            assert_eq!(reader.next_deadline(), None);
        });
    }

    #[test]
    fn lost_live_follow_reports_failure_and_resumes_without_another_frame() {
        use super::super::{Connectivity, ReaderPolicy};
        use futures::{FutureExt as _, channel::oneshot};
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::task::{Wake, Waker};

        struct Woken(AtomicBool);
        impl Wake for Woken {
            fn wake(self: Arc<Self>) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        futures::executor::block_on(async {
            for append_before_reconnect in [false, true] {
                let provider = Arc::new(MemoryStream::default());
                let path = StreamPath::new("reader/follow-loss").unwrap();
                provider
                    .append(AppendRequest {
                        path: path.clone(),
                        records: vec![b"retained".as_slice().into()],
                        if_tail: None,
                        idempotency_key: IdempotencyKey::new(b"retained".to_vec()).unwrap(),
                    })
                    .await
                    .unwrap();
                let head = provider.head(path.clone()).await.unwrap();
                let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
                let mut driver = Driver::new(Arc::clone(&provider));
                let woken = Arc::new(Woken(AtomicBool::new(false)));
                let waker = Waker::from(Arc::clone(&woken));
                let mut cx = Context::from_waker(&waker);
                driver.attach(&mut reader, 0);
                reader.observed_head(&path, head, None);
                reader.set_follows([path.clone()]);
                driver.step(&mut reader, 0, &mut cx);
                assert_eq!(driver.open_follows(), 1);
                let retained = reader.cache().record(&path, 0).unwrap().clone();

                // Keep the actual provider subscription, with a channel modeling
                // its connection owner. Losing that producer ends this live link.
                let (connection, disconnected) = oneshot::channel::<()>();
                let id = driver.follows.remove(&path).unwrap();
                let source = driver.ready.take(id).unwrap();
                driver
                    .opened(
                        path.clone(),
                        source.position,
                        source.accepted,
                        Box::pin(source.records.take_until(disconnected.map(|_| ()))),
                    )
                    .unwrap();
                driver.step(&mut reader, 1, &mut cx);
                assert_eq!(reader.next_deadline(), None);
                woken.0.store(false, Ordering::SeqCst);
                drop(connection);
                assert!(woken.0.load(Ordering::SeqCst));
                driver.step(&mut reader, 2, &mut cx);
                assert_eq!(driver.open_follows(), 0);
                let Connectivity::Degraded { retry_at } = reader.connectivity() else {
                    panic!("lost live producer must report transport failure");
                };
                assert_eq!(reader.next_deadline(), Some(retry_at));
                assert!(retry_at > 2);
                assert!(reader.issues().next().is_none());
                assert_eq!(reader.cache().record(&path, 0), Some(&retained));

                let append = || {
                    provider.append(AppendRequest {
                        path: path.clone(),
                        records: vec![b"resumed".as_slice().into()],
                        if_tail: None,
                        idempotency_key: IdempotencyKey::new(b"resumed".to_vec()).unwrap(),
                    })
                };
                if append_before_reconnect {
                    append().await.unwrap();
                }
                driver.step(&mut reader, retry_at - 1, &mut cx);
                assert_eq!(driver.open_follows(), 0);
                // The original retry deadline resumes the retained cursor; no new
                // viewport plan, history read or provider record fixture is needed.
                driver.step(&mut reader, retry_at, &mut cx);
                assert_eq!(driver.open_follows(), 1);
                assert_eq!(reader.issued(), 0);
                assert!(matches!(reader.connectivity(), Connectivity::Online));
                assert_eq!(reader.next_deadline(), None);
                if !append_before_reconnect {
                    assert_eq!(
                        driver
                            .ready
                            .get(*driver.follows.get(&path).unwrap())
                            .unwrap()
                            .position,
                        1
                    );
                    woken.0.store(false, Ordering::SeqCst);
                    append().await.unwrap();
                    assert!(woken.0.load(Ordering::SeqCst));
                    driver.step(&mut reader, retry_at, &mut cx);
                }
                assert_eq!(reader.cache().record(&path, 0), Some(&retained));
                assert_eq!(
                    reader.cache().record(&path, 1).unwrap().as_ref(),
                    b"resumed"
                );
                assert!(matches!(reader.connectivity(), Connectivity::Online));
                reader.set_follows([]);
                driver.step(&mut reader, retry_at, &mut cx);
                assert_eq!(driver.open_follows(), 0);
                assert_eq!(reader.next_deadline(), None);
                assert!(matches!(reader.connectivity(), Connectivity::Online));
            }
        });
    }

    #[test]
    fn lost_check_producer_retires_the_request_without_waiting_for_timeout() {
        use super::super::{Connectivity, Priority, ReaderPolicy, Want};
        use acyclic_auth::credential::{
            CredentialAction, CredentialBinding, CredentialCache, CredentialCacheError,
            CredentialError, CredentialProvider, CredentialRequest, CredentialRevision,
            CredentialSnapshot, CredentialValidity, ResolvedCredential,
        };
        use futures::channel::oneshot;
        use std::sync::{
            Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        };
        use std::task::{Wake, Waker};

        struct WakeFlag(AtomicBool);
        impl Wake for WakeFlag {
            fn wake(self: Arc<Self>) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        struct Request {
            lease: ResolvedCredential,
            wait_at: usize,
            calls: Arc<AtomicUsize>,
            receive: Mutex<Option<oneshot::Receiver<()>>>,
            revoke_on_loss: bool,
        }
        impl CredentialRequest for Request {
            fn token(&self) -> &str {
                self.lease.as_str()
            }
            fn binding(&self) -> Option<CredentialBinding> {
                self.lease.binding()
            }
            fn check(&self) -> CredentialAction<()> {
                if self.calls.fetch_add(1, Ordering::SeqCst) + 1 == self.wait_at {
                    let receive = self.receive.lock().unwrap().take().unwrap();
                    let lease = self.lease.clone();
                    let revoke_on_loss = self.revoke_on_loss;
                    CredentialAction::Pending(Box::pin(async move {
                        if receive.await.is_err() {
                            if revoke_on_loss {
                                lease.reject().await?;
                            }
                            return Err(CredentialError::Provider(Box::new(
                                CredentialCacheError::Unavailable,
                            )));
                        }
                        lease.check().await
                    }))
                } else {
                    self.lease.check()
                }
            }
            fn retained_access(&self) -> Option<Result<(), CredentialError>> {
                self.lease.retained_access()
            }
            fn reject(&self) -> CredentialAction<bool> {
                self.lease.reject()
            }
        }

        futures::executor::block_on(async {
            let stream = Arc::new(MemoryStream::default());
            let path = StreamPath::new("check/loss").unwrap();
            stream
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![b"committed".as_slice().into()],
                    if_tail: None,
                    idempotency_key: IdempotencyKey::new(b"check loss".to_vec()).unwrap(),
                })
                .await
                .unwrap();
            let head = stream.head(path.clone()).await.unwrap();
            // The first check admits the original request. Lose the producer
            // at each read or follow boundary on the actual provider.
            for (follow, wait_at, revoke_on_loss) in [
                (false, 2, false),
                (false, 3, false),
                (true, 2, false),
                (true, 3, false),
                (true, 4, false),
                (false, 2, true),
                (false, 3, true),
            ] {
                let provider: CredentialProvider = CredentialCache::new(
                    |_| async {
                        CredentialSnapshot::new(
                            "credential".into(),
                            CredentialRevision::from([1; 32]),
                            20,
                        )
                    },
                    || CredentialValidity {
                        now_unix_seconds: 10,
                        revision: Some(CredentialRevision::from([1; 32])),
                    },
                )
                .into();
                let (send, receive) = oneshot::channel();
                let calls = Arc::new(AtomicUsize::new(0));
                let lease = ResolvedCredential::from_custodian(Request {
                    lease: provider.resolve().await.unwrap(),
                    wait_at,
                    calls: calls.clone(),
                    receive: Mutex::new(Some(receive)),
                    revoke_on_loss,
                })
                .await
                .unwrap();
                let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
                let mut driver = Driver::new(stream.clone());
                driver.attach(&mut reader, 0);
                reader.cache.access = super::super::Access::Lease(lease);
                if follow {
                    reader.cache.observe_origin(&path, head.origin.clone());
                    reader.cache.observe_tail(&path, head.tail);
                    reader.set_follows([path.clone()]);
                } else {
                    reader.plan([(
                        Want::Range {
                            stream: path.clone(),
                            from: 0,
                            to: 1,
                        },
                        Priority::Visible,
                    )]);
                }
                let ready = Arc::new(WakeFlag(AtomicBool::new(false)));
                let waker = Waker::from(ready.clone());
                let mut cx = Context::from_waker(&waker);
                driver.step(&mut reader, 0, &mut cx);
                assert_eq!(reader.residency().requests, usize::from(!follow));
                assert_eq!(driver.open_follows(), usize::from(follow));
                ready.0.store(false, Ordering::SeqCst);
                drop(send);
                assert!(ready.0.load(Ordering::SeqCst));
                driver.step(&mut reader, 0, &mut cx);
                assert_eq!(reader.residency().requests, 0);
                assert_eq!(driver.open_follows(), 0);
                assert_eq!(calls.load(Ordering::SeqCst), wait_at);
                assert!(driver.aborts.is_empty());
                assert!(reader.cache().record(&path, 0).is_none());
                assert!(reader.issues().next().is_none());
                if revoke_on_loss {
                    assert!(!reader.readable());
                } else {
                    assert!(matches!(
                        reader.connectivity(),
                        Connectivity::Degraded { .. }
                    ));
                }
            }
        });
    }

    #[test]
    fn changed_hierarchy_abandons_old_pages_before_another_driver_wake() {
        use super::super::{Priority, ReaderPolicy, Want};

        futures::executor::block_on(async {
            let provider = MemoryStream::default();
            for path in ["a/one", "a/two", "b/one", "b/two"] {
                provider
                    .append(AppendRequest {
                        path: StreamPath::new(path).unwrap(),
                        records: vec![b"value".as_slice().into()],
                        if_tail: None,
                        idempotency_key: IdempotencyKey::new(path.as_bytes().to_vec()).unwrap(),
                    })
                    .await
                    .unwrap();
            }
            let mut reader = Reader::new(ReaderPolicy {
                listing_limit: 1,
                ..ReaderPolicy::default()
            })
            .unwrap();
            let record_path = StreamPath::new("a/one").unwrap();
            let head = provider.head(record_path.clone()).await.unwrap();
            reader.observed_head(&record_path, head, None);
            let parents = [StreamPath::new("a").unwrap(), StreamPath::new("b").unwrap()];
            let want = |parent: &StreamPath, index| {
                (
                    Want::Listing {
                        parent: parent.clone(),
                        index,
                        tail: None,
                        witness: None,
                    },
                    Priority::Visible,
                )
            };
            for parent in &parents {
                reader.plan([want(parent, 0)]);
                let op = reader.poll(0).pop().unwrap();
                let id = op.id();
                reader.complete(id, execute(&provider, op).await, 0);
            }
            reader.plan(parents.iter().map(|parent| want(parent, 1)));
            let mut ops = reader.poll(0);
            assert_eq!(ops.len(), 2);
            let changed = ops.pop().unwrap();
            let late = ops.pop().unwrap();
            let late_id = late.id();
            let old_page = execute(&provider, late).await;
            assert!(matches!(&old_page, Outcome::Children { .. }));
            provider
                .append(AppendRequest {
                    path: StreamPath::new("a/new").unwrap(),
                    records: vec![b"new".as_slice().into()],
                    if_tail: None,
                    idempotency_key: IdempotencyKey::new(b"new child".to_vec()).unwrap(),
                })
                .await
                .unwrap();
            let changed_id = changed.id();
            let outcome = execute(&provider, changed).await;
            assert!(matches!(&outcome, Outcome::Failed(Failure::Changed)));
            reader.complete(changed_id, outcome, 0);
            assert_eq!(reader.take_cancelled(), vec![late_id]);
            reader.complete(late_id, old_page, 0);
            assert!(reader.poll(0).is_empty());
            assert!(
                parents
                    .iter()
                    .all(|parent| reader.cache().listings(parent).next().is_none())
            );
            assert_eq!(
                reader.cache().record(&record_path, 0).unwrap().as_ref(),
                b"value"
            );
            reader.plan(parents.iter().map(|parent| want(parent, 1)));
            let fresh = reader.poll(0);
            assert_eq!(fresh.len(), 2);
            assert!(fresh.iter().all(|op| matches!(
                op,
                Op::Children {
                    index: 0,
                    after: None,
                    version: None,
                    ..
                }
            )));
        });
    }

    #[test]
    fn freed_credit_completes_the_plan_without_another_ui_frame() {
        use super::super::{Priority, ReaderPolicy, Want};
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::task::{Wake, Waker};

        struct Ready(AtomicBool);
        impl Wake for Ready {
            fn wake(self: Arc<Self>) {
                self.wake_by_ref();
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        futures::executor::block_on(async {
            let provider = Arc::new(MemoryStream::default());
            let path = StreamPath::new("reader/credit").unwrap();
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: [b"first".as_slice(), b"second", b"third"]
                        .into_iter()
                        .map(|value| value.to_vec().into())
                        .collect(),
                    if_tail: None,
                    idempotency_key: IdempotencyKey::new(b"credit".to_vec()).unwrap(),
                })
                .await
                .unwrap();
            let head = provider.head(path.clone()).await.unwrap();
            let mut driver = Driver::new(provider);
            let mut reader = Reader::new(ReaderPolicy {
                requests: 2,
                prefetch_requests: 1,
                read_limit: 1,
                ..ReaderPolicy::default()
            })
            .unwrap();
            let ready = Arc::new(Ready(AtomicBool::new(true)));
            let waker = Waker::from(Arc::clone(&ready));
            let mut cx = Context::from_waker(&waker);
            driver.attach(&mut reader, 0);
            reader.cache_mut().observe_origin(&path, head.origin);
            reader.cache_mut().observe_tail(&path, head.tail);
            reader.plan((0..head.tail).map(|from| {
                (
                    Want::Range {
                        stream: path.clone(),
                        from,
                        to: from + 1,
                    },
                    Priority::Visible,
                )
            }));
            // An explicit startup attachment and subsequent steps retain the
            // same plan; attachment must not register/reset the source twice.
            driver.attach(&mut reader, 0);
            // Drive only actual wakes to quiescence, without another plan.
            while ready.0.swap(false, Ordering::SeqCst) {
                driver.step(&mut reader, 0, &mut cx);
                assert!(reader.issued() <= head.tail, "completed read was reissued");
            }
            assert_eq!(reader.issued(), head.tail);
            assert!(reader.cache().has_range(&path, 0, head.tail));
            assert!(reader.poll(0).is_empty());
        });
    }

    /// Uses the actual provider's subtree fork: its root is forked at zero,
    /// but its conversation inherits two records at that same publication.
    #[test]
    fn head_and_child_keep_their_own_primitive_origin_cuts() {
        futures::executor::block_on(async {
            let provider = MemoryStream::default();
            let source = StreamPath::new("source/conversation").unwrap();
            provider
                .append(AppendRequest {
                    path: StreamPath::new("source").unwrap(),
                    records: vec![b"root".to_vec().into()],
                    if_tail: None,
                    idempotency_key: IdempotencyKey::new(b"root".to_vec()).unwrap(),
                })
                .await
                .unwrap();
            provider
                .append(AppendRequest {
                    path: source.clone(),
                    records: vec![b"first".to_vec().into(), b"second".to_vec().into()],
                    if_tail: None,
                    idempotency_key: IdempotencyKey::new(b"append".to_vec()).unwrap(),
                })
                .await
                .unwrap();
            let fork = provider
                .fork(ForkRequest {
                    source: StreamPath::new("source").unwrap(),
                    destination: StreamPath::new("agents/child").unwrap(),
                    at_tail: Some(0),
                    exclude: Vec::new(),
                    idempotency_key: IdempotencyKey::new(b"fork".to_vec()).unwrap(),
                })
                .await
                .unwrap();
            let path = StreamPath::new("agents/child/conversation").unwrap();
            let Outcome::Head(head) = execute(
                &provider,
                Op::Head {
                    id: 1,
                    stream: path.clone(),
                },
            )
            .await
            else {
                panic!("head observation");
            };
            let origin = head.origin.as_ref().unwrap();
            assert_eq!(
                (&origin.source, origin.forked_at, origin.commit_id),
                (&source, 2, fork.commit_id)
            );
            assert_eq!(head.last.as_ref().unwrap().value.as_slice(), b"second");
            let pointer = head.last.as_ref().unwrap().value.as_slice().as_ptr();
            let Outcome::Children { children, .. } = execute(
                &provider,
                Op::Children {
                    id: 2,
                    parent: StreamPath::new("agents").unwrap(),
                    index: 0,
                    after: None,
                    limit: 8,
                    tail: Some(StreamPath::new("conversation").unwrap()),
                    version: None,
                },
            )
            .await
            else {
                panic!("children observation");
            };
            let [child] = children.as_slice() else {
                panic!("one child");
            };
            assert_eq!(child.origin.as_ref().unwrap().forked_at, 0);
            assert_eq!(child.origin.as_ref().unwrap().commit_id, fork.commit_id);
            assert_eq!(child.tail.as_ref().unwrap().value.as_ref(), b"second");

            let mut reader = Reader::new(super::super::ReaderPolicy::default()).unwrap();
            let observed = reader.cache().observation(&path);
            assert_eq!(
                reader.observed_head(&path, head, observed),
                HeadObservation::Applied
            );
            assert_eq!(reader.cache().record(&path, 1).unwrap().as_ptr(), pointer);
            reader.plan([(
                super::super::Want::Range {
                    stream: path.clone(),
                    from: 0,
                    to: 2,
                },
                super::super::Priority::Visible,
            )]);
            let ops = reader.poll(0);
            assert!(
                matches!(ops.as_slice(), [Op::Read { stream, from: 0, .. }] if stream == &path),
                "{ops:?}"
            );
        });
    }
}
