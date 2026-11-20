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
                let record = record
                    .unwrap_or(Err(StreamError::Unavailable))
                    .and_then(|record| {
                        source.position = record
                            .sequence
                            .checked_add(1)
                            .ok_or(StreamError::InvalidResponse)?;
                        source.accepted = source.accepted.max(source.position);
                        Ok(record)
                    });
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
    provider: acyclic_stream::ProviderOwner<P>,
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
    pub fn new(provider: acyclic_stream::ProviderOwner<P>) -> Self {
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
                            reader.refuse_follow(&stream, StreamError::InvalidResponse);
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
                        failed(reader, &stream, &error, now);
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
            acyclic_stream_wire::wire::SessionChannel {
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
        let provider = self.provider.clone();
        let id = op.id();
        let (abort, registration) = AbortHandle::new_pair();
        self.aborts.insert(id, abort);
        let work = async move {
            if !access.check().await {
                return Some((id, Outcome::Failed(StreamError::Unavailable)));
            }
            let outcome = execute(provider.as_ref(), op).await;
            Some((
                id,
                if access.check().await {
                    outcome
                } else {
                    Outcome::Failed(StreamError::Unavailable)
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
            let provider = self.provider.clone();
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
                    let failure = Failure::of(&error);
                    let transient = matches!(failure, Failure::Unavailable | Failure::Overloaded);
                    if transient {
                        reader.follow_failed(failure, now);
                    }
                    for (path, _) in std::mem::take(&mut self.follows) {
                        if !transient {
                            failed(reader, &path, &error, now);
                        }
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

/// Retire definite failures; only transient transport failures back off.
fn failed(reader: &mut Reader, stream: &StreamPath, error: &StreamError, now: u64) {
    match Failure::of(error) {
        Failure::RetentionLost => reader.replaced(stream),
        Failure::Denied => reader.denied(stream),
        Failure::Refused | Failure::Changed => reader.refuse_follow(stream, error.clone()),
        failure @ (Failure::Unavailable | Failure::Overloaded) => {
            reader.follow_failed(failure, now)
        }
    }
}

/// Preserve the primitive allocation owner; the cache decides whether a
/// transport slice needs detaching to establish a bounded retained charge.
pub(super) fn fetched(record: Record) -> Fetched {
    Fetched {
        sequence: record.sequence,
        value: record.value,
        commit: record.commit_id,
        origin: record.origin,
        committed_at_micros: record.committed_at_micros,
    }
}

pub(super) async fn execute<P: StreamProvider + ?Sized>(provider: &P, op: Op) -> Outcome {
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
            index,
            limit,
            tail,
            version,
            ..
        } => children(provider, parent, index, limit, tail, version).await,
    };
    result.unwrap_or_else(Outcome::Failed)
}

async fn children<P: StreamProvider + ?Sized>(
    provider: &P,
    parent: StreamPath,
    index: u32,
    limit: u32,
    tail: Option<StreamPath>,
    mut version: Option<acyclic_stream::CommitId>,
) -> Result<Outcome, StreamError> {
    let mut children = Vec::new();
    let mut cursor = acyclic_stream::demand::ListingCursor::new(index, limit)?;
    while let Some((offset, chunk)) = cursor.request() {
        let page = provider
            .children_page(ChildrenPageRequest {
                offset,
                tail_stream: tail.clone(),
                parent: Some(parent.clone()),
                after: None,
                hierarchy_version: version,
                limit: chunk,
            })
            .await?;
        let count = u32::try_from(page.children.len()).map_err(|_| StreamError::InvalidResponse)?;
        if count > chunk || version.is_some_and(|selected| selected != page.hierarchy_version) {
            return Err(StreamError::InvalidResponse);
        }
        version = Some(page.hierarchy_version);
        cursor.advance(&page)?;
        children.extend(page.children.into_iter().map(|child| ListedChild {
            path: child.path,
            origin: child.origin,
            tail: child.tail_record.map(fetched),
        }));
        if cursor.request().is_none() {
            return Ok(Outcome::Children {
                children,
                next: page.next_after,
                version: page.hierarchy_version,
            });
        }
    }
    Err(StreamError::InvalidArgument)
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
    use acyclic_stream::{AppendRequest, IdempotencyKey, MemoryStream};

    #[test]
    fn indexed_listing_reaches_cold_and_evicted_pages_in_one_request() {
        use super::super::{Priority, ReaderPolicy, Want};

        futures::executor::block_on(async {
            let provider = MemoryStream::default();
            let parent = StreamPath::new("indexed/children").unwrap();
            for child in 0..257u32 {
                provider
                    .append(AppendRequest {
                        path: StreamPath::new(format!("{parent}/{child:03}")).unwrap(),
                        records: vec![b"child".as_slice().into()],
                        if_tail: None,
                        idempotency_key: IdempotencyKey::unique(),
                    })
                    .await
                    .unwrap();
            }
            let mut reader = Reader::new(ReaderPolicy {
                listing_limit: 2,
                ..ReaderPolicy::default()
            })
            .unwrap();
            // No earlier page is fetched or retained, including after eviction.
            // The maximum page also exercises multiplication above u32::MAX.
            for (frame, index) in [100, u32::MAX, 100].into_iter().enumerate() {
                reader.cache.clear_listings();
                reader.begin_frame(frame as u64);
                reader.plan([(
                    Want::Listing {
                        parent: parent.clone(),
                        index,
                        tail: None,
                        witness: None,
                        limit: 2,
                    },
                    Priority::Visible,
                )]);
                let [op]: [Op; 1] = reader.poll(frame as u64).try_into().unwrap();
                assert!(matches!(&op, Op::Children { index: selected, .. } if *selected == index));
                let id = op.id();
                reader.complete(id, execute(&provider, op).await, frame as u64);
                let page = reader.cache.listing(&parent, index, 2).unwrap();
                if index == u32::MAX {
                    assert!(page.children.is_empty());
                    assert!(page.next.is_none());
                } else {
                    assert_eq!(
                        page.children
                            .iter()
                            .map(StreamPath::as_str)
                            .collect::<Vec<_>>(),
                        ["indexed/children/200", "indexed/children/201"]
                    );
                    assert_eq!(page.next.as_ref(), page.children.last());
                }
                assert!(reader.poll(frame as u64).is_empty());
                assert_eq!(reader.cache.listings(&parent).count(), 1);
            }
        });
    }

    #[test]
    fn differently_sized_pages_coexist_and_quiesce() {
        use super::super::{Priority, ReaderPolicy, RecordCache, Want};
        futures::executor::block_on(async {
            let provider = MemoryStream::default();
            let parent = StreamPath::new("sized").unwrap();
            for name in ["a", "b", "c", "d", "e"] {
                provider
                    .append(AppendRequest {
                        path: StreamPath::new(format!("sized/{name}")).unwrap(),
                        records: vec![b"child".as_slice().into()],
                        if_tail: None,
                        idempotency_key: IdempotencyKey::unique(),
                    })
                    .await
                    .unwrap();
            }
            for cache_bytes in [0, 64 * 1024] {
                let mut reader = Reader::new(ReaderPolicy {
                    cache_bytes,
                    ..ReaderPolicy::default()
                })
                .unwrap();
                let wants = [1, 2].map(|limit| {
                    (
                        Want::Listing {
                            parent: parent.clone(),
                            index: 1,
                            limit,
                            tail: None,
                            witness: None,
                        },
                        Priority::Visible,
                    )
                });
                reader.begin_frame(0);
                reader.plan(wants.clone());
                let mut seen = Vec::new();
                for _ in 0..2 {
                    let [op]: [Op; 1] = reader
                        .poll(0)
                        .try_into()
                        .expect("one distinct page operation");
                    let Op::Children { limit, .. } = &op else {
                        panic!("listing operation");
                    };
                    seen.push(*limit);
                    let id = op.id();
                    reader.complete(id, execute(&provider, op).await, 0);
                }
                assert_eq!(seen, [1, 2], "different page sizes coalesced");
                assert!(
                    reader.cache().listing(&parent, 1, 3).is_none(),
                    "an unrequested page size borrowed another page's authority"
                );
                for (limit, expected) in [(1, vec!["sized/b"]), (2, vec!["sized/c", "sized/d"])] {
                    let page = reader
                        .cache()
                        .listing(&parent, 1, limit)
                        .expect("page size lost its cache identity");
                    assert_eq!(
                        page.children
                            .iter()
                            .map(StreamPath::as_str)
                            .collect::<Vec<_>>(),
                        expected
                    );
                    assert_eq!(
                        reader
                            .cache()
                            .listing_containing(&parent, &page.children[0], limit)
                            .unwrap()
                            .0,
                        1
                    );
                }
                reader.begin_frame(1);
                reader.plan(wants);
                assert!(
                    reader.poll(1).is_empty(),
                    "satisfied sized pages were reissued"
                );
                if cache_bytes > 0 {
                    let snapshot = reader.cache().snapshot();
                    let mut restored = RecordCache::new(cache_bytes, 16);
                    restored.restore(&snapshot).unwrap();
                    assert_eq!(
                        restored.listing(&parent, 1, 1).unwrap().children[0].as_str(),
                        "sized/b"
                    );
                    assert_eq!(
                        restored.listing(&parent, 1, 2).unwrap().children[0].as_str(),
                        "sized/c"
                    );
                    let mut obsolete = snapshot;
                    let version = obsolete
                        .windows(b"acyclic.records.v2".len())
                        .position(|x| x == b"acyclic.records.v2")
                        .unwrap();
                    obsolete[version + b"acyclic.records.v".len()] = b'1';
                    assert!(
                        restored.restore(&obsolete).is_err(),
                        "obsolete page identity format accepted"
                    );
                }
            }
        });
    }
}
