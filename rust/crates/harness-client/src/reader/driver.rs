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
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{
    ChildrenPageRequest, PathHead, ReadRequest, Record, RecordStream, StreamError, StreamPath,
    StreamProvider,
};
use futures::future::{AbortHandle, Abortable, LocalBoxFuture};
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::task::{Context, Poll};

type Opened = (
    StreamPath,
    u64,
    Option<(u64, u64)>,
    Result<(PathHead, RecordStream), StreamError>,
);
type Opening = LocalBoxFuture<'static, Option<Opened>>;

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
    follows: BTreeMap<StreamPath, (u64, u64, RecordStream)>,
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
        self.reconcile(reader, now);
        while let Poll::Ready(Some(done)) = self.opening.poll_next_unpin(cx) {
            // Cancelled opens never complete, so this is the current one.
            let Some((stream, from, observed, opened)) = done else {
                continue;
            };
            self.pending.remove(&stream);
            if !reader
                .follows()
                .any(|(path, wanted)| path == &stream && wanted == from)
            {
                continue;
            }
            match opened {
                Ok((head, records)) => {
                    let accepted = head.tail.max(reader.cache().tail(&stream).unwrap_or(0));
                    match reader.observed_head(&stream, head, observed) {
                        HeadObservation::Invalid => {
                            reader.follow_failed(Failure::Refused, now);
                            continue;
                        }
                        HeadObservation::Superseded => {
                            cx.waker().wake_by_ref();
                            continue;
                        }
                        HeadObservation::Applied => {}
                    }
                    // A changed origin invalidated this opening and its old
                    // cursor. The next frame opens the newly learned history.
                    if !reader.follows().any(|(path, _)| path == &stream) {
                        continue;
                    }
                    reader.follow_opened(&stream, from);
                    self.follows.insert(stream, (from, accepted, records));
                }
                Err(error) => failed(reader, &stream, &error, now),
            }
        }
        self.drain(reader, now, cx);
    }

    fn start(&mut self, op: Op, access: super::Access) {
        let provider = Arc::clone(&self.provider);
        let id = op.id();
        let (abort, registration) = AbortHandle::new_pair();
        self.aborts.insert(id, abort);
        let work = async move {
            if !access.check().await {
                return None;
            }
            let outcome = execute(provider.as_ref(), op).await;
            access.check().await.then_some((id, outcome))
        };
        self.ops.push(Box::pin(async move {
            Abortable::new(work, registration).await.ok().flatten()
        }));
    }

    fn reconcile(&mut self, reader: &Reader, now: u64) {
        let wanted: BTreeMap<StreamPath, u64> = reader
            .follows()
            .map(|(path, from)| (path.clone(), from))
            .collect();
        // Dropping a follow cancels it on the session; so does aborting an
        // open, so opens never outnumber the wanted follows.
        self.follows
            .retain(|path, (position, _, _)| wanted.get(path).is_some_and(|from| from <= position));
        self.pending.retain(|path, (from, abort)| {
            let keep = wanted.get(path) == Some(from);
            if !keep {
                abort.abort();
            }
            keep
        });
        if !reader.ready(now) {
            return;
        }
        for (path, from) in wanted {
            if self.follows.contains_key(&path) || self.pending.contains_key(&path) {
                continue;
            }
            let (abort, registration) = AbortHandle::new_pair();
            self.pending.insert(path.clone(), (from, abort));
            let anchor = reader.follow_anchor(&path, from);
            let observed = reader.cache().observation(&path);
            let provider = Arc::clone(&self.provider);
            let access = reader.cache.access.clone();
            let open = async move {
                let opened = async {
                    if !access.check().await {
                        return Err(StreamError::AccessDenied);
                    }
                    let head = provider.head(path.clone()).await?;
                    if !access.check().await {
                        return Err(StreamError::AccessDenied);
                    }
                    let records = provider
                        .follow(acyclic_stream::FollowRequest {
                            preceding: anchor,
                            ..acyclic_stream::FollowRequest::new(path.clone(), from)
                        })
                        .await
                        .map(acyclic_stream::records_only)?;
                    Ok((head, records))
                }
                .await;
                access
                    .check()
                    .await
                    .then_some((path, from, observed, opened))
            };
            self.opening.push(Box::pin(async move {
                Abortable::new(open, registration).await.ok().flatten()
            }));
        }
    }

    fn drain(&mut self, reader: &mut Reader, now: u64, cx: &mut Context<'_>) {
        let mut ended = Vec::new();
        loop {
            let mut progressed = false;
            for (path, (position, accepted, records)) in &mut self.follows {
                if !reader.accepting() {
                    break;
                }
                match records.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(record))) => {
                        *position = record.sequence + 1;
                        *accepted = (*accepted).max(record.sequence + 1);
                        reader.deliver(path, fetched(&record), *accepted);
                        progressed = true;
                    }
                    Poll::Ready(Some(Err(error))) => {
                        failed(reader, path, &error, now);
                        ended.push(path.clone());
                    }
                    Poll::Ready(None) => ended.push(path.clone()),
                    Poll::Pending => {}
                }
            }
            for path in ended.drain(..) {
                self.follows.remove(&path);
            }
            if !progressed || !reader.accepting() {
                break;
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
pub(super) fn fetched(record: &Record) -> Fetched {
    Fetched {
        sequence: record.sequence,
        value: record.value.clone(),
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
                        tail: child.tail_record.as_ref().map(fetched),
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
        out.push(fetched(&record));
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
