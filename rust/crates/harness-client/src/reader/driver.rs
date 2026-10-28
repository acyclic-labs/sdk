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

    /// Advance once: start admitted reads, fold completions, reconcile and
    /// drain follows within the frame budget. Wakes `cx` on progress.
    pub fn step(&mut self, reader: &mut Reader, now: u64, cx: &mut Context<'_>) {
        for id in reader.take_cancelled() {
            if let Some(abort) = self.aborts.remove(&id) {
                abort.abort();
            }
        }
        for op in reader.poll(now) {
            self.start(op);
        }
        while let Poll::Ready(Some(done)) = self.ops.poll_next_unpin(cx) {
            if let Some((id, outcome)) = done {
                self.aborts.remove(&id);
                reader.complete(id, outcome, now);
            }
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

    fn start(&mut self, op: Op) {
        let provider = Arc::clone(&self.provider);
        let id = op.id();
        let (abort, registration) = AbortHandle::new_pair();
        self.aborts.insert(id, abort);
        let work = async move { (id, execute(provider.as_ref(), op).await) };
        self.ops.push(Box::pin(async move {
            Abortable::new(work, registration).await.ok()
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
            let open = async move {
                let opened = async {
                    let head = provider.head(path.clone()).await?;
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
                (path, from, observed, opened)
            };
            self.opening.push(Box::pin(async move {
                Abortable::new(open, registration).await.ok()
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

/// The cache charges exactly what it keeps: copy the visible bytes out of
/// the transport's (possibly larger) backing allocation.
pub(super) fn fetched(record: &Record) -> Fetched {
    Fetched {
        sequence: record.sequence,
        value: bytes::Bytes::copy_from_slice(record.value.as_slice()),
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
