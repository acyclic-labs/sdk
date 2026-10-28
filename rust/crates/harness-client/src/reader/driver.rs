//! The one platform driver: performs the reader's ops against any
//! [`StreamProvider`] and keeps its follows open on that provider. A session
//! provider multiplexes every follow onto its one physical session, whose
//! receive credit is the backpressure: the driver stops polling follows once
//! the frame's delivery budget is spent. Native and WASM share this code.
//!
//! A follow that resumes after cached records is checked (`follow_checked`)
//! against the commit of the last one, so a provisional suffix the server
//! replaced is detected and dropped instead of joined to new history.

use super::{Failure, Fetched, ListedChild, Op, Outcome, Reader};
use acyclic_stream::{
    ChildrenPageRequest, ReadRequest, Record, RecordStream, StreamError, StreamPath, StreamProvider,
};
use futures::future::{AbortHandle, Abortable, LocalBoxFuture};
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;
use std::task::{Context, Poll};

type Opened = (StreamPath, u64, Result<RecordStream, StreamError>);
type Opening = LocalBoxFuture<'static, Option<Opened>>;

/// Executes one reader's ops and follows on one provider.
pub struct Driver<P: ?Sized> {
    provider: Arc<P>,
    ops: FuturesUnordered<LocalBoxFuture<'static, Option<(u64, Outcome)>>>,
    aborts: BTreeMap<u64, AbortHandle>,
    opening: FuturesUnordered<Opening>,
    /// Opens in flight, each cancelled once its stream or start is unwanted.
    pending: BTreeMap<StreamPath, (u64, AbortHandle)>,
    follows: BTreeMap<StreamPath, (u64, RecordStream)>,
    follow_after: Option<StreamPath>,
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
            follow_after: None,
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
            let Some((stream, from, opened)) = done else {
                continue;
            };
            self.pending.remove(&stream);
            match opened {
                Ok(records) => {
                    reader.follow_opened();
                    self.follows.insert(stream, (from, records));
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
            .retain(|path, (position, _)| wanted.get(path).is_some_and(|from| from <= position));
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
            let provider = Arc::clone(&self.provider);
            let open = async move {
                let opened = provider
                    .follow(acyclic_stream::FollowRequest {
                        preceding: anchor,
                        ..acyclic_stream::FollowRequest::new(path.clone(), from)
                    })
                    .await
                    .map(acyclic_stream::records_only);
                (path, from, opened)
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
            let after = self.follow_after.clone();
            let passes = [
                (after.as_ref().map_or(Unbounded, Excluded), Unbounded),
                (Unbounded, after.as_ref().map_or(Unbounded, Included)),
            ];
            for bounds in passes.into_iter().take(1 + usize::from(after.is_some())) {
                for (path, (position, records)) in self.follows.range_mut::<StreamPath, _>(bounds) {
                    if !reader.accepting() {
                        break;
                    }
                    self.follow_after = Some(path.clone());
                    match records.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(record))) => {
                            *position = record.sequence + 1;
                            let accepted = record.sequence + 1;
                            reader.deliver(path, fetched(&record), accepted);
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
            }
            for path in ended.drain(..) {
                self.follows.remove(&path);
            }
            if !progressed || !reader.accepting() {
                if progressed {
                    cx.waker().wake_by_ref();
                }
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
fn fetched(record: &Record) -> Fetched {
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
        Op::Tail { stream, .. } => provider.tail(stream).await.map(Outcome::Tail),
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::reader::ReaderPolicy;
    use acyclic_stream::{AppendRequest, FollowRequest, IdempotencyKey, MemoryStream};

    #[test]
    fn every_busy_follow_progresses_across_delivery_frames() {
        futures::executor::block_on(async {
            let provider = Arc::new(MemoryStream::default());
            let mut driver = Driver::new(Arc::clone(&provider));
            let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
            // More than two frames of streams; every early stream remains
            // ready after its first delivery, reproducing starvation.
            for index in 0..513 {
                let path = StreamPath::new(format!("fair/{index:04}")).unwrap();
                provider
                    .append(AppendRequest {
                        path: path.clone(),
                        records: vec![b"record".as_slice().into(); 4],
                        if_tail: Some(0),
                        idempotency_key: IdempotencyKey::new(format!("fair/{index:04}")).unwrap(),
                    })
                    .await
                    .unwrap();
                let records = provider
                    .follow(FollowRequest::new(path.clone(), 0))
                    .await
                    .unwrap();
                reader.follows.insert(path.clone(), 0);
                driver
                    .follows
                    .insert(path, (0, acyclic_stream::records_only(records)));
            }
            let waker = futures::task::noop_waker();
            let mut context = Context::from_waker(&waker);
            for frame in 0..3 {
                reader.begin_frame(frame);
                driver.drain(&mut reader, frame, &mut context);
                assert!(!reader.accepting());
            }
            assert_eq!(driver.open_follows(), 513);
            assert!(reader.follows().all(|(_, position)| position >= 1));
        });
    }
}
