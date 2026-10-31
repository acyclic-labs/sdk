//! Follows the signed trust Streams into one [`Follower`]: the environment
//! Stream and one child Stream per followed account, multiplexed in one task
//! with no task or timer per account. Freshness is follow lag: a record is
//! served as soon as its Stream delivers it, never on a refresh interval.
//! Opening a Stream runs alongside the live follows, so one slow account
//! never delays another's revocations. Any follow ending fences everything;
//! a refused account record fences that account and is retained, in order
//! with its successors, until an environment record lets it apply.
use crate::{FollowFilter, Record, Stream, StreamError, StreamProvider, UnixMillisClock};
use acyclic_auth::trust::{
    Applied, FenceCause, Follower, FollowerState, account_path, environment_path,
};
use futures::{
    StreamExt as _,
    channel::mpsc,
    future::{self, BoxFuture, Either},
    stream::{self, BoxStream, SelectAll},
};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

/// Live records of one trust Stream.
pub type Records = BoxStream<'static, Result<Record, StreamError>>;

/// What a [`TrustFeed`] reads: a Stream's tail and a live follow from an
/// offset. Every [`StreamProvider`] is a source; a host may supply its own.
pub trait Source: Send + Sync + 'static {
    /// The Stream's current tail (next sequence).
    fn tail<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<u64, StreamError>>;
    /// Records from `from`, then live, until the Stream is lost.
    fn follow<'a>(
        &'a self,
        path: &'a str,
        from: u64,
    ) -> BoxFuture<'a, Result<Records, StreamError>>;
}

impl<P: StreamProvider + ?Sized> Source for Arc<P> {
    fn tail<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<u64, StreamError>> {
        Box::pin(async move { Stream::new(Arc::clone(self), path)?.tail().await })
    }

    fn follow<'a>(
        &'a self,
        path: &'a str,
        from: u64,
    ) -> BoxFuture<'a, Result<Records, StreamError>> {
        Box::pin(async move {
            let records = Stream::new(Arc::clone(self), path)?
                .follow(from, FollowFilter::new(Default::default())?)
                .await?;
            Ok(records.boxed())
        })
    }
}

/// Adds accounts to a running [`TrustFeed`]; following is lazy, by account.
#[derive(Clone)]
pub struct Accounts(mpsc::UnboundedSender<String>);

impl Accounts {
    /// Follow `account` from now on. Its trust is served once its Stream is
    /// caught up to the tail observed when the follow opened.
    pub fn follow(&self, account: impl Into<String>) {
        let _ = self.0.unbounded_send(account.into());
    }
}

/// One event of one followed Stream, in Stream order.
enum Event {
    /// The follow opened; trust is served once the Stream reaches `tail`.
    Opened(u64),
    Record(Record),
    /// The follow ended: the Stream was lost.
    Ended(StreamError),
}

type Item = (Option<String>, Event);

/// Fences the follower however a run ends, a dropped (cancelled) run included.
struct Fence {
    follower: Arc<Follower>,
    cause: FenceCause,
}

impl Drop for Fence {
    fn drop(&mut self) {
        self.follower.fence(self.cause);
    }
}

/// One follow session over a [`Source`]. Offsets persist across
/// [`Self::run`] calls, so a reconnect resumes where the lost follow stopped.
pub struct TrustFeed<S> {
    source: S,
    follower: Arc<Follower>,
    clock: Arc<dyn UnixMillisClock>,
    next: HashMap<Option<String>, u64>,
    /// Absent once every [`Accounts`] handle is dropped.
    added: Option<mpsc::UnboundedReceiver<String>>,
}

impl<S: Source> TrustFeed<S> {
    /// A feed applying `source`'s trust Streams to `follower` at `clock`'s
    /// protected time.
    pub fn new(
        source: S,
        follower: Arc<Follower>,
        clock: Arc<dyn UnixMillisClock>,
    ) -> (Self, Accounts) {
        let (sender, added) = mpsc::unbounded();
        let mut next = HashMap::new();
        next.insert(None, 0);
        (
            Self {
                source,
                follower,
                clock,
                next,
                added: Some(added),
            },
            Accounts(sender),
        )
    }

    /// Follows until a Stream is lost, then fences the follower and returns
    /// the transport error; call again to reconnect (backoff only for a
    /// transient transport failure). The follower is fenced from the start
    /// of each call, and again however the call ends, cancellation included;
    /// it serves each Stream once it reaches the tail observed when that
    /// Stream's follow opened.
    ///
    /// # Errors
    /// The lost Stream's error, or `AccessDenied` for a refused environment
    /// record, which keeps everything fenced.
    pub async fn run(&mut self) -> Result<(), StreamError> {
        let mut fence = Fence {
            follower: Arc::clone(&self.follower),
            cause: FenceCause::LostFollow,
        };
        self.follower.connect();
        let result = self.follow().await;
        if matches!(result, Err(StreamError::AccessDenied)) {
            fence.cause = FenceCause::Refused;
        }
        result
    }

    /// The follower's serving state: await it to learn when trust is served.
    #[must_use]
    pub fn state(&self) -> tokio::sync::watch::Receiver<FollowerState> {
        self.follower.state()
    }

    /// Stops the feed for good: the follower refuses trust and every change
    /// wait resolves closed.
    pub fn close(self) {
        self.follower.close(FenceCause::Shutdown);
    }

    async fn follow(&mut self) -> Result<(), StreamError> {
        let Self {
            source,
            follower,
            clock,
            next,
            added,
        } = self;
        let environment = follower
            .environment()
            .map_err(|_| StreamError::Unavailable)?;
        let mut follows: SelectAll<BoxStream<'_, Item>> = SelectAll::new();
        for (key, from) in next.iter() {
            follows.push(open(&*source, &environment, key.clone(), *from));
        }
        let mut tails = HashMap::new();
        let mut blocked: HashMap<String, VecDeque<Record>> = HashMap::new();
        loop {
            let account = async {
                match added {
                    Some(added) => added.next().await,
                    None => future::pending().await,
                }
            };
            futures::pin_mut!(account);
            let event = match future::select(follows.next(), account).await {
                Either::Left((item, _)) => Either::Left(item),
                Either::Right((account, _)) => Either::Right(account),
            };
            match event {
                Either::Left(Some((key, Event::Opened(tail)))) => {
                    if next.get(&key).is_some_and(|from| *from >= tail) {
                        serve(follower, key.as_deref());
                    } else {
                        tails.insert(key, tail);
                    }
                }
                Either::Left(Some((None, Event::Record(record)))) => {
                    let now = follower.protected_now(clock.now_unix_millis() / 1000);
                    let applied = follower
                        .apply(&record.value, now)
                        .map_err(|_| StreamError::AccessDenied)?;
                    advance(follower, next, &mut tails, None, record.sequence);
                    if applied == Applied::Changed {
                        // New environment keys may admit retained records.
                        blocked.retain(|account, records| {
                            while let Some(record) = records.front() {
                                if follower.apply(&record.value, now).is_err() {
                                    follower.refuse(account);
                                    return true;
                                }
                                let key = Some(account.clone());
                                advance(follower, next, &mut tails, key, record.sequence);
                                records.pop_front();
                            }
                            false
                        });
                    }
                }
                Either::Left(Some((Some(account), Event::Record(record)))) => {
                    if let Some(records) = blocked.get_mut(&account) {
                        records.push_back(record);
                        continue;
                    }
                    let now = follower.protected_now(clock.now_unix_millis() / 1000);
                    if follower.apply(&record.value, now).is_ok() {
                        advance(follower, next, &mut tails, Some(account), record.sequence);
                    } else {
                        // Fenced, and retained without advancing the offset:
                        // its environment signer may not have arrived yet.
                        follower.refuse(&account);
                        blocked.entry(account).or_default().push_back(record);
                    }
                }
                Either::Left(Some((_, Event::Ended(error)))) => return Err(error),
                // Every follow ends with an `Ended` event first.
                Either::Left(None) => return Err(StreamError::Unavailable),
                Either::Right(Some(account)) => {
                    let key = Some(account);
                    if !next.contains_key(&key) {
                        next.insert(key.clone(), 0);
                        follows.push(open(&*source, &environment, key, 0));
                    }
                }
                // Every `Accounts` handle dropped: the followed set is final.
                Either::Right(None) => *added = None,
            }
        }
    }
}

/// One Stream's follow as events: `Opened` once its tail and follow are in
/// hand, its records, then `Ended` however the follow stops. Opening is part
/// of the stream, so it never blocks the other follows.
fn open<'a, S: Source>(
    source: &'a S,
    environment: &str,
    key: Option<String>,
    from: u64,
) -> BoxStream<'a, Item> {
    let path = key.as_deref().map_or_else(
        || environment_path(environment),
        |account| account_path(environment, account),
    );
    let opened = async move {
        let tail = source.tail(&path).await?;
        let records = source.follow(&path, from).await?;
        Ok::<_, StreamError>((tail, records))
    };
    stream::once(opened)
        .flat_map(|opened| match opened {
            Ok((tail, records)) => stream::once(future::ready(Event::Opened(tail)))
                .chain(records.map(|record| match record {
                    Ok(record) => Event::Record(record),
                    Err(error) => Event::Ended(error),
                }))
                .chain(stream::once(future::ready(Event::Ended(
                    StreamError::Unavailable,
                ))))
                .boxed(),
            Err(error) => stream::once(future::ready(Event::Ended(error))).boxed(),
        })
        .map(move |event| (key.clone(), event))
        .boxed()
}

/// `key`'s record at `sequence` applied: resume after it, and serve the
/// Stream once it reaches the tail observed when its follow opened.
fn advance(
    follower: &Follower,
    next: &mut HashMap<Option<String>, u64>,
    tails: &mut HashMap<Option<String>, u64>,
    key: Option<String>,
    sequence: u64,
) {
    let after = sequence.saturating_add(1);
    if tails.get(&key).is_some_and(|tail| after >= *tail) {
        tails.remove(&key);
        serve(follower, key.as_deref());
    }
    next.insert(key, after);
}

fn serve(follower: &Follower, account: Option<&str>) {
    match account {
        None => follower.resume(),
        Some(account) => follower.serve(account),
    }
}

#[cfg(test)]
#[path = "trust_feed_tests.rs"]
mod tests;
