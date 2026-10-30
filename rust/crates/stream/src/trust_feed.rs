//! Follows the signed trust Streams into one [`Follower`]: the environment
//! Stream and one child Stream per followed account, multiplexed in one task
//! with no task or timer per account. Freshness is follow lag: a record is
//! served as soon as its Stream delivers it, never on a refresh interval.
use crate::{FollowFilter, Stream, StreamError, StreamProvider, UnixMillisClock};
use acyclic_auth::trust::{Follower, account_path, environment_path};
use futures::{
    StreamExt as _,
    channel::mpsc,
    future::{self, Either},
    stream::{BoxStream, SelectAll},
};
use std::{collections::HashMap, sync::Arc};

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

type Item = (Option<String>, Result<crate::Record, StreamError>);

/// One follow session over a provider. Offsets persist across [`Self::run`]
/// calls, so a reconnect resumes where the lost follow stopped.
pub struct TrustFeed<P: ?Sized> {
    provider: Arc<P>,
    follower: Arc<Follower>,
    clock: Arc<dyn UnixMillisClock>,
    next: HashMap<Option<String>, u64>,
    /// Absent once every [`Accounts`] handle is dropped.
    added: Option<mpsc::UnboundedReceiver<String>>,
}

impl<P: StreamProvider + ?Sized> TrustFeed<P> {
    /// A feed applying `provider`'s trust Streams to `follower` at `clock`'s
    /// protected time.
    pub fn new(
        provider: Arc<P>,
        follower: Arc<Follower>,
        clock: Arc<dyn UnixMillisClock>,
    ) -> (Self, Accounts) {
        let (sender, added) = mpsc::unbounded();
        let mut next = HashMap::new();
        next.insert(None, 0);
        (
            Self {
                provider,
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
    /// of each call and serves each Stream once it reaches the tail observed
    /// when that Stream's follow opened.
    ///
    /// # Errors
    /// The lost Stream's error, or `AccessDenied` for a refused environment
    /// record, which keeps everything fenced.
    pub async fn run(&mut self) -> Result<(), StreamError> {
        self.follower.fence();
        let result = self.follow().await;
        self.follower.fence();
        result
    }

    async fn follow(&mut self) -> Result<(), StreamError> {
        let mut follows: SelectAll<BoxStream<'static, Item>> = SelectAll::new();
        let mut tails = HashMap::new();
        let keys = self.next.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            self.open(key, &mut follows, &mut tails).await?;
        }
        loop {
            let added = async {
                match &mut self.added {
                    Some(added) => added.next().await,
                    None => future::pending().await,
                }
            };
            futures::pin_mut!(added);
            let event = match future::select(follows.next(), added).await {
                Either::Left((item, _)) => Either::Left(item),
                Either::Right((account, _)) => Either::Right(account),
            };
            match event {
                Either::Left(Some((key, Ok(record)))) => {
                    let now = self
                        .follower
                        .protected_now(self.clock.now_unix_millis() / 1000);
                    let refused = self.follower.apply(&record.value, now).is_err();
                    if refused && key.is_none() {
                        return Err(StreamError::AccessDenied);
                    }
                    let next = record.sequence.saturating_add(1);
                    self.next.insert(key.clone(), next);
                    if tails.get(&key).is_some_and(|tail| next >= *tail) {
                        tails.remove(&key);
                        self.serve(key.as_deref());
                    }
                }
                Either::Left(Some((_, Err(error)))) => return Err(error),
                // Live follows end only when their Stream is lost.
                Either::Left(None) => return Err(StreamError::Unavailable),
                Either::Right(Some(account)) => {
                    let key = Some(account);
                    if !self.next.contains_key(&key) {
                        self.next.insert(key.clone(), 0);
                        self.open(key, &mut follows, &mut tails).await?;
                    }
                }
                // Every `Accounts` handle dropped: the followed set is final.
                Either::Right(None) => self.added = None,
            }
        }
    }

    async fn open(
        &self,
        key: Option<String>,
        follows: &mut SelectAll<BoxStream<'static, Item>>,
        tails: &mut HashMap<Option<String>, u64>,
    ) -> Result<(), StreamError> {
        let environment = self
            .follower
            .environment()
            .map_err(|_| StreamError::Unavailable)?;
        let path = key.as_deref().map_or_else(
            || environment_path(&environment),
            |account| account_path(&environment, account),
        );
        let stream = Stream::new(Arc::clone(&self.provider), path)?;
        let from = self.next.get(&key).copied().unwrap_or(0);
        let tail = stream.tail().await?;
        let records = stream
            .follow(from, FollowFilter::new(Default::default())?)
            .await?;
        let tag = key.clone();
        follows.push(records.map(move |record| (tag.clone(), record)).boxed());
        if from >= tail {
            self.serve(key.as_deref());
        } else {
            tails.insert(key, tail);
        }
        Ok(())
    }

    fn serve(&self, account: Option<&str>) {
        match account {
            None => self.follower.resume(),
            Some(account) => self.follower.serve(account),
        }
    }
}
