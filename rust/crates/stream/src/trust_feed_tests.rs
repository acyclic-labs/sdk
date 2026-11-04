//! Feed controls over the original memory provider. Fault adapters only
//! interrupt a follow or hold its opening; they never construct records.
use super::*;
use crate::{IdempotencyKey, MemoryStream, OwnedBytes};
use acyclic_auth::trust::{ChangeSource as _, EnvironmentFrontier, Issuer};
use acyclic_auth::verify::Trust as _;
use ed25519_dalek::SigningKey;
use futures::{channel::oneshot, stream};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

const NOW: u64 = 1_000;

fn key(byte: u8) -> SigningKey {
    SigningKey::from_bytes(&[byte; 32])
}

struct Clock;
impl UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        NOW * 1000
    }
}

#[derive(Default)]
struct Path {
    live: Option<oneshot::Sender<()>>,
    read: Arc<AtomicUsize>,
}

/// Observe real follows and hold selected openings behind their own producer.
#[derive(Default)]
struct ControlledSource {
    provider: Arc<MemoryStream>,
    paths: Mutex<HashMap<String, Path>>,
    slow: Mutex<HashMap<String, oneshot::Receiver<()>>>,
    appended: AtomicUsize,
}

impl ControlledSource {
    async fn push(&self, path: &str, value: Vec<u8>) {
        Stream::new(self.provider.clone(), path)
            .unwrap()
            .append(
                OwnedBytes::from(value),
                IdempotencyKey::new(
                    self.appended
                        .fetch_add(1, Ordering::SeqCst)
                        .to_le_bytes()
                        .to_vec(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }

    /// Lose the original observed follow of `path`.
    fn end(&self, path: &str) {
        if let Some(path) = self.paths.lock().unwrap().get_mut(path) {
            path.live.take();
        }
    }
}

struct Src(Arc<ControlledSource>);

impl Source for Src {
    fn tail<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<u64, StreamError>> {
        let held = self.0.slow.lock().unwrap().remove(path);
        async move {
            if let Some(held) = held {
                held.await.map_err(|_| StreamError::Unavailable)?;
            }
            Source::tail(&self.0.provider, path).await
        }
        .boxed()
    }

    fn follow<'a>(
        &'a self,
        path: &'a str,
        from: u64,
    ) -> BoxFuture<'a, Result<Records, StreamError>> {
        async move {
            let records = Source::follow(&self.0.provider, path, from).await?;
            let (sender, ended) = oneshot::channel();
            let read = {
                let mut paths = self.0.paths.lock().unwrap();
                let path = paths.entry(path.to_owned()).or_default();
                path.live = Some(sender);
                Arc::clone(&path.read)
            };
            Ok(records
                .take_until(ended)
                .inspect(move |_| {
                    read.fetch_add(1, Ordering::SeqCst);
                })
                .boxed())
        }
        .boxed()
    }
}

struct World {
    source: Arc<ControlledSource>,
    follower: Arc<Follower>,
    feed: TrustFeed<Src>,
    accounts: Accounts,
}

/// `account`'s Stream holds `genesis`; the environment Stream is empty.
async fn world(anchor: &Issuer, genesis: Vec<Vec<u8>>) -> World {
    let source = Arc::new(ControlledSource::default());
    for value in genesis {
        source.push(&account_path("test", "account"), value).await;
    }
    let follower = Arc::new(Follower::new("test", anchor.anchor()));
    let (feed, accounts) = TrustFeed::new(
        Src(Arc::clone(&source)),
        Arc::clone(&follower),
        Arc::new(Clock),
    );
    accounts.follow("account").unwrap();
    World {
        source,
        follower,
        feed,
        accounts,
    }
}

fn step<F: Future>(run: &mut Pin<Box<F>>) -> Option<F::Output> {
    run.as_mut().now_or_never()
}

fn served(follower: &Follower, account: &str) -> bool {
    follower.account(account).is_ok()
}

/// Fail the original rewind after its revocation checkpoint, before the
/// duplicate current keyring. Later follows retain the same source history.
struct InterruptedReplay {
    source: Arc<MemoryStream>,
    follows: AtomicUsize,
}

impl Source for InterruptedReplay {
    fn tail<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<u64, StreamError>> {
        Source::tail(&self.source, path)
    }

    fn follow<'a>(
        &'a self,
        path: &'a str,
        from: u64,
    ) -> BoxFuture<'a, Result<Records, StreamError>> {
        async move {
            let records = Source::follow(&self.source, path, from).await?;
            if path == account_path("test", "account")
                && self.follows.fetch_add(1, Ordering::SeqCst) == 1
            {
                Ok(records
                    .take(2)
                    .chain(stream::once(future::ready(Err(StreamError::Unavailable))))
                    .boxed())
            } else {
                Ok(records)
            }
        }
        .boxed()
    }
}

#[tokio::test]
async fn reconnect_completes_an_interrupted_authenticated_replay() {
    let anchor = Issuer::new("test", key(1));
    let current = Issuer::new("test", key(9));
    let mut frontier = EnvironmentFrontier::genesis("test", anchor.anchor());
    let environment = anchor
        .environment_keyring(&mut frontier, &current.anchor(), NOW)
        .unwrap();
    let (mut root, mut history) = anchor.genesis("account", key(2), NOW).unwrap();
    history.push(current.keyring(&mut root, NOW).unwrap());
    let source = Arc::new(MemoryStream::default());
    for (path, records) in [
        (account_path("test", "account"), history),
        (environment_path("test"), vec![environment]),
    ] {
        let stream = Stream::new(source.clone(), path).unwrap();
        for (index, value) in records.into_iter().enumerate() {
            stream
                .append(
                    OwnedBytes::from(value),
                    IdempotencyKey::new(format!("{}-{index}", stream.path()).into_bytes()).unwrap(),
                )
                .await
                .unwrap();
        }
    }
    let follower = Arc::new(Follower::new("test", anchor.anchor()));
    let (mut feed, accounts) = TrustFeed::new(
        InterruptedReplay {
            source,
            follows: AtomicUsize::new(0),
        },
        Arc::clone(&follower),
        Arc::new(Clock),
    );
    let mut run = Box::pin(feed.run());
    assert!(step(&mut run).is_none());
    accounts.follow("account").unwrap();
    assert_eq!(step(&mut run), Some(Err(StreamError::Unavailable)));
    drop(run);
    assert!(follower.initialized("account").unwrap());
    assert!(!served(&follower, "account"));
    assert_eq!(feed.next[&Some("account".into())], 0);
    let mut run = Box::pin(feed.run());
    assert!(step(&mut run).is_none());
    assert!(served(&follower, "account"));
}

#[tokio::test]
async fn losing_one_account_follow_fences_everything() {
    let issuer = Issuer::new("test", key(1));
    let (_root, genesis) = issuer.genesis("account", key(2), NOW).unwrap();
    let mut world = world(&issuer, genesis).await;
    let (source, follower) = (Arc::clone(&world.source), Arc::clone(&world.follower));
    let mut run = Box::pin(world.feed.run());
    assert!(step(&mut run).is_none());
    assert!(served(&follower, "account"));
    // The environment follow stays live; only the account's ends.
    source.end(&account_path("test", "account"));
    assert_eq!(step(&mut run), Some(Err(StreamError::Unavailable)));
    assert!(!served(&follower, "account"));
    assert_eq!(
        *follower.state().borrow(),
        FollowerState::Fenced(FenceCause::LostFollow)
    );
}

#[tokio::test]
async fn an_account_record_before_its_environment_signer_is_retained_then_applied() {
    let anchor = Issuer::new("test", key(1));
    let rotated = Issuer::new("test", key(9));
    let mut frontier = EnvironmentFrontier::genesis("test", anchor.anchor());
    let keys = [anchor.anchor()[0], rotated.anchor()[0]];
    let environment = anchor
        .environment_keyring(&mut frontier, &keys, NOW)
        .unwrap();
    // Signed by a key the environment Stream has not delivered yet.
    let (_root, genesis) = rotated.genesis("account", key(2), NOW).unwrap();
    let mut world = world(&anchor, genesis).await;
    let (source, follower) = (Arc::clone(&world.source), Arc::clone(&world.follower));
    let mut run = Box::pin(world.feed.run());
    assert!(step(&mut run).is_none());
    assert_eq!(*follower.state().borrow(), FollowerState::Serving);
    assert!(!served(&follower, "account"));
    source.push(&environment_path("test"), environment).await;
    assert!(step(&mut run).is_none());
    assert!(served(&follower, "account"));
}

#[tokio::test]
async fn an_undecodable_account_record_fences_that_account_only() {
    let issuer = Issuer::new("test", key(1));
    let (mut root, genesis) = issuer.genesis("account", key(2), NOW).unwrap();
    let mut world = world(&issuer, genesis).await;
    let (source, follower) = (Arc::clone(&world.source), Arc::clone(&world.follower));
    let mut run = Box::pin(world.feed.run());
    assert!(step(&mut run).is_none());
    assert!(served(&follower, "account"));
    let path = account_path("test", "account");
    source.push(&path, vec![0xFF, 0xFF]).await;
    assert!(step(&mut run).is_none());
    assert!(!served(&follower, "account"));
    assert_eq!(*follower.state().borrow(), FollowerState::Serving);
    let read = Arc::clone(&source.paths.lock().unwrap()[&path].read);
    let before = read.load(Ordering::SeqCst);
    // The original follow is paused at the refused record. Successors stay
    // behind its provider's credit instead of accumulating in the feed.
    source
        .push(
            &path,
            root.revoke_keys(&[("retired", NOW + 50)], NOW).unwrap(),
        )
        .await;
    assert!(step(&mut run).is_none());
    assert!(!served(&follower, "account"));
    assert_eq!(read.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn a_new_account_replays_from_a_current_checkpoint_after_environment_rotation() {
    let anchor = Issuer::new("test", key(1));
    let current = Issuer::new("test", key(9));
    let mut frontier = EnvironmentFrontier::genesis("test", anchor.anchor());
    let environment = anchor
        .environment_keyring(&mut frontier, &current.anchor(), NOW)
        .unwrap();
    let (mut root, mut history) = anchor.genesis("late", key(2), NOW).unwrap();
    history.push(current.keyring(&mut root, NOW).unwrap());
    let (_, existing) = anchor.genesis("account", key(2), NOW).unwrap();
    let mut world = world(&anchor, existing).await;
    let (source, follower) = (Arc::clone(&world.source), Arc::clone(&world.follower));
    let mut run = Box::pin(world.feed.run());
    assert!(step(&mut run).is_none());
    source.push(&environment_path("test"), environment).await;
    assert!(step(&mut run).is_none());
    for record in history {
        source.push(&account_path("test", "late"), record).await;
    }
    world.accounts.follow("late").unwrap();
    assert!(step(&mut run).is_none());
    assert!(served(&follower, "late"));
    // The current key authenticates the checkpoint; a historical signature
    // alone cannot bootstrap another account after that key was retired.
    let (_, historical) = anchor.genesis("foreign", key(3), NOW).unwrap();
    for record in historical {
        source.push(&account_path("test", "foreign"), record).await;
    }
    // Even a valid environment publication has no authority on an account
    // follow. Without the path binding it could revive the historical signer.
    source
        .push(
            &account_path("test", "foreign"),
            current
                .environment_keyring(&mut frontier, &anchor.anchor(), NOW)
                .unwrap(),
        )
        .await;
    world.accounts.follow("foreign").unwrap();
    assert!(step(&mut run).is_none());
    assert!(!served(&follower, "foreign"));
    assert!(served(&follower, "late"));
    assert_eq!(
        &*follower.environment_keys("test").unwrap(),
        current.anchor().as_slice()
    );
}

#[tokio::test]
async fn a_slow_account_open_never_delays_another_accounts_revocations() {
    let issuer = Issuer::new("test", key(1));
    let (mut root, genesis) = issuer.genesis("account", key(2), NOW).unwrap();
    let mut world = world(&issuer, genesis).await;
    let (source, follower) = (Arc::clone(&world.source), Arc::clone(&world.follower));
    let (producer, held) = oneshot::channel();
    source
        .slow
        .lock()
        .unwrap()
        .insert(account_path("test", "slow"), held);
    world.accounts.follow("slow").unwrap();
    let mut run = Box::pin(world.feed.run());
    assert!(step(&mut run).is_none());
    assert!(served(&follower, "account"));
    let changed = follower.changed();
    source
        .push(
            &account_path("test", "account"),
            root.revoke_keys(&[("retired", NOW + 50)], NOW).unwrap(),
        )
        .await;
    assert!(step(&mut run).is_none());
    assert_eq!(changed.now_or_never(), Some(Ok(())));
    drop(producer);
    assert_eq!(step(&mut run), Some(Err(StreamError::Unavailable)));
    assert!(!served(&follower, "account"));
}
