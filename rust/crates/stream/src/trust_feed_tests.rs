//! Feed proofs over an in-memory source, stepped by hand: every assertion
//! follows one poll of the run with all input ready, so nothing waits.
use super::*;
use crate::{CommitId, RecordHeaders, StreamPath};
use acyclic_auth::trust::{ChangeSource as _, EnvironmentFrontier, Issuer};
use acyclic_auth::verify::Trust as _;
use bytes::Bytes;
use ed25519_dalek::SigningKey;
use futures::FutureExt as _;
use std::{collections::HashSet, future::Future, pin::Pin, sync::Mutex};

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
    records: Vec<Bytes>,
    live: Vec<mpsc::UnboundedSender<Result<Record, StreamError>>>,
}

/// Streams in memory; a `slow` path never answers its tail.
#[derive(Default)]
struct Fake {
    paths: Mutex<HashMap<String, Path>>,
    slow: Mutex<HashSet<String>>,
}

fn record(sequence: usize, value: Bytes) -> Record {
    Record {
        sequence: sequence as u64,
        value: value.into(),
        commit_id: CommitId::from_bytes([0; 32]),
        committed_at_micros: 0,
        headers: RecordHeaders::default(),
        origin: StreamPath::new("trust").unwrap(),
    }
}

impl Fake {
    fn push(&self, path: &str, value: Vec<u8>) {
        let mut paths = self.paths.lock().unwrap();
        let path = paths.entry(path.to_owned()).or_default();
        let value = Bytes::from(value);
        let sequence = path.records.len();
        path.records.push(value.clone());
        path.live.retain(|live| {
            live.unbounded_send(Ok(record(sequence, value.clone())))
                .is_ok()
        });
    }

    /// Lose every live follow of `path`.
    fn end(&self, path: &str) {
        if let Some(path) = self.paths.lock().unwrap().get_mut(path) {
            path.live.clear();
        }
    }
}

struct Src(Arc<Fake>);

impl Source for Src {
    fn tail<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<u64, StreamError>> {
        if self.0.slow.lock().unwrap().contains(path) {
            return future::pending().boxed();
        }
        let tail = self
            .0
            .paths
            .lock()
            .unwrap()
            .get(path)
            .map_or(0, |path| path.records.len() as u64);
        future::ready(Ok(tail)).boxed()
    }

    fn follow<'a>(
        &'a self,
        path: &'a str,
        from: u64,
    ) -> BoxFuture<'a, Result<Records, StreamError>> {
        let mut paths = self.0.paths.lock().unwrap();
        let path = paths.entry(path.to_owned()).or_default();
        let (sender, live) = mpsc::unbounded();
        path.live.push(sender);
        let backlog = path
            .records
            .iter()
            .enumerate()
            .skip(from as usize)
            .map(|(sequence, value)| Ok(record(sequence, value.clone())))
            .collect::<Vec<_>>();
        future::ready(Ok(stream::iter(backlog).chain(live).boxed())).boxed()
    }
}

struct World {
    fake: Arc<Fake>,
    follower: Arc<Follower>,
    feed: TrustFeed<Src>,
    accounts: Accounts,
}

/// `account`'s Stream holds `genesis`; the environment Stream is empty.
fn world(anchor: &Issuer, genesis: Vec<Vec<u8>>) -> World {
    let fake = Arc::new(Fake::default());
    for value in genesis {
        fake.push(&account_path("test", "account"), value);
    }
    let follower = Arc::new(Follower::new("test", anchor.anchor()));
    let (feed, accounts) = TrustFeed::new(
        Src(Arc::clone(&fake)),
        Arc::clone(&follower),
        Arc::new(Clock),
    );
    accounts.follow("account");
    World {
        fake,
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

#[test]
fn a_cancelled_run_fences_the_trust_it_served() {
    let issuer = Issuer::new("test", key(1));
    let (_root, genesis) = issuer.genesis("account", key(2), NOW).unwrap();
    let mut world = world(&issuer, genesis);
    let follower = Arc::clone(&world.follower);
    let mut run = Box::pin(world.feed.run());
    assert!(step(&mut run).is_none());
    assert_eq!(*follower.state().borrow(), FollowerState::Serving);
    assert!(served(&follower, "account"));
    drop(run);
    assert_eq!(
        *follower.state().borrow(),
        FollowerState::Fenced(FenceCause::LostFollow)
    );
    assert!(!served(&follower, "account"));
}

#[test]
fn losing_one_account_follow_fences_everything() {
    let issuer = Issuer::new("test", key(1));
    let (_root, genesis) = issuer.genesis("account", key(2), NOW).unwrap();
    let mut world = world(&issuer, genesis);
    let (fake, follower) = (Arc::clone(&world.fake), Arc::clone(&world.follower));
    let mut run = Box::pin(world.feed.run());
    assert!(step(&mut run).is_none());
    assert!(served(&follower, "account"));
    // The environment follow stays live; only the account's ends.
    fake.end(&account_path("test", "account"));
    assert_eq!(step(&mut run), Some(Err(StreamError::Unavailable)));
    assert!(!served(&follower, "account"));
    assert_eq!(
        *follower.state().borrow(),
        FollowerState::Fenced(FenceCause::LostFollow)
    );
}

#[test]
fn an_account_record_before_its_environment_signer_is_retained_then_applied() {
    let anchor = Issuer::new("test", key(1));
    let rotated = Issuer::new("test", key(9));
    let mut frontier = EnvironmentFrontier::genesis("test", anchor.anchor());
    let keys = [anchor.anchor()[0], rotated.anchor()[0]];
    let environment = anchor
        .environment_keyring(&mut frontier, &keys, NOW)
        .unwrap();
    // Signed by a key the environment Stream has not delivered yet.
    let (_root, genesis) = rotated.genesis("account", key(2), NOW).unwrap();
    let mut world = world(&anchor, genesis);
    let (fake, follower) = (Arc::clone(&world.fake), Arc::clone(&world.follower));
    let mut run = Box::pin(world.feed.run());
    assert!(step(&mut run).is_none());
    assert_eq!(*follower.state().borrow(), FollowerState::Serving);
    assert!(!served(&follower, "account"));
    fake.push(&environment_path("test"), environment);
    assert!(step(&mut run).is_none());
    assert!(served(&follower, "account"));
}

#[test]
fn an_undecodable_account_record_fences_that_account_only() {
    let issuer = Issuer::new("test", key(1));
    let (mut root, genesis) = issuer.genesis("account", key(2), NOW).unwrap();
    let mut world = world(&issuer, genesis);
    let (fake, follower) = (Arc::clone(&world.fake), Arc::clone(&world.follower));
    let mut run = Box::pin(world.feed.run());
    assert!(step(&mut run).is_none());
    assert!(served(&follower, "account"));
    let path = account_path("test", "account");
    fake.push(&path, vec![0xFF, 0xFF]);
    assert!(step(&mut run).is_none());
    assert!(!served(&follower, "account"));
    assert_eq!(*follower.state().borrow(), FollowerState::Serving);
    // Later records queue behind the refused one: the account stays fenced.
    fake.push(
        &path,
        root.revoke_keys(&[("retired", NOW + 50)], NOW).unwrap(),
    );
    assert!(step(&mut run).is_none());
    assert!(!served(&follower, "account"));
}

#[test]
fn a_slow_account_open_never_delays_another_accounts_revocations() {
    let issuer = Issuer::new("test", key(1));
    let (mut root, genesis) = issuer.genesis("account", key(2), NOW).unwrap();
    let mut world = world(&issuer, genesis);
    let (fake, follower) = (Arc::clone(&world.fake), Arc::clone(&world.follower));
    fake.slow
        .lock()
        .unwrap()
        .insert(account_path("test", "slow"));
    world.accounts.follow("slow");
    let mut run = Box::pin(world.feed.run());
    assert!(step(&mut run).is_none());
    assert!(served(&follower, "account"));
    let changed = follower.changed();
    fake.push(
        &account_path("test", "account"),
        root.revoke_keys(&[("retired", NOW + 50)], NOW).unwrap(),
    );
    assert!(step(&mut run).is_none());
    assert_eq!(changed.now_or_never(), Some(Ok(())));
}
