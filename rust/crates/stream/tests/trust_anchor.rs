//! Anchor recovery uses signed records through the original memory provider.
use acyclic_auth::{
    trust::{
        EnvironmentFrontier, FenceCause, Follower, FollowerState, Issuer, account_path,
        environment_path,
    },
    verify::{Trust as _, VerifyError},
};
use acyclic_stream::{
    IdempotencyKey, MemoryStream, OwnedBytes, Stream, StreamError, UnixMillisClock,
    trust_feed::TrustFeed,
};
use ed25519_dalek::SigningKey;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

#[derive(Default)]
struct PublicationWake(AtomicBool);
impl Wake for PublicationWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn step<F: Future>(run: Pin<&mut F>, wake: &Arc<PublicationWake>) -> Option<F::Output> {
    let waker = Waker::from(wake.clone());
    match run.poll(&mut Context::from_waker(&waker)) {
        Poll::Ready(value) => Some(value),
        Poll::Pending => None,
    }
}

struct Clock;
impl UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        1_000_000
    }
}

#[tokio::test]
async fn placeholder_anchor_recovers_on_key_publication_without_restart() {
    let issuer = Issuer::new("test", SigningKey::from_bytes(&[1; 32]));
    let placeholder = Issuer::new("test", SigningKey::from_bytes(&[9; 32]));
    let provider = Arc::new(MemoryStream::default());
    let stream = Stream::new(provider.clone(), environment_path("test")).unwrap();
    let mut frontier = EnvironmentFrontier::genesis("test", issuer.anchor());
    let record = issuer
        .environment_keyring(&mut frontier, &issuer.anchor(), 1_000)
        .unwrap();
    stream
        .append(
            OwnedBytes::from(record),
            IdempotencyKey::new(b"anchor".to_vec()).unwrap(),
        )
        .await
        .unwrap();
    let follower = Arc::new(Follower::new("test", placeholder.anchor()));
    let (keys, watched) = tokio::sync::watch::channel(Arc::<[_]>::from(placeholder.anchor()));
    let (feed, accounts) = TrustFeed::new(provider.clone(), follower.clone(), Arc::new(Clock));
    let mut feed = feed.watch_anchor(watched);
    let mut run = Box::pin(feed.run());
    let wake = Arc::new(PublicationWake::default());
    assert!(step(run.as_mut(), &wake).is_none());
    assert_eq!(
        *follower.state().borrow(),
        FollowerState::Fenced(FenceCause::Refused)
    );
    assert_eq!(
        follower.environment_keys("test").err(),
        Some(VerifyError::Unavailable)
    );
    assert_eq!(follower.environment_refusal(), Some(VerifyError::Signature));
    // Neither another wrong key nor withdrawing every key authenticates it.
    for rejected in [placeholder.anchor(), vec![]] {
        wake.0.store(false, Ordering::SeqCst);
        keys.send_replace(rejected.into());
        assert!(
            wake.0.load(Ordering::SeqCst),
            "key publication must wake its suspended driver"
        );
        assert!(step(run.as_mut(), &wake).is_none());
        assert_eq!(
            *follower.state().borrow(),
            FollowerState::Fenced(FenceCause::Refused)
        );
        assert_eq!(
            follower.environment_keys("test").err(),
            Some(VerifyError::Unavailable)
        );
    }
    wake.0.store(false, Ordering::SeqCst);
    keys.send_replace(issuer.anchor().into());
    assert!(wake.0.load(Ordering::SeqCst));
    assert!(step(run.as_mut(), &wake).is_none());
    assert_eq!(*follower.state().borrow(), FollowerState::Serving);
    assert_eq!(
        follower.environment_keys("test").unwrap().as_ref(),
        issuer.anchor()
    );
    assert_eq!(follower.environment_refusal(), None);

    // The same run still serves the original provider and account follows.
    let (_, genesis) = issuer
        .genesis("account", SigningKey::from_bytes(&[2; 32]), 1_000)
        .unwrap();
    let account = Stream::new(provider, account_path("test", "account")).unwrap();
    for (index, value) in genesis.into_iter().enumerate() {
        account
            .append(
                OwnedBytes::from(value),
                IdempotencyKey::new(index.to_le_bytes().to_vec()).unwrap(),
            )
            .await
            .unwrap();
    }
    accounts.follow("account").unwrap();
    assert!(step(run.as_mut(), &wake).is_none());
    assert!(follower.account("account").is_ok());

    // A new bootstrap set cannot overwrite an authenticated signed frontier.
    keys.send_replace(placeholder.anchor().into());
    assert!(step(run.as_mut(), &wake).is_none());
    assert_eq!(
        follower.environment_keys("test").unwrap().as_ref(),
        issuer.anchor()
    );
    let next = issuer
        .environment_keyring(&mut frontier, &issuer.anchor(), 1_000)
        .unwrap();
    stream
        .append(
            OwnedBytes::from(next),
            IdempotencyKey::new(b"next".to_vec()).unwrap(),
        )
        .await
        .unwrap();
    assert!(step(run.as_mut(), &wake).is_none());
    assert!(follower.account("account").is_ok());
    wake.0.store(false, Ordering::SeqCst);
    drop(keys);
    assert!(
        wake.0.load(Ordering::SeqCst),
        "producer loss must wake its driver"
    );
    assert_eq!(
        step(run.as_mut(), &wake),
        Some(Err(StreamError::Unavailable))
    );
    assert_eq!(
        *follower.state().borrow(),
        FollowerState::Fenced(FenceCause::AnchorLost)
    );
}
