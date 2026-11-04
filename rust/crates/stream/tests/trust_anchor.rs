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
use futures::FutureExt as _;
use std::sync::Arc;

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
    let (keys, watched) = tokio::sync::watch::channel(placeholder.anchor());
    let (feed, accounts) = TrustFeed::new(provider.clone(), follower.clone(), Arc::new(Clock));
    let mut feed = feed.watch_anchor(watched);
    let mut run = Box::pin(feed.run());
    assert!(run.as_mut().now_or_never().is_none());
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
        keys.send_replace(rejected);
        assert!(run.as_mut().now_or_never().is_none());
        assert_eq!(
            *follower.state().borrow(),
            FollowerState::Fenced(FenceCause::Refused)
        );
        assert_eq!(
            follower.environment_keys("test").err(),
            Some(VerifyError::Unavailable)
        );
    }
    keys.send_replace(issuer.anchor());
    assert!(run.as_mut().now_or_never().is_none());
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
    assert!(run.as_mut().now_or_never().is_none());
    assert!(follower.account("account").is_ok());

    // A new bootstrap set cannot overwrite an authenticated signed frontier.
    keys.send_replace(placeholder.anchor());
    assert!(run.as_mut().now_or_never().is_none());
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
    assert!(run.as_mut().now_or_never().is_none());
    assert!(follower.account("account").is_ok());
    drop(keys);
    assert_eq!(
        run.as_mut().now_or_never(),
        Some(Err(StreamError::Unavailable))
    );
    assert_eq!(
        *follower.state().borrow(),
        FollowerState::Fenced(FenceCause::AnchorLost)
    );
}
