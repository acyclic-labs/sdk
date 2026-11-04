//! Anchor recovery uses signed records through the original memory provider.
use acyclic_auth::{
    trust::{EnvironmentFrontier, FenceCause, Follower, FollowerState, Issuer, environment_path},
    verify::{Trust as _, VerifyError},
};
use acyclic_stream::{
    IdempotencyKey, MemoryStream, OwnedBytes, Stream, UnixMillisClock, trust_feed::TrustFeed,
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
    let (mut feed, _accounts) = TrustFeed::new(provider, follower.clone(), Arc::new(Clock));
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
}
