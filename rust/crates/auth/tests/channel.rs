//! Credential custody through the maintained tonic client and reply boundary.
#![cfg(all(feature = "grpc", not(target_arch = "wasm32")))]

use acyclic_auth::{
    channel,
    credential::{CredentialCache, CredentialRevision, CredentialSnapshot, CredentialValidity},
};
use futures::{FutureExt, channel::oneshot};
use std::sync::{Arc, Mutex};
use tonic::{Code, Response, transport::Endpoint};

#[derive(Clone, PartialEq, prost::Message)]
struct Empty {}

#[tokio::test]
async fn local_dispatch_refusal_survives_the_tonic_client_without_remote_rejection() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let now = Arc::new(AtomicU64::new(1));
    let observed = now.clone();
    let revision = CredentialRevision([1; 32]);
    let cache = CredentialCache::new(
        move |_| async move { CredentialSnapshot::new("synthetic".into(), revision, 100) },
        move || CredentialValidity {
            now_unix_seconds: observed.load(Ordering::SeqCst),
            revision: Some(revision),
        },
    );
    let lease = cache.resolve().await.unwrap();
    let watch = cache.watch().unwrap();
    let provider = cache.clone().into();
    let dispatch_clock = now.clone();
    let result = channel::call(
        Endpoint::from_static("https://fixture.invalid").connect_lazy(),
        &provider,
        tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        Empty {},
        move |channel, request| async move {
            // Expire after request preparation, before tonic service dispatch.
            dispatch_clock.store(100, Ordering::SeqCst);
            let mut client = tonic::client::Grpc::new(channel);
            client.ready().await?;
            client
                .unary(
                    request,
                    tonic::codegen::http::uri::PathAndQuery::from_static("/synthetic.Service/Call"),
                    tonic_prost::ProstCodec::<Empty, Empty>::default(),
                )
                .await
        },
    )
    .await;
    let error = result.unwrap_err();
    assert_eq!(error.code(), Code::Unauthenticated);
    assert!(std::error::Error::source(&error).is_some());
    assert!(
        watch.now_or_never().is_none(),
        "local refusal cannot reject the source"
    );
    now.store(1, Ordering::SeqCst);
    cache.check(&lease).unwrap();
}

#[tokio::test]
async fn successful_reply_requires_the_original_credential_to_remain_current() {
    for change in ["account", "expiry", "unavailable", "unchanged"] {
        let revision = CredentialRevision([1; 32]);
        let validity = Arc::new(Mutex::new(CredentialValidity {
            now_unix_seconds: 1,
            revision: Some(revision),
        }));
        let observed = validity.clone();
        let cache = CredentialCache::new(
            move |_| async move { CredentialSnapshot::new("synthetic".into(), revision, 100) },
            move || *observed.lock().unwrap(),
        );
        let lease = cache.resolve().await.unwrap();
        let watch = cache.watch().unwrap();
        let provider = cache.clone().into();
        let (entered, started) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let call = channel::call(
            Endpoint::from_static("https://fixture.invalid").connect_lazy(),
            &provider,
            tokio::time::Instant::now() + std::time::Duration::from_secs(30),
            (),
            move |_, _| async move {
                entered.send(()).unwrap();
                released.await.unwrap();
                Ok(Response::new(()))
            },
        );
        futures::pin_mut!(call);
        futures::select! {
            result = call.as_mut().fuse() => panic!("held reply completed: {result:?}"),
            _ = started.fuse() => {},
        }
        {
            let mut validity = validity.lock().unwrap();
            match change {
                "account" => validity.revision = Some(CredentialRevision([2; 32])),
                "expiry" => validity.now_unix_seconds = 100,
                "unavailable" => validity.revision = None,
                _ => {}
            }
        }
        release.send(()).unwrap();
        let result = call.await;
        if change == "unchanged" {
            assert!(result.is_ok());
        } else {
            let error = result.unwrap_err();
            assert_eq!(error.code(), Code::Unauthenticated);
            assert!(std::error::Error::source(&error).is_some());
        }
        assert!(
            watch.now_or_never().is_none(),
            "local refusal is not remote rejection"
        );
        *validity.lock().unwrap() = CredentialValidity {
            now_unix_seconds: 1,
            revision: Some(revision),
        };
        cache.check(&lease).unwrap();
    }
}
