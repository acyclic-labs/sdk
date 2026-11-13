//! Credential custody through the maintained tonic client and reply boundary.
#![cfg(all(feature = "grpc", not(target_arch = "wasm32")))]

use acyclic_auth::{
    channel,
    credential::{
        CredentialAction, CredentialBinding, CredentialCache, CredentialCacheError,
        CredentialError, CredentialProvider, CredentialRequest, CredentialResolution,
        CredentialRevision, CredentialSnapshot, CredentialSource, CredentialValidity,
        CredentialWatch, ResolvedCredential,
    },
};
use futures::{FutureExt, channel::oneshot};
use std::sync::{Arc, Mutex};
use tonic::{Code, Response, transport::Endpoint};

struct DeferredSource {
    cache: CredentialCache,
    admission: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    entered: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl CredentialSource for DeferredSource {
    fn resolve(&self) -> CredentialResolution<'_> {
        Box::pin(async {
            ResolvedCredential::from_custodian(DeferredRequest {
                lease: CredentialProvider::from(self.cache.clone())
                    .resolve()
                    .await?,
                first: std::sync::atomic::AtomicBool::new(true),
                admission: self.admission.clone(),
                entered: self.entered.clone(),
            })
            .await
        })
    }
    fn watch(&self) -> CredentialAction<Option<CredentialWatch>> {
        self.cache
            .watch()
            .map(Some)
            .map_err(CredentialError::from)
            .into()
    }
}

struct DeferredRequest {
    lease: ResolvedCredential,
    first: std::sync::atomic::AtomicBool,
    admission: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    entered: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl CredentialRequest for DeferredRequest {
    fn token(&self) -> &str {
        self.lease.as_str()
    }
    fn binding(&self) -> Option<CredentialBinding> {
        self.lease.binding()
    }
    fn check(&self) -> CredentialAction<()> {
        if self.first.swap(false, std::sync::atomic::Ordering::SeqCst) {
            return self.lease.check();
        }
        let Some(admission) = self.admission.lock().unwrap().take() else {
            return self.lease.check();
        };
        let entered = self.entered.lock().unwrap().take().unwrap();
        let lease = self.lease.clone();
        CredentialAction::Pending(Box::pin(async move {
            entered.send(()).unwrap();
            admission
                .await
                .map_err(|_| CredentialError::from(CredentialCacheError::Unavailable))?;
            lease.check().await
        }))
    }
    fn reject(&self) -> CredentialAction<bool> {
        self.lease.reject()
    }
}

#[tokio::test]
async fn tonic_dispatch_waits_for_its_owner_and_releases_cancelled_admission() {
    use std::sync::atomic::{AtomicU64, Ordering};
    for completion in ["expiry", "producer-loss", "cancel"] {
        let now = Arc::new(AtomicU64::new(1));
        let observed = now.clone();
        let cache = CredentialCache::new(
            |_| async { CredentialSnapshot::new("synthetic".into(), [1; 32].into(), 100) },
            move || CredentialValidity {
                now_unix_seconds: observed.load(Ordering::SeqCst),
                revision: Some([1; 32].into()),
            },
        );
        let original = cache.resolve().await.unwrap();
        let changed = cache.watch().unwrap();
        let (complete, completed) = oneshot::channel();
        let (entered, started) = oneshot::channel();
        let provider = CredentialProvider::from_custodian(DeferredSource {
            cache: cache.clone(),
            admission: Arc::new(Mutex::new(Some(completed))),
            entered: Arc::new(Mutex::new(Some(entered))),
        });
        let mut operation = Box::pin(channel::call(
            Endpoint::from_static("https://fixture.invalid").connect_lazy(),
            &provider,
            channel::Budget::Deadline(Some(
                tokio::time::Instant::now() + std::time::Duration::from_secs(30),
            )),
            Empty {},
            |channel, request| async move {
                let mut client = tonic::client::Grpc::new(channel);
                client.ready().await?;
                client
                    .unary(
                        request,
                        tonic::codegen::http::uri::PathAndQuery::from_static(
                            "/synthetic.Service/Call",
                        ),
                        tonic_prost::ProstCodec::<Empty, Empty>::default(),
                    )
                    .await
            },
        ));
        futures::select! {
            result = operation.as_mut().fuse() => panic!("owner has not admitted dispatch: {result:?}"),
            _ = started.fuse() => {},
        }
        // Keeping the producer alive cannot admit a dispatch on its own.
        assert!(operation.as_mut().now_or_never().is_none());
        match completion {
            "cancel" => {
                drop(operation);
                assert!(complete.is_canceled());
            }
            other => {
                if other == "expiry" {
                    now.store(100, Ordering::SeqCst);
                    complete.send(()).unwrap();
                } else {
                    drop(complete);
                }
                let error = operation
                    .now_or_never()
                    .expect("owner event must finish admission")
                    .unwrap_err();
                assert_eq!(
                    error.code(),
                    if other == "expiry" {
                        Code::Unauthenticated
                    } else {
                        Code::Unavailable
                    },
                    "losing the credential producer is availability loss, not withdrawal"
                );
                assert!(std::error::Error::source(&error).is_some());
            }
        }
        assert!(
            changed.now_or_never().is_none(),
            "local failure must not reject the snapshot"
        );
        now.store(1, Ordering::SeqCst);
        cache.check(&original).unwrap();
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct Empty {}

struct AdmissionClock(std::sync::atomic::AtomicU64);
impl acyclic_auth::operation::Clock for AdmissionClock {
    fn now_unix_millis(&self) -> Result<u64, acyclic_auth::operation::Stopped> {
        Ok(self.0.load(std::sync::atomic::Ordering::SeqCst))
    }
    fn expires_at(
        &self,
        _: u64,
    ) -> Result<acyclic_auth::operation::StopFuture, acyclic_auth::operation::Stopped> {
        panic!("transport must retain the original deadline registration")
    }
}

struct ChangeDuringAdmission {
    lease: ResolvedCredential,
    first: std::sync::atomic::AtomicBool,
    change: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl CredentialRequest for ChangeDuringAdmission {
    fn token(&self) -> &str {
        self.lease.as_str()
    }
    fn binding(&self) -> Option<CredentialBinding> {
        self.lease.binding()
    }
    fn check(&self) -> CredentialAction<()> {
        if self.first.swap(false, std::sync::atomic::Ordering::SeqCst) {
            return self.lease.check();
        }
        let change = self.change.lock().unwrap().take().unwrap();
        let release = self.release.lock().unwrap().take();
        let lease = self.lease.clone();
        match release {
            Some(release) => CredentialAction::Pending(Box::pin(async move {
                release.await.map_err(|_| CredentialError::Invalid)?;
                change();
                lease.check().await
            })),
            None => {
                change();
                lease.check()
            }
        }
    }
    fn reject(&self) -> CredentialAction<bool> {
        self.lease.reject()
    }
}

#[tokio::test]
async fn a_credential_owner_cannot_stop_then_admit_native_dispatch() {
    use acyclic_auth::operation::{Context, Stopped};
    use tonic::codegen::Service;
    for mode in ["cancel", "clock-expiry"] {
        for deferred in [false, true] {
            let cache = CredentialCache::new(
                |_| async { CredentialSnapshot::new("synthetic".into(), [1; 32].into(), 100) },
                || CredentialValidity {
                    now_unix_seconds: 1,
                    revision: Some([1; 32].into()),
                },
            );
            let original = cache.resolve().await.unwrap();
            let watch = cache.watch().unwrap();
            let (context, change): (Context, Box<dyn FnOnce() + Send>) = if mode == "cancel" {
                let (cancel, cancelled) = oneshot::channel();
                (
                    Context::new(u64::MAX, std::future::pending(), async {
                        let _ = cancelled.await;
                    }),
                    Box::new(move || {
                        cancel.send(()).unwrap();
                    }),
                )
            } else {
                let clock = Arc::new(AdmissionClock(std::sync::atomic::AtomicU64::new(1)));
                (
                    Context::from_clock(2, std::future::pending(), clock.clone()),
                    Box::new(move || clock.0.store(2, std::sync::atomic::Ordering::SeqCst)),
                )
            };
            let (release, released) = oneshot::channel();
            let lease = ResolvedCredential::from_custodian(ChangeDuringAdmission {
                lease: CredentialProvider::from(cache.clone())
                    .resolve()
                    .await
                    .unwrap(),
                first: std::sync::atomic::AtomicBool::new(true),
                change: Mutex::new(Some(change)),
                release: Mutex::new(deferred.then_some(released)),
            })
            .await
            .unwrap();
            let mut channel =
                channel::guard(Endpoint::from_static("https://fixture.invalid").connect_lazy())
                    .with_context(Some(context));
            futures::future::poll_fn(|cx| channel.poll_ready(cx))
                .await
                .unwrap();
            let mut request = tonic::codegen::http::Request::new(tonic::body::Body::empty());
            request.extensions_mut().insert(lease);
            let mut dispatch = Box::pin(channel.call(request));
            if deferred {
                assert!(dispatch.as_mut().now_or_never().is_none());
                release.send(()).unwrap();
            }
            let error = dispatch
                .now_or_never()
                .expect("owner stop must refuse before transport dispatch")
                .unwrap_err();
            assert_eq!(
                error.code(),
                if mode == "cancel" {
                    Code::Cancelled
                } else {
                    Code::DeadlineExceeded
                }
            );
            assert_eq!(
                std::error::Error::source(&error)
                    .unwrap()
                    .downcast_ref::<Stopped>(),
                Some(&if mode == "cancel" {
                    Stopped::Cancelled
                } else {
                    Stopped::Deadline
                })
            );
            assert!(
                watch.now_or_never().is_none(),
                "local cancellation cannot reject a valid lease"
            );
            cache.check(&original).unwrap();
        }
    }
}

#[tokio::test]
async fn local_dispatch_refusal_survives_the_tonic_client_without_remote_rejection() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let now = Arc::new(AtomicU64::new(1));
    let observed = now.clone();
    let cache = CredentialCache::new(
        move |_| async move {
            CredentialSnapshot::new("synthetic".into(), CredentialRevision::from([1; 32]), 100)
        },
        move || CredentialValidity {
            now_unix_seconds: observed.load(Ordering::SeqCst),
            revision: Some(CredentialRevision::from([1; 32])),
        },
    );
    let lease = cache.resolve().await.unwrap();
    let watch = cache.watch().unwrap();
    let provider = cache.clone().into();
    let dispatch_clock = now.clone();
    let result = channel::call(
        Endpoint::from_static("https://fixture.invalid").connect_lazy(),
        &provider,
        channel::Budget::Deadline(Some(
            tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        )),
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
    for change in ["account", "expiry", "unavailable", "unchanged", "cancel"] {
        let validity = Arc::new(Mutex::new(CredentialValidity {
            now_unix_seconds: 1,
            revision: Some(CredentialRevision::from([1; 32])),
        }));
        let observed = validity.clone();
        let cache = CredentialCache::new(
            move |_| async move {
                CredentialSnapshot::new("synthetic".into(), CredentialRevision::from([1; 32]), 100)
            },
            move || observed.lock().unwrap().clone(),
        );
        let lease = cache.resolve().await.unwrap();
        let watch = cache.watch().unwrap();
        let provider = cache.clone().into();
        let (entered, started) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let (cancel, cancelled) = oneshot::channel::<()>();
        let context = acyclic_auth::operation::Context::from_stopped(u64::MAX, async move {
            let _ = cancelled.await;
            acyclic_auth::operation::Stopped::Cancelled
        });
        let call = channel::call(
            Endpoint::from_static("https://fixture.invalid").connect_lazy(),
            &provider,
            channel::Budget::Operation(&context),
            (),
            move |_, request| async move {
                assert!(!request.metadata().contains_key("grpc-timeout"));
                entered.send(()).unwrap();
                released.await.unwrap();
                Ok(Response::new(()))
            },
        );
        let mut call = core::pin::pin!(call);
        futures::select! {
            result = call.as_mut().fuse() => panic!("held reply completed: {result:?}"),
            _ = started.fuse() => {},
        }
        {
            let mut validity = validity.lock().unwrap();
            match change {
                "account" => validity.revision = Some(CredentialRevision::from([2; 32])),
                "expiry" => validity.now_unix_seconds = 100,
                "unavailable" => validity.revision = None,
                _ => {}
            }
        }
        if change == "cancel" {
            cancel.send(()).unwrap();
        }
        release.send(()).unwrap();
        let result = call.await;
        if change == "unchanged" {
            assert!(result.is_ok());
        } else {
            let error = result.unwrap_err();
            assert_eq!(
                error.code(),
                if change == "cancel" {
                    Code::Cancelled
                } else if change == "expiry" {
                    Code::Unauthenticated
                } else {
                    Code::Unavailable
                }
            );
            assert!(std::error::Error::source(&error).is_some());
        }
        assert!(
            watch.now_or_never().is_none(),
            "local refusal is not remote rejection"
        );
        *validity.lock().unwrap() = CredentialValidity {
            now_unix_seconds: 1,
            revision: Some(CredentialRevision::from([1; 32])),
        };
        cache.check(&lease).unwrap();
    }
}
