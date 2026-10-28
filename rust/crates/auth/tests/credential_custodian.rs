//! Synthetic custodian vectors preserve ownership across an adapter boundary.
use acyclic_auth::credential::*;
use futures::{FutureExt, channel::oneshot, executor::block_on, future::join_all};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

struct DeferredRequest {
    lease: ResolvedCredential,
    admission: Mutex<Option<oneshot::Receiver<()>>>,
    rejection: Mutex<Option<oneshot::Receiver<()>>>,
}

fn producer_lost(_: oneshot::Canceled) -> CredentialError {
    CredentialError::Provider(Box::new(CredentialCacheError::Unavailable))
}

impl CredentialRequest for DeferredRequest {
    fn token(&self) -> &str {
        self.lease.as_str()
    }
    fn binding(&self) -> Option<CredentialBinding> {
        self.lease.binding()
    }
    fn check(&self) -> CredentialAction<()> {
        let Some(admission) = self.admission.lock().unwrap().take() else {
            return self.lease.check();
        };
        let lease = self.lease.clone();
        CredentialAction::Pending(Box::pin(async move {
            admission.await.map_err(producer_lost)?;
            lease.check().await
        }))
    }
    fn reject(&self) -> CredentialAction<bool> {
        let Some(rejection) = self.rejection.lock().unwrap().take() else {
            return self.lease.reject();
        };
        let lease = self.lease.clone();
        CredentialAction::Pending(Box::pin(async move {
            rejection.await.map_err(producer_lost)?;
            lease.reject().await
        }))
    }
}

#[test]
fn deferred_owner_requires_its_event_and_preserves_exact_rejection_after_replacement() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let cache = CredentialCache::new(
        move |_| {
            let call = count.fetch_add(1, Ordering::SeqCst);
            async move { CredentialSnapshot::new(format!("synthetic-{call}"), [1; 32].into(), 100) }
        },
        || CredentialValidity {
            now_unix_seconds: 1,
            revision: Some([1; 32].into()),
        },
    );
    let provider: CredentialProvider = cache.clone().into();
    let lease = block_on(provider.resolve()).unwrap();
    for completion in ["admit", "producer-loss", "cancel"] {
        let (complete, completed) = oneshot::channel();
        let mut admission = Box::pin(ResolvedCredential::from_custodian(DeferredRequest {
            lease: lease.clone(),
            admission: Mutex::new(Some(completed)),
            rejection: Mutex::new(None),
        }));
        assert!(admission.as_mut().now_or_never().is_none());
        match completion {
            "admit" => {
                complete.send(()).unwrap();
                block_on(admission).unwrap();
            }
            "producer-loss" => {
                drop(complete);
                let error = admission
                    .now_or_never()
                    .expect("producer loss must wake admission")
                    .unwrap_err();
                assert!(
                    matches!(error, CredentialError::Provider(source) if matches!(source.downcast_ref::<CredentialCacheError>(), Some(CredentialCacheError::Unavailable)))
                );
            }
            _ => {
                drop(admission);
                assert!(complete.is_canceled());
            }
        }
        // Refusal/cancellation cannot reject a shared snapshot or refresh it.
        block_on(lease.check()).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    let (complete, completed) = oneshot::channel();
    let old = block_on(ResolvedCredential::from_custodian(DeferredRequest {
        lease,
        admission: Mutex::new(None),
        rejection: Mutex::new(Some(completed)),
    }))
    .unwrap();
    let mut rejection = Box::pin(old.reject());
    assert!(rejection.as_mut().now_or_never().is_none());
    cache.invalidate().unwrap();
    let current = block_on(provider.resolve()).unwrap();
    complete.send(()).unwrap();
    assert!(!block_on(rejection).unwrap());
    block_on(current.check()).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

struct Source {
    provider: CredentialProvider,
    closed: Arc<AtomicUsize>,
}

impl CredentialSource for Source {
    fn resolve(&self) -> CredentialResolution<'_> {
        Box::pin(async {
            ResolvedCredential::from_custodian(Request(self.provider.resolve().await?)).await
        })
    }

    fn watch(&self) -> CredentialAction<Option<CredentialWatch>> {
        struct Registration(Arc<AtomicUsize>);
        impl Drop for Registration {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let watch = self.provider.watch();
        let closed = self.closed.clone();
        CredentialAction::Pending(Box::pin(async move {
            Ok(watch.await?.map(|watch| {
                let validity = watch.validity();
                let registration = Registration(closed);
                CredentialWatch::from_custodian(validity, async move {
                    let _registration = registration;
                    watch.await
                })
            }))
        }))
    }
}

struct Request(ResolvedCredential);
impl CredentialRequest for Request {
    fn token(&self) -> &str {
        self.0.as_str()
    }
    fn binding(&self) -> Option<CredentialBinding> {
        self.0.binding()
    }
    fn check(&self) -> CredentialAction<()> {
        self.0.check()
    }
    fn reject(&self) -> CredentialAction<bool> {
        self.0.reject()
    }
}

#[test]
fn custodian_adapter_retains_singleflight_exact_rejection_and_drop_owned_watch() {
    let calls = Arc::new(AtomicUsize::new(0));
    let now = Arc::new(AtomicU64::new(1));
    let counted = calls.clone();
    let observed = now.clone();
    // More than 32 UTF-16 code units, including an unpaired surrogate: lossless,
    // source-supplied metadata, never a hash or interpretation of bearer bytes.
    let revision = CredentialRevision::from(vec![0xd800; 257]);
    let resolved_revision = revision.clone();
    let observed_revision = revision.clone();
    let (release, released) = oneshot::channel::<()>();
    let gate = released.shared();
    let cache = CredentialCache::new(
        move |_| {
            let gate = gate.clone();
            let revision = resolved_revision.clone();
            let count = counted.fetch_add(1, Ordering::SeqCst);
            async move {
                gate.await.unwrap();
                CredentialSnapshot::new(format!("synthetic-{count}"), revision, 100)
            }
        },
        move || CredentialValidity {
            now_unix_seconds: observed.load(Ordering::SeqCst),
            revision: Some(observed_revision.clone()),
        },
    );
    let closed = Arc::new(AtomicUsize::new(0));
    let provider = CredentialProvider::from_custodian(Source {
        provider: cache.clone().into(),
        closed: closed.clone(),
    });
    let mut cohort = Box::pin(join_all((0..100).map(|_| provider.resolve())));
    assert!(cohort.as_mut().now_or_never().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    release.send(()).unwrap();
    let requests: Vec<_> = block_on(cohort).into_iter().map(Result::unwrap).collect();
    let old = &requests[0];
    let binding = old.binding().unwrap();
    assert_eq!(binding.revision, revision);
    assert_ne!(
        binding.revision,
        CredentialRevision::from(vec![0xfffd; 257])
    );
    assert_eq!(binding.expires_at_unix_seconds, 100);

    drop(block_on(provider.watch()).unwrap());
    assert_eq!(closed.load(Ordering::SeqCst), 1);
    let watch = block_on(provider.watch()).unwrap().unwrap();
    assert_eq!(watch.validity().revision, Some(revision));
    assert!(block_on(old.reject()).unwrap());
    block_on(watch).unwrap();
    assert_eq!(closed.load(Ordering::SeqCst), 2);
    assert!(
        requests
            .iter()
            .all(|request| block_on(request.check()).is_err())
    );
    let current = block_on(provider.resolve()).unwrap();
    assert!(!block_on(old.reject()).unwrap());
    block_on(current.check()).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    now.store(100, Ordering::SeqCst);
    assert!(block_on(current.check()).is_err());

    let raw = CredentialProvider::from_custodian(Source {
        provider: CredentialProvider::new(|| async { Ok("synthetic-raw".into()) }),
        closed,
    });
    assert!(block_on(raw.watch()).unwrap().is_none());
    assert!(block_on(raw.resolve()).unwrap().binding().is_none());

    let source = CredentialProvider::from_custodian(Source {
        provider: CredentialCache::new(
            |_| async { CredentialSnapshot::new("synthetic".into(), [0; 32].into(), 100) },
            || CredentialValidity {
                now_unix_seconds: 1,
                revision: Some([0; 32].into()),
            },
        )
        .into(),
        closed: Arc::new(AtomicUsize::new(0)),
    });
    let mut watch = Box::pin(block_on(source.watch()).unwrap().unwrap());
    assert!(watch.as_mut().now_or_never().is_none());
    drop(source);
    assert!(matches!(
        watch.now_or_never(),
        Some(Err(CredentialCacheError::Unavailable))
    ));
}
