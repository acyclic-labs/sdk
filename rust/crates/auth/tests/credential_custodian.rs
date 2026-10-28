//! Synthetic custodian vectors preserve ownership across an adapter boundary.
use acyclic_auth::credential::*;
use futures::{FutureExt, channel::oneshot, executor::block_on, future::join_all};
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

struct Source {
    provider: CredentialProvider,
    closed: Arc<AtomicUsize>,
}

impl CredentialSource for Source {
    fn resolve(&self) -> CredentialResolution<'_> {
        Box::pin(async {
            ResolvedCredential::from_custodian(Request(self.provider.resolve().await?))
        })
    }

    fn watch(&self) -> Result<Option<CredentialWatch>, CredentialError> {
        struct Registration(Arc<AtomicUsize>);
        impl Drop for Registration {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        Ok(self.provider.watch()?.map(|watch| {
            let validity = watch.validity();
            let registration = Registration(self.closed.clone());
            CredentialWatch::from_custodian(validity, async move {
                let _registration = registration;
                watch.await;
            })
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
    fn check(&self) -> Result<(), CredentialError> {
        self.0.check()
    }
    fn reject(&self) -> Result<bool, CredentialError> {
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

    drop(provider.watch().unwrap());
    assert_eq!(closed.load(Ordering::SeqCst), 1);
    let watch = provider.watch().unwrap().unwrap();
    assert_eq!(watch.validity().revision, Some(revision));
    assert!(old.reject().unwrap());
    block_on(watch);
    assert_eq!(closed.load(Ordering::SeqCst), 2);
    assert!(requests.iter().all(|request| request.check().is_err()));
    let current = block_on(provider.resolve()).unwrap();
    assert!(!old.reject().unwrap());
    current.check().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    now.store(100, Ordering::SeqCst);
    assert!(current.check().is_err());

    let raw = CredentialProvider::from_custodian(Source {
        provider: CredentialProvider::new(|| async { Ok("synthetic-raw".into()) }),
        closed,
    });
    assert!(raw.watch().unwrap().is_none());
    assert!(block_on(raw.resolve()).unwrap().binding().is_none());
}
