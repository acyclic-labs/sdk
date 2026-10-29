//! Synthetic trusted-source vectors; no captured external credentials.
use acyclic_auth::credential::*;
use futures::{
    FutureExt,
    channel::oneshot,
    executor::block_on,
    future::{self, join_all},
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

fn revision(value: u64) -> CredentialRevision {
    CredentialRevision::from([value as u8; 32])
}
fn snapshot(value: &str, rev: u64, expires: u64) -> CredentialSnapshot {
    CredentialSnapshot::new(value.into(), revision(rev), expires).unwrap()
}
fn observer(clock: Arc<AtomicU64>, rev: Arc<AtomicU64>) -> impl Fn() -> CredentialValidity {
    move || CredentialValidity {
        now_unix_seconds: clock.load(Ordering::SeqCst),
        revision: Some(revision(rev.load(Ordering::SeqCst))),
    }
}
fn pending(future: Pin<&mut impl Future>) {
    let mut cx = Context::from_waker(futures::task::noop_waker_ref());
    assert!(matches!(future.poll(&mut cx), Poll::Pending));
}

#[test]
fn provider_projects_trusted_binding_and_push_changes_without_raw_identity() {
    let rev = Arc::new(AtomicU64::new(1));
    let source_rev = rev.clone();
    let cache = CredentialCache::new(
        move |_| {
            future::ready(Ok(snapshot(
                "synthetic-binding",
                source_rev.load(Ordering::SeqCst),
                100,
            )))
        },
        observer(Arc::new(AtomicU64::new(1)), rev.clone()),
    );
    let provider: CredentialProvider = cache.clone().into();
    let mut change = Box::pin(block_on(provider.watch()).unwrap().unwrap());
    assert_eq!(change.validity().revision, Some(revision(1)));
    pending(change.as_mut());
    let old = block_on(provider.resolve()).unwrap();
    assert_eq!(
        old.binding(),
        Some(CredentialBinding {
            revision: revision(1),
            expires_at_unix_seconds: 100
        })
    );
    rev.store(2, Ordering::SeqCst);
    cache.invalidate().unwrap();
    block_on(change).unwrap();
    assert!(block_on(old.check()).is_err());

    let next = block_on(provider.resolve()).unwrap();
    let mut change = Box::pin(block_on(provider.watch()).unwrap().unwrap());
    assert_eq!(change.validity().revision, Some(revision(2)));
    assert_eq!(next.binding().unwrap().revision, revision(2));
    assert!(!block_on(old.reject()).unwrap());
    pending(change.as_mut());
    assert!(block_on(next.reject()).unwrap());
    block_on(change).unwrap();
    // Dropped subscriptions release their registration and do not affect peers.
    drop(block_on(provider.watch()).unwrap());
    cache.invalidate().unwrap();

    let raw = CredentialProvider::new(|| future::ready(Ok("synthetic-raw".into())));
    assert!(block_on(raw.watch()).unwrap().is_none());
    assert_eq!(block_on(raw.resolve()).unwrap().binding(), None);
}

#[test]
fn source_watch_retires_epochs_and_reports_last_producer_loss_without_waiting() {
    use futures::task::{ArcWake, waker};

    struct Reenter {
        source: CredentialCache,
        watch: Mutex<Option<CredentialWatch>>,
    }
    impl ArcWake for Reenter {
        fn wake_by_ref(this: &Arc<Self>) {
            *this.watch.lock().unwrap() = Some(this.source.watch().unwrap());
        }
    }
    let source = CredentialCache::new(
        |_| future::ready(Ok(snapshot("synthetic", 1, 100))),
        || CredentialValidity {
            now_unix_seconds: 1,
            revision: Some(revision(1)),
        },
    );
    let mut first = Box::pin(source.watch().unwrap());
    let mut peer = Box::pin(source.watch().unwrap());
    let reenter = Arc::new(Reenter {
        source: source.clone(),
        watch: Mutex::new(None),
    });
    let wake = waker(reenter.clone());
    assert!(
        first
            .as_mut()
            .poll(&mut Context::from_waker(&wake))
            .is_pending()
    );
    pending(peer.as_mut());
    source.invalidate().unwrap();
    assert!(matches!(first.now_or_never(), Some(Ok(()))));
    assert!(matches!(peer.now_or_never(), Some(Ok(()))));
    let mut next = Box::pin(reenter.watch.lock().unwrap().take().unwrap());
    // Registration from a wake belongs to the next epoch, not the retired one.
    pending(next.as_mut());
    drop(wake);
    drop(reenter);

    // Discarded subscriptions cannot retain their task wakers or the source.
    let temporary = Arc::new(Reenter {
        source: source.clone(),
        watch: Mutex::new(None),
    });
    let weak = Arc::downgrade(&temporary);
    let wake = waker(temporary.clone());
    let mut discarded = Box::pin(source.watch().unwrap());
    assert!(
        discarded
            .as_mut()
            .poll(&mut Context::from_waker(&wake))
            .is_pending()
    );
    drop(discarded);
    drop(wake);
    drop(temporary);
    assert!(weak.upgrade().is_none());

    let retained = source.clone();
    drop(source);
    pending(next.as_mut()); // A live producer is the negative control.
    drop(retained);
    assert!(matches!(
        next.now_or_never(),
        Some(Err(CredentialCacheError::Unavailable))
    ));
}

#[test]
fn cold_cohort_and_warm_calls_share_one_snapshot() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (send, receive) = oneshot::channel::<()>();
    let gate = receive.shared();
    let count = calls.clone();
    let cache = CredentialCache::new(
        move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            let gate = gate.clone();
            async move {
                gate.await.unwrap();
                Ok(snapshot("bearer", 1, 100))
            }
        },
        || CredentialValidity {
            now_unix_seconds: 1,
            revision: Some(revision(1)),
        },
    );
    let mut cohort = Box::pin(join_all((0..64).map(|_| cache.resolve())));
    pending(cohort.as_mut());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    send.send(()).unwrap();
    let leases: Vec<_> = block_on(cohort).into_iter().map(Result::unwrap).collect();
    for lease in leases.iter().skip(1) {
        assert!(Arc::ptr_eq(&leases[0], lease));
    }
    let warm = block_on(join_all((0..64).map(|_| cache.resolve())));
    for lease in warm {
        assert!(Arc::ptr_eq(&leases[0], &lease.unwrap()));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(!format!("{:?}", leases[0]).contains("bearer"));
}

#[test]
fn expiry_revision_and_unavailable_source_never_use_the_old_bearer() {
    let clock = Arc::new(AtomicU64::new(9));
    let rev = Arc::new(AtomicU64::new(1));
    let calls = Arc::new(AtomicUsize::new(0));
    let source_rev = rev.clone();
    let count = calls.clone();
    let cache = CredentialCache::new(
        move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            future::ready(Ok(snapshot(
                "bearer",
                source_rev.load(Ordering::SeqCst),
                10,
            )))
        },
        observer(clock.clone(), rev.clone()),
    );
    let first = block_on(cache.resolve()).unwrap();
    rev.store(2, Ordering::SeqCst);
    let second = block_on(cache.resolve()).unwrap();
    assert_eq!(second.revision(), revision(2));
    assert!(!Arc::ptr_eq(&first, &second));
    clock.store(10, Ordering::SeqCst);
    assert!(matches!(
        block_on(cache.resolve()),
        Err(CredentialCacheError::Expired)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let unavailable = CredentialCache::new(
        |_| async { panic!("unavailable source must refuse before IO") },
        || CredentialValidity {
            now_unix_seconds: 0,
            revision: None,
        },
    );
    assert!(matches!(
        block_on(unavailable.resolve()),
        Err(CredentialCacheError::Unavailable)
    ));
}

struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn one_waiter_cancels_independently_last_waiter_drops_source_io() {
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let source_drops = drops.clone();
    let cache = CredentialCache::new(
        move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            let owned = Dropped(source_drops.clone());
            async move {
                let _owned = owned;
                future::pending().await
            }
        },
        || CredentialValidity {
            now_unix_seconds: 1,
            revision: Some(revision(1)),
        },
    );
    let mut first = Box::pin(cache.resolve());
    let mut second = Box::pin(cache.resolve());
    pending(first.as_mut());
    pending(second.as_mut());
    drop(first);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(second);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let mut next = Box::pin(cache.resolve());
    pending(next.as_mut());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    drop(next);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

#[test]
fn failed_refresh_cohort_shares_the_original_diagnostic_without_caching_failure() {
    let (send, receive) = oneshot::channel::<()>();
    let gate = receive.shared();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let cache = CredentialCache::new(
        move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            let gate = gate.clone();
            async move {
                gate.await.unwrap();
                Err(CredentialError::Provider(Box::new(std::io::Error::other(
                    "source unavailable",
                ))))
            }
        },
        || CredentialValidity {
            now_unix_seconds: 1,
            revision: Some(revision(1)),
        },
    );
    let mut cohort = Box::pin(join_all((0..8).map(|_| cache.resolve())));
    pending(cohort.as_mut());
    send.send(()).unwrap();
    let errors: Vec<_> = block_on(cohort)
        .into_iter()
        .map(|result| match result {
            Err(CredentialCacheError::Provider(error)) => error,
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    for error in errors.iter().skip(1) {
        assert!(Arc::ptr_eq(&errors[0], error));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(block_on(cache.resolve()).is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn invalidation_aborts_old_refresh_and_late_revision_completion_refuses() {
    let (send, receive) = oneshot::channel::<()>();
    let gate = receive.shared();
    let rev = Arc::new(AtomicU64::new(1));
    let cache = CredentialCache::new(
        move |_| {
            let gate = gate.clone();
            async move {
                gate.await.unwrap();
                Ok(snapshot("old", 1, 100))
            }
        },
        observer(Arc::new(AtomicU64::new(1)), rev.clone()),
    );
    let mut old = Box::pin(cache.resolve());
    pending(old.as_mut());
    cache.invalidate().unwrap();
    assert!(matches!(block_on(old), Err(CredentialCacheError::Stale)));
    let mut newer = Box::pin(cache.resolve());
    pending(newer.as_mut());
    rev.store(2, Ordering::SeqCst);
    send.send(()).unwrap();
    assert!(matches!(block_on(newer), Err(CredentialCacheError::Stale)));
}

#[test]
fn exact_401_invalidation_passes_rejected_bearer_and_protects_replacement() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let rejected = Arc::new(Mutex::new(Vec::new()));
    let source = rejected.clone();
    let cache = CredentialCache::new(
        move |previous| {
            source
                .lock()
                .unwrap()
                .push(previous.map(|snapshot| snapshot.token().to_owned()));
            let index = count.fetch_add(1, Ordering::SeqCst);
            future::ready(Ok(snapshot(&format!("bearer{index}"), 1, 100)))
        },
        || CredentialValidity {
            now_unix_seconds: 1,
            revision: Some(revision(1)),
        },
    );
    let old = block_on(cache.resolve()).unwrap();
    assert!(cache.reject(&old).unwrap());
    let new = block_on(cache.resolve()).unwrap();
    assert!(!cache.reject(&old).unwrap());
    assert!(Arc::ptr_eq(&new, &block_on(cache.resolve()).unwrap()));
    assert_eq!(
        *rejected.lock().unwrap(),
        vec![None, Some("bearer0".into())]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let reused = CredentialCache::new(
        |_| future::ready(Ok(snapshot("rejected", 1, 100))),
        || CredentialValidity {
            now_unix_seconds: 1,
            revision: Some(revision(1)),
        },
    );
    let old = block_on(reused.resolve()).unwrap();
    reused.reject(&old).unwrap();
    assert!(matches!(
        block_on(reused.resolve()),
        Err(CredentialCacheError::Rejected)
    ));
}

#[test]
fn cached_callback_composes_with_existing_provider_and_redacts_invalid_bytes() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let clock = Arc::new(AtomicU64::new(1));
    let cache = CredentialCache::new(
        move |_| {
            future::ready(Ok(snapshot(
                &format!("opaque{}", count.fetch_add(1, Ordering::SeqCst)),
                1,
                100,
            )))
        },
        observer(clock.clone(), Arc::new(AtomicU64::new(1))),
    );
    let provider = CredentialProvider::from(cache);
    let old = block_on(provider.resolve()).unwrap();
    block_on(old.check()).unwrap();
    assert!(block_on(old.reject()).unwrap());
    let current = block_on(provider.resolve()).unwrap();
    assert!(!block_on(old.reject()).unwrap());
    assert!(block_on(old.check()).is_err());
    block_on(current.check()).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    clock.store(100, Ordering::SeqCst);
    assert!(block_on(current.check()).is_err());
    assert!(CredentialSnapshot::new("bad\r\nheader".into(), revision(1), 100).is_err());
    fn send_sync<T: Send + Sync>() {}
    send_sync::<CredentialCache>();
}
