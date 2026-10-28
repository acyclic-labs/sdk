//! Source custody for the shared reader, driven by its existing task.

use acyclic_auth::credential::{
    CredentialBinding, CredentialProvider, CredentialWatch, ResolvedCredential,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// Qualification of the reader's retained data, independently of connectivity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialStatus {
    /// No trusted source binding is available.
    Unqualified,
    /// A registered source is resolving an admitted lease.
    Pending,
    /// The retained lease still passes the source's exact validity check.
    Current,
    /// Source availability, invalidation or expiry prevents reuse.
    Unavailable,
}

#[derive(Clone, Default)]
pub(super) enum Access {
    /// Explicit sans-I/O use; no remote qualification is asserted.
    #[default]
    Local,
    Unqualified,
    Closed,
    Lease(ResolvedCredential),
}

impl Access {
    pub(super) fn allows(&self) -> bool {
        match self {
            Self::Local | Self::Unqualified => true,
            Self::Closed => false,
            Self::Lease(lease) => lease.check().is_ok(),
        }
    }

    pub(super) fn binding(&self) -> Result<Option<CredentialBinding>, super::SnapshotError> {
        match self {
            Self::Local => Ok(None),
            Self::Lease(lease) if lease.check().is_ok() => {
                lease.binding().map(Some).ok_or(super::SnapshotError)
            }
            _ => Err(super::SnapshotError),
        }
    }
}

type Resolution = acyclic_stream::BoxProviderFuture<
    'static,
    Result<ResolvedCredential, acyclic_auth::credential::CredentialError>,
>;

pub(super) struct Credentials {
    provider: CredentialProvider,
    watch: Option<CredentialWatch>,
    resolving: Option<Resolution>,
    lease: Option<ResolvedCredential>,
    registered_at: u64,
    timeout_ms: u64,
    deadline: Option<u64>,
    status: CredentialStatus,
}

impl Credentials {
    pub(super) fn new(provider: CredentialProvider, now: u64, timeout_ms: u64) -> Self {
        let mut state = Self {
            provider,
            watch: None,
            resolving: None,
            lease: None,
            registered_at: now,
            timeout_ms,
            deadline: None,
            status: CredentialStatus::Pending,
        };
        state.restart(now);
        state
    }

    fn restart(&mut self, now: u64) {
        self.watch = None;
        self.resolving = None;
        self.lease = None;
        self.deadline = None;
        self.registered_at = now;
        // Registration precedes both resolution and any retained-data access.
        self.watch = match self.provider.watch() {
            Ok(Some(watch)) => Some(watch),
            Ok(None) => {
                self.status = CredentialStatus::Unqualified;
                return;
            }
            Err(_) => {
                self.status = CredentialStatus::Unavailable;
                return;
            }
        };
        if self
            .watch
            .as_ref()
            .is_none_or(|watch| watch.validity().revision.is_none())
        {
            self.status = CredentialStatus::Unavailable;
            return;
        }
        self.status = CredentialStatus::Pending;
        self.deadline = Some(now.saturating_add(self.timeout_ms));
        let provider = self.provider.clone();
        self.resolving = Some(Box::pin(async move { provider.resolve().await }));
    }

    pub(super) fn status(&self) -> CredentialStatus {
        if self.status == CredentialStatus::Current
            && self
                .lease
                .as_ref()
                .is_none_or(|lease| lease.check().is_err())
        {
            CredentialStatus::Unavailable
        } else {
            self.status
        }
    }

    pub(super) fn access(&self) -> Access {
        match self.status() {
            CredentialStatus::Unqualified => Access::Unqualified,
            CredentialStatus::Current => self.lease.clone().map_or(Access::Closed, Access::Lease),
            _ => Access::Closed,
        }
    }

    pub(super) fn deadline(&self) -> Option<u64> {
        self.deadline
    }

    pub(super) fn retry(&mut self, now: u64) {
        self.restart(now);
    }

    /// A source transition invalidates all data even when a replacement uses
    /// the same revision. Exact-current rejection also ends the old custody.
    pub(super) fn poll(&mut self, now: u64, cx: &mut Context<'_>) -> bool {
        let changed = self
            .watch
            .as_mut()
            .is_some_and(|watch| Pin::new(watch).poll(cx).is_ready());
        let expired = self.status == CredentialStatus::Current
            && (self
                .lease
                .as_ref()
                .is_none_or(|lease| lease.check().is_err())
                || self.deadline.is_some_and(|deadline| now >= deadline));
        let mut reset = changed || expired;
        if reset {
            self.restart(now);
        }
        if self.status == CredentialStatus::Pending
            && self.deadline.is_some_and(|deadline| now >= deadline)
        {
            self.resolving = None;
            self.deadline = None;
            self.status = CredentialStatus::Unavailable;
            return true;
        }
        let resolved = match self.resolving.as_mut() {
            Some(resolving) => match resolving.as_mut().poll(cx) {
                Poll::Ready(result) => Some(result),
                Poll::Pending => None,
            },
            None => None,
        };
        if let Some(result) = resolved {
            self.resolving = None;
            self.deadline = None;
            self.status = CredentialStatus::Unavailable;
            if let Ok(lease) = result
                && let Some(binding) = lease.binding()
                && let Some(watch) = &self.watch
                && watch.validity().revision.as_ref() == Some(&binding.revision)
                && lease.check().is_ok()
            {
                let remaining = binding
                    .expires_at_unix_seconds
                    .saturating_sub(watch.validity().now_unix_seconds);
                self.deadline = Some(
                    self.registered_at
                        .saturating_add(remaining.saturating_mul(1000)),
                );
                self.lease = Some(lease);
                self.status = CredentialStatus::Current;
            }
            // Wake/register the source watch after resolving as well: a
            // refresh may have invalidated its registration while awaited.
            if self
                .watch
                .as_mut()
                .is_some_and(|watch| Pin::new(watch).poll(cx).is_ready())
            {
                self.restart(now);
                cx.waker().wake_by_ref();
            }
            reset = true;
        }
        reset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::{Fetched, Outcome, Priority, Reader, ReaderPolicy, Want};
    use acyclic_auth::credential::{
        CredentialCache, CredentialRevision, CredentialSnapshot, CredentialValidity,
    };
    use acyclic_stream::{AppendRequest, IdempotencyKey, MemoryStream, StreamPath, StreamProvider};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };
    use std::task::{Wake, Waker};

    struct Ready(AtomicBool);
    impl Wake for Ready {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn pending_source_uses_the_original_deadline_without_retry() {
        use std::sync::atomic::AtomicUsize;
        let calls = Arc::new(AtomicUsize::new(0));
        let available = Arc::new(AtomicBool::new(false));
        let refreshed = calls.clone();
        let renewed = available.clone();
        let source = CredentialCache::new(
            move |_| {
                refreshed.fetch_add(1, Ordering::SeqCst);
                let available = renewed.clone();
                async move {
                    if !available.load(Ordering::SeqCst) {
                        futures::future::pending::<()>().await;
                    }
                    CredentialSnapshot::new(
                        "credential".into(),
                        CredentialRevision::from([1; 32]),
                        20,
                    )
                }
            },
            || CredentialValidity {
                now_unix_seconds: 10,
                revision: Some(CredentialRevision::from([1; 32])),
            },
        );
        let mut credentials = Credentials::new(source.clone().into(), 10, 25);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(!credentials.poll(10, &mut cx));
        assert_eq!(credentials.deadline(), Some(35));
        assert!(credentials.poll(35, &mut cx));
        assert_eq!(credentials.status(), CredentialStatus::Unavailable);
        assert_eq!(credentials.deadline(), None);
        assert!(!credentials.poll(1_000, &mut cx));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        available.store(true, Ordering::SeqCst);
        source.invalidate().unwrap();
        assert!(credentials.poll(1_000, &mut cx));
        assert_eq!(credentials.status(), CredentialStatus::Current);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn source_change_hides_retained_data_before_the_watch_is_driven() {
        futures::executor::block_on(async {
            let validity = Arc::new(Mutex::new(CredentialValidity {
                now_unix_seconds: 10,
                revision: Some(CredentialRevision::from([1; 32])),
            }));
            let observed = validity.clone();
            let refreshed = validity.clone();
            let source = CredentialCache::new(
                move |_| {
                    let observation = refreshed.lock().unwrap().clone();
                    async move {
                        CredentialSnapshot::new(
                            "credential".into(),
                            observation.revision.unwrap(),
                            20,
                        )
                    }
                },
                move || observed.lock().unwrap().clone(),
            );
            let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
            reader.credentials = Some(Credentials::new(source.clone().into(), 500, 10_000));
            reader.reset_source();
            assert!(!reader.readable());
            let ready = Arc::new(Ready(AtomicBool::new(false)));
            let waker = Waker::from(ready.clone());
            let mut cx = Context::from_waker(&waker);
            assert!(reader.advance_credentials(500, &mut cx));
            assert_eq!(reader.credential_status(), CredentialStatus::Current);
            assert_eq!(reader.next_deadline(), Some(10_500));
            let old_lease = reader.cache.access.clone();

            let provider = MemoryStream::default();
            let path = StreamPath::new("source/record").unwrap();
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![b"committed".as_slice().into()],
                    if_tail: None,
                    idempotency_key: IdempotencyKey::new(b"source record".to_vec()).unwrap(),
                })
                .await
                .unwrap();
            let head = provider.head(path.clone()).await.unwrap();
            reader.plan([(
                Want::Last {
                    stream: path.clone(),
                    count: 1,
                },
                Priority::Visible,
            )]);
            let id = reader.poll(500).pop().unwrap().id();
            reader.complete(id, Outcome::Head(head), 500);
            let view = reader.cache().record(&path, 0).unwrap().clone();
            let old_backing = reader.cache().backing_bytes();
            let snapshot = reader.snapshot();
            assert!(!snapshot.is_empty());
            let mut restored = Reader::new(ReaderPolicy::default()).unwrap();
            restored.credentials = Some(Credentials::new(source.clone().into(), 500, 10_000));
            restored.reset_source();
            restored.restore(&snapshot).unwrap();
            assert_eq!(restored.residency().snapshot_bytes, snapshot.len());
            assert!(restored.cache().record(&path, 0).is_none());
            restored.advance_credentials(500, &mut cx);
            assert_eq!(restored.residency().snapshot_bytes, 0);
            assert_eq!(
                restored.cache().record(&path, 0).unwrap().as_ref(),
                b"committed"
            );
            reader.plan([(
                Want::Range {
                    stream: path.clone(),
                    from: 1,
                    to: 2,
                },
                Priority::Visible,
            )]);
            // Keep a real in-flight observation to reject after replacement.
            let late_path = StreamPath::new("source/late").unwrap();
            reader.plan([(
                Want::Last {
                    stream: late_path,
                    count: 1,
                },
                Priority::Visible,
            )]);
            let late = reader.poll(500).pop().unwrap().id();
            validity.lock().unwrap().revision = None;
            assert!(!reader.readable());
            assert!(reader.cache().record(&path, 0).is_none());
            assert!(reader.cache().tail(&path).is_none());
            assert!(!reader.cache().origin_known(&path));
            assert!(reader.snapshot().is_empty());
            source.invalidate().unwrap();
            assert!(ready.0.load(Ordering::SeqCst));
            reader.advance_credentials(600, &mut cx);
            assert_eq!(reader.credential_status(), CredentialStatus::Unavailable);
            assert_eq!(reader.residency().requests, 0);
            assert_eq!(reader.cache().backing_bytes(), old_backing);
            assert_eq!(view.as_ref(), b"committed");

            validity.lock().unwrap().revision = Some(CredentialRevision::from([2; 32]));
            source.invalidate().unwrap();
            reader.advance_credentials(700, &mut cx);
            assert_eq!(reader.credential_status(), CredentialStatus::Current);
            assert!(reader.restore(&snapshot).is_err());
            reader.complete(
                late,
                Outcome::Records(vec![Fetched {
                    sequence: 0,
                    value: b"late".as_slice().into(),
                    commit: acyclic_stream::CommitId::from_bytes([7; 32]),
                }]),
                700,
            );
            assert!(reader.cache().record(&path, 0).is_none());
            let Access::Lease(old_lease) = old_lease else {
                panic!("missing admitted lease");
            };
            assert!(!old_lease.reject().unwrap());
            assert!(reader.readable());
            validity.lock().unwrap().now_unix_seconds = 20;
            assert!(!reader.readable());
            assert_eq!(reader.credential_status(), CredentialStatus::Unavailable);
            drop(view);
            assert_eq!(reader.cache().backing_bytes(), 0);
        });
    }

    #[test]
    fn persisted_partitions_preserve_utf16_and_original_absolute_expiry() {
        futures::executor::block_on(async {
            use std::sync::atomic::AtomicU64;
            let provider = MemoryStream::default();
            let path = StreamPath::new("snapshot/record").unwrap();
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![b"immutable".as_slice().into()],
                    if_tail: None,
                    idempotency_key: IdempotencyKey::new(b"snapshot record".to_vec()).unwrap(),
                })
                .await
                .unwrap();
            let mut snapshots: Vec<Vec<u8>> = Vec::new();
            for revision in [
                CredentialRevision::from([4; 32]),
                CredentialRevision::from(vec![0xd800, 0, 0xdc00]),
                CredentialRevision::from(vec![0xd801, 0, 0xdc00]),
            ] {
                let observed = revision.clone();
                let expires = Arc::new(AtomicU64::new(20));
                let refreshed = expires.clone();
                let source = CredentialCache::new(
                    move |_| {
                        let revision = revision.clone();
                        let expiry = refreshed.load(Ordering::SeqCst);
                        async move { CredentialSnapshot::new("credential".into(), revision, expiry) }
                    },
                    move || CredentialValidity {
                        now_unix_seconds: 10,
                        revision: Some(observed.clone()),
                    },
                );
                let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
                reader.credentials = Some(Credentials::new(source.clone().into(), 0, 10_000));
                reader.reset_source();
                let waker = futures::task::noop_waker();
                let mut cx = Context::from_waker(&waker);
                reader.advance_credentials(0, &mut cx);
                reader.plan([(
                    Want::Last {
                        stream: path.clone(),
                        count: 1,
                    },
                    Priority::Visible,
                )]);
                let id = reader.poll(0).pop().unwrap().id();
                reader.complete(
                    id,
                    Outcome::Head(provider.head(path.clone()).await.unwrap()),
                    0,
                );
                let snapshot = reader.snapshot();
                reader.restore(&snapshot).unwrap();
                assert_eq!(
                    reader.cache().record(&path, 0).unwrap().as_ref(),
                    b"immutable"
                );
                for other in &snapshots {
                    assert!(reader.restore(other).is_err());
                }
                expires.store(25, Ordering::SeqCst);
                source.invalidate().unwrap();
                reader.advance_credentials(1, &mut cx);
                assert!(reader.readable());
                assert!(reader.restore(&snapshot).is_err());
                snapshots.push(snapshot);
            }
        });
    }
}
