//! Source custody for the shared reader, driven by its existing task.

use acyclic_auth::credential::{
    CredentialAction, CredentialBinding, CredentialProvider, CredentialWatch, ResolvedCredential,
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
    /// The configured source cannot guard synchronous retained views.
    Unsupported,
    /// The registered notification producer ended. Replace the configured
    /// source to recover; connectivity changes cannot revive this producer.
    SourceLost,
}

#[derive(Clone, Default)]
pub(super) enum Access {
    /// Explicit sans-I/O use; no remote qualification is asserted.
    #[default]
    Local,
    Closed,
    Lease(ResolvedCredential),
}

impl Access {
    pub(super) async fn check(&self) -> bool {
        match self {
            Self::Local => true,
            Self::Closed => false,
            Self::Lease(lease) => lease.check().await.is_ok(),
        }
    }

    pub(super) fn allows(&self) -> bool {
        match self {
            Self::Local => true,
            Self::Closed => false,
            Self::Lease(lease) => matches!(lease.retained_access(), Some(Ok(()))),
        }
    }

    pub(super) fn binding(&self) -> Result<Option<CredentialBinding>, super::SnapshotError> {
        match self {
            Self::Local => Ok(None),
            Self::Lease(lease) if matches!(lease.retained_access(), Some(Ok(()))) => {
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
    registering: Option<CredentialAction<Option<CredentialWatch>>>,
    watch: Option<CredentialWatch>,
    resolving: Option<Resolution>,
    lease: Option<ResolvedCredential>,
    watch_ready_at: u64,
    timeout_ms: u64,
    deadline: Option<u64>,
    status: CredentialStatus,
}

impl Credentials {
    pub(super) fn new(provider: CredentialProvider, now: u64, timeout_ms: u64) -> Self {
        let mut state = Self {
            provider,
            registering: None,
            watch: None,
            resolving: None,
            lease: None,
            watch_ready_at: 0,
            timeout_ms,
            deadline: None,
            status: CredentialStatus::Pending,
        };
        state.restart(now);
        state
    }

    fn restart(&mut self, now: u64) {
        self.registering = None;
        self.watch = None;
        self.resolving = None;
        self.lease = None;
        self.deadline = Some(now.saturating_add(self.timeout_ms));
        // Registration precedes both resolution and any retained-data access.
        self.registering = Some(self.provider.watch());
        self.status = CredentialStatus::Pending;
    }

    fn registered(
        &mut self,
        result: Result<Option<CredentialWatch>, acyclic_auth::credential::CredentialError>,
        now: u64,
    ) {
        self.registering = None;
        self.watch = match result {
            Ok(Some(watch)) => Some(watch),
            Ok(None) => {
                self.deadline = None;
                self.status = CredentialStatus::Unsupported;
                return;
            }
            Err(_) => {
                self.deadline = None;
                self.status = CredentialStatus::SourceLost;
                return;
            }
        };
        if self
            .watch
            .as_ref()
            .is_none_or(|watch| watch.validity().revision.is_none())
        {
            self.deadline = None;
            self.status = CredentialStatus::Unavailable;
            return;
        }
        // An awaited owner can observe its source clock after registration
        // starts. Anchor its remaining lifetime when that observation arrives;
        // retained_access still enforces the original absolute expiry.
        self.watch_ready_at = now;
        self.status = CredentialStatus::Pending;
        let provider = self.provider.clone();
        self.resolving = Some(Box::pin(async move {
            let lease = provider.resolve().await?;
            lease.check().await?;
            Ok(lease)
        }));
    }

    pub(super) fn pending(&self) -> bool {
        self.status == CredentialStatus::Pending
    }

    pub(super) fn status(&self) -> CredentialStatus {
        if self.status == CredentialStatus::Current
            && self
                .lease
                .as_ref()
                .is_none_or(|lease| !matches!(lease.retained_access(), Some(Ok(()))))
        {
            CredentialStatus::Unavailable
        } else {
            self.status
        }
    }

    pub(super) fn access(&self) -> Access {
        match self.status() {
            CredentialStatus::Current => self.lease.clone().map_or(Access::Closed, Access::Lease),
            _ => Access::Closed,
        }
    }

    pub(super) fn deadline(&self) -> Option<u64> {
        self.deadline
    }

    pub(super) fn retry(&mut self, now: u64) {
        if self.status != CredentialStatus::SourceLost {
            self.restart(now);
        }
    }

    fn poll_watch(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
        match self.watch.as_mut().map(|watch| Pin::new(watch).poll(cx)) {
            Some(Poll::Ready(Ok(()))) => Poll::Ready(Ok(())),
            Some(Poll::Ready(Err(_))) => {
                self.registering = None;
                self.watch = None;
                self.resolving = None;
                self.lease = None;
                self.deadline = None;
                self.status = CredentialStatus::SourceLost;
                Poll::Ready(Err(()))
            }
            _ => Poll::Pending,
        }
    }

    /// A source transition invalidates all data even when a replacement uses
    /// the same revision. Exact-current rejection also ends the old custody.
    pub(super) fn poll(&mut self, now: u64, cx: &mut Context<'_>) -> bool {
        let changed = match self.poll_watch(cx) {
            Poll::Ready(Ok(())) => true,
            Poll::Ready(Err(())) => return true,
            Poll::Pending => false,
        };
        let expired = self.status == CredentialStatus::Current
            && (self
                .lease
                .as_ref()
                .is_none_or(|lease| !matches!(lease.retained_access(), Some(Ok(()))))
                || self.deadline.is_some_and(|deadline| now >= deadline));
        let mut reset = changed || expired;
        if reset {
            self.restart(now);
        }
        if self.status == CredentialStatus::Pending
            && self.deadline.is_some_and(|deadline| now >= deadline)
        {
            self.registering = None;
            self.resolving = None;
            self.deadline = None;
            self.status = CredentialStatus::Unavailable;
            return true;
        }
        if let Some(registering) = &mut self.registering {
            match Pin::new(registering).poll(cx) {
                Poll::Ready(result) => {
                    self.registered(result, now);
                    reset |= self.status != CredentialStatus::Pending;
                    // Registration can observe a source which already changed
                    // or lost its producer. Never resolve against that cut.
                    match self.poll_watch(cx) {
                        Poll::Ready(Ok(())) => {
                            self.restart(now);
                            cx.waker().wake_by_ref();
                            return true;
                        }
                        Poll::Ready(Err(())) => return true,
                        Poll::Pending => {}
                    }
                }
                Poll::Pending => return reset,
            }
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
            {
                match lease.retained_access() {
                    Some(Ok(())) => {
                        let remaining = binding
                            .expires_at_unix_seconds
                            .saturating_sub(watch.validity().now_unix_seconds);
                        self.deadline = Some(
                            self.watch_ready_at
                                .saturating_add(remaining.saturating_mul(1000)),
                        );
                        self.lease = Some(lease);
                        self.status = CredentialStatus::Current;
                    }
                    None => self.status = CredentialStatus::Unsupported,
                    Some(Err(_)) => {}
                }
            }
            // Wake/register the source watch after resolving as well: a
            // refresh may have invalidated its registration while awaited.
            match self.poll_watch(cx) {
                Poll::Ready(Ok(())) => {
                    self.restart(now);
                    cx.waker().wake_by_ref();
                }
                Poll::Ready(Err(())) | Poll::Pending => {}
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
    use acyclic_stream::{
        AppendRequest, IdempotencyKey, MemoryStream, StreamPath, StreamProviderExt as _,
    };
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
    fn async_only_and_raw_sources_refuse_retained_views_without_dispatching_checks() {
        use acyclic_auth::credential::{
            CredentialCacheError, CredentialError, CredentialRequest, CredentialResolution,
            CredentialSource,
        };
        use futures::channel::oneshot;
        use std::sync::atomic::AtomicUsize;

        struct Source {
            provider: CredentialProvider,
            admission: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
            checks: Arc<AtomicUsize>,
        }
        struct Request {
            lease: ResolvedCredential,
            admission: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
            checks: Arc<AtomicUsize>,
        }
        impl CredentialRequest for Request {
            fn token(&self) -> &str {
                self.lease.as_str()
            }
            fn binding(&self) -> Option<CredentialBinding> {
                self.lease.binding()
            }
            fn check(&self) -> CredentialAction<()> {
                self.checks.fetch_add(1, Ordering::SeqCst);
                let lease = self.lease.clone();
                match self.admission.lock().unwrap().take() {
                    Some(receive) => CredentialAction::Pending(Box::pin(async move {
                        receive.await.map_err(|_| {
                            CredentialError::Provider(Box::new(CredentialCacheError::Unavailable))
                        })?;
                        lease.check().await
                    })),
                    None => lease.check(),
                }
            }
            fn reject(&self) -> CredentialAction<bool> {
                self.lease.reject()
            }
        }
        impl CredentialSource for Source {
            fn resolve(&self) -> CredentialResolution<'_> {
                Box::pin(async move {
                    ResolvedCredential::from_custodian(Request {
                        lease: self.provider.resolve().await?,
                        admission: self.admission.clone(),
                        checks: self.checks.clone(),
                    })
                    .await
                })
            }
            fn watch(&self) -> CredentialAction<Option<CredentialWatch>> {
                self.provider.watch()
            }
        }

        let cache = CredentialCache::new(
            |_| async {
                CredentialSnapshot::new("credential".into(), CredentialRevision::from([1; 32]), 20)
            },
            || CredentialValidity {
                now_unix_seconds: 10,
                revision: Some(CredentialRevision::from([1; 32])),
            },
        );
        let (send, receive) = oneshot::channel();
        let checks = Arc::new(AtomicUsize::new(0));
        let provider = CredentialProvider::from_custodian(Source {
            provider: cache.into(),
            admission: Arc::new(Mutex::new(Some(receive))),
            checks: checks.clone(),
        });
        let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
        reader.credentials = Some(Credentials::new(provider, 0, 100));
        reader.reset_source();
        let ready = Arc::new(Ready(AtomicBool::new(false)));
        let waker = Waker::from(ready.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(!reader.advance_credentials(0, &mut cx));
        assert_eq!(checks.load(Ordering::SeqCst), 1);
        assert!(!reader.readable());
        assert!(reader.snapshot().is_none());
        assert!(!reader.advance_credentials(1, &mut cx));
        assert_eq!(checks.load(Ordering::SeqCst), 1);
        send.send(()).unwrap();
        assert!(ready.0.load(Ordering::SeqCst));
        assert!(reader.advance_credentials(1, &mut cx));
        assert_eq!(reader.credential_status(), CredentialStatus::Unsupported);
        let admitted_checks = checks.load(Ordering::SeqCst);
        assert_eq!(admitted_checks, 2);
        assert!(!reader.readable());
        assert!(reader.snapshot().unwrap().is_empty());
        assert!(reader.restore(b"untrusted").is_err());
        assert_eq!(reader.next_deadline(), None);
        reader.network_changed(true, 2);
        assert!(!reader.advance_credentials(2, &mut cx));
        assert_eq!(checks.load(Ordering::SeqCst), admitted_checks);

        let calls = Arc::new(AtomicUsize::new(0));
        let resolved = calls.clone();
        reader.credentials = Some(Credentials::new(
            CredentialProvider::new(move || {
                resolved.fetch_add(1, Ordering::SeqCst);
                async { Ok("credential".into()) }
            }),
            0,
            100,
        ));
        reader.reset_source();
        assert!(reader.advance_credentials(0, &mut cx));
        assert_eq!(reader.credential_status(), CredentialStatus::Unsupported);
        assert!(!reader.readable());
        assert!(reader.snapshot().unwrap().is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn awaited_registration_preserves_its_event_cut_and_original_deadline() {
        use acyclic_auth::credential::{
            CredentialCacheError, CredentialError, CredentialResolution, CredentialSource,
        };
        use futures::channel::oneshot;
        use std::sync::atomic::AtomicUsize;

        struct Source {
            provider: CredentialProvider,
            first: Mutex<Option<oneshot::Receiver<CredentialWatch>>>,
            registrations: Arc<AtomicUsize>,
            resolutions: Arc<AtomicUsize>,
        }
        impl CredentialSource for Source {
            fn resolve(&self) -> CredentialResolution<'_> {
                self.resolutions.fetch_add(1, Ordering::SeqCst);
                Box::pin(self.provider.resolve())
            }
            fn watch(&self) -> CredentialAction<Option<CredentialWatch>> {
                self.registrations.fetch_add(1, Ordering::SeqCst);
                match self.first.lock().unwrap().take() {
                    Some(receive) => CredentialAction::Pending(Box::pin(async move {
                        receive.await.map(Some).map_err(|_| {
                            CredentialError::Provider(Box::new(CredentialCacheError::Unavailable))
                        })
                    })),
                    None => self.provider.watch(),
                }
            }
        }

        // Complete registration, lose its producer, cancel at the original
        // deadline, or deliver a cut invalidated while registration awaited.
        for case in 0..5 {
            let validity = Arc::new(Mutex::new(CredentialValidity {
                now_unix_seconds: 10,
                revision: Some(CredentialRevision::from([1; 32])),
            }));
            let observed = validity.clone();
            let refreshed = validity.clone();
            let cache = CredentialCache::new(
                move |_| {
                    let revision = refreshed.lock().unwrap().revision.clone().unwrap();
                    async move { CredentialSnapshot::new("credential".into(), revision, 20) }
                },
                move || observed.lock().unwrap().clone(),
            );
            let watch = (case != 4).then(|| cache.watch().unwrap());
            let (send, receive) = oneshot::channel();
            let registrations = Arc::new(AtomicUsize::new(0));
            let resolutions = Arc::new(AtomicUsize::new(0));
            let provider = CredentialProvider::from_custodian(Source {
                provider: cache.clone().into(),
                first: Mutex::new(Some(receive)),
                registrations: registrations.clone(),
                resolutions: resolutions.clone(),
            });
            let timeout = if case == 4 {
                ReaderPolicy::default().timeout_ms
            } else {
                25
            };
            let mut state = Credentials::new(provider, 10, timeout);
            let ready = Arc::new(Ready(AtomicBool::new(false)));
            let waker = Waker::from(ready.clone());
            let mut cx = Context::from_waker(&waker);
            assert!(!state.poll(10, &mut cx));
            assert_eq!(state.status(), CredentialStatus::Pending);
            assert!(!state.access().allows());
            assert_eq!(state.deadline(), Some(10 + timeout));
            assert_eq!(resolutions.load(Ordering::SeqCst), 0);

            match case {
                4 => {
                    // The owner captures its clock after awaiting registration,
                    // with one second left on a still-valid original lease.
                    validity.lock().unwrap().now_unix_seconds = 19;
                    assert!(send.send(cache.watch().unwrap()).is_ok());
                    assert!(ready.0.load(Ordering::SeqCst));
                    assert!(state.poll(2_010, &mut cx));
                    assert_eq!(state.status(), CredentialStatus::Current);
                    assert_eq!(state.deadline(), Some(3_010));
                    assert!(!state.poll(2_010, &mut cx));
                    assert_eq!(resolutions.load(Ordering::SeqCst), 1);
                    // The wakeup estimate cannot extend exact owner custody,
                    // even when the source clock advances before that wakeup.
                    validity.lock().unwrap().now_unix_seconds = 20;
                    assert!(!state.access().allows());
                    assert_eq!(state.status(), CredentialStatus::Unavailable);
                }
                1 => {
                    drop(send);
                    assert!(ready.0.load(Ordering::SeqCst));
                    assert!(state.poll(11, &mut cx));
                    assert_eq!(state.status(), CredentialStatus::SourceLost);
                    state.retry(12);
                    assert_eq!(state.status(), CredentialStatus::SourceLost);
                }
                2 => {
                    assert!(state.poll(35, &mut cx));
                    assert!(send.is_canceled());
                    assert_eq!(state.status(), CredentialStatus::Unavailable);
                    assert_eq!(state.deadline(), None);
                    assert!(!state.poll(1_000, &mut cx));
                    assert_eq!(resolutions.load(Ordering::SeqCst), 0);
                }
                _ => {
                    if case == 3 {
                        validity.lock().unwrap().revision = Some(CredentialRevision::from([2; 32]));
                        cache.invalidate().unwrap();
                    }
                    assert!(send.send(watch.unwrap()).is_ok());
                    assert!(ready.0.load(Ordering::SeqCst));
                    assert!(state.poll(34, &mut cx));
                    if case == 3 {
                        assert_eq!(state.status(), CredentialStatus::Pending);
                        assert_eq!(resolutions.load(Ordering::SeqCst), 0);
                        assert!(state.poll(34, &mut cx));
                    }
                    assert_eq!(state.status(), CredentialStatus::Current);
                    assert_eq!(resolutions.load(Ordering::SeqCst), 1);
                }
            }
            assert_eq!(
                registrations.load(Ordering::SeqCst),
                if case == 3 { 2 } else { 1 }
            );
        }
    }

    #[test]
    fn lost_producer_ends_pending_and_admitted_custody_without_rearming() {
        use acyclic_auth::credential::{
            CredentialCacheError, CredentialError, CredentialResolution, CredentialSource,
        };
        use std::sync::atomic::AtomicUsize;

        struct Source {
            provider: CredentialProvider,
            watch: Mutex<Option<CredentialWatch>>,
            registrations: Arc<AtomicUsize>,
        }
        impl CredentialSource for Source {
            fn resolve(&self) -> CredentialResolution<'_> {
                Box::pin(self.provider.resolve())
            }
            fn watch(&self) -> CredentialAction<Option<CredentialWatch>> {
                self.registrations.fetch_add(1, Ordering::SeqCst);
                self.watch
                    .lock()
                    .unwrap()
                    .take()
                    .map(Some)
                    .ok_or(CredentialError::Invalid)
                    .into()
            }
        }

        futures::executor::block_on(async {
            let stream = MemoryStream::default();
            let path = StreamPath::new("source/lost").unwrap();
            stream
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![b"committed".as_slice().into()],
                    if_tail: None,
                    idempotency_key: IdempotencyKey::new(b"lost source".to_vec()).unwrap(),
                })
                .await
                .unwrap();
            let head = stream.head(path.clone()).await.unwrap();
            // Lose the event producer while resolution is pending, during its
            // completion, and after admission. No deadline or retry drives it.
            for loss in 0..3 {
                let (producer, receive) = futures::channel::oneshot::channel::<()>();
                let producer = Arc::new(Mutex::new(Some(producer)));
                let resolving = producer.clone();
                let validity = CredentialValidity {
                    now_unix_seconds: 10,
                    revision: Some(CredentialRevision::from([1; 32])),
                };
                let observed = validity.clone();
                let cache = CredentialCache::new(
                    move |_| {
                        let producer = resolving.clone();
                        async move {
                            if loss == 0 {
                                futures::future::pending::<()>().await;
                            }
                            if loss == 1 {
                                drop(producer.lock().unwrap().take());
                            }
                            CredentialSnapshot::new(
                                "credential".into(),
                                CredentialRevision::from([1; 32]),
                                20,
                            )
                        }
                    },
                    move || observed.clone(),
                );
                let registrations = Arc::new(AtomicUsize::new(0));
                let provider = CredentialProvider::from_custodian(Source {
                    provider: cache.into(),
                    watch: Mutex::new(Some(CredentialWatch::from_custodian(
                        validity,
                        async move { receive.await.map_err(|_| CredentialCacheError::Unavailable) },
                    ))),
                    registrations: registrations.clone(),
                });
                let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
                reader.credentials = Some(Credentials::new(provider, 0, 10_000));
                reader.reset_source();
                let ready = Arc::new(Ready(AtomicBool::new(false)));
                let waker = Waker::from(ready.clone());
                let mut cx = Context::from_waker(&waker);
                reader.advance_credentials(0, &mut cx);
                let mut late = None;
                if loss == 2 {
                    assert_eq!(reader.credential_status(), CredentialStatus::Current);
                    reader.plan([(
                        Want::Last {
                            stream: path.clone(),
                            count: 1,
                        },
                        Priority::Visible,
                    )]);
                    late = Some(reader.poll(0).pop().unwrap().id());
                }
                if loss != 1 {
                    ready.0.store(false, Ordering::SeqCst);
                    drop(producer.lock().unwrap().take());
                    assert!(ready.0.load(Ordering::SeqCst), "producer drop did not wake");
                    assert!(reader.advance_credentials(0, &mut cx));
                }
                assert_eq!(reader.credential_status(), CredentialStatus::SourceLost);
                assert!(!reader.readable());
                assert_eq!(reader.residency().requests, 0);
                assert_eq!(reader.next_deadline(), None);
                if let Some(id) = late {
                    reader.complete(id, Outcome::Head(head.clone()), 0);
                }
                assert!(reader.cache().record(&path, 0).is_none());
                assert!(reader.snapshot().unwrap().is_empty());
                reader.network_changed(true, 0);
                assert!(!reader.advance_credentials(0, &mut cx));
                reader.credentials.as_mut().unwrap().retry(0);
                assert_eq!(reader.credential_status(), CredentialStatus::SourceLost);
                assert_eq!(registrations.load(Ordering::SeqCst), 1);
            }
        });
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
            let snapshot = reader.snapshot().unwrap();
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
            assert!(reader.snapshot().unwrap().is_empty());
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
            assert!(!old_lease.reject().await.unwrap());
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
                let snapshot = reader.snapshot().unwrap();
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
