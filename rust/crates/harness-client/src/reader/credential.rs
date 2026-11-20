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
    timeout_ms: Option<u64>,
    deadline: Option<u64>,
    status: CredentialStatus,
}

impl Credentials {
    pub(super) fn new(provider: CredentialProvider, now: u64, timeout_ms: Option<u64>) -> Self {
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
        self.deadline = self.timeout_ms.map(|timeout| now.saturating_add(timeout));
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
