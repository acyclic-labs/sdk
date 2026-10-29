//! Opt-in cache: validity comes from the source custodian, never decoded claims.
use super::{CredentialError, Shared, valid_credential};
use futures::future::{AbortHandle, Abortable, FutureExt, Shared as SharedFuture};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
use zeroize::Zeroizing;

mod slot;
use super::{CredentialWatch, watch::Changes};
use slot::Slot;

/// Source identity/revision, partitioned by account, audience and auth context.
/// The source must change this when external credentials or authority change.
/// It is freshness metadata, not an authenticated policy revision or permission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialRevision {
    /// Fixed-width source revision supplied by a native custodian.
    Bytes([u8; 32]),
    /// Opaque UTF-16 revision, including unpaired surrogate code units, retained
    /// without hashing, truncation, token decoding or normalization. Distinct
    /// from byte revisions; adapters must preserve the original code units.
    Text(Shared<[u16]>),
}

impl From<[u8; 32]> for CredentialRevision {
    fn from(value: [u8; 32]) -> Self {
        Self::Bytes(value)
    }
}

impl From<String> for CredentialRevision {
    fn from(value: String) -> Self {
        Self::from(value.encode_utf16().collect::<Vec<_>>())
    }
}

impl From<Vec<u16>> for CredentialRevision {
    fn from(value: Vec<u16>) -> Self {
        Self::Text(value.into())
    }
}

/// One trusted source/clock observation. Observation must do no blocking IO;
/// source watchers publish revisions before changed credentials may be reused.
#[derive(Clone, Debug)]
pub struct CredentialValidity {
    /// Trusted current Unix seconds; expiry uses a half-open interval.
    pub now_unix_seconds: u64,
    /// None means source state is unavailable: refuse, without fallback.
    pub revision: Option<CredentialRevision>,
}

/// A bounded credential and its custodian-supplied validity. Debug redacts bytes.
pub struct CredentialSnapshot {
    token: Zeroizing<String>,
    revision: CredentialRevision,
    expires_at: u64,
}

impl std::fmt::Debug for CredentialSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialSnapshot")
            .field("revision", &self.revision)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

impl CredentialSnapshot {
    /// Use only expiry/revision authenticated or owned by the source custodian.
    /// Credentials without known validity should use a raw CredentialProvider.
    pub fn new(
        token: String,
        revision: CredentialRevision,
        expires_at: u64,
    ) -> Result<Self, CredentialError> {
        let token = Zeroizing::new(token);
        if !valid_credential(&token) {
            return Err(CredentialError::Invalid);
        }
        Ok(Self {
            token,
            revision,
            expires_at,
        })
    }

    /// Borrow secret bytes only for the admitted transport request.
    pub fn token(&self) -> &str {
        &self.token
    }
    /// The trusted source revision at publication.
    pub fn revision(&self) -> CredentialRevision {
        self.revision.clone()
    }
    /// Absolute half-open expiry, never a new TTL on a cache hit.
    pub fn expires_at(&self) -> u64 {
        self.expires_at
    }
}

/// Immutable snapshot ownership; native Arc, browser Rc. Dropping the last
/// lease scrubs its token. A lease does not extend its absolute expiry.
pub type CredentialLease = Shared<CredentialSnapshot>;

/// Typed refresh/validity failures. No failure returns an older credential.
#[derive(Clone, Debug, thiserror::Error)]
pub enum CredentialCacheError {
    /// Original source diagnostic, shared by the refresh cohort.
    #[error("credential refresh failed")]
    Provider(#[source] Arc<CredentialError>),
    /// Source freshness cannot be established, or internal state is poisoned.
    #[error("credential source unavailable")]
    Unavailable,
    /// Source changed or the exact rejected snapshot was invalidated.
    #[error("credential source changed during refresh")]
    Stale,
    /// Refresh returned a credential at or beyond its absolute expiry.
    #[error("credential expired")]
    Expired,
    /// A refresh attempted to return the bearer already rejected by the server.
    #[error("credential was rejected by server")]
    Rejected,
}

type ResultLease = Result<CredentialLease, CredentialCacheError>;
#[cfg(not(target_arch = "wasm32"))]
type RefreshFuture = Pin<Box<dyn Future<Output = ResultLease> + Send>>;
#[cfg(target_arch = "wasm32")]
type RefreshFuture = Pin<Box<dyn Future<Output = ResultLease>>>;
#[cfg(not(target_arch = "wasm32"))]
type Refresh = dyn Fn(Option<CredentialLease>) -> RefreshFuture + Send + Sync;
#[cfg(target_arch = "wasm32")]
type Refresh = dyn Fn(Option<CredentialLease>) -> RefreshFuture;
#[cfg(not(target_arch = "wasm32"))]
type Observe = dyn Fn() -> CredentialValidity + Send + Sync;
#[cfg(target_arch = "wasm32")]
type Observe = dyn Fn() -> CredentialValidity;

struct Flight {
    id: Shared<()>,
    future: SharedFuture<RefreshFuture>,
    abort: AbortHandle,
    waiters: usize,
    published: bool,
}
struct Inner {
    current: Slot,
    rejected: Slot,
    flight: Mutex<Option<Flight>>,
    refresh: Shared<Refresh>,
    observe: Shared<Observe>,
    changes: Shared<Changes>,
}

/// One credential source/auth partition, shared by all its product requests.
/// Native warm reads use an atomic snapshot, without the refresh mutex. Refresh
/// is polled by its callers, with no detached task, timer, retry or fallback.
#[derive(Clone)]
pub struct CredentialCache(Shared<Inner>);

impl CredentialCache {
    /// Source callback and nonblocking trusted validity observation, captured
    /// once. Refresh receives the rejected lease after a server rejection; it
    /// must replace that bearer. Callers retain their original deadline and
    /// cancellation around resolve and check expiry at the transport boundary.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new<F, Fut, O>(refresh: F, observe: O) -> Self
    where
        F: Fn(Option<CredentialLease>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<CredentialSnapshot, CredentialError>> + Send + 'static,
        O: Fn() -> CredentialValidity + Send + Sync + 'static,
    {
        Self::from_callbacks(
            Shared::new(move |rejected| {
                let future = refresh(rejected);
                Box::pin(async move {
                    future
                        .await
                        .map(Shared::new)
                        .map_err(|error| CredentialCacheError::Provider(Arc::new(error)))
                })
            }),
            Shared::new(observe),
        )
    }

    /// Browser counterpart; JavaScript futures and callbacks need not be Send.
    #[cfg(target_arch = "wasm32")]
    pub fn new<F, Fut, O>(refresh: F, observe: O) -> Self
    where
        F: Fn(Option<CredentialLease>) -> Fut + 'static,
        Fut: Future<Output = Result<CredentialSnapshot, CredentialError>> + 'static,
        O: Fn() -> CredentialValidity + 'static,
    {
        Self::from_callbacks(
            Shared::new(move |rejected| {
                let future = refresh(rejected);
                Box::pin(async move {
                    future
                        .await
                        .map(Shared::new)
                        .map_err(|error| CredentialCacheError::Provider(Arc::new(error)))
                })
            }),
            Shared::new(observe),
        )
    }

    fn from_callbacks(refresh: Shared<Refresh>, observe: Shared<Observe>) -> Self {
        Self(Shared::new(Inner {
            current: Slot::default(),
            rejected: Slot::default(),
            flight: Mutex::new(None),
            refresh,
            observe,
            changes: Shared::new(Changes::default()),
        }))
    }

    /// Resolve one current snapshot; failed/expired/stale refreshes refuse.
    pub async fn resolve(&self) -> ResultLease {
        let validity = (self.0.observe)();
        validity
            .revision
            .as_ref()
            .ok_or(CredentialCacheError::Unavailable)?;
        if let Some(snapshot) = self.0.current.load() {
            if valid(&snapshot, &validity).is_ok() && self.0.current.matches(&snapshot) {
                return Ok(snapshot);
            }
        }
        let waiter = match self.waiter(validity)? {
            Resolution::Ready(snapshot) => return Ok(snapshot),
            Resolution::Pending(waiter) => waiter,
        };
        let result = waiter.future.clone().await;
        let validity = (self.0.observe)();
        let mut state = self
            .0
            .flight
            .lock()
            .map_err(|_| CredentialCacheError::Unavailable)?;
        let flight = state
            .as_mut()
            .filter(|flight| Shared::ptr_eq(&flight.id, &waiter.id))
            .ok_or(CredentialCacheError::Stale)?;
        if let Ok(snapshot) = &result {
            valid(snapshot, &validity)?;
            if self
                .0
                .rejected
                .load()
                .is_some_and(|rejected| rejected.token() == snapshot.token())
            {
                return Err(CredentialCacheError::Rejected);
            }
            if !flight.published {
                self.0.current.store(Some(snapshot.clone()));
                self.0.rejected.store(None);
                flight.published = true;
            }
        }
        drop(state);
        result
    }

    fn waiter(&self, validity: CredentialValidity) -> Result<Resolution, CredentialCacheError> {
        // Declared before the guard so a retired flight drops outside the lock.
        let mut _retired = None;
        let mut state = self
            .0
            .flight
            .lock()
            .map_err(|_| CredentialCacheError::Unavailable)?;
        // A preceding refresh may have published between the warm miss and lock.
        if let Some(snapshot) = self
            .0
            .current
            .load()
            .filter(|snapshot| valid(snapshot, &validity).is_ok())
        {
            return Ok(Resolution::Ready(snapshot));
        }
        // A published flight whose snapshot is no longer valid must not be
        // joined: a paused waiter can keep it stored past expiry. Its waiters
        // keep their own future clone; new callers start a fresh refresh.
        if state.as_ref().is_some_and(|flight| flight.published) {
            _retired = state.take();
        }
        if state.is_none() {
            let refresh = self.0.refresh.clone();
            let rejected = self.0.rejected.load();
            let (abort, registration) = AbortHandle::new_pair();
            // Invoke the source only when polled, outside the state mutex.
            let future: RefreshFuture = Box::pin(async move {
                Abortable::new(async move { refresh(rejected).await }, registration)
                    .await
                    .unwrap_or(Err(CredentialCacheError::Stale))
            });
            *state = Some(Flight {
                id: Shared::new(()),
                future: future.shared(),
                abort,
                waiters: 0,
                published: false,
            });
        }
        let flight = state.as_mut().ok_or(CredentialCacheError::Unavailable)?;
        flight.waiters += 1;
        Ok(Resolution::Pending(Waiter {
            inner: self.0.clone(),
            id: flight.id.clone(),
            future: flight.future.clone(),
        }))
    }

    /// Invalidate only the exact snapshot rejected by a server. A late 401 for
    /// an older request cannot discard a replacement credential or its refresh.
    pub fn reject(&self, rejected: &CredentialLease) -> Result<bool, CredentialCacheError> {
        let flight = {
            let mut state = self
                .0
                .flight
                .lock()
                .map_err(|_| CredentialCacheError::Unavailable)?;
            if !self.0.current.matches(rejected) {
                return Ok(false);
            }
            self.0.current.store(None);
            self.0.rejected.store(Some(rejected.clone()));
            if state.as_ref().is_some_and(|flight| flight.published) {
                state.take()
            } else {
                None
            }
        };
        if let Some(flight) = flight {
            flight.abort.abort();
        }
        self.0.changes.notify()?;
        Ok(true)
    }

    /// Recheck the exact lease at dispatch after channel acquisition or queuing.
    /// This does not extend its expiry or observe the source through blocking IO.
    pub fn check(&self, lease: &CredentialLease) -> Result<(), CredentialCacheError> {
        valid(lease, &(self.0.observe)())?;
        if !self.0.current.matches(lease) {
            return Err(CredentialCacheError::Stale);
        }
        Ok(())
    }

    /// Register for source invalidation or exact server rejection. Source
    /// custodians must publish changed/unavailable validity before invalidate.
    pub fn watch(&self) -> Result<CredentialWatch, CredentialCacheError> {
        self.0.changes.watch(|| (self.0.observe)())
    }

    /// Custodian push invalidation (external CLI/account/policy/audience change).
    /// Revision observation must also change or become unavailable before reuse.
    pub fn invalidate(&self) -> Result<(), CredentialCacheError> {
        let flight = {
            let mut state = self
                .0
                .flight
                .lock()
                .map_err(|_| CredentialCacheError::Unavailable)?;
            self.0.current.store(None);
            self.0.rejected.store(None);
            state.take()
        };
        if let Some(flight) = flight {
            flight.abort.abort();
        }
        self.0.changes.notify()?;
        Ok(())
    }
}

fn valid(
    snapshot: &CredentialSnapshot,
    validity: &CredentialValidity,
) -> Result<(), CredentialCacheError> {
    let revision = validity
        .revision
        .as_ref()
        .ok_or(CredentialCacheError::Unavailable)?;
    if revision != &snapshot.revision {
        return Err(CredentialCacheError::Stale);
    }
    if validity.now_unix_seconds >= snapshot.expires_at {
        return Err(CredentialCacheError::Expired);
    }
    Ok(())
}

enum Resolution {
    Ready(CredentialLease),
    Pending(Waiter),
}

struct Waiter {
    inner: Shared<Inner>,
    id: Shared<()>,
    future: SharedFuture<RefreshFuture>,
}
impl Drop for Waiter {
    fn drop(&mut self) {
        let retained = if let Ok(mut state) = self.inner.flight.lock() {
            if let Some(flight) = state
                .as_mut()
                .filter(|flight| Shared::ptr_eq(&flight.id, &self.id))
            {
                flight.waiters -= 1;
                if flight.waiters == 0 {
                    // Release the retained shared future. The last caller's
                    // future then drops the source IO without a detached task.
                    state.take()
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        // Source futures can own arbitrary destructors; release outside lock.
        drop(retained);
    }
}
