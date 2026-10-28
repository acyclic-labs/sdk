//! Credential callbacks and validity-aware snapshots shared by product transports.
mod cache;
mod watch;
pub use cache::{
    CredentialCache, CredentialCacheError, CredentialLease, CredentialRevision, CredentialSnapshot,
    CredentialValidity,
};
use std::{future::Future, pin::Pin};
pub use watch::CredentialWatch;
use zeroize::Zeroizing;

#[cfg(target_arch = "wasm32")]
use std::rc::Rc as Shared;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc as Shared;

/// Non-secret source partition and absolute lifetime of an admitted lease.
/// This is freshness metadata, never an authenticated permission or policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CredentialBinding {
    /// Custodian-supplied account/audience/auth-context revision.
    pub revision: CredentialRevision,
    /// Half-open absolute expiry; reading this does not extend the lease.
    pub expires_at_unix_seconds: u64,
}

/// Maximum UTF-8 byte length of an authorization credential.
pub const MAX_CREDENTIAL_BYTES: usize = 12 * 1024;

/// A failed callback or a value that cannot be sent as a credential.
#[derive(Debug, thiserror::Error)]
pub enum CredentialError {
    /// Provider failure, with its original diagnostic available as the source.
    #[error("credential provider failed")]
    Provider(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Empty, oversized or header-delimited credentials fail before transport.
    #[error("invalid credential")]
    Invalid,
}

/// One native renewal invocation, cancelled by dropping its future.
#[cfg(not(target_arch = "wasm32"))]
pub type CredentialFuture = Pin<Box<dyn Future<Output = Result<String, CredentialError>> + Send>>;
/// One browser renewal invocation, cancelled by dropping its future.
#[cfg(target_arch = "wasm32")]
pub type CredentialFuture = Pin<Box<dyn Future<Output = Result<String, CredentialError>>>>;

#[cfg(not(target_arch = "wasm32"))]
type Callback = dyn Fn() -> CredentialFuture + Send + Sync;
#[cfg(target_arch = "wasm32")]
type Callback = dyn Fn() -> CredentialFuture;

/// Immutable callback configuration. Raw callbacks resolve afresh. Convert a
/// validity-aware `CredentialCache` into this type to share trusted snapshots.
#[derive(Clone)]
pub struct CredentialProvider(Source);

#[derive(Clone)]
enum Source {
    Raw(Shared<Callback>),
    Cached(CredentialCache),
}

/// Exact credential custody for one transport attempt. Retain through the reply
/// and reject only on authenticated credential rejection (401/UNAUTHENTICATED),
/// never on resource policy denial, transport failure or caller cancellation.
#[derive(Clone)]
pub struct ResolvedCredential(Resolved);

#[derive(Clone)]
enum Resolved {
    Raw(Zeroizing<String>),
    Cached {
        cache: CredentialCache,
        lease: CredentialLease,
    },
}

impl std::fmt::Debug for ResolvedCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ResolvedCredential(..)")
    }
}

impl ResolvedCredential {
    /// Project trusted source metadata without exposing or decoding bearer bytes.
    /// Raw callbacks have no trusted validity and return None. Retain this exact
    /// request and call check before dispatch or accepting its result.
    pub fn binding(&self) -> Option<CredentialBinding> {
        match &self.0 {
            Resolved::Raw(_) => None,
            Resolved::Cached { lease, .. } => Some(CredentialBinding {
                revision: lease.revision(),
                expires_at_unix_seconds: lease.expires_at(),
            }),
        }
    }

    /// Borrow only for the admitted request.
    pub fn as_str(&self) -> &str {
        match &self.0 {
            Resolved::Raw(token) => token,
            Resolved::Cached { lease, .. } => lease.token(),
        }
    }

    /// Revalidate after transport acquisition, immediately before dispatch.
    pub fn check(&self) -> Result<(), CredentialError> {
        match &self.0 {
            Resolved::Raw(_) => Ok(()),
            Resolved::Cached { cache, lease } => cache.check(lease).map_err(cache_error),
        }
    }

    /// A late rejection cannot discard a newer request's snapshot.
    pub fn reject(&self) -> Result<bool, CredentialError> {
        match &self.0 {
            Resolved::Raw(_) => Ok(false),
            Resolved::Cached { cache, lease } => cache.reject(lease).map_err(cache_error),
        }
    }
}

impl std::ops::Deref for ResolvedCredential {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

fn cache_error(error: CredentialCacheError) -> CredentialError {
    CredentialError::Provider(Box::new(error))
}

impl std::fmt::Debug for CredentialProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CredentialProvider(..)")
    }
}

impl From<CredentialCache> for CredentialProvider {
    fn from(cache: CredentialCache) -> Self {
        Self(Source::Cached(cache))
    }
}

impl CredentialProvider {
    /// Subscribe before resolving or accepting cached reader state, then check
    /// the admitted request's binding and validity. Custodians must publish a
    /// changed/unavailable revision and call CredentialCache::invalidate on
    /// source events. Raw callbacks return None, never a fabricated identity.
    pub fn watch(&self) -> Result<Option<CredentialWatch>, CredentialError> {
        match &self.0 {
            Source::Raw(_) => Ok(None),
            Source::Cached(cache) => cache.watch().map(Some).map_err(cache_error),
        }
    }

    /// Capture a provider once, invoking it afresh for every request.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new<F, Fut>(callback: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, CredentialError>> + Send + 'static,
    {
        Self(Source::Raw(Shared::new(move || -> CredentialFuture {
            Box::pin(callback())
        })))
    }

    /// Capture a browser provider without requiring JavaScript futures to be Send.
    #[cfg(target_arch = "wasm32")]
    pub fn new<F, Fut>(callback: F) -> Self
    where
        F: Fn() -> Fut + 'static,
        Fut: Future<Output = Result<String, CredentialError>> + 'static,
    {
        Self(Source::Raw(Shared::new(move || -> CredentialFuture {
            Box::pin(callback())
        })))
    }

    /// Resolve and validate before sending. Errors propagate without fallback;
    /// the returned owned buffer is scrubbed when the request finishes using it.
    pub async fn resolve(&self) -> Result<ResolvedCredential, CredentialError> {
        match &self.0 {
            Source::Raw(callback) => {
                let token = Zeroizing::new(callback().await?);
                if !valid_credential(&token) {
                    return Err(CredentialError::Invalid);
                }
                Ok(ResolvedCredential(Resolved::Raw(token)))
            }
            Source::Cached(cache) => {
                let lease = cache.resolve().await.map_err(cache_error)?;
                Ok(ResolvedCredential(Resolved::Cached {
                    cache: cache.clone(),
                    lease,
                }))
            }
        }
    }
}

/// Bounded opaque credential admission, shared with static-token constructors.
pub fn valid_credential(value: &str) -> bool {
    value.len() <= MAX_CREDENTIAL_BYTES
        && !value.trim().is_empty()
        && !value.contains(['\r', '\n', '\0'])
}
