//! Request-scoped credential callbacks shared by product transports.
use std::{future::Future, pin::Pin};
use zeroize::Zeroizing;

#[cfg(target_arch = "wasm32")]
use std::rc::Rc as Shared;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc as Shared;

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

/// Immutable callback configuration. Each resolution owns its result; no token
/// cache, synchronization protocol or background task is introduced here.
#[derive(Clone)]
pub struct CredentialProvider(Shared<Callback>);

impl std::fmt::Debug for CredentialProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CredentialProvider(..)")
    }
}

impl CredentialProvider {
    /// Capture a provider once, invoking it afresh for every request.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new<F, Fut>(callback: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, CredentialError>> + Send + 'static,
    {
        Self(Shared::new(move || -> CredentialFuture {
            Box::pin(callback())
        }))
    }

    /// Capture a browser provider without requiring JavaScript futures to be Send.
    #[cfg(target_arch = "wasm32")]
    pub fn new<F, Fut>(callback: F) -> Self
    where
        F: Fn() -> Fut + 'static,
        Fut: Future<Output = Result<String, CredentialError>> + 'static,
    {
        Self(Shared::new(move || -> CredentialFuture {
            Box::pin(callback())
        }))
    }

    /// Resolve and validate before sending. Errors propagate without fallback;
    /// the returned owned buffer is scrubbed when the request finishes using it.
    pub async fn resolve(&self) -> Result<Zeroizing<String>, CredentialError> {
        let token = Zeroizing::new((self.0)().await?);
        if !valid_credential(&token) {
            return Err(CredentialError::Invalid);
        }
        Ok(token)
    }
}

/// Bounded opaque credential admission, shared with static-token constructors.
pub fn valid_credential(value: &str) -> bool {
    value.len() <= MAX_CREDENTIAL_BYTES
        && !value.trim().is_empty()
        && !value.contains(['\r', '\n', '\0'])
}
