//! Adapters retain an existing custodian; they never introduce another cache.
use super::{
    CredentialAction, CredentialBinding, CredentialError, CredentialWatch, ResolvedCredential,
};
use std::{future::Future, pin::Pin};

/// Native custodians can cross transport threads; browser custodians stay local.
#[cfg(not(target_arch = "wasm32"))]
pub trait CustodianPlatform: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync + ?Sized> CustodianPlatform for T {}
/// Browser custodians may retain JavaScript request objects.
#[cfg(target_arch = "wasm32")]
pub trait CustodianPlatform {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> CustodianPlatform for T {}

/// Caller-owned source resolution. Dropping it cancels only this interest.
#[cfg(not(target_arch = "wasm32"))]
pub type CredentialResolution<'a> =
    Pin<Box<dyn Future<Output = Result<ResolvedCredential, CredentialError>> + Send + 'a>>;
/// Browser resolution need not be Send.
#[cfg(target_arch = "wasm32")]
pub type CredentialResolution<'a> =
    Pin<Box<dyn Future<Output = Result<ResolvedCredential, CredentialError>> + 'a>>;

/// An existing credential owner, including one across a language boundary.
/// Forward its resolution and registration directly: no refresh, timer, cache,
/// token decoding or authority identity may be added by an adapter.
pub trait CredentialSource: CustodianPlatform + 'static {
    /// Resolve with the caller's original deadline/cancellation surrounding it.
    fn resolve(&self) -> CredentialResolution<'_>;
    /// Register before resolution or cache lookup. None means no trusted source
    /// identity; a raw callback must not acquire one through conversion.
    fn watch(&self) -> CredentialAction<Option<CredentialWatch>>;
}

/// Exact request custody retained by its original owner. An adapter must hold
/// the original request object and forward these operations to that same object.
/// Clone shares custody; it does not resolve again or extend absolute expiry.
pub trait CredentialRequest: CustodianPlatform + 'static {
    /// Borrow only for this admitted transport attempt.
    fn token(&self) -> &str;
    /// Lossless custodian-supplied partition/expiry, absent for raw callbacks.
    /// Binding is immutable and must describe this exact request's check.
    fn binding(&self) -> Option<CredentialBinding>;
    /// Recheck source, account, audience and absolute expiry before admission.
    fn check(&self) -> CredentialAction<()>;
    /// Observe retained-data access synchronously, without dispatching work.
    /// Some means this original custodian can locally check this exact request's
    /// source changes, rejection and original absolute expiry at the instant of
    /// access. None explicitly leaves synchronous retained views unsupported.
    /// Implementations must never queue foreign work or reuse an earlier check.
    /// Transport dispatch and reply admission still require `check`.
    fn retained_access(&self) -> Option<Result<(), CredentialError>> {
        None
    }
    /// Reject only this exact snapshot on authenticated remote rejection.
    fn reject(&self) -> CredentialAction<bool>;
}
