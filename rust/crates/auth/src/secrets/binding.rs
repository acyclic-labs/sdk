//! Protected operations on explicitly admitted secret selections. This host
//! port is never installed in guest env; only a named verification handle is.
#[cfg(not(target_arch = "wasm32"))]
use super::{Resolver, SecretRef};
#[cfg(not(target_arch = "wasm32"))]
use std::collections::BTreeMap;
use std::{future::Future, pin::Pin};
use ts_rs::TS;

/// The one host-call shape, also projected into TypeScript by Auth generation.
/// Bodies and signatures are raw bytes (no hex, prefix, text decoding or JSON
/// normalization). The only supported algorithm is exactly `HMAC-SHA256`.
#[derive(Clone, TS)]
#[ts(rename_all = "camelCase")]
pub struct VerifyHmacRequest {
    /// An environment name, never an arbitrary secret reference.
    pub secret_binding: String,
    /// Unknown/mismatched algorithms return false without resolving a secret.
    pub alg: String,
    /// Exact incoming message bytes.
    #[ts(type = "Uint8Array")]
    pub body: Vec<u8>,
    /// Full 32-byte MAC; truncation and wrong lengths return false.
    #[ts(type = "Uint8Array")]
    pub signature: Vec<u8>,
}

/// Fixed diagnostics: no resolver source chain or secret bytes are returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BindingError {
    /// The invocation did not select this binding.
    #[error("unknown secret binding")]
    UnknownBinding,
    /// Ambiguous environment names are refused before guest entry.
    #[error("duplicate secret binding")]
    DuplicateBinding,
    /// Includes missing, revoked, stale-version and unavailable secrets.
    #[error("secret binding unavailable")]
    Unavailable,
}

/// Host-owned asynchronous result. Values never include a MAC or a key.
pub type VerifyFuture<'a> = Pin<Box<dyn Future<Output = Result<bool, BindingError>> + Send + 'a>>;

/// Shared protected binding contract for local and Cloud host adapters.
pub trait SecretBindingHost: Send + Sync {
    /// Verify under this invocation's selected reference and authority.
    fn verify_hmac(&self, request: VerifyHmacRequest) -> VerifyFuture<'_>;
}

/// Native host implementation. The caller supplies only selections already
/// admitted for the executing invocation and its original scoped resolver.
/// Selection lookup is O(log n); a call resolves exactly one immutable version.
#[cfg(not(target_arch = "wasm32"))]
pub struct LocalSecretBindings<R> {
    selected: BTreeMap<String, SecretRef>,
    resolver: R,
}

#[cfg(not(target_arch = "wasm32"))]
impl<R: Resolver> LocalSecretBindings<R> {
    /// No resolution or secret copies occur while installing the selections.
    pub fn new(
        selected: impl IntoIterator<Item = (String, SecretRef)>,
        resolver: R,
    ) -> Result<Self, BindingError> {
        let mut bindings = BTreeMap::new();
        for (name, reference) in selected {
            if bindings.insert(name, reference).is_some() {
                return Err(BindingError::DuplicateBinding);
            }
        }
        Ok(Self {
            selected: bindings,
            resolver,
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<R: Resolver> SecretBindingHost for LocalSecretBindings<R> {
    fn verify_hmac(&self, request: VerifyHmacRequest) -> VerifyFuture<'_> {
        use hmac::{Hmac, KeyInit, Mac};
        use sha2::Sha256;
        Box::pin(async move {
            let reference = self
                .selected
                .get(&request.secret_binding)
                .ok_or(BindingError::UnknownBinding)?;
            if request.alg != "HMAC-SHA256" || request.signature.len() != 32 {
                return Ok(false);
            }
            let secret = self
                .resolver
                .resolve(reference)
                .await
                .map_err(|_| BindingError::Unavailable)?;
            let mut mac = Hmac::<Sha256>::new_from_slice(secret.expose())
                .map_err(|_| BindingError::Unavailable)?;
            mac.update(&request.body);
            // RustCrypto verifies equal-length tags in constant time. No
            // computed MAC is returned, even when verification fails.
            Ok(mac.verify_slice(&request.signature).is_ok())
        })
    }
}
