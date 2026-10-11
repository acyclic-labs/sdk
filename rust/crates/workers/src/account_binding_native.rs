//! Native marshalling for the same shared-runtime account holder codec.
use acyclic_native_runtime::account::{self, AccountHolderError, Clock};
use napi::bindgen_prelude::{BigInt, Uint8Array};
use napi::{Error, Result, Status};
use napi_derive::napi;

pub(crate) struct NativeClock;
impl Clock for NativeClock {
    fn now_ms(&self) -> std::result::Result<u64, AccountHolderError> {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_err(|_| AccountHolderError::Expired)?;
        u64::try_from(now.as_millis()).map_err(|_| AccountHolderError::Expired)
    }
}
pub(crate) fn error(value: impl std::fmt::Display) -> Error { Error::new(Status::InvalidArg, value.to_string()) }
pub(crate) fn lifetime(value: &BigInt) -> Result<u64> {
    let (negative, value, lossless) = value.get_u64();
    if negative || !lossless { return Err(error(AccountHolderError::Scope)); }
    Ok(value)
}
fn disposed() -> Error { error("account preparation disposed") }
/// Non-authorizing metadata from the caller's own signed public certificates.
#[napi]
pub struct AccountHolderMetadata { inner: account::HolderMetadata }
#[napi]
impl AccountHolderMetadata {
    /// Birth environment.
    #[napi(getter)]
    pub fn environment(&self) -> String { self.inner.environment().into() }
    /// Own certificate account.
    #[napi(getter)]
    pub fn account_id(&self) -> String { self.inner.account_id().into() }
    /// Certified own leaf identifier.
    #[napi(getter)]
    pub fn key_id(&self) -> String { self.inner.key_id().into() }
    /// Canonical public key.
    #[napi(getter)]
    pub fn public_key(&self) -> String { self.inner.public_key().into() }
    /// Exact certificate expiry, without narrowing.
    #[napi(getter)]
    pub fn certificate_expires_at_unix_millis(&self) -> BigInt { self.inner.certificate_expires_at_unix_millis().into() }
}
/// Opaque transport credential and the expiry computed by the same Rust mint.
#[napi]
pub struct AccountIssuedCredential { inner: account::IssuedCredential }
impl AccountIssuedCredential {
    pub(crate) fn from_inner(inner: account::IssuedCredential) -> Self { Self { inner } }
}
#[napi]
impl AccountIssuedCredential {
    /// Sensitive own-leaf bearer.
    #[napi(getter)]
    pub fn bearer(&self) -> String { self.inner.bearer().into() }
    /// Actual signed expiry.
    #[napi(getter)]
    pub fn expires_at_unix_millis(&self) -> BigInt { self.inner.expires_at_unix_millis().into() }
    /// Containing certificate expiry.
    #[napi(getter)]
    pub fn certificate_expires_at_unix_millis(&self) -> BigInt { self.inner.certificate_expires_at_unix_millis().into() }
}
/// Rust-owned prepared account bytes; no private key enters this object.
#[napi]
pub struct AccountPreparedBearer { inner: Option<account::PreparedAccountBearer> }
impl AccountPreparedBearer {
    fn prepared(&self) -> Result<&account::PreparedAccountBearer> { self.inner.as_ref().ok_or_else(disposed) }
}
#[napi]
impl AccountPreparedBearer {
    /// Exact canonical bytes for the matching nonextractable platform leaf.
    #[napi(getter)]
    pub fn signing_bytes(&self) -> Result<Uint8Array> { Ok(self.prepared()?.signing_bytes().to_vec().into()) }
    /// Bound own account.
    #[napi(getter)]
    pub fn account_id(&self) -> Result<String> { Ok(self.prepared()?.metadata().account_id().into()) }
    /// Bound certificate key identifier.
    #[napi(getter)]
    pub fn key_id(&self) -> Result<String> { Ok(self.prepared()?.metadata().key_id().into()) }
    /// Bound canonical public key.
    #[napi(getter)]
    pub fn public_key(&self) -> Result<String> { Ok(self.prepared()?.metadata().public_key().into()) }
    /// Actual prepared expiry.
    #[napi(getter)]
    pub fn expires_at_unix_millis(&self) -> Result<BigInt> { Ok(self.prepared()?.expires_at_unix_millis().into()) }
    /// Containing certificate expiry.
    #[napi(getter)]
    pub fn certificate_expires_at_unix_millis(&self) -> Result<BigInt> { Ok(self.prepared()?.metadata().certificate_expires_at_unix_millis().into()) }
    /// Consume preparation after actual signature and finish-time expiry checks.
    #[napi]
    pub fn finish(&mut self, signature: Uint8Array) -> Result<AccountIssuedCredential> {
        let prepared = self.inner.take().ok_or_else(disposed)?;
        Ok(AccountIssuedCredential { inner: prepared.finish(signature.as_ref(), &NativeClock).map_err(error)? })
    }
    /// Release unused preparation. Idempotent even after finish or refusal.
    #[napi]
    pub fn dispose(&mut self) { self.inner = None; }
}
/// Canonically encode a public Ed25519 key without exposing a private key.
#[napi]
pub fn encode_account_public_key(bytes: Uint8Array) -> Result<String> {
    let bytes: &[u8; 32] = bytes.as_ref().try_into().map_err(|_| error(AccountHolderError::Encoding))?;
    let key = ed25519_dalek::VerifyingKey::from_bytes(bytes).map_err(|_| error(AccountHolderError::Encoding))?;
    if key.is_weak() { return Err(error(AccountHolderError::Signature)); }
    Ok(account::encode_key(&key))
}
/// Inspect own-leaf binding even when its certificate has expired.
#[napi]
pub fn inspect_account_holder(public_key: String, birth: String, certificate: String) -> Result<AccountHolderMetadata> {
    let key = account::public_key(&public_key).map_err(error)?;
    Ok(AccountHolderMetadata { inner: account::inspect_holder(&key, &birth, &certificate).map_err(error)? })
}
/// Prepare canonical bytes under the actual host clock and own certificate.
#[napi]
pub fn prepare_account_bearer(public_key: String, birth: String, certificate: String, credential_id: String, lifetime_seconds: BigInt) -> Result<AccountPreparedBearer> {
    let key = account::public_key(&public_key).map_err(error)?;
    Ok(AccountPreparedBearer { inner: Some(account::prepare(&key, &birth, &certificate, &credential_id, &NativeClock, lifetime(&lifetime_seconds)?).map_err(error)?) })
}
