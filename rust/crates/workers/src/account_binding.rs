//! Browser marshalling for the sole shared-runtime account holder codec.
use acyclic_native_runtime::account::{self, AccountHolderError, Clock};
use wasm_bindgen::prelude::*;

struct BrowserClock;
impl Clock for BrowserClock {
    fn now_ms(&self) -> Result<u64, AccountHolderError> {
        let now = js_sys::Date::now();
        if !now.is_finite() || now < 0.0 || now.fract() != 0.0 || now > 9_007_199_254_740_991.0 {
            return Err(AccountHolderError::Expired);
        }
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, reason = "Date milliseconds were admitted as a finite exact nonnegative safe integer")]
        Ok(now as u64)
    }
}
fn error(value: AccountHolderError) -> JsValue { js_sys::Error::new(&value.to_string()).into() }
fn disposed() -> JsValue { js_sys::Error::new("account preparation disposed").into() }

/// Non-authorizing metadata from the caller's own signed public certificates.
#[wasm_bindgen]
pub struct AccountHolderMetadata { inner: account::HolderMetadata }
#[wasm_bindgen]
impl AccountHolderMetadata {
    /// Birth environment.
    #[wasm_bindgen(getter)]
    pub fn environment(&self) -> String { self.inner.environment().into() }
    /// Own certificate account.
    #[wasm_bindgen(getter, js_name = accountId)]
    pub fn account_id(&self) -> String { self.inner.account_id().into() }
    /// Certified own leaf identifier.
    #[wasm_bindgen(getter, js_name = keyId)]
    pub fn key_id(&self) -> String { self.inner.key_id().into() }
    /// Canonical public key.
    #[wasm_bindgen(getter, js_name = publicKey)]
    pub fn public_key(&self) -> String { self.inner.public_key().into() }
    /// Exact certificate expiry, without narrowing.
    #[wasm_bindgen(getter, js_name = certificateExpiresAtUnixMillis)]
    pub fn certificate_expires_at_unix_millis(&self) -> u64 { self.inner.certificate_expires_at_unix_millis() }
}
/// Opaque transport credential and the expiry computed by the same Rust mint.
#[wasm_bindgen]
pub struct AccountIssuedCredential { inner: account::IssuedCredential }
#[wasm_bindgen]
impl AccountIssuedCredential {
    /// Sensitive own-leaf bearer.
    #[wasm_bindgen(getter)]
    pub fn bearer(&self) -> String { self.inner.bearer().into() }
    /// Actual signed expiry.
    #[wasm_bindgen(getter, js_name = expiresAtUnixMillis)]
    pub fn expires_at_unix_millis(&self) -> u64 { self.inner.expires_at_unix_millis() }
    /// Containing certificate expiry.
    #[wasm_bindgen(getter, js_name = certificateExpiresAtUnixMillis)]
    pub fn certificate_expires_at_unix_millis(&self) -> u64 { self.inner.certificate_expires_at_unix_millis() }
}
/// Rust-owned prepared account bytes; no private key enters this object.
#[wasm_bindgen]
pub struct AccountPreparedBearer { inner: Option<account::PreparedAccountBearer> }
impl AccountPreparedBearer {
    fn prepared(&self) -> Result<&account::PreparedAccountBearer, JsValue> { self.inner.as_ref().ok_or_else(disposed) }
}
#[wasm_bindgen]
impl AccountPreparedBearer {
    /// Exact canonical bytes for the matching nonextractable platform leaf.
    #[wasm_bindgen(getter, js_name = signingBytes)]
    pub fn signing_bytes(&self) -> Result<Vec<u8>, JsValue> { Ok(self.prepared()?.signing_bytes().to_vec()) }
    /// Bound own account.
    #[wasm_bindgen(getter, js_name = accountId)]
    pub fn account_id(&self) -> Result<String, JsValue> { Ok(self.prepared()?.metadata().account_id().into()) }
    /// Bound certificate key identifier.
    #[wasm_bindgen(getter, js_name = keyId)]
    pub fn key_id(&self) -> Result<String, JsValue> { Ok(self.prepared()?.metadata().key_id().into()) }
    /// Bound canonical public key.
    #[wasm_bindgen(getter, js_name = publicKey)]
    pub fn public_key(&self) -> Result<String, JsValue> { Ok(self.prepared()?.metadata().public_key().into()) }
    /// Actual prepared expiry.
    #[wasm_bindgen(getter, js_name = expiresAtUnixMillis)]
    pub fn expires_at_unix_millis(&self) -> Result<u64, JsValue> { Ok(self.prepared()?.expires_at_unix_millis()) }
    /// Containing certificate expiry.
    #[wasm_bindgen(getter, js_name = certificateExpiresAtUnixMillis)]
    pub fn certificate_expires_at_unix_millis(&self) -> Result<u64, JsValue> { Ok(self.prepared()?.metadata().certificate_expires_at_unix_millis()) }
    /// Consume preparation after actual signature and finish-time expiry checks.
    pub fn finish(&mut self, signature: &[u8]) -> Result<AccountIssuedCredential, JsValue> {
        let prepared = self.inner.take().ok_or_else(disposed)?;
        Ok(AccountIssuedCredential { inner: prepared.finish(signature, &BrowserClock).map_err(error)? })
    }
    /// Release unused preparation. Idempotent even after finish or refusal.
    pub fn dispose(&mut self) { self.inner = None; }
}
/// Canonically encode a public Ed25519 key without exposing a private key.
#[wasm_bindgen(js_name = encodeAccountPublicKey)]
pub fn encode_account_public_key(bytes: &[u8]) -> Result<String, JsValue> {
    let bytes: &[u8; 32] = bytes.try_into().map_err(|_| error(AccountHolderError::Encoding))?;
    let key = ed25519_dalek::VerifyingKey::from_bytes(bytes).map_err(|_| error(AccountHolderError::Encoding))?;
    if key.is_weak() { return Err(error(AccountHolderError::Signature)); }
    Ok(account::encode_key(&key))
}
/// Inspect own-leaf binding even when its certificate has expired.
#[wasm_bindgen(js_name = inspectAccountHolder)]
pub fn inspect_account_holder(public_key: &str, birth: &str, certificate: &str) -> Result<AccountHolderMetadata, JsValue> {
    let key = account::public_key(public_key).map_err(error)?;
    Ok(AccountHolderMetadata { inner: account::inspect_holder(&key, birth, certificate).map_err(error)? })
}
/// Prepare canonical bytes under the actual browser clock and own certificate.
#[wasm_bindgen(js_name = prepareAccountBearer)]
pub fn prepare_account_bearer(public_key: &str, birth: &str, certificate: &str, credential_id: &str, lifetime_seconds: u64) -> Result<AccountPreparedBearer, JsValue> {
    let key = account::public_key(public_key).map_err(error)?;
    Ok(AccountPreparedBearer { inner: Some(account::prepare(&key, birth, certificate, credential_id, &BrowserClock, lifetime_seconds).map_err(error)?) })
}
