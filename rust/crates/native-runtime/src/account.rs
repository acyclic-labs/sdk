//! Customer-held account bearers. Signed certificates remain opaque public data;
//! only the server verifies their signatures, current keyring and permissions.
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// Maximum certified-key lifetime, in seconds.
pub const MAX_KEY_LIFETIME_SECS: u64 = 30 * 24 * 60 * 60;
/// Maximum own-leaf bearer lifetime, in seconds.
pub const MAX_BEARER_LIFETIME_SECS: u64 = 60 * 60;
const MAX_BEARER: usize = 12_288;
const TOKEN_TYPE: &str = "acyclic-account+jwt";

/// Holder failures never include signed credentials or private key material.
#[derive(Debug, thiserror::Error)]
pub enum AccountHolderError {
    /// Malformed or noncanonical signed data.
    #[error("invalid account holder encoding")]
    Encoding,
    /// Metadata does not belong to this leaf or account.
    #[error("invalid account holder scope")]
    Scope,
    /// Certificate or requested bearer window is invalid or expired.
    #[error("expired account holder window")]
    Expired,
    /// Public key or externally supplied signature is invalid.
    #[error("invalid account holder signature")]
    Signature,
}
/// Actual admission clock supplied by the hosting native or browser binding.
pub trait Clock {
    /// Return whole Unix milliseconds.
    fn now_ms(&self) -> Result<u64, AccountHolderError>;
}
/// Canonical account JWS header, shared with the server consumer.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountBearerHeader<S = String> {
    /// Signature algorithm.
    pub alg: S,
    /// Account bearer token type.
    pub typ: S,
    /// Certified leaf identifier.
    pub kid: S,
}
/// Canonical account bearer claims, preserving signed field order.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountBearerClaims<E = String, T = u64, S = String> {
    /// Wire version.
    pub version: u32,
    /// Birth-certified environment.
    pub environment: E,
    /// Certificate account.
    pub account: S,
    /// Certified leaf identifier.
    pub sub: S,
    /// Original credential operation identifier.
    pub jti: S,
    /// Issuance in whole Unix seconds.
    pub iat: T,
    /// Expiry in whole Unix seconds.
    pub exp: T,
    /// Original opaque signed birth certificate.
    pub birth: S,
    /// Original opaque signed key certificate.
    pub key: S,
}
#[derive(Deserialize)]
struct BirthMetadata { environment: String, account_id: String }
#[derive(Deserialize)]
struct CertificateMetadata {
    version: u32,
    account_id: String,
    key_id: String,
    public_key: String,
    key_epoch: u64,
    not_before: u64,
    not_after: u64,
}
/// Non-authorizing own-leaf metadata; expiration does not prevent inspection.
pub struct HolderMetadata {
    environment: String,
    account_id: String,
    key_id: String,
    public_key: String,
    not_before: u64,
    certificate_expires_at_unix_millis: u64,
}
impl HolderMetadata {
    /// Birth environment, not a server authorization decision.
    pub fn environment(&self) -> &str { &self.environment }
    /// Own certificate account.
    pub fn account_id(&self) -> &str { &self.account_id }
    /// Own certified leaf identifier.
    pub fn key_id(&self) -> &str { &self.key_id }
    /// Canonical public key, base64url without padding.
    pub fn public_key(&self) -> &str { &self.public_key }
    /// Exact certificate expiry in Unix milliseconds.
    pub const fn certificate_expires_at_unix_millis(&self) -> u64 { self.certificate_expires_at_unix_millis }
}
/// Opaque bearer and expiry computed by the same canonical mint operation.
pub struct IssuedCredential {
    bearer: String,
    expires_at_unix_millis: u64,
    certificate_expires_at_unix_millis: u64,
}
impl IssuedCredential {
    /// Sensitive transport credential; never a provider or issuer key.
    pub fn bearer(&self) -> &str { &self.bearer }
    /// Exact signed bearer expiry in Unix milliseconds.
    pub const fn expires_at_unix_millis(&self) -> u64 { self.expires_at_unix_millis }
    /// Exact containing certificate expiry in Unix milliseconds.
    pub const fn certificate_expires_at_unix_millis(&self) -> u64 { self.certificate_expires_at_unix_millis }
}
/// Immutable Rust-prepared signing bytes for a nonextractable platform leaf.
pub struct PreparedAccountBearer {
    signed: String,
    key: VerifyingKey,
    metadata: HolderMetadata,
    issued_at: u64,
    expires_at_unix_millis: u64,
}
impl PreparedAccountBearer {
    /// Exact canonical bytes to sign with the matching own leaf.
    pub fn signing_bytes(&self) -> &[u8] { self.signed.as_bytes() }
    /// Bound own-leaf metadata.
    pub const fn metadata(&self) -> &HolderMetadata { &self.metadata }
    /// Exact signed expiry in Unix milliseconds.
    pub const fn expires_at_unix_millis(&self) -> u64 { self.expires_at_unix_millis }
    /// Consume prepared bytes after verifying the platform signature and clock.
    pub fn finish(self, signature: &[u8], clock: &impl Clock) -> Result<IssuedCredential, AccountHolderError> {
        self.check_window(clock)?;
        let signature = Signature::from_slice(signature).map_err(|_| AccountHolderError::Signature)?;
        self.key.verify_strict(self.signing_bytes(), &signature).map_err(|_| AccountHolderError::Signature)?;
        self.issue(&signature)
    }
    fn check_window(&self, clock: &impl Clock) -> Result<(), AccountHolderError> {
        let now = clock.now_ms()?;
        if now / 1000 < self.issued_at || now >= self.expires_at_unix_millis { return Err(AccountHolderError::Expired); }
        Ok(())
    }
    fn issue(mut self, signature: &Signature) -> Result<IssuedCredential, AccountHolderError> {
        self.signed.push('.');
        URL_SAFE_NO_PAD.encode_string(signature.to_bytes(), &mut self.signed);
        if self.signed.len() > MAX_BEARER { return Err(AccountHolderError::Encoding); }
        Ok(IssuedCredential { bearer: self.signed, expires_at_unix_millis: self.expires_at_unix_millis,
            certificate_expires_at_unix_millis: self.metadata.certificate_expires_at_unix_millis })
    }
}
/// Inspect only leaf/account binding and structural certificate time bounds.
/// This does not verify Root signatures or authorize any certificate permission.
pub fn inspect_holder(key: &VerifyingKey, birth: &str, certificate: &str) -> Result<HolderMetadata, AccountHolderError> {
    let birth: BirthMetadata = payload(birth)?;
    let certificate: CertificateMetadata = payload(certificate)?;
    let decoded = public_key(&certificate.public_key)?;
    if certificate.version != 1 || !identifier(&certificate.account_id) || !identifier(&certificate.key_id)
        || certificate.key_epoch == 0 || decoded != *key || birth.account_id != certificate.account_id {
        return Err(AccountHolderError::Scope);
    }
    if certificate.not_after <= certificate.not_before || certificate.not_after - certificate.not_before > MAX_KEY_LIFETIME_SECS
        || certificate.not_before.checked_mul(1000).is_none() { return Err(AccountHolderError::Expired); }
    let expires = certificate.not_after.checked_mul(1000).ok_or(AccountHolderError::Expired)?;
    Ok(HolderMetadata { environment: birth.environment, account_id: certificate.account_id, key_id: certificate.key_id,
        public_key: certificate.public_key, not_before: certificate.not_before, certificate_expires_at_unix_millis: expires })
}
/// Prepare the one canonical own-leaf account bearer without exporting a secret.
pub fn prepare(key: &VerifyingKey, birth: &str, certificate: &str, credential_id: &str,
    clock: &impl Clock, lifetime_secs: u64) -> Result<PreparedAccountBearer, AccountHolderError> {
    let metadata = inspect_holder(key, birth, certificate)?;
    if !text(credential_id, 256) || lifetime_secs == 0 || lifetime_secs > MAX_BEARER_LIFETIME_SECS { return Err(AccountHolderError::Scope); }
    let now = clock.now_ms()? / 1000;
    let expires = now.checked_add(lifetime_secs).ok_or(AccountHolderError::Expired)?
        .min(metadata.certificate_expires_at_unix_millis / 1000);
    if expires <= now || now < metadata.not_before { return Err(AccountHolderError::Expired); }
    let header = AccountBearerHeader { alg: "EdDSA", typ: TOKEN_TYPE, kid: metadata.key_id.as_str() };
    let claims = AccountBearerClaims { version: 1, environment: metadata.environment.as_str(), account: metadata.account_id.as_str(),
        sub: metadata.key_id.as_str(), jti: credential_id, iat: now, exp: expires, birth, key: certificate };
    let signed = format!("{}.{}", encode(&header)?, encode(&claims)?);
    if signed.len().saturating_add(87) > MAX_BEARER { return Err(AccountHolderError::Encoding); }
    Ok(PreparedAccountBearer { signed, key: *key, metadata, issued_at: now, expires_at_unix_millis: expires * 1000 })
}
/// Mint with the customer's certified leaf using the same canonical preparation.
pub fn mint(key: &SigningKey, birth: &str, certificate: &str, credential_id: &str,
    clock: &impl Clock, lifetime_secs: u64) -> Result<IssuedCredential, AccountHolderError> {
    let prepared = prepare(&key.verifying_key(), birth, certificate, credential_id, clock, lifetime_secs)?;
    let signature = key.sign(prepared.signing_bytes());
    prepared.check_window(clock)?;
    prepared.issue(&signature)
}
/// Decode a canonical, nonweak own-leaf public key.
pub fn public_key(encoded: &str) -> Result<VerifyingKey, AccountHolderError> {
    let bytes: [u8; 32] = canonical(encoded)?.try_into().map_err(|_| AccountHolderError::Encoding)?;
    let key = VerifyingKey::from_bytes(&bytes).map_err(|_| AccountHolderError::Encoding)?;
    if key.is_weak() { return Err(AccountHolderError::Signature); }
    Ok(key)
}
/// Encode a public key without padding; this never exposes a private seed.
pub fn encode_key(key: &VerifyingKey) -> String { URL_SAFE_NO_PAD.encode(key.as_bytes()) }
fn payload<T: DeserializeOwned>(compact: &str) -> Result<T, AccountHolderError> {
    if compact.len() > MAX_BEARER { return Err(AccountHolderError::Encoding); }
    let (body, _) = compact.split_once('.').ok_or(AccountHolderError::Encoding)?;
    serde_json::from_slice(&canonical(body)?).map_err(|_| AccountHolderError::Encoding)
}
fn canonical(value: &str) -> Result<Vec<u8>, AccountHolderError> {
    let bytes = URL_SAFE_NO_PAD.decode(value).map_err(|_| AccountHolderError::Encoding)?;
    if URL_SAFE_NO_PAD.encode(&bytes) != value { return Err(AccountHolderError::Encoding); }
    Ok(bytes)
}
fn encode(value: &impl Serialize) -> Result<String, AccountHolderError> {
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(value).map_err(|_| AccountHolderError::Encoding)?))
}
fn text(value: &str, limit: usize) -> bool { !value.is_empty() && value.len() <= limit && !value.chars().any(char::is_control) }
fn identifier(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && value.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.'))
}
