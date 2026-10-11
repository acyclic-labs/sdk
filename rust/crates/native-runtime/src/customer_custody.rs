//! Customer-owned leaf and SQL-session custody. No private material is a binding value.
//! Signed birth/certificate data stays public and caller-owned; the OS vault holds only
//! one atomic private-key/session pair. Vault access never falls back to files or environment.

use crate::account::{self, AccountHolderError, Clock, IssuedCredential};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{SigningKey, VerifyingKey};
use sha2::{Digest, Sha256};
use std::fmt;
use zeroize::Zeroizing;

#[cfg(windows)]
#[path = "customer_custody/windows.rs"]
mod platform;
#[cfg(target_os = "macos")]
#[path = "customer_custody/apple.rs"]
mod platform;
#[cfg(target_os = "linux")]
#[path = "customer_custody/linux.rs"]
mod platform;
#[path = "customer_custody/lock.rs"]
mod lock;

const MAGIC: &[u8; 8] = b"ACYLEAF1";
const HEADER_BYTES: usize = 8 + 32 + 32 + 32 + 4;
const MAX_RECORD_BYTES: usize = 2560;
const REFERENCE_PREFIX: &str = "acyclic-customer-custody-ref-v1:";

/// Credential-free failures. Never includes secret values or platform error text.
#[derive(Debug)]
pub enum CustodyError {
    /// No entry exists (including explicit local deletion).
    NotFound,
    /// Another login replaced this handle's credential generation.
    Stale,
    /// The OS vault is locked; unlock it through the OS, not an SDK fallback.
    Locked,
    /// The OS vault, random source, or namespace serialization is unavailable.
    Unavailable,
    /// Native status code without potentially sensitive diagnostic text.
    Platform(i64),
    /// The namespace has invalid or unbounded identifying data.
    InvalidNamespace,
    /// The nonsecret generation-fencing receipt is not bounded canonical public data.
    InvalidReference,
    /// The distinct opaque SQL session is empty or contains control characters.
    InvalidSession,
    /// A complete pair exceeds the actual Windows Credential Manager capacity.
    TooLarge,
    /// The vault item has an invalid local custody format or namespace.
    Corrupt,
    /// More than one OS entry matches; no arbitrary entry is selected.
    Ambiguous,
    /// Namespace lock metadata is not private and owned by this OS user.
    UnsafeLock,
    /// Public certificate data does not bind to this namespace.
    Binding,
    /// Canonical holder validation or signing failed.
    Holder(AccountHolderError),
}

impl fmt::Display for CustodyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Platform(code) => write!(f, "customer custody OS failure ({code})"),
            _ => f.write_str(match self {
                Self::NotFound => "customer credential not found",
                Self::Stale => "customer credential replaced",
                Self::Locked => "customer credential vault locked",
                Self::Unavailable => "customer credential custody unavailable",
                Self::InvalidNamespace => "invalid customer credential namespace",
                Self::InvalidReference => "invalid customer custody reference",
                Self::InvalidSession => "invalid SQL login session",
                Self::TooLarge => "customer credential exceeds OS vault capacity",
                Self::Corrupt => "invalid customer credential vault item",
                Self::Ambiguous => "ambiguous customer credential vault items",
                Self::UnsafeLock => "unsafe customer custody lock",
                Self::Binding => "customer certificate namespace mismatch",
                Self::Holder(_) => "customer holder validation failed",
                Self::Platform(_) => unreachable!(),
            }),
        }
    }
}

impl std::error::Error for CustodyError {}

/// Exact configured login origin/environment/account/certificate-key namespace.
/// The caller must supply its canonical transport origin, not a redirect or alias.
#[derive(Clone)]
pub struct CustomerCustodyNamespace {
    name: String,
    digest: [u8; 32],
    origin: String,
    environment: String,
    account_id: String,
    credential_id: String,
}

impl CustomerCustodyNamespace {
    /// Uses length-prefixed SHA-256 fields, so separators and Unicode cannot collide.
    pub fn new(origin: &str, environment: &str, account_id: &str, credential_id: &str) -> Result<Self, CustodyError> {
        let fields = [origin, environment, account_id, credential_id];
        if fields.iter().any(|value| value.is_empty() || value.len() > 4096 || value.chars().any(char::is_control)) {
            return Err(CustodyError::InvalidNamespace);
        }
        let mut hash = Sha256::new();
        hash.update(b"acyclic-customer-leaf-custody-v1\0");
        for value in fields {
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
        let digest: [u8; 32] = hash.finalize().into();
        let mut name = String::with_capacity(64);
        use std::fmt::Write;
        for byte in digest {
            write!(name, "{byte:02x}").map_err(|_| CustodyError::Unavailable)?;
        }
        Ok(Self { name, digest, origin: origin.into(), environment: environment.into(), account_id: account_id.into(), credential_id: credential_id.into() })
    }

    fn bind(&self, key: &VerifyingKey, birth: &str, certificate: &str) -> Result<(), CustodyError> {
        let metadata = account::inspect_holder(key, birth, certificate).map_err(CustodyError::Holder)?;
        if metadata.account_id() != self.account_id || metadata.key_id() != self.credential_id
            || metadata.environment() != self.environment {
            return Err(CustodyError::Binding);
        }
        Ok(())
    }
}

/// Nonsecret public receipt for reopening exactly an acknowledged vault generation.
/// Contains only namespace digest, generation nonce and own public key, never a seed,
/// SQL session, authority grant, or authenticated proof about the current OS entry.
pub struct CustomerCustodyReference {
    namespace_digest: [u8; 32],
    generation: [u8; 32],
    public_key: [u8; 32],
}

impl CustomerCustodyReference {
    /// Canonical bounded public persistence format, owned by Rust rather than JS.
    pub fn encode(&self) -> String {
        let mut bytes = [0u8; 96];
        bytes[..32].copy_from_slice(&self.namespace_digest);
        bytes[32..64].copy_from_slice(&self.generation);
        bytes[64..].copy_from_slice(&self.public_key);
        let mut encoded = String::with_capacity(REFERENCE_PREFIX.len() + 128);
        encoded.push_str(REFERENCE_PREFIX);
        URL_SAFE_NO_PAD.encode_string(&bytes, &mut encoded);
        encoded
    }

    /// Requires the exact version, length and unpadded URL-safe alphabet.
    pub fn decode(encoded: &str) -> Result<Self, CustodyError> {
        let payload = encoded.strip_prefix(REFERENCE_PREFIX).ok_or(CustodyError::InvalidReference)?;
        if payload.len() != 128 {
            return Err(CustodyError::InvalidReference);
        }
        let mut bytes = [0u8; 96];
        let decoded = URL_SAFE_NO_PAD.decode_slice(payload, &mut bytes).map_err(|_| CustodyError::InvalidReference)?;
        if decoded != bytes.len() {
            return Err(CustodyError::InvalidReference);
        }
        let mut namespace_digest = [0u8; 32];
        let mut generation = [0u8; 32];
        let mut public_key = [0u8; 32];
        namespace_digest.copy_from_slice(&bytes[..32]);
        generation.copy_from_slice(&bytes[32..64]);
        public_key.copy_from_slice(&bytes[64..]);
        Ok(Self { namespace_digest, generation, public_key })
    }
}

/// A newly generated customer leaf, held only in zeroizing Rust memory until commit.
/// Not Clone, Debug, Serialize, or a raw private-key binding value.
pub struct PendingCustomerLeaf {
    key: SigningKey,
}

impl PendingCustomerLeaf {
    /// Generates the same Ed25519 signing key used by the canonical holder.
    pub fn generate() -> Result<Self, CustodyError> {
        let mut seed = Zeroizing::new([0u8; 32]);
        getrandom::fill(seed.as_mut()).map_err(|_| CustodyError::Unavailable)?;
        Ok(Self { key: SigningKey::from_bytes(&seed) })
    }

    /// The sole exportable key material.
    pub fn public_key(&self) -> [u8; 32] {
        self.key.verifying_key().to_bytes()
    }

    /// Binds this exact pending key to the returned public certificate, then atomically
    /// replaces the complete key/SQL-session pair. A failed write never deletes the old pair.
    /// Expired certificates remain restorable for SQL renewal; mint still enforces expiry.
    pub fn commit(self, namespace: CustomerCustodyNamespace, birth: &str, certificate: &str, sql_session: Zeroizing<String>) -> Result<RestoredCustomerLeaf, CustodyError> {
        namespace.bind(&self.key.verifying_key(), birth, certificate)?;
        self.store_pair(namespace, sql_session)
    }

    fn store_pair(self, namespace: CustomerCustodyNamespace, sql_session: Zeroizing<String>) -> Result<RestoredCustomerLeaf, CustodyError> {
        if sql_session.is_empty() || sql_session.chars().any(char::is_control) {
            return Err(CustodyError::InvalidSession);
        }
        let size = HEADER_BYTES.checked_add(sql_session.len()).ok_or(CustodyError::TooLarge)?;
        if size > MAX_RECORD_BYTES {
            return Err(CustodyError::TooLarge);
        }
        let mut generation = [0u8; 32];
        getrandom::fill(&mut generation).map_err(|_| CustodyError::Unavailable)?;
        let mut record = Zeroizing::new(Vec::with_capacity(size));
        record.extend_from_slice(MAGIC);
        record.extend_from_slice(&namespace.digest);
        record.extend_from_slice(&generation);
        record.extend_from_slice(self.key.as_bytes());
        record.extend_from_slice(&(sql_session.len() as u32).to_be_bytes());
        record.extend_from_slice(sql_session.as_bytes());
        let _lock = lock::NamespaceLock::acquire(&namespace.name)?;
        platform::write(&namespace.name, &record)?;
        Ok(RestoredCustomerLeaf { public_key: self.public_key(), generation, namespace })
    }
}

struct Record {
    bytes: Zeroizing<Vec<u8>>,
    key: SigningKey,
    generation: [u8; 32],
}

fn read_record(namespace: &CustomerCustodyNamespace) -> Result<Record, CustodyError> {
    let bytes = platform::read(&namespace.name)?;
    if bytes.len() < HEADER_BYTES || bytes.len() > MAX_RECORD_BYTES || &bytes[..8] != MAGIC
        || bytes[8..40] != namespace.digest {
        return Err(CustodyError::Corrupt);
    }
    let generation = bytes[40..72].try_into().map_err(|_| CustodyError::Corrupt)?;
    let seed: &[u8; 32] = bytes[72..104].try_into().map_err(|_| CustodyError::Corrupt)?;
    let key = SigningKey::from_bytes(seed);
    let length = u32::from_be_bytes(bytes[104..108].try_into().map_err(|_| CustodyError::Corrupt)?) as usize;
    if length == 0 || length != bytes.len() - HEADER_BYTES {
        return Err(CustodyError::Corrupt);
    }
    let session = std::str::from_utf8(&bytes[HEADER_BYTES..]).map_err(|_| CustodyError::Corrupt)?;
    if session.chars().any(char::is_control) {
        return Err(CustodyError::Corrupt);
    }
    Ok(Record { bytes, key, generation })
}

/// A generation-bound opaque handle. Reopening never exposes the private key or session.
/// Each use rechecks the OS vault, so deletion and replacement invalidate existing handles.
pub struct RestoredCustomerLeaf {
    namespace: CustomerCustodyNamespace,
    generation: [u8; 32],
    public_key: [u8; 32],
}
impl RestoredCustomerLeaf {
    /// Opens a stored pair without needing a currently unexpired public certificate.
    pub fn open(namespace: CustomerCustodyNamespace) -> Result<Self, CustodyError> {
        let _lock = lock::NamespaceLock::acquire(&namespace.name)?;
        let record = read_record(&namespace)?;
        Ok(Self { public_key: record.key.verifying_key().to_bytes(), generation: record.generation, namespace })
    }

    /// Reopens only the saved receipt's namespace, own public key and exact generation.
    /// A newer entry is Stale, not an opportunity to adopt its generation for cleanup.
    pub fn open_at(namespace: CustomerCustodyNamespace, reference: &CustomerCustodyReference) -> Result<Self, CustodyError> {
        if namespace.digest != reference.namespace_digest {
            return Err(CustodyError::Binding);
        }
        let _lock = lock::NamespaceLock::acquire(&namespace.name)?;
        let record = read_record(&namespace)?;
        let public_key = record.key.verifying_key().to_bytes();
        if record.generation != reference.generation || public_key != reference.public_key {
            return Err(CustodyError::Stale);
        }
        Ok(Self { namespace, generation: record.generation, public_key })
    }

    /// Receipts fence this handle's generation; they never adopt a newer vault entry.
    pub fn reference(&self) -> CustomerCustodyReference {
        CustomerCustodyReference {
            namespace_digest: self.namespace.digest,
            generation: self.generation,
            public_key: self.public_key,
        }
    }

    /// Returns only the public key, never a private seed.
    pub fn public_key(&self) -> [u8; 32] {
        self.public_key
    }

    fn current(&self) -> Result<Record, CustodyError> {
        let record = read_record(&self.namespace)?;
        if record.generation != self.generation || record.key.verifying_key().to_bytes() != self.public_key {
            return Err(CustodyError::Stale);
        }
        Ok(record)
    }

    /// Produces the actual canonical leaf bearer; signed public data is not authorization.
    pub fn mint(&self, birth: &str, certificate: &str, credential_id: &str, clock: &impl Clock, lifetime_secs: u64) -> Result<IssuedCredential, CustodyError> {
        let _lock = lock::NamespaceLock::acquire(&self.namespace.name)?;
        let record = self.current()?;
        self.namespace.bind(&record.key.verifying_key(), birth, certificate)?;
        account::mint(&record.key, birth, certificate, credential_id, clock, lifetime_secs).map_err(CustodyError::Holder)
    }

    /// Stores a newly certified identifier for the same private leaf and SQL session.
    /// Origin, environment, account and public key cannot change. The old pair is not
    /// deleted implicitly: persist the new public tuple, then explicitly delete the old
    /// handle when the namespaces differ. Failed new writes preserve the original pair.
    /// Retrying a lost acknowledgement returns an identical existing pair's actual
    /// generation without rewriting it; a different destination pair is never replaced.
    pub fn recertify(&self, new_namespace: CustomerCustodyNamespace, birth: &str, certificate: &str) -> Result<Self, CustodyError> {
        if new_namespace.origin != self.namespace.origin
            || new_namespace.environment != self.namespace.environment
            || new_namespace.account_id != self.namespace.account_id {
            return Err(CustodyError::Binding);
        }
        let (first, second) = if self.namespace.name <= new_namespace.name {
            (&self.namespace.name, &new_namespace.name)
        } else {
            (&new_namespace.name, &self.namespace.name)
        };
        let _first = lock::NamespaceLock::acquire(first)?;
        let _second = if first != second { Some(lock::NamespaceLock::acquire(second)?) } else { None };
        let mut record = self.current()?;
        new_namespace.bind(&record.key.verifying_key(), birth, certificate)?;
        if self.namespace.name == new_namespace.name {
            return Ok(Self { namespace: new_namespace, generation: record.generation, public_key: self.public_key });
        }
        match read_record(&new_namespace) {
            Ok(existing) => {
                // Ignore namespace/generation headers; compare the exact seed and SQL pair.
                if existing.bytes[72..] != record.bytes[72..] {
                    return Err(CustodyError::Stale);
                }
                return Ok(Self { namespace: new_namespace, generation: existing.generation, public_key: self.public_key });
            }
            Err(CustodyError::NotFound) => (),
            Err(error) => return Err(error),
        }
        let mut generation = [0u8; 32];
        getrandom::fill(&mut generation).map_err(|_| CustodyError::Unavailable)?;
        // Reuse the zeroizing record buffer; the private seed/session never leave Rust.
        record.bytes[8..40].copy_from_slice(&new_namespace.digest);
        record.bytes[40..72].copy_from_slice(&generation);
        platform::write(&new_namespace.name, &record.bytes)?;
        Ok(Self { namespace: new_namespace, generation, public_key: self.public_key })
    }

    /// Rust transport-only access to the distinct opaque SQL login session. Do not expose
    /// this callback through generated JS bindings or reinterpret it as a leaf bearer.
    pub fn with_sql_session<R>(&self, transport: impl FnOnce(&str) -> R) -> Result<R, CustodyError> {
        let record = {
            let _lock = lock::NamespaceLock::acquire(&self.namespace.name)?;
            self.current()?
        };
        // Admit a consistent snapshot, then release the namespace lock before caller I/O.
        let session = std::str::from_utf8(&record.bytes[HEADER_BYTES..]).map_err(|_| CustodyError::Corrupt)?;
        Ok(transport(session))
    }

    /// Explicit local custody deletion. Server revocation is a separate authenticated
    /// operation; a failed or stale deletion never removes a newer login's credential.
    /// Physical absence is idempotent, but an existing newer generation is never deleted.
    pub fn delete(&self) -> Result<(), CustodyError> {
        let _lock = lock::NamespaceLock::acquire(&self.namespace.name)?;
        match self.current() {
            Ok(_record) => match platform::delete(&self.namespace.name) {
                Ok(()) | Err(CustodyError::NotFound) => Ok(()),
                Err(error) => Err(error),
            },
            Err(CustodyError::NotFound) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
#[path = "customer_custody/smoke.rs"]
mod smoke;

#[cfg(test)]
#[path = "customer_custody/conformance.rs"]
mod conformance;
