use super::*;

/// Acting identity at the declaring block: last lineage entry, otherwise root id.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct SubjectId {}

/// Ancestor-or-self identities through the declaring block, frozen on append.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct SubjectLineage {}

/// Read reference frozen at its declaring block.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct SubjectReference {
    /// Exactly one structured source.
    #[wire(oneof = "1, 2")]
    pub kind: SubjectReferenceKind,
}
/// Structured read subject alternatives.
#[acyclic_contract_derive::oneof(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub enum SubjectReferenceKind {
    /// Acting identity.
    #[wire(tag = 1, message)]
    Id(SubjectId),
    /// Ancestor or self.
    #[wire(tag = 2, message)]
    Lineage(SubjectLineage),
}

/// Offline identity delegation.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::lineage)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct LineageAppend {
    /// Append-only acting identities; cannot assert policy claims.
    #[wire(tag = 1, string)]
    pub ids: Vec<String>,
}

/// Certificate validity; no millisecond interpretation.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::validity)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct Validity {
    /// Inclusive Unix seconds, UTC.
    #[wire(tag = 1, uint64)]
    pub not_before_unix_seconds: u64,
    /// Exclusive Unix seconds, UTC; at most 30 days after not_before.
    #[wire(tag = 2, uint64)]
    pub expires_at_unix_seconds: u64,
}

/// Public account birth claims.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::birth)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct Birth {
    /// Account identity.
    #[wire(tag = 1, string)]
    pub account_id: String,
    /// Trust environment.
    #[wire(tag = 2, string)]
    pub environment: String,
    /// 32-byte account genesis key.
    #[wire(tag = 3, bytes)]
    pub genesis_public_key: Vec<u8>,
    /// Unix seconds, UTC.
    #[wire(tag = 4, uint64)]
    pub issued_at_unix_seconds: u64,
}

/// Environment-signed public certificate.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::birth_certificate)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct BirthCertificate {
    /// Public claims signed by the environment trust root.
    #[wire(tag = 1, message)]
    pub birth: Birth,
    /// 64-byte Ed25519 signature over BIRTH_DOMAIN and canonical Birth.
    #[wire(tag = 2, bytes)]
    pub signature: Vec<u8>,
}

/// Public online key claims. Every ancestor remains in the presented certificate chain.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::certified_key)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct CertifiedKey {
    /// Same account as Birth.
    #[wire(tag = 1, string)]
    pub account_id: String,
    /// Unique online key identity, checked against keyring revocations.
    #[wire(tag = 2, string)]
    pub key_id: String,
    /// 32-byte Ed25519 leaf public key.
    #[wire(tag = 3, bytes)]
    pub public_key: Vec<u8>,
    /// Must meet the current keyring minimum epoch.
    #[wire(tag = 4, uint64)]
    pub key_epoch: u64,
    /// Validity bounded by every parent.
    #[wire(tag = 5, message)]
    pub validity: Validity,
    /// Issuer-attested grant ceiling.
    #[wire(tag = 6, message)]
    pub grants: GrantSet,
    /// Issuer-attested roles, groups and attributes; immutable offline.
    #[wire(tag = 7, message)]
    pub principal: Principal,
    /// Previous certificate key id; absent only for genesis-signed first key.
    #[wire(tag = 8, string)]
    pub parent_key_id: Option<String>,
    /// All parent capability block ids at mint time; check every id.
    #[wire(tag = 9, bytes)]
    pub parent_revocation_ids: Vec<Vec<u8>>,
}

/// Public certificate; cannot be presented as an account bearer.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::key_certificate)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct KeyCertificate {
    /// Public claims signed only by the trusted account issuer root, never a leaf.
    #[wire(tag = 1, message)]
    pub key: CertifiedKey,
    /// 64-byte issuer-root Ed25519 over KEY_DOMAIN and canonical CertifiedKey.
    #[wire(tag = 2, bytes)]
    pub signature: Vec<u8>,
}

/// Certificate ancestry, never account JWTs or private keys.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::certificates)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct CertificateChain {
    /// Trusted environment account birth.
    #[wire(tag = 1, message)]
    pub birth: BirthCertificate,
    /// Complete oldest-to-leaf ancestry; check every epoch, key id and parent block id.
    #[wire(tag = 2, message)]
    pub keys: Vec<KeyCertificate>,
}

/// Published block revocation.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::revocation)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct Revocation {
    /// 32-byte block transcript id; invalidates it and all descendants.
    #[wire(tag = 1, bytes)]
    pub id: Vec<u8>,
    /// Retention deadline, no earlier than the revoked authority expiry.
    #[wire(tag = 2, uint64)]
    pub expires_at_unix_seconds: u64,
    /// Audit reason.
    #[wire(tag = 3, string)]
    pub reason: String,
    /// Advisory issuer label.
    #[wire(tag = 4, string)]
    pub label: Option<String>,
}

/// Followed keyring state. Apply only after root signature, sequence and digest checks.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::keyring_snapshot)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct KeyringSnapshot {
    /// Owning account.
    #[wire(tag = 1, string)]
    pub account_id: String,
    /// Strictly consecutive per account; gaps require resnapshot before acceptance.
    #[wire(tag = 2, uint64)]
    pub sequence: u64,
    /// 32-byte digest of previous signed snapshot; zero for initial snapshot.
    #[wire(tag = 3, bytes)]
    pub previous_digest: Vec<u8>,
    /// Monotone epoch floor.
    #[wire(tag = 4, uint64)]
    pub minimum_key_epoch: u64,
    /// Cumulative revoked online key ids, including ancestors.
    #[wire(tag = 5, string)]
    pub revoked_key_ids: Vec<String>,
    /// Cumulative unexpired block revocations.
    #[wire(tag = 6, message)]
    pub revocations: Vec<Revocation>,
    /// Current account root key, 32 bytes.
    #[wire(tag = 7, bytes)]
    pub root_public_key: Vec<u8>,
    /// Unix seconds, UTC.
    #[wire(tag = 8, uint64)]
    pub issued_at_unix_seconds: u64,
}

/// Authenticated keyring publication.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::signed_snapshot)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct SignedKeyringSnapshot {
    /// Canonical published state.
    #[wire(tag = 1, message)]
    pub snapshot: KeyringSnapshot,
    /// Ed25519 account-root signature over KEYRING_DOMAIN and canonical snapshot.
    #[wire(tag = 2, bytes)]
    pub signature: Vec<u8>,
}

/// Shared audit event for hosted and local runtimes.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::audit)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct AuditRecord {
    /// Unix seconds, UTC.
    #[wire(tag = 1, uint64)]
    pub at_unix_seconds: u64,
    /// Oldest-to-tip block revocation ids.
    #[wire(tag = 2, bytes)]
    pub chain_ids: Vec<Vec<u8>>,
    /// Issuer-attested identity.
    #[wire(tag = 3, message)]
    pub subject: Principal,
    /// Complete append-only delegation lineage, last entry is acting identity.
    #[wire(tag = 4, string)]
    pub lineage: Vec<String>,
    /// Authenticated presenting account.
    #[wire(tag = 5, message)]
    pub holder: Holder,
    /// Declared semantic action.
    #[wire(tag = 6, string)]
    pub action: String,
    /// Canonical resource.
    #[wire(tag = 7, string)]
    pub resource: String,
    /// SHA-256 of effective canonical filter.
    #[wire(tag = 8, bytes)]
    pub filter_hash: Vec<u8>,
    /// Typed decision or lifecycle event.
    #[wire(tag = 9, enumeration)]
    pub decision: AuditDecision,
    /// Decision reason.
    #[wire(tag = 10, string)]
    pub reason: String,
}

/// Single-valued record headers. Construction and wire admission reject every
/// repeated name, even when its values agree. All authorization consumers share
/// this validated type; callers cannot mutate its entries after admission.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::validation::attributes)]
#[derive(Clone, Debug, Default, PartialEq, TS)]
pub struct RecordAttributes {
    /// Unique header names with opaque values.
    #[wire(tag = 1, message)]
    attributes: Vec<Attribute>,
}
impl RecordAttributes {
    /// Admit decoded record headers once, before any authorization consumer.
    pub fn try_new(attributes: Vec<Attribute>) -> Result<Self, Error> {
        let value = Self { attributes };
        crate::validation::attributes(&value)?;
        Ok(value)
    }
    /// Decode canonical record headers, rejecting duplicate names and unknown fields.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        use prost::Message;
        let wire = RecordAttributesProto::decode(bytes).map_err(|_| Error::Invalid)?;
        if wire.encode_to_vec() != bytes {
            return Err(Error::Invalid);
        }
        wire.try_into()
    }
    /// Immutable admitted record headers.
    pub fn as_slice(&self) -> &[Attribute] {
        &self.attributes
    }
}

/// Stable audit decisions shared by local and hosted exports.
#[acyclic_contract_derive::enumeration(error = Error, unknown = Error::UnknownEnum)]
#[derive(TS)]
pub enum AuditDecision {
    /// Invalid unspecified audit outcome.
    Unspecified = 0,
    /// Request permitted.
    Allow = 1,
    /// Request denied.
    Deny = 2,
    /// Authority minted.
    Mint = 3,
    /// Authority revoked.
    Revoke = 4,
    /// Subscription or socket channel opened.
    ChannelOpen = 5,
}
