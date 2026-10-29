use super::*;

/// Exact principal or recipient identity, independent of the workload account.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = identity)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct CallerIdentity {
    /// Opaque account bytes, preserved without normalization.
    #[wire(tag = 1, bytes)]
    pub account: Vec<u8>,
    /// Authority-issued namespace bytes.
    #[wire(tag = 2, bytes)]
    pub ns: Vec<u8>,
    /// Authority-issued principal or recipient id bytes.
    #[wire(tag = 3, bytes)]
    pub id: Vec<u8>,
}

/// One certificate's exact revocation dependency, including its admitted epoch.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = key)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct CallerKeyRevocation {
    /// Account-scoped signing key identity; never public-key material.
    #[wire(tag = 1, string)]
    pub key_id: String,
    /// Epoch checked against the account's current explicit revoke-all floor.
    #[wire(tag = 2, uint64)]
    pub key_epoch: u64,
}

/// Public revocation dependencies within this authority's principal account.
/// A snapshot is descriptive: only the host's current trust owner admits use.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = revocation)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct CallerRevocation {
    /// Every certificate ancestor, oldest first.
    #[wire(tag = 1, message)]
    pub keys: Vec<CallerKeyRevocation>,
    /// Every capability and certificate-parent block revocation id, 32 bytes each.
    #[wire(tag = 2, bytes)]
    pub blocks: Vec<Vec<u8>>,
}

/// Independently verified original customer or forwarding platform authority.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = authority)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct CallerAuthority {
    /// The authority's own account, namespace and id.
    #[wire(tag = 1, message)]
    pub principal: CallerIdentity,
    /// Exclusive expiry, UTC Unix seconds.
    #[wire(tag = 2, uint64)]
    pub expires_at_unix_seconds: u64,
    /// All dependencies rechecked by the host before privileged use.
    #[wire(tag = 3, message)]
    pub revocation: CallerRevocation,
}

/// Host-admitted invocation context for both direct and delegated fetch/RPC.
/// Decoding checks representation only, never authentication or delegation.
/// Hosts bind the entire context to the authenticated dispatch and retain the
/// verified capabilities outside guest memory; guest headers cannot supply it.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = context)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct CallerContext {
    /// Exactly 1.
    #[wire(tag = 1, uint32)]
    pub version: u32,
    /// Original customer; never replaced by the workload or forwarding account.
    #[wire(tag = 2, message)]
    pub caller: CallerAuthority,
    /// Exact recipient account/namespace/id, independently of the caller account.
    #[wire(tag = 3, message)]
    pub audience: CallerIdentity,
    /// Forwarding authorities, first platform delegate to immediate presenter.
    /// Empty for direct calls. Each hop needs an authenticated delegation binding;
    /// unrelated valid credentials cannot establish acting-for authority.
    #[wire(tag = 4, message)]
    pub delegation: Vec<CallerAuthority>,
    /// Minimum of the original caller and every forwarding authority's expiry.
    #[wire(tag = 5, uint64)]
    pub expires_at_unix_seconds: u64,
    /// Opaque invocation-scoped host capability, not a portable account bearer.
    /// The host checks current trust, exact target/owner policy, audience,
    /// expiry and invocation on use. Closing its owner ends pending uses.
    #[wire(tag = 6, bytes)]
    pub capability_handle: Vec<u8>,
}

fn identity(value: &CallerIdentity) -> Result<(), Error> {
    if value.account.is_empty() || value.ns.is_empty() || value.id.is_empty() {
        return Err(Error::Invalid);
    }
    Ok(())
}
fn key(value: &CallerKeyRevocation) -> Result<(), Error> {
    if value.key_id.is_empty() {
        return Err(Error::Invalid);
    }
    Ok(())
}
fn revocation(value: &CallerRevocation) -> Result<(), Error> {
    if value.keys.is_empty()
        || value.blocks.is_empty()
        || value
            .blocks
            .iter()
            .any(|block| block.len() != crate::REVOCATION_ID_BYTES)
    {
        return Err(Error::Invalid);
    }
    Ok(())
}
fn authority(value: &CallerAuthority) -> Result<(), Error> {
    if value.expires_at_unix_seconds == 0
        || value.expires_at_unix_seconds > crate::validation::MAX_UNIX_SECONDS
    {
        return Err(Error::Invalid);
    }
    Ok(())
}
fn context(value: &CallerContext) -> Result<(), Error> {
    if value.version != 1 {
        return Err(Error::UnsupportedVersion(value.version));
    }
    let expiry = value
        .delegation
        .iter()
        .fold(value.caller.expires_at_unix_seconds, |expiry, authority| {
            expiry.min(authority.expires_at_unix_seconds)
        });
    if value.capability_handle.is_empty() || value.expires_at_unix_seconds != expiry {
        return Err(Error::Invalid);
    }
    Ok(())
}
