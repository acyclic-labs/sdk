use super::*;

/// Targeted account key revocation, including every descendant of that key.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct KeyRevocation {
    /// Account-scoped identity. Issuers must never reuse key identities.
    #[wire(tag = 1, string)]
    pub key_id: String,
    /// Exclusive retirement time, covering all authority issued by this key.
    /// The issuer, not the requesting holder, supplies this bound.
    #[wire(tag = 2, uint64)]
    pub expires_at_unix_seconds: u64,
}

/// One consecutive account-root-authenticated change; size never changes its scope.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct RevocationDelta {
    /// Owning account.
    #[wire(tag = 1, string)]
    pub account_id: String,
    /// Previous sequence plus one. Exhaustion refuses; it never wraps.
    #[wire(tag = 2, uint64)]
    pub sequence: u64,
    /// Digest of the previously accepted signed publication.
    #[wire(tag = 3, bytes)]
    pub previous_digest: Vec<u8>,
    /// Monotone publication time, no later than the verifier's trusted clock.
    #[wire(tag = 4, uint64)]
    pub issued_at_unix_seconds: u64,
    /// Exactly these keys and their descendants; unrelated keys remain valid.
    #[wire(tag = 5, message)]
    pub keys: Vec<KeyRevocation>,
    /// Exactly these blocks and their descendants.
    #[wire(tag = 6, message)]
    pub blocks: Vec<Revocation>,
    /// Explicit revoke-all: reject every certificate below this epoch.
    /// Absent for targeted revocation; never inferred from counts or sizes.
    #[wire(tag = 7, uint64)]
    pub revoke_all_before_epoch: Option<u64>,
}

/// Signed replacement for a retired prefix. Persist the accepted sequence,
/// digest and retirement time atomically with this state before acknowledging it.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct RevocationCheckpoint {
    /// Owning account.
    #[wire(tag = 1, string)]
    pub account_id: String,
    /// Publication sequence, strictly newer than the retained state.
    #[wire(tag = 2, uint64)]
    pub sequence: u64,
    /// Trusted time below which this state must never authorize requests.
    #[wire(tag = 3, uint64)]
    pub retired_before_unix_seconds: u64,
    /// Cumulative explicit revoke-all floor; cannot decrease.
    #[wire(tag = 4, uint64)]
    pub minimum_key_epoch: u64,
    /// Complete unexpired targeted key revocations.
    #[wire(tag = 5, message)]
    pub keys: Vec<KeyRevocation>,
    /// Complete unexpired targeted block revocations.
    #[wire(tag = 6, message)]
    pub blocks: Vec<Revocation>,
    /// Monotone publication time, independent of the historical retirement floor.
    /// Reloading this checkpoint retains the same publication ordering.
    #[wire(tag = 7, uint64)]
    pub issued_at_unix_seconds: u64,
}

/// Canonical signed publication. Verify against an independently authenticated
/// account issuer root, never a key supplied by this publication or an online leaf.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct SignedRevocation {
    /// Exact delta or checkpoint.
    #[wire(oneof = "1, 2")]
    pub publication: RevocationPublication,
    /// Ed25519 over REVOCATION_DOMAIN followed by canonical publication bytes.
    #[wire(tag = 3, bytes)]
    pub signature: Vec<u8>,
}

/// Distinct checkpoint and incremental update signing transcripts.
#[acyclic_contract_derive::oneof(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub enum RevocationPublication {
    /// Consecutive targeted change or explicit revoke-all.
    #[wire(tag = 1, message)]
    Delta(RevocationDelta),
    /// Complete replacement with safe retirement.
    #[wire(tag = 2, message)]
    Checkpoint(RevocationCheckpoint),
}
