//! Public remote issuance projects the same certificate claims and result.
use super::*;

/// Versioned public request; carries no private key or bearer credential.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = request)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct IssuanceRequest {
    /// Exactly 1; recovery retains this version and all original fields.
    #[wire(tag = 1, uint32)]
    pub version: u32,
    /// Original 32-byte operation key, scoped to issuer and account.
    #[wire(tag = 2, bytes)]
    pub operation_key: Vec<u8>,
    /// Exact original 32-byte account root public key.
    #[wire(tag = 3, bytes)]
    pub issuer: Vec<u8>,
    /// Original authenticated environment.
    #[wire(tag = 4, string)]
    pub environment: String,
    /// Existing canonical public claims, including complete acting principal.
    #[wire(tag = 5, message)]
    pub claims: CertifiedKey,
}

fn request(value: &IssuanceRequest) -> Result<(), Error> {
    version(value.version)?;
    if value.operation_key.len() != 32
        || value.environment.is_empty()
        || value.claims.parent_key_id.is_some()
        || crate::crypto::public_key(&value.issuer).is_err()
        || crate::crypto::public_key(&value.claims.public_key).is_err()
    {
        return Err(Error::Invalid);
    }
    Ok(())
}

fn version(value: u32) -> Result<(), Error> {
    if value == 1 {
        Ok(())
    } else {
        Err(Error::UnsupportedVersion(value))
    }
}

/// Definitive no-commit reason, never a transport status or absent record.
#[acyclic_contract_derive::enumeration(error = Error, unknown = Error::UnknownEnum)]
#[derive(TS)]
pub enum IssuanceRefusal {
    /// Invalid unspecified reason.
    Unspecified = 0,
    /// Current original admission or owner policy refused.
    Denied = 1,
    /// Original operation key is bound to different request bytes.
    Conflict = 2,
    /// Original root, account or epoch changed.
    IssuerChanged = 3,
}

/// Retained definitive no-commit decision.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = refusal)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct IssuanceRejected {
    /// A specified refusal from the original authenticated serving owner.
    #[wire(tag = 1, enumeration)]
    pub reason: IssuanceRefusal,
}

fn refusal(value: &IssuanceRejected) -> Result<(), Error> {
    if value.reason == IssuanceRefusal::Unspecified {
        Err(Error::Invalid)
    } else {
        Ok(())
    }
}

/// No terminal decision can be established; absence does not prove refusal.
#[acyclic_contract_derive::message(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct IssuanceUnknown {}

/// The original serving transaction's outcome; acceptance is public ancestry.
#[acyclic_contract_derive::oneof(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, TS)]
pub enum IssuanceDecision {
    /// Retained exact public certificates.
    #[wire(tag = 3, message)]
    Accepted(CertificateChain),
    /// Definitive retained no-commit decision.
    #[wire(tag = 4, message)]
    Refused(IssuanceRejected),
    /// Missing record or another uncertain outcome.
    #[wire(tag = 5, message)]
    Unknown(IssuanceUnknown),
}

/// Authenticated reply; does not itself prove transport admission or durability.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = reply)]
#[derive(Clone, Debug, PartialEq, TS)]
pub struct IssuanceReply {
    /// Exactly 1.
    #[wire(tag = 1, uint32)]
    pub version: u32,
    /// Exact 32-byte request commitment retained by the original transaction.
    #[wire(tag = 2, bytes)]
    pub request_digest: Vec<u8>,
    /// Exactly one original decision.
    #[wire(oneof = "3, 4, 5")]
    pub decision: IssuanceDecision,
}

fn reply(value: &IssuanceReply) -> Result<(), Error> {
    version(value.version)?;
    if value.request_digest.len() == 32 {
        Ok(())
    } else {
        Err(Error::Invalid)
    }
}
