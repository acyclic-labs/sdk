//! Normative signature transcripts shared with the runtime's key custodian.
use crate::{BLOCK_DOMAIN, VERSION, verify::VerifyError};
use ed25519_dalek::{Signature, VerifyingKey};
use prost::Message;
use sha2::{Digest, Sha256};

/// Content-addressed block revocation identity.
pub type BlockId = [u8; 32];

/// Build the exact block signing transcript. The caller's runtime owns root signing.
pub fn block_transcript(index: u64, previous: &BlockId, payload: &[u8]) -> Vec<u8> {
    let mut message = Vec::new();
    block_message(&mut message, index, previous, payload);
    message
}

pub(crate) fn block_message(message: &mut Vec<u8>, index: u64, previous: &BlockId, payload: &[u8]) {
    message.clear();
    message.extend_from_slice(BLOCK_DOMAIN);
    message.extend_from_slice(&index.to_le_bytes());
    message.extend_from_slice(&VERSION.to_le_bytes());
    message.extend_from_slice(previous);
    message.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    message.extend_from_slice(payload);
}

/// SHA-256 of the exact signing transcript followed by its Ed25519 signature.
pub fn block_id(transcript: &[u8], signature: &[u8]) -> BlockId {
    Sha256::new()
        .chain_update(transcript)
        .chain_update(signature)
        .finalize()
        .into()
}

pub(crate) fn claims_message(
    message: &mut Vec<u8>,
    domain: &[u8],
    claims: &impl Message,
) -> Result<(), VerifyError> {
    message.clear();
    message.extend_from_slice(domain);
    claims.encode(message).map_err(|_| VerifyError::Malformed)
}

pub(crate) fn public_key(bytes: &[u8]) -> Result<VerifyingKey, VerifyError> {
    let bytes = bytes.try_into().map_err(|_| VerifyError::Malformed)?;
    let key = VerifyingKey::from_bytes(bytes).map_err(|_| VerifyError::Signature)?;
    if key.is_weak() {
        return Err(VerifyError::Signature);
    }
    Ok(key)
}

pub(crate) fn verify_signature(
    key: &VerifyingKey,
    message: &[u8],
    signature: &[u8],
) -> Result<(), VerifyError> {
    let signature = Signature::from_slice(signature).map_err(|_| VerifyError::Signature)?;
    key.verify_strict(message, &signature)
        .map_err(|_| VerifyError::Signature)
}
