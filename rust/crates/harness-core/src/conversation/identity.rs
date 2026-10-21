//! Stable turn publication identities shared by writers and streaming clients.
use crate::OperationId;

/// Canonical assistant message for an admitted turn, including after replay.
pub fn assistant_message_id(turn: OperationId) -> uuid::Uuid {
    uuid::Uuid::from_bytes(derived_operation_id(turn, b"conversation-assistant").into_bytes())
}

pub(crate) fn derived_operation_id(turn: OperationId, domain: &[u8]) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(&turn.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}
