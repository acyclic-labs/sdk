//! Lifecycle effects retain protected immutable dispatch and context records.
use super::*;
use crate::OperationId;

pub(crate) fn phase_operation(tool: OperationId, event: HookEvent) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness:tool-hook:v1\0");
    hasher.update(&tool.into_bytes());
    hasher.update(event.name().as_bytes());
    let mut id = [0; 16];
    id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(id)
}

pub(crate) fn approval_identity(manifest: [u8; 32]) -> crate::registry::ComponentIdentity {
    crate::registry::ComponentIdentity {
        name: "hooks.pre-tool".into(),
        version: "1".into(),
        digest: manifest,
    }
}
