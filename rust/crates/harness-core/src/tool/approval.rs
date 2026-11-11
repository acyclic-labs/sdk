use super::*;

/// Trusted journal check for a resolved approval bound to one exact tool definition.
pub trait ToolApprovalVerifier: acyclic_stream::ProviderPlatform {
    /// Rejects absent, declined, mismatched, or indeterminate approvals.
    fn verify<'a>(
        &'a self,
        interaction_id: InteractionId,
        operation_id: OperationId,
        definition_digest: [u8; 32],
    ) -> BoxFuture<'a, Result<()>>;

    /// Verifies on the caller's original native ledger, including the future.
    /// Ordinary-only verifiers cannot authorize a native publication.
    #[cfg(not(target_arch = "wasm32"))]
    fn verify_with_native_work<'a>(
        &'a self,
        _interaction_id: InteractionId,
        _operation_id: OperationId,
        _definition_digest: [u8; 32],
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeFutureBundle<'a, (), crate::Error>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "approval verifier requires original native work support",
        })
    }
}
