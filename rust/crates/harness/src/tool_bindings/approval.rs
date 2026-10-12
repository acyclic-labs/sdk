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

    /// Optional actual bounded approval replay and artifact read.
    #[cfg(feature = "filesystem")]
    fn verify_measured<'a>(
        &'a self, _interaction_id: InteractionId, _operation_id: OperationId,
        _definition_digest: [u8; 32], _budget: acyclic_fs::WorkBudget,
        _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        Box::pin(async { Err(acyclic_fs::OperationFailure::before_work(Error::Unsupported(
            "finite original approval verification is unavailable".into()))) })
    }

    /// Verifies that same retained approval on the original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_with_native_work<'a>(
        &'a self,
        _interaction_id: InteractionId,
        _operation_id: OperationId,
        _definition_digest: [u8; 32],
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "approval verifier has no native journal observer",
        })
    }
}
