use super::*;

/// Provider-owned effect capability boundary.
pub trait EffectProvider: acyclic_stream::ProviderPlatform {
    /// Stable provider identity.
    fn id(&self) -> &str;

    /// Delivery guarantees actually supported for an effect kind.
    fn guarantees(&self, effect_kind: &str) -> BTreeSet<EffectGuarantee>;

    /// Whether exactly-once claims are backed by linearizable apply/reconcile semantics.
    fn linearizable_reconciliation(&self) -> bool;

    /// Dispatches one already-durable attempt.
    fn dispatch<'a>(&'a self, request: EffectDispatch) -> BoxFuture<'a, Result<EffectObservation>>;

    /// Queries an existing attempt without redispatching it.
    fn reconcile<'a>(
        &'a self,
        attempt_id: EffectAttemptId,
    ) -> BoxFuture<'a, Result<Option<EffectObservation>>>;

    /// Dispatches the same durable attempt on the supplied original ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn dispatch_with_native_work<'a>(
        &'a self,
        _request: EffectDispatch,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (EffectObservation, acyclic_fs::NativeAllocation)>>
    {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "effect provider has no native dispatch observer",
        })
    }

    /// Observes only the original attempt, retaining actual returned metadata.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_with_native_work<'a>(
        &'a self,
        _attempt_id: EffectAttemptId,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<'a, Option<(EffectObservation, acyclic_fs::NativeAllocation)>>,
    > {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "effect provider has no native reconciliation observer",
        })
    }

    /// Tests the pinned guarantee without constructing an unbounded catalog.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_guarantee_with_native_work(
        &self,
        _effect_kind: &str,
        _guarantee: EffectGuarantee,
        _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "effect provider has no native guarantee observer",
        })
    }
}
