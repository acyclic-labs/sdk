use super::*;

/// One generation-fenced checkout. Mutable operations are serialized per handle.
#[napi]
pub struct NativeCheckout {
    pub(in crate::bindings) inner: Arc<SharedCheckout<LocalAuthorityBackend, LocalObjectBackend>>,
    pub(in crate::bindings) config: VolumeConfig,
    pub(in crate::bindings) cancellation: CancellationToken,
    pub(in crate::bindings) acquisition_work: acyclic_fs::WorkCounters,
}

#[napi]
impl NativeCheckout {
    /// Returns exact bounded work used to acquire this checkout handle.
    #[napi(getter)]
    pub fn acquisition_work_json(&self) -> Result<String> {
        work_json(&self.acquisition_work)
    }

    /// Discards the private overlay and returns to its current immutable base.
    #[napi]
    pub async fn discard(&self) -> Result<NativeMutationResult> {
        let mut checkout = Box::pin(self.inner.lock()).await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .discard(acyclic_fs::performance::BOUNDARY_BUDGET, &self.cancellation)
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Cooperatively cancels this checkout's future operations.
    #[napi]
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
}
