use super::*;

/// One immutable-generation checkout with optional private COW mutations.
#[wasm_bindgen]
pub struct BrowserCheckout {
    pub(in crate::bindings) engine: Checkout<BrowserAuthority, CachedObjectStore<BrowserObjects>>,
    pub(in crate::bindings) acquisition_work: acyclic_fs::WorkCounters,
}

#[wasm_bindgen]
impl BrowserCheckout {
    /// Returns exact bounded work used to acquire this checkout handle.
    #[wasm_bindgen(getter, js_name = acquisitionWork)]
    pub fn acquisition_work(&self) -> Result<Ts<BrowserWorkCounters>, JsValue> {
        ts(browser_work(self.acquisition_work))
    }

    /// Discards the private overlay and returns to its immutable base.
    pub async fn discard(&mut self) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .discard(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
            .await
            .map_err(js_error)?;
        mutation_value(receipt.work)
    }
}
