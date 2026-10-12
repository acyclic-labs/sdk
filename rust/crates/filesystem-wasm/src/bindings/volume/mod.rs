use super::*;

/// One independently configured browser volume.
#[wasm_bindgen]
pub struct BrowserVolume {
    pub(in crate::bindings) engine: Volume<BrowserAuthority, CachedObjectStore<BrowserObjects>>,
    pub(in crate::bindings) acquisition_work: acyclic_fs::WorkCounters,
}

#[wasm_bindgen]
impl BrowserVolume {
    /// Returns the canonical 16-byte volume identity.
    #[wasm_bindgen(getter)]
    #[must_use]
    pub fn id(&self) -> Vec<u8> {
        self.engine.id().into_bytes().to_vec()
    }

    /// Returns exact bounded work used to acquire this volume handle.
    #[wasm_bindgen(getter, js_name = acquisitionWork)]
    pub fn acquisition_work(&self) -> Result<Ts<BrowserWorkCounters>, JsValue> {
        ts(browser_work(self.acquisition_work))
    }

    /// Computes one bounded Merkle-aware semantic generation diff.
    #[wasm_bindgen(js_name = diffGenerations)]
    pub async fn diff_generations(
        &self,
        before: Vec<u8>,
        after: Vec<u8>,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_changes: JsValue,
    ) -> Result<Ts<BrowserGenerationDiffResult>, JsValue> {
        let maximum_changes = wasm_u32(&maximum_changes)?;

        let before = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &before,
            "before generation identity",
        )?));
        let after = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &after,
            "after generation identity",
        )?));
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .diff_generations(
                before,
                after,
                maximum_changes,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        encode_generation_diff(receipt.value, receipt.work)
    }

    /// Opens the volume head with explicit access and consistency semantics.
    pub async fn checkout(&self, options: JsValue) -> Result<BrowserCheckout, JsValue> {
        let options: CheckoutOptions = serde_wasm_bindgen::from_value(options).map_err(js_error)?;
        let mode = checkout_mode(&options);
        let cancellation = CancellationToken::default();
        let (engine, acquisition_work) = {
            let volume = &self.engine;
            let receipt = volume
                .checkout(
                    GenerationSelector::Head,
                    mode,
                    acyclic_fs::performance::BOUNDARY_BUDGET,
                    &cancellation,
                )
                .await
                .map_err(js_error)?;
            (receipt.value, receipt.work)
        };
        Ok(BrowserCheckout {
            engine,
            acquisition_work,
        })
    }
}
