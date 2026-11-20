use super::*;

/// One independently configured browser volume.
#[wasm_bindgen]
pub struct BrowserVolume {
    pub(in crate::bindings) engine: Volume<BrowserAuthority, CachedObjectStore<BrowserObjects>>,
    pub(in crate::bindings) acquisition_work: acyclic_fs_work::performance::WorkCounters,
}

#[wasm_bindgen]
impl BrowserVolume {
    #[wasm_bindgen(getter)]
    #[must_use]
    pub fn id(&self) -> Vec<u8> {
        self.engine.id().into_bytes().to_vec()
    }

    #[wasm_bindgen(getter, js_name = acquisitionWork)]
    pub fn acquisition_work(&self) -> Result<Ts<BrowserWorkCounters>, JsValue> {
        ts(browser_work(self.acquisition_work))
    }

    #[wasm_bindgen(js_name = diffGenerations)]
    pub async fn diff_generations(
        &self,
        before: Vec<u8>,
        after: Vec<u8>,
        #[wasm_bindgen(unchecked_param_type = "number | undefined")] maximum_changes: JsValue,
    ) -> Result<Ts<BrowserGenerationDiffResult>, JsValue> {
        let maximum_changes = wasm_optional_u32(&maximum_changes)?;

        let before = decode_generation_id(&before)?;
        let after = decode_generation_id(&after)?;
        let receipt = browser_call!(self, diff_generations(before, after, maximum_changes,))?;
        encode_generation_diff(receipt.value, receipt.work)
    }

    pub async fn checkout(&self, options: JsValue) -> Result<BrowserCheckout, JsValue> {
        let options: BrowserCheckoutOptions =
            serde_wasm_bindgen::from_value(options).map_err(js_error)?;
        let mode = checkout_mode(&options);
        let receipt = browser_call!(self, checkout(GenerationSelector::Head, mode,))?;
        Ok(BrowserCheckout {
            engine: receipt.value,
            acquisition_work: receipt.work,
        })
    }
}
