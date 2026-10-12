use super::*;

#[wasm_bindgen]
impl WasmClientViews {
    /// Select only one demanded record and explicitly supplied overlays.
    pub fn view(
        &self,
        #[wasm_bindgen(unchecked_param_type = "string")] key: &JsValue,
        overlays: &JsValue,
    ) -> Result<JsValue, JsValue> {
        let overlays = array(overlays, self.client.limits.visible).map_err(failure)?;
        let overlays = overlays
            .iter()
            .map(|value| id(&text(&value, 256)?))
            .collect::<Result<Vec<_>, _>>()
            .map_err(failure)?;
        let view = self
            .client
            .view(
                &text(key, self.client.limits.bytes).map_err(failure)?,
                &overlays,
            )
            .map_err(failure)?;
        let (basis, branch, adapter) = match view.provenance {
            Provenance::Authoritative(basis) => (basis, JsValue::NULL, JsValue::NULL),
            Provenance::Hypothesis { id, basis, adapter } => (
                basis,
                JsValue::from_str(&id_text(id)),
                JsValue::from_str(&adapter.to_string()),
            ),
        };
        object(&[
            ("value", view.value.as_ref().clone()),
            ("basis", JsValue::from_str(&basis)),
            ("branch", branch),
            ("adapter", adapter),
        ])
        .map_err(failure)
    }

    /// Narrow changed hypothesis IDs and work; no full-state export.
    pub fn observe(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "string")] key: &JsValue,
        evidence: &JsValue,
    ) -> Result<JsValue, JsValue> {
        let key = text(key, self.client.limits.bytes).map_err(failure)?;
        let changes = self.client.observe(key, evidence).map_err(failure)?;
        encode(&(
            changes.authoritative,
            changes
                .hypotheses
                .into_iter()
                .map(id_text)
                .collect::<Vec<_>>(),
            changes.work,
        ))
        .map_err(failure)
    }

    /// Inspect outcome metadata without serializing hypothesis bodies.
    pub fn status(
        &self,
        #[wasm_bindgen(unchecked_param_type = "string")] branch: &JsValue,
    ) -> Result<JsValue, JsValue> {
        let branch = text(branch, 256).map_err(failure)?;
        let Some(branch) = self.client.hypothesis(id(&branch).map_err(failure)?) else {
            return Ok(JsValue::NULL);
        };
        encode(&(branch.prediction, branch.outcome)).map_err(failure)
    }

    /// Remove one local prediction; never cancels a durable effect.
    pub fn discard(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "string")] branch: &JsValue,
    ) -> Result<JsValue, JsValue> {
        let changed = self
            .client
            .discard(id(&text(branch, 256).map_err(failure)?).map_err(failure)?)
            .map_err(failure)?;
        encode(&changed.into_iter().map(id_text).collect::<Vec<_>>()).map_err(failure)
    }

    /// Explicit logical retention tick, never a hidden timer.
    pub fn advance(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "bigint")] now: JsValue,
    ) -> Result<JsValue, JsValue> {
        let changed = self
            .client
            .advance(decode(now).map_err(failure)?)
            .map_err(failure)?;
        encode(&changed.into_iter().map(id_text).collect::<Vec<_>>()).map_err(failure)
    }

    /// Release an unreferenced demanded record.
    pub fn release(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "string")] key: &JsValue,
    ) -> Result<(), JsValue> {
        self.client
            .release(&text(key, self.client.limits.bytes).map_err(failure)?)
            .map_err(failure)
    }

    /// Resident record/branch/edge counts and conservatively accounted bytes.
    pub fn residency(&self) -> Result<JsValue, JsValue> {
        encode(&self.client.residency()).map_err(failure)
    }
}
