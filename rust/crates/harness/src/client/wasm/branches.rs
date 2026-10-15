use super::*;

#[wasm_bindgen]
impl WasmClientViews {
    /// Data-only allocation watermark. Never recycles an imported identity.
    pub fn sequence(&self) -> u64 {
        self.client.sequence()
    }

    /// Export one explicitly selected hypothesis, preserving reference values.
    pub fn checkpoint(
        &self,
        #[wasm_bindgen(unchecked_param_type = "string")] branch: &JsValue,
    ) -> Result<JsValue, JsValue> {
        let id = id(&text(branch, 256).map_err(failure)?).map_err(failure)?;
        hypothesis_object(
            self.client
                .hypothesis(id)
                .ok_or_else(|| failure(Error::Missing))?,
        )
        .map_err(failure)
    }

    /// Imports a checkpoint only through the trusted domain validator.
    pub fn restore(&mut self, checkpoint: &JsValue) -> Result<(), JsValue> {
        let branch = (|| {
            let dependencies = dependencies(checkpoint, self.client.limits)?;
            let operation = field(checkpoint, "operation")?;
            let operation = if operation.is_null() {
                None
            } else {
                Some(text(&operation, self.client.limits.bytes)?)
            };
            Ok::<_, Error>(Hypothesis {
                id: id(&text(&field(checkpoint, "branch")?, 256)?)?,
                adapter: text(&field(checkpoint, "adapter")?, 160)?
                    .parse()
                    .map_err(|_| Error::Conflict)?,
                key: text(&field(checkpoint, "key")?, self.client.limits.bytes)?,
                basis: Arc::new(text(
                    &field(checkpoint, "basis")?,
                    self.client.limits.bytes,
                )?),
                operation,
                dependencies,
                assumption: field(checkpoint, "assumption")?,
                predicted: Arc::new(field(checkpoint, "predicted")?),
                expires: decode(field(checkpoint, "expires")?)?,
                prediction: decode(field(checkpoint, "prediction")?)?,
                outcome: decode(field(checkpoint, "outcome")?)?,
                bytes: 0,
            })
        })()
        .map_err(failure)?;
        self.client.restore(branch).map_err(failure)
    }

    /// Begins an explicit hypothesis while preserving JavaScript value references.
    pub fn begin(
        &mut self,
        metadata: &JsValue,
        predicted: JsValue,
        assumption: JsValue,
    ) -> Result<String, JsValue> {
        let request = (|| {
            let dependencies = dependencies(metadata, self.client.limits)?;
            let operation = field(metadata, "operation")?;
            let operation = if operation.is_null() || operation.is_undefined() {
                None
            } else {
                Some(text(&operation, self.client.limits.bytes)?)
            };
            Ok::<_, Error>(Begin {
                key: text(&field(metadata, "key")?, self.client.limits.bytes)?,
                basis: Arc::new(text(&field(metadata, "basis")?, self.client.limits.bytes)?),
                operation,
                assumption,
                predicted: Arc::new(predicted),
                dependencies,
                expires: decode(field(metadata, "expires")?)?,
            })
        })()
        .map_err(failure)?;
        self.client.begin(request).map(id_text).map_err(failure)
    }
}
