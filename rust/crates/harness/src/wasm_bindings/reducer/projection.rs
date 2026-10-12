use super::*;

#[wasm_bindgen]
impl WasmReducer {
    /// Composes a captured immutable prefix after owner authorization.
    #[wasm_bindgen(js_name = prepareInheritedModelRequest)]
    pub async fn prepare_inherited_model_request(
        &self,
        scope: JsValue,
        request: JsValue,
        prefix: JsValue,
        files: JsValue,
        limits: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        let limits: Limits = from_js(limits)?;
        limits.validate(&mut None).map_err(js_error)?;
        let files = WasmProjectionResolver::from_js(files)?;
        let total = files
            .files
            .values()
            .try_fold(0_u64, |sum, bytes| sum.checked_add(bytes.len() as u64));
        if total.is_none()
            || files.files.len()
                > limits
                    .context_messages
                    .saturating_mul(limits.attachments.saturating_add(2))
        {
            return Err(JsValue::from_str("captured prefix files exceed limits"));
        }
        let reader = WasmPrefixReader {
            files,
            verifier: self.issuer.verifier(&mut None).map_err(js_error)?,
            scope: from_js(scope)?,
        };
        reader
            .verifier
            .verify(&reader.scope, &mut None)
            .map_err(js_error)?;
        let prefix: FileRef = from_js(prefix)?;
        let input: WasmModelRequestInput = from_js(request)?;
        let prepared = crate::model::PreparedModelRequest::inherit(
            input.into(),
            &prefix,
            &reader,
            limits,
            None,
        )
        .await
        .map_err(js_error)?;
        Ok(prepared.bytes().to_vec())
    }
}
