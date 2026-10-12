use super::*;

#[wasm_bindgen]
impl WasmReducer {
    /// Computes the canonical Rust capability for one validated volume operation.
    #[wasm_bindgen(js_name = volumeCapability)]
    pub fn volume_capability(&self, volume: JsValue, operation: String) -> Result<String, JsValue> {
        let volume: VolumeRef = from_js(volume)?;
        let operation = match operation.as_str() {
            "read" => VolumeOperation::Read,
            "write" => VolumeOperation::Write,
            _ => {
                return Err(js_error(crate::Error::Invalid(
                    "volume operation is invalid".into(),
                )));
            }
        };
        volume.capability(operation, &mut None).map_err(js_error)
    }

    /// Computes the exact-version read capability without embedding it in a ref.
    #[wasm_bindgen(js_name = fileReadCapability)]
    pub fn file_read_capability(&self, file: JsValue) -> Result<String, JsValue> {
        let file: FileRef = from_js(file)?;
        file.read_capability(&mut None).map_err(js_error)
    }

    /// Computes the canonical capability for one private directory boundary.
    #[wasm_bindgen(js_name = directoryReadCapability)]
    pub fn directory_read_capability(
        &self,
        volume: JsValue,
        prefix: String,
    ) -> Result<String, JsValue> {
        let volume: VolumeRef = from_js(volume)?;
        volume
            .directory_read_capability(&prefix, &mut None)
            .map_err(js_error)
    }

    /// Authenticates an owner-issued read grant for this exact immutable file.
    #[wasm_bindgen(js_name = verifyContentRead)]
    pub fn verify_content_read(&self, scope: JsValue, file: JsValue) -> Result<(), JsValue> {
        let scope: Scope = from_js(scope)?;
        let file: FileRef = from_js(file)?;
        ContentGrant::verify_read(
            &self.issuer.verifier(&mut None).map_err(js_error)?,
            &scope,
            &file,
        )
        .map_err(js_error)?;
        Ok(())
    }

    /// Authenticates access beneath an owner-granted private directory.
    #[wasm_bindgen(js_name = verifyPrivateDirectoryRead)]
    pub fn verify_private_directory_read(
        &self,
        scope: JsValue,
        volume: JsValue,
        granted_prefix: String,
        path: String,
    ) -> Result<(), JsValue> {
        let scope: Scope = from_js(scope)?;
        let volume: VolumeRef = from_js(volume)?;
        let grant = ContentGrant::verify_directory_read(
            &self.issuer.verifier(&mut None).map_err(js_error)?,
            &scope,
            &volume,
            &granted_prefix,
            &mut None,
        )
        .map_err(js_error)?;
        grant
            .require_directory_path(&volume, &path, None)
            .map_err(js_error)
    }

    /// Authenticates a writer against the original private-volume owner.
    #[wasm_bindgen(js_name = verifyContentWrite)]
    pub fn verify_content_write(&self, scope: JsValue, volume: JsValue) -> Result<(), JsValue> {
        let scope: Scope = from_js(scope)?;
        let volume: VolumeRef = from_js(volume)?;
        ContentGrant::verify(
            &self.issuer.verifier(&mut None).map_err(js_error)?,
            &scope,
            &volume,
            VolumeOperation::Write,
            u64::MAX,
            &mut None,
        )
        .map_err(js_error)?;
        Ok(())
    }

    /// Stable physical namespace shared by Filesystem and Objects adapters.
    #[wasm_bindgen(js_name = volumeStorageName)]
    pub fn volume_storage_name(&self, volume: JsValue) -> Result<String, JsValue> {
        let volume: VolumeRef = from_js(volume)?;
        volume.storage_name(&mut None).map_err(js_error)
    }

    /// Applies configured runtime bounds to one exact file reference.
    #[wasm_bindgen(js_name = validateFileUnderLimits)]
    pub fn validate_file_under_limits(
        &self,
        file: JsValue,
        limits: JsValue,
    ) -> Result<(), JsValue> {
        let file: FileRef = from_js(file)?;
        let limits: Limits = from_js(limits)?;
        limits.validate_file(&file, &mut None).map_err(js_error)
    }
}
