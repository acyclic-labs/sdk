use super::*;

#[wasm_bindgen]
impl BrowserCheckout {
    #[wasm_bindgen(js_name = createFile)]
    pub async fn create_file(
        &mut self,
        path: String,
        bytes: Vec<u8>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let bytes = bytes::Bytes::from(bytes);
        let receipt = browser_call!(
            self,
            create(acyclic_fs::AuthoredMutation::CreateFile {
                path: path,
                bytes: (bytes).into(),
                metadata: acyclic_fs::kernel::FileMetadata::default(),
            },)
        )?;
        mutation_created(&receipt)
    }

    #[wasm_bindgen(js_name = createDirectory)]
    pub async fn create_directory(
        &mut self,
        path: String,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let receipt = browser_call!(
            self,
            create(acyclic_fs::AuthoredMutation::CreateDirectory {
                path: path,
                metadata: acyclic_fs::kernel::FileMetadata::default(),
            },)
        )?;
        mutation_created(&receipt)
    }

    #[wasm_bindgen(js_name = createSymbolicLink)]
    pub async fn create_symbolic_link(
        &mut self,
        path: String,
        target: Vec<u8>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let target = bytes::Bytes::from(target);
        let receipt = browser_call!(
            self,
            create(acyclic_fs::AuthoredMutation::CreateSymbolicLink {
                path: path,
                target: (target).into(),
                metadata: acyclic_fs::kernel::FileMetadata::default(),
            },)
        )?;
        mutation_created(&receipt)
    }

    #[wasm_bindgen(js_name = createSpecial)]
    pub async fn create_special(
        &mut self,
        path: String,
        kind: String,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let kind = empty_special_kind(&kind)?;
        let receipt = browser_call!(
            self,
            create(acyclic_fs::AuthoredMutation::CreateEmptySpecial {
                path: path,
                kind: kind,
                metadata: acyclic_fs::kernel::FileMetadata::default(),
            },)
        )?;
        mutation_created(&receipt)
    }

    #[wasm_bindgen(js_name = createDevice)]
    pub async fn create_device(
        &mut self,
        path: String,
        kind: String,
        #[wasm_bindgen(unchecked_param_type = "number")] major: JsValue,
        #[wasm_bindgen(unchecked_param_type = "number")] minor: JsValue,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let major = wasm_u32(&major)?;
        let minor = wasm_u32(&minor)?;

        let path = browser_path(&path, self.engine.volume_config())?;
        let kind = device_kind(&kind)?;
        let receipt = browser_call!(
            self,
            create(acyclic_fs::AuthoredMutation::CreateDevice {
                path: path,
                kind: kind,
                major: major,
                minor: minor,
                metadata: acyclic_fs::kernel::FileMetadata::default(),
            },)
        )?;
        mutation_created(&receipt)
    }

    #[wasm_bindgen(js_name = createReparsePoint)]
    pub async fn create_reparse_point(
        &mut self,
        path: String,
        payload: Vec<u8>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let receipt = browser_call!(
            self,
            create(acyclic_fs::AuthoredMutation::CreateReparsePoint {
                path: path,
                payload: (bytes::Bytes::from(payload)).into(),
                metadata: acyclic_fs::kernel::FileMetadata::default(),
            },)
        )?;
        mutation_created(&receipt)
    }

    pub async fn remove(
        &mut self,
        path: String,
        expected_file_id: Option<Vec<u8>>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let expected = expected_file_id
            .as_deref()
            .map(fixed_16)
            .transpose()?
            .map(FileId::from_bytes);
        let receipt = browser_call!(
            self,
            mutate(vec![acyclic_fs::kernel::Mutation::Remove {
                path: path,
                expected_file_id: (expected).map_or(
                    acyclic_fs::kernel::MetadataField::Unavailable,
                    acyclic_fs::kernel::MetadataField::Value,
                ),
            }],)
        )?;
        mutation_value(receipt.work)
    }

    pub async fn rename(
        &mut self,
        source: String,
        destination: String,
        replace: bool,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let source = browser_path(&source, self.engine.volume_config())?;
        let destination = browser_path(&destination, self.engine.volume_config())?;
        let receipt = browser_call!(
            self,
            mutate(vec![acyclic_fs::kernel::Mutation::Rename {
                source: source,
                destination: destination,
                replace: replace,
            }],)
        )?;
        mutation_value(receipt.work)
    }

    #[wasm_bindgen(js_name = hardLink)]
    pub async fn hard_link(
        &mut self,
        source: String,
        destination: String,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let source = browser_path(&source, self.engine.volume_config())?;
        let destination = browser_path(&destination, self.engine.volume_config())?;
        let receipt = browser_call!(
            self,
            mutate(vec![acyclic_fs::kernel::Mutation::Link {
                source: source,
                destination: destination,
            }],)
        )?;
        mutation_value(receipt.work)
    }
}
