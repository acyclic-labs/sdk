use super::*;

#[wasm_bindgen]
impl BrowserCheckout {
    #[wasm_bindgen(js_name = createFile)]
    pub async fn create_file(
        &mut self,
        path: String,
        bytes: Vec<u8>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        self.create_operation(TransactionOperation::CreateFile {
            path,
            bytes: bytes.into(),
        })
        .await
    }

    #[wasm_bindgen(js_name = createDirectory)]
    pub async fn create_directory(
        &mut self,
        path: String,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        self.create_operation(TransactionOperation::CreateDirectory { path })
            .await
    }

    #[wasm_bindgen(js_name = createSymbolicLink)]
    pub async fn create_symbolic_link(
        &mut self,
        path: String,
        target: Vec<u8>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        self.create_operation(TransactionOperation::CreateSymbolicLink {
            path,
            target: target.into(),
        })
        .await
    }

    #[wasm_bindgen(js_name = createSpecial)]
    pub async fn create_special(
        &mut self,
        path: String,
        kind: String,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        self.create_operation(TransactionOperation::CreateSpecial {
            path,
            file_kind: kind,
        })
        .await
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

        self.create_operation(TransactionOperation::CreateDevice {
            path,
            file_kind: kind,
            major,
            minor,
        })
        .await
    }

    #[wasm_bindgen(js_name = createReparsePoint)]
    pub async fn create_reparse_point(
        &mut self,
        path: String,
        payload: Vec<u8>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        self.create_operation(TransactionOperation::CreateReparsePoint {
            path,
            payload: payload.into(),
        })
        .await
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
                path,
                expected_file_id: (expected).map_or(
                    acyclic_fs_format::metadata::MetadataField::Unavailable,
                    acyclic_fs_format::metadata::MetadataField::Value,
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
                source,
                destination,
                replace,
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
                source,
                destination,
            }],)
        )?;
        mutation_value(receipt.work)
    }
}

impl BrowserCheckout {
    async fn create_operation(
        &mut self,
        operation: TransactionOperation,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let mutation = authored_transaction(operation, self.engine.volume_config())?;
        let receipt = browser_call!(self, create(mutation,))?;
        mutation_created(&receipt)
    }
}
