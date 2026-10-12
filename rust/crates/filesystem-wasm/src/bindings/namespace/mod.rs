use super::*;

#[wasm_bindgen]
impl BrowserCheckout {
    /// Creates one regular file in the private COW overlay.
    #[wasm_bindgen(js_name = createFile)]
    pub async fn create_file(
        &mut self,
        path: String,
        bytes: Vec<u8>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(
            &path,
            self.engine.volume_config().profile,
            self.engine.volume_config().limits,
        )?;
        let cancellation = CancellationToken::default();
        let bytes = bytes::Bytes::from(bytes);
        let receipt = self
            .engine
            .create(
                acyclic_fs::AuthoredMutation::CreateFile {
                    path: path,
                    bytes: (bytes).into(),
                    metadata: acyclic_fs::kernel::FileMetadata::default(),
                },
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        ts(BrowserMutationResult {
            file_id: Some(receipt.value.into_bytes().to_vec().into()),
            work: browser_work(receipt.work),
        })
    }

    /// Creates one empty directory in the private COW overlay.
    #[wasm_bindgen(js_name = createDirectory)]
    pub async fn create_directory(
        &mut self,
        path: String,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(
            &path,
            self.engine.volume_config().profile,
            self.engine.volume_config().limits,
        )?;
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .create(
                acyclic_fs::AuthoredMutation::CreateDirectory {
                    path: path,
                    metadata: acyclic_fs::kernel::FileMetadata::default(),
                },
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        ts(BrowserMutationResult {
            file_id: Some(receipt.value.into_bytes().to_vec().into()),
            work: browser_work(receipt.work),
        })
    }

    /// Creates one symbolic link with exact opaque target bytes.
    #[wasm_bindgen(js_name = createSymbolicLink)]
    pub async fn create_symbolic_link(
        &mut self,
        path: String,
        target: Vec<u8>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(
            &path,
            self.engine.volume_config().profile,
            self.engine.volume_config().limits,
        )?;
        let cancellation = CancellationToken::default();
        let target = bytes::Bytes::from(target);
        let receipt = self
            .engine
            .create(
                acyclic_fs::AuthoredMutation::CreateSymbolicLink {
                    path: path,
                    target: (target).into(),
                    metadata: acyclic_fs::kernel::FileMetadata::default(),
                },
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        ts(BrowserMutationResult {
            file_id: Some(receipt.value.into_bytes().to_vec().into()),
            work: browser_work(receipt.work),
        })
    }

    /// Creates an exact empty special namespace entry.
    #[wasm_bindgen(js_name = createSpecial)]
    pub async fn create_special(
        &mut self,
        path: String,
        kind: String,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(
            &path,
            self.engine.volume_config().profile,
            self.engine.volume_config().limits,
        )?;
        let kind = empty_special_kind(&kind)?;
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .create(
                acyclic_fs::AuthoredMutation::CreateEmptySpecial {
                    path: path,
                    kind: kind,
                    metadata: acyclic_fs::kernel::FileMetadata::default(),
                },
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        mutation_created(&receipt)
    }

    /// Creates an exact POSIX character or block device identity.
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

        let path = browser_path(
            &path,
            self.engine.volume_config().profile,
            self.engine.volume_config().limits,
        )?;
        let kind = device_kind(&kind)?;
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .create(
                acyclic_fs::AuthoredMutation::CreateDevice {
                    path: path,
                    kind: kind,
                    major: major,
                    minor: minor,
                    metadata: acyclic_fs::kernel::FileMetadata::default(),
                },
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        mutation_created(&receipt)
    }

    /// Creates an opaque exact Windows reparse-point payload.
    #[wasm_bindgen(js_name = createReparsePoint)]
    pub async fn create_reparse_point(
        &mut self,
        path: String,
        payload: Vec<u8>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(
            &path,
            self.engine.volume_config().profile,
            self.engine.volume_config().limits,
        )?;
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .create(
                acyclic_fs::AuthoredMutation::CreateReparsePoint {
                    path: path,
                    payload: (bytes::Bytes::from(payload)).into(),
                    metadata: acyclic_fs::kernel::FileMetadata::default(),
                },
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        mutation_created(&receipt)
    }

    /// Removes one namespace binding.
    pub async fn remove(
        &mut self,
        path: String,
        expected_file_id: Option<Vec<u8>>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(
            &path,
            self.engine.volume_config().profile,
            self.engine.volume_config().limits,
        )?;
        let expected = expected_file_id
            .as_deref()
            .map(fixed_16)
            .transpose()?
            .map(FileId::from_bytes);
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .mutate(
                vec![acyclic_fs::kernel::Mutation::Remove {
                    path: path,
                    expected_file_id: (expected).map_or(
                        acyclic_fs::kernel::MetadataField::Unavailable,
                        acyclic_fs::kernel::MetadataField::Value,
                    ),
                }],
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        mutation_value(receipt.work)
    }

    /// Atomically renames one binding within the volume.
    pub async fn rename(
        &mut self,
        source: String,
        destination: String,
        replace: bool,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let source = browser_path(
            &source,
            self.engine.volume_config().profile,
            self.engine.volume_config().limits,
        )?;
        let destination = browser_path(
            &destination,
            self.engine.volume_config().profile,
            self.engine.volume_config().limits,
        )?;
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .mutate(
                vec![acyclic_fs::kernel::Mutation::Rename {
                    source: source,
                    destination: destination,
                    replace: replace,
                }],
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        mutation_value(receipt.work)
    }

    /// Creates one hard link within the volume.
    #[wasm_bindgen(js_name = hardLink)]
    pub async fn hard_link(
        &mut self,
        source: String,
        destination: String,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let source = browser_path(
            &source,
            self.engine.volume_config().profile,
            self.engine.volume_config().limits,
        )?;
        let destination = browser_path(
            &destination,
            self.engine.volume_config().profile,
            self.engine.volume_config().limits,
        )?;
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .mutate(
                vec![acyclic_fs::kernel::Mutation::Link {
                    source: source,
                    destination: destination,
                }],
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        mutation_value(receipt.work)
    }
}
