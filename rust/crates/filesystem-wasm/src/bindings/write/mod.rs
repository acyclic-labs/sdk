use super::*;

#[wasm_bindgen]
impl BrowserCheckout {
    #[wasm_bindgen(js_name = writeFile)]
    pub async fn write_file(
        &mut self,
        path: String,
        offset: u64,
        bytes: Vec<u8>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let bytes = bytes::Bytes::from(bytes);
        let receipt = browser_call!(
            self,
            change_content(
                path,
                acyclic_fs::ContentChange::Write {
                    offset: offset,
                    bytes: (bytes).into(),
                },
                acyclic_fs::ContentTimes::Preserve,
            )
        )?;
        mutation_value(receipt.work)
    }

    #[wasm_bindgen(js_name = writeFileById)]
    pub async fn write_file_by_id(
        &mut self,
        file_id: Vec<u8>,
        offset: u64,
        bytes: Vec<u8>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let bytes = bytes::Bytes::from(bytes);
        let receipt = browser_call!(
            self,
            change_content(
                file_id,
                acyclic_fs::ContentChange::Write {
                    offset: offset,
                    bytes: (bytes).into(),
                },
                acyclic_fs::ContentTimes::Preserve,
            )
        )?;
        mutation_value(receipt.work)
    }

    #[wasm_bindgen(js_name = resizeFile)]
    pub async fn resize_file(
        &mut self,
        path: String,
        logical_bytes: u64,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let receipt = browser_call!(
            self,
            change_content(
                path,
                acyclic_fs::ContentChange::Resize {
                    logical_bytes: logical_bytes,
                },
                acyclic_fs::ContentTimes::Preserve,
            )
        )?;
        mutation_value(receipt.work)
    }

    #[wasm_bindgen(js_name = resizeFileById)]
    pub async fn resize_file_by_id(
        &mut self,
        file_id: Vec<u8>,
        logical_bytes: u64,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let receipt = browser_call!(
            self,
            change_content(
                file_id,
                acyclic_fs::ContentChange::Resize {
                    logical_bytes: logical_bytes,
                },
                acyclic_fs::ContentTimes::Preserve,
            )
        )?;
        mutation_value(receipt.work)
    }

    #[wasm_bindgen(js_name = zeroFileRange)]
    pub async fn zero_file_range(
        &mut self,
        path: String,
        offset: u64,
        length: u64,
        allocated: bool,
        extend: bool,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let receipt = browser_call!(
            self,
            change_content(
                path,
                acyclic_fs::ContentChange::ZeroRange {
                    range: ByteRange { offset, length },
                    allocated: allocated,
                    extend: extend,
                },
                acyclic_fs::ContentTimes::Preserve,
            )
        )?;
        mutation_value(receipt.work)
    }

    #[wasm_bindgen(js_name = zeroFileRangeById)]
    pub async fn zero_file_range_by_id(
        &mut self,
        file_id: Vec<u8>,
        offset: u64,
        length: u64,
        allocated: bool,
        extend: bool,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let receipt = browser_call!(
            self,
            change_content(
                file_id,
                acyclic_fs::ContentChange::ZeroRange {
                    range: ByteRange { offset, length },
                    allocated: allocated,
                    extend: extend,
                },
                acyclic_fs::ContentTimes::Preserve,
            )
        )?;
        mutation_value(receipt.work)
    }

    #[wasm_bindgen(js_name = preallocateFile)]
    pub async fn preallocate_file(
        &mut self,
        path: String,
        offset: u64,
        length: u64,
        keep_size: bool,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let receipt = browser_call!(
            self,
            change_content(
                path,
                acyclic_fs::ContentChange::Preallocate {
                    range: ByteRange { offset, length },
                    keep_size: keep_size,
                },
                acyclic_fs::ContentTimes::Preserve,
            )
        )?;
        mutation_value(receipt.work)
    }

    #[wasm_bindgen(js_name = preallocateFileById)]
    pub async fn preallocate_file_by_id(
        &mut self,
        file_id: Vec<u8>,
        offset: u64,
        length: u64,
        keep_size: bool,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let receipt = browser_call!(
            self,
            change_content(
                file_id,
                acyclic_fs::ContentChange::Preallocate {
                    range: ByteRange { offset, length },
                    keep_size: keep_size,
                },
                acyclic_fs::ContentTimes::Preserve,
            )
        )?;
        mutation_value(receipt.work)
    }

    #[wasm_bindgen(js_name = cloneFileRange)]
    pub async fn clone_file_range(
        &mut self,
        source: String,
        source_offset: u64,
        destination: String,
        destination_offset: u64,
        length: u64,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let request = FileCloneRequest {
            source: browser_path(&source, self.engine.volume_config())?,
            source_offset,
            destination: browser_path(&destination, self.engine.volume_config())?,
            destination_offset,
            length,
        };
        let receipt = browser_call!(
            self,
            change_content(
                request.destination,
                acyclic_fs::ContentChange::CloneFrom {
                    source: request.source,
                    source_offset: request.source_offset,
                    offset: request.destination_offset,
                    length: request.length,
                },
                acyclic_fs::ContentTimes::Preserve,
            )
        )?;
        mutation_value(receipt.work)
    }

    #[wasm_bindgen(js_name = cloneFileRangeById)]
    pub async fn clone_file_range_by_id(
        &mut self,
        source_file_id: Vec<u8>,
        source_offset: u64,
        destination_file_id: Vec<u8>,
        destination_offset: u64,
        length: u64,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let source_file_id = FileId::from_bytes(fixed_16(&source_file_id)?);
        let destination_file_id = FileId::from_bytes(fixed_16(&destination_file_id)?);
        let receipt = browser_call!(
            self,
            change_content(
                destination_file_id,
                acyclic_fs::ContentChange::CloneFrom {
                    source: source_file_id,
                    source_offset: source_offset,
                    offset: destination_offset,
                    length: length,
                },
                acyclic_fs::ContentTimes::Preserve,
            )
        )?;
        mutation_value(receipt.work)
    }
}
