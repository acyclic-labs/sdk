use super::*;

#[derive(Deserialize, Serialize, Tsify)]
#[serde(transparent)]
#[allow(
    dead_code,
    reason = "exists only to emit the BrowserPathBatch TypeScript type named by unchecked_param_type"
)]
pub struct BrowserPathBatch(pub(in crate::bindings) Vec<String>);

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserLookupResult {
    pub(in crate::bindings) exists: bool,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) file_id: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) file_kind: Option<FileKind>,
    pub(in crate::bindings) resolved_components: u16,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserBatchLookupEntryResult {
    pub(in crate::bindings) exists: bool,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) file_id: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) file_kind: Option<FileKind>,
    pub(in crate::bindings) resolved_components: u16,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserBatchLookupResult {
    pub(in crate::bindings) entries: Vec<BrowserBatchLookupEntryResult>,
    pub(in crate::bindings) retained_allocation_bytes: u64,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserFileReadResult {
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) bytes: Vec<u8>,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[wasm_bindgen]
impl BrowserCheckout {
    #[wasm_bindgen(js_name = lookupNoFollow)]
    pub async fn lookup_no_follow(
        &mut self,
        path: String,
    ) -> Result<Ts<BrowserLookupResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let receipt = browser_call!(self, lookup_no_follow(&path,))?;
        let record = receipt.value.record;
        ts(BrowserLookupResult {
            exists: record.is_some(),
            file_id: record.map(|value| value.file_id.into_bytes().to_vec().into()),
            file_kind: record.map(|value| value.kind),
            resolved_components: receipt.value.resolved_components,
            work: browser_work(receipt.work),
        })
    }

    #[wasm_bindgen(js_name = lookupBatchNoFollow)]
    pub async fn lookup_batch_no_follow(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "BrowserPathBatch")] paths: JsValue,
    ) -> Result<Ts<BrowserBatchLookupResult>, JsValue> {
        let decoded: Vec<String> = serde_wasm_bindgen::from_value(paths).map_err(js_error)?;
        if decoded.is_empty() {
            return Err(js_error("lookup path batch is empty"));
        }
        let paths = decoded
            .iter()
            .map(|path| browser_path(path, self.engine.volume_config()))
            .collect::<Result<Vec<_>, _>>()?;
        let receipt = browser_call!(self, lookup_batch_no_follow(&paths,))?;
        ts(BrowserBatchLookupResult {
            entries: receipt
                .value
                .entries
                .into_iter()
                .map(|entry| BrowserBatchLookupEntryResult {
                    exists: entry.record.is_some(),
                    file_id: entry
                        .record
                        .map(|record| record.file_id.into_bytes().to_vec().into()),
                    file_kind: entry.record.map(|record| record.kind),
                    resolved_components: entry.resolved_components,
                })
                .collect(),
            retained_allocation_bytes: receipt.value.retained_allocation_bytes,
            work: browser_work(receipt.work),
        })
    }

    #[wasm_bindgen(js_name = statNoFollow)]
    pub async fn stat_no_follow(&mut self, path: String) -> Result<Ts<BrowserStatResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let receipt = browser_call!(self, lookup_no_follow_with_metadata(&path,))?;
        let (record, metadata_canonical_bytes) = match receipt.value {
            Some(value) => (
                Some(encode_file_record(value.record)),
                Some(
                    encode_file_metadata(
                        value.metadata,
                        |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
                        |_| acyclic_fs::kernel::CanonicalDecodeError::AllocationFailed,
                    )
                    .map_err(js_error)?,
                ),
            ),
            None => (None, None),
        };
        ts(BrowserStatResult {
            exists: record.is_some(),
            record,
            metadata_canonical_bytes: metadata_canonical_bytes.map(Into::into),
            work: browser_work(receipt.work),
        })
    }

    #[wasm_bindgen(js_name = readFileRecordById)]
    pub async fn read_file_record_by_id(
        &mut self,
        file_id: Vec<u8>,
    ) -> Result<Ts<BrowserFileRecordReadResult>, JsValue> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let receipt = browser_call!(self, read_file_record_by_id(file_id,))?;
        ts(BrowserFileRecordReadResult {
            record: encode_file_record(receipt.value),
            work: browser_work(receipt.work),
        })
    }

    #[wasm_bindgen(js_name = resolveFiles)]
    pub async fn resolve_files(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "BrowserPathBatch")] paths: JsValue,
    ) -> Result<BrowserResolvedFiles, JsValue> {
        let paths: Vec<String> = serde_wasm_bindgen::from_value(paths).map_err(js_error)?;
        let mut parsed = Vec::new();
        parsed
            .try_reserve_exact(paths.len())
            .map_err(|error| js_error(error.to_string()))?;
        for path in paths {
            parsed.push(browser_path(&path, self.engine.volume_config())?);
        }
        let cancellation = CancellationToken::default();
        let (files, work) = {
            let checkout = &mut self.engine;
            let reader = checkout.pinned_reader().map_err(js_error)?;
            let receipt = reader
                .resolve_files(&parsed, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(js_error)?;
            (
                receipt
                    .value
                    .into_iter()
                    .map(|file| file.map(|file| BrowserResolvedFile { engine: file }))
                    .collect(),
                receipt.work,
            )
        };
        Ok(BrowserResolvedFiles { files, work })
    }

    #[wasm_bindgen(js_name = readFileRange)]
    pub async fn read_file_range(
        &mut self,
        path: String,
        offset: u64,
        length: u64,
    ) -> Result<Ts<BrowserFileReadResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let receipt = browser_call!(self, read_file_range(&path, ByteRange { offset, length },))?;
        ts(BrowserFileReadResult {
            bytes: receipt.value.bytes.to_vec(),
            work: browser_work(receipt.work),
        })
    }

    #[wasm_bindgen(js_name = readFileRangeById)]
    pub async fn read_file_range_by_id(
        &mut self,
        file_id: Vec<u8>,
        offset: u64,
        length: u64,
    ) -> Result<Ts<BrowserFileReadResult>, JsValue> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let receipt = browser_call!(
            self,
            read_file_range(file_id, ByteRange { offset, length },)
        )?;
        ts(BrowserFileReadResult {
            bytes: receipt.value.bytes.to_vec(),
            work: browser_work(receipt.work),
        })
    }

    #[wasm_bindgen(js_name = readSymbolicLink)]
    pub async fn read_symbolic_link(
        &mut self,
        path: String,
    ) -> Result<Ts<BrowserFileReadResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let receipt = browser_call!(self, read_symbolic_link(&path,))?;
        ts(BrowserFileReadResult {
            bytes: receipt.value.bytes.to_vec(),
            work: browser_work(receipt.work),
        })
    }

    #[wasm_bindgen(js_name = readReparsePoint)]
    pub async fn read_reparse_point(
        &mut self,
        path: String,
    ) -> Result<Ts<BrowserFileReadResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let receipt = browser_call!(self, read_reparse_point(&path,))?;
        ts(BrowserFileReadResult {
            bytes: receipt.value.to_vec(),
            work: browser_work(receipt.work),
        })
    }
}
