use super::*;

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserDirectoryEntryResult {
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) name: Vec<u8>,
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) file_id: Vec<u8>,
    pub(in crate::bindings) file_kind: FileKind,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserDirectoryPageResult {
    pub(in crate::bindings) entries: Vec<BrowserDirectoryEntryResult>,
    pub(in crate::bindings) has_more: bool,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserDirectoryRecordEntryResult {
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) name: Vec<u8>,
    pub(in crate::bindings) record: BrowserFileRecordResult,
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) metadata_canonical_bytes: Vec<u8>,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserDirectoryRecordPageResult {
    pub(in crate::bindings) entries: Vec<BrowserDirectoryRecordEntryResult>,
    pub(in crate::bindings) has_more: bool,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[wasm_bindgen]
impl BrowserCheckout {
    #[wasm_bindgen(js_name = listDirectory)]
    pub async fn list_directory(
        &mut self,
        path: String,
        after: Option<String>,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_entries: JsValue,
    ) -> Result<Ts<BrowserDirectoryPageResult>, JsValue> {
        let maximum_entries = wasm_u32(&maximum_entries)?;

        let path = browser_path(&path, self.engine.volume_config())?;
        let after = after
            .as_deref()
            .map(|value| browser_name(value, self.engine.volume_config()))
            .transpose()?;
        let receipt = browser_call!(
            self,
            list_directory(&path, after.as_ref(), maximum_entries,)
        )?;
        let entries = receipt
            .value
            .entries
            .into_iter()
            .map(|entry| BrowserDirectoryEntryResult {
                name: entry.name.as_bytes().to_vec(),
                file_id: entry.file_id.into_bytes().to_vec(),
                file_kind: entry.kind,
            })
            .collect();
        ts(BrowserDirectoryPageResult {
            entries,
            has_more: receipt.value.has_more,
            work: browser_work(receipt.work),
        })
    }

    #[wasm_bindgen(js_name = listDirectoryRecords)]
    pub async fn list_directory_records(
        &mut self,
        path: String,
        after: Option<String>,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_entries: JsValue,
    ) -> Result<Ts<BrowserDirectoryRecordPageResult>, JsValue> {
        let maximum_entries = wasm_u32(&maximum_entries)?;

        let path = browser_path(&path, self.engine.volume_config())?;
        let after = after
            .as_deref()
            .map(|value| browser_name(value, self.engine.volume_config()))
            .transpose()?;
        let receipt = browser_call!(
            self,
            list_directory_records(
                &path,
                after
                    .as_ref()
                    .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                maximum_entries,
            )
        )?;
        let entries = receipt
            .value
            .entries
            .into_iter()
            .map(|entry| {
                Ok(BrowserDirectoryRecordEntryResult {
                    name: entry.name.as_bytes().to_vec(),
                    record: encode_file_record(entry.record),
                    metadata_canonical_bytes: encode_file_metadata(
                        entry.metadata,
                        |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
                        |_| acyclic_fs::kernel::CanonicalDecodeError::AllocationFailed,
                    )
                    .map_err(js_error)?,
                })
            })
            .collect::<Result<Vec<_>, JsValue>>()?;
        ts(BrowserDirectoryRecordPageResult {
            entries,
            has_more: receipt.value.has_more,
            work: browser_work(receipt.work),
        })
    }
}
