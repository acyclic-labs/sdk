use super::*;

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserExportManifestResult {
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) manifest_bytes: Vec<u8>,
    #[tsify(type = "Uint8Array[]")]
    pub(in crate::bindings) objects: Vec<serde_bytes::ByteBuf>,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::bindings) struct ImportManifest {
    #[serde(with = "serde_bytes")]
    pub(in crate::bindings) manifest_bytes: Vec<u8>,
    pub(in crate::bindings) objects: Vec<serde_bytes::ByteBuf>,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct GenerationTransferBatchResult {
    pub(in crate::bindings) first_object: u64,
    pub(in crate::bindings) next_object: Option<u64>,
    #[tsify(type = "Uint8Array[]")]
    pub(in crate::bindings) objects: Vec<serde_bytes::ByteBuf>,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct GenerationTransferCursorResult {
    pub(in crate::bindings) next_object: u64,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[wasm_bindgen]
impl BrowserCheckout {
    #[wasm_bindgen(js_name = exportManifest)]
    pub async fn export_manifest(&self) -> Result<Ts<BrowserExportManifestResult>, JsValue> {
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .export_manifest(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
            .await
            .map_err(js_error)?;
        encode_export_manifest(receipt.value, receipt.work)
    }
}

pub(in crate::bindings) fn encode_export_manifest(
    manifest: GenerationExportManifest,
    work: acyclic_fs::WorkCounters,
) -> Result<Ts<BrowserExportManifestResult>, JsValue> {
    let manifest_bytes = encode_generation_export_manifest(&manifest).map_err(js_error)?;
    ts(BrowserExportManifestResult {
        manifest_bytes,
        objects: manifest
            .objects
            .into_iter()
            .map(|object| encode_object_id(object).into())
            .collect(),
        work: browser_work(work),
    })
}

pub(in crate::bindings) fn decode_export_manifest(
    manifest: &ImportManifest,
) -> Result<GenerationExportManifest, JsValue> {
    const MAXIMUM_MANIFEST_BYTES: u64 = 256 * 1024 * 1024;
    const MAXIMUM_MANIFEST_OBJECTS: u64 = 1_000_000;
    let decoded = decode_generation_export_manifest(
        &manifest.manifest_bytes,
        MAXIMUM_MANIFEST_BYTES,
        MAXIMUM_MANIFEST_OBJECTS,
    )
    .map_err(js_error)?;
    if manifest.objects.len() != decoded.objects.len() {
        return Err(js_error(
            "manifest object list does not match canonical manifest",
        ));
    }
    for (encoded, expected) in manifest.objects.iter().zip(&decoded.objects) {
        if decode_object_id(encoded)? != *expected {
            return Err(js_error(
                "manifest object list does not match canonical manifest",
            ));
        }
    }
    Ok(decoded)
}
