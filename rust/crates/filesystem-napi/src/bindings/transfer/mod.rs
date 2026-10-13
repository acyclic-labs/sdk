use super::*;

/// Complete resumable generation-transfer manifest.
#[napi(object)]
pub struct NativeExportManifest {
    pub manifest_bytes: Buffer,
    pub objects: Vec<Buffer>,
    pub work_json: String,
}

/// One manifest-ordered immutable-object transfer page.
#[napi(object)]
pub struct NativeGenerationTransferBatch {
    pub first_object: BigInt,
    pub next_object: Option<BigInt>,
    pub objects: Vec<Buffer>,
    pub work_json: String,
}

/// Cursor after one idempotently imported transfer page.
#[napi(object)]
pub struct NativeGenerationTransferCursor {
    pub next_object: BigInt,
    pub work_json: String,
}

#[napi]
impl NativeCheckout {
    #[napi]
    pub async fn export_manifest(&self) -> Result<NativeExportManifest> {
        let checkout = Box::pin(self.inner.lock()).await;
        let receipt = checkout
            .export_manifest(acyclic_fs::performance::BOUNDARY_BUDGET, &self.cancellation)
            .await
            .map_err(napi_error)?;
        encode_export_manifest(receipt.value, receipt.work)
    }
}

pub(in crate::bindings) fn encode_export_manifest(
    manifest: GenerationExportManifest,
    work: acyclic_fs::WorkCounters,
) -> Result<NativeExportManifest> {
    let manifest_bytes = encode_generation_export_manifest(&manifest).map_err(napi_error)?;
    Ok(NativeExportManifest {
        manifest_bytes: Buffer::from(manifest_bytes),
        objects: manifest.objects.into_iter().map(encode_object_id).collect(),
        work_json: work_json(&work)?,
    })
}

pub(in crate::bindings) fn decode_export_manifest(
    manifest: &NativeExportManifest,
) -> Result<GenerationExportManifest> {
    const MAXIMUM_MANIFEST_BYTES: u64 = 256 * 1024 * 1024;
    const MAXIMUM_MANIFEST_OBJECTS: u64 = 1_000_000;
    decode_generation_export_manifest(
        &manifest.manifest_bytes,
        MAXIMUM_MANIFEST_BYTES,
        MAXIMUM_MANIFEST_OBJECTS,
    )
    .map_err(napi_error)
}
