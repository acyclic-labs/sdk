use super::*;

pub(in crate::bindings) fn wire_error(error: impl std::fmt::Display) -> JsValue {
    browser_error("AcyclicCompatibilityWireError", &error.to_string())
}

/// Serializes an export result inside the function body, where a failure
#[allow(
    clippy::needless_pass_by_value,
    reason = "exports hand over the DTO they just built"
)]
pub(in crate::bindings) fn ts<T: Tsify + Serialize>(value: T) -> Result<Ts<T>, JsValue> {
    value.into_ts().map_err(js_error)
}

pub(in crate::bindings) fn browser_work(value: acyclic_fs::WorkCounters) -> BrowserWorkCounters {
    BrowserWorkCounters {
        authority_records_read: value.authority_records_read,
        authority_records_appended: value.authority_records_appended,
        authority_bytes_read: value.authority_bytes_read,
        authority_bytes_written: value.authority_bytes_written,
        object_probes: value.object_probes,
        backend_read_operations: value.backend_read_operations,
        backend_write_operations: value.backend_write_operations,
        durability_operations: value.durability_operations,
        page_reads: value.page_reads,
        page_writes: value.page_writes,
        object_bytes_read: value.object_bytes_read,
        object_bytes_written: value.object_bytes_written,
        bytes_hashed: value.bytes_hashed,
        bytes_copied: value.bytes_copied,
        bytes_encoded: value.bytes_encoded,
        source_bytes_read: value.source_bytes_read,
        source_path_components: value.source_path_components,
        source_entries_visited: value.source_entries_visited,
        output_bytes: value.output_bytes,
        items_examined: value.items_examined,
        items_returned: value.items_returned,
        allocation_operations: value.allocation_operations,
        peak_allocation_bytes: value.peak_allocation_bytes,
        materializations: value.materializations,
    }
}

compat_json!(
    encode_multi_root_plan_json,
    encode_multi_root_plan_payload,
    encodeMultiRootPlanJson,
    "Encodes a multi-root plan payload in the versioned compatibility envelope."
);

compat_json!(
    decode_multi_root_plan_json,
    decode_multi_root_plan_payload,
    decodeMultiRootPlanJson,
    "Decodes a versioned multi-root plan envelope to its canonical payload."
);

compat_json!(
    encode_multi_root_candidate_json,
    encode_multi_root_candidate_payload,
    encodeMultiRootCandidateJson,
    "Encodes a multi-root candidate payload in the compatibility envelope."
);

compat_json!(
    decode_multi_root_candidate_json,
    decode_multi_root_candidate_payload,
    decodeMultiRootCandidateJson,
    "Decodes a multi-root candidate envelope to its canonical payload."
);

compat_json!(
    encode_publication_json,
    encode_publication_payload,
    encodePublicationJson,
    "Encodes a publication payload in the versioned compatibility envelope."
);

compat_json!(
    decode_publication_json,
    decode_publication_payload,
    decodePublicationJson,
    "Decodes a versioned publication envelope to its canonical payload."
);

#[derive(Serialize, Tsify)]
#[serde(rename_all = "kebab-case")]
pub(in crate::bindings) enum BrowserTransactionConflictRegion {
    FileRecord,
    Metadata,
    FileLength,
    ContentRange,
    SparseSeek,
    DirectoryName,
    DirectoryRange,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "kebab-case")]
pub(in crate::bindings) enum BrowserTransactionDependencyUse {
    Observation,
    Mutation,
    ObservationAndMutation,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "kebab-case")]
pub(in crate::bindings) enum BrowserTransactionSparseSeek {
    Data,
    Hole,
}

#[derive(Deserialize, Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserForkOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::bindings) paths: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) idempotency_key: Option<Vec<u8>>,
}

pub(in crate::bindings) fn browser_fork_options<A, O>(
    generation: Generation<A, O>,
    key: IdempotencyKey,
    paths: Option<Vec<String>>,
) -> ForkOptions<A, O> {
    let options = ForkOptions::from_generation(generation, key);
    match paths {
        Some(paths) => options.inherit_paths(paths),
        None => options,
    }
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserAdmissionResult {
    #[tsify(type = "\"admitted\" | \"rejected\"")]
    pub(in crate::bindings) status: &'static str,
    #[tsify(type = "string | undefined")]
    pub(in crate::bindings) rejection: Option<&'static str>,
}

#[derive(Deserialize)]
pub(in crate::bindings) struct BrowserObjectCacheOptions {
    #[serde(rename = "maximumEntries")]
    pub(in crate::bindings) entries: u32,
    #[serde(rename = "maximumBytes")]
    pub(in crate::bindings) bytes: u64,
    #[serde(rename = "maximumInFlight")]
    pub(in crate::bindings) in_flight: u32,
    #[serde(rename = "maximumWaitersPerObject")]
    pub(in crate::bindings) waiters_per_object: u32,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserWorkCounters {
    pub(in crate::bindings) authority_records_read: u64,
    pub(in crate::bindings) authority_records_appended: u64,
    pub(in crate::bindings) authority_bytes_read: u64,
    pub(in crate::bindings) authority_bytes_written: u64,
    pub(in crate::bindings) object_probes: u64,
    pub(in crate::bindings) backend_read_operations: u64,
    pub(in crate::bindings) backend_write_operations: u64,
    pub(in crate::bindings) durability_operations: u64,
    pub(in crate::bindings) page_reads: u64,
    pub(in crate::bindings) page_writes: u64,
    pub(in crate::bindings) object_bytes_read: u64,
    pub(in crate::bindings) object_bytes_written: u64,
    pub(in crate::bindings) bytes_hashed: u64,
    pub(in crate::bindings) bytes_copied: u64,
    pub(in crate::bindings) bytes_encoded: u64,
    pub(in crate::bindings) source_bytes_read: u64,
    pub(in crate::bindings) source_path_components: u64,
    pub(in crate::bindings) source_entries_visited: u64,
    pub(in crate::bindings) output_bytes: u64,
    pub(in crate::bindings) items_examined: u64,
    pub(in crate::bindings) items_returned: u64,
    pub(in crate::bindings) allocation_operations: u64,
    pub(in crate::bindings) peak_allocation_bytes: u64,
    pub(in crate::bindings) materializations: u64,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserCheckpointResult {
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) generation_id: Vec<u8>,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct NameComponentResult {
    #[tsify(type = "\"utf8\" | \"posix-bytes\" | \"windows-utf16le\"")]
    pub(in crate::bindings) encoding: &'static str,
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) bytes: Vec<u8>,
}

pub(in crate::bindings) fn js_error(error: impl std::fmt::Display) -> JsValue {
    browser_error("AcyclicFilesystemError", &error.to_string())
}

pub(in crate::bindings) fn browser_error(name: &str, message: &str) -> JsValue {
    let error = js_sys::Error::new(message);
    error.set_name(name);
    error.into()
}

pub(in crate::bindings) fn browser_path(
    path: &str,
    config: VolumeConfig,
) -> Result<NamespacePath, JsValue> {
    let portable = PortablePath::parse(path, config.limits).map_err(js_error)?;
    NamespacePath::from_portable_in_profile(&portable, config.profile, config.limits)
        .map_err(js_error)
}

pub(in crate::bindings) fn browser_name(
    name: &str,
    config: VolumeConfig,
) -> Result<LogicalName, JsValue> {
    let path = browser_path(&format!("/{name}"), config)?;
    match path.components() {
        [name] => Ok(name.clone()),
        _ => Err(js_error("invalid directory cursor")),
    }
}

pub(in crate::bindings) fn browser_decode_limits(limits: VolumeLimits) -> DecodeLimits {
    DecodeLimits {
        maximum_object_bytes: limits.maximum_object_bytes,
        maximum_name_bytes: limits.maximum_component_bytes,
        maximum_page_items: limits.maximum_directory_page_entries,
        maximum_page_bytes: u32::try_from(limits.maximum_object_bytes).unwrap_or(u32::MAX),
        maximum_page_height: limits.maximum_page_height,
        maximum_visited_pages: u32::try_from(limits.maximum_objects_per_generation)
            .unwrap_or(u32::MAX),
    }
}

pub(in crate::bindings) fn fixed_16(bytes: &[u8]) -> Result<[u8; 16], JsValue> {
    bytes
        .try_into()
        .map_err(|_| js_error("identity must be exactly 16 bytes"))
}

pub(in crate::bindings) fn wasm_u32(value: &JsValue) -> Result<u32, JsValue> {
    let number = value
        .as_f64()
        .ok_or_else(|| js_error("expected a JavaScript number"))?;
    acyclic_fs::exact_u32_from_f64(number).map_err(js_error)
}

pub(in crate::bindings) fn fixed_32(bytes: &[u8], label: &str) -> Result<[u8; 32], JsValue> {
    bytes
        .try_into()
        .map_err(|_| js_error(format_args!("{label} must be exactly 32 bytes")))
}

pub(in crate::bindings) fn closed_error() -> JsValue {
    js_error("browser filesystem is closed")
}

pub(in crate::bindings) fn encode_object_id(value: ObjectId) -> Vec<u8> {
    value.to_bytes().to_vec()
}

pub(in crate::bindings) fn storage_tier(value: &str) -> Result<StorageTier, JsValue> {
    StorageTier::from_public_str(value).ok_or_else(|| js_error("unknown speculation storage tier"))
}

pub(in crate::bindings) fn decode_object_id(bytes: &[u8]) -> Result<ObjectId, JsValue> {
    ObjectId::try_from(bytes).map_err(js_error)
}

pub(in crate::bindings) fn encode_checkpoint_result(
    receipt: &acyclic_fs::FsReceipt<acyclic_fs::GenerationId>,
) -> Result<Ts<BrowserCheckpointResult>, JsValue> {
    ts(BrowserCheckpointResult {
        generation_id: receipt.value.digest().into_bytes().to_vec(),
        work: browser_work(receipt.work),
    })
}

pub(in crate::bindings) fn encode_tree_entry(entry: &TreeEntry) -> BrowserTreeEntryResult {
    BrowserTreeEntryResult {
        name: encode_name_component(&entry.name),
        file_id: entry.file_id.into_bytes().to_vec(),
        file_kind: entry.kind,
    }
}

pub(in crate::bindings) fn encode_name_component(name: &LogicalName) -> NameComponentResult {
    NameComponentResult {
        encoding: name.encoding().as_str(),
        bytes: name.as_bytes().to_vec(),
    }
}

pub(in crate::bindings) fn empty_special_kind(kind: &str) -> Result<FileKind, JsValue> {
    match kind {
        "fifo" => Ok(FileKind::Fifo),
        "socket" => Ok(FileKind::Socket),
        "mount-boundary" => Ok(FileKind::MountBoundary),
        _ => Err(js_error("unknown empty special-file kind")),
    }
}

pub(in crate::bindings) fn device_kind(kind: &str) -> Result<FileKind, JsValue> {
    match kind {
        "character-device" => Ok(FileKind::CharacterDevice),
        "block-device" => Ok(FileKind::BlockDevice),
        _ => Err(js_error("unknown device kind")),
    }
}
