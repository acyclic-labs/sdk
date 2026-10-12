use super::*;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::bindings) struct BrowserObjectCacheStats {
    pub(in crate::bindings) hits: String,
    pub(in crate::bindings) decoded_hits: String,
    pub(in crate::bindings) misses: String,
    pub(in crate::bindings) coalesced_reads: String,
    pub(in crate::bindings) evictions: String,
    pub(in crate::bindings) resident_entries: String,
    pub(in crate::bindings) resident_bytes: String,
    pub(in crate::bindings) resident_canonical_objects: String,
    pub(in crate::bindings) resident_canonical_bytes: String,
    pub(in crate::bindings) resident_decoded_pages: String,
    pub(in crate::bindings) resident_decoded_bytes: String,
    pub(in crate::bindings) in_flight: String,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserStatResult {
    pub(in crate::bindings) exists: bool,
    pub(in crate::bindings) record: Option<BrowserFileRecordResult>,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) metadata_canonical_bytes: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserFileRecordReadResult {
    pub(in crate::bindings) record: BrowserFileRecordResult,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserFileRecordResult {
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) file_id: Vec<u8>,
    pub(in crate::bindings) file_kind: FileKind,
    pub(in crate::bindings) link_count: u64,
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) metadata_object: Vec<u8>,
    pub(in crate::bindings) payload_kind: FilePayloadKind,
    pub(in crate::bindings) logical_bytes: Option<u64>,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) payload_object: Option<serde_bytes::ByteBuf>,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) inline_bytes: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) device_major: Option<u32>,
    pub(in crate::bindings) device_minor: Option<u32>,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserFileRecordChangeResult {
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) file_id: Vec<u8>,
    pub(in crate::bindings) before: Option<BrowserFileRecordResult>,
    pub(in crate::bindings) after: Option<BrowserFileRecordResult>,
}

pub(in crate::bindings) fn encode_file_record(record: FileRecord) -> BrowserFileRecordResult {
    let payload_kind = record.payload.kind();
    let mut result = BrowserFileRecordResult {
        file_id: record.file_id.into_bytes().to_vec(),
        file_kind: record.kind,
        link_count: record.link_count,
        metadata_object: encode_object_id(record.metadata),
        payload_kind,
        logical_bytes: None,
        payload_object: None,
        inline_bytes: None,
        device_major: None,
        device_minor: None,
    };
    match record.payload {
        FilePayload::InlineRegular(bytes) => {
            result.logical_bytes = Some(u64::try_from(bytes.as_bytes().len()).unwrap_or(u64::MAX));
            result.inline_bytes = Some(bytes.as_bytes().to_vec().into());
        }
        FilePayload::Regular {
            logical_bytes,
            extents: object,
        }
        | FilePayload::SymbolicLink {
            target_bytes: logical_bytes,
            target: object,
        }
        | FilePayload::ReparsePoint {
            payload_bytes: logical_bytes,
            payload: object,
        } => {
            result.logical_bytes = Some(logical_bytes);
            result.payload_object = Some(encode_object_id(object).into());
        }
        FilePayload::Directory { entries } => {
            result.payload_object = Some(encode_object_id(entries).into());
        }
        FilePayload::Empty => {}
        FilePayload::Device { major, minor } => {
            result.device_major = Some(major);
            result.device_minor = Some(minor);
        }
    }
    result
}
