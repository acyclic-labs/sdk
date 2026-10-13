use super::*;

/// Exact cumulative and resident immutable-object accelerator observations.
#[napi(object)]
pub struct NativeObjectCacheStats {
    pub hits: BigInt,
    pub decoded_hits: BigInt,
    pub misses: BigInt,
    pub coalesced_reads: BigInt,
    pub evictions: BigInt,
    pub resident_entries: BigInt,
    pub resident_bytes: BigInt,
    pub resident_canonical_objects: BigInt,
    pub resident_canonical_bytes: BigInt,
    pub resident_decoded_pages: BigInt,
    pub resident_decoded_bytes: BigInt,
    pub in_flight: BigInt,
}

/// Complete no-follow file record and canonical metadata.
#[napi(object)]
pub struct NativeStat {
    pub exists: bool,
    pub record: Option<NativeFileRecord>,
    pub metadata_canonical_bytes: Option<Buffer>,
    pub work_json: String,
}

/// One complete stable file-record read.
#[napi(object)]
pub struct NativeFileRecordRead {
    pub record: NativeFileRecord,
    pub work_json: String,
}

/// Complete path-independent file record crossing the native boundary.
#[napi(object)]
pub struct NativeFileRecord {
    pub file_id: Buffer,
    pub file_kind: FileKind,
    pub link_count: BigInt,
    pub metadata_object: Buffer,
    pub payload_kind: FilePayloadKind,
    pub logical_bytes: Option<BigInt>,
    pub payload_object: Option<Buffer>,
    pub inline_bytes: Option<Buffer>,
    pub device_major: Option<u32>,
    pub device_minor: Option<u32>,
}

/// One file-record generation change.
#[napi(object)]
pub struct NativeFileRecordChange {
    pub file_id: Buffer,
    pub before: Option<NativeFileRecord>,
    pub after: Option<NativeFileRecord>,
}

/// Locks shared binding state, failing closed if a holder panicked mid-update.
pub(in crate::bindings) fn lock_state<'a, T>(
    mutex: &'a std::sync::Mutex<T>,
    state: &str,
) -> Result<std::sync::MutexGuard<'a, T>> {
    mutex
        .lock()
        .map_err(|_| Error::new(Status::GenericFailure, format!("{state} is poisoned")))
}

pub(in crate::bindings) fn encode_file_record(record: FileRecord) -> NativeFileRecord {
    let payload_kind = record.payload.kind();
    let mut result = NativeFileRecord {
        file_id: Buffer::from(record.file_id.into_bytes().to_vec()),
        file_kind: record.kind,
        link_count: bigint(record.link_count),
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
            result.logical_bytes = Some(bigint(u64::try_from(bytes.as_bytes().len()).unwrap_or(0)));
            result.inline_bytes = Some(Buffer::from(bytes.as_bytes().to_vec()));
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
            result.logical_bytes = Some(bigint(logical_bytes));
            result.payload_object = Some(encode_object_id(object));
        }
        FilePayload::Directory { entries } => {
            result.payload_object = Some(encode_object_id(entries));
        }
        FilePayload::Empty => {}
        FilePayload::Device { major, minor } => {
            result.device_major = Some(major);
            result.device_minor = Some(minor);
        }
    }
    result
}
