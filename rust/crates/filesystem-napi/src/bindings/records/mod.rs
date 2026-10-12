use super::*;

/// Exact cumulative and resident immutable-object accelerator observations.
#[napi(object)]
pub struct NativeObjectCacheStats {
    /// Complete reads served from resident authenticated bytes.
    pub hits: BigInt,
    /// Authenticated decoded pages reused without canonical decode.
    pub decoded_hits: BigInt,
    /// Reads requiring backing-store work or an in-flight wait.
    pub misses: BigInt,
    /// Reads sharing one concurrent backing-store request.
    pub coalesced_reads: BigInt,
    /// Deterministic least-recently-used removals.
    pub evictions: BigInt,
    /// Current number of resident canonical objects.
    pub resident_entries: BigInt,
    /// Current canonical plus decoded logical bytes.
    pub resident_bytes: BigInt,
    /// Current resident canonical immutable objects.
    pub resident_canonical_objects: BigInt,
    /// Current resident canonical immutable-object bytes.
    pub resident_canonical_bytes: BigInt,
    /// Current resident authenticated decoded pages.
    pub resident_decoded_pages: BigInt,
    /// Current resident decoded-page logical bytes.
    pub resident_decoded_bytes: BigInt,
    /// Current distinct backing-store reads in flight.
    pub in_flight: BigInt,
}

/// Complete no-follow file record and canonical metadata.
#[napi(object)]
pub struct NativeStat {
    /// Whether the path exists.
    pub exists: bool,
    /// Complete path-independent file record when present.
    pub record: Option<NativeFileRecord>,
    /// Complete canonical metadata bytes when present.
    pub metadata_canonical_bytes: Option<Buffer>,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One complete stable file-record read.
#[napi(object)]
pub struct NativeFileRecordRead {
    /// Complete path-independent candidate record.
    pub record: NativeFileRecord,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// Complete path-independent file record crossing the native boundary.
#[napi(object)]
pub struct NativeFileRecord {
    /// Stable 16-byte file identity.
    pub file_id: Buffer,
    /// Canonical file-kind name.
    pub file_kind: FileKind,
    /// Number of namespace bindings referring to this record.
    pub link_count: BigInt,
    /// Canonical 33-byte metadata object identity.
    pub metadata_object: Buffer,
    /// Canonical payload variant name.
    pub payload_kind: FilePayloadKind,
    /// Logical payload byte length when the variant has one.
    pub logical_bytes: Option<BigInt>,
    /// Canonical 33-byte payload object identity when externally stored.
    pub payload_object: Option<Buffer>,
    /// Inline regular-file bytes when present.
    pub inline_bytes: Option<Buffer>,
    /// Device major number for device records.
    pub device_major: Option<u32>,
    /// Device minor number for device records.
    pub device_minor: Option<u32>,
}

/// One file-record generation change.
#[napi(object)]
pub struct NativeFileRecordChange {
    /// Stable 16-byte file identity.
    pub file_id: Buffer,
    /// Record before the change, absent for creation.
    pub before: Option<NativeFileRecord>,
    /// Record after the change, absent for deletion.
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
