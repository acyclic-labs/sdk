use super::*;

/// A JavaScript number accepted at the N-API boundary as an exact `u32`.
#[derive(Clone, Copy)]
#[napi(transparent, object_from_js = false, object_to_js = false)]
pub struct NapiU32(pub(in crate::bindings) u32);

impl NapiU32 {
    pub(in crate::bindings) const fn into_inner(self) -> u32 {
        self.0
    }
}

pub(in crate::bindings) fn validate_napi_u32(number: f64) -> Result<u32> {
    acyclic_fs::exact_u32_from_f64(number)
        .map_err(|message| Error::new(Status::InvalidArg, message))
}

#[allow(
    unsafe_code,
    reason = "N-API trait hooks require a narrow unsafe boundary"
)]
mod napi_u32_codec {
    //! N-API supplies live handles; the maintained codecs perform conversion.

    use super::{NapiU32, Result, validate_napi_u32};
    use napi::bindgen_prelude::{FromNapiValue, ToNapiValue};
    use napi::{JsNumber, Unknown, ValueType};

    impl FromNapiValue for NapiU32 {
        /// # Safety
        unsafe fn from_napi_value(
            env: napi::sys::napi_env,
            value: napi::sys::napi_value,
        ) -> Result<Self> {
            // SAFETY: N-API provided the live environment and value handles.
            let unknown = unsafe { <Unknown as FromNapiValue>::from_napi_value(env, value)? };
            if unknown.get_type()? != ValueType::Number {
                return Err(napi::Error::new(
                    napi::Status::InvalidArg,
                    "expected a JavaScript number",
                ));
            }
            // SAFETY: the value type was checked as Number immediately above.
            let number = unsafe { unknown.cast::<JsNumber>()? }.get_double()?;
            Ok(Self(validate_napi_u32(number)?))
        }
    }

    impl ToNapiValue for NapiU32 {
        /// # Safety
        unsafe fn to_napi_value(
            env: napi::sys::napi_env,
            value: Self,
        ) -> Result<napi::sys::napi_value> {
            // SAFETY: N-API supplied the live environment for this export hook.
            unsafe { <u32 as ToNapiValue>::to_napi_value(env, value.0) }
        }
    }
}

/// Serialize exact work counters as decimal strings so native JSON never
pub(in crate::bindings) fn work_json(work: &acyclic_fs::WorkCounters) -> Result<String> {
    let mut value = serde_json::to_value(work).map_err(napi_error)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| napi_error("work counters must serialize as an object"))?;
    for (field, value) in object {
        let number = value
            .as_u64()
            .ok_or_else(|| napi_error(format!("work counter {field} must serialize as u64")))?;
        *value = serde_json::Value::String(number.to_string());
    }
    serde_json::to_string(&value).map_err(napi_error)
}

compat_json!(
    encode_multi_root_plan_json,
    encode_multi_root_plan_payload,
    "Encodes a multi-root plan payload in the versioned compatibility envelope."
);

compat_json!(
    decode_multi_root_plan_json,
    decode_multi_root_plan_payload,
    "Decodes a versioned multi-root plan envelope to its canonical payload."
);

compat_json!(
    encode_multi_root_candidate_json,
    encode_multi_root_candidate_payload,
    "Encodes a multi-root candidate payload in the compatibility envelope."
);

compat_json!(
    decode_multi_root_candidate_json,
    decode_multi_root_candidate_payload,
    "Decodes a multi-root candidate envelope to its canonical payload."
);

compat_json!(
    encode_publication_json,
    encode_publication_payload,
    "Encodes a publication payload in the versioned compatibility envelope."
);

compat_json!(
    decode_publication_json,
    decode_publication_payload,
    "Decodes a versioned publication envelope to its canonical payload."
);

/// Exact hard limits for the process-local immutable-object accelerator.
#[napi(object)]
pub struct NativeObjectCacheOptions {
    /// Maximum resident immutable objects.
    pub maximum_entries: NapiU32,
    /// Maximum resident canonical object bytes.
    pub maximum_bytes: BigInt,
    /// Maximum distinct backing-store reads in flight.
    pub maximum_in_flight: NapiU32,
    /// Maximum followers retained behind one in-flight read.
    pub maximum_waiters_per_object: NapiU32,
}

/// One exact namespace component crossing the native binding boundary.
#[napi(object)]
pub struct NativePathComponent {
    /// `utf8`, `posix-bytes`, or `windows-utf16le`.
    pub encoding: String,
    /// Exact component bytes in the declared representation.
    pub bytes: Buffer,
}

/// One exact absolute volume path; an empty vector denotes the root.
#[napi(object)]
pub struct NativeNamespacePath {
    /// Root-to-leaf exact components.
    pub components: Vec<NativePathComponent>,
}

/// Immutable content-addressed candidate built without publishing authority.
#[napi(object)]
pub struct NativeCheckpointResult {
    /// Stable 32-byte generation identity.
    pub generation_id: Buffer,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

#[derive(Clone, Copy)]
pub(in crate::bindings) struct PendingReconcile {
    pub(in crate::bindings) operation_id: OperationId,
    pub(in crate::bindings) epoch: u64,
    pub(in crate::bindings) baseline: CaptureReceipt,
    pub(in crate::bindings) work: acyclic_fs::WorkCounters,
}

/// Explicit immutable inheritance and stable retry identity.
#[napi(object)]
pub struct NativeForkOptions {
    /// Absent inherits all; empty inherits no paths while preserving lineage.
    pub paths: Option<Vec<String>>,
    /// Exact caller-owned stable creation key.
    pub idempotency_key: Option<Buffer>,
}

/// One exact transaction dependency conflict.
#[napi(object)]
#[allow(
    missing_docs,
    reason = "plain N-API object whose fields the struct doc describes as a whole"
)]
pub struct NativeTransactionConflict {
    pub region: String,
    pub file_id: Option<Buffer>,
    pub directory_id: Option<Buffer>,
    pub offset: Option<BigInt>,
    pub length: Option<BigInt>,
    pub sparse_target: Option<String>,
    pub name: Option<NativeWorkspaceName>,
    pub maximum_entries: Option<u32>,
    pub usage: String,
    pub expected: Option<Buffer>,
    pub actual: Option<Buffer>,
}

pub(in crate::bindings) fn native_idempotency_key(value: Option<Buffer>) -> Result<IdempotencyKey> {
    value.map_or_else(
        || Ok(IdempotencyKey::new()),
        |value| {
            value
                .as_ref()
                .try_into()
                .map(IdempotencyKey::from_bytes)
                .map_err(|_| {
                    Error::new(
                        Status::InvalidArg,
                        "idempotency key must be exactly 16 bytes",
                    )
                })
        },
    )
}

pub(in crate::bindings) const fn operation_reconcile_limits(
    options: NativeOperationReconcileOptions,
) -> OperationReconcileLimits {
    OperationReconcileLimits {
        maximum_generations: options.maximum_generations.into_inner(),
        maximum_changes: options.maximum_changes.into_inner(),
        maximum_conflicts: options.maximum_conflicts.into_inner(),
    }
}

pub(in crate::bindings) fn native_reconcile_outcome(
    outcome: ReconcileOutcome<LocalAuthorityBackend, LocalObjectBackend>,
) -> NativeSourceResult {
    match outcome {
        ReconcileOutcome::Clean(generation) => NativeSourceResult {
            status: "clean".to_owned(),
            reason: None,
            generation_id: Some(Buffer::from(generation.id().digest().into_bytes().to_vec())),
        },
        ReconcileOutcome::NeedsRescan(reason) => NativeSourceResult {
            status: "needs-rescan".to_owned(),
            reason: Some(watch_reason(reason).to_owned()),
            generation_id: None,
        },
        ReconcileOutcome::Conflict => NativeSourceResult {
            status: "conflict".to_owned(),
            reason: None,
            generation_id: None,
        },
    }
}

pub(in crate::bindings) fn napi_error(error: impl std::fmt::Display) -> Error {
    Error::new(Status::GenericFailure, error.to_string())
}

pub(in crate::bindings) fn napi_wire_error(error: impl std::fmt::Display) -> Error {
    Error::new(Status::InvalidArg, error.to_string())
}

pub(in crate::bindings) fn ensure_no_pending_interval(
    pending: &std::sync::Mutex<Option<PendingWatchBatch>>,
) -> Result<()> {
    if lock_state(pending, "native watcher state")?.is_some() {
        return Err(napi_error(
            "watch interval must be durably acknowledged before reconciliation",
        ));
    }
    Ok(())
}

pub(in crate::bindings) fn fixed_16(bytes: &[u8]) -> Result<[u8; 16]> {
    bytes.try_into().map_err(|_| {
        Error::new(
            Status::InvalidArg,
            "volume identity must be exactly 16 bytes",
        )
    })
}

pub(in crate::bindings) fn fixed_32(bytes: &[u8], label: &str) -> Result<[u8; 32]> {
    bytes.try_into().map_err(|_| {
        Error::new(
            Status::InvalidArg,
            format!("{label} must be exactly 32 bytes"),
        )
    })
}

pub(in crate::bindings) fn generation_id(bytes: &[u8]) -> Result<acyclic_fs::GenerationId> {
    Ok(acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
        bytes,
        "generation identity",
    )?)))
}

pub(in crate::bindings) fn publication_permit(
    lease: Option<&NativeOperationWindowLease>,
) -> Result<acyclic_fs::PublicationPermit> {
    Ok(lease
        .map(operation_window_lease)
        .transpose()?
        .map_or(acyclic_fs::PublicationPermit::Unrestricted, |lease| {
            lease.publication_permit()
        }))
}

pub(in crate::bindings) fn native_generation_buffer(generation: GenerationId) -> Buffer {
    Buffer::from(generation.digest().into_bytes().to_vec())
}

pub(in crate::bindings) fn encode_object_id(value: ObjectId) -> Buffer {
    Buffer::from(value.to_bytes().to_vec())
}

pub(in crate::bindings) fn encode_tree_entry(entry: &TreeEntry) -> NativeTreeEntry {
    NativeTreeEntry {
        name: encode_name_component(&entry.name),
        file_id: Buffer::from(entry.file_id.into_bytes().to_vec()),
        file_kind: entry.kind,
    }
}

pub(in crate::bindings) fn encode_name_component(name: &LogicalName) -> NativePathComponent {
    NativePathComponent {
        encoding: name.encoding().as_str().to_owned(),
        bytes: Buffer::from(name.as_bytes().to_vec()),
    }
}

pub(in crate::bindings) fn decode_object_id(bytes: &[u8]) -> Result<ObjectId> {
    ObjectId::try_from(bytes).map_err(napi_wire_error)
}

pub(in crate::bindings) fn native_storage_tier(value: &str) -> Result<StorageTier> {
    StorageTier::from_public_str(value)
        .ok_or_else(|| Error::new(Status::InvalidArg, "unknown speculation storage tier"))
}

pub(in crate::bindings) fn encode_checkpoint_receipt(
    receipt: &acyclic_fs::FsReceipt<acyclic_fs::GenerationId>,
) -> Result<NativeCheckpointResult> {
    Ok(NativeCheckpointResult {
        generation_id: Buffer::from(receipt.value.digest().into_bytes().to_vec()),
        work_json: work_json(&receipt.work)?,
    })
}

pub(in crate::bindings) fn encode_namespace_path(path: &NamespacePath) -> NativeNamespacePath {
    NativeNamespacePath {
        components: path
            .components()
            .iter()
            .map(|component| NativePathComponent {
                encoding: component.encoding().as_str().to_owned(),
                bytes: Buffer::from(component.as_bytes().to_vec()),
            })
            .collect(),
    }
}

pub(in crate::bindings) fn native_path(path: &str, config: VolumeConfig) -> Result<NamespacePath> {
    let portable = PortablePath::parse(path, config.limits).map_err(napi_error)?;
    NamespacePath::from_portable_in_profile(&portable, config.profile, config.limits)
        .map_err(napi_error)
}

pub(in crate::bindings) fn native_name(name: &str, config: VolumeConfig) -> Result<LogicalName> {
    let path = native_path(&format!("/{name}"), config)?;
    match path.components() {
        [name] => Ok(name.clone()),
        _ => Err(Error::new(Status::InvalidArg, "invalid directory cursor")),
    }
}

pub(in crate::bindings) fn required<T>(value: Option<T>, field: &str) -> Result<T> {
    value.ok_or_else(|| Error::new(Status::InvalidArg, format!("transaction requires {field}")))
}

pub(in crate::bindings) fn required_bigint(value: Option<BigInt>, field: &str) -> Result<u64> {
    bigint_u64(&required(value, field)?, field)
}

pub(in crate::bindings) fn native_decode_limits(limits: VolumeLimits) -> DecodeLimits {
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

pub(in crate::bindings) fn bigint_u64(value: &BigInt, field: &str) -> Result<u64> {
    let (negative, value, lossless) = value.get_u64();
    if negative || !lossless {
        return Err(Error::new(
            Status::InvalidArg,
            format!("{field} must be a lossless unsigned 64-bit integer"),
        ));
    }
    Ok(value)
}

pub(in crate::bindings) fn u16_from_u32(value: u32, field: &str) -> Result<u16> {
    u16::try_from(value).map_err(|_| {
        Error::new(
            Status::InvalidArg,
            format!("{field} must fit an unsigned 16-bit integer"),
        )
    })
}

pub(in crate::bindings) fn bigint(value: u64) -> BigInt {
    BigInt {
        sign_bit: false,
        words: vec![value],
    }
}

pub(in crate::bindings) fn live_outcome_resolved(outcome: &LiveMutationOutcome) -> bool {
    matches!(
        outcome,
        LiveMutationOutcome::Committed { .. }
            | LiveMutationOutcome::AlreadyCommitted { .. }
            | LiveMutationOutcome::IdempotencyConflict { .. }
    )
}

pub(in crate::bindings) fn signed_bigint(value: i64) -> BigInt {
    BigInt {
        sign_bit: value.is_negative(),
        words: vec![value.unsigned_abs()],
    }
}

pub(in crate::bindings) fn empty_special_kind(kind: &str) -> Result<FileKind> {
    match kind {
        "fifo" => Ok(FileKind::Fifo),
        "socket" => Ok(FileKind::Socket),
        "mount-boundary" => Ok(FileKind::MountBoundary),
        _ => Err(Error::new(
            Status::InvalidArg,
            "unknown empty special-file kind",
        )),
    }
}

pub(in crate::bindings) fn device_kind(kind: &str) -> Result<FileKind> {
    match kind {
        "character-device" => Ok(FileKind::CharacterDevice),
        "block-device" => Ok(FileKind::BlockDevice),
        _ => Err(Error::new(Status::InvalidArg, "unknown device kind")),
    }
}
