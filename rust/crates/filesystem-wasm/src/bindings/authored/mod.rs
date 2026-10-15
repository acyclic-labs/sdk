use super::*;

#[derive(Deserialize, Serialize, Tsify)]
#[serde(
    tag = "kind",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
#[tsify(large_number_types_as_bigints)]
pub enum TransactionOperation {
    CreateFile {
        path: String,
        #[tsify(type = "Uint8Array")]
        bytes: serde_bytes::ByteBuf,
    },
    CreateDirectory {
        path: String,
    },
    CreateSymbolicLink {
        path: String,
        #[tsify(type = "Uint8Array")]
        target: serde_bytes::ByteBuf,
    },
    CreateSpecial {
        path: String,
        #[tsify(type = "\"fifo\" | \"socket\" | \"mount-boundary\"")]
        file_kind: String,
    },
    CreateDevice {
        path: String,
        #[tsify(type = "\"character-device\" | \"block-device\"")]
        file_kind: String,
        major: u32,
        minor: u32,
    },
    CreateReparsePoint {
        path: String,
        #[tsify(type = "Uint8Array")]
        payload: serde_bytes::ByteBuf,
    },
    Remove {
        path: String,
        #[tsify(type = "Uint8Array | undefined")]
        expected_file_id: Option<serde_bytes::ByteBuf>,
    },
    Rename {
        source: String,
        destination: String,
        replace: bool,
    },
    HardLink {
        source: String,
        destination: String,
    },
    Write {
        path: String,
        offset: u64,
        #[tsify(type = "Uint8Array")]
        bytes: serde_bytes::ByteBuf,
    },
    SetMetadata {
        path: String,
        #[tsify(type = "Uint8Array")]
        canonical_bytes: serde_bytes::ByteBuf,
    },
    Resize {
        path: String,
        logical_bytes: u64,
    },
    ZeroRange {
        path: String,
        offset: u64,
        length: u64,
        allocated: bool,
        extend: bool,
    },
    Preallocate {
        path: String,
        offset: u64,
        length: u64,
        keep_size: bool,
    },
    CloneRange {
        source: String,
        source_offset: u64,
        destination: String,
        destination_offset: u64,
        length: u64,
    },
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserTransactionResult {
    #[tsify(type = "(Uint8Array | undefined)[]")]
    pub(in crate::bindings) created_file_ids: Vec<Option<serde_bytes::ByteBuf>>,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserLiveTransactionResult {
    #[tsify(
        type = "\"committed\" | \"already-committed\" | \"conflicted\" | \"retry-limit\" | \"fenced\" | \"idempotency-conflict\""
    )]
    pub(in crate::bindings) status: &'static str,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) generation_id: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) epoch: Option<u64>,
    pub(in crate::bindings) sequence: Option<u64>,
    pub(in crate::bindings) conflict_count: u32,
    pub(in crate::bindings) truncated: bool,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) committed_fingerprint: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) work: BrowserWorkCounters,
    #[tsify(type = "(Uint8Array | undefined)[]")]
    pub(in crate::bindings) created_file_ids: Vec<Option<serde_bytes::ByteBuf>>,
}

#[wasm_bindgen]
impl BrowserCheckout {
    #[wasm_bindgen(js_name = applyTransaction)]
    pub async fn apply_transaction(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "TransactionOperation[]")] operations: JsValue,
    ) -> Result<Ts<BrowserTransactionResult>, JsValue> {
        let authored = decode_authored_transactions(operations, self.engine.volume_config())?;
        let receipt = browser_call!(self, apply_authored_transaction(authored,))?;
        ts(BrowserTransactionResult {
            created_file_ids: receipt
                .value
                .created_file_ids
                .into_iter()
                .map(|identity| identity.map(|value| value.into_bytes().to_vec().into()))
                .collect(),
            work: browser_work(receipt.work),
        })
    }
}

pub(in crate::bindings) fn decode_authored_transactions(
    operations: JsValue,
    config: VolumeConfig,
) -> Result<Vec<AuthoredMutation>, JsValue> {
    if !js_sys::Array::is_array(&operations) {
        return Err(js_error("transaction operations must be an array"));
    }
    let maximum = config.limits.maximum_mutations_per_batch;
    if js_sys::Array::from(&operations).length() > maximum {
        return Err(js_error(
            "transaction exceeds the configured mutation bound",
        ));
    }
    let decoded: Vec<TransactionOperation> =
        serde_wasm_bindgen::from_value(operations).map_err(js_error)?;
    if decoded.capacity() > usize::try_from(maximum).unwrap_or(usize::MAX) {
        return Err(js_error(
            "transaction exceeds the configured mutation bound",
        ));
    }
    decoded
        .into_iter()
        .map(|operation| authored_transaction(operation, config))
        .collect()
}

#[allow(
    clippy::too_many_lines,
    reason = "one exhaustive match lowers every browser transaction operation"
)]
pub(in crate::bindings) fn authored_transaction(
    operation: TransactionOperation,
    config: VolumeConfig,
) -> Result<AuthoredMutation, JsValue> {
    let metadata = FileMetadata::default();
    Ok(match operation {
        TransactionOperation::CreateFile { path, bytes } => AuthoredMutation::CreateFile {
            path: browser_path(&path, config)?,
            bytes: acyclic_stream::OwnedBytes::from(bytes.into_vec()),
            metadata,
        },
        TransactionOperation::CreateDirectory { path } => AuthoredMutation::CreateDirectory {
            path: browser_path(&path, config)?,
            metadata,
        },
        TransactionOperation::CreateSymbolicLink { path, target } => {
            AuthoredMutation::CreateSymbolicLink {
                path: browser_path(&path, config)?,
                target: acyclic_stream::OwnedBytes::from(target.into_vec()),
                metadata,
            }
        }
        TransactionOperation::CreateSpecial { path, file_kind } => {
            AuthoredMutation::CreateEmptySpecial {
                path: browser_path(&path, config)?,
                kind: empty_special_kind(&file_kind)?,
                metadata,
            }
        }
        TransactionOperation::CreateDevice {
            path,
            file_kind,
            major,
            minor,
        } => AuthoredMutation::CreateDevice {
            path: browser_path(&path, config)?,
            kind: device_kind(&file_kind)?,
            major,
            minor,
            metadata,
        },
        TransactionOperation::CreateReparsePoint { path, payload } => {
            AuthoredMutation::CreateReparsePoint {
                path: browser_path(&path, config)?,
                payload: acyclic_stream::OwnedBytes::from(payload.into_vec()),
                metadata,
            }
        }
        TransactionOperation::Remove {
            path,
            expected_file_id,
        } => AuthoredMutation::Remove {
            path: browser_path(&path, config)?,
            expected_file_id: expected_file_id
                .as_ref()
                .map(serde_bytes::ByteBuf::as_ref)
                .map(fixed_16)
                .transpose()?
                .map(FileId::from_bytes),
        },
        TransactionOperation::Rename {
            source,
            destination,
            replace,
        } => AuthoredMutation::Rename {
            source: browser_path(&source, config)?,
            destination: browser_path(&destination, config)?,
            replace,
        },
        TransactionOperation::HardLink {
            source,
            destination,
        } => AuthoredMutation::HardLink {
            source: browser_path(&source, config)?,
            destination: browser_path(&destination, config)?,
        },
        TransactionOperation::Write {
            path,
            offset,
            bytes,
        } => AuthoredMutation::Write {
            path: browser_path(&path, config)?,
            offset,
            bytes: acyclic_stream::OwnedBytes::from(bytes.into_vec()),
        },
        TransactionOperation::SetMetadata {
            path,
            canonical_bytes,
        } => AuthoredMutation::SetMetadata {
            path: browser_path(&path, config)?,
            metadata: decode_file_metadata(
                canonical_bytes.as_ref(),
                browser_decode_limits(config.limits),
                &mut |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
            )
            .map_err(js_error)?,
        },
        TransactionOperation::Resize {
            path,
            logical_bytes,
        } => AuthoredMutation::Resize {
            path: browser_path(&path, config)?,
            logical_bytes,
        },
        TransactionOperation::ZeroRange {
            path,
            offset,
            length,
            allocated,
            extend,
        } => AuthoredMutation::ZeroRange {
            path: browser_path(&path, config)?,
            range: ByteRange { offset, length },
            allocated,
            extend,
        },
        TransactionOperation::Preallocate {
            path,
            offset,
            length,
            keep_size,
        } => AuthoredMutation::Preallocate {
            path: browser_path(&path, config)?,
            range: ByteRange { offset, length },
            keep_size,
        },
        TransactionOperation::CloneRange {
            source,
            source_offset,
            destination,
            destination_offset,
            length,
        } => AuthoredMutation::CloneRange(FileCloneRequest {
            source: browser_path(&source, config)?,
            source_offset,
            destination: browser_path(&destination, config)?,
            destination_offset,
            length,
        }),
    })
}

pub(in crate::bindings) fn authored_live_mutation_value(
    result: acyclic_fs::AuthoredLiveMutationResult,
    work: acyclic_fs::WorkCounters,
) -> Result<Ts<BrowserLiveTransactionResult>, JsValue> {
    let value = live_mutation_value(result.outcome, work)?;
    ts(BrowserLiveTransactionResult {
        status: value.status,
        generation_id: value.generation_id,
        epoch: value.epoch,
        sequence: value.sequence,
        conflict_count: value.conflict_count,
        truncated: value.truncated,
        committed_fingerprint: value.committed_fingerprint,
        work: value.work,
        created_file_ids: result
            .created_file_ids
            .into_iter()
            .map(|identity| identity.map(|value| value.into_bytes().to_vec().into()))
            .collect(),
    })
}
