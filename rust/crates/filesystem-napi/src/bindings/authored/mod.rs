use super::*;

/// One strict discriminated operation in an atomic within-volume transaction.
#[napi(object)]
pub struct NativeTransactionOperation {
    pub kind: String,
    pub path: Option<String>,
    pub source: Option<String>,
    pub destination: Option<String>,
    pub bytes: Option<Buffer>,
    pub target: Option<Buffer>,
    pub payload: Option<Buffer>,
    pub expected_file_id: Option<Buffer>,
    pub file_kind: Option<String>,
    pub offset: Option<BigInt>,
    pub source_offset: Option<BigInt>,
    pub destination_offset: Option<BigInt>,
    pub length: Option<BigInt>,
    pub logical_bytes: Option<BigInt>,
    pub major: Option<NapiU32>,
    pub minor: Option<NapiU32>,
    pub replace: Option<bool>,
    pub allocated: Option<bool>,
    pub extend: Option<bool>,
    pub keep_size: Option<bool>,
    pub canonical_bytes: Option<Buffer>,
}

/// Stable result positions for one atomic transaction.
#[napi(object)]
pub struct NativeTransactionResult {
    pub created_file_ids: Vec<Option<Buffer>>,
    pub work_json: String,
}

/// One direct-live authored transaction result with stable create positions.
#[napi(object)]
pub struct NativeAuthoredLiveMutationResult {
    pub created_file_ids: Vec<Option<Buffer>>,
    pub status: String,
    pub generation_id: Option<Buffer>,
    pub epoch: Option<BigInt>,
    pub sequence: Option<BigInt>,
    pub conflict_count: u32,
    pub truncated: bool,
    pub committed_fingerprint: Option<Buffer>,
    pub work_json: String,
}

#[napi]
impl NativeCheckout {
    #[napi]
    pub async fn apply_transaction(
        &self,
        operations: Vec<NativeTransactionOperation>,
    ) -> Result<NativeTransactionResult> {
        let maximum =
            usize::try_from(self.config.limits.maximum_mutations_per_batch).unwrap_or(usize::MAX);
        if operations.len() > maximum || operations.capacity() > maximum {
            return Err(Error::new(
                Status::InvalidArg,
                "transaction exceeds the configured mutation bound",
            ));
        }
        let authored = operations
            .into_iter()
            .map(|operation| native_authored_transaction(operation, self.config))
            .collect::<Result<Vec<_>>>()?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = Box::pin(checkout.apply_authored_transaction(
            authored,
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &self.cancellation,
        ))
        .await
        .map_err(napi_error)?;
        Ok(NativeTransactionResult {
            created_file_ids: receipt
                .value
                .created_file_ids
                .into_iter()
                .map(|identity| identity.map(|value| Buffer::from(value.into_bytes().to_vec())))
                .collect(),
            work_json: work_json(&receipt.work)?,
        })
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one exhaustive match lowers every native transaction operation"
)]
pub(in crate::bindings) fn native_authored_transaction(
    operation: NativeTransactionOperation,
    config: VolumeConfig,
) -> Result<AuthoredMutation> {
    let NativeTransactionOperation {
        kind,
        path,
        source,
        destination,
        bytes,
        target,
        payload,
        expected_file_id,
        file_kind,
        offset,
        source_offset,
        destination_offset,
        length,
        logical_bytes,
        major,
        minor,
        replace,
        allocated,
        extend,
        keep_size,
        canonical_bytes,
    } = operation;
    let limits = config.limits;
    let metadata = FileMetadata::default();
    Ok(match kind.as_str() {
        "create-file" => AuthoredMutation::CreateFile {
            path: native_path(&required(path, "path")?, config)?,
            bytes: required(bytes, "bytes")?.to_vec().into(),
            metadata,
        },
        "create-directory" => AuthoredMutation::CreateDirectory {
            path: native_path(&required(path, "path")?, config)?,
            metadata,
        },
        "create-symbolic-link" => AuthoredMutation::CreateSymbolicLink {
            path: native_path(&required(path, "path")?, config)?,
            target: required(target, "target")?.to_vec().into(),
            metadata,
        },
        "create-special" => AuthoredMutation::CreateEmptySpecial {
            path: native_path(&required(path, "path")?, config)?,
            kind: empty_special_kind(&required(file_kind, "fileKind")?)?,
            metadata,
        },
        "create-device" => AuthoredMutation::CreateDevice {
            path: native_path(&required(path, "path")?, config)?,
            kind: device_kind(&required(file_kind, "fileKind")?)?,
            major: required(major.map(NapiU32::into_inner), "major")?,
            minor: required(minor.map(NapiU32::into_inner), "minor")?,
            metadata,
        },
        "create-reparse-point" => AuthoredMutation::CreateReparsePoint {
            path: native_path(&required(path, "path")?, config)?,
            payload: required(payload, "payload")?.to_vec().into(),
            metadata,
        },
        "remove" => AuthoredMutation::Remove {
            path: native_path(&required(path, "path")?, config)?,
            expected_file_id: expected_file_id
                .as_deref()
                .map(fixed_16)
                .transpose()?
                .map(FileId::from_bytes),
        },
        "rename" => AuthoredMutation::Rename {
            source: native_path(&required(source, "source")?, config)?,
            destination: native_path(&required(destination, "destination")?, config)?,
            replace: required(replace, "replace")?,
        },
        "hard-link" => AuthoredMutation::HardLink {
            source: native_path(&required(source, "source")?, config)?,
            destination: native_path(&required(destination, "destination")?, config)?,
        },
        "write" => AuthoredMutation::Write {
            path: native_path(&required(path, "path")?, config)?,
            offset: required_bigint(offset, "offset")?,
            bytes: required(bytes, "bytes")?.to_vec().into(),
        },
        "set-metadata" => AuthoredMutation::SetMetadata {
            path: native_path(&required(path, "path")?, config)?,
            metadata: decode_file_metadata(
                &required(canonical_bytes, "canonicalBytes")?,
                native_decode_limits(limits),
                &mut |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
            )
            .map_err(napi_error)?,
        },
        "resize" => AuthoredMutation::Resize {
            path: native_path(&required(path, "path")?, config)?,
            logical_bytes: required_bigint(logical_bytes, "logicalBytes")?,
        },
        "zero-range" => AuthoredMutation::ZeroRange {
            path: native_path(&required(path, "path")?, config)?,
            range: ByteRange {
                offset: required_bigint(offset, "offset")?,
                length: required_bigint(length, "length")?,
            },
            allocated: required(allocated, "allocated")?,
            extend: required(extend, "extend")?,
        },
        "preallocate" => AuthoredMutation::Preallocate {
            path: native_path(&required(path, "path")?, config)?,
            range: ByteRange {
                offset: required_bigint(offset, "offset")?,
                length: required_bigint(length, "length")?,
            },
            keep_size: required(keep_size, "keepSize")?,
        },
        "clone-range" => AuthoredMutation::CloneRange(FileCloneRequest {
            source: native_path(&required(source, "source")?, config)?,
            source_offset: required_bigint(source_offset, "sourceOffset")?,
            destination: native_path(&required(destination, "destination")?, config)?,
            destination_offset: required_bigint(destination_offset, "destinationOffset")?,
            length: required_bigint(length, "length")?,
        }),
        _ => {
            return Err(Error::new(
                Status::InvalidArg,
                "unknown transaction operation",
            ));
        }
    })
}
