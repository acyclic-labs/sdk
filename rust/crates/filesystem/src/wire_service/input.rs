fn metadata(value: Option<wire::Metadata>) -> Result<FileMetadata, Status> {
    let value = value.unwrap_or_default();
    if value.has_named_attributes || value.has_acl || value.has_security_descriptor {
        return Err(Status::invalid_argument(
            "opaque metadata bodies are required separately",
        ));
    }
    Ok(FileMetadata {
        posix_mode: input_u32(value.posix_mode)?,
        posix_uid: input_u32(value.posix_uid)?,
        posix_gid: input_u32(value.posix_gid)?,
        posix_flags: input_u64(value.posix_flags)?,
        windows_attributes: input_u32(value.windows_attributes)?,
        created_ns: input_i64(value.created_ns)?,
        modified_ns: input_i64(value.modified_ns)?,
        accessed_ns: input_i64(value.accessed_ns)?,
        changed_ns: input_i64(value.changed_ns)?,
        named_attributes: MetadataField::Unavailable,
        acl: MetadataField::Unavailable,
        security_descriptor: MetadataField::Unavailable,
    })
}

fn input_u32(value: Option<wire::OptionalU32>) -> Result<MetadataField<u32>, Status> {
    match value.and_then(|value| value.value) {
        None | Some(wire::optional_u32::Value::Unavailable(true)) => Ok(MetadataField::Unavailable),
        Some(wire::optional_u32::Value::Present(value)) => Ok(MetadataField::Value(value)),
        Some(wire::optional_u32::Value::Unavailable(false)) => {
            Err(Status::invalid_argument("unavailable marker must be true"))
        }
    }
}

fn input_u64(value: Option<wire::OptionalU64>) -> Result<MetadataField<u64>, Status> {
    match value.and_then(|value| value.value) {
        None | Some(wire::optional_u64::Value::Unavailable(true)) => Ok(MetadataField::Unavailable),
        Some(wire::optional_u64::Value::Present(value)) => Ok(MetadataField::Value(value)),
        Some(wire::optional_u64::Value::Unavailable(false)) => {
            Err(Status::invalid_argument("unavailable marker must be true"))
        }
    }
}

fn input_i64(value: Option<wire::OptionalI64>) -> Result<MetadataField<i64>, Status> {
    match value.and_then(|value| value.value) {
        None | Some(wire::optional_i64::Value::Unavailable(true)) => Ok(MetadataField::Unavailable),
        Some(wire::optional_i64::Value::Present(value)) => Ok(MetadataField::Value(value)),
        Some(wire::optional_i64::Value::Unavailable(false)) => {
            Err(Status::invalid_argument("unavailable marker must be true"))
        }
    }
}

fn profile(value: i32) -> Result<EngineProfile, Status> {
    let value = wire::FilesystemProfile::try_from(value)
        .ok()
        .ok_or_else(|| Status::invalid_argument("filesystem profile is required"))?;
    hosted_contract::profile_from_wire(value)
        .ok_or_else(|| Status::invalid_argument("filesystem profile is required"))
}

fn profile_message(value: EngineProfile) -> i32 {
    hosted_contract::profile(value) as i32
}

fn source_state_message(
    value: HostedSourceState,
) -> (wire::SourceState, wire::SourceInvalidationReason) {
    let none = wire::SourceInvalidationReason::Unspecified;
    match value {
        HostedSourceState::Clean => (wire::SourceState::Clean, none),
        HostedSourceState::PendingCapture => (wire::SourceState::PendingCapture, none),
        HostedSourceState::Conflict => (wire::SourceState::Conflict, none),
        HostedSourceState::Sealed => (wire::SourceState::Sealed, none),
        HostedSourceState::NeedsRescan(reason) => (
            wire::SourceState::NeedsRescan,
            match reason {
                HostedSourceInvalidation::InitialSnapshotRequired => {
                    wire::SourceInvalidationReason::InitialSnapshotRequired
                }
                HostedSourceInvalidation::QueueOverflow => {
                    wire::SourceInvalidationReason::QueueOverflow
                }
                HostedSourceInvalidation::NativeRescanRequired => {
                    wire::SourceInvalidationReason::NativeRescanRequired
                }
                HostedSourceInvalidation::BackendError => {
                    wire::SourceInvalidationReason::BackendError
                }
                HostedSourceInvalidation::UnrepresentablePath => {
                    wire::SourceInvalidationReason::UnrepresentablePath
                }
                HostedSourceInvalidation::AmbiguousRename => {
                    wire::SourceInvalidationReason::AmbiguousRename
                }
                HostedSourceInvalidation::RootChanged => {
                    wire::SourceInvalidationReason::RootChanged
                }
            },
        ),
    }
}

fn operation(value: Option<wire::OperationOptions>) -> Result<IdempotencyKey, Status> {
    let value = required(value, "operation")?;
    Ok(IdempotencyKey::from_bytes(raw_operation_bytes(
        &value.idempotency_key,
    )?))
}

fn raw_operation(value: &[u8]) -> Result<crate::OperationId, Status> {
    Ok(crate::OperationId::from_bytes(raw_operation_bytes(value)?))
}

fn raw_operation_bytes(value: &[u8]) -> Result<[u8; 16], Status> {
    value
        .try_into()
        .map_err(|_| Status::invalid_argument("idempotency_key must contain 16 bytes"))
}

fn generation_id(value: &[u8]) -> Result<GenerationId, Status> {
    let bytes: [u8; 32] = value
        .try_into()
        .map_err(|_| Status::invalid_argument("generation_id must contain 32 bytes"))?;
    Ok(GenerationId::new(Digest::from_bytes(bytes)))
}
