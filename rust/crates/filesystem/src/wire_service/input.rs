fn metadata(value: Option<wire::Metadata>) -> Result<FileMetadata, Status> {
    crate::binding_contract::metadata_from_proto(value).map_err(Status::invalid_argument)
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
    GenerationId::from_bytes(value).ok_or_else(|| {
        Status::invalid_argument("generation_id must be a canonical generation-root reference")
    })
}
