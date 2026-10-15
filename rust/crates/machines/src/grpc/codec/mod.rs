use super::*;

mod error;
mod image;
mod observation;

pub(super) use error::*;
pub(super) use image::*;
pub(super) use observation::*;

pub(super) fn protocol() -> wire::ProtocolVersion {
    wire::ProtocolVersion {
        major: PROTOCOL_MAJOR,
        minor: PROTOCOL_MINOR,
    }
}
pub(super) fn encode_key(value: IdempotencyKey) -> wire::IdempotencyKey {
    wire::IdempotencyKey {
        value: value.as_bytes().to_vec(),
    }
}
pub(super) fn encode_machine(value: MachineId) -> wire::MachineId {
    wire::MachineId {
        value: value.as_bytes().to_vec(),
    }
}
pub(super) fn encode_checkpoint(value: CheckpointId) -> wire::CheckpointId {
    wire::CheckpointId {
        value: value.as_bytes().to_vec(),
    }
}
pub(super) fn operation_request(value: OperationId) -> wire::OperationRequest {
    wire::OperationRequest {
        protocol: Some(protocol()),
        operation: Some(wire::OperationId {
            value: value.as_bytes().to_vec(),
        }),
    }
}
pub(super) fn decode_uuid(value: &[u8]) -> Result<Uuid, ProviderError> {
    let bytes: [u8; 16] = value
        .try_into()
        .map_err(|_| ProviderError::Rejected("identity width is invalid".into()))?;
    let value = Uuid::from_bytes(bytes);
    if value.is_nil() {
        Err(ProviderError::Rejected("nil identity is invalid".into()))
    } else {
        Ok(value)
    }
}
pub(super) fn decode_operation(
    value: Option<&wire::OperationId>,
) -> Result<OperationId, ProviderError> {
    decode_uuid(
        &value
            .ok_or_else(|| ProviderError::Rejected("operation identity is missing".into()))?
            .value,
    )
    .map(OperationId)
}
pub(super) fn decode_machine(value: Option<&wire::MachineId>) -> Result<MachineId, ProviderError> {
    decode_uuid(
        &value
            .ok_or_else(|| ProviderError::Rejected("machine identity is missing".into()))?
            .value,
    )
    .map(MachineId)
}
pub(super) fn decode_checkpoint(
    value: Option<&wire::CheckpointId>,
) -> Result<CheckpointId, ProviderError> {
    decode_uuid(
        &value
            .ok_or_else(|| ProviderError::Rejected("checkpoint identity is missing".into()))?
            .value,
    )
    .map(CheckpointId)
}
