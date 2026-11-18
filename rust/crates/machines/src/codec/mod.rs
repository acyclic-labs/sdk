use super::*;

pub mod error;
pub mod image;
pub mod observation;
pub mod results;

pub use error::*;
pub use image::*;
pub use observation::*;
pub use results::*;

pub fn protocol() -> wire::ProtocolVersion {
    wire::ProtocolVersion {
        major: PROTOCOL_MAJOR,
        minor: PROTOCOL_MINOR,
    }
}
pub fn encode_key(value: IdempotencyKey) -> wire::IdempotencyKey {
    wire::IdempotencyKey {
        value: value.as_bytes().to_vec(),
    }
}
pub fn encode_machine(value: MachineId) -> wire::MachineId {
    wire::MachineId {
        value: value.as_bytes().to_vec(),
    }
}
pub fn encode_checkpoint(value: CheckpointId) -> wire::CheckpointId {
    wire::CheckpointId {
        value: value.as_bytes().to_vec(),
    }
}
pub fn encode_operation(value: OperationId) -> wire::OperationId {
    wire::OperationId {
        value: value.as_bytes().to_vec(),
    }
}
pub fn operation_request(value: OperationId) -> wire::OperationRequest {
    wire::OperationRequest {
        protocol: Some(protocol()),
        operation: Some(encode_operation(value)),
    }
}
pub fn decode_uuid(value: &[u8]) -> Result<Uuid, ProviderError> {
    let bytes: [u8; 16] = value
        .try_into()
        .map_err(|_| ProviderError::Rejected("identity width is invalid".into()))?;
    acyclic_machines::admit_uuid(Uuid::from_bytes(bytes))
        .map_err(|_| ProviderError::Rejected("nil identity is invalid".into()))
}
pub fn decode_operation(
    value: Option<&wire::OperationId>,
) -> Result<OperationId, ProviderError> {
    decode_uuid(
        &value
            .ok_or_else(|| ProviderError::Rejected("operation identity is missing".into()))?
            .value,
    )
    .map(OperationId)
}
pub fn decode_machine(value: Option<&wire::MachineId>) -> Result<MachineId, ProviderError> {
    decode_uuid(
        &value
            .ok_or_else(|| ProviderError::Rejected("machine identity is missing".into()))?
            .value,
    )
    .map(MachineId)
}
pub fn decode_checkpoint(
    value: Option<&wire::CheckpointId>,
) -> Result<CheckpointId, ProviderError> {
    decode_uuid(
        &value
            .ok_or_else(|| ProviderError::Rejected("checkpoint identity is missing".into()))?
            .value,
    )
    .map(CheckpointId)
}
