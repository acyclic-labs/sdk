//! Canonical conversions between the Machines contract and protobuf wire types.

use super::*;

#[cfg(feature = "test-support")]
/// On-demand semantic checks against the production gRPC conversions.
pub mod codec_fuzz;
mod error;
mod files;
mod guest;
mod image;
mod input;
mod observation;
mod recovery;
mod results;

pub use error::*;
pub use files::*;
pub use guest::*;
pub use image::*;
pub use input::encode_exec_input;
pub use observation::*;
pub use recovery::{
    ForkAdmitted, decode_fork_admissions, decode_recovered, settled_change, settled_checkpoint,
    settled_fork, settled_machine, validate_recovered_admission,
};
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
pub(super) fn decode_uuid(value: &[u8]) -> Result<Uuid, ProviderError> {
    let bytes: [u8; 16] = value
        .try_into()
        .map_err(|_| ProviderError::Rejected("identity width is invalid".into()))?;
    crate::contract::admit_uuid(Uuid::from_bytes(bytes))
        .map_err(|_| ProviderError::Rejected("nil identity is invalid".into()))
}
pub fn decode_operation(value: Option<&wire::OperationId>) -> Result<OperationId, ProviderError> {
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
