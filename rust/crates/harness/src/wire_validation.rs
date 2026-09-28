//! Transport-independent validation of protobuf wire messages.
//!
//! These validators are deliberately free of host-only state so that HTTP, gRPC,
//! and the WebAssembly adapter enforce exactly the same identity rules.

use crate::{
    Error, Result, wire,
    wire_codec::{protocol_identity, validate_protocol},
};
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
use prost::Message as _;

/// Returns the protocol identity compiled into this crate.
#[must_use]
pub fn current_protocol() -> wire::ProtocolIdentity {
    protocol_identity()
}

/// Validates a handshake and constructs the canonical response.
pub fn negotiate(
    request: &wire::HandshakeRequest,
    supported: &wire::CapabilitySet,
) -> Result<wire::HandshakeResponse> {
    let expected = current_protocol();
    let actual = request
        .protocol
        .as_ref()
        .ok_or_else(|| Error::Invalid("handshake protocol is missing".into()))?;
    if actual != &expected {
        return Err(Error::Unsupported("protocol identity mismatch".into()));
    }
    let available = supported
        .capabilities
        .iter()
        .map(|capability| ((capability.name.as_str(), capability.version.as_str()), ()))
        .collect::<std::collections::BTreeMap<_, _>>();
    for required in request
        .required
        .as_ref()
        .map_or(&[][..], |set| set.capabilities.as_slice())
    {
        if required.name.is_empty()
            || required.version.is_empty()
            || !available.contains_key(&(required.name.as_str(), required.version.as_str()))
        {
            return Err(Error::Unsupported(format!(
                "unsupported capability {}@{}",
                required.name, required.version
            )));
        }
    }
    Ok(wire::HandshakeResponse {
        protocol: Some(expected),
        supported: Some(supported.clone()),
    })
}

/// Rejects malformed or non-identity-preserving admission results.
pub fn validate_admission(
    command: &wire::CommandEnvelope,
    admission: &wire::Admission,
) -> Result<()> {
    let expected = command
        .operation
        .as_ref()
        .ok_or_else(|| Error::Invalid("command operation identity is missing".into()))?;
    let actual = admission
        .operation
        .as_ref()
        .ok_or_else(|| Error::Conflict("admission operation identity is missing".into()))?;
    if expected.operation_id.is_empty() || expected.idempotency_key.is_empty() || actual != expected
    {
        return Err(Error::Conflict("admission identity mismatch".into()));
    }
    Ok(())
}

/// Rejects a status that is not bound to the exact observe request.
pub fn validate_operation_status(
    request: &wire::ObserveRequest,
    status: &wire::OperationStatus,
) -> Result<()> {
    validate_protocol(status.protocol.as_ref())?;
    if request.operation_id.is_empty()
        || status
            .operation
            .as_ref()
            .is_none_or(|operation| operation.operation_id != request.operation_id)
        || request.owner.is_none()
        || status.owner.is_none()
        || status.owner != request.owner
        || status.error.as_ref().is_some_and(|error| {
            !error.operation_id.is_empty() && error.operation_id != request.operation_id
        })
        || wire::CompletionState::try_from(status.state)
            .map_or(true, |state| state == wire::CompletionState::Unspecified)
    {
        return Err(Error::Conflict("operation status identity mismatch".into()));
    }
    Ok(())
}

/// Rejects a cancellation result that loses request or status identity.
pub fn validate_cancel_response(
    request: &wire::CancelRequest,
    response: &wire::CancelResponse,
) -> Result<()> {
    let operation = wire::OperationIdentity {
        operation_id: request.operation_id.clone(),
        idempotency_key: request.idempotency_key.clone(),
    };
    if request.operation_id.is_empty()
        || request.idempotency_key.is_empty()
        || response.operation.as_ref() != Some(&operation)
    {
        return Err(Error::Conflict("cancellation identity mismatch".into()));
    }
    let status = response
        .status
        .as_ref()
        .ok_or_else(|| Error::Conflict("cancellation status is missing".into()))?;
    validate_operation_status(
        &wire::ObserveRequest {
            protocol: request.protocol.clone(),
            owner: request.owner.clone(),
            operation_id: request.operation_id.clone(),
            scope: request.scope.clone(),
        },
        status,
    )
}

/// Validates a protobuf-encoded handshake request and response.
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub fn validate_wire_handshake(request: &[u8], response: &[u8]) -> Result<()> {
    let request = wire::HandshakeRequest::decode(request)
        .map_err(|error| Error::Invalid(format!("invalid handshake request: {error}")))?;
    let response = wire::HandshakeResponse::decode(response)
        .map_err(|error| Error::Invalid(format!("invalid handshake response: {error}")))?;
    let supported = response
        .supported
        .as_ref()
        .ok_or_else(|| Error::Invalid("handshake supported capabilities are missing".into()))?;
    let expected = negotiate(&request, supported)?;
    if response.protocol != expected.protocol || response.supported != expected.supported {
        return Err(Error::Conflict(
            "handshake response identity mismatch".into(),
        ));
    }
    Ok(())
}

/// Validates protobuf-encoded command admission identity.
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub fn validate_wire_admission(command: &[u8], admission: &[u8]) -> Result<()> {
    let command = wire::CommandEnvelope::decode(command)
        .map_err(|error| Error::Invalid(format!("invalid command envelope: {error}")))?;
    let admission = wire::Admission::decode(admission)
        .map_err(|error| Error::Invalid(format!("invalid admission: {error}")))?;
    validate_admission(&command, &admission)
}

/// Validates protobuf-encoded operation status identity.
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub fn validate_wire_status(request: &[u8], status: &[u8]) -> Result<()> {
    let request = wire::ObserveRequest::decode(request)
        .map_err(|error| Error::Invalid(format!("invalid observe request: {error}")))?;
    let status = wire::OperationStatus::decode(status)
        .map_err(|error| Error::Invalid(format!("invalid operation status: {error}")))?;
    validate_operation_status(&request, &status)
}

/// Validates protobuf-encoded cancellation response identity.
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub fn validate_wire_cancellation(request: &[u8], response: &[u8]) -> Result<()> {
    let request = wire::CancelRequest::decode(request)
        .map_err(|error| Error::Invalid(format!("invalid cancel request: {error}")))?;
    let response = wire::CancelResponse::decode(response)
        .map_err(|error| Error::Invalid(format!("invalid cancel response: {error}")))?;
    validate_cancel_response(&request, &response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_rejects_missing_identity_and_empty_capability() {
        let supported = wire::CapabilitySet {
            capabilities: vec![wire::Capability {
                name: "replay".into(),
                version: "1".into(),
            }],
        };
        let missing = wire::HandshakeRequest {
            protocol: None,
            required: None,
        };
        assert!(matches!(
            negotiate(&missing, &supported),
            Err(Error::Invalid(_))
        ));
        let empty = wire::HandshakeRequest {
            protocol: Some(current_protocol()),
            required: Some(wire::CapabilitySet {
                capabilities: vec![wire::Capability {
                    name: String::new(),
                    version: "1".into(),
                }],
            }),
        };
        assert!(matches!(
            negotiate(&empty, &supported),
            Err(Error::Unsupported(_))
        ));
    }

    #[test]
    fn admission_rejects_missing_or_empty_identity() {
        let command = wire::CommandEnvelope {
            operation: None,
            ..Default::default()
        };
        assert!(matches!(
            validate_admission(&command, &wire::Admission::default()),
            Err(Error::Invalid(_))
        ));
        let command = wire::CommandEnvelope {
            operation: Some(wire::OperationIdentity {
                operation_id: String::new(),
                idempotency_key: "key".into(),
            }),
            ..Default::default()
        };
        let admission = wire::Admission {
            operation: command.operation.clone(),
            ..Default::default()
        };
        assert!(matches!(
            validate_admission(&command, &admission),
            Err(Error::Conflict(_))
        ));
    }

    #[test]
    fn operation_control_requires_owner_and_retry_identity() {
        let request = wire::ObserveRequest {
            operation_id: "operation".into(),
            ..Default::default()
        };
        let status = wire::OperationStatus {
            protocol: Some(current_protocol()),
            operation: Some(wire::OperationIdentity {
                operation_id: request.operation_id.clone(),
                ..Default::default()
            }),
            state: wire::CompletionState::Running as i32,
            ..Default::default()
        };
        assert!(matches!(
            validate_operation_status(&request, &status),
            Err(Error::Conflict(_))
        ));

        let cancel = wire::CancelRequest {
            operation_id: request.operation_id,
            ..Default::default()
        };
        let response = wire::CancelResponse {
            operation: Some(wire::OperationIdentity {
                operation_id: cancel.operation_id.clone(),
                ..Default::default()
            }),
            status: Some(status),
        };
        assert!(matches!(
            validate_cancel_response(&cancel, &response),
            Err(Error::Conflict(_))
        ));
    }
}
