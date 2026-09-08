//! Transport-neutral server port implemented identically by every wire adapter.

use crate::{
    Error, IdempotencyKey, OperationId, Outcome, Result,
    core::{Authority, Scope},
    scheduler::{OperationPhase, OperationState},
    wire,
    wire_codec::{
        decode_authority, decode_scope, encode_authority, protocol_identity, validate_protocol,
    },
};
use futures::{future::BoxFuture, stream::BoxStream};
use std::collections::BTreeMap;

/// Complete application-facing wire API. Adapters own framing, never semantics.
///
/// Operation-control implementations must authenticate the decoded scope against
/// the requested owner; [`crate::distributed::DistributedCoordinator::observe_operation`] and
/// [`crate::distributed::DistributedCoordinator::cancel_operation`] provide the canonical checks.
/// Wire validation alone deliberately cannot authenticate host-held key material.
pub trait HarnessWireApi: Send + Sync + 'static {
    /// Negotiates the exact protocol and required capabilities before other calls.
    fn handshake<'a>(
        &'a self,
        request: wire::HandshakeRequest,
    ) -> BoxFuture<'a, Result<wire::HandshakeResponse>>;

    /// Admits one identity-bound command.
    fn submit<'a>(
        &'a self,
        command: wire::CommandEnvelope,
    ) -> BoxFuture<'a, Result<wire::Admission>>;

    /// Replays validated gapless deliveries and then follows live events.
    fn replay<'a>(
        &'a self,
        request: wire::ResumeRequest,
    ) -> BoxFuture<'a, Result<BoxStream<'static, Result<wire::Delivery>>>>;

    /// Authenticates one decoded operation-control scope against host-held trust roots.
    fn authorize_operation_control<'a>(
        &'a self,
        request: &'a OperationControlRequest,
    ) -> BoxFuture<'a, Result<()>>;

    /// Observes one authenticated durable operation projection.
    fn observe<'a>(
        &'a self,
        request: wire::ObserveRequest,
    ) -> BoxFuture<'a, Result<wire::OperationStatus>>;

    /// Requests authenticated idempotent cancellation.
    fn cancel<'a>(
        &'a self,
        request: wire::CancelRequest,
    ) -> BoxFuture<'a, Result<wire::CancelResponse>>;
}

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
        .collect::<BTreeMap<_, _>>();
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

/// Requires every stateless command to carry the exact negotiated wire identity.
pub fn validate_command_protocol(command: &wire::CommandEnvelope) -> Result<()> {
    validate_protocol(command.protocol.as_ref())
}

/// Requires every replay request to carry the exact negotiated wire identity.
pub fn validate_resume_protocol(request: &wire::ResumeRequest) -> Result<()> {
    validate_protocol(request.protocol.as_ref())
}

/// Decoded operation-control request shared by all server adapters.
pub struct OperationControlRequest {
    /// Durable owner claimed by the caller and verified by the host.
    pub owner: Authority,
    /// Target operation.
    pub operation_id: OperationId,
    /// Complete signed scope to verify before observing or cancelling.
    pub scope: Scope,
}

impl OperationControlRequest {
    /// Verifies the signed scope against the host-selected owner trust root.
    pub fn authenticate(&self, verifier: &crate::core::AuthorityVerifier) -> Result<()> {
        verifier.verify_audience(&self.owner)?;
        verifier.verify(&self.scope)
    }
}

/// Validates and decodes an observe request without authenticating its proof.
pub fn validate_observe_request(request: &wire::ObserveRequest) -> Result<OperationControlRequest> {
    validate_protocol(request.protocol.as_ref())?;
    decode_control(
        request.owner.clone(),
        request.operation_id.clone(),
        request.scope.clone(),
        "operation:observe",
    )
}

/// Validates and decodes a cancellation request without authenticating its proof.
pub fn validate_cancel_request(
    request: &wire::CancelRequest,
) -> Result<(OperationControlRequest, IdempotencyKey, bool)> {
    validate_protocol(request.protocol.as_ref())?;
    let key = IdempotencyKey::new(request.idempotency_key.clone())?;
    let control = decode_control(
        request.owner.clone(),
        request.operation_id.clone(),
        request.scope.clone(),
        "operation:cancel",
    )?;
    Ok((control, key, request.recursive))
}

/// Converts the authoritative scheduler projection to its canonical wire status.
#[must_use]
pub fn operation_status(state: &OperationState) -> wire::OperationStatus {
    let owner = match &state.spec.owner {
        crate::scheduler::DurableOwner::Attached { authority }
        | crate::scheduler::DurableOwner::Detached { authority } => authority,
    };
    let (completion, error) = match state.outcome.as_ref() {
        Some(Outcome::Succeeded(_)) => (wire::CompletionState::Succeeded, None),
        Some(Outcome::Failed { message }) => (
            wire::CompletionState::Failed,
            Some(wire::Error {
                code: wire::ErrorCode::Unspecified as i32,
                message: message.clone(),
                operation_id: state.spec.operation_id.to_string(),
            }),
        ),
        Some(Outcome::Cancelled) => (wire::CompletionState::Cancelled, None),
        Some(Outcome::Indeterminate { operation_id }) => (
            wire::CompletionState::Indeterminate,
            Some(wire::Error {
                code: wire::ErrorCode::Indeterminate as i32,
                message: "operation completion is indeterminate".into(),
                operation_id: operation_id.to_string(),
            }),
        ),
        None if state.phase == OperationPhase::Terminal => (
            wire::CompletionState::Indeterminate,
            Some(wire::Error {
                code: wire::ErrorCode::Indeterminate as i32,
                message: "terminal operation outcome is missing".into(),
                operation_id: state.spec.operation_id.to_string(),
            }),
        ),
        None => (wire::CompletionState::Running, None),
    };
    wire::OperationStatus {
        operation: Some(wire::OperationIdentity {
            operation_id: state.spec.operation_id.to_string(),
            idempotency_key: String::new(),
        }),
        state: completion as i32,
        error,
        protocol: Some(current_protocol()),
        owner: Some(encode_authority(owner)),
        cancellation_requested: state.cancellation_requested,
        revision: state.revision,
    }
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
    if response.operation.as_ref() != Some(&operation) {
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

fn decode_control(
    owner: Option<wire::Authority>,
    operation_id: String,
    scope: Option<wire::Scope>,
    capability: &str,
) -> Result<OperationControlRequest> {
    let owner =
        decode_authority(owner.ok_or_else(|| Error::Invalid("operation owner is missing".into()))?)
            .map_err(as_invalid_control_input)?;
    owner.stream_path()?;
    let operation_id = OperationId::parse(&operation_id)?;
    let scope = decode_scope(
        scope.ok_or_else(|| Error::Invalid("operation control scope is missing".into()))?,
    )?;
    if scope.id().is_empty() || !scope.capabilities().contains(capability) {
        return Err(Error::Unauthorized(format!(
            "operation control scope lacks {capability}"
        )));
    }
    Ok(OperationControlRequest {
        owner,
        operation_id,
        scope,
    })
}

fn as_invalid_control_input(error: Error) -> Error {
    match error {
        Error::Storage(message) => Error::Invalid(message),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message as _;

    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyOperationStatus {
        #[prost(message, optional, tag = "1")]
        operation: Option<wire::OperationIdentity>,
        #[prost(enumeration = "wire::CompletionState", tag = "2")]
        state: i32,
        #[prost(message, optional, tag = "3")]
        error: Option<wire::Error>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyObserveRequest {
        #[prost(string, tag = "1")]
        operation_id: String,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyCancelRequest {
        #[prost(string, tag = "1")]
        operation_id: String,
    }

    #[test]
    fn negotiation_is_exact_and_capability_checked() -> Result<()> {
        let supported = wire::CapabilitySet {
            capabilities: vec![wire::Capability {
                name: "replay".into(),
                version: "1".into(),
            }],
        };
        let request = wire::HandshakeRequest {
            protocol: Some(current_protocol()),
            required: Some(supported.clone()),
        };
        assert_eq!(negotiate(&request, &supported)?.supported, Some(supported));
        let mut incompatible = request;
        incompatible
            .protocol
            .as_mut()
            .ok_or_else(|| Error::Invalid("protocol".into()))?
            .version = "2".into();
        assert!(matches!(
            negotiate(&incompatible, &wire::CapabilitySet::default()),
            Err(Error::Unsupported(_))
        ));
        Ok(())
    }

    #[test]
    fn stateless_calls_require_the_exact_protocol() {
        let mut command = wire::CommandEnvelope::default();
        let mut resume = wire::ResumeRequest::default();
        assert!(validate_command_protocol(&command).is_err());
        assert!(validate_resume_protocol(&resume).is_err());
        command.protocol = Some(current_protocol());
        resume.protocol = Some(current_protocol());
        assert!(validate_command_protocol(&command).is_ok());
        assert!(validate_resume_protocol(&resume).is_ok());
    }

    #[test]
    fn operation_control_is_protocol_scope_and_response_identity_bound() -> Result<()> {
        let operation_id = OperationId::from_bytes([7; 16]).to_string();
        let owner = wire::Authority {
            kind: wire::AggregateKind::Task as i32,
            id: "owner".into(),
        };
        let scope = wire::Scope {
            id: "control".into(),
            capabilities: vec!["operation:observe".into(), "operation:cancel".into()],
            issuer: "runtime".into(),
            parent_proof: Vec::new(),
            proof: vec![1; 32],
        };
        let observe = wire::ObserveRequest {
            protocol: Some(current_protocol()),
            owner: Some(owner.clone()),
            operation_id: operation_id.clone(),
            scope: Some(scope.clone()),
        };
        let decoded = validate_observe_request(&observe)?;
        assert_eq!(decoded.operation_id.to_string(), operation_id);
        let status = wire::OperationStatus {
            operation: Some(wire::OperationIdentity {
                operation_id: operation_id.clone(),
                idempotency_key: String::new(),
            }),
            state: wire::CompletionState::Running as i32,
            error: None,
            protocol: Some(current_protocol()),
            owner: Some(owner.clone()),
            cancellation_requested: false,
            revision: 3,
        };
        validate_operation_status(&observe, &status)?;

        let mut mismatched_error = status.clone();
        mismatched_error.error = Some(wire::Error {
            code: wire::ErrorCode::Indeterminate as i32,
            message: "uncertain".into(),
            operation_id: OperationId::from_bytes([9; 16]).to_string(),
        });
        assert!(matches!(
            validate_operation_status(&observe, &mismatched_error),
            Err(Error::Conflict(_))
        ));

        let cancel = wire::CancelRequest {
            operation_id: operation_id.clone(),
            protocol: Some(current_protocol()),
            owner: Some(owner),
            scope: Some(scope),
            recursive: true,
            idempotency_key: "cancel-1".into(),
        };
        validate_cancel_request(&cancel)?;
        let response = wire::CancelResponse {
            status: Some(status),
            operation: Some(wire::OperationIdentity {
                operation_id,
                idempotency_key: cancel.idempotency_key.clone(),
            }),
        };
        validate_cancel_response(&cancel, &response)?;

        let mut mismatched = response;
        mismatched
            .operation
            .as_mut()
            .ok_or_else(|| Error::Invalid("operation".into()))?
            .idempotency_key = "other".into();
        assert!(matches!(
            validate_cancel_response(&cancel, &mismatched),
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[test]
    fn operation_control_preserves_existing_v1_field_numbers() -> Result<()> {
        let operation_id = OperationId::from_bytes([8; 16]).to_string();
        let identity = wire::OperationIdentity {
            operation_id: operation_id.clone(),
            idempotency_key: "legacy".into(),
        };
        let legacy_status = LegacyOperationStatus {
            operation: Some(identity.clone()),
            state: wire::CompletionState::Running as i32,
            error: None,
        };
        let current = wire::OperationStatus::decode(legacy_status.encode_to_vec().as_slice())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(current.operation, Some(identity.clone()));
        let legacy_round_trip = LegacyOperationStatus::decode(current.encode_to_vec().as_slice())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(legacy_round_trip.operation, Some(identity));

        let legacy_observe = LegacyObserveRequest {
            operation_id: operation_id.clone(),
        };
        let current_observe =
            wire::ObserveRequest::decode(legacy_observe.encode_to_vec().as_slice())
                .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(current_observe.operation_id, operation_id);
        let legacy_cancel = LegacyCancelRequest {
            operation_id: operation_id.clone(),
        };
        let current_cancel = wire::CancelRequest::decode(legacy_cancel.encode_to_vec().as_slice())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(current_cancel.operation_id, operation_id);
        Ok(())
    }
}
