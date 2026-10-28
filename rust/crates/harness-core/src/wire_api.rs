//! Wire validation and projection shared by every client adapter.

use crate::contract::capability;
pub use crate::wire_validation::{
    current_protocol, negotiate, validate_admission_identity, validate_cancel_response,
    validate_operation_status,
};
use crate::{
    Error, IdempotencyKey, OperationId, Result,
    core::{Authority, Scope},
    wire,
    wire_codec::{COMPILED_PROTOCOL, decode_authority, decode_scope, validate_protocol},
};

/// Whether a replay stops at its opening cut or remains subscribed to appends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayMode {
    /// Finite inspection of events committed when replay opens.
    Snapshot,
    /// Replay followed by every later accepted event.
    Follow,
}

/// Requires every replay request to carry the exact negotiated wire identity.
pub fn validate_resume_protocol(request: &wire::ResumeRequest) -> Result<()> {
    validate_request_protocol(request.protocol.as_ref())
}

fn validate_request_protocol(protocol: Option<&wire::ProtocolIdentity>) -> Result<()> {
    validate_protocol(protocol, COMPILED_PROTOCOL, wire::ErrorCode::Invalid, &None)
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[path = "wire_api_protocol_tests.rs"]
mod protocol_tests;

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
        verifier.verify_audience(&self.owner, &mut None)?;
        verifier.verify(&self.scope, &mut None)
    }
}

/// Validates and decodes an observe request without authenticating its proof.
pub fn validate_observe_request(request: &wire::ObserveRequest) -> Result<OperationControlRequest> {
    validate_request_protocol(request.protocol.as_ref())?;
    decode_control(
        request.owner.clone(),
        &request.operation_id,
        request.scope.clone(),
        capability::OPERATION_OBSERVE,
    )
}

/// Decoded cancellation request shared by all server adapters.
pub struct CancelControlRequest {
    /// Operation-control identity to authenticate before cancelling.
    pub control: OperationControlRequest,
    /// Caller-owned retry identity of this cancellation.
    pub idempotency_key: IdempotencyKey,
    /// Whether same-owner descendants are cancelled too.
    pub recursive: bool,
}

/// Validates and decodes a cancellation request without authenticating its proof.
pub fn validate_cancel_request(request: &wire::CancelRequest) -> Result<CancelControlRequest> {
    validate_request_protocol(request.protocol.as_ref())?;
    let idempotency_key = IdempotencyKey::new(request.idempotency_key.clone())?;
    let control = decode_control(
        request.owner.clone(),
        &request.operation_id,
        request.scope.clone(),
        capability::OPERATION_CANCEL,
    )?;
    Ok(CancelControlRequest {
        control,
        idempotency_key,
        recursive: request.recursive,
    })
}

fn decode_control(
    owner: Option<wire::Authority>,
    operation_id: &str,
    scope: Option<wire::Scope>,
    capability: &str,
) -> Result<OperationControlRequest> {
    let owner = decode_authority(
        owner.ok_or_else(|| Error::Invalid("operation owner is missing".into()))?,
        wire::ErrorCode::Invalid,
        &None,
    )?;
    owner.stream_path(&mut None)?;
    let operation_id = OperationId::parse(operation_id)?;
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let mut resume = wire::ResumeRequest::default();
        assert!(validate_resume_protocol(&resume).is_err());
        // A malformed request is the caller's fault, never a retryable storage failure.
        assert!(matches!(
            validate_resume_protocol(&resume),
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            validate_observe_request(&wire::ObserveRequest::default()),
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            validate_cancel_request(&wire::CancelRequest::default()),
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            validate_observe_request(&wire::ObserveRequest {
                protocol: Some(current_protocol()),
                owner: Some(wire::Authority::default()),
                ..wire::ObserveRequest::default()
            }),
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            validate_resume_protocol(&wire::ResumeRequest {
                protocol: Some(wire::ProtocolIdentity::default()),
                ..wire::ResumeRequest::default()
            }),
            Err(Error::Unsupported(_))
        ));
        resume.protocol = Some(current_protocol());
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
            workload: Vec::new(),
            proof: vec![1; 32],
            agent_id: String::new(),
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
            replay_through: None,
        };
        validate_operation_status(&observe.operation_id, observe.owner.as_ref(), &status)?;

        let mut mismatched_error = status.clone();
        mismatched_error.error = Some(wire::Error {
            code: wire::ErrorCode::Indeterminate as i32,
            message: "uncertain".into(),
            operation_id: OperationId::from_bytes([9; 16]).to_string(),
        });
        assert!(matches!(
            validate_operation_status(
                &observe.operation_id,
                observe.owner.as_ref(),
                &mismatched_error
            ),
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
}
