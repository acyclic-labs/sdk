//! Public protocol admission keeps the original caller-selected identities.
use acyclic_harness_core::{Result, wire, wire_validation as v};
use prost::Message;

fn refusal<T>(result: Result<T>, expected: wire::ErrorCode) {
    assert_eq!(result.err().map(|error| error.code()), Some(expected));
}

fn admitted<W: Message + Default>(wire: &W, validate: impl Fn(&W) -> Result<()>) -> bool {
    if validate(wire).is_err() {
        return false;
    }
    let restored = W::decode(wire.encode_to_vec().as_slice()).expect("typed writer output decodes");
    assert!(validate(&restored).is_ok(), "normalized admission changed");
    true
}

fn handshake(request: &wire::HandshakeRequest) {
    let supported = request.required.clone().unwrap_or_default();
    if let Ok(response) = v::negotiate(request, &supported) {
        let normalized =
            wire::HandshakeRequest::decode(request.encode_to_vec().as_slice()).unwrap();
        assert_eq!(v::negotiate(&normalized, &supported).unwrap(), response);
        assert_eq!(response.protocol, Some(v::current_protocol()));
        assert_eq!(response.supported, Some(supported));
        if request
            .required
            .as_ref()
            .is_some_and(|set| !set.capabilities.is_empty())
        {
            refusal(
                v::negotiate(request, &wire::CapabilitySet::default()),
                wire::ErrorCode::Unsupported,
            );
        }
    }
    let mut missing = request.clone();
    missing.protocol = None;
    refusal(
        v::negotiate(&missing, &wire::CapabilitySet::default()),
        wire::ErrorCode::Invalid,
    );
    // Even an invalid required capability cannot run before format admission.
    missing.required = Some(wire::CapabilitySet {
        capabilities: vec![wire::Capability::default()],
    });
    for version in ["0", "unknown"] {
        let mut protocol = v::current_protocol();
        protocol.version = version.into();
        missing.protocol = Some(protocol);
        refusal(
            v::negotiate(&missing, &wire::CapabilitySet::default()),
            wire::ErrorCode::Unsupported,
        );
    }
    let mut protocol = v::current_protocol();
    protocol.descriptor_digest.push('\0');
    missing.protocol = Some(protocol);
    refusal(
        v::negotiate(&missing, &wire::CapabilitySet::default()),
        wire::ErrorCode::Unsupported,
    );
}

fn admission(wire: &wire::Admission) {
    let expected = wire.operation.clone();
    if admitted(wire, |value| {
        v::validate_admission_identity(expected.as_ref(), value)
    }) {
        let mut foreign = expected.clone().expect("admitted original operation");
        foreign.idempotency_key.push('\0');
        refusal(
            v::validate_admission_identity(Some(&foreign), wire),
            wire::ErrorCode::Conflict,
        );
        let mut missing = wire.clone();
        missing.operation = None;
        refusal(
            v::validate_admission_identity(expected.as_ref(), &missing),
            wire::ErrorCode::Conflict,
        );
    }
    refusal(
        v::validate_admission_identity(None, wire),
        wire::ErrorCode::Invalid,
    );
}

fn status(wire: &wire::OperationStatus) {
    let operation = wire
        .operation
        .as_ref()
        .map_or("", |operation| operation.operation_id.as_str());
    let owner = wire.owner.clone();
    if admitted(wire, |value| {
        v::validate_operation_status(operation, owner.as_ref(), value)
    }) {
        let foreign = format!("{operation}\0");
        refusal(
            v::validate_operation_status(&foreign, owner.as_ref(), wire),
            wire::ErrorCode::Conflict,
        );
        let mut changed_owner = owner.clone().expect("admitted original owner");
        changed_owner.id.push('\0');
        refusal(
            v::validate_operation_status(operation, Some(&changed_owner), wire),
            wire::ErrorCode::Conflict,
        );
        let mut changed = wire.clone();
        changed.error = Some(wire::Error {
            operation_id: foreign,
            ..Default::default()
        });
        refusal(
            v::validate_operation_status(operation, owner.as_ref(), &changed),
            wire::ErrorCode::Conflict,
        );
        changed.protocol = None;
        refusal(
            v::validate_operation_status(operation, owner.as_ref(), &changed),
            wire::ErrorCode::Conflict,
        );
        let mut protocol = v::current_protocol();
        protocol.version = "0".into();
        changed.protocol = Some(protocol);
        refusal(
            v::validate_operation_status(operation, owner.as_ref(), &changed),
            wire::ErrorCode::Unsupported,
        );
    }
}

fn cancellation(request: &wire::CancelRequest, response: &wire::CancelResponse) {
    if admitted(response, |value| {
        v::validate_cancel_response(request, value)
    }) {
        let mut foreign = request.clone();
        foreign.idempotency_key.push('\0');
        refusal(
            v::validate_cancel_response(&foreign, response),
            wire::ErrorCode::Conflict,
        );
        foreign = request.clone();
        foreign.operation_id.push('\0');
        refusal(
            v::validate_cancel_response(&foreign, response),
            wire::ErrorCode::Conflict,
        );
        foreign = request.clone();
        foreign.owner = None;
        refusal(
            v::validate_cancel_response(&foreign, response),
            wire::ErrorCode::Conflict,
        );
        let mut missing = response.clone();
        missing.status = None;
        refusal(
            v::validate_cancel_response(request, &missing),
            wire::ErrorCode::Conflict,
        );
    }
}

fn response(request: &wire::CancelRequest) -> wire::CancelResponse {
    let operation = wire::OperationIdentity {
        operation_id: request.operation_id.clone(),
        idempotency_key: request.idempotency_key.clone(),
    };
    wire::CancelResponse {
        operation: Some(operation.clone()),
        status: Some(wire::OperationStatus {
            operation: Some(operation),
            owner: request.owner.clone(),
            protocol: Some(v::current_protocol()),
            state: wire::CompletionState::Running.into(),
            ..Default::default()
        }),
    }
}

pub(super) fn check(name: &str, bytes: &[u8]) {
    match name {
        "acyclic.protocol.v1.HandshakeRequest" => {
            if let Ok(wire) = wire::HandshakeRequest::decode(bytes) {
                handshake(&wire);
            }
        }
        "acyclic.harness.v2.Admission" => {
            if let Ok(wire) = wire::Admission::decode(bytes) {
                admission(&wire);
            }
        }
        "acyclic.harness.v2.OperationStatus" => {
            if let Ok(wire) = wire::OperationStatus::decode(bytes) {
                status(&wire);
            }
        }
        "acyclic.harness.v2.CancelRequest" => {
            if let Ok(request) = wire::CancelRequest::decode(bytes) {
                cancellation(&request, &response(&request));
            }
        }
        "acyclic.harness.v2.CancelResponse" => {
            if let Ok(response) = wire::CancelResponse::decode(bytes) {
                let operation = response.operation.clone().unwrap_or_default();
                let request = wire::CancelRequest {
                    operation_id: operation.operation_id,
                    idempotency_key: operation.idempotency_key,
                    owner: response
                        .status
                        .as_ref()
                        .and_then(|status| status.owner.clone()),
                    ..Default::default()
                };
                cancellation(&request, &response);
            }
        }
        _ => {}
    }
}

pub(super) fn seeds() -> Result<Vec<(&'static str, &'static str, Vec<u8>)>> {
    let capabilities = wire::CapabilitySet {
        capabilities: vec![wire::Capability {
            name: "conversation".into(),
            version: "2".into(),
        }],
    };
    let handshake = wire::HandshakeRequest {
        protocol: Some(v::current_protocol()),
        required: Some(capabilities.clone()),
    };
    let negotiated = v::negotiate(&handshake, &capabilities)?;
    let cancel = wire::CancelRequest {
        operation_id: "operation".into(),
        idempotency_key: "original".into(),
        protocol: Some(v::current_protocol()),
        owner: Some(wire::Authority {
            kind: wire::AggregateKind::Task.into(),
            id: "owner".into(),
        }),
        ..Default::default()
    };
    let response = response(&cancel);
    v::validate_cancel_response(&cancel, &response)?;
    let admission = wire::Admission {
        operation: response.operation.clone(),
        state: wire::AdmissionState::Accepted.into(),
        ..Default::default()
    };
    v::validate_admission_identity(response.operation.as_ref(), &admission)?;
    let mut outputs = vec![
        (
            "acyclic.protocol.v1.HandshakeRequest",
            "required",
            handshake.encode_to_vec(),
        ),
        (
            "acyclic.protocol.v1.HandshakeResponse",
            "negotiated",
            negotiated.encode_to_vec(),
        ),
        (
            "acyclic.harness.v2.Admission",
            "accepted",
            admission.encode_to_vec(),
        ),
        (
            "acyclic.harness.v2.CancelRequest",
            "bound",
            cancel.encode_to_vec(),
        ),
        (
            "acyclic.harness.v2.CancelResponse",
            "bound",
            response.encode_to_vec(),
        ),
    ];
    for (variant, state) in [
        ("running", wire::CompletionState::Running),
        ("succeeded", wire::CompletionState::Succeeded),
        ("failed", wire::CompletionState::Failed),
        ("cancelled", wire::CompletionState::Cancelled),
        ("indeterminate", wire::CompletionState::Indeterminate),
    ] {
        let mut status = response
            .status
            .clone()
            .expect("actual admitted response has a status");
        status.state = state.into();
        v::validate_operation_status(&cancel.operation_id, cancel.owner.as_ref(), &status)?;
        outputs.push((
            "acyclic.harness.v2.OperationStatus",
            variant,
            status.encode_to_vec(),
        ));
    }
    Ok(outputs)
}
