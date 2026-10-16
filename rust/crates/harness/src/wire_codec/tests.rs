use super::*;
use prost::Message as _;

pub(super) fn fixture() -> Vec<u8> {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../conformance/native-wasm-event-v2.json")).unwrap();
    fixture["event_wire_hex"]
        .as_str()
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
#[test]
fn protocol_digest_and_error_semantics_are_preserved() -> Result<()> {
    let mut envelope = wire::EventEnvelope::decode(fixture().as_slice())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    for protocol in [
        wire::ProtocolIdentity {
            version: "1".into(),
            ..protocol_identity()
        },
        wire::ProtocolIdentity {
            descriptor_digest: "wrong".into(),
            ..protocol_identity()
        },
    ] {
        envelope.protocol = Some(protocol);
        assert!(matches!(
            decode_event(&envelope.encode_to_vec(), &mut None),
            Err(Error::Unsupported(_))
        ));
    }
    let operation = OperationId::from_bytes([8; 16]);
    let error = encode_error(&Error::Indeterminate(operation));
    assert_eq!(error.code, wire::ErrorCode::Indeterminate as i32);
    assert_eq!(error.operation_id, operation.to_string());
    for (reason, code) in [
        (
            crate::InteractionRejection::Declined,
            wire::ErrorCode::InteractionDeclined,
        ),
        (
            crate::InteractionRejection::Cancelled,
            wire::ErrorCode::InteractionCancelled,
        ),
        (
            crate::InteractionRejection::Expired,
            wire::ErrorCode::InteractionExpired,
        ),
        (
            crate::InteractionRejection::Denied,
            wire::ErrorCode::InteractionDenied,
        ),
    ] {
        assert_eq!(
            encode_error(&Error::InteractionRejected(reason)).code,
            code as i32
        );
    }
    Ok(())
}
#[test]
fn native_event_bytes_match_cross_language_fixture() -> Result<()> {
    let bytes = fixture();
    let ((authority, event), _) = decode_event(&bytes, &mut None)?;
    assert_eq!(encode_event(&authority, &event, &mut None)?, bytes);
    Ok(())
}
pub(super) fn command_fixture(action: &Action) -> Result<wire::CommandEnvelope> {
    Ok(wire::CommandEnvelope {
        protocol: Some(protocol_identity()),
        authority: Some(wire::Authority {
            kind: wire::AggregateKind::Task as i32,
            id: "native-source".into(),
        }),
        operation: Some(wire::OperationIdentity {
            operation_id: OperationId::from_bytes([31; 16]).to_string(),
            idempotency_key: "native-once".into(),
        }),
        scope: Some(wire::Scope {
            id: "explicit-producer-scope".into(),
            issuer: "producer".into(),
            capabilities: vec!["lifecycle:transition".into()],
            proof: vec![7; 32],
            ..Default::default()
        }),
        action_type: action.tag().into(),
        canonical_action_json: canonical_json_bytes(action)?,
        ..Default::default()
    })
}
#[test]
fn native_command_envelopes_require_exact_payload_protocol_and_retry_identity() -> Result<()> {
    let action = Action::TransitionLifecycle {
        to: crate::core::LifecycleState::Active,
        reason: None,
    };
    let mut envelope = command_fixture(&action)?;
    let (_, command) = decode_command_envelope(envelope.clone())?;
    assert_eq!(command.operation_id, OperationId::from_bytes([31; 16]));
    assert_eq!(command.idempotency_key, IdempotencyKey::new("native-once")?);
    let mut wrong = envelope.clone();
    wrong.canonical_action_json =
        serde_json::to_vec_pretty(&action).map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(matches!(
        decode_command_envelope(wrong),
        Err(Error::Invalid(_))
    ));
    let mut wrong = envelope.clone();
    wrong.action_type = "append_custom".into();
    assert!(matches!(
        decode_command_envelope(wrong),
        Err(Error::Invalid(_))
    ));
    let mut wrong = envelope.clone();
    wrong.operation.as_mut().unwrap().idempotency_key.clear();
    assert!(matches!(
        decode_command_envelope(wrong),
        Err(Error::Invalid(_))
    ));
    envelope.intent_digest = vec![0; 32];
    assert!(matches!(
        decode_command_envelope(envelope),
        Err(Error::Conflict(_))
    ));
    Ok(())
}
