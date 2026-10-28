use super::*;

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
