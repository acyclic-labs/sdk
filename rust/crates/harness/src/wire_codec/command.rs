use super::*;
use prost::Message as _;

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) fn decode_command(bytes: &[u8]) -> Result<(Authority, Command)> {
    let envelope =
        wire::CommandEnvelope::decode(bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    decode_command_envelope(envelope)
}

/// Decodes an already framed command through the same validation as WASM.
pub fn decode_command_envelope(envelope: wire::CommandEnvelope) -> Result<(Authority, Command)> {
    validate_protocol(
        envelope.protocol.as_ref(),
        &protocol_identity(),
        Error::Invalid,
    )?;
    let authority = decode_authority(
        envelope
            .authority
            .ok_or_else(|| Error::Invalid("command authority is missing".into()))?,
        Error::Invalid,
    )?;
    let operation = envelope
        .operation
        .ok_or_else(|| Error::Invalid("command operation is missing".into()))?;
    let action: Action = crate::contract::json_from_slice(&envelope.canonical_action_json)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    if canonical_json_bytes(&action)? != envelope.canonical_action_json {
        return Err(Error::Invalid(
            "command action is not canonical JSON".into(),
        ));
    }
    if envelope.action_type != action.tag() {
        return Err(Error::Invalid(
            "command action type disagrees with its payload".into(),
        ));
    }
    let command = Command {
        operation_id: OperationId::parse(&operation.operation_id)?,
        idempotency_key: IdempotencyKey::new(operation.idempotency_key)?,
        expected_revision: envelope.expected_revision,
        scope: decode_scope(
            envelope
                .scope
                .ok_or_else(|| Error::Invalid("command scope is missing".into()))?,
        )?,
        causal_parent: envelope
            .causal_parent
            .map(|reference| decode_reference(reference, Error::Invalid))
            .transpose()?,
        action,
    };
    if !envelope.intent_digest.is_empty()
        && envelope.intent_digest.as_slice()
            != crate::contract::canonical_json_digest(
                &command,
                crate::store::default_projection_read_limits().maximum_bytes,
                &mut None,
            )?
    {
        return Err(Error::Conflict("command intent digest mismatch".into()));
    }
    Ok((authority, command))
}

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) fn encode_apply_result(authority: &Authority, result: &ApplyResult) -> Result<Vec<u8>> {
    let (state, event) = match result {
        ApplyResult::Applied { event } => (wire::ApplyState::Applied, event),
        ApplyResult::Replayed { event } => (wire::ApplyState::Replayed, event),
    };
    let event = encode_event_envelope(authority, event)?;
    Ok(wire::ApplyResponse {
        state: state as i32,
        event: Some(event),
    }
    .encode_to_vec())
}
