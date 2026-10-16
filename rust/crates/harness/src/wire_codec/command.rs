use super::*;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
use prost::Message as _;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) fn decode_command(bytes: &[u8]) -> Result<(Authority, Command)> {
    decode_command_envelope(
        wire::CommandEnvelope::decode(bytes).map_err(|error| Error::Invalid(error.to_string()))?,
    )
}
/// Decodes an identity-bound canonical command.
pub fn decode_command_envelope(envelope: wire::CommandEnvelope) -> Result<(Authority, Command)> {
    validate_protocol(
        envelope.protocol.as_ref(),
        COMPILED_PROTOCOL,
        Invalid,
        &None,
    )?;
    let authority = decode_authority(
        envelope
            .authority
            .ok_or_else(|| Error::Invalid("missing command authority".into()))?,
        Invalid,
        &None,
    )?;
    let operation = envelope
        .operation
        .ok_or_else(|| Error::Invalid("missing command operation".into()))?;
    let action: Action = crate::contract::json_from_slice(&envelope.canonical_action_json)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    if !crate::contract::canonical_json_matches(&action, &envelope.canonical_action_json)?
        || envelope.action_type != action.tag()
    {
        return Err(Error::Invalid("noncanonical command action".into()));
    }
    let command = Command {
        operation_id: OperationId::parse(&operation.operation_id)?,
        idempotency_key: IdempotencyKey::new(operation.idempotency_key)?,
        expected_revision: envelope.expected_revision,
        scope: decode_scope(
            envelope
                .scope
                .ok_or_else(|| Error::Invalid("missing command scope".into()))?,
        )?,
        causal_parent: envelope
            .causal_parent
            .map(|parent| decode_reference(parent, Invalid, &None))
            .transpose()?,
        action,
    };
    if !envelope.intent_digest.is_empty()
        && envelope.intent_digest.as_slice()
            != crate::executor::json::canonical_json_digest(
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
    Ok(wire::ApplyResponse {
        state: state as i32,
        event: Some(encode_event_envelope(authority, event, &mut None)?),
    }
    .encode_to_vec())
}
