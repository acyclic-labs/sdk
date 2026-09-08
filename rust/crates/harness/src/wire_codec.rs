use crate::{
    Capabilities, Error, OperationId, Result,
    core::{AggregateKind, Authority, Event, EventPayload, EventReference, Scope},
    wire,
};
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
use crate::{
    IdempotencyKey,
    core::{Action, ApplyResult, Command, canonical_intent},
};
use prost::Message as _;

const EVENT_WIRE_VERSION: &str = "1";

pub(crate) fn encode_event(authority: &Authority, event: &Event) -> Result<Vec<u8>> {
    let payload =
        serde_json::to_vec(&event.payload).map_err(|error| Error::Invalid(error.to_string()))?;
    let (issuer, parent_proof, proof) = event.scope.wire_parts();
    Ok(wire::EventEnvelope {
        protocol: Some(protocol_identity()),
        authority: Some(encode_authority(authority)),
        revision: event.revision,
        operation_id: event.operation_id.to_string(),
        intent_digest: event.intent_digest.to_vec(),
        scope: Some(wire::Scope {
            id: event.scope.id().into(),
            capabilities: event
                .scope
                .capabilities()
                .iter()
                .map(ToOwned::to_owned)
                .collect(),
            issuer: issuer.into(),
            parent_proof: parent_proof.map_or_else(Vec::new, |value| value.to_vec()),
            proof: proof.to_vec(),
        }),
        causal_parent: event.causal_parent.as_ref().map(encode_reference),
        event_type: event_type(&event.payload).into(),
        canonical_payload_json: payload,
    }
    .encode_to_vec())
}

#[cfg(feature = "host")]
pub(crate) fn decode_event(bytes: &[u8]) -> Result<(Authority, Event)> {
    let envelope =
        wire::EventEnvelope::decode(bytes).map_err(|error| Error::Storage(error.to_string()))?;
    validate_protocol(envelope.protocol.as_ref())?;
    let authority = decode_authority(
        envelope
            .authority
            .ok_or_else(|| Error::Storage("event authority is missing".into()))?,
    )?;
    let operation_id = OperationId::parse(&envelope.operation_id)?;
    let intent_digest: [u8; 32] = envelope
        .intent_digest
        .try_into()
        .map_err(|_| Error::Storage("event intent digest must be 32 bytes".into()))?;
    let scope = envelope
        .scope
        .ok_or_else(|| Error::Storage("event scope is missing".into()))?;
    let payload: EventPayload = serde_json::from_slice(&envelope.canonical_payload_json)
        .map_err(|error| Error::Storage(error.to_string()))?;
    if envelope.event_type != event_type(&payload) {
        return Err(Error::Storage(
            "event type disagrees with its payload".into(),
        ));
    }
    let parent_proof = if scope.parent_proof.is_empty() {
        None
    } else {
        Some(
            scope
                .parent_proof
                .try_into()
                .map_err(|_| Error::Storage("scope parent proof must be 32 bytes".into()))?,
        )
    };
    let proof = scope
        .proof
        .try_into()
        .map_err(|_| Error::Storage("scope proof must be 32 bytes".into()))?;
    Ok((
        authority,
        Event {
            revision: envelope.revision,
            operation_id,
            intent_digest,
            scope: Scope::from_wire(
                scope.id,
                Capabilities::new(scope.capabilities),
                scope.issuer,
                parent_proof,
                proof,
            ),
            causal_parent: envelope.causal_parent.map(decode_reference).transpose()?,
            payload,
        },
    ))
}

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) fn decode_command(bytes: &[u8]) -> Result<(Authority, Command)> {
    let envelope =
        wire::CommandEnvelope::decode(bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    validate_protocol(envelope.protocol.as_ref())?;
    let authority = decode_authority(
        envelope
            .authority
            .ok_or_else(|| Error::Invalid("command authority is missing".into()))?,
    )?;
    let operation = envelope
        .operation
        .ok_or_else(|| Error::Invalid("command operation is missing".into()))?;
    let action: Action = serde_json::from_slice(&envelope.canonical_action_json)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    if envelope.action_type != action_type(&action) {
        return Err(Error::Invalid(
            "command action type disagrees with its payload".into(),
        ));
    }
    let command = Command {
        operation_id: OperationId::parse(&operation.operation_id)?,
        idempotency_key: IdempotencyKey(operation.idempotency_key),
        expected_revision: envelope.expected_revision,
        scope: decode_scope(
            envelope
                .scope
                .ok_or_else(|| Error::Invalid("command scope is missing".into()))?,
        )?,
        causal_parent: envelope.causal_parent.map(decode_reference).transpose()?,
        action,
    };
    if !envelope.intent_digest.is_empty()
        && envelope.intent_digest.as_slice() != canonical_intent(&command)?
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
    let event = wire::EventEnvelope::decode(encode_event(authority, event)?.as_slice())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(wire::ApplyResponse {
        state: state as i32,
        event: Some(event),
    }
    .encode_to_vec())
}

pub(crate) fn protocol_identity() -> wire::ProtocolIdentity {
    wire::ProtocolIdentity {
        version: EVENT_WIRE_VERSION.into(),
        descriptor_digest: blake3::hash(crate::FILE_DESCRIPTOR_SET)
            .to_hex()
            .to_string(),
    }
}

/// Converts a semantic error without losing storage or indeterminate state.
#[must_use]
pub fn encode_error(error: &Error) -> wire::Error {
    let (code, operation_id) = match error {
        Error::NotFound(_) => (wire::ErrorCode::NotFound, String::new()),
        Error::Conflict(_) => (wire::ErrorCode::Conflict, String::new()),
        Error::Unsupported(_) => (wire::ErrorCode::Unsupported, String::new()),
        Error::Invalid(_) => (wire::ErrorCode::Invalid, String::new()),
        Error::Unauthorized(_) => (wire::ErrorCode::Unauthorized, String::new()),
        Error::Storage(_) => (wire::ErrorCode::Storage, String::new()),
        Error::Indeterminate(operation_id) => {
            (wire::ErrorCode::Indeterminate, operation_id.to_string())
        }
    };
    wire::Error {
        code: code as i32,
        message: error.to_string(),
        operation_id,
    }
}

pub(crate) fn validate_protocol(protocol: Option<&wire::ProtocolIdentity>) -> Result<()> {
    let actual = protocol.ok_or_else(|| Error::Storage("event protocol is missing".into()))?;
    let expected = protocol_identity();
    if actual.version != EVENT_WIRE_VERSION
        || actual.descriptor_digest != expected.descriptor_digest
    {
        return Err(Error::Unsupported("unsupported event wire version".into()));
    }
    Ok(())
}

fn event_type(payload: &EventPayload) -> &'static str {
    match payload {
        EventPayload::LifecycleTransitioned { .. } => "lifecycle_transitioned",
        EventPayload::Custom { .. } => "custom",
        EventPayload::EffectPlanned { .. } => "effect_planned",
        EventPayload::EffectDispatched { .. } => "effect_dispatched",
        EventPayload::EffectResolved { .. } => "effect_resolved",
        EventPayload::InteractionOpened { .. } => "interaction_opened",
        EventPayload::InteractionResolved { .. } => "interaction_resolved",
        EventPayload::ForkPublished { .. } => "fork_published",
    }
}

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
fn action_type(action: &Action) -> &'static str {
    match action {
        Action::TransitionLifecycle { .. } => "transition_lifecycle",
        Action::AppendCustom { .. } => "append_custom",
        Action::PlanEffect { .. } => "plan_effect",
        Action::MarkEffectDispatched { .. } => "mark_effect_dispatched",
        Action::ResolveEffect { .. } => "resolve_effect",
        Action::OpenInteraction { .. } => "open_interaction",
        Action::ResolveInteraction { .. } => "resolve_interaction",
        Action::PublishFork { .. } => "publish_fork",
    }
}

fn encode_authority(authority: &Authority) -> wire::Authority {
    wire::Authority {
        kind: match authority.kind {
            AggregateKind::Agent => wire::AggregateKind::Agent as i32,
            AggregateKind::Conversation => wire::AggregateKind::Conversation as i32,
            AggregateKind::Session => wire::AggregateKind::Session as i32,
            AggregateKind::Turn => wire::AggregateKind::Turn as i32,
            AggregateKind::Task => wire::AggregateKind::Task as i32,
        },
        id: authority.id.clone(),
    }
}

fn decode_authority(authority: wire::Authority) -> Result<Authority> {
    let kind = match wire::AggregateKind::try_from(authority.kind)
        .map_err(|_| Error::Storage("event aggregate kind is invalid".into()))?
    {
        wire::AggregateKind::Agent => AggregateKind::Agent,
        wire::AggregateKind::Conversation => AggregateKind::Conversation,
        wire::AggregateKind::Session => AggregateKind::Session,
        wire::AggregateKind::Turn => AggregateKind::Turn,
        wire::AggregateKind::Task => AggregateKind::Task,
        wire::AggregateKind::Unspecified => {
            return Err(Error::Storage("event aggregate kind is unspecified".into()));
        }
    };
    if authority.id.is_empty() {
        return Err(Error::Storage("event authority identity is empty".into()));
    }
    Ok(Authority {
        kind,
        id: authority.id,
    })
}

fn encode_reference(reference: &EventReference) -> wire::EventReference {
    wire::EventReference {
        authority: Some(encode_authority(&reference.authority)),
        revision: reference.revision,
    }
}

fn decode_reference(reference: wire::EventReference) -> Result<EventReference> {
    Ok(EventReference {
        authority: decode_authority(
            reference
                .authority
                .ok_or_else(|| Error::Storage("causal authority is missing".into()))?,
        )?,
        revision: reference.revision,
    })
}

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
fn decode_scope(scope: wire::Scope) -> Result<Scope> {
    let parent_proof = if scope.parent_proof.is_empty() {
        None
    } else {
        Some(
            scope
                .parent_proof
                .try_into()
                .map_err(|_| Error::Invalid("scope parent proof must be 32 bytes".into()))?,
        )
    };
    let proof = scope
        .proof
        .try_into()
        .map_err(|_| Error::Invalid("scope proof must be 32 bytes".into()))?;
    Ok(Scope::from_wire(
        scope.id,
        Capabilities::new(scope.capabilities),
        scope.issuer,
        parent_proof,
        proof,
    ))
}

#[cfg(all(test, feature = "host"))]
mod tests {
    use super::*;
    use crate::OperationId;

    #[test]
    fn protocol_digest_and_error_semantics_are_preserved() -> Result<()> {
        let event = Event {
            revision: 1,
            operation_id: OperationId::from_bytes([1; 16]),
            intent_digest: [2; 32],
            scope: Scope::from_wire(
                "scope".into(),
                Capabilities::new(["event:append"]),
                "issuer".into(),
                None,
                [3; 32],
            ),
            causal_parent: None,
            payload: EventPayload::Custom {
                schema: "example.event".into(),
                version: 1,
                schema_digest: [4; 32],
                value: serde_json::Value::Null,
            },
        };
        let authority = Authority {
            kind: AggregateKind::Task,
            id: "task-1".into(),
        };
        let bytes = encode_event(&authority, &event)?;
        let mut envelope = wire::EventEnvelope::decode(bytes.as_slice())
            .map_err(|error| Error::Storage(error.to_string()))?;
        envelope
            .protocol
            .as_mut()
            .ok_or_else(|| Error::Storage("protocol".into()))?
            .descriptor_digest = "wrong".into();
        assert!(matches!(
            decode_event(&envelope.encode_to_vec()),
            Err(Error::Unsupported(_))
        ));

        let operation = OperationId::from_bytes([8; 16]);
        let encoded = encode_error(&Error::Indeterminate(operation));
        assert_eq!(encoded.code, wire::ErrorCode::Indeterminate as i32);
        assert_eq!(encoded.operation_id, operation.to_string());
        Ok(())
    }
}
