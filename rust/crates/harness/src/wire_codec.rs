use crate::core::RecordedScope;
use crate::{
    AgentId, Capabilities, Error, OperationId, Result,
    contract::canonical_json_bytes,
    core::{AggregateKind, Authority, Event, EventPayload, EventReference, Scope},
    wire,
};
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
use crate::{
    IdempotencyKey,
    core::{Action, ApplyResult, Command, canonical_intent},
};
use prost::Message as _;

const EVENT_WIRE_VERSION: &str = "2";

pub(crate) fn encode_event(authority: &Authority, event: &Event) -> Result<Vec<u8>> {
    let payload = canonical_json_bytes(&event.payload)?;
    let (issuer, agent) = event.scope.wire_parts();
    Ok(wire::EventEnvelope {
        protocol: Some(protocol_identity()),
        authority: Some(encode_authority(authority)),
        revision: event.revision,
        operation_id: event.operation_id.to_string(),
        intent_digest: event.intent_digest.to_vec(),
        scope: Some(wire::RecordedScope {
            id: event.scope.id().into(),
            capabilities: event
                .scope
                .capabilities()
                .iter()
                .map(ToOwned::to_owned)
                .collect(),
            issuer: issuer.into(),
            agent_id: agent.map_or_else(String::new, |value| value.to_string()),
        }),
        causal_parent: event.causal_parent.as_ref().map(encode_reference),
        event_type: event.payload.tag().into(),
        canonical_payload_json: payload,
        attestation: event.attestation.to_vec(),
    }
    .encode_to_vec())
}

pub(crate) fn decode_event(bytes: &[u8]) -> Result<(Authority, Event)> {
    let envelope =
        wire::EventEnvelope::decode(bytes).map_err(|error| Error::Storage(error.to_string()))?;
    validate_protocol(
        envelope.protocol.as_ref(),
        &protocol_identity(),
        Error::Storage,
    )?;
    let authority = decode_authority(
        envelope
            .authority
            .ok_or_else(|| Error::Storage("event authority is missing".into()))?,
        Error::Storage,
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
    if envelope.event_type != payload.tag() {
        return Err(Error::Storage(
            "event type disagrees with its payload".into(),
        ));
    }
    let attestation = envelope
        .attestation
        .try_into()
        .map_err(|_| Error::Storage("event attestation must be 32 bytes".into()))?;
    let event = Event {
        revision: envelope.revision,
        operation_id,
        intent_digest,
        scope: RecordedScope::from_wire(
            scope.id,
            Capabilities::new(scope.capabilities),
            scope.issuer,
            parse_scope_agent(&scope.agent_id)?,
        ),
        attestation,
        causal_parent: envelope
            .causal_parent
            .map(|reference| decode_reference(reference, Error::Storage))
            .transpose()?,
        payload,
    };
    if encode_event(&authority, &event)?.as_slice() != bytes {
        return Err(Error::Storage(
            "event envelope is not canonical v2 encoding".into(),
        ));
    }
    Ok((authority, event))
}

/// Decodes one canonical event payload and checks its generated discriminator.
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) fn decode_event_payload(event_type_name: &str, bytes: &[u8]) -> Result<EventPayload> {
    let payload: EventPayload =
        serde_json::from_slice(bytes).map_err(|error| Error::Storage(error.to_string()))?;
    if event_type_name != payload.tag() {
        return Err(Error::Storage(
            "event type disagrees with its payload".into(),
        ));
    }
    Ok(payload)
}

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) fn decode_command(bytes: &[u8]) -> Result<(Authority, Command)> {
    let envelope =
        wire::CommandEnvelope::decode(bytes).map_err(|error| Error::Invalid(error.to_string()))?;
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
    let action: Action = serde_json::from_slice(&envelope.canonical_action_json)
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
        idempotency_key: IdempotencyKey(operation.idempotency_key),
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
    wire::Error {
        code: error.code() as i32,
        message: error.to_string(),
        operation_id: match error {
            Error::Indeterminate(operation_id) => operation_id.to_string(),
            _ => String::new(),
        },
    }
}

/// Classifies malformed input: [`Error::Storage`] for persisted events,
/// [`Error::Invalid`] for client requests.
pub(crate) type Fault = fn(String) -> Error;

/// Requires the exact `expected` protocol identity; `missing` classifies its absence.
pub(crate) fn validate_protocol(
    protocol: Option<&wire::ProtocolIdentity>,
    expected: &wire::ProtocolIdentity,
    missing: Fault,
) -> Result<()> {
    match protocol {
        None => Err(missing("protocol identity is missing".into())),
        Some(actual) if actual == expected => Ok(()),
        Some(_) => Err(Error::Unsupported("protocol identity mismatch".into())),
    }
}

pub(crate) fn encode_authority(authority: &Authority) -> wire::Authority {
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

pub(crate) fn decode_aggregate_kind(kind: i32, fault: Fault) -> Result<AggregateKind> {
    match wire::AggregateKind::try_from(kind)
        .map_err(|_| fault("aggregate kind is invalid".into()))?
    {
        wire::AggregateKind::Agent => Ok(AggregateKind::Agent),
        wire::AggregateKind::Conversation => Ok(AggregateKind::Conversation),
        wire::AggregateKind::Session => Ok(AggregateKind::Session),
        wire::AggregateKind::Turn => Ok(AggregateKind::Turn),
        wire::AggregateKind::Task => Ok(AggregateKind::Task),
        wire::AggregateKind::Unspecified => Err(fault("aggregate kind is unspecified".into())),
    }
}

pub(crate) fn decode_authority(authority: wire::Authority, fault: Fault) -> Result<Authority> {
    let kind = decode_aggregate_kind(authority.kind, fault)?;
    if authority.id.is_empty() {
        return Err(fault("authority identity is empty".into()));
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

fn decode_reference(reference: wire::EventReference, fault: Fault) -> Result<EventReference> {
    Ok(EventReference {
        authority: decode_authority(
            reference
                .authority
                .ok_or_else(|| fault("causal authority is missing".into()))?,
            fault,
        )?,
        revision: reference.revision,
    })
}

pub(crate) fn decode_scope(scope: wire::Scope) -> Result<Scope> {
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
        parse_scope_agent(&scope.agent_id)?,
        parent_proof,
        proof,
    ))
}

fn parse_scope_agent(value: &str) -> Result<Option<AgentId>> {
    if value.is_empty() {
        Ok(None)
    } else {
        AgentId::parse(value).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::{
        ConversationMessage, FileDescriptor, FileRef, MessageKind, VolumeClass, VolumeOwner,
        VolumeRef,
    };
    use crate::core::{Action, ApplyResult, AuthorityIssuer, Command, Reducer, SchemaRegistry};
    use crate::resources::ProviderRef;
    use crate::{AgentId, Capabilities, IdempotencyKey, OperationId};
    use std::collections::BTreeMap;
    use uuid::Uuid;

    #[test]
    fn protocol_digest_and_error_semantics_are_preserved() -> Result<()> {
        let event = Event {
            revision: 1,
            operation_id: OperationId::from_bytes([1; 16]),
            intent_digest: [2; 32],
            scope: RecordedScope::from_wire(
                "scope".into(),
                Capabilities::new(["event:append"]),
                "issuer".into(),
                None,
            ),
            attestation: [3; 32],
            causal_parent: None,
            payload: EventPayload::Custom {
                record: crate::core::ExtensionRecord {
                    name: "example.event".into(),
                    version: 1,
                    schema_digest: [4; 32],
                    implementation_digest: [5; 32],
                    fork_policy: crate::core::ExtensionForkPolicy::Inherit,
                    content: FileRef::new(
                        VolumeRef::new(
                            ProviderRef::new("test", "filesystem", "2")?,
                            "extension",
                            VolumeClass::AgentPrivate,
                            VolumeOwner::Agent(AgentId::from_bytes([9; 16])),
                        )?,
                        "extension/event.json",
                        "generation-1",
                        FileDescriptor::from_bytes(b"null", "application/json")?,
                        "event.json",
                    )?,
                },
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
            .version = "1".into();
        assert!(matches!(
            decode_event(&envelope.encode_to_vec()),
            Err(Error::Unsupported(_))
        ));
        envelope
            .protocol
            .as_mut()
            .ok_or_else(|| Error::Storage("protocol".into()))?
            .version = "2".into();
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
        let authority = Authority {
            kind: AggregateKind::Conversation,
            id: "conversation-1".into(),
        };
        let issuer = AuthorityIssuer::new("test", [7; 32], authority.clone());
        let mut reducer = Reducer::new(authority.clone(), issuer.verifier(), SchemaRegistry::new());
        let agent = AgentId::from_bytes([4; 16]);
        let scope = issuer.root(
            "root",
            Capabilities::new(["conversation:bind", "conversation:append"]),
        );
        reducer.apply(Command {
            operation_id: OperationId::from_bytes([1; 16]),
            idempotency_key: IdempotencyKey("bind-1".into()),
            expected_revision: 0,
            scope: scope.clone(),
            causal_parent: None,
            action: Action::BindConversation { agent },
        })?;
        let file = FileRef::new(
            VolumeRef::new(
                ProviderRef::new("fixture", "filesystem", "2")?,
                "scratch",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(agent),
            )?,
            "messages/input.txt",
            "pinned-generation",
            FileDescriptor::from_bytes(b"fixture text", "text/plain")?,
            "input.txt",
        )?;
        let command = Command {
            operation_id: OperationId::from_bytes([2; 16]),
            idempotency_key: IdempotencyKey("append-2".into()),
            expected_revision: 1,
            scope,
            causal_parent: None,
            action: Action::AppendConversationMessage {
                message: Box::new(ConversationMessage {
                    id: Uuid::from_bytes([3; 16]),
                    sequence: 1,
                    kind: MessageKind::User,
                    content: file,
                    attachments: Vec::new().into(),
                    reply_to: None,
                    tool_call_id: None,
                    extensions: BTreeMap::new(),
                }),
            },
        };
        let ApplyResult::Applied { event } = reducer.apply(command.clone())? else {
            return Err(Error::Conflict("first vector application replayed".into()));
        };
        let ApplyResult::Replayed { event: replayed } = reducer.apply(command)? else {
            return Err(Error::Conflict("vector retry was not replayed".into()));
        };
        let native = encode_event(&authority, &event)?;
        assert_eq!(native, encode_event(&authority, &replayed)?);
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../conformance/native-wasm-event-v2.json"))
                .map_err(|error| Error::Invalid(error.to_string()))?;
        let expected = fixture
            .get("event_wire_hex")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| Error::Invalid("native/WASM fixture is missing event bytes".into()))?;
        let actual = native
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(
            actual, expected,
            "replace the native/WASM fixture with: {actual}"
        );
        Ok(())
    }
}
