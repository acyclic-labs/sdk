//! Canonical typed wire conversions shared by native and WASM consumers.

use crate::core::RecordedScope;
use crate::{
    AgentId, Capabilities, Error, IdempotencyKey, OperationId, Result,
    contract::canonical_json_bytes,
    core::{Action, AggregateKind, Authority, Command, Event, EventPayload, EventReference, Scope, canonical_intent},
    wire,
};
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
use crate::core::ApplyResult;
use prost::Message as _;
mod admission;
pub(crate) use admission::Quota as CodecQuota;
#[cfg(all(test, feature = "filesystem"))]
#[path = "wire_codec/admission_tests.rs"]
mod admission_tests;

const EVENT_WIRE_VERSION: &str = "2";

pub(crate) fn encode_event(
    authority: &Authority, event: &Event,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Vec<u8>> {
    let native = native_event_work(work);
    let mut metadata = admission::Quota::new(work, admission::encode_metadata_quota(authority, event)?)?;
    let encoded = crate::store::history::canonical_bytes_with_work(
        &event.payload, acyclic_stream::MAX_RECORD_BYTES as u64, metadata.work());
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let encoded = encoded.map_err(|mut error| {
        if let Error::NativeEncode { custody, .. } = &mut error
            && custody.is_none()
            && native
        {
            let retained = metadata.retained();
            *custody = Some(metadata.work().as_deref_mut()
                .expect("native event observer").take_native_custody(retained)
                .expect("preclaimed event encoder diagnostic custody"));
            metadata.transfer();
        }
        error
    });
    let payload = encoded?;
    metadata.adopt(payload.capacity() as u64)?;
    if let Some(work) = metadata.work().as_deref_mut() {
        work.hashed_bytes(crate::FILE_DESCRIPTOR_SET.len() as u64)?;
    }
    let copied = admission::encode_metadata_copy_bytes(authority, event)?;
    if let Some(work) = metadata.work().as_deref_mut() { work.admit_copy(copied)?; }
    let envelope = event_envelope(authority, event, payload);
    if let Some(work) = metadata.work().as_deref_mut() { work.copy_bytes(copied)?; }
    metadata.replace(admission::envelope_owned(&envelope))?;
    let length = envelope.encoded_len();
    let mut output = Vec::new();
    if let Some(work) = metadata.work().as_deref_mut() {
        work.admit_encoding(length as u64)?;
        work.reserve_bytes(&mut output, length)?;
    } else {
        output.reserve_exact(length);
    }
    metadata.adopt(output.capacity() as u64)?;
    let result = if native {
        envelope.encode_raw(&mut output);
        Ok(())
    } else {
        envelope.encode(&mut output).map_err(|error| Error::Storage(error.to_string()))
    };
    if let Some(work) = metadata.work().as_deref_mut() { work.encoded_bytes(output.len() as u64)?; }
    drop(envelope);
    result?;
    metadata.replace(output.capacity() as u64)?;
    metadata.transfer();
    Ok(output)
}

/// Constructs the exact canonical event envelope without encoding it twice.
pub fn encode_event_envelope(authority: &Authority, event: &Event) -> Result<wire::EventEnvelope> {
    Ok(event_envelope(authority, event, canonical_json_bytes(&event.payload)?))
}

fn event_envelope(authority: &Authority, event: &Event, payload: Vec<u8>) -> wire::EventEnvelope {
    let (issuer, agent) = event.scope.wire_parts();
    wire::EventEnvelope {
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
}

/// Returns the event's original retained claim, excluding the authority header.
pub(crate) fn decode_event(
    bytes: &[u8], work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<((Authority, Event), u64)> {
    let native = native_event_work(work);
    if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(event_boundary(native, wire::ErrorCode::Storage,
            "event exceeds SDK record boundary"));
    }
    let admitted = admission::decode_quota(bytes, work)?;
    // Carrier fields and buffers must drop before this reservation.
    let mut payload_quota = admission::Quota::new(work, admitted)?;
    let diagnostic = admission::prost_diagnostic_preclaim(payload_quota.work())?;
    let mut envelope = wire::EventEnvelope::decode(bytes)
        .map_err(|source| diagnostic.forward(source))?;
    if let Some(work) = payload_quota.work().as_deref_mut() {
        work.hashed_bytes(crate::FILE_DESCRIPTOR_SET.len() as u64)?;
    }
    validate_event_protocol(envelope.protocol.as_ref(), native)?;
    let authority = decode_event_authority(envelope.authority.take()
        .ok_or_else(|| event_boundary(native, wire::ErrorCode::Storage,
            "event authority is missing"))?, native)?;
    let operation_id = if native {
        let id = uuid::Uuid::parse_str(&envelope.operation_id)
            .map_err(|_| event_boundary(native, wire::ErrorCode::Invalid,
                "operation UUID is invalid"))?;
        OperationId::from_bytes(*id.as_bytes())
    } else {
        OperationId::parse(&envelope.operation_id)?
    };
    let intent_digest: [u8; 32] = envelope.intent_digest.as_slice().try_into()
        .map_err(|_| event_boundary(native, wire::ErrorCode::Storage,
            "event intent digest must be 32 bytes"))?;
    let scope = envelope.scope.take()
        .ok_or_else(|| event_boundary(native, wire::ErrorCode::Storage,
            "event scope is missing"))?;
    let bounds = crate::contract::JsonBounds {
        maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
        maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64 + 1,
        maximum_depth: 256,
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let payload: EventPayload = if native {
        let decoded = crate::contract::json_from_slice_bounded_native_trusted(
            &envelope.canonical_payload_json, bounds, 0, wire::ErrorCode::Storage,
            crate::contract::JsonValueSchema::EventPayload, &mut |charge| {
                match charge {
                    crate::contract::JsonCodecCharge::Admission { value_shape, .. } => {
                        let witness = crate::contract::json_decode_owned_bytes(
                            value_shape, false, Some(wire::ErrorCode::Storage))?;
                        let retained = admitted.checked_add(witness.0)
                            .ok_or_else(|| event_boundary(native, wire::ErrorCode::Storage,
                                "event payload custody overflows"))?;
                        payload_quota.replace(retained)
                    }
                    charge => payload_quota.work().as_deref_mut()
                        .expect("native event observer").codec_charge(charge),
                }
            },
        );
        decoded.map_err(|mut error| {
            if let Error::NativeDecode { custody, .. } = &mut error
                && custody.is_none()
            {
                // Only split the already admitted original claim: a new quota check must never replace the actual foreign Serde cause.
                let retained = payload_quota.retained();
                *custody = Some(payload_quota.work().as_deref_mut()
                    .expect("native event observer").take_native_custody(retained)
                    .expect("preclaimed event decoder custody"));
                payload_quota.transfer();
            }
            error
        })?
    } else {
        decode_portable_event_payload(&envelope.canonical_payload_json, bounds,
            admitted, &mut payload_quota)?
    };
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let payload: EventPayload = decode_portable_event_payload(
        &envelope.canonical_payload_json, bounds, admitted, &mut payload_quota)?;
    if envelope.event_type != payload.tag() {
        return Err(event_boundary(native, wire::ErrorCode::Storage,
            "event type disagrees with its payload"));
    }
    let attestation = envelope.attestation.as_slice().try_into()
        .map_err(|_| event_boundary(native, wire::ErrorCode::Storage,
            "event attestation must be 32 bytes"))?;
    let scope_agent = if native && !scope.agent_id.is_empty() {
        let id = uuid::Uuid::parse_str(&scope.agent_id)
            .map_err(|_| event_boundary(native, wire::ErrorCode::Invalid,
                "scope agent UUID is invalid"))?;
        Some(AgentId::from_bytes(*id.as_bytes()))
    } else {
        parse_scope_agent(&scope.agent_id)?
    };
    let causal_parent = envelope.causal_parent.take().map(|reference| {
        if !native { return decode_reference(reference, Error::Storage); }
        Ok(EventReference {
            authority: decode_event_authority(reference.authority
                .ok_or_else(|| event_boundary(native, wire::ErrorCode::Storage,
                    "causal authority is missing"))?, native)?,
            revision: reference.revision,
        })
    }).transpose()?;
    let event = Event {
        revision: envelope.revision,
        operation_id,
        intent_digest,
        scope: RecordedScope::from_wire(scope.id, Capabilities::new(scope.capabilities),
            scope.issuer, scope_agent),
        attestation,
        causal_parent,
        payload,
    };
    let canonical = encode_event(&authority, &event, payload_quota.work())?;
    let matches = canonical.as_slice() == bytes;
    let capacity = canonical.capacity() as u64;
    drop(canonical);
    if let Some(work) = payload_quota.work().as_deref_mut() { work.release_owned(capacity)?; }
    if !matches {
        return Err(event_boundary(native, wire::ErrorCode::Storage,
            "event envelope is not canonical v2 encoding"));
    }
    // Retire all carrier-only fields before shrinking the original reservation.
    drop(envelope);
    drop(scope.agent_id);
    let retained = crate::core::event_owned_bytes(&event)?;
    let total = retained.checked_add(authority.id.capacity() as u64)
        .ok_or_else(|| event_boundary(native, wire::ErrorCode::Storage,
            "event custody overflows"))?;
    payload_quota.replace(total)?;
    payload_quota.transfer();
    Ok(((authority, event), retained))
}

fn decode_portable_event_payload(
    bytes: &[u8], bounds: crate::contract::JsonBounds, carrier: u64,
    quota: &mut admission::Quota<'_, '_>,
) -> Result<EventPayload> {
    crate::contract::event_payload_json_admission(bytes, bounds, &mut |charge| {
        match charge {
            crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes, .. } => {
                let retained = carrier.checked_add(maximum_owned_bytes)
                    .ok_or_else(|| Error::Storage("event payload custody overflows".into()))?;
                quota.replace(retained)
            }
            charge => match quota.work().as_deref_mut() {
                Some(work) => work.codec_charge(charge),
                None => Ok(()),
            },
        }
    })?;
    crate::contract::json_from_slice(bytes).map_err(|error| Error::Storage(error.to_string()))
}

fn native_event_work(work: &Option<&mut dyn crate::store::history::HistoryIo>) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    { work.as_deref().is_some_and(|work| work.original_native_work().is_some()) }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { let _ = work; false }
}

fn event_boundary(native: bool, code: wire::ErrorCode, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if native { return Error::NativeBoundary { code, reason }; }
    let _ = native;
    match code {
        wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        _ => Error::Storage(reason.into()),
    }
}

fn validate_event_protocol(protocol: Option<&wire::ProtocolIdentity>, native: bool) -> Result<()> {
    if !native { return validate_protocol(protocol, &protocol_identity(), Error::Storage); }
    let actual = protocol.ok_or_else(|| event_boundary(native, wire::ErrorCode::Storage,
        "protocol identity is missing"))?;
    let digest = blake3::hash(crate::FILE_DESCRIPTOR_SET).to_hex();
    if actual.version != EVENT_WIRE_VERSION || actual.descriptor_digest != digest.as_str() {
        return Err(event_boundary(native, wire::ErrorCode::Unsupported, "protocol identity mismatch"));
    }
    Ok(())
}

fn decode_event_authority(authority: wire::Authority, native: bool) -> Result<Authority> {
    if !native { return decode_authority(authority, Error::Storage); }
    let kind = match wire::AggregateKind::try_from(authority.kind) {
        Ok(wire::AggregateKind::Agent) => AggregateKind::Agent,
        Ok(wire::AggregateKind::Conversation) => AggregateKind::Conversation,
        Ok(wire::AggregateKind::Session) => AggregateKind::Session,
        Ok(wire::AggregateKind::Turn) => AggregateKind::Turn,
        Ok(wire::AggregateKind::Task) => AggregateKind::Task,
        Ok(wire::AggregateKind::Unspecified) => return Err(event_boundary(native,
            wire::ErrorCode::Storage, "aggregate kind is unspecified")),
        Err(_) => return Err(event_boundary(native,
            wire::ErrorCode::Storage, "aggregate kind is invalid")),
    };
    if authority.id.is_empty() {
        return Err(event_boundary(native, wire::ErrorCode::Storage, "authority identity is empty"));
    }
    Ok(Authority { kind, id: authority.id })
}

/// Decodes one canonical event payload and checks its generated discriminator.
pub fn decode_event_payload(event_type_name: &str, bytes: &[u8]) -> Result<EventPayload> {
    let payload: EventPayload = crate::contract::json_from_slice(bytes)
        .map_err(|error| Error::Storage(error.to_string()))?;
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
    let event = encode_event_envelope(authority, event)?;
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

/// Classifies malformed input: [`Error::Storage`] for persisted events, [`Error::Invalid`] for client requests.
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

/// Converts a semantic aggregate identity without conflating its typed kind.
#[must_use]
pub fn encode_authority(authority: &Authority) -> wire::Authority {
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
        let bytes = encode_event(&authority, &event, &mut None)?;
        assert_eq!(bytes, encode_event_envelope(&authority, &event)?.encode_to_vec());
        let mut envelope = wire::EventEnvelope::decode(bytes.as_slice())
            .map_err(|error| Error::Storage(error.to_string()))?;
        envelope
            .protocol
            .as_mut()
            .ok_or_else(|| Error::Storage("protocol".into()))?
            .version = "1".into();
        assert!(matches!(
            decode_event(&envelope.encode_to_vec(), &mut None),
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
            decode_event(&envelope.encode_to_vec(), &mut None),
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
                    attachments: crate::conversation::ReferencedAttachments::Inline { items: Vec::new() },
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
        let native = encode_event(&authority, &event, &mut None)?;
        assert_eq!(native, encode_event(&authority, &replayed, &mut None)?);
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

    #[test]
    fn native_command_envelopes_require_exact_payload_protocol_and_retry_identity() -> Result<()> {
        let action = Action::TransitionLifecycle { to: crate::core::LifecycleState::Active, reason: None };
        let envelope = wire::CommandEnvelope {
            protocol: Some(protocol_identity()),
            authority: Some(wire::Authority { kind: wire::AggregateKind::Task as i32, id: "native-source".into() }),
            operation: Some(wire::OperationIdentity {
                operation_id: OperationId::from_bytes([31; 16]).to_string(),
                idempotency_key: "native-once".into(),
            }),
            scope: Some(wire::Scope {
                id: "explicit-producer-scope".into(), issuer: "producer".into(),
                capabilities: vec!["lifecycle:transition".into()], proof: vec![7; 32],
                ..wire::Scope::default()
            }),
            action_type: "transition_lifecycle".into(),
            canonical_action_json: canonical_json_bytes(&action)?,
            ..wire::CommandEnvelope::default()
        };
        let (authority, command) = decode_command_envelope(envelope.clone())?;
        assert_eq!(authority.kind, AggregateKind::Task);
        assert_eq!(command.operation_id, OperationId::from_bytes([31; 16]));
        assert_eq!(command.idempotency_key, IdempotencyKey::new("native-once")?);
        let mut wrong = envelope.clone();
        wrong.canonical_action_json = serde_json::to_vec_pretty(&action)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(matches!(decode_command_envelope(wrong), Err(Error::Invalid(_))));
        let mut wrong = envelope.clone();
        wrong.action_type = "append_custom".into();
        assert!(matches!(decode_command_envelope(wrong), Err(Error::Invalid(_))));
        let mut wrong = envelope.clone();
        wrong.operation.as_mut().ok_or_else(|| Error::Invalid("operation missing".into()))?.idempotency_key.clear();
        assert!(matches!(decode_command_envelope(wrong), Err(Error::Invalid(_))));
        let mut wrong = envelope;
        wrong.intent_digest = vec![0; 32];
        assert!(matches!(decode_command_envelope(wrong), Err(Error::Conflict(_))));
        Ok(())
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    mod native_custody_controls {
        use super::*;
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};

        fn budget() -> WorkBudget {
            WorkBudget {
                source_bytes_read: 16_777_216, bytes_copied: 16_777_216,
                bytes_encoded: 16_777_216, bytes_hashed: 16_777_216,
                items_examined: 1_000_000, allocation_operations: 10_000,
                peak_allocation_bytes: 16_777_216, ..WorkBudget::default()
            }
        }

        fn fixture() -> (Authority, Event) {
            (Authority { kind: AggregateKind::Conversation, id: "native-original".into() },
                Event {
                    revision: 1, operation_id: OperationId::from_bytes([42; 16]),
                    intent_digest: [43; 32], attestation: [44; 32], causal_parent: None,
                    scope: RecordedScope::from_wire("scope".into(),
                        Capabilities::new(["conversation:bind"]), "issuer".into(), None),
                    payload: EventPayload::ConversationBound { agent: AgentId::from_bytes([45; 16]) },
                })
        }

        #[test]
        fn native_retained_scalar_is_original_event_claim_without_header() -> Result<()> {
            let (authority, event) = fixture();
            let bytes = encode_event(&authority, &event, &mut None)?;
            let cancellation = CancellationToken::new();
            let work = NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
            let baseline = work.live_allocation_bytes();
            let mut io = crate::store::history::native_work_io(&work).expect("original observer");
            let ((decoded_authority, decoded_event), retained) =
                decode_event(&bytes, &mut Some(&mut io))?;
            assert_eq!((&decoded_authority, &decoded_event), (&authority, &event));
            assert_eq!(retained, crate::core::event_owned_bytes(&decoded_event)?);
            let custody = io.into_custody();
            assert_eq!(custody.bytes(), retained + decoded_authority.id.capacity() as u64);
            assert_eq!(work.receipt().backend_read_operations, 0);
            assert_eq!(work.receipt().authority_bytes_read, 0);
            drop(decoded_event);
            drop(decoded_authority);
            drop(custody);
            assert_eq!(work.live_allocation_bytes(), baseline);
            Ok(())
        }

        #[test]
        fn native_quota_precedes_allocating_original_deserialize_failure() -> Result<()> {
            let (authority, event) = fixture();
            let mut envelope = encode_event_envelope(&authority, &event)?;
            envelope.canonical_payload_json =
                format!(r#"{{"type":"not_an_event","untrusted":["{}"]}}"#, "x".repeat(4096)).into_bytes();
            assert!(serde_json::from_slice::<EventPayload>(&envelope.canonical_payload_json).is_err());
            let bytes = envelope.encode_to_vec();
            let carrier = admission::decode_quota(&bytes, &mut None)?;
            let mut allowance = budget();
            allowance.peak_allocation_bytes =
                NativeMaterializationWork::accounting_state_bytes() + carrier + 8192;
            let cancellation = CancellationToken::new();
            let work = NativeMaterializationWork::new(allowance, &cancellation).expect("original ledger");
            let mut io = crate::store::history::native_work_io(&work).expect("original observer");
            let error = decode_event(&bytes, &mut Some(&mut io))
                .expect_err("closed payload admission must deny before original Deserialize");
            assert!(!matches!(error, Error::NativeDecode { .. } | Error::NativeProstDecode { .. }));
            assert_eq!(work.receipt().bytes_encoded, 0);
            assert!(work.receipt().items_examined > 0);
            drop(error);
            assert_eq!(io.into_custody().bytes(), 0);
            Ok(())
        }

        #[test]
        fn native_partial_payload_cause_keeps_original_guard_until_cause_drop() -> Result<()> {
            let (authority, event) = fixture();
            let mut envelope = encode_event_envelope(&authority, &event)?;
            envelope.canonical_payload_json =
                br#"{"type":"conversation_bound","agent":"not-a-UUID"}"#.to_vec();
            let original = serde_json::from_slice::<EventPayload>(&envelope.canonical_payload_json)
                .expect_err("original concrete UUID parser rejects data");
            let bytes = envelope.encode_to_vec();
            let cancellation = CancellationToken::new();
            let work = NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
            let baseline = work.live_allocation_bytes();
            let mut io = crate::store::history::native_work_io(&work).expect("original observer");
            let error = decode_event(&bytes, &mut Some(&mut io)).expect_err("original typed cause");
            let Error::NativeDecode { code, source, custody: Some(custody) } = error else {
                panic!("typed original source and original guard required");
            };
            assert_eq!(code, wire::ErrorCode::Storage);
            assert_eq!(source.classify(), original.classify());
            assert_eq!((source.line(), source.column()), (original.line(), original.column()));
            assert!(work.shares_ledger(custody.work()));
            assert!(custody.bytes() > 0);
            assert_eq!(io.into_custody().bytes(), 0);
            assert_eq!(work.live_allocation_bytes(), baseline + custody.bytes());
            drop(source);
            assert_eq!(work.live_allocation_bytes(), baseline + custody.bytes());
            drop(custody);
            assert_eq!(work.live_allocation_bytes(), baseline);
            Ok(())
        }

        #[test]
        fn native_prost_partial_carrier_retains_actual_decode_cause() -> Result<()> {
            let (authority, event) = fixture();
            let mut bytes = encode_event(&authority, &event, &mut None)?;
            // Duplicate operation_id with invalid UTF-8 fails inside the original prost engine after its preceding fields allocated.
            bytes.extend_from_slice(&[0x22, 0x01, 0xff]);
            let original = wire::EventEnvelope::decode(bytes.as_slice()).expect_err("original prost error");
            let cancellation = CancellationToken::new();
            let work = NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
            let baseline = work.live_allocation_bytes();
            let mut io = crate::store::history::native_work_io(&work).expect("original observer");
            let error = decode_event(&bytes, &mut Some(&mut io)).expect_err("original prost failure");
            let Error::NativeProstDecode { code, source, custody: Some(custody) } = error else {
                panic!("original prost source and guard required");
            };
            assert_eq!(code, wire::ErrorCode::Storage);
            assert_eq!(source.to_string(), original.to_string());
            assert_eq!(io.into_custody().bytes(), 0);
            assert!(custody.bytes() > 0);
            assert_eq!(work.live_allocation_bytes(), baseline + custody.bytes());
            drop(source);
            drop(custody);
            assert_eq!(work.live_allocation_bytes(), baseline);
            Ok(())
        }

        #[test]
        fn native_canonical_tamper_retires_all_original_carriers() -> Result<()> {
            let (authority, event) = fixture();
            let mut bytes = encode_event(&authority, &event, &mut None)?;
            bytes.extend_from_slice(&[0x78, 0x01]);
            let cancellation = CancellationToken::new();
            let work = NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
            let baseline = work.live_allocation_bytes();
            let mut io = crate::store::history::native_work_io(&work).expect("original observer");
            let error = decode_event(&bytes, &mut Some(&mut io)).expect_err("canonical tamper");
            assert!(matches!(error, Error::NativeBoundary { code: wire::ErrorCode::Storage,
                reason: "event envelope is not canonical v2 encoding" }));
            assert!(work.receipt().bytes_encoded > 0);
            drop(error);
            assert_eq!(io.into_custody().bytes(), 0);
            assert_eq!(work.live_allocation_bytes(), baseline);
            Ok(())
        }
    }
}
