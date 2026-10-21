use super::*;
use prost::Message as _;

pub(crate) fn encode_event(
    authority: &Authority,
    event: &Event,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Vec<u8>> {
    let mut quota = CodecQuota::new(work, 0)?;
    let envelope = encode_event_envelope(authority, event, quota.work())?;
    quota.adopt(admission::envelope_owned(&envelope))?;
    let length = envelope.encoded_len();
    let mut bytes = Vec::new();
    if let Some(io) = quota.work().as_deref_mut() {
        io.admit_encoding(length as u64)?;
        crate::store::history::reserve(&mut bytes, length, io)?;
    } else {
        bytes.reserve_exact(length);
    }
    quota.adopt(bytes.capacity() as u64)?;
    envelope.encode_raw(&mut bytes);
    if let Some(io) = quota.work().as_deref_mut() {
        io.encoded_bytes(bytes.len() as u64)?;
    }
    drop(envelope);
    quota.replace(bytes.capacity() as u64)?;
    quota.transfer();
    Ok(bytes)
}
/// Constructs a canonical envelope, retaining its buffers on the supplied observer.
pub fn encode_event_envelope(
    authority: &Authority,
    event: &Event,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<wire::EventEnvelope> {
    let (owned, copied) = admission::metadata_quota(authority, event)?;
    let mut quota = CodecQuota::new(work, owned)?;
    let code = quota
        .work()
        .as_deref()
        .is_some_and(crate::store::operations::original_native_decode_mode)
        .then_some(Invalid);
    let result = crate::executor::json::canonical_json_bytes_bounded(
        &event.payload,
        PAYLOAD_BOUNDS,
        code,
        &mut |charge| match quota.work().as_deref_mut() {
            Some(io) => io.codec_charge(charge),
            None => Ok(()),
        },
    );
    let payload = result.map_err(|error| quota.json_error(error))?;
    quota.adopt(payload.capacity() as u64)?;
    if let Some(io) = quota.work().as_deref_mut() {
        io.hashed_bytes(crate::FILE_DESCRIPTOR_SET.len() as u64)?;
        io.admit_copy(copied)?;
    }
    let (issuer, agent) = event.scope.wire_parts();
    let envelope = wire::EventEnvelope {
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
    };
    if let Some(io) = quota.work().as_deref_mut() {
        io.copy_bytes(copied)?;
    }
    quota.replace(admission::envelope_owned(&envelope))?;
    quota.transfer();
    Ok(envelope)
}
pub(crate) fn decode_event(
    bytes: &[u8],
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<((Authority, Event), u64)> {
    let (owned, guard) = admission::decode_quota(bytes, work)?;
    // The carrier must drop before its reservation.
    let mut quota = CodecQuota::new(work, owned)?;
    let mut envelope = wire::EventEnvelope::decode(bytes)
        .map_err(|source| admission::prost_error(guard, source))?;
    if let Some(io) = quota.work().as_deref_mut() {
        io.hashed_bytes(crate::FILE_DESCRIPTOR_SET.len() as u64)?;
    }
    validate_protocol(
        envelope.protocol.as_ref(),
        COMPILED_PROTOCOL,
        Storage,
        quota.work(),
    )?;
    let authority = decode_authority(
        envelope
            .authority
            .take()
            .ok_or_else(|| boundary(quota.work(), Storage, "missing event authority"))?,
        Storage,
        quota.work(),
    )?;
    let operation_id = OperationId::from_bytes(
        *uuid::Uuid::parse_str(&envelope.operation_id)
            .map_err(|_| boundary(quota.work(), Invalid, "invalid operation UUID"))?
            .as_bytes(),
    );
    let intent_digest = envelope
        .intent_digest
        .as_slice()
        .try_into()
        .map_err(|_| boundary(quota.work(), Storage, "invalid event intent digest"))?;
    let scope = envelope
        .scope
        .take()
        .ok_or_else(|| boundary(quota.work(), Storage, "missing event scope"))?;
    let (payload, claim) = decode_event_payload(
        &envelope.event_type,
        &envelope.canonical_payload_json,
        quota.work(),
    )?;
    quota.adopt(claim)?;
    let event = Event {
        revision: envelope.revision,
        operation_id,
        intent_digest,
        scope: RecordedScope::from_wire(
            scope.id,
            Capabilities::new(scope.capabilities),
            scope.issuer,
            parse_scope_agent(&scope.agent_id, quota.work())?,
        ),
        attestation: envelope
            .attestation
            .as_slice()
            .try_into()
            .map_err(|_| boundary(quota.work(), Storage, "invalid event attestation"))?,
        causal_parent: envelope
            .causal_parent
            .take()
            .map(|parent| decode_reference(parent, Storage, quota.work()))
            .transpose()?,
        payload,
    };
    let canonical = encode_event(&authority, &event, quota.work())?;
    let matches = canonical.as_slice() == bytes;
    let capacity = canonical.capacity() as u64;
    drop(canonical);
    if let Some(io) = quota.work().as_deref_mut() {
        io.release_owned(capacity)?;
    }
    if !matches {
        return Err(boundary(
            quota.work(),
            Storage,
            "noncanonical event envelope",
        ));
    }
    drop(envelope);
    drop(scope.agent_id);
    let retained = crate::core::event_owned_bytes(&event)?;
    let total = retained
        .checked_add(authority.id.capacity() as u64)
        .ok_or_else(|| boundary(quota.work(), Storage, "event custody overflow"))?;
    quota.replace(total)?;
    quota.transfer();
    Ok(((authority, event), retained))
}
/// Decodes a bounded payload and returns its retained claim on the supplied observer.
pub fn decode_event_payload(
    tag: &str,
    bytes: &[u8],
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(EventPayload, u64)> {
    use crate::executor::json::{
        JsonCodecCharge, JsonValueSchema, json_decode_owned_bytes, json_from_slice_bounded,
    };
    let code = work
        .as_deref()
        .is_some_and(crate::store::operations::original_native_decode_mode)
        .then_some(Storage);
    let mut quota = CodecQuota::new(work, 0)?;
    let result = json_from_slice_bounded(
        std::marker::PhantomData::<EventPayload>,
        bytes,
        PAYLOAD_BOUNDS,
        0,
        code,
        Some(JsonValueSchema::EventPayload),
        &mut |charge| match charge {
            JsonCodecCharge::Admission { value_shape, .. } => {
                quota.replace(json_decode_owned_bytes(value_shape, false, code)?.0)
            }
            charge => match quota.work().as_deref_mut() {
                Some(io) => io.codec_charge(charge),
                None => Ok(()),
            },
        },
    );
    let payload = result.map_err(|error| match quota.json_error(error) {
        Error::Invalid(reason) => Error::Storage(reason),
        error => error,
    })?;
    if tag != payload.tag() {
        return Err(boundary(
            quota.work(),
            Storage,
            "event type disagrees with payload",
        ));
    }
    let retained = quota.retained();
    quota.transfer();
    Ok((payload, retained))
}
