use super::*;
use prost::Message as _;

pub(crate) fn encode_event(
    authority: &Authority,
    event: &Event,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Vec<u8>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let native = work
        .as_deref()
        .is_some_and(|io| io.original_native_work().is_some());
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let native = false;
    let mut metadata =
        admission::Quota::new(work, admission::encode_metadata_quota(authority, event)?)?;
    let encoded = crate::store::history::canonical_bytes_with_work(
        &event.payload,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        metadata.work(),
    );
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let encoded = encoded.map_err(|mut error| {
        if let Error::NativeJson { custody, .. } = &mut error
            && custody.is_none()
            && native
        {
            let retained = metadata.retained();
            *custody = Some(
                metadata
                    .work()
                    .as_deref_mut()
                    .expect("native event observer")
                    .take_native_custody(retained)
                    .expect("preclaimed event encoder diagnostic custody"),
            );
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
    if let Some(work) = metadata.work().as_deref_mut() {
        work.admit_copy(copied)?;
    }
    let envelope = event_envelope(authority, event, payload);
    if let Some(work) = metadata.work().as_deref_mut() {
        work.copy_bytes(copied)?;
    }
    metadata.replace(admission::envelope_owned(&envelope))?;
    let length = envelope.encoded_len();
    let mut output = Vec::new();
    if let Some(work) = metadata.work().as_deref_mut() {
        work.admit_encoding(length as u64)?;
        crate::store::history::reserve(&mut output, length, work)?;
    } else {
        output.reserve_exact(length);
    }
    metadata.adopt(output.capacity() as u64)?;
    envelope.encode_raw(&mut output);
    if let Some(work) = metadata.work().as_deref_mut() {
        work.encoded_bytes(output.len() as u64)?;
    }
    drop(envelope);
    metadata.replace(output.capacity() as u64)?;
    metadata.transfer();
    Ok(output)
}

/// Constructs the exact canonical event envelope without encoding it twice.
pub fn encode_event_envelope(authority: &Authority, event: &Event) -> Result<wire::EventEnvelope> {
    Ok(event_envelope(
        authority,
        event,
        canonical_json_bytes(&event.payload)?,
    ))
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
    bytes: &[u8],
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<((Authority, Event), u64)> {
    if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(boundary(
            work,
            wire::ErrorCode::Storage,
            "event exceeds SDK record boundary",
        ));
    }
    let admitted = admission::decode_quota(bytes, work)?;
    // Carrier fields and buffers must drop before this reservation.
    let mut payload_quota = admission::Quota::new(work, admitted)?;
    let diagnostic = admission::prost_diagnostic_preclaim(payload_quota.work())?;
    let mut envelope = wire::EventEnvelope::decode(bytes)
        .map_err(|source| admission::prost_error(diagnostic, source))?;
    if let Some(work) = payload_quota.work().as_deref_mut() {
        work.hashed_bytes(crate::FILE_DESCRIPTOR_SET.len() as u64)?;
    }
    validate_protocol(
        envelope.protocol.as_ref(),
        (
            EVENT_WIRE_VERSION,
            blake3::hash(crate::FILE_DESCRIPTOR_SET).to_hex().as_str(),
        ),
        wire::ErrorCode::Storage,
        payload_quota.work(),
    )?;
    let authority = decode_authority(
        envelope.authority.take().ok_or_else(|| {
            boundary(
                payload_quota.work(),
                wire::ErrorCode::Storage,
                "event authority is missing",
            )
        })?,
        wire::ErrorCode::Storage,
        payload_quota.work(),
    )?;
    let operation_id = OperationId::from_bytes(
        *uuid::Uuid::parse_str(&envelope.operation_id)
            .map_err(|_| {
                boundary(
                    payload_quota.work(),
                    wire::ErrorCode::Invalid,
                    "operation UUID is invalid",
                )
            })?
            .as_bytes(),
    );
    let intent_digest: [u8; 32] = envelope.intent_digest.as_slice().try_into().map_err(|_| {
        boundary(
            payload_quota.work(),
            wire::ErrorCode::Storage,
            "event intent digest must be 32 bytes",
        )
    })?;
    let scope = envelope.scope.take().ok_or_else(|| {
        boundary(
            payload_quota.work(),
            wire::ErrorCode::Storage,
            "event scope is missing",
        )
    })?;
    let (payload, claim) = decode_event_payload(
        &envelope.event_type,
        &envelope.canonical_payload_json,
        payload_quota.work(),
    )?;
    payload_quota.adopt(claim)?;
    let attestation = envelope.attestation.as_slice().try_into().map_err(|_| {
        boundary(
            payload_quota.work(),
            wire::ErrorCode::Storage,
            "event attestation must be 32 bytes",
        )
    })?;
    let scope_agent = if scope.agent_id.is_empty() {
        None
    } else {
        Some(AgentId::from_bytes(
            *uuid::Uuid::parse_str(&scope.agent_id)
                .map_err(|_| {
                    boundary(
                        payload_quota.work(),
                        wire::ErrorCode::Invalid,
                        "scope agent UUID is invalid",
                    )
                })?
                .as_bytes(),
        ))
    };
    let causal_parent = envelope
        .causal_parent
        .take()
        .map(|reference| {
            decode_reference(reference, wire::ErrorCode::Storage, payload_quota.work())
        })
        .transpose()?;
    let event = Event {
        revision: envelope.revision,
        operation_id,
        intent_digest,
        scope: RecordedScope::from_wire(
            scope.id,
            Capabilities::new(scope.capabilities),
            scope.issuer,
            scope_agent,
        ),
        attestation,
        causal_parent,
        payload,
    };
    let canonical = encode_event(&authority, &event, payload_quota.work())?;
    let matches = canonical.as_slice() == bytes;
    let capacity = canonical.capacity() as u64;
    drop(canonical);
    if let Some(work) = payload_quota.work().as_deref_mut() {
        work.release_owned(capacity)?;
    }
    if !matches {
        return Err(boundary(
            payload_quota.work(),
            wire::ErrorCode::Storage,
            "event envelope is not canonical v2 encoding",
        ));
    }
    // Retire all carrier-only fields before shrinking the original reservation.
    drop(envelope);
    drop(scope.agent_id);
    let retained = crate::core::event_owned_bytes(&event)?;
    let total = retained
        .checked_add(authority.id.capacity() as u64)
        .ok_or_else(|| {
            boundary(
                payload_quota.work(),
                wire::ErrorCode::Storage,
                "event custody overflows",
            )
        })?;
    payload_quota.replace(total)?;
    payload_quota.transfer();
    Ok(((authority, event), retained))
}

/// Decodes a bounded typed payload and returns its retained claim on the supplied observer.
pub fn decode_event_payload(
    event_type_name: &str,
    bytes: &[u8],
    work: &mut Option<&mut dyn crate::store::HistoryIo>,
) -> Result<(EventPayload, u64)> {
    use crate::executor::json::{
        JsonBounds, JsonCodecCharge, JsonValueSchema, json_decode_owned_bytes,
        json_from_slice_bounded,
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let code = work
        .as_deref()
        .and_then(|io| io.original_native_work())
        .map(|_| wire::ErrorCode::Storage);
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let code = None;
    let mut quota = admission::Quota::new(work, 0)?;
    let decoded = json_from_slice_bounded(
        std::marker::PhantomData::<EventPayload>,
        bytes,
        JsonBounds {
            maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
            maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64 + 1,
            maximum_depth: 256,
        },
        0,
        code,
        Some(JsonValueSchema::EventPayload),
        &mut |charge| match charge {
            JsonCodecCharge::Admission { value_shape, .. } => {
                quota.replace(json_decode_owned_bytes(value_shape, false, code)?.0)
            }
            charge => match quota.work().as_deref_mut() {
                Some(work) => work.codec_charge(charge),
                None => Ok(()),
            },
        },
    );
    let payload = decoded.map_err(|mut error| {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Error::NativeJson { custody, .. } = &mut error
            && custody.is_none()
        {
            let retained = quota.retained();
            *custody = Some(
                quota
                    .work()
                    .as_deref_mut()
                    .expect("native payload observer")
                    .take_native_custody(retained)
                    .expect("preclaimed payload diagnostic custody"),
            );
            quota.transfer();
        }
        match error {
            Error::Invalid(reason) => Error::Storage(reason),
            error => error,
        }
    })?;
    if event_type_name != payload.tag() {
        return Err(boundary(
            quota.work(),
            wire::ErrorCode::Storage,
            "event type disagrees with its payload",
        ));
    }
    let retained = quota.retained();
    quota.transfer();
    Ok((payload, retained))
}
