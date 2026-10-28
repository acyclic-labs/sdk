use super::*;

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
/// Decodes a bounded payload and returns its retained claim on the supplied observer.
pub fn decode_event_payload(
    tag: &str,
    bytes: &[u8],
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(EventPayload, u64)> {
    use crate::executor::json::{
        JsonCodecCharge, JsonValueSchema, history_diagnostic_bytes, json_from_slice_bounded,
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
        code,
        Some(JsonValueSchema::EventPayload),
        &mut |charge| match charge {
            JsonCodecCharge::Admission {
                maximum_owned_bytes,
                value_shape,
            } => {
                let reserved = maximum_owned_bytes
                    .checked_add(history_diagnostic_bytes(value_shape, code)?)
                    .ok_or_else(|| boundary(quota.work(), Storage, "event custody overflow"))?;
                quota.replace(reserved)
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
