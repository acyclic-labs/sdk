use super::*;
use prost::Message as _;

pub(crate) fn encode_event(
    authority: &Authority,
    event: &Event,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Vec<u8>> {
    let native = native_event_work(work);
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
    let result = if native {
        envelope.encode_raw(&mut output);
        Ok(())
    } else {
        envelope
            .encode(&mut output)
            .map_err(|error| Error::Storage(error.to_string()))
    };
    if let Some(work) = metadata.work().as_deref_mut() {
        work.encoded_bytes(output.len() as u64)?;
    }
    drop(envelope);
    result?;
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
    let native = native_event_work(work);
    if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(event_boundary(
            native,
            wire::ErrorCode::Storage,
            "event exceeds SDK record boundary",
        ));
    }
    let admitted = admission::decode_quota(bytes, work)?;
    // Carrier fields and buffers must drop before this reservation.
    let mut payload_quota = admission::Quota::new(work, admitted)?;
    let diagnostic = admission::prost_diagnostic_preclaim(payload_quota.work())?;
    let mut envelope =
        wire::EventEnvelope::decode(bytes).map_err(|source| diagnostic.forward(source))?;
    if let Some(work) = payload_quota.work().as_deref_mut() {
        work.hashed_bytes(crate::FILE_DESCRIPTOR_SET.len() as u64)?;
    }
    validate_event_protocol(envelope.protocol.as_ref(), native)?;
    let authority = decode_event_authority(
        envelope.authority.take().ok_or_else(|| {
            event_boundary(
                native,
                wire::ErrorCode::Storage,
                "event authority is missing",
            )
        })?,
        native,
    )?;
    let operation_id = if native {
        let id = uuid::Uuid::parse_str(&envelope.operation_id).map_err(|_| {
            event_boundary(
                native,
                wire::ErrorCode::Invalid,
                "operation UUID is invalid",
            )
        })?;
        OperationId::from_bytes(*id.as_bytes())
    } else {
        OperationId::parse(&envelope.operation_id)?
    };
    let intent_digest: [u8; 32] = envelope.intent_digest.as_slice().try_into().map_err(|_| {
        event_boundary(
            native,
            wire::ErrorCode::Storage,
            "event intent digest must be 32 bytes",
        )
    })?;
    let scope = envelope.scope.take().ok_or_else(|| {
        event_boundary(native, wire::ErrorCode::Storage, "event scope is missing")
    })?;
    let bounds = crate::contract::JsonBounds {
        maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
        maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64 + 1,
        maximum_depth: 256,
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let payload: EventPayload = if native {
        let decoded = crate::contract::json_from_slice_bounded(
            std::marker::PhantomData,
            &envelope.canonical_payload_json,
            bounds,
            0,
            Some(wire::ErrorCode::Storage),
            Some(crate::contract::JsonValueSchema::EventPayload),
            &mut |charge| match charge {
                crate::contract::JsonCodecCharge::Admission { value_shape, .. } => {
                    let witness = crate::contract::json_decode_owned_bytes(
                        value_shape,
                        false,
                        Some(wire::ErrorCode::Storage),
                    )?;
                    let retained = admitted.checked_add(witness.0).ok_or_else(|| {
                        event_boundary(
                            native,
                            wire::ErrorCode::Storage,
                            "event payload custody overflows",
                        )
                    })?;
                    payload_quota.replace(retained)
                }
                charge => payload_quota
                    .work()
                    .as_deref_mut()
                    .expect("native event observer")
                    .codec_charge(charge),
            },
        );
        decoded.map_err(|mut error| {
            if let Error::NativeJson { custody, .. } = &mut error
                && custody.is_none()
            {
                // Only split the already admitted original claim: a new quota check must never replace the actual foreign Serde cause.
                let retained = payload_quota.retained();
                *custody = Some(
                    payload_quota
                        .work()
                        .as_deref_mut()
                        .expect("native event observer")
                        .take_native_custody(retained)
                        .expect("preclaimed event decoder custody"),
                );
                payload_quota.transfer();
            }
            error
        })?
    } else {
        decode_portable_event_payload(
            &envelope.canonical_payload_json,
            bounds,
            admitted,
            &mut payload_quota,
        )?
    };
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let payload: EventPayload = decode_portable_event_payload(
        &envelope.canonical_payload_json,
        bounds,
        admitted,
        &mut payload_quota,
    )?;
    if envelope.event_type != payload.tag() {
        return Err(event_boundary(
            native,
            wire::ErrorCode::Storage,
            "event type disagrees with its payload",
        ));
    }
    let attestation = envelope.attestation.as_slice().try_into().map_err(|_| {
        event_boundary(
            native,
            wire::ErrorCode::Storage,
            "event attestation must be 32 bytes",
        )
    })?;
    let scope_agent = if native && !scope.agent_id.is_empty() {
        let id = uuid::Uuid::parse_str(&scope.agent_id).map_err(|_| {
            event_boundary(
                native,
                wire::ErrorCode::Invalid,
                "scope agent UUID is invalid",
            )
        })?;
        Some(AgentId::from_bytes(*id.as_bytes()))
    } else {
        parse_scope_agent(&scope.agent_id)?
    };
    let causal_parent = envelope
        .causal_parent
        .take()
        .map(|reference| {
            if !native {
                return decode_reference(reference, Error::Storage);
            }
            Ok(EventReference {
                authority: decode_event_authority(
                    reference.authority.ok_or_else(|| {
                        event_boundary(
                            native,
                            wire::ErrorCode::Storage,
                            "causal authority is missing",
                        )
                    })?,
                    native,
                )?,
                revision: reference.revision,
            })
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
        return Err(event_boundary(
            native,
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
            event_boundary(native, wire::ErrorCode::Storage, "event custody overflows")
        })?;
    payload_quota.replace(total)?;
    payload_quota.transfer();
    Ok(((authority, event), retained))
}

fn decode_portable_event_payload(
    bytes: &[u8],
    bounds: crate::contract::JsonBounds,
    carrier: u64,
    quota: &mut admission::Quota<'_, '_>,
) -> Result<EventPayload> {
    crate::contract::event_payload_json_admission(bytes, bounds, &mut |charge| match charge {
        crate::contract::JsonCodecCharge::Admission {
            maximum_owned_bytes,
            ..
        } => {
            let retained = carrier
                .checked_add(maximum_owned_bytes)
                .ok_or_else(|| Error::Storage("event payload custody overflows".into()))?;
            quota.replace(retained)
        }
        charge => match quota.work().as_deref_mut() {
            Some(work) => work.codec_charge(charge),
            None => Ok(()),
        },
    })?;
    crate::contract::json_from_slice(bytes).map_err(|error| Error::Storage(error.to_string()))
}

fn native_event_work(work: &Option<&mut dyn crate::store::history::HistoryIo>) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        work.as_deref()
            .is_some_and(|work| work.original_native_work().is_some())
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        let _ = work;
        false
    }
}

fn event_boundary(native: bool, code: wire::ErrorCode, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if native {
        return Error::NativeBoundary { code, reason };
    }
    let _ = native;
    match code {
        wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        _ => Error::Storage(reason.into()),
    }
}

fn validate_event_protocol(protocol: Option<&wire::ProtocolIdentity>, native: bool) -> Result<()> {
    if !native {
        return validate_protocol(protocol, &protocol_identity(), Error::Storage);
    }
    let actual = protocol.ok_or_else(|| {
        event_boundary(
            native,
            wire::ErrorCode::Storage,
            "protocol identity is missing",
        )
    })?;
    let digest = blake3::hash(crate::FILE_DESCRIPTOR_SET).to_hex();
    if actual.version != EVENT_WIRE_VERSION || actual.descriptor_digest != digest.as_str() {
        return Err(event_boundary(
            native,
            wire::ErrorCode::Unsupported,
            "protocol identity mismatch",
        ));
    }
    Ok(())
}

fn decode_event_authority(authority: wire::Authority, native: bool) -> Result<Authority> {
    if !native {
        return decode_authority(authority, Error::Storage);
    }
    let kind = match wire::AggregateKind::try_from(authority.kind) {
        Ok(wire::AggregateKind::Agent) => AggregateKind::Agent,
        Ok(wire::AggregateKind::Conversation) => AggregateKind::Conversation,
        Ok(wire::AggregateKind::Session) => AggregateKind::Session,
        Ok(wire::AggregateKind::Turn) => AggregateKind::Turn,
        Ok(wire::AggregateKind::Task) => AggregateKind::Task,
        Ok(wire::AggregateKind::Unspecified) => {
            return Err(event_boundary(
                native,
                wire::ErrorCode::Storage,
                "aggregate kind is unspecified",
            ));
        }
        Err(_) => {
            return Err(event_boundary(
                native,
                wire::ErrorCode::Storage,
                "aggregate kind is invalid",
            ));
        }
    };
    if authority.id.is_empty() {
        return Err(event_boundary(
            native,
            wire::ErrorCode::Storage,
            "authority identity is empty",
        ));
    }
    Ok(Authority {
        kind,
        id: authority.id,
    })
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
