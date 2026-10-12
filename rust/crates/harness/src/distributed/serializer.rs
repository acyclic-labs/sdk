use super::*;
use prost::Message as _;

pub(super) fn validate_idempotency_key(
    value: &str,
    work: &Option<&mut dyn HistoryIo>,
) -> Result<()> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(coordinator_boundary(
            work,
            wire::ErrorCode::Invalid,
            "idempotency key is invalid",
        ));
    }
    Ok(())
}

pub(super) fn formatted_text(
    length: usize,
    arguments: std::fmt::Arguments<'_>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<String> {
    use std::fmt::Write as _;
    let mut custody = CodecQuota::new(work, 0)?;
    let mut text = String::new();
    if let Some(work) = custody.work().as_deref_mut() {
        work.reserve_text(&mut text, length)?;
        custody.adopt(text.capacity() as u64)?;
        custody
            .work()
            .as_deref_mut()
            .expect("format observer")
            .copy_bytes(length as u64)?;
    } else {
        text.reserve_exact(length);
    }
    text.write_fmt(arguments)
        .expect("primitive coordinator formatting");
    custody.transfer();
    Ok(text)
}

pub(super) fn encode(
    revision: u64,
    operation_id: OperationId,
    key: &str,
    digest: [u8; 32],
    canonical: Vec<u8>,
    committed_at_ms: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Vec<u8>> {
    let retained = if work.is_some() {
        canonical.capacity() as u64
    } else {
        0
    };
    let mut quota = CodecQuota::from_retained(work, retained);
    let protocol = wire::ProtocolIdentity {
        version: crate::core::copy_text(COORDINATOR_WIRE_VERSION, &mut quota)?,
        descriptor_digest: crate::core::copy_text(COORDINATOR_WIRE_DIGEST, &mut quota)?,
    };
    let operation_id = formatted_text(36, format_args!("{operation_id}"), quota.work())?;
    if quota.work().is_some() {
        quota.adopt(operation_id.capacity() as u64)?;
    }
    let envelope = wire::SchedulerEventEnvelope {
        protocol: Some(protocol),
        revision,
        operation_id,
        idempotency_key: crate::core::copy_text(key, &mut quota)?,
        canonical_event_json: canonical,
        event_digest: crate::core::copy_copy_vector(&digest, &mut quota)?,
        committed_at_ms: Some(committed_at_ms),
    };
    let mut output = Vec::new();
    let length = envelope.encoded_len();
    if let Some(io) = quota.work().as_deref_mut() {
        io.admit_encoding(length as u64)?;
        crate::store::history::reserve(&mut output, length, io)?;
        quota.adopt(output.capacity() as u64)?;
    } else {
        output.reserve_exact(length);
    }
    envelope.encode_raw(&mut output);
    if let Some(io) = quota.work().as_deref_mut() {
        io.encoded_bytes(output.len() as u64)?;
    }
    drop(envelope);
    let retained = if quota.work().is_some() {
        output.capacity() as u64
    } else {
        0
    };
    quota.replace(retained)?;
    quota.transfer();
    Ok(output)
}

fn scheduler_json_owned_bytes(input_bytes: u64, items: u64) -> Result<u64> {
    use serde::__private229::de::Content;
    // Scheduler frames and item counts are bounded by MAX_RECORD_BYTES.
    const CONTENT_LAYERS: u64 = 3;
    let slots = items * (2 + 8);
    let content_bytes =
        slots * std::mem::size_of::<(Content<'static>, Content<'static>)>() as u64 * CONTENT_LAYERS;
    let typed_bytes = slots * std::mem::size_of::<(String, serde_json::Value)>() as u64;
    let strings = input_bytes * (CONTENT_LAYERS + 2);
    let scratch = input_bytes.max(8) * 2;
    const LABEL_BYTES: u64 = 9955;
    let diagnostics = crate::executor::json::audited_serde_diagnostic_bytes(input_bytes, LABEL_BYTES)?;
    Ok(content_bytes
        + typed_bytes * 2
        + strings
        + scratch
        + diagnostics
        + std::mem::size_of::<OperationSpec>() as u64)
}

pub(super) fn decode_json<T: serde::de::DeserializeOwned>(
    input: &[u8],
    schema: crate::executor::json::JsonValueSchema,
    owned: fn(&T) -> Result<u64>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(T, u64)> {
    use crate::executor::json::{JsonCodecCharge, JsonValueSchema};
    let result_value = matches!(schema, JsonValueSchema::Root);
    let code = if result_value {
        wire::ErrorCode::Invalid
    } else {
        wire::ErrorCode::Storage
    };
    let invalid = |message: String| {
        if result_value {
            Error::Invalid(format!("scheduler result is invalid JSON: {message}"))
        } else {
            Error::Storage(message)
        }
    };
    if work.is_none() {
        return crate::contract::json_from_slice(input)
            .map(|value| (value, 0))
            .map_err(|source| invalid(source.to_string()));
    }
    let native = work
        .as_deref()
        .is_some_and(operations::original_native_decode_mode);
    let diagnostic = if result_value {
        crate::executor::json::audited_serde_diagnostic_bytes(
            input.len() as u64,
            "Value Number map sequence string number boolean null invalid type invalid value".len()
                as u64,
        )?
    } else {
        0
    };
    let bound = if result_value {
        input.len() as u64
    } else {
        acyclic_stream::MAX_RECORD_BYTES as u64
    };
    let mut reservation = 0;
    let mut observed_failure = false;
    let observer = work.as_deref_mut().expect("JSON observer");
    observer.check_cancellation()?;
    let decoded = operations::decode_preclaimed_json_with_mode::<T, _>(
        input,
        crate::executor::json::JsonBounds {
            maximum_bytes: bound,
            maximum_items: bound + 1,
            maximum_depth: 256,
        },
        0,
        code,
        native,
        schema,
        &mut |charge| {
            let charged = (|| match charge {
                JsonCodecCharge::Admission { value_shape, .. } => {
                    let planned = if result_value {
                        crate::executor::json::json_value_decode_owned_bytes(value_shape)?
                            .checked_add(diagnostic)
                    } else {
                        scheduler_json_owned_bytes(
                            value_shape.input_bytes(),
                            value_shape.lexical_items(),
                        )?
                        .checked_add(
                            crate::executor::json::json_value_virtual_owned_bytes(value_shape)?,
                        )
                    }
                    .ok_or_else(|| {
                        coordinator_boundary(
                            &Some(&mut *observer),
                            wire::ErrorCode::Invalid,
                            "JSON ownership admission overflows",
                        )
                    })?;
                    observer.codec_charge(JsonCodecCharge::Admission {
                        maximum_owned_bytes: planned,
                        value_shape,
                    })?;
                    reservation = planned;
                    Ok(())
                }
                charge => observer.codec_charge(charge),
            })();
            observed_failure |= charged.is_err();
            charged
        },
    );
    let decoded = operations::retain_original_decode_error(decoded, &mut reservation, observer);
    let mut custody = CodecQuota::from_retained(work, reservation);
    let value = decoded.map_err(|error| match error {
        Error::Invalid(message) if !observed_failure => invalid(message),
        error => error,
    })?;
    custody
        .work()
        .as_deref_mut()
        .expect("JSON observer")
        .check_cancellation()?;
    let retained = owned(&value)?;
    let slack = reservation.checked_sub(retained).ok_or_else(|| {
        coordinator_boundary(custody.work(), code, "JSON decoder exceeds its admission")
    })?;
    custody.relinquish(slack)?;
    custody.transfer();
    Ok((value, retained))
}

pub(super) fn inspect_envelope<'a>(
    mut input: &'a [u8],
    protocol: bool,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(u64, u64, Option<&'a str>)> {
    use prost::encoding::{DecodeContext, WireType, decode_key, skip_field};
    let (mut payload, mut buffers, mut key) = (0, 0, None);
    while !input.is_empty() {
        if let Some(io) = work.as_deref_mut() {
            io.codec_charge(crate::executor::json::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        }
        let (tag, kind) = decode_key(&mut input).map_err(|_| {
            coordinator_boundary(
                work,
                wire::ErrorCode::Storage,
                "invalid scheduler protobuf field",
            )
        })?;
        let framed = input;
        skip_field(kind, tag, &mut input, DecodeContext::default()).map_err(|_| {
            coordinator_boundary(
                work,
                wire::ErrorCode::Storage,
                "invalid scheduler protobuf framing",
            )
        })?;
        let nested = !protocol && tag == 1;
        let owned = if protocol {
            matches!(tag, 1 | 2)
        } else {
            matches!(tag, 3 | 4 | 5 | 6)
        };
        if kind != WireType::LengthDelimited || !(nested || owned) {
            continue;
        }
        let header = framed
            .iter()
            .position(|byte| byte & 0x80 == 0)
            .expect("prost accepted the length varint")
            + 1;
        let length = framed.len() - input.len() - header;
        let value = &framed[header..header + length];
        if !protocol && tag == 4 {
            key = Some(std::str::from_utf8(value).map_err(|_| {
                coordinator_boundary(
                    work,
                    wire::ErrorCode::Storage,
                    "scheduler retry key is not UTF-8",
                )
            })?);
        }
        let (bytes, count, _) = if nested {
            inspect_envelope(value, true, work)?
        } else {
            (length as u64, 1, None)
        };
        // Disjoint fields contribute no more than their encoded frame.
        payload += bytes;
        buffers += count;
    }
    Ok((payload, buffers, key))
}

pub(super) fn decode(
    record: &acyclic_stream::Record,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(u64, String, [u8; 32], u64, SchedulerEvent)> {
    let bytes = record.value.as_ref();
    let diagnostic = if work.is_some() {
        crate::executor::json::audited_serde_diagnostic_bytes(bytes.len() as u64, 335)?
    } else {
        0
    };
    let mut quota = CodecQuota::new(work, diagnostic)?;
    if quota.work().is_some() {
        let (payload, buffers, _) = inspect_envelope(bytes, false, quota.work())?;
        quota.replace(quota.retained() + payload * 2 + buffers * 8)?;
    }
    let mut envelope = wire::SchedulerEventEnvelope::decode(bytes).map_err(|_| {
        coordinator_boundary(
            quota.work(),
            wire::ErrorCode::Storage,
            "invalid scheduler protobuf envelope",
        )
    })?;
    let fields = if quota.work().is_some() {
        (envelope.canonical_event_json.capacity()
            + envelope.event_digest.capacity()
            + envelope.operation_id.capacity()
            + envelope.idempotency_key.capacity()
            + envelope
                .protocol
                .as_ref()
                .map_or(0, |p| p.version.capacity() + p.descriptor_digest.capacity()))
            as u64
    } else {
        0
    };
    if fields > quota.retained() {
        return Err(coordinator_boundary(
            quota.work(),
            wire::ErrorCode::Storage,
            "scheduler protobuf exceeds its concrete admission",
        ));
    }
    quota.replace(fields)?;
    if envelope.protocol.as_ref().is_none_or(|p| {
        p.version != COORDINATOR_WIRE_VERSION || p.descriptor_digest != COORDINATOR_WIRE_DIGEST
    }) {
        return Err(coordinator_boundary(
            quota.work(),
            wire::ErrorCode::Storage,
            "scheduler protocol identity differs",
        ));
    }
    let digest: [u8; 32] = envelope.event_digest.as_slice().try_into().map_err(|_| {
        coordinator_boundary(
            quota.work(),
            wire::ErrorCode::Storage,
            "scheduler digest must be 32 bytes",
        )
    })?;
    if let Some(io) = quota.work().as_deref_mut() {
        io.hashed_bytes(envelope.canonical_event_json.len() as u64)?;
    }
    if *blake3::hash(&envelope.canonical_event_json).as_bytes() != digest {
        return Err(coordinator_boundary(
            quota.work(),
            wire::ErrorCode::Storage,
            "scheduler event digest mismatch",
        ));
    }
    let (event, retained) = decode_json(
        &envelope.canonical_event_json,
        crate::executor::json::JsonValueSchema::SchedulerEvent,
        SchedulerEvent::logical_owned_bytes,
        quota.work(),
    )?;
    quota.adopt(retained)?;
    let committed_at_ms = envelope
        .committed_at_ms
        .filter(|time| *time > 0)
        .ok_or_else(|| {
            coordinator_boundary(
                quota.work(),
                wire::ErrorCode::Storage,
                "coordinator event lacks commit time",
            )
        })?;
    let operation = uuid::Uuid::parse_str(&envelope.operation_id).map_err(|_| {
        coordinator_boundary(
            quota.work(),
            wire::ErrorCode::Invalid,
            "scheduler operation id is not a UUID",
        )
    })?;
    if envelope.revision != next_revision(record.sequence)?
        || event.operation_id() != OperationId::from_bytes(*operation.as_bytes())
    {
        return Err(coordinator_boundary(
            quota.work(),
            wire::ErrorCode::Storage,
            "coordinator record identity differs",
        ));
    }
    let revision = envelope.revision;
    let key = std::mem::take(&mut envelope.idempotency_key);
    let key_bytes = if quota.work().is_some() {
        key.capacity() as u64
    } else {
        0
    };
    drop(envelope);
    quota.relinquish(fields - key_bytes)?;
    quota.transfer();
    Ok((revision, key, digest, committed_at_ms, event))
}

pub(super) fn stream_path(
    value: &str,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<acyclic_stream::StreamPath> {
    if let Some(work) = work.as_deref_mut() {
        work.admit_allocation(value.len() as u64)?;
        work.copy_bytes(value.len() as u64)?;
    }
    let path = acyclic_stream::StreamPath::new(value)
        .map_err(|error| operations::stream_error_with_work(error, work))?;
    if let Some(work) = work.as_deref_mut() {
        work.retain_owned(value.len() as u64)?;
    }
    Ok(path)
}

pub(super) fn stream_key(
    length: usize,
    key: std::fmt::Arguments<'_>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(StreamIdempotencyKey, u64)> {
    use std::io::Write as _;
    if let Some(work) = work.as_deref_mut() {
        work.hashed_bytes((b"acyclic-harness-coordinator-v3".len() + length) as u64)?;
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-harness-coordinator-v3");
    hasher
        .write_fmt(key)
        .expect("primitive formatting into an infallible hash writer");
    digest_key(hasher.finalize().as_bytes(), work)
}

pub(crate) fn digest_key(
    digest: &[u8; 32],
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(StreamIdempotencyKey, u64)> {
    let mut custody = CodecQuota::new(work, 0)?;
    let bytes = crate::core::copy_copy_vector(digest, &mut custody)?;
    custody.transfer();
    let (bytes, retained) = operations::owned_bytes_from_retained_vec(bytes, custody.work())?;
    Ok((
        StreamIdempotencyKey::new(bytes).expect("32-byte digest is a bounded SDK key"),
        retained,
    ))
}

pub(crate) fn execution_path(
    operation: OperationId,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<acyclic_stream::StreamPath> {
    const PREFIX: &[u8] = b"harness/v2/execution/";
    let mut text = [0; PREFIX.len() + 36];
    if let Some(work) = work.as_deref_mut() {
        work.copy_bytes(text.len() as u64)?;
    }
    text[..PREFIX.len()].copy_from_slice(PREFIX);
    uuid::Uuid::from_bytes(operation.into_bytes())
        .hyphenated()
        .encode_lower(&mut text[PREFIX.len()..]);
    stream_path(
        std::str::from_utf8(&text).expect("ASCII execution path"),
        work,
    )
}
