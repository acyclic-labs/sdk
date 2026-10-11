//! Derived exact-operation locations, committed with their canonical event.

use crate::{
    Error, OperationId, Result,
    core::{Authority, AuthorityVerifier, Event},
};
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitRequest, CommittedEnvelope, CommittedMutation,
    IdempotencyKey, Record, StreamClient, StreamPath, StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};

pub(super) fn original_native_decode_mode(observer: &dyn super::history::HistoryIo) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    { observer.original_native_work().is_some() }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { let _ = observer; false }
}

pub(super) fn decode_preclaimed_json_with_mode<T, F>(
    bytes: &[u8],
    bounds: crate::contract::JsonBounds,
    maximum_owned_bytes: u64,
    code: crate::wire::ErrorCode,
    native: bool,
    charge: &mut F,
) -> Result<T>
where
    T: serde::de::DeserializeOwned,
    F: FnMut(crate::contract::JsonCodecCharge) -> Result<()>,
{
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if native {
        return crate::contract::json_from_slice_bounded_native_trusted(
            bytes, bounds, maximum_owned_bytes, code, charge);
    }
    let _ = (native, code);
    crate::contract::json_from_slice_bounded_trusted(bytes, bounds, maximum_owned_bytes, charge)
}

pub(super) fn retain_original_decode_error<T>(
    mut result: Result<T>,
    reservation: &mut u64,
    observer: &mut dyn super::history::HistoryIo,
) -> Result<T> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Err(Error::NativeDecode { custody, .. }) = &mut result
        && custody.is_none()
    {
        // The original Admission callback retained this exact reservation.
        // Original-native adapters split it without cancellation/admission;
        // another fallible claim must not replace the original serde cause.
        *custody = Some(observer.take_native_custody(*reservation)
            .expect("original native decoder reservation has source custody"));
        *reservation = 0;
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { let _ = (&mut result, reservation, observer); }
    result
}
pub(super) fn validate_canonical_json_with_work(
    bytes: &[u8],
    maximum_bytes: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<()> {
    if work.is_none() {
        let _: serde_json::Value = crate::executor::decode_json(bytes)?;
        return Ok(());
    }
    let mut items = 0u64;
    let mut reservation = 0;
    let diagnostic_bytes = crate::contract::audited_serde_diagnostic_bytes(
        bytes.len() as u64,
        ["Value", "sequence", "map", "string", "number", "boolean", "null"]
            .iter().map(|label| label.len() as u64 + 4).sum(),
    )?;
    let mut observed_failure = false;
    let parsed = {
        let observer = work.as_deref_mut().expect("observed canonical validation");
        let native = original_native_decode_mode(observer);
        decode_preclaimed_json_with_mode::<serde_json::Value, _>(
            bytes,
            crate::contract::JsonBounds {
                maximum_bytes,
                maximum_items: maximum_bytes,
                maximum_depth: 256,
            },
            bytes.len() as u64,
            crate::wire::ErrorCode::Storage,
            native,
            &mut |charge| {
                let charged = match charge {
                crate::contract::JsonCodecCharge::Work { items: visited, .. } => {
                    items = items.checked_add(visited)
                        .ok_or_else(|| Error::Invalid("index JSON item count overflows".into()))?;
                    observer.codec_charge(charge)
                }
                crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes } => {
                    // Value owns string payloads and logical sequence/map slots.
                    // The lexical count admits minimum-eight/geometric vectors;
                    // opaque map allocator overhead is outside this contract.
                    let slot = std::mem::size_of::<serde_json::Value>()
                        .max(std::mem::size_of::<(String, serde_json::Value)>()) as u64;
                    let planned = items.checked_mul(10).and_then(|count| count.checked_mul(slot))
                        .and_then(|slots| slots.checked_add(maximum_owned_bytes))
                        .and_then(|owned| owned.checked_add(diagnostic_bytes))
                        .ok_or_else(|| Error::Invalid("index JSON reservation overflows".into()))?;
                    observer.retain_owned(planned)?;
                    reservation = planned;
                    Ok(())
                }
                _ => observer.codec_charge(charge),
                };
                if charged.is_err() { observed_failure = true; }
                charged
            },
        )
    };
    let parsed = retain_original_decode_error(
        parsed, &mut reservation, work.as_deref_mut().expect("observed canonical validation"));
    let parsed = parsed.map_err(|error| {
        if observed_failure { error } else {
            match error {
                Error::Invalid(message) => Error::Storage(format!("execution journal JSON is invalid: {message}")),
                other => other,
            }
        }
    });
    let result = match parsed {
        Ok(parsed) => {
            let encoded = super::history::canonical_bytes_with_work(&parsed, maximum_bytes, work);
            drop(parsed);
            match encoded {
                Ok(encoded) => {
                    let identical = encoded == bytes;
                    let capacity = encoded.capacity() as u64;
                    drop(encoded);
                    if let Some(observer) = work.as_deref_mut() {
                        observer.release_owned(capacity)?;
                    }
                    if identical { Ok(()) } else {
                        Err(Error::Storage("execution journal JSON is not canonical".into()))
                    }
                }
                Err(error) => Err(error),
            }
        }
        Err(error) => Err(error),
    };
    release_owned_after_drop(result, reservation, work)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OperationLocation {
    pub(super) authority: Authority,
    pub(super) operation_id: OperationId,
    pub(super) revision: u64,
    pub(super) intent_digest: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) effect_position: Option<u64>,
}

pub(super) fn decode_location_with_work(
    bytes: &[u8],
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<OperationLocation> {
    if work.is_some() {
        validate_canonical_json_with_work(bytes, acyclic_stream::MAX_RECORD_BYTES as u64, work)?;
    }
    let Some(observer) = work.as_deref_mut() else {
        return crate::executor::decode_json(bytes);
    };
    // This named-field schema owns only Authority.id; every other field is
    // fixed-width. Decoded UTF-8 string storage cannot exceed lexical input.
    let mut reservation = 0;
    let diagnostic_bytes = crate::contract::audited_serde_diagnostic_bytes(
        bytes.len() as u64,
        ["OperationLocation", "Authority", "AggregateKind", "authority", "operation_id",
         "revision", "intent_digest", "effect_position", "kind", "id", "agent",
         "conversation", "session", "turn", "task"]
            .iter().map(|label| label.len() as u64 + 4).sum(),
    )?;
    let mut observed_failure = false;
    let native = original_native_decode_mode(observer);
    let decoded = decode_preclaimed_json_with_mode::<OperationLocation, _>(
        bytes,
        crate::contract::JsonBounds {
            maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
            maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64,
            maximum_depth: 256,
        },
        bytes.len() as u64,
        crate::wire::ErrorCode::Storage,
        native,
        &mut |charge| {
            let charged = if let crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes } = charge {
                let planned = maximum_owned_bytes.checked_add(diagnostic_bytes)
                    .ok_or_else(|| Error::Invalid("operation locator reservation overflows".into()))?;
                observer.retain_owned(planned)?;
                reservation = planned;
                Ok(())
            } else {
                observer.codec_charge(charge)
            };
            if charged.is_err() { observed_failure = true; }
            charged
        },
    );
    let decoded = retain_original_decode_error(decoded, &mut reservation, observer);
    let decoded = decoded.map_err(|error| {
        if observed_failure { error } else {
            match error {
                Error::Invalid(message) => Error::Storage(format!("execution journal content is invalid: {message}")),
                other => other,
            }
        }
    });
    let location = match decoded {
        Ok(location) => location,
        Err(error) => {
            let _ = observer.release_owned(reservation);
            return Err(error);
        }
    };
    let owned = location.authority.id.capacity() as u64;
    let retained = observer.release_owned(reservation - owned)
        .and_then(|()| observer.copy_bytes(location.authority.id.len() as u64));
    match retained {
        Ok(()) => Ok(location),
        Err(error) => {
            drop(location);
            let _ = observer.release_owned(owned);
            Err(error)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MessageLocation {
    pub(super) location: OperationLocation,
    pub(super) message_id: uuid::Uuid,
    pub(super) sequence: u64,
}

pub(super) fn decode_message_location_with_work(
    bytes: &[u8],
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<MessageLocation> {
    if work.is_some() {
        validate_canonical_json_with_work(bytes, acyclic_stream::MAX_RECORD_BYTES as u64, work)?;
    }
    let Some(observer) = work.as_deref_mut() else { return crate::executor::decode_json(bytes); };
    let diagnostic_bytes = crate::contract::audited_serde_diagnostic_bytes(
        bytes.len() as u64,
        ["MessageLocation", "OperationLocation", "Authority", "AggregateKind", "location",
         "message_id", "sequence", "authority", "operation_id", "revision", "intent_digest",
         "effect_position", "kind", "id", "agent", "conversation", "session", "turn", "task"]
            .iter().map(|label| label.len() as u64 + 4).sum(),
    )?;
    let mut reservation = 0;
    let mut observed_failure = false;
    let native = original_native_decode_mode(observer);
    let decoded = decode_preclaimed_json_with_mode::<MessageLocation, _>(
        bytes,
        crate::contract::JsonBounds {
            maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
            maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64,
            maximum_depth: 256,
        },
        bytes.len() as u64,
        crate::wire::ErrorCode::Storage,
        native,
        &mut |charge| {
            let charged = if let crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes } = charge {
                let planned = maximum_owned_bytes.checked_add(diagnostic_bytes)
                    .ok_or_else(|| Error::Invalid("message locator reservation overflows".into()))?;
                observer.retain_owned(planned)?;
                reservation = planned;
                Ok(())
            } else { observer.codec_charge(charge) };
            if charged.is_err() { observed_failure = true; }
            charged
        },
    );
    let decoded = retain_original_decode_error(decoded, &mut reservation, observer);
    let decoded = decoded.map_err(|error| {
        if observed_failure { error } else {
            match error {
                Error::Invalid(message) => Error::Storage(format!("execution journal content is invalid: {message}")),
                other => other,
            }
        }
    });
    let location = match decoded {
        Ok(location) => location,
        Err(error) => {
            let _ = observer.release_owned(reservation);
            return Err(error);
        }
    };
    let owned = location.location.authority.id.capacity() as u64;
    let retained = observer.release_owned(reservation - owned)
        .and_then(|()| observer.copy_bytes(location.location.authority.id.len() as u64));
    match retained {
        Ok(()) => Ok(location),
        Err(error) => {
            drop(location);
            let _ = observer.release_owned(owned);
            Err(error)
        }
    }
}

fn message_path(authority: &Authority, message: uuid::Uuid) -> Result<StreamPath> {
    if authority.kind != crate::core::AggregateKind::Conversation {
        return Err(Error::Invalid(
            "message index requires a conversation".into(),
        ));
    }
    let path = authority.stream_path()?;
    let suffix = path
        .strip_prefix("harness/v2/")
        .ok_or_else(|| Error::Invalid("message index authority path is invalid".into()))?;
    Ok(StreamPath::new(format!(
        "harness/v2/conversation-messages/{suffix}/{message}"
    ))?)
}

fn message_head_path(authority: &Authority) -> Result<StreamPath> {
    if authority.kind != crate::core::AggregateKind::Conversation {
        return Err(Error::Invalid(
            "message head requires a conversation".into(),
        ));
    }
    let path = authority.stream_path()?;
    let suffix = path
        .strip_prefix("harness/v2/")
        .ok_or_else(|| Error::Invalid("message head authority path is invalid".into()))?;
    Ok(StreamPath::new(format!(
        "harness/v2/conversation-heads/{suffix}"
    ))?)
}

fn message_sequence_path(authority: &Authority, sequence: u64) -> Result<StreamPath> {
    if sequence == 0 || authority.kind != crate::core::AggregateKind::Conversation {
        return Err(Error::Invalid(
            "message sequence index requires a positive conversation sequence".into(),
        ));
    }
    let path = authority.stream_path()?;
    let suffix = path
        .strip_prefix("harness/v2/")
        .ok_or_else(|| Error::Invalid("message sequence authority path is invalid".into()))?;
    Ok(StreamPath::new(format!(
        "harness/v2/conversation-sequences/{suffix}/{sequence}"
    ))?)
}

pub(crate) fn operation_path(authority: &Authority, operation: OperationId) -> Result<StreamPath> {
    let path = authority.stream_path()?;
    let suffix = path
        .strip_prefix("harness/v2/")
        .ok_or_else(|| Error::Invalid("aggregate operation path is invalid".into()))?;
    Ok(StreamPath::new(format!(
        "harness/v2/aggregate-operations/{suffix}/{operation}"
    ))?)
}

pub(super) fn release_owned_after_drop<T>(
    result: Result<T>,
    bytes: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<T> {
    let released = match work.as_deref_mut() {
        Some(observer) => observer.release_owned(bytes),
        None => Ok(()),
    };
    match result {
        Ok(value) => released.map(|()| value),
        Err(error) => Err(error),
    }
}

// Both original String buffers and the SDK Arc<str> character payload are
// known here. Arc headers and allocator bookkeeping are deliberately excluded.
pub(super) fn publication_path_with_work(
    authority: &Authority,
    prefix: &str,
    last: Option<(&dyn std::fmt::Display, usize)>,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<(StreamPath, u64)> {
    use std::fmt::Write as _;
    let canonical = authority.stream_path_with_work(work)?;
    let canonical_capacity = canonical.capacity() as u64;
    let result = (|| {
        let suffix = canonical.strip_prefix("harness/v2/")
            .ok_or_else(|| Error::Invalid("publication authority path is invalid".into()))?;
        let capacity = prefix.len().checked_add(suffix.len())
            .and_then(|bytes| bytes.checked_add(last.map_or(0, |(_, bytes)| bytes + 1)))
            .ok_or_else(|| Error::Invalid("publication path length overflows".into()))?;
        let mut text = String::new();
        let result = (|| {
            match work.as_deref_mut() {
                Some(observer) => observer.reserve_text(&mut text, capacity)?,
                None => text.reserve_exact(capacity),
            }
            text.push_str(prefix);
            text.push_str(suffix);
            if let Some((last, _)) = last {
                write!(&mut text, "/{last}")
                    .map_err(|_| Error::Invalid("publication path formatting failed".into()))?;
            }
            if let Some(observer) = work.as_deref_mut() {
                observer.copy_bytes(text.len() as u64)?;
                observer.admit_allocation(text.len() as u64)?;
                observer.copy_bytes(text.len() as u64)?;
            }
            let path = StreamPath::new(&text)?;
            let retained = if let Some(observer) = work.as_deref_mut() {
                let payload = path.as_str().len() as u64;
                observer.retain_owned(payload)?;
                payload
            } else { 0 };
            Ok((path, retained))
        })();
        let text_capacity = text.capacity() as u64;
        drop(text);
        release_owned_after_drop(result, text_capacity, work)
    })();
    drop(canonical);
    release_owned_after_drop(result, canonical_capacity, work)
}

fn canonical_publication_path_with_work(
    authority: &Authority,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<(StreamPath, u64)> {
    let text = authority.stream_path_with_work(work)?;
    let capacity = text.capacity() as u64;
    let result = (|| {
        if let Some(observer) = work.as_deref_mut() {
            observer.admit_allocation(text.len() as u64)?;
            observer.copy_bytes(text.len() as u64)?;
        }
        let path = StreamPath::new(&text)?;
        let retained = if let Some(observer) = work.as_deref_mut() {
            let payload = path.as_str().len() as u64;
            observer.retain_owned(payload)?;
            payload
        } else { 0 };
        Ok((path, retained))
    })();
    drop(text);
    release_owned_after_drop(result, capacity, work)
}

/// The existing event and a ref-only location, without another admission lifecycle.
pub(crate) struct IndexedPublication {
    pub(crate) operation_id: OperationId,
    pub(crate) path: StreamPath,
    pub(crate) expected_tail: u64,
    pub(crate) bytes: Bytes,
    indexes: Vec<(StreamPath, u64, Bytes)>,
    retained_payload_bytes: u64,
    operation_index_capacity: u64,
}

fn reserve_indexes(
    indexes: &mut Vec<(StreamPath, u64, Bytes)>,
    additional: usize,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<()> {
    let Some(observer) = work.as_deref_mut() else { return Ok(()); };
    let needed = indexes.len().checked_add(additional)
        .ok_or_else(|| Error::Invalid("publication index count overflows".into()))?;
    if needed <= indexes.capacity() { return Ok(()); }
    let element_bytes = std::mem::size_of::<(StreamPath, u64, Bytes)>() as u64;
    let old_capacity = indexes.capacity();
    observer.admit_allocation((needed - old_capacity) as u64 * element_bytes)?;
    indexes.try_reserve_exact(additional)
        .map_err(|cause| observer.allocation_failure(cause))?;
    observer.retain_owned((indexes.capacity() - old_capacity) as u64 * element_bytes)?;
    Ok(())
}

impl IndexedPublication {
    pub(crate) fn new(authority: &Authority, event: &Event, bytes: Vec<u8>) -> Result<Self> {
        Self::new_with_work(authority, event, bytes, &mut None)
    }

    pub(crate) fn new_with_work(
        authority: &Authority,
        event: &Event,
        bytes: Vec<u8>,
        work: &mut Option<&mut dyn super::history::HistoryIo>,
    ) -> Result<Self> {
        let byte_capacity = bytes.capacity() as u64;
        let prepared = (|| {
            if let Some(observer) = work.as_deref_mut() {
                observer.check_cancellation()?;
            }
            let expected_tail = event.revision.checked_sub(1)
                .ok_or_else(|| Error::Invalid("indexed event revision must be positive".into()))?;
            let path = canonical_publication_path_with_work(authority, work)?;
            Ok((expected_tail, path))
        })();
        let (expected_tail, (path, path_owned)) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                drop(bytes);
                return release_owned_after_drop(Err(error), byte_capacity, work);
            }
        };
        let mut publication = Self {
            operation_id: event.operation_id,
            retained_payload_bytes: byte_capacity + path_owned,
            operation_index_capacity: 0,
            path,
            expected_tail,
            bytes: Bytes::from(bytes),
            indexes: Vec::new(),
        };
        let result = (|| {
            let location = OperationLocation {
                authority: match work.as_deref_mut() {
                    Some(observer) => observer.clone_authority(authority)?,
                    None => authority.clone(),
                },
                operation_id: event.operation_id,
                revision: event.revision,
                intent_digest: event.intent_digest,
                effect_position: None,
            };
            let location_capacity = location.authority.id.capacity() as u64;
            let result = (|| {
                let initial_indexes = match event.payload {
                    crate::core::EventPayload::ConversationBound { .. } => 2,
                    crate::core::EventPayload::ConversationMessageAppended { .. }
                        | crate::core::EventPayload::ProjectMergePublished { .. } => 4,
                    _ => 1,
                };
                reserve_indexes(&mut publication.indexes, initial_indexes, work)?;
                let path = if work.is_none() {
                    (operation_path(authority, event.operation_id)?, 0)
                } else {
                    publication_path_with_work(authority, "harness/v2/aggregate-operations/",
                        Some((&event.operation_id, 36)), work)?
                };
                let encoded = super::history::canonical_bytes_with_work(
                    &location, acyclic_stream::MAX_RECORD_BYTES as u64, work);
                if let Ok(encoded) = &encoded {
                    publication.operation_index_capacity = encoded.capacity() as u64;
                }
                publication.add_derived_index_with_work(path, 0, encoded, work)?;
                if matches!(event.payload, crate::core::EventPayload::ConversationBound { .. }) {
                    if authority.kind != crate::core::AggregateKind::Conversation {
                        return Err(Error::Invalid("message head requires a conversation".into()));
                    }
                    let binding_location = MessageLocation {
                        location: OperationLocation {
                            authority: match work.as_deref_mut() {
                                Some(observer) => observer.clone_authority(&location.authority)?,
                                None => location.authority.clone(),
                            },
                            operation_id: location.operation_id,
                            revision: location.revision,
                            intent_digest: location.intent_digest,
                            effect_position: location.effect_position,
                        },
                        message_id: uuid::Uuid::nil(),
                        sequence: 0,
                    };
                    let binding_capacity = binding_location.location.authority.id.capacity() as u64;
                    let encoded = (|| {
                        let path = if work.is_none() { (message_head_path(authority)?, 0) } else {
                            publication_path_with_work(authority, "harness/v2/conversation-heads/", None, work)?
                        };
                        let encoded = super::history::canonical_bytes_with_work(
                            &binding_location, acyclic_stream::MAX_RECORD_BYTES as u64, work);
                        publication.add_derived_index_with_work(path, 0, encoded, work)
                    })();
                    drop(binding_location);
                    release_owned_after_drop(encoded, binding_capacity, work)?;
                }
                Ok(())
            })();
            let message = match &event.payload {
                crate::core::EventPayload::ConversationMessageAppended { message } => Some(message.as_ref()),
                crate::core::EventPayload::ProjectMergePublished { receipt } => Some(&receipt.notice),
                _ => None,
            };
            let result = result.and_then(|()| {
                if let Some(message) = message {
                    let location = MessageLocation {
                        location,
                        message_id: message.id,
                        sequence: message.sequence,
                    };
                    let path = if authority.kind != crate::core::AggregateKind::Conversation {
                        Err(Error::Invalid("message index requires a conversation".into()))
                    } else if work.is_none() { message_path(authority, message.id).map(|path| (path, 0)) } else {
                        publication_path_with_work(authority, "harness/v2/conversation-messages/",
                            Some((&message.id, 36)), work)
                    };
                    let encoded = path.and_then(|path| {
                        let bytes = super::history::canonical_bytes_with_work(
                            &location, acyclic_stream::MAX_RECORD_BYTES as u64, work);
                        publication.add_derived_index_with_work(path, 0, bytes, work)
                    });
                    drop(location);
                    encoded?;
                    let index = publication.indexes.len() - 1;
                    let path = if work.is_none() { (message_sequence_path(authority, message.sequence)?, 0) } else {
                        if message.sequence == 0 || authority.kind != crate::core::AggregateKind::Conversation {
                            return Err(Error::Invalid("message sequence index requires a positive conversation sequence".into()));
                        }
                        publication_path_with_work(authority, "harness/v2/conversation-sequences/",
                            Some((&message.sequence, 20)), work)?
                    };
                    publication.push_index_with_work(path, 0, publication.indexes[index].2.clone(), 0, work)?;
                    let path = if work.is_none() { (message_head_path(authority)?, 0) } else {
                        publication_path_with_work(authority, "harness/v2/conversation-heads/", None, work)?
                    };
                    // These three locators share the original encoded allocation.
                    publication.push_index_with_work(
                        path, message.sequence, publication.indexes[index].2.clone(), 0, work)?;
                } else {
                    drop(location);
                }
                Ok(())
            });
            release_owned_after_drop(result, location_capacity, work)
        })();
        match result {
            Ok(()) => Ok(publication),
            Err(error) => {
                let owned = publication.into_retained_owned();
                release_owned_after_drop(Err(error), owned, work)
            }
        }
    }

    /// Exact source capacities, not encoded lengths or opaque SDK/provider heap.
    pub(crate) fn retained_owned(&self) -> u64 {
        self.retained_payload_bytes
            + self.indexes.capacity() as u64 * std::mem::size_of::<(StreamPath, u64, Bytes)>() as u64
    }

    /// The caller must first drop request/ACK clones sharing these Bytes/paths.
    /// This physically drops publication storage before returning its release size.
    pub(crate) fn into_retained_owned(self) -> u64 {
        let owned = self.retained_owned();
        drop(self);
        owned
    }

    pub(super) fn set_effect_position_with_work(
        &mut self,
        position: u64,
        work: &mut Option<&mut dyn super::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
        }
        let (_, _, bytes) = self
            .indexes
            .first_mut()
            .ok_or_else(|| Error::Storage("publication operation index is absent".into()))?;
        let mut location = decode_location_with_work(bytes, work)?;
        location.effect_position = Some(position);
        let location_capacity = location.authority.id.capacity() as u64;
        let encoded = super::history::canonical_bytes_with_work(
            &location, acyclic_stream::MAX_RECORD_BYTES as u64, work);
        drop(location);
        let encoded = release_owned_after_drop(encoded, location_capacity, work)?;
        let capacity = encoded.capacity() as u64;
        let old = std::mem::replace(bytes, Bytes::from(encoded));
        drop(old);
        let old_capacity = std::mem::replace(&mut self.operation_index_capacity, capacity);
        self.retained_payload_bytes = self.retained_payload_bytes - old_capacity + capacity;
        release_owned_after_drop(Ok(()), old_capacity, work)?;
        Ok(())
    }

    pub(super) fn add_derived_index_with_work(
        &mut self,
        path: (StreamPath, u64),
        tail: u64,
        bytes: Result<Vec<u8>>,
        work: &mut Option<&mut dyn super::history::HistoryIo>,
    ) -> Result<()> {
        let bytes = match bytes {
            Ok(bytes) => bytes,
            Err(error) => {
                let (path, retained) = path;
                drop(path);
                return release_owned_after_drop(Err(error), retained, work);
            }
        };
        let capacity = bytes.capacity() as u64;
        self.push_index_with_work(path, tail, Bytes::from(bytes), capacity, work)
    }

    fn push_index_with_work(
        &mut self,
        path: (StreamPath, u64),
        tail: u64,
        bytes: Bytes,
        capacity: u64,
        work: &mut Option<&mut dyn super::history::HistoryIo>,
    ) -> Result<()> {
        let (path, path_bytes) = path;
        let admitted = reserve_indexes(&mut self.indexes, 1, work);
        if let Err(error) = admitted {
            drop(path);
            drop(bytes);
            return release_owned_after_drop(Err(error), capacity + path_bytes, work);
        }
        self.retained_payload_bytes += capacity + path_bytes;
        self.indexes.push((path, tail, bytes));
        Ok(())
    }

    pub(crate) fn add_index(&self, request: &mut CommitRequest) {
        for (path, tail, bytes) in &self.indexes {
            request.conditions.push(if *tail == 0 {
                CommitCondition::Absent { path: path.clone() }
            } else {
                CommitCondition::Tail {
                    path: path.clone(),
                    expected: *tail,
                }
            });
            request.mutations.push(CommitMutation::Append {
                path: path.clone(),
                records: vec![bytes.clone()],
            });
        }
    }

    pub(crate) fn add_index_with_work(
        &self,
        request: &mut crate::distributed::PreparedCommitRequest<'_, '_>,
    ) -> Result<()> {
        for (path, tail, bytes) in &self.indexes {
            request.push_condition(if *tail == 0 {
                CommitCondition::Absent { path: path.clone() }
            } else {
                CommitCondition::Tail { path: path.clone(), expected: *tail }
            })?;
            request.append(path.clone(), bytes.clone())?;
        }
        Ok(())
    }

    pub(crate) async fn request<P: StreamProvider>(
        &self,
        client: &StreamClient<P>,
        key: IdempotencyKey,
    ) -> Result<CommitRequest> {
        let target = match client.bounds(self.path.as_str()).await {
            Ok(_) => CommitCondition::Tail {
                path: self.path.clone(),
                expected: self.expected_tail,
            },
            Err(acyclic_stream::StreamError::NotFound) if self.expected_tail == 0 => {
                CommitCondition::Absent {
                    path: self.path.clone(),
                }
            }
            Err(error) => return Err(error.into()),
        };
        let mut request = CommitRequest {
            conditions: vec![target],
            mutations: vec![CommitMutation::Append {
                path: self.path.clone(),
                records: vec![self.bytes.clone()],
            }],
            idempotency_key: key,
        };
        self.add_index(&mut request);
        Ok(request)
    }

    pub(crate) fn verify(&self, envelope: &CommittedEnvelope) -> Result<()> {
        if envelope.mutations.len() != 1 + self.indexes.len() {
            return Err(Error::Storage(
                "indexed publication has invalid mutation count".into(),
            ));
        }
        for (path, tail, bytes) in std::iter::once((&self.path, self.expected_tail, &self.bytes))
            .chain(
                self.indexes
                    .iter()
                    .map(|(path, tail, bytes)| (path, *tail, bytes)),
            )
        {
            let append = envelope
                .mutations
                .iter()
                .find_map(|mutation| match mutation {
                    CommittedMutation::Append(append) if &append.path == path => Some(append),
                    _ => None,
                })
                .ok_or_else(|| Error::Storage("indexed publication is missing an append".into()))?;
            let end = tail
                .checked_add(1)
                .ok_or_else(|| Error::Storage("indexed publication tail overflows".into()))?;
            if append.start != tail
                || append.end != end
                || append.tail != end
                || append.records.len() != 1
                || append.records.first().is_none_or(|record| {
                    record.sequence != tail
                        || record.value != bytes
                        || record.commit_id != envelope.commit_id
                })
            {
                return Err(Error::Storage(
                    "indexed publication append is invalid".into(),
                ));
            }
        }
        Ok(())
    }
}

pub(super) async fn one_record<P: StreamProvider>(
    client: &StreamClient<P>,
    path: &StreamPath,
    from: u64,
) -> Result<Option<Record>> {
    one_record_with_work(client, path, from, &mut None).await
}

/// The original one-record lookup with receipts at each visible SDK boundary.
pub(crate) async fn one_record_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    path: &StreamPath,
    from: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<Option<Record>> {
    if let Some(work) = work.as_deref_mut() { work.begin_call()?; }
    let mut records = match client.stream(path.as_str())?.read(from, 1).await {
        Ok(records) => records,
        Err(acyclic_stream::StreamError::NotFound) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if let Some(work) = work.as_deref_mut() { work.check_cancellation()?; }
    let first = records.try_next().await?;
    if let Some(record) = &first
        && let Some(work) = work.as_deref_mut()
    {
        work.receive_record(record.value.len() as u64)?;
    }
    if let Some(work) = work.as_deref_mut() { work.check_cancellation()?; }
    if let Some(extra) = records.try_next().await? {
        if let Some(work) = work.as_deref_mut() {
            work.receive_record(extra.value.len() as u64)?;
        }
        return Err(Error::Storage(
            "operation lookup exceeded its one-record bound".into(),
        ));
    }
    if let Some(record) = &first
        && (record.sequence != from || record.value.len() > acyclic_stream::MAX_RECORD_BYTES)
    {
        return Err(Error::Storage(
            "operation location record is invalid".into(),
        ));
    }
    Ok(first)
}

pub(crate) async fn find_operation<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    operation: OperationId,
) -> Result<Option<Event>> {
    find_operation_bounded(
        client,
        authority,
        verifier,
        operation,
        2 * acyclic_stream::MAX_RECORD_BYTES as u64,
    )
    .await
    .map(|(event, _)| event)
}

/// Counts the atomic locator and canonical record against one caller-owned budget.
pub(crate) async fn find_operation_bounded<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    operation: OperationId,
    maximum_bytes: u64,
) -> Result<(Option<Event>, u64)> {
    find_operation_bounded_with_work(client, authority, verifier, operation, maximum_bytes, true, &mut None).await
}

pub(crate) async fn find_operation_bounded_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    operation: OperationId,
    maximum_bytes: u64,
    return_event: bool,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<(Option<Event>, u64)> {
    verifier.verify_audience(authority)?;
    if maximum_bytes == 0 {
        return Err(Error::Invalid(
            "operation lookup byte bound must be positive".into(),
        ));
    }
    let path = operation_path(authority, operation)?;
    let Some(index) = one_record_with_work(client, &path, 0, work).await? else {
        if let Some(work) = work.as_deref_mut() { work.begin_call()?; }
        return match client.stream(path.as_str())?.tail().await {
            Err(acyclic_stream::StreamError::NotFound) => Ok((None, 0)),
            Ok(_) => Err(Error::Storage(
                "operation location is missing before its committed tail".into(),
            )),
            Err(error) => Err(error.into()),
        };
    };
    let index_bytes = index.value.len() as u64;
    if index_bytes >= maximum_bytes {
        return Err(Error::Invalid("operation lookup exceeds byte bound".into()));
    }
    if let Some(work) = work.as_deref_mut() { work.begin_call()?; }
    if client.stream(path.as_str())?.tail().await? != 1 {
        return Err(Error::Storage(
            "operation location is not a single immutable record".into(),
        ));
    }
    let location = decode_location_with_work(&index.value, work)?;
    if &location.authority != authority
        || location.operation_id != operation
        || location.revision == 0
    {
        return Err(Error::Storage(
            "operation location identity is invalid".into(),
        ));
    }
    let (event, bytes) = verify_operation_locator_with_work(
        client, authority, verifier, &index, &location, maximum_bytes, work,
    ).await?;
    let location_bytes = location.authority.id.capacity() as u64;
    drop(location);
    if let Some(work) = work.as_deref_mut() { work.release_owned(location_bytes)?; }
    drop(index);
    if let Some(work) = work.as_deref_mut() {
        if return_event { work.stage_output(bytes - index_bytes, 1)?; }
        work.release_record(index_bytes)?;
    }
    Ok((Some(event), bytes))
}

pub(super) async fn verify_operation_locator<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    index: &Record,
    location: &OperationLocation,
    maximum_bytes: u64,
) -> Result<(Event, u64)> {
    verify_operation_locator_with_work(client, authority, verifier, index, location, maximum_bytes, &mut None).await
}

pub(crate) async fn verify_operation_locator_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    index: &Record,
    location: &OperationLocation,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<(Event, u64)> {
    verifier.verify_audience(authority)?;
    let index_bytes = index.value.len() as u64;
    if index_bytes >= maximum_bytes {
        return Err(Error::Invalid("operation lookup exceeds byte bound".into()));
    }
    if &location.authority != authority || location.revision == 0 {
        return Err(Error::Storage(
            "operation location identity is invalid".into(),
        ));
    }
    let canonical = StreamPath::new(authority.stream_path()?)?;
    let sequence = location.revision - 1;
    let record = one_record_with_work(client, &canonical, sequence, work)
        .await?
        .ok_or_else(|| Error::Storage("indexed canonical event is missing".into()))?;
    let consumed_bytes = index_bytes
        .checked_add(record.value.len() as u64)
        .filter(|bytes| *bytes <= maximum_bytes)
        .ok_or_else(|| Error::Invalid("operation lookup exceeds byte bound".into()))?;
    let event = super::history::verify_history_record_with_work(verifier, authority, sequence, &record, work)?;
    verify_operation_binding(authority, index, location, &record, &event)?;
    let record_bytes = record.value.len() as u64;
    drop(record);
    if let Some(work) = work.as_deref_mut() { work.release_record(record_bytes)?; }
    Ok((event, consumed_bytes))
}

pub(super) fn verify_operation_binding(
    authority: &Authority,
    index: &Record,
    location: &OperationLocation,
    canonical: &Record,
    event: &Event,
) -> Result<()> {
    if &location.authority != authority
        || location.revision != event.revision
        || canonical.sequence.checked_add(1) != Some(event.revision)
        || event.operation_id != location.operation_id
        || event.intent_digest != location.intent_digest
        || canonical.commit_id != index.commit_id
    {
        return Err(Error::Storage(
            "operation location differs from its atomic canonical event".into(),
        ));
    }
    Ok(())
}

/// Loads one exact archived message without reconstructing a conversation projection.
/// Both the derived location and canonical record must belong to the same commit.
pub(crate) async fn find_message<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    message_id: uuid::Uuid,
    through_revision: u64,
    maximum_bytes: u64,
) -> Result<Option<crate::conversation::ConversationMessage>> {
    find_message_counted(
        client,
        authority,
        verifier,
        message_id,
        through_revision,
        maximum_bytes,
    )
    .await
    .map(|(message, _)| message)
}

pub(crate) async fn find_message_counted<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    message_id: uuid::Uuid,
    through_revision: u64,
    maximum_bytes: u64,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    let path = message_path(authority, message_id)?;
    find_message_at_locator(
        client,
        authority,
        verifier,
        path,
        Some(message_id),
        None,
        through_revision,
        maximum_bytes,
    )
    .await
}

pub(crate) async fn find_message_sequence<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    sequence: u64,
    through_revision: u64,
    maximum_bytes: u64,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    find_message_sequence_with_work(client, authority, verifier, sequence, through_revision, maximum_bytes, &mut None).await
}

pub(crate) async fn find_message_sequence_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    sequence: u64,
    through_revision: u64,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    let path = message_sequence_path(authority, sequence)?;
    find_message_at_locator_with_work(
        client,
        authority,
        verifier,
        path,
        None,
        Some(sequence),
        through_revision,
        maximum_bytes,
        work,
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "one atomic locator verifier binds its exact ID or sequence and pinned byte/read boundary"
)]
async fn find_message_at_locator<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    path: StreamPath,
    expected_id: Option<uuid::Uuid>,
    expected_sequence: Option<u64>,
    through_revision: u64,
    maximum_bytes: u64,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    find_message_at_locator_with_work(client, authority, verifier, path, expected_id,
        expected_sequence, through_revision, maximum_bytes, &mut None).await
}

#[allow(clippy::too_many_arguments, reason = "same exact indexed locator and pinned cut with optional SDK receipts")]
async fn find_message_at_locator_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    path: StreamPath,
    expected_id: Option<uuid::Uuid>,
    expected_sequence: Option<u64>,
    through_revision: u64,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    verifier.verify_audience(authority)?;
    if maximum_bytes == 0 {
        return Err(Error::Invalid(
            "message lookup byte bound must be positive".into(),
        ));
    }
    let Some(index) = one_record_with_work(client, &path, 0, work).await? else {
        if let Some(work) = work.as_deref_mut() { work.begin_call()?; }
        return match client.stream(path.as_str())?.tail().await {
            Err(acyclic_stream::StreamError::NotFound) => Ok((None, 0)),
            Ok(_) => Err(Error::Storage(
                "message location is missing before its committed tail".into(),
            )),
            Err(error) => Err(error.into()),
        };
    };
    if index.value.len() as u64 > maximum_bytes {
        return Err(Error::Invalid("message lookup exceeds byte bound".into()));
    }
    if let Some(work) = work.as_deref_mut() { work.begin_call()?; }
    if client.stream(path.as_str())?.tail().await? != 1 {
        return Err(Error::Storage("message location is not immutable".into()));
    }
    verify_message_locator_with_work(
        client,
        authority,
        verifier,
        index,
        expected_id,
        expected_sequence,
        through_revision,
        maximum_bytes,
        work,
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "shared verification binds one atomic locator to its exact identity and read boundary"
)]
async fn verify_message_locator<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    index: Record,
    expected_id: Option<uuid::Uuid>,
    expected_sequence: Option<u64>,
    through_revision: u64,
    maximum_bytes: u64,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    verify_message_locator_with_work(client, authority, verifier, index, expected_id,
        expected_sequence, through_revision, maximum_bytes, &mut None).await
}

#[allow(clippy::too_many_arguments, reason = "shared original atomic locator verification plus optional receipts")]
async fn verify_message_locator_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    index: Record,
    expected_id: Option<uuid::Uuid>,
    expected_sequence: Option<u64>,
    through_revision: u64,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    let location = decode_message_location_with_work(&index.value, work)?;
    if &location.location.authority != authority
        || expected_id.is_some_and(|id| location.message_id != id)
        || expected_sequence.is_some_and(|sequence| location.sequence != sequence)
        || location.location.revision == 0
        || (location.sequence == 0 && expected_sequence != Some(0))
    {
        return Err(Error::Storage(
            "message location identity is invalid".into(),
        ));
    }
    let (event, consumed_bytes) = verify_operation_locator_with_work(
        client,
        authority,
        verifier,
        &index,
        &location.location,
        maximum_bytes,
        work,
    )
    .await?;
    let index_bytes = index.value.len() as u64;
    drop(index);
    if let Some(work) = work.as_deref_mut() { work.release_record(index_bytes)?; }
    let message_id = location.message_id;
    let sequence = location.sequence;
    let location_bytes = location.location.authority.id.capacity() as u64;
    drop(location);
    if let Some(work) = work.as_deref_mut() { work.release_owned(location_bytes)?; }
    if sequence == 0 {
        let bound = message_id.is_nil()
            && matches!(&event.payload, crate::core::EventPayload::ConversationBound { .. });
        let event_bytes = if work.is_some() { crate::core::event_clone_reservation(&event)? } else { 0 };
        drop(event);
        if let Some(work) = work.as_deref_mut() { work.release_owned(event_bytes)?; }
        return if bound {
            Ok((None, consumed_bytes))
        } else {
            Err(Error::Storage(
                "empty message head differs from its canonical binding".into(),
            ))
        };
    }
    let message = match event.payload {
        crate::core::EventPayload::ConversationMessageAppended { message } => *message,
        crate::core::EventPayload::ProjectMergePublished { receipt } => receipt.notice,
        _ => {
            return Err(Error::Storage(
                "message location points to another event kind".into(),
            ));
        }
    };
    if message.id != message_id || message.sequence != sequence {
        return Err(Error::Storage(
            "message location differs from its canonical message".into(),
        ));
    }
    if event.revision <= through_revision
        && let Some(work) = work.as_deref_mut()
    {
        work.stage_output(consumed_bytes - index_bytes, 1)?;
    }
    Ok((
        (event.revision <= through_revision).then_some(message),
        consumed_bytes,
    ))
}

/// Captures one logical message head without a reducer or retained-history scan.
/// The head locator and canonical record share the existing publication commit.
pub(crate) async fn latest_message<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    maximum_bytes: u64,
) -> Result<Option<crate::conversation::ConversationMessage>> {
    verifier.verify_audience(authority)?;
    if maximum_bytes == 0 {
        return Err(Error::Invalid(
            "message lookup byte bound must be positive".into(),
        ));
    }
    let path = message_head_path(authority)?;
    let through = match client.stream(path.as_str())?.tail().await {
        Ok(0) => return Err(Error::Storage("message head is empty".into())),
        Ok(through) => through,
        Err(acyclic_stream::StreamError::NotFound) => {
            return match client.stream(authority.stream_path()?)?.tail().await {
                Ok(0) | Err(acyclic_stream::StreamError::NotFound) => Ok(None),
                Ok(_) => Err(Error::Storage(
                    "message head is absent for a nonempty aggregate".into(),
                )),
                Err(error) => Err(error.into()),
            };
        }
        Err(error) => return Err(error.into()),
    };
    let index = one_record(client, &path, through - 1)
        .await?
        .ok_or_else(|| {
            Error::Storage("message head is missing before its committed tail".into())
        })?;
    if index.value.len() as u64 > maximum_bytes {
        return Err(Error::Invalid("message lookup exceeds byte bound".into()));
    }
    verify_message_locator(
        client,
        authority,
        verifier,
        index,
        None,
        Some(through - 1),
        u64::MAX,
        maximum_bytes,
    )
    .await
    .map(|(message, _)| message)
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod native_publication_custody_tests {
    use super::*;
    use super::super::history::{HistoryIo, native_work_io};
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    use acyclic_stream::MemoryStream;
    use std::sync::Arc;

    fn budget() -> WorkBudget {
        WorkBudget {
            authority_records_read: 32,
            authority_bytes_read: 1_048_576,
            backend_read_operations: 32,
            source_bytes_read: 1_048_576,
            bytes_copied: 1_048_576,
            bytes_encoded: 1_048_576,
            bytes_hashed: 1_048_576,
            authority_records_appended: 32,
            authority_bytes_written: 1_048_576,
            backend_write_operations: 32,
            items_examined: 1_048_576,
            allocation_operations: 1_048_576,
            peak_allocation_bytes: 1_048_576,
            ..WorkBudget::default()
        }
    }

    #[tokio::test]
    async fn original_provider_publication_releases_exact_source_after_receipt_drop() -> Result<()> {
        use crate::core::{Action, AggregateKind, ApplyResult, AuthorityIssuer, Command,
            LifecycleState, Reducer, SchemaRegistry};
        let authority = Authority { kind: AggregateKind::Agent, id: "publication-custody".into() };
        let issuer = AuthorityIssuer::new("publication-test", [7; 32], authority.clone());
        let reducer = Reducer::new(authority.clone(), issuer.verifier(), SchemaRegistry::new());
        let command = Command {
            operation_id: OperationId::from_bytes([91; 16]),
            idempotency_key: crate::IdempotencyKey("publication-custody".into()),
            expected_revision: 0,
            scope: issuer.root("root", crate::Capabilities::new(["lifecycle:manage"])),
            causal_parent: None,
            action: Action::TransitionLifecycle { to: LifecycleState::Active, reason: None },
        };
        let ApplyResult::Applied { event } = reducer.plan(&command)? else {
            panic!("fresh original lifecycle command");
        };
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original finite publication ledger");
        let mut io = native_work_io(&native).expect("original source custody");
        let mut work: Option<&mut dyn HistoryIo> = Some(&mut io);
        let encoded = crate::wire_codec::encode_event_with_work(&authority, &event, &mut work)?;
        let original_capacity = encoded.capacity() as u64;
        let mut publication = IndexedPublication::new_with_work(
            &authority, &event, encoded, &mut work)?;
        assert!(publication.retained_owned() >= original_capacity);
        publication.set_effect_position_with_work(3, &mut work)?;
        let retained = publication.retained_owned();
        drop(work);
        let mut custody = io.into_custody();
        assert_eq!(custody.bytes(), retained);
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let request = publication.request(&client,
            IdempotencyKey::new(Bytes::from_static(b"publication-custody"))?).await?;
        let acyclic_stream::CommitOutcome::Committed(envelope) = client.commit(request).await? else {
            panic!("original absent/Tail atomic publication");
        };
        publication.verify(&envelope)?;
        drop(envelope);
        assert_eq!(publication.into_retained_owned(), retained);
        custody.release(retained).expect("physically dropped source publication");
        assert_eq!(custody.bytes(), 0);
        Ok(())
    }

    #[test]
    fn malformed_native_locator_preserves_original_serde_and_diagnostic_custody() -> Result<()> {
        let source = br#"{"authority":{"id":"native-custody","kind":"agent"},"intent_digest":[3],"operation_id":"00000000-0000-0000-0000-000000000001","revision":1}"#;
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original finite decode ledger");
        let mut io = native_work_io(&native).expect("original diagnostic source custody");
        let error = decode_location_with_work(source, &mut Some(&mut io))
            .expect_err("original fixed digest schema rejects malformed length");
        let Error::NativeDecode { code, source, custody: Some(custody) } = error else {
            panic!("the original serde diagnostic must retain its source guard");
        };
        assert_eq!(code, crate::wire::ErrorCode::Storage);
        assert!(source.is_data());
        assert!(custody.bytes() > 0);
        assert!(native.shares_ledger(custody.work()));
        assert_eq!(io.into_custody().bytes(), 0);
        drop(source);
        drop(custody);
        Ok(())
    }
}
