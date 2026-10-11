//! Derived exact-operation locations, committed with their canonical event.

use crate::{
    Error, OperationId, Result,
    core::{Authority, AuthorityVerifier, Event},
};
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitRequest, CommittedEnvelope, CommittedMutation,
    IdempotencyKey, OwnedBytes, Record, StreamClient, StreamPath, StreamProvider,
};
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};

use crate::distributed::SdkCallError;
pub(crate) fn original_native_decode_mode(observer: &dyn super::history::HistoryIo) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    { observer.original_native_work().is_some() }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { let _ = observer; false }
}

pub(crate) fn decode_preclaimed_json_with_mode<T, F>(
    bytes: &[u8],
    bounds: crate::contract::JsonBounds,
    maximum_owned_bytes: u64,
    code: crate::wire::ErrorCode,
    native: bool,
    value_schema: crate::contract::JsonValueSchema,
    charge: &mut F,
) -> Result<T>
where
    T: serde::de::DeserializeOwned,
    F: FnMut(crate::contract::JsonCodecCharge) -> Result<()>,
{
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if native {
        return crate::contract::json_from_slice_bounded_native_trusted(
            bytes, bounds, maximum_owned_bytes, code, value_schema, charge);
    }
    let _ = (native, code, value_schema);
    crate::contract::json_from_slice_bounded_trusted(bytes, bounds, maximum_owned_bytes, charge)
}

pub(crate) fn retain_original_decode_error<T>(
    mut result: Result<T>,
    reservation: &mut u64,
    observer: &mut dyn super::history::HistoryIo,
) -> Result<T> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Err(Error::NativeDecode { custody, .. }) = &mut result
        && custody.is_none()
    {
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
            0, // The sealed Value witness owns its strings; codec scratch is additional.
            crate::wire::ErrorCode::Storage,
            native,
            crate::contract::JsonValueSchema::Root,
            &mut |charge| {
                let charged = match charge {
                crate::contract::JsonCodecCharge::Work { items: visited, .. } => {
                    items = items.checked_add(visited)
                        .ok_or_else(|| Error::Invalid("index JSON item count overflows".into()))?;
                    observer.codec_charge(charge)
                }
                crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes, value_shape } => {
                    let planned = crate::contract::json_value_decode_owned_bytes(value_shape)?
                        .checked_add(maximum_owned_bytes)
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
                        Err(message_lookup_error(work, crate::wire::ErrorCode::Storage,
                            "execution journal JSON is not canonical"))
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
        crate::contract::JsonValueSchema::None,
        &mut |charge| {
            let charged = if let crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes, .. } = charge {
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
        crate::contract::JsonValueSchema::None,
        &mut |charge| {
            let charged = if let crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes, .. } = charge {
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
            .ok_or_else(|| message_lookup_error(work, crate::wire::ErrorCode::Invalid,
                "publication authority path is invalid"))?;
        let capacity = prefix.len().checked_add(suffix.len())
            .and_then(|bytes| bytes.checked_add(last.map_or(0, |(_, bytes)| bytes + 1)))
            .ok_or_else(|| message_lookup_error(work, crate::wire::ErrorCode::Invalid,
                "publication path length overflows"))?;
        let mut text = String::new();
        let result = (|| {
            match work.as_deref_mut() {
                Some(observer) => observer.reserve_text(&mut text, capacity)?,
                None => text.reserve_exact(capacity),
            }
            if let Some(observer) = work.as_deref_mut() {
                observer.copy_bytes((prefix.len() + suffix.len()) as u64)?;
            }
            text.push_str(prefix);
            text.push_str(suffix);
            if let Some((last, _)) = last {
                struct Count(usize);
                impl std::fmt::Write for Count {
                    fn write_str(&mut self, value: &str) -> std::fmt::Result {
                        self.0 += value.len();
                        Ok(())
                    }
                }
                let mut count = Count(0);
                write!(&mut count, "/{last}").map_err(|_| message_lookup_error(work,
                    crate::wire::ErrorCode::Invalid, "publication path formatting failed"))?;
                if let Some(observer) = work.as_deref_mut() { observer.copy_bytes(count.0 as u64)?; }
                write!(&mut text, "/{last}").map_err(|_| message_lookup_error(work,
                    crate::wire::ErrorCode::Invalid, "publication path formatting failed"))?;
            }
            let retained = if let Some(observer) = work.as_deref_mut() {
                let payload = text.len() as u64;
                observer.admit_allocation(payload)?;
                observer.copy_bytes(payload)?;
                observer.retain_owned(payload)?;
                payload
            } else { 0 };
            match StreamPath::new(&text) {
                Ok(path) => Ok((path, retained)),
                Err(error) => release_owned_after_drop(
                    Err(stream_error_with_work(error, work)), retained, work),
            }
        })();
        let text_capacity = text.capacity() as u64;
        drop(text);
        release_owned_after_drop(result, text_capacity, work)
    })();
    drop(canonical);
    release_owned_after_drop(result, canonical_capacity, work)
}

pub(super) fn canonical_publication_path_with_work(
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
        let retained = if let Some(observer) = work.as_deref_mut() {
            let payload = text.len() as u64;
            observer.retain_owned(payload)?;
            payload
        } else { 0 };
        match StreamPath::new(&text) {
            Ok(path) => Ok((path, retained)),
            Err(error) => release_owned_after_drop(
                Err(stream_error_with_work(error, work)), retained, work),
        }
    })();
    drop(text);
    release_owned_after_drop(result, capacity, work)
}

/// Freezes a producer-retained vector without replacing its original claim.
pub(crate) fn owned_bytes_from_retained_vec(
    bytes: Vec<u8>,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<(OwnedBytes, u64)> {
    let capacity = bytes.capacity() as u64;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(original) = work.as_deref().and_then(|observer|
        observer.original_native_work()).cloned()
    {
        let custody = match work.as_deref_mut().expect("original native observer")
            .take_native_custody(capacity)
        {
            Ok(custody) => custody,
            Err(error) => {
                drop(bytes);
                return release_owned_after_drop(Err(error), capacity, work);
            },
        };
        return OwnedBytes::from_vec_with_guard(bytes, custody, original.runtime_meter_owner())
            .map(|bytes| (bytes, 0))
            .map_err(|failure| match failure.into_error() {
                acyclic_stream::PreparedOwnedBytesError::Admission(source) => Error::NativeWork {
                    cause: original.runtime_error(source), custody: None,
                },
                _ => Error::NativeWork {
                    cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout, custody: None,
                },
            });
    }
    Ok((OwnedBytes::from(bytes), if work.is_some() { capacity } else { 0 }))
}

/// The existing event and a ref-only location, without another admission lifecycle.
pub(crate) struct IndexedPublication {
    pub(crate) operation_id: OperationId,
    pub(crate) path: StreamPath,
    pub(crate) expected_tail: u64,
    pub(crate) bytes: OwnedBytes,
    indexes: Vec<(StreamPath, u64, OwnedBytes)>,
    retained_payload_bytes: u64,
    operation_index_capacity: u64,
}

fn reserve_indexes(
    indexes: &mut Vec<(StreamPath, u64, OwnedBytes)>,
    additional: usize,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<()> {
    let Some(observer) = work.as_deref_mut() else { return Ok(()); };
    let needed = indexes.len().checked_add(additional)
        .ok_or_else(|| Error::Invalid("publication index count overflows".into()))?;
    if needed <= indexes.capacity() { return Ok(()); }
    let element_bytes = std::mem::size_of::<(StreamPath, u64, OwnedBytes)>() as u64;
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
        let (bytes, byte_owned) = match owned_bytes_from_retained_vec(bytes, work) {
            Ok(bytes) => bytes,
            Err(error) => {
                drop(path);
                return release_owned_after_drop(Err(error), path_owned, work);
            },
        };
        let mut publication = Self {
            operation_id: event.operation_id,
            retained_payload_bytes: byte_owned + path_owned,
            operation_index_capacity: 0,
            path,
            expected_tail,
            bytes,
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
            + self.indexes.capacity() as u64 * std::mem::size_of::<(StreamPath, u64, OwnedBytes)>() as u64
    }

    /// Ordinary callers must first drop request/ACK clones sharing bytes/paths.
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
        let (encoded, capacity) = owned_bytes_from_retained_vec(encoded, work)?;
        let old = std::mem::replace(bytes, encoded);
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
        let (bytes, capacity) = match owned_bytes_from_retained_vec(bytes, work) {
            Ok(bytes) => bytes,
            Err(error) => {
                let (path, retained) = path;
                drop(path);
                return release_owned_after_drop(Err(error), retained, work);
            },
        };
        if self.indexes.is_empty() {
            self.operation_index_capacity = capacity;
        }
        self.push_index_with_work(path, tail, bytes, capacity, work)
    }

    fn push_index_with_work(
        &mut self,
        path: (StreamPath, u64),
        tail: u64,
        bytes: OwnedBytes,
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
        let target = self.target_condition_with_work(client, &mut None).await?;
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

    pub(crate) async fn request_with_work<'a, 'b, P: StreamProvider>(
        &self,
        client: &StreamClient<P>,
        key: IdempotencyKey,
        work: &'a mut Option<&'b mut dyn super::history::HistoryIo>,
    ) -> Result<crate::distributed::PreparedCommitRequest<'a, 'b>> {
        let key_owned = if work.as_deref().is_some_and(|observer|
            !original_native_decode_mode(observer)) { 32 } else { 0 };
        let mut request = crate::distributed::PreparedCommitRequest::new_with_retained_key(
            key, key_owned, work);
        let target = self.target_condition_with_work(client, request.work()).await?;
        request.push_condition(target)?;
        request.append(self.path.clone(), self.bytes.clone())?;
        self.add_index_with_work(&mut request)?;
        Ok(request)
    }

    async fn target_condition_with_work<P: StreamProvider>(
        &self,
        client: &StreamClient<P>,
        work: &mut Option<&mut dyn super::history::HistoryIo>,
    ) -> Result<CommitCondition> {
        if let Some(observer) = work.as_deref() { observer.check_cancellation()?; }
        let bounds = bounds_with_work(client, &self.path, work).await?;
        match bounds {
            Ok(_) => Ok(CommitCondition::Tail {
                path: self.path.clone(),
                expected: self.expected_tail,
            }),
            Err(error) if *error.source() == acyclic_stream::StreamError::NotFound && self.expected_tail == 0 => {
                drop(error);
                Ok(CommitCondition::Absent { path: self.path.clone() })
            }
            Err(error) => Err(error.into_error(work)),
        }
    }

    pub(crate) fn verify(&self, envelope: &CommittedEnvelope) -> Result<()> {
        self.verify_with_work(envelope, &mut None)
    }

    pub(crate) fn verify_with_work(
        &self,
        envelope: &CommittedEnvelope,
        work: &mut Option<&mut dyn super::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref() { observer.check_cancellation()?; }
        let native = work.as_deref().is_some_and(original_native_decode_mode);
        if envelope.mutations.len() != 1 + self.indexes.len() {
            return Err(indexed_receipt_error(native, "indexed publication has invalid mutation count"));
        }
        for (path, tail, bytes) in std::iter::once((&self.path, self.expected_tail, &self.bytes))
            .chain(
                self.indexes
                    .iter()
                    .map(|(path, tail, bytes)| (path, *tail, bytes)),
            )
        {
            if let Some(observer) = work.as_deref() { observer.check_cancellation()?; }
            let append = envelope
                .mutations
                .iter()
                .find_map(|mutation| match mutation {
                    CommittedMutation::Append(append) if &append.path == path => Some(append),
                    _ => None,
                })
                .ok_or_else(|| indexed_receipt_error(native, "indexed publication is missing an append"))?;
            let end = tail
                .checked_add(1)
                .ok_or_else(|| indexed_receipt_error(native, "indexed publication tail overflows"))?;
            if append.start != tail
                || append.end != end
                || append.tail != end
                || append.records.len() != 1
                || append.records.first().is_none_or(|record| {
                    record.sequence != tail
                        || record.value != *bytes
                        || record.commit_id != envelope.commit_id
                })
            {
                return Err(indexed_receipt_error(native, "indexed publication append is invalid"));
            }
        }
        Ok(())
    }
}

fn indexed_receipt_error(native: bool, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if native {
        return Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, reason };
    }
    let _ = native;
    Error::Storage(reason.into())
}

pub(crate) struct ObservedStream<P> {
    pub(crate) stream: acyclic_stream::Stream<P>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _path_custody: Option<acyclic_fs::NativeAllocation>,
}

pub(crate) fn stream_error_with_work(
    source: acyclic_stream::StreamError,
    work: &Option<&mut dyn super::history::HistoryIo>,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(original_native_decode_mode) {
        let code = if matches!(&source, acyclic_stream::StreamError::IdempotencyMismatch) {
            crate::wire::ErrorCode::Conflict
        } else {
            crate::wire::ErrorCode::Storage
        };
        return Error::NativeStream { code, source, custody: None };
    }
    let _ = work;
    source.into()
}

pub(crate) fn stream_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    path: &StreamPath,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<ObservedStream<P>> {
    stream_text_with_work(client, path.as_str(), work)
}

pub(crate) fn stream_text_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    path: &str,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<ObservedStream<P>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let path_custody = if let Some(observer) = work.as_deref_mut()
        .filter(|observer| observer.original_native_work().is_some())
    {
        let bytes = path.len() as u64;
        observer.admit_owned(bytes)?;
        observer.admit_allocation(bytes)?;
        observer.admit_copy(bytes)?;
        observer.retain_owned(bytes)?;
        let custody = observer.take_native_custody(bytes)?;
        observer.copy_bytes(bytes)?;
        Some(custody)
    } else { None };
    let stream = client.stream(path)
        .map_err(|error| stream_error_with_work(error, work))?;
    Ok(ObservedStream {
        stream,
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        _path_custody: path_custody,
    })
}

/// Native payload custody is intrinsic to the SDK's `OwnedBytes`.
pub(crate) fn sdk_record_retained_bytes(
    record: &Record,
    work: &Option<&mut dyn super::history::HistoryIo>,
) -> u64 {
    if work.as_deref().is_some_and(original_native_decode_mode) { 0 }
    else { record.value.len() as u64 }
}

/// The original finite producer; the native cursor guard follows its box.
pub(crate) enum ObservedRecords {
    Ordinary(acyclic_stream::RecordStream),
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Native(acyclic_stream::NativeStreamOutput<acyclic_stream::NativeRecordStream>),
}

/// Retains the actual record's SDK metadata, never a second byte owner.
pub(crate) enum ObservedRecord {
    Ordinary(Record),
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Native(acyclic_stream::NativeStreamOutput<Record>),
}

impl std::ops::Deref for ObservedRecord {
    type Target = Record;
    fn deref(&self) -> &Record {
        match self {
            Self::Ordinary(record) => record,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::Native(output) => output.value(),
        }
    }
}

impl std::fmt::Debug for ObservedRecord {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, formatter)
    }
}
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn original_sdk_metadata_output<T>(
    original: &acyclic_fs::NativeMaterializationWork,
    output: acyclic_stream::NativeStreamOutput<T>,
) -> Result<acyclic_stream::NativeStreamOutput<T>> {
    let (value, allocation) = output.into_parts();
    let same_meter = std::sync::Arc::ptr_eq(allocation.meter(), &original.runtime_meter_owner());
    let output = acyclic_stream::NativeStreamOutput::new(value, allocation);
    if !same_meter {
        return Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout, custody: None,
        });
    }
    Ok(output)
}

impl ObservedRecords {
    pub(crate) async fn next(
        &mut self,
        work: &mut Option<&mut dyn super::history::HistoryIo>,
    ) -> Result<Option<ObservedRecord>> {
        match self {
            Self::Ordinary(records) => {
                let record = records.try_next().await
                    .map_err(|error| stream_error_with_work(error, work))?;
                if let Some(record) = &record
                    && let Some(observer) = work.as_deref_mut()
                {
                    observer.receive_record(record.value.len() as u64)?;
                }
                Ok(record.map(ObservedRecord::Ordinary))
            }
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::Native(records) => {
                let original = work.as_deref().and_then(|observer| observer.original_native_work())
                    .expect("native cursor has its original observer").clone();
                let Some(record) = records.value_mut().try_next().await
                    .map_err(|error| Error::NativeWork { cause: original.runtime_error(error), custody: None })?
                else { return Ok(None); };
                let record = super::history::native_sdk_result(&original, Ok(record))?;
                work.as_deref_mut().expect("native cursor observer")
                    .receive_native_record(record.value().value.len() as u64)?;
                Ok(Some(ObservedRecord::Native(
                    original_sdk_metadata_output(&original, record)?)))
            }
        }
    }
}

pub(crate) async fn read_records_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    stream: &acyclic_stream::Stream<P>,
    from: u64,
    count: u32,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<std::result::Result<ObservedRecords, SdkCallError>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(original) = work.as_deref().and_then(|observer| observer.original_native_work()) {
        return native_records_with_work(client, stream.path(), from, count, original).await;
    }
    if let Some(observer) = work.as_deref_mut() { observer.begin_call()?; }
    Ok(stream.read(from, count).await.map(ObservedRecords::Ordinary).map_err(SdkCallError::ordinary))
}
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
async fn native_records_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    path: &StreamPath,
    from: u64,
    count: u32,
    original: &acyclic_fs::NativeMaterializationWork,
) -> Result<std::result::Result<ObservedRecords, SdkCallError>> {
    let result = client.native_read(acyclic_stream::ReadRequest {
        path: path.clone(), from, limit: count,
    }, original.runtime_meter_owner()).await;
    match crate::distributed::native_call_result(original, result)? {
        Ok(output) => Ok(Ok(ObservedRecords::Native(
            original_sdk_metadata_output(original, output)?))),
        Err(error) => Ok(Err(error)),
    }
}

async fn read_path_records_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    path: &StreamPath,
    from: u64,
    count: u32,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<std::result::Result<ObservedRecords, SdkCallError>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(original) = work.as_deref().and_then(|observer| observer.original_native_work()) {
        return native_records_with_work(client, path, from, count, original).await;
    }
    let handle = stream_with_work(client, path, work)?;
    read_records_with_work(client, &handle.stream, from, count, work).await
}

/// Preserves the provider's NotFound result for the canonical CAS selector.
pub(crate) async fn bounds_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    path: &StreamPath,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<std::result::Result<acyclic_stream::StreamBounds, SdkCallError>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(original) = work.as_deref().and_then(|observer| observer.original_native_work()) {
        let result = client.native_bounds(path.clone(), original.runtime_meter_owner()).await;
        return crate::distributed::native_call_result(original, result)
            .map(|result| result.map(|output| *output.value()));
    }
    if work.is_none() {
        return Ok(client.bounds(path.as_str()).await.map_err(SdkCallError::ordinary));
    }
    let handle = stream_with_work(client, path, work)?;
    if let Some(observer) = work.as_deref_mut() { observer.begin_call()?; }
    Ok(handle.stream.bounds().await.map_err(SdkCallError::ordinary))
}

pub(crate) async fn tail_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    path: &StreamPath,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<u64> {
    tail_optional_with_work(client, path, work).await?
        .ok_or_else(|| stream_error_with_work(acyclic_stream::StreamError::NotFound, work))
}

pub(crate) async fn tail_optional_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    path: &StreamPath,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<Option<u64>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(original) = work.as_deref().and_then(|observer| observer.original_native_work()) {
        let result = client.native_tail(path.clone(), original.runtime_meter_owner()).await;
        let result = crate::distributed::native_call_result(original, result)?;
        return match result {
            Ok(output) => Ok(Some(*output.value())),
            Err(error) if *error.source() == acyclic_stream::StreamError::NotFound => {
                drop(error);
                Ok(None)
            },
            Err(error) => Err(error.into_error(work)),
        };
    }
    let handle = stream_with_work(client, path, work)?;
    if let Some(observer) = work.as_deref_mut() { observer.begin_call()?; }
    match handle.stream.tail().await {
        Ok(tail) => Ok(Some(tail)),
        Err(acyclic_stream::StreamError::NotFound) => Ok(None),
        Err(error) => Err(stream_error_with_work(error, work)),
    }
}

pub(super) async fn one_record<P: StreamProvider>(
    client: &StreamClient<P>,
    path: &StreamPath,
    from: u64,
) -> Result<Option<Record>> {
    match one_record_with_work(client, path, from, &mut None).await? {
        None => Ok(None),
        Some(ObservedRecord::Ordinary(record)) => Ok(Some(record)),
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        Some(ObservedRecord::Native(_)) => unreachable!("ordinary lookup cannot produce native metadata"),
    }
}

/// The original one-record lookup with receipts at each visible SDK boundary.
pub(crate) async fn one_record_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    path: &StreamPath,
    from: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<Option<ObservedRecord>> {
    let mut records = match read_path_records_with_work(client, path, from, 1, work).await? {
        Ok(records) => records,
        Err(error) if *error.source() == acyclic_stream::StreamError::NotFound => {
            drop(error);
            return Ok(None);
        },
        Err(error) => return Err(error.into_error(work)),
    };
    let mut first = None;
    let mut extra = None;
    let mut received = 0;
    let result = async {
        if let Some(observer) = work.as_deref_mut() { observer.check_cancellation()?; }
        first = records.next(work).await?;
        if let Some(record) = &first {
            received += sdk_record_retained_bytes(record, work);
        }
        if let Some(observer) = work.as_deref_mut() { observer.check_cancellation()?; }
        extra = records.next(work).await?;
        if let Some(record) = &extra {
            received += sdk_record_retained_bytes(record, work);
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(original_native_decode_mode) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage,
                    reason: "operation lookup exceeded its one-record bound" });
            }
            return Err(Error::Storage("operation lookup exceeded its one-record bound".into()));
        }
        if let Some(record) = &first
            && (record.sequence != from || record.value.len() > acyclic_stream::MAX_RECORD_BYTES)
        {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(original_native_decode_mode) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage,
                    reason: "operation location record is invalid" });
            }
            return Err(Error::Storage("operation location record is invalid".into()));
        }
        Ok(())
    }.await;
    drop(records);
    drop(extra);
    match result {
        Ok(()) => Ok(first),
        Err(error) => {
            drop(first);
            if let Some(observer) = work.as_deref_mut() {
                let _ = observer.release_record(received);
            }
            Err(error)
        }
    }
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
    bounded_operation_with_retained_quota(
        client, authority, verifier, operation, maximum_bytes, return_event, work,
    ).await.map(|(event, bytes)| (event.map(|(event, _)| event), bytes))
}

/// Carries the actual original wire decoder's retained event claim, without inventorying the returned event again or replacing its source admission.
pub(crate) async fn bounded_operation_with_retained_quota<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    operation: OperationId,
    maximum_bytes: u64,
    return_event: bool,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<(Option<(Event, u64)>, u64)> {
    verifier.verify_audience_with_work(authority, work)?;
    if maximum_bytes == 0 {
        return Err(message_lookup_error(work, crate::wire::ErrorCode::Invalid,
            "operation lookup byte bound must be positive"));
    }
    let (path, path_owned) = if work.is_none() {
        (operation_path(authority, operation)?, 0)
    } else {
        publication_path_with_work(authority, "harness/v2/aggregate-operations/",
            Some((&operation, 36)), work)?
    };
    let mut location_owned = 0;
    let mut raw_owned = 0;
    let mut staged = None;
    let result = async {
    let Some(index) = one_record_with_work(client, &path, 0, work).await? else {
        return match tail_optional_with_work(client, &path, work).await? {
            None => Ok(0),
            Some(_) => Err(message_lookup_error(work, crate::wire::ErrorCode::Storage,
                "operation location is missing before its committed tail")),
        };
    };
    raw_owned = sdk_record_retained_bytes(&index, work);
    let index_bytes = index.value.len() as u64;
    if index_bytes >= maximum_bytes {
        return Err(message_lookup_error(work, crate::wire::ErrorCode::Invalid,
            "operation lookup exceeds byte bound"));
    }
    if tail_with_work(client, &path, work).await? != 1 {
        return Err(message_lookup_error(work, crate::wire::ErrorCode::Storage,
            "operation location is not a single immutable record"));
    }
    let location = decode_location_with_work(&index.value, work)?;
    location_owned = location.authority.id.capacity() as u64;
    if &location.authority != authority
        || location.operation_id != operation
        || location.revision == 0
    {
        return Err(message_lookup_error(work, crate::wire::ErrorCode::Storage,
            "operation location identity is invalid"));
    }
    let ((event, event_owned), bytes) = verify_operation_locator_with_retained_quota(
        client, authority, verifier, &index, &location, maximum_bytes, work,
    ).await?;
    staged = Some((event, event_owned));
    drop(location);
    drop(index);
    Ok(bytes)
    }.await;
    drop(path);
    let released = match work.as_deref_mut() {
        Some(observer) => {
            let raw = observer.release_record(raw_owned);
            let location = observer.release_owned(location_owned);
            let path = observer.release_owned(path_owned);
            raw.and(location).and(path)
        }
        None => Ok(()),
    };
    let result = match result {
        Ok(bytes) => released.map(|()| bytes),
        Err(error) => Err(error),
    };
    let result = result.and_then(|bytes| {
        if return_event
            && staged.is_some()
            && let Some(observer) = work.as_deref_mut()
        {
            observer.stage_output(bytes - raw_owned, 1)?;
        }
        Ok(bytes)
    });
    match result {
        Ok(bytes) => Ok((staged, bytes)),
        Err(error) => {
            let event_owned = staged.as_ref().map_or(0, |(_, owned)| *owned);
            drop(staged);
            release_owned_after_drop(Err(error), event_owned, work)
        }
    }
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
    verify_operation_locator_with_retained_quota(
        client, authority, verifier, index, location, maximum_bytes, work,
    ).await.map(|((event, _), bytes)| (event, bytes))
}

pub(super) async fn verify_operation_locator_with_retained_quota<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    index: &Record,
    location: &OperationLocation,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<((Event, u64), u64)> {
    verifier.verify_audience_with_work(authority, work)?;
    let index_bytes = index.value.len() as u64;
    if index_bytes >= maximum_bytes {
        return Err(message_lookup_error(work, crate::wire::ErrorCode::Invalid,
            "operation lookup exceeds byte bound"));
    }
    if &location.authority != authority || location.revision == 0 {
        return Err(message_lookup_error(work, crate::wire::ErrorCode::Storage,
            "operation location identity is invalid"));
    }
    let (canonical, path_owned) = if work.is_none() {
        (StreamPath::new(authority.stream_path()?)?, 0)
    } else {
        canonical_publication_path_with_work(authority, work)?
    };
    let sequence = location.revision - 1;
    let received = one_record_with_work(client, &canonical, sequence, work).await;
    drop(canonical);
    let record = release_owned_after_drop(received, path_owned, work)?
        .ok_or_else(|| message_lookup_error(work, crate::wire::ErrorCode::Storage,
            "indexed canonical event is missing"))?;
    let record_bytes = record.value.len() as u64;
    let result = (|| {
        let consumed_bytes = index_bytes.checked_add(record_bytes)
            .filter(|bytes| *bytes <= maximum_bytes)
            .ok_or_else(|| message_lookup_error(work, crate::wire::ErrorCode::Invalid,
                "operation lookup exceeds byte bound"))?;
        let (event, owned) = super::history::verify_history_record_with_retained_quota(
            verifier, authority, sequence, &record, work)?;
        if let Err(error) = verify_operation_binding_with_work(
            authority, index, location, &record, &event, work)
        {
            drop(event);
            return release_owned_after_drop(Err(error), owned, work);
        }
        Ok(((event, owned), consumed_bytes))
    })();
    let record_retained = sdk_record_retained_bytes(&record, work);
    drop(record);
    let released = match work.as_deref_mut() {
        Some(observer) => observer.release_record(record_retained),
        None => Ok(()),
    };
    match result {
        Ok(((event, owned), bytes)) => match released {
            Ok(()) => Ok(((event, owned), bytes)),
            Err(error) => {
                drop(event);
                release_owned_after_drop(Err(error), owned, work)
            }
        },
        Err(error) => Err(error),
    }
}

pub(super) fn verify_operation_binding(
    authority: &Authority,
    index: &Record,
    location: &OperationLocation,
    canonical: &Record,
    event: &Event,
) -> Result<()> {
    verify_operation_binding_with_work(authority, index, location, canonical, event, &mut None)
}

fn verify_operation_binding_with_work(
    authority: &Authority,
    index: &Record,
    location: &OperationLocation,
    canonical: &Record,
    event: &Event,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<()> {
    if &location.authority != authority
        || location.revision != event.revision
        || canonical.sequence.checked_add(1) != Some(event.revision)
        || event.operation_id != location.operation_id
        || event.intent_digest != location.intent_digest
        || canonical.commit_id != index.commit_id
    {
        return Err(message_lookup_error(work, crate::wire::ErrorCode::Storage,
            "operation location differs from its atomic canonical event"));
    }
    Ok(())
}

fn message_lookup_error(
    work: &Option<&mut dyn super::history::HistoryIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(original_native_decode_mode) {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match code {
        crate::wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        _ => Error::Storage(reason.into()),
    }
}

/// Loads one exact archived message without reconstructing a conversation projection.
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
    find_message_with_work(client, authority, verifier, message_id, through_revision,
        maximum_bytes, &mut None).await
}

pub(crate) async fn find_message_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    message_id: uuid::Uuid,
    through_revision: u64,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    verifier.verify_audience(authority)?;
    let (path, owned) = if work.is_none() {
        (message_path(authority, message_id)?, 0)
    } else {
        if authority.kind != crate::core::AggregateKind::Conversation {
            return Err(message_lookup_error(work, crate::wire::ErrorCode::Invalid,
                "message index requires a conversation"));
        }
        publication_path_with_work(authority, "harness/v2/conversation-messages/",
            Some((&message_id, 36)), work)?
    };
    let result = find_message_at_locator_with_work(client, authority, verifier, &path,
        Some(message_id), None, through_revision, maximum_bytes, work).await;
    drop(path);
    release_owned_after_drop(result, owned, work)
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
    verifier.verify_audience(authority)?;
    let (path, owned) = if work.is_none() {
        (message_sequence_path(authority, sequence)?, 0)
    } else {
        if sequence == 0 || authority.kind != crate::core::AggregateKind::Conversation {
            return Err(message_lookup_error(work, crate::wire::ErrorCode::Invalid,
                "message sequence index requires a positive conversation sequence"));
        }
        publication_path_with_work(authority, "harness/v2/conversation-sequences/",
            Some((&sequence, 20)), work)?
    };
    let result = find_message_at_locator_with_work(client, authority, verifier, &path,
        None, Some(sequence), through_revision, maximum_bytes, work).await;
    drop(path);
    release_owned_after_drop(result, owned, work)
}

#[allow(clippy::too_many_arguments, reason = "same exact indexed locator and pinned cut with optional SDK receipts")]
async fn find_message_at_locator_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    path: &StreamPath,
    expected_id: Option<uuid::Uuid>,
    expected_sequence: Option<u64>,
    through_revision: u64,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    verifier.verify_audience(authority)?;
    if maximum_bytes == 0 {
        return Err(message_lookup_error(work, crate::wire::ErrorCode::Invalid,
            "message lookup byte bound must be positive"));
    }
    let Some(index) = one_record_with_work(client, path, 0, work).await? else {
        return match tail_optional_with_work(client, path, work).await? {
            None => Ok((None, 0)),
            Some(_) => Err(message_lookup_error(work, crate::wire::ErrorCode::Storage,
                "message location is missing before its committed tail")),
        };
    };
    let index_bytes = index.value.len() as u64;
    let result = async {
        if index_bytes > maximum_bytes {
            return Err(message_lookup_error(work, crate::wire::ErrorCode::Invalid,
                "message lookup exceeds byte bound"));
        }
        if tail_with_work(client, path, work).await? != 1 {
            return Err(message_lookup_error(work, crate::wire::ErrorCode::Storage,
                "message location is not immutable"));
        }
        verify_message_locator_with_work(client, authority, verifier, &index,
            expected_id, expected_sequence, through_revision, maximum_bytes, work).await
    }.await;
    let index_retained = sdk_record_retained_bytes(&index, work);
    drop(index);
    let released = match work.as_deref_mut() {
        Some(observer) => observer.release_record(index_retained),
        None => Ok(()),
    };
    match result {
        Ok(value) => released.map(|()| value),
        Err(error) => Err(error),
    }
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
    verify_message_locator_with_work(client, authority, verifier, &index, expected_id,
        expected_sequence, through_revision, maximum_bytes, &mut None).await
}

#[allow(clippy::too_many_arguments, reason = "shared original atomic locator verification plus optional receipts")]
async fn verify_message_locator_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    index: &Record,
    expected_id: Option<uuid::Uuid>,
    expected_sequence: Option<u64>,
    through_revision: u64,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    let location = decode_message_location_with_work(&index.value, work)?;
    let location_bytes = location.location.authority.id.capacity() as u64;
    let result = async {
        if &location.location.authority != authority
            || expected_id.is_some_and(|id| location.message_id != id)
            || expected_sequence.is_some_and(|sequence| location.sequence != sequence)
            || location.location.revision == 0
            || (location.sequence == 0 && expected_sequence != Some(0))
        {
            return Err(message_lookup_error(work, crate::wire::ErrorCode::Storage,
                "message location identity is invalid"));
        }
        let (event, consumed_bytes) = verify_operation_locator_with_work(
            client, authority, verifier, index, &location.location, maximum_bytes, work).await?;
        let owned = if work.is_some() { crate::core::event_clone_reservation(&event)? } else { 0 };
        let selected = (|| {
            if let Some(observer) = work.as_deref_mut() { observer.check_cancellation()?; }
            if location.sequence == 0 {
                if !location.message_id.is_nil()
                    || !matches!(&event.payload, crate::core::EventPayload::ConversationBound { .. })
                {
                    return Err(message_lookup_error(work, crate::wire::ErrorCode::Storage,
                        "empty message head differs from its canonical binding"));
                }
                return Ok(false);
            }
            let message = match &event.payload {
                crate::core::EventPayload::ConversationMessageAppended { message } => message.as_ref(),
                crate::core::EventPayload::ProjectMergePublished { receipt } => &receipt.notice,
                _ => return Err(message_lookup_error(work, crate::wire::ErrorCode::Storage,
                    "message location points to another event kind")),
            };
            if message.id != location.message_id || message.sequence != location.sequence {
                return Err(message_lookup_error(work, crate::wire::ErrorCode::Storage,
                    "message location differs from its canonical message"));
            }
            if event.revision > through_revision { return Ok(false); }
            if let Some(observer) = work.as_deref_mut() {
                observer.check_cancellation()?;
                observer.stage_output(consumed_bytes - index.value.len() as u64, 1)?;
            }
            Ok(true)
        })();
        match selected {
            Ok(true) => {
                let message = match event.payload {
                    crate::core::EventPayload::ConversationMessageAppended { message } => *message,
                    crate::core::EventPayload::ProjectMergePublished { receipt } => receipt.notice,
                    _ => unreachable!("selected canonical message payload"),
                };
                drop(event.scope);
                drop(event.causal_parent);
                Ok((Some(message), consumed_bytes))
            }
            selected => {
                drop(event);
                release_owned_after_drop(selected.map(|_| (None, consumed_bytes)), owned, work)
            }
        }
    }.await;
    drop(location);
    release_owned_after_drop(result, location_bytes, work)
}

/// Captures one logical message head without a reducer or retained-history scan.
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
    async fn native_message_lookup_provider_corruption_bound_and_cancellation_release_records() -> Result<()> {
        let authority = Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: "native-message-lookup".into(),
        };
        let issuer = crate::core::AuthorityIssuer::new("message-lookup", [9; 32], authority.clone());
        let message_id = uuid::Uuid::from_bytes([11; 16]);
        let path = message_path(&authority, message_id)?;
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let outcome = client.commit(CommitRequest {
            conditions: vec![CommitCondition::Absent { path: path.clone() }],
            mutations: vec![CommitMutation::Append {
                path, records: vec![OwnedBytes::from_static(b"{{")],
            }],
            idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"native-corrupt-message"))?,
        }).await?;
        assert!(matches!(outcome, acyclic_stream::CommitOutcome::Committed(_)));
        drop(outcome);
        for mode in 0..4 {
            let cancellation = CancellationToken::new();
            let mut allowance = budget();
            if mode == 3 { allowance.source_bytes_read = 0; }
            let native = NativeMaterializationWork::new(allowance, &cancellation)
                .expect("original message query ledger");
            let mut io = native_work_io(&native).expect("original query custody");
            if mode == 2 { cancellation.cancel(); }
            let error = find_message_with_work(&client, &authority, &issuer.verifier(),
                message_id, 1, if mode == 1 { 1 } else { 1_048_576 },
                &mut Some(&mut io)).await.expect_err("original provider record is corrupt or denied");
            match mode {
                0 => assert!(matches!(&error, Error::NativeDecode { .. })),
                1 => assert!(matches!(&error, Error::NativeBoundary { .. })),
                2 => assert!(matches!(&error, Error::NativeWork {
                    cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
                })),
                _ => assert!(matches!(&error, Error::NativeWork { .. })),
            }
            drop(error);
            assert_eq!(io.into_custody().bytes(), 0);
        }
        let missing_id = uuid::Uuid::from_bytes([12; 16]);
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original missing query ledger");
        let mut io = native_work_io(&native).expect("original query custody");
        let absent = find_message_with_work(&client, &authority, &issuer.verifier(),
            missing_id, 1, 1_048_576, &mut Some(&mut io)).await?;
        assert_eq!(absent, (None, 0));
        assert_eq!(io.into_custody().bytes(), 0);

        let location = MessageLocation {
            location: OperationLocation {
                authority: authority.clone(),
                operation_id: OperationId::from_bytes([13; 16]),
                revision: 1,
                intent_digest: [0; 32],
                effect_position: None,
            },
            message_id: missing_id,
            sequence: 1,
        };
        let path = message_path(&authority, missing_id)?;
        client.commit(CommitRequest {
            conditions: vec![CommitCondition::Absent { path: path.clone() }],
            mutations: vec![CommitMutation::Append {
                path,
                records: vec![OwnedBytes::from(crate::contract::canonical_json_bytes(&location)?)],
            }],
            idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"native-missing-canonical"))?,
        }).await?;
        let native = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original missing canonical ledger");
        let mut io = native_work_io(&native).expect("original query custody");
        let error = find_message_with_work(&client, &authority, &issuer.verifier(),
            missing_id, 1, 1_048_576, &mut Some(&mut io)).await
            .expect_err("original canonical stream is absent");
        assert!(matches!(&error, Error::NativeBoundary {
            reason: "indexed canonical event is missing", ..
        }));
        drop(error);
        assert_eq!(io.into_custody().bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn foreign_cursor_record_metadata_is_rejected_without_byte_rebinding() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let path = StreamPath::new("harness/v3/foreign-record-metadata")?;
        client.stream(path.as_str())?.append_at(OwnedBytes::from_static(b"record"), 0).await?;
        let cancellation = CancellationToken::new();
        let producer = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("genuine native record producer");
        let receiver = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("different receiver ledger");
        let mut records = native_records_with_work(&client, &path, 0, 1, &producer)
            .await?.map_err(|error| error.into_error(&None))?;
        let mut io = native_work_io(&receiver).expect("receiver custody");
        let error = records.next(&mut Some(&mut io)).await
            .expect_err("foreign SDK output metadata is not this call's custody");
        assert!(matches!(error, Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout, custody: None,
        }));
        assert_eq!(receiver.receipt().authority_records_read, 1,
            "receiver prefix is recorded before rejection or decode");
        drop(records);
        assert_eq!(io.into_custody().bytes(), 0);
        assert_eq!(producer.live_allocation_bytes(), 0);
        assert_eq!(receiver.live_allocation_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn ordinary_vector_capacity_remains_external_through_request_and_ack_drop() -> Result<()> {
        let cancellation = CancellationToken::new();
        let allowance = budget();
        let mut io = super::super::history::HistoryWork::new(allowance, &cancellation);
        let mut data = Vec::new();
        io.reserve_bytes(&mut data, 128)?;
        data.extend_from_slice(b"ordinary-carrier");
        let capacity = data.capacity() as u64;
        let pointer = data.as_ptr();
        let (bytes, external) = owned_bytes_from_retained_vec(data, &mut Some(&mut io))?;
        assert_eq!(external, capacity);
        assert_eq!(bytes.as_ptr(), pointer);
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let path = StreamPath::new("harness/v3/ordinary-record-custody")?;
        let request = CommitRequest {
            conditions: vec![CommitCondition::Absent { path: path.clone() }],
            mutations: vec![CommitMutation::Append { path, records: vec![bytes.clone()] }],
            idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"ordinary-custody"))?,
        };
        drop(bytes);
        assert_eq!(io.remaining()?.peak_allocation_bytes, allowance.peak_allocation_bytes - capacity);
        let acyclic_stream::CommitOutcome::Committed(ack) = client.commit(request).await? else {
            panic!("ordinary source publication");
        };
        let CommittedMutation::Append(append) = &ack.mutations[0] else {
            panic!("actual SDK append receipt");
        };
        assert_eq!(append.records[0].value.as_ptr(), pointer);
        assert_eq!(io.remaining()?.peak_allocation_bytes, allowance.peak_allocation_bytes - capacity);
        drop(ack);
        io.release_owned(external)?;
        assert_eq!(io.remaining()?.peak_allocation_bytes, allowance.peak_allocation_bytes);
        Ok(())
    }

    #[test]
    fn original_vector_custody_survives_empty_alias_until_last_drop() -> Result<()> {
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original finite vector ledger");
        let mut io = native_work_io(&native).expect("original vector custody");
        let mut data = Vec::new();
        io.reserve_bytes(&mut data, 128)?;
        data.extend_from_slice(b"original-carrier");
        let capacity = data.capacity() as u64;
        let pointer = data.as_ptr();
        let (bytes, external) = owned_bytes_from_retained_vec(data, &mut Some(&mut io))?;
        assert_eq!(external, 0);
        assert_eq!(bytes.as_ptr(), pointer, "original allocation is never copied");
        let alias = bytes.clone();
        let empty = bytes.slice(0..0);
        assert_eq!(io.into_custody().bytes(), 0);
        drop(bytes);
        assert!(native.live_allocation_bytes() >= capacity);
        drop(alias);
        assert!(native.live_allocation_bytes() >= capacity);
        assert!(empty.is_empty());
        assert_eq!(empty.retained_capacity(), Some(capacity as usize));
        drop(empty);
        assert_eq!(native.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn original_vector_factory_failure_drops_payload_and_existing_claim() -> Result<()> {
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original finite vector ledger");
        let mut io = native_work_io(&native).expect("original vector custody");
        let mut data = Vec::new();
        io.reserve_bytes(&mut data, 128)?;
        data.extend_from_slice(b"original-carrier");
        assert!(native.live_allocation_bytes() >= data.capacity() as u64);
        cancellation.cancel();
        let error = owned_bytes_from_retained_vec(data, &mut Some(&mut io))
            .expect_err("original metadata allocation observes cancellation");
        assert!(matches!(error, Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None,
        }));
        assert_eq!(io.into_custody().bytes(), 0);
        assert_eq!(native.live_allocation_bytes(), 0);
        Ok(())
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
        assert_eq!(publication.bytes.retained_capacity(), Some(original_capacity as usize));
        assert!(publication.bytes.matches_meter(&native.runtime_meter_owner()));
        assert!(publication.indexes.iter().all(|(_, _, bytes)|
            bytes.matches_meter(&native.runtime_meter_owner())));
        let old_snapshot = publication.indexes[0].2.clone();
        let old_snapshot_capacity = old_snapshot.retained_capacity().expect("guarded original index");
        publication.set_effect_position_with_work(3, &mut work)?;
        let retained = publication.retained_owned();
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let request = publication.request(&client,
            IdempotencyKey::new(OwnedBytes::from_static(b"publication-custody"))?).await?;
        let acyclic_stream::CommitOutcome::Committed(mut envelope) = client.commit(request).await? else {
            panic!("original absent/Tail atomic publication");
        };
        let retry = publication.request(&client,
            IdempotencyKey::new(OwnedBytes::from_static(b"publication-custody"))?).await?;
        let acyclic_stream::CommitOutcome::Committed(retried) = client.commit(retry).await? else {
            panic!("same original request is an idempotent publication");
        };
        assert_eq!(retried.commit_id, envelope.commit_id);
        publication.verify(&retried)?;
        drop(retried);

        let query_cancellation = CancellationToken::new();
        let query = NativeMaterializationWork::new(budget(), &query_cancellation)
            .expect("distinct native cache reader");
        let mut query_io = native_work_io(&query).expect("query metadata custody");
        let cached = one_record_with_work(&client, &publication.path, 0,
            &mut Some(&mut query_io)).await?.expect("real provider cached event");
        assert_eq!(cached.value, publication.bytes);
        assert!(cached.value.matches_meter(&native.runtime_meter_owner()));
        assert!(!cached.value.matches_meter(&query.runtime_meter_owner()));
        assert_eq!(query.receipt().authority_records_read, 1);
        drop(cached);
        assert_eq!(query_io.into_custody().bytes(), 0);
        assert_eq!(query.live_allocation_bytes(), 0);
        publication.verify(&envelope)?;
        publication.verify_with_work(&envelope, &mut work)?;
        let mut replay_io = native_work_io(&native).expect("same original replay decoder");
        let (replayed, consumed) = bounded_operation_with_retained_quota(
            &client, &authority, &issuer.verifier(), event.operation_id,
            2 * acyclic_stream::MAX_RECORD_BYTES as u64, false, &mut Some(&mut replay_io),
        ).await?;
        let (replayed, replay_paid) = replayed.expect("actual immutable locator resolves original event");
        assert_eq!(replayed, event);
        assert!(consumed > publication.bytes.len() as u64);
        let mut replay_custody = replay_io.into_custody();
        assert_eq!(replay_custody.bytes(), replay_paid);
        drop(replayed);
        replay_custody.release(replay_paid).expect("original decoded event physically dropped");
        assert_eq!(replay_custody.bytes(), 0);
        if let CommittedMutation::Append(append) = &mut envelope.mutations[0] {
            append.start += 1;
        }
        assert!(matches!(publication.verify_with_work(&envelope, &mut work),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage,
                reason: "indexed publication append is invalid" })));
        if let CommittedMutation::Append(append) = &mut envelope.mutations[0] {
            append.start -= 1;
        }
        let key = super::super::stream_idempotency_key_with_work(
            publication.path.as_str(), &command.idempotency_key, &mut work)?;
        cancellation.cancel();
        let denied = publication.request_with_work(&client, key, &mut work).await
            .err().expect("cancellation before original SDK bounds");
        assert!(matches!(denied, Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None,
        }));
        assert!(matches!(publication.verify_with_work(&envelope, &mut work),
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None,
            })));
        drop(work);
        let mut custody = io.into_custody();
        assert_eq!(custody.bytes(), retained);
        drop(envelope);
        assert_eq!(publication.into_retained_owned(), retained);
        custody.release(retained).expect("physically dropped source publication");
        assert_eq!(custody.bytes(), 0);
        assert!(native.live_allocation_bytes() >= original_capacity);
        drop(client);
        assert!(native.live_allocation_bytes() >= old_snapshot_capacity as u64);
        drop(old_snapshot);
        assert_eq!(native.live_allocation_bytes(), 0);
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

#[cfg(all(test, feature = "native-execution", not(target_arch = "wasm32")))]
mod native_only_provider_tests {
    use super::*;
    use acyclic_stream::{
        AppendOutcome, AppendRequest, ChildStream, ChildrenRequest, CommitId, CommitOutcome,
        ForkReceipt, ForkRequest, IdempotencyObservation, NativeProviderFuture, NativeRecordStream,
        ReadRequest, RecordStream, StreamBounds, StreamError,
    };
    use acyclic_native_runtime::NativeReadMeter;
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    use std::sync::Arc;

    struct NativeOnly<P>(Arc<P>);

    #[async_trait::async_trait]
    impl<P: StreamProvider> StreamProvider for NativeOnly<P> {
        async fn inspect_idempotency(&self, _: IdempotencyKey)
            -> std::result::Result<Option<IdempotencyObservation>, StreamError> {
            panic!("ordinary inspection on a native-only provider")
        }
        async fn tail(&self, _: StreamPath) -> std::result::Result<u64, StreamError> {
            panic!("ordinary tail on a native-only provider")
        }
        async fn bounds(&self, _: StreamPath) -> std::result::Result<StreamBounds, StreamError> {
            panic!("ordinary bounds on a native-only provider")
        }
        async fn append(&self, _: AppendRequest) -> std::result::Result<AppendOutcome, StreamError> {
            panic!("ordinary append on a native-only provider")
        }
        async fn fork(&self, _: ForkRequest) -> std::result::Result<ForkReceipt, StreamError> {
            panic!("ordinary fork on a native-only provider")
        }
        async fn read(&self, _: ReadRequest) -> std::result::Result<RecordStream, StreamError> {
            panic!("ordinary read on a native-only provider")
        }
        async fn follow(&self, _: StreamPath, _: u64) -> std::result::Result<RecordStream, StreamError> {
            panic!("ordinary follow on a native-only provider")
        }
        async fn children(&self, _: ChildrenRequest) -> std::result::Result<ChildStream, StreamError> {
            panic!("ordinary children on a native-only provider")
        }
        async fn commit(&self, _: CommitRequest) -> std::result::Result<CommitOutcome, StreamError> {
            panic!("ordinary commit on a native-only provider")
        }
        async fn read_commit(&self, _: CommitId) -> std::result::Result<CommittedEnvelope, StreamError> {
            panic!("ordinary envelope read on a native-only provider")
        }
        fn native_tail(&self, path: StreamPath, meter: Arc<dyn NativeReadMeter>)
            -> NativeProviderFuture<'_, u64> {
            self.0.native_tail(path, meter)
        }
        fn native_bounds(&self, path: StreamPath, meter: Arc<dyn NativeReadMeter>)
            -> NativeProviderFuture<'_, StreamBounds> {
            self.0.native_bounds(path, meter)
        }
        fn native_read(&self, request: ReadRequest, meter: Arc<dyn NativeReadMeter>)
            -> NativeProviderFuture<'_, NativeRecordStream> {
            self.0.native_read(request, meter)
        }
        fn native_commit(&self, request: CommitRequest, meter: Arc<dyn NativeReadMeter>)
            -> NativeProviderFuture<'_, CommitOutcome> {
            self.0.native_commit(request, meter)
        }
    }

    fn budget() -> WorkBudget {
        WorkBudget {
            authority_records_read: 32, authority_bytes_read: 1_048_576,
            backend_read_operations: 32, source_bytes_read: 1_048_576,
            bytes_copied: 1_048_576, bytes_encoded: 1_048_576, bytes_hashed: 1_048_576,
            authority_records_appended: 32, authority_bytes_written: 1_048_576,
            backend_write_operations: 32, items_examined: 1_048_576,
            allocation_operations: 1_048_576, peak_allocation_bytes: 1_048_576,
            ..WorkBudget::default()
        }
    }

    async fn exercise<P: StreamProvider>(provider: Arc<P>) -> Result<()> {
        use crate::core::{Action, AggregateKind, ApplyResult, AuthorityIssuer, Command,
            LifecycleState, Reducer, SchemaRegistry};
        use super::super::history::{HistoryIo, HistoryReader, HistoryReadLimits, native_work_io};
        let ordinary = StreamClient::new(provider.clone());
        let authority = Authority { kind: AggregateKind::Agent, id: "native-only-reader".into() };
        let issuer = AuthorityIssuer::new("native-only", [29; 32], authority.clone());
        let reducer = Reducer::new(authority.clone(), issuer.verifier(), SchemaRegistry::new());
        let command = Command {
            operation_id: OperationId::from_bytes([31; 16]),
            idempotency_key: crate::IdempotencyKey("native-only-reader".into()),
            expected_revision: 0,
            scope: issuer.root("root", crate::Capabilities::new(["lifecycle:manage"])),
            causal_parent: None,
            action: Action::TransitionLifecycle { to: LifecycleState::Active, reason: None },
        };
        let ApplyResult::Applied { event } = reducer.plan(&command)? else {
            panic!("fresh lifecycle event")
        };
        let encoded = crate::wire_codec::encode_event(&authority, &event)?;
        let event_bytes = encoded.len() as u64;
        let publication = IndexedPublication::new(&authority, &event, encoded)?;
        let request = publication.request(&ordinary,
            IdempotencyKey::new(OwnedBytes::from_static(b"native-only-reader"))?).await?;
        assert!(matches!(ordinary.commit(request).await?, CommitOutcome::Committed(_)));
        let corrupt_message = uuid::Uuid::from_bytes([37; 16]);
        let corrupt_path = message_path(&authority, corrupt_message)?;
        assert!(matches!(ordinary.commit(CommitRequest {
            conditions: vec![CommitCondition::Absent { path: corrupt_path.clone() }],
            mutations: vec![CommitMutation::Append {
                path: corrupt_path, records: vec![OwnedBytes::from_static(b"{{")],
            }],
            idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"native-only-corrupt"))?,
        }).await?, CommitOutcome::Committed(_)));
        let client = StreamClient::new(Arc::new(NativeOnly(provider)));
        let reader = HistoryReader::new(&client, &authority, issuer.verifier())?;
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("finite original reader work");
        let mut io = native_work_io(&native).expect("original reader observer");
        let cursor = reader.pin_with_work(0, Some(&mut io)).await?;
        assert_eq!(cursor.through_revision, 1);
        let page = reader.read_page_with_work(&cursor,
            HistoryReadLimits { maximum_events: 1, maximum_bytes: event_bytes }, true,
            Some(&mut io)).await?;
        assert_eq!(page.page.events.as_slice(), std::slice::from_ref(&event));
        assert_eq!(page.records.len(), 1);
        assert_eq!(native.receipt().authority_records_read, 1);
        assert_eq!(native.receipt().authority_bytes_read, event_bytes);
        assert_eq!(native.receipt().source_bytes_read, event_bytes);
        assert_eq!(native.receipt().backend_read_operations, 3,
            "pin tail, page tail, and finite read each have one SDK boundary");
        let mut work: Option<&mut dyn HistoryIo> = Some(&mut io);
        let bounds = bounds_with_work(&client, &publication.path, &mut work).await?
            .map_err(|error| error.into_error(&work))?;
        assert_eq!(bounds.tail, 1);
        drop(page);
        drop(cursor);
        drop(work);
        drop(io.into_custody());

        let native = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original malformed locator work");
        let mut io = native_work_io(&native).expect("original malformed locator observer");
        let error = find_message_with_work(&client, &authority, &issuer.verifier(),
            corrupt_message, 1, 1_048_576, &mut Some(&mut io)).await
            .expect_err("real raw locator reaches the original guarded wire decoder");
        assert!(matches!(&error, Error::NativeDecode { custody: Some(custody), .. }
            if native.shares_ledger(custody.work())));
        assert_eq!(native.receipt().authority_records_read, 1);
        assert_eq!(native.receipt().authority_bytes_read, 2);
        drop(error);
        assert_eq!(io.into_custody().bytes(), 0);

        let native = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original checkpoint commit work");
        let mut io = native_work_io(&native).expect("original checkpoint observer");
        super::super::checkpoints::stage_immutable_record_with_work(
            &client, StreamPath::new("harness/v3/native-only-checkpoint")?,
            OwnedBytes::from_static(b"native-checkpoint"), &mut Some(&mut io),
        ).await?;
        assert_eq!(native.receipt().authority_records_appended, 1);
        assert_eq!(io.into_custody().bytes(), 0);

        for maximum_bytes in [1, event_bytes] {
            let native = NativeMaterializationWork::new(budget(), &cancellation)
                .expect("separate finite denial work");
            let mut io = native_work_io(&native).expect("original denial observer");
            let cursor = reader.pin_with_work(0, Some(&mut io)).await?;
            if maximum_bytes == 1 {
                assert!(reader.read_page_with_work(&cursor,
                    HistoryReadLimits { maximum_events: 1, maximum_bytes }, false, Some(&mut io))
                    .await.is_err(), "raw encoded bound must reject before decode");
                assert_eq!(native.receipt().authority_records_read, 1);
            } else {
                cancellation.cancel();
                assert!(reader.read_page_with_work(&cursor,
                    HistoryReadLimits { maximum_events: 1, maximum_bytes }, false, Some(&mut io))
                    .await.is_err(), "native cancellation must fail before provider I/O");
            }
            drop(cursor);
            drop(io.into_custody());
        }
        Ok(())
    }

    #[tokio::test]
    async fn real_memory_reader_never_uses_ordinary_provider_methods() -> Result<()> {
        exercise(Arc::new(acyclic_stream::MemoryStream::default())).await
    }

    #[cfg(feature = "filesystem-local")]
    #[tokio::test]
    async fn real_local_reader_never_uses_ordinary_provider_methods() -> Result<()> {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let local = acyclic_stream::LocalStream::open(
            directory.path(), acyclic_stream::LocalStreamLimits::default(),
        ).await.map_err(|error| Error::Storage(error.to_string()))?;
        exercise(Arc::new(local)).await
    }
}
