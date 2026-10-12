//! Durable accelerators over the same authenticated reducer and canonical Stream.

use super::{HistoryReadLimits, history::HistoryIo, operations};
use crate::{
    Error, Result,
    core::{Authority, AuthorityVerifier, Event, Reducer, Snapshot},
};
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitOutcome, CommitRequest, CommittedMutation,
    IdempotencyKey, OwnedBytes, StreamClient, StreamError, StreamPath, StreamProvider,
};
use serde::{Deserialize, Serialize};

/// Default checkpoint interval and cold decoded-event allowance, including its anchor.
pub const DEFAULT_PROJECTION_EVENTS: u32 = 64;
const MAX_PROJECTION_BYTES: u64 = 16 * acyclic_stream::MAX_COMMAND_BYTES as u64;

/// Default finite cold input allowance; includes pointer, snapshot, anchor and suffix.
#[must_use]
pub const fn default_projection_read_limits() -> HistoryReadLimits {
    HistoryReadLimits {
        maximum_events: DEFAULT_PROJECTION_EVENTS,
        maximum_bytes: 32 * acyclic_stream::MAX_COMMAND_BYTES as u64,
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProjectionLocation {
    pub(super) authority: Authority,
    pub(super) revision: u64,
    pub(super) bytes: u64,
    pub(super) digest: [u8; 32],
    pub(super) chunks: Vec<ProjectionChunk>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProjectionChunk {
    pub(super) digest: [u8; 32],
    pub(super) bytes: u32,
}

fn projection_boundary(native: bool, code: crate::wire::ErrorCode, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if native { return Error::NativeBoundary { code, reason }; }
    let _ = native;
    match code {
        crate::wire::ErrorCode::Storage => Error::Storage(reason.into()),
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}

pub(super) fn decode_projection_location_with_work(
    bytes: &[u8], work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(ProjectionLocation, u64)> {
    let Some(observer) = work.as_deref_mut() else {
        return crate::contract::json_from_slice(bytes)
            .map(|location| (location, 0))
            .map_err(|error| Error::Storage(error.to_string()));
    };
    let native = operations::original_native_decode_mode(observer);
    observer.check_cancellation()?;
    let diagnostic_labels = ["ProjectionLocation", "ProjectionChunk", "Authority", "AggregateKind",
        "String", "Vec", "u8", "u32", "u64", "authority", "revision", "bytes", "digest",
        "chunks", "kind", "id", "agent", "conversation", "session", "turn", "task"]
        .iter().map(|label| label.len() as u64 + 4).sum();
    let mut reservation = 0;
    let mut observed_failure = false;
    let decoded = operations::decode_preclaimed_json_with_mode::<ProjectionLocation, _>(
        bytes,
        crate::contract::JsonBounds {
            maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
            maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64,
            maximum_depth: 256,
        },
        0,
        crate::wire::ErrorCode::Storage,
        native,
        crate::contract::JsonValueSchema::None,
        &mut |charge| {
            let result = if let crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes, value_shape } = charge {
                if value_shape.input_bytes() != bytes.len() as u64
                    || value_shape.virtual_items() != 0
                    || (native && value_shape.unresolved_raw())
                {
                    return Err(projection_boundary(native, crate::wire::ErrorCode::Storage,
                        "projection locator lexical witness differs"));
                }
                let diagnostic = crate::contract::json_typed_diagnostic_owned_bytes(
                    value_shape, diagnostic_labels, native.then_some(crate::wire::ErrorCode::Storage))?;
                let planned = value_shape.lexical_items().checked_mul(2)
                    .and_then(|slots| slots.checked_add(4))
                    .and_then(|slots| slots.checked_mul(std::mem::size_of::<ProjectionChunk>() as u64))
                    .and_then(|owned| owned.checked_add(value_shape.input_bytes()))
                    .and_then(|owned| owned.checked_add(std::mem::size_of::<ProjectionLocation>() as u64))
                    .and_then(|owned| owned.checked_add(maximum_owned_bytes))
                    .and_then(|owned| owned.checked_add(diagnostic))
                    .ok_or_else(|| projection_boundary(native, crate::wire::ErrorCode::Storage,
                        "projection locator reservation overflows"))?;
                observer.admit_copy(value_shape.input_bytes())
                    .map_err(|error| { observed_failure = true; error })?;
                observer.admit_owned(planned).map_err(|error| { observed_failure = true; error })?;
                observer.retain_owned(planned).map_err(|error| { observed_failure = true; error })?;
                reservation = planned;
                Ok(())
            } else {
                observer.codec_charge(charge)
            };
            if result.is_err() { observed_failure = true; }
            result
        },
    );
    let decoded = operations::retain_original_decode_error(decoded, &mut reservation, observer);
    let decoded = decoded.map_err(|error| match error {
        Error::Invalid(message) if !observed_failure => Error::Storage(message),
        other => other,
    });
    let location = match decoded {
        Ok(location) => location,
        Err(error) => {
            let _ = observer.release_owned(reservation);
            return Err(error);
        }
    };
    let owned = location.authority.id.capacity() as u64
        + location.chunks.capacity() as u64 * std::mem::size_of::<ProjectionChunk>() as u64
        + std::mem::size_of::<ProjectionLocation>() as u64;
    let result = observer.copy_bytes(location.authority.id.len() as u64)
        .and_then(|()| observer.release_owned(reservation - owned));
    match result {
        Ok(()) => Ok((location, owned)),
        Err(error) => {
            drop(location);
            let _ = observer.release_owned(reservation);
            Err(error)
        }
    }
}

pub(super) struct LoadedProjection {
    pub(super) snapshot: Option<Snapshot>,
    pub(super) through_revision: u64,
    pub(super) consumed_bytes: u64,
    pub(super) consumed_events: u32,
}

fn suffix(authority: &Authority) -> Result<String> {
    authority
        .stream_path()?
        .strip_prefix("harness/v2/")
        .map(str::to_owned)
        .ok_or_else(|| Error::Invalid("projection authority path is invalid".into()))
}

fn head_path(authority: &Authority) -> Result<StreamPath> {
    Ok(StreamPath::new(format!(
        "harness/v3/projection-checkpoints/{}",
        suffix(authority)?
    ))?)
}

fn projection_chunks<'a>(
    bytes: &'a [u8],
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Vec<&'a [u8]>> {
    let mut chunks = Vec::new();
    let result = (|| {
        let mut start = 0;
        let mut rolling = 0_u64;
        for (index, byte) in bytes.iter().enumerate() {
            let mut gear = u64::from(*byte).wrapping_add(0x9e37_79b9_7f4a_7c15);
            gear = (gear ^ (gear >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            gear = (gear ^ (gear >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            rolling = (rolling << 1).wrapping_add(gear ^ (gear >> 31));
            let length = index + 1 - start;
            if length == acyclic_stream::MAX_RECORD_BYTES
                || (length >= 16 * 1024 && rolling & 0x3fff == 0)
            {
                reserve_chunk_slot(&mut chunks, bytes.len() / (16 * 1024) + 1, work)?;
                chunks.push(bytes.get(start..=index)
                    .ok_or_else(|| Error::Invalid("projection chunk boundary is invalid".into()))?);
                start = index + 1;
            }
        }
        if start < bytes.len() {
            reserve_chunk_slot(&mut chunks, bytes.len() / (16 * 1024) + 1, work)?;
            chunks.push(bytes.get(start..)
                .ok_or_else(|| Error::Invalid("projection final chunk boundary is invalid".into()))?);
        }
        Ok(())
    })();
    match result {
        Ok(()) => Ok(chunks),
        Err(error) => {
            let owned = chunks.capacity() as u64 * std::mem::size_of::<&[u8]>() as u64;
            drop(chunks);
            operations::release_owned_after_drop(Err(error), owned, work)
        }
    }
}

fn reserve_chunk_slot<T>(
    values: &mut Vec<T>,
    maximum: usize,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    let Some(observer) = work.as_deref_mut() else { return Ok(()); };
    if values.len() < values.capacity() { return Ok(()); }
    let target = (values.capacity().max(1) * 2).min(maximum);
    let element_bytes = std::mem::size_of::<T>() as u64;
    let old_capacity = values.capacity();
    observer.admit_allocation((target - old_capacity) as u64 * element_bytes)?;
    values.try_reserve_exact(target - values.len())
        .map_err(|cause| observer.allocation_failure(cause))?;
    observer.retain_owned((values.capacity() - old_capacity) as u64 * element_bytes)?;
    Ok(())
}

fn chunk_path(authority: &Authority, chunk: &ProjectionChunk) -> Result<StreamPath> {
    Ok(StreamPath::new(format!(
        "harness/v3/projection-chunks/{}/{}",
        suffix(authority)?,
        blake3::Hash::from_bytes(chunk.digest).to_hex(),
    ))?)
}


/// Consumes the original encoder buffer and transfers native allocation custody into the SDK carrier before any request alias or authority write.
pub(super) async fn stage_immutable_vec_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    path: (StreamPath, u64),
    bytes: Result<Vec<u8>>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    let bytes = match bytes {
        Ok(bytes) => bytes,
        Err(error) => {
            let (path, retained) = path;
            drop(path);
            return operations::release_owned_after_drop(Err(error), retained, work);
        }
    };
    let (path, retained) = path;
    let (bytes, external_owned) = match operations::owned_bytes_from_retained_vec(bytes, work) {
        Ok(bytes) => bytes,
        Err(error) => {
            drop(path);
            return operations::release_owned_after_drop(Err(error), retained, work);
        }
    };
    let staged = stage_owned_immutable_record_with_work(
        client, path, bytes, retained, work).await;
    operations::release_owned_after_drop(staged, external_owned, work)
}

pub(super) async fn stage_immutable_record_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    path: StreamPath,
    bytes: OwnedBytes,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    stage_owned_immutable_record_with_work(client, path, bytes, 0, work).await
}

async fn stage_owned_immutable_record_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    path: StreamPath,
    bytes: OwnedBytes,
    path_retained: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    let mut retained = path_retained;
    let result = async {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
            if path_retained == 0 {
                observer.admit_owned(path.as_str().len() as u64)?;
                observer.retain_owned(path.as_str().len() as u64)?;
                retained += path.as_str().len() as u64;
            }
            observer.hashed_bytes(path.as_str().len() as u64)?;
        }
        let path_owned = retained;
        let digest = *blake3::hash(path.as_str().as_bytes()).as_bytes();
        let key = if let Some(observer) = work.as_deref_mut() {
            let mut key = Vec::new();
            observer.reserve_bytes(&mut key, 32)?;
            let copied = observer.copy_bytes(32);
            if let Err(error) = copied {
                let capacity = key.capacity() as u64;
                drop(key);
                return operations::release_owned_after_drop(Err(error), capacity, work);
            }
            key.extend_from_slice(&digest);
            let (key, external_owned) = operations::owned_bytes_from_retained_vec(key, work)?;
            retained += external_owned;
            IdempotencyKey::new(key)?
        } else {
            IdempotencyKey::new(OwnedBytes::copy_from_slice(&digest))?
        };
        let native = work.as_deref().is_some_and(operations::original_native_decode_mode);
        let (outcome, native_received) = if native {
            let key_owned = retained - path_owned;
            retained -= key_owned;
            let mut request = crate::distributed::PreparedCommitRequest::new(
                key, key_owned, work);
            request.push_condition(CommitCondition::Absent { path: path.clone() })?;
            request.append(path.clone(), bytes.clone())?;
            request.commit(client, None, None, None, None).await?
        } else {
            let mut conditions = if work.is_none() { Vec::with_capacity(1) } else { Vec::new() };
            reserve_chunk_slot(&mut conditions, 1, work)?;
            retained += conditions.capacity() as u64 * std::mem::size_of::<CommitCondition>() as u64;
            conditions.push(CommitCondition::Absent { path: path.clone() });
            let mut records = if work.is_none() { Vec::with_capacity(1) } else { Vec::new() };
            reserve_chunk_slot(&mut records, 1, work)?;
            retained += records.capacity() as u64 * std::mem::size_of::<OwnedBytes>() as u64;
            records.push(bytes.clone());
            let mut mutations = if work.is_none() { Vec::with_capacity(1) } else { Vec::new() };
            reserve_chunk_slot(&mut mutations, 1, work)?;
            retained += mutations.capacity() as u64 * std::mem::size_of::<CommitMutation>() as u64;
            mutations.push(CommitMutation::Append { path: path.clone(), records });
            let request = CommitRequest { conditions, mutations, idempotency_key: key };
            if let Some(observer) = work.as_deref_mut() {
                observer.admit_acknowledgment(bytes.len() as u64, 1)?;
                observer.begin_write()?;
            }
            let request_owned = retained - path_owned;
            let outcome = client.provider.commit(request).await.map_err(Error::from);
            let outcome = operations::release_owned_after_drop(outcome, request_owned, work);
            retained -= request_owned;
            (outcome?, 0)
        };
        if let CommitOutcome::Committed(envelope) = outcome {
            let mut received_failure = None;
            let mut received_bytes = 0_u64;
            let mut received_owned = native_received;
            if let Some(observer) = work.as_deref_mut().filter(|_| !native) {
                received_owned = envelope.mutations.capacity() as u64
                    * std::mem::size_of::<CommittedMutation>() as u64;
                received_failure = observer.retain_owned(received_owned).err();
                for mutation in &envelope.mutations {
                    let records = match mutation {
                        CommittedMutation::Append(append) => &append.records,
                        CommittedMutation::Fork(fork) => &fork.records,
                    };
                    let slots = records.capacity() as u64 * std::mem::size_of::<acyclic_stream::Record>() as u64;
                    received_owned += slots;
                    let received = observer.retain_owned(slots);
                    if received_failure.is_none() { received_failure = received.err(); }
                    for record in records {
                        received_bytes += record.value.len() as u64;
                        let received = observer.receive_record(record.value.len() as u64);
                        if received_failure.is_none() { received_failure = received.err(); }
                    }
                }
            }
            let valid = matches!(envelope.mutations.as_slice(),
                [CommittedMutation::Append(append)]
                    if append.path == path && append.start == 0 && append.end == 1
                        && append.tail == 1
                        && matches!(append.records.as_slice(), [record]
                            if record.sequence == 0 && record.value == bytes
                                && record.commit_id == envelope.commit_id));
            let verified = if let Some(error) = received_failure {
                Err(error)
            } else if valid {
                match work.as_deref_mut().filter(|_| !native) {
                    Some(observer) => observer.acknowledged(bytes.len() as u64, 1),
                    None => Ok(()),
                }
            } else {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if work.as_deref().is_some_and(operations::original_native_decode_mode) {
                    Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage,
                        reason: "immutable record receipt is invalid" })
                } else {
                    Err(Error::Storage("immutable record receipt is invalid".into()))
                }
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                { Err(Error::Storage("immutable record receipt is invalid".into())) }
            };
            drop(envelope);
            let released = match work.as_deref_mut() {
                Some(observer) => {
                    let records = observer.release_record(received_bytes);
                    let slots = observer.release_owned(received_owned);
                    records.and(slots)
                }
                None => Ok(()),
            };
            return match verified {
                Ok(()) => released,
                Err(error) => Err(error),
            };
        }
        drop(outcome);
        if let Some(observer) = work.as_deref_mut() { observer.release_owned(native_received)?; }
        let record = operations::one_record(client, &path, 0, work).await?;
        let identical = record.as_ref().is_some_and(|record| record.value == bytes);
        let received_bytes = record.as_ref().map_or(0, |record| operations::sdk_record_retained_bytes(record, work));
        drop(record);
        let verified = if identical { Ok(()) } else {
            Err(Error::Conflict("immutable record identity differs".into()))
        };
        let released = match work.as_deref_mut() {
            Some(observer) => observer.release_record(received_bytes),
            None => Ok(()),
        };
        match verified {
            Ok(()) => released?,
            Err(error) => return Err(error),
        }
        let tail = operations::tail_with_work(client, &path, work).await?;
        if tail == 1 { Ok(()) } else {
            Err(Error::Conflict("immutable record identity differs".into()))
        }
    }.await;
    drop(bytes);
    drop(path);
    operations::release_owned_after_drop(result, retained, work)
}


pub(super) async fn publication_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    reducer: &Reducer,
    event: &Event,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<operations::IndexedPublication> {
    let mut publication = operations::IndexedPublication::new_with_work(
        reducer.authority(),
        event,
        crate::wire_codec::encode_event(reducer.authority(), event, work)?,
        work,
    )?;
    let result = async {
        super::effects::add_publication_index_with_work(
            client, reducer, event, &mut publication, work).await?;
        if !event.revision.is_multiple_of(u64::from(DEFAULT_PROJECTION_EVENTS)) {
            return Ok(());
        }
        let (snapshot, snapshot_reservation) =
            reducer.publication_snapshot_with_work(event, 1, work)?;
        let encoded = super::history::canonical_bytes_with_work(&snapshot, MAX_PROJECTION_BYTES, work);
        drop(snapshot);
        let bytes = operations::release_owned_after_drop(encoded, snapshot_reservation, work)?;
        let source_capacity = bytes.capacity() as u64;
        let mut descriptors = Vec::new();
        let mut location = None;
        let mut manifest = Vec::new();
        let mut chunks = Vec::new();
        let staged = async {
            chunks = projection_chunks(&bytes, work)?;
            if let Some(observer) = work.as_deref_mut() {
                observer.hashed_bytes(bytes.len() as u64)?;
            }
            let digest = *blake3::hash(&bytes).as_bytes();
            for chunk in &chunks {
                reserve_chunk_slot(&mut descriptors, chunks.len(), work)?;
                let length = u32::try_from(chunk.len())
                    .map_err(|_| Error::Invalid("projection chunk exceeds platform".into()))?;
                if let Some(observer) = work.as_deref_mut() {
                    observer.hashed_bytes(chunk.len() as u64)?;
                }
                descriptors.push(ProjectionChunk {
                    digest: *blake3::hash(chunk).as_bytes(),
                    bytes: length,
                });
            }
            let authority = match work.as_deref_mut() {
                Some(observer) => observer.clone_authority(reducer.authority())?,
                None => reducer.authority().clone(),
            };
            location = Some(ProjectionLocation {
                authority,
                revision: event.revision,
                bytes: bytes.len() as u64,
                digest,
                chunks: std::mem::take(&mut descriptors),
            });
            let location = location.as_ref().expect("original projection location");
            manifest = super::history::canonical_bytes_with_work(
                location, acyclic_stream::MAX_RECORD_BYTES as u64, work)?;
            if manifest.len() > acyclic_stream::MAX_RECORD_BYTES {
                return Err(Error::Invalid("projection manifest exceeds one record".into()));
            }
            for (descriptor, chunk) in location.chunks.iter().zip(&chunks) {
                let path = if work.is_none() {
                    (chunk_path(&location.authority, descriptor)?, 0)
                } else {
                    let hash = blake3::Hash::from_bytes(descriptor.digest).to_hex();
                    operations::publication_path_with_work(
                        &location.authority, "harness/v3/projection-chunks/",
                        Some((&hash, hash.len())), work)?
                };
                let mut copied = Vec::new();
                let copied_result = (|| {
                    match work.as_deref_mut() {
                        Some(observer) => observer.reserve_bytes(&mut copied, chunk.len())?,
                        None => copied.reserve_exact(chunk.len()),
                    }
                    if let Some(observer) = work.as_deref_mut() {
                        observer.copy_bytes(chunk.len() as u64)?;
                    }
                    copied.extend_from_slice(chunk);
                    Ok(())
                })();
                let copied = match copied_result {
                    Ok(()) => Ok(copied),
                    Err(error) => {
                        let capacity = copied.capacity() as u64;
                        drop(copied);
                        operations::release_owned_after_drop(Err(error), capacity, work)
                    }
                };
                stage_immutable_vec_with_work(client, path, copied, work).await?;
            }
            let path = if work.is_none() {
                (head_path(reducer.authority())?, 0)
            } else {
                operations::publication_path_with_work(
                    reducer.authority(), "harness/v3/projection-checkpoints/", None, work)?
            };
            publication.add_derived_index_with_work(
                path,
                event.revision / u64::from(DEFAULT_PROJECTION_EVENTS) - 1,
                Ok(std::mem::take(&mut manifest)),
                work,
            )
        }.await;
        let location_owned = location.as_ref().map_or(0, |location| {
            location.authority.id.capacity() as u64
                + location.chunks.capacity() as u64 * std::mem::size_of::<ProjectionChunk>() as u64
        });
        let owned = source_capacity + manifest.capacity() as u64 + location_owned
            + chunks.capacity() as u64 * std::mem::size_of::<&[u8]>() as u64
            + descriptors.capacity() as u64 * std::mem::size_of::<ProjectionChunk>() as u64;
        drop(manifest);
        drop(location);
        drop(descriptors);
        drop(chunks);
        drop(bytes);
        operations::release_owned_after_drop(staged, owned, work)
    }.await;
    match result {
        Ok(()) => Ok(publication),
        Err(error) => {
            let owned = publication.into_retained_owned();
            operations::release_owned_after_drop(Err(error), owned, work)
        }
    }
}

async fn tail<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    if work.is_none() {
        let stream = client.stream(authority.stream_path()?)?;
        return match stream.tail().await {
            Ok(tail) => Ok(tail),
            Err(StreamError::NotFound) => Ok(0),
            Err(error) => Err(error.into()),
        };
    }
    let (path, retained) = operations::canonical_publication_path_with_work(authority, work)?;
    let result = operations::tail_optional_with_work(client, &path, work).await
        .map(|tail| tail.unwrap_or(0));
    drop(path);
    operations::release_owned_after_drop(result, retained, work)
}

fn projection_cut(
    committed_tail: u64, original_revision: Option<u64>, work: &Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    let through_revision = original_revision.unwrap_or(committed_tail);
    if through_revision > committed_tail {
        return Err(super::guarded_boundary(work, crate::wire::ErrorCode::Invalid,
            "projection revision exceeds committed tail"));
    }
    Ok(through_revision)
}

fn charge(used: &mut u64, bytes: u64, maximum: u64, work: &Option<&mut dyn HistoryIo>) -> Result<()> {
    *used = used
        .checked_add(bytes)
        .filter(|total| *total <= maximum)
        .ok_or_else(|| super::guarded_boundary(work, crate::wire::ErrorCode::Invalid,
            "projection read exceeds byte allowance"))?;
    Ok(())
}

pub(super) async fn supplied<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    snapshot: Snapshot,
    limits: HistoryReadLimits,
    original_revision: Option<u64>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<LoadedProjection> {
    if &snapshot.authority != authority {
        return Err(Error::Invalid(
            "snapshot authority does not match requested aggregate".into(),
        ));
    }
    let consumed_events = u32::try_from(snapshot.events.len())
        .ok()
        .filter(|count| *count <= limits.maximum_events)
        .ok_or_else(|| Error::Invalid("snapshot cache exceeds event allowance".into()))?;
    let consumed_bytes = crate::contract::json_byte_length(&snapshot, limits.maximum_bytes)?;
    Ok(LoadedProjection {
        snapshot: Some(snapshot),
        through_revision: projection_cut(tail(client, authority, work).await?, original_revision, work)?,
        consumed_bytes,
        consumed_events,
    })
}


pub(super) async fn load_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    limits: HistoryReadLimits,
    original_revision: Option<u64>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<LoadedProjection> {
    verifier.verify_audience_with_work(authority, work)?;
    if limits.maximum_events == 0 || limits.maximum_bytes == 0 {
        return Err(super::guarded_boundary(work, crate::wire::ErrorCode::Invalid,
            "projection read bounds must be positive"));
    }
    let through_revision = projection_cut(tail(client, authority, work).await?, original_revision, work)?;
    let checkpoint_count = through_revision / u64::from(DEFAULT_PROJECTION_EVENTS);
    if checkpoint_count == 0 {
        return Ok(LoadedProjection {
            snapshot: None,
            through_revision,
            consumed_bytes: 0,
            consumed_events: 0,
        });
    }
    let (path, path_owned) = operations::publication_path_with_work(
        authority, "harness/v3/projection-checkpoints/", None, work)?;
    let index = operations::one_record(client, &path, checkpoint_count - 1, work).await;
    drop(path);
    let index = operations::release_owned_after_drop(index, path_owned, work)?
        .ok_or_else(|| super::guarded_boundary(work, crate::wire::ErrorCode::Unsupported,
            "canonical history requires a durable projection checkpoint"))?;
    let result = async {
        let mut consumed_bytes = 0;
        charge(&mut consumed_bytes, index.value.len() as u64, limits.maximum_bytes, work)?;
        let (location, location_owned) = decode_projection_location_with_work(&index.value, work)?;
        let result = async {
            if &location.authority != authority
                || location.revision
                    != checkpoint_count
                        .checked_mul(u64::from(DEFAULT_PROJECTION_EVENTS))
                        .ok_or_else(|| super::guarded_boundary(work, crate::wire::ErrorCode::Storage,
                            "projection head revision overflows"))?
                || location.revision > through_revision
                || location.bytes == 0
                || location.bytes > MAX_PROJECTION_BYTES
                || through_revision - location.revision >= u64::from(limits.maximum_events)
            {
                return Err(super::guarded_boundary(work, crate::wire::ErrorCode::Invalid,
                    "projection checkpoint boundary exceeds read allowance"));
            }
            if location.bytes > limits.maximum_bytes.saturating_sub(consumed_bytes) {
                return Err(super::guarded_boundary(work, crate::wire::ErrorCode::Invalid,
                    "projection read exceeds byte allowance"));
            }
            let snapshot = location.load_snapshot(
                client, verifier, &index.commit_id, &mut consumed_bytes, limits.maximum_bytes, work,
            ).await?;
            Ok(LoadedProjection {
                snapshot: Some(snapshot), through_revision, consumed_bytes, consumed_events: 1,
            })
        }.await;
        drop(location);
        operations::release_owned_after_drop(result, location_owned, work)
    }.await;
    let index_bytes = operations::sdk_record_retained_bytes(&index, work);
    drop(index);
    let released = match work.as_deref_mut() {
        Some(observer) => observer.release_record(index_bytes),
        None => Ok(()),
    };
    match result {
        Ok(value) => released.map(|()| value),
        Err(error) => Err(error),
    }
}

impl ProjectionLocation {
    async fn load_snapshot<P: StreamProvider>(
        &self,
        client: &StreamClient<P>,
        verifier: &AuthorityVerifier,
        commit_id: &acyclic_stream::CommitId,
        consumed_bytes: &mut u64,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Snapshot> {
        let (path, path_owned) = operations::canonical_publication_path_with_work(&self.authority, work)?;
        let canonical = operations::one_record(client, &path, self.revision - 1, work).await;
        drop(path);
        let canonical = operations::release_owned_after_drop(canonical, path_owned, work)?
            .ok_or_else(|| super::guarded_boundary(work, crate::wire::ErrorCode::Storage,
                "projection canonical anchor is missing"))?;
        let result = async {
            charge(consumed_bytes, canonical.value.len() as u64, maximum_bytes, work)?;
            if &canonical.commit_id != commit_id {
                return Err(super::guarded_boundary(work, crate::wire::ErrorCode::Storage,
                    "projection head is outside its canonical atomic commit"));
            }
            let ((actual_authority, event), event_retained) =
                crate::wire_codec::decode_event(&canonical.value, work)?;
            let authority_owned = if work.is_some() { actual_authority.id.capacity() as u64 } else { 0 };
            let result = async {
                verifier.verify_event(&event, work)?;
                if actual_authority != self.authority || event.revision != self.revision {
                    return Err(super::guarded_boundary(work, crate::wire::ErrorCode::Storage,
                        "projection canonical anchor differs"));
                }
                if let Some(observer) = work.as_deref_mut() { observer.stage_output(self.bytes, 1)?; }
                let bytes = load_chunks(client, self, consumed_bytes, maximum_bytes, work).await?;
                let result = (|| {
                    let snapshot = Snapshot::decode_with_work(&bytes, maximum_bytes, work)?;
                    if snapshot.authority != self.authority
                        || snapshot.revision != self.revision
                        || snapshot.events.last() != Some(&event)
                    {
                        return Err(super::guarded_boundary(work, crate::wire::ErrorCode::Storage,
                            "projection snapshot differs from its canonical anchor"));
                    }
                    if let Some(observer) = work.as_deref_mut() { observer.check_cancellation()?; }
                    Ok(snapshot)
                })();
                let capacity = bytes.capacity() as u64;
                drop(bytes);
                let released = match work.as_deref_mut() {
                    Some(observer) => observer.release_bytes(capacity),
                    None => Ok(()),
                };
                match result {
                    Ok(snapshot) => released.map(|()| snapshot),
                    Err(error) => Err(error),
                }
            }.await;
            drop(event);
            drop(actual_authority);
            let result = operations::release_owned_after_drop(result, event_retained, work);
            operations::release_owned_after_drop(result, authority_owned, work)
        }.await;
        let canonical_bytes = operations::sdk_record_retained_bytes(&canonical, work);
        drop(canonical);
        let released = match work.as_deref_mut() {
            Some(observer) => observer.release_record(canonical_bytes),
            None => Ok(()),
        };
        match result {
            Ok(snapshot) => released.map(|()| snapshot),
            Err(error) => Err(error),
        }
    }
}

async fn load_chunks<P: StreamProvider>(
    client: &StreamClient<P>,
    location: &ProjectionLocation,
    consumed: &mut u64,
    maximum: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Vec<u8>> {
    let length = usize::try_from(location.bytes)
        .map_err(|_| super::guarded_boundary(work, crate::wire::ErrorCode::Invalid,
            "projection size exceeds platform"))?;
    let mut total = 0_u64;
    if location.chunks.is_empty() {
        return Err(super::guarded_boundary(work, crate::wire::ErrorCode::Storage,
            "projection chunk manifest is empty"));
    }
    for (index, chunk) in location.chunks.iter().enumerate() {
        if let Some(observer) = work.as_deref_mut() { observer.check_cancellation()?; }
        if chunk.bytes == 0
            || chunk.bytes as usize > acyclic_stream::MAX_RECORD_BYTES
            || (index + 1 < location.chunks.len() && chunk.bytes < 16 * 1024)
        {
            return Err(super::guarded_boundary(work, crate::wire::ErrorCode::Storage,
                "projection chunk manifest has invalid lengths"));
        }
        total = total
            .checked_add(u64::from(chunk.bytes))
            .ok_or_else(|| super::guarded_boundary(work, crate::wire::ErrorCode::Storage,
                "projection chunk manifest overflows"))?;
    }
    if total != location.bytes {
        return Err(super::guarded_boundary(work, crate::wire::ErrorCode::Storage,
            "projection chunk manifest total differs"));
    }
    let mut bytes = match work.as_deref_mut() {
        Some(work) => {
            let mut bytes = Vec::new();
            work.reserve_bytes(&mut bytes, length)?;
            bytes
        }
        None => Vec::with_capacity(length),
    };
    let result = async {
        for chunk in &location.chunks {
            let expected = chunk.bytes as usize;
            if expected as u64 > maximum.saturating_sub(*consumed) {
                return Err(super::guarded_boundary(work, crate::wire::ErrorCode::Invalid,
                    "projection read exceeds byte allowance"));
            }
            let digest = blake3::Hash::from_bytes(chunk.digest);
            let (path, path_owned) = operations::publication_path_with_work(
                &location.authority, "harness/v3/projection-chunks/", Some((&digest.to_hex(), 64)), work)?;
            let record = operations::one_record(client, &path, 0, work).await;
            drop(path);
            let record = operations::release_owned_after_drop(record, path_owned, work)?
                .ok_or_else(|| super::guarded_boundary(work, crate::wire::ErrorCode::Storage,
                    "projection chunk is missing"))?;
            let result = (|| {
                charge(consumed, record.value.len() as u64, maximum, work)?;
                let digest_matches = if record.value.len() == expected {
                    if let Some(observer) = work.as_deref_mut() { observer.hashed_bytes(record.value.len() as u64)?; }
                    blake3::hash(&record.value).as_bytes() == &chunk.digest
                } else { false };
                if !digest_matches {
                    return Err(super::guarded_boundary(work, crate::wire::ErrorCode::Storage,
                        "projection chunk length or digest differs"));
                }
                if let Some(observer) = work.as_deref_mut() { observer.copy_bytes(record.value.len() as u64)?; }
                bytes.extend_from_slice(&record.value);
                Ok(())
            })();
            let record_bytes = operations::sdk_record_retained_bytes(&record, work);
            drop(record);
            let released = match work.as_deref_mut() {
                Some(observer) => observer.release_record(record_bytes),
                None => Ok(()),
            };
            match result {
                Ok(()) => released?,
                Err(error) => return Err(error),
            }
        }
        if let Some(observer) = work.as_deref_mut() { observer.hashed_bytes(bytes.len() as u64)?; }
        if blake3::hash(&bytes).as_bytes() != &location.digest {
            return Err(super::guarded_boundary(work, crate::wire::ErrorCode::Storage,
                "projection payload digest differs"));
        }
        Ok(())
    }.await;
    match result {
        Ok(()) => Ok(bytes),
        Err(error) => {
            let capacity = bytes.capacity() as u64;
            drop(bytes);
            if let Some(observer) = work.as_deref_mut() { let _ = observer.release_bytes(capacity); }
            Err(error)
        }
    }
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod native_staging_custody_tests {
    use super::*;
    use super::super::history::native_work_io;
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    use acyclic_stream::MemoryStream;
    use std::sync::{Arc, atomic::Ordering};

    pub(super) fn budget() -> WorkBudget {
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
    async fn native_byte_control_denial_precedes_authority_io_and_releases_source() -> Result<()> {
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = StreamClient::new(provider.clone());
        let cancellation = CancellationToken::new();
        let mut allowance = budget();
        allowance.allocation_operations = 1;
        let native = NativeMaterializationWork::new(allowance, &cancellation)
            .expect("original finite encoder ledger");
        let mut io = native_work_io(&native).expect("original source custody");
        let mut source = Vec::new();
        io.reserve_bytes(&mut source, 4096)?;
        io.copy_bytes(9)?;
        source.extend_from_slice(b"immutable");
        let error = stage_immutable_vec_with_work(
            &client, (StreamPath::new("harness/v3/native-stage-custody/control-denied")?, 0),
            Ok(source), &mut Some(&mut io)).await
            .expect_err("SDK guard metadata uses the original exhausted allocation allowance");
        assert!(matches!(error, Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::BudgetExceeded {
                counter: "allocation_operations", ..
            }), custody: None,
        }));
        assert_eq!(provider.io_snapshot().writes, 0);
        assert_eq!(io.into_custody().bytes(), 0);
        assert_eq!(native.remaining().expect("failed controls retire the actual source").peak_allocation_bytes,
            allowance.peak_allocation_bytes);
        Ok(())
    }

    #[tokio::test]
    async fn original_staged_vec_capacity_releases_on_ack_error_cancel_and_retry() -> Result<()> {
        for mode in 0..5 {
            let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
            let client = StreamClient::new(provider.clone());
            let path = StreamPath::new(format!("harness/v3/native-stage-custody/{mode}"))?;
            let cancellation = CancellationToken::new();
            let mut allowance = budget();
            if mode == 4 { allowance.backend_write_operations = 0; }
            let native = NativeMaterializationWork::new(allowance, &cancellation)
                .expect("original finite immutable publication ledger");
            let mut io = native_work_io(&native).expect("original source custody");
            let mut source = Vec::new();
            io.reserve_bytes(&mut source, 4096)?;
            io.copy_bytes(9)?;
            source.extend_from_slice(b"immutable");
            assert!(source.capacity() > source.len());
            let source_capacity = source.capacity();
            let source_pointer = source.as_ptr();
            if mode == 1 { provider.forbid_writes.store(true, Ordering::SeqCst); }
            if mode == 2 { provider.commit_lose_ack.store(true, Ordering::SeqCst); }
            if mode == 3 { cancellation.cancel(); }
            let staged = stage_immutable_vec_with_work(
                &client, (StreamPath::new(path.as_str())?, 0), Ok(source), &mut Some(&mut io)).await;
            if mode == 0 { staged?; } else { assert!(staged.is_err()); }
            assert_eq!(io.into_custody().bytes(), 0, "source custody moved into SDK aliases");
            if matches!(mode, 3 | 4) { assert_eq!(provider.io_snapshot().writes, 0); }
            if mode == 2 {
                assert_eq!(client.bounds(path.as_str()).await?.tail, 1);
                let mut io = native_work_io(&native).expect("same original retry ledger");
                let mut source = Vec::new();
                io.reserve_bytes(&mut source, 4096)?;
                io.copy_bytes(9)?;
                source.extend_from_slice(b"immutable");
                stage_immutable_vec_with_work(
                    &client, (StreamPath::new(path.as_str())?, 0), Ok(source), &mut Some(&mut io)).await?;
                assert_eq!(io.into_custody().bytes(), 0);
                assert_eq!(client.bounds(path.as_str()).await?.tail, 1);
            }
            if matches!(mode, 0 | 2) {
                let record = operations::one_record(&client, &path, 0, &mut None).await?
                    .expect("original immutable publication persisted");
                assert_eq!(record.value.as_ref(), b"immutable");
                let capacity = record.value.retained_capacity()
                    .expect("original native encoder capacity stays intrinsic");
                assert_eq!(capacity, source_capacity);
                assert_eq!(record.value.as_ref().as_ptr(), source_pointer);
                let alias = record.value.clone();
                let empty = alias.slice(0..0);
                drop(record);
                drop(alias);
                assert!(native.remaining().expect("original remaining ledger").peak_allocation_bytes
                    <= budget().peak_allocation_bytes - capacity as u64);
                drop(client);
                drop(provider);
                assert_eq!(empty.retained_capacity(), Some(capacity));
                assert!(native.remaining().expect("last alias retains original claim").peak_allocation_bytes
                    <= budget().peak_allocation_bytes - capacity as u64);
                let remaining = native.remaining().expect("original finite quota").peak_allocation_bytes;
                assert!(matches!(native.claim(remaining + 1),
                    Err(acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::BudgetExceeded {
                        counter: "peak_allocation_bytes", ..
                    }))));
                drop(empty);
                assert_eq!(native.remaining().expect("last alias retires original claim").peak_allocation_bytes,
                    budget().peak_allocation_bytes);
            } else if mode != 3 {
                drop(client);
                drop(provider);
                assert_eq!(native.remaining().expect("failed write releases original claim").peak_allocation_bytes,
                    budget().peak_allocation_bytes);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn competing_immutable_identity_recovers_or_rejects_without_retained_source() -> Result<()> {
        for identical in [true, false] {
            let client = StreamClient::new(Arc::new(MemoryStream::default()));
            let path = StreamPath::new("harness/v3/native-stage-custody/competing")?;
            client.stream(path.as_str())?.append_at(OwnedBytes::from_static(b"immutable"), 0).await?;
            let cancellation = CancellationToken::new();
            let native = NativeMaterializationWork::new(budget(), &cancellation)
                .expect("original finite competitor-read ledger");
            let mut io = native_work_io(&native).expect("original staging source custody");
            let mut source = Vec::new();
            io.reserve_bytes(&mut source, 4096)?;
            let body: &[u8] = if identical { b"immutable" } else { b"hostile" };
            io.copy_bytes(body.len() as u64)?;
            source.extend_from_slice(body);
            let staged = stage_immutable_vec_with_work(
                &client, (StreamPath::new(path.as_str())?, 0), Ok(source), &mut Some(&mut io)).await;
            assert_eq!(staged.is_ok(), identical);
            assert_eq!(io.into_custody().bytes(), 0);
            assert_eq!(client.bounds(path.as_str()).await?.tail, 1);
            drop(client);
            assert_eq!(native.remaining().expect("competitor recovery retires all original aliases").peak_allocation_bytes,
                budget().peak_allocation_bytes);
        }
        Ok(())
    }

    #[tokio::test]
    async fn malformed_chunk_manifest_rejects_before_source_growth_and_provider_io() -> Result<()> {
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = StreamClient::new(provider.clone());
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original finite recovery ledger");
        let mut io = native_work_io(&native).expect("original recovery observer");
        let location = ProjectionLocation {
            authority: Authority { kind: crate::core::AggregateKind::Agent, id: "malformed".into() },
            revision: 64,
            bytes: 1,
            digest: [0; 32],
            chunks: vec![ProjectionChunk { digest: [0; 32], bytes: 0 }],
        };
        let mut consumed = 0;
        assert!(load_chunks(&client, &location, &mut consumed, 1024, &mut Some(&mut io)).await.is_err());
        assert_eq!(io.into_custody().bytes(), 0);
        let sdk = provider.io_snapshot();
        assert_eq!(sdk.reads + sdk.tails as u64 + sdk.writes + sdk.inspections + sdk.commit_reads, 0);
        Ok(())
    }
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod native_malformed_receipt_tests {
    use super::*;
    use super::native_staging_custody_tests::budget;
    use super::super::history::native_work_io;
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError, WorkError};
    use acyclic_stream::MemoryStream;
    use std::sync::{Arc, atomic::Ordering};

    #[tokio::test]
    async fn original_corrupt_receipt_drops_source_and_keeps_first_native_quota_cause() -> Result<()> {
        for (fault, deny_received_bytes) in [(4, false), (5, false), (5, true)] {
            let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
            let client = StreamClient::new(provider.clone());
            let cancellation = CancellationToken::new();
            let mut allowance = budget();
            if deny_received_bytes { allowance.authority_bytes_read = 0; }
            let native = NativeMaterializationWork::new(allowance, &cancellation)
                .expect("original finite malformed receipt ledger");
            let mut io = native_work_io(&native).expect("original immutable source custody");
            let authority = Authority {
                kind: crate::core::AggregateKind::Agent,
                id: "malformed-immutable-receipt".into(),
            };
            let path = operations::publication_path_with_work(
                &authority, "harness/v2/aggregate-operations/", None, &mut Some(&mut io))?;
            let mut source = Vec::new();
            io.reserve_bytes(&mut source, 4096)?;
            io.copy_bytes(9)?;
            source.extend_from_slice(b"immutable");
            provider.aggregate_commit_fault.store(fault, Ordering::SeqCst);
            let error = stage_immutable_vec_with_work(
                &client, path, Ok(source), &mut Some(&mut io)).await
                .expect_err("actual provider receipt corruption must be rejected");
            if deny_received_bytes {
                assert!(matches!(error, Error::NativeWork {
                    cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                        counter: "authority_bytes_read", ..
                    }), custody: None,
                }));
            } else {
                assert!(matches!(error, Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Storage,
                    reason: "immutable record receipt is invalid",
                }));
            }
            assert_eq!(io.into_custody().bytes(), 0);
            drop(client);
            drop(provider);
            if !deny_received_bytes {
                assert_eq!(native.remaining().expect("corrupt receipt aliases retire with the provider").peak_allocation_bytes,
                    budget().peak_allocation_bytes);
            }
        }
        Ok(())
    }
}
