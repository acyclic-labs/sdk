//! Exact effect transitions derived from their original canonical atomic commits.

use super::{HistoryCursor, HistoryReadLimits, effect_heads, history::HistoryIo, operations};
use crate::{
    EffectId, Error, Result,
    core::{Authority, AuthorityVerifier, Event, EventPayload, Reducer},
};
use acyclic_stream::{Record, StreamClient, StreamError, StreamPath, StreamProvider};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EffectLocation {
    location: operations::OperationLocation,
    effect_id: EffectId,
}

fn decode_location_with_work(
    bytes: &[u8],
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<EffectLocation> {
    if work.is_some() {
        operations::validate_canonical_json_with_work(bytes, acyclic_stream::MAX_RECORD_BYTES as u64, work)?;
    }
    let Some(observer) = work.as_deref_mut() else { return crate::executor::decode_json(bytes); };
    // This fixed named-field schema owns only its original Authority.id.
    let mut reservation = 0;
    let diagnostic_bytes = crate::contract::audited_serde_diagnostic_bytes(
        bytes.len() as u64,
        ["EffectLocation", "OperationLocation", "Authority", "AggregateKind", "location",
         "effect_id", "authority", "operation_id", "revision", "intent_digest",
         "effect_position", "kind", "id", "agent", "conversation", "session", "turn", "task"]
            .iter().map(|label| label.len() as u64 + 4).sum(),
    )?;
    let mut observed_failure = false;
    let decoded = crate::contract::json_from_slice_bounded_trusted::<EffectLocation, _>(
        bytes,
        crate::contract::JsonBounds {
            maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
            maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64,
            maximum_depth: 256,
        },
        bytes.len() as u64,
        &mut |charge| {
            let charged = if let crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes } = charge {
                reservation = maximum_owned_bytes.checked_add(diagnostic_bytes)
                    .ok_or_else(|| Error::Invalid("effect location reservation overflows".into()))?;
                observer.retain_owned(reservation)
            } else { observer.codec_charge(charge) };
            if charged.is_err() { observed_failure = true; }
            charged
        },
    );
    let decoded = decoded.map_err(|error| {
        if observed_failure { error } else {
            match error {
                Error::Invalid(message) => Error::Storage(format!("execution journal content is invalid: {message}")),
                other => other,
            }
        }
    });
    observer.release_owned(reservation)?;
    let location = decoded?;
    observer.retain_owned(location.location.authority.id.capacity() as u64)?;
    observer.copy_bytes(location.location.authority.id.len() as u64)?;
    Ok(location)
}

fn path(authority: &Authority, effect: EffectId) -> Result<StreamPath> {
    let canonical = authority.stream_path()?;
    let suffix = canonical
        .strip_prefix("harness/v2/")
        .ok_or_else(|| Error::Invalid("effect index authority path is invalid".into()))?;
    Ok(StreamPath::new(format!(
        "harness/v2/effect-transitions/{suffix}/{effect}"
    ))?)
}

async fn tail<P: StreamProvider>(client: &StreamClient<P>, path: &StreamPath) -> Result<u64> {
    tail_with_work(client, path, &mut None).await
}

async fn tail_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    path: &StreamPath,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    let stream = client.stream(path.as_str())?;
    if let Some(work) = work.as_deref_mut() { work.begin_call()?; }
    match stream.tail().await {
        Ok(tail) => Ok(tail),
        Err(StreamError::NotFound) => Ok(0),
        Err(error) => Err(error.into()),
    }
}

async fn verified<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    effect: EffectId,
    record: &Record,
    maximum_bytes: u64,
) -> Result<(Event, u64)> {
    verified_with_work(client, authority, verifier, effect, record, maximum_bytes, &mut None).await
}

async fn verified_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    effect: EffectId,
    record: &Record,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(Event, u64)> {
    if record.value.len() as u64 >= maximum_bytes {
        return Err(Error::Invalid("effect lookup exceeds byte bound".into()));
    }
    let location = decode_location_with_work(&record.value, work)?;
    if location.effect_id != effect || location.location.effect_position != Some(record.sequence) {
        return Err(Error::Storage("effect locator identity differs".into()));
    }
    let (event, bytes) = operations::verify_operation_locator_with_work(
        client,
        authority,
        verifier,
        record,
        &location.location,
        maximum_bytes,
        work,
    )
    .await?;
    let location_capacity = location.location.authority.id.capacity() as u64;
    drop(location);
    if let Some(observer) = work.as_deref_mut() {
        observer.release_owned(location_capacity)?;
    }
    if crate::core::event_effect_id(&event.payload) != Some(effect) {
        return Err(Error::Storage(
            "effect locator targets another canonical transition".into(),
        ));
    }
    Ok((event, bytes))
}

pub(super) async fn add_publication_index<P: StreamProvider>(
    client: &StreamClient<P>,
    reducer: &Reducer,
    event: &Event,
    publication: &mut operations::IndexedPublication,
) -> Result<()> {
    add_publication_index_with_work(client, reducer, event, publication, &mut None).await
}

pub(super) async fn add_publication_index_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    reducer: &Reducer,
    event: &Event,
    publication: &mut operations::IndexedPublication,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    let Some(effect_id) = crate::core::event_effect_id(&event.payload) else {
        return effect_heads::publication_with_work(
            client,
            reducer.authority(),
            &reducer.event_verifier(),
            event,
            None,
            publication,
            work,
        )
        .await;
    };
    let path = path(reducer.authority(), effect_id)?;
    let expected = if matches!(event.payload, EventPayload::EffectPlanned { .. }) {
        // The immutable first record is also the lifetime effect-ID fence.
        // A new operation cannot reuse a retired effect identity.
        0
    } else {
        let count = tail_with_work(client, &path, work).await?;
        if count == 0 {
            return Err(Error::Unsupported(
                "effect transition requires its original atomic index".into(),
            ));
        }
        let record = operations::one_record_with_work(client, &path, count - 1, work)
            .await?
            .ok_or_else(|| Error::Storage("effect transition head is missing".into()))?;
        let (prior, _) = verified_with_work(
            client,
            reducer.authority(),
            &reducer.event_verifier(),
            effect_id,
            &record,
            2 * acyclic_stream::MAX_RECORD_BYTES as u64,
            work,
        )
        .await?;
        if let Some(observer) = work.as_deref_mut() {
            observer.release_record(record.value.len() as u64)?;
        }
        if prior.revision > reducer.revision() {
            return Err(Error::Conflict(
                "effect index is ahead of this reducer".into(),
            ));
        }
        count
    };
    let location = EffectLocation {
        location: operations::OperationLocation {
            authority: match work.as_deref_mut() {
                Some(observer) => observer.clone_authority(reducer.authority())?,
                None => reducer.authority().clone(),
            },
            operation_id: event.operation_id,
            revision: event.revision,
            intent_digest: event.intent_digest,
            effect_position: Some(expected),
        },
        effect_id,
    };
    publication.set_effect_position_with_work(expected, work)?;
    publication.add_derived_index_with_work(
        path,
        expected,
        Bytes::from(super::history::canonical_bytes_with_work(&location, acyclic_stream::MAX_RECORD_BYTES as u64, work)?),
        work,
    )?;
    effect_heads::publication_with_work(
        client,
        reducer.authority(),
        &reducer.event_verifier(),
        event,
        Some((
            effect_id,
            effect_heads::Head {
                revision: event.revision,
                position: expected,
            },
        )),
        publication,
        work,
    )
    .await
}

pub(super) async fn read<P: StreamProvider>(
    client: &StreamClient<P>,
    verifier: &AuthorityVerifier,
    cursor: &HistoryCursor,
    effect: EffectId,
    limits: HistoryReadLimits,
) -> Result<Vec<Event>> {
    verifier.verify_audience(&cursor.authority)?;
    if limits.maximum_events == 0
        || limits.maximum_bytes == 0
        || cursor.after_revision > cursor.through_revision
    {
        return Err(Error::Invalid(
            "effect history bounds or cursor are invalid".into(),
        ));
    }
    if cursor.through_revision == 0 {
        return Ok(Vec::new());
    }
    let canonical_tail = match client.stream(cursor.authority.stream_path()?)?.tail().await {
        Ok(tail) => tail,
        Err(StreamError::NotFound) => 0,
        Err(error) => return Err(error.into()),
    };
    if cursor.through_revision > canonical_tail {
        return Err(Error::Invalid(
            "effect cursor exceeds committed canonical tail".into(),
        ));
    }
    let selected = effect_heads::select(
        client,
        &cursor.authority,
        verifier,
        effect,
        cursor.through_revision,
        limits.maximum_bytes,
    )
    .await?;
    let Some(head) = selected.head else {
        return Ok(Vec::new());
    };
    let count = head
        .position
        .checked_add(1)
        .ok_or_else(|| Error::Storage("effect original prefix count overflows".into()))?;
    if count > u64::from(limits.maximum_events) {
        return Err(Error::Invalid(
            "effect history exceeds event allowance".into(),
        ));
    }
    let path = path(&cursor.authority, effect)?;
    let mut events = Vec::new();
    let mut used = selected.consumed_bytes;
    let mut previous_revision = 0;
    for position in 0..count {
        let remaining = limits
            .maximum_bytes
            .checked_sub(used)
            .filter(|bytes| *bytes > 0)
            .ok_or_else(|| Error::Invalid("effect history exceeds byte bound".into()))?;
        let record = operations::one_record(client, &path, position)
            .await?
            .ok_or_else(|| Error::Storage("effect transition index is incomplete".into()))?;
        let (event, bytes) = verified(
            client,
            &cursor.authority,
            verifier,
            effect,
            &record,
            remaining,
        )
        .await?;
        used = used
            .checked_add(bytes)
            .ok_or_else(|| Error::Invalid("effect history byte count overflows".into()))?;
        if event.revision <= previous_revision {
            return Err(Error::Storage(
                "effect transition revisions are not increasing".into(),
            ));
        }
        if event.revision > head.revision
            || (position == head.position && event.revision != head.revision)
        {
            return Err(Error::Storage(
                "effect transition differs from complete original cut".into(),
            ));
        }
        previous_revision = event.revision;
        events.push(event);
    }
    Ok(events)
}

/// Proves retirement against an already authenticated canonical record. The
/// two derived records are charged without decoding the canonical event again.
pub(super) async fn retirement_proof<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    event: &Event,
    canonical: &Record,
    maximum_bytes: u64,
) -> Result<(bool, u64)> {
    retirement_proof_with_work(client, authority, event, canonical, maximum_bytes, &mut None).await
}

pub(super) async fn retirement_proof_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    event: &Event,
    canonical: &Record,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(bool, u64)> {
    let EventPayload::EffectResolved { observation } = &event.payload else {
        return Ok((false, 0));
    };
    if !crate::core::settled_effect(&observation.status) {
        return Ok((false, 0));
    }
    if maximum_bytes == 0 {
        return Err(Error::Invalid(
            "effect retirement exceeds byte allowance".into(),
        ));
    }
    let operation_path = operations::operation_path(authority, event.operation_id)?;
    let Some(operation) = operations::one_record_with_work(client, &operation_path, 0, work).await? else {
        return if tail_with_work(client, &operation_path, work).await? == 0 {
            Ok((false, 0))
        } else {
            Err(Error::Storage("terminal operation index is missing".into()))
        };
    };
    let mut used = operation.value.len() as u64;
    if used > maximum_bytes {
        return Err(Error::Invalid(
            "effect retirement exceeds byte allowance".into(),
        ));
    }
    if tail_with_work(client, &operation_path, work).await? != 1 {
        return Err(Error::Storage(
            "terminal operation index is not immutable".into(),
        ));
    }
    let location = operations::decode_location_with_work(&operation.value, work)?;
    operations::verify_operation_binding(authority, &operation, &location, canonical, event)?;
    let Some(position) = location.effect_position else {
        let location_bytes = location.authority.id.capacity() as u64;
        drop(location);
        if let Some(work) = work.as_deref_mut() { work.release_owned(location_bytes)?; }
        let operation_bytes = operation.value.len() as u64;
        drop(operation);
        if let Some(work) = work.as_deref_mut() { work.release_record(operation_bytes)?; }
        return Ok((false, used));
    };
    if used == maximum_bytes {
        return Err(Error::Invalid(
            "effect retirement exceeds byte allowance".into(),
        ));
    }
    let record = operations::one_record_with_work(client, &path(authority, observation.effect_id)?, position, work)
        .await?
        .ok_or_else(|| Error::Storage("terminal effect transition index is missing".into()))?;
    used = used
        .checked_add(record.value.len() as u64)
        .filter(|bytes| *bytes <= maximum_bytes)
        .ok_or_else(|| Error::Invalid("effect retirement exceeds byte allowance".into()))?;
    let effect = decode_location_with_work(&record.value, work)?;
    if effect.effect_id != observation.effect_id
        || effect.location != location
        || record.commit_id != canonical.commit_id
    {
        return Err(Error::Storage(
            "terminal effect index differs from original atomic publication".into(),
        ));
    }
    let effect_bytes = effect.location.authority.id.capacity() as u64;
    let location_bytes = location.authority.id.capacity() as u64;
    drop(effect);
    drop(location);
    if let Some(work) = work.as_deref_mut() {
        work.release_owned(effect_bytes)?;
        work.release_owned(location_bytes)?;
    }
    let record_bytes = record.value.len() as u64;
    let operation_bytes = operation.value.len() as u64;
    drop(record);
    drop(operation);
    if let Some(work) = work.as_deref_mut() {
        work.release_record(record_bytes)?;
        work.release_record(operation_bytes)?;
    }
    Ok((true, used))
}

pub(super) async fn retirement_proof_for_event<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    event: &Event,
) -> Result<bool> {
    retirement_proof_for_event_with_work(client, authority, event, &mut None).await
}

pub(super) async fn retirement_proof_for_event_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    event: &Event,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<bool> {
    let EventPayload::EffectResolved { observation } = &event.payload else {
        return Ok(false);
    };
    if !crate::core::settled_effect(&observation.status) {
        return Ok(false);
    }
    let position = event
        .revision
        .checked_sub(1)
        .ok_or_else(|| Error::Invalid("effect revision must be positive".into()))?;
    let record = operations::one_record_with_work(
        client,
        &StreamPath::new(authority.stream_path()?)?,
        position,
        work,
    )
    .await?
    .ok_or_else(|| Error::Storage("terminal canonical event is missing".into()))?;
    if let Some(work) = work.as_deref_mut() { work.check_cancellation()?; }
    let expected = crate::wire_codec::encode_event_with_work(authority, event, work)?;
    if record.value.as_ref() != expected.as_slice() {
        return Err(Error::Storage("terminal canonical event differs".into()));
    }
    let capacity = expected.capacity() as u64;
    drop(expected);
    if let Some(work) = work.as_deref_mut() { work.release_bytes(capacity)?; }
    let (verified, _) = retirement_proof_with_work(
        client,
        authority,
        event,
        &record,
        2 * acyclic_stream::MAX_RECORD_BYTES as u64,
        work,
    )
    .await?;
    let record_bytes = record.value.len() as u64;
    drop(record);
    if let Some(work) = work.as_deref_mut() { work.release_record(record_bytes)?; }
    Ok(verified)
}
