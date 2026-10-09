//! Exact effect transitions derived from their original canonical atomic commits.

use super::{HistoryCursor, HistoryReadLimits, operations};
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
    match client.stream(path.as_str())?.tail().await {
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
    if record.value.len() as u64 >= maximum_bytes {
        return Err(Error::Invalid("effect lookup exceeds byte bound".into()));
    }
    let location: EffectLocation = crate::executor::decode_json(&record.value)?;
    if location.effect_id != effect
        || location
            .location
            .effect_position
            .is_some_and(|position| position != record.sequence)
    {
        return Err(Error::Storage("effect locator identity differs".into()));
    }
    let (event, bytes) = operations::verify_operation_locator(
        client,
        authority,
        verifier,
        record,
        &location.location,
        maximum_bytes,
    )
    .await?;
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
    let Some(effect_id) = crate::core::event_effect_id(&event.payload) else {
        return Ok(());
    };
    let path = path(reducer.authority(), effect_id)?;
    let expected = if matches!(event.payload, EventPayload::EffectPlanned { .. }) {
        // The immutable first record is also the lifetime effect-ID fence.
        // A new operation cannot reuse a retired effect identity.
        0
    } else {
        let count = tail(client, &path).await?;
        if count == 0 {
            return Err(Error::Unsupported(
                "effect transition requires its original atomic index".into(),
            ));
        }
        let record = operations::one_record(client, &path, count - 1)
            .await?
            .ok_or_else(|| Error::Storage("effect transition head is missing".into()))?;
        let (prior, _) = verified(
            client,
            reducer.authority(),
            &reducer.event_verifier(),
            effect_id,
            &record,
            2 * acyclic_stream::MAX_RECORD_BYTES as u64,
        )
        .await?;
        if prior.revision > reducer.revision() {
            return Err(Error::Conflict(
                "effect index is ahead of this reducer".into(),
            ));
        }
        count
    };
    let location = EffectLocation {
        location: operations::OperationLocation {
            authority: reducer.authority().clone(),
            operation_id: event.operation_id,
            revision: event.revision,
            intent_digest: event.intent_digest,
            effect_position: Some(expected),
        },
        effect_id,
    };
    publication.set_effect_position(expected)?;
    publication.add_derived_index(
        path,
        expected,
        Bytes::from(crate::contract::canonical_json_bytes(&location)?),
    );
    Ok(())
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
    let path = path(&cursor.authority, effect)?;
    let count = tail(client, &path).await?;
    if count > u64::from(limits.maximum_events) {
        return Err(Error::Invalid(
            "effect history exceeds event allowance".into(),
        ));
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
    if count == 0 && cursor.through_revision != 0 {
        return Err(Error::Unsupported(
            "missing original effect index cannot prove absence".into(),
        ));
    }
    let mut events = Vec::new();
    let mut used = 0_u64;
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
        if event.revision > cursor.through_revision {
            break;
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
    let Some(operation) = operations::one_record(client, &operation_path, 0).await? else {
        return if tail(client, &operation_path).await? == 0 {
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
    if tail(client, &operation_path).await? != 1 {
        return Err(Error::Storage(
            "terminal operation index is not immutable".into(),
        ));
    }
    let location: operations::OperationLocation = crate::executor::decode_json(&operation.value)?;
    operations::verify_operation_binding(authority, &operation, &location, canonical, event)?;
    let Some(position) = location.effect_position else {
        return Ok((false, used));
    };
    if used == maximum_bytes {
        return Err(Error::Invalid(
            "effect retirement exceeds byte allowance".into(),
        ));
    }
    let record = operations::one_record(client, &path(authority, observation.effect_id)?, position)
        .await?
        .ok_or_else(|| Error::Storage("terminal effect transition index is missing".into()))?;
    used = used
        .checked_add(record.value.len() as u64)
        .filter(|bytes| *bytes <= maximum_bytes)
        .ok_or_else(|| Error::Invalid("effect retirement exceeds byte allowance".into()))?;
    let effect: EffectLocation = crate::executor::decode_json(&record.value)?;
    if effect.effect_id != observation.effect_id
        || effect.location != location
        || record.commit_id != canonical.commit_id
    {
        return Err(Error::Storage(
            "terminal effect index differs from original atomic publication".into(),
        ));
    }
    Ok((true, used))
}

pub(super) async fn retirement_proof_for_event<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    event: &Event,
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
    let record = operations::one_record(
        client,
        &StreamPath::new(authority.stream_path()?)?,
        position,
    )
    .await?
    .ok_or_else(|| Error::Storage("terminal canonical event is missing".into()))?;
    if record.value.as_ref() != crate::wire_codec::encode_event(authority, event)?.as_slice() {
        return Err(Error::Storage("terminal canonical event differs".into()));
    }
    retirement_proof(
        client,
        authority,
        event,
        &record,
        2 * acyclic_stream::MAX_RECORD_BYTES as u64,
    )
    .await
    .map(|(verified, _)| verified)
}
