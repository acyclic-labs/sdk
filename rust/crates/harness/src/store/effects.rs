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
    if location.effect_id != effect {
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
        },
        effect_id,
    };
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
