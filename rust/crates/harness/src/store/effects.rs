//! Exact effect transitions derived from their original canonical atomic commits.

use super::{HistoryCursor, HistoryReadLimits, effect_heads, history::HistoryIo, operations};
use crate::{
    EffectId, Error, Result,
    core::{Authority, AuthorityVerifier, Event, EventPayload, Reducer},
};
use acyclic_stream::{Record, StreamPath, StreamProvider};
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
    operations::decode_index(
        bytes,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        &[
            "EffectLocation",
            "OperationLocation",
            "Authority",
            "AggregateKind",
            "location",
            "effect_id",
            "authority",
            "operation_id",
            "revision",
            "intent_digest",
            "effect_position",
            "kind",
            "id",
            "agent",
            "conversation",
            "session",
            "turn",
            "task",
        ],
        |_, _| Ok(0),
        |effect: &EffectLocation| {
            (
                effect.location.authority.id.capacity() as u64,
                effect.location.authority.id.len() as u64,
            )
        },
        work,
    )
}

async fn verified_with_work<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    effect: EffectId,
    record: &Record,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<((Event, u64), u64)> {
    if record.value.len() as u64 >= maximum_bytes {
        return Err(Error::Invalid("effect lookup exceeds byte bound".into()));
    }
    let location = decode_location_with_work(&record.value, work)?;
    let result = async {
        if location.effect_id != effect
            || location.location.effect_position != Some(record.sequence)
        {
            return Err(Error::Storage("effect locator identity differs".into()));
        }
        operations::verify_operation_locator(
            client,
            authority,
            verifier,
            record,
            &location.location,
            maximum_bytes,
            work,
        )
        .await
    }
    .await;
    let location_capacity = location.location.authority.id.capacity() as u64;
    drop(location);
    let released = match work.as_deref_mut() {
        Some(observer) => observer.release_owned(location_capacity),
        None => Ok(()),
    };
    let ((event, owned), bytes) = match result {
        Ok(value) => value,
        Err(error) => return Err(error),
    };
    let verified = released.and_then(|()| {
        if crate::core::event_effect_id(&event.payload) != Some(effect) {
            Err(Error::Storage(
                "effect locator targets another canonical transition".into(),
            ))
        } else {
            Ok(())
        }
    });
    match verified {
        Ok(()) => Ok(((event, owned), bytes)),
        Err(error) => {
            drop(event);
            operations::release_owned_after_drop(Err(error), owned, work)
        }
    }
}

pub(super) async fn add_publication_index_with_work<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    reducer: &Reducer,
    event: &Event,
    publication: &mut operations::IndexedPublication,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    let verifier = reducer.event_verifier();
    let result = async {
        let Some(effect_id) = crate::core::event_effect_id(&event.payload) else {
            return effect_heads::publication_with_work(
                client,
                reducer.authority(),
                verifier,
                event,
                None,
                publication,
                work,
            )
            .await;
        };
        let mut path = Some(operations::publication_path(
            reducer.authority(),
            "harness/v2/effect-transitions/",
            Some((&effect_id, 36)),
            work,
        )?);
        let result = async {
            let expected = if matches!(event.payload, EventPayload::EffectPlanned { .. }) {
                0
            } else {
                let source_path = &path.as_ref().expect("original effect path").0;
                let count = operations::tail_optional_with_work(client, source_path, work)
                    .await?
                    .unwrap_or(0);
                if count == 0 {
                    return Err(Error::Unsupported(
                        "effect transition requires its original atomic index".into(),
                    ));
                }
                let record = operations::one_record(client, source_path, count - 1, work)
                    .await?
                    .ok_or_else(|| Error::Storage("effect transition head is missing".into()))?;
                let prior = verified_with_work(
                    client,
                    reducer.authority(),
                    verifier,
                    effect_id,
                    &record,
                    2 * acyclic_stream::MAX_RECORD_BYTES as u64,
                    work,
                )
                .await;
                let record_bytes = operations::sdk_record_retained_bytes(&record, work);
                drop(record);
                let released = match work.as_deref_mut() {
                    Some(observer) => observer.release_owned(record_bytes),
                    None => Ok(()),
                };
                let ((prior, owned), _) = prior?;
                let valid = if prior.revision > reducer.revision() {
                    Err(Error::Conflict(
                        "effect index is ahead of this reducer".into(),
                    ))
                } else {
                    released
                };
                drop(prior);
                operations::release_owned_after_drop(valid, owned, work)?;
                count
            };
            publication.set_effect_position_with_work(expected, work)?;
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
            let encoded = super::history::canonical_bytes_with_work(
                &location,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                work,
            );
            let location_capacity = location.location.authority.id.capacity() as u64;
            drop(location);
            let appended = publication.add_derived_index_with_work(
                path.take().expect("original effect path"),
                expected,
                encoded,
                work,
            );
            operations::release_owned_after_drop(appended, location_capacity, work)?;
            effect_heads::publication_with_work(
                client,
                reducer.authority(),
                verifier,
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
        .await;
        match path {
            Some((path, capacity)) => {
                drop(path);
                operations::release_owned_after_drop(result, capacity, work)
            }
            None => result,
        }
    }
    .await;
    result
}

/// Reads authenticated effect events at a bounded committed cut.
pub async fn effect_events<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    verifier: &AuthorityVerifier,
    cursor: &HistoryCursor,
    effect: EffectId,
    limits: HistoryReadLimits,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(Vec<Event>, u64)> {
    verifier.verify_audience(&cursor.authority, work)?;
    if limits.maximum_events == 0
        || limits.maximum_bytes == 0
        || cursor.after_revision > cursor.through_revision
    {
        return Err(Error::Invalid(
            "effect history bounds or cursor are invalid".into(),
        ));
    }
    if cursor.through_revision == 0 {
        return Ok((Vec::new(), 0));
    }
    let (canonical_path, canonical_capacity) =
        operations::publication_path(&cursor.authority, "harness/v2/", None, work)?;
    let canonical_tail = operations::tail_optional_with_work(client, &canonical_path, work)
        .await
        .map(|tail| tail.unwrap_or(0));
    drop(canonical_path);
    let canonical_tail =
        operations::release_owned_after_drop(canonical_tail, canonical_capacity, work)?;
    if cursor.through_revision > canonical_tail {
        return Err(Error::Invalid(
            "effect cursor exceeds committed canonical tail".into(),
        ));
    }
    let (head, consumed_bytes) = effect_heads::select_with_work(
        client,
        &cursor.authority,
        verifier,
        effect,
        cursor.through_revision,
        limits.maximum_bytes,
        work,
    )
    .await?;
    let Some(head) = head else {
        return Ok((Vec::new(), 0));
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
    let (path, path_capacity) = operations::publication_path(
        &cursor.authority,
        "harness/v2/effect-transitions/",
        Some((&effect, 36)),
        work,
    )?;
    let mut events = Vec::new();
    let mut event_claims = 0_u64;
    let mut staged = None;
    let result = async {
        let mut used = consumed_bytes;
        let mut previous_revision = 0;
        for position in 0..count {
            let remaining = limits
                .maximum_bytes
                .checked_sub(used)
                .filter(|bytes| *bytes > 0)
                .ok_or_else(|| Error::Invalid("effect history exceeds byte bound".into()))?;
            let record = operations::one_record(client, &path, position, work)
                .await?
                .ok_or_else(|| Error::Storage("effect transition index is incomplete".into()))?;
            let result = async {
                let ((event, owned), bytes) = verified_with_work(
                    client,
                    &cursor.authority,
                    verifier,
                    effect,
                    &record,
                    remaining,
                    work,
                )
                .await?;
                staged = Some((event, owned));
                let event = &staged.as_ref().expect("original authenticated event").0;
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
                if let Some(observer) = work.as_deref_mut() {
                    observer.stage_output(bytes - record.value.len() as u64, 1)?;
                    if events.len() == events.capacity() {
                        observer.reserve_events(&mut events, limits.maximum_events as usize)?;
                    }
                }
                Ok(())
            }
            .await;
            let record_bytes = operations::sdk_record_retained_bytes(&record, work);
            drop(record);
            operations::release_owned_after_drop(result, record_bytes, work)?;
            let (event, owned) = staged.take().expect("original authenticated output event");
            event_claims += owned;
            events.push(event);
        }
        Ok(())
    }
    .await;
    drop(path);
    let staged_claim = staged.as_ref().map_or(0, |(_, owned)| *owned);
    drop(staged);
    let vector_claim = if work.is_some() {
        events.capacity() as u64 * std::mem::size_of::<Event>() as u64
    } else {
        0
    };
    let released_path = operations::release_owned_after_drop(result, path_capacity, work);
    let result = operations::release_owned_after_drop(released_path, staged_claim, work);
    match result {
        Ok(()) => Ok((events, event_claims + vector_claim)),
        Err(error) => {
            drop(events);
            operations::release_owned_after_drop(Err(error), event_claims + vector_claim, work)
        }
    }
}

pub(super) async fn retirement_proof_with_work<P: StreamProvider>(
    client: &std::sync::Arc<P>,
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
    let (operation_path, operation_path_capacity) = {
        operations::publication_path(
            authority,
            "harness/v2/aggregate-operations/",
            Some((&event.operation_id, 36)),
            work,
        )?
    };
    let result = async {
        let Some(operation) = operations::one_record(client, &operation_path, 0, work).await?
        else {
            return if operations::tail_optional_with_work(client, &operation_path, work)
                .await?
                .unwrap_or(0)
                == 0
            {
                Ok((false, 0))
            } else {
                Err(Error::Storage("terminal operation index is missing".into()))
            };
        };
        let result = async {
            let used = operation.value.len() as u64;
            if used > maximum_bytes {
                return Err(Error::Invalid(
                    "effect retirement exceeds byte allowance".into(),
                ));
            }
            if operations::tail_optional_with_work(client, &operation_path, work)
                .await?
                .unwrap_or(0)
                != 1
            {
                return Err(Error::Storage(
                    "terminal operation index is not immutable".into(),
                ));
            }
            let location = operations::decode_location_with_work(&operation.value, work)?;
            let result = async {
                operations::verify_operation_binding(
                    authority, &operation, &location, canonical, event, work,
                )?;
                let Some(position) = location.effect_position else {
                    return Ok((false, used));
                };
                if used == maximum_bytes {
                    return Err(Error::Invalid(
                        "effect retirement exceeds byte allowance".into(),
                    ));
                }
                let (path, path_capacity) = operations::publication_path(
                    authority,
                    "harness/v2/effect-transitions/",
                    Some((&observation.effect_id, 36)),
                    work,
                )?;
                let record = operations::one_record(client, &path, position, work).await;
                drop(path);
                let record = operations::release_owned_after_drop(record, path_capacity, work)?
                    .ok_or_else(|| {
                        Error::Storage("terminal effect transition index is missing".into())
                    })?;
                let result = (|| {
                    let used = used
                        .checked_add(record.value.len() as u64)
                        .filter(|bytes| *bytes <= maximum_bytes)
                        .ok_or_else(|| {
                            Error::Invalid("effect retirement exceeds byte allowance".into())
                        })?;
                    let effect = decode_location_with_work(&record.value, work)?;
                    let result = if effect.effect_id != observation.effect_id
                        || effect.location != location
                        || record.commit_id != canonical.commit_id
                    {
                        Err(Error::Storage(
                            "terminal effect index differs from original atomic publication".into(),
                        ))
                    } else {
                        Ok((true, used))
                    };
                    let effect_bytes = effect.location.authority.id.capacity() as u64;
                    drop(effect);
                    operations::release_owned_after_drop(result, effect_bytes, work)
                })();
                let record_bytes = operations::sdk_record_retained_bytes(&record, work);
                drop(record);
                operations::release_owned_after_drop(result, record_bytes, work)
            }
            .await;
            let location_bytes = location.authority.id.capacity() as u64;
            drop(location);
            operations::release_owned_after_drop(result, location_bytes, work)
        }
        .await;
        let operation_bytes = operations::sdk_record_retained_bytes(&operation, work);
        drop(operation);
        operations::release_owned_after_drop(result, operation_bytes, work)
    }
    .await;
    drop(operation_path);
    operations::release_owned_after_drop(result, operation_path_capacity, work)
}

pub(super) async fn retirement_proof_for_event_with_work<P: StreamProvider>(
    client: &std::sync::Arc<P>,
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
    let (path, path_capacity) = operations::publication_path(authority, "harness/v2/", None, work)?;
    let record = operations::one_record(client, &path, position, work).await;
    drop(path);
    let record = operations::release_owned_after_drop(record, path_capacity, work)?
        .ok_or_else(|| Error::Storage("terminal canonical event is missing".into()))?;
    let result = async {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
        }
        let expected = crate::wire_codec::encode_event(authority, event, work)?;
        let matches = if record.value.as_ref() != expected.as_slice() {
            Err(Error::Storage("terminal canonical event differs".into()))
        } else {
            Ok(())
        };
        let capacity = expected.capacity() as u64;
        drop(expected);
        let released = match work.as_deref_mut() {
            Some(observer) => observer.release_owned(capacity),
            None => Ok(()),
        };
        matches?;
        released?;
        retirement_proof_with_work(
            client,
            authority,
            event,
            &record,
            2 * acyclic_stream::MAX_RECORD_BYTES as u64,
            work,
        )
        .await
        .map(|(verified, _)| verified)
    }
    .await;
    let record_bytes = operations::sdk_record_retained_bytes(&record, work);
    drop(record);
    operations::release_owned_after_drop(result, record_bytes, work)
}
