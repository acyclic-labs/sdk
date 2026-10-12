//! Exact effect transitions derived from their original canonical atomic commits.

use super::{HistoryCursor, HistoryReadLimits, effect_heads, history::HistoryIo, operations};
use crate::{
    EffectId, Error, Result,
    core::{Authority, AuthorityVerifier, Event, EventPayload, Reducer},
};
use acyclic_stream::{Record, StreamClient, StreamPath, StreamProvider};
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
    let mut reservation = 0;
    let diagnostic_bytes = crate::contract::audited_serde_diagnostic_bytes(
        bytes.len() as u64,
        ["EffectLocation", "OperationLocation", "Authority", "AggregateKind", "location",
         "effect_id", "authority", "operation_id", "revision", "intent_digest",
         "effect_position", "kind", "id", "agent", "conversation", "session", "turn", "task"]
            .iter().map(|label| label.len() as u64 + 4).sum(),
    )?;
    let mut observed_failure = false;
    let native = operations::original_native_decode_mode(observer);
    let decoded = operations::decode_preclaimed_json_with_mode::<EffectLocation, _>(
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
                    .ok_or_else(|| Error::Invalid("effect location reservation overflows".into()))?;
                observer.retain_owned(planned)?;
                reservation = planned;
                Ok(())
            } else { observer.codec_charge(charge) };
            if charged.is_err() { observed_failure = true; }
            charged
        },
    );
    let decoded = operations::retain_original_decode_error(decoded, &mut reservation, observer);
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

fn path(authority: &Authority, effect: EffectId) -> Result<StreamPath> {
    let canonical = authority.stream_path()?;
    let suffix = canonical
        .strip_prefix("harness/v2/")
        .ok_or_else(|| Error::Invalid("effect index authority path is invalid".into()))?;
    Ok(StreamPath::new(format!(
        "harness/v2/effect-transitions/{suffix}/{effect}"
    ))?)
}

fn path_with_work(
    authority: &Authority,
    effect: EffectId,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(StreamPath, u64)> {
    if work.is_none() {
        return Ok((path(authority, effect)?, 0));
    }
    operations::publication_path_with_work(
        authority, "harness/v2/effect-transitions/", Some((&effect, 36)), work)
}

fn canonical_path_with_work(
    authority: &Authority,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(StreamPath, u64)> {
    if work.is_none() {
        return Ok((StreamPath::new(authority.stream_path()?)?, 0));
    }
    operations::publication_path_with_work(authority, "harness/v2/", None, work)
}

fn release_record_after_drop<T>(
    result: Result<T>,
    bytes: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<T> {
    let released = match work.as_deref_mut() {
        Some(observer) => observer.release_record(bytes),
        None => Ok(()),
    };
    match result {
        Ok(value) => released.map(|()| value),
        Err(error) => Err(error),
    }
}

async fn tail_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    path: &StreamPath,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    operations::tail_optional_with_work(client, path, work).await
        .map(|tail| tail.unwrap_or(0))
}

async fn verified_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
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
        if location.effect_id != effect || location.location.effect_position != Some(record.sequence) {
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
    }.await;
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
            Err(Error::Storage("effect locator targets another canonical transition".into()))
        } else { Ok(()) }
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
    client: &StreamClient<P>,
    reducer: &Reducer,
    event: &Event,
    publication: &mut operations::IndexedPublication,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    let verifier = reducer.event_verifier_with_work(work)?;
    let result = async {
        let Some(effect_id) = crate::core::event_effect_id(&event.payload) else {
            return effect_heads::publication_with_work(
                client, reducer.authority(), &verifier, event, None, publication, work,
            ).await;
        };
        let mut path = Some(path_with_work(reducer.authority(), effect_id, work)?);
        let result = async {
            let expected = if matches!(event.payload, EventPayload::EffectPlanned { .. }) {
                0
            } else {
                let source_path = &path.as_ref().expect("original effect path").0;
                let count = tail_with_work(client, source_path, work).await?;
                if count == 0 {
                    return Err(Error::Unsupported(
                        "effect transition requires its original atomic index".into(),
                    ));
                }
                let record = operations::one_record(client, source_path, count - 1, work)
                    .await?
                    .ok_or_else(|| Error::Storage("effect transition head is missing".into()))?;
                let prior = verified_with_work(
                    client, reducer.authority(), &verifier, effect_id, &record,
                    2 * acyclic_stream::MAX_RECORD_BYTES as u64, work,
                ).await;
                let record_bytes = operations::sdk_record_retained_bytes(&record, work);
                drop(record);
                let released = match work.as_deref_mut() {
                    Some(observer) => observer.release_record(record_bytes),
                    None => Ok(()),
                };
                let ((prior, owned), _) = prior?;
                let valid = if prior.revision > reducer.revision() {
                    Err(Error::Conflict("effect index is ahead of this reducer".into()))
                } else { released };
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
                &location, acyclic_stream::MAX_RECORD_BYTES as u64, work);
            let location_capacity = location.location.authority.id.capacity() as u64;
            drop(location);
            let appended = publication.add_derived_index_with_work(
                path.take().expect("original effect path"), expected, encoded, work);
            operations::release_owned_after_drop(appended, location_capacity, work)?;
            effect_heads::publication_with_work(
                client, reducer.authority(), &verifier, event,
                Some((effect_id, effect_heads::Head { revision: event.revision, position: expected })),
                publication, work,
            ).await
        }.await;
        match path {
            Some((path, capacity)) => {
                drop(path);
                operations::release_owned_after_drop(result, capacity, work)
            }
            None => result,
        }
    }.await;
    let verifier_capacity = verifier.logical_owned_bytes();
    drop(verifier);
    match verifier_capacity {
        Ok(capacity) => operations::release_owned_after_drop(result, capacity, work),
        Err(error) => match result { Err(original) => Err(original), Ok(()) => Err(error) },
    }
}

/// Reads authenticated effect events at a bounded committed cut.
pub async fn effect_events<P: StreamProvider>(
    client: &StreamClient<P>,
    verifier: &AuthorityVerifier,
    cursor: &HistoryCursor,
    effect: EffectId,
    limits: HistoryReadLimits,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(Vec<Event>, u64)> {
    verifier.verify_audience_with_work(&cursor.authority, work)?;
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
    let (canonical_path, canonical_capacity) = canonical_path_with_work(&cursor.authority, work)?;
    let canonical_tail = tail_with_work(client, &canonical_path, work).await;
    drop(canonical_path);
    let canonical_tail = operations::release_owned_after_drop(canonical_tail, canonical_capacity, work)?;
    if cursor.through_revision > canonical_tail {
        return Err(Error::Invalid(
            "effect cursor exceeds committed canonical tail".into(),
        ));
    }
    let selected = effect_heads::select_with_work(
        client,
        &cursor.authority,
        verifier,
        effect,
        cursor.through_revision,
        limits.maximum_bytes,
        work,
    )
    .await?;
    let Some(head) = selected.head else {
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
    let (path, path_capacity) = path_with_work(&cursor.authority, effect, work)?;
    let mut events = Vec::new();
    let mut event_claims = 0_u64;
    let mut staged = None;
    let result = async {
        let mut used = selected.consumed_bytes;
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
                    client, &cursor.authority, verifier, effect, &record, remaining, work,
                ).await?;
                staged = Some((event, owned));
                let event = &staged.as_ref().expect("original authenticated event").0;
                used = used.checked_add(bytes)
                    .ok_or_else(|| Error::Invalid("effect history byte count overflows".into()))?;
                if event.revision <= previous_revision {
                    return Err(Error::Storage("effect transition revisions are not increasing".into()));
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
            }.await;
            let record_bytes = operations::sdk_record_retained_bytes(&record, work);
            drop(record);
            release_record_after_drop(result, record_bytes, work)?;
            let (event, owned) = staged.take().expect("original authenticated output event");
            event_claims += owned;
            events.push(event);
        }
        Ok(())
    }.await;
    drop(path);
    let staged_claim = staged.as_ref().map_or(0, |(_, owned)| *owned);
    drop(staged);
    let vector_claim = if work.is_some() {
        events.capacity() as u64 * std::mem::size_of::<Event>() as u64
    } else { 0 };
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
    let (operation_path, operation_path_capacity) = if work.is_none() {
        (operations::operation_path(authority, event.operation_id)?, 0)
    } else {
        operations::publication_path_with_work(
            authority, "harness/v2/aggregate-operations/", Some((&event.operation_id, 36)), work)?
    };
    let result = async {
        let Some(operation) = operations::one_record(client, &operation_path, 0, work).await? else {
            return if tail_with_work(client, &operation_path, work).await? == 0 {
                Ok((false, 0))
            } else {
                Err(Error::Storage("terminal operation index is missing".into()))
            };
        };
        let result = async {
            let used = operation.value.len() as u64;
            if used > maximum_bytes {
                return Err(Error::Invalid("effect retirement exceeds byte allowance".into()));
            }
            if tail_with_work(client, &operation_path, work).await? != 1 {
                return Err(Error::Storage("terminal operation index is not immutable".into()));
            }
            let location = operations::decode_location_with_work(&operation.value, work)?;
            let result = async {
                operations::verify_operation_binding(authority, &operation, &location, canonical, event)?;
                let Some(position) = location.effect_position else {
                    return Ok((false, used));
                };
                if used == maximum_bytes {
                    return Err(Error::Invalid("effect retirement exceeds byte allowance".into()));
                }
                let (path, path_capacity) = path_with_work(authority, observation.effect_id, work)?;
                let record = operations::one_record(client, &path, position, work).await;
                drop(path);
                let record = operations::release_owned_after_drop(record, path_capacity, work)?
                    .ok_or_else(|| Error::Storage("terminal effect transition index is missing".into()))?;
                let result = (|| {
                    let used = used.checked_add(record.value.len() as u64)
                        .filter(|bytes| *bytes <= maximum_bytes)
                        .ok_or_else(|| Error::Invalid("effect retirement exceeds byte allowance".into()))?;
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
                release_record_after_drop(result, record_bytes, work)
            }.await;
            let location_bytes = location.authority.id.capacity() as u64;
            drop(location);
            operations::release_owned_after_drop(result, location_bytes, work)
        }.await;
        let operation_bytes = operations::sdk_record_retained_bytes(&operation, work);
        drop(operation);
        release_record_after_drop(result, operation_bytes, work)
    }.await;
    drop(operation_path);
    operations::release_owned_after_drop(result, operation_path_capacity, work)
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
    let (path, path_capacity) = canonical_path_with_work(authority, work)?;
    let record = operations::one_record(client, &path, position, work).await;
    drop(path);
    let record = operations::release_owned_after_drop(record, path_capacity, work)?
        .ok_or_else(|| Error::Storage("terminal canonical event is missing".into()))?;
    let result = async {
        if let Some(observer) = work.as_deref_mut() { observer.check_cancellation()?; }
        let expected = crate::wire_codec::encode_event(authority, event, work)?;
        let matches = if record.value.as_ref() != expected.as_slice() {
            Err(Error::Storage("terminal canonical event differs".into()))
        } else {
            Ok(())
        };
        let capacity = expected.capacity() as u64;
        drop(expected);
        let released = match work.as_deref_mut() {
            Some(observer) => observer.release_bytes(capacity),
            None => Ok(()),
        };
        matches?;
        released?;
        retirement_proof_with_work(
            client, authority, event, &record,
            2 * acyclic_stream::MAX_RECORD_BYTES as u64, work,
        ).await.map(|(verified, _)| verified)
    }.await;
    let record_bytes = operations::sdk_record_retained_bytes(&record, work);
    drop(record);
    release_record_after_drop(result, record_bytes, work)
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod native_source_custody_tests {
    use super::*;
    use super::super::history::native_work_io;
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};

    fn budget() -> WorkBudget {
        WorkBudget {
            source_bytes_read: 1_048_576,
            bytes_copied: 1_048_576,
            bytes_encoded: 1_048_576,
            bytes_hashed: 1_048_576,
            items_examined: 1_048_576,
            allocation_operations: 1_048_576,
            peak_allocation_bytes: 1_048_576,
            ..WorkBudget::default()
        }
    }

    #[test]
    fn original_effect_location_decoder_retains_exact_serde_custody() -> Result<()> {
        let source = br#"{"effect_id":"00000000-0000-0000-0000-000000000002","location":{"authority":{"id":"native-effect","kind":"agent"},"effect_position":0,"intent_digest":[3],"operation_id":"00000000-0000-0000-0000-000000000001","revision":1}}"#;
        assert!(matches!(decode_location_with_work(source, &mut None), Err(Error::Storage(_))));
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
        let mut io = native_work_io(&native).expect("original observer");
        let error = decode_location_with_work(source, &mut Some(&mut io))
            .err().expect("original fixed digest rejects malformed length");
        let Error::NativeDecode { code, source, custody: Some(custody) } = error else {
            panic!("original effect serde cause and source diagnostic guard");
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

    #[test]
    fn original_effect_location_and_path_release_real_capacities() -> Result<()> {
        let location = EffectLocation {
            location: operations::OperationLocation {
                authority: Authority { kind: crate::core::AggregateKind::Agent, id: "native-effect".into() },
                operation_id: crate::OperationId::from_bytes([1; 16]),
                revision: 1,
                intent_digest: [3; 32],
                effect_position: Some(0),
            },
            effect_id: EffectId::from_bytes([2; 16]),
        };
        let source = super::super::history::canonical_bytes_with_work(
            &location, acyclic_stream::MAX_RECORD_BYTES as u64, &mut None)?;
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
        let mut io = native_work_io(&native).expect("original observer");
        let decoded = decode_location_with_work(&source, &mut Some(&mut io))?;
        assert_eq!(decoded.location, location.location);
        assert_eq!(decoded.effect_id, location.effect_id);
        let capacity = decoded.location.authority.id.capacity() as u64;
        drop(decoded);
        io.release_owned(capacity)?;
        let (path, payload) = path_with_work(&location.location.authority, location.effect_id, &mut Some(&mut io))?;
        assert_eq!(path.as_str(), super::path(&location.location.authority, location.effect_id)?.as_str());
        assert_eq!(payload, path.as_str().len() as u64);
        assert!(native.receipt().bytes_copied > source.len() as u64);
        drop(path);
        io.release_owned(payload)?;
        assert_eq!(io.into_custody().bytes(), 0);
        Ok(())
    }

    #[test]
    fn original_effect_decoder_preserves_cancellation_and_copy_denial() -> Result<()> {
        let source = br#"{"effect_id":false,"location":false}"#;
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
        let mut io = native_work_io(&native).expect("original observer");
        cancellation.cancel();
        assert!(matches!(decode_location_with_work(source, &mut Some(&mut io)),
            Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
        assert_eq!(io.into_custody().bytes(), 0);

        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(WorkBudget { bytes_copied: 0, ..budget() }, &cancellation)
            .expect("original copy-denied ledger");
        let mut io = native_work_io(&native).expect("original observer");
        assert!(matches!(decode_location_with_work(source, &mut Some(&mut io)),
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Work(
                    acyclic_fs::WorkError::BudgetExceeded { counter: "bytes_copied", .. }), ..
            })));
        assert_eq!(io.into_custody().bytes(), 0);
        Ok(())
    }
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod native_effect_paid_point_tests {
    use super::*;
    use super::super::{checkpoints, history::native_work_io};
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    use acyclic_stream::{IdempotencyKey, MemoryStream};
    use crate::{
        Capabilities, IdempotencyKey as CommandKey, OperationId,
        conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
        core::{Action, AggregateKind, ApplyResult, AuthorityIssuer, Command, EffectGuarantee, SchemaRegistry},
        resources::ProviderRef,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn original_effect_point_returns_actual_decoder_and_vector_paid_claim() -> Result<()> {
        let authority = Authority { kind: AggregateKind::Agent, id: "paid-effect-point".into() };
        let issuer = AuthorityIssuer::new("paid-effect-point", [7; 32], authority.clone());
        let reducer = Reducer::new(authority.clone(), issuer.verifier(), SchemaRegistry::new());
        let volume = VolumeRef::new(
            ProviderRef::new("test", "filesystem", "2")?, "effect-volume",
            VolumeClass::AgentPrivate, VolumeOwner::Agent(crate::AgentId::from_bytes([8; 16])),
        )?;
        let request = FileRef::new(volume.clone(), "effects/request.json", "generation-1",
            FileDescriptor::from_bytes(b"{}", "application/json")?, "request.json")?;
        let schema = FileRef::new(volume, "effects/schema.json", "generation-1",
            FileDescriptor::from_bytes(br#"{"type":"object"}"#, "application/schema+json")?, "schema.json")?;
        let effect = EffectId::from_bytes([91; 16]);
        let command = Command {
            operation_id: OperationId::from_bytes([92; 16]),
            idempotency_key: CommandKey::new("paid-effect-point")?,
            expected_revision: 0,
            scope: issuer.root("effects", Capabilities::new([
                "effect:run", "effect:plan", "effect:provider:test.effects",
            ])),
            causal_parent: None,
            action: Action::PlanEffect {
                effect_id: effect, provider: "test.effects".into(),
                guarantee: EffectGuarantee::IdempotentRetry, effect_kind: "test.write".into(),
                request, result_schema: schema,
            },
        };
        let ApplyResult::Applied { event } = reducer.plan(&command)? else {
            panic!("fresh original effect plan");
        };
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let publication = checkpoints::publication_with_work(&client, &reducer, &event, &mut None).await?;
        let request = publication.request(&client,
            IdempotencyKey::new(Bytes::from_static(b"paid-effect-point"))?).await?;
        let acyclic_stream::CommitOutcome::Committed(envelope) = client.provider.commit(request).await? else {
            panic!("original atomic effect publication");
        };
        publication.verify(&envelope)?;
        drop(envelope);
        drop(publication);

        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(WorkBudget {
            authority_records_read: 64, authority_bytes_read: 1_048_576,
            backend_read_operations: 64, source_bytes_read: 1_048_576,
            bytes_copied: 1_048_576, bytes_encoded: 1_048_576, bytes_hashed: 1_048_576,
            output_bytes: 1_048_576, items_returned: 64, items_examined: 1_048_576,
            allocation_operations: 1_048_576, peak_allocation_bytes: 1_048_576,
            ..WorkBudget::default()
        }, &cancellation).expect("same original finite effect point ledger");
        let mut io = native_work_io(&native).expect("original point decoder source custody");
        let cursor = HistoryCursor { authority, after_revision: 0, through_revision: 1 };
        let (events, paid) = read(
            &client, &issuer.verifier(), &cursor, effect,
            HistoryReadLimits { maximum_events: 8, maximum_bytes: 1_048_576 },
            &mut Some(&mut io),
        ).await?;
        assert_eq!(events, vec![event]);
        let mut custody = io.into_custody();
        assert_eq!(custody.bytes(), paid);
        assert!(paid >= events.capacity() as u64 * std::mem::size_of::<Event>() as u64);
        drop(events);
        custody.release(paid).expect("original event vector physically dropped before paid retirement");
        assert_eq!(custody.bytes(), 0);
        Ok(())
    }
}
