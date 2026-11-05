//! Resource formats use their production admission and snapshot writer.
//! Stream authentication and provider attribution are outside these codec checks.
use acyclic_harness_core::{
    conversation::{self as c, epochs as e},
    executor::{
        ModelCacheAccess, ModelCacheBreakpoint, ModelCacheError, ModelCacheKey,
        ModelCacheObservation, ModelCacheResidency, ModelCacheTtl,
    },
    filesystem::PreparationIo,
    resources::ProviderRef,
};
use acyclic_stream::{Record, StreamPath};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{error::Error, fmt::Debug, sync::OnceLock};

fn roundtrip<T: Serialize + DeserializeOwned + PartialEq + Debug>(bytes: &[u8]) -> Option<T> {
    let value: T = serde_json::from_slice(bytes).ok()?;
    let encoded = serde_json::to_vec(&value).unwrap();
    assert_eq!(serde_json::from_slice::<T>(&encoded).unwrap(), value);
    Some(value)
}

// Field separation checks the original digest without reimplementing its format.
fn summary_key(value: &e::SummaryKey) {
    let digest = value.digest();
    for (field, diagnostic) in [
        (0, "summary key source identity changed"),
        (1, "summary key range identity changed"),
        (2, "summary key renderer identity changed"),
        (3, "summary key model identity changed"),
    ] {
        let mut changed = value.clone();
        match field {
            0 => {
                // StreamPath admits nonempty ASCII. Keep the byte length so
                // the length prefix cannot conceal omitted source bytes.
                let mut path = value.source.as_str().to_owned();
                let first = if path.starts_with('a') { "b" } else { "a" };
                path.replace_range(..1, first);
                changed.source = StreamPath::new(path).unwrap();
            }
            1 => changed.through ^= 1,
            2 => changed.renderer ^= 1,
            _ => changed.model[0] ^= 1,
        }
        assert_ne!(digest, changed.digest(), "{diagnostic}");
    }
}

fn epoch(value: &e::Epoch) {
    summary_key(&value.key);
    let foreign = if value.key.source == source() {
        StreamPath::new("corpus/foreign/conversation").unwrap()
    } else {
        source()
    };
    for through in [0, value.cursor(), u64::MAX] {
        for keep_summary in [false, true] {
            let mut changed = value.clone();
            changed.key.through = through;
            if !keep_summary {
                changed.summary = None;
            }
            for requested in [&value.key.source, &foreign] {
                for prefix in [0, through.saturating_sub(1), through, u64::MAX] {
                    let expected = if requested != &value.key.source {
                        Err(e::InvalidEpoch::Source)
                    } else if through > prefix {
                        Err(e::InvalidEpoch::OutsidePrefix)
                    } else if (through == 0) == changed.summary.is_some() {
                        Err(e::InvalidEpoch::MissingCoverage)
                    } else {
                        Ok(())
                    };
                    assert_eq!(
                        changed.validate(requested, prefix),
                        expected,
                        "epoch admission contract changed",
                    );
                }
            }
        }
    }
}

fn summary(value: &e::SummaryRef) {
    let original = serde_json::to_value(value).unwrap();
    for version in [0, 2, u32::MAX] {
        let mut changed = original.clone();
        changed["version"] = version.into();
        assert!(
            serde_json::from_value::<e::SummaryRef>(changed).is_err(),
            "unsupported summary version admitted",
        );
    }
    let mut changed = original.clone();
    changed["portable"]["descriptor"]["media_type"] = "application/json".into();
    assert!(
        serde_json::from_value::<e::SummaryRef>(changed).is_err(),
        "nonplaintext portable summary admitted",
    );
    for index in 0..value.native().len() {
        for field in ["provider", "model", "endpoint"] {
            let mut changed = original.clone();
            changed["native"][index]["provenance"][field] = " ".into();
            assert!(
                serde_json::from_value::<e::SummaryRef>(changed).is_err(),
                "incomplete native summary provenance admitted",
            );
        }
    }
}

fn residency(previous: ModelCacheResidency) {
    for version in [0, 1, u32::MAX] {
        let unsupported = ModelCacheResidency {
            version,
            ..previous
        };
        assert_eq!(
            unsupported.estimated_resident_at(previous.clock_domain, previous.observed_at_ns),
            None,
            "unsupported cache residency query admitted",
        );
        assert_eq!(
            ModelCacheResidency::observe(
                previous.key,
                Some(unsupported),
                previous.clock_domain,
                previous.observed_at_ns,
                ModelCacheAccess::Miss
            ),
            Err(ModelCacheError::Version),
            "unsupported cache residency update admitted",
        );
    }
    let mut foreign_clock = previous.clock_domain;
    foreign_clock[0] ^= 1;
    let mut foreign_scope = previous.key;
    foreign_scope.credential_scope[0] ^= 1;
    let mut foreign_prefix = previous.key;
    foreign_prefix.prefix_digest[0] ^= 1;
    for key in [foreign_scope, foreign_prefix] {
        assert_eq!(
            ModelCacheResidency::observe(
                key,
                Some(previous),
                previous.clock_domain,
                previous.observed_at_ns,
                ModelCacheAccess::Miss,
            ),
            Err(if previous.version == ModelCacheResidency::VERSION {
                ModelCacheError::KeyMismatch
            } else {
                ModelCacheError::Version
            }),
            "foreign cache key admitted",
        );
    }
    let seconds = previous.written_at_ns.unwrap_or(u64::MAX);
    for at in [0, previous.observed_at_ns, u64::MAX] {
        let supported = previous.version == ModelCacheResidency::VERSION;
        let expected = (supported && at >= previous.observed_at_ns)
            .then(|| previous.expires_at_ns.map(|expiry| at < expiry))
            .flatten();
        assert_eq!(
            previous.estimated_resident_at(previous.clock_domain, at),
            expected,
            "cache query original time contract changed",
        );
        assert_eq!(
            previous.estimated_resident_at(foreign_clock, at),
            None,
            "foreign clock cache query admitted",
        );
        assert_eq!(
            ModelCacheResidency::observe(
                previous.key,
                Some(previous),
                foreign_clock,
                at,
                ModelCacheAccess::Miss,
            ),
            Err(if supported {
                ModelCacheError::ClockMismatch
            } else {
                ModelCacheError::Version
            }),
            "foreign clock cache update admitted",
        );
        let ttl = ModelCacheTtl::Explicit { seconds };
        for prior in [None, Some(previous)] {
            for access in [
                ModelCacheAccess::Write(ModelCacheTtl::ProviderManaged),
                ModelCacheAccess::Write(ttl),
                ModelCacheAccess::Hit { refresh: None },
                ModelCacheAccess::Hit {
                    refresh: Some(ModelCacheTtl::ProviderManaged),
                },
                ModelCacheAccess::Hit { refresh: Some(ttl) },
                ModelCacheAccess::Miss,
            ] {
                let result = ModelCacheResidency::observe(
                    previous.key,
                    prior,
                    previous.clock_domain,
                    at,
                    access,
                );
                if prior.is_some_and(|value| value.version != ModelCacheResidency::VERSION) {
                    assert_eq!(result, Err(ModelCacheError::Version));
                    continue;
                }
                if prior.is_some_and(|value| at < value.observed_at_ns) {
                    assert_eq!(
                        result,
                        Err(ModelCacheError::TimeReversed),
                        "reversed cache update admitted"
                    );
                    continue;
                }
                // Wider arithmetic checks both multiplication and addition
                // independently of the production checked-u64 implementation.
                let deadline = u128::from(at) + u128::from(seconds) * 1_000_000_000;
                let refreshes = matches!(
                    access,
                    ModelCacheAccess::Write(ModelCacheTtl::Explicit { .. })
                        | ModelCacheAccess::Hit {
                            refresh: Some(ModelCacheTtl::Explicit { .. })
                        }
                );
                if refreshes && deadline > u128::from(u64::MAX) {
                    assert_eq!(
                        result,
                        Err(ModelCacheError::DeadlineOverflow),
                        "cache lifetime overflow admitted"
                    );
                    continue;
                }
                let next = result.unwrap();
                assert_eq!(
                    (next.version, next.key, next.clock_domain),
                    (
                        ModelCacheResidency::VERSION,
                        previous.key,
                        previous.clock_domain
                    )
                );
                assert_eq!(next.observed_at_ns, at);
                assert_eq!(
                    next.written_at_ns,
                    if matches!(access, ModelCacheAccess::Write(_)) {
                        Some(at)
                    } else {
                        prior.and_then(|value| value.written_at_ns)
                    }
                );
                assert_eq!(
                    next.last_hit_at_ns,
                    if matches!(access, ModelCacheAccess::Hit { .. }) {
                        Some(at)
                    } else {
                        prior.and_then(|value| value.last_hit_at_ns)
                    }
                );
                let expiry = match access {
                    ModelCacheAccess::Miss => Some(at),
                    ModelCacheAccess::Hit { refresh: None } => prior
                        .and_then(|value| value.expires_at_ns)
                        .filter(|end| at < *end),
                    _ if refreshes => Some(u64::try_from(deadline).unwrap()),
                    _ => None,
                };
                assert_eq!(next.expires_at_ns, expiry, "cache residency expiry changed");
                assert_eq!(
                    ModelCacheResidency::observe(
                        next.key,
                        Some(next),
                        next.clock_domain,
                        at,
                        access,
                    ),
                    Ok(next)
                );
            }
        }
    }
}

// Forks retain the entire settled state and refuse every pending producer.
fn snapshot_forks(snapshot: &e::Snapshot) {
    use e::protocol::Refusal;
    let before = snapshot.state();
    for common in [
        0,
        before.cursor().saturating_sub(1),
        before.cursor(),
        u64::MAX,
    ] {
        let expected = if before.cursor() > common {
            Err(Refusal::Range)
        } else if before.pending().is_some() {
            Err(Refusal::NotReady)
        } else {
            Ok(before)
        };
        assert_eq!(
            before.fork(common),
            expected,
            "producer fork admission changed"
        );
    }
}

// Preparation retains one producer even when its terminal outcome is replayed.
fn snapshot_preparations(snapshot: &e::Snapshot) {
    use e::protocol::{Pending, Refusal};
    let before = snapshot.state();
    for through in [
        before.cursor(),
        before.cursor().saturating_add(1),
        before.pending().map_or(u64::MAX, |pending| pending.through),
        u64::MAX,
    ] {
        let mut key = snapshot.epoch().key.clone();
        key.through = through;
        let summary = key.digest();
        let mut foreign = summary;
        foreign[0] ^= 1;
        for head in [through.saturating_sub(1), through, u64::MAX] {
            let expected = |identity| {
                if through <= before.cursor() || through > head {
                    Err(Refusal::Range)
                } else if before.pending().is_some_and(|pending| {
                    (pending.summary, pending.through) != (identity, through)
                }) {
                    Err(Refusal::Busy)
                } else {
                    Ok(())
                }
            };
            for identity in [
                summary,
                before.pending().map_or(summary, |pending| pending.summary),
                foreign,
            ] {
                let mut state = before;
                let result = expected(identity);
                assert_eq!(
                    state.prepare(identity, through, head),
                    result,
                    "producer preparation admission changed",
                );
                if result.is_err() || before.pending().is_some() {
                    assert_eq!(
                        state, before,
                        "preparation changed existing producer custody"
                    );
                } else {
                    assert_eq!(
                        (state.generation(), state.cursor()),
                        (before.generation(), before.cursor())
                    );
                    assert_eq!(
                        state.pending(),
                        Some(Pending {
                            summary: identity,
                            generation: before.generation(),
                            through,
                            completion: None,
                        }),
                        "new preparation lost producer custody"
                    );
                }
            }
            let mut changed = snapshot.clone();
            let result = changed.prepare(through, head);
            if let Err(refusal) = expected(summary) {
                assert!(
                    matches!(result, Err(e::SnapshotError::Transition(actual)) if actual == refusal),
                    "snapshot preparation admission changed",
                );
                assert_eq!(
                    &changed, snapshot,
                    "refused preparation changed snapshot custody"
                );
            } else {
                assert_eq!(
                    result.unwrap(),
                    summary,
                    "snapshot preparation identity changed"
                );
                let mut state = before;
                state.prepare(summary, through, head).unwrap();
                assert_eq!(
                    changed.state(),
                    state,
                    "snapshot preparation lost producer custody"
                );
                assert_eq!(changed.epoch(), snapshot.epoch());
                assert_eq!(changed.outcome(), snapshot.outcome());
                if before.pending().is_some() {
                    assert_eq!(
                        &changed, snapshot,
                        "replayed preparation changed snapshot custody"
                    );
                }
            }
        }
    }
}

// Mutate the actual writer's completion field, then compare public transitions
// with both the wire contract and the production scalar reducer.
fn snapshot_completions(original: &Value, body: &e::SummaryRef) {
    use e::protocol::{Completion, Demand, Refusal};
    for prior in [
        None,
        Some(e::SummaryOutcome::Failed),
        Some(e::SummaryOutcome::Ready(body.clone())),
    ] {
        let mut wire = original.clone();
        wire["snapshot"]["pending"]["completion"] = serde_json::to_value(&prior).unwrap();
        let mut record = envelope().clone();
        record.value = serde_json::to_vec(&wire).unwrap().into();
        let before = e::Snapshot::decode(&record, &source(), u64::MAX).unwrap();
        let summary = before.state().pending().unwrap().summary;
        let expected_demand = match &prior {
            None => Demand::Await(summary),
            Some(e::SummaryOutcome::Failed) => Demand::Failed(summary),
            Some(e::SummaryOutcome::Ready(_)) => Demand::Swap(summary),
        };
        assert_eq!(
            before.state().demand(),
            expected_demand,
            "snapshot terminal demand changed"
        );
        let mut state = before.state();
        if !matches!(prior, Some(e::SummaryOutcome::Ready(_))) {
            assert_eq!(
                state.swap(summary, state.generation(), u64::MAX),
                Err(Refusal::NotReady),
                "producer swap admission changed",
            );
            assert_eq!(
                state,
                before.state(),
                "refused swap changed producer custody"
            );
        }
        let mut foreign = summary;
        foreign[0] ^= 1;
        for desired in [
            e::SummaryOutcome::Failed,
            e::SummaryOutcome::Ready(body.clone()),
        ] {
            let completion = match &desired {
                e::SummaryOutcome::Failed => Completion::Failed,
                e::SummaryOutcome::Ready(_) => Completion::Ready,
            };
            for identity in [summary, foreign] {
                let expected_result = if identity != summary {
                    Err(Refusal::Stale)
                } else if prior.is_none() || prior.as_ref() == Some(&desired) {
                    Ok(())
                } else {
                    Err(Refusal::Contradiction)
                };
                let mut state = before.state();
                assert_eq!(
                    state.complete(identity, completion),
                    expected_result,
                    "producer completion admission changed"
                );
                let mut changed = before.clone();
                let result = changed.complete(identity, desired.clone());
                if let Err(refusal) = expected_result {
                    assert!(
                        matches!(result, Err(e::SnapshotError::Transition(actual)) if actual == refusal),
                        "snapshot completion admission changed"
                    );
                    assert_eq!(
                        changed, before,
                        "refused completion changed snapshot custody"
                    );
                    assert_eq!(
                        state,
                        before.state(),
                        "refused completion changed producer custody"
                    );
                    continue;
                }
                result.unwrap();
                wire["snapshot"]["pending"]["completion"] = serde_json::to_value(&desired).unwrap();
                record.value = serde_json::to_vec(&wire).unwrap().into();
                let expected = e::Snapshot::decode(&record, &source(), u64::MAX).unwrap();
                assert_eq!(
                    changed, expected,
                    "completed snapshot lost producer custody"
                );
                assert_eq!(
                    state,
                    expected.state(),
                    "completion reducer lost producer custody"
                );
                assert_eq!(
                    state.demand(),
                    match &desired {
                        e::SummaryOutcome::Failed => Demand::Failed(summary),
                        e::SummaryOutcome::Ready(_) => Demand::Swap(summary),
                    },
                    "snapshot terminal demand changed"
                );
            }
        }
    }
}

// Full result references distinguish two Ready outcomes with the same completion.
fn snapshot_transitions(snapshot: &e::Snapshot, wire: &Value) {
    let Some(pending) = snapshot.state().pending() else {
        return;
    };
    let Some(e::SummaryOutcome::Ready(body)) = snapshot.outcome() else {
        return;
    };
    snapshot_completions(wire, body);
    let Some(mut settled) = snapshot_swaps(snapshot) else {
        return;
    };
    let Some(through) = pending.through.checked_add(1) else {
        return;
    };
    let summary = settled.prepare(through, u64::MAX).unwrap();
    settled
        .complete(summary, e::SummaryOutcome::Ready(body.clone()))
        .unwrap();
    let Ok(bytes) = encoded(&settled) else {
        return;
    };
    // Use the actual next writer record, then mutate only its matching fences.
    // This is full-width admission coverage, not a claim of MAX prior swaps.
    let mut boundary: Value = serde_json::from_slice(&bytes).unwrap();
    boundary["snapshot"]["generation"] = u64::MAX.into();
    boundary["snapshot"]["pending"]["generation"] = u64::MAX.into();
    let mut record = envelope().clone();
    record.value = serde_json::to_vec(&boundary).unwrap().into();
    let boundary = e::Snapshot::decode(&record, &source(), u64::MAX).unwrap();
    assert!(snapshot_swaps(&boundary).is_none());
}

fn snapshot_swaps(snapshot: &e::Snapshot) -> Option<e::Snapshot> {
    use e::protocol::Refusal;
    let pending = snapshot.state().pending()?;
    let e::SummaryOutcome::Ready(body) = snapshot.outcome()? else {
        return None;
    };
    let mut foreign = pending.summary;
    foreign[0] ^= 1;
    for (summary, head, refusal, diagnostic) in [
        (
            foreign,
            u64::MAX,
            Refusal::Stale,
            "producer swap identity admission changed",
        ),
        (
            pending.summary,
            pending.through - 1,
            Refusal::Range,
            "producer swap range admission changed",
        ),
    ] {
        let mut state = snapshot.state();
        assert_eq!(
            state.swap(summary, state.generation(), head),
            Err(refusal),
            "{diagnostic}",
        );
        assert_eq!(
            state,
            snapshot.state(),
            "refused swap changed producer custody"
        );
    }
    let mut replay = snapshot.clone();

    // Change only the immutable revision in the admitted producer's canonical
    // reference. This is a metadata mutation, never authenticated file content.
    let mut changed = serde_json::to_value(body).unwrap();
    changed["portable"]["version"] = if body.portable().version() == "fuzz-alternate" {
        "fuzz-original"
    } else {
        "fuzz-alternate"
    }
    .into();
    let other: e::SummaryRef = serde_json::from_value(changed).unwrap();
    assert_ne!(other, *body);
    assert!(
        matches!(
            replay.complete(pending.summary, e::SummaryOutcome::Ready(other.clone())),
            Err(e::SnapshotError::Transition(
                e::protocol::Refusal::Contradiction
            ))
        ),
        "different terminal snapshot reference admitted",
    );
    assert_eq!(
        replay, *snapshot,
        "refused completion changed snapshot custody"
    );

    let mut next = snapshot.epoch().clone();
    next.key.through = pending.through;
    next.summary = Some(body.clone());
    let Some(generation) = snapshot.state().generation().checked_add(1) else {
        let mut state = snapshot.state();
        assert_eq!(
            state.swap(pending.summary, state.generation(), u64::MAX),
            Err(Refusal::Exhausted),
            "producer swap exhaustion admission changed",
        );
        assert_eq!(
            state,
            snapshot.state(),
            "refused swap changed producer custody"
        );
        assert!(
            matches!(
                replay.swap(snapshot.state().generation(), next, u64::MAX),
                Err(e::SnapshotError::Transition(Refusal::Exhausted))
            ),
            "snapshot swap exhaustion admission changed",
        );
        assert_eq!(replay, *snapshot, "refused swap changed snapshot custody");
        return None;
    };
    next.summary = Some(other);
    assert!(
        matches!(
            replay.swap(snapshot.state().generation(), next.clone(), u64::MAX),
            Err(e::SnapshotError::ResultReference)
        ),
        "different swap result reference admitted",
    );
    assert_eq!(replay, *snapshot, "refused swap changed snapshot custody");
    next.summary = Some(body.clone());
    assert!(
        matches!(
            replay.swap(snapshot.state().generation() ^ 1, next.clone(), u64::MAX),
            Err(e::SnapshotError::Transition(e::protocol::Refusal::Stale))
        ),
        "snapshot stale generation admitted",
    );
    assert_eq!(replay, *snapshot, "refused swap changed snapshot custody");
    replay
        .swap(snapshot.state().generation(), next.clone(), u64::MAX)
        .unwrap();
    assert_eq!(replay.epoch(), &next);
    assert_eq!(replay.state().generation(), generation);
    assert_eq!(replay.state().cursor(), pending.through);
    assert!(replay.state().pending().is_none());
    assert!(replay.outcome().is_none());
    let mut settled = replay.state();
    assert_eq!(
        settled.swap(pending.summary, generation, u64::MAX),
        Err(e::protocol::Refusal::Stale),
        "unprepared producer swap admitted",
    );
    assert_eq!(
        settled,
        replay.state(),
        "refused swap changed producer custody"
    );
    Some(replay)
}

// Receipt fields are fuzz mutations of the real envelope, not publication
// authority. The original journal tests retain that separate obligation.
fn snapshot_confirmation(record: &Record) {
    let original = record.commit_id;
    let mut different = *original.as_bytes();
    different[0] ^= 1;
    let different = acyclic_stream::CommitId::from_bytes(different);
    for sequence in [0, u64::MAX - 1, u64::MAX] {
        let mut admitted = record.clone();
        admitted.sequence = sequence;
        let before = e::Snapshot::decode(&admitted, &source(), u64::MAX).unwrap();
        for commit in [original, different] {
            for next in [0, sequence, sequence.saturating_add(1), u64::MAX] {
                // Widen before incrementing: no wrapping arithmetic from the
                // production implementation is reused by this predicate.
                let accepted = (next == sequence && commit == original)
                    || (commit != original && u128::from(next) == u128::from(sequence) + 1);
                let mut changed = before.clone();
                let result = changed.confirm(next, commit);
                assert!(
                    if accepted {
                        result.is_ok()
                    } else {
                        matches!(
                            result,
                            Err(e::SnapshotError::Transition(e::protocol::Refusal::Stale))
                        )
                    },
                    "snapshot receipt admission changed",
                );
                if accepted {
                    let mut confirmed = admitted.clone();
                    confirmed.sequence = next;
                    confirmed.commit_id = commit;
                    assert_eq!(
                        changed,
                        e::Snapshot::decode(&confirmed, &source(), u64::MAX).unwrap(),
                        "confirmed snapshot lost receipt custody",
                    );
                } else {
                    assert_eq!(
                        changed, before,
                        "refused confirmation changed snapshot custody"
                    );
                }
            }
        }
    }
}

fn source() -> StreamPath {
    StreamPath::new("corpus/agent/conversation").unwrap()
}

fn io() -> PreparationIo {
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .unwrap();
    PreparationIo::from_native_work(&work).unwrap()
}

fn genesis() -> e::Snapshot {
    e::Snapshot::genesis(e::Epoch {
        header: [1; 32],
        profile: [2; 32],
        key: e::SummaryKey {
            source: source(),
            through: 0,
            // Retain the renderer input recorded in resource-epoch-genesis.
            renderer: 3,
            model: [3; 32],
        },
        summary: None,
    })
    .unwrap()
}

fn encoded(snapshot: &e::Snapshot) -> acyclic_harness_core::Result<Vec<u8>> {
    let io = io();
    let owner = snapshot.encode(StreamPath::new("corpus/agent/epochs").unwrap(), &io)?;
    let bytes = owner.parts().1.to_vec();
    let mut record = envelope().clone();
    record.value = bytes.clone().into();
    let restored = e::Snapshot::decode(&record, &source(), u64::MAX)
        .expect("original writer snapshot refused");
    // A decoder/encoder round trip alone can preserve the same loss twice.
    // Compare against the original producer before any admission has occurred.
    assert_eq!(
        (restored.epoch(), restored.state(), restored.outcome()),
        (snapshot.epoch(), snapshot.state(), snapshot.outcome()),
        "writer snapshot lost epoch or producer custody",
    );
    Ok(bytes)
}

// Keep the original envelope from one real writer/commit/read. Fuzzing replaces
// only its value; this does not turn a mutation into authenticated stream data.
fn envelope() -> &'static Record {
    static RECORD: OnceLock<Record> = OnceLock::new();
    RECORD.get_or_init(|| {
        use acyclic_stream::{
            AppendRecord, CommitMutation, CommitRequest, IdempotencyKey, MemoryStream, ReadRequest,
            StreamProvider, StreamProviderExt as _,
        };
        use futures::TryStreamExt as _;
        futures::executor::block_on(async {
            let provider = MemoryStream::default();
            let path = StreamPath::new("corpus/agent/epochs").unwrap();
            let snapshot = genesis();
            let io = io();
            let owner = snapshot.encode(path.clone(), &io).unwrap();
            provider
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    conditions: vec![owner.parts().0.clone()],
                    mutations: vec![CommitMutation::Append {
                        path: path.clone(),
                        records: vec![AppendRecord::from(owner.parts().1.clone())],
                    }],
                    idempotency_key: IdempotencyKey::unique(),
                })
                .await
                .unwrap();
            let mut records = provider
                .read(ReadRequest {
                    key: None,
                    path,
                    from: 0,
                    limit: 1,
                    at_least: Default::default(),
                })
                .await
                .unwrap();
            records.try_next().await.unwrap().unwrap()
        })
    })
}

pub(super) fn check(bytes: &[u8]) {
    let _ = roundtrip::<ModelCacheBreakpoint>(bytes);
    let _ = roundtrip::<ModelCacheTtl>(bytes);
    let _ = roundtrip::<ModelCacheKey>(bytes);
    if let Some(previous) = roundtrip::<ModelCacheResidency>(bytes) {
        residency(previous);
    }
    if let Ok(metadata) = serde_json::from_slice::<Value>(bytes) {
        let expected = metadata
            .get("cache")
            .and_then(|cache| serde_json::from_value::<ModelCacheObservation>(cache.clone()).ok())
            .filter(|cache| cache.version == ModelCacheObservation::VERSION);
        assert_eq!(ModelCacheObservation::reported(&metadata), expected);
    }
    if let Some(cache) = roundtrip::<ModelCacheObservation>(bytes) {
        let mut metadata = serde_json::json!({"cache": cache});
        for version in [cache.version, 0, 2, u32::MAX] {
            let observed = ModelCacheObservation { version, ..cache };
            let expected = match (version, cache.credential_scope, cache.breakpoint) {
                (1, Some(scope), Some(cut)) => Some((scope, cut.digest)),
                _ => None,
            };
            assert_eq!(
                observed
                    .key()
                    .map(|key| (key.credential_scope, key.prefix_digest)),
                expected,
                "cache observation key identity changed",
            );
            metadata["cache"]["version"] = version.into();
            assert_eq!(
                ModelCacheObservation::reported(&metadata),
                (version == ModelCacheObservation::VERSION).then_some(observed),
                "unsupported cache observation version admitted",
            );
        }
    }
    if let Some(value) = roundtrip::<e::SummaryRef>(bytes) {
        summary(&value);
    }
    let _ = roundtrip::<e::SummaryOutcome>(bytes);
    if let Some(value) = roundtrip::<e::Epoch>(bytes) {
        epoch(&value);
    }
    if let Some(value) = roundtrip::<e::SummaryKey>(bytes) {
        summary_key(&value);
    }
    let mut record = envelope().clone();
    record.value = bytes.to_vec().into();
    let Ok(snapshot) = e::Snapshot::decode(&record, &source(), u64::MAX) else {
        return;
    };
    epoch(snapshot.epoch());
    snapshot_confirmation(&record);
    let state = snapshot.state();
    if let Some(head) = state.cursor().checked_sub(1) {
        // Reach the reducer directly: epoch admission otherwise masks this
        // guard. Remove pending work so its range cannot conceal cursor loss.
        assert_eq!(
            e::protocol::State::restore(state.generation(), state.cursor(), None, head),
            Err(e::protocol::Refusal::Range),
            "producer cursor beyond source head admitted",
        );
    }
    assert!(
        matches!(
            e::Snapshot::decode(
                &record,
                &StreamPath::new("corpus/foreign/conversation").unwrap(),
                u64::MAX,
            ),
            Err(e::SnapshotError::Epoch(e::InvalidEpoch::Source)),
        ),
        "snapshot foreign source admission changed",
    );
    if state.pending().is_none()
        && let Some(head) = state.cursor().checked_sub(1)
    {
        assert!(
            matches!(
                e::Snapshot::decode(&record, &source(), head),
                Err(e::SnapshotError::Epoch(e::InvalidEpoch::OutsidePrefix)),
            ),
            "settled epoch beyond source head admitted",
        );
    }
    // Mutated values may exceed the stream writer's protective frame size.
    // Such a writer refusal is not a decoder semantic failure.
    let Ok(written) = encoded(&snapshot) else {
        return;
    };
    // Serde also admits positional struct fields. Mutate the same writer's
    // canonical object spelling instead of assuming the admitted input's shape.
    let original: Value = serde_json::from_slice(&written).unwrap();
    snapshot_forks(&snapshot);
    snapshot_preparations(&snapshot);
    snapshot_transitions(&snapshot, &original);
    record.value = written.into();
    assert_eq!(
        e::Snapshot::decode(&record, &source(), u64::MAX).unwrap(),
        snapshot
    );
    let mut changed = original.clone();
    let generation = if state.generation() == 0 { 1 } else { 0 };
    changed["snapshot"]["generation"] = generation.into();
    if state.pending().is_some() {
        changed["snapshot"]["pending"]["generation"] = generation.into();
    }
    record.value = serde_json::to_vec(&changed).unwrap().into();
    assert!(
        matches!(
            e::Snapshot::decode(&record, &source(), u64::MAX),
            Err(e::SnapshotError::Transition(e::protocol::Refusal::Stale)),
        ),
        "snapshot generation parity admission changed",
    );
    for version in ["0", "1", "3"] {
        let mut changed = original.clone();
        changed["version"] = version.into();
        record.value = serde_json::to_vec(&changed).unwrap().into();
        assert!(
            matches!(
                e::Snapshot::decode(&record, &source(), u64::MAX),
                Err(e::SnapshotError::Encoding(_)),
            ),
            "unsupported snapshot version admitted",
        );
    }
    if let Some(pending) = state.pending() {
        // Leave the epoch generation unchanged to reach the restored pending
        // identity fence, rather than the earlier epoch/generation parity check.
        let mut changed = original.clone();
        changed["snapshot"]["pending"]["generation"] = (pending.generation ^ 1).into();
        record.value = serde_json::to_vec(&changed).unwrap().into();
        assert!(
            matches!(
                e::Snapshot::decode(&record, &source(), u64::MAX),
                Err(e::SnapshotError::Transition(e::protocol::Refusal::Stale)),
            ),
            "foreign pending epoch generation admitted",
        );
        for (through, head, diagnostic) in [
            (
                Some(state.cursor()),
                u64::MAX,
                "already covered pending epoch range admitted",
            ),
            (
                pending.through.checked_add(1),
                pending.through,
                "pending epoch range beyond source head admitted",
            ),
        ] {
            let Some(through) = through else { continue };
            // Preserve the content address so the later identity fence cannot
            // conceal a missing range check in the original restore reducer.
            let mut key = snapshot.epoch().key.clone();
            key.through = through;
            let mut changed = original.clone();
            changed["snapshot"]["pending"]["through"] = through.into();
            changed["snapshot"]["pending"]["summary"] = serde_json::to_value(key.digest()).unwrap();
            record.value = serde_json::to_vec(&changed).unwrap().into();
            assert!(
                matches!(
                    e::Snapshot::decode(&record, &source(), head),
                    Err(e::SnapshotError::Transition(e::protocol::Refusal::Range)),
                ),
                "{diagnostic}",
            );
        }
        let mut changed = original;
        changed["snapshot"]["pending"]["summary"][0] = (pending.summary[0] ^ 1).into();
        record.value = serde_json::to_vec(&changed).unwrap().into();
        assert!(
            matches!(
                e::Snapshot::decode(&record, &source(), u64::MAX),
                Err(e::SnapshotError::Identity),
            ),
            "foreign snapshot preparation identity admitted",
        );
    }
}

/// Generated internal writer inputs, not captured provider or hosted traffic.
pub fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>, Box<dyn Error>> {
    let volume = c::VolumeRef::new(
        ProviderRef::new("example".into(), "filesystem".into(), "1".into(), &mut None)?,
        "summaries".into(),
        c::VolumeClass::Project,
        c::VolumeOwner::Project("corpus".into()),
        &mut None,
    )?;
    let portable = c::FileRef::new(
        volume,
        "summary.txt".into(),
        "one".into(),
        c::FileDescriptor::from_bytes(b"summary", "text/plain", &mut None)?,
        "summary.txt".into(),
        &mut None,
    )?;
    let plain = e::SummaryRef::new(portable.clone(), Vec::new())?;
    let summary = e::SummaryRef::new(
        portable.clone(),
        vec![e::NativeSummary {
            provenance: acyclic_model_render::Provenance {
                family: acyclic_model_render::Family::Anthropic,
                provider: "example".into(),
                model: "summary-model".into(),
                endpoint: "https://example.invalid/v1".into(),
                credential_scope: Some([9; 32]),
            },
            artifact: portable,
        }],
    )?;
    let breakpoint = ModelCacheBreakpoint {
        digest: [4; 32],
        parent_digest: [5; 32],
        messages: 2,
    };
    let cache = ModelCacheObservation {
        version: ModelCacheObservation::VERSION,
        credential_scope: Some([6; 32]),
        profile_prefix_digest: None,
        ttl: ModelCacheTtl::Explicit { seconds: 300 },
        breakpoint: Some(breakpoint),
    };
    let mut seeds = vec![
        ("summary", serde_json::to_vec(&summary)?),
        ("summary-portable", serde_json::to_vec(&plain)?),
        ("cache", serde_json::to_vec(&cache)?),
        (
            "cache-metadata",
            serde_json::to_vec(&serde_json::json!({"cache": cache}))?,
        ),
        ("cache-breakpoint", serde_json::to_vec(&breakpoint)?),
        ("cache-ttl", serde_json::to_vec(&cache.ttl)?),
        ("cache-key", serde_json::to_vec(&cache.key().unwrap())?),
        (
            "cache-unknown",
            serde_json::to_vec(&ModelCacheObservation {
                credential_scope: None,
                breakpoint: None,
                ttl: ModelCacheTtl::ProviderManaged,
                ..cache
            })?,
        ),
        ("epoch-genesis", envelope().value.to_vec()),
    ];
    for (name, access) in [
        ("cache-residency", ModelCacheAccess::Write(cache.ttl)),
        (
            "cache-residency-unknown",
            ModelCacheAccess::Hit { refresh: None },
        ),
    ] {
        seeds.push((
            name,
            serde_json::to_vec(&ModelCacheResidency::observe(
                cache.key().unwrap(),
                None,
                [7; 16],
                10,
                access,
            )?)?,
        ));
    }
    let mut prepared = genesis();
    let id = prepared.prepare(10, 12)?;
    seeds.push(("epoch-prepared", encoded(&prepared)?));
    let mut failed = prepared.clone();
    failed.complete(id, e::SummaryOutcome::Failed)?;
    seeds.push(("epoch-failed", encoded(&failed)?));
    prepared.complete(id, e::SummaryOutcome::Ready(summary.clone()))?;
    seeds.push(("epoch-ready", encoded(&prepared)?));
    let mut next = prepared.epoch().clone();
    next.key.through = 10;
    next.summary = Some(summary);
    prepared.swap(0, next, 12)?;
    seeds.push(("epoch-swapped", encoded(&prepared)?));
    seeds.push(("epoch", serde_json::to_vec(prepared.epoch())?));
    seeds.push(("summary-key", serde_json::to_vec(&prepared.epoch().key)?));
    seeds.push((
        "summary-outcome",
        serde_json::to_vec(&e::SummaryOutcome::Failed)?,
    ));
    for (original, alternate) in [
        ("epoch-genesis", "epoch-genesis-positional"),
        ("epoch-prepared", "epoch-prepared-positional"),
        ("epoch-failed", "epoch-failed-positional"),
        ("epoch-ready", "epoch-ready-positional"),
        ("epoch-swapped", "epoch-swapped-positional"),
    ] {
        let bytes = &seeds.iter().find(|(name, _)| *name == original).unwrap().1;
        let mut value: Value = serde_json::from_slice(bytes)?;
        let snapshot = value["snapshot"].as_object_mut().unwrap();
        let fields =
            ["epoch", "generation", "pending"].map(|field| snapshot.remove(field).unwrap());
        value["snapshot"] = Value::Array(fields.into());
        let bytes = serde_json::to_vec(&value)?;
        let mut record = envelope().clone();
        record.value = bytes.clone().into();
        // Require the real decoder to admit the alternate spelling before
        // handing it to the oracle; this is derived from the same writer bytes.
        e::Snapshot::decode(&record, &source(), u64::MAX)?;
        seeds.push((alternate, bytes));
    }
    Ok(seeds)
}
