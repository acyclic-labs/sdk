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

fn epoch(value: &e::Epoch) {
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

// Exercise the public wrapper as well as its scalar protocol reducer. Full
// result references distinguish two Ready outcomes with the same completion.
fn snapshot_transitions(snapshot: &e::Snapshot) {
    let Some(pending) = snapshot.state().pending() else {
        return;
    };
    let Some(e::SummaryOutcome::Ready(body)) = snapshot.outcome() else {
        return;
    };
    let mut replay = snapshot.clone();
    replay
        .complete(pending.summary, e::SummaryOutcome::Ready(body.clone()))
        .unwrap();
    assert_eq!(
        replay, *snapshot,
        "terminal snapshot replay changed custody"
    );

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

    let Some(generation) = snapshot.state().generation().checked_add(1) else {
        return;
    };
    let mut next = snapshot.epoch().clone();
    next.key.through = pending.through;
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
    replay
        .swap(snapshot.state().generation(), next.clone(), u64::MAX)
        .unwrap();
    assert_eq!(replay.epoch(), &next);
    assert_eq!(replay.state().generation(), generation);
    assert_eq!(replay.state().cursor(), pending.through);
    assert!(replay.state().pending().is_none());
    assert!(replay.outcome().is_none());
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
            renderer: acyclic_model_render::RENDERER_VERSION,
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
        assert_eq!(
            ModelCacheObservation::reported(&metadata),
            (cache.version == ModelCacheObservation::VERSION).then_some(cache),
        );
        assert_eq!(
            cache.key().is_some(),
            cache.version == ModelCacheObservation::VERSION
                && cache.credential_scope.is_some()
                && cache.breakpoint.is_some(),
        );
        for version in [0, 2, u32::MAX] {
            metadata["cache"]["version"] = version.into();
            assert!(
                ModelCacheObservation::reported(&metadata).is_none(),
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
    let _ = roundtrip::<e::SummaryKey>(bytes);
    let mut record = envelope().clone();
    record.value = bytes.to_vec().into();
    let Ok(snapshot) = e::Snapshot::decode(&record, &source(), u64::MAX) else {
        return;
    };
    epoch(snapshot.epoch());
    snapshot_transitions(&snapshot);
    let state = snapshot.state();
    assert!(
        e::Snapshot::decode(
            &record,
            &StreamPath::new("corpus/foreign/conversation").unwrap(),
            u64::MAX,
        )
        .is_err()
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
    assert!(e::Snapshot::decode(&record, &source(), u64::MAX).is_err());
    for version in ["0", "1", "3"] {
        let mut changed = original.clone();
        changed["version"] = version.into();
        record.value = serde_json::to_vec(&changed).unwrap().into();
        assert!(e::Snapshot::decode(&record, &source(), u64::MAX).is_err());
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
