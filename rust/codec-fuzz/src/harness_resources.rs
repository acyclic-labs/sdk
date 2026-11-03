//! Resource formats use their production admission and snapshot writer.
//! Stream authentication and provider attribution are outside these codec checks.
use acyclic_harness_core::{
    conversation::{self as c, epochs as e},
    executor::{
        ModelCacheBreakpoint, ModelCacheKey, ModelCacheObservation, ModelCacheResidency,
        ModelCacheTtl,
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
    assert!(serde_json::from_value::<e::SummaryRef>(changed).is_err());
    for index in 0..value.native().len() {
        for field in ["provider", "model", "endpoint"] {
            let mut changed = original.clone();
            changed["native"][index]["provenance"][field] = " ".into();
            assert!(serde_json::from_value::<e::SummaryRef>(changed).is_err());
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
    Ok(owner.parts().1.to_vec())
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
    let _ = roundtrip::<ModelCacheResidency>(bytes);
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
    let _ = roundtrip::<e::Epoch>(bytes);
    let _ = roundtrip::<e::SummaryKey>(bytes);
    let mut record = envelope().clone();
    record.value = bytes.to_vec().into();
    let Ok(snapshot) = e::Snapshot::decode(&record, &source(), u64::MAX) else {
        return;
    };
    let state = snapshot.state();
    assert!(
        e::Snapshot::decode(
            &record,
            &StreamPath::new("corpus/foreign/conversation").unwrap(),
            u64::MAX,
        )
        .is_err()
    );
    let through = state
        .pending()
        .map_or(state.cursor(), |pending| pending.through);
    if let Some(head) = through.checked_sub(1) {
        assert!(e::Snapshot::decode(&record, &source(), head).is_err());
    }
    // Mutated values may exceed the stream writer's protective frame size.
    // Such a writer refusal is not a decoder semantic failure.
    if let Ok(written) = encoded(&snapshot) {
        record.value = written.into();
        assert_eq!(
            e::Snapshot::decode(&record, &source(), u64::MAX).unwrap(),
            snapshot
        );
    }
    let original: Value = serde_json::from_slice(bytes).unwrap();
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
        (
            "cache-residency",
            serde_json::to_vec(&ModelCacheResidency::observe(
                cache.key().unwrap(),
                None,
                [7; 16],
                10,
                acyclic_harness_core::executor::ModelCacheAccess::Write(cache.ttl),
            )?)?,
        ),
        ("epoch-genesis", envelope().value.to_vec()),
    ];
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
    Ok(seeds)
}
