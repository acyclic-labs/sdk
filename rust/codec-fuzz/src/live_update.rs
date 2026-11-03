//! Exercise the public deployment documents through the shipped v1 codec.
use acyclic_actors::{guest, live_update as live};
use std::{collections::BTreeMap, error::Error, num::NonZeroU64};

/// Compare admission with the production writer for every public document type.
/// Parsing the candidate value separately makes refusal as well as acceptance
/// observable: neither reject-all nor accepting noncanonical bytes can pass.
pub fn check(bytes: &[u8]) {
    let document = serde_json::from_slice::<serde_json::Value>(bytes).ok();
    let value = document.as_ref().and_then(|document| document.get("value"));
    let v1 = document.as_ref().is_some_and(|document| {
        document.as_object().is_some_and(|object| object.len() == 2)
            && document.get("v").and_then(serde_json::Value::as_u64) == Some(1)
    });
    macro_rules! documents {
        ($($ty:ty),+ $(,)?) => {$({
            let encoded = value.and_then(|value| {
                serde_json::from_value::<$ty>(value.clone()).ok()
            }).map(|value| live::encode(&value).expect("live-update writer"));
            let expected = v1 && encoded.as_deref() == Some(bytes);
            let admitted = live::decode::<$ty>(bytes);
            assert_eq!(admitted.is_ok(), expected,
                "live-update document admission differs from canonical writer: {}", stringify!($ty));
            if let Ok(value) = admitted {
                assert_eq!(live::encode(&value).unwrap(), bytes);
            }
        })+};
    }
    documents!(
        live::DeploymentId,
        live::Version,
        live::Artifact,
        live::Pin,
        live::Fraction,
        live::Split,
        live::Selection,
        live::ChangeKind,
        live::SelectionChange,
        live::Schema,
        live::RpcSchema,
        live::PeerRange,
        live::HandoffRequirements,
        live::Adapter,
        live::CompatibilityManifest,
        live::Publication,
        live::CallEnvelope,
        live::JournalPosition,
        live::DurableAwait,
        live::CallPair,
        live::RolloutPolicy,
        live::GuardDecision,
        live::RolloutStatus,
    );
    let expected = live::decode::<live::CallEnvelope>(bytes)
        .ok()
        .filter(|envelope| envelope.validate().is_ok());
    assert_eq!(live::decode_envelope(bytes).ok(), expected);
}

/// Capture real public writer output, including nested optional/enum variants
/// and decimal boundaries. These are codec inputs, not physical host evidence.
pub fn seeds() -> Result<Vec<(&'static str, Vec<u8>, &'static str)>, Box<dyn Error>> {
    let mut seeds = Vec::new();
    macro_rules! save {
        ($name:literal, $value:expr, $ty:ty) => {{
            let value: $ty = $value;
            let bytes = live::encode(&value)?;
            assert_eq!(
                live::decode::<$ty>(&bytes)?,
                value,
                "live-update writer must be admitted"
            );
            seeds.push((
                concat!("live-update-", $name),
                bytes,
                concat!("live_update::encode::<", stringify!($ty), ">"),
            ));
        }};
    }
    let deployment = live::DeploymentId {
        account: b"account".to_vec(),
        name: "codec".into(),
    };
    let version = live::Version {
        number: NonZeroU64::MIN,
        digest: [1; 32],
    };
    let target = live::Version {
        number: NonZeroU64::MAX,
        digest: [2; 32],
    };
    let wave = live::Fraction {
        numerator: u64::MAX,
        denominator: NonZeroU64::MAX,
    };
    let split = live::Split {
        stable: version.clone(),
        target: target.clone(),
        wave: wave.clone(),
    };
    let selection = live::Selection {
        deployment: deployment.clone(),
        epoch: NonZeroU64::MIN,
        split: split.clone(),
    };
    let codec = guest::CodecIdentity {
        name: "continuation".into(),
        version: "1".into(),
    };
    let schema = live::Schema {
        descriptor_set: acyclic_actors::FILE_DESCRIPTOR_SET.to_vec(),
        message: "acyclic.actors.v1.InvokeActorRequest".into(),
    };
    let rpc = live::RpcSchema {
        request: schema.clone(),
        reply: schema.clone(),
    };
    let peer = live::PeerRange {
        deployment: deployment.clone(),
        first: NonZeroU64::MIN,
        last: None,
    };
    let handoff = live::HandoffRequirements {
        writes: codec.clone(),
        reads: vec![codec.clone()],
        journal_reads: vec![codec.clone()],
        durable_await: true,
    };
    let adapter = live::Adapter {
        from: [1; 32],
        to: [2; 32],
        code: [3; 32],
    };
    let compatibility = live::CompatibilityManifest {
        rpcs: BTreeMap::from([("call".into(), rpc.clone())]),
        messages: BTreeMap::from([("message".into(), schema.clone())]),
        peers: vec![peer.clone()],
        handoff: handoff.clone(),
        adapters: vec![adapter.clone()],
    };
    let artifact = live::Artifact {
        code: [1; 32],
        configuration: [2; 32],
        schema: compatibility.schema_digest()?,
    };
    let publication = live::Publication {
        deployment: deployment.clone(),
        expected_version: 0,
        artifacts: BTreeMap::from([("namespace".into(), artifact.clone())]),
        compatibility: compatibility.clone(),
    };
    publication.version()?;
    let actor =
        acyclic_actors::addressing::ActorReference::new(b"account", b"namespace", b"actor")?;
    let pin = live::Pin::Actor(actor.encode());
    let envelope = live::CallEnvelope {
        deployment: deployment.clone(),
        pin: pin.clone(),
        epoch: NonZeroU64::MAX,
        version: target.clone(),
        contract: [3; 32],
    };
    assert_eq!(live::decode_envelope(&live::encode(&envelope)?)?, envelope);
    let position = live::JournalPosition {
        stream: acyclic_stream::StreamPath::new("codec/journal")?,
        commit: [4; 32],
        sequence: u64::MAX,
        operation: b"operation".to_vec(),
    };
    let pending = live::DurableAwait {
        continuation: guest::Checkpoint {
            codec,
            bytes: bytes::Bytes::from_static(b"continuation"),
        },
        pending: vec![position.clone()],
    };
    let calls = live::CallPair {
        caller: version.clone(),
        callee: target.clone(),
        caller_deployment: deployment.clone(),
        callee_deployment: deployment.clone(),
        attempted: u64::MAX,
        completed: 0,
        failures: BTreeMap::from([("unknown".into(), u64::MAX)]),
        queue_micros: 0,
        latency_micros: u64::MAX,
    };
    let policy = live::RolloutPolicy {
        minimum_completed: NonZeroU64::MIN,
        maximum_failure_fraction: wave.clone(),
        maximum_mean_queue_micros: None,
        maximum_mean_latency_micros: Some(NonZeroU64::MAX),
    };
    save!("deployment", deployment, live::DeploymentId);
    save!("version", version, live::Version);
    save!("artifact", artifact, live::Artifact);
    save!("actor-pin", pin, live::Pin);
    save!("cohort-pin", live::Pin::Cohort("cohort".into()), live::Pin);
    save!("fraction", wave, live::Fraction);
    save!("split", split, live::Split);
    save!("selection", selection.clone(), live::Selection);
    save!("change-kind", live::ChangeKind::Promote, live::ChangeKind);
    save!(
        "promotion",
        selection.promote(selection.split.clone())?,
        live::SelectionChange
    );
    let rollback = live::Split {
        stable: target,
        target: selection.split.stable.clone(),
        wave: selection.split.wave.clone(),
    };
    save!(
        "rollback",
        selection.rollback(rollback)?,
        live::SelectionChange
    );
    save!("schema", schema, live::Schema);
    save!("rpc", rpc, live::RpcSchema);
    save!("peer-unbounded", peer.clone(), live::PeerRange);
    save!(
        "peer-bounded",
        live::PeerRange {
            last: Some(NonZeroU64::MAX),
            ..peer
        },
        live::PeerRange
    );
    save!("handoff", handoff, live::HandoffRequirements);
    save!("adapter", adapter, live::Adapter);
    save!("compatibility", compatibility, live::CompatibilityManifest);
    save!("publication", publication, live::Publication);
    save!("envelope", envelope, live::CallEnvelope);
    save!("journal", position, live::JournalPosition);
    save!("durable-await", pending, live::DurableAwait);
    save!("calls", calls.clone(), live::CallPair);
    save!("policy", policy, live::RolloutPolicy);
    for (name, status_name, guard) in [
        (
            "live-update-guard-awaiting",
            "live-update-status-awaiting",
            live::GuardDecision::AwaitingCoverage,
        ),
        (
            "live-update-guard-advance",
            "live-update-status-advance",
            live::GuardDecision::Advance { policy: [1; 32] },
        ),
        (
            "live-update-guard-rollback",
            "live-update-status-rollback",
            live::GuardDecision::Rollback {
                policy: [2; 32],
                reason: "failed".into(),
            },
        ),
        (
            "live-update-guard-blocked",
            "live-update-status-blocked",
            live::GuardDecision::HandoffBlocked {
                reason: "unknown".into(),
            },
        ),
    ] {
        seeds.push((
            name,
            live::encode(&guard)?,
            "live_update::encode::<GuardDecision>",
        ));
        let status = live::RolloutStatus {
            selection: selection.clone(),
            next_page: Some(vec![0]),
            adopted: u64::MAX,
            guard,
            calls: vec![calls.clone()],
            rollback_epoch: Some(NonZeroU64::MAX),
        };
        // The outer status retains the same guard variant in each capture.
        seeds.push((
            status_name,
            live::encode(&status)?,
            "live_update::encode::<RolloutStatus>",
        ));
    }
    // Derive refusal inputs from an actual writer, rather than inventing frames.
    let original = live::encode(&selection)?;
    let mut whitespace = original.clone();
    whitespace.push(b'\n');
    seeds.push((
        "live-update-noncanonical",
        whitespace,
        "live_update::encode::<Selection> plus trailing whitespace",
    ));
    let mut future: serde_json::Value = serde_json::from_slice(&original)?;
    future["v"] = 2.into();
    seeds.push((
        "live-update-future-version",
        serde_json::to_vec(&future)?,
        "live_update::encode::<Selection> with unsupported version",
    ));
    Ok(seeds)
}
