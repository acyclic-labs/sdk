use super::{
    Options,
    population::{Fixture, id, require_root},
    report::{Result, emit},
};
use acyclic_harness_core::{agent::IntentId, filesystem::hosting::RootSubmission};
use acyclic_stream::{
    CommitMutation, CommitRequest, CommittedEnvelope, CommittedMutation, CutSimulationError,
    IdempotencyOutcome, PersistenceCut, StreamError, StreamProvider, StreamProviderExt as _,
};
use acyclic_test_support::scale::Operation;
use sha2::{Digest as _, Sha256};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

struct Cut {
    stage: usize,
    events: [AtomicU64; 3],
    crashes: AtomicU64,
    simulation_error: AtomicU64,
}

impl Cut {
    fn new(stage: usize) -> Self {
        Self {
            stage,
            events: std::array::from_fn(|_| AtomicU64::new(0)),
            crashes: AtomicU64::new(0),
            simulation_error: AtomicU64::new(0),
        }
    }

    fn event(&self, stage: usize) -> bool {
        self.events[stage].fetch_add(1, Ordering::SeqCst);
        self.stage != stage
    }

    fn counts(&self) -> [u64; 3] {
        std::array::from_fn(|stage| self.events[stage].load(Ordering::SeqCst))
    }

    fn require_faithful(&self) -> Result {
        let expected = std::array::from_fn(|stage| u64::from(stage <= self.stage));
        if self.counts() != expected
            || self.crashes.load(Ordering::SeqCst) != 1
            || self.simulation_error.load(Ordering::SeqCst) != 0
        {
            return Err("root crash did not faithfully cut its original journal frame".into());
        }
        Ok(())
    }
}

impl PersistenceCut for Cut {
    fn admit(&self) -> bool {
        self.event(0)
    }

    fn written(&self) -> bool {
        self.event(1)
    }

    fn acknowledge(&self) -> bool {
        self.event(2)
    }

    fn crashed(&self, simulation: std::result::Result<(), CutSimulationError>) {
        self.crashes.fetch_add(1, Ordering::SeqCst);
        self.simulation_error.store(
            match simulation {
                Ok(()) => 0,
                Err(CutSimulationError::Lock) => 1,
                Err(CutSimulationError::Truncate(_)) => 2,
                Err(CutSimulationError::Task) => 3,
            },
            Ordering::SeqCst,
        );
    }
}

/// The switch is captured by the original journal dispatch on this thread.
struct FailedTruncation;
impl FailedTruncation {
    fn inject() -> Self {
        acyclic_stream::fail_cut_truncation(true);
        Self
    }
}
impl Drop for FailedTruncation {
    fn drop(&mut self) {
        acyclic_stream::fail_cut_truncation(false);
    }
}

pub(super) fn run(options: &Options) -> Result {
    if options.operations != [Operation::SessionStart] {
        return Err("native crash controls currently require exactly swarm.session_start".into());
    }
    std::fs::create_dir(&options.root)?;
    emit(serde_json::json!({
        "schema": 1, "kind": "proof_identity", "source": options.source,
        "executable_sha256": hex::encode(Sha256::digest(std::fs::read(std::env::current_exe()?)?)),
        "os": std::env::consts::OS, "arch": std::env::consts::ARCH,
        "operation": "swarm.session_start", "root": options.root,
        "durability": "full", "runtime_threads": 1,
        "scope": "original joined root publication; physical journal cuts and exact immutable recovery",
    }))?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            super::proofs_resources::native_timers(&options.recovery()).await?;
            for (stage, name) in ["before-append", "before-flush", "before-ack"]
                .into_iter()
                .enumerate()
            {
                cut_root(&options.root.join(name), stage, name, false)
                    .await
                    .map_err(|error| format!("native root cut {name}: {error}"))?;
            }
            cut_root(
                &options.root.join("failed-truncation"),
                1,
                "before-flush",
                true,
            )
            .await
        })?;
    emit(serde_json::json!({
        "schema": 1, "kind": "proof_end", "native_root_cuts_complete": true,
        "native_timer_injection_control_complete": true,
        "composed_proof_complete": false, "population_cost_proven": false,
        "same_owner_failed_producer_retry_complete": true,
        "same_owner_resubmission_proven": false,
        "open": ["same-owner resubmission after producer loss", "physical release of retained publication holds", "population-independent original work"],
    }))
}

async fn cut_root(root: &Path, stage: usize, name: &str, fail_simulation: bool) -> Result {
    std::fs::create_dir(root)?;
    let fixture = Fixture::providers(root, None).await?;
    let prepared = Box::pin(fixture.bound.prepare_root(
        &IntentId::try_from("root-crash-proof")?,
        fixture.root_request(id(0)?)?,
    ))
    .await?;
    let expected_child = prepared.child().clone();
    let original = prepared.request().clone();
    let request_digest = acyclic_stream::request::commit_digest(&mut original.clone())?;
    // This fixture creates only fresh paths. Inspect all requested paths before
    // installing the cut; no pre-existing participant may hide a partial root.
    require_absent(&fixture, &original).await?;
    let cut = Arc::new(Cut::new(stage));
    fixture.stream.inner().cut_persistence(cut.clone());
    let failure = fail_simulation.then(FailedTruncation::inject);
    let outcome = Box::pin(fixture.bound.submit_root(prepared)).await?;
    drop(failure);
    if require_root(&outcome, &expected_child).is_ok() {
        return Err("root receipt oracle accepted an uncertain physical publication".into());
    }
    let RootSubmission::Unknown(held) = outcome else {
        return Err("original root producer loss did not return typed Unknown".into());
    };
    if held.request() != &original || held.child() != &expected_child {
        return Err("uncertain root changed its retained request or child identity".into());
    }
    // The same failed producer must terminate later publication attempts rather
    // than leave an event waiter parked behind a dead persistence worker.
    if fixture.stream.commit(original.clone()).await != Err(StreamError::Unavailable) {
        return Err("cut journal did not terminate a later submission as unavailable".into());
    }
    // Re-enter the public composition with the SAME retained owner. The
    // failed journal's original termination event must settle this observer,
    // returning the original uncertain commit without another physical stage.
    let stages_before_retry = cut.counts();
    let retried = Box::pin(fixture.bound.submit_root(held)).await?;
    if require_root(&retried, &expected_child).is_ok() {
        return Err("failed root producer retry fabricated a created root".into());
    }
    let RootSubmission::Unknown(held) = retried else {
        return Err("failed root producer retry discarded original uncertainty".into());
    };
    if held.request() != &original
        || held.child() != &expected_child
        || cut.counts() != stages_before_retry
    {
        return Err("same-owner root retry changed its request or reached a dead journal".into());
    }
    if fail_simulation {
        if cut.require_faithful().is_ok()
            || cut.crashes.load(Ordering::SeqCst) != 1
            || cut.simulation_error.load(Ordering::SeqCst) != 2
            || cut.counts() != [1, 1, 0]
        {
            return Err("crash oracle accepted failed physical truncation".into());
        }
        return emit(serde_json::json!({
            "schema": 1, "kind": "proof_negative_control", "operation": "swarm.session_start",
            "control": "original journal fails to discard unflushed bytes", "result": "rejected",
            "journal_events": cut.counts(), "typed_outcome": "Unknown",
            "simulation_error": "Truncate",
            "request_digest": hex::encode(request_digest),
        }));
    }
    cut.require_faithful()?;
    drop(held);
    drop(fixture);
    let reopened = Fixture::providers(root, None).await?;
    let observation = reopened
        .stream
        .inspect_idempotency(original.idempotency_key.clone())
        .await?;
    let recovered = if stage == 2 {
        let observation =
            observation.ok_or("durable root lost its original idempotency receipt")?;
        if observation.idempotency_key != original.idempotency_key {
            return Err("recovered root changed its idempotency identity".into());
        }
        if observation.request_digest != request_digest {
            return Err("recovered root receipt is bound to different request bytes".into());
        }
        let IdempotencyOutcome::Commit(envelope) = observation.outcome else {
            return Err("recovered root is not a committed mutation envelope".into());
        };
        require_envelope(&original, &envelope)?;
        if reopened.stream.read_commit(envelope.commit_id).await? != envelope {
            return Err("recovered root receipt differs from its immutable commit".into());
        }
        for mutation in &envelope.mutations {
            let CommittedMutation::Append(append) = mutation else {
                return Err("fresh root unexpectedly contains a fork".into());
            };
            let head = reopened.stream.head(append.path.clone()).await?;
            if head.tail != append.tail
                || head.durable != append.tail
                || head.last.as_ref() != append.records.last()
            {
                return Err(
                    "recovered root participant differs from its whole durable envelope".into(),
                );
            }
        }
        let mut partial = envelope.clone();
        partial.mutations.pop().ok_or("root envelope is empty")?;
        if require_envelope(&original, &partial).is_ok() {
            return Err("root recovery oracle accepted an omitted participant".into());
        }
        "whole-original-envelope"
    } else {
        if observation.is_some() {
            return Err("unflushed root retained an idempotency result".into());
        }
        require_absent(&reopened, &original).await?;
        "no-participants-or-idempotency-result"
    };
    emit(serde_json::json!({
        "schema": 1, "kind": "proof_cut", "operation": "swarm.session_start", "cut": name,
        "journal_events": cut.counts(), "crashes": 1, "faithful_simulation": true,
        "typed_outcome": "Unknown", "retained_request_exact": true,
        "request_digest": hex::encode(request_digest),
        "idempotency_key": hex::encode(original.idempotency_key.as_bytes()),
        "later_original_producer_result": "Unavailable", "recovered": recovered,
        "same_owner_retry_result": "Unknown", "same_owner_retry_request_exact": true,
        "same_owner_retry_additional_journal_stages": [0, 0, 0],
        "participants": original.mutations.len(),
        "negative_controls": if stage == 2 { vec!["Unknown is not Created", "omitted recovered participant"] } else { vec!["Unknown is not Created"] },
    }))
}

async fn require_absent(fixture: &Fixture, original: &CommitRequest) -> Result {
    for mutation in &original.mutations {
        let CommitMutation::Append { path, .. } = mutation else {
            return Err("fresh root request unexpectedly contains a fork".into());
        };
        if fixture.stream.head(path.clone()).await != Err(StreamError::NotFound) {
            return Err(format!("fresh root participant unexpectedly exists: {path:?}").into());
        }
    }
    Ok(())
}

fn require_envelope(original: &CommitRequest, envelope: &CommittedEnvelope) -> Result {
    let mut canonical = original.clone();
    acyclic_stream::request::validate_commit(&mut canonical)?;
    if canonical.mutations.len() != envelope.mutations.len() {
        return Err("recovered root omitted or added a participant".into());
    }
    for (requested, committed) in canonical.mutations.iter().zip(&envelope.mutations) {
        let (CommitMutation::Append { path, records }, CommittedMutation::Append(append)) =
            (requested, committed)
        else {
            return Err("recovered fresh root changed its mutation shape".into());
        };
        let end = u64::try_from(records.len())?;
        if path != &append.path
            || append.start != 0
            || append.end != end
            || append.tail != end
            || records.len() != append.records.len()
        {
            return Err("recovered root changed participant path or record extent".into());
        }
        for (index, (expected, actual)) in records.iter().zip(&append.records).enumerate() {
            if expected.value != actual.value
                || expected.headers != actual.headers
                || actual.sequence != u64::try_from(index)?
                || actual.commit_id != envelope.commit_id
                || actual.committed_at_micros != envelope.committed_at_micros
                || &actual.origin != path
            {
                return Err("recovered root changed original record bytes or identity".into());
            }
        }
    }
    Ok(())
}
