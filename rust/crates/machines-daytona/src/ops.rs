//! Process-local operation registry.
//!
//! Daytona has no idempotency keys and no operation objects of its own, so the provider keeps
//! this registry: one record per idempotency key bound to the digest of the intent it admitted,
//! the sandbox it acted on, its phase, and its terminal outcome. It also retains the last
//! observation and the state-event history of every machine the provider has seen, which is
//! what `events` and post-destroy `inspect_machine` are served from.
//!
//! Everything here is in memory. A restarted provider recovers `create` and `fork` outcomes by
//! sandbox label lookup (see `DaytonaProvider::recover`); other outcomes are lost with the
//! process, and `recover` reports them as not found.

use std::{
    collections::BTreeMap,
    sync::{Mutex, PoisonError},
    time::Duration,
};

use acyclic_machines::{
    CheckpointId, CheckpointObservation, EventFact, IdempotencyKey, MachineEvent, MachineId,
    MachineObservation, MachineState, MutationOutcome, OperationId, OperationObservation,
    OperationPhase, ProviderError,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

/// Maximum retained state events per machine before the oldest are dropped.
const MAX_EVENTS_PER_MACHINE: usize = 4_096;

/// One admitted mutation.
#[derive(Clone, Debug)]
pub struct OperationRecord {
    /// Operation identity derived from the key.
    pub id: OperationId,
    /// Idempotency key the operation was admitted under.
    pub key: IdempotencyKey,
    /// SHA-256 of the canonical intent encoding, used to detect key rebinding.
    pub intent_digest: [u8; 32],
    /// Daytona sandbox ids the operation created or acted on, once known.
    pub sandboxes: Vec<String>,
    /// Current phase.
    pub phase: OperationPhase,
    /// Terminal outcome when `phase` is `Succeeded`.
    pub outcome: Option<MutationOutcome>,
}

impl OperationRecord {
    /// Customer-visible view of the record.
    #[must_use]
    pub fn observation(&self) -> OperationObservation {
        OperationObservation {
            id: self.id,
            phase: self.phase,
        }
    }
}

/// Result of admitting an intent under a key.
#[derive(Clone, Debug)]
pub enum Admission {
    /// The key already reached a terminal success with the same intent; replay this outcome.
    Replay(Box<MutationOutcome>),
    /// The key is new; the operation is now pending under this id.
    Fresh(OperationId),
}

#[derive(Default)]
struct Inner {
    operations: BTreeMap<OperationId, OperationRecord>,
    by_key: BTreeMap<IdempotencyKey, OperationId>,
    machines: BTreeMap<MachineId, MachineObservation>,
    events: BTreeMap<MachineId, Vec<MachineEvent>>,
    checkpoints: BTreeMap<CheckpointId, CheckpointObservation>,
}

/// Registry keyed by idempotency key; see the module documentation.
#[derive(Default)]
pub struct OperationRegistry {
    inner: Mutex<Inner>,
}

impl std::fmt::Debug for OperationRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        formatter
            .debug_struct("OperationRegistry")
            .field("operations", &inner.operations.len())
            .field("machines", &inner.machines.len())
            .field("checkpoints", &inner.checkpoints.len())
            .finish()
    }
}

/// Operation identity derived deterministically from an idempotency key.
#[must_use]
pub fn operation_id(key: IdempotencyKey) -> OperationId {
    let mut hash = Sha256::new();
    hash.update(b"acyclic-machines-daytona/operation");
    hash.update(key.as_bytes());
    let digest: [u8; 32] = hash.finalize().into();
    let [
        a,
        b,
        c,
        d,
        e,
        f,
        g,
        h,
        i,
        j,
        rest @ ..,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
    ] = digest;
    // Stamp version 5 and the RFC 4122 variant so the result parses as a UUID.
    let (g, i) = ((g & 0x0f) | 0x50, (i & 0x3f) | 0x80);
    let text = format!(
        "{}-{}-{}-{}-{}",
        crate::map::hex(&[a, b, c, d]),
        crate::map::hex(&[e, f]),
        crate::map::hex(&[g, h]),
        crate::map::hex(&[i, j]),
        crate::map::hex(&rest)
    );
    // A version-5-shaped UUID built from a SHA-256 prefix is never nil.
    OperationId::parse(&text).unwrap_or_default()
}

/// Canonical digest of an intent value.
///
/// # Errors
/// Returns [`ProviderError::Invalid`] when the intent cannot be encoded.
pub fn intent_digest<T: Serialize>(intent: &T) -> Result<[u8; 32], ProviderError> {
    let encoded = serde_json::to_vec(intent)
        .map_err(|error| ProviderError::Invalid(format!("intent cannot be encoded: {error}")))?;
    Ok(Sha256::digest(encoded).into())
}

impl OperationRegistry {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Admits `intent` under `key`.
    ///
    /// # Errors
    /// - [`ProviderError::Conflict`] when the key is bound to a different intent.
    /// - [`ProviderError::Indeterminate`] when the key's operation is still pending.
    /// - [`ProviderError::Failed`] / [`ProviderError::Cancelled`] when the key reached that
    ///   terminal phase; the caller may not redispatch under the same key.
    pub fn admit<T: Serialize>(
        &self,
        key: IdempotencyKey,
        intent: &T,
    ) -> Result<Admission, ProviderError> {
        let digest = intent_digest(intent)?;
        let mut inner = self.lock();
        if let Some(id) = inner.by_key.get(&key).copied()
            && let Some(record) = inner.operations.get(&id)
        {
            if record.intent_digest != digest {
                return Err(ProviderError::Conflict(
                    "idempotency key is bound to another intent".into(),
                ));
            }
            return match (record.phase, &record.outcome) {
                (OperationPhase::Succeeded, Some(outcome)) => {
                    Ok(Admission::Replay(Box::new(outcome.clone())))
                }
                (OperationPhase::Pending | OperationPhase::Indeterminate, _)
                | (OperationPhase::Succeeded, None) => Err(ProviderError::Indeterminate(key)),
                (OperationPhase::Failed, _) => Err(ProviderError::Failed),
                (OperationPhase::Cancelled, _) => Err(ProviderError::Cancelled),
            };
        }
        let id = operation_id(key);
        inner.operations.insert(
            id,
            OperationRecord {
                id,
                key,
                intent_digest: digest,
                sandboxes: Vec::new(),
                phase: OperationPhase::Pending,
                outcome: None,
            },
        );
        inner.by_key.insert(key, id);
        Ok(Admission::Fresh(id))
    }

    /// Records a sandbox the pending operation is acting on, so `cancel` can reach it.
    pub fn bind_sandbox(&self, operation: OperationId, sandbox_id: &str) {
        if let Some(record) = self.lock().operations.get_mut(&operation) {
            record.sandboxes.push(sandbox_id.to_owned());
        }
    }

    /// Marks the operation succeeded with `outcome`, unless it was cancelled meanwhile.
    pub fn complete(&self, operation: OperationId, outcome: MutationOutcome) {
        if let Some(record) = self.lock().operations.get_mut(&operation)
            && record.phase == OperationPhase::Pending
        {
            record.phase = OperationPhase::Succeeded;
            record.outcome = Some(outcome);
        }
    }

    /// Marks the operation terminal in a non-success phase derived from `error`.
    pub fn fail(&self, operation: OperationId, error: &ProviderError) {
        let phase = match error {
            ProviderError::Indeterminate(_) | ProviderError::Unavailable => {
                OperationPhase::Indeterminate
            }
            ProviderError::Cancelled => OperationPhase::Cancelled,
            _ => OperationPhase::Failed,
        };
        if let Some(record) = self.lock().operations.get_mut(&operation)
            && record.phase == OperationPhase::Pending
        {
            record.phase = phase;
        }
    }

    /// Releases a key whose operation was rejected before any Daytona side effect happened,
    /// so the caller can retry under the same key with a corrected request.
    pub fn forget(&self, operation: OperationId) {
        let mut inner = self.lock();
        if let Some(record) = inner.operations.remove(&operation) {
            inner.by_key.remove(&record.key);
        }
    }

    /// Reads one operation.
    #[must_use]
    pub fn inspect(&self, operation: OperationId) -> Option<OperationObservation> {
        self.lock()
            .operations
            .get(&operation)
            .map(OperationRecord::observation)
    }

    /// Reads the full record of one operation.
    #[must_use]
    pub fn record(&self, operation: OperationId) -> Option<OperationRecord> {
        self.lock().operations.get(&operation).cloned()
    }

    /// Reads the record admitted under `key`.
    #[must_use]
    pub fn record_for_key(&self, key: IdempotencyKey) -> Option<OperationRecord> {
        let inner = self.lock();
        inner
            .by_key
            .get(&key)
            .and_then(|id| inner.operations.get(id))
            .cloned()
    }

    /// Marks a pending operation cancelled and returns the sandboxes it had bound, which the
    /// caller deletes best-effort. Terminal operations are returned unchanged.
    #[must_use]
    pub fn cancel(&self, operation: OperationId) -> Option<(OperationObservation, Vec<String>)> {
        let mut inner = self.lock();
        let record = inner.operations.get_mut(&operation)?;
        if record.phase == OperationPhase::Pending {
            record.phase = OperationPhase::Cancelled;
            Some((record.observation(), std::mem::take(&mut record.sandboxes)))
        } else {
            Some((record.observation(), Vec::new()))
        }
    }

    /// Polls `inspect` until the operation leaves `Pending` or `timeout` elapses.
    ///
    /// Mutations in this provider complete inline, so the loop normally returns on the first
    /// iteration; it exists for callers that hold an operation id from another task.
    ///
    /// # Errors
    /// Returns [`ProviderError::NotFound`] for an unknown operation and
    /// [`ProviderError::Indeterminate`] with the operation's key when the timeout elapses.
    pub async fn watch(
        &self,
        operation: OperationId,
        poll: Duration,
        timeout: Duration,
    ) -> Result<OperationObservation, ProviderError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let record = self
                .record(operation)
                .ok_or_else(|| ProviderError::NotFound(operation.to_string()))?;
            if record.phase != OperationPhase::Pending {
                return Ok(record.observation());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ProviderError::Indeterminate(record.key));
            }
            tokio::time::sleep(poll).await;
        }
    }

    /// Records the latest observation of a machine and appends a state event when the state
    /// differs from the last one recorded (or none was recorded yet).
    pub fn observe_machine(&self, observation: MachineObservation, observed_at_unix_ms: u64) {
        let mut inner = self.lock();
        let id = observation.id;
        let previous = inner.machines.get(&id).map(|value| value.state);
        if previous != Some(observation.state) {
            let events = inner.events.entry(id).or_default();
            if events.len() >= MAX_EVENTS_PER_MACHINE {
                events.remove(0);
            }
            let sequence = events
                .last()
                .map_or(1, |event| event.sequence.saturating_add(1));
            events.push(MachineEvent {
                machine: id,
                sequence,
                observed_at_unix_ms,
                fact: EventFact::State(observation.state),
            });
        }
        inner.machines.insert(id, observation);
    }

    /// Last recorded observation of a machine.
    #[must_use]
    pub fn machine(&self, machine: MachineId) -> Option<MachineObservation> {
        self.lock().machines.get(&machine).cloned()
    }

    /// Records a machine as destroyed at `observed_at_unix_ms`, keeping its last contract.
    pub fn mark_destroyed(&self, machine: MachineId, observed_at_unix_ms: u64) {
        let Some(mut observation) = self.machine(machine) else {
            return;
        };
        observation.state = MachineState::Destroyed;
        observation.changed_at_unix_ms = observed_at_unix_ms;
        self.observe_machine(observation, observed_at_unix_ms);
    }

    /// Every recorded machine, in identity order.
    #[must_use]
    pub fn machines(&self) -> Vec<MachineObservation> {
        self.lock().machines.values().cloned().collect()
    }

    /// Bounded page of recorded state events after `after_sequence`.
    #[must_use]
    pub fn events(
        &self,
        machine: MachineId,
        after_sequence: Option<u64>,
        limit: usize,
    ) -> (Vec<MachineEvent>, Option<u64>) {
        let inner = self.lock();
        let mut values = inner
            .events
            .get(&machine)
            .into_iter()
            .flatten()
            .filter(|event| after_sequence.is_none_or(|cursor| event.sequence > cursor))
            .take(limit.saturating_add(1))
            .cloned()
            .collect::<Vec<_>>();
        let next = if values.len() > limit {
            values.pop();
            values.last().map(|event| event.sequence)
        } else {
            None
        };
        (values, next)
    }

    /// Records a checkpoint observation.
    pub fn remember_checkpoint(&self, checkpoint: CheckpointObservation) {
        self.lock().checkpoints.insert(checkpoint.id, checkpoint);
    }

    /// Last recorded observation of a checkpoint.
    #[must_use]
    pub fn checkpoint(&self, checkpoint: CheckpointId) -> Option<CheckpointObservation> {
        self.lock().checkpoints.get(&checkpoint).cloned()
    }

    /// Records that a checkpoint no longer accepts forks.
    pub fn mark_checkpoint_destroyed(&self, checkpoint: CheckpointId) {
        if let Some(value) = self.lock().checkpoints.get_mut(&checkpoint) {
            value.forkable = false;
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;

    fn key(suffix: u8) -> IdempotencyKey {
        IdempotencyKey::parse(&format!("00000000-0000-0000-0000-0000000000{suffix:02x}")).unwrap()
    }

    #[test]
    fn admission_replays_success_and_rejects_rebinding() {
        let registry = OperationRegistry::default();
        let Admission::Fresh(id) = registry.admit(key(1), &"intent-a").unwrap() else {
            panic!("expected fresh")
        };
        assert!(matches!(
            registry.admit(key(1), &"intent-a"),
            Err(ProviderError::Indeterminate(_))
        ));
        assert!(matches!(
            registry.admit(key(1), &"intent-b"),
            Err(ProviderError::Conflict(_))
        ));
        let machine = MachineId::new();
        registry.complete(id, MutationOutcome::Suspended(machine));
        let Admission::Replay(outcome) = registry.admit(key(1), &"intent-a").unwrap() else {
            panic!("expected replay")
        };
        assert_eq!(*outcome, MutationOutcome::Suspended(machine));
        assert_eq!(operation_id(key(1)), id);
        assert_ne!(operation_id(key(1)), operation_id(key(2)));
    }

    #[test]
    fn cancel_returns_bound_sandboxes_once() {
        let registry = OperationRegistry::default();
        let Admission::Fresh(id) = registry.admit(key(3), &"x").unwrap() else {
            panic!("expected fresh")
        };
        registry.bind_sandbox(id, "sb-1");
        let (observation, sandboxes) = registry.cancel(id).unwrap();
        assert_eq!(observation.phase, OperationPhase::Cancelled);
        assert_eq!(sandboxes, vec!["sb-1".to_owned()]);
        assert!(registry.cancel(id).unwrap().1.is_empty());
        registry.complete(id, MutationOutcome::Woken(MachineId::new()));
        assert_eq!(
            registry.inspect(id).unwrap().phase,
            OperationPhase::Cancelled
        );
    }

    #[test]
    fn observations_append_events_only_on_state_change() {
        let registry = OperationRegistry::default();
        let sandbox: crate::api::Sandbox =
            serde_json::from_str(include_str!("../tests/fixtures/sandbox_started.json")).unwrap();
        let mut observation = crate::map::sandbox_to_observation(&sandbox, None, None, 0).unwrap();
        registry.observe_machine(observation.clone(), 1);
        registry.observe_machine(observation.clone(), 2);
        observation.state = MachineState::Suspended;
        registry.observe_machine(observation.clone(), 3);
        observation.state = MachineState::Running;
        registry.observe_machine(observation.clone(), 4);
        let (events, next) = registry.events(observation.id, None, 2);
        assert_eq!(events.len(), 2);
        assert_eq!(next, Some(2));
        let (rest, next) = registry.events(observation.id, next, 16);
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].fact, EventFact::State(MachineState::Running));
        assert_eq!(next, None);
        registry.mark_destroyed(observation.id, 5);
        assert_eq!(
            registry.machine(observation.id).unwrap().state,
            MachineState::Destroyed
        );
    }
}
