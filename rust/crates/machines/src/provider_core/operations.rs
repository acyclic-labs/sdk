//! Shared process-local lifecycle journal for remote machine adapters.
//!
//! Keys bind to canonical mutation intents before dispatch. Terminal outcomes,
//! resource ownership, observations and state events survive for this provider
//! instance only; restart recovery remains the adapter's responsibility.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
};

pub use crate::intent_digest;
use crate::{
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
    /// provider resource ids the operation created. Cancelling the operation deletes these.
    pub created: Vec<String>,
    /// Existing provider resource ids the operation acts on (suspend, wake, policy, checkpoint,
    /// destroy). They belong to the machine, not to the operation, so cancellation never
    /// deletes them.
    pub targets: Vec<String>,
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

impl Inner {
    fn observe_machine(&mut self, observation: MachineObservation, observed_at_unix_ms: u64) {
        let inner = self;
        let id = observation.id;
        let previous = inner.machines.get(&id).map(|value| value.state);
        // Destruction is terminal. A response captured before cancellation or
        // expiry must not resurrect an observation after either completes.
        if previous == Some(MachineState::Destroyed) && observation.state != MachineState::Destroyed
        {
            return;
        }
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
}

/// Registry keyed by idempotency key; see the module documentation.
#[derive(Clone, Default)]
pub struct OperationRegistry {
    inner: Arc<Mutex<Inner>>,
    changed: Arc<tokio::sync::Notify>,
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
    // Preserve operation identities admitted by the original Daytona adapter.
    hash.update(b"acyclic-machines-daytona/operation");
    hash.update(key.as_bytes());
    OperationId::from_digest(hash.finalize().into())
}

impl OperationRegistry {
    /// Admits and applies one mutation, retaining ownership for cancellation.
    /// Definite failures roll back only resources created by this operation;
    /// uncertain dispatches remain recoverable and are never retried here.
    ///
    /// # Errors
    /// Returns admission errors or the error from the adapter's action.
    pub async fn apply<T, F, R>(
        &self,
        key: IdempotencyKey,
        intent: &T,
        action: F,
        rollback: R,
    ) -> Result<MutationOutcome, ProviderError>
    where
        T: Serialize,
        F: AsyncFnOnce(OperationId) -> Result<MutationOutcome, ProviderError>,
        R: AsyncFnOnce(OperationId),
    {
        let operation = match self.admit(key, intent)? {
            Admission::Replay(outcome) => return Ok(*outcome),
            Admission::Fresh(operation) => operation,
        };
        match action(operation).await {
            Ok(outcome) => {
                self.complete(operation, outcome.clone());
                match self.inspect(operation).map(|value| value.phase) {
                    Some(OperationPhase::Cancelled) => Err(ProviderError::Cancelled),
                    _ => Ok(outcome),
                }
            }
            Err(error) => {
                let touched = self
                    .record(operation)
                    .is_some_and(|record| !record.created.is_empty() || !record.targets.is_empty());
                let before_side_effect = matches!(
                    error,
                    ProviderError::Invalid(_)
                        | ProviderError::Unsupported(_)
                        | ProviderError::NotFound(_)
                        | ProviderError::Conflict(_)
                        | ProviderError::Rejected(_)
                );
                if !touched && before_side_effect {
                    self.forget(operation);
                } else {
                    self.fail(operation, &error);
                    // A definite failure leaves nothing the caller can use or replay, so undo
                    // whatever the operation created. An indeterminate outcome keeps its sandboxes
                    // for `recover`, and `cancel` undoes it.
                    if !matches!(
                        error,
                        ProviderError::Indeterminate(_)
                            | ProviderError::Unavailable
                            | ProviderError::Cancelled
                    ) {
                        rollback(operation).await;
                    }
                }
                Err(error)
            }
        }
    }

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
                created: Vec::new(),
                targets: Vec::new(),
                phase: OperationPhase::Pending,
                outcome: None,
            },
        );
        inner.by_key.insert(key, id);
        Ok(Admission::Fresh(id))
    }

    /// Records a sandbox the pending operation created, so `cancel` can delete it.
    ///
    /// Returns `false`, without recording anything, when the operation is no longer pending
    /// (it was cancelled while the create or fork request was in flight). The caller then owns
    /// the orphan and must delete it itself.
    #[must_use]
    pub fn bind_created(&self, operation: OperationId, sandbox_id: &str) -> bool {
        match self.lock().operations.get_mut(&operation) {
            Some(record) if record.phase == OperationPhase::Pending => {
                record.created.push(sandbox_id.to_owned());
                true
            }
            _ => false,
        }
    }

    /// Records a sandbox the pending operation adopted from an earlier attempt under its key,
    /// as a target that cancellation and rollback leave alone. Returns `false`, without
    /// recording anything, when the operation is no longer pending.
    #[must_use]
    pub fn bind_adopted(&self, operation: OperationId, sandbox_id: &str) -> bool {
        match self.lock().operations.get_mut(&operation) {
            Some(record) if record.phase == OperationPhase::Pending => {
                record.targets.push(sandbox_id.to_owned());
                true
            }
            _ => false,
        }
    }

    /// Records an existing sandbox the pending operation acts on. Cancellation leaves it alone.
    pub fn bind_target(&self, operation: OperationId, sandbox_id: &str) {
        if let Some(record) = self.lock().operations.get_mut(&operation) {
            record.targets.push(sandbox_id.to_owned());
        }
    }

    /// Marks the operation succeeded with `outcome`, unless it was cancelled meanwhile.
    pub fn complete(&self, operation: OperationId, outcome: MutationOutcome) {
        if let Some(record) = self.lock().operations.get_mut(&operation)
            && record.phase == OperationPhase::Pending
        {
            record.phase = OperationPhase::Succeeded;
            record.outcome = Some(outcome);
            // Success transfers resource ownership to the returned outcome.
            // A later terminal cancel or rollback must never reclaim it.
            record.created.clear();
        }
        self.changed.notify_waiters();
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
        self.changed.notify_waiters();
    }

    /// Releases a key whose operation was rejected before any provider side effect happened,
    /// so the caller can retry under the same key with a corrected request.
    pub fn forget(&self, operation: OperationId) {
        let mut inner = self.lock();
        if let Some(record) = inner.operations.remove(&operation) {
            inner.by_key.remove(&record.key);
        }
        self.changed.notify_waiters();
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

    /// Cancels an operation and returns the sandboxes it had *created*, which the caller deletes
    /// best-effort. Sandboxes it merely acted on are never returned.
    ///
    /// A pending or indeterminate operation becomes cancelled. A failed or already-cancelled
    /// one keeps its phase but still hands back any created sandbox an earlier rollback or
    /// cancel could not delete, so cancelling again finishes undoing a mutation that did not
    /// succeed. A succeeded operation is returned unchanged.
    #[must_use]
    pub fn cancel(&self, operation: OperationId) -> Option<(OperationObservation, Vec<String>)> {
        let mut inner = self.lock();
        let record = inner.operations.get_mut(&operation)?;
        self.changed.notify_waiters();
        match record.phase {
            OperationPhase::Pending | OperationPhase::Indeterminate => {
                record.phase = OperationPhase::Cancelled;
                Some((record.observation(), std::mem::take(&mut record.created)))
            }
            OperationPhase::Failed | OperationPhase::Cancelled => {
                Some((record.observation(), std::mem::take(&mut record.created)))
            }
            OperationPhase::Succeeded => Some((record.observation(), Vec::new())),
        }
    }

    /// Takes the created sandboxes of a failed operation for rollback.
    #[must_use]
    pub fn take_created(&self, operation: OperationId) -> Vec<String> {
        self.lock()
            .operations
            .get_mut(&operation)
            .map(|record| std::mem::take(&mut record.created))
            .unwrap_or_default()
    }

    /// Hands back created sandboxes a rollback could not delete, so a later `cancel` retries.
    pub fn restore_created(&self, operation: OperationId, sandboxes: Vec<String>) {
        if let Some(record) = self.lock().operations.get_mut(&operation) {
            record.created.extend(sandboxes);
        }
    }

    /// Streams the current phase immediately, then changes through a terminal
    /// phase. Registering before reading closes the completion/wait race.
    pub fn watch(&self, operation: OperationId) -> Result<crate::OperationStream, ProviderError> {
        use futures::StreamExt;
        let current = self
            .inspect(operation)
            .ok_or_else(|| ProviderError::NotFound(operation.to_string()))?;
        let registry = self.clone();
        let first = futures::stream::once(async move { Ok(current) });
        let changes = futures::stream::try_unfold(
            (registry, current),
            move |(registry, previous)| async move {
                if previous.phase != OperationPhase::Pending {
                    return Ok(None);
                }
                loop {
                    let changed = registry.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    let current = registry
                        .inspect(operation)
                        .ok_or_else(|| ProviderError::NotFound(operation.to_string()))?;
                    if current != previous {
                        drop(changed);
                        return Ok(Some((current, (registry.clone(), current))));
                    }
                    changed.await;
                }
            },
        );
        Ok(first.chain(changes).boxed())
    }

    /// Records the latest observation of a machine and appends a state event when the state
    /// differs from the last one recorded (or none was recorded yet).
    pub fn observe_machine(&self, observation: MachineObservation, observed_at_unix_ms: u64) {
        self.lock()
            .observe_machine(observation, observed_at_unix_ms);
    }

    /// Publishes the initial observation and cancellation ownership together.
    /// A cancelled in-flight create remains the caller's orphan to delete.
    pub fn bind_created_machine(
        &self,
        operation: OperationId,
        sandbox: &str,
        observation: MachineObservation,
    ) -> bool {
        let mut inner = self.lock();
        let Some(record) = inner.operations.get_mut(&operation) else {
            return false;
        };
        if record.phase != OperationPhase::Pending {
            return false;
        }
        record.created.push(sandbox.into());
        let at = observation.changed_at_unix_ms;
        inner.observe_machine(observation, at);
        true
    }

    /// Publishes readiness only while its create still owns the machine.
    pub fn observe_pending_machine(
        &self,
        operation: OperationId,
        observation: MachineObservation,
    ) -> bool {
        let mut inner = self.lock();
        if !inner
            .operations
            .get(&operation)
            .is_some_and(|record| record.phase == OperationPhase::Pending)
            || inner
                .machines
                .get(&observation.id)
                .is_some_and(|value| value.state == MachineState::Destroyed)
        {
            return false;
        }
        let at = observation.changed_at_unix_ms;
        inner.observe_machine(observation, at);
        true
    }

    /// Last recorded observation of a machine.
    #[must_use]
    pub fn machine(&self, machine: MachineId) -> Option<MachineObservation> {
        self.lock().machines.get(&machine).cloned()
    }

    /// Records a machine as destroyed at `observed_at_unix_ms`, keeping its last contract.
    pub fn mark_destroyed(&self, machine: MachineId, observed_at_unix_ms: u64) {
        self.observe_state(machine, MachineState::Destroyed, observed_at_unix_ms);
    }

    /// Updates provider state without overwriting a concurrently changed contract.
    /// Destruction is terminal, including when an older read finishes afterwards.
    pub fn observe_state(
        &self,
        machine: MachineId,
        state: MachineState,
        at: u64,
    ) -> Option<MachineObservation> {
        let mut inner = self.lock();
        let mut observation = inner.machines.get(&machine)?.clone();
        if observation.state != MachineState::Destroyed && observation.state != state {
            observation.state = state;
            observation.changed_at_unix_ms = at;
            inner.observe_machine(observation.clone(), at);
        }
        Some(observation)
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
