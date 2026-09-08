//! Explicit versioned resumable state machines for durable authoring.

use crate::{Error, Result};
#[cfg(feature = "host")]
use crate::{IdempotencyKey, OperationId};
#[cfg(feature = "host")]
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

/// Stable identity of a resumable state-machine implementation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MachineIdentity {
    /// Namespaced machine name.
    pub name: String,
    /// Semantic implementation version.
    pub version: String,
    /// Digest pinning implementation and schemas.
    pub digest: [u8; 32],
}

/// Explicit state-machine suspension or completion.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MachineStatus {
    /// Await another durable input.
    Suspended,
    /// Terminal successful value.
    Completed {
        /// Terminal schema-defined result.
        value: Value,
    },
    /// Terminal deterministic failure.
    Failed {
        /// Stable failure description.
        message: String,
    },
}

/// Result of one deterministic state-machine step.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MachineTransition {
    /// Complete next serializable state.
    pub state: Value,
    /// Ordered durable commands/effects emitted by this step.
    pub commands: Vec<Value>,
    /// Suspension or completion state.
    pub status: MachineStatus,
}

/// Portable durable checkpoint for a pinned implementation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MachineCheckpoint {
    /// Pinned implementation identity.
    pub machine: MachineIdentity,
    /// Monotonic number of applied inputs.
    pub revision: u64,
    /// Complete schema-defined machine state.
    pub state: Value,
}

/// Semantic foundation for all durable work.
pub trait ResumableMachine: Send + Sync {
    /// Returns the immutable implementation identity.
    fn identity(&self) -> &MachineIdentity;

    /// Returns the JSON Schema for durable state.
    fn state_schema(&self) -> &Value;

    /// Creates initial durable state from an admitted input.
    fn initialize(&self, input: &Value) -> Result<Value>;

    /// Applies one durable input synchronously and deterministically.
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition>;
}

/// Immutable registry used to restore pinned resumable work.
#[derive(Clone, Default)]
pub struct MachineRegistry(BTreeMap<(String, String, [u8; 32]), Arc<dyn ResumableMachine>>);

impl MachineRegistry {
    /// Registers one exact implementation.
    pub fn register(&mut self, machine: Arc<dyn ResumableMachine>) -> Result<()> {
        let identity = machine.identity();
        if identity.name.trim().is_empty() || identity.version.trim().is_empty() {
            return Err(Error::Invalid(
                "machine name and version are required".into(),
            ));
        }
        jsonschema::validator_for(machine.state_schema())
            .map_err(|error| Error::Invalid(format!("invalid machine state schema: {error}")))?;
        let key = (
            identity.name.clone(),
            identity.version.clone(),
            identity.digest,
        );
        if self.0.insert(key, machine).is_some() {
            return Err(Error::Conflict(
                "machine identity is already registered".into(),
            ));
        }
        Ok(())
    }

    /// Resolves exactly the implementation pinned in a checkpoint.
    #[must_use]
    pub fn resolve(&self, identity: &MachineIdentity) -> Option<&Arc<dyn ResumableMachine>> {
        self.0.get(&(
            identity.name.clone(),
            identity.version.clone(),
            identity.digest,
        ))
    }

    /// Applies one replay-safe step and returns the next checkpoint.
    pub fn step(
        &self,
        checkpoint: &MachineCheckpoint,
        input: &Value,
    ) -> Result<(MachineCheckpoint, MachineTransition)> {
        let machine = self.resolve(&checkpoint.machine).ok_or_else(|| {
            Error::Unsupported("pinned machine implementation is unavailable".into())
        })?;
        validate_state(machine.state_schema(), &checkpoint.state)?;
        let transition = machine.transition(&checkpoint.state, input)?;
        validate_state(machine.state_schema(), &transition.state)?;
        let next = MachineCheckpoint {
            machine: checkpoint.machine.clone(),
            revision: checkpoint
                .revision
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("machine revision exhausted".into()))?,
            state: transition.state.clone(),
        };
        Ok((next, transition))
    }

    /// Validates that a checkpoint is restorable by its exactly pinned implementation.
    pub fn validate_checkpoint(&self, checkpoint: &MachineCheckpoint) -> Result<()> {
        let machine = self.resolve(&checkpoint.machine).ok_or_else(|| {
            Error::Unsupported("pinned machine implementation is unavailable".into())
        })?;
        validate_state(machine.state_schema(), &checkpoint.state)
    }
}

/// One atomic durable state-machine transition and its complete command outbox.
#[cfg(feature = "host")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowRecord {
    /// Stable operation that supplied this input.
    pub operation_id: OperationId,
    /// Caller retry identity permanently bound to the operation.
    pub idempotency_key: IdempotencyKey,
    /// Canonical digest of the input.
    pub input_digest: [u8; 32],
    /// Exact prior checkpoint.
    pub prior: MachineCheckpoint,
    /// Durable input retained for deterministic replay.
    pub input: Value,
    /// Complete deterministic transition, including emitted commands.
    pub transition: MachineTransition,
    /// Complete next checkpoint committed with the commands.
    pub next: MachineCheckpoint,
}

/// Outcome of an atomic workflow journal commit.
#[cfg(feature = "host")]
#[derive(Clone, Debug, PartialEq)]
pub enum WorkflowCommitOutcome {
    /// A new record committed.
    Applied(WorkflowRecord),
    /// The exact retry was already committed.
    Replayed(WorkflowRecord),
}

/// Durable journal boundary; one commit atomically stores checkpoint and commands.
#[cfg(feature = "host")]
pub trait WorkflowJournal: Send + Sync {
    /// Replays all retained records in gapless order.
    fn replay(&self) -> BoxFuture<'_, Result<Vec<WorkflowRecord>>>;

    /// Commits exactly one complete transition with CAS and idempotency.
    fn commit<'a>(
        &'a self,
        expected_revision: u64,
        idempotency_key: IdempotencyKey,
        record: WorkflowRecord,
    ) -> BoxFuture<'a, Result<WorkflowCommitOutcome>>;
}

/// Replay-safe async authoring host compiled onto explicit state-machine records.
#[cfg(feature = "host")]
pub struct DurableWorkflowHost {
    registry: MachineRegistry,
    checkpoint: MachineCheckpoint,
    journal: Arc<dyn WorkflowJournal>,
    intents: BTreeMap<OperationId, (IdempotencyKey, WorkflowRecord)>,
}

#[cfg(feature = "host")]
impl DurableWorkflowHost {
    /// Opens a workflow by replaying and validating every atomic transition.
    pub async fn open(
        registry: MachineRegistry,
        initial: MachineCheckpoint,
        journal: Arc<dyn WorkflowJournal>,
    ) -> Result<Self> {
        registry.validate_checkpoint(&initial)?;
        let mut checkpoint = initial;
        let mut intents = BTreeMap::new();
        for record in journal.replay().await? {
            IdempotencyKey::new(record.idempotency_key.0.clone())?;
            if intents
                .insert(
                    record.operation_id,
                    (record.idempotency_key.clone(), record.clone()),
                )
                .is_some()
            {
                return Err(Error::Conflict(
                    "workflow history repeats an operation identity".into(),
                ));
            }
            if record.prior != checkpoint || record.input_digest != input_digest(&record.input)? {
                return Err(Error::Conflict("workflow history is not contiguous".into()));
            }
            let (next, transition) = registry.step(&checkpoint, &record.input)?;
            if next != record.next || transition != record.transition {
                return Err(Error::Conflict(
                    "workflow history disagrees with its pinned machine".into(),
                ));
            }
            checkpoint = next;
        }
        Ok(Self {
            registry,
            checkpoint,
            journal,
            intents,
        })
    }

    /// Returns the latest committed checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &MachineCheckpoint {
        &self.checkpoint
    }

    /// Calculates then atomically commits a checkpoint and all emitted commands.
    pub async fn step(
        &mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        input: Value,
    ) -> Result<MachineTransition> {
        IdempotencyKey::new(idempotency_key.0.clone())?;
        if let Some((existing_key, record)) = self.intents.get(&operation_id) {
            return if existing_key == &idempotency_key && record.input == input {
                Ok(record.transition.clone())
            } else {
                Err(Error::Conflict(
                    "workflow operation identity is bound to another input".into(),
                ))
            };
        }
        let (next, transition) = self.registry.step(&self.checkpoint, &input)?;
        let record = WorkflowRecord {
            operation_id,
            idempotency_key: idempotency_key.clone(),
            input_digest: input_digest(&input)?,
            prior: self.checkpoint.clone(),
            input,
            transition: transition.clone(),
            next: next.clone(),
        };
        let observed = self
            .journal
            .commit(
                self.checkpoint.revision,
                idempotency_key.clone(),
                record.clone(),
            )
            .await?;
        let committed = match observed {
            WorkflowCommitOutcome::Applied(value) | WorkflowCommitOutcome::Replayed(value) => value,
        };
        if committed != record {
            return Err(Error::Conflict(
                "workflow retry identity is bound to another transition".into(),
            ));
        }
        self.checkpoint = next;
        self.intents.insert(operation_id, (idempotency_key, record));
        Ok(transition)
    }
}

#[cfg(feature = "host")]
fn input_digest(input: &Value) -> Result<[u8; 32]> {
    let bytes = serde_json::to_vec(input).map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

fn validate_state(schema: &Value, state: &Value) -> Result<()> {
    jsonschema::validator_for(schema)
        .map_err(|error| Error::Invalid(format!("invalid machine state schema: {error}")))?
        .validate(state)
        .map_err(|error| Error::Invalid(format!("machine state failed validation: {error}")))
}

#[cfg(all(test, feature = "host"))]
mod tests {
    use super::*;
    use futures::FutureExt as _;
    use serde_json::json;
    use std::sync::Mutex;

    struct Counter {
        identity: MachineIdentity,
        schema: Value,
    }

    impl ResumableMachine for Counter {
        fn identity(&self) -> &MachineIdentity {
            &self.identity
        }
        fn state_schema(&self) -> &Value {
            &self.schema
        }
        fn initialize(&self, input: &Value) -> Result<Value> {
            Ok(input.clone())
        }
        fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
            let next = state.as_u64().unwrap_or(0) + input.as_u64().unwrap_or(0);
            Ok(MachineTransition {
                state: json!(next),
                commands: vec![json!({"publish": next})],
                status: MachineStatus::Suspended,
            })
        }
    }

    #[derive(Default)]
    struct MemoryJournal(Mutex<Vec<(String, WorkflowRecord)>>);

    impl WorkflowJournal for MemoryJournal {
        fn replay(&self) -> BoxFuture<'_, Result<Vec<WorkflowRecord>>> {
            async move {
                self.0
                    .lock()
                    .map(|values| values.iter().map(|(_, record)| record.clone()).collect())
                    .map_err(|_| Error::Storage("journal poisoned".into()))
            }
            .boxed()
        }

        fn commit<'a>(
            &'a self,
            expected_revision: u64,
            idempotency_key: IdempotencyKey,
            record: WorkflowRecord,
        ) -> BoxFuture<'a, Result<WorkflowCommitOutcome>> {
            async move {
                let mut values = self
                    .0
                    .lock()
                    .map_err(|_| Error::Storage("journal poisoned".into()))?;
                if let Some((_, existing)) = values
                    .iter()
                    .find(|(key, _)| key == idempotency_key.as_str())
                {
                    return Ok(WorkflowCommitOutcome::Replayed(existing.clone()));
                }
                if values.len() as u64 != expected_revision {
                    return Err(Error::Conflict("stale workflow revision".into()));
                }
                values.push((idempotency_key.0, record.clone()));
                Ok(WorkflowCommitOutcome::Applied(record))
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn checkpoint_and_commands_commit_as_one_replayable_record() -> Result<()> {
        let identity = MachineIdentity {
            name: "example.counter".into(),
            version: "1".into(),
            digest: [1; 32],
        };
        let mut registry = MachineRegistry::default();
        registry.register(Arc::new(Counter {
            identity: identity.clone(),
            schema: json!({"type": "integer", "minimum": 0}),
        }))?;
        let journal = Arc::new(MemoryJournal::default());
        let initial = MachineCheckpoint {
            machine: identity,
            revision: 0,
            state: json!(0),
        };
        let mut host =
            DurableWorkflowHost::open(registry.clone(), initial.clone(), journal.clone()).await?;
        let transition = host
            .step(
                OperationId::from_bytes([1; 16]),
                IdempotencyKey::new("step-1")?,
                json!(2),
            )
            .await?;
        assert_eq!(transition.commands, vec![json!({"publish": 2})]);
        assert_eq!(host.checkpoint().revision, 1);
        let replayed = host
            .step(
                OperationId::from_bytes([1; 16]),
                IdempotencyKey::new("step-1")?,
                json!(2),
            )
            .await?;
        assert_eq!(replayed, transition);
        assert_eq!(host.checkpoint().revision, 1);
        assert!(matches!(
            host.step(
                OperationId::from_bytes([1; 16]),
                IdempotencyKey::new("different-key")?,
                json!(2),
            )
            .await,
            Err(Error::Conflict(_))
        ));
        let reopened = DurableWorkflowHost::open(registry, initial, journal).await?;
        assert_eq!(reopened.checkpoint(), host.checkpoint());
        Ok(())
    }
}
