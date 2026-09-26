//! Explicit versioned resumable state machines for durable authoring.

#[cfg(feature = "host")]
use crate::IdempotencyKey;
use crate::{Error, OperationId, Result, conversation::FileRef};
#[cfg(feature = "host")]
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(feature = "host")]
use std::sync::Mutex;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Replay-safe command emitted by a machine. The operation ID is stable and
/// all variable arguments live in an immutable owner-resolved file version.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowCommand {
    /// Stable identity used for external dispatch and reconciliation.
    pub operation_id: OperationId,
    /// Namespaced executor or effect kind pinned by the consuming registry.
    pub kind: String,
    /// Ref-only command arguments, never an inline body or bearer scope.
    pub payload: FileRef,
}

impl WorkflowCommand {
    /// Rejects malformed public command contracts before checkpoint commit.
    pub fn validate(&self) -> Result<()> {
        crate::contract::validate_component_label(&self.kind, "workflow command kind")?;
        self.payload.validate()
    }
}

/// Stable identity of a resumable state-machine implementation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineIdentity {
    /// Namespaced machine name.
    pub name: String,
    /// Semantic implementation version.
    pub version: String,
    /// Digest pinning implementation and schemas.
    pub digest: [u8; 32],
}

impl MachineIdentity {
    /// Rejects an unpinned or malformed state-machine implementation.
    pub fn validate(&self) -> Result<()> {
        crate::contract::validate_component_label(&self.name, "machine name")?;
        crate::contract::validate_component_label(&self.version, "machine version")?;
        if self.digest == [0; 32] {
            return Err(Error::Invalid(
                "machine needs an implementation digest".into(),
            ));
        }
        Ok(())
    }
}

/// Explicit state-machine suspension or completion.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct MachineTransition {
    /// Complete next serializable state.
    pub state: Value,
    /// Ordered durable commands/effects emitted by this step.
    pub commands: Vec<WorkflowCommand>,
    /// Suspension or completion state.
    pub status: MachineStatus,
}

/// Portable durable checkpoint for a pinned implementation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
        identity.validate()?;
        jsonschema::validator_for(machine.state_schema())
            .map_err(|error| Error::Invalid(format!("invalid machine state schema: {error}")))?;
        let key = (
            identity.name.clone(),
            identity.version.clone(),
            identity.digest,
        );
        if self.0.contains_key(&key) {
            return Err(Error::Conflict(
                "machine identity is already registered".into(),
            ));
        }
        self.0.insert(key, machine);
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
        validate_commands(&transition.commands)?;
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
#[serde(deny_unknown_fields)]
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

#[cfg(feature = "host")]
impl WorkflowRecord {
    /// Identities reserved atomically by this transition, including its outbox.
    pub fn operation_ids(&self) -> impl Iterator<Item = OperationId> + '_ {
        std::iter::once(self.operation_id).chain(
            self.transition
                .commands
                .iter()
                .map(|command| command.operation_id),
        )
    }

    /// Checks all provider-independent transition invariants before storage or replay.
    pub fn validate(&self) -> Result<()> {
        IdempotencyKey::new(self.idempotency_key.0.clone())?;
        if self.input_digest != input_digest(&self.input)?
            || self.prior.machine != self.next.machine
            || self.next.revision
                != self
                    .prior
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("workflow revision exhausted".into()))?
            || self.next.state != self.transition.state
        {
            return Err(Error::Conflict(
                "workflow transition envelope is inconsistent".into(),
            ));
        }
        validate_commands(&self.transition.commands)?;
        if self
            .transition
            .commands
            .iter()
            .any(|command| command.operation_id == self.operation_id)
        {
            return Err(Error::Conflict(
                "workflow command reuses its transition identity".into(),
            ));
        }
        Ok(())
    }
}

fn validate_commands(commands: &[WorkflowCommand]) -> Result<()> {
    if commands.len() > 1_024 {
        return Err(Error::Invalid(
            "workflow command count exceeds its bound".into(),
        ));
    }
    let mut identities = BTreeSet::new();
    for command in commands {
        command.validate()?;
        if !identities.insert(command.operation_id) {
            return Err(Error::Conflict(
                "workflow command identity is duplicated".into(),
            ));
        }
    }
    Ok(())
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

/// Immutable identity of one admitted workflow instance. An owner stores this
/// before the first transition so an empty journal cannot be reopened with a
/// different input or implementation after a lost acknowledgement.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowAdmission {
    /// Stable caller operation for this workflow instance.
    pub operation_id: OperationId,
    /// Canonical digest of the definition and admitted input.
    pub request_digest: [u8; 32],
    /// Exact initial checkpoint pinned before effects begin.
    pub initial: MachineCheckpoint,
}

impl WorkflowAdmission {
    /// Validates the immutable admission envelope before an owner retains it.
    pub fn validate(&self) -> Result<()> {
        if self.request_digest == [0; 32] || self.initial.revision != 0 {
            return Err(Error::Invalid(
                "workflow admission identity is invalid".into(),
            ));
        }
        self.initial.machine.validate()
    }
}

/// Durable journal boundary; one commit atomically stores checkpoint and commands.
#[cfg(feature = "host")]
pub trait WorkflowJournal: Send + Sync {
    /// Atomically retains one exact workflow admission. Providers must return
    /// the existing admission on retries and reject an identity conflict.
    fn admit<'a>(
        &'a self,
        admission: WorkflowAdmission,
    ) -> BoxFuture<'a, Result<WorkflowAdmission>>;

    /// Observes the exact owner-retained admission before replay or dispatch.
    fn admission(&self) -> BoxFuture<'_, Result<Option<WorkflowAdmission>>>;

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

/// Complete in-process journal for local resumable work and conformance
/// tests. Admission, checkpoint transitions, and command outboxes are retained
/// under one lock; production providers can implement the same contract on
/// durable storage.
#[cfg(feature = "host")]
#[derive(Default)]
pub struct MemoryWorkflowJournal {
    state: Mutex<MemoryWorkflowState>,
}

#[cfg(feature = "host")]
#[derive(Default)]
struct MemoryWorkflowState {
    admission: Option<WorkflowAdmission>,
    records: Vec<(IdempotencyKey, WorkflowRecord)>,
}

#[cfg(feature = "host")]
impl WorkflowJournal for MemoryWorkflowJournal {
    fn admit<'a>(
        &'a self,
        admission: WorkflowAdmission,
    ) -> BoxFuture<'a, Result<WorkflowAdmission>> {
        Box::pin(async move {
            admission.validate()?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::Storage("workflow journal poisoned".into()))?;
            match &state.admission {
                Some(existing) if existing == &admission => Ok(existing.clone()),
                Some(_) => Err(Error::Conflict(
                    "workflow admission identity is already bound".into(),
                )),
                None => {
                    if !state.records.is_empty() {
                        return Err(Error::Conflict(
                            "workflow already has transitions without admission".into(),
                        ));
                    }
                    state.admission = Some(admission.clone());
                    Ok(admission)
                }
            }
        })
    }

    fn replay(&self) -> BoxFuture<'_, Result<Vec<WorkflowRecord>>> {
        Box::pin(async move {
            let records = self
                .state
                .lock()
                .map(|state| {
                    state
                        .records
                        .iter()
                        .map(|(_, record)| record.clone())
                        .collect::<Vec<_>>()
                })
                .map_err(|_| Error::Storage("workflow journal poisoned".into()))?;
            let mut reserved = BTreeSet::new();
            let mut terminal = false;
            for record in &records {
                if terminal {
                    return Err(Error::Storage(
                        "workflow history continues after a terminal transition".into(),
                    ));
                }
                record.validate()?;
                if record
                    .operation_ids()
                    .any(|operation_id| !reserved.insert(operation_id))
                {
                    return Err(Error::Storage(
                        "workflow history reuses a command or transition identity".into(),
                    ));
                }
                terminal = !matches!(&record.transition.status, MachineStatus::Suspended);
            }
            Ok(records)
        })
    }

    fn admission(&self) -> BoxFuture<'_, Result<Option<WorkflowAdmission>>> {
        Box::pin(async move {
            self.state
                .lock()
                .map(|state| state.admission.clone())
                .map_err(|_| Error::Storage("workflow journal poisoned".into()))
        })
    }

    fn commit<'a>(
        &'a self,
        expected_revision: u64,
        idempotency_key: IdempotencyKey,
        record: WorkflowRecord,
    ) -> BoxFuture<'a, Result<WorkflowCommitOutcome>> {
        Box::pin(async move {
            record.validate()?;
            if record.idempotency_key != idempotency_key
                || record.prior.revision != expected_revision
            {
                return Err(Error::Invalid(
                    "workflow commit identity or revision is invalid".into(),
                ));
            }
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::Storage("workflow journal poisoned".into()))?;
            if let Some((_, existing)) = state
                .records
                .iter()
                .find(|(key, _)| key == &idempotency_key)
            {
                return if existing == &record {
                    Ok(WorkflowCommitOutcome::Replayed(existing.clone()))
                } else {
                    Err(Error::Conflict(
                        "workflow retry identity is already bound".into(),
                    ))
                };
            }
            if state.records.last().is_some_and(|(_, last)| {
                !matches!(&last.transition.status, MachineStatus::Suspended)
            }) {
                return Err(Error::Conflict(
                    "workflow cannot advance after a terminal transition".into(),
                ));
            }
            let admission = state.admission.as_ref().ok_or_else(|| {
                Error::Conflict("workflow transition has no retained admission".into())
            })?;
            let expected_prior = state
                .records
                .last()
                .map_or(&admission.initial, |(_, last)| &last.next);
            if &record.prior != expected_prior {
                return Err(Error::Conflict(
                    "workflow transition differs from admission".into(),
                ));
            }
            if state.records.len() as u64 != expected_revision {
                return Err(Error::Conflict("stale workflow revision".into()));
            }
            let requested: BTreeSet<_> = record.operation_ids().collect();
            if state.records.iter().any(|(_, existing)| {
                existing
                    .operation_ids()
                    .any(|operation_id| requested.contains(&operation_id))
            }) {
                return Err(Error::Conflict(
                    "workflow command or transition identity is already bound".into(),
                ));
            }
            state.records.push((idempotency_key, record.clone()));
            Ok(WorkflowCommitOutcome::Applied(record))
        })
    }
}

/// Replay-safe async authoring host compiled onto explicit state-machine records.
#[cfg(feature = "host")]
pub struct DurableWorkflowHost {
    registry: MachineRegistry,
    checkpoint: MachineCheckpoint,
    journal: Arc<dyn WorkflowJournal>,
    intents: BTreeMap<OperationId, (IdempotencyKey, WorkflowRecord)>,
    reserved_operations: BTreeSet<OperationId>,
    terminal: bool,
}

#[cfg(feature = "host")]
impl DurableWorkflowHost {
    /// Opens a workflow by replaying and validating every atomic transition.
    pub async fn open(
        registry: MachineRegistry,
        initial: MachineCheckpoint,
        journal: Arc<dyn WorkflowJournal>,
    ) -> Result<Self> {
        let admission = journal
            .admission()
            .await?
            .ok_or_else(|| Error::Conflict("workflow has no retained admission".into()))?;
        admission.validate()?;
        if admission.initial != initial {
            return Err(Error::Conflict(
                "workflow initial checkpoint differs from admission".into(),
            ));
        }
        registry.validate_checkpoint(&initial)?;
        let mut checkpoint = initial;
        let mut intents = BTreeMap::new();
        let mut reserved_operations = BTreeSet::new();
        let mut terminal = false;
        for record in journal.replay().await? {
            if terminal {
                return Err(Error::Conflict(
                    "workflow history continues after a terminal transition".into(),
                ));
            }
            record.validate()?;
            if !reserved_operations.insert(record.operation_id)
                || record
                    .transition
                    .commands
                    .iter()
                    .any(|command| !reserved_operations.insert(command.operation_id))
            {
                return Err(Error::Conflict(
                    "workflow history reuses a command or transition identity".into(),
                ));
            }
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
            if record.prior != checkpoint {
                return Err(Error::Conflict("workflow history is not contiguous".into()));
            }
            let (next, transition) = registry.step(&checkpoint, &record.input)?;
            if next != record.next || transition != record.transition {
                return Err(Error::Conflict(
                    "workflow history disagrees with its pinned machine".into(),
                ));
            }
            checkpoint = next;
            terminal = !matches!(&record.transition.status, MachineStatus::Suspended);
        }
        Ok(Self {
            registry,
            checkpoint,
            journal,
            intents,
            reserved_operations,
            terminal,
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
        self.step_checked(operation_id, idempotency_key, input, |_| Ok(()))
            .await
    }

    /// Validates a transition before its checkpoint and outbox are committed.
    /// This is used by typed task/tool wrappers to enforce result schemas at
    /// the durable boundary, including on idempotent replay.
    pub async fn step_checked<F>(
        &mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        input: Value,
        check: F,
    ) -> Result<MachineTransition>
    where
        F: FnOnce(&MachineTransition) -> Result<()>,
    {
        IdempotencyKey::new(idempotency_key.0.clone())?;
        if let Some((existing_key, record)) = self.intents.get(&operation_id) {
            return if existing_key == &idempotency_key && record.input == input {
                check(&record.transition)?;
                Ok(record.transition.clone())
            } else {
                Err(Error::Conflict(
                    "workflow operation identity is bound to another input".into(),
                ))
            };
        }
        if self.terminal {
            return Err(Error::Conflict(
                "workflow cannot advance after a terminal transition".into(),
            ));
        }
        if self.reserved_operations.contains(&operation_id) {
            return Err(Error::Conflict(
                "workflow step reuses a command identity".into(),
            ));
        }
        let (next, transition) = self.registry.step(&self.checkpoint, &input)?;
        check(&transition)?;
        let record = WorkflowRecord {
            operation_id,
            idempotency_key: idempotency_key.clone(),
            input_digest: input_digest(&input)?,
            prior: self.checkpoint.clone(),
            input,
            transition: transition.clone(),
            next: next.clone(),
        };
        record.validate()?;
        if record
            .transition
            .commands
            .iter()
            .any(|command| self.reserved_operations.contains(&command.operation_id))
        {
            return Err(Error::Conflict(
                "workflow command identity was already committed".into(),
            ));
        }
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
        self.terminal = !matches!(&record.transition.status, MachineStatus::Suspended);
        self.reserved_operations.insert(operation_id);
        for command in &record.transition.commands {
            self.reserved_operations.insert(command.operation_id);
        }
        self.intents.insert(operation_id, (idempotency_key, record));
        Ok(transition)
    }
}

#[cfg(feature = "host")]
fn input_digest(input: &Value) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest(input)
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
    use serde_json::json;

    #[test]
    fn v2_workflow_admission_fixture_is_canonical() -> Result<()> {
        let fixture = include_str!("../fixtures/v2/workflow-admission.json").trim();
        let admission: WorkflowAdmission =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        admission.validate()?;
        assert_eq!(
            crate::contract::canonical_json_bytes(&admission)?,
            fixture.as_bytes()
        );
        Ok(())
    }

    #[tokio::test]
    async fn journal_replays_an_exact_old_retry_after_later_commits() -> Result<()> {
        use crate::{
            AgentId,
            conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
            resources::ProviderRef,
        };
        let journal = MemoryWorkflowJournal::default();
        let initial = MachineCheckpoint {
            machine: MachineIdentity {
                name: "test.tool".into(),
                version: "1".into(),
                digest: [1; 32],
            },
            revision: 0,
            state: json!(0),
        };
        let admission = WorkflowAdmission {
            operation_id: OperationId::from_bytes([3; 16]),
            request_digest: [4; 32],
            initial: initial.clone(),
        };
        journal.admit(admission.clone()).await?;
        let make_record = |operation_id,
                           key: IdempotencyKey,
                           prior: MachineCheckpoint|
         -> Result<WorkflowRecord> {
            let next = MachineCheckpoint {
                machine: prior.machine.clone(),
                revision: prior.revision + 1,
                state: json!(prior.revision + 1),
            };
            let input = json!(1);
            Ok(WorkflowRecord {
                operation_id,
                idempotency_key: key,
                input_digest: input_digest(&input)?,
                prior,
                input,
                transition: MachineTransition {
                    state: next.state.clone(),
                    commands: vec![],
                    status: MachineStatus::Suspended,
                },
                next,
            })
        };
        let first_key = IdempotencyKey::new("first")?;
        let mut first = make_record(OperationId::from_bytes([5; 16]), first_key.clone(), initial)?;
        let payload = FileRef::new(
            VolumeRef::new(
                ProviderRef::new("workflow-test", "filesystem", "2")?,
                "private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([4; 16])),
            )?,
            "commands/first.json",
            "immutable-version",
            FileDescriptor::from_bytes(b"{}", "application/json")?,
            "first.json",
        )?;
        first.transition.commands.push(WorkflowCommand {
            operation_id: OperationId::from_bytes([8; 16]),
            kind: "test.publish".into(),
            payload: payload.clone(),
        });
        assert!(matches!(
            journal.commit(0, first_key.clone(), first.clone()).await?,
            WorkflowCommitOutcome::Applied(_)
        ));
        let orphan = MemoryWorkflowJournal::default();
        {
            let mut orphan_state = orphan
                .state
                .lock()
                .map_err(|_| Error::Storage("workflow journal poisoned".into()))?;
            orphan_state
                .records
                .push((first_key.clone(), first.clone()));
        }
        assert!(matches!(
            orphan.admit(admission).await,
            Err(Error::Conflict(_))
        ));
        let second_key = IdempotencyKey::new("second")?;
        let second = make_record(
            OperationId::from_bytes([6; 16]),
            second_key.clone(),
            first.next.clone(),
        )?;
        let reused_transition = make_record(
            OperationId::from_bytes([8; 16]),
            IdempotencyKey::new("reused-transition")?,
            first.next.clone(),
        )?;
        assert!(matches!(
            journal
                .commit(
                    1,
                    reused_transition.idempotency_key.clone(),
                    reused_transition
                )
                .await,
            Err(Error::Conflict(_))
        ));
        let mut reused_command = second.clone();
        reused_command.transition.commands.push(WorkflowCommand {
            operation_id: OperationId::from_bytes([8; 16]),
            kind: "test.publish".into(),
            payload,
        });
        assert!(matches!(
            journal.commit(1, second_key.clone(), reused_command).await,
            Err(Error::Conflict(_))
        ));
        assert!(matches!(
            journal.commit(1, second_key, second).await?,
            WorkflowCommitOutcome::Applied(_)
        ));
        assert_eq!(
            journal.commit(0, first_key.clone(), first.clone()).await?,
            WorkflowCommitOutcome::Replayed(first.clone())
        );
        let mut changed = first;
        changed.operation_id = OperationId::from_bytes([7; 16]);
        assert!(matches!(
            journal.commit(0, first_key, changed).await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    struct Counter {
        identity: MachineIdentity,
        schema: Value,
        command_payload: FileRef,
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
                commands: vec![WorkflowCommand {
                    operation_id: OperationId::from_bytes([9; 16]),
                    kind: "counter.publish".into(),
                    payload: self.command_payload.clone(),
                }],
                status: if next >= 2 {
                    MachineStatus::Completed { value: json!(next) }
                } else {
                    MachineStatus::Suspended
                },
            })
        }
    }

    #[tokio::test]
    async fn checkpoint_and_commands_commit_as_one_replayable_record() -> Result<()> {
        use crate::{
            AgentId,
            conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
            resources::ProviderRef,
        };
        let volume = VolumeRef::new(
            ProviderRef::new("workflow-test", "filesystem", "2")?,
            "private",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([4; 16])),
        )?;
        let command_payload = FileRef::new(
            volume,
            "commands/count.json",
            "immutable-version",
            FileDescriptor::from_bytes(b"2", "application/json")?,
            "count.json",
        )?;
        let identity = MachineIdentity {
            name: "example.counter".into(),
            version: "1".into(),
            digest: [1; 32],
        };
        let mut registry = MachineRegistry::default();
        registry.register(Arc::new(Counter {
            identity: identity.clone(),
            schema: json!({"type": "integer", "minimum": 0}),
            command_payload: command_payload.clone(),
        }))?;
        let journal = Arc::new(MemoryWorkflowJournal::default());
        let initial = MachineCheckpoint {
            machine: identity,
            revision: 0,
            state: json!(0),
        };
        let unadmitted = Arc::new(MemoryWorkflowJournal::default());
        assert!(matches!(
            DurableWorkflowHost::open(registry.clone(), initial.clone(), unadmitted).await,
            Err(Error::Conflict(_))
        ));
        let admission = WorkflowAdmission {
            operation_id: OperationId::from_bytes([3; 16]),
            request_digest: [7; 32],
            initial: initial.clone(),
        };
        assert_eq!(journal.admit(admission.clone()).await?, admission);
        assert_eq!(journal.admit(admission.clone()).await?, admission);
        let mut changed_admission = admission;
        changed_admission.request_digest = [8; 32];
        assert!(matches!(
            journal.admit(changed_admission).await,
            Err(Error::Conflict(_))
        ));
        let mut host =
            DurableWorkflowHost::open(registry.clone(), initial.clone(), journal.clone()).await?;
        assert!(matches!(
            host.step_checked(
                OperationId::from_bytes([1; 16]),
                IdempotencyKey::new("step-1")?,
                json!(2),
                |_| Err(Error::Invalid("output schema rejected transition".into())),
            )
            .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(host.checkpoint().revision, 0);
        assert!(journal.replay().await?.is_empty());
        let transition = host
            .step(
                OperationId::from_bytes([1; 16]),
                IdempotencyKey::new("step-1")?,
                json!(2),
            )
            .await?;
        assert_eq!(
            transition.commands,
            vec![WorkflowCommand {
                operation_id: OperationId::from_bytes([9; 16]),
                kind: "counter.publish".into(),
                payload: command_payload,
            }]
        );
        let mut unsafe_command = serde_json::to_value(&transition.commands[0])
            .map_err(|error| Error::Invalid(error.to_string()))?;
        unsafe_command["body"] = json!("inline attachment bytes");
        assert!(serde_json::from_value::<WorkflowCommand>(unsafe_command).is_err());
        assert!(
            validate_commands(&[
                transition.commands[0].clone(),
                transition.commands[0].clone(),
            ])
            .is_err()
        );
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
        assert!(matches!(
            host.step(
                OperationId::from_bytes([2; 16]),
                IdempotencyKey::new("after-terminal")?,
                json!(1),
            )
            .await,
            Err(Error::Conflict(_))
        ));
        let mut reopened = DurableWorkflowHost::open(registry, initial, journal).await?;
        assert_eq!(reopened.checkpoint(), host.checkpoint());
        assert_eq!(
            reopened
                .step(
                    OperationId::from_bytes([1; 16]),
                    IdempotencyKey::new("step-1")?,
                    json!(2),
                )
                .await?,
            transition
        );
        assert!(matches!(
            reopened
                .step(
                    OperationId::from_bytes([2; 16]),
                    IdempotencyKey::new("after-terminal")?,
                    json!(1),
                )
                .await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }
}
