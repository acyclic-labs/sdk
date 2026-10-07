//! Explicit versioned resumable state machines for durable authoring.

use crate::IdempotencyKey;
use crate::contract::next_revision;
use crate::{Error, OperationId, Result, conversation::FileRef};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
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
pub trait ResumableMachine: acyclic_stream::ProviderPlatform {
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
        crate::contract::compile_json_schema(machine.state_schema(), "machine state")?;
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
            revision: next_revision(checkpoint.revision)?,
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
            || self.next.revision != next_revision(self.prior.revision)?
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

/// Internal replay batch size; callers may request any positive page allowance.
pub(crate) const WORKFLOW_REPLAY_PAGE_RECORDS: u32 = 64;

pub(crate) fn validate_workflow_page(_after: u64, maximum: u32) -> Result<()> {
    if maximum == 0 {
        return Err(Error::Invalid("workflow page bounds are invalid".into()));
    }
    Ok(())
}

pub(crate) fn validate_workflow_commit(
    expected_revision: u64,
    key: &IdempotencyKey,
    record: &WorkflowRecord,
) -> Result<()> {
    IdempotencyKey::new(key.0.clone())?;
    record.validate()?;
    if record.idempotency_key != *key || record.prior.revision != expected_revision {
        return Err(Error::Invalid(
            "workflow commit identity or revision is invalid".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_workflow_next(
    checkpoint: &MachineCheckpoint,
    terminal: bool,
    reserved: &BTreeSet<OperationId>,
    keys: &BTreeSet<IdempotencyKey>,
    record: &WorkflowRecord,
) -> Result<()> {
    record.validate()?;
    if record.prior != *checkpoint || terminal {
        return Err(Error::Conflict(
            "workflow transition differs from its current checkpoint".into(),
        ));
    }
    if keys.contains(&record.idempotency_key)
        || record.operation_ids().any(|id| reserved.contains(&id))
    {
        return Err(Error::Conflict(
            "workflow command, transition or retry identity is already bound".into(),
        ));
    }
    Ok(())
}

/// Durable journal boundary; one commit atomically stores checkpoint and commands.
pub trait WorkflowJournal: acyclic_stream::ProviderPlatform {
    /// Exact task lease whose coordinator condition fences every publication.
    /// Unbound workflow providers do not authorize registered task execution.
    fn task_binding(&self) -> Option<(crate::TaskId, crate::scheduler::LeaseFence)> {
        None
    }

    /// Atomically retains one exact workflow admission. Providers must return
    /// the existing admission on retries and reject an identity conflict.
    fn admit<'a>(
        &'a self,
        admission: WorkflowAdmission,
    ) -> BoxFuture<'a, Result<WorkflowAdmission>>;

    /// Observes the exact owner-retained admission before replay or dispatch.
    fn admission(&self) -> BoxFuture<'_, Result<Option<WorkflowAdmission>>>;

    /// Reads at most `maximum` records starting at the zero-based revision.
    /// Records and payloads are validated individually; the consuming host
    /// validates the complete chain incrementally. Empty means current tail.
    fn replay(&self, after: u64, maximum: u32) -> BoxFuture<'_, Result<Vec<WorkflowRecord>>>;

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
#[derive(Default)]
pub struct MemoryWorkflowJournal {
    state: Mutex<MemoryWorkflowState>,
}

#[derive(Default)]
struct MemoryWorkflowState {
    admission: Option<WorkflowAdmission>,
    records: Vec<(IdempotencyKey, WorkflowRecord)>,
}

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

    fn replay(&self, after: u64, maximum: u32) -> BoxFuture<'_, Result<Vec<WorkflowRecord>>> {
        Box::pin(async move {
            validate_workflow_page(after, maximum)?;
            let state = self
                .state
                .lock()
                .map_err(|_| Error::Storage("workflow journal poisoned".into()))?;
            let records = state
                .records
                .iter()
                .skip(
                    usize::try_from(after)
                        .map_err(|_| Error::Invalid("workflow revision is not portable".into()))?,
                )
                .take(maximum as usize)
                .map(|(_, record)| record.clone())
                .collect::<Vec<_>>();
            for record in &records {
                record.validate()?;
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
            validate_workflow_commit(expected_revision, &idempotency_key, &record)?;
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
pub struct DurableWorkflowHost {
    registry: MachineRegistry,
    checkpoint: MachineCheckpoint,
    journal: Arc<dyn WorkflowJournal>,
    intents: BTreeMap<OperationId, (u64, [u8; 32])>,
    reserved_operations: BTreeSet<OperationId>,
    terminal: bool,
}

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
        let mut keys = BTreeSet::new();
        loop {
            let page = journal
                .replay(checkpoint.revision, WORKFLOW_REPLAY_PAGE_RECORDS)
                .await?;
            if page.len() > WORKFLOW_REPLAY_PAGE_RECORDS as usize {
                return Err(Error::Storage(
                    "workflow provider exceeded page bound".into(),
                ));
            }
            if page.is_empty() {
                break;
            }
            for record in page {
                validate_workflow_next(
                    &checkpoint,
                    terminal,
                    &reserved_operations,
                    &keys,
                    &record,
                )?;
                keys.insert(record.idempotency_key.clone());
                reserved_operations.extend(record.operation_ids());
                intents.insert(
                    record.operation_id,
                    (
                        record.prior.revision,
                        crate::contract::canonical_json_digest(&record)?,
                    ),
                );
                let (next, transition) = registry.step(&checkpoint, &record.input)?;
                if next != record.next || transition != record.transition {
                    return Err(Error::Conflict(
                        "workflow history disagrees with its pinned machine".into(),
                    ));
                }
                checkpoint = next;
                terminal = !matches!(&record.transition.status, MachineStatus::Suspended);
            }
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

    /// Reads the latest committed outbox/status without retaining transition history.
    pub async fn latest_transition(&self) -> Result<Option<MachineTransition>> {
        if self.checkpoint.revision == 0 {
            return Ok(None);
        }
        let records = self.journal.replay(self.checkpoint.revision - 1, 1).await?;
        let [record] = records.as_slice() else {
            return Err(Error::Storage(
                "latest workflow record is unavailable".into(),
            ));
        };
        if record.next != self.checkpoint
            || self.intents.get(&record.operation_id)
                != Some(&(
                    record.prior.revision,
                    crate::contract::canonical_json_digest(record)?,
                ))
        {
            return Err(Error::Storage("latest workflow record changed".into()));
        }
        Ok(Some(record.transition.clone()))
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
        if let Some((revision, digest)) = self.intents.get(&operation_id) {
            let records = self.journal.replay(*revision, 1).await?;
            let [record] = records.as_slice() else {
                return Err(Error::Storage(
                    "workflow retry record is unavailable".into(),
                ));
            };
            if crate::contract::canonical_json_digest(record)? != *digest {
                return Err(Error::Storage("workflow retry record changed".into()));
            }
            return if record.idempotency_key == idempotency_key && record.input == input {
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
        self.intents.insert(
            operation_id,
            (
                record.prior.revision,
                crate::contract::canonical_json_digest(&record)?,
            ),
        );
        Ok(transition)
    }
}

fn input_digest(input: &Value) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest(input)
}

fn validate_state(schema: &Value, state: &Value) -> Result<()> {
    crate::contract::validate_json_schema_value(schema, state, "machine state")
}

#[cfg(test)]
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

    struct PagingCounter {
        identity: MachineIdentity,
        schema: Value,
    }

    #[test]
    fn workflow_history_and_command_counts_have_no_fixed_ceiling() -> Result<()> {
        use crate::{
            AgentId,
            conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
            resources::ProviderRef,
        };
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
        let prior = MachineCheckpoint {
            machine: MachineIdentity {
                name: "test.large".into(),
                version: "1".into(),
                digest: [1; 32],
            },
            revision: 1_000_001,
            state: json!(0),
        };
        let mut record = WorkflowRecord {
            operation_id: OperationId::from_bytes(1_000_000_u128.to_be_bytes()),
            idempotency_key: IdempotencyKey::new("large-history")?,
            input_digest: input_digest(&Value::Null)?,
            prior: prior.clone(),
            input: Value::Null,
            transition: MachineTransition {
                state: json!(1),
                commands: (100_000_u128..101_025)
                    .map(|id| WorkflowCommand {
                        operation_id: OperationId::from_bytes(id.to_be_bytes()),
                        kind: "test.publish".into(),
                        payload: payload.clone(),
                    })
                    .collect(),
                status: MachineStatus::Suspended,
            },
            next: MachineCheckpoint {
                revision: prior.revision + 1,
                state: json!(1),
                ..prior.clone()
            },
        };
        let reserved = (1_u128..=65_536)
            .map(|id| OperationId::from_bytes(id.to_be_bytes()))
            .collect();
        validate_workflow_commit(prior.revision, &record.idempotency_key, &record)?;
        validate_workflow_next(&prior, false, &reserved, &BTreeSet::new(), &record)?;
        validate_workflow_page(prior.revision, u32::MAX)?;
        crate::executor::validate_execution_page(prior.revision, u32::MAX)?;
        record.transition.commands[0].operation_id = OperationId::from_bytes(1_u128.to_be_bytes());
        assert!(matches!(
            validate_workflow_next(&prior, false, &reserved, &BTreeSet::new(), &record),
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    impl ResumableMachine for PagingCounter {
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
            Ok(MachineTransition {
                state: json!(state.as_u64().unwrap_or(0) + input.as_u64().unwrap_or(0)),
                commands: vec![],
                status: MachineStatus::Suspended,
            })
        }
    }

    #[tokio::test]
    async fn workflow_pages_preserve_old_retries_and_reject_cross_page_corruption() -> Result<()> {
        let identity = MachineIdentity {
            name: "test.paged".into(),
            version: "1".into(),
            digest: [1; 32],
        };
        let mut registry = MachineRegistry::default();
        registry.register(Arc::new(PagingCounter {
            identity: identity.clone(),
            schema: json!({"type":"integer","minimum":0}),
        }))?;
        let initial = MachineCheckpoint {
            machine: identity,
            revision: 0,
            state: json!(0),
        };
        let journal = Arc::new(MemoryWorkflowJournal::default());
        journal
            .admit(WorkflowAdmission {
                operation_id: OperationId::new(),
                request_digest: [2; 32],
                initial: initial.clone(),
            })
            .await?;
        let mut host =
            DurableWorkflowHost::open(registry.clone(), initial.clone(), journal.clone()).await?;
        let first = OperationId::new();
        let first_key = IdempotencyKey::new("paged-first")?;
        let transition = host.step(first, first_key.clone(), json!(1)).await?;
        for index in 1..=WORKFLOW_REPLAY_PAGE_RECORDS {
            host.step(
                OperationId::new(),
                IdempotencyKey::new(format!("paged-{index}"))?,
                json!(1),
            )
            .await?;
        }
        assert_eq!(
            journal.replay(0, WORKFLOW_REPLAY_PAGE_RECORDS).await?.len(),
            WORKFLOW_REPLAY_PAGE_RECORDS as usize
        );
        assert_eq!(
            journal
                .replay(
                    u64::from(WORKFLOW_REPLAY_PAGE_RECORDS),
                    WORKFLOW_REPLAY_PAGE_RECORDS
                )
                .await?
                .len(),
            1
        );
        assert_eq!(
            journal
                .replay(0, WORKFLOW_REPLAY_PAGE_RECORDS + 1)
                .await?
                .len(),
            (WORKFLOW_REPLAY_PAGE_RECORDS + 1) as usize
        );
        assert!(journal.replay(0, 0).await.is_err());
        let mut reopened =
            DurableWorkflowHost::open(registry.clone(), initial.clone(), journal.clone()).await?;
        assert_eq!(
            reopened.checkpoint().revision,
            u64::from(WORKFLOW_REPLAY_PAGE_RECORDS) + 1
        );
        assert_eq!(
            reopened.step(first, first_key.clone(), json!(1)).await?,
            transition
        );
        assert!(
            reopened
                .step(first, first_key.clone(), json!(2))
                .await
                .is_err()
        );
        // A duplicate retry key appears beyond the first page and remains a hard error.
        let original_key = {
            let mut state = journal
                .state
                .lock()
                .map_err(|_| Error::Storage("workflow journal poisoned".into()))?;
            let last = state
                .records
                .last_mut()
                .ok_or_else(|| Error::Storage("missing workflow record".into()))?;
            let original = last.1.idempotency_key.clone();
            last.1.idempotency_key = first_key.clone();
            original
        };
        assert!(
            DurableWorkflowHost::open(registry.clone(), initial.clone(), journal.clone())
                .await
                .is_err()
        );
        {
            let mut state = journal
                .state
                .lock()
                .map_err(|_| Error::Storage("workflow journal poisoned".into()))?;
            let last = state
                .records
                .last_mut()
                .ok_or_else(|| Error::Storage("missing workflow record".into()))?;
            last.1.idempotency_key = original_key;
            last.1.operation_id = first;
        }
        assert!(
            DurableWorkflowHost::open(registry, initial, journal.clone())
                .await
                .is_err()
        );
        // Retry records are loaded by revision and compared with the verified digest.
        {
            let mut state = journal
                .state
                .lock()
                .map_err(|_| Error::Storage("workflow journal poisoned".into()))?;
            state.records[0].1.idempotency_key = IdempotencyKey::new("changed-first")?;
        }
        assert!(matches!(
            reopened.step(first, first_key, json!(1)).await,
            Err(Error::Storage(_))
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
        assert!(
            journal
                .replay(0, WORKFLOW_REPLAY_PAGE_RECORDS)
                .await?
                .is_empty()
        );
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
