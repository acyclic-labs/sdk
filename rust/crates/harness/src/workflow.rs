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

fn copy_machine_identity(
    source: &MachineIdentity, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<MachineIdentity> {
    Ok(MachineIdentity {
        name: crate::core::copy_text(&source.name, quota)?,
        version: crate::core::copy_text(&source.version, quota)?,
        digest: source.digest,
    })
}

fn copy_machine_checkpoint(
    source: &MachineCheckpoint, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<MachineCheckpoint> {
    Ok(MachineCheckpoint {
        machine: copy_machine_identity(&source.machine, quota)?,
        revision: source.revision,
        state: crate::core::copy_value(&source.state, quota)?,
    })
}

fn copy_machine_transition(
    source: &MachineTransition, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<MachineTransition> {
    Ok(MachineTransition {
        state: crate::core::copy_value(&source.state, quota)?,
        commands: crate::core::copy_vector(&source.commands, quota, |command, quota| {
            Ok(WorkflowCommand {
                operation_id: command.operation_id,
                kind: crate::core::copy_text(&command.kind, quota)?,
                payload: crate::core::copy_file(&command.payload, quota)?,
            })
        })?,
        status: match &source.status {
            MachineStatus::Suspended => MachineStatus::Suspended,
            MachineStatus::Completed { value } => MachineStatus::Completed {
                value: crate::core::copy_value(value, quota)?,
            },
            MachineStatus::Failed { message } => MachineStatus::Failed {
                message: crate::core::copy_text(message, quota)?,
            },
        },
    })
}

fn copy_workflow_key(
    source: &IdempotencyKey, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<IdempotencyKey> {
    Ok(IdempotencyKey(crate::core::copy_text(&source.0, quota)?))
}

fn copy_workflow_record(
    source: &WorkflowRecord, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<WorkflowRecord> {
    Ok(WorkflowRecord {
        operation_id: source.operation_id,
        idempotency_key: copy_workflow_key(&source.idempotency_key, quota)?,
        input_digest: source.input_digest,
        prior: copy_machine_checkpoint(&source.prior, quota)?,
        input: crate::core::copy_value(&source.input, quota)?,
        transition: copy_machine_transition(&source.transition, quota)?,
        next: copy_machine_checkpoint(&source.next, quota)?,
    })
}

macro_rules! workflow_physical_copy {
    ($name:ident, $kind:ty, $factory:path) => {
        pub(crate) fn $name(
            source: &$kind, io: Option<&crate::executor::PreparationIo>,
        ) -> Result<$kind> {
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                let mut observer = io.history_io();
                let mut work = Some(&mut observer as &mut dyn crate::store::history::HistoryIo);
                let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
                let copied = $factory(source, &mut quota)?;
                quota.transfer();
                return Ok(copied);
            }
            let _ = io;
            Ok(source.clone())
        }
    };
}

workflow_physical_copy!(workflow_copy_text, String, crate::core::copy_text);
workflow_physical_copy!(workflow_copy_value, Value, crate::core::copy_value);
workflow_physical_copy!(workflow_copy_identity, MachineIdentity, copy_machine_identity);
workflow_physical_copy!(workflow_copy_checkpoint, MachineCheckpoint, copy_machine_checkpoint);
workflow_physical_copy!(workflow_copy_transition, MachineTransition, copy_machine_transition);
workflow_physical_copy!(workflow_copy_record, WorkflowRecord, copy_workflow_record);
workflow_physical_copy!(workflow_copy_key, IdempotencyKey, copy_workflow_key);

/// Replay-safe command emitted by a machine.
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
        self.validate_with_work(&mut None)
    }

    pub(crate) fn validate_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
            observer.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = work.as_ref().is_some_and(|observer| {
            observer.original_native_work().is_some()
        });
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let native = false;
        if native {
            if !crate::contract::is_valid_component_label(&self.kind) {
                return Err(workflow_command_invalid());
            }
        } else {
            crate::contract::validate_component_label(&self.kind, "workflow command kind")?;
        }
        self.payload.validate(work)
    }
}

fn workflow_command_invalid() -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    { Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid,
        reason: "workflow command kind is invalid",
    } }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { Error::Invalid("workflow command kind is invalid".into()) }
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
        self.validate_with_io(None)
    }

    /// Builds the pinned SDK identity with paid strings before their growth.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn new_with_native_work(
        name: &str, version: &str, digest: [u8; 32],
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let result = (|| {
            workflow_label(name, "machine name", Some(&io))?;
            workflow_label(version, "machine version", Some(&io))?;
            if digest == [0; 32] {
                return Err(workflow_invalid_error(Some(&io),
                    "machine implementation digest must be a nonzero 32-byte value"));
            }
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
            use crate::store::history::HistoryIo as _;
            let mut observer = io.history_io();
            Ok(Self {
                name: observer.clone_text(name)?, version: observer.clone_text(version)?, digest,
            })
        })();
        let retained = io.owned_bytes()?;
        finish_native_io(result, io, retained)
    }

    fn validate_with_io(&self, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        workflow_label(&self.name, "machine name", io)?;
        workflow_label(&self.version, "machine version", io)?;
        if self.digest == [0; 32] {
            return Err(workflow_invalid_error(io,
                "machine implementation digest must be a nonzero 32-byte value"));
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

    /// Borrows the original SDK-published identity without an ordinary callback.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn identity_with_native_work<'a>(
        &'a self, _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<&'a MachineIdentity> {
        Err(native_workflow_unsupported("machine has no native identity callback"))
    }

    /// Returns the JSON Schema for durable state.
    fn state_schema(&self) -> &Value;

    /// Creates initial durable state from an admitted input.
    fn initialize(&self, input: &Value) -> Result<Value>;

    /// Builds initial native state after admitting its actual SDK-owned DTOs.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn initialize_with_native_work(
        &self,
        _input: &Value,
        _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        Err(native_workflow_unsupported("machine has no native initialization callback"))
    }

    /// Applies one durable input synchronously and deterministically.
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition>;

    /// Whether genuine native identity, initialization and transition callbacks are available.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_work(&self) -> bool {
        false
    }

    /// Moves the response into native hosting state after admitting construction.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn transition_with_native_work(
        &self,
        _state: &Value,
        _input: Value,
        _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        Err(native_workflow_unsupported("machine has no native state callback"))
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
type WorkflowCustody = Option<acyclic_fs::NativeAllocation>;
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
type WorkflowCustody = Option<()>;

/// Immutable registry used to restore pinned resumable work.
#[derive(Clone, Default)]
pub struct MachineRegistry(BTreeMap<(String, String, [u8; 32]), Arc<dyn ResumableMachine>>);

impl MachineRegistry {
    /// Registers one exact implementation.
    pub fn register(&mut self, machine: Arc<dyn ResumableMachine>) -> Result<()> {
        self.register_with_io(machine, None)
    }

    pub(crate) fn register_with_io(
        &mut self, machine: Arc<dyn ResumableMachine>, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if io.is_some() && !machine.supports_native_work() {
            return Err(native_workflow_unsupported("machine has no native state callbacks"));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let identity = if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| {
                native_workflow_unsupported("native machine metadata requires original work")
            })?;
            machine.identity_with_native_work(work)?
        } else {
            machine.identity()
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let identity = machine.identity();
        identity.validate_with_io(io)?;
        crate::contract::compile_json_schema(machine.state_schema(), "machine state", io)?;
        let key = (
            workflow_copy_text(&identity.name, io)?,
            workflow_copy_text(&identity.version, io)?,
            identity.digest,
        );
        if self.0.contains_key(&key) {
            return Err(workflow_recovery_error(io, "machine identity is already registered", false));
        }
        self.0.insert(key, machine);
        Ok(())
    }

    /// Registers through the same engine, retaining its real key metadata.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn register_with_native_work(
        &mut self, machine: Arc<dyn ResumableMachine>, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeAllocation> {
        if !machine.supports_native_work() {
            return Err(native_workflow_unsupported("machine has no native state callbacks"));
        }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let result = self.register_with_io(machine, Some(&io));
        let retained = io.owned_bytes()?;
        let (_, custody) = finish_native_io(result, io, retained)?;
        Ok(custody)
    }

    /// Resolves pinned metadata on the optional history observer.
    pub(crate) fn resolve(
        &self, identity: &MachineIdentity,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Option<&Arc<dyn ResumableMachine>>> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
            observer.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
            observer.codec_charge(crate::contract::JsonCodecCharge::Work {
                bytes: identity.name.len() as u64 + identity.version.len() as u64 + 32,
                items: self.0.len() as u64,
            })?;
        }
        Ok(self.0.iter().find_map(|((name, version, digest), machine)| {
            (name == &identity.name && version == &identity.version && digest == &identity.digest)
                .then_some(machine)
        }))
    }

    /// Applies one replay-safe step and returns the next checkpoint.
    pub fn step(
        &self,
        checkpoint: &MachineCheckpoint,
        input: &Value,
    ) -> Result<(MachineCheckpoint, MachineTransition)> {
        self.step_with_io(checkpoint, input, None, None)
            .map(|(next, transition, _, _)| (next, transition))
    }

    fn step_with_io(
        &self,
        checkpoint: &MachineCheckpoint,
        input: &Value,
        #[allow(unused_variables)] native_input: Option<Value>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(MachineCheckpoint, MachineTransition, u64, u64)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let machine = if io.is_some() {
            self.native_machine(&checkpoint.machine)?
        } else {
            self.resolve(&checkpoint.machine, &mut None)?.ok_or_else(|| {
                Error::Unsupported("pinned machine implementation is unavailable".into())
            })?
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let machine = self.resolve(&checkpoint.machine, &mut None)?.ok_or_else(|| {
            Error::Unsupported("pinned machine implementation is unavailable".into())
        })?;
        #[allow(unused_mut)]
        let mut transition_retained = 0;
        crate::contract::validate_json_schema_value(machine.state_schema(), &checkpoint.state, "machine state", io)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let transition = if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| native_workflow_unsupported(
                "native state requires the original ledger",
            ))?;
            let input = native_input.ok_or_else(|| native_workflow_unsupported(
                "native state requires owned input",
            ))?;
            let (transition, custody) =
                machine.transition_with_native_work(&checkpoint.state, input, work)?;
            transition_retained = custody.bytes();
            io.absorb_native_custody(custody)?;
            transition
        } else {
            machine.transition(&checkpoint.state, input)?
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let transition = machine.transition(&checkpoint.state, input)?;
        crate::contract::validate_json_schema_value(machine.state_schema(), &transition.state, "machine state", io)?;
        validate_commands(&transition.commands, io)?;
        let before = workflow_owned_bytes(io)?;
        let next = MachineCheckpoint {
            machine: workflow_copy_identity(&checkpoint.machine, io)?,
            revision: workflow_next_revision(checkpoint.revision, io)?,
            state: workflow_copy_value(&transition.state, io)?,
        };
        let checkpoint_retained = workflow_owned_bytes(io)? - before;
        Ok((next, transition, checkpoint_retained, transition_retained))
    }

    /// Rejects custom machines before native invocation or provider IO.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_machine(&self, identity: &MachineIdentity) -> Result<&Arc<dyn ResumableMachine>> {
        let machine = self.resolve(identity, &mut None)?
            .ok_or_else(|| native_workflow_unsupported("pinned native machine is unavailable"))?;
        if !machine.supports_native_work() {
            return Err(native_workflow_unsupported("machine has no native state callback"));
        }
        Ok(machine)
    }

    /// Checks native capability without allocating a lookup key or doing IO.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn require_native_work(&self, identity: &MachineIdentity) -> Result<()> {
        self.native_machine(identity).map(|_| ())
    }

    /// Initializes fresh native state on the original ledger without cloning the lookup key, invoking ordinary initialization, or boxing a future.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn initialize_with_native_work(
        &self,
        identity: &MachineIdentity,
        input: &Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        let machine = self.native_machine(identity)?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let result = (|| {
            let (state, custody) = machine.initialize_with_native_work(input, work)?;
            let retained = custody.bytes();
            io.absorb_native_custody(custody)?;
            crate::contract::validate_json_schema_value(machine.state_schema(), &state, "machine state", Some(&io))?;
            Ok((state, retained))
        })();
        let retained = result.as_ref().map_or(0, |(_, retained)| *retained);
        let ((state, _), custody) = finish_native_io(result, io, retained)?;
        Ok((state, custody))
    }

    /// Runs the original step engine with admitted owned-input construction.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn step_with_native_work(
        &self,
        checkpoint: &MachineCheckpoint,
        input: Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(MachineCheckpoint, MachineTransition, acyclic_fs::NativeAllocation)> {
        self.require_native_work(&checkpoint.machine)?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let result = self.step_with_io(checkpoint, &Value::Null, Some(input), Some(&io));
        let retained = result.as_ref().map_or(0, |(_, _, checkpoint, transition)| {
            checkpoint.saturating_add(*transition)
        });
        let ((next, transition, _, _), custody) = finish_native_io(result, io, retained)?;
        Ok((next, transition, custody))
    }

    /// Validates the original guarded checkpoint without copying its state.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn validate_checkpoint_with_native_work(
        &self, checkpoint: &MachineCheckpoint, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.require_native_work(&checkpoint.machine)?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let result = self.validate_checkpoint(checkpoint, Some(&io));
        let (_, _temporary_custody) = finish_native_io(result, io, 0)?;
        Ok(())
    }

    /// Checks the checkpoint against its pinned machine schema.
    pub(crate) fn validate_checkpoint(
        &self, checkpoint: &MachineCheckpoint, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let machine = if io.is_some() {
            self.native_machine(&checkpoint.machine)?
        } else {
            self.resolve(&checkpoint.machine, &mut None)?.ok_or_else(|| {
                Error::Unsupported("pinned machine implementation is unavailable".into())
            })?
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let machine = self.resolve(&checkpoint.machine, &mut None)?.ok_or_else(|| {
            Error::Unsupported("pinned machine implementation is unavailable".into())
        })?;
        crate::contract::validate_json_schema_value(machine.state_schema(), &checkpoint.state, "machine state", io)
    }

    fn replayed_checkpoint_body(
        &self, checkpoint: &MachineCheckpoint, record: &WorkflowRecord,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(MachineCheckpoint, u64)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native_input = io.map(|io| {
            workflow_copy_value(&record.input, Some(io))
        }).transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let native_input = None;
        let (next, transition, retained, _) = self.step_with_io(
            checkpoint, &record.input, native_input, io,
        )?;
        if next != record.next || transition != record.transition {
            return Err(workflow_recovery_error(io,
                "workflow history disagrees with its pinned machine", false));
        }
        drop(transition);
        Ok((next, retained))
    }

    fn replayed_checkpoint(
        &self, checkpoint: &MachineCheckpoint, record: &WorkflowRecord,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(MachineCheckpoint, WorkflowCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| native_workflow_unsupported(
                "native replay requires original work",
            ))?;
            let proof_io = crate::filesystem::PreparationIo::from_native_work(work)?;
            let result = self.replayed_checkpoint_body(checkpoint, record, Some(&proof_io));
            let retained = result.as_ref().map_or(0, |(_, retained)| *retained);
            let ((checkpoint, _), custody) = finish_native_io(result, proof_io, retained)?;
            return Ok((checkpoint, Some(custody)));
        }
        let (checkpoint, _) = self.replayed_checkpoint_body(checkpoint, record, io)?;
        Ok((checkpoint, None))
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
        self.validate_with_io(None)
    }

    pub(crate) fn validate_with_io(&self, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        validate_workflow_key(&self.idempotency_key, io)?;
        if self.input_digest != crate::executor::preparation_digest(&self.input, io)?
            || self.prior.machine != self.next.machine
            || self.next.revision != workflow_next_revision(self.prior.revision, io)?
            || self.next.state != self.transition.state
        {
            return Err(workflow_recovery_error(io,
                "workflow transition envelope is inconsistent", false));
        }
        validate_commands(&self.transition.commands, io)?;
        if self
            .transition
            .commands
            .iter()
            .any(|command| command.operation_id == self.operation_id)
        {
            return Err(workflow_recovery_error(io,
                "workflow command reuses its transition identity", false));
        }
        Ok(())
    }
}

/// Outcome of an atomic workflow journal commit.
#[derive(Clone, Debug, PartialEq)]
pub enum WorkflowCommitOutcome {
    /// A new record committed.
    Applied(WorkflowRecord),
    /// The exact retry was already committed.
    Replayed(WorkflowRecord),
}

/// Immutable identity of one admitted workflow instance.
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
        self.validate_with_io(None)
    }

    pub(crate) fn validate_with_io(&self, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        if self.request_digest == [0; 32] || self.initial.revision != 0 {
            return Err(workflow_invalid_error(io, "workflow admission identity is invalid"));
        }
        self.initial.machine.validate_with_io(io)
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
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    validate_workflow_key(key, io)?;
    record.validate_with_io(io)?;
    if record.idempotency_key != *key || record.prior.revision != expected_revision {
        return Err(workflow_invalid_error(io, "workflow commit identity or revision is invalid"));
    }
    Ok(())
}

pub(crate) fn validate_workflow_next(
    checkpoint: &MachineCheckpoint,
    terminal: bool,
    reserved: &BTreeSet<OperationId>,
    keys: &BTreeSet<IdempotencyKey>,
    record: &WorkflowRecord,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    record.validate_with_io(io)?;
    if record.prior != *checkpoint || terminal {
        return Err(workflow_recovery_error(io,
            "workflow transition differs from its current checkpoint", false));
    }
    if keys.contains(&record.idempotency_key)
        || record.operation_ids().any(|id| reserved.contains(&id))
    {
        return Err(workflow_recovery_error(io,
            "workflow command, transition or retry identity is already bound", false));
    }
    Ok(())
}

/// Durable journal boundary; one commit atomically stores checkpoint and commands.
pub trait WorkflowJournal: acyclic_stream::ProviderPlatform {
    /// Exact task lease whose coordinator condition fences every publication.
    fn task_binding(&self) -> Option<(crate::TaskId, &crate::scheduler::LeaseFence)> {
        None
    }

    /// Borrows the original admitted identity retained by a bound owner.
    fn original_machine_identity(&self) -> Option<&MachineIdentity> {
        None
    }

    /// Atomically retains one exact workflow admission.
    fn admit<'a>(
        &'a self,
        admission: WorkflowAdmission,
    ) -> BoxFuture<'a, Result<WorkflowAdmission>>;

    /// Observes the exact owner-retained admission before replay or dispatch.
    fn admission(&self) -> BoxFuture<'_, Result<Option<WorkflowAdmission>>>;

    /// Observes admission through the original journal with retained DTO custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn admission_with_native_work<'a>(
        &'a self,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<WorkflowAdmission>, acyclic_fs::NativeAllocation)>> {
        Err(native_workflow_unsupported("journal has no native admission callback"))
    }

    /// Retains fresh admission using the original fenced admission publication.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn admit_with_native_work<'a>(
        &'a self,
        _admission: WorkflowAdmission,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (WorkflowAdmission, acyclic_fs::NativeAllocation)>> {
        Err(native_workflow_unsupported("journal has no native admission publication callback"))
    }

    /// Reads at most `maximum` records starting at the zero-based revision.
    fn replay(&self, after: u64, maximum: u32) -> BoxFuture<'_, Result<Vec<WorkflowRecord>>>;

    /// Commits exactly one complete transition with CAS and idempotency.
    fn commit<'a>(
        &'a self,
        expected_revision: u64,
        idempotency_key: IdempotencyKey,
        record: WorkflowRecord,
    ) -> BoxFuture<'a, Result<WorkflowCommitOutcome>>;

    /// Native callbacks must share the ordinary journal and fenced commit engine.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_work(&self) -> bool {
        false
    }

    /// Reads records retaining original-ledger decode custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn replay_with_native_work<'a>(
        &'a self,
        _after: u64,
        _maximum: u32,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<WorkflowRecord>, acyclic_fs::NativeAllocation)>> {
        Err(native_workflow_unsupported("journal has no native replay callback"))
    }

    /// Uses the original CAS, cancellation and coordinator fence.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn commit_with_native_work<'a>(
        &'a self,
        _expected_revision: u64,
        _idempotency_key: IdempotencyKey,
        _record: WorkflowRecord,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (WorkflowCommitOutcome, acyclic_fs::NativeAllocation)>> {
        Err(native_workflow_unsupported("journal has no native commit callback"))
    }
}

/// Complete in-process journal for local resumable work and conformance tests.
#[derive(Default)]
pub struct MemoryWorkflowJournal {
    state: Mutex<MemoryWorkflowState>,
}

#[derive(Default)]
struct MemoryWorkflowState {
    admission: Option<WorkflowAdmission>,
    records: Vec<WorkflowRecord>,
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
                .cloned()
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
            validate_workflow_commit(expected_revision, &idempotency_key, &record, None)?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::Storage("workflow journal poisoned".into()))?;
            if let Some(existing) = state
                .records
                .iter()
                .find(|record| record.idempotency_key == idempotency_key)
            {
                return if existing == &record {
                    Ok(WorkflowCommitOutcome::Replayed(existing.clone()))
                } else {
                    Err(Error::Conflict(
                        "workflow retry identity is already bound".into(),
                    ))
                };
            }
            if state.records.last().is_some_and(|last| {
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
                .map_or(&admission.initial, |last| &last.next);
            if &record.prior != expected_prior {
                return Err(Error::Conflict(
                    "workflow transition differs from admission".into(),
                ));
            }
            if state.records.len() as u64 != expected_revision {
                return Err(Error::Conflict("stale workflow revision".into()));
            }
            let requested: BTreeSet<_> = record.operation_ids().collect();
            if state.records.iter().any(|existing| {
                existing
                    .operation_ids()
                    .any(|operation_id| requested.contains(&operation_id))
            }) {
                return Err(Error::Conflict(
                    "workflow command or transition identity is already bound".into(),
                ));
            }
            state.records.push(record.clone());
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
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    checkpoint_custody: Option<acyclic_fs::NativeAllocation>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    index_custody: Option<acyclic_fs::NativeAllocation>,
}

impl DurableWorkflowHost {
    /// Opens a workflow by replaying and validating every atomic transition.
    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.harness.workflow.open",
            level = "info",
            skip_all,
            fields(rev = crate::obs::Empty, outcome = crate::obs::Empty, error.kind = crate::obs::Empty)
        )
    )]
    pub async fn open(
        registry: MachineRegistry,
        initial: MachineCheckpoint,
        journal: Arc<dyn WorkflowJournal>,
    ) -> Result<Self> {
        crate::obs::outcome(Self::open_untraced(registry, initial, journal, None).await)
    }

    /// Restores the original journal and machine engine on the original work.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn open_with_native_work(
        registry: MachineRegistry,
        initial: MachineCheckpoint,
        journal: Arc<dyn WorkflowJournal>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        registry.require_native_work(&initial.machine)?;
        if !journal.supports_native_work() {
            return Err(native_workflow_unsupported("journal has no native workflow callbacks"));
        }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let result = Self::open_untraced(registry, initial, journal, Some(&io)).await;
        let (host, _retired) = finish_native_io(result, io, 0)?;
        Ok(host)
    }

    async fn open_untraced(
        registry: MachineRegistry,
        initial: MachineCheckpoint,
        journal: Arc<dyn WorkflowJournal>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        let mut observed = workflow_admission_with_io(journal.as_ref(), io).await?;
        let admission = observed.0.take().ok_or_else(|| {
            workflow_recovery_error(io, "workflow has no retained admission", false)
        })?;
        admission.validate_with_io(io)?;
        if admission.initial != initial {
            return Err(workflow_recovery_error(
                io, "workflow initial checkpoint differs from admission", false,
            ));
        }
        registry.validate_checkpoint(&initial, io)?;
        let checkpoint = if io.is_some() { admission.initial } else { initial };
        let mut host = Self {
            registry,
            checkpoint,
            journal,
            intents: BTreeMap::new(),
            reserved_operations: BTreeSet::new(),
            terminal: false,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            checkpoint_custody: observed.1.take(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            index_custody: None,
        };
        let mut keys = BTreeSet::new();
        loop {
            let page = workflow_page_with_io(
                host.journal.as_ref(), host.checkpoint.revision, WORKFLOW_REPLAY_PAGE_RECORDS, io,
            ).await?;
            if page.0.len() > WORKFLOW_REPLAY_PAGE_RECORDS as usize {
                return Err(workflow_recovery_error(io, "workflow provider exceeded page bound", true));
            }
            if page.0.is_empty() {
                break;
            }
            for record in page.0 {
                validate_workflow_next(
                    &host.checkpoint, host.terminal, &host.reserved_operations, &keys, &record, io,
                )?;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if let Some(io) = io {
                    host.reserve_native_index(record.transition.commands.len(), io)?;
                }
                keys.insert(workflow_copy_key(&record.idempotency_key, io)?);
                host.reserved_operations.extend(record.operation_ids());
                host.intents.insert(
                    record.operation_id,
                    (record.prior.revision, crate::executor::preparation_digest(&record, io)?),
                );
                let restored = host.registry.replayed_checkpoint(&host.checkpoint, &record, io)?;
                host.checkpoint = restored.0;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                {
                    host.checkpoint_custody = restored.1;
                }
                host.terminal = !matches!(&record.transition.status, MachineStatus::Suspended);
            }
            // Page data drops before its guard and the next read.
        }
        crate::obs::obs_record!("rev" = host.checkpoint.revision);
        Ok(host)
    }

    /// Returns the latest committed checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &MachineCheckpoint {
        &self.checkpoint
    }

    /// Reads the latest committed outbox/status without retaining transition history.
    pub async fn latest_transition(&self) -> Result<Option<MachineTransition>> {
        Ok(self.latest_transition_with_io(None).await?.0)
    }

    /// Moves the actual guarded latest transition out of its decoded record.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn latest_transition_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<MachineTransition>, acyclic_fs::NativeAllocation)> {
        self.require_original_native_work(work)?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let result = self.latest_transition_with_io(Some(&io)).await;
        let (mut latest, mut custody) = finish_native_io(result, io, 0)?;
        if let Some(mut incoming) = latest.1.take() {
            custody.absorb(&mut incoming).map_err(|cause| Error::NativeWork {
                cause, custody: Some(incoming),
            })?;
        }
        Ok((latest.0, custody))
    }

    async fn latest_transition_with_io(
        &self, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Option<MachineTransition>, WorkflowCustody)> {
        if self.checkpoint.revision == 0 {
            return Ok((None, None));
        }
        let mut page = workflow_page_with_io(
            self.journal.as_ref(), self.checkpoint.revision - 1, 1, io,
        ).await?;
        let [record] = page.0.as_slice() else {
            return Err(workflow_recovery_error(io, "latest workflow record is unavailable", true));
        };
        if record.next != self.checkpoint
            || self.intents.get(&record.operation_id) != Some(&(
                record.prior.revision, crate::executor::preparation_digest(record, io)?,
            ))
        {
            return Err(workflow_recovery_error(io, "latest workflow record changed", true));
        }
        let record = page.0.pop().ok_or_else(|| {
            workflow_recovery_error(io, "latest workflow record is unavailable", true)
        })?;
        Ok((Some(record.transition), page.1.take()))
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
    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.harness.workflow.step",
            level = "info",
            skip_all,
            fields(rev = self.checkpoint.revision, outcome = crate::obs::Empty, error.kind = crate::obs::Empty)
        )
    )]
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
        crate::obs::outcome(
            self.step_checked_untraced(operation_id, idempotency_key, input, check, None)
                .await.map(|(transition, _, _)| transition),
        )
    }

    /// Rejects native-incompatible machine/journal composition before provider IO.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn require_native_work(&self) -> Result<()> {
        self.registry.require_native_work(&self.checkpoint.machine)?;
        if !self.journal.supports_native_work() {
            return Err(native_workflow_unsupported("journal has no native workflow callbacks"));
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn require_original_native_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        self.require_native_work()?;
        let held = self.checkpoint_custody.as_ref().ok_or_else(|| {
            native_workflow_unsupported("workflow was not opened with native custody")
        })?;
        if !held.work().shares_ledger(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "workflow checkpoint belongs to another native ledger",
            });
        }
        Ok(())
    }

    /// Runs the ordinary workflow body with the borrowed original work observer.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn step_checked_with_native_work<F>(
        &mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        input: Value,
        work: &acyclic_fs::NativeMaterializationWork,
        check: F,
    ) -> Result<(MachineTransition, acyclic_fs::NativeAllocation)>
    where
        F: FnOnce(&MachineTransition) -> Result<()>,
    {
        self.require_original_native_work(work)?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let original_index_bytes = self.index_custody.as_ref().map(acyclic_fs::NativeAllocation::bytes);
        let result = self.step_checked_untraced(
            operation_id, idempotency_key, input, check, Some(&io),
        ).await;
        if result.is_err() {
            match (self.index_custody.as_mut(), original_index_bytes) {
                (Some(previous), Some(bytes)) => { let _ = previous.substitute(bytes); }
                _ => self.index_custody = None,
            }
        }
        let retained = result.as_ref().map_or(0, |(_, checkpoint, transition)| {
            checkpoint.saturating_add(*transition)
        });
        let ((transition, checkpoint_retained, _), mut custody) =
            finish_native_io(result, io, retained)?;
        if checkpoint_retained != 0 {
            self.checkpoint_custody = Some(custody.split_off(checkpoint_retained)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?);
        }
        Ok((transition, custody))
    }

    async fn step_checked_untraced<F>(
        &mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        input: Value,
        check: F,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(MachineTransition, u64, u64)>
    where
        F: FnOnce(&MachineTransition) -> Result<()>,
    {
        validate_workflow_key(&idempotency_key, io)?;
        if let Some((revision, digest)) = self.intents.get(&operation_id) {
            let page = workflow_page_with_io(self.journal.as_ref(), *revision, 1, io).await?;
            let [record] = page.0.as_slice() else {
                return Err(workflow_recovery_error(io, "workflow retry record is unavailable", true));
            };
            if crate::executor::preparation_digest(record, io)? != *digest {
                return Err(workflow_recovery_error(io, "workflow retry record changed", true));
            }
            return if record.idempotency_key == idempotency_key && record.input == input {
                check(&record.transition)?;
                let before = workflow_owned_bytes(io)?;
                let transition = workflow_copy_transition(&record.transition, io)?;
                Ok((transition, 0, workflow_owned_bytes(io)? - before))
            } else {
                Err(workflow_recovery_error(io, "workflow operation identity is bound to another input", false))
            };
        }
        if self.terminal {
            return Err(workflow_recovery_error(io, "workflow cannot advance after a terminal transition", false));
        }
        if self.reserved_operations.contains(&operation_id) {
            return Err(workflow_recovery_error(io, "workflow step reuses a command identity", false));
        }
        // The durable v2 envelope retains both input and machine state.
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let (input, native_input) = if io.is_some() {
            (workflow_copy_value(&input, io)?, Some(input))
        } else {
            (input, None)
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let native_input = None;
        let (next, transition, checkpoint_retained, transition_retained) =
            self.registry.step_with_io(&self.checkpoint, &input, native_input, io)?;
        check(&transition)?;
        let record = WorkflowRecord {
            operation_id,
            idempotency_key: workflow_copy_key(&idempotency_key, io)?,
            input_digest: crate::executor::preparation_digest(&input, io)?,
            prior: workflow_copy_checkpoint(&self.checkpoint, io)?,
            input,
            transition: workflow_copy_transition(&transition, io)?,
            next: workflow_copy_checkpoint(&next, io)?,
        };
        record.validate_with_io(io)?;
        if record.transition.commands.iter().any(|command| {
            self.reserved_operations.contains(&command.operation_id)
        }) {
            return Err(workflow_recovery_error(io, "workflow command identity was already committed", false));
        }
        // Ordinary None retains its original digest timing.
        let digest = if io.is_some() {
            Some(crate::executor::preparation_digest(&record, io)?)
        } else {
            None
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io {
            self.reserve_native_index(record.transition.commands.len(), io)?;
        }
        let committed_record = workflow_copy_record(&record, io)?;
        let commit_key = workflow_copy_key(&idempotency_key, io)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let observed = if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| native_workflow_unsupported(
                "native commit requires the original ledger",
            ))?;
            let (observed, custody) = self.journal.commit_with_native_work(
                self.checkpoint.revision, commit_key, committed_record, work,
            )?.await?;
            io.absorb_native_custody(custody)?;
            observed
        } else {
            self.journal.commit(self.checkpoint.revision, commit_key, committed_record).await?
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let observed = self.journal.commit(
            self.checkpoint.revision, commit_key, committed_record,
        ).await?;
        let committed = match observed {
            WorkflowCommitOutcome::Applied(value) | WorkflowCommitOutcome::Replayed(value) => value,
        };
        if committed != record {
            return Err(workflow_recovery_error(io, "workflow retry identity is bound to another transition", false));
        }
        self.checkpoint = next;
        self.terminal = !matches!(&record.transition.status, MachineStatus::Suspended);
        self.reserved_operations.insert(operation_id);
        for command in &record.transition.commands {
            self.reserved_operations.insert(command.operation_id);
        }
        self.intents.insert(operation_id, (record.prior.revision, match digest {
            Some(digest) => digest,
            None => crate::contract::canonical_json_digest(&record)?,
        }));
        Ok((transition, checkpoint_retained, transition_retained))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reserve_native_index(
        &mut self, commands: usize, io: &crate::executor::PreparationIo,
    ) -> Result<()> {
        let work = io.original_native_work().ok_or_else(|| native_workflow_unsupported(
            "native index requires original work",
        ))?;
        let bytes = (commands as u64).checked_add(1)
            .and_then(|count| count.checked_mul(std::mem::size_of::<OperationId>() as u64))
            .and_then(|bytes| bytes.checked_add(
                std::mem::size_of::<(OperationId, (u64, [u8; 32]))>() as u64,
            )).ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native workflow index slot overflow",
            })?;
        let mut incoming = work.retain_owned(bytes)?;
        if let Some(previous) = &mut self.index_custody {
            previous.absorb(&mut incoming).map_err(|cause| Error::NativeWork {
                cause, custody: Some(incoming),
            })?;
        } else {
            self.index_custody = Some(incoming);
        }
        Ok(())
    }
}

pub(crate) fn workflow_owned_bytes(io: Option<&crate::executor::PreparationIo>) -> Result<u64> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        return io.owned_bytes();
    }
    #[cfg(not(feature = "filesystem"))]
    let _ = io;
    Ok(0)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn finish_native_io<T>(
    result: Result<T>, io: crate::filesystem::PreparationIo, retained: u64,
) -> Result<(T, acyclic_fs::NativeAllocation)> {
    match result {
        Ok(value) => Ok((value, io.into_native_custody(retained)?)),
        Err(Error::NativeWork { cause, custody }) => {
            // No cancellation/clock/authority observation after failure.
            let owned = match io.owned_bytes() {
                Ok(owned) => owned,
                Err(_) => return Err(Error::NativeWork { cause, custody }),
            };
            let mut held = match io.into_native_custody(owned) {
                Ok(held) => held,
                Err(_) => return Err(Error::NativeWork { cause, custody }),
            };
            if let Some(mut incoming) = custody {
                if !held.work().shares_ledger(incoming.work()) {
                    return Err(Error::NativeWork { cause, custody: Some(incoming) });
                }
                if held.absorb(&mut incoming).is_err() {
                    return Err(Error::NativeWork { cause, custody: Some(incoming) });
                }
            }
            Err(Error::NativeWork { cause, custody: Some(held) })
        }
        Err(error) => Err(error),
    }
}

fn validate_commands(
    commands: &[WorkflowCommand], io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    let _workspace = io.map(|io| crate::executor::reserve_preparation_workspace(&commands, io)).transpose()?;
    #[cfg(feature = "filesystem")]
    let mut observer = io.map(crate::executor::PreparationIo::history_io);
    #[cfg(feature = "filesystem")]
    let mut original = observer.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
    #[cfg(not(feature = "filesystem"))]
    let mut original = { let _ = io; None };
    let work: &mut Option<&mut dyn crate::store::history::HistoryIo> = &mut original;

    let bytes = commands.len().checked_mul(std::mem::size_of::<OperationId>())
        .ok_or_else(|| {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_ref().is_some_and(|observer| observer.original_native_work().is_some()) {
                return Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "workflow command set ownership overflow" };
            }
            Error::Invalid("workflow command set ownership overflow".into())
        })? as u64;
    let mut custody = crate::wire_codec::CodecQuota::new(work, bytes)?;
    let mut identities = BTreeSet::new();
    for command in commands {
        command.validate_with_work(custody.work())?;
        if let Some(observer) = custody.work().as_deref_mut() {
            observer.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
        }
        if !identities.insert(command.operation_id) {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if custody.work().as_ref().is_some_and(|observer| observer.original_native_work().is_some()) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "workflow command identity is duplicated",
                });
            }
            return Err(Error::Conflict(
                "workflow command identity is duplicated".into(),
            ));
        }
    }
    Ok(())
}

fn validate_workflow_key(
    key: &IdempotencyKey, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    if key.0.is_empty() || key.0.chars().any(char::is_control) {
        return Err(workflow_invalid_error(io, "idempotency key is invalid"));
    }
    Ok(())
}

fn workflow_label(value: &str, field: &str, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io {
        io.check_cancellation()?;
        io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 }, false)?;
        return if crate::contract::is_valid_component_label(value) {
            Ok(())
        } else {
            Err(workflow_invalid_error(Some(io), "workflow component label is invalid"))
        };
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    crate::contract::validate_component_label(value, field)
}

fn workflow_next_revision(revision: u64, io: Option<&crate::executor::PreparationIo>) -> Result<u64> {
    if io.is_some() && revision == u64::MAX {
        return Err(workflow_invalid_error(io, "revision exhausted"));
    }
    next_revision(revision)
}

fn workflow_invalid_error(io: Option<&crate::executor::PreparationIo>, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.is_some() {
        return Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason };
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    Error::Invalid(reason.into())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_workflow_unsupported(reason: &'static str) -> Error {
    Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, reason }
}

async fn workflow_admission_with_io(
    journal: &dyn WorkflowJournal, io: Option<&crate::executor::PreparationIo>,
) -> Result<(Option<WorkflowAdmission>, WorkflowCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io {
        let work = io.original_native_work().ok_or_else(|| native_workflow_unsupported(
            "native admission requires original work",
        ))?;
        let (value, custody) = journal.admission_with_native_work(work)?.await?;
        if !work.shares_ledger(custody.work()) {
            drop(value);
            return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                custody: Some(custody),
            });
        }
        return Ok((value, Some(custody)));
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    Ok((journal.admission().await?, None))
}

async fn workflow_page_with_io(
    journal: &dyn WorkflowJournal, after: u64, maximum: u32,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(Vec<WorkflowRecord>, WorkflowCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io {
        let work = io.original_native_work().ok_or_else(|| native_workflow_unsupported(
            "native replay requires original work",
        ))?;
        let (records, custody) = journal.replay_with_native_work(after, maximum, work)?.await?;
        if !work.shares_ledger(custody.work()) {
            drop(records);
            return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                custody: Some(custody),
            });
        }
        return Ok((records, Some(custody)));
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    Ok((journal.replay(after, maximum).await?, None))
}

fn workflow_recovery_error(
    io: Option<&crate::executor::PreparationIo>, reason: &'static str, storage: bool,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.is_some() {
        return Error::NativeBoundary {
            code: if storage { crate::wire::ErrorCode::Storage } else { crate::wire::ErrorCode::Conflict },
            reason,
        };
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    if storage { Error::Storage(reason.into()) } else { Error::Conflict(reason.into()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
            input_digest: crate::contract::canonical_json_digest(&Value::Null)?,
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
        validate_workflow_commit(prior.revision, &record.idempotency_key, &record, None)?;
        validate_workflow_next(&prior, false, &reserved, &BTreeSet::new(), &record, None)?;
        validate_workflow_page(prior.revision, u32::MAX)?;
        crate::executor::validate_execution_page(prior.revision, u32::MAX)?;
        record.transition.commands[0].operation_id = OperationId::from_bytes(1_u128.to_be_bytes());
        assert!(matches!(
            validate_workflow_next(&prior, false, &reserved, &BTreeSet::new(), &record, None),
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
            let original = last.idempotency_key.clone();
            last.idempotency_key = first_key.clone();
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
            last.idempotency_key = original_key;
            last.operation_id = first;
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
            state.records[0].idempotency_key = IdempotencyKey::new("changed-first")?;
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
            ], None)
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
