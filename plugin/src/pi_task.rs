//! Pi task lifetime over the existing registered workflow and execution owner.

use super::pi_root::PiRootIntent;
use super::{PiScratchAllocation, PiTaskBinding};
use acyclic_harness::{
    OperationId, Outcome,
    conversation::FileRef,
    filesystem::EFFECT_TASK_COMMAND_KIND,
    runtime::TaskDefinition,
    workflow::{
        MachineIdentity, MachineStatus, MachineTransition, ResumableMachine, WorkflowCommand,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(test)]
use serde_json::json;
use std::sync::Arc;

/// Resolves a creator-seeded native intent under the actual admitted child scope before staging its one-shot process effect.
pub(crate) const NATIVE_LAUNCH_TASK_COMMAND_KIND: &str = "acyclic.pi.native.launch.v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiControlInput {
    pub(crate) command: WorkflowCommand,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiControlReply {
    pub(crate) operation_id: OperationId,
    pub(crate) value: Value,
}

/// Immutable admission input.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiTaskInput {
    pub(crate) binding: PiTaskBinding,
    pub(crate) execution: PiTaskExecution,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum PiTaskExecution {
    Host {
        root: PiRootIntent,
        preparation: WorkflowCommand,
    },
    Child {
        launch: WorkflowCommand,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum PiTaskState {
    Preparing { command: WorkflowCommand },
    Allocating { operation: OperationId },
    Hosting {
        allocation: FileRef,
        #[serde(default)]
        reply: Option<PiControlReply>,
    },
    Controlling { allocation: FileRef, operation: OperationId },
    Ready { command: WorkflowCommand },
    Awaiting { operation: OperationId },
    Completed { result: FileRef },
    Failed { message: String },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandResults {
    commands: Vec<CommandResult>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandResult {
    operation_id: OperationId,
    value: Outcome<FileRef>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlResults {
    commands: Vec<PiControlReply>,
}

/// Closed first-party decoder contracts: never select a schema for arbitrary T.
#[cfg(not(target_arch = "wasm32"))]
trait NativeMetadata: serde::de::DeserializeOwned {
    const VALUE_SCHEMA: acyclic_harness::JsonValueSchema;
    const DIAGNOSTIC_LABEL_BYTES: u64;
}

#[cfg(not(target_arch = "wasm32"))]
const PI_COMMAND_DIAGNOSTIC_LABEL_BYTES: u64 =
    super::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES
    + super::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "WorkflowCommand", "operation_id", "kind", "payload",
    ]);

#[cfg(not(target_arch = "wasm32"))]
pub(crate) const PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES: u64 =
    PI_COMMAND_DIAGNOSTIC_LABEL_BYTES
    + super::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "PiTaskInput", "binding", "execution", "PiTaskExecution", "kind",
        "host", "child", "root", "preparation", "launch",
    ]);

#[cfg(not(target_arch = "wasm32"))]
impl NativeMetadata for PiTaskInput {
    const VALUE_SCHEMA: acyclic_harness::JsonValueSchema = acyclic_harness::JsonValueSchema::None;
    const DIAGNOSTIC_LABEL_BYTES: u64 = PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES;
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeMetadata for PiControlInput {
    const VALUE_SCHEMA: acyclic_harness::JsonValueSchema = acyclic_harness::JsonValueSchema::None;
    const DIAGNOSTIC_LABEL_BYTES: u64 = PI_COMMAND_DIAGNOSTIC_LABEL_BYTES
        + super::pi_root_provider::pi_serde_diagnostic_labels_bytes(&["PiControlInput", "command"]);
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeMetadata for CommandResults {
    // Outcome<FileRef> is a closed typed terminal/indeterminate outcome.
    const VALUE_SCHEMA: acyclic_harness::JsonValueSchema = acyclic_harness::JsonValueSchema::None;
    const DIAGNOSTIC_LABEL_BYTES: u64 = PI_COMMAND_DIAGNOSTIC_LABEL_BYTES
        + super::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
            "CommandResults", "commands", "CommandResult", "operation_id", "value",
            "Outcome", "Succeeded", "Failed", "Cancelled", "Indeterminate", "message",
        ]);
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeMetadata for PiTaskState {
    const VALUE_SCHEMA: acyclic_harness::JsonValueSchema = acyclic_harness::JsonValueSchema::Root;
    const DIAGNOSTIC_LABEL_BYTES: u64 = PI_COMMAND_DIAGNOSTIC_LABEL_BYTES
        + super::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
            "PiTaskState", "state", "preparing", "command", "allocating", "operation",
            "hosting", "allocation", "reply", "controlling", "ready", "awaiting",
            "completed", "result", "failed", "message",
            "PiControlReply", "operation_id", "value",
        ]);
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeMetadata for ControlResults {
    // Likewise covers commands[*].value through the genuine SDK shape scan.
    const VALUE_SCHEMA: acyclic_harness::JsonValueSchema = acyclic_harness::JsonValueSchema::Root;
    const DIAGNOSTIC_LABEL_BYTES: u64 = PI_COMMAND_DIAGNOSTIC_LABEL_BYTES
        + super::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
            "ControlResults", "commands", "PiControlReply", "operation_id", "value",
        ]);
}
pub(crate) struct PiTaskMachine {
    identity: MachineIdentity,
    schema: Value,
    #[cfg(not(target_arch = "wasm32"))]
    _native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl PiTaskMachine {
    pub(crate) fn new() -> Self {
        Self {
            identity: MachineIdentity {
                name: "acyclic.pi".to_owned(),
                version: "1".to_owned(),
                digest: machine_digest(),
            },
            schema: serde_json::to_value(state_schema_descriptor()).expect("fixed Pi state schema"),
            #[cfg(not(target_arch = "wasm32"))]
            _native_custody: None,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn new_with_native_work(
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<Arc<Self>> {
        let source_bytes = machine_sources().iter().map(|source| source.len() as u64 + 8).sum();
        work.charge(acyclic_fs::WorkCounters {
            items_examined: source_bytes,
            bytes_hashed: source_bytes,
            ..acyclic_fs::WorkCounters::default()
        }).map_err(native_work_error)?;
        let mut identity = MachineIdentity::new_with_native_work(
            "acyclic.pi", "1", machine_digest(), work,
        )?;
        let mut schema = super::pi_root_provider::guarded_pi_value(
            &state_schema_descriptor(), NATIVE_METADATA_MAXIMUM, work,
        )?;
        identity.1.absorb(&mut schema.1).map_err(native_work_error)?;
        let alignment = std::mem::align_of::<Self>();
        let header = 2 * std::mem::size_of::<usize>();
        let bytes = (header.div_ceil(alignment) * alignment + std::mem::size_of::<Self>()) as u64;
        let mut publication = work.retain_owned(bytes).map_err(native_work_error)?;
        identity.1.absorb(&mut publication).map_err(native_work_error)?;
        work.charge(acyclic_fs::WorkCounters {
            allocation_operations: 1, ..acyclic_fs::WorkCounters::default()
        }).map_err(native_work_error)?;
        Ok(Arc::new(Self {
            identity: identity.0,
            schema: schema.0,
            // Data fields are declared before their original custody.
            _native_custody: Some(identity.1),
        }))
    }

    pub(crate) fn definition(
        self: &Arc<Self>,
    ) -> acyclic_harness::Result<TaskDefinition<PiTaskInput, FileRef>> {
        TaskDefinition::resumable(
            self.clone(),
            serde_json::to_value(task_input_schema(PiRootIntent::schema_descriptor()))
                .map_err(|error| invalid(error.to_string()))?,
            serde_json::to_value(TypeSchema { kind: "object" })
                .map_err(|error| invalid(error.to_string()))?,
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn definition_with_native_work(
        self: &Arc<Self>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(TaskDefinition<PiTaskInput, FileRef>, acyclic_fs::NativeAllocation)> {
        let mut input = super::pi_root_provider::guarded_pi_value(
            &task_input_schema(PiRootIntent::schema_descriptor()), NATIVE_METADATA_MAXIMUM, work,
        )?;
        let mut output = super::pi_root_provider::guarded_pi_value(
            &TypeSchema { kind: "object" }, NATIVE_METADATA_MAXIMUM, work,
        )?;
        input.1.absorb(&mut output.1).map_err(native_work_error)?;
        TaskDefinition::resumable_with_native_work(
            self.clone(), input.0, output.0, input.1, work,
        )
    }
}

fn machine_sources() -> [&'static [u8]; 3] {
    [
        include_bytes!("pi_task.rs").as_slice(),
        include_bytes!("pi_binding.rs").as_slice(),
        include_bytes!("pi_root.rs").as_slice(),
    ]
}

fn machine_digest() -> [u8; 32] {
    // Pin actual deterministic behavior/binding validation, not process setup.
    let mut digest = blake3::Hasher::new();
    for source in machine_sources() {
        digest.update(&(source.len() as u64).to_le_bytes());
        digest.update(source);
    }
    *digest.finalize().as_bytes()
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ObjectSchema<R, P> {
    #[serde(rename = "type")]
    kind: &'static str,
    additional_properties: bool,
    required: R,
    properties: P,
}

fn object_schema<R, P>(required: R, properties: P) -> ObjectSchema<R, P> {
    ObjectSchema { kind: "object", additional_properties: false, required, properties }
}

#[derive(Serialize)]
struct TypeSchema {
    #[serde(rename = "type")]
    kind: &'static str,
}

#[derive(Serialize)]
struct ConstantSchema {
    #[serde(rename = "const")]
    value: &'static str,
}

#[derive(Serialize)]
struct UuidSchema {
    #[serde(rename = "type")]
    kind: &'static str,
    format: &'static str,
}

#[derive(Serialize)]
struct CommandKindSchema {
    #[serde(rename = "enum")]
    kinds: [&'static str; 2],
}

#[derive(Serialize)]
struct CommandProperties {
    operation_id: UuidSchema,
    kind: CommandKindSchema,
    payload: TypeSchema,
}

fn command_schema_descriptor() -> ObjectSchema<[&'static str; 3], CommandProperties> {
    object_schema(["operation_id", "kind", "payload"], CommandProperties {
        operation_id: UuidSchema { kind: "string", format: "uuid" },
        kind: CommandKindSchema { kinds: [EFFECT_TASK_COMMAND_KIND, NATIVE_LAUNCH_TASK_COMMAND_KIND] },
        // FileRef's shared constructor validates shape and semantic invariants.
        payload: TypeSchema { kind: "object" },
    })
}

#[derive(Serialize)]
struct OneOfSchema<T> {
    #[serde(rename = "oneOf")]
    alternatives: T,
}

#[derive(Serialize)]
struct TaggedStateProperties<T> {
    state: ConstantSchema,
    #[serde(flatten)]
    fields: T,
}

#[derive(Serialize)]
struct CommandStateProperties {
    command: ObjectSchema<[&'static str; 3], CommandProperties>,
}

#[derive(Serialize)]
struct OperationStateProperties {
    operation: UuidSchema,
}

#[derive(Serialize)]
struct HostingStateProperties {
    allocation: TypeSchema,
    reply: ReplySchema,
}

#[derive(Serialize)]
struct ControllingStateProperties {
    allocation: TypeSchema,
    operation: UuidSchema,
}

#[derive(Serialize)]
struct CompletedStateProperties {
    result: TypeSchema,
}

#[derive(Serialize)]
struct FailedStateProperties {
    message: TypeSchema,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReplySchema {
    #[serde(rename = "type")]
    kinds: [&'static str; 2],
    additional_properties: bool,
    required: [&'static str; 2],
    properties: ReplyProperties,
}

#[derive(Serialize)]
struct ReplyProperties {
    operation_id: UuidSchema,
    value: EmptySchema,
}

struct EmptySchema;

impl Serialize for EmptySchema {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap as _;
        serializer.serialize_map(Some(0))?.end()
    }
}

fn state_schema_descriptor() -> impl Serialize {
    fn tagged<T>(state: &'static str, fields: T) -> TaggedStateProperties<T> {
        TaggedStateProperties { state: ConstantSchema { value: state }, fields }
    }
    fn operation() -> UuidSchema {
        UuidSchema { kind: "string", format: "uuid" }
    }
    OneOfSchema { alternatives: (
        object_schema(["state", "command"], tagged("preparing", CommandStateProperties {
            command: command_schema_descriptor(),
        })),
        object_schema(["state", "operation"], tagged("allocating", OperationStateProperties {
            operation: operation(),
        })),
        object_schema(["state", "allocation"], tagged("hosting", HostingStateProperties {
            allocation: TypeSchema { kind: "object" },
            reply: ReplySchema {
                kinds: ["object", "null"], additional_properties: false,
                required: ["operation_id", "value"],
                properties: ReplyProperties { operation_id: operation(), value: EmptySchema },
            },
        })),
        object_schema(["state", "allocation", "operation"], tagged("controlling", ControllingStateProperties {
            allocation: TypeSchema { kind: "object" }, operation: operation(),
        })),
        object_schema(["state", "command"], tagged("ready", CommandStateProperties {
            command: command_schema_descriptor(),
        })),
        object_schema(["state", "operation"], tagged("awaiting", OperationStateProperties {
            operation: operation(),
        })),
        object_schema(["state", "result"], tagged("completed", CompletedStateProperties {
            result: TypeSchema { kind: "object" },
        })),
        object_schema(["state", "message"], tagged("failed", FailedStateProperties {
            message: TypeSchema { kind: "string" },
        })),
    ) }
}

#[derive(Serialize)]
struct InputProperties<R> {
    binding: TypeSchema,
    execution: ExecutionSchema<R>,
}

#[derive(Serialize)]
struct ExecutionSchema<R> {
    #[serde(rename = "oneOf")]
    alternatives: (
        ObjectSchema<[&'static str; 3], HostProperties<R>>,
        ObjectSchema<[&'static str; 2], ChildProperties>,
    ),
}

#[derive(Serialize)]
struct HostProperties<R> {
    kind: ConstantSchema,
    root: R,
    preparation: ObjectSchema<[&'static str; 3], CommandProperties>,
}

#[derive(Serialize)]
struct ChildProperties {
    kind: ConstantSchema,
    launch: ObjectSchema<[&'static str; 3], CommandProperties>,
}

fn task_input_schema<R>(root: R) -> ObjectSchema<[&'static str; 2], InputProperties<R>> {
    object_schema(["binding", "execution"], InputProperties {
        binding: TypeSchema { kind: "object" },
        execution: ExecutionSchema {
            alternatives: (
                object_schema(["kind", "root", "preparation"], HostProperties {
                    kind: ConstantSchema { value: "host" }, root, preparation: command_schema_descriptor(),
                }),
                object_schema(["kind", "launch"], ChildProperties {
                    kind: ConstantSchema { value: "child" }, launch: command_schema_descriptor(),
                }),
            ),
        },
    })
}

fn validate_effect_command(command: &WorkflowCommand) -> acyclic_harness::Result<()> {
    command.validate()?;
    if command.kind != EFFECT_TASK_COMMAND_KIND {
        return Err(invalid("Pi task requires the existing effect command"));
    }
    Ok(())
}

fn validate_native_command(command: &WorkflowCommand) -> acyclic_harness::Result<()> {
    command.validate()?;
    if command.kind != EFFECT_TASK_COMMAND_KIND && command.kind != NATIVE_LAUNCH_TASK_COMMAND_KIND {
        return Err(invalid("Pi child requires its retained native launch command"));
    }
    Ok(())
}

fn validate_control_command(command: &WorkflowCommand) -> acyclic_harness::Result<()> {
    command.validate()?;
    if !matches!(command.kind.as_str(),
        "acyclic.task.admit.v1" | "acyclic.task.admit_prepared.v1" |
        "acyclic.task.observe.v1" | "acyclic.mail.send.v1" |
        "acyclic.mail.receive.v1" | "acyclic.timer.v1" | "acyclic.fork.prepare.v1" |
        "acyclic.pi.fork" | "acyclic.pi.import" | "acyclic.pi.writeback" |
        "acyclic.pi.observe-source" | "acyclic.pi.inspect" | "acyclic.pi.diff" |
        "acyclic.pi.operator-approval" | "acyclic.pi.builtin")
    {
        return Err(invalid("Pi control command has no installed scoped adapter"));
    }
    Ok(())
}

pub(crate) fn hosting_allocation(state: &Value) -> acyclic_harness::Result<Option<FileRef>> {
    Ok(match decode::<PiTaskState>(state)? {
        PiTaskState::Hosting { allocation, .. } | PiTaskState::Controlling { allocation, .. } => Some(allocation),
        _ => None,
    })
}

pub(crate) fn control_reply(state: &Value) -> acyclic_harness::Result<Option<PiControlReply>> {
    Ok(match decode::<PiTaskState>(state)? {
        PiTaskState::Hosting { reply, .. } => reply,
        _ => None,
    })
}

/// The caller retains the checkpoint's original custody through extraction and transfers it to the moved body.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn control_reply_owned(
    mut state: Value,
    work: &acyclic_fs::NativeMaterializationWork,
) -> acyclic_harness::Result<(Option<PiControlReply>, acyclic_fs::NativeAllocation)> {
    let (metadata, custody) = native_metadata::<PiTaskState>(
        &MetadataValue(&state, MetadataRole::State), work,
    )?;
    Ok((extract_control_reply(&mut state, metadata, true)?, custody))
}

fn extract_control_reply(
    state: &mut Value,
    metadata: PiTaskState,
    native: bool,
) -> acyclic_harness::Result<Option<PiControlReply>> {
    let PiTaskState::Hosting { reply: Some(mut reply), .. } = metadata else {
        return Ok(None);
    };
    // Closed-state and FileRef validation completed before touching the body.
    let body = state.get_mut("reply").and_then(Value::as_object_mut)
        .and_then(|reply| reply.remove("value"))
        .ok_or_else(|| transition_error("Pi control reply has no value", native))?;
    reply.value = body;
    Ok(Some(reply))
}

impl ResumableMachine for PiTaskMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn identity_with_native_work<'a>(
        &'a self,
        _work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<&'a MachineIdentity> {
        Ok(&self.identity)
    }

    fn state_schema(&self) -> &Value {
        &self.schema
    }

    fn initialize(&self, input: &Value) -> acyclic_harness::Result<Value> {
        encode(&initial_state(decode(input)?, false)?)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn initialize_with_native_work(
        &self,
        input: &Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(Value, acyclic_fs::NativeAllocation)> {
        let mut decoded = native_metadata::<PiTaskInput>(input, work)?;
        let state = initial_state(decoded.0, true)?;
        let mut encoded = super::pi_root_provider::guarded_pi_value(
            &state, NATIVE_METADATA_MAXIMUM, work,
        )?;
        decoded.1.absorb(&mut encoded.1).map_err(native_work_error)?;
        Ok((encoded.0, decoded.1))
    }

    fn transition(
        &self,
        state: &Value,
        input: &Value,
    ) -> acyclic_harness::Result<MachineTransition> {
        let state = decode(state)?;
        let input = transition_input(&state, input)?;
        let (state, command, status) = advance(state, input, false, |value| encode(value))?;
        Ok(MachineTransition {
            state: encode(&state)?,
            commands: command.into_iter().collect(),
            status,
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_work(&self) -> bool {
        true
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn transition_with_native_work(
        &self,
        state: &Value,
        mut input: Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        use super::pi_root_provider::guarded_pi_value;
        let mut state_metadata = native_metadata::<PiTaskState>(
            &MetadataValue(state, MetadataRole::State), work,
        )?;
        let unchanged_hosting = matches!(&state_metadata.0, PiTaskState::Hosting { .. }) && input.is_null();
        let mut moved_reply = None;
        let typed_input = match &state_metadata.0 {
            PiTaskState::Hosting { .. } if !input.is_null() => {
                let mut decoded = native_metadata::<PiControlInput>(&input, work)?;
                state_metadata.1.absorb(&mut decoded.1).map_err(native_work_error)?;
                TransitionInput::Control(decoded.0)
            }
            PiTaskState::Controlling { .. } => {
                require_single_command(&input, true)?;
                let mut decoded = native_metadata::<ControlResults>(
                    &MetadataValue(&input, MetadataRole::Results), work,
                )?;
                state_metadata.1.absorb(&mut decoded.1).map_err(native_work_error)?;
                moved_reply = Some(take_result_body(&mut input)?);
                TransitionInput::Controls(decoded.0)
            }
            PiTaskState::Allocating { .. } | PiTaskState::Awaiting { .. } => {
                require_single_command(&input, true)?;
                let mut decoded = native_metadata::<CommandResults>(&input, work)?;
                state_metadata.1.absorb(&mut decoded.1).map_err(native_work_error)?;
                TransitionInput::Results(decoded.0)
            }
            _ if input.is_null() => TransitionInput::Null,
            _ => TransitionInput::Other,
        };
        let (next, command, status) = advance(state_metadata.0, typed_input, true, |result| {
            let mut encoded = guarded_pi_value(result, NATIVE_METADATA_MAXIMUM, work)?;
            state_metadata.1.absorb(&mut encoded.1).map_err(native_work_error)?;
            Ok(encoded.0)
        })?;
        let mut encoded_state = if unchanged_hosting {
            #[derive(Serialize)]
            struct HostingCheckpoint<'a> {
                state: &'static str,
                allocation: &'a FileRef,
                reply: Option<ReplyCheckpoint<'a>>,
            }
            #[derive(Serialize)]
            struct ReplyCheckpoint<'a> { operation_id: OperationId, value: &'a Value }
            let PiTaskState::Hosting { allocation, reply } = &next else {
                unreachable!("null hosting transition retains hosting state");
            };
            let reply = match reply {
                Some(reply) => Some(ReplyCheckpoint {
                    operation_id: reply.operation_id,
                    value: state.get("reply").and_then(|reply| reply.get("value"))
                        .ok_or_else(|| transition_error("Pi control reply has no value", true))?,
                }),
                None => None,
            };
            guarded_pi_value(&HostingCheckpoint { state: "hosting", allocation, reply },
                work.remaining().map_err(native_work_error)?.bytes_encoded, work)?
        } else {
            guarded_pi_value(&next, NATIVE_METADATA_MAXIMUM, work)?
        };
        state_metadata.1.absorb(&mut encoded_state.1).map_err(native_work_error)?;
        if let Some(body) = moved_reply {
            *encoded_state.0.get_mut("reply").and_then(|reply| reply.get_mut("value"))
                .ok_or_else(|| transition_error("Pi native reply header is missing", true))? = body;
        }
        Ok((MachineTransition {
            state: encoded_state.0,
            // The opaque closed-DTO quota is held before this one-slot Vec grows.
            commands: command.into_iter().collect(),
            status,
        }, state_metadata.1))
    }
}

fn decode<T: serde::de::DeserializeOwned>(value: &Value) -> acyclic_harness::Result<T> {
    serde_json::from_value(value.clone()).map_err(|error| invalid(error.to_string()))
}

fn encode(value: &impl Serialize) -> acyclic_harness::Result<Value> {
    serde_json::to_value(value).map_err(|error| invalid(error.to_string()))
}

fn invalid(message: impl Into<String>) -> acyclic_harness::Error {
    acyclic_harness::Error::Invalid(message.into())
}

fn initial_state(input: PiTaskInput, native: bool) -> acyclic_harness::Result<PiTaskState> {
    input.binding.validate(None,
        input.binding.project_volumes.keys().map(String::as_str), true)?;
    if let PiScratchAllocation::Created(creation) = &input.binding.scratch.allocation {
        // Preserve original SDK generation/provider/storage-name failures.
        creation.validate(&input.binding.scratch.volume)?;
    }
    Ok(match input.execution {
        PiTaskExecution::Host { root, preparation } => {
            if !matches!(input.binding.scratch.allocation, PiScratchAllocation::Preparing) {
                return Err(transition_error("Pi root admission must precede scratch allocation", native));
            }
            root.validate(&input.binding).map_err(|error| {
                if native {
                    transition_error("Pi root intent differs from its admitted binding", true)
                } else {
                    acyclic_harness::Error::Invalid(error)
                }
            })?;
            validate_effect_command(&preparation).map_err(|error| native_validation(
                error, native, "Pi task requires the existing effect command",
            ))?;
            PiTaskState::Preparing { command: preparation }
        }
        PiTaskExecution::Child { launch } => {
            validate_native_command(&launch).map_err(|error| native_validation(
                error, native, "Pi child requires its retained native launch command",
            ))?;
            PiTaskState::Ready { command: launch }
        }
    })
}

enum TransitionInput {
    Null,
    Control(PiControlInput),
    Controls(ControlResults),
    Results(CommandResults),
    Other,
}

fn transition_input(state: &PiTaskState, input: &Value) -> acyclic_harness::Result<TransitionInput> {
    Ok(match state {
        PiTaskState::Hosting { .. } if !input.is_null() => TransitionInput::Control(decode(input)?),
        PiTaskState::Controlling { .. } => TransitionInput::Controls(decode(input)?),
        PiTaskState::Allocating { .. } | PiTaskState::Awaiting { .. } =>
            TransitionInput::Results(decode(input)?),
        _ if input.is_null() => TransitionInput::Null,
        _ => TransitionInput::Other,
    })
}

/// One pure typed engine serves both ordinary and original-work transitions.
fn advance(
    state: PiTaskState,
    input: TransitionInput,
    native: bool,
    mut encode_result: impl FnMut(&FileRef) -> acyclic_harness::Result<Value>,
) -> acyclic_harness::Result<(PiTaskState, Option<WorkflowCommand>, MachineStatus)> {
    let reject = |reason| transition_error(reason, native);
    Ok(match (state, input) {
        (PiTaskState::Hosting { allocation, reply }, TransitionInput::Null) => (
            PiTaskState::Hosting { allocation, reply }, None, MachineStatus::Suspended,
        ),
        (PiTaskState::Hosting { allocation, reply }, TransitionInput::Control(input)) => {
            validate_control_command(&input.command)
                .map_err(|error| native_validation(error, native, "Pi control command has no installed scoped adapter"))?;
            if reply.as_ref().is_some_and(|reply| reply.operation_id == input.command.operation_id) {
                return Err(reject("Pi control result is already committed"));
            }
            (PiTaskState::Controlling { allocation, operation: input.command.operation_id },
                Some(input.command), MachineStatus::Suspended)
        }
        (PiTaskState::Controlling { allocation, operation }, TransitionInput::Controls(input)) => {
            let [reply]: [PiControlReply; 1] = input.commands.try_into()
                .map_err(|_| reject("Pi control differs from its retained command"))?;
            if reply.operation_id != operation {
                return Err(reject("Pi control differs from its retained command"));
            }
            (PiTaskState::Hosting { allocation, reply: Some(reply) }, None, MachineStatus::Suspended)
        }
        (PiTaskState::Preparing { command }, TransitionInput::Null) => {
            validate_effect_command(&command)
                .map_err(|error| native_validation(error, native, "Pi task requires the existing effect command"))?;
            (PiTaskState::Allocating { operation: command.operation_id }, Some(command), MachineStatus::Suspended)
        }
        (PiTaskState::Ready { command }, TransitionInput::Null) => {
            validate_native_command(&command)
                .map_err(|error| native_validation(error, native, "Pi child requires its retained native launch command"))?;
            (PiTaskState::Awaiting { operation: command.operation_id }, Some(command), MachineStatus::Suspended)
        }
        (state @ (PiTaskState::Allocating { .. } | PiTaskState::Awaiting { .. }), TransitionInput::Results(input)) => {
            let allocating = matches!(&state, PiTaskState::Allocating { .. });
            let operation = match state {
                PiTaskState::Allocating { operation } | PiTaskState::Awaiting { operation } => operation,
                _ => unreachable!(),
            };
            let [result]: [CommandResult; 1] = input.commands.try_into()
                .map_err(|_| reject("Pi result differs from its retained effect command"))?;
            if result.operation_id != operation {
                return Err(reject("Pi result differs from its retained effect command"));
            }
            match result.value {
                Outcome::Succeeded(result) => {
                    result.validate()
                        .map_err(|error| native_validation(error, native, "Pi result reference is invalid"))?;
                    if allocating {
                        (PiTaskState::Hosting { allocation: result, reply: None }, None, MachineStatus::Suspended)
                    } else {
                        let value = encode_result(&result)?;
                        (PiTaskState::Completed { result }, None, MachineStatus::Completed { value })
                    }
                }
                Outcome::Failed { message } => (
                    PiTaskState::Failed { message: message.clone() }, None, MachineStatus::Failed { message },
                ),
                Outcome::Cancelled | Outcome::Indeterminate { .. } =>
                    return Err(reject("unsettled Pi effect cannot advance its workflow")),
            }
        }
        _ => return Err(reject("Pi task does not accept this transition")),
    })
}

fn native_validation(
    error: acyclic_harness::Error,
    native: bool,
    reason: &'static str,
) -> acyclic_harness::Error {
    match error {
        acyclic_harness::Error::Invalid(_) if native => transition_error(reason, true),
        error => error,
    }
}

fn transition_error(reason: &'static str, native: bool) -> acyclic_harness::Error {
    #[cfg(not(target_arch = "wasm32"))]
    if native {
        return acyclic_harness::Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid, reason,
        };
    }
    let _ = native;
    invalid(reason)
}

enum MetadataRole { State, Results, Replies, Reply }
struct MetadataValue<'a>(&'a Value, MetadataRole);

impl Serialize for MetadataValue<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{SerializeMap, SerializeSeq};
        match (&self.1, self.0) {
            (MetadataRole::Replies, Value::Array(values)) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    sequence.serialize_element(&MetadataValue(value, MetadataRole::Reply))?;
                }
                sequence.end()
            }
            (role, Value::Object(fields)) => {
                let mut map = serializer.serialize_map(Some(fields.len()))?;
                for (name, value) in fields {
                    match (role, name.as_str()) {
                        (MetadataRole::State, "reply") if fields.get("state").and_then(Value::as_str) == Some("hosting") =>
                            map.serialize_entry(name, &MetadataValue(value, MetadataRole::Reply))?,
                        (MetadataRole::Results, "commands") =>
                            map.serialize_entry(name, &MetadataValue(value, MetadataRole::Replies))?,
                        (MetadataRole::Reply, "value") => map.serialize_entry(name, &())?,
                        _ => map.serialize_entry(name, value)?,
                    }
                }
                map.end()
            }
            _ => self.0.serialize(serializer),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
const NATIVE_METADATA_MAXIMUM: u64 = crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8;

#[cfg(not(target_arch = "wasm32"))]
fn native_work_error(cause: acyclic_fs::NativeTreeMaterializationError) -> acyclic_harness::Error {
    acyclic_harness::Error::NativeWork { cause, custody: None }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_metadata_quota<T>(bytes: u64, lexical_items: u64) -> acyclic_harness::Result<u64> {
    let overflow = || native_work_error(acyclic_fs::NativeTreeMaterializationError::Work(
        acyclic_fs::WorkError::Overflow,
    ));
    let tagged_slot = (4 * std::mem::size_of::<Value>()) as u64;
    lexical_items.checked_mul(tagged_slot)
        .and_then(|owned| bytes.checked_mul(8).and_then(|strings| owned.checked_add(strings)))
        .and_then(|owned| owned.checked_add(std::mem::size_of::<T>() as u64))
        .and_then(|owned| owned.checked_add(std::mem::size_of::<WorkflowCommand>() as u64))
        .ok_or_else(overflow)
}

#[cfg(not(target_arch = "wasm32"))]
fn native_metadata<T: NativeMetadata>(
    value: &impl Serialize,
    work: &acyclic_fs::NativeMaterializationWork,
) -> acyclic_harness::Result<(T, acyclic_fs::NativeAllocation)> {
    let before_items = work.receipt().items_examined;
    let (bytes, bytes_custody) = super::pi_root_provider::guarded_pi_bytes(value, NATIVE_METADATA_MAXIMUM, work)?;
    let quota = work.receipt().items_examined.checked_sub(before_items)
        .ok_or_else(|| native_work_error(acyclic_fs::NativeTreeMaterializationError::Work(
            acyclic_fs::WorkError::Overflow,
        )))
        .and_then(|items| native_metadata_quota::<T>(bytes.len() as u64, items));
    let quota = match quota {
        Ok(quota) => quota,
        Err(error) => {
            drop(bytes);
            drop(bytes_custody);
            return Err(error);
        }
    };
    let decoded = super::pi_root_provider::guarded_pi_decode::<T>(
        &bytes, NATIVE_METADATA_MAXIMUM, quota, T::VALUE_SCHEMA, T::DIAGNOSTIC_LABEL_BYTES, work,
    );
    drop(bytes);
    drop(bytes_custody);
    decoded.map_err(|error| match error {
        acyclic_harness::Error::Invalid(_) => transition_error("Pi native metadata is malformed", true),
        error => error,
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn require_single_command(input: &Value, native: bool) -> acyclic_harness::Result<()> {
    if input.get("commands").and_then(Value::as_array).is_some_and(|commands| commands.len() == 1) {
        Ok(())
    } else {
        Err(transition_error("Pi result differs from its retained command", native))
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn take_result_body(input: &mut Value) -> acyclic_harness::Result<Value> {
    input.get_mut("commands").and_then(Value::as_array_mut)
        .and_then(|commands| commands.first_mut()).and_then(Value::as_object_mut)
        .and_then(|reply| reply.remove("value"))
        .ok_or_else(|| transition_error("Pi control reply has no value", true))
}

#[cfg(test)]
#[path = "pi_task_tests.rs"]
mod tests;
