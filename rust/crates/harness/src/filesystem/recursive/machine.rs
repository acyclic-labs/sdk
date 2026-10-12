
use super::*;
use crate::filesystem::{PreparationIo, PreparationRecord};
use crate::filesystem::default_tools::{decode, encode, recursive_reject, recursive_command_outbox};
use crate::wire::ErrorCode;

/// The ordinary preparation task emits one command and settles its exact report.
pub struct ForkPreparationMachine { identity: MachineIdentity, schema: Value }

#[derive(Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
enum State {
    Ready { command: WorkflowCommand, request_digest: [u8; 32] },
    Waiting { operation_id: OperationId, request_digest: [u8; 32] },
    Settled,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandResults { commands: [CommandResult; 1] }
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandResult { operation_id: OperationId, value: Outcome<ForkReport> }
impl PreparationRecord for State {
    fn maximum_slot_bytes() -> u64 {
        crate::filesystem::default_tools::recursive_decode_slot_bytes::<Self>() + std::mem::size_of::<CommandResult>() as u64
    }
}
impl PreparationRecord for CommandResults {
    fn maximum_slot_bytes() -> u64 {
        crate::filesystem::default_tools::recursive_decode_slot_bytes::<Self>() + std::mem::size_of::<CommandResult>() as u64
    }
    fn json_value_schema() -> crate::contract::JsonValueSchema { crate::contract::JsonValueSchema::ForkPreparationCommandResults }
}

impl ForkPreparationMachine {
    /// Registers the mechanism without provider side effects.
    #[must_use]
    pub fn new() -> Self {
        let mut digest = blake3::Hasher::new();
        for code in [include_str!("mod.rs"), include_str!("source.rs"), include_str!("native.rs"),
            include_str!("preparation.rs"), include_str!("machine.rs")] {
            for line in code.split_inclusive('\n') {
                match line.strip_suffix("\r\n") {
                    Some(line) => { digest.update(line.as_bytes()); digest.update(b"\n"); }
                    None => { digest.update(line.as_bytes()); }
                }
            }
        }
        Self { identity: MachineIdentity { name: "acyclic.fork_preparation".into(), version: "1".into(),
            digest: *digest.finalize().as_bytes() },
            schema: serde_json::from_slice(PREPARATION_STATE_SCHEMA).expect("static preparation state schema") }
    }
    /// Defines the typed preparation task.
    pub fn definition(self: &Arc<Self>) -> Result<TaskDefinition<ResourcePreparationRequest, ForkReport>> { self.registered_definition() }
    #[cfg(target_arch = "wasm32")]
    pub(crate) fn wire_definition(self: &Arc<Self>) -> Result<TaskDefinition<Value, Value>> { self.registered_definition() }
    fn registered_definition<I, O>(self: &Arc<Self>) -> Result<TaskDefinition<I, O>> {
        TaskDefinition::resumable(self.clone(),
            serde_json::from_slice(PREPARATION_INPUT_SCHEMA).expect("static preparation input schema"),
            serde_json::from_slice(PREPARATION_OUTPUT_SCHEMA).expect("static preparation output schema"))?.requires("host")
    }
    fn initial_state(&self, input: &Value, io: Option<&PreparationIo>) -> Result<Value> {
        let request: ResourcePreparationRequest = decode(input, io)?;
        crate::executor::with_history_io(io, |work| request.validate(work))?;
        let command = request.command.ok_or_else(|| recursive_reject(io, ErrorCode::Invalid, "fork preparation has no admitted command"))?;
        if command.kind != FORK_PREPARE_TASK_COMMAND_KIND {
            return Err(recursive_reject(io, ErrorCode::Invalid, "fork preparation command kind differs"));
        }
        encode(&State::Ready { command, request_digest: payload_digest(&request.payload, io)? }, io)
    }
    fn next_state(&self, state: &Value, input: &Value, io: Option<&PreparationIo>) -> Result<MachineTransition> {
        let (state, commands, status) = match decode::<State>(state, io)? {
            State::Ready { command, request_digest } => {
                crate::executor::with_history_io(io, |work| command.validate(work))?;
                if !input.is_null() || command.kind != FORK_PREPARE_TASK_COMMAND_KIND {
                    return Err(recursive_reject(io, ErrorCode::Invalid, "fork preparation initial trigger changed"));
                }
                (State::Waiting { operation_id: command.operation_id, request_digest },
                    recursive_command_outbox(command, io)?, MachineStatus::Suspended)
            }
            State::Waiting { operation_id, request_digest } => {
                let CommandResults { commands: [result] } = decode(input, io)?;
                if result.operation_id != operation_id {
                    return Err(recursive_reject(io, ErrorCode::Unauthorized, "fork preparation result belongs to another command"));
                }
                let status = match result.value {
                    Outcome::Succeeded(report) => {
                        crate::executor::with_history_io(io, |work| report.validate(work))?;
                        if report.request.operation_id != operation_id || payload_digest(&report.request, io)? != request_digest {
                            return Err(recursive_reject(io, ErrorCode::Unauthorized, "fork report changed the exact admitted source/request"));
                        }
                        MachineStatus::Completed { value: encode(&report, io)? }
                    }
                    Outcome::Failed { message } => MachineStatus::Failed { message },
                    Outcome::Cancelled => MachineStatus::Failed { message: stock_string_copy("fork preparation cancelled; allocation may remain uncertain", io)? },
                    Outcome::Indeterminate { .. } => return Err(recursive_reject(io, ErrorCode::Conflict,
                        "uncertain preparation cannot settle as a completed report")),
                };
                (State::Settled, Vec::new(), status)
            }
            State::Settled => return Err(recursive_reject(io, ErrorCode::Conflict, "fork preparation is already settled")),
        };
        Ok(MachineTransition { state: encode(&state, io)?, commands, status })
    }
}
impl Default for ForkPreparationMachine { fn default() -> Self { Self::new() } }
impl ResumableMachine for ForkPreparationMachine {
    fn identity(&self) -> &MachineIdentity { &self.identity }
    fn state_schema(&self) -> &Value { &self.schema }
    fn initialize(&self, input: &Value) -> Result<Value> { self.initial_state(input, None) }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> { self.next_state(state, input, None) }
    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_work(&self) -> bool { true }
    #[cfg(not(target_arch = "wasm32"))]
    fn identity_with_native_work<'a>(&'a self, work: &acyclic_fs::NativeMaterializationWork) -> Result<&'a MachineIdentity> {
        work.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
        Ok(&self.identity)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn initialize_with_native_work(&self, input: &Value, work: &acyclic_fs::NativeMaterializationWork) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        let io = PreparationIo::from_native_work(work)?;
        let state = self.initial_state(input, Some(&io))?;
        let custody = io.take_native_custody(crate::filesystem::default_tools::recursive_value_owned_bytes(&state)?)?;
        Ok((state, custody))
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn transition_with_native_work(&self, state: &Value, input: Value, work: &acyclic_fs::NativeMaterializationWork)
        -> Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        let io = PreparationIo::from_native_work(work)?;
        let transition = self.next_state(state, &input, Some(&io))?;
        drop(input);
        let custody = io.take_native_custody(crate::filesystem::default_tools::recursive_transition_owned_bytes(&transition, &io)?)?;
        Ok((transition, custody))
    }
}
fn payload_digest<T: Serialize>(value: &T, io: Option<&PreparationIo>) -> Result<[u8; 32]> {
    match io {
        Some(io) => crate::contract::canonical_json_digest_with_work(value, io.remaining()?.bytes_encoded, &mut Some(&mut io.history_io())),
        None => crate::contract::canonical_json_digest(value),
    }
}
const PREPARATION_STATE_SCHEMA: &[u8] = br#"{"oneOf":[{"type":"object","required":["phase","command","request_digest"],"properties":{"phase":{"const":"ready"},"command":{"type":"object"},"request_digest":{"type":"array","minItems":32,"maxItems":32,"items":{"type":"integer","minimum":0,"maximum":255}}},"additionalProperties":false},{"type":"object","required":["phase","operation_id","request_digest"],"properties":{"phase":{"const":"waiting"},"operation_id":{"type":"string"},"request_digest":{"type":"array","minItems":32,"maxItems":32,"items":{"type":"integer","minimum":0,"maximum":255}}},"additionalProperties":false},{"type":"object","required":["phase"],"properties":{"phase":{"const":"settled"}},"additionalProperties":false}]}"#;
const PREPARATION_INPUT_SCHEMA: &[u8] = br#"{"type":"object","required":["provider","child","resources","creation_grant","maximum_resources","maximum_work","maximum_bytes","payload","command"],"properties":{"provider":{"type":"object"},"child":{"type":"object"},"resources":{"type":"array","minItems":1,"items":{"type":"object"}},"creation_grant":{"const":"fork:publish"},"maximum_resources":{"type":"integer","minimum":1,"maximum":4294967295},"maximum_work":{"type":"integer","minimum":1,"maximum":18446744073709551614},"maximum_bytes":{"type":"integer","minimum":1,"maximum":18446744073709551614},"payload":{"type":"object"},"source_checkpoint":{"type":["object","null"]},"input_files":{"type":"array","items":{"type":"object"}},"read_authorization":{"type":["object","null"]},"command":{"type":"object","required":["operation_id","kind","payload"],"properties":{"operation_id":{"type":"string","minLength":1},"kind":{"const":"acyclic.fork.prepare.v1"},"payload":{"type":"object"}},"additionalProperties":false}},"additionalProperties":false}"#;
const PREPARATION_OUTPUT_SCHEMA: &[u8] = br#"{"type":"object","required":["request","captures","child_private_volume","child_private_generation","inherited_context","summary","inherited_through_sequence","shared_grants","reference_grants","history_witness","attachment_manifests"],"properties":{"request":{"type":"object"},"captures":{"type":"array"},"child_private_volume":{"type":"object"},"child_private_generation":{"type":"object"},"child_result_volume":{"type":["object","null"]},"child_result_generation":{"type":["object","null"]},"inherited_context":{"type":"array"},"summary":{"type":["object","null"]},"inherited_through_sequence":{"type":"integer","minimum":0},"shared_grants":{"type":"array"},"reference_grants":{"type":"array"},"history_witness":{"type":["array","null"]},"attachment_manifests":{"type":"array"}},"additionalProperties":false}"#;

use crate::executor::{stock_string_copy};
