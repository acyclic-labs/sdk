use super::JsonValueSchema;

pub(super) struct Schema {
    pub fields: &'static [(&'static str, &'static Schema, u8)],
    pub elements: &'static [&'static Schema],
    pub tag: &'static str,
    pub tags: &'static [(&'static str, u8)],
}
macro_rules! schema {
    ($name:ident, $tag:literal, [$($label:literal => $mask:literal),*], [$($field:literal => $child:ident : $gate:literal),*], [$($element:ident),*]) => {
        pub(super) static $name: Schema = Schema {
            fields: &[$(($field, &$child, $gate)),*], elements: &[$(&$element),*],
            tag: $tag, tags: &[$(($label, $mask)),*],
        };
    };
}
schema!(NONE, "", [], [], []);
schema!(ROOT, "", [], [], []);
schema!(UNKNOWN, "", [], [], []);
schema!(INTERACTION, "type", ["question" => 1, "form" => 2], ["response_schema" => ROOT : 1, "schema" => ROOT : 2], []);
schema!(CONTROL_RESPONSE, "", [], ["result" => ROOT : 0], []);
schema!(EVENT_PAYLOAD, "kind", ["interaction_resolved" => 1, "fork_published" => 2, "project_merge_published" => 4], ["resolution" => RESOLUTION : 1, "seed" => FORK_SEED : 2, "receipt" => PROJECT_MERGE_RECEIPT : 4], []);
schema!(SNAPSHOT, "", [], ["events" => EVENTS : 0, "projection" => PROJECTION : 0], []);
schema!(CONTEXT, "", [], ["messages" => MESSAGES : 0], []);
schema!(RESPONSE_PROJECTION, "", [], ["context" => CONTEXT : 0], []);
schema!(MODEL_EVENT, "kind", ["tool_call" => 1, "completed" => 2], ["arguments" => ROOT : 1, "metadata" => ROOT : 2], []);
schema!(MODEL_REQUEST, "", [], ["model" => MODEL : 0, "messages" => MESSAGES : 0, "tools" => TOOLS : 0], []);
schema!(MODEL_CONTENT_PART, "kind", ["tool_call" => 1, "tool_result" => 2], ["arguments" => ROOT : 1, "content" => TOOL_RESULT_CONTENT : 2], []);
schema!(TOOL_RESULT_CONTENT, "kind", ["json" => 1], ["value" => ROOT : 1], []);
schema!(WORKFLOW_RECORD, "", [], ["input" => ROOT : 0, "prior" => STATE_RECORD : 0, "transition" => TRANSITION : 0], []);
schema!(WORKFLOW_ADMISSION, "", [], ["initial" => STATE_RECORD : 0], []);
schema!(MACHINE_CHECKPOINT, "", [], ["state" => ROOT : 0], []);
schema!(RESOURCE_PREPARATION_REQUEST, "", [], ["payload" => ROOT : 0, "child" => TASK_ADMISSION_RECORD : 0, "read_authorization" => PROJECT_SOURCE_AUTHORIZATION : 0], []);
schema!(SCHEDULER_EVENT, "kind", ["declared" => 1, "orchestrated" => 2], ["spec" => OPERATION_SPEC : 1, "reducer" => ENTRYPOINT_REF : 2], []);
schema!(INTERACTION_RESPONSE, "type", ["question" => 1, "form" => 1], ["value" => ROOT : 1], []);
schema!(TASK_ADMISSION_RECORD, "", [], ["input" => ROOT : 0, "input_schema" => ROOT : 0, "output_schema" => ROOT : 0], []);
schema!(RECURSIVE_FORK_TOOL_PLAN, "", [], ["request" => RESOURCE_PREPARATION_REQUEST : 0, "preparation" => TASK_ADMISSION_RECORD : 0], []);
schema!(RECURSIVE_PLAN, "kind", ["fork" => 1, "merge" => 2], ["plan" => RECURSIVE_PLAN_PAYLOAD : 0], []);
schema!(RECURSIVE_STATE, "phase", ["ready" => 1, "source_pinned" => 1, "waiting" => 3, "complete" => 4], ["arguments" => ROOT : 1, "intent" => RECURSIVE_FORK_SOURCE_INTENT : 1, "report" => FORK_REPORT : 2, "plan" => RECURSIVE_PLAN : 2, "receipt" => PROJECT_MERGE_RECEIPT : 2, "value" => ROOT : 4], []);
schema!(RECURSIVE_INITIAL_PLAN, "", [], ["plan" => RECURSIVE_PLAN : 0], []);
schema!(RECURSIVE_RECEIPT, "", [], ["value" => TASK_OUTCOME : 0], []);
schema!(TASK_OUTCOME, "", [], ["Succeeded" => ROOT : 0], []);
schema!(PROJECT_MERGE_RECEIPT, "", [], ["provider_proof" => PROVIDER_PROOF : 0], []);
schema!(TOOL_TASK_COMMAND, "", [], ["arguments" => ROOT : 0], []);
schema!(TASK_ADMIT_COMMAND, "", [], ["input" => ROOT : 0], []);
schema!(PREPARED_TASK_ADMIT_COMMAND, "", [], ["admission" => TASK_ADMISSION_RECORD : 0], []);
schema!(MODEL_TASK_COMMAND, "", [], ["input" => MODEL_CONTENT : 0, "selected_context" => CONTEXT : 0], []);
schema!(NATIVE_PROCESS_REQUEST, "", [], ["mcp_stdio" => MCP_STDIO_REQUEST : 0], []);
schema!(PROJECT_SOURCE_AUTHORIZATION, "", [], ["native_snapshot" => ROOT : 0], []);
schema!(FORK_REQUEST, "", [], ["authorized_source" => PROJECT_SOURCE_AUTHORIZATION : 0], []);
schema!(FORK_REPORT, "", [], ["request" => FORK_REQUEST : 0], []);
schema!(FORK_SEED, "", [], ["authorized_source" => PROJECT_SOURCE_AUTHORIZATION : 0], []);
schema!(RECURSIVE_FORK_SOURCE_INTENT, "", [], ["request" => FORK_REQUEST : 0], []);
schema!(RECURSIVE_SOURCE_PIN, "", [], ["intent" => RECURSIVE_FORK_SOURCE_INTENT : 0], []);
schema!(FORK_PREPARATION_COMMAND_RESULTS, "", [], ["commands" => FORK_PREPARATION_COMMANDS : 0], []);
schema!(CONFIGURED_RECURSIVE_SOURCE_CHECKPOINT, "", [], ["snapshot" => SNAPSHOT : 0, "request" => FORK_REQUEST : 0], []);
schema!(RECURSIVE_MERGE_TOOL_PLAN, "", [], ["authorized_source" => PROJECT_SOURCE_AUTHORIZATION : 0], []);
schema!(RESOLUTION, "", [], ["outcome" => ROOT : 0], []);
schema!(EVENTS, "", [], [], [EVENT]);
schema!(EVENT, "", [], ["payload" => EVENT_PAYLOAD : 0], []);
schema!(PROJECTION, "", [], ["interactions" => RESOLUTIONS : 0, "forks" => FORKS : 0], []);
schema!(RESOLUTIONS, "", [], ["*" => RESOLUTION_TUPLE : 0], []);
schema!(RESOLUTION_TUPLE, "", [], [], [NONE, RESOLUTION]);
schema!(FORKS, "", [], [], [FORK_TUPLE]);
schema!(FORK_TUPLE, "", [], [], [NONE, FORK_SEED]);
schema!(MESSAGES, "", [], [], [MESSAGE]);
schema!(MESSAGE, "", [], ["content" => MODEL_CONTENT : 0], []);
schema!(MODEL, "", [], ["options" => ROOT : 0], []);
schema!(TOOLS, "", [], [], [TOOL]);
schema!(TOOL, "", [], ["input_schema" => ROOT : 0, "output_schema" => ROOT : 0, "projection_schema" => ROOT : 0], []);
schema!(STATE_RECORD, "", [], ["state" => ROOT : 0], []);
schema!(TRANSITION, "", [], ["state" => ROOT : 0, "status" => STATUS : 0], []);
schema!(STATUS, "kind", ["completed" => 1], ["value" => ROOT : 1], []);
schema!(OPERATION_SPEC, "", [], ["entrypoint" => ENTRYPOINT_REF : 0, "orchestration" => ORCHESTRATION : 0], []);
schema!(ENTRYPOINT_REF, "", [], ["result_schema" => ROOT : 0], []);
schema!(ORCHESTRATION, "kind", ["reduce" => 1], ["reducer" => ENTRYPOINT_REF : 1], []);
schema!(PROVIDER_PROOF, "", [], ["statement" => ROOT : 0], []);
schema!(MCP_STDIO_REQUEST, "", [], ["params" => ROOT : 0], []);
schema!(
    FORK_PREPARATION_COMMANDS,
    "",
    [],
    [],
    [FORK_PREPARATION_COMMAND_RESULT]
);
schema!(FORK_PREPARATION_COMMAND_RESULT, "", [], ["value" => FORK_REPORT_OUTCOME : 0], []);
schema!(FORK_REPORT_OUTCOME, "", [], ["Succeeded" => FORK_REPORT : 0], []);
schema!(RECURSIVE_PLAN_PAYLOAD, "", [], ["request" => RESOURCE_PREPARATION_REQUEST : 1, "preparation" => TASK_ADMISSION_RECORD : 1, "authorized_source" => PROJECT_SOURCE_AUTHORIZATION : 2], []);
pub(super) static MODEL_CONTENT: Schema = Schema {
    fields: MODEL_CONTENT_PART.fields,
    tag: MODEL_CONTENT_PART.tag,
    tags: MODEL_CONTENT_PART.tags,
    elements: &[&MODEL_CONTENT_PART],
};
impl Schema {
    pub fn child(&'static self, key: &[u8], index: Option<usize>) -> (&'static Self, u8) {
        if std::ptr::eq(self, &ROOT) || std::ptr::eq(self, &UNKNOWN) {
            return (self, 0);
        }
        if let Some(index) = index {
            return (
                self.elements
                    .get(if self.elements.len() == 1 { 0 } else { index })
                    .copied()
                    .unwrap_or(&NONE),
                0,
            );
        }
        self.fields
            .iter()
            .find(|(name, _, _)| name.as_bytes() == key || *name == "*")
            .map_or((&NONE, 0), |(_, child, gate)| (*child, *gate))
    }
    pub fn mask(&self, tag: &[u8]) -> u8 {
        self.tags
            .iter()
            .find(|(name, _)| name.as_bytes() == tag)
            .map_or(0, |(_, mask)| *mask)
    }
}
impl JsonValueSchema {
    pub(super) fn descriptor(self) -> &'static Schema {
        match self {
            Self::None => &NONE,
            Self::Root => &ROOT,
            Self::Interaction => &INTERACTION,
            Self::ControlResponse => &CONTROL_RESPONSE,
            Self::EventPayload => &EVENT_PAYLOAD,
            Self::Snapshot => &SNAPSHOT,
            Self::Context => &CONTEXT,
            Self::ResponseProjection => &RESPONSE_PROJECTION,
            Self::ModelEvent => &MODEL_EVENT,
            Self::ModelRequest => &MODEL_REQUEST,
            Self::ModelContent => &MODEL_CONTENT,
            Self::ModelContentPart => &MODEL_CONTENT_PART,
            Self::WorkflowRecord => &WORKFLOW_RECORD,
            Self::WorkflowAdmission => &WORKFLOW_ADMISSION,
            Self::MachineCheckpoint => &MACHINE_CHECKPOINT,
            Self::ToolResultContent => &TOOL_RESULT_CONTENT,
            Self::ResourcePreparationRequest => &RESOURCE_PREPARATION_REQUEST,
            Self::SchedulerEvent => &SCHEDULER_EVENT,
            Self::InteractionResponse => &INTERACTION_RESPONSE,
            Self::TaskAdmissionRecord => &TASK_ADMISSION_RECORD,
            Self::RecursiveForkToolPlan => &RECURSIVE_FORK_TOOL_PLAN,
            Self::RecursivePlan => &RECURSIVE_PLAN,
            Self::RecursiveState => &RECURSIVE_STATE,
            Self::RecursiveInitialPlan => &RECURSIVE_INITIAL_PLAN,
            Self::RecursiveReceipt => &RECURSIVE_RECEIPT,
            Self::TaskOutcome => &TASK_OUTCOME,
            Self::ProjectMergeReceipt => &PROJECT_MERGE_RECEIPT,
            Self::ToolTaskCommand => &TOOL_TASK_COMMAND,
            Self::TaskAdmitCommand => &TASK_ADMIT_COMMAND,
            Self::PreparedTaskAdmitCommand => &PREPARED_TASK_ADMIT_COMMAND,
            Self::ModelTaskCommand => &MODEL_TASK_COMMAND,
            Self::NativeProcessRequest => &NATIVE_PROCESS_REQUEST,
            Self::ProjectSourceAuthorization => &PROJECT_SOURCE_AUTHORIZATION,
            Self::ForkRequest => &FORK_REQUEST,
            Self::ForkReport => &FORK_REPORT,
            Self::ForkSeed => &FORK_SEED,
            Self::RecursiveForkSourceIntent => &RECURSIVE_FORK_SOURCE_INTENT,
            Self::RecursiveSourcePin => &RECURSIVE_SOURCE_PIN,
            Self::ForkPreparationCommandResults => &FORK_PREPARATION_COMMAND_RESULTS,
            Self::ConfiguredRecursiveSourceCheckpoint => &CONFIGURED_RECURSIVE_SOURCE_CHECKPOINT,
            Self::RecursiveMergeToolPlan => &RECURSIVE_MERGE_TOOL_PLAN,
        }
    }
}
