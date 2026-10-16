/// Value reachability in one closed SDK record.
#[derive(Debug)]
pub struct JsonValueSchema {
    pub(super) fields: &'static [(&'static str, &'static JsonValueSchema, u8)],
    pub(super) elements: &'static [&'static JsonValueSchema],
    pub(super) tag: &'static str,
    pub(super) tags: &'static [(&'static str, u8)],
}
macro_rules! schema {
    ($name:ident $(=> $public:ident)?, $tag:literal, [$($label:literal => $mask:literal),*], [$($field:literal => $child:ident : $gate:literal),*], [$($element:ident),*]) => {
        pub(super) static $name: JsonValueSchema = JsonValueSchema {
            fields: &[$(($field, &$child, $gate)),*], elements: &[$(&$element),*],
            tag: $tag, tags: &[$(($label, $mask)),*],
        };
        $(impl JsonValueSchema {
            #[doc = concat!("Value fields for ", stringify!($public), ".")]
            #[allow(non_upper_case_globals, reason = "Schema constants mirror SDK record type names.")]
            pub const $public: &'static Self = &$name;
        })?
    };
}
schema!(NONE => None, "", [], [], []);
schema!(ROOT => Root, "", [], [], []);
schema!(UNKNOWN, "", [], [], []);
schema!(INTERACTION => Interaction, "type", ["question" => 1, "form" => 2], ["response_schema" => ROOT : 1, "schema" => ROOT : 2], []);
schema!(CONTROL_RESPONSE => ControlResponse, "", [], ["result" => ROOT : 0], []);
schema!(EVENT_PAYLOAD => EventPayload, "kind", ["interaction_resolved" => 1, "fork_published" => 2, "project_merge_published" => 4], ["resolution" => RESOLUTION : 1, "seed" => FORK_SEED : 2, "receipt" => PROJECT_MERGE_RECEIPT : 4], []);
schema!(SNAPSHOT => Snapshot, "", [], ["events" => EVENTS : 0, "projection" => PROJECTION : 0], []);
schema!(CONTEXT => Context, "", [], ["messages" => MESSAGES : 0], []);
schema!(SUMMARY_SOURCE => SummarySource, "", [], ["context" => CONTEXT : 0], []);
schema!(RESPONSE_PROJECTION => ResponseProjection, "", [], ["context" => CONTEXT : 0], []);
schema!(MODEL_EVENT => ModelEvent, "kind", ["tool_call" => 1, "completed" => 2], ["arguments" => ROOT : 1, "metadata" => ROOT : 3], []);
schema!(MODEL_REQUEST => ModelRequest, "", [], ["model" => MODEL : 0, "messages" => MESSAGES : 0, "tools" => TOOLS : 0], []);
schema!(MODEL_CONTENT_PART => ModelContentPart, "kind", ["tool_call" => 1, "tool_result" => 2], ["arguments" => ROOT : 1, "metadata" => ROOT : 1, "content" => TOOL_RESULT_CONTENT : 2], []);
schema!(TOOL_RESULT_CONTENT => ToolResultContent, "kind", ["json" => 1], ["value" => ROOT : 1], []);
schema!(WORKFLOW_RECORD => WorkflowRecord, "", [], ["input" => ROOT : 0, "prior" => STATE_RECORD : 0, "transition" => TRANSITION : 0], []);
schema!(WORKFLOW_ADMISSION => WorkflowAdmission, "", [], ["initial" => STATE_RECORD : 0], []);
schema!(MACHINE_CHECKPOINT => MachineCheckpoint, "", [], ["state" => ROOT : 0], []);
schema!(RESOURCE_PREPARATION_REQUEST => ResourcePreparationRequest, "", [], ["payload" => ROOT : 0, "child" => TASK_ADMISSION_RECORD : 0, "read_authorization" => PROJECT_SOURCE_AUTHORIZATION : 0], []);
schema!(SCHEDULER_EVENT => SchedulerEvent, "kind", ["declared" => 1, "orchestrated" => 2], ["spec" => OPERATION_SPEC : 1, "reducer" => ENTRYPOINT_REF : 2], []);
schema!(INTERACTION_RESPONSE => InteractionResponse, "type", ["question" => 1, "form" => 1], ["value" => ROOT : 1], []);
schema!(TASK_ADMISSION_RECORD => TaskAdmissionRecord, "", [], ["input" => ROOT : 0, "input_schema" => ROOT : 0, "output_schema" => ROOT : 0], []);
schema!(RECURSIVE_FORK_TOOL_PLAN => RecursiveForkToolPlan, "", [], ["request" => RESOURCE_PREPARATION_REQUEST : 0, "preparation" => TASK_ADMISSION_RECORD : 0], []);
schema!(RECURSIVE_PLAN => RecursivePlan, "kind", ["fork" => 1, "merge" => 2], ["plan" => RECURSIVE_PLAN_PAYLOAD : 0], []);
schema!(RECURSIVE_STATE => RecursiveState, "phase", ["ready" => 1, "source_pinned" => 1, "waiting" => 3, "complete" => 4], ["arguments" => ROOT : 1, "intent" => RECURSIVE_FORK_SOURCE_INTENT : 1, "report" => FORK_REPORT : 2, "plan" => RECURSIVE_PLAN : 2, "receipt" => PROJECT_MERGE_RECEIPT : 2, "value" => ROOT : 4], []);
schema!(RECURSIVE_INITIAL_PLAN => RecursiveInitialPlan, "", [], ["plan" => RECURSIVE_PLAN : 0], []);
schema!(RECURSIVE_RECEIPT => RecursiveReceipt, "", [], ["value" => TASK_OUTCOME : 0], []);
schema!(TASK_OUTCOME => TaskOutcome, "", [], ["Succeeded" => ROOT : 0], []);
schema!(PROJECT_MERGE_RECEIPT => ProjectMergeReceipt, "", [], ["provider_proof" => PROVIDER_PROOF : 0], []);
schema!(TOOL_TASK_COMMAND => ToolTaskCommand, "", [], ["arguments" => ROOT : 0], []);
schema!(TASK_ADMIT_COMMAND => TaskAdmitCommand, "", [], ["input" => ROOT : 0], []);
schema!(PREPARED_TASK_ADMIT_COMMAND => PreparedTaskAdmitCommand, "", [], ["admission" => TASK_ADMISSION_RECORD : 0], []);
schema!(CONTEXT_MANIFEST => ContextManifest, "", [], ["discovery" => DISCOVERED_CONTEXT : 0, "tools" => TOOLS : 0], []);
schema!(DISCOVERED_CONTEXT, "", [], ["skills" => SKILLS : 0], []);
schema!(SKILLS, "", [], [], [SKILL]);
schema!(SKILL, "", [], ["fields" => ROOT : 0], []);
schema!(MODEL_TASK_COMMAND => ModelTaskCommand, "", [], ["input" => MODEL_CONTENT : 0, "selected_context" => CONTEXT : 0], []);
schema!(NATIVE_PROCESS_REQUEST => NativeProcessRequest, "", [], ["mcp_stdio" => MCP_STDIO_REQUEST : 0], []);
schema!(PROJECT_SOURCE_AUTHORIZATION => ProjectSourceAuthorization, "", [], ["native_snapshot" => ROOT : 0], []);
schema!(FORK_REQUEST => ForkRequest, "", [], ["authorized_source" => PROJECT_SOURCE_AUTHORIZATION : 0], []);
schema!(FORK_REPORT => ForkReport, "", [], ["request" => FORK_REQUEST : 0], []);
schema!(FORK_SEED => ForkSeed, "", [], ["authorized_source" => PROJECT_SOURCE_AUTHORIZATION : 0], []);
schema!(RECURSIVE_FORK_SOURCE_INTENT => RecursiveForkSourceIntent, "", [], ["request" => FORK_REQUEST : 0], []);
schema!(RECURSIVE_SOURCE_PIN => RecursiveSourcePin, "", [], ["intent" => RECURSIVE_FORK_SOURCE_INTENT : 0], []);
schema!(FORK_PREPARATION_COMMAND_RESULTS => ForkPreparationCommandResults, "", [], ["commands" => FORK_PREPARATION_COMMANDS : 0], []);
schema!(CONFIGURED_RECURSIVE_SOURCE_CHECKPOINT => ConfiguredRecursiveSourceCheckpoint, "", [], ["snapshot" => SNAPSHOT : 0, "request" => FORK_REQUEST : 0], []);
schema!(RECURSIVE_MERGE_TOOL_PLAN => RecursiveMergeToolPlan, "", [], ["authorized_source" => PROJECT_SOURCE_AUTHORIZATION : 0], []);
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
pub(super) static MODEL_CONTENT: JsonValueSchema = JsonValueSchema {
    fields: MODEL_CONTENT_PART.fields,
    tag: MODEL_CONTENT_PART.tag,
    tags: MODEL_CONTENT_PART.tags,
    elements: &[&MODEL_CONTENT_PART],
};
impl JsonValueSchema {
    /// Selects the schema for one object field or array element.
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
    /// Returns the field mask selected by a record discriminant.
    pub fn mask(&self, tag: &[u8]) -> u8 {
        self.tags
            .iter()
            .find(|(name, _)| name.as_bytes() == tag)
            .map_or(0, |(_, mask)| *mask)
    }
}

impl JsonValueSchema {
    /// Value fields for a model content envelope.
    #[allow(
        non_upper_case_globals,
        reason = "Schema constants mirror SDK record type names."
    )]
    pub const ModelContent: &'static Self = &MODEL_CONTENT;
}
impl PartialEq for JsonValueSchema {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}
impl Eq for JsonValueSchema {}
