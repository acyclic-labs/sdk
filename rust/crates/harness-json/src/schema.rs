/// Value reachability in one closed SDK record.
#[derive(Debug)]
pub struct JsonValueSchema {
    fields: &'static [(&'static str, &'static JsonValueSchema, u8)],
    elements: &'static [&'static JsonValueSchema],
    tag: &'static str,
    tags: &'static [(&'static str, u8)],
}
macro_rules! schema {
    ($visibility:vis $name:ident $(=> $public:ident)?, $tag:literal, [$($label:literal => $mask:literal),*], [$($field:literal => $child:ident : $gate:literal),*], [$($element:ident),*]) => {
        $visibility static $name: JsonValueSchema = JsonValueSchema {
            fields: &[$(($field, &$child, $gate)),*], elements: &[$(&$element),*],
            tag: $tag, tags: &[$(($label, $mask)),*],
        };
        $(impl JsonValueSchema {
            #[doc = concat!("Value fields for ", stringify!($public), ".")]
            pub const $public: &'static Self = &$name;
        })?
    };
}
schema!(pub NONE => NONE, "", [], [], []);
schema!(pub ROOT => ROOT, "", [], [], []);
schema!(pub UNKNOWN, "", [], [], []);
schema!(SCHEDULE => SCHEDULE, "", [], ["body" => ROOT : 0], []);
// ApprovalSubject::Tool arguments use deserialize_json_value: serde's private
// marker keys stay literal. Their derived HeapLayout accounts their tree;
// unlike question/form Values, they have no RawValue expansion to inspect.
schema!(INTERACTION => INTERACTION, "type", ["question" => 1, "form" => 2], ["response_schema" => ROOT : 1, "schema" => ROOT : 2], []);
schema!(CONTROL_RESPONSE => CONTROL_RESPONSE, "", [], ["result" => ROOT : 0], []);
schema!(CONTEXT => CONTEXT, "", [], ["messages" => MESSAGES : 0], []);
schema!(SUMMARY_SOURCE => SUMMARY_SOURCE, "", [], ["context" => CONTEXT : 0], []);
schema!(RESPONSE_PROJECTION => RESPONSE_PROJECTION, "", [], ["context" => CONTEXT : 0, "steering" => CONTEXT : 0], []);
schema!(MESSAGE_SEGMENT => MESSAGE_SEGMENT, "", [], ["suffix" => MESSAGES : 0], []);
schema!(MODEL_EVENT => MODEL_EVENT, "kind", ["tool_call" => 1, "completed" => 2], ["arguments" => ROOT : 1, "metadata" => ROOT : 3], []);
schema!(MODEL_REQUEST => MODEL_REQUEST, "", [], ["model" => MODEL : 0, "messages" => MESSAGES : 0, "tools" => TOOLS : 0], []);
schema!(MODEL_CONTENT_PART => MODEL_CONTENT_PART, "kind", ["tool_call" => 1, "tool_result" => 2], ["arguments" => ROOT : 1, "metadata" => ROOT : 1, "content" => TOOL_RESULT_CONTENT : 2], []);
schema!(TOOL_RESULT_CONTENT => TOOL_RESULT_CONTENT, "kind", ["json" => 1], ["value" => ROOT : 1], []);
schema!(WORKFLOW_RECORD => WORKFLOW_RECORD, "", [], ["input" => ROOT : 0, "prior" => STATE_RECORD : 0, "transition" => TRANSITION : 0], []);
schema!(WORKFLOW_ADMISSION => WORKFLOW_ADMISSION, "", [], ["initial" => STATE_RECORD : 0], []);
schema!(MACHINE_CHECKPOINT => MACHINE_CHECKPOINT, "", [], ["state" => ROOT : 0], []);
schema!(RESOURCE_PREPARATION_REQUEST => RESOURCE_PREPARATION_REQUEST, "", [], ["payload" => ROOT : 0, "child" => TASK_ADMISSION_RECORD : 0, "read_authorization" => PROJECT_SOURCE_AUTHORIZATION : 0], []);
schema!(SCHEDULER_EVENT => SCHEDULER_EVENT, "kind", ["declared" => 1, "orchestrated" => 2], ["spec" => OPERATION_SPEC : 1, "reducer" => ENTRYPOINT_REF : 2], []);
schema!(INTERACTION_RESPONSE => INTERACTION_RESPONSE, "type", ["question" => 1, "form" => 1], ["value" => ROOT : 1], []);
schema!(TASK_ADMISSION_RECORD => TASK_ADMISSION_RECORD, "", [], ["input" => ROOT : 0, "input_schema" => ROOT : 0, "output_schema" => ROOT : 0], []);
schema!(RECURSIVE_FORK_TOOL_PLAN => RECURSIVE_FORK_TOOL_PLAN, "", [], ["request" => RESOURCE_PREPARATION_REQUEST : 0, "preparation" => TASK_ADMISSION_RECORD : 0], []);
schema!(RECURSIVE_PLAN => RECURSIVE_PLAN, "kind", ["fork" => 1, "merge" => 2], ["plan" => RECURSIVE_PLAN_PAYLOAD : 0], []);
schema!(RECURSIVE_STATE => RECURSIVE_STATE, "phase", ["ready" => 1, "source_pinned" => 1, "waiting" => 3, "complete" => 4], ["arguments" => ROOT : 1, "intent" => RECURSIVE_FORK_SOURCE_INTENT : 1, "report" => FORK_REPORT : 2, "plan" => RECURSIVE_PLAN : 2, "receipt" => PROJECT_MERGE_RECEIPT : 2, "value" => ROOT : 4], []);
schema!(RECURSIVE_INITIAL_PLAN => RECURSIVE_INITIAL_PLAN, "", [], ["plan" => RECURSIVE_PLAN : 0], []);
schema!(RECURSIVE_RECEIPT => RECURSIVE_RECEIPT, "", [], ["value" => TASK_OUTCOME : 0], []);
schema!(TASK_OUTCOME => TASK_OUTCOME, "", [], ["Succeeded" => ROOT : 0], []);
schema!(PROJECT_MERGE_RECEIPT => PROJECT_MERGE_RECEIPT, "", [], ["provider_proof" => PROVIDER_PROOF : 0], []);
schema!(TOOL_TASK_COMMAND => TOOL_TASK_COMMAND, "", [], ["arguments" => ROOT : 0], []);
schema!(TOOL_INVOCATION => TOOL_INVOCATION, "", [], ["arguments" => ROOT : 0], []);
schema!(SELECTED_TOOL_CALL, "", [], ["invocation" => TOOL_INVOCATION : 0], []);
schema!(TOOL_SELECTION_RESULT, "", [], ["Ok" => SELECTED_TOOL_CALL : 0], []);
schema!(AUTHORED_TOOL_SELECTION => AUTHORED_TOOL_SELECTION, "", [], ["call" => TOOL_SELECTION_RESULT : 0], []);
schema!(TASK_ADMIT_COMMAND => TASK_ADMIT_COMMAND, "", [], ["input" => ROOT : 0], []);
schema!(PREPARED_TASK_ADMIT_COMMAND => PREPARED_TASK_ADMIT_COMMAND, "", [], ["admission" => TASK_ADMISSION_RECORD : 0], []);
schema!(CONTEXT_MANIFEST => CONTEXT_MANIFEST, "", [], ["discovery" => DISCOVERED_CONTEXT : 0, "tools" => TOOLS : 0], []);
schema!(DISCOVERED_CONTEXT, "", [], ["skills" => SKILLS : 0], []);
schema!(SKILLS, "", [], [], [SKILL]);
schema!(SKILL, "", [], ["fields" => ROOT : 0], []);
schema!(MODEL_TASK_COMMAND => MODEL_TASK_COMMAND, "", [], ["input" => MODEL_CONTENT : 0, "selected_context" => CONTEXT : 0], []);
schema!(NATIVE_PROCESS_REQUEST => NATIVE_PROCESS_REQUEST, "", [], ["mcp_stdio" => MCP_STDIO_REQUEST : 0], []);
schema!(PROJECT_SOURCE_AUTHORIZATION => PROJECT_SOURCE_AUTHORIZATION, "", [], ["native_snapshot" => ROOT : 0], []);
schema!(FORK_REQUEST => FORK_REQUEST, "", [], ["authorized_source" => PROJECT_SOURCE_AUTHORIZATION : 0], []);
schema!(FORK_REPORT => FORK_REPORT, "", [], ["request" => FORK_REQUEST : 0], []);
schema!(FORK_SEED => FORK_SEED, "", [], ["authorized_source" => PROJECT_SOURCE_AUTHORIZATION : 0], []);
schema!(RECURSIVE_FORK_SOURCE_INTENT => RECURSIVE_FORK_SOURCE_INTENT, "", [], ["request" => FORK_REQUEST : 0], []);
schema!(RECURSIVE_SOURCE_PIN => RECURSIVE_SOURCE_PIN, "", [], ["intent" => RECURSIVE_FORK_SOURCE_INTENT : 0], []);
schema!(FORK_PREPARATION_COMMAND_RESULTS => FORK_PREPARATION_COMMAND_RESULTS, "", [], ["commands" => FORK_PREPARATION_COMMANDS : 0], []);
schema!(RECURSIVE_MERGE_TOOL_PLAN => RECURSIVE_MERGE_TOOL_PLAN, "", [], ["authorized_source" => PROJECT_SOURCE_AUTHORIZATION : 0], []);
schema!(MESSAGES, "", [], [], [MESSAGE]);
schema!(MESSAGE, "", [], ["content" => MODEL_CONTENT : 0], []);
schema!(MODEL, "", [], ["options" => ROOT : 0], []);
schema!(TOOLS => TOOLS, "", [], [], [TOOL]);
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
static MODEL_CONTENT: JsonValueSchema = JsonValueSchema {
    fields: MODEL_CONTENT_PART.fields,
    tag: MODEL_CONTENT_PART.tag,
    tags: MODEL_CONTENT_PART.tags,
    elements: &[&MODEL_CONTENT_PART],
};
impl JsonValueSchema {
    /// Field selecting the active variant, empty for an untagged schema.
    pub const fn tag(&self) -> &'static str {
        self.tag
    }

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
    pub const MODEL_CONTENT: &'static Self = &MODEL_CONTENT;
}
impl PartialEq for JsonValueSchema {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}
impl Eq for JsonValueSchema {}
