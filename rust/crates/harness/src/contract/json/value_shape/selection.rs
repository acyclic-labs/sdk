use super::*;

impl ValueInspector {
    pub(super) fn value_scope(selection: Selection) -> Scope {
        if matches!(
            selection.node,
            Node::Schema(JsonValueSchema::Root) | Node::Unknown
        ) {
            selection.scope
        } else {
            Scope::Ignore
        }
    }
    pub(super) fn tag_key(node: Node) -> &'static [u8] {
        match node {
            Node::Schema(JsonValueSchema::Interaction)
            | Node::Schema(JsonValueSchema::InteractionResponse) => b"type",
            Node::Schema(JsonValueSchema::EventPayload)
            | Node::Schema(JsonValueSchema::ModelContentPart)
            | Node::Schema(JsonValueSchema::ToolResultContent) => b"kind",
            Node::Schema(JsonValueSchema::ModelEvent) => b"kind",
            Node::Status => b"kind",
            Node::Schema(JsonValueSchema::SchedulerEvent)
            | Node::Orchestration
            | Node::Schema(JsonValueSchema::RecursivePlan) => b"kind",
            Node::Schema(JsonValueSchema::RecursiveState) => b"phase",
            _ => b"",
        }
    }
    pub(super) fn tag_branches(node: Node, tag: &[u8]) -> u8 {
        match (node, tag) {
            (Node::Schema(JsonValueSchema::Interaction), b"question")
            | (Node::Schema(JsonValueSchema::EventPayload), b"interaction_resolved")
            | (Node::Schema(JsonValueSchema::ModelContentPart), b"tool_call")
            | (Node::Schema(JsonValueSchema::ToolResultContent), b"json")
            | (Node::Schema(JsonValueSchema::ModelEvent), b"tool_call")
            | (Node::Status, b"completed") => 1,
            (Node::Schema(JsonValueSchema::Interaction), b"form")
            | (Node::Schema(JsonValueSchema::ModelContentPart), b"tool_result")
            | (Node::Schema(JsonValueSchema::ModelEvent), b"completed") => 2,
            (Node::Schema(JsonValueSchema::EventPayload), b"fork_published") => 2,
            (Node::Schema(JsonValueSchema::EventPayload), b"project_merge_published") => 4,
            (Node::Schema(JsonValueSchema::SchedulerEvent), b"declared")
            | (Node::Orchestration, b"reduce") => 1,
            (Node::Schema(JsonValueSchema::SchedulerEvent), b"orchestrated") => 2,
            (Node::Schema(JsonValueSchema::InteractionResponse), b"question" | b"form")
            | (Node::Schema(JsonValueSchema::RecursivePlan), b"fork") => 1,
            (Node::Schema(JsonValueSchema::RecursivePlan), b"merge") => 2,
            (Node::Schema(JsonValueSchema::RecursiveState), b"ready" | b"source_pinned") => 1,
            (Node::Schema(JsonValueSchema::RecursiveState), b"waiting") => 3,
            (Node::Schema(JsonValueSchema::RecursiveState), b"complete") => 4,
            _ => 0,
        }
    }
    pub(super) fn select(&self, layer: usize) -> Selection {
        if self.frame_len == self.layers[layer].base {
            return self.layers[layer].root;
        }
        let index = self.frame_len - 1;
        let frame = &self.frames[index];
        let scope = frame.selection.scope;
        let gate = |branch| Scope::Gate(index as u16, branch);
        let (node, scope) = match (frame.selection.node, frame.key) {
            (Node::Schema(JsonValueSchema::Root), _) => {
                (Node::Schema(JsonValueSchema::Root), scope)
            }
            (Node::Unknown, _) => (Node::Unknown, Scope::Unknown),
            (Node::Schema(JsonValueSchema::Interaction), b"response_schema") => {
                (Node::Schema(JsonValueSchema::Root), gate(0))
            }
            (Node::Schema(JsonValueSchema::Interaction), b"schema") => {
                (Node::Schema(JsonValueSchema::Root), gate(1))
            }
            (Node::Schema(JsonValueSchema::ControlResponse), b"result") => {
                (Node::Schema(JsonValueSchema::Root), scope)
            }
            (Node::Schema(JsonValueSchema::EventPayload), b"resolution") => {
                (Node::Resolution, gate(0))
            }
            (Node::Schema(JsonValueSchema::EventPayload), b"seed") => {
                (Node::Schema(JsonValueSchema::ForkSeed), gate(1))
            }
            (Node::Schema(JsonValueSchema::EventPayload), b"receipt") => {
                (Node::Schema(JsonValueSchema::ProjectMergeReceipt), gate(2))
            }
            (Node::Resolution, b"outcome") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Schema(JsonValueSchema::Snapshot), b"events") => (Node::Events, scope),
            (Node::Schema(JsonValueSchema::Snapshot), b"projection") => (Node::Projection, scope),
            (Node::Events, _) if !frame.object => (Node::Event, scope),
            (Node::Event, b"payload") => (Node::Schema(JsonValueSchema::EventPayload), scope),
            (Node::Projection, b"interactions") => (Node::Resolutions, scope),
            (Node::Projection, b"forks") => (Node::Forks, scope),
            (Node::Forks, _) if !frame.object => (Node::ForkTuple, scope),
            (Node::ForkTuple, _) if !frame.object && frame.array_index == 1 => {
                (Node::Schema(JsonValueSchema::ForkSeed), scope)
            }
            (Node::Resolutions, _) if frame.object => (Node::ResolutionTuple, scope),
            (Node::ResolutionTuple, _) if !frame.object && frame.array_index == 1 => {
                (Node::Resolution, scope)
            }
            (Node::Schema(JsonValueSchema::Context), b"messages") => (Node::Messages, scope),
            (Node::Messages, _) if !frame.object => (Node::Message, scope),
            (Node::Message, b"content") => (Node::Schema(JsonValueSchema::ModelContent), scope),
            (Node::Schema(JsonValueSchema::ResponseProjection), b"context") => {
                (Node::Schema(JsonValueSchema::Context), scope)
            }
            (Node::Schema(JsonValueSchema::ModelRequest), b"model") => (Node::Model, scope),
            (Node::Model, b"options") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Schema(JsonValueSchema::ModelRequest), b"messages") => (Node::Messages, scope),
            (Node::Schema(JsonValueSchema::ModelRequest), b"tools") => (Node::Tools, scope),
            (Node::Tools, _) if !frame.object => (Node::Tool, scope),
            (Node::Tool, b"input_schema" | b"output_schema" | b"projection_schema") => {
                (Node::Schema(JsonValueSchema::Root), scope)
            }
            (Node::Parts, _) if !frame.object => {
                (Node::Schema(JsonValueSchema::ModelContentPart), scope)
            }
            (Node::Schema(JsonValueSchema::ModelContentPart), b"arguments") => {
                (Node::Schema(JsonValueSchema::Root), gate(0))
            }
            (Node::Schema(JsonValueSchema::ModelContentPart), b"content") => {
                (Node::Schema(JsonValueSchema::ToolResultContent), gate(1))
            }
            (Node::Schema(JsonValueSchema::ToolResultContent), b"value") => {
                (Node::Schema(JsonValueSchema::Root), gate(0))
            }
            (Node::Schema(JsonValueSchema::ModelEvent), b"arguments") => {
                (Node::Schema(JsonValueSchema::Root), gate(0))
            }
            (Node::Schema(JsonValueSchema::ModelEvent), b"metadata") => {
                (Node::Schema(JsonValueSchema::Root), gate(1))
            }
            (Node::Schema(JsonValueSchema::WorkflowRecord), b"input") => {
                (Node::Schema(JsonValueSchema::Root), scope)
            }
            (Node::Schema(JsonValueSchema::WorkflowRecord), b"prior") => {
                (Node::StateRecord, scope)
            }
            (Node::Schema(JsonValueSchema::WorkflowRecord), b"transition") => {
                (Node::Transition, scope)
            }
            (
                Node::StateRecord
                | Node::Transition
                | Node::Schema(JsonValueSchema::MachineCheckpoint),
                b"state",
            ) => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Transition, b"status") => (Node::Status, scope),
            (Node::Status, b"value") => (Node::Schema(JsonValueSchema::Root), gate(0)),
            (Node::Schema(JsonValueSchema::WorkflowAdmission), b"initial") => {
                (Node::StateRecord, scope)
            }
            (Node::Schema(JsonValueSchema::ResourcePreparationRequest), b"payload") => {
                (Node::Schema(JsonValueSchema::Root), scope)
            }
            (Node::Schema(JsonValueSchema::ResourcePreparationRequest), b"child") => {
                (Node::Schema(JsonValueSchema::TaskAdmissionRecord), scope)
            }
            (Node::Schema(JsonValueSchema::ResourcePreparationRequest), b"read_authorization") => (
                Node::Schema(JsonValueSchema::ProjectSourceAuthorization),
                scope,
            ),
            (
                Node::Schema(JsonValueSchema::TaskAdmissionRecord),
                b"input" | b"input_schema" | b"output_schema",
            ) => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Schema(JsonValueSchema::SchedulerEvent), b"spec") => {
                (Node::OperationSpec, gate(0))
            }
            (Node::Schema(JsonValueSchema::SchedulerEvent), b"reducer") => {
                (Node::EntrypointRef, gate(1))
            }
            (Node::OperationSpec, b"entrypoint") => (Node::EntrypointRef, scope),
            (Node::OperationSpec, b"orchestration") => (Node::Orchestration, scope),
            (Node::Orchestration, b"reducer") => (Node::EntrypointRef, gate(0)),
            (Node::EntrypointRef, b"result_schema") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Schema(JsonValueSchema::InteractionResponse), b"value") => {
                (Node::Schema(JsonValueSchema::Root), gate(0))
            }
            (
                Node::Schema(JsonValueSchema::RecursiveForkToolPlan) | Node::RecursivePlanPayload,
                b"request",
            ) => (
                Node::Schema(JsonValueSchema::ResourcePreparationRequest),
                scope,
            ),
            (
                Node::Schema(JsonValueSchema::RecursiveForkToolPlan) | Node::RecursivePlanPayload,
                b"preparation",
            ) => (Node::Schema(JsonValueSchema::TaskAdmissionRecord), scope),
            (Node::Schema(JsonValueSchema::RecursivePlan), b"plan") => {
                (Node::RecursivePlanPayload, gate(0))
            }
            (Node::Schema(JsonValueSchema::RecursiveInitialPlan), b"plan") => {
                (Node::Schema(JsonValueSchema::RecursivePlan), scope)
            }
            (Node::Schema(JsonValueSchema::RecursiveState), b"arguments") => {
                (Node::Schema(JsonValueSchema::Root), gate(0))
            }
            (Node::Schema(JsonValueSchema::RecursiveState), b"intent") => (
                Node::Schema(JsonValueSchema::RecursiveForkSourceIntent),
                gate(0),
            ),
            (Node::Schema(JsonValueSchema::RecursiveState), b"report") => {
                (Node::Schema(JsonValueSchema::ForkReport), gate(1))
            }
            (Node::Schema(JsonValueSchema::RecursiveState), b"plan") => {
                (Node::Schema(JsonValueSchema::RecursivePlan), gate(1))
            }
            (Node::Schema(JsonValueSchema::RecursiveState), b"receipt") => {
                (Node::Schema(JsonValueSchema::ProjectMergeReceipt), gate(1))
            }
            (Node::Schema(JsonValueSchema::RecursiveState), b"value") => {
                (Node::Schema(JsonValueSchema::Root), gate(2))
            }
            (Node::Schema(JsonValueSchema::RecursiveReceipt), b"value") => {
                (Node::Schema(JsonValueSchema::TaskOutcome), scope)
            }
            (Node::Schema(JsonValueSchema::TaskOutcome), b"Succeeded") => {
                (Node::Schema(JsonValueSchema::Root), scope)
            }
            (Node::Schema(JsonValueSchema::ProjectMergeReceipt), b"provider_proof") => {
                (Node::ProviderProof, scope)
            }
            (Node::ProviderProof, b"statement") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Schema(JsonValueSchema::ProjectSourceAuthorization), b"native_snapshot") => {
                (Node::Schema(JsonValueSchema::Root), scope)
            }
            (
                Node::Schema(JsonValueSchema::ForkRequest)
                | Node::Schema(JsonValueSchema::ForkSeed),
                b"authorized_source",
            ) => (
                Node::Schema(JsonValueSchema::ProjectSourceAuthorization),
                scope,
            ),
            (
                Node::Schema(JsonValueSchema::ForkReport)
                | Node::Schema(JsonValueSchema::RecursiveForkSourceIntent),
                b"request",
            ) => (Node::Schema(JsonValueSchema::ForkRequest), scope),
            (Node::Schema(JsonValueSchema::RecursiveSourcePin), b"intent") => (
                Node::Schema(JsonValueSchema::RecursiveForkSourceIntent),
                scope,
            ),
            (Node::Schema(JsonValueSchema::ForkPreparationCommandResults), b"commands") => {
                (Node::ForkPreparationCommands, scope)
            }
            (Node::ForkPreparationCommands, _) if !frame.object => {
                (Node::ForkPreparationCommandResult, scope)
            }
            (Node::ForkPreparationCommandResult, b"value") => (Node::ForkReportOutcome, scope),
            (Node::ForkReportOutcome, b"Succeeded") => {
                (Node::Schema(JsonValueSchema::ForkReport), scope)
            }
            (Node::Schema(JsonValueSchema::ConfiguredRecursiveSourceCheckpoint), b"snapshot") => {
                (Node::Schema(JsonValueSchema::Snapshot), scope)
            }
            (Node::Schema(JsonValueSchema::ConfiguredRecursiveSourceCheckpoint), b"request") => {
                (Node::Schema(JsonValueSchema::ForkRequest), scope)
            }
            (Node::Schema(JsonValueSchema::RecursiveMergeToolPlan), b"authorized_source") => (
                Node::Schema(JsonValueSchema::ProjectSourceAuthorization),
                scope,
            ),
            (Node::RecursivePlanPayload, b"authorized_source") => {
                let scope = match scope {
                    Scope::Gate(owner, _) => Scope::Gate(owner, 1),
                    _ => Scope::Ignore,
                };
                (
                    Node::Schema(JsonValueSchema::ProjectSourceAuthorization),
                    scope,
                )
            }
            (Node::Schema(JsonValueSchema::ToolTaskCommand), b"arguments") => {
                (Node::Schema(JsonValueSchema::Root), scope)
            }
            (Node::Schema(JsonValueSchema::TaskAdmitCommand), b"input") => {
                (Node::Schema(JsonValueSchema::Root), scope)
            }
            (Node::Schema(JsonValueSchema::PreparedTaskAdmitCommand), b"admission") => {
                (Node::Schema(JsonValueSchema::TaskAdmissionRecord), scope)
            }
            (Node::Schema(JsonValueSchema::ModelTaskCommand), b"input") => {
                (Node::Schema(JsonValueSchema::ModelContent), scope)
            }
            (Node::Schema(JsonValueSchema::ModelTaskCommand), b"selected_context") => {
                (Node::Schema(JsonValueSchema::Context), scope)
            }
            (Node::Schema(JsonValueSchema::NativeProcessRequest), b"mcp_stdio") => {
                (Node::McpStdioRequest, scope)
            }
            (Node::McpStdioRequest, b"params") => (Node::Schema(JsonValueSchema::Root), scope),
            _ => (Node::Schema(JsonValueSchema::None), Scope::Ignore),
        };
        Selection { node, scope }
    }
}
