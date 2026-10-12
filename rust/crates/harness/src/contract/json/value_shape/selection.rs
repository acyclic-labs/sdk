use super::JsonValueSchema as S;
use super::*;

impl ValueInspector {
    pub(super) fn value_scope(selection: Selection) -> Scope {
        if matches!(selection.node, Node::Schema(S::Root) | Node::Unknown) {
            selection.scope
        } else {
            Scope::Ignore
        }
    }
    pub(super) fn tag_key(node: Node) -> &'static [u8] {
        match node {
            Node::Schema(S::Interaction) | Node::Schema(S::InteractionResponse) => b"type",
            Node::Schema(S::EventPayload)
            | Node::Schema(S::ModelContentPart)
            | Node::Schema(S::ToolResultContent) => b"kind",
            Node::Schema(S::ModelEvent) => b"kind",
            Node::Status => b"kind",
            Node::Schema(S::SchedulerEvent)
            | Node::Orchestration
            | Node::Schema(S::RecursivePlan) => b"kind",
            Node::Schema(S::RecursiveState) => b"phase",
            _ => b"",
        }
    }
    pub(super) fn tag_branches(node: Node, tag: &[u8]) -> u8 {
        match (node, tag) {
            (Node::Schema(S::Interaction), b"question")
            | (Node::Schema(S::EventPayload), b"interaction_resolved")
            | (Node::Schema(S::ModelContentPart), b"tool_call")
            | (Node::Schema(S::ToolResultContent), b"json")
            | (Node::Schema(S::ModelEvent), b"tool_call")
            | (Node::Status, b"completed") => 1,
            (Node::Schema(S::Interaction), b"form")
            | (Node::Schema(S::ModelContentPart), b"tool_result")
            | (Node::Schema(S::ModelEvent), b"completed") => 2,
            (Node::Schema(S::EventPayload), b"fork_published") => 2,
            (Node::Schema(S::EventPayload), b"project_merge_published") => 4,
            (Node::Schema(S::SchedulerEvent), b"declared") | (Node::Orchestration, b"reduce") => 1,
            (Node::Schema(S::SchedulerEvent), b"orchestrated") => 2,
            (Node::Schema(S::InteractionResponse), b"question" | b"form")
            | (Node::Schema(S::RecursivePlan), b"fork") => 1,
            (Node::Schema(S::RecursivePlan), b"merge") => 2,
            (Node::Schema(S::RecursiveState), b"ready" | b"source_pinned") => 1,
            (Node::Schema(S::RecursiveState), b"waiting") => 3,
            (Node::Schema(S::RecursiveState), b"complete") => 4,
            _ => 0,
        }
    }
    pub(super) fn select(&self, _layer: usize) -> Selection {
        if self.frame_len == 0 {
            unreachable!("root selection is passed directly");
        }
        let index = self.frame_len - 1;
        let frame = &self.frames[index];
        let scope = frame.selection.scope;
        let gate = |branch| Scope::Gate(index as u16, branch);
        let (node, scope) = match (frame.selection.node, frame.key.bytes()) {
            (Node::Schema(S::Root), _) => (Node::Schema(S::Root), scope),
            (Node::Unknown, _) => (Node::Unknown, Scope::Unknown),
            (Node::Schema(S::Interaction), b"response_schema") => (Node::Schema(S::Root), gate(0)),
            (Node::Schema(S::Interaction), b"schema") => (Node::Schema(S::Root), gate(1)),
            (Node::Schema(S::ControlResponse), b"result") => (Node::Schema(S::Root), scope),
            (Node::Schema(S::EventPayload), b"resolution") => (Node::Resolution, gate(0)),
            (Node::Schema(S::EventPayload), b"seed") => (Node::Schema(S::ForkSeed), gate(1)),
            (Node::Schema(S::EventPayload), b"receipt") => {
                (Node::Schema(S::ProjectMergeReceipt), gate(2))
            }
            (Node::Resolution, b"outcome") => (Node::Schema(S::Root), scope),
            (Node::Schema(S::Snapshot), b"events") => (Node::Events, scope),
            (Node::Schema(S::Snapshot), b"projection") => (Node::Projection, scope),
            (Node::Events, _) if !frame.object => (Node::Event, scope),
            (Node::Event, b"payload") => (Node::Schema(S::EventPayload), scope),
            (Node::Projection, b"interactions") => (Node::Resolutions, scope),
            (Node::Projection, b"forks") => (Node::Forks, scope),
            (Node::Forks, _) if !frame.object => (Node::ForkTuple, scope),
            (Node::ForkTuple, _) if !frame.object && frame.array_index == 1 => {
                (Node::Schema(S::ForkSeed), scope)
            }
            (Node::Resolutions, _) if frame.object => (Node::ResolutionTuple, scope),
            (Node::ResolutionTuple, _) if !frame.object && frame.array_index == 1 => {
                (Node::Resolution, scope)
            }
            (Node::Schema(S::Context), b"messages") => (Node::Messages, scope),
            (Node::Messages, _) if !frame.object => (Node::Message, scope),
            (Node::Message, b"content") => (Node::Schema(S::ModelContent), scope),
            (Node::Schema(S::ResponseProjection), b"context") => (Node::Schema(S::Context), scope),
            (Node::Schema(S::ModelRequest), b"model") => (Node::Model, scope),
            (Node::Model, b"options") => (Node::Schema(S::Root), scope),
            (Node::Schema(S::ModelRequest), b"messages") => (Node::Messages, scope),
            (Node::Schema(S::ModelRequest), b"tools") => (Node::Tools, scope),
            (Node::Tools, _) if !frame.object => (Node::Tool, scope),
            (Node::Tool, b"input_schema" | b"output_schema" | b"projection_schema") => {
                (Node::Schema(S::Root), scope)
            }
            (Node::Parts, _) if !frame.object => (Node::Schema(S::ModelContentPart), scope),
            (Node::Schema(S::ModelContentPart), b"arguments") => (Node::Schema(S::Root), gate(0)),
            (Node::Schema(S::ModelContentPart), b"content") => {
                (Node::Schema(S::ToolResultContent), gate(1))
            }
            (Node::Schema(S::ToolResultContent), b"value") => (Node::Schema(S::Root), gate(0)),
            (Node::Schema(S::ModelEvent), b"arguments") => (Node::Schema(S::Root), gate(0)),
            (Node::Schema(S::ModelEvent), b"metadata") => (Node::Schema(S::Root), gate(1)),
            (Node::Schema(S::ProjectedToolInvocation), b"arguments") => {
                (Node::Schema(S::Root), scope)
            }
            (Node::Schema(S::WorkflowRecord), b"input") => (Node::Schema(S::Root), scope),
            (Node::Schema(S::WorkflowRecord), b"prior" | b"next") => (Node::StateRecord, scope),
            (Node::Schema(S::WorkflowRecord), b"transition") => (Node::Transition, scope),
            (
                Node::StateRecord | Node::Transition | Node::Schema(S::MachineCheckpoint),
                b"state",
            ) => (Node::Schema(S::Root), scope),
            (Node::Transition, b"status") => (Node::Status, scope),
            (Node::Status, b"value") => (Node::Schema(S::Root), gate(0)),
            (Node::Schema(S::WorkflowAdmission), b"initial") => (Node::StateRecord, scope),
            (Node::Schema(S::ResourcePreparationRequest), b"payload") => {
                (Node::Schema(S::Root), scope)
            }
            (Node::Schema(S::ResourcePreparationRequest), b"child") => {
                (Node::Schema(S::TaskAdmissionRecord), scope)
            }
            (Node::Schema(S::ResourcePreparationRequest), b"read_authorization") => {
                (Node::Schema(S::ProjectSourceAuthorization), scope)
            }
            (
                Node::Schema(S::TaskAdmissionRecord),
                b"input" | b"input_schema" | b"output_schema",
            ) => (Node::Schema(S::Root), scope),
            (Node::Schema(S::SchedulerEvent), b"spec") => (Node::OperationSpec, gate(0)),
            (Node::Schema(S::SchedulerEvent), b"reducer") => (Node::EntrypointRef, gate(1)),
            (Node::OperationSpec, b"entrypoint") => (Node::EntrypointRef, scope),
            (Node::OperationSpec, b"orchestration") => (Node::Orchestration, scope),
            (Node::Orchestration, b"reducer") => (Node::EntrypointRef, gate(0)),
            (Node::EntrypointRef, b"result_schema") => (Node::Schema(S::Root), scope),
            (Node::Schema(S::InteractionResponse), b"value") => (Node::Schema(S::Root), gate(0)),
            (Node::Schema(S::RecursiveForkToolPlan) | Node::RecursivePlanPayload, b"request") => {
                (Node::Schema(S::ResourcePreparationRequest), scope)
            }
            (
                Node::Schema(S::RecursiveForkToolPlan) | Node::RecursivePlanPayload,
                b"preparation",
            ) => (Node::Schema(S::TaskAdmissionRecord), scope),
            (Node::Schema(S::RecursivePlan), b"plan") => (Node::RecursivePlanPayload, gate(0)),
            (Node::Schema(S::RecursiveInitialPlan), b"plan") => {
                (Node::Schema(S::RecursivePlan), scope)
            }
            (Node::Schema(S::RecursiveState), b"arguments") => (Node::Schema(S::Root), gate(0)),
            (Node::Schema(S::RecursiveState), b"intent") => {
                (Node::Schema(S::RecursiveForkSourceIntent), gate(0))
            }
            (Node::Schema(S::RecursiveState), b"report") => (Node::Schema(S::ForkReport), gate(1)),
            (Node::Schema(S::RecursiveState), b"plan") => (Node::Schema(S::RecursivePlan), gate(1)),
            (Node::Schema(S::RecursiveState), b"receipt") => {
                (Node::Schema(S::ProjectMergeReceipt), gate(1))
            }
            (Node::Schema(S::RecursiveState), b"value") => (Node::Schema(S::Root), gate(2)),
            (Node::Schema(S::RecursiveReceipt), b"value") => (Node::Schema(S::TaskOutcome), scope),
            (Node::Schema(S::TaskOutcome), b"Succeeded") => (Node::Schema(S::Root), scope),
            (Node::Schema(S::ProjectMergeReceipt), b"provider_proof") => {
                (Node::ProviderProof, scope)
            }
            (Node::ProviderProof, b"statement") => (Node::Schema(S::Root), scope),
            (Node::Schema(S::ProjectSourceAuthorization), b"native_snapshot") => {
                (Node::Schema(S::Root), scope)
            }
            (Node::Schema(S::ForkRequest) | Node::Schema(S::ForkSeed), b"authorized_source") => {
                (Node::Schema(S::ProjectSourceAuthorization), scope)
            }
            (
                Node::Schema(S::ForkReport) | Node::Schema(S::RecursiveForkSourceIntent),
                b"request",
            ) => (Node::Schema(S::ForkRequest), scope),
            (Node::Schema(S::RecursiveSourcePin), b"intent") => {
                (Node::Schema(S::RecursiveForkSourceIntent), scope)
            }
            (Node::Schema(S::ForkPreparationCommandResults), b"commands") => {
                (Node::ForkPreparationCommands, scope)
            }
            (Node::ForkPreparationCommands, _) if !frame.object => {
                (Node::ForkPreparationCommandResult, scope)
            }
            (Node::ForkPreparationCommandResult, b"value") => (Node::ForkReportOutcome, scope),
            (Node::ForkReportOutcome, b"Succeeded") => (Node::Schema(S::ForkReport), scope),
            (Node::Schema(S::ConfiguredRecursiveSourceCheckpoint), b"snapshot") => {
                (Node::Schema(S::Snapshot), scope)
            }
            (Node::Schema(S::ConfiguredRecursiveSourceCheckpoint), b"request") => {
                (Node::Schema(S::ForkRequest), scope)
            }
            (Node::Schema(S::RecursiveMergeToolPlan), b"authorized_source") => {
                (Node::Schema(S::ProjectSourceAuthorization), scope)
            }
            (Node::RecursivePlanPayload, b"authorized_source") => {
                let scope = match scope {
                    Scope::Gate(owner, _) => Scope::Gate(owner, 1),
                    _ => Scope::Ignore,
                };
                (Node::Schema(S::ProjectSourceAuthorization), scope)
            }
            (Node::Schema(S::ToolTaskCommand), b"arguments") => (Node::Schema(S::Root), scope),
            (Node::Schema(S::TaskAdmitCommand), b"input") => (Node::Schema(S::Root), scope),
            (Node::Schema(S::PreparedTaskAdmitCommand), b"admission") => {
                (Node::Schema(S::TaskAdmissionRecord), scope)
            }
            (Node::Schema(S::ModelTaskCommand), b"input") => (Node::Schema(S::ModelContent), scope),
            (Node::Schema(S::ModelTaskCommand), b"selected_context") => {
                (Node::Schema(S::Context), scope)
            }
            (Node::Schema(S::NativeProcessRequest), b"mcp_stdio") => (Node::McpStdioRequest, scope),
            (Node::McpStdioRequest, b"params") => (Node::Schema(S::Root), scope),
            _ => (Node::Schema(S::None), Scope::Ignore),
        };
        Selection { node, scope }
    }
}
