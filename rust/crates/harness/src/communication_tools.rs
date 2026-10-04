//! Version-pinned model tools for explicit task messages and waits.
//!
//! These tools are intentionally thin adapters over [`DurableCommunication`].
//! Model-visible JSON contains only the requested target and explicit content
//! references.  The sender, wait owner, operation identity, and authorization
//! come from the authenticated [`ToolContext`], and never from model content.

use crate::{
    Error, Outcome, Result, TaskId,
    communication::{
        DurableCommunication, DurableWaitStore, MessageRequest, MessageTarget, WaitCompletion,
        WaitRequest, WaitTarget,
    },
    conversation::FileRef,
    runtime::ToolContext,
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry,
        ToolResult,
    },
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::sync::watch;

/// Stable model-visible name for explicit parent/child messages.
pub const MESSAGE_TOOL_NAME: &str = "swarm.message";
/// Stable model-visible name for explicit task, inbox, or deadline waits.
pub const WAIT_TOOL_NAME: &str = "swarm.wait";
/// Revision of the model-facing message contract.
pub const TOOL_REVISION: &str = "2";
const WAIT_TOOL_REVISION: &str = "3";

/// Runtime-owned cancellation source for authenticated wait calls.
///
/// Implementations return a receiver for the admitted task's cancellation
/// scope. This is runtime provenance and never enters model-visible content.
pub trait WaitCancellationSource: Send + Sync {
    /// Returns a live cancellation receiver for one admitted task.
    fn receiver(&self, task_id: TaskId) -> Option<watch::Receiver<bool>>;

    /// Requests cancellation for one admitted task when this source is also
    /// owner-controlled. Read-only sources may retain the default refusal.
    fn cancel(&self, _task_id: TaskId) -> Result<()> {
        Err(Error::Unsupported(
            "cancellation source is observation-only".into(),
        ))
    }
}

/// Local authenticated cancellation registry for task execution hosts.
///
/// A runtime registers a task when it admits the task, passes this source to
/// the communication tool registry, and calls [`Self::cancel`] from the same
/// owner authority that controls the task. The registry is deliberately a
/// live cancellation bridge; durable cancellation declarations remain owned
/// by the task journal and should recreate the cancelled state on resume.
#[derive(Default)]
pub struct LocalTaskCancellationSource {
    scopes: Mutex<BTreeMap<TaskId, watch::Sender<bool>>>,
}

impl LocalTaskCancellationSource {
    /// Registers one admitted task's live cancellation scope. Re-registering
    /// an active task preserves its sender so existing waits remain attached
    /// to the same cancellation channel; call `remove` before a new scope.
    pub fn register(&self, task_id: TaskId) -> Result<()> {
        if task_id.into_bytes() == [0; 16] {
            return Err(Error::Invalid("cancellation task identity is nil".into()));
        }
        let mut scopes = self
            .scopes
            .lock()
            .map_err(|_| Error::Storage("cancellation registry lock poisoned".into()))?;
        if scopes.contains_key(&task_id) {
            return Ok(());
        }
        let (sender, _) = watch::channel(false);
        scopes.insert(task_id, sender);
        Ok(())
    }

    /// Requests cancellation for one registered task.
    pub fn cancel(&self, task_id: TaskId) -> Result<()> {
        let scopes = self
            .scopes
            .lock()
            .map_err(|_| Error::Storage("cancellation registry lock poisoned".into()))?;
        let sender = scopes
            .get(&task_id)
            .ok_or_else(|| Error::NotFound("cancellation task scope".into()))?;
        sender.send_replace(true);
        Ok(())
    }

    /// Removes one task's live scope after its task loop exits.
    pub fn remove(&self, task_id: TaskId) -> Result<()> {
        self.scopes
            .lock()
            .map_err(|_| Error::Storage("cancellation registry lock poisoned".into()))?
            .remove(&task_id);
        Ok(())
    }
}

impl WaitCancellationSource for LocalTaskCancellationSource {
    fn receiver(&self, task_id: TaskId) -> Option<watch::Receiver<bool>> {
        self.scopes
            .lock()
            .ok()
            .and_then(|scopes| scopes.get(&task_id).map(|sender| sender.subscribe()))
    }

    fn cancel(&self, task_id: TaskId) -> Result<()> {
        LocalTaskCancellationSource::cancel(self, task_id)
    }
}

/// Message target selected by the model.  The durable layer verifies the
/// selected relationship against immutable admissions before publishing.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageToolTarget {
    /// Deliver to the sender's direct parent.
    Parent,
    /// Deliver to the sender's direct child.
    Child,
}

impl From<MessageToolTarget> for MessageTarget {
    fn from(value: MessageToolTarget) -> Self {
        match value {
            MessageToolTarget::Parent => Self::Parent,
            MessageToolTarget::Child => Self::Child,
        }
    }
}

fn wait_cancellation_id(operation_id: crate::OperationId) -> crate::OperationId {
    let digest = blake3::hash(
        &[
            b"harness:wait-cancellation:v1".as_slice(),
            operation_id.into_bytes().as_slice(),
        ]
        .concat(),
    );
    let mut identity = [0_u8; 16];
    identity.copy_from_slice(&digest.as_bytes()[..16]);
    crate::OperationId::from_bytes(identity)
}

/// Exact arguments accepted by the message tool.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageToolInput {
    /// Direct parent or child task identity.
    pub recipient: String,
    /// Relationship the caller claims for the recipient.
    pub target: MessageToolTarget,
    /// Version-pinned explicit message content.
    pub payload: FileRef,
}

/// Result exposed after a durable message has been admitted or reconciled.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageToolOutput {
    /// Durable operation identity, assigned by the runtime.
    pub message_id: String,
    /// Recipient selected in the request.
    pub recipient: String,
    /// True only after the host reports observed publication.
    pub delivered: bool,
}

/// Exact arguments accepted by the wait tool.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitToolInput {
    /// Observe the requested direct children in request order.
    Tasks {
        /// Direct child task identities.
        task_ids: Vec<String>,
        /// Optional absolute timeout in Unix milliseconds.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_epoch_ms: Option<u64>,
    },
    /// Observe new messages in the caller's own inbox.
    Messages {
        /// Last consumed sequence number.
        after: u64,
        /// Maximum number of messages to return.
        limit: usize,
        /// Optional absolute timeout in Unix milliseconds.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_epoch_ms: Option<u64>,
    },
    /// Suspend until an absolute deadline.
    Deadline {
        /// Absolute Unix deadline in milliseconds.
        deadline_epoch_ms: u64,
        /// Optional absolute timeout for the wait operation.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_epoch_ms: Option<u64>,
    },
}

/// Explicit terminal observation for one waited child.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitTaskOutput {
    /// Child identity.
    pub task_id: String,
    /// Terminal state observed by the durable host.
    pub status: WaitTaskStatus,
    /// Successful result, if one was returned by the child.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// Failure description, if the child failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Identity requiring reconciliation when completion is uncertain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
}

/// Stable status vocabulary for waited child outcomes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitTaskStatus {
    /// Child returned a successful value.
    Succeeded,
    /// Child returned a failure message.
    Failed,
    /// Child was cancelled.
    Cancelled,
    /// Child completion remains uncertain and needs reconciliation.
    Indeterminate,
}

/// One explicit inbox record returned by a message wait.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitMessageOutput {
    /// Gapless inbox sequence.
    pub sequence: u64,
    /// Sender-defined idempotency identity.
    pub message_id: String,
    /// Authenticated sender retained by the owner mailbox.
    pub sender: TaskId,
    /// Owner-clock delivery timestamp.
    pub delivered_at_epoch_ms: u64,
    /// Version-pinned message content.
    pub payload: FileRef,
}

/// Typed result of a wait operation.  Cancellation and timeout are terminal
/// values and are never represented as successful observations.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitToolOutput {
    /// Child outcomes in exactly the requested order.
    Tasks {
        /// One result for each requested child.
        outcomes: Vec<WaitTaskOutput>,
    },
    /// Newly delivered records after the requested cursor.
    Messages {
        /// Ordered records after the requested cursor.
        items: Vec<WaitMessageOutput>,
    },
    /// The requested deadline was reached.
    Deadline,
    /// The caller cancelled the wait.
    Cancelled,
    /// The absolute timeout elapsed first.
    TimedOut,
}

/// Returns the two model-facing communication tools bound to one durable host.
pub fn communication_tools(host: Arc<dyn crate::runtime::DurableTaskHost>) -> Result<ToolRegistry> {
    communication_tools_with_wait_store_and_cancellation(host, None, None)
}

/// Returns the communication tools with owner-retained wait persistence.
///
/// The wait store is deliberately supplied separately from the task host so a
/// coordinator can bind its existing journal without exposing that journal to
/// model content or creating a second orchestration engine.
pub fn communication_tools_with_wait_store(
    host: Arc<dyn crate::runtime::DurableTaskHost>,
    waits: Option<Arc<dyn DurableWaitStore>>,
) -> Result<ToolRegistry> {
    communication_tools_with_wait_store_and_cancellation(host, waits, None)
}

/// Returns communication tools with owner-retained wait persistence and the
/// task's authenticated cancellation source.
pub fn communication_tools_with_wait_store_and_cancellation(
    host: Arc<dyn crate::runtime::DurableTaskHost>,
    waits: Option<Arc<dyn DurableWaitStore>>,
    cancellation: Option<Arc<dyn WaitCancellationSource>>,
) -> Result<ToolRegistry> {
    let mut registry = ToolRegistry::new();
    registry.register(Tool {
        definition: message_definition(),
        executor: Arc::new(CommunicationExecutor {
            host: host.clone(),
            waits: waits.clone(),
            cancellation: cancellation.clone(),
            kind: CommunicationToolKind::Message,
        }),
        projection: Arc::new(CommunicationProjection),
    })?;
    registry.register(Tool {
        definition: wait_definition(),
        executor: Arc::new(CommunicationExecutor {
            host,
            waits,
            cancellation,
            kind: CommunicationToolKind::Wait,
        }),
        projection: Arc::new(CommunicationProjection),
    })?;
    Ok(registry)
}

/// Creates a typed model tool definition for messages.
#[must_use]
pub fn message_definition() -> ToolDefinition {
    ToolDefinition {
        name: MESSAGE_TOOL_NAME.into(),
        revision: TOOL_REVISION.into(),
        description: "Send explicit version-pinned content to a direct parent or child task."
            .into(),
        input_schema: message_input_schema(),
        output_schema: message_output_schema(),
        model_output_schema: message_output_schema(),
    }
}

/// Creates a typed model tool definition for waits.
#[must_use]
pub fn wait_definition() -> ToolDefinition {
    ToolDefinition {
        name: WAIT_TOOL_NAME.into(),
        revision: WAIT_TOOL_REVISION.into(),
        description: "Wait for named direct children, new inbox messages, or an absolute deadline."
            .into(),
        input_schema: wait_input_schema(),
        output_schema: wait_output_schema(false),
        model_output_schema: wait_output_schema(true),
    }
}

#[derive(Clone, Copy)]
enum CommunicationToolKind {
    Message,
    Wait,
}

struct CommunicationExecutor {
    host: Arc<dyn crate::runtime::DurableTaskHost>,
    waits: Option<Arc<dyn DurableWaitStore>>,
    cancellation: Option<Arc<dyn WaitCancellationSource>>,
    kind: CommunicationToolKind,
}

impl CommunicationExecutor {
    async fn execute_for_task(
        &self,
        waiter: TaskId,
        operation_id: crate::OperationId,
        invocation: ToolInvocation,
    ) -> Result<ToolResult> {
        invocation.validate()?;
        let expected_name = match self.kind {
            CommunicationToolKind::Message => MESSAGE_TOOL_NAME,
            CommunicationToolKind::Wait => WAIT_TOOL_NAME,
        };
        if invocation.name != expected_name {
            return Err(Error::Conflict(
                "communication invocation names another tool".into(),
            ));
        }
        match self.kind {
            CommunicationToolKind::Message => {
                let input: MessageToolInput = serde_json::from_value(invocation.arguments)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                let recipient = TaskId::parse(&input.recipient)?;
                DurableCommunication::new(self.host.clone())
                    .send(MessageRequest {
                        sender: waiter,
                        recipient,
                        message_id: operation_id,
                        target: input.target.into(),
                        payload: input.payload,
                    })
                    .await?;
                Ok(ToolResult {
                    value: serde_json::to_value(MessageToolOutput {
                        message_id: operation_id.to_string(),
                        recipient: recipient.to_string(),
                        delivered: true,
                    })
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                })
            }
            CommunicationToolKind::Wait => {
                if self.waits.is_none() {
                    return Err(Error::Unsupported(
                        "wait tools require an owner-retained durable wait store".into(),
                    ));
                }
                let input: WaitToolInput = serde_json::from_value(invocation.arguments)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                let timeout_epoch_ms = wait_timeout(&input);
                let target = wait_target(&input, waiter)?;
                let request = WaitRequest {
                    operation_id,
                    waiter,
                    target,
                    timeout_epoch_ms,
                    cancellation_id: Some(wait_cancellation_id(operation_id)),
                };
                let communication = DurableCommunication::new(self.host.clone());
                let communication = match &self.waits {
                    Some(waits) => communication.with_wait_store(waits.clone()),
                    None => communication,
                };
                let cancellation = self
                    .cancellation
                    .as_ref()
                    .and_then(|source| source.receiver(waiter));
                let completion = communication.wait(request, cancellation).await?;
                Ok(ToolResult {
                    value: serde_json::to_value(wait_output(completion))
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                })
            }
        }
    }

    async fn execute_scoped(
        &self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> Result<ToolResult> {
        invocation.validate()?;
        let expected_name = match self.kind {
            CommunicationToolKind::Message => MESSAGE_TOOL_NAME,
            CommunicationToolKind::Wait => WAIT_TOOL_NAME,
        };
        if invocation.name != expected_name {
            return Err(Error::Conflict(
                "communication invocation names another tool".into(),
            ));
        }
        let waiter = context.task().durable_task_id().ok_or_else(|| {
            Error::Unsupported("communication tools require an admitted durable task".into())
        })?;
        self.execute_for_task(waiter, context.operation_id(), invocation)
            .await
    }
}

impl ToolExecutor for CommunicationExecutor {
    fn execute<'a>(&'a self, _invocation: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "communication tools require authenticated task context".into(),
            ))
        })
    }

    fn execute_in_model_batch<'a>(
        &'a self,
        context: crate::tool::ModelToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            context.validate_invocation(&invocation)?;
            let waiter = context.task_id.ok_or_else(|| {
                Error::Unsupported("communication tools require authenticated task context".into())
            })?;
            self.execute_for_task(waiter, invocation.operation_id, invocation)
                .await
        })
    }

    fn reconcile_in_model_batch<'a>(
        &'a self,
        context: crate::tool::ModelToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            context.validate_invocation(&invocation)?;
            let waiter = context.task_id.ok_or_else(|| {
                Error::Unsupported("communication tools require authenticated task context".into())
            })?;
            self.execute_for_task(waiter, invocation.operation_id, invocation)
                .await
                .map(Some)
        })
    }

    fn execute_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(self.execute_scoped(context, invocation))
    }

    fn reconcile<'a>(
        &'a self,
        _invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "communication reconciliation requires authenticated task context".into(),
            ))
        })
    }

    fn reconcile_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move { self.execute_scoped(context, invocation).await.map(Some) })
    }
}

struct CommunicationProjection;

impl ToolProjection for CommunicationProjection {
    fn project(&self, _invocation: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        Ok(result.value.clone())
    }
}

fn wait_target(input: &WaitToolInput, waiter: TaskId) -> Result<WaitTarget> {
    match input {
        WaitToolInput::Tasks { task_ids, .. } => Ok(WaitTarget::Tasks {
            task_ids: task_ids
                .iter()
                .map(|task| TaskId::parse(task))
                .collect::<Result<Vec<_>>>()?,
        }),
        WaitToolInput::Messages { after, limit, .. } => Ok(WaitTarget::Messages {
            task_id: waiter,
            after: *after,
            limit: *limit,
        }),
        WaitToolInput::Deadline {
            deadline_epoch_ms, ..
        } => Ok(WaitTarget::Deadline {
            deadline_epoch_ms: *deadline_epoch_ms,
        }),
    }
}

fn wait_timeout(input: &WaitToolInput) -> Option<u64> {
    match input {
        WaitToolInput::Tasks {
            timeout_epoch_ms, ..
        }
        | WaitToolInput::Messages {
            timeout_epoch_ms, ..
        }
        | WaitToolInput::Deadline {
            timeout_epoch_ms, ..
        } => *timeout_epoch_ms,
    }
}

fn wait_output(completion: WaitCompletion) -> WaitToolOutput {
    match completion {
        WaitCompletion::Tasks { outcomes } => WaitToolOutput::Tasks {
            outcomes: outcomes
                .into_iter()
                .map(|(task_id, outcome)| {
                    let mut output = WaitTaskOutput {
                        task_id: task_id.to_string(),
                        status: WaitTaskStatus::Cancelled,
                        value: None,
                        message: None,
                        operation_id: None,
                    };
                    match outcome {
                        Outcome::Succeeded(value) => {
                            output.status = WaitTaskStatus::Succeeded;
                            output.value = Some(value);
                        }
                        Outcome::Failed { message } => {
                            output.status = WaitTaskStatus::Failed;
                            output.message = Some(message);
                        }
                        Outcome::Cancelled => {}
                        Outcome::Indeterminate { operation_id } => {
                            output.status = WaitTaskStatus::Indeterminate;
                            output.operation_id = Some(operation_id.to_string());
                        }
                    }
                    output
                })
                .collect(),
        },
        WaitCompletion::Messages { items } => WaitToolOutput::Messages {
            items: items
                .into_iter()
                .map(|item| WaitMessageOutput {
                    sequence: item.sequence,
                    message_id: item.message_id,
                    sender: item.sender,
                    delivered_at_epoch_ms: item.delivered_at_epoch_ms,
                    payload: item.payload,
                })
                .collect(),
        },
        WaitCompletion::Deadline => WaitToolOutput::Deadline,
        WaitCompletion::Cancelled => WaitToolOutput::Cancelled,
        WaitCompletion::TimedOut => WaitToolOutput::TimedOut,
    }
}

fn file_ref_schema() -> Value {
    json!({
        "type": "object", "additionalProperties": false,
        "required": ["volume", "path", "version", "descriptor", "display_name"],
        "properties": {
            "volume": {"type":"object", "additionalProperties":false,
                "required":["provider","id","class","owner"], "properties": {
                    "provider": {"type":"object", "additionalProperties":false,
                        "required":["namespace","family","version"], "properties": {
                            "namespace":{"type":"string"}, "family":{"type":"string"}, "version":{"type":"string"}
                        }},
                    "id":{"type":"string"}, "class":{"type":"string", "enum":["project","agent_private","session_shared"]},
                    "owner":{"type":"object", "additionalProperties":false, "required":["kind","id"],
                        "properties":{"kind":{"type":"string","enum":["project","agent","session"]},"id":{}}}
                }},
            "path":{"type":"string"}, "version":{"type":"string"}, "display_name":{"type":"string"},
            "descriptor":{"type":"object", "additionalProperties":false,
                "required":["sha256","byte_length","media_type"], "properties": {
                    "sha256":{"type":"array", "minItems":32, "maxItems":32, "items":{"type":"integer","minimum":0,"maximum":255}},
                    "byte_length":{"type":"integer","minimum":0}, "media_type":{"type":"string"}
                }}
        }
    })
}

fn message_input_schema() -> Value {
    json!({"type":"object", "additionalProperties":false, "required":["recipient","target","payload"],
        "properties":{"recipient":{"type":"string"}, "target":{"type":"string","enum":["parent","child"]}, "payload":file_ref_schema()}})
}

fn message_output_schema() -> Value {
    json!({"type":"object", "additionalProperties":false, "required":["message_id","recipient","delivered"],
        "properties":{"message_id":{"type":"string"},"recipient":{"type":"string"},"delivered":{"const":true}}})
}

fn wait_input_schema() -> Value {
    // `0` is rejected by the durable wait contract; an omitted or null
    // timeout remains the explicit unbounded form.
    let timeout = json!({"type":["integer","null"], "minimum":1});
    json!({"oneOf":[
        {"type":"object","additionalProperties":false,"required":["kind","task_ids"],"properties":{"kind":{"const":"tasks"},"task_ids":{"type":"array","minItems":1,"maxItems":64,"items":{"type":"string"}},"timeout_epoch_ms":timeout}},
        {"type":"object","additionalProperties":false,"required":["kind","after","limit"],"properties":{"kind":{"const":"messages"},"after":{"type":"integer","minimum":0},"limit":{"type":"integer","minimum":1,"maximum":1024},"timeout_epoch_ms":timeout}},
        {"type":"object","additionalProperties":false,"required":["kind","deadline_epoch_ms"],"properties":{"kind":{"const":"deadline"},"deadline_epoch_ms":{"type":"integer","minimum":1},"timeout_epoch_ms":timeout}}
    ]})
}

fn wait_output_schema(declare_references: bool) -> Value {
    let mut payload = file_ref_schema();
    if declare_references {
        payload["x-acyclic-file-ref"] = json!(true);
    }
    let mut branches = vec![
        json!({"type":"object","additionalProperties":false,"required":["kind","outcomes"],"properties":{"kind":{"const":"tasks"},"outcomes":{"type":"array","items":{"type":"object","additionalProperties":false,"required":["task_id","status"],"properties":{"task_id":{"type":"string"},"status":{"type":"string","enum":["succeeded","failed","cancelled","indeterminate"]},"value":{},"message":{"type":"string"},"operation_id":{"type":"string"}}}}}}),
        json!({"type":"object","additionalProperties":false,"required":["kind","items"],"properties":{"kind":{"const":"messages"},"items":{"type":"array","items":{"type":"object","additionalProperties":false,"required":["sequence","message_id","sender","delivered_at_epoch_ms","payload"],"properties":{"sequence":{"type":"integer","minimum":1},"message_id":{"type":"string"},"sender":{"type":"string"},"delivered_at_epoch_ms":{"type":"integer","minimum":1},"payload":payload}}}}}),
    ];
    for kind in ["deadline", "cancelled", "timed_out"] {
        branches.push(json!({"type":"object","additionalProperties":false,
            "required":["kind"], "properties":{"kind":{"const":kind}}}));
    }
    json!({"oneOf":branches})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        OperationId,
        conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
        resources::ProviderRef,
        runtime::{DurableTaskHost, TaskAdmissionRecord},
        tool::ModelToolContext,
    };
    use futures::future::BoxFuture;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    fn task(value: u8) -> TaskId {
        TaskId::from_bytes([value; 16])
    }
    fn operation(value: u8) -> OperationId {
        OperationId::from_bytes([value; 16])
    }

    #[test]
    fn definitions_are_strict_and_stable() -> Result<()> {
        let message = message_definition();
        let wait = wait_definition();
        assert_eq!(message.revision, "2");
        assert_eq!(wait.revision, "3");
        message.validate()?;
        wait.validate()?;
        assert_eq!(message.digest()?, message_definition().digest()?);
        assert!(
            jsonschema::validator_for(&message.input_schema)
                .unwrap()
                .validate(&json!({"recipient":"x","target":"parent"}))
                .is_err()
        );
        assert!(
            jsonschema::validator_for(&wait.input_schema)
                .unwrap()
                .validate(&json!({"kind":"messages","after":0,"limit":1,"extra":true}))
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn wait_message_results_declare_only_explicit_immutable_payloads() -> Result<()> {
        let payload = FileRef::new(
            VolumeRef::new(ProviderRef::new("test", "filesystem", "2")?,
                "inbox", VolumeClass::AgentPrivate,
                VolumeOwner::Agent(crate::AgentId::from_bytes([9; 16])))?,
            "message.txt", "generation-1",
            FileDescriptor::from_bytes("message λ🦀".as_bytes(), "text/plain")?,
            "message.txt",
        )?;
        let definition = wait_definition();
        let mut output = json!({"kind":"messages", "items":[{
            "sequence":1, "message_id":"message-1", "sender":task(2).to_string(),
            "delivered_at_epoch_ms":1, "payload":payload
        }]});
        assert_eq!(definition.model_output_file_refs(&output)?, vec![payload]);
        for kind in ["deadline", "cancelled", "timed_out"] {
            assert!(definition.model_output_file_refs(&json!({"kind":kind}))?.is_empty());
        }
        assert!(definition.model_output_file_refs(&json!({"kind":"tasks", "outcomes":[]}))?.is_empty());
        output["items"][0].as_object_mut().expect("message fixture").remove("payload");
        assert!(matches!(definition.model_output_file_refs(&output), Err(Error::Invalid(_))));
        assert!(matches!(definition.model_output_file_refs(&json!({"kind":"messages"})), Err(Error::Invalid(_))));
        Ok(())
    }

    #[test]
    fn wait_outputs_preserve_order_and_terminal_states() -> Result<()> {
        let output = wait_output(WaitCompletion::Tasks {
            outcomes: vec![
                (task(2), Outcome::Succeeded(json!({"ok": true}))),
                (
                    task(3),
                    Outcome::Indeterminate {
                        operation_id: operation(7),
                    },
                ),
            ],
        });
        let value =
            serde_json::to_value(output).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(value["kind"], "tasks");
        assert_eq!(value["outcomes"][0]["task_id"], task(2).to_string());
        assert_eq!(value["outcomes"][1]["status"], "indeterminate");
        Ok(())
    }

    #[test]
    fn local_cancellation_source_is_task_scoped() -> Result<()> {
        let source = LocalTaskCancellationSource::default();
        let owner = task(8);
        let sibling = task(9);
        source.register(owner)?;
        source.register(sibling)?;
        let owner_receiver = source.receiver(owner).expect("owner scope");
        assert!(!*owner_receiver.borrow());
        assert!(!*source.receiver(sibling).expect("sibling scope").borrow());
        // Re-registering an admitted task must not replace the sender held by
        // an in-flight wait. The original receiver still observes the
        // durable cancellation signal.
        source.register(owner)?;
        source.cancel(owner)?;
        assert!(*owner_receiver.borrow());
        assert!(*source.receiver(owner).expect("owner scope").borrow());
        assert!(!*source.receiver(sibling).expect("sibling scope").borrow());
        assert!(matches!(source.cancel(task(10)), Err(Error::NotFound(_))));
        Ok(())
    }

    struct RecordingHost(Mutex<Option<TaskId>>);

    impl DurableTaskHost for RecordingHost {
        fn outcome<'a>(
            &'a self,
            _: TaskId,
        ) -> BoxFuture<'a, Result<Option<crate::Outcome<serde_json::Value>>>> {
            Box::pin(async { Err(Error::Unsupported("test host is unbound".into())) })
        }

        fn cancel<'a>(&'a self, _: TaskId) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Err(Error::Unsupported("test host is unbound".into())) })
        }

        fn observe_admission<'a>(
            &'a self,
            task_id: TaskId,
        ) -> BoxFuture<'a, Result<TaskAdmissionRecord>> {
            *self.0.lock().expect("recording host lock") = Some(task_id);
            Box::pin(async { Err(Error::Unsupported("test host is unbound".into())) })
        }
    }

    #[tokio::test]
    async fn model_batch_communication_requires_and_uses_authenticated_task_identity() -> Result<()> {
        let host = Arc::new(RecordingHost(Mutex::new(None)));
        let executor = CommunicationExecutor {
            host: host.clone(),
            waits: None,
            cancellation: None,
            kind: CommunicationToolKind::Message,
        };
        let parent_operation = operation(12);
        let invocation = ToolInvocation::for_model_call(
            parent_operation,
            0,
            "message-call".into(),
            MESSAGE_TOOL_NAME.into(),
            json!({"recipient": task(2).to_string(), "target": "parent", "payload": "missing"}),
        );
        let unauthenticated = ModelToolContext {
            parent_operation,
            step: 0,
            task_id: None,
        };
        assert!(matches!(
            executor
                .execute_in_model_batch(unauthenticated, invocation.clone())
                .await,
            Err(Error::Unsupported(message)) if message.contains("authenticated task context")
        ));
        assert_eq!(*host.0.lock().expect("recording host lock"), None);

        let authenticated = ModelToolContext {
            parent_operation,
            step: 0,
            task_id: Some(task(7)),
        };
        assert!(matches!(
            executor.execute_in_model_batch(authenticated, invocation.clone()).await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(*host.0.lock().expect("recording host lock"), None);
        let volume = VolumeRef::new(
            ProviderRef::new("test", "filesystem", "2")?,
            "private",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([9; 16])),
        )?;
        let payload = FileRef::new(
            volume, "message.json", "v1",
            FileDescriptor::from_bytes(b"{}", "application/json")?, "message.json",
        )?;
        let mut invocation = invocation;
        invocation.arguments["payload"] = serde_json::to_value(payload)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(matches!(
            executor
                .execute_in_model_batch(authenticated, invocation)
                .await,
            Err(Error::Unsupported(message)) if message.contains("test host")
        ));
        assert_eq!(*host.0.lock().expect("recording host lock"), Some(task(7)));
        Ok(())
    }
}
