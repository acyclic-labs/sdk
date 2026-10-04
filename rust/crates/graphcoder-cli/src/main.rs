//! Native JSON-lines host for the local GraphCoder composition.
//!
//! This binary deliberately contains only protocol decoding and composition.
//! Durable session state, model admission, workspace effects, and recovery are
//! owned by `PersistentLocalSwarm`. Methods whose durable projection is not
//! exposed by that constructor return a typed `unsupported` response rather
//! than maintaining a second store in the host.

#![deny(unsafe_code)]
#![cfg_attr(test, allow(clippy::expect_used, clippy::indexing_slicing))]

use futures::StreamExt;
use acyclic_harness::{
    conversation::{Limits, VolumeClass, VolumeOwner, VolumeRef},
    filesystem::{
        FilesystemHost, LocalFilesystemForkResolver, LocalSessionPhase, LocalSwarmBindings,
        LocalSwarmConfig, PersistentLocalSwarm,
    },
    core::Scope,
    interaction::InteractionResponse,
    model::{Model, ModelAttempt, ModelContent, ModelContentPart, ModelEvent, ModelProvider, ModelRequest},
    resources::ProviderRef,
    Error as HarnessError, InteractionId, OperationId, Result, TaskId,
};
use acyclic_fs::{LocalFs, LocalOptions};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use clap::Parser;
use futures::{FutureExt, future::BoxFuture, stream::BoxStream};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;

#[cfg(test)]
use tokio::io::AsyncReadExt;

const MAX_LINE_BYTES: usize = 16 * 1024 * 1024;
/// Bound the number of response tasks retained while stdin remains open.
/// Requests beyond this window wait for an earlier response task to finish,
/// so a producer cannot turn a slow model call into unbounded host memory.
const MAX_IN_FLIGHT: usize = 64;
const MAX_CONTROL_IN_FLIGHT: usize = 8;

/// Runtime configuration. The model is intentionally a deterministic fixture:
/// production model adapters are selected by a future host package.
#[derive(Debug, Parser)]
#[command(
    name = "graphcoder-runtime",
    version,
    about = "Local GraphCoder JSON-lines host"
)]
struct Args {
    /// Durable swarm root. It is created by the SDK's local storage provider.
    #[arg(long, env = "GRAPHCODER_ROOT")]
    root: PathBuf,
    /// Explicit deterministic model fixture.
    #[arg(long, default_value = "echo")]
    model_fixture: String,
}

#[derive(Clone)]
struct EchoModel {
    fixture: String,
    calls: Arc<AtomicUsize>,
}

impl ModelProvider for EchoModel {
    fn generate<'a>(
        &'a self,
        prepared: acyclic_harness::model_input::PreparedModelInput,
    ) -> BoxStream<'a, acyclic_harness::Result<ModelEvent>> {
        let request = prepared.request().clone();
        let call = self.calls.fetch_add(1, Ordering::Relaxed);
        if self.fixture == "stage" && call > 0 {
            let result = request.messages.iter().rev().find_map(|message| match &message.content {
                acyclic_harness::model::ModelContent::Part(
                    acyclic_harness::model::ModelContentPart::ToolResult { value, .. }
                ) => Some(value),
                _ => None,
            });
            if !result.is_some_and(|value| value.get("file").is_some()) {
                return Box::pin(futures::stream::iter([Err(HarnessError::Storage(
                    format!("stage fixture lacks successful file result: {result:?}")
                ))]));
            }
        }
        let text = match self.fixture.as_str() {
            "echo" => request
                .messages
                .iter()
                .rev()
                .find_map(|message| match &message.content {
                    acyclic_harness::model::ModelContent::Text(value) => Some(value.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| "fixture:echo".to_owned()),
            "complete" => "fixture:complete".to_owned(),
            "stage" if call > 0 => "fixture:stage complete".to_owned(),
            _ => "fixture:echo".to_owned(),
        };
        if self.fixture == "recursive" {
            let child_a = OperationId::from_bytes([0xa1; 16]).to_string();
            let child_b = OperationId::from_bytes([0xb1; 16]).to_string();
            let grandchild = OperationId::from_bytes([0xc1; 16]).to_string();
            let has_text = |needle: &str| {
                request.messages.iter().any(|message| match &message.content {
                    ModelContent::Text(value) => value.contains(needle),
                    ModelContent::Part(ModelContentPart::Text { text }) => text.contains(needle),
                    ModelContent::Parts(parts) => parts.iter().any(|part| {
                        matches!(part, ModelContentPart::Text { text } if text.contains(needle))
                    }),
                    ModelContent::Part(ModelContentPart::ToolResult { value, .. }) => {
                        value.to_string().contains(needle)
                    }
                    ModelContent::Part(ModelContentPart::ToolCall { arguments, .. }) => {
                        arguments.to_string().contains(needle)
                    }
                    ModelContent::Part(ModelContentPart::File { .. }) => false,
                })
            };
            let has_tool_result = |name: &str| {
                request.messages.iter().any(|message| {
                    matches!(
                        &message.content,
                        ModelContent::Part(ModelContentPart::ToolResult { name: result_name, .. })
                            if result_name == name
                    )
                })
            };
            let root = !has_text("child task: child-a")
                && !has_text("child task: child-b")
                && !has_text("child task: grandchild");
            if root && !has_tool_result("acyclic.fork_child") {
                return Box::pin(futures::stream::iter([
                    Ok(ModelEvent::Content {
                        delta: "fixture:recursive root".to_owned(),
                    }),
                    Ok(ModelEvent::ToolCall {
                        call_id: "graphcoder-recursive-child-a".to_owned(),
                        name: "acyclic.fork_child".to_owned(),
                        arguments: json!({
                            "child_operation": child_a,
                            "task": "child-a",
                            "prompt": "recursive child a"
                        }),
                    }),
                    Ok(ModelEvent::ToolCall {
                        call_id: "graphcoder-recursive-child-b".to_owned(),
                        name: "acyclic.fork_child".to_owned(),
                        arguments: json!({
                            "child_operation": child_b,
                            "task": "child-b",
                            "prompt": "recursive child b"
                        }),
                    }),
                    Ok(ModelEvent::Completed {
                        metadata: Value::Null,
                    }),
                ]));
            }
            if has_text("child task: child-a") && !has_tool_result("acyclic.fork_child") {
                return Box::pin(futures::stream::iter([
                    Ok(ModelEvent::Content {
                        delta: "fixture:recursive child".to_owned(),
                    }),
                    Ok(ModelEvent::ToolCall {
                        call_id: "graphcoder-recursive-grandchild".to_owned(),
                        name: "acyclic.fork_child".to_owned(),
                        arguments: json!({
                            "child_operation": grandchild,
                            "task": "grandchild",
                            "prompt": "recursive grandchild"
                        }),
                    }),
                    Ok(ModelEvent::Completed {
                        metadata: Value::Null,
                    }),
                ]));
            }
            return Box::pin(futures::stream::iter([
                Ok(ModelEvent::Content {
                    delta: "fixture:recursive complete".to_owned(),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]));
        }
        if self.fixture == "stage" && call == 0 {
            return Box::pin(futures::stream::iter([
                Ok(ModelEvent::Content {
                    delta: "fixture:stage".to_owned(),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "graphcoder-stage-1".to_owned(),
                    name: "acyclic.stage_file".to_owned(),
                    arguments: json!({
                        "path": "graphcoder-fixture.txt",
                        "text": "fixture:stage",
                        "media_type": "text/plain",
                        "display_name": "graphcoder-fixture.txt",
                    }),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]));
        }
        Box::pin(futures::stream::iter([
            Ok(ModelEvent::Content { delta: text }),
            Ok(ModelEvent::Completed {
                metadata: Value::Null,
            }),
        ]))
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: ModelAttempt,
    ) -> BoxFuture<'a, acyclic_harness::Result<Option<Vec<ModelEvent>>>> {
        async { Ok(None) }.boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRequest {
    request_id: String,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum WireResponse {
    Ok {
        request_id: String,
        ok: bool,
        result: Value,
    },
    Err {
        request_id: String,
        ok: bool,
        error: WireError,
    },
}

#[derive(Debug, Serialize)]
struct WireError {
    code: &'static str,
    message: String,
}

impl WireResponse {
    fn ok(request_id: &str, result: Value) -> Self {
        Self::Ok {
            request_id: request_id.to_owned(),
            ok: true,
            result,
        }
    }

    fn error(request_id: &str, code: &'static str, message: impl Into<String>) -> Self {
        Self::Err {
            request_id: request_id.to_owned(),
            ok: false,
            error: WireError {
                code,
                message: message.into(),
            },
        }
    }
}

struct Runtime {
    swarm: Arc<PersistentLocalSwarm>,
    model_fixture: String,
    /// Host-only approval authority. The terminal receives a callback for the
    /// exact pending invocation and never creates an issuer or scope itself.
    approval_authorizer: ApprovalAuthorizer,
}

/// Host-owned operator authorization for one exact pending interaction.
///
/// A production host supplies this callback when it composes the runtime. The
/// default terminal entrypoint remains fail-closed because it has no authority
/// from which it could mint a responder scope.
type ApprovalAuthorizer = Arc<
    dyn Fn(TaskId, InteractionId) -> BoxFuture<'static, Result<Scope>> + Send + Sync,
>;

fn unavailable_approval_authorizer(_: TaskId, _: InteractionId) -> BoxFuture<'static, Result<Scope>> {
    async {
        Err(HarnessError::Unsupported(
            "approval resolution requires a host-owned operator authorizer",
        ))
    }
    .boxed()
}

async fn recursive_project(
    root: &std::path::Path,
) -> Result<(
    Arc<FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>>,
    StreamClient<LocalStream>,
    VolumeRef,
)> {
    let provider = ProviderRef::new("local", "filesystem", "2")?;
    let host = Arc::new(
        FilesystemHost::new(
            LocalFs::local(LocalOptions::new(root.join("filesystem"))).await
                .map_err(|error| HarnessError::Storage(error.to_string()))?,
            provider.clone(),
        )?,
    );
    let stream = StreamClient::new(Arc::new(
        LocalStream::open(root.join("conversation"), LocalStreamLimits::default())
            .await
            .map_err(|error| HarnessError::Storage(error.to_string()))?,
    ));
    let project = VolumeRef::new(
        provider,
        "graphcoder-recursive-project",
        VolumeClass::Project,
        VolumeOwner::Project("graphcoder-recursive-fixture".into()),
    )?;
    host.create_volume(&project).await?;
    Ok((host, stream, project))
}

impl Runtime {
    async fn open(args: &Args) -> Result<Self, HarnessError> {
        Self::open_with_authorizer(args, Arc::new(unavailable_approval_authorizer)).await
    }

    /// Opens the terminal with a host-owned operator callback. The callback is
    /// passed the exact task and pending interaction identity so the host can
    /// issue only the corresponding responder grant.
    async fn open_with_authorizer(
        args: &Args,
        approval_authorizer: ApprovalAuthorizer,
    ) -> Result<Self, HarnessError> {
        let fixture = match args.model_fixture.as_str() {
            "echo" | "complete" | "stage" | "recursive" => args.model_fixture.clone(),
            value => {
                return Err(HarnessError::Invalid(format!(
                    "unknown model fixture {value}"
                )));
            }
        };
        let model = Model::new(
            "graphcoder.mock",
            format!("fixture:{fixture}"),
            "1",
            json!({ "fixture": fixture }),
        )?;
        let config = LocalSwarmConfig::new(model.clone(), Limits::default())?;
        let provider = Arc::new(EchoModel {
            fixture: fixture.clone(),
            calls: Arc::new(AtomicUsize::new(0)),
        });
        let swarm = if fixture == "recursive" {
            let (host, stream, project) = recursive_project(&args.root).await?;
            let resolver = Arc::new(
                LocalFilesystemForkResolver::new(
                    host,
                    stream,
                    ProviderRef::new("local", "stream", "2")?,
                    project,
                )?
                .with_host_secret([0x5a; 32])?,
            );
            PersistentLocalSwarm::open_shared_with_model_and_bindings(
                &args.root,
                model,
                provider,
                Limits::default(),
                LocalSwarmBindings::default().with_filesystem_fork_resolver(resolver),
            )
            .await?
        } else {
            Arc::new(PersistentLocalSwarm::open(&args.root, config, provider).await?)
        };
        Ok(Self {
            swarm,
            model_fixture: fixture,
            approval_authorizer,
        })
    }

    async fn dispatch(&self, request: WireRequest) -> WireResponse {
        if request.request_id.is_empty() || request.request_id.len() > 256 {
            return WireResponse::error(
                &request.request_id,
                "invalid_input",
                "request_id must be between 1 and 256 bytes",
            );
        }
        let result = match request.method.as_str() {
            "list_sessions" => self.list_sessions(&request.params).await,
            "start_session" => self.start_session(&request.params).await,
            "open_session" => self.open_session(&request.params, false).await,
            "resume_session" => self.open_session(&request.params, true).await,
            "read_activity" => self.read_activity(&request.params).await,
            "read_messages" => self.read_messages(&request.params).await,
            "send_message" => {
                self.send_message(&request.request_id, &request.params)
                    .await
            }
            "list_approvals" => self.list_approvals(&request.params).await,
            "resolve_approval" => self.resolve_approval(&request.params).await,
            "cancel_session" => self.cancel_session(&request.params).await,
            "read_file" => self.read_file(&request.params).await,
            "list_changes" | "read_change" | "approve_writeback" => {
                Err(DispatchError::unsupported(
                    "the durable local constructor does not expose this projection yet",
                ))
            }
            _ => Err(DispatchError::invalid("unknown GraphCoder method")),
        };
        match result {
            Ok(result) => WireResponse::ok(&request.request_id, result),
            Err(error) => WireResponse::error(&request.request_id, error.code, error.message),
        }
    }

    async fn list_sessions(&self, params: &Value) -> Result<Value, DispatchError> {
        let (after, limit) = page_bounds(params)?;
        let mut summaries = self
            .swarm
            .sessions()
            .await
            .into_iter()
            .map(session_summary)
            .collect::<Vec<_>>();
        summaries.sort_by(|left, right| {
            left.get("id")
                .and_then(Value::as_str)
                .cmp(&right.get("id").and_then(Value::as_str))
        });
        let start = match after {
            Some(cursor) => summaries
                .iter()
                .position(|item| item.get("id").and_then(Value::as_str) == Some(cursor))
                .map(|index| index + 1)
                .ok_or_else(|| DispatchError::invalid("page cursor does not identify a session"))?,
            None => 0,
        };
        let end = (start + limit).min(summaries.len());
        let items = summaries[start..end].to_vec();
        let next = (end < summaries.len())
            .then(|| {
                items
                    .last()
                    .and_then(|item| item.get("id"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .flatten();
        let mut result = json!({ "items": items });
        if let Some(next) = next {
            result["next"] = Value::String(next);
        }
        Ok(result)
    }

    async fn start_session(&self, params: &Value) -> Result<Value, DispatchError> {
        let params = object(params)?;
        let prompt = required_text(params, "prompt")?;
        let operation_id = required_text(params, "operation_id")?;
        if operation_id.trim().is_empty() || operation_id.len() > 256 {
            return Err(DispatchError::invalid(
                "operation_id must be nonempty and at most 256 bytes",
            ));
        }
        if prompt.trim().is_empty() || prompt.len() > 64 * 1024 {
            return Err(DispatchError::invalid(
                "prompt must be nonempty and at most 64 KiB",
            ));
        }
        if let Some(fixture) = params.get("model_fixture") {
            if fixture.as_str() != Some(self.model_fixture.as_str()) {
                return Err(DispatchError::invalid(
                    "requested model fixture is not configured",
                ));
            }
        }
        let operation = operation_for(operation_id);
        let output = self
            .swarm
            .run_root(operation, prompt)
            .await
            .map_err(DispatchError::from_harness)?;
        let task = self
            .swarm
            .root_task()
            .await
            .map_err(DispatchError::from_harness)?;
        let mut snapshot = self.snapshot(task).await?;
        snapshot["outcome"] = serde_json::to_value(output).map_err(|error| {
            DispatchError::invalid(format!("outcome is not serializable: {error}"))
        })?;
        Ok(snapshot)
    }

    async fn open_session(&self, params: &Value, resume: bool) -> Result<Value, DispatchError> {
        let params = object(params)?;
        let task = task_from_value(params, "session_id")?;
        let session = if resume {
            self.swarm.resume(task).await
        } else {
            self.swarm.session(task).await
        }
        .map_err(DispatchError::from_harness)?;
        self.snapshot_from_session(session).await
    }

    async fn read_activity(&self, params: &Value) -> Result<Value, DispatchError> {
        let params = object(params)?;
        let task = task_from_value(params, "session_id")?;
        let (after, limit) = page_bounds_object(params)?;
        let after_revision = parse_cursor(after, "activity cursor")?;
        let events = self
            .swarm
            .read_activity(task, after_revision, limit)
            .await
            .map_err(DispatchError::from_harness)?;
        let next = (events.len() == limit)
            .then(|| events.last().map(|event| event.revision.to_string()))
            .flatten();
        let items = events.into_iter().map(activity_event).collect::<Vec<_>>();
        Ok(page_result(task, items, next))
    }

    async fn read_messages(&self, params: &Value) -> Result<Value, DispatchError> {
        let params = object(params)?;
        let task = task_from_value(params, "session_id")?;
        let (after, limit) = page_bounds_object(params)?;
        let include_body = message_body_requested(params)?;
        let after_sequence = parse_cursor(after, "message cursor")?;
        let messages = self
            .swarm
            .read_inbox(task, after_sequence, limit)
            .await
            .map_err(DispatchError::from_harness)?;
        let generation = if include_body {
            Some(
                self.swarm
                    .list_files(task, "", None, None, 1)
                    .await
                    .map_err(DispatchError::from_harness)?
                    .generation,
            )
        } else {
            None
        };
        let mut items = Vec::with_capacity(messages.len());
        for message in messages {
            let content = serde_json::to_value(&message.payload).map_err(|error| {
                DispatchError::invalid(format!(
                    "message content reference is not serializable: {error}"
                ))
            })?;
            let mut projected = json!({
                "id": message.message_id,
                "sequence": message.sequence.to_string(),
                "session_id": task.to_string(),
                "sender_id": message.sender.to_string(),
                "recipient_id": task.to_string(),
                "content": content,
                "delivered_at": message.delivered_at_epoch_ms.to_string(),
            });
            if let Some(generation) = generation.as_ref() {
                let body = self
                    .swarm
                    .read_file(task, message.payload.path(), Some(generation))
                    .await
                    .map_err(DispatchError::from_harness)
                    .and_then(|(_, bytes)| {
                        String::from_utf8(bytes).map_err(|_| DispatchError {
                            code: "transport",
                            message: "message content is not UTF-8".into(),
                        })
                    })?;
                projected["body"] = Value::String(body);
            }
            items.push(projected);
        }
        let next = (items.len() == limit)
            .then(|| messages_last_sequence(&items))
            .flatten();
        Ok(page_result(task, items, next))
    }

    async fn send_message(&self, request_id: &str, params: &Value) -> Result<Value, DispatchError> {
        let params = object(params)?;
        let session = task_from_value(params, "session_id")?;
        let sender = task_from_value(params, "sender_id")?;
        let recipient = task_from_value(params, "recipient_id")?;
        let body = required_text(params, "body")?;
        if body.is_empty() || body.len() > 64 * 1024 {
            return Err(DispatchError::invalid(
                "message body must be between 1 and 64 KiB",
            ));
        }
        if sender != session && recipient != session {
            return Err(DispatchError::invalid(
                "message participants must include the requested session",
            ));
        }
        let message_id = operation_for(request_id);
        let message = self
            .swarm
            .send_message(sender, recipient, message_id, body.as_bytes())
            .await
            .map_err(DispatchError::from_harness)?;
        Ok(json!({
            "id": message.message_id.to_string(),
            "session_id": session.to_string(),
            "sender_id": message.sender.to_string(),
            "recipient_id": message.recipient.to_string(),
            "body": body,
            "delivered_at": Value::Null,
        }))
    }

    async fn list_approvals(&self, params: &Value) -> Result<Value, DispatchError> {
        let params = object(params)?;
        let task = task_from_value(params, "session_id")?;
        let (after, limit) = page_bounds_object(params)?;
        let approvals = self
            .swarm
            .list_approvals(task)
            .await
            .map_err(DispatchError::from_harness)?;
        let start = after
            .map(|cursor| {
                approvals
                    .iter()
                    .position(|approval| approval.ticket.id.to_string() == cursor)
                    .map(|index| index + 1)
                    .ok_or_else(|| DispatchError::invalid("approval cursor is unknown"))
            })
            .transpose()?
            .unwrap_or(0);
        let selected = approvals
            .into_iter()
            .skip(start)
            .take(limit)
            .collect::<Vec<_>>();
        let next = (selected.len() == limit)
            .then(|| {
                selected
                    .last()
                    .map(|approval| approval.ticket.id.to_string())
            })
            .flatten();
        let items = selected.into_iter().map(approval_value).collect::<Vec<_>>();
        Ok(page_result(task, items, next))
    }

    async fn resolve_approval(&self, params: &Value) -> Result<Value, DispatchError> {
        let params = object(params)?;
        let task = task_from_value(params, "session_id")?;
        let id = InteractionId::parse(required_text(params, "approval_id")?)
            .map_err(DispatchError::from_harness)?;
        let approved = params
            .get("approved")
            .and_then(Value::as_bool)
            .ok_or_else(|| DispatchError::invalid("approved must be boolean"))?;
        let responder = (self.approval_authorizer)(task, id)
            .await
            .map_err(DispatchError::from_harness)?;
        self.swarm
            .resolve_approval(
                task,
                id,
                InteractionResponse::Approval {
                    approved,
                    reason: None,
                },
                &responder,
            )
            .await
            .map_err(DispatchError::from_harness)?;
        let approval = self
            .swarm
            .list_approvals(task)
            .await
            .map_err(DispatchError::from_harness)?
            .into_iter()
            .find(|approval| approval.ticket.id.as_bytes() == &id.into_bytes())
            .ok_or_else(|| {
                DispatchError::from_harness(HarnessError::NotFound("approval".into()))
            })?;
        Ok(approval_value(approval))
    }

    async fn cancel_session(&self, params: &Value) -> Result<Value, DispatchError> {
        let params = object(params)?;
        let task = task_from_value(params, "session_id")?;
        let session = self
            .swarm
            .cancel(task)
            .await
            .map_err(DispatchError::from_harness)?;
        self.snapshot_from_session(session).await
    }

    async fn read_file(&self, params: &Value) -> Result<Value, DispatchError> {
        let params = object(params)?;
        let task = task_from_value(params, "session_id")?;
        let path = required_text(params, "path")?;
        let requested_generation = required_text(params, "generation")?;
        let page = self
            .swarm
            .list_files(task, "", None, None, 1)
            .await
            .map_err(DispatchError::from_harness)?;
        let generation = generation_token(&page.generation);
        if generation != requested_generation {
            return Err(DispatchError {
                code: "stale",
                message: "file generation does not match the current workspace generation".into(),
            });
        }
        let (file, bytes) = self
            .swarm
            .read_file(task, path, Some(&page.generation))
            .await
            .map_err(DispatchError::from_harness)?;
        Ok(json!({
            "session_id": task.to_string(),
            "path": path,
            "media_type": file.descriptor().media_type(),
            "bytes": bytes,
            "generation": generation,
        }))
    }

    async fn snapshot(&self, task: TaskId) -> Result<Value, DispatchError> {
        let session = self
            .swarm
            .session(task)
            .await
            .map_err(DispatchError::from_harness)?;
        self.snapshot_from_session(session).await
    }

    async fn snapshot_from_session(
        &self,
        session: acyclic_harness::filesystem::LocalSwarmSession,
    ) -> Result<Value, DispatchError> {
        let sessions = self.swarm.sessions().await;
        let agents = sessions
            .iter()
            .map(|item| {
                json!({
                    "id": item.task.to_string(),
                    "parent_id": item.parent.map(|id| id.to_string()),
                    "task": item.task_description.clone(),
                    "state": agent_state(&item.phase),
                    "depth": item.depth,
                    "children": sessions.iter().filter(|child| child.parent == Some(item.task)).map(|child| child.task.to_string()).collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "summary": session_summary(session.clone()),
            "agents": agents,
            "workspace_generation": self
                .swarm
                .session_snapshot(session.task)
                .await
                .map_err(DispatchError::from_harness)?
                .workspace_generation
                .as_ref()
                .map(generation_token)
                .unwrap_or_else(|| "0".to_owned()),
        }))
    }
}

#[derive(Debug)]
struct DispatchError {
    code: &'static str,
    message: String,
}

impl DispatchError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: "invalid_input",
            message: message.into(),
        }
    }

    fn unsupported(message: impl Into<String>) -> Self {
        Self {
            code: "unsupported",
            message: message.into(),
        }
    }

    fn from_harness(error: HarnessError) -> Self {
        let code = match error {
            HarnessError::NotFound(_) => "not_found",
            HarnessError::Unauthorized(_) => "denied",
            HarnessError::Conflict(_) => "stale",
            HarnessError::Invalid(_) => "invalid_input",
            _ => "transport",
        };
        Self {
            code,
            message: error.to_string(),
        }
    }
}

fn object(value: &Value) -> Result<&serde_json::Map<String, Value>, DispatchError> {
    value
        .as_object()
        .ok_or_else(|| DispatchError::invalid("params must be an object"))
}

fn required_text<'a>(
    object: &'a serde_json::Map<String, Value>,
    key: &str,
) -> Result<&'a str, DispatchError> {
    object
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| DispatchError::invalid(format!("{key} must be text")))
}

fn task_from_value(
    object: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<TaskId, DispatchError> {
    TaskId::parse(required_text(object, key)?).map_err(DispatchError::from_harness)
}

fn page_bounds(params: &Value) -> Result<(Option<&str>, usize), DispatchError> {
    page_bounds_object(object(params)?)
}

fn page_bounds_object(object: &serde_json::Map<String, Value>) -> Result<(Option<&str>, usize), DispatchError> {
    let Some(query) = object.get("query") else {
        return Ok((None, 1024));
    };
    let query = query
        .as_object()
        .ok_or_else(|| DispatchError::invalid("query must be an object"))?;
    let after = if let Some(after) = query.get("after") {
        let Some(after) = after.as_str() else {
            return Err(DispatchError::invalid("page cursor must be nonempty text"));
        };
        if after.is_empty() {
            return Err(DispatchError::invalid("page cursor must be nonempty text"));
        }
        Some(after)
    } else {
        None
    };
    let limit = query.get("limit").map_or(Ok(1024), |value| {
        value
            .as_u64()
            .filter(|value| (1..=1024).contains(value))
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| DispatchError::invalid("page limit must be between 1 and 1024"))
    })?;
    Ok((after, limit))
}

fn message_body_requested(
    object: &serde_json::Map<String, Value>,
) -> Result<bool, DispatchError> {
    let Some(query) = object.get("query") else {
        return Ok(false);
    };
    let query = query
        .as_object()
        .ok_or_else(|| DispatchError::invalid("query must be an object"))?;
    query.get("include_body").map_or(Ok(false), |value| {
        value
            .as_bool()
            .ok_or_else(|| DispatchError::invalid("include_body must be a boolean"))
    })
}

fn parse_cursor(value: Option<&str>, label: &str) -> Result<u64, DispatchError> {
    value
        .unwrap_or("0")
        .parse::<u64>()
        .map_err(|_| DispatchError::invalid(format!("{label} must be an unsigned decimal")))
}

fn generation_token<T: Serialize>(generation: &T) -> String {
    let bytes = serde_json::to_vec(generation).unwrap_or_default();
    let digest = blake3::hash(&bytes);
    let mut token = [0_u8; 16];
    token.copy_from_slice(&digest.as_bytes()[..16]);
    u128::from_le_bytes(token).to_string()
}

fn page_result(task: TaskId, items: Vec<Value>, next: Option<String>) -> Value {
    let mut result = json!({
        "session_id": task.to_string(),
        "items": items,
    });
    if let Some(next) = next {
        result["next"] = Value::String(next);
    }
    result
}

fn messages_last_sequence(items: &[Value]) -> Option<String> {
    items
        .last()
        .and_then(|item| item.get("sequence"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn activity_event(event: acyclic_harness::core::Event) -> Value {
    let kind = match &event.payload {
        acyclic_harness::core::EventPayload::ConversationMessageAppended { .. } => "message",
        acyclic_harness::core::EventPayload::InteractionOpened { .. }
        | acyclic_harness::core::EventPayload::InteractionResolved { .. } => "approval",
        acyclic_harness::core::EventPayload::ForkPublished { .. } => "agent",
        acyclic_harness::core::EventPayload::ProjectMergePublished { .. } => "workspace",
        _ => "model",
    };
    json!({
        "sequence": event.revision.to_string(),
        "id": event.operation_id.to_string(),
        "kind": kind,
        "actor_id": Value::Null,
        "text": serde_json::to_string(&event.payload).unwrap_or_else(|_| "{}".to_owned()),
        "at": "0",
    })
}

fn approval_value(approval: acyclic_harness::filesystem::LocalSwarmApproval) -> Value {
    let (operation_id, action_digest) = approval
        .ticket
        .approval
        .as_ref()
        .map(|binding| {
            (
                binding.operation_id.to_string(),
                binding
                    .action_digest
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>(),
            )
        })
        .unwrap_or_else(|| ("".to_owned(), "".to_owned()));
    let state = approval
        .resolution
        .as_ref()
        .map(|resolution| match &resolution.outcome {
            acyclic_harness::interaction::InteractionOutcome::Approved => "approved",
            acyclic_harness::interaction::InteractionOutcome::Declined => "declined",
            acyclic_harness::interaction::InteractionOutcome::Cancelled => "cancelled",
            acyclic_harness::interaction::InteractionOutcome::Expired => "expired",
            acyclic_harness::interaction::InteractionOutcome::Denied => "denied",
            acyclic_harness::interaction::InteractionOutcome::Answered { .. }
            | acyclic_harness::interaction::InteractionOutcome::Indeterminate { .. } => "pending",
        })
        .unwrap_or("pending");
    json!({
        "id": approval.ticket.id.to_string(),
        "session_id": approval.task.to_string(),
        "agent_id": approval.task.to_string(),
        "operation_id": operation_id,
        "action_digest": action_digest,
        "description": "approval",
        "state": state,
        "created_at": "0",
    })
}

fn operation_for(request_id: &str) -> OperationId {
    let digest = blake3::hash(request_id.as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    if bytes == [0; 16] {
        bytes[0] = 1;
    }
    OperationId::from_bytes(bytes)
}

fn session_summary(session: acyclic_harness::filesystem::LocalSwarmSession) -> Value {
    let acyclic_harness::filesystem::LocalSwarmSession {
        task,
        task_description,
        phase,
        ..
    } = session;
    json!({
        "id": task.to_string(),
        "title": task_description,
        "state": session_state(&phase),
        "updated_at": "0",
        "root_agent_id": task.to_string(),
    })
}

fn session_state(phase: &LocalSessionPhase) -> &'static str {
    match phase {
        LocalSessionPhase::Ready => "idle",
        LocalSessionPhase::Activating => "running",
        LocalSessionPhase::Completed => "completed",
        LocalSessionPhase::Cancelled => "cancelled",
        LocalSessionPhase::Failed(_) => "failed",
    }
}

fn agent_state(phase: &LocalSessionPhase) -> &'static str {
    match phase {
        LocalSessionPhase::Ready => "queued",
        LocalSessionPhase::Activating => "running",
        LocalSessionPhase::Completed => "completed",
        LocalSessionPhase::Cancelled => "cancelled",
        LocalSessionPhase::Failed(_) => "failed",
    }
}

async fn serve<R, W>(runtime: Arc<Runtime>, input: R, output: W) -> std::io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let input = BufReader::new(input);
    let mut frames = BoundedFrames::new(input);
    let output = Arc::new(Mutex::new(tokio::io::BufWriter::new(output)));
    let mut jobs: futures::stream::FuturesUnordered<tokio::task::JoinHandle<std::io::Result<()>>> = futures::stream::FuturesUnordered::new();
    let mut control_jobs: futures::stream::FuturesUnordered<tokio::task::JoinHandle<std::io::Result<()>>> = futures::stream::FuturesUnordered::new();
    loop {
        while let Some(job) = jobs.next().now_or_never().flatten() {
            job.map_err(std::io::Error::other)??;
        }
        while let Some(job) = control_jobs.next().now_or_never().flatten() {
            job.map_err(std::io::Error::other)??;
        }
        let Some(frame) = frames.next().await? else {
            break;
        };
        let line = match frame {
            BoundedFrame::Line(line) => line,
            BoundedFrame::TooLong => {
                let response = WireResponse::error("", "invalid_input", "request line exceeds 16 MiB");
                let output = output.clone();
                write_direct_error(&output, response).await?;
                continue;
            }
            BoundedFrame::InvalidUtf8 => {
                let response = WireResponse::error("", "invalid_input", "request line is not valid UTF-8");
                let output = output.clone();
                write_direct_error(&output, response).await?;
                continue;
            }
        };
        let request = match serde_json::from_str::<WireRequest>(&line) {
            Ok(request) => request,
            Err(error) => {
                let response = WireResponse::error(
                    request_id_from_malformed_line(&line).as_deref().unwrap_or(""),
                    "invalid_input",
                    format!("invalid request: {error}"),
                );
                write_direct_error(&output, response).await?;
                continue;
            }
        };
        let is_control = request.method == "cancel_session";
        if is_control && control_jobs.len() >= MAX_CONTROL_IN_FLIGHT {
            write_direct_error(
                &output,
                WireResponse::error(&request.request_id, "invalid_input", "control request limit reached"),
            )
            .await?;
            continue;
        }
        if !is_control && jobs.len() >= MAX_IN_FLIGHT {
            write_direct_error(
                &output,
                WireResponse::error(&request.request_id, "invalid_input", "in-flight request limit reached"),
            )
            .await?;
            continue;
        }
        let runtime = runtime.clone();
        let output = output.clone();
        let target = if is_control { &mut control_jobs } else { &mut jobs };
        target.push(tokio::spawn(async move {
            let response = runtime.dispatch(request).await;
            let mut output = output.lock().await;
            write_response(&mut *output, &response).await
        }));
    }
    while let Some(job) = jobs.next().await {
        job.map_err(std::io::Error::other)??;
    }
    while let Some(job) = control_jobs.next().await {
        job.map_err(std::io::Error::other)??;
    }
    output.lock().await.flush().await
}

async fn write_direct_error<W>(
    output: &Arc<Mutex<tokio::io::BufWriter<W>>>,
    response: WireResponse,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut output = output.lock().await;
    write_response(&mut *output, &response).await
}

enum BoundedFrame {
    Line(String),
    TooLong,
    InvalidUtf8,
}

struct BoundedFrames<R> {
    input: R,
    bytes: Vec<u8>,
    discarding: bool,
}

impl<R: AsyncBufRead + Unpin> BoundedFrames<R> {
    fn new(input: R) -> Self {
        Self {
            input,
            bytes: Vec::new(),
            discarding: false,
        }
    }

    async fn next(&mut self) -> std::io::Result<Option<BoundedFrame>> {
        loop {
        let available = self.input.fill_buf().await?;
        if available.is_empty() {
            if self.discarding {
                self.discarding = false;
                return Ok(None);
            }
            if self.bytes.is_empty() {
                return Ok(None);
            }
            return Ok(Some(match String::from_utf8(std::mem::take(&mut self.bytes)) {
                Ok(line) => BoundedFrame::Line(line),
                Err(_) => BoundedFrame::InvalidUtf8,
            }));
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |index| index + 1);
        let content_len = newline.map_or(available.len(), |index| index);
        if self.discarding {
            self.input.consume(consumed);
            if newline.is_some() {
                self.discarding = false;
            }
            continue;
        }
        let remaining = MAX_LINE_BYTES.saturating_sub(self.bytes.len());
        if content_len > remaining {
            self.input.consume(consumed);
            self.bytes.clear();
            self.discarding = newline.is_none();
            return Ok(Some(BoundedFrame::TooLong));
        }
        self.bytes.extend_from_slice(&available[..content_len]);
        self.input.consume(consumed);
        if newline.is_some() {
            return Ok(Some(match String::from_utf8(std::mem::take(&mut self.bytes)) {
                Ok(line) => BoundedFrame::Line(line),
                Err(_) => BoundedFrame::InvalidUtf8,
            }));
        }
    }
}
}

fn request_id_from_malformed_line(line: &str) -> Option<String> {
    let value = serde_json::from_str::<Value>(line).ok()?;
    let request_id = value.get("request_id")?.as_str()?;
    if request_id.is_empty() || request_id.len() > 256 {
        return None;
    }
    Some(request_id.to_owned())
}

async fn write_response<W: AsyncWrite + Unpin>(
    output: &mut W,
    response: &WireResponse,
) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(response).map_err(std::io::Error::other)?;
    if bytes.len() > MAX_LINE_BYTES {
        let request_id = match response {
            WireResponse::Ok { request_id, .. } | WireResponse::Err { request_id, .. } => request_id,
        };
        let fallback = WireResponse::error(
            request_id,
            "transport",
            "response exceeds the configured size",
        );
        let fallback = serde_json::to_vec(&fallback).map_err(std::io::Error::other)?;
        output.write_all(&fallback).await?;
    } else {
        output.write_all(&bytes).await?;
    }
    output.write_all(b"\n").await?;
    // JSON-lines clients keep stdin open while they await each response.
    // Flush the complete envelope so interactive callers do not wait for EOF.
    output.flush().await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let runtime = Arc::new(Runtime::open(&args).await?);
    serve(runtime, tokio::io::stdin(), tokio::io::stdout()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bounded_frames_drain_oversized_input_before_next_request() {
        let (mut writer, reader) = tokio::io::duplex(MAX_LINE_BYTES + 64);
        let writer_task = tokio::spawn(async move {
            writer
                .write_all(&vec![b'x'; MAX_LINE_BYTES + 1])
                .await
                .expect("oversized frame writes");
            writer.write_all(b"\n{}\n").await.expect("next frame writes");
            writer.shutdown().await.expect("input closes");
        });
        let mut reader = BoundedFrames::new(BufReader::new(reader));
        assert!(matches!(
            reader.next().await.expect("frame reads"),
            Some(BoundedFrame::TooLong)
        ));
        assert!(matches!(
            reader.next().await.expect("frame reads"),
            Some(BoundedFrame::Line(line)) if line == "{}"
        ));
        writer_task.await.expect("writer task joins");
    }

    #[tokio::test]
    async fn bounded_frames_reject_unterminated_input_before_eof() {
        let (mut writer, reader) = tokio::io::duplex(MAX_LINE_BYTES + 1);
        writer
            .write_all(&vec![b'x'; MAX_LINE_BYTES + 1])
            .await
            .expect("unterminated frame writes");
        let mut reader = BoundedFrames::new(BufReader::new(reader));
        assert!(matches!(
            reader.next().await.expect("frame reads"),
            Some(BoundedFrame::TooLong)
        ));
        writer.shutdown().await.expect("input closes");
        assert!(reader.next().await.expect("frame reads").is_none());
    }

    async fn exchange(runtime: Arc<Runtime>, request: Value) -> Value {
        let (mut request_writer, request_reader) = tokio::io::duplex(64 * 1024);
        let (response_writer, mut response_reader) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(serve(runtime, request_reader, response_writer));
        let bytes = serde_json::to_vec(&request).expect("request serializes");
        request_writer
            .write_all(&bytes)
            .await
            .expect("request writes");
        request_writer
            .write_all(b"\n")
            .await
            .expect("request newline writes");
        request_writer.shutdown().await.expect("request closes");
        let mut response = Vec::new();
        response_reader
            .read_to_end(&mut response)
            .await
            .expect("response reads");
        server
            .await
            .expect("server joins")
            .expect("server succeeds");
        serde_json::from_slice(
            response
                .split(|byte| *byte == b'\n')
                .next()
                .expect("response line"),
        )
        .expect("response JSON")
    }

    async fn exchange_raw(runtime: Arc<Runtime>, request: Vec<u8>) -> Value {
        let (mut request_writer, request_reader) = tokio::io::duplex(64 * 1024);
        let (response_writer, mut response_reader) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(serve(runtime, request_reader, response_writer));
        request_writer
            .write_all(&request)
            .await
            .expect("request writes");
        request_writer.shutdown().await.expect("request closes");
        let mut response = Vec::new();
        response_reader
            .read_to_end(&mut response)
            .await
            .expect("response reads");
        server
            .await
            .expect("server joins")
            .expect("server succeeds");
        serde_json::from_slice(
            response
                .split(|byte| *byte == b'\n')
                .next()
                .expect("response line"),
        )
        .expect("response JSON")
    }

    fn runtime_args(root: PathBuf, fixture: &str) -> Args {
        Args {
            root,
            model_fixture: fixture.to_owned(),
        }
    }

    #[tokio::test]
    async fn json_lines_lists_lazily_then_runs_echo_fixture() {
        let root = tempfile::tempdir().expect("temporary root");
        let runtime = Arc::new(
            Runtime::open(&runtime_args(root.path().to_owned(), "echo"))
                .await
                .expect("runtime opens"),
        );
        let listed = exchange(
            runtime.clone(),
            json!({"request_id":"list-1","method":"list_sessions","params":{}}),
        )
        .await;
        assert_eq!(listed["ok"], true);
        assert_eq!(
            listed["result"]["items"]
                .as_array()
                .expect("session page")
                .len(),
            1
        );
        let started = exchange(
            runtime,
            json!({
                "request_id":"start-1",
                "method":"start_session",
                "params":{"prompt":"hello","operation_id":"op-echo-1","model_fixture":"echo"}
            }),
        )
        .await;
        assert_eq!(started["ok"], true);
        assert_eq!(started["result"]["summary"]["state"], "completed");
        assert!(started["result"]["workspace_generation"]
            .as_str()
            .is_some_and(|generation| !generation.is_empty()));
    }

    #[tokio::test]
    async fn json_lines_flushes_each_response_before_input_eof() {
        let root = tempfile::tempdir().expect("temporary root");
        let runtime = Arc::new(
            Runtime::open(&runtime_args(root.path().to_owned(), "echo"))
                .await
                .expect("runtime opens"),
        );
        let (mut request_writer, request_reader) = tokio::io::duplex(64 * 1024);
        let (response_writer, response_reader) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(serve(runtime, request_reader, response_writer));
        request_writer
            .write_all(
                serde_json::to_string(&json!({
                    "request_id": "interactive-1",
                    "method": "list_sessions",
                    "params": {}
                }))
                .expect("request serializes")
                .as_bytes(),
            )
            .await
            .expect("request writes");
        request_writer
            .write_all(b"\n")
            .await
            .expect("request newline writes");
        let mut response_reader = BufReader::new(response_reader);
        let mut line = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            response_reader.read_line(&mut line),
        )
        .await
        .expect("response arrives before input closes")
        .expect("response reads");
        let response: Value = serde_json::from_str(&line).expect("response JSON");
        assert_eq!(response["request_id"], "interactive-1");
        assert_eq!(response["ok"], true);
        request_writer.shutdown().await.expect("request closes");
        server
            .await
            .expect("server joins")
            .expect("server succeeds");
    }

    #[tokio::test]
    async fn json_lines_runs_stage_fixture_and_reads_staged_file_through_sdk() {
        let root = tempfile::tempdir().expect("temporary root");
        let runtime = Arc::new(
            Runtime::open(&runtime_args(root.path().to_owned(), "stage"))
                .await
                .expect("runtime opens"),
        );
        // Commit the durable operation, then drop the response as if the
        // connection failed after execution and before transport delivery.
        let _dropped_response = runtime
            .dispatch(WireRequest {
                request_id: "stage-1".into(),
                method: "start_session".into(),
                params: json!({
                    "prompt":"write fixture",
                    "operation_id":"op-stage-1",
                    "model_fixture":"stage"
                }),
            })
            .await;
        drop(runtime);
        let reopened = Arc::new(
            Runtime::open(&runtime_args(root.path().to_owned(), "stage"))
                .await
                .expect("runtime reopens"),
        );
        let retried = exchange(
            reopened.clone(),
            json!({
                "request_id":"stage-retry",
                "method":"start_session",
                "params":{"prompt":"write fixture","operation_id":"op-stage-1","model_fixture":"stage"}
            }),
        )
        .await;
        assert_eq!(retried["ok"], true, "{retried}");
        let session_id = retried["result"]["summary"]["id"]
            .as_str()
            .expect("stage session id")
            .to_owned();
        let generation = retried["result"]["workspace_generation"]
            .as_str()
            .expect("stage workspace generation")
            .to_owned();
        let file = exchange(
            reopened.clone(),
            json!({
                "request_id":"stage-file",
                "method":"read_file",
                "params":{
                    "session_id": session_id,
                    "path":"graphcoder-fixture.txt",
                    "generation": generation
                }
            }),
        )
        .await;
        assert_eq!(file["ok"], true, "{file}");
        assert_eq!(file["result"]["path"], "graphcoder-fixture.txt");
        assert_eq!(file["result"]["media_type"], "text/plain");
        assert_eq!(
            file["result"]["bytes"],
            json!(b"fixture:stage".as_slice())
        );
        let resumed = exchange(
            reopened.clone(),
            json!({
                "request_id":"stage-reopen",
                "method":"open_session",
                "params":{"session_id": session_id}
            }),
        )
        .await;
        assert_eq!(resumed["ok"], true);
        assert_eq!(resumed["result"]["summary"]["state"], "completed");
        let activity = exchange(
            reopened,
            json!({
                "request_id":"activity-1",
                "method":"read_activity",
                "params":{"session_id": session_id}
            }),
        )
        .await;
        assert_eq!(activity["ok"], true);
        assert!(activity["result"]["items"]
            .as_array()
            .is_some_and(|items| !items.is_empty()));
    }

    #[tokio::test]
    async fn malformed_fixture_and_wire_lines_fail_closed() {
        let root = tempfile::tempdir().expect("temporary root");
        let invalid_fixture = Runtime::open(&runtime_args(root.path().to_owned(), "unknown")).await;
        assert!(matches!(
            invalid_fixture,
            Err(HarnessError::Invalid(message)) if message.contains("unknown model fixture")
        ));

        let runtime = Arc::new(
            Runtime::open(&runtime_args(root.path().to_owned(), "echo"))
                .await
                .expect("runtime opens"),
        );
        let malformed = exchange_raw(runtime.clone(), b"not-json\n".to_vec()).await;
        assert_eq!(malformed["ok"], false);
        assert_eq!(malformed["error"]["code"], "invalid_input");

        let oversized = exchange_raw(
            runtime,
            format!("{}\n", "x".repeat(MAX_LINE_BYTES)).into_bytes(),
        )
        .await;
        assert_eq!(oversized["ok"], false);
        assert_eq!(oversized["error"]["code"], "invalid_input");

        let empty_operation_root = tempfile::tempdir().expect("temporary root");
        let runtime = Arc::new(
            Runtime::open(&runtime_args(empty_operation_root.path().to_owned(), "echo"))
                .await
                .expect("runtime opens"),
        );
        let empty_operation = runtime
            .dispatch(WireRequest {
                request_id: "empty-operation".into(),
                method: "start_session".into(),
                params: json!({"prompt":"hello", "operation_id":""}),
            })
            .await;
        assert!(matches!(empty_operation, WireResponse::Err { error: WireError { code: "invalid_input", .. }, .. }));
    }

    #[tokio::test]
    async fn approval_authorizer_receives_the_exact_pending_invocation() {
        let expected_task = TaskId::from_bytes([0x71; 16]);
        let expected_id = InteractionId::from_bytes([0x72; 16]);
        let seen = Arc::new(Mutex::new(None));
        let callback_seen = seen.clone();
        let authorizer: ApprovalAuthorizer = Arc::new(move |task, id| {
            let callback_seen = callback_seen.clone();
            async move {
                *callback_seen.lock().await = Some((task, id));
                Err(HarnessError::Unsupported("test host authorizer"))
            }
            .boxed()
        });

        let result = authorizer(expected_task, expected_id).await;
        assert!(matches!(result, Err(HarnessError::Unsupported(message)) if message == "test host authorizer"));
        assert_eq!(*seen.lock().await, Some((expected_task, expected_id)));
    }
}
