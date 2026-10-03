//! Native JSON-lines host for the local GraphCoder composition.
//!
//! This binary deliberately contains only protocol decoding and composition.
//! Durable session state, model admission, workspace effects, and recovery are
//! owned by `PersistentLocalSwarm`. Methods whose durable projection is not
//! exposed by that constructor return a typed `unsupported` response rather
//! than maintaining a second store in the host.

#![deny(unsafe_code)]

use acyclic_harness::{
    Error as HarnessError, OperationId, TaskId,
    conversation::Limits,
    filesystem::{LocalSessionPhase, LocalSwarmConfig, PersistentLocalSwarm},
    model::{Model, ModelAttempt, ModelEvent, ModelProvider, ModelRequest},
};
use clap::Parser;
use futures::{FutureExt, StreamExt, future::BoxFuture, stream::BoxStream};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::{Arc, atomic::{AtomicUsize, Ordering}}};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};

const MAX_LINE_BYTES: usize = 16 * 1024 * 1024;

/// Runtime configuration. The model is intentionally a deterministic fixture:
/// production model adapters are selected by a future host package.
#[derive(Debug, Parser)]
#[command(name = "graphcoder-runtime", version, about = "Local GraphCoder JSON-lines host")]
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
    fn generate<'a>(&'a self, request: ModelRequest) -> BoxStream<'a, acyclic_harness::Result<ModelEvent>> {
        let call = self.calls.fetch_add(1, Ordering::Relaxed);
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
        if self.fixture == "stage" && call == 0 {
            return Box::pin(futures::stream::iter([
                Ok(ModelEvent::Content { delta: "fixture:stage".to_owned() }),
                Ok(ModelEvent::ToolCall {
                    call_id: "graphcoder-stage-1".to_owned(),
                    name: "acyclic.stage_file".to_owned(),
                    arguments: json!({
                        "path": "graphcoder-fixture.txt",
                        "text": request.messages.iter().rev().find_map(|message| match &message.content {
                            acyclic_harness::model::ModelContent::Text(value) => Some(value.clone()),
                            _ => None,
                        }).unwrap_or_else(|| "fixture:stage".to_owned()),
                        "media_type": "text/plain",
                        "display_name": "graphcoder-fixture.txt",
                    }),
                }),
                Ok(ModelEvent::Completed { metadata: Value::Null }),
            ]));
        }
        Box::pin(futures::stream::iter([
            Ok(ModelEvent::Content { delta: text }),
            Ok(ModelEvent::Completed { metadata: Value::Null }),
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
    swarm: PersistentLocalSwarm,
    model_fixture: String,
}

impl Runtime {
    async fn open(args: &Args) -> Result<Self, HarnessError> {
        let fixture = match args.model_fixture.as_str() {
            "echo" | "complete" | "stage" => args.model_fixture.clone(),
            value => return Err(HarnessError::Invalid(format!("unknown model fixture {value}"))),
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
        let swarm = PersistentLocalSwarm::open(&args.root, config, provider).await?;
        Ok(Self {
            swarm,
            model_fixture: fixture,
        })
    }

    async fn dispatch(&self, request: WireRequest) -> WireResponse {
        let result = match request.method.as_str() {
            "list_sessions" => self.list_sessions(&request.params).await,
            "start_session" => self.start_session(&request.request_id, &request.params).await,
            "open_session" => self.open_session(&request.params, false).await,
            "resume_session" => self.open_session(&request.params, true).await,
            "read_activity" | "read_messages" | "send_message" | "list_approvals"
            | "resolve_approval" | "cancel_session" | "list_changes" | "read_change"
            | "read_file" | "approve_writeback" => Err(DispatchError::unsupported(
                "the durable local constructor does not expose this projection yet",
            )),
            _ => Err(DispatchError::invalid("unknown GraphCoder method")),
        };
        match result {
            Ok(result) => WireResponse::ok(&request.request_id, result),
            Err(error) => WireResponse::error(&request.request_id, error.code, error.message),
        }
    }

    async fn list_sessions(&self, params: &Value) -> Result<Value, DispatchError> {
        validate_page_params(params)?;
        let items = self
            .swarm
            .sessions()
            .await
            .into_iter()
            .map(session_summary)
            .collect::<Vec<_>>();
        Ok(json!({ "items": items }))
    }

    async fn start_session(&self, request_id: &str, params: &Value) -> Result<Value, DispatchError> {
        let params = object(params)?;
        let prompt = required_text(params, "prompt")?;
        if prompt.trim().is_empty() || prompt.len() > 64 * 1024 {
            return Err(DispatchError::invalid("prompt must be nonempty and at most 64 KiB"));
        }
        if let Some(fixture) = params.get("model_fixture") {
            if fixture.as_str() != Some(self.model_fixture.as_str()) {
                return Err(DispatchError::invalid("requested model fixture is not configured"));
            }
        }
        let operation = operation_for(request_id);
        self.swarm
            .run_root(operation, prompt)
            .await
            .map_err(DispatchError::from_harness)?;
        let task = self.swarm.root_task().await.map_err(DispatchError::from_harness)?;
        self.snapshot(task).await
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

    async fn snapshot(&self, task: TaskId) -> Result<Value, DispatchError> {
        let session = self.swarm.session(task).await.map_err(DispatchError::from_harness)?;
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
            "summary": session_summary(session),
            "agents": agents,
            // Workspace generation is deliberately a stable zero until the
            // constructor exposes its Filesystem generation projection.
            "workspace_generation": "0",
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
        Self { code: "invalid_input", message: message.into() }
    }

    fn unsupported(message: impl Into<String>) -> Self {
        Self { code: "unsupported", message: message.into() }
    }

    fn from_harness(error: HarnessError) -> Self {
        let code = match error {
            HarnessError::NotFound(_) => "not_found",
            HarnessError::Unauthorized(_) => "denied",
            HarnessError::Conflict(_) => "stale",
            HarnessError::Invalid(_) => "invalid_input",
            _ => "transport",
        };
        Self { code, message: error.to_string() }
    }
}

fn object(value: &Value) -> Result<&serde_json::Map<String, Value>, DispatchError> {
    value.as_object().ok_or_else(|| DispatchError::invalid("params must be an object"))
}

fn required_text<'a>(object: &'a serde_json::Map<String, Value>, key: &str) -> Result<&'a str, DispatchError> {
    object.get(key).and_then(Value::as_str).ok_or_else(|| DispatchError::invalid(format!("{key} must be text")))
}

fn task_from_value(object: &serde_json::Map<String, Value>, key: &str) -> Result<TaskId, DispatchError> {
    TaskId::parse(required_text(object, key)?).map_err(DispatchError::from_harness)
}

fn validate_page_params(params: &Value) -> Result<(), DispatchError> {
    let object = object(params)?;
    let Some(query) = object.get("query") else { return Ok(()); };
    let query = query.as_object().ok_or_else(|| DispatchError::invalid("query must be an object"))?;
    if let Some(after) = query.get("after") {
        if after.as_str().is_none_or(str::is_empty) {
            return Err(DispatchError::invalid("page cursor must be nonempty text"));
        }
    }
    if let Some(limit) = query.get("limit") {
        let valid = limit.as_u64().is_some_and(|value| (1..=1024).contains(&value));
        if !valid { return Err(DispatchError::invalid("page limit must be between 1 and 1024")); }
    }
    Ok(())
}

fn operation_for(request_id: &str) -> OperationId {
    let digest = blake3::hash(request_id.as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    if bytes == [0; 16] { bytes[0] = 1; }
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
        LocalSessionPhase::Failed(_) => "failed",
    }
}

fn agent_state(phase: &LocalSessionPhase) -> &'static str {
    match phase {
        LocalSessionPhase::Ready => "queued",
        LocalSessionPhase::Activating => "running",
        LocalSessionPhase::Completed => "completed",
        LocalSessionPhase::Failed(_) => "failed",
    }
}

async fn serve<R, W>(runtime: &Runtime, input: R, output: W) -> std::io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut lines = BufReader::new(input).lines();
    let mut output = tokio::io::BufWriter::new(output);
    while let Some(line) = lines.next_line().await? {
        if line.len() > MAX_LINE_BYTES {
            let response = WireResponse::error("", "invalid_input", "request line exceeds 16 MiB");
            write_response(&mut output, &response).await?;
            continue;
        }
        let response = match serde_json::from_str::<WireRequest>(&line) {
            Ok(request) => runtime.dispatch(request).await,
            Err(error) => WireResponse::error("", "invalid_input", format!("invalid request: {error}")),
        };
        write_response(&mut output, &response).await?;
    }
    output.flush().await
}

async fn write_response<W: AsyncWrite + Unpin>(output: &mut W, response: &WireResponse) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(response).map_err(std::io::Error::other)?;
    output.write_all(&bytes).await?;
    output.write_all(b"\n").await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let runtime = Runtime::open(&args).await?;
    serve(&runtime, tokio::io::stdin(), tokio::io::stdout()).await?;
    Ok(())
}
