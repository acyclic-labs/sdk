//! Native JSON-lines host for the local GraphCoder composition.
//!
//! This binary deliberately contains only protocol decoding and composition.
//! Durable session state, model admission, workspace effects, and recovery are
//! owned by `PersistentLocalSwarm`. Methods whose durable projection is not
//! exposed by that constructor return a typed `unsupported` response rather
//! than maintaining a second store in the host.

#![deny(unsafe_code)]
#![cfg_attr(test, allow(clippy::expect_used, clippy::indexing_slicing))]

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
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;

const MAX_LINE_BYTES: usize = 16 * 1024 * 1024;

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
        request: ModelRequest,
    ) -> BoxStream<'a, acyclic_harness::Result<ModelEvent>> {
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
                Ok(ModelEvent::Content {
                    delta: "fixture:stage".to_owned(),
                }),
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
    swarm: PersistentLocalSwarm,
    model_fixture: String,
}

impl Runtime {
    async fn open(args: &Args) -> Result<Self, HarnessError> {
        let fixture = match args.model_fixture.as_str() {
            "echo" | "complete" | "stage" => args.model_fixture.clone(),
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
        let swarm = PersistentLocalSwarm::open(&args.root, config, provider).await?;
        Ok(Self {
            swarm,
            model_fixture: fixture,
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
            "start_session" => {
                self.start_session(&request.request_id, &request.params)
                    .await
            }
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

    async fn start_session(
        &self,
        request_id: &str,
        params: &Value,
    ) -> Result<Value, DispatchError> {
        let params = object(params)?;
        let prompt = required_text(params, "prompt")?;
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
        let operation = operation_for(request_id);
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
    let object = object(params)?;
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

async fn serve<R, W>(runtime: Arc<Runtime>, input: R, output: W) -> std::io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut lines = BufReader::new(input).lines();
    let output = Arc::new(Mutex::new(tokio::io::BufWriter::new(output)));
    let mut jobs = Vec::new();
    while let Some(line) = lines.next_line().await? {
        let runtime = runtime.clone();
        let output = output.clone();
        jobs.push(tokio::spawn(async move {
            let response = if line.len() > MAX_LINE_BYTES {
                WireResponse::error("", "invalid_input", "request line exceeds 16 MiB")
            } else {
                match serde_json::from_str::<WireRequest>(&line) {
                    Ok(request) => runtime.dispatch(request).await,
                    Err(error) => WireResponse::error(
                        request_id_from_malformed_line(&line)
                            .as_deref()
                            .unwrap_or(""),
                        "invalid_input",
                        format!("invalid request: {error}"),
                    ),
                }
            };
            let mut output = output.lock().await;
            write_response(&mut *output, &response).await
        }));
    }
    for job in jobs {
        job.await.map_err(std::io::Error::other)??;
    }
    output.lock().await.flush().await
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
    output.write_all(&bytes).await?;
    output.write_all(b"\n").await
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
                "params":{"prompt":"hello","model_fixture":"echo"}
            }),
        )
        .await;
        assert_eq!(started["ok"], true);
        assert_eq!(started["result"]["summary"]["state"], "completed");
        assert_eq!(started["result"]["workspace_generation"], "0");
    }

    #[tokio::test]
    async fn json_lines_runs_stage_fixture_and_keeps_unexposed_methods_typed() {
        let root = tempfile::tempdir().expect("temporary root");
        let runtime = Arc::new(
            Runtime::open(&runtime_args(root.path().to_owned(), "stage"))
                .await
                .expect("runtime opens"),
        );
        let started = exchange(
            runtime.clone(),
            json!({
                "request_id":"stage-1",
                "method":"start_session",
                "params":{"prompt":"write fixture","model_fixture":"stage"}
            }),
        )
        .await;
        assert_eq!(started["ok"], true);
        let attachment = &started["result"]["outcome"]["attachments"][0];
        assert_eq!(attachment["file"]["path"], "graphcoder-fixture.txt");
        assert_eq!(attachment["file"]["display_name"], "graphcoder-fixture.txt");
        assert_eq!(attachment["file"]["descriptor"]["media_type"], "text/plain");
        assert_eq!(attachment["file"]["descriptor"]["byte_length"], 13);
        assert_eq!(
            attachment["file"]["descriptor"]["sha256"],
            json!([
                0x6f, 0x18, 0x86, 0x95, 0x7c, 0xff, 0xd5, 0x20, 0xa3, 0x9e, 0x7f, 0x0c, 0x30, 0xd6,
                0xd3, 0xe9, 0x9c, 0x74, 0x9f, 0x30, 0x0b, 0x13, 0x1d, 0x75, 0x83, 0xda, 0x85, 0x43,
                0x77, 0xef, 0x8a, 0x4d
            ])
        );
        let session_id = started["result"]["summary"]["id"]
            .as_str()
            .expect("stage session id")
            .to_owned();
        drop(runtime);
        let reopened = Arc::new(
            Runtime::open(&runtime_args(root.path().to_owned(), "stage"))
                .await
                .expect("runtime reopens"),
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
        assert_eq!(activity["ok"], false);
        assert_eq!(activity["error"]["code"], "unsupported");
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
    }
}
