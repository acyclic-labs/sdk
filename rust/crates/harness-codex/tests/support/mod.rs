//! Shared fakes for the acceptance tests: a scripted Responses upstream, a fake
//! `codex` binary that replays recorded fixtures, an in-memory journal, and a
//! small tool registry.

#![allow(dead_code, reason = "each test binary uses a different subset")]
#![allow(clippy::expect_used, clippy::panic, reason = "test fakes")]

use acyclic_harness::{
    AgentId, Capabilities, Error, InteractionId, OperationId, Result,
    conversation::{FileDescriptor, FileRef, Limits, VolumeClass, VolumeOwner, VolumeRef},
    executor::{ExecutionEvent, ExecutionJournal, ExecutionRecord},
    interaction::{Interaction, InteractionOutcome},
    resources::ProviderRef,
    runtime::RuntimeScope,
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry,
        ToolResult,
    },
};
use acyclic_harness_codex::meter::{MeterVerdict, ResponsesUsage, UsageMeter};
use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, Method, StatusCode, Uri},
    response::Response,
};
use futures::{FutureExt as _, future::BoxFuture};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

/// The fixtures recorded from Codex 0.155.1.
pub fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/codex-0.155.1")
}

// ---------------------------------------------------------------- upstream

/// What the fake upstream answers one request with.
pub enum Reply {
    /// A 200 SSE stream of these events, `gap` apart.
    Sse { events: Vec<Value>, gap: Duration },
    /// A plain JSON error.
    Status(u16, Value),
}

/// One request the upstream received.
#[derive(Clone, Debug)]
pub struct Recorded {
    pub method: String,
    pub path: String,
    pub headers: HashMap<String, String>,
    pub body: Value,
}

type Responder = Arc<dyn Fn(usize, &Recorded) -> Reply + Send + Sync>;

/// A scripted `OpenAI` Responses server on 127.0.0.1.
pub struct FakeUpstream {
    pub url: String,
    requests: Arc<Mutex<Vec<Recorded>>>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeUpstream {
    /// Starts a server that answers request `n` (0-based) with `respond(n, request)`.
    pub async fn start(
        respond: impl Fn(usize, &Recorded) -> Reply + Send + Sync + 'static,
    ) -> Self {
        let respond: Responder = Arc::new(respond);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let app = axum::Router::new().fallback(
            move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
                let respond = respond.clone();
                let seen = seen.clone();
                async move {
                    let recorded = Recorded {
                        method: method.to_string(),
                        path: uri.path().to_owned(),
                        headers: headers
                            .iter()
                            .map(|(k, v)| {
                                (k.to_string(), v.to_str().unwrap_or_default().to_owned())
                            })
                            .collect(),
                        body: serde_json::from_slice(&body).unwrap_or(Value::Null),
                    };
                    let n = {
                        let mut all = seen.lock().expect("requests");
                        all.push(recorded.clone());
                        all.len() - 1
                    };
                    reply(respond(n, &recorded))
                }
            },
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self {
            url,
            requests,
            task,
        }
    }

    /// Always answers with `text` and the standard usage.
    pub async fn message(text: &'static str) -> Self {
        Self::start(move |n, _| sse(message_turn(n, text))).await
    }

    /// Every request received so far.
    pub fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().expect("requests").clone()
    }
}

impl Drop for FakeUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn reply(reply: Reply) -> Response {
    match reply {
        Reply::Status(code, body) => Response::builder()
            .status(StatusCode::from_u16(code).expect("status"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .expect("response"),
        Reply::Sse { events, gap } => {
            let frames = futures::stream::iter(events.into_iter().enumerate()).then(
                move |(i, event)| async move {
                    if i > 0 && !gap.is_zero() {
                        tokio::time::sleep(gap).await;
                    }
                    let kind = event["type"].as_str().unwrap_or_default().to_owned();
                    Ok::<_, std::convert::Infallible>(format!("event: {kind}\ndata: {event}\n\n"))
                },
            );
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(frames))
                .expect("response")
        }
    }
}

use futures::StreamExt as _;

/// A 200 SSE reply with no delay between frames.
pub fn sse(events: Vec<Value>) -> Reply {
    Reply::Sse {
        events,
        gap: Duration::ZERO,
    }
}

/// The usage every scripted response reports (matches the fixtures).
pub fn usage() -> Value {
    json!({
        "input_tokens": 100,
        "input_tokens_details": {"cached_tokens": 40},
        "output_tokens": 7,
        "output_tokens_details": {"reasoning_tokens": 3},
        "total_tokens": 107
    })
}

/// The usage above as the meter should record it.
pub const METERED: ResponsesUsage = ResponsesUsage {
    input_tokens: 100,
    cached_input_tokens: 40,
    output_tokens: 7,
};

fn turn(n: usize, item: &Value) -> Vec<Value> {
    let id = format!("resp_{n}");
    vec![
        json!({"type": "response.created", "response": {"id": id}}),
        json!({"type": "response.output_item.added", "output_index": 0, "item": item}),
        json!({"type": "response.output_item.done", "output_index": 0, "item": item}),
        json!({"type": "response.completed", "response": {"id": id, "status": "completed", "usage": usage()}}),
    ]
}

/// A response whose only output is an assistant message.
pub fn message_turn(n: usize, text: &str) -> Vec<Value> {
    turn(
        n,
        &json!({
            "type": "message", "id": format!("msg_{n}"), "role": "assistant", "status": "completed",
            "content": [{"type": "output_text", "text": text, "annotations": []}]
        }),
    )
}

/// A response whose only output is a function call.
pub fn function_call_turn(n: usize, name: &str, arguments: &Value) -> Vec<Value> {
    turn(
        n,
        &json!({
            "type": "function_call", "id": format!("fc_{n}"), "call_id": format!("call_{n}"),
            "name": name, "arguments": arguments.to_string(), "status": "completed"
        }),
    )
}

/// Whether a request already carries a tool result (the model's next step).
pub fn has_tool_output(request: &Recorded) -> bool {
    request.body["input"].as_array().is_some_and(|input| {
        input.iter().any(|item| {
            matches!(
                item["type"].as_str(),
                Some("function_call_output" | "custom_tool_call_output")
            )
        })
    })
}

// ------------------------------------------------------------------- meter

/// Records every usage; stops once `stop_after` calls have completed.
#[derive(Default)]
pub struct RecordingMeter {
    pub recorded: Mutex<Vec<ResponsesUsage>>,
    pub stop_after: Option<usize>,
}

impl RecordingMeter {
    pub fn stopping_after(calls: usize) -> Self {
        Self {
            stop_after: Some(calls),
            ..Self::default()
        }
    }

    pub fn recorded(&self) -> Vec<ResponsesUsage> {
        self.recorded.lock().expect("meter").clone()
    }

    fn verdict(&self) -> MeterVerdict {
        match self.stop_after {
            Some(limit) if self.recorded().len() >= limit => MeterVerdict::Stop {
                reason: "test budget exhausted".into(),
            },
            _ => MeterVerdict::Continue,
        }
    }
}

impl UsageMeter for RecordingMeter {
    fn admit(&self, _: &str) -> MeterVerdict {
        self.verdict()
    }

    fn record(&self, _: &str, usage: &ResponsesUsage) -> MeterVerdict {
        self.recorded.lock().expect("meter").push(*usage);
        self.verdict()
    }
}

// --------------------------------------------------------------- fake codex

/// How the fake `codex` binary behaves for one test.
#[derive(Clone, Debug, Default)]
pub struct FakeCodex {
    /// Fixture stem under `fixtures/codex-0.155.1` replayed to stdout.
    pub fixture: &'static str,
    /// Exit code after replaying.
    pub exit: i32,
    /// Seconds to sleep after `thread.started` (for deadline tests).
    pub sleep_secs: u32,
    /// A file to create in the `-C` workspace, as Codex editing would.
    pub touch: Option<&'static str>,
    /// Text written to stderr.
    pub stderr: Option<&'static str>,
}

impl FakeCodex {
    /// Writes the script into `dir` and returns its path. Every invocation
    /// appends its argv, the env vars the executor sets, and `CODEX_HOME`'s
    /// files to `dir/record/<n>/`.
    pub fn install(&self, dir: &Path) -> PathBuf {
        let fixtures = fixture_dir();
        let stdout = fixtures.join(format!("{}.stdout.jsonl", self.fixture));
        let record = dir.join("record");
        std::fs::create_dir_all(&record).expect("record dir");
        let touch = self.touch.unwrap_or("");
        let stderr = self.stderr.unwrap_or("");
        let script = format!(
            r#"#!/usr/bin/env bash
set -u
if [ "${{1:-}}" = "--version" ]; then echo "codex-cli 0.155.1"; exit 0; fi
# Codex blocks on an open stdin; the executor must give it a closed one.
# (Timed, not by exit code: bash 3.2 on macOS reports a read timeout as 1.)
t0=$SECONDS; IFS= read -r -t 3 _line
if [ $((SECONDS - t0)) -ge 2 ]; then echo "stdin was left open" >&2; exit 97; fi
n=$(ls {record} | wc -l | tr -d ' ')
out={record}/$n; mkdir -p "$out"
printf '%s\n' "$@" > "$out/argv"
printf 'CODEX_HOME=%s\nACYCLIC_CODEX_PROXY_KEY=%s\nACYCLIC_CODEX_MCP_TOKEN=%s\nOPENAI_API_KEY=%s\n' \
  "${{CODEX_HOME:-}}" "${{ACYCLIC_CODEX_PROXY_KEY:-}}" "${{ACYCLIC_CODEX_MCP_TOKEN:-}}" "${{OPENAI_API_KEY:-}}" > "$out/env"
[ -n "${{CODEX_HOME:-}}" ] && cp -R "$CODEX_HOME" "$out/home" 2>/dev/null
trap 'echo terminated > "$out/signal"; exit 143' TERM
ws=""; prev=""
for a in "$@"; do [ "$prev" = "-C" ] && ws="$a"; prev="$a"; done
[ -n "{touch}" ] && [ -n "$ws" ] && echo touched > "$ws/{touch}"
[ -n "{stderr}" ] && printf '%s\n' "{stderr}" >&2
head -n 1 {stdout}
if [ {sleep} -gt 0 ]; then sleep {sleep} & wait $!; fi
tail -n +2 {stdout}
exit {exit}
"#,
            record = record.display(),
            stdout = stdout.display(),
            sleep = self.sleep_secs,
            exit = self.exit,
        );
        let path = dir.join("codex");
        std::fs::write(&path, script).expect("write fake codex");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        }
        path
    }

    /// The recorded invocations, oldest first.
    pub fn invocations(dir: &Path) -> Vec<PathBuf> {
        let mut all: Vec<PathBuf> = std::fs::read_dir(dir.join("record"))
            .map(|entries| entries.filter_map(|e| e.ok().map(|e| e.path())).collect())
            .unwrap_or_default();
        all.sort_by_key(|path| {
            path.file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.parse::<u32>().ok())
        });
        all
    }
}

// ------------------------------------------------------------------- tools

/// Echoes its arguments; fails when `fail` is true.
struct Echo;

impl ToolExecutor for Echo {
    fn execute<'a>(&'a self, invocation: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        async move {
            if invocation.arguments["fail"].as_bool() == Some(true) {
                return Err(Error::Invalid("echo was asked to fail".into()));
            }
            Ok(ToolResult {
                value: json!({"echo": invocation.arguments["text"]}),
            })
        }
        .boxed()
    }

    fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        async { Ok(None) }.boxed()
    }
}

struct Identity;

impl ToolProjection for Identity {
    fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        Ok(result.value.clone())
    }
}

fn tool(name: &str) -> Tool {
    Tool {
        definition: ToolDefinition {
            name: name.into(),
            revision: "1".into(),
            description: format!("{name} for tests"),
            input_schema: json!({
                "type": "object",
                "properties": {"text": {"type": "string"}, "fail": {"type": "boolean"}},
                "required": ["text"],
                "additionalProperties": false
            }),
            output_schema: json!({"type": "object"}),
        },
        executor: Arc::new(Echo),
        projection: Arc::new(Identity),
    }
}

/// `acyclic.echo` (granted by [`scope`]) and `acyclic.secret` (registered, never granted).
pub fn registry() -> ToolRegistry {
    let mut registry = ToolRegistry::default();
    registry
        .register(tool("acyclic.echo"))
        .expect("register echo");
    registry
        .register(tool("acyclic.secret"))
        .expect("register secret");
    registry
}

/// Grants only `acyclic.echo`.
pub fn scope() -> RuntimeScope {
    RuntimeScope::new(
        Capabilities::new(["tool:call:acyclic.echo"]),
        Limits::default(),
    )
    .expect("scope")
}

// ----------------------------------------------------------------- journal

/// An in-memory journal with the same rules as the harness's test journal.
#[derive(Default)]
pub struct Journal {
    records: Mutex<Vec<ExecutionRecord>>,
    staged: Mutex<HashMap<String, (FileRef, Vec<u8>)>>,
}

impl Journal {
    /// The events recorded for `operation`, in order.
    pub fn events(&self, operation: OperationId) -> Vec<ExecutionEvent> {
        self.records
            .lock()
            .expect("journal")
            .iter()
            .filter(|record| record.operation_id == operation)
            .map(|record| record.event.clone())
            .collect()
    }

    /// Loads a staged file as JSON.
    pub fn json(&self, reference: &FileRef) -> Value {
        let staged = self.staged.lock().expect("journal");
        let bytes = staged
            .values()
            .find(|(stored, _)| stored == reference)
            .map(|(_, bytes)| bytes.clone())
            .expect("staged file");
        serde_json::from_slice(&bytes).expect("staged JSON")
    }
}

fn poisoned<T>(_: T) -> Error {
    Error::Storage("journal lock poisoned".into())
}

impl ExecutionJournal for Journal {
    fn replay<'a>(
        &'a self,
        operation_id: OperationId,
    ) -> BoxFuture<'a, Result<Vec<ExecutionRecord>>> {
        async move {
            Ok(self
                .records
                .lock()
                .map_err(poisoned)?
                .iter()
                .filter(|record| record.operation_id == operation_id)
                .cloned()
                .collect())
        }
        .boxed()
    }

    fn append<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: String,
        event: ExecutionEvent,
    ) -> BoxFuture<'a, Result<()>> {
        async move {
            let mut records = self.records.lock().map_err(poisoned)?;
            if let Some(existing) = records
                .iter()
                .find(|r| r.operation_id == operation_id && r.idempotency_key == idempotency_key)
            {
                return if existing.event == event {
                    Ok(())
                } else {
                    Err(Error::Conflict("journal retry identity reused".into()))
                };
            }
            let sequence = records
                .iter()
                .filter(|r| r.operation_id == operation_id)
                .count() as u64
                + 1;
            records.push(ExecutionRecord {
                operation_id,
                sequence,
                idempotency_key,
                event,
            });
            Ok(())
        }
        .boxed()
    }

    fn append_if_tail<'a>(
        &'a self,
        operation_id: OperationId,
        expected_tail: u64,
        idempotency_key: String,
        event: ExecutionEvent,
    ) -> BoxFuture<'a, Result<bool>> {
        async move {
            let mut records = self.records.lock().map_err(poisoned)?;
            let tail = records
                .iter()
                .filter(|r| r.operation_id == operation_id)
                .count() as u64;
            if tail != expected_tail {
                return Ok(false);
            }
            records.push(ExecutionRecord {
                operation_id,
                sequence: tail + 1,
                idempotency_key,
                event,
            });
            Ok(true)
        }
        .boxed()
    }

    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: String,
        bytes: Vec<u8>,
        media_type: &'static str,
    ) -> BoxFuture<'a, Result<FileRef>> {
        async move {
            let key = format!("{operation_id}:{idempotency_key}");
            let mut staged = self.staged.lock().map_err(poisoned)?;
            if let Some((reference, existing)) = staged.get(&key) {
                return if existing == &bytes {
                    Ok(reference.clone())
                } else {
                    Err(Error::Conflict("journal staging identity reused".into()))
                };
            }
            let volume = VolumeRef::new(
                ProviderRef::new("test", "filesystem", "2")?,
                "journal",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([0; 16])),
            )?;
            let n = staged.len();
            let reference = FileRef::new(
                volume,
                format!("journal/{n}.json"),
                n.to_string(),
                FileDescriptor::from_bytes(&bytes, media_type)?,
                "event.json",
            )?;
            staged.insert(key, (reference.clone(), bytes));
            Ok(reference)
        }
        .boxed()
    }

    fn load<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
        async move {
            self.staged
                .lock()
                .map_err(poisoned)?
                .values()
                .find(|(stored, _)| stored == reference)
                .map(|(_, bytes)| bytes.clone())
                .ok_or_else(|| Error::NotFound("journal content is missing".into()))
        }
        .boxed()
    }

    fn open_interaction<'a>(
        &'a self,
        _: InteractionId,
        _: Interaction,
    ) -> BoxFuture<'a, Result<()>> {
        async { Err(Error::Unsupported("interactions".into())) }.boxed()
    }

    fn interaction_outcome<'a>(
        &'a self,
        _: InteractionId,
    ) -> BoxFuture<'a, Result<Option<InteractionOutcome>>> {
        async { Ok(None) }.boxed()
    }
}
