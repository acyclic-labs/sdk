//! `CodexExecutor`: one turn as one `codex exec --json` process (A1, A7).
//!
//! Journal shape, chosen to read like the stock loop's:
//! - `Started` first, over a digest of everything that determines the turn.
//! - `Model { step: 0, Completed { metadata: {codex: {thread_started}} } }` as
//!   soon as Codex names its thread, so a crashed turn resumes that thread.
//! - Per proxied model call `n`: `ModelStarted { n }`, then
//!   `Model { n, Completed { metadata: {usage} } }` when it completes.
//! - `agent_message` items: `Model { n, Content { delta } }`.
//! - Tool-like items: `ToolStarted` then `ToolCompleted` (or `ToolFailed`), with
//!   the item's id as the call id and its JSON as invocation and result.
//! - Last: `Model { n, Completed { metadata: {harness: "codex", …, turn} } }`.
//!   A journal ending in this record replays without starting Codex.

use crate::{
    CODEX_VERSION, EXECUTOR_ID,
    config::{HomeConfig, MCP_TOKEN_ENV, PROXY_KEY_ENV},
    events::{CodexEvent, CodexItem, ItemKind, ItemStatus, Transcript, parse_line},
    mcp::McpEndpoint,
    meter::{Unmetered, UsageMeter},
    process::{CodexProcess, Dirs, Launch, Next, check_version},
    proxy::{ProxyCall, ProxyStop, ResponsesProxy},
};
use acyclic_harness::{
    Error, OperationId, Result,
    conversation::FileRef,
    executor::{
        ExecutionEvent, ExecutionJournal, ExecutionRecord, Executor, TurnInput, TurnOutput,
    },
    model::{ModelContent, ModelContentPart, ModelEvent},
    runtime::{RuntimeScope, ToolPolicy},
    tool::ToolRegistry,
};
use futures::{FutureExt as _, future::BoxFuture};
use serde::Serialize;
use serde_json::{Value, json};
use std::{collections::BTreeSet, ffi::OsString, path::PathBuf, sync::Arc, time::Instant};
use tokio::sync::mpsc;

/// Where the proxy forwards model calls.
#[derive(Clone, Debug, PartialEq)]
pub struct Upstream {
    /// Responses API base, e.g. `https://api.openai.com/v1`.
    pub base_url: String,
    /// The real key. Only the proxy holds it; Codex gets a dummy.
    pub api_key: String,
    /// Fields merged into every request body (e.g. `reasoning`).
    pub extra_body: Value,
}

/// Everything one Codex turn needs besides tools.
#[derive(Clone, Debug)]
pub struct CodexConfig {
    /// The `codex` binary; must report [`crate::CODEX_VERSION`].
    pub binary: PathBuf,
    /// Upstream model id, e.g. `gpt-5.5`.
    pub model: String,
    /// Where model calls go.
    pub upstream: Upstream,
    /// The directory Codex works in (`-C`).
    pub workspace: PathBuf,
    /// The role prompt, written to `CODEX_HOME/AGENTS.md`.
    pub instructions: String,
    /// Whether Codex may use its own subagents.
    pub subagents: bool,
    /// Cap on proxied model calls for the turn.
    pub max_steps: u32,
    /// Wall-clock limit; past it Codex is terminated.
    pub deadline: Option<Instant>,
    /// Durable directory for per-operation `CODEX_HOME`s. Codex keeps its
    /// session there, so a crashed turn can only resume if this survives.
    pub state_dir: PathBuf,
    /// Continue this Codex thread instead of starting one (follow-up turns).
    pub resume_thread: Option<String>,
}

/// Receives every Codex event as it arrives, for logs and trace spans.
pub trait CodexObserver: Send + Sync {
    /// Called once per parsed line, in order.
    fn event(&self, event: &CodexEvent);
}

/// What a crashed run is resumed with.
const CONTINUE_PROMPT: &str =
    "Your previous run of this task was interrupted. Continue the task from where you stopped.";

/// Runs a turn as one `codex exec` process.
pub struct CodexExecutor {
    config: CodexConfig,
    tools: ToolRegistry,
    scope: Option<RuntimeScope>,
    policy: Option<Arc<dyn ToolPolicy>>,
    meter: Arc<dyn UsageMeter>,
    observer: Option<Arc<dyn CodexObserver>>,
}

impl CodexExecutor {
    /// Creates an executor. No tools are offered until a scope grants them.
    #[must_use]
    pub fn new(config: CodexConfig, tools: ToolRegistry) -> Self {
        Self {
            config,
            tools,
            scope: None,
            policy: None,
            meter: Arc::new(Unmetered),
            observer: None,
        }
    }

    /// Exposes the tools `scope` grants, under `policy`, as the stock loop does.
    ///
    /// # Errors
    /// When the policy identity is invalid.
    pub fn with_tool_authority(
        mut self,
        scope: RuntimeScope,
        policy: Option<Arc<dyn ToolPolicy>>,
    ) -> Result<Self> {
        if let Some(policy) = &policy
            && policy.identity().digest == [0; 32]
        {
            return Err(Error::Invalid("tool policy identity has no digest".into()));
        }
        self.scope = Some(scope);
        self.policy = policy;
        Ok(self)
    }

    /// Meters and caps every model call.
    #[must_use]
    pub fn with_meter(mut self, meter: Arc<dyn UsageMeter>) -> Self {
        self.meter = meter;
        self
    }

    /// Streams Codex events to `observer`.
    #[must_use]
    pub fn with_observer(mut self, observer: Arc<dyn CodexObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// The configuration this executor runs with.
    #[must_use]
    pub const fn config(&self) -> &CodexConfig {
        &self.config
    }

    fn granted_definitions(&self) -> Result<Vec<Value>> {
        let Some(scope) = &self.scope else {
            return Ok(Vec::new());
        };
        self.tools
            .definitions()?
            .into_iter()
            .filter(|definition| {
                scope
                    .grants()
                    .contains(&format!("tool:call:{}", definition.name))
            })
            .map(|definition| to_json(&definition))
            .collect()
    }

    fn request_digest(&self, input: &TurnInput, max_steps: u32) -> Result<[u8; 32]> {
        let request = json!({
            "executor": EXECUTOR_ID,
            "codex_version": CODEX_VERSION,
            "model": self.config.model,
            "input": to_json(&input.input)?,
            "subagents": self.config.subagents,
            "max_steps": max_steps,
            "instructions": self.config.instructions,
            "resume_thread": self.config.resume_thread,
            "tools": self.granted_definitions()?,
            "policy": self.policy.as_ref().map(|policy| {
                let identity = policy.identity();
                json!({"name": identity.name, "version": identity.version, "digest": identity.digest})
            }),
        });
        Ok(*blake3::hash(&canonical(&request)?).as_bytes())
    }

    async fn run(&self, input: TurnInput, journal: &dyn ExecutionJournal) -> Result<TurnOutput> {
        let operation = input.operation_id;
        let max_steps = input.max_steps.min(self.config.max_steps);
        if max_steps == 0 {
            return Err(Error::Invalid(
                "a codex turn needs at least one step".into(),
            ));
        }
        let prompt = prompt_text(&input.input)?;
        let digest = self.request_digest(&input, max_steps)?;

        let records = journal.replay(operation).await?;
        let prior = Prior::read(journal, &records, digest).await?;
        if let Some(output) = prior.finished {
            return Ok(output);
        }
        if !prior.started {
            journal
                .append(
                    operation,
                    "codex:started".into(),
                    ExecutionEvent::Started {
                        request_digest: digest,
                    },
                )
                .await?;
        }
        check_version(&self.config.binary).await?;

        // Resume a crashed run's thread; otherwise a follow-up's, if given.
        let (resume, prompt) = match (&prior.thread, &self.config.resume_thread) {
            (Some(thread), _) => (Some(thread.clone()), CONTINUE_PROMPT.to_owned()),
            (None, Some(thread)) => (Some(thread.clone()), prompt),
            (None, None) => (None, prompt),
        };
        let mut session = self
            .launch(operation, max_steps, resume.as_deref(), prompt)
            .await?;
        let mut turn = TurnJournal {
            journal,
            operation,
            run: uuid::Uuid::new_v4().simple().to_string(),
            offset: prior.last_step,
            proxy: &session.proxy,
            calls: session.calls,
            thread: prior.thread.clone(),
            started_items: BTreeSet::new(),
        };
        let ended = self
            .drive(&mut session.process, session.deadline, &mut turn)
            .await?;
        conclude(&mut turn, ended).await
    }

    /// Starts the proxy, the MCP endpoint and Codex for one run.
    async fn launch(
        &self,
        operation: OperationId,
        max_steps: u32,
        resume: Option<&str>,
        prompt: String,
    ) -> Result<Session> {
        let dirs = Dirs::create(&self.config.state_dir, &operation.to_string())?;
        let (proxy, calls) = ResponsesProxy::start_reporting(
            self.config.upstream.clone(),
            self.meter.clone(),
            max_steps,
        )
        .await?;
        let mcp = match &self.scope {
            Some(scope) => Some(
                McpEndpoint::start(self.tools.clone(), scope.clone(), self.policy.clone()).await?,
            )
            .filter(|endpoint| !endpoint.is_empty()),
            None => None,
        };
        HomeConfig {
            model: self.config.model.clone(),
            proxy_base_url: proxy.base_url().to_owned(),
            mcp_url: mcp.as_ref().map(|endpoint| endpoint.url().to_owned()),
            tool_timeout_sec: self.config.deadline.map_or(600, |deadline| {
                deadline
                    .saturating_duration_since(Instant::now())
                    .as_secs()
                    .max(1)
            }),
            subagents: self.config.subagents,
            request_max_retries: 2,
            stream_max_retries: 2,
        }
        .write(&dirs.codex_home, &self.config.instructions)
        .map_err(|error| Error::Storage(format!("cannot write CODEX_HOME: {error}")))?;

        let mut args: Vec<OsString> = [
            "exec",
            "--json",
            "--strict-config",
            "--skip-git-repo-check",
            "-m",
        ]
        .iter()
        .map(OsString::from)
        .collect();
        args.push(self.config.model.clone().into());
        args.push("-C".into());
        args.push(self.config.workspace.clone().into());
        if let Some(thread) = resume {
            args.push("resume".into());
            args.push(thread.into());
        }
        args.push(prompt.into());
        let mut env = vec![
            ("HOME".to_owned(), dirs.home.display().to_string()),
            (
                "CODEX_HOME".to_owned(),
                dirs.codex_home.display().to_string(),
            ),
            (
                PROXY_KEY_ENV.to_owned(),
                uuid::Uuid::new_v4().simple().to_string(),
            ),
        ];
        if let Some(endpoint) = &mcp {
            env.push((MCP_TOKEN_ENV.to_owned(), endpoint.token().to_owned()));
        }
        let process = CodexProcess::spawn(Launch {
            binary: &self.config.binary,
            args,
            env,
            workspace: &self.config.workspace,
        })?;
        Ok(Session {
            proxy,
            calls,
            _mcp: mcp,
            process,
            deadline: self.config.deadline.map(tokio::time::Instant::from_std),
        })
    }

    /// Reads Codex's events to the end, journaling them as they arrive.
    async fn drive(
        &self,
        process: &mut CodexProcess,
        deadline: Option<tokio::time::Instant>,
        turn: &mut TurnJournal<'_>,
    ) -> Result<Ended> {
        let mut transcript = Transcript::default();
        let mut deadline_hit = false;
        loop {
            match process.next(deadline).await {
                Next::Line(line) => {
                    turn.drain_calls().await?;
                    let Ok(event) = parse_line(&line) else {
                        tracing::warn!(line, "codex wrote a line that is not an event");
                        continue;
                    };
                    if let Some(observer) = &self.observer {
                        observer.event(&event);
                    }
                    transcript.observe(&event);
                    turn.record(&event, &line).await?;
                }
                Next::Eof => break,
                Next::Deadline => {
                    deadline_hit = true;
                    break;
                }
            }
        }
        if deadline_hit || process.wait(deadline).await.is_none() {
            process.terminate().await;
            deadline_hit = true;
        }
        let status = process.wait(None).await;
        let stderr = process.stderr_tail().await;
        turn.drain_calls().await?;
        Ok(Ended {
            transcript,
            deadline_hit,
            status: status.map_or_else(|| "unknown status".to_owned(), |s| s.to_string()),
            stderr,
        })
    }
}

/// What one run holds while Codex works. The MCP endpoint lives as long as this.
struct Session {
    proxy: ResponsesProxy,
    calls: mpsc::UnboundedReceiver<ProxyCall>,
    _mcp: Option<McpEndpoint>,
    process: CodexProcess,
    deadline: Option<tokio::time::Instant>,
}

/// How a run ended.
struct Ended {
    transcript: Transcript,
    deadline_hit: bool,
    status: String,
    stderr: String,
}

/// Turns a finished run into the turn's output, or the error that explains it.
async fn conclude(turn: &mut TurnJournal<'_>, ended: Ended) -> Result<TurnOutput> {
    let Ended {
        transcript,
        deadline_hit,
        status,
        stderr,
    } = ended;
    if deadline_hit {
        return Err(Error::Conflict(
            "codex deadline reached; the process group was terminated".into(),
        ));
    }
    if let Some(stop) = turn.proxy.stopped()
        && transcript.usage.is_none()
    {
        return Err(stop_error(&stop));
    }
    if let Some(failure) = &transcript.failure {
        return Err(Error::Invalid(format!("codex turn failed: {failure}")));
    }
    let Some(usage) = transcript.usage else {
        if transcript.thread_id.is_none() {
            let detail = if stderr.is_empty() {
                "no output"
            } else {
                &stderr
            };
            return Err(Error::Invalid(format!(
                "codex exited ({status}) before starting a turn: {detail}"
            )));
        }
        // Killed mid-turn: the thread is journaled, so a retry resumes it.
        return Err(Error::Indeterminate(turn.operation));
    };

    let steps = turn.offset.saturating_add(turn.proxy.steps());
    let metadata = json!({
        "harness": "codex",
        "codex_version": CODEX_VERSION,
        "thread_id": transcript.thread_id,
        "usage": usage,
    });
    let output = TurnOutput {
        text: transcript.final_message.unwrap_or_default(),
        attachments: Vec::new(),
        metadata: metadata.clone(),
        steps,
    };
    let mut marker = metadata;
    if let Some(object) = marker.as_object_mut() {
        object.insert(
            "turn".into(),
            json!({"text": output.text, "steps": output.steps}),
        );
    }
    turn.model_event(
        steps,
        "completed",
        &ModelEvent::Completed { metadata: marker },
    )
    .await?;
    Ok(output)
}

impl Executor for CodexExecutor {
    fn execute<'a>(
        &'a self,
        input: TurnInput,
        journal: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, Result<TurnOutput>> {
        self.run(input, journal).boxed()
    }
}

fn stop_error(stop: &ProxyStop) -> Error {
    match stop {
        ProxyStop::Budget(reason) => {
            Error::Unauthorized(format!("codex turn stopped: budget exhausted ({reason})"))
        }
        ProxyStop::StepLimit(limit) => {
            Error::Conflict(format!("executor step limit reached ({limit} model calls)"))
        }
    }
}

fn prompt_text(content: &ModelContent) -> Result<String> {
    let text = match content {
        ModelContent::Text(text) => text.clone(),
        ModelContent::Part(part) => part_text(part).unwrap_or_default(),
        ModelContent::Parts(parts) => parts
            .iter()
            .filter_map(part_text)
            .collect::<Vec<_>>()
            .join("\n\n"),
    };
    if text.trim().is_empty() {
        return Err(Error::Invalid(
            "a codex turn takes text input; files are not passed to codex".into(),
        ));
    }
    Ok(text)
}

fn part_text(part: &ModelContentPart) -> Option<String> {
    match part {
        ModelContentPart::Text { text } => Some(text.clone()),
        _ => None,
    }
}

fn to_json<T: Serialize>(value: &T) -> Result<Value> {
    serde_json::to_value(value).map_err(|error| Error::Invalid(error.to_string()))
}

/// Canonical JSON: sorted keys (`serde_json` maps are ordered), no whitespace.
fn canonical<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(&to_json(value)?).map_err(|error| Error::Invalid(error.to_string()))
}

/// What an earlier execution of this operation left in the journal.
struct Prior {
    started: bool,
    thread: Option<String>,
    last_step: u32,
    finished: Option<TurnOutput>,
}

impl Prior {
    async fn read(
        journal: &dyn ExecutionJournal,
        records: &[ExecutionRecord],
        digest: [u8; 32],
    ) -> Result<Self> {
        let mut prior = Self {
            started: false,
            thread: None,
            last_step: 0,
            finished: None,
        };
        for record in records {
            match &record.event {
                ExecutionEvent::Started { request_digest } => {
                    if *request_digest != digest {
                        return Err(Error::Conflict(
                            "this operation already ran a different codex turn".into(),
                        ));
                    }
                    prior.started = true;
                }
                ExecutionEvent::Model { step, event } => {
                    prior.last_step = prior.last_step.max(*step);
                    let Ok(ModelEvent::Completed { metadata }) = load(journal, event).await else {
                        continue;
                    };
                    if let Some(thread) = metadata
                        .pointer("/codex/thread_started")
                        .and_then(Value::as_str)
                    {
                        prior.thread = Some(thread.to_owned());
                    }
                    if metadata.get("harness").and_then(Value::as_str) == Some("codex") {
                        let mut metadata = metadata.clone();
                        let turn = metadata
                            .as_object_mut()
                            .and_then(|object| object.remove("turn"))
                            .unwrap_or(Value::Null);
                        prior.finished = Some(TurnOutput {
                            text: turn
                                .get("text")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_owned(),
                            attachments: Vec::new(),
                            metadata,
                            steps: turn
                                .get("steps")
                                .and_then(Value::as_u64)
                                .and_then(|steps| u32::try_from(steps).ok())
                                .unwrap_or(0),
                        });
                    }
                }
                ExecutionEvent::ModelStarted { step, .. }
                | ExecutionEvent::ToolStarted { step, .. }
                | ExecutionEvent::ToolCompleted { step, .. }
                | ExecutionEvent::ToolFailed { step, .. } => {
                    prior.last_step = prior.last_step.max(*step);
                }
            }
        }
        if !prior.started && !records.is_empty() {
            return Err(Error::Conflict(
                "the journal for this operation was not started by the codex executor".into(),
            ));
        }
        Ok(prior)
    }
}

async fn load(journal: &dyn ExecutionJournal, reference: &FileRef) -> Result<ModelEvent> {
    let bytes = journal.load(reference).await?;
    serde_json::from_slice(&bytes).map_err(|error| Error::Storage(error.to_string()))
}

/// Writes one run's records.
struct TurnJournal<'a> {
    journal: &'a dyn ExecutionJournal,
    operation: OperationId,
    /// Distinguishes this run's keys from a crashed run's (item ids restart).
    run: String,
    /// Steps journaled by earlier runs of this operation.
    offset: u32,
    proxy: &'a ResponsesProxy,
    calls: mpsc::UnboundedReceiver<ProxyCall>,
    thread: Option<String>,
    started_items: BTreeSet<String>,
}

impl TurnJournal<'_> {
    fn step(&self) -> u32 {
        self.offset.saturating_add(self.proxy.steps())
    }

    async fn stage(&self, key: &str, value: &impl Serialize) -> Result<FileRef> {
        self.journal
            .stage(
                self.operation,
                format!("codex:{}:{key}", self.run),
                canonical(value)?,
                "application/json",
            )
            .await
    }

    async fn append(&self, key: &str, event: ExecutionEvent) -> Result<()> {
        self.journal
            .append(self.operation, format!("codex:{}:{key}", self.run), event)
            .await
    }

    async fn model_event(&self, step: u32, key: &str, event: &ModelEvent) -> Result<()> {
        let staged = self.stage(&format!("{key}:event"), event).await?;
        self.append(
            key,
            ExecutionEvent::Model {
                step,
                event: staged,
            },
        )
        .await
    }

    async fn drain_calls(&mut self) -> Result<()> {
        while let Ok(call) = self.calls.try_recv() {
            match call {
                ProxyCall::Started {
                    step,
                    request_digest,
                } => {
                    let step = self.offset.saturating_add(step);
                    self.append(
                        &format!("model:{step}:started"),
                        ExecutionEvent::ModelStarted {
                            step,
                            request_digest,
                        },
                    )
                    .await?;
                }
                ProxyCall::Completed { step, usage } => {
                    let step = self.offset.saturating_add(step);
                    self.model_event(
                        step,
                        &format!("model:{step}:usage"),
                        &ModelEvent::Completed {
                            metadata: json!({"usage": usage}),
                        },
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn record(&mut self, event: &CodexEvent, line: &str) -> Result<()> {
        let raw = || -> Value {
            serde_json::from_str::<Value>(line)
                .ok()
                .and_then(|value| value.get("item").cloned())
                .unwrap_or(Value::Null)
        };
        match event {
            CodexEvent::ThreadStarted { thread_id } => {
                if self.thread.as_deref() != Some(thread_id) {
                    self.thread = Some(thread_id.clone());
                    self.model_event(
                        0,
                        "thread",
                        &ModelEvent::Completed {
                            metadata: json!({"codex": {"thread_started": thread_id}}),
                        },
                    )
                    .await?;
                }
            }
            CodexEvent::ItemStarted(item) if is_tool(&item.kind) => {
                self.tool_started(item, &raw()).await?;
            }
            CodexEvent::ItemCompleted(item) => match &item.kind {
                ItemKind::AgentMessage { text } => {
                    let step = self.step();
                    self.model_event(
                        step,
                        &format!("item:{}:message", item.id),
                        &ModelEvent::Content {
                            delta: text.clone(),
                        },
                    )
                    .await?;
                }
                kind if is_tool(kind) => {
                    let raw = raw();
                    self.tool_started(item, &raw).await?;
                    let step = self.step();
                    let failed = matches!(
                        item_status(kind),
                        Some(ItemStatus::Failed | ItemStatus::Declined)
                    );
                    let event = if failed {
                        ExecutionEvent::ToolFailed {
                            step,
                            call_id: item.id.clone(),
                            reason: acyclic_harness::executor::ToolFailureKind::ExecutorRejected,
                        }
                    } else {
                        let result = self
                            .stage(&format!("item:{}:result", item.id), &raw)
                            .await?;
                        ExecutionEvent::ToolCompleted {
                            step,
                            call_id: item.id.clone(),
                            result: result.clone(),
                            projection: result,
                        }
                    };
                    self.append(&format!("item:{}:finished", item.id), event)
                        .await?;
                }
                _ => {}
            },
            _ => {}
        }
        Ok(())
    }

    async fn tool_started(&mut self, item: &CodexItem, raw: &Value) -> Result<()> {
        if !self.started_items.insert(item.id.clone()) {
            return Ok(());
        }
        let step = self.step();
        let invocation = self
            .stage(&format!("item:{}:invocation", item.id), raw)
            .await?;
        self.append(
            &format!("item:{}:started", item.id),
            ExecutionEvent::ToolStarted {
                step,
                call_id: item.id.clone(),
                invocation,
            },
        )
        .await
    }
}

const fn is_tool(kind: &ItemKind) -> bool {
    matches!(
        kind,
        ItemKind::CommandExecution { .. }
            | ItemKind::FileChange { .. }
            | ItemKind::McpToolCall { .. }
            | ItemKind::CollabToolCall { .. }
            | ItemKind::WebSearch { .. }
    )
}

const fn item_status(kind: &ItemKind) -> Option<ItemStatus> {
    match kind {
        ItemKind::CommandExecution { status, .. }
        | ItemKind::FileChange { status, .. }
        | ItemKind::McpToolCall { status, .. } => Some(*status),
        _ => None,
    }
}
