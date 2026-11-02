//! Headless conversations: one `-p` prompt, or JSONL requests on stdin.
//! Every prompt settles through the conversation's committed reply, read
//! after subscribing and again at each committed status, which every
//! conversation entry commits with; the frontend keeps only its outstanding
//! prompts.
use acyclic_harness::OperationId;
use acyclic_harness::agent::{ConversationEntry, IntentId, Phase, Reply, TurnOutcome};
use acyclic_harness::local::{
    Event,
    agent_conversation::{Conversation, Sent, ended},
};
use acyclic_native_runtime::{InheritedOutput, lines::AsyncLines};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};

type Failure = Box<dyn std::error::Error>;
use acyclic_harness::client::ExecutionPosition;

/// Each input names one action. Approval retries retain the original shown
/// binding instead of discovering a potentially different pending action.
#[derive(serde::Deserialize)]
#[serde(untagged, deny_unknown_fields)]
enum Request {
    Prompt {
        prompt: String,
        id: Option<String>,
    },
    Approval {
        approve: IntentId,
        binding: acyclic_harness::interaction::ApprovalBinding,
        approved: bool,
    },
    Cancel {
        cancel: bool,
    },
}

/// The original invocation, with a generated operation made explicit for replay.
#[derive(serde::Serialize)]
pub struct Recovery {
    shell: &'static str,
    command: String,
    stdin: &'static str,
}

impl Recovery {
    pub fn capture(operation: Option<&str>) -> Result<Self, std::fmt::Error> {
        let mut arguments = std::env::args_os();
        let mut command = String::new();
        #[cfg(windows)]
        command.push_str("$PSNativeCommandArgumentPassing = 'Standard'; & ");
        for argument in arguments.next().into_iter().chain(
            operation
                .into_iter()
                .flat_map(|operation| ["--operation".into(), operation.into()])
                .chain(arguments),
        ) {
            if !command.is_empty() && !command.ends_with(' ') {
                command.push(' ');
            }
            quote_argument(&mut command, &argument)?;
        }
        Ok(Self {
            shell: if cfg!(windows) { "pwsh" } else { "bash" },
            command,
            stdin: "replay the original JSONL input, if any",
        })
    }
}

#[cfg(unix)]
fn quote_argument(output: &mut String, argument: &std::ffi::OsStr) -> std::fmt::Result {
    use std::{fmt::Write as _, os::unix::ffi::OsStrExt as _};
    if let Some(argument) = argument.to_str() {
        output.push('\'');
        for character in argument.chars() {
            if character == '\'' {
                output.push_str("'\\''");
            } else {
                output.push(character);
            }
        }
        output.push('\'');
        return Ok(());
    }
    // Bash's byte escapes preserve non-UTF-8 paths and trailing newlines too.
    output.push_str("$'");
    for byte in argument.as_bytes() {
        write!(output, "\\x{byte:02x}")?;
    }
    output.push('\'');
    Ok(())
}

#[cfg(windows)]
fn quote_argument(output: &mut String, argument: &std::ffi::OsStr) -> std::fmt::Result {
    use std::{fmt::Write as _, os::windows::ffi::OsStrExt as _};
    if let Some(argument) = argument.to_str() {
        output.push('\'');
        output.push_str(&argument.replace('\'', "''"));
        output.push('\'');
    } else {
        // Preserve native paths even when their UTF-16 has unpaired surrogates.
        output.push_str("(-join [char[]]@(");
        for (index, unit) in argument.encode_wide().enumerate() {
            if index != 0 {
                output.push(',');
            }
            write!(output, "{unit}")?;
        }
        output.push_str("))");
    }
    Ok(())
}

#[derive(Debug)]
pub struct PreparationFailure(pub Failure);

impl std::fmt::Display for PreparationFailure {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(output, "prepare output: {}", self.0)
    }
}

impl std::error::Error for PreparationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// File-backed output configuration is admitted before a harness can start.
pub struct Prepared {
    pub schema: Option<acyclic_harness::model::OutputSchema>,
    pub projection: Option<Projection>,
}

impl Prepared {
    pub fn open(args: &super::args::Args) -> Result<Self, Failure> {
        let schema = args
            .output_schema
            .as_ref()
            .map(|path| -> Result<_, Failure> {
                Ok(acyclic_harness::model::OutputSchema::new(
                    serde_json::from_slice(&std::fs::read(path)?)?,
                )?)
            })
            .transpose()?;
        let projection = if args.output_format == super::args::OutputFormat::Jsonl {
            Some(Projection::open(
                args.operation.as_deref().ok_or("event output: operation absent; recovery: graphcoder --output-format jsonl --operation OPERATION exec")?,
                args.event_cursor.as_deref(),
            )?)
        } else {
            None
        };
        Ok(Self { schema, projection })
    }
}

pub struct Projection {
    state: acyclic_harness::local::EventProjection,
    path: Option<std::path::PathBuf>,
}

/// One caller-owned frame waits on the inherited output's original OS event.
/// A cursor can advance only after this same owned write completes.
struct EventOutput<'a> {
    writer: &'a mut Output,
    task: acyclic_harness::OperationId,
}

impl EventOutput<'_> {
    async fn event(&mut self, event: &impl serde::Serialize) -> std::io::Result<()> {
        write_event(self.writer, self.task, event).await
    }
}

impl Projection {
    pub fn open(operation: &str, path: Option<&std::path::Path>) -> Result<Self, Failure> {
        if path.is_some_and(|path| path.as_os_str() == "-") {
            return Err("event cursor: stdout is not a cursor file; recovery: pass --event-cursor events.cursor.json".into());
        }
        let state = match path.map(std::fs::read).transpose() {
            Ok(Some(bytes)) => serde_json::from_slice(&bytes)?,
            Ok(None) => acyclic_harness::local::EventProjection::new(operation.to_owned()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                acyclic_harness::local::EventProjection::new(operation.to_owned())
            }
            Err(error) => return Err(error.into()),
        };
        state.validate(operation)?;
        Ok(Self {
            state,
            path: path.map(std::path::Path::to_owned),
        })
    }

    async fn emit(
        &mut self,
        output: &mut EventOutput<'_>,
        delivery: &acyclic_harness::local::AgentDelivery,
    ) -> Result<(), Failure> {
        let event = self.state.event(delivery);
        output.event(&event).await?;
        let cursor = event.after;
        self.accepted_source(delivery.resource.clone(), cursor)
    }

    async fn defer(
        &self,
        output: &mut EventOutput<'_>,
        delivery: &acyclic_harness::local::AgentDelivery,
    ) -> Result<u64, Failure> {
        let mut event = self.state.event(delivery);
        let sequence = event
            .sequence
            .ok_or("approval status lacks its original sequence")?;
        event.after = Some(self.state.cursor(&delivery.resource));
        output.event(&event).await?;
        Ok(sequence)
    }

    fn accepted_source(
        &mut self,
        resource: String,
        cursor: Option<acyclic_harness::client::EventCursor>,
    ) -> Result<(), Failure> {
        if self.state.accepted(resource, cursor) {
            self.save()?;
        }
        Ok(())
    }

    fn save(&self) -> Result<(), Failure> {
        if let Some(path) = &self.path {
            let mut export = super::usage_export::Output::open(path)?;
            serde_json::to_writer(export.writer(), &self.state)?;
            export.finish()?;
        }
        Ok(())
    }

    fn accepted_execution(
        &mut self,
        agent: &acyclic_harness::agent::AgentPath,
        after: ExecutionPosition,
    ) -> Result<(), Failure> {
        if self.state.accepted_execution(agent, after)? {
            self.save()?;
        }
        Ok(())
    }
}

struct ExecutionExport {
    position: ExecutionPosition,
    cut: u64,
    batch: Option<ExecutionBatch>,
}

/// One bounded page and one scalar chunk continuation. A suspended range read
/// remains pinned in the caller's select beside stdin and original follows.
struct ExecutionBatch {
    query: acyclic_harness::local::agent_conversation::ExecutionQuery,
    record: usize,
    field: usize,
    offset: u64,
    announced: bool,
}

#[derive(Clone, Copy)]
struct PayloadRead {
    input: u64,
    sequence: u64,
    field: acyclic_harness::executor::ExecutionPayloadField,
    offset: u64,
}

impl ExecutionBatch {
    fn after(&self) -> Result<ExecutionPosition, Failure> {
        let page = self.query.page.as_ref().ok_or("execution page is absent")?;
        let record = page
            .items
            .get(self.record)
            .ok_or("execution observation is absent")?;
        Ok(if record.sequence == page.end {
            self.query.continuation()?
        } else {
            ExecutionPosition {
                input: self.query.input,
                after: record.sequence,
            }
        })
    }
}

impl ExecutionExport {
    fn load(&mut self, query: acyclic_harness::local::agent_conversation::ExecutionQuery) {
        self.batch = Some(ExecutionBatch {
            query,
            record: 0,
            field: 0,
            offset: 0,
            announced: false,
        });
    }

    /// At most one page of inline observations is processed here. Large payloads
    /// yield one bounded range to the original select; they cannot starve stdin.
    async fn prepare(
        &mut self,
        projection: &mut Projection,
        output: &mut EventOutput<'_>,
        agent: &acyclic_harness::agent::AgentPath,
    ) -> Result<Option<PayloadRead>, Failure> {
        use acyclic_harness::executor::ExecutionPayloadField;
        let Some(batch) = &mut self.batch else {
            return Ok(None);
        };
        while let Some(record) = batch
            .query
            .page
            .as_ref()
            .and_then(|page| page.items.get(batch.record))
        {
            let after = batch.after()?;
            let payload = ExecutionPayloadField::fields(&record.event).nth(batch.field);
            if !batch.announced {
                // A reference envelope alone does not acknowledge unread bytes.
                let complete = if payload.is_some() {
                    ExecutionPosition {
                        input: batch.query.input,
                        after: record.sequence - 1,
                    }
                } else {
                    after
                };
                let event =
                    projection
                        .state
                        .execution_event(agent, batch.query.input, record, complete);
                output.event(&event).await?;
                batch.announced = true;
            }
            if let Some((field, _)) = payload {
                return Ok(Some(PayloadRead {
                    input: batch.query.input,
                    sequence: record.sequence,
                    field,
                    offset: batch.offset,
                }));
            }
            projection.accepted_execution(agent, after)?;
            batch.record += 1;
            batch.field = 0;
            batch.offset = 0;
            batch.announced = false;
        }
        self.position = batch.query.continuation()?;
        projection.accepted_execution(agent, self.position)?;
        self.batch = None;
        Ok(None)
    }

    async fn chunk(
        &mut self,
        projection: &mut Projection,
        output: &mut EventOutput<'_>,
        agent: &acyclic_harness::agent::AgentPath,
        bytes: &[u8],
        length: u64,
    ) -> Result<(), Failure> {
        use acyclic_harness::executor::ExecutionPayloadField;
        let batch = self
            .batch
            .as_mut()
            .ok_or("execution payload page is absent")?;
        let after = batch.after()?;
        let record = batch
            .query
            .page
            .as_ref()
            .and_then(|page| page.items.get(batch.record))
            .ok_or("execution payload observation is absent")?;
        let (field, reference) = ExecutionPayloadField::fields(&record.event)
            .nth(batch.field)
            .ok_or("execution payload field is absent")?;
        if length != reference.descriptor().byte_length() {
            return Err("execution payload read changed its original bounds".into());
        }
        let end = batch
            .offset
            .checked_add(bytes.len() as u64)
            .ok_or("execution payload offset overflows")?;
        let last = end == length
            && ExecutionPayloadField::fields(&record.event)
                .nth(batch.field + 1)
                .is_none();
        let complete = if last {
            after
        } else {
            ExecutionPosition {
                input: batch.query.input,
                after: record.sequence - 1,
            }
        };
        let event = projection.state.execution_payload_event(
            agent,
            batch.query.input,
            record,
            field,
            batch.offset,
            bytes,
            complete,
        )?;
        output.event(&event).await?;
        if last {
            projection.accepted_execution(agent, after)?;
            batch.record += 1;
            batch.field = 0;
            batch.offset = 0;
            batch.announced = false;
        } else if end == length {
            batch.field += 1;
            batch.offset = 0;
        } else {
            batch.offset = end;
        }
        Ok(())
    }
}

/// Retain one original status while its immutable request ranges flush.
struct ApprovalExport {
    delivery: acyclic_harness::local::AgentDelivery,
    status: acyclic_harness::agent::StatusRecord,
    sequence: u64,
    offset: u64,
    length: Option<u64>,
}

impl ApprovalExport {
    async fn chunk(
        &mut self,
        projection: &mut Projection,
        output: &mut EventOutput<'_>,
        bytes: &[u8],
        length: u64,
    ) -> Result<bool, Failure> {
        if self.length.is_some_and(|original| original != length) {
            return Err("approval request read changed its original length".into());
        }
        let event =
            projection
                .state
                .approval_payload_event(&self.delivery, self.offset, bytes, length)?;
        output.event(&event).await?;
        let after = event.after;
        self.offset += bytes.len() as u64;
        self.length = Some(length);
        if self.offset != length {
            return Ok(false);
        }
        projection.accepted_source(self.delivery.resource.clone(), Some(after))?;
        Ok(true)
    }
}

/// The original output owner, unavailable while its owned write is outstanding.
/// Failure or observer loss consumes it; neither permits a replacement writer.
pub(super) struct Output(Option<InheritedOutput>);

impl Output {
    pub(super) fn new(output: InheritedOutput) -> Self {
        Self(Some(output))
    }

    pub(super) fn available(&self) -> bool {
        self.0.is_some()
    }

    async fn write<B: AsRef<[u8]> + Send + 'static>(
        &mut self,
        task: OperationId,
        bytes: B,
    ) -> std::io::Result<()> {
        let output = self.0.take().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "an earlier write did not return the original output writer",
            )
        })?;
        // The same task scope follows the original descriptor and buffer into
        // the native producer. Accepted synchronous writes retain both after
        // observer loss; that loss does not certify their physical completion.
        let (output, _bytes) = acyclic_native_runtime::obs::waiting(
            "inherited-output",
            task,
            output.write_owned(bytes),
        )
        .await?;
        self.0 = Some(output);
        Ok(())
    }
}

/// Stdin's lines, read off the coordination runtime's blocking slot: an open
/// stdin's read may never return.
fn stdin_lines() -> std::io::Result<AsyncLines<acyclic_native_runtime::Stdin>> {
    Ok(AsyncLines::new(
        acyclic_native_runtime::stdin()?,
        usize::MAX,
    ))
}

/// Send `prompt` as message `operation` and print its answer; rerunning the
/// operation replays it. An approval request is answered on stdin (`y`
/// approves); the exit code is 0 once the answer is printed.
pub(super) async fn prompt(
    conversation: &Arc<Conversation>,
    operation: &str,
    prompt: &str,
    output: &mut Output,
) -> Result<u8, Failure> {
    let mut events = conversation.observe(conversation.settled().await?).await?;
    let inbox = conversation.send(operation, prompt).await?;
    // Stdin is read only once an approval asks for an answer.
    let mut answers = None;
    let mut diagnostics = None;
    // Subscribed first, so a reply committed after this read wakes a status.
    let mut status = None;
    loop {
        match conversation.reply(operation, inbox).await? {
            Reply::Answered { entry, .. } => return print(conversation, &entry, output).await,
            Reply::Dropped => {
                diagnostic(
                    &mut diagnostics,
                    acyclic_native_runtime::InheritedOutput::stderr,
                    conversation.task(),
                    format!("graphcoder: {}\n", conversation.dropped().await?),
                )
                .await?;
                return Ok(1);
            }
            Reply::Pending => {}
        }
        if let Some(status) = status.take() {
            if let Some(reason) = ended(&status) {
                diagnostic(
                    &mut diagnostics,
                    acyclic_native_runtime::InheritedOutput::stderr,
                    conversation.task(),
                    format!("graphcoder: {reason}\n"),
                )
                .await?;
                return Ok(1);
            }
            if let Phase::WaitingApproval { id, .. } = status.phase {
                diagnostic(
                    &mut diagnostics,
                    acyclic_native_runtime::InheritedOutput::stderr,
                    conversation.task(),
                    format!("graphcoder: approve request {}? [y/N] ", id.as_str()),
                )
                .await?;
                let answers = match &mut answers {
                    Some(answers) => answers,
                    None => answers.insert(stdin_lines()?),
                };
                let answer = answers.next_line().await?.unwrap_or_default();
                let approved = matches!(answer.trim(), "y" | "Y" | "yes");
                conversation.decide(&id, approved).await?;
            }
        }
        status = loop {
            if let Event::Status(next) = events.next().await? {
                break Some(next);
            }
        };
    }
}

// Admit stderr only when a diagnostic exists, and reuse its original writer.
// The original native write retains the formatted message through completion.
async fn diagnostic(
    output: &mut Option<Output>,
    open: impl FnOnce() -> std::io::Result<InheritedOutput>,
    task: OperationId,
    message: String,
) -> std::io::Result<()> {
    let output = match output {
        Some(output) => output,
        None => output.insert(Output::new(open()?)),
    };
    output.write(task, message.into_bytes()).await
}

/// Prints the answer text of the prompt's settled `entry`.
async fn print(
    conversation: &Conversation,
    entry: &ConversationEntry,
    output: &mut Output,
) -> Result<u8, Failure> {
    let (_, text) = conversation.turn(entry).await?;
    output.write(conversation.task(), text.into_bytes()).await?;
    output.write(conversation.task(), [b'\n']).await?;
    Ok(0)
}

/// The printed answer of prompt `id`, settled as `entry`.
async fn answer(
    conversation: &Conversation,
    id: &str,
    entry: &ConversationEntry,
    inbox: u64,
) -> Result<Value, Failure> {
    let (_, text) = conversation.turn(entry).await?;
    Ok(json!({"id": id, "inbox": inbox, "text": text}))
}

/// The printed line of prompt `id` at `inbox` once `reply` settled it.
async fn settled(
    conversation: &Conversation,
    id: &str,
    inbox: u64,
    reply: Reply,
) -> Result<Option<Value>, Failure> {
    Ok(match reply {
        Reply::Answered { entry, origin, .. } => {
            let inbox = origin.as_ref().map_or(entry.inbox, |origin| origin.inbox);
            // Only a completed turn is an answer. A cancelled turn's partial
            // output is never a result, so it cannot replace a final artifact.
            match entry.outcome {
                TurnOutcome::Completed => Some(answer(conversation, id, &entry, inbox).await?),
                TurnOutcome::Cancelled => {
                    Some(json!({"id": id, "inbox": inbox, "error": "turn cancelled"}))
                }
                TurnOutcome::RestoreConflict => {
                    let (_, text) = conversation.turn(&entry).await?;
                    Some(json!({"id": id, "inbox": inbox, "error": text}))
                }
            }
        }
        Reply::Dropped => {
            Some(json!({"id": id, "inbox": inbox, "error": conversation.dropped().await?}))
        }
        Reply::Pending => None,
    })
}

fn final_value(
    mut line: Value,
    schema: Option<&acyclic_harness::model::OutputSchema>,
    path: Option<&std::path::Path>,
) -> Result<Value, Failure> {
    let Some(text) = line.get("text").and_then(Value::as_str) else {
        return Ok(line);
    };
    let value = schema.map(|schema| schema.project(text)).transpose()?;
    if let Some(path) = path {
        let mut output = super::usage_export::Output::open(path)?;
        match &value {
            Some(value) => serde_json::to_writer(output.writer(), value)?,
            None => output.writer().write_all(text.as_bytes())?,
        }
        output.finish()?;
    }
    if let Some(value) = value {
        line["structured"] = value;
    }
    Ok(line)
}

struct VersionedLine<'a> {
    line: &'a Value,
    operation: &'a str,
}

impl serde::Serialize for VersionedLine<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap as _;
        let mut fields = serializer.serialize_map(None)?;
        fields.serialize_entry("version", &1_u32)?;
        fields.serialize_entry("operation", self.operation)?;
        if let Some(line) = self.line.as_object() {
            if !line.contains_key("kind") {
                let kind = if line.contains_key("approval") {
                    "approval"
                } else {
                    "result"
                };
                fields.serialize_entry("kind", kind)?;
            }
            for (name, value) in line {
                if name != "version" && name != "operation" {
                    fields.serialize_entry(name, value)?;
                }
            }
        }
        fields.end()
    }
}

pub(super) async fn write_event(
    output: &mut Output,
    task: acyclic_harness::OperationId,
    event: &impl serde::Serialize,
) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(event)?;
    bytes.push(b'\n');
    output.write(task, bytes).await
}

async fn emit(
    output: &mut EventOutput<'_>,
    line: &Value,
    operation: Option<&str>,
) -> std::io::Result<()> {
    match operation {
        Some(operation) => output.event(&VersionedLine { line, operation }).await,
        None => output.event(line).await,
    }
}

/// A terminal failure is versioned like deliveries and results, before exit.
pub(super) async fn terminal_error(
    output: &mut Output,
    operation: &str,
    error: &str,
    step: &str,
    recovery: &Recovery,
) -> std::io::Result<()> {
    let mut output = EventOutput {
        writer: output,
        task: acyclic_harness::OperationId::new(),
    };
    emit(
        &mut output,
        &json!({"kind": "error", "code": "execution_failed", "step": step, "error": error,
        "recovery": recovery}),
        Some(operation),
    )
    .await
}

/// The JSONL stream's outstanding prompts.
#[derive(Default)]
struct Outstanding {
    events_operation: Option<String>,
    /// Accepted prompts by inbox sequence: message id and display id.
    pending: BTreeMap<u64, (String, String)>,
    /// Largest accepted input cut from this invocation, including exact replays.
    accepted_tail: u64,
    /// Prompts whose send outcome is unknown (message id, display id and
    /// text), resent exactly at the next event until accepted or refused.
    uncertain: Vec<(String, String, String)>,
    /// The reason the agent ended, once a status said so.
    finished: Option<String>,
    failed: bool,
    schema: Option<acyclic_harness::model::OutputSchema>,
    final_output: Option<std::path::PathBuf>,
}

impl Outstanding {
    /// Sends `prompt` as `message`. Only a refusal is settled at once; an
    /// unknown outcome may have committed, so it stays outstanding under
    /// the same message id.
    async fn send(
        &mut self,
        conversation: &Conversation,
        output: &mut EventOutput<'_>,
        message: String,
        id: String,
        prompt: String,
    ) -> Result<(), Failure> {
        let inbox = match conversation.deliver(&message, &prompt).await? {
            Sent::Accepted(inbox) => inbox,
            // A refused prompt, such as an unknown `/name`, is its own
            // record; the stream goes on.
            Sent::Refused(reason) => {
                self.failed = true;
                emit(
                    output,
                    &json!({"id": id, "error": reason}),
                    self.events_operation.as_deref(),
                )
                .await?;
                return Ok(());
            }
            Sent::Unknown => {
                self.uncertain.push((message, id, prompt));
                return Ok(());
            }
        };
        self.accepted_tail = self.accepted_tail.max(
            inbox
                .checked_add(1)
                .ok_or("accepted input continuation overflows")?,
        );
        // An exact replay of a settled prompt is answered at once, whether
        // or not the agent has since ended.
        let reply = conversation.reply(&message, inbox).await?;
        match (
            settled(conversation, &id, inbox, reply).await?,
            &self.finished,
        ) {
            (Some(line), _) => {
                self.failed |= line.get("error").is_some();
                let line = final_value(line, self.schema.as_ref(), self.final_output.as_deref())?;
                emit(output, &line, self.events_operation.as_deref()).await?;
            }
            (None, Some(reason)) => {
                self.failed = true;
                emit(
                    output,
                    &json!({"id": id, "inbox": inbox, "error": reason}),
                    self.events_operation.as_deref(),
                )
                .await?;
            }
            (None, None) => {
                self.pending.insert(inbox, (message, id));
            }
        }
        Ok(())
    }

    /// Resends every prompt of unknown outcome; an exact resend commits
    /// nothing twice and answers from its committed record.
    async fn resend(
        &mut self,
        conversation: &Conversation,
        output: &mut EventOutput<'_>,
    ) -> Result<(), Failure> {
        for (message, id, prompt) in std::mem::take(&mut self.uncertain) {
            self.send(conversation, output, message, id, prompt).await?;
        }
        Ok(())
    }
}

/// One JSONL request per stdin line: `{"prompt": text, "id"?: id}`,
/// `{"approve": request, "binding": shown, "approved": bool}` or
/// `{"cancel": true}`. A prompt is
/// message `operation:id`, so rerunning the operation replays it. Each settled
/// prompt prints `{"id", "inbox", "text"}` (or `"error"`, including for a
/// cancelled turn), each approval request `{"approval": request, "binding"}`.
/// An ended agent is terminal: it admits no further input, so every prompt it leaves unanswered, outstanding when
/// it ends or sent later, is answered with its reason; an exact replay of a
/// prompt it answered still prints its saved answer. A prompt whose send
/// outcome is unknown is resent exactly at the next event. Exits,
/// once stdin closes and every prompt settled. Failed prompts and unresolved
/// delivery outcomes make the exit status nonzero; rerunning the operation
/// replays the same input identities.
pub(super) async fn jsonl(
    conversation: &Arc<Conversation>,
    operation: &str,
    output: &mut Output,
    mut projection: Option<Projection>,
    initial_prompt: Option<&str>,
    schema: Option<acyclic_harness::model::OutputSchema>,
    final_output: Option<std::path::PathBuf>,
    input_format: super::args::InputFormat,
) -> Result<u8, Failure> {
    let mut framed = EventOutput {
        writer: output,
        task: conversation.task(),
    };
    let output = &mut framed;
    let mut events = match &projection {
        Some(projection) => conversation.replay(&projection.state)?,
        None => conversation.observe(conversation.settled().await?).await?,
    };
    let mut lines = stdin_lines()?;
    let mut open = true;
    let mut sent = 0_u64;
    let event_operation = projection.as_ref().map(|_| operation);
    let mut outstanding = Outstanding {
        events_operation: event_operation.map(str::to_owned),
        schema,
        final_output,
        ..Default::default()
    };
    let mut announced = None;
    let mut approval = None;
    let mut drain = None;
    // Prompts share the awaited native stderr writer, so a full stderr pipe
    // delays only this prompt, never the coordination runtime.
    let mut diagnostics = None;
    // A saved cursor may already be past the status that announced the
    // pending approval. Text input answers it from the current phase, read
    // once at reconnect; replay of that same status does not prompt twice.
    if projection.is_some()
        && input_format == super::args::InputFormat::Text
        && let Phase::WaitingApproval { id, .. } = conversation.phase().await?
    {
        diagnostic(
            &mut diagnostics,
            acyclic_native_runtime::InheritedOutput::stderr,
            conversation.task(),
            format!("graphcoder: approve request {}? [y/N] ", id.as_str()),
        )
        .await?;
        approval = Some(id.clone());
        announced = Some(id);
    }
    if let Some(prompt) = initial_prompt {
        outstanding
            .send(
                conversation,
                output,
                operation.to_owned(),
                operation.to_owned(),
                prompt.to_owned(),
            )
            .await?;
    }
    let mut execution = if let Some(projection) = &mut projection {
        let position = projection.state.execution_position(conversation.path())?;
        let query = conversation
            .execution_page(position.input, position.after, false)
            .await?;
        let mut export = ExecutionExport {
            position,
            cut: query.inbox_tail,
            batch: None,
        };
        export.load(query);
        Some(export)
    } else {
        None
    };
    // Keep this exact source future pinned across unrelated stdin/follow events.
    // It needs no allocation, task or repeated control exchange while quiet.
    let mut execution_wait = std::pin::pin!(None);
    let mut payload_wait = std::pin::pin!(None);
    let mut approval_export: Option<ApprovalExport> = None;
    let mut approval_wait = std::pin::pin!(None);
    loop {
        let payload = match (&mut execution, &mut projection) {
            (Some(export), Some(projection)) => {
                export
                    .prepare(projection, output, conversation.path())
                    .await?
            }
            _ => None,
        };
        if let Some(payload) = payload
            && payload_wait.is_none()
        {
            payload_wait.set(Some(conversation.execution_payload(
                payload.input,
                payload.sequence,
                payload.field,
                payload.offset,
            )));
        }
        if let Some(export) = &approval_export
            && approval_wait.is_none()
        {
            approval_wait.set(Some(
                conversation.approval_request(export.sequence, export.offset),
            ));
        }
        if (!open || initial_prompt.is_some())
            && outstanding.pending.is_empty()
            && approval_export.is_none()
        {
            outstanding.resend(conversation, output).await?;
            if !outstanding.uncertain.is_empty() {
                let ids = outstanding.uncertain.iter().map(|(_, id, _)| id.as_str());
                let ids = ids.collect::<Vec<_>>().join(", ");
                return Err(format!("commit outcome unknown for prompts {ids}").into());
            }
            if outstanding.pending.is_empty() {
                let execution_complete = execution.as_ref().is_none_or(|export| {
                    export.batch.is_none()
                        && export.position.input >= export.cut.max(outstanding.accepted_tail)
                });
                match &projection {
                    None => return Ok(0),
                    Some(projection) if drain.is_none() && execution_complete => {
                        // This single reopen establishes opening cuts after every
                        // input settled. The follows themselves deliver the cuts;
                        // no head probes or readiness retries are involved.
                        events = conversation.replay(&projection.state)?;
                        drain = Some(acyclic_harness::local::EventDrain::new(
                            events.sources().into_iter().map(|source| {
                                let from = projection.state.cursor(&source).next;
                                (source, from)
                            }),
                        ));
                    }
                    Some(_)
                        if execution_complete
                            && drain.as_ref().is_some_and(|drain| drain.complete()) =>
                    {
                        emit(
                            output,
                            &json!({"kind": "completed", "success": !outstanding.failed}),
                            event_operation,
                        )
                        .await?;
                        return Ok(u8::from(outstanding.failed));
                    }
                    Some(_) => {}
                }
            }
        }
        let execution_active = execution.as_ref().is_some_and(|export| {
            export.batch.is_none()
                && export.position.input < export.cut.max(outstanding.accepted_tail)
        });
        if let Some(export) = &execution
            && execution_active
            && execution_wait.is_none()
        {
            execution_wait.set(Some(conversation.execution_page(
                export.position.input,
                export.position.after,
                true,
            )));
        }
        let event = tokio::select! {
            query = async {
                let waiting = execution_wait.as_mut().as_pin_mut()
                    .ok_or("execution source wait is absent")?;
                waiting.await.map_err(Failure::from)
            }, if execution_active => {
                let query = query?;
                let export = execution.as_mut().ok_or("execution projection is absent")?;
                execution_wait.set(None);
                export.load(query);
                None
            }
            chunk = async {
                let waiting = payload_wait.as_mut().as_pin_mut().ok_or("execution payload wait is absent")?;
                waiting.await.map_err(Failure::from)
            }, if payload.is_some() => {
                let (bytes, length) = chunk?;
                payload_wait.set(None);
                let export = execution.as_mut().ok_or("execution projection is absent")?;
                let projection = projection.as_mut().ok_or("event projection is absent")?;
                export.chunk(projection, output, conversation.path(), &bytes, length).await?;
                None
            }
            chunk = async {
                let waiting = approval_wait.as_mut().as_pin_mut().ok_or("approval request wait is absent")?;
                waiting.await.map_err(Failure::from)
            }, if approval_export.is_some() => {
                let (bytes, length) = chunk?;
                approval_wait.set(None);
                let export = approval_export.as_mut().ok_or("approval export is absent")?;
                let projection = projection.as_mut().ok_or("event projection is absent")?;
                if export.chunk(projection, output, &bytes, length).await? {
                    let export = approval_export.take().ok_or("completed approval export is absent")?;
                    if let Some(drain) = &mut drain { drain.accepted(&export.delivery); }
                    Some(Event::Status(export.status))
                } else {
                    None
                }
            }
            line = lines.next_line(), if open && (input_format == super::args::InputFormat::Jsonl || approval.is_some()) => {
                let line = line?;
                if input_format == super::args::InputFormat::Text {
                    let id = approval.take().ok_or("approval input: no pending request; recovery: graphcoder exec --input-format jsonl")?;
                    let answer = line.as_deref().unwrap_or_default();
                    conversation.decide(&id, matches!(answer.trim(), "y" | "Y" | "yes")).await?;
                    continue;
                }
                let Some(line) = line else {
                    open = false;
                    continue;
                };
                if line.trim().is_empty() {
                    continue;
                }
                let request: Request = serde_json::from_str(&line)
                    .map_err(|error| format!("invalid JSONL request: {error}"))?;
                outstanding.resend(conversation, output).await?;
                match request {
                Request::Prompt { prompt, id } => {
                    if initial_prompt.is_some() { return Err("exec input: positional task already supplied; recovery: use approvals or cancellation on stdin".into()); }
                    sent += 1;
                    let id = id.unwrap_or_else(|| format!("jsonl-{sent}"));
                    let message = format!("{operation}:{id}");
                    outstanding
                        .send(conversation, output, message, id, prompt)
                        .await?;
                }
                Request::Approval { approve, binding, approved } => {
                    conversation
                        .decide_with_binding(&approve, binding, approved)
                        .await?;
                }
                Request::Cancel { cancel: true } => {
                    conversation.cancel().await?;
                }
                Request::Cancel { cancel: false } => {
                    return Err("JSONL cancellation requires cancel=true".into());
                }
                }
                None
            }
            event = async {
                match &mut projection {
                    Some(_) => {
                        let (event, delivery) = events.next_projected().await?;
                        Ok::<_, Failure>((event, Some(delivery)))
                    }
                    None => Ok((Ok(Some(events.next().await?)), None)),
                }
            }, if approval_export.is_none() => {
                match event? {
                    (Ok(Some(Event::Status(status))), Some(delivery))
                        if matches!(status.phase, Phase::WaitingApproval { .. }) => {
                        let projection = projection.as_ref().ok_or("event projection is absent")?;
                        let sequence = projection.defer(output, &delivery).await?;
                        approval_export = Some(ApprovalExport {
                            delivery, status, sequence, offset: 0, length: None,
                        });
                        None
                    }
                    (event, Some(delivery)) => {
                        projection.as_mut().ok_or("event projection is absent")?.emit(output, &delivery).await?;
                        if let Some(drain) = &mut drain { drain.accepted(&delivery); }
                        event?
                    }
                    (event, None) => event?,
                }
            }
        };
        match event {
            None => {}
            // Each entry commits with a status, which settles it below.
            Some(Event::Turn(_)) | Some(Event::Output) => {}
            Some(Event::Status(status)) => {
                let ended = ended(&status);
                // Prompts the agent consumed settle from its committed reply.
                let pending = &mut outstanding.pending;
                let later = pending.split_off(&status.cursor);
                for (inbox, (message, id)) in std::mem::replace(pending, later) {
                    let reply = conversation.reply(&message, inbox).await?;
                    let line = match (&ended, reply) {
                        (Some(reason), Reply::Pending | Reply::Dropped) => {
                            Some(json!({"id": id, "inbox": inbox, "error": reason}))
                        }
                        (_, reply) => settled(conversation, &id, inbox, reply).await?,
                    };
                    match line {
                        Some(line) => {
                            outstanding.failed |= line.get("error").is_some();
                            let line = final_value(
                                line,
                                outstanding.schema.as_ref(),
                                outstanding.final_output.as_deref(),
                            )?;
                            emit(output, &line, event_operation).await?;
                        }
                        None => {
                            pending.insert(inbox, (message, id));
                        }
                    }
                }
                // An ended agent answers every outstanding prompt with
                // its reason; each is a record, so the stream goes on
                // until stdin closes.
                if let Some(reason) = ended {
                    for (inbox, (_, id)) in std::mem::take(pending) {
                        outstanding.failed = true;
                        emit(
                            output,
                            &json!({"id": id, "inbox": inbox, "error": reason}),
                            event_operation,
                        )
                        .await?;
                    }
                    outstanding.finished = Some(reason);
                }
                if !matches!(status.phase, Phase::WaitingApproval { .. }) {
                    approval = None;
                    announced = None;
                }
                if let Phase::WaitingApproval { id, .. } = &status.phase
                    && announced.as_ref() != Some(id)
                {
                    // The decision input requires the exact shown binding;
                    // one no longer pending was already decided elsewhere.
                    let binding = match event_operation {
                        Some(_) => None,
                        None => conversation.approval_binding(id).await?,
                    };
                    let line = match &binding {
                        Some(binding) => json!({"approval": id.as_str(), "binding": binding}),
                        None => json!({"approval": id.as_str()}),
                    };
                    emit(output, &line, event_operation).await?;
                    // Replay exports historical statuses too. Only the agent's
                    // current pending request admits a text answer; a live
                    // observer starts at the latest status, so it is current.
                    if input_format == super::args::InputFormat::Text
                        && (event_operation.is_none()
                            || matches!(
                                conversation.phase().await?,
                                Phase::WaitingApproval { id: pending, .. } if pending == *id
                            ))
                    {
                        diagnostic(
                            &mut diagnostics,
                            acyclic_native_runtime::InheritedOutput::stderr,
                            conversation.task(),
                            format!("graphcoder: approve request {}? [y/N] ", id.as_str()),
                        )
                        .await?;
                        approval = Some(id.clone());
                    }
                    announced = Some(id.clone());
                }
                // A status is the stream's event for a prompt of
                // unknown outcome: resend it, answered once committed.
                outstanding.resend(conversation, output).await?;
            }
        }
    }
}

#[test]
fn schema_refusal_preserves_the_previous_final_artifact() -> Result<(), Failure> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("answer.json");
    std::fs::write(&path, b"previous")?;
    let schema = acyclic_harness::model::OutputSchema::new(json!({
        "type": "object", "required": ["answer"],
        "properties": {"answer": {"type": "string"}}, "additionalProperties": false
    }))?;
    for text in ["not JSON", "{\"answer\":7}"] {
        assert!(final_value(json!({"text": text}), Some(&schema), Some(&path)).is_err());
        assert_eq!(std::fs::read(&path)?, b"previous");
    }
    let projected = final_value(
        json!({"text": "{\"answer\":\"saved\"}"}),
        Some(&schema),
        Some(&path),
    )?;
    assert_eq!(projected["structured"], json!({"answer":"saved"}));
    assert_eq!(
        serde_json::from_slice::<Value>(&std::fs::read(&path)?)?,
        projected["structured"]
    );
    Ok(())
}
#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use acyclic_native_runtime::{
        InheritedOutput,
        obs::{ReportAck, ReportOutcome, ReportPort, ReportRequest, WaitReport},
    };
    use futures::FutureExt as _;
    use std::{
        future::{Future as _, poll_fn},
        io::{Read as _, Seek as _},
        os::fd::AsFd as _,
        pin::Pin,
        task::Poll,
    };
    use tokio::io::{AsyncBufReadExt as _, AsyncWrite as _, AsyncWriteExt as _, BufReader};

    #[test]
    fn a_full_answer_pipe_keeps_report_and_shutdown_runnable() -> Result<(), Failure> {
        // Redirected output is a synchronous sink: the borrowed adapter refuses
        // it. The same headless owner must return before the next frame writes.
        acyclic_native_runtime::coordination_runtime()?.block_on(async {
            let file = tempfile::tempfile()?;
            let mut reader = file.try_clone()?;
            let mut output = Output::new(InheritedOutput::from_file(file)?);
            let task = OperationId::from_bytes([19; 16]);
            let line = json!({"id": "original-answer", "text": "settled"});
            let mut framed = EventOutput {
                writer: &mut output,
                task,
            };
            emit(&mut framed, &line, None).await?;
            output
                .write(task, String::from("original text").into_bytes())
                .await?;
            output.write(task, [b'\n']).await?;
            drop(output);
            reader.rewind()?;
            let mut received = Vec::new();
            reader.read_to_end(&mut received)?;
            let mut expected = serde_json::to_vec(&line)?;
            expected.extend_from_slice(b"\noriginal text\n");
            assert_eq!(received, expected);
            Ok::<(), Failure>(())
        })?;
        for terminal in [Terminal::Cancel, Terminal::Drain, Terminal::ReaderLost] {
            full_output_pipe(terminal, false)?;
        }
        Ok(())
    }

    #[test]
    fn a_full_diagnostic_pipe_keeps_report_and_shutdown_runnable() -> Result<(), Failure> {
        for terminal in [Terminal::Cancel, Terminal::Drain, Terminal::ReaderLost] {
            full_output_pipe(terminal, true)?;
        }
        Ok(())
    }

    #[derive(Clone, Copy)]
    enum Terminal {
        Cancel,
        Drain,
        ReaderLost,
    }

    fn full_output_pipe(terminal: Terminal, diagnostics: bool) -> Result<(), Failure> {
        let report = WaitReport::default();
        let dispatch = tracing::Dispatch::new(
            crate::lifecycle::subscriber(&report, Some("off".into()), || Ok(None))?.0,
        );
        let _subscriber = tracing::dispatcher::set_default(&dispatch);
        acyclic_native_runtime::coordination_runtime()?.block_on(async {
            let (mut reader, original) = std::io::pipe()?;
            let mut output =
                InheritedOutput::from_file(original.as_fd().try_clone_to_owned()?.into())?;
            let chunk = [b'x'; 8192];
            let mut filled = output.write(&chunk).await?;
            loop {
                let written = poll_fn(|cx| {
                    Poll::Ready(match Pin::new(&mut output).poll_write(cx, &chunk) {
                        Poll::Ready(result) => Some(result),
                        Poll::Pending => None,
                    })
                })
                .await;
                let Some(written) = written else { break };
                let written = written?;
                assert_ne!(written, 0);
                filled += written;
            }
            let mut output = Output::new(output);

            let request = ReportRequest {
                request_id: "abcdef0123456789abcdef0123456789".into(),
                generation: "0123456789abcdef0123456789abcdef".into(),
                deadline_unix_ms: u64::MAX,
            };
            let (owner, child) = tokio::io::duplex(4096);
            let port = ReportPort::new(request.generation.clone(), child)?;
            let (entered, blocked) = tokio::sync::oneshot::channel();
            let (terminate, signal) = tokio::sync::oneshot::channel();
            let shutdown = async move {
                signal.await.unwrap();
                Ok(143)
            }
            .boxed()
            .shared();
            let task = acyclic_harness::OperationId::from_bytes([19; 16]);
            let line = json!({"id": "original-answer", "text": "settled"});
            let message = format!("graphcoder: approve request {task}? [y/N] ");
            let mut diagnostic_output = None;
            let opened = std::cell::Cell::new(0);
            let operation = async {
                let writing = async {
                    if diagnostics {
                        diagnostic(
                            &mut diagnostic_output,
                            || {
                                opened.set(opened.get() + 1);
                                InheritedOutput::from_file(
                                    original.as_fd().try_clone_to_owned()?.into(),
                                )
                            },
                            task,
                            message.clone(),
                        )
                        .await?;
                        diagnostic(
                            &mut diagnostic_output,
                            || panic!("a repeated diagnostic must reuse its admitted writer"),
                            task,
                            message.clone(),
                        )
                        .await
                    } else {
                        let mut output = EventOutput {
                            writer: &mut output,
                            task,
                        };
                        emit(&mut output, &line, None).await
                    }
                };
                tokio::pin!(writing);
                poll_fn(|cx| {
                    assert!(writing.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                entered.send(()).unwrap();
                writing.await?;
                Ok(0)
            };
            let host = async {
                let mut owner = BufReader::new(owner);
                let mut line = String::new();
                owner.read_line(&mut line).await.unwrap();
                blocked.await.unwrap();
                let mut bytes = serde_json::to_vec(&request).unwrap();
                bytes.push(b'\n');
                owner.get_mut().write_all(&bytes).await.unwrap();
                line.clear();
                owner.read_line(&mut line).await.unwrap();
                let ack: ReportAck = serde_json::from_str(&line).unwrap();
                assert_eq!(ack.request_id, request.request_id);
                assert_eq!(ack.generation, request.generation);
                assert_eq!(ack.outcome, ReportOutcome::Report);
                assert_eq!(ack.waits.len(), 1);
                assert_eq!(ack.waits[0].primitive, "inherited-output");
                assert_eq!(ack.waits[0].key, task.to_string());
                let mut terminate = Some(terminate);
                let reader = match terminal {
                    Terminal::Cancel => {
                        terminate.take().unwrap().send(()).unwrap();
                        Some(reader)
                    }
                    Terminal::Drain => {
                        drain_prefix(&mut reader, filled, &chunk).unwrap();
                        Some(reader)
                    }
                    Terminal::ReaderLost => {
                        drop(reader);
                        None
                    }
                };
                (owner, reader, terminate)
            };
            let ((result, report_result, _failure_report), (owner, reader, _terminate)) = tokio::join!(
                crate::lifecycle::observe(&report, Ok(Some(port)), shutdown, false, operation),
                host,
            );
            match terminal {
                Terminal::Cancel => assert_eq!(result?, 143),
                Terminal::Drain => assert_eq!(result?, 0),
                Terminal::ReaderLost => assert_eq!(
                    result
                        .unwrap_err()
                        .downcast_ref::<std::io::Error>()
                        .unwrap()
                        .kind(),
                    std::io::ErrorKind::BrokenPipe,
                ),
            }
            report_result?;
            assert_eq!(opened.get(), usize::from(diagnostics));
            if !matches!(terminal, Terminal::Drain) {
                let unavailable = if diagnostics {
                    diagnostic(
                        &mut diagnostic_output,
                        || panic!("a consumed diagnostic writer must never be reopened"),
                        task,
                        message.clone(),
                    )
                    .await
                } else {
                    let recovery = Recovery::capture(None)?;
                    terminal_error(
                        &mut output,
                        "original-operation",
                        "original output write did not complete",
                        "write_output",
                        &recovery,
                    )
                    .await
                };
                assert_eq!(
                    unavailable.unwrap_err().kind(),
                    std::io::ErrorKind::NotConnected,
                );
            }
            drop(diagnostic_output);
            assert!(report.snapshot().is_empty());
            drop(output);
            drop(original);
            if let Some(mut reader) = reader {
                if matches!(terminal, Terminal::Cancel) {
                    // Cancellation accepted no answer bytes. Only the exact
                    // previously accepted prefix remains before original EOF.
                    drain_prefix(&mut reader, filled, &chunk)?;
                }
                let mut received = Vec::new();
                reader.read_to_end(&mut received)?;
                let expected = if matches!(terminal, Terminal::Drain) {
                    if diagnostics {
                        message.repeat(2).into_bytes()
                    } else {
                        let mut bytes = serde_json::to_vec(&line)?;
                        bytes.push(b'\n');
                        bytes
                    }
                } else {
                    Vec::new()
                };
                assert_eq!(received, expected);
            }
            drop(owner);
            Ok(())
        })
    }

    fn drain_prefix(
        reader: &mut impl std::io::Read,
        mut remaining: usize,
        chunk: &[u8; 8192],
    ) -> std::io::Result<()> {
        while remaining != 0 {
            let count = remaining.min(chunk.len());
            let mut received = [0; 8192];
            reader.read_exact(&mut received[..count])?;
            assert_eq!(&received[..count], &chunk[..count]);
            remaining -= count;
        }
        Ok(())
    }
}
