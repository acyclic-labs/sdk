//! Headless conversations: one `-p` prompt, or JSONL requests on stdin.
//! Every prompt settles through the conversation's committed reply, read
//! after subscribing and again at each committed status, which every
//! conversation entry commits with; the frontend keeps only its outstanding
//! prompts.
use acyclic_harness::local::{
    Event,
    agent_conversation::{Conversation, Sent, ended},
};
use acyclic_harness::{
    OperationId,
    agent::{ConversationEntry, IntentId, Phase, Reply},
};
use acyclic_native_runtime::{InheritedOutput, lines::AsyncLines};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};

type Failure = Box<dyn std::error::Error>;

/// The original output owner, unavailable while its owned write is outstanding.
/// Failure or observer loss consumes it; neither permits a replacement writer.
pub(super) struct Output(Option<InheritedOutput>);

impl Output {
    pub(super) fn new(output: InheritedOutput) -> Self {
        Self(Some(output))
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
            if let Phase::WaitingApproval { id } = status.phase {
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
            Some(answer(conversation, id, &entry, inbox).await?)
        }
        Reply::Dropped => {
            Some(json!({"id": id, "inbox": inbox, "error": conversation.dropped().await?}))
        }
        Reply::Pending => None,
    })
}

async fn emit(output: &mut Output, task: OperationId, line: &Value) -> std::io::Result<()> {
    // This one caller-owned frame remains live through its original write.
    // The synchronous serializer never writes to an OS handle or a second queue.
    let mut bytes = serde_json::to_vec(line)?;
    bytes.push(b'\n');
    // The original task is opaque; caller-chosen display/message ids are not
    // diagnostic keys. This scope ends with the original write or its drop.
    output.write(task, bytes).await
}

/// The JSONL stream's outstanding prompts.
#[derive(Default)]
struct Outstanding {
    /// Accepted prompts by inbox sequence: message id and display id.
    pending: BTreeMap<u64, (String, String)>,
    /// Prompts whose send outcome is unknown (message id, display id and
    /// text), resent exactly at the next event until accepted or refused.
    uncertain: Vec<(String, String, String)>,
    /// The reason the agent ended, once a status said so.
    finished: Option<String>,
}

impl Outstanding {
    /// Sends `prompt` as `message`. Only a refusal is settled at once; an
    /// unknown outcome may have committed, so it stays outstanding under
    /// the same message id.
    async fn send(
        &mut self,
        conversation: &Conversation,
        output: &mut Output,
        message: String,
        id: String,
        prompt: String,
    ) -> Result<(), Failure> {
        let inbox = match conversation.deliver(&message, &prompt).await? {
            Sent::Accepted(inbox) => inbox,
            // A refused prompt, such as an unknown `/name`, is its own
            // record; the stream goes on.
            Sent::Refused(reason) => {
                emit(
                    output,
                    conversation.task(),
                    &json!({"id": id, "error": reason}),
                )
                .await?;
                return Ok(());
            }
            Sent::Unknown => {
                self.uncertain.push((message, id, prompt));
                return Ok(());
            }
        };
        // An exact replay of a settled prompt is answered at once, whether
        // or not the agent has since ended.
        let reply = conversation.reply(&message, inbox).await?;
        match (
            settled(conversation, &id, inbox, reply).await?,
            &self.finished,
        ) {
            (Some(line), _) => emit(output, conversation.task(), &line).await?,
            (None, Some(reason)) => {
                emit(
                    output,
                    conversation.task(),
                    &json!({"id": id, "inbox": inbox, "error": reason}),
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
        output: &mut Output,
    ) -> Result<(), Failure> {
        for (message, id, prompt) in std::mem::take(&mut self.uncertain) {
            self.send(conversation, output, message, id, prompt).await?;
        }
        Ok(())
    }
}

/// One JSONL request per stdin line: `{"prompt": text, "id"?: id}`,
/// `{"approve": request, "approved": bool}` or `{"cancel": true}`. A prompt is
/// message `operation:id`, so rerunning the operation replays it. Each settled
/// prompt prints `{"id", "inbox", "text"}` (or `"error"`), each approval
/// request `{"approval": request}`. An ended agent is terminal: it admits
/// no further input, so every prompt it leaves unanswered, outstanding when
/// it ends or sent later, is answered with its reason; an exact replay of a
/// prompt it answered still prints its saved answer. A prompt whose send
/// outcome is unknown is resent exactly at the next event. Exits,
/// successfully, once stdin closes and every prompt settled, whether
/// answered or failed; a prompt still of unknown outcome then fails the
/// run, and rerunning the operation replays it.
pub(super) async fn jsonl(
    conversation: &Arc<Conversation>,
    operation: &str,
    output: &mut Output,
) -> Result<u8, Failure> {
    let mut events = conversation.observe(conversation.settled().await?).await?;
    let mut lines = stdin_lines()?;
    let mut open = true;
    let mut sent = 0_u64;
    let mut outstanding = Outstanding::default();
    let mut announced = None;
    loop {
        if !open && outstanding.pending.is_empty() {
            outstanding.resend(conversation, output).await?;
            if !outstanding.uncertain.is_empty() {
                let ids = outstanding.uncertain.iter().map(|(_, id, _)| id.as_str());
                let ids = ids.collect::<Vec<_>>().join(", ");
                return Err(format!("commit outcome unknown for prompts {ids}").into());
            }
            if outstanding.pending.is_empty() {
                return Ok(0);
            }
        }
        tokio::select! {
            line = lines.next_line(), if open => {
                let Some(line) = line? else {
                    open = false;
                    continue;
                };
                if line.trim().is_empty() {
                    continue;
                }
                let request: Value = serde_json::from_str(&line)
                    .map_err(|error| format!("invalid JSONL request: {error}"))?;
                outstanding.resend(conversation, output).await?;
                if let Some(prompt) = request.get("prompt").and_then(Value::as_str) {
                    sent += 1;
                    let id = request
                        .get("id")
                        .and_then(Value::as_str)
                        .map_or_else(|| format!("jsonl-{sent}"), str::to_owned);
                    let message = format!("{operation}:{id}");
                    outstanding
                        .send(conversation, output, message, id, prompt.to_owned())
                        .await?;
                } else if let Some(request_id) = request.get("approve").and_then(Value::as_str) {
                    let approved = request.get("approved").and_then(Value::as_bool).unwrap_or(false);
                    conversation
                        .decide(&IntentId::try_from(request_id)?, approved)
                        .await?;
                } else if request.get("cancel").is_some() {
                    conversation.cancel().await?;
                } else {
                    return Err(format!("unknown JSONL request: {line}").into());
                }
            }
            event = events.next() => match event? {
                // Each entry commits with a status, which settles it below.
                Event::Turn(_) | Event::Output => {}
                Event::Status(status) => {
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
                            Some(line) => emit(output, conversation.task(), &line).await?,
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
                            emit(output, conversation.task(), &json!({"id": id, "inbox": inbox, "error": reason})).await?;
                        }
                        outstanding.finished = Some(reason);
                    }
                    if let Phase::WaitingApproval { id } = &status.phase
                        && announced.as_ref() != Some(id)
                    {
                        emit(output, conversation.task(), &json!({"approval": id.as_str()})).await?;
                        announced = Some(id.clone());
                    }
                    // A status is the stream's event for a prompt of
                    // unknown outcome: resend it, answered once committed.
                    outstanding.resend(conversation, output).await?;
                }
            },
        }
    }
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
            emit(&mut output, task, &line).await?;
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
        let dispatch =
            tracing::Dispatch::new(crate::lifecycle::subscriber(&report, Some("off".into())));
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
            let task = OperationId::from_bytes([19; 16]);
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
                        emit(&mut output, task, &line).await
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
            let ((result, report_result), (owner, reader, _terminate)) = tokio::join!(
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
                    emit(&mut output, task, &line).await
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
