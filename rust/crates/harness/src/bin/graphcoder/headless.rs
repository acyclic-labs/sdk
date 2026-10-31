//! Headless conversations: one `-p` prompt, or JSONL requests on stdin.
//! Every prompt settles through the conversation's committed reply, read
//! after subscribing and again at each committed status, which every
//! conversation entry commits with; the frontend keeps only its outstanding
//! prompts.
use acyclic_harness::agent::{ConversationEntry, IntentId, Phase, Reply};
use acyclic_harness::local::{
    Event,
    agent_conversation::{Conversation, Sent, ended},
};
use acyclic_native_runtime::lines::AsyncLines;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use tokio::io::{AsyncWrite, AsyncWriteExt as _};

type Failure = Box<dyn std::error::Error>;

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
pub async fn prompt(
    conversation: &Arc<Conversation>,
    operation: &str,
    prompt: &str,
    output: &mut (impl AsyncWrite + Unpin),
) -> Result<u8, Failure> {
    let mut events = conversation.observe(conversation.settled().await?).await?;
    let inbox = conversation.send(operation, prompt).await?;
    // Stdin is read only once an approval asks for an answer.
    let mut answers = None;
    // Subscribed first, so a reply committed after this read wakes a status.
    let mut status = None;
    loop {
        match conversation.reply(operation, inbox).await? {
            Reply::Answered { entry, .. } => return print(conversation, &entry, output).await,
            Reply::Dropped => {
                eprintln!("graphcoder: {}", conversation.dropped().await?);
                return Ok(1);
            }
            Reply::Pending => {}
        }
        if let Some(status) = status.take() {
            if let Some(reason) = ended(&status) {
                eprintln!("graphcoder: {reason}");
                return Ok(1);
            }
            if let Phase::WaitingApproval { id } = status.phase {
                eprint!("graphcoder: approve request {}? [y/N] ", id.as_str());
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

/// Prints the answer text of the prompt's settled `entry`.
async fn print(
    conversation: &Conversation,
    entry: &ConversationEntry,
    output: &mut (impl AsyncWrite + Unpin),
) -> Result<u8, Failure> {
    let (_, text) = conversation.turn(entry).await?;
    output.write_all(text.as_bytes()).await?;
    output.write_all(b"\n").await?;
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

async fn emit(output: &mut (impl AsyncWrite + Unpin), line: &Value) -> std::io::Result<()> {
    // This one caller-owned frame remains live through its original write.
    // The synchronous serializer never writes to an OS handle or a second queue.
    let mut bytes = serde_json::to_vec(line)?;
    bytes.push(b'\n');
    output.write_all(&bytes).await
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
        output: &mut (impl AsyncWrite + Unpin),
        message: String,
        id: String,
        prompt: String,
    ) -> Result<(), Failure> {
        let inbox = match conversation.deliver(&message, &prompt).await? {
            Sent::Accepted(inbox) => inbox,
            // A refused prompt, such as an unknown `/name`, is its own
            // record; the stream goes on.
            Sent::Refused(reason) => {
                emit(output, &json!({"id": id, "error": reason})).await?;
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
            (Some(line), _) => emit(output, &line).await?,
            (None, Some(reason)) => {
                emit(output, &json!({"id": id, "inbox": inbox, "error": reason})).await?;
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
        output: &mut (impl AsyncWrite + Unpin),
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
pub async fn jsonl(
    conversation: &Arc<Conversation>,
    operation: &str,
    output: &mut (impl AsyncWrite + Unpin),
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
                            Some(line) => emit(output, &line).await?,
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
                            emit(output, &json!({"id": id, "inbox": inbox, "error": reason})).await?;
                        }
                        outstanding.finished = Some(reason);
                    }
                    if let Phase::WaitingApproval { id } = &status.phase
                        && announced.as_ref() != Some(id)
                    {
                        emit(output, &json!({"approval": id.as_str()})).await?;
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
        io::Read as _,
        os::fd::AsFd as _,
        pin::Pin,
        task::Poll,
    };
    use tokio::io::{AsyncBufReadExt as _, BufReader};

    #[test]
    fn a_full_answer_pipe_keeps_report_and_shutdown_runnable() -> Result<(), Failure> {
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

            let report = WaitReport::default();
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
            let line = json!({"id": "original-answer", "text": "settled"});
            let operation = async {
                let writing = emit(&mut output, &line);
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
                assert_eq!(ack.outcome, ReportOutcome::Empty);
                assert!(ack.waits.is_empty());
                terminate.send(()).unwrap();
                owner
            };
            let ((result, diagnostic), owner) = tokio::join!(
                crate::lifecycle::observe(&report, Ok(Some(port)), shutdown, false, operation),
                host,
            );
            assert_eq!(result?, 143);
            diagnostic?;
            output.shutdown().await?;
            drop(original);
            // Cancellation accepted no answer bytes. Drain only the exact
            // previously accepted prefix, then join the original pipe EOF.
            while filled != 0 {
                let count = filled.min(chunk.len());
                let mut received = [0; 8192];
                reader.read_exact(&mut received[..count])?;
                assert_eq!(&received[..count], &chunk[..count]);
                filled -= count;
            }
            assert_eq!(reader.read(&mut [0])?, 0);
            drop(owner);
            Ok(())
        })
    }
}
