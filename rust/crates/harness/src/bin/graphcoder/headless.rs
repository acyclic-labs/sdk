//! Headless conversations: one `-p` prompt, or JSONL requests on stdin.
//! Every prompt settles through the conversation's committed reply; the
//! frontend keeps only its outstanding prompts.
use acyclic_harness::agent::{ConversationEntry, IntentId, Phase, Reply};
use acyclic_harness::local::{
    Event,
    agent_conversation::{Conversation, ended},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, io::Write, sync::Arc};
use tokio::io::{AsyncBufReadExt as _, BufReader};

type Failure = Box<dyn std::error::Error>;

fn stdin_lines() -> tokio::io::Lines<BufReader<tokio::io::Stdin>> {
    BufReader::new(tokio::io::stdin()).lines()
}

/// Send `prompt` as message `operation` and print its answer; rerunning the
/// operation replays it. An approval request is answered on stdin (`y`
/// approves); the exit code is 0 once the answer is printed.
pub async fn prompt(
    conversation: &Arc<Conversation>,
    operation: &str,
    prompt: &str,
    output: &mut impl Write,
) -> Result<u8, Failure> {
    let mut events = conversation.observe(conversation.settled().await?).await?;
    let inbox = conversation.send(operation, prompt).await?;
    let mut answers = stdin_lines();
    // An exact replay of a settled prompt is answered at once.
    let mut reply = conversation.reply(operation, inbox).await?;
    loop {
        match reply {
            Reply::Answered { entry, .. } => return print(conversation, &entry, output).await,
            Reply::Dropped => {
                eprintln!("graphcoder: {}", conversation.dropped().await?);
                return Ok(1);
            }
            Reply::Pending => {}
        }
        match acyclic_native_runtime::obs::waiting("stream-append", operation, events.next())
            .await?
        {
            Event::Turn(entry) if entry.inbox == inbox => {
                return print(conversation, &entry, output).await;
            }
            Event::Turn(_) | Event::Output => {}
            Event::Failed(error) => {
                eprintln!("graphcoder: agent turn failed: {error}");
                return Ok(1);
            }
            Event::Status(status) => {
                if status.cursor > inbox {
                    reply = conversation.reply(operation, inbox).await?;
                    if !matches!(reply, Reply::Pending) {
                        continue;
                    }
                }
                if let Some(reason) = ended(&status) {
                    eprintln!("graphcoder: {reason}");
                    return Ok(1);
                }
                if let Phase::WaitingApproval { id } = status.phase {
                    eprint!("graphcoder: approve request {}? [y/N] ", id.as_str());
                    let answer = acyclic_native_runtime::obs::waiting(
                        "stdin-read",
                        id.as_str(),
                        answers.next_line(),
                    )
                    .await?
                    .unwrap_or_default();
                    let approved = matches!(answer.trim(), "y" | "Y" | "yes");
                    conversation.decide(&id, approved).await?;
                }
            }
        }
    }
}

/// Prints the answer text of the prompt's settled `entry`.
async fn print(
    conversation: &Conversation,
    entry: &ConversationEntry,
    output: &mut impl Write,
) -> Result<u8, Failure> {
    let (_, text) = conversation.turn(entry).await?;
    writeln!(output, "{text}")?;
    Ok(0)
}

/// The printed answer of prompt `id`, settled as `entry`.
async fn answer(
    conversation: &Conversation,
    id: &str,
    entry: &ConversationEntry,
) -> Result<Value, Failure> {
    let (_, text) = conversation.turn(entry).await?;
    Ok(json!({"id": id, "inbox": entry.inbox, "text": text}))
}

/// The printed line of prompt `id` at `inbox` once `reply` settled it.
async fn settled(
    conversation: &Conversation,
    id: &str,
    inbox: u64,
    reply: Reply,
) -> Result<Option<Value>, Failure> {
    Ok(match reply {
        Reply::Answered { entry, .. } => Some(answer(conversation, id, &entry).await?),
        Reply::Dropped => {
            Some(json!({"id": id, "inbox": inbox, "error": conversation.dropped().await?}))
        }
        Reply::Pending => None,
    })
}

fn emit(output: &mut impl Write, line: &Value) -> std::io::Result<()> {
    writeln!(output, "{line}")?;
    output.flush()
}

/// One JSONL request per stdin line: `{"prompt": text, "id"?: id}`,
/// `{"approve": request, "approved": bool}` or `{"cancel": true}`. A prompt is
/// message `operation:id`, so rerunning the operation replays it. Each settled
/// prompt prints `{"id", "inbox", "text"}` (or `"error"`), each approval
/// request `{"approval": request}`. Exits once stdin closes and every prompt
/// settled.
pub async fn jsonl(
    conversation: &Arc<Conversation>,
    operation: &str,
    output: &mut impl Write,
) -> Result<u8, Failure> {
    let mut events = conversation.observe(conversation.settled().await?).await?;
    let mut lines = stdin_lines();
    let mut open = true;
    let mut sent = 0_u64;
    // Outstanding prompts by inbox sequence: message id and display id.
    let mut pending = BTreeMap::<u64, (String, String)>::new();
    let mut announced = None;
    loop {
        if !open && pending.is_empty() {
            return Ok(0);
        }
        tokio::select! {
            line = acyclic_native_runtime::obs::waiting("stdin-read", operation, lines.next_line()), if open => {
                let Some(line) = line? else {
                    open = false;
                    continue;
                };
                if line.trim().is_empty() {
                    continue;
                }
                let request: Value = serde_json::from_str(&line)
                    .map_err(|error| format!("invalid JSONL request: {error}"))?;
                if let Some(prompt) = request.get("prompt").and_then(Value::as_str) {
                    sent += 1;
                    let id = request
                        .get("id")
                        .and_then(Value::as_str)
                        .map_or_else(|| format!("jsonl-{sent}"), str::to_owned);
                    let message = format!("{operation}:{id}");
                    let inbox = conversation.send(&message, prompt).await?;
                    // An exact replay of a settled prompt is answered at once.
                    let reply = conversation.reply(&message, inbox).await?;
                    match settled(conversation, &id, inbox, reply).await? {
                        Some(line) => emit(output, &line)?,
                        None => {
                            pending.insert(inbox, (message, id));
                        }
                    }
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
            event = acyclic_native_runtime::obs::waiting("stream-append", operation, events.next()) => match event? {
                Event::Turn(entry) => {
                    if let Some((_, id)) = pending.remove(&entry.inbox) {
                        emit(output, &answer(conversation, &id, &entry).await?)?;
                    }
                }
                Event::Failed(error) => {
                    let error = format!("agent turn failed: {error}");
                    for (inbox, (_, id)) in std::mem::take(&mut pending) {
                        emit(output, &json!({"id": id, "inbox": inbox, "error": error}))?;
                    }
                }
                Event::Output => {}
                Event::Status(status) => {
                    let ended = ended(&status);
                    // Prompts the agent consumed settle from its committed reply.
                    let later = pending.split_off(&status.cursor);
                    for (inbox, (message, id)) in std::mem::replace(&mut pending, later) {
                        let reply = conversation.reply(&message, inbox).await?;
                        let line = match (&ended, reply) {
                            (Some(reason), Reply::Pending | Reply::Dropped) => {
                                Some(json!({"id": id, "inbox": inbox, "error": reason}))
                            }
                            (_, reply) => settled(conversation, &id, inbox, reply).await?,
                        };
                        match line {
                            Some(line) => emit(output, &line)?,
                            None => {
                                pending.insert(inbox, (message, id));
                            }
                        }
                    }
                    if let Some(reason) = ended {
                        for (inbox, (_, id)) in std::mem::take(&mut pending) {
                            emit(output, &json!({"id": id, "inbox": inbox, "error": reason}))?;
                        }
                        return Ok(1);
                    }
                    if let Phase::WaitingApproval { id } = &status.phase
                        && announced.as_ref() != Some(id)
                    {
                        emit(output, &json!({"approval": id.as_str()}))?;
                        announced = Some(id.clone());
                    }
                }
            },
        }
    }
}
