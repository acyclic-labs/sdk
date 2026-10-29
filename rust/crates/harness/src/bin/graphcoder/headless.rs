//! Headless conversations: one `-p` prompt, or JSONL requests on stdin.
//! Every prompt settles through the conversation's committed reply, read
//! after subscribing and again at each committed status, which every
//! conversation entry commits with; the frontend keeps only its outstanding
//! prompts.
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
/// request `{"approval": request}`. An ended agent is terminal: it admits
/// no further input, so every prompt outstanding when it ends, and every
/// later one, is answered with its reason and never sent. Exits,
/// successfully, once stdin closes and every prompt settled, whether
/// answered or failed.
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
    // The reason the agent ended, once a status said so.
    let mut finished: Option<String> = None;
    loop {
        if !open && pending.is_empty() {
            return Ok(0);
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
                if let Some(prompt) = request.get("prompt").and_then(Value::as_str) {
                    sent += 1;
                    let id = request
                        .get("id")
                        .and_then(Value::as_str)
                        .map_or_else(|| format!("jsonl-{sent}"), str::to_owned);
                    if let Some(reason) = &finished {
                        emit(output, &json!({"id": id, "error": reason}))?;
                        continue;
                    }
                    let message = format!("{operation}:{id}");
                    // A refused prompt, such as an unknown `/name`, is its own
                    // record; the stream goes on.
                    let inbox = match conversation.send(&message, prompt).await {
                        Ok(inbox) => inbox,
                        Err(error) => {
                            emit(output, &json!({"id": id, "error": error.to_string()}))?;
                            continue;
                        }
                    };
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
            event = events.next() => match event? {
                // Each entry commits with a status, which settles it below.
                Event::Turn(_) | Event::Output => {}
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
                    // An ended agent answers every outstanding prompt with
                    // its reason; each is a record, so the stream goes on
                    // until stdin closes.
                    if let Some(reason) = ended {
                        for (inbox, (_, id)) in std::mem::take(&mut pending) {
                            emit(output, &json!({"id": id, "inbox": inbox, "error": reason}))?;
                        }
                        finished = Some(reason);
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
