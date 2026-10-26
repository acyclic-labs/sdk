//! Headless conversations: one `-p` prompt, or JSONL requests on stdin.
use crate::agent::{Conversation, Event, ended};
use acyclic_harness::agent::{IntentId, Phase};
use futures::StreamExt as _;
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
    let mut events = conversation.events(0).await?;
    let inbox = conversation.send(operation, prompt).await?;
    let mut answers = stdin_lines();
    // Entries observed, and the conversation tail once the agent consumed the
    // message: no entry for it by then means it was cancelled.
    let mut seen = 0_u64;
    let mut consumed = None;
    while let Some(event) = events.next().await {
        match event? {
            Event::Turn {
                inbox: Some(settled),
                entry,
            } if settled == inbox => {
                let (_, text) = conversation.turn(&entry).await?;
                writeln!(output, "{text}")?;
                return Ok(0);
            }
            Event::Turn { .. } => seen += 1,
            Event::Failed(error) => {
                eprintln!("graphcoder: agent turn failed: {error}");
                return Ok(1);
            }
            Event::Status(status) => {
                if let Some(reason) = ended(&status) {
                    eprintln!("graphcoder: {reason}");
                    return Ok(1);
                }
                if status.phase == Phase::Idle && status.cursor > inbox {
                    consumed = Some(conversation.settled().await?);
                }
                if let Phase::WaitingApproval { id } = status.phase {
                    eprint!("graphcoder: approve request {}? [y/N] ", id.as_str());
                    let answer = answers.next_line().await?.unwrap_or_default();
                    let approved = matches!(answer.trim(), "y" | "Y" | "yes");
                    conversation.decide(&id, approved).await?;
                }
            }
        }
        if consumed.is_some_and(|tail| seen >= tail) {
            eprintln!("graphcoder: the prompt was cancelled");
            return Ok(1);
        }
    }
    Err("agent status follow ended".into())
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
    let mut events = conversation.events(0).await?;
    let mut lines = stdin_lines();
    let mut open = true;
    let mut sent = 0_u64;
    // Unsettled prompts by inbox sequence, and every settled turn by its
    // message, so a repeated or replayed prompt finds its earlier turn.
    let mut pending = BTreeMap::new();
    let mut settled = BTreeMap::new();
    // Conversation entries observed; a message the agent consumed (its
    // cursor passed it) with no entry by the conversation tail as of that
    // status was cancelled.
    let mut seen = 0_u64;
    let mut consumed = None;
    let mut failure = None;
    let mut status = None;
    let mut announced = None;
    loop {
        // Settle every pending prompt the observed turns and status answer.
        for (inbox, id) in std::mem::take(&mut pending) {
            let cancelled = consumed.is_some_and(|(cursor, tail)| inbox < cursor && seen >= tail);
            let line = if let Some(entry) = settled.get(&inbox) {
                let (_, text) = conversation.turn(entry).await?;
                json!({"id": id, "inbox": inbox, "text": text})
            } else if let Some(reason) = status.as_ref().and_then(ended) {
                json!({"id": id, "inbox": inbox, "error": reason})
            } else if cancelled {
                json!({"id": id, "inbox": inbox, "error": "cancelled"})
            } else if let Some(error) = &failure {
                json!({"id": id, "inbox": inbox, "error": format!("agent turn failed: {error}")})
            } else {
                pending.insert(inbox, id);
                continue;
            };
            writeln!(output, "{line}")?;
            output.flush()?;
        }
        failure = None;
        let done = status.as_ref().and_then(ended).is_some();
        if done {
            return Ok(1);
        }
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
                    let inbox = conversation.send(&format!("{operation}:{id}"), prompt).await?;
                    pending.insert(inbox, id);
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
            event = events.next() => match event.ok_or("agent status follow ended")?? {
                Event::Turn { inbox, entry } => {
                    seen += 1;
                    if let Some(inbox) = inbox {
                        settled.insert(inbox, entry);
                    }
                }
                Event::Failed(error) => failure = Some(error),
                Event::Status(observed) => {
                    if matches!(observed.phase, Phase::Idle | Phase::Done { .. }) {
                        consumed = Some((observed.cursor, conversation.settled().await?));
                    }
                    if let Phase::WaitingApproval { id } = &observed.phase
                        && announced.as_ref() != Some(id)
                    {
                        writeln!(output, "{}", json!({"approval": id.as_str()}))?;
                        output.flush()?;
                        announced = Some(id.clone());
                    }
                    status = Some(observed);
                }
            },
        }
    }
}
