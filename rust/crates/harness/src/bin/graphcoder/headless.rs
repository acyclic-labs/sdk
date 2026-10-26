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
    let inbox = conversation.send(operation, prompt).await?;
    let mut events = conversation.events(0).await?;
    let mut answers = stdin_lines();
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
            Event::Turn { .. } => {}
            Event::Status(status) => {
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
    // Unsettled prompts by inbox sequence; settled turns no prompt awaits yet,
    // so a replayed prompt finds its earlier turn.
    let mut pending = BTreeMap::new();
    let mut settled = BTreeMap::new();
    let mut status = None;
    let mut announced = None;
    loop {
        // Settle every pending prompt the observed turns and status answer.
        for (inbox, id) in std::mem::take(&mut pending) {
            let line = if let Some(entry) = settled.remove(&inbox) {
                let (_, text) = conversation.turn(&entry).await?;
                json!({"id": id, "inbox": inbox, "text": text})
            } else if let Some(reason) = status.as_ref().and_then(ended) {
                json!({"id": id, "inbox": inbox, "error": reason})
            } else {
                pending.insert(inbox, id);
                continue;
            };
            writeln!(output, "{line}")?;
            output.flush()?;
        }
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
                Event::Turn { inbox: Some(inbox), entry } => {
                    settled.insert(inbox, entry);
                }
                Event::Turn { .. } => {}
                Event::Status(observed) => {
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
