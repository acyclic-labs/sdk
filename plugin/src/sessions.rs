//! MCP coordination shim. Native CLIs own agent loops, tools and policy; the
//! harness's [`NativeChildren`] owns their managed children. This adapter only
//! maps MCP tool calls onto it and parks waits until their child is idle.
use super::*;
use acyclic_harness::local::host_process::children::NativeChildren;
use std::{io::Write as _, sync::mpsc, time::Duration};

/// Waits parked at once, bounded like the host's request concurrency.
const WAITS: usize = 16;

/// One coordination tool call; `None` parks a wait until its child is idle.
fn call(children: &mut NativeChildren, name: &str, args: &Value) -> Result<Option<Value>> {
    let object = args.as_object().ok_or("tool arguments must be an object")?;
    if object
        .keys()
        .any(|key| !matches!(key.as_str(), "reference" | "prompt"))
    {
        return Err("unsupported coordination argument".into());
    }
    let reference = object
        .get("reference")
        .and_then(Value::as_str)
        .ok_or("reference is required")?;
    let prompt = || {
        object
            .get("prompt")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or("prompt is required")
    };
    Ok(match name {
        "spawn" => Some(children.spawn(reference, prompt()?)?),
        "send" => Some(children.send(reference, prompt()?)?),
        "resume" => Some(children.resume(reference, prompt()?)?),
        "archive" => Some(children.archive(reference)?),
        "cancel" => Some(children.cancel(reference)?),
        "wait" => children.wait(reference)?,
        _ => return Err("unknown coordination tool".into()),
    })
}

/// Answers every parked wait on `reference` once its child is idle.
fn resolve(
    children: &NativeChildren,
    reference: &str,
    waiting: &mut Vec<(Value, String)>,
) -> Result<()> {
    if !waiting.iter().any(|(_, pending)| pending == reference) {
        return Ok(());
    }
    if let Some(value) = children.wait(reference)? {
        for (id, _) in waiting.extract_if(.., |(_, pending)| pending == reference) {
            reply(&id, Ok(value.clone()))?;
        }
    }
    Ok(())
}

fn reply(id: &Value, result: Result<Value>) -> Result<()> {
    let result = match result {
        Ok(value) => json!({"content":[{"type":"text","text":value.to_string()}]}),
        Err(error) => json!({"isError":true,"content":[{"type":"text","text":error.to_string()}]}),
    };
    emit(&json!({"jsonrpc":"2.0","id":id,"result":result}))
}
fn emit(value: &Value) -> Result<()> {
    let mut output = io::stdout().lock();
    serde_json::to_writer(&mut output, value)?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}

fn input_messages() -> mpsc::Receiver<io::Result<Value>> {
    let (send, receive) = mpsc::sync_channel(16);
    std::thread::spawn(move || {
        let input = io::stdin();
        for message in acyclic_harness::mcp::stdio::JsonLineReader::new(input.lock(), 65_536) {
            if send.send(message).is_err() {
                break;
            }
        }
    });
    receive
}

pub(super) fn serve(host: &str) -> Result<()> {
    let receive = input_messages();
    let mut children: Option<NativeChildren> = None;
    let mut waiting: Vec<(Value, String)> = Vec::new();
    let serving = (|| -> Result<()> {
        loop {
            let (mut running, mut progressed) = (false, false);
            if let Some(owner) = children.as_mut() {
                let (flowed, settled) = owner.pump()?;
                for reference in settled {
                    resolve(owner, &reference, &mut waiting)?;
                }
                progressed = flowed;
                running = owner.running();
            }
            // Only this owner's own processes can settle a wait, so with none
            // running it blocks on its host. Ready output schedules another
            // bounded pass; idle waiting cannot throttle a backpressured pipe.
            let message = if running {
                receive.recv_timeout(if progressed {
                    Duration::ZERO
                } else {
                    Duration::from_millis(20)
                })
            } else {
                receive
                    .recv()
                    .map_err(|_| mpsc::RecvTimeoutError::Disconnected)
            };
            let message = match message {
                Ok(Ok(value)) => value,
                Ok(Err(_)) => {
                    emit(
                        &json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"invalid JSON"}}),
                    )?;
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(_) => break,
            };
            let params = message.get("params").unwrap_or(&Value::Null);
            if message.get("method").and_then(Value::as_str) == Some("notifications/cancelled") {
                waiting.retain(|(id, _)| Some(id) != params.get("requestId"));
                continue;
            }
            let Some(id) = message.get("id") else {
                continue;
            };
            match message.get("method").and_then(Value::as_str) {
                Some("initialize") => emit(
                    &json!({"jsonrpc":"2.0","id":id,"result":{"protocolVersion":params.get("protocolVersion").and_then(Value::as_str).unwrap_or("2025-06-18"),"capabilities":{"tools":{}},"serverInfo":{"name":"acyclic-sessions","version":env!("CARGO_PKG_VERSION")}}}),
                )?,
                Some("ping") => emit(&json!({"jsonrpc":"2.0","id":id,"result":{}}))?,
                Some("tools/list") => emit(&json!({"jsonrpc":"2.0","id":id,"result":catalog()}))?,
                Some("tools/call") => {
                    let name = params
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let args = params.get("arguments").unwrap_or(&Value::Null);
                    let result = (|| -> Result<Option<Value>> {
                        if children.is_none() {
                            children = Some(NativeChildren::open(host)?);
                        }
                        let owner = children.as_mut().ok_or("session owner absent")?;
                        let result = call(owner, name, args)?;
                        if matches!(name, "archive" | "cancel")
                            && let Some(reference) = args.get("reference").and_then(Value::as_str)
                        {
                            resolve(owner, reference, &mut waiting)?;
                        }
                        Ok(result)
                    })();
                    match result {
                        Ok(None) if waiting.len() >= WAITS => {
                            reply(id, Err("native wait concurrency bound reached".into()))?;
                        }
                        Ok(None) => waiting.push((
                            id.clone(),
                            args.get("reference")
                                .and_then(Value::as_str)
                                .ok_or("reference absent")?
                                .into(),
                        )),
                        Ok(Some(value)) => reply(id, Ok(value))?,
                        Err(error) => reply(id, Err(error))?,
                    }
                }
                _ => emit(
                    &json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"method not found"}}),
                )?,
            }
        }
        Ok(())
    })();
    if let Some(mut children) = children {
        children.shutdown()?;
    }
    serving
}

fn catalog() -> Value {
    json!({"tools":(["spawn","send","wait","archive","cancel","resume"].map(|name| {
        let prompt=matches!(name,"spawn"|"send"|"resume");
        json!({"name":name,"description":format!("{name} a separate native session in an admitted harness view. Native tools and policy are preserved. Native subagents are unmanaged."),"inputSchema":{"type":"object","properties":{"reference":{"type":"string"},"prompt":{"type":"string"}},"required":if prompt {vec!["reference","prompt"]} else {vec!["reference"]},"additionalProperties":false}})
    }))})
}
