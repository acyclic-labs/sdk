//! CLI and MCP request forwarding.

use super::*;
use tracing::Instrument as _;

pub(crate) async fn send_cli_control_request(
    data: &Path,
    request: &ControlRequest,
) -> Result<Value, String> {
    let identity = service_identity(data)?;
    // Sandboxed hosts may expose the already-running local endpoint while denying the client's
    // direct view of per-user state. Probe that endpoint before attempting a filesystem-backed
    // cold start. The published marker is an instance nonce, not a binary compatibility identity.
    let ping = ping_request()?;
    for attempt in 0..10 {
        match send_control_request(data, &ping).await {
            Ok(active)
                if active.get("identity").and_then(Value::as_str) == Some(identity.as_str()) =>
            {
                return send_control_request(data, request)
                    .await
                    .map_err(|error| error.to_string());
            }
            Err(ControlRequestError::Unavailable(_)) if attempt < 9 => {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Ok(_) | Err(ControlRequestError::Unavailable(_)) => break,
            Err(error) => return Err(error.to_string()),
        }
    }
    ensure_service(data).await?;
    send_control_request(data, request)
        .await
        .map_err(|error| error.to_string())
}

pub(crate) fn control_request_from_argv(
    cwd: &Path,
    mut argv: Vec<String>,
) -> Result<ControlRequest, String> {
    let cwd = cwd.canonicalize().map_err(display)?;
    let mut selected_cwd = cwd.clone();
    if argv.first().is_some_and(|argument| argument == "-C") {
        if argv.len() < 2 {
            return Err("acyclic -C requires a path".to_owned());
        }
        selected_cwd = PathBuf::from(argv.remove(1))
            .canonicalize()
            .map_err(display)?;
        argv.remove(0);
    }
    let command = match argv.first().map(String::as_str) {
        Some("git") => ControlCommand::Git,
        Some("agents") => ControlCommand::Agents,
        Some("doctor") => ControlCommand::Doctor,
        Some("discard") => ControlCommand::Discard,
        _ => return Err("unsupported Acyclic command".to_owned()),
    };
    Ok(ControlRequest {
        version: 1,
        command,
        cwd,
        argv: argv.get(1..).unwrap_or_default().to_vec(),
        name: String::new(),
        arguments: cli_routing(selected_cwd),
    })
}

pub(crate) async fn run_rpc_proxy(
    data: &Path,
    mut reader: impl BufRead,
    mut writer: impl Write,
    commandless: bool,
) -> Result<(), String> {
    while let Some(line) = read_bounded_rpc_line(&mut reader)? {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let request: Value = serde_json::from_slice(&line).map_err(display)?;
        let Some(id) = request.get("id").cloned() else {
            continue;
        };
        let method = request.get("method").and_then(Value::as_str).unwrap_or("");
        let span = tracing::info_span!(
            target: "acyclic_plugin",
            "acyclic.plugin.rpc",
            method = obs::rpc_method(method),
            outcome = tracing::field::Empty,
        );
        let response = async { Ok::<_, String>(match method {
            "initialize" => {
                json!({"jsonrpc":"2.0","id":id,"result":{"protocolVersion":"2025-06-18","capabilities":{"tools":{"listChanged":false}},"serverInfo":{"name":"acyclic","version":env!("CARGO_PKG_VERSION")}}})
            }
            "ping" => json!({"jsonrpc":"2.0","id":id,"result":{}}),
            "tools/list" => {
                json!({"jsonrpc":"2.0","id":id,"result":{"tools":public_tools(commandless)}})
            }
            "tools/call" => {
                let params = request.get("params").cloned().unwrap_or(Value::Null);
                let name = params.get("name").and_then(Value::as_str).unwrap_or("");
                let arguments = params
                    .get("arguments")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                let forwarded = if commandless && name == "acyclic" {
                    let argv = arguments
                        .get("argv")
                        .and_then(Value::as_array)
                        .ok_or_else(|| "acyclic requires an argv string array".to_owned())?
                        .iter()
                        .map(|value| {
                            value
                                .as_str()
                                .map(str::to_owned)
                                .ok_or_else(|| "acyclic argv entries must be strings".to_owned())
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    control_request_from_argv(&env::current_dir().map_err(display)?, argv)?
                } else {
                    return Err("this Acyclic MCP endpoint does not expose that tool".to_owned());
                };
                let result = send_control_request(data, &forwarded).await;
                obs::record_outcome(&result);
                match result {
                    Ok(result) => {
                        json!({"jsonrpc":"2.0","id":id,"result":{"content":[{"type":"text","text":serde_json::to_string(&result).map_err(display)?}]}})
                    }
                    Err(error) => {
                        json!({"jsonrpc":"2.0","id":id,"result":{"isError":true,"content":[{"type":"text","text":error.to_string()}]}})
                    }
                }
            }
            _ => {
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"method not found"}})
            }
        })}
        .instrument(span)
        .await?;
        serde_json::to_writer(&mut writer, &response).map_err(display)?;
        writer.write_all(b"\n").map_err(display)?;
        writer.flush().map_err(display)?;
    }
    Ok(())
}

pub(crate) fn read_bounded_rpc_line(reader: &mut impl BufRead) -> Result<Option<Vec<u8>>, String> {
    let mut line = Vec::new();
    let read = reader
        .take((MAXIMUM_CONTROL_MESSAGE_BYTES + 1) as u64)
        .read_until(b'\n', &mut line)
        .map_err(display)?;
    if read == 0 {
        return Ok(None);
    }
    if line.len() > MAXIMUM_CONTROL_MESSAGE_BYTES {
        return Err("Acyclic MCP request exceeds the maximum frame size".to_owned());
    }
    if line.last() == Some(&b'\n') {
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
    }
    Ok(Some(line))
}
