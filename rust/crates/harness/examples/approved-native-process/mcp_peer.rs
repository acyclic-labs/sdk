//! MCP peer protocol for the approved process example.
use super::*;

pub(super) fn verify_result(
    request: &NativeProcessRequest,
    output: &NativeProcessResult,
    lost_response: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    if lost_response {
        if !output.stop.as_ref().is_some_and(|stop| {
            stop.kind == NativeProcessStopKind::Timeout && stop.cleanup_completed
        }) || !matches!(request.mcp_response(output), Err(acyclic_harness::Error::Indeterminate(operation)) if operation == OperationId::from_bytes([32; 16]))
        {
            return Err(
                "lost MCP response became authoritative or cleanup was not confirmed".into(),
            );
        }
    } else if request
        .mcp_stdio
        .as_ref()
        .is_some_and(|request| request.method == "tools/list")
    {
        let page: rmcp::model::ListToolsResult =
            serde_json::from_value(request.mcp_response(output)?)?;
        if page.next_cursor.is_some()
            || page.tools.len() != 1
            || page.tools.first().is_none_or(|tool| {
                tool.name != "echo"
                    || tool.description.as_deref() != Some("héllo")
                    || tool.input_schema.as_ref() != json!({"type":"object"}).as_object().unwrap()
            })
        {
            return Err("incorrect MCP discovery page".into());
        }
    } else if let Some(descriptor) = request
        .mcp_stdio
        .as_ref()
        .filter(|descriptor| !matches!(descriptor.method.as_str(), "tools/call" | "tools/list"))
    {
        let value = request.mcp_response(output)?;
        let expected = match descriptor.method.as_str() {
            "resources/list" => json!({"resources":[{"uri":"example://content","name":"content"}]}),
            "resources/templates/list" => {
                json!({"resourceTemplates":[{"uriTemplate":"example://{name}","name":"named"}]})
            }
            "resources/read" => json!({"contents":[{"uri":"example://content","text":"héllo"}]}),
            "prompts/list" => json!({"prompts":[{"name":"example"}]}),
            "prompts/get" => {
                json!({"messages":[{"role":"user","content":{"type":"text","text":"héllo"}}]})
            }
            _ => return Err("service selection changed".into()),
        };
        if value != expected {
            return Err("incorrect MCP resource/prompt result".into());
        }
    } else if request.mcp_response(output)?
        != json!({
            "content":[{"type":"text","text":"héllo"}],
            "structuredContent":{"text":"héllo","sequence":MAX_EXACT_JS_INTEGER}
        })
    {
        return Err("incorrect MCP process result".into());
    }
    Ok(())
}

fn mcp_peer_result(
    call: &Value,
) -> std::result::Result<(Value, &'static [u8]), Box<dyn std::error::Error>> {
    match call.get("method").and_then(Value::as_str) {
        Some("tools/list") if call.get("params") == Some(&json!({"cursor":"next"})) => Ok((
            json!({"tools":[{"name":"echo","description":"héllo","inputSchema":{"type":"object"}}]}),
            b"list\n",
        )),
        Some("tools/call")
            if call.pointer("/params/name").and_then(Value::as_str) == Some("echo")
                && call.pointer("/params/arguments")
                    == Some(&json!({"text":"héllo","sequence":MAX_EXACT_JS_INTEGER})) =>
        {
            Ok((
                json!({"content":[{"type":"text","text":"héllo"}],"structuredContent":call.pointer("/params/arguments")}),
                b"call\n",
            ))
        }
        Some("resources/list") if call.get("params") == Some(&json!({})) => Ok((
            json!({"resources":[{"uri":"example://content","name":"content"}]}),
            b"resources\n",
        )),
        Some("resources/templates/list") if call.get("params") == Some(&json!({})) => Ok((
            json!({"resourceTemplates":[{"uriTemplate":"example://{name}","name":"named"}]}),
            b"templates\n",
        )),
        Some("resources/read")
            if call.get("params") == Some(&json!({"uri":"example://content"})) =>
        {
            Ok((
                json!({"contents":[{"uri":"example://content","text":"héllo"}]}),
                b"read\n",
            ))
        }
        Some("prompts/list") if call.get("params") == Some(&json!({})) => {
            Ok((json!({"prompts":[{"name":"example"}]}), b"prompts\n"))
        }
        Some("prompts/get")
            if call.get("params") == Some(&json!({"name":"example","arguments":{}})) =>
        {
            Ok((
                json!({"messages":[{"role":"user","content":{"type":"text","text":"héllo"}}]}),
                b"prompt\n",
            ))
        }
        _ => Err("incorrect pinned MCP request".into()),
    }
}

pub(super) fn child(lost_response: bool) -> std::result::Result<(), Box<dyn std::error::Error>> {
    use bytes::BytesMut;
    use rmcp::transport::async_rw::JsonRpcMessageCodec;
    use std::io::{BufRead as _, Read as _, Write as _};
    use tokio_util::codec::{Decoder as _, Encoder as _};
    let maximum: u32 = acyclic_harness::test_support::environment("MCP_MAX_BYTES")
        .and_then(|value| value.into_string().ok())
        .ok_or("missing explicit child input")?
        .parse()?;
    if maximum == 0
        || acyclic_harness::test_support::environment("APPROVED_TOKEN")
            .and_then(|value| value.into_string().ok())
            .ok_or("missing explicit child input")?
            != "exact-token"
    {
        return Err("invalid explicit server input".into());
    }
    let mut input = std::io::stdin().lock();
    let mut codec = JsonRpcMessageCodec::<Value>::new_with_max_length(maximum as usize);
    let mut read = || -> std::result::Result<Value, Box<dyn std::error::Error>> {
        let mut line = Vec::new();
        input
            .by_ref()
            .take(u64::from(maximum) + 1)
            .read_until(b'\n', &mut line)?;
        if line.len() > maximum as usize || !line.ends_with(b"\n") {
            return Err("invalid bounded client frame".into());
        }
        codec
            .decode(&mut BytesMut::from(line.as_slice()))?
            .ok_or_else(|| "missing client message".into())
    };
    let write = |value: Value| -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut bytes = BytesMut::new();
        JsonRpcMessageCodec::new().encode(value, &mut bytes)?;
        std::io::stdout().write_all(&bytes)?;
        std::io::stdout().flush()?;
        Ok(())
    };
    let initialization = read()?;
    if initialization.get("method").and_then(Value::as_str) != Some("initialize")
        || initialization
            .pointer("/params/protocolVersion")
            .and_then(Value::as_str)
            != Some(PROTOCOL_VERSION)
    {
        return Err("incorrect client initialization".into());
    }
    let initialization_id = initialization
        .get("id")
        .ok_or("missing initialization id")?;
    write(json!({"jsonrpc":"2.0","id":initialization_id,"result":{
        "protocolVersion":PROTOCOL_VERSION,"capabilities":{"tools":{}},
        "serverInfo":{"name":"example.echo","version":"1"}}}))?;
    if read()?.get("method").and_then(Value::as_str) != Some("notifications/initialized") {
        return Err("tool called before initialized notification".into());
    }
    let call = read()?;
    let (result, trace) = mcp_peer_result(&call)?;
    std::fs::write("destination/output.txt", std::fs::read("source/input.txt")?)?;
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("destination/calls.txt")?
        .write_all(trace)?;
    let operation = call.get("id").ok_or("missing tool operation")?;
    if !lost_response {
        write(json!({"jsonrpc":"2.0","id":operation,"result":result}))?;
    }
    let mut byte = [0];
    let _ = input.read(&mut byte)?;
    Ok(())
}
