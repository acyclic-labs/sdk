use serde_json::{Value, json};
use std::io::{Read as _, Write as _};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const CLAUDE_SUBAGENT_MARKER: &str = "ACYCLIC_DETERMINISTIC_CHILD";
const CLAUDE_FAILED_CHILD: &str = "__ACYCLIC_FAILED_CHILD__";

#[derive(Clone, Copy)]
pub enum ProviderProtocol {
    Responses,
    AnthropicMessages,
}

pub struct ScriptedProvider {
    address: String,
    requests: Arc<Mutex<Vec<Value>>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl ScriptedProvider {
    pub fn start(protocol: ProviderProtocol, shell_command: &str) -> Self {
        Self::start_with_mode(protocol, shell_command, false, false)
    }

    pub fn start_stalled(protocol: ProviderProtocol) -> Self {
        Self::start_with_mode(protocol, "", true, false)
    }

    pub fn start_claude_subagent(relative_target: &str) -> Self {
        Self::start_with_mode(
            ProviderProtocol::AnthropicMessages,
            relative_target,
            false,
            true,
        )
    }

    pub fn start_claude_failed_subagent() -> Self {
        Self::start_claude_subagent(CLAUDE_FAILED_CHILD)
    }

    fn start_with_mode(
        protocol: ProviderProtocol,
        shell_command: &str,
        stalled: bool,
        claude_subagent: bool,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind scripted provider");
        listener
            .set_nonblocking(true)
            .expect("scripted provider nonblocking");
        let address = listener.local_addr().expect("provider address").to_string();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let thread_requests = Arc::clone(&requests);
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let command = shell_command.to_owned();
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => handle(
                        stream,
                        protocol,
                        &command,
                        stalled,
                        claude_subagent,
                        &thread_requests,
                        &thread_stop,
                    ),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            address,
            requests,
            stop,
            thread: Some(thread),
        }
    }

    pub fn base_url(&self) -> String {
        format!("http://{}", self.address)
    }

    pub fn wait_for_requests(&self, count: usize, timeout: Duration) -> Vec<Value> {
        let deadline = Instant::now() + timeout;
        loop {
            let requests = self.requests.lock().expect("provider requests").clone();
            if requests.len() >= count || Instant::now() >= deadline {
                return requests;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for ScriptedProvider {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ =
            TcpStream::connect(&self.address).and_then(|stream| stream.shutdown(Shutdown::Both));
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn handle(
    mut stream: TcpStream,
    protocol: ProviderProtocol,
    command: &str,
    stalled: bool,
    claude_subagent: bool,
    requests: &Mutex<Vec<Value>>,
    stop: &AtomicBool,
) {
    let Some((method, path, body)) = read_request(&mut stream) else {
        return;
    };
    if method == "GET" && path.ends_with("/models") {
        write_response(
            &mut stream,
            200,
            "application/json",
            r#"{"object":"list","data":[]}"#,
        );
        return;
    }
    if method != "POST" {
        write_response(
            &mut stream,
            426,
            "application/json",
            r#"{"error":"streaming HTTP required"}"#,
        );
        return;
    }
    let Ok(request) = serde_json::from_slice::<Value>(&body) else {
        write_response(
            &mut stream,
            400,
            "application/json",
            r#"{"error":"invalid JSON"}"#,
        );
        return;
    };
    let completed_tool = contains_type(&request, "function_call_output")
        || contains_type(&request, "apply_patch_call_output")
        || contains_type(&request, "custom_tool_call_output")
        || contains_type(&request, "tool_result");
    let child_request = request.to_string().contains(CLAUDE_SUBAGENT_MARKER);
    let offered_tools = request
        .get("tools")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    requests
        .lock()
        .expect("provider request capture")
        .push(request);
    if stalled {
        while !stop.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(5));
        }
        return;
    }
    let events = if claude_subagent && !completed_tool && !child_request {
        anthropic_named_tool_events(
            "Agent",
            &json!({
                "description":"deterministic child isolation",
                "prompt":format!("{CLAUDE_SUBAGENT_MARKER}: write the requested relative file"),
                "subagent_type":"general-purpose"
            })
            .to_string(),
        )
    } else if claude_subagent && !completed_tool && child_request {
        let failed = command == CLAUDE_FAILED_CHILD;
        let (tool, input) = if offered_tools.iter().any(|tool| tool == "PowerShell") {
            (
                "PowerShell",
                if failed {
                    json!({"command":"exit 7"})
                } else {
                    json!({"command":format!("Set-Content -NoNewline -LiteralPath '{}' -Value isolated", command.replace('\'', "''"))})
                },
            )
        } else if offered_tools.iter().any(|tool| tool == "Bash") {
            (
                "Bash",
                if failed {
                    json!({"command":"exit 7"})
                } else {
                    json!({"command":format!("printf isolated > '{}'", command.replace('\'', "'\"'\"'"))})
                },
            )
        } else {
            assert!(!failed, "Claude did not expose a shell tool");
            ("Write", json!({"file_path":command,"content":"isolated"}))
        };
        anthropic_named_tool_events(tool, &input.to_string())
    } else {
        match (protocol, completed_tool) {
            (ProviderProtocol::Responses, false) => responses_tool_events(command, requests),
            (ProviderProtocol::Responses, true) => responses_text_events(),
            (ProviderProtocol::AnthropicMessages, false) => {
                anthropic_tool_events(command, requests)
            }
            (ProviderProtocol::AnthropicMessages, true) => anthropic_text_events(),
        }
    };
    write_response(&mut stream, 200, "text/event-stream", &events);
}

fn read_request(stream: &mut TcpStream) -> Option<(String, String, Vec<u8>)> {
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .ok()?;
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut buffer).ok()?;
        if read == 0 {
            return None;
        }
        bytes.extend_from_slice(buffer.get(..read)?);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
        if bytes.len() > 1024 * 1024 {
            return None;
        }
    };
    let headers = std::str::from_utf8(bytes.get(..header_end)?).ok()?;
    let mut lines = headers.lines();
    let mut request_line = lines.next()?.split_whitespace();
    let method = request_line.next()?.to_owned();
    let path = request_line.next()?.to_owned();
    let fields = lines
        .filter_map(|line| line.split_once(':'))
        .collect::<Vec<_>>();
    let content_length = fields
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok());
    let chunked = fields.iter().any(|(name, value)| {
        name.eq_ignore_ascii_case("transfer-encoding")
            && value
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
    });
    if chunked {
        while !bytes
            .get(header_end..)
            .is_some_and(|body| body.windows(5).any(|window| window == b"0\r\n\r\n"))
        {
            let read = stream.read(&mut buffer).ok()?;
            if read == 0 {
                return None;
            }
            bytes.extend_from_slice(buffer.get(..read)?);
        }
        return Some((method, path, decode_chunked(bytes.get(header_end..)?)?));
    }
    let content_length = content_length.unwrap_or(0);
    while bytes.len() < header_end + content_length {
        let read = stream.read(&mut buffer).ok()?;
        if read == 0 {
            return None;
        }
        bytes.extend_from_slice(buffer.get(..read)?);
    }
    Some((
        method,
        path,
        bytes.get(header_end..header_end + content_length)?.to_vec(),
    ))
}

fn decode_chunked(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut cursor = 0;
    let mut decoded = Vec::new();
    loop {
        let remainder = bytes.get(cursor..)?;
        let line_end = remainder.windows(2).position(|window| window == b"\r\n")?;
        let size = std::str::from_utf8(remainder.get(..line_end)?)
            .ok()?
            .split(';')
            .next()?
            .trim();
        let size = usize::from_str_radix(size, 16).ok()?;
        cursor = cursor.checked_add(line_end + 2)?;
        if size == 0 {
            return Some(decoded);
        }
        let end = cursor.checked_add(size)?;
        decoded.extend_from_slice(bytes.get(cursor..end)?);
        if bytes.get(end..end + 2)? != b"\r\n" {
            return None;
        }
        cursor = end + 2;
    }
}

fn write_response(stream: &mut TcpStream, status: u16, content_type: &str, body: &str) {
    let reason = if status == 200 { "OK" } else { "Error" };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

fn contains_type(value: &Value, expected: &str) -> bool {
    match value {
        Value::Object(values) => {
            values.get("type").and_then(Value::as_str) == Some(expected)
                || values.values().any(|value| contains_type(value, expected))
        }
        Value::Array(values) => values.iter().any(|value| contains_type(value, expected)),
        _ => false,
    }
}

fn sse(events: impl IntoIterator<Item = Value>) -> String {
    events
        .into_iter()
        .map(|event| {
            let kind = event
                .get("type")
                .and_then(Value::as_str)
                .expect("scripted event type");
            format!("event: {kind}\ndata: {event}\n\n")
        })
        .collect()
}

fn responses_tool_events(_command: &str, _requests: &Mutex<Vec<Value>>) -> String {
    let item = json!({
        "id":"ctc_1",
        "type":"custom_tool_call",
        "call_id":"call_1",
        "name":"exec",
        "input":"const result = await tools.apply_patch(\"*** Begin Patch\\n*** Add File: codex-e2e.txt\\n+qualified\\n*** End Patch\"); text(result);",
        "status":"completed"
    });
    sse([
        json!({"type":"response.created","response":{"id":"resp_tool","object":"response","status":"in_progress","model":"gpt-5.6-sol","output":[]}}),
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
        json!({"type":"response.completed","response":{"id":"resp_tool","object":"response","status":"completed","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}),
    ])
}

fn responses_text_events() -> String {
    sse([
        json!({"type":"response.created","response":{"id":"resp_done","object":"response","status":"in_progress","model":"gpt-5.6-sol","output":[]}}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"qualified","annotations":[]}]}}),
        json!({"type":"response.completed","response":{"id":"resp_done","object":"response","status":"completed","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}),
    ])
}

fn anthropic_tool_events(target: &str, requests: &Mutex<Vec<Value>>) -> String {
    let captured = requests.lock().expect("provider request capture");
    let tool = captured
        .last()
        .and_then(|request| request.get("tools"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str))
        .find(|name| *name == "Write")
        .or_else(|| {
            captured
                .last()
                .and_then(|request| request.get("tools"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|tool| tool.get("name").and_then(Value::as_str))
                .find(|name| *name == "Bash" || *name == "PowerShell")
        })
        .unwrap_or("Write");
    let input = if tool == "Write" {
        json!({"file_path": target, "content": "qualified"})
    } else {
        json!({"command": target, "description": "write qualification sentinel"})
    }
    .to_string();
    anthropic_named_tool_events(tool, &input)
}

fn anthropic_named_tool_events(tool: &str, input: &str) -> String {
    sse([
        json!({"type":"message_start","message":{"id":"msg_tool","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":tool,"input":{}}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":input}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":1}}),
        json!({"type":"message_stop"}),
    ])
}

fn anthropic_text_events() -> String {
    sse([
        json!({"type":"message_start","message":{"id":"msg_done","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"qualified"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":1}}),
        json!({"type":"message_stop"}),
    ])
}
