use serde_json::{Value, json};
use std::io::{Read as _, Write as _};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const CLAUDE_SUCCESS_MARKER: &str = "ACYCLIC_SUCCESS_CHILD";
const CLAUDE_FAILURE_MARKER: &str = "ACYCLIC_FAILURE_CHILD";
const CODEX_WORKFLOW_MARKER: &str = "ACYCLIC_CODEX_WORKFLOW_CHILD";
const CODEX_CHILD_ROLE_MARKER: &str = "immediately delivered back to your parent agent";
const OVERLAY_WORKFLOW_MARKER: &str = "ACYCLIC_OVERLAY_WORKFLOW_OK";
const ISOLATION_DENIAL: &str = "Acyclic denied the tool because workspace isolation failed";

#[derive(Clone)]
enum Scenario {
    CodexLifecycle {
        root_target: String,
        child_target: String,
    },
    Stalled,
    ClaudeLifecycle {
        root_target: String,
        child_target: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RequestFingerprint {
    CodexRootPrompt,
    CodexRootToolResult,
    CodexRootSpawnResult,
    CodexChildPrompt,
    CodexChildToolResult,
    ClaudeRootPrompt,
    ClaudeRootContinuation,
    ClaudeRootToolResult,
    ClaudeRootFailedChildResult,
    ClaudeSuccessChildPrompt,
    ClaudeSuccessChildResult,
    ClaudeFailureChildPrompt,
    ClaudeFailureChildResult,
    InvalidToolResult,
}

pub struct ScriptedProvider {
    address: String,
    requests: Arc<(Mutex<Vec<Value>>, Condvar)>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl ScriptedProvider {
    pub fn start_codex(root_target: &str, child_target: &str) -> Self {
        Self::start_scenario(Scenario::CodexLifecycle {
            root_target: root_target.to_owned(),
            child_target: child_target.to_owned(),
        })
    }

    pub fn start_stalled() -> Self {
        Self::start_scenario(Scenario::Stalled)
    }

    pub fn start_claude_lifecycle(root_target: &str, child_target: &str) -> Self {
        Self::start_scenario(Scenario::ClaudeLifecycle {
            root_target: root_target.to_owned(),
            child_target: child_target.to_owned(),
        })
    }

    fn start_scenario(scenario: Scenario) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind scripted provider");
        let address = listener.local_addr().expect("provider address").to_string();
        let requests = Arc::new((Mutex::new(Vec::new()), Condvar::new()));
        let thread_requests = Arc::clone(&requests);
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let scenario = Arc::new(scenario);
        let thread = thread::spawn(move || {
            let mut handlers = Vec::new();
            while let Ok((stream, _)) = listener.accept() {
                if thread_stop.load(Ordering::Acquire) {
                    break;
                }
                let scenario = Arc::clone(&scenario);
                let requests = Arc::clone(&thread_requests);
                let stop = Arc::clone(&thread_stop);
                handlers.push(thread::spawn(move || {
                    handle(stream, &scenario, &requests, &stop);
                }));
            }
            for handler in handlers {
                let _ = handler.join();
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
        let (requests, changed) = &*self.requests;
        let requests = requests.lock().expect("provider requests");
        let (requests, _) = changed
            .wait_timeout_while(requests, timeout, |requests| requests.len() < count)
            .expect("wait for provider requests");
        requests.clone()
    }

    pub fn semantic_fingerprints(
        &self,
        count: usize,
        timeout: Duration,
    ) -> Vec<RequestFingerprint> {
        self.wait_for_requests(count, timeout)
            .iter()
            .map(request_fingerprint)
            .collect()
    }
}

impl Drop for ScriptedProvider {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.requests.1.notify_all();
        let _ =
            TcpStream::connect(&self.address).and_then(|stream| stream.shutdown(Shutdown::Both));
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one request transaction selects and emits the exact scripted response"
)]
fn handle(
    mut stream: TcpStream,
    scenario: &Scenario,
    requests: &(Mutex<Vec<Value>>, Condvar),
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
    let fingerprint = request_fingerprint(&request);
    let spawn_denied = contains_text(&request, ISOLATION_DENIAL);
    let offered_tools = request
        .get("tools")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    {
        let mut captured = requests.0.lock().expect("provider request capture");
        captured.push(request);
        requests.1.notify_all();
    }
    if matches!(scenario, Scenario::Stalled) {
        let mut captured = requests.0.lock().expect("provider request capture");
        while !stop.load(Ordering::Acquire) {
            captured = requests
                .1
                .wait(captured)
                .expect("wait for stalled provider shutdown");
        }
        return;
    }
    let events = match (scenario, fingerprint) {
        (
            Scenario::ClaudeLifecycle {
                root_target,
                child_target: _,
            },
            RequestFingerprint::ClaudeRootPrompt,
        ) => anthropic_lifecycle_events(root_target),
        (
            Scenario::ClaudeLifecycle { child_target, .. },
            RequestFingerprint::ClaudeSuccessChildPrompt,
        ) => match claude_child_tool_events(Some(child_target), &offered_tools) {
            Some(events) => events,
            None => return write_conflict(&mut stream, "Claude did not expose a shell tool"),
        },
        (Scenario::ClaudeLifecycle { .. }, RequestFingerprint::ClaudeFailureChildPrompt) => {
            match claude_child_tool_events(None, &offered_tools) {
                Some(events) => events,
                None => return write_conflict(&mut stream, "Claude did not expose a shell tool"),
            }
        }
        (Scenario::ClaudeLifecycle { .. }, RequestFingerprint::ClaudeRootToolResult) => {
            if spawn_denied {
                anthropic_text_events()
            } else {
                match wait_for_fingerprint_or_isolation_denial(
                    requests,
                    stop,
                    RequestFingerprint::ClaudeSuccessChildResult,
                ) {
                    WaitOutcome::Expected => anthropic_failed_agent_events(),
                    WaitOutcome::IsolationDenied => anthropic_text_events(),
                    WaitOutcome::Stopped => {
                        return write_conflict(&mut stream, "successful child did not complete");
                    }
                }
            }
        }
        (Scenario::ClaudeLifecycle { .. }, RequestFingerprint::ClaudeRootFailedChildResult) => {
            if !wait_for_fingerprint(
                requests,
                stop,
                RequestFingerprint::ClaudeFailureChildResult,
                Duration::from_secs(10),
                false,
            ) {
                return write_conflict(&mut stream, "failing child did not complete with an error");
            }
            anthropic_text_events()
        }
        (
            Scenario::ClaudeLifecycle { .. },
            RequestFingerprint::ClaudeRootContinuation
            | RequestFingerprint::ClaudeSuccessChildResult
            | RequestFingerprint::ClaudeFailureChildResult,
        ) => anthropic_text_events(),
        (Scenario::CodexLifecycle { root_target, .. }, RequestFingerprint::CodexRootPrompt) => {
            responses_root_tool_events(root_target)
        }
        (Scenario::CodexLifecycle { .. }, RequestFingerprint::CodexRootToolResult) => {
            responses_spawn_events()
        }
        (Scenario::CodexLifecycle { .. }, RequestFingerprint::CodexRootSpawnResult) => {
            if spawn_denied {
                responses_text_events()
            } else {
                if !wait_for_fingerprint(
                    requests,
                    stop,
                    RequestFingerprint::CodexChildToolResult,
                    Duration::from_secs(80),
                    true,
                ) {
                    let observed = requests
                        .0
                        .lock()
                        .expect("provider request capture")
                        .iter()
                        .map(request_fingerprint)
                        .collect::<Vec<_>>();
                    return write_conflict(
                        &mut stream,
                        &format!("Codex child workflow did not complete; observed={observed:?}"),
                    );
                }
                responses_text_events()
            }
        }
        (Scenario::CodexLifecycle { child_target, .. }, RequestFingerprint::CodexChildPrompt) => {
            responses_workflow_tool_events(child_target)
        }
        (Scenario::CodexLifecycle { .. }, RequestFingerprint::CodexChildToolResult) => {
            responses_text_events()
        }
        (Scenario::Stalled, _) => {
            unreachable!("stalled scenarios return before response selection")
        }
        _ => {
            return write_conflict(
                &mut stream,
                &format!("unexpected semantic request fingerprint: {fingerprint:?}"),
            );
        }
    };
    write_response(&mut stream, 200, "text/event-stream", &events);
}

fn request_fingerprint(request: &Value) -> RequestFingerprint {
    if request.get("messages").is_none() {
        let child = contains_text(request, CODEX_CHILD_ROLE_MARKER);
        let custom_result = contains_type(request, "custom_tool_call_output")
            || contains_type(request, "apply_patch_call_output");
        if child {
            return if custom_result && contains_text(request, OVERLAY_WORKFLOW_MARKER) {
                RequestFingerprint::CodexChildToolResult
            } else if custom_result {
                RequestFingerprint::InvalidToolResult
            } else {
                RequestFingerprint::CodexChildPrompt
            };
        }
        if contains_call_output(request, "call_codex_spawn") {
            return RequestFingerprint::CodexRootSpawnResult;
        }
        return if custom_result {
            RequestFingerprint::CodexRootToolResult
        } else {
            RequestFingerprint::CodexRootPrompt
        };
    }

    // Parent turns retain Agent inputs in assistant messages. Only a marker in a
    // user message identifies the corresponding child conversation.
    let success_child = user_message_contains(request, CLAUDE_SUCCESS_MARKER);
    let failed_child = user_message_contains(request, CLAUDE_FAILURE_MARKER);
    let current_results = latest_user_message(request)
        .map(tool_results)
        .unwrap_or_default();
    let completed_tool = !current_results.is_empty();
    let has_tool_history = contains_text(request, "toolu_");
    let has_failed_child_history = contains_text(request, CLAUDE_FAILURE_MARKER);
    match (success_child, failed_child, completed_tool) {
        (true, false, false) => RequestFingerprint::ClaudeSuccessChildPrompt,
        (true, false, true) => {
            if expected_tool_result(
                &current_results,
                "toolu_success_child_1",
                false,
                Some(OVERLAY_WORKFLOW_MARKER),
            ) {
                RequestFingerprint::ClaudeSuccessChildResult
            } else {
                RequestFingerprint::InvalidToolResult
            }
        }
        (false, true, false) => RequestFingerprint::ClaudeFailureChildPrompt,
        (false, true, true) => {
            if expected_tool_result(&current_results, "toolu_failed_child_1", true, None) {
                RequestFingerprint::ClaudeFailureChildResult
            } else {
                RequestFingerprint::InvalidToolResult
            }
        }
        (false, false, false) if has_tool_history => RequestFingerprint::ClaudeRootContinuation,
        (false, false, false) => RequestFingerprint::ClaudeRootPrompt,
        (false, false, true) if has_failed_child_history => {
            RequestFingerprint::ClaudeRootFailedChildResult
        }
        (false, false, true) => RequestFingerprint::ClaudeRootToolResult,
        (true, true, _) => unreachable!("one conversation cannot identify both scripted children"),
    }
}

#[derive(Clone, Copy)]
struct ToolResult<'a> {
    id: &'a str,
    is_error: bool,
    content: Option<&'a Value>,
}

fn tool_results(value: &Value) -> Vec<ToolResult<'_>> {
    let mut results = Vec::new();
    collect_tool_results(value, &mut results);
    results
}

fn collect_tool_results<'a>(value: &'a Value, results: &mut Vec<ToolResult<'a>>) {
    match value {
        Value::Object(values)
            if values.get("type").and_then(Value::as_str) == Some("tool_result") =>
        {
            if let Some(id) = values.get("tool_use_id").and_then(Value::as_str) {
                results.push(ToolResult {
                    id,
                    is_error: values
                        .get("is_error")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    content: values.get("content"),
                });
            }
        }
        Value::Object(values) => values
            .values()
            .for_each(|value| collect_tool_results(value, results)),
        Value::Array(values) => values
            .iter()
            .for_each(|value| collect_tool_results(value, results)),
        _ => {}
    }
}

fn expected_tool_result(
    results: &[ToolResult<'_>],
    id: &str,
    is_error: bool,
    receipt: Option<&str>,
) -> bool {
    let mut matching = results.iter().filter(|result| result.id == id);
    matching.next().is_some_and(|result| {
        result.is_error == is_error
            && receipt.is_none_or(|receipt| {
                result
                    .content
                    .is_some_and(|content| contains_text(content, receipt))
            })
    }) && matching.next().is_none()
}

fn wait_for_fingerprint(
    requests: &(Mutex<Vec<Value>>, Condvar),
    stop: &AtomicBool,
    expected: RequestFingerprint,
    timeout: Duration,
    reject_invalid: bool,
) -> bool {
    let deadline = Instant::now() + timeout;
    let mut captured = requests.0.lock().expect("provider request capture");
    loop {
        if captured
            .iter()
            .any(|request| request_fingerprint(request) == expected)
        {
            return true;
        }
        if reject_invalid
            && captured.iter().any(|request| {
                request_fingerprint(request) == RequestFingerprint::InvalidToolResult
            })
        {
            return false;
        }
        if stop.load(Ordering::Acquire) || Instant::now() >= deadline {
            return false;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        (captured, _) = requests
            .1
            .wait_timeout(captured, remaining)
            .expect("wait for scripted lifecycle transition");
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WaitOutcome {
    Expected,
    IsolationDenied,
    Stopped,
}

fn wait_for_fingerprint_or_isolation_denial(
    requests: &(Mutex<Vec<Value>>, Condvar),
    stop: &AtomicBool,
    expected: RequestFingerprint,
) -> WaitOutcome {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut captured = requests.0.lock().expect("provider request capture");
    loop {
        if captured
            .iter()
            .any(|request| request_fingerprint(request) == expected)
        {
            return WaitOutcome::Expected;
        }
        if captured
            .iter()
            .any(|request| contains_text(request, ISOLATION_DENIAL))
        {
            return WaitOutcome::IsolationDenied;
        }
        if stop.load(Ordering::Acquire) || Instant::now() >= deadline {
            return WaitOutcome::Stopped;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        (captured, _) = requests
            .1
            .wait_timeout(captured, remaining)
            .expect("wait for scripted lifecycle transition");
    }
}

fn write_conflict(stream: &mut TcpStream, message: &str) {
    write_response(
        stream,
        409,
        "application/json",
        &json!({"error":message}).to_string(),
    );
}

fn latest_user_message(request: &Value) -> Option<&Value> {
    request
        .get("messages")?
        .as_array()?
        .iter()
        .rev()
        .find(|message| message.get("role").and_then(Value::as_str) == Some("user"))
}

fn claude_child_tool_events(target: Option<&String>, offered_tools: &[String]) -> Option<String> {
    let failed = target.is_none();
    let target = target.map_or("", String::as_str);
    let (tool, input) = if offered_tools.iter().any(|tool| tool == "PowerShell") {
        (
            "PowerShell",
            if failed {
                json!({"command":"exit 7"})
            } else {
                json!({"command":format!(
                    "rustc --edition 2021 acyclic-workflow.rs -o acyclic-workflow-bin.exe; .\\acyclic-workflow-bin.exe '{}'",
                    target.replace('\'', "''")
                )})
            },
        )
    } else if offered_tools.iter().any(|tool| tool == "Bash") {
        (
            "Bash",
            if failed {
                json!({"command":"exit 7"})
            } else {
                json!({"command":format!(
                    "rustc --edition 2021 acyclic-workflow.rs -o acyclic-workflow-bin && ./acyclic-workflow-bin '{}'",
                    target.replace('\'', "'\"'\"'")
                )})
            },
        )
    } else {
        return None;
    };
    Some(anthropic_named_tool_events_with_prefix(
        if failed {
            "failed_child"
        } else {
            "success_child"
        },
        tool,
        &input.to_string(),
    ))
}

fn anthropic_lifecycle_events(root_target: &str) -> String {
    anthropic_named_tool_sequence_with_prefix(
        "lifecycle",
        [
            (
                "Write",
                json!({"file_path":root_target,"content":"qualified"}).to_string(),
            ),
            (
                "Agent",
                json!({
                    "description":"deterministic child isolation",
                    "prompt":format!("{CLAUDE_SUCCESS_MARKER}: write the requested relative file"),
                    "subagent_type":"general-purpose"
                })
                .to_string(),
            ),
        ],
    )
}

fn anthropic_failed_agent_events() -> String {
    anthropic_named_tool_events_with_prefix(
        "failed_agent",
        "Agent",
        &json!({
            "description":"deterministic failed child",
            "prompt":format!("{CLAUDE_FAILURE_MARKER}: run the requested failing command"),
            "subagent_type":"general-purpose"
        })
        .to_string(),
    )
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

fn user_message_contains(value: &Value, expected: &str) -> bool {
    value
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .any(|message| {
            message.get("role").and_then(Value::as_str) == Some("user")
                && message
                    .get("content")
                    .is_some_and(|content| contains_text(content, expected))
        })
}

fn contains_call_output(value: &Value, call_id: &str) -> bool {
    match value {
        Value::Object(values) => {
            (values.get("type").and_then(Value::as_str) == Some("function_call_output")
                && values.get("call_id").and_then(Value::as_str) == Some(call_id))
                || values
                    .values()
                    .any(|value| contains_call_output(value, call_id))
        }
        Value::Array(values) => values
            .iter()
            .any(|value| contains_call_output(value, call_id)),
        _ => false,
    }
}

fn contains_text(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(value) => value.contains(expected),
        Value::Array(values) => values.iter().any(|value| contains_text(value, expected)),
        Value::Object(values) => values.values().any(|value| contains_text(value, expected)),
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

fn responses_root_tool_events(target: &str) -> String {
    let target = target.replace('\\', "/");
    assert!(
        !target.contains(['\r', '\n']),
        "scripted target must remain on one line"
    );
    let item = json!({
        "id":"ctc_1",
        "type":"custom_tool_call",
        "call_id":"call_1",
        "name":"exec",
        "input":format!(
            "const result = await tools.apply_patch({}); text(result);",
            serde_json::to_string(&format!(
                "*** Begin Patch\n*** Add File: {target}\n+qualified\n*** End Patch"
            ))
            .expect("scripted patch")
        ),
        "status":"completed"
    });
    sse([
        json!({"type":"response.created","response":{"id":"resp_tool","object":"response","status":"in_progress","model":"gpt-5.6-sol","output":[]}}),
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
        json!({"type":"response.completed","response":{"id":"resp_tool","object":"response","status":"completed","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}),
    ])
}

fn responses_spawn_events() -> String {
    let item = json!({
        "id":"fc_codex_spawn",
        "type":"function_call",
        "call_id":"call_codex_spawn",
        "name":"spawn_agent",
        "namespace":"collaboration",
        "arguments":serde_json::to_string(&json!({
            "task_name":"overlay_workflow",
            "message":format!("{CODEX_WORKFLOW_MARKER}: compile and run the supplied workflow")
        })).expect("spawn arguments"),
        "status":"completed"
    });
    responses_item_events("resp_spawn", &item)
}

fn responses_workflow_tool_events(target: &str) -> String {
    assert!(
        !target.contains(['\r', '\n', '\'', '"']),
        "scripted workflow target must be a simple relative path"
    );
    let (compile, run) = if cfg!(windows) {
        (
            "rustc --edition 2021 acyclic-workflow.rs -o acyclic-workflow-bin.exe",
            format!(".\\acyclic-workflow-bin.exe '{target}'"),
        )
    } else {
        (
            "rustc --edition 2021 acyclic-workflow.rs -o acyclic-workflow-bin",
            format!("./acyclic-workflow-bin '{target}'"),
        )
    };
    let input = format!(
        "// @exec: {{\"yield_time_ms\": 120000}}\nasync function finish(cmd) {{ let result = await tools.exec_command({{cmd, yield_time_ms: 1000}}); let output = String(result.output ?? ''); for (let i = 0; result.session_id && i < 16; i++) {{ result = await tools.write_stdin({{session_id: result.session_id, chars: '', yield_time_ms: 5000}}); output = (output + String(result.output ?? '')).slice(-4000); }} if (result.session_id) throw new Error('workflow command timed out: ' + cmd + ': ' + output); return {{exit_code: result.exit_code, output}}; }} const compile = await finish({}); if (compile.exit_code !== 0) throw new Error('rustc failed: ' + compile.output); const run = await finish({}); text(run.output); if (run.exit_code !== 0) throw new Error('workflow failed: ' + run.output);",
        json!(compile),
        json!(run),
    );
    let item = json!({
        "id":"ctc_codex_workflow",
        "type":"custom_tool_call",
        "call_id":"call_codex_workflow",
        "name":"exec",
        "input":input,
        "status":"completed"
    });
    responses_item_events("resp_workflow", &item)
}

fn responses_item_events(response_id: &str, item: &Value) -> String {
    sse([
        json!({"type":"response.created","response":{"id":response_id,"object":"response","status":"in_progress","model":"gpt-5.6-sol","output":[]}}),
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
        json!({"type":"response.completed","response":{"id":response_id,"object":"response","status":"completed","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}),
    ])
}

fn responses_text_events() -> String {
    sse([
        json!({"type":"response.created","response":{"id":"resp_done","object":"response","status":"in_progress","model":"gpt-5.6-sol","output":[]}}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"qualified","annotations":[]}]}}),
        json!({"type":"response.completed","response":{"id":"resp_done","object":"response","status":"completed","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}),
    ])
}

fn anthropic_named_tool_events_with_prefix(prefix: &str, tool: &str, input: &str) -> String {
    anthropic_named_tool_sequence_with_prefix(prefix, [(tool, input.to_owned())])
}

fn anthropic_named_tool_sequence_with_prefix<'a>(
    prefix: &str,
    tools: impl IntoIterator<Item = (&'a str, String)>,
) -> String {
    let mut events = vec![
        json!({"type":"message_start","message":{"id":"msg_tool","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}}),
    ];
    for (index, (tool, input)) in tools.into_iter().enumerate() {
        events.extend([
            json!({"type":"content_block_start","index":index,"content_block":{"type":"tool_use","id":format!("toolu_{prefix}_{}", index + 1),"name":tool,"input":{}}}),
            json!({"type":"content_block_delta","index":index,"delta":{"type":"input_json_delta","partial_json":input}}),
            json!({"type":"content_block_stop","index":index}),
        ]);
    }
    events.extend([
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":1}}),
        json!({"type":"message_stop"}),
    ]);
    sse(events)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_uses_the_current_claude_turn_not_historical_results() {
        let history = vec![
            json!({"role":"assistant","content":[{
                "type":"tool_use",
                "id":"toolu_failed_agent_1",
                "input":{"prompt":CLAUDE_FAILURE_MARKER}
            }]}),
            json!({"role":"user","content":[{
                "type":"tool_result",
                "tool_use_id":"toolu_failed_agent_1",
                "is_error":true
            }]}),
        ];
        let mut continuation = history.clone();
        continuation.push(json!({"role":"assistant","content":[{"type":"text","text":"done"}]}));
        continuation.push(json!({"role":"user","content":[{"type":"text","text":"hook context"}]}));
        assert_eq!(
            request_fingerprint(&json!({"messages":continuation})),
            RequestFingerprint::ClaudeRootContinuation
        );
        assert_eq!(
            request_fingerprint(&json!({"messages":history})),
            RequestFingerprint::ClaudeRootFailedChildResult
        );
    }

    #[test]
    fn fingerprint_requires_the_exact_child_outcome_and_workflow_receipt() {
        let child = |marker: &str, tool_use_id: &str, is_error: bool, content: &str| {
            json!({"messages":[
                {"role":"user","content":[{"type":"text","text":marker}]},
                {"role":"user","content":[{
                    "type":"tool_result",
                    "tool_use_id":tool_use_id,
                    "is_error":is_error,
                    "content":content
                }]}
            ]})
        };
        assert_eq!(
            request_fingerprint(&child(
                CLAUDE_SUCCESS_MARKER,
                "toolu_success_child_1",
                true,
                OVERLAY_WORKFLOW_MARKER,
            )),
            RequestFingerprint::InvalidToolResult
        );
        assert_eq!(
            request_fingerprint(&child(
                CLAUDE_SUCCESS_MARKER,
                "toolu_success_child_1",
                false,
                "missing workflow receipt",
            )),
            RequestFingerprint::InvalidToolResult
        );
        assert_eq!(
            request_fingerprint(&child(
                CLAUDE_SUCCESS_MARKER,
                "toolu_success_child_1",
                false,
                OVERLAY_WORKFLOW_MARKER,
            )),
            RequestFingerprint::ClaudeSuccessChildResult
        );
        assert_eq!(
            request_fingerprint(&child(
                CLAUDE_FAILURE_MARKER,
                "toolu_failed_child_1",
                false,
                "",
            )),
            RequestFingerprint::InvalidToolResult
        );

        let batched = json!({"messages":[
            {"role":"user","content":[{"type":"text","text":CLAUDE_SUCCESS_MARKER}]},
            {"role":"user","content":[
                {"type":"tool_result","tool_use_id":"unrelated","is_error":true},
                {"type":"tool_result","tool_use_id":"toolu_success_child_1","is_error":false,"content":OVERLAY_WORKFLOW_MARKER}
            ]}
        ]});
        assert_eq!(
            request_fingerprint(&batched),
            RequestFingerprint::ClaudeSuccessChildResult
        );

        let duplicate = json!({"messages":[
            {"role":"user","content":[{"type":"text","text":CLAUDE_SUCCESS_MARKER}]},
            {"role":"user","content":[
                {"type":"tool_result","tool_use_id":"toolu_success_child_1","is_error":false,"content":OVERLAY_WORKFLOW_MARKER},
                {"type":"tool_result","tool_use_id":"toolu_success_child_1","is_error":true,"content":OVERLAY_WORKFLOW_MARKER}
            ]}
        ]});
        assert_eq!(
            request_fingerprint(&duplicate),
            RequestFingerprint::InvalidToolResult
        );

        let historical_receipt = json!({"messages":[
            {"role":"user","content":[{"type":"text","text":CLAUDE_SUCCESS_MARKER}]},
            {"role":"assistant","content":[{"type":"text","text":OVERLAY_WORKFLOW_MARKER}]},
            {"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_success_child_1","is_error":false,"content":"no receipt"}]}
        ]});
        assert_eq!(
            request_fingerprint(&historical_receipt),
            RequestFingerprint::InvalidToolResult
        );
    }

    #[test]
    fn claude_workflow_fails_closed_without_a_shell() {
        assert!(claude_child_tool_events(Some(&"target".into()), &["Write".into()]).is_none());
    }
}
