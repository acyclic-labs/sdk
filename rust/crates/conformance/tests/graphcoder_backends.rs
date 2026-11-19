//! One `GraphCoder` binary, built once, run unchanged against each backend:
//! this machine's harness, the self-hosted endpoint (`acyclic serve`)
//! over gRPC, and the same server entry point with injected seams
//! (`graphcoder-seams serve`, its own verifier). Only the endpoint
//! differs for the client, and only the seams for the server. Each runs one
//! scenario on the loopback fixture model, a shell call held for one human
//! approval, a file write and version-pinned read, a spawned child's turn and
//! a reconnect while the approval is pending, and must commit the same history
//! shape.
//!
//! The binary, built with fixture signing custody, is the test's declared
//! `acyclic` tool; `ACYCLIC_TEST_BINARY` selects another.
use acyclic_harness::local::{Connection, agent_conversation::Conversation, service::Directory};
use acyclic_native_runtime::{
    CaptureHandler, CaptureSchedule, ProcessCaptureStream, ProcessCommand, ProcessInput,
    ProcessOutput, ProcessPipeReader, ProcessStdio, ProcessTree, SERVICE_READY_ARGUMENT,
    ServiceReadiness, ServiceStartOutcome, TaskSpawner,
};
use axum::{Json, Router, http::StatusCode, response::IntoResponse as _, routing::post};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    io::{self, Write as _},
    path::{Path, PathBuf},
    process::{ChildStdin, ExitStatus},
    sync::{Arc, Mutex, PoisonError},
};

type Outcome<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const SCENARIO: &str = "conformance scenario";
const CHILD: &str = "conformance child";
/// The child's answer, which its result carries back to the root.
const CHILD_REPLY: &str = "child reply";
const FOLLOWUP: &str = "conformance follow-up";
const WRITTEN: &str = "conformance write";
const INPUT_TOKENS: u64 = 64;

/// The binary every backend runs: the plugin's `acyclic`, which the test
/// declares as its tool, or the one `ACYCLIC_TEST_BINARY` selects.
fn executable() -> io::Result<PathBuf> {
    executable_from(
        std::env::var_os("CARGO_BIN_EXE_acyclic"),
        std::env::var_os("ACYCLIC_TEST_BINARY"),
        &std::env::current_exe()?,
    )
}

fn executable_from(
    cargo: Option<std::ffi::OsString>,
    selected: Option<std::ffi::OsString>,
    current_exe: &Path,
) -> io::Result<PathBuf> {
    if let Some(path) = cargo {
        return Ok(path.into());
    }
    if let Some(path) = selected {
        return Ok(path.into());
    }
    // Cargo builds `acyclic` as part of another package, so it does not set
    // this test's CARGO_BIN_EXE. The documented workflow builds the plugin
    // into the shared target directory; Bazel always supplies the tool above.
    let target = current_exe.parent().and_then(Path::parent).ok_or_else(|| {
        io::Error::other("GraphCoder test executable has no shared target directory")
    })?;
    let path = target.join(format!("acyclic{}", std::env::consts::EXE_SUFFIX));
    assert!(
        path.exists(),
        "build the acyclic binary first: cargo build -p acyclic-plugin --features acyclic-native-runtime/test-support ({})",
        path.display()
    );
    Ok(path)
}

#[test]
fn cargo_workflow_finds_the_plugin_binary_in_the_shared_target_directory() -> io::Result<()> {
    let root = tempfile::tempdir().unwrap();
    let deps = root.path().join("target/debug/deps");
    std::fs::create_dir_all(&deps).unwrap();
    let binary = root
        .path()
        .join("target/debug")
        .join(format!("acyclic{}", std::env::consts::EXE_SUFFIX));
    std::fs::write(&binary, b"").unwrap();
    let current = deps.join("graphcoder_backends");
    assert_eq!(executable_from(None, None, &current)?, binary);
    assert_eq!(
        executable_from(Some("cargo".into()), None, &current)?,
        PathBuf::from("cargo")
    );
    assert_eq!(
        executable_from(None, Some("selected".into()), &current)?,
        PathBuf::from("selected")
    );
    Ok(())
}

/// A Server-Sent Events body of one Responses generation.
fn events(items: &[Value]) -> Outcome<String> {
    let mut body = String::new();
    for (index, item) in items.iter().enumerate() {
        let event = |kind: &str| json!({"type": kind, "output_index": index, "item": item});
        if item.get("type").and_then(Value::as_str) == Some("message") {
            let text = item
                .get("content")
                .and_then(|content| content.get(0))
                .and_then(|part| part.get("text"))
                .and_then(Value::as_str)
                .ok_or("fixture message has no text")?;
            body += &format!(
                "data: {}\n\n",
                json!({"type": "response.output_text.delta", "delta": text})
            );
        } else {
            body += &format!("data: {}\n\n", event("response.output_item.added"));
            body += &format!("data: {}\n\n", event("response.output_item.done"));
        }
    }
    body += &format!(
        "data: {}\n\n",
        json!({"type": "response.completed", "response": {"status": "completed",
            "usage": {"input_tokens": INPUT_TOKENS, "output_tokens": 2}}})
    );
    Ok(body)
}

fn call(id: &str, name: &str, arguments: &Value) -> Value {
    json!({"type": "function_call", "call_id": id, "name": name,
        "arguments": arguments.to_string()})
}

fn text(answer: &str) -> Value {
    json!({"type": "message", "content": [{"type": "output_text", "text": answer}]})
}

/// Arguments for the request's own `write` tool: every required property
/// of its schema, filled for one small text file.
fn write_arguments(request: &Value) -> Outcome<Value> {
    let tool = request
        .get("tools")
        .and_then(Value::as_array)
        .and_then(|tools| {
            tools
                .iter()
                .find(|tool| tool.get("name").and_then(Value::as_str) == Some("write"))
        })
        .ok_or("the turn did not offer the write tool")?;
    let mut arguments = serde_json::Map::new();
    for name in tool
        .pointer("/parameters/required")
        .and_then(Value::as_array)
        .ok_or("write tool has no required properties")?
    {
        let name = name.as_str().ok_or("write property name is not a string")?;
        let value = match name {
            "path" | "display_name" => "conformance.txt",
            "media_type" => "text/plain",
            _ => WRITTEN,
        };
        arguments.insert(name.to_owned(), value.into());
    }
    Ok(Value::Object(arguments))
}

/// Each tool call's committed result, by call.
type Outputs = std::collections::BTreeMap<String, String>;

/// One step a backend asked the model for: whose turn, and how many tool
/// results it already held.
type Step = (&'static str, usize);

/// The scripted turns, told apart by their newest user message: the root's
/// scenario turn calls shell, write and spawn, then answers; the child
/// answers at once; the root acknowledges its child's report; the root's
/// follow-up waits for the child's original completed reply, then answers.
fn answer(
    request: &Value,
    steps: &Mutex<BTreeSet<Step>>,
    outputs: &Mutex<Outputs>,
) -> Outcome<String> {
    let input = request["input"].as_array().cloned().unwrap_or_default();
    if input.last().is_some_and(|instruction| {
        instruction
            .to_string()
            .contains("You manage this worker's context outside its turns.")
    }) {
        return events(&[text(
            &json!({
                "version": 1,
                "summary": null,
                "review_at_input_tokens": INPUT_TOKENS * 2,
            })
            .to_string(),
        )]);
    }
    let newest = input
        .iter()
        .rposition(|item| item["role"] == "user")
        .unwrap_or_default();
    let prompt = input.get(newest).map(Value::to_string).unwrap_or_default();
    for item in &input {
        if item["type"] == "function_call_output"
            && let (Some(call), Some(output)) = (item["call_id"].as_str(), item["output"].as_str())
        {
            outputs
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(call.to_owned(), output.to_owned());
        }
    }
    let results = input
        .iter()
        .skip(newest)
        .filter(|item| item["type"] == "function_call_output")
        .count();
    if prompt.contains(CHILD) {
        steps
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(("child", results));
        return events(&[text(CHILD_REPLY)]);
    }
    // The root's report of its child's result, which carries the child's
    // reply. The child decides when it arrives, before or after the
    // follow-up, so it records no step.
    if prompt.contains(CHILD_REPLY) {
        return events(&[text("child noted")]);
    }
    if prompt.contains(FOLLOWUP) {
        steps
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(("followup", results));
        return events(&[if results == 0 {
            call("wait", "wait", &json!({"agents": ["child"]}))
        } else {
            text("followup answer")
        }]);
    }
    // A server may open a root agent before the test's scenario turn. That
    // turn is not the scenario, so it must not run it.
    if !prompt.contains(SCENARIO) {
        return events(&[text("idle")]);
    }
    steps
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(("root", results));
    events(&[match results {
        0 => call(
            "shell",
            "shell",
            &json!({"command": "echo conformance-shell"}),
        ),
        1 => call("write", "write", &write_arguments(request)?),
        2 => {
            // Select the original publication, not whichever path head exists
            // when the read executes. A write receipt alone proves no bytes.
            let outputs = outputs.lock().unwrap_or_else(PoisonError::into_inner);
            let written: Value =
                serde_json::from_str(outputs.get("write").ok_or("missing write result")?)?;
            call(
                "read",
                "read",
                &json!({"path": written["path"], "version": written["version"]}),
            )
        }
        3 => call(
            "spawn",
            "spawn",
            &json!({"task": CHILD, "name": "child", "role": "worker"}),
        ),
        _ => text("root answer"),
    }])
}

/// The loopback Responses endpoint and its catalog.
struct Model {
    catalog: PathBuf,
    steps: Arc<Mutex<BTreeSet<Step>>>,
    outputs: Arc<Mutex<Outputs>>,
    last_request: Arc<Mutex<Option<Value>>>,
    stop: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<io::Result<()>>,
}

impl Model {
    async fn close(self) -> Outcome {
        let stopping = self.stop.send(());
        self.server.await??;
        stopping.map_err(|()| "fixture model already stopped".into())
    }
}

async fn model(directory: &Path) -> Outcome<Model> {
    std::fs::create_dir_all(directory)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let steps = Arc::new(Mutex::new(BTreeSet::new()));
    let recorded = Arc::clone(&steps);
    let outputs = Arc::new(Mutex::new(Outputs::new()));
    let seen = Arc::clone(&outputs);
    let last_request = Arc::new(Mutex::new(None));
    let captured = Arc::clone(&last_request);
    let app = Router::new()
        .route(
            "/count",
            post(|| async { Json(json!({"input_tokens": INPUT_TOKENS})) }),
        )
        .route(
            "/responses",
            post(move |Json(body): Json<Value>| {
                let reply = answer(&body, &recorded, &seen);
                *captured.lock().unwrap_or_else(PoisonError::into_inner) = Some(body);
                async move {
                    match reply {
                        Ok(body) => ([("content-type", "text/event-stream")], body).into_response(),
                        Err(error) => {
                            (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
                        }
                    }
                }
            }),
        );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let catalog = directory.join("catalog.json");
    std::fs::write(
        &catalog,
        serde_json::to_vec(&json!({"conformance":{
            "adapter":"responses","authentication":"optional","base_url":format!("http://{address}"),
            "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}}},"max_event_bytes":65536,
            "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
        }}))?,
    )?;
    Ok(Model {
        catalog,
        steps,
        outputs,
        last_request,
        stop,
        server,
    })
}

/// One isolated harness home: custody, workspace and settings.
struct Home {
    custody: acyclic_native_runtime::signing_key::FixtureCustody,
    root: PathBuf,
}

impl Home {
    fn new(directory: &Path, name: &str) -> io::Result<Self> {
        let root = directory.join(name);
        for path in ["workspace", "settings/.codex"] {
            std::fs::create_dir_all(root.join(path))?;
        }
        Ok(Self {
            custody: acyclic_native_runtime::signing_key::FixtureCustody::new()?,
            root,
        })
    }

    fn workspace(&self) -> PathBuf {
        self.root.join("workspace")
    }

    fn owner_state(&self) -> PathBuf {
        self.root.join("owner")
    }

    /// The binary with this home's harness arguments, then `args`.
    fn command(
        &self,
        model: &Model,
        policy: Option<&Path>,
        args: &[&str],
    ) -> io::Result<ProcessCommand> {
        Ok(self.command_of(&executable()?, model, policy, args))
    }

    /// `binary`, a build of `GraphCoder`, with this home's harness arguments,
    /// then `args`.
    fn command_of(
        &self,
        binary: &Path,
        model: &Model,
        policy: Option<&Path>,
        args: &[&str],
    ) -> ProcessCommand {
        let mut command = self.client(binary);
        let settings = self.root.join("settings");
        command
            .arg("--settings-root")
            .arg(&settings)
            .arg("--catalog")
            .arg(&model.catalog)
            .args(["--model", "conformance/tiny"]);
        if let Some(policy) = policy {
            command.arg("--approval-policy").arg(policy);
        }
        command.args(args);
        command
    }

    fn client(&self, binary: &Path) -> ProcessCommand {
        let settings = self.root.join("settings");
        let mut command = ProcessCommand::new(binary);
        self.custody.configure(|key, value| {
            command.env(key, value);
        });
        command
            .env("CODEX_HOME", settings.join(".codex"))
            .env("HOME", &settings)
            .env("USERPROFILE", &settings)
            .env("LOCALAPPDATA", &settings)
            .env("XDG_DATA_HOME", &settings)
            .env("TERM", "dumb")
            .env("ACYCLIC_LOG", "off")
            .env_remove("ACYCLIC_MODEL")
            .env_remove("ACYCLIC_ENDPOINT")
            .env_remove("ACYCLIC_API_KEY")
            .current_dir(self.workspace())
            .arg("--workspace")
            .arg(self.workspace())
            .stdin(ProcessStdio::piped())
            .stdout(ProcessStdio::piped())
            .stderr(ProcessStdio::piped());
        command
    }

    async fn local_owner(&self, model: &Model, policy: &Path) -> io::Result<ProcessTree> {
        let mut command = self.command(
            model,
            Some(policy),
            &["--local-service", SERVICE_READY_ARGUMENT],
        )?;
        command
            .arg("--host-state-directory")
            .arg(self.owner_state())
            .stdin(ProcessStdio::null());
        let mut owner = ProcessTree::spawn(&mut command)?;
        drop(command);
        let mut ready = ProcessPipeReader::take_stdout(&mut owner)?;
        match ServiceReadiness::wait_from(&mut ready).await? {
            ServiceStartOutcome::Ready => Ok(owner),
            ServiceStartOutcome::Exited => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "local owner exited before announcing readiness",
            )),
            ServiceStartOutcome::Failed(reason) => Err(io::Error::other(reason)),
        }
    }
}

/// The shared native pump retains both pipes and the original process tree.
struct OutputHandler;

impl CaptureHandler for OutputHandler {
    fn check(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn observe(&mut self, _chunk: ProcessOutput<'_>) -> io::Result<ProcessInput> {
        Ok(ProcessInput::default())
    }
}

/// An interactive binary, with one credit-driven output stream and no readers
/// or timers of its own. Native exit closes descendant-held pipes before EOF.
struct Running {
    output: Option<ProcessCaptureStream<OutputHandler>>,
    stdin: Option<ChildStdin>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    line_start: usize,
    status: Option<ExitStatus>,
}

impl Running {
    fn start(mut command: ProcessCommand) -> io::Result<Self> {
        let mut child = ProcessTree::spawn(&mut command)?;
        let stdin = child.take_stdin();
        let output = child.stream_capture(
            &TaskSpawner::current(),
            CaptureSchedule::Unbounded,
            usize::MAX,
            None,
            OutputHandler,
        );
        Ok(Self {
            output: Some(output),
            stdin,
            stdout: Vec::new(),
            stderr: Vec::new(),
            line_start: 0,
            status: None,
        })
    }

    /// One actual output delivery, or original physical completion. Task loss
    /// and pipe failure retain their typed native error instead of an EOF guess.
    async fn receive(&mut self) -> io::Result<bool> {
        let Some(output) = self.output.as_mut() else {
            return Ok(false);
        };
        if let Some(chunk) = output.recv().await {
            self.stdout.extend(chunk.stdout);
            self.stderr.extend(chunk.stderr);
            return Ok(true);
        }
        let completed = self
            .output
            .take()
            .ok_or_else(|| self.closed("process capture settlement"))?
            .finish()
            .await?;
        self.status = Some(completed.result.map_err(io::Error::other)?.status);
        Ok(false)
    }

    fn closed(&self, expected: &str) -> io::Error {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!(
                "binary exited {:?} before {expected}: {}",
                self.status,
                String::from_utf8_lossy(&self.stderr)
            ),
        )
    }

    /// The next stdout line, examining each newly delivered byte once.
    async fn line(&mut self) -> io::Result<String> {
        let mut scanned = self.line_start;
        loop {
            if let Some(offset) = self
                .stdout
                .get(scanned..)
                .ok_or_else(|| self.closed("the stdout scan position"))?
                .iter()
                .position(|byte| *byte == b'\n')
            {
                let end = scanned + offset;
                let line = self
                    .stdout
                    .get(self.line_start..end)
                    .ok_or_else(|| self.closed("the stdout line"))?;
                let line = line.strip_suffix(b"\r").unwrap_or(line);
                let line = String::from_utf8(line.to_vec()).map_err(io::Error::other)?;
                self.line_start = end + 1;
                return Ok(line);
            }
            scanned = self.stdout.len();
            if !self.receive().await? {
                if self.line_start == self.stdout.len() {
                    return Err(self.closed("a stdout line"));
                }
                let line = self.remaining_stdout()?;
                self.line_start = self.stdout.len();
                return Ok(line);
            }
        }
    }

    /// Waits on original deliveries; closed output is a terminal error.
    async fn wait_for(&mut self, needle: &str) -> io::Result<()> {
        if needle.is_empty() {
            return Ok(());
        }
        let mut scanned = 0;
        loop {
            if self
                .stderr
                .get(scanned..)
                .ok_or_else(|| self.closed("the stderr scan position"))?
                .windows(needle.len())
                .any(|bytes| bytes == needle.as_bytes())
            {
                return Ok(());
            }
            scanned = self.stderr.len().saturating_sub(needle.len() - 1);
            if !self.receive().await? {
                return Err(self.closed(needle));
            }
        }
    }

    fn answer(&mut self, input: &str) -> io::Result<()> {
        self.stdin
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "process stdin is closed"))?
            .write_all(input.as_bytes())
    }

    fn remaining_stdout(&self) -> io::Result<String> {
        let tail = self
            .stdout
            .get(self.line_start..)
            .ok_or_else(|| self.closed("the unread stdout"))?;
        String::from_utf8(tail.to_vec()).map_err(io::Error::other)
    }

    /// Original exit and pipe EOF settle before either output is inspected.
    async fn finish(mut self) -> io::Result<(String, String)> {
        drop(self.stdin.take());
        while self.receive().await? {}
        let status = self.status.ok_or_else(|| self.closed("process status"))?;
        let stdout = self.remaining_stdout()?;
        let stderr = String::from_utf8(self.stderr).map_err(io::Error::other)?;
        if !status.success() {
            return Err(io::Error::other(format!(
                "exit {status:?}\n{stdout}\n{stderr}"
            )));
        }
        Ok((stdout, stderr))
    }

    /// Closing output credit wakes even a quiet or backpressured child. Join
    /// the same native cleanup owner before starting the replacement client.
    /// Keep stdin open until process exit to preserve the crash boundary.
    async fn kill(mut self) -> io::Result<()> {
        if let Some(output) = self.output.take() {
            let completed = output.finish().await?;
            if let Err(failure) = completed.result {
                assert!(failure.cleanup_completed, "{failure:?}");
            }
        }
        drop(self.stdin.take());
        Ok(())
    }
}

/// The real child used by the stream control below. Its input byte is the
/// producer's settle event between the two parts of a UTF-8 stderr character.
#[test]
fn interactive_output_child() -> io::Result<()> {
    if std::env::var_os("ACYCLIC_CONFORMANCE_OUTPUT_CHILD").is_none() {
        return Ok(());
    }
    std::io::stderr().write_all(&[0xe2])?;
    std::io::stderr().flush()?;
    writeln!(std::io::stdout(), "child-ready")?;
    std::io::stdout().flush()?;
    std::io::Read::read_exact(&mut std::io::stdin(), &mut [0])?;
    std::io::stderr().write_all(&[0x98, 0x83])?;
    writeln!(std::io::stderr(), " stderr delivered")?;
    writeln!(std::io::stdout(), "final output")?;
    std::process::exit(0);
}

#[tokio::test]
async fn interactive_capture_preserves_bytes_and_reports_original_eof() -> io::Result<()> {
    let mut command = ProcessCommand::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "graphcoder_backends::interactive_output_child",
            "--nocapture",
        ])
        .env("ACYCLIC_CONFORMANCE_OUTPUT_CHILD", "1")
        .stdin(ProcessStdio::piped())
        .stdout(ProcessStdio::piped())
        .stderr(ProcessStdio::piped());
    let mut child = Running::start(command)?;
    // Consume the test executable's own banner, then the original child's
    // readiness line. Closing that child before the line is a typed EOF.
    while !child.line().await?.ends_with("child-ready") {}
    while child.stderr.is_empty() {
        assert!(child.receive().await?);
    }
    assert_eq!(child.stderr, [0xe2]);
    child.answer("!")?;
    child.wait_for("☃ stderr delivered").await?;
    let missing = child
        .wait_for("never written by the child")
        .await
        .err()
        .ok_or_else(|| io::Error::other("closed child reported output it never wrote"))?;
    assert_eq!(missing.kind(), io::ErrorKind::UnexpectedEof);
    let (stdout, stderr) = child.finish().await?;
    assert!(stdout.contains("final output"));
    assert_eq!(stderr, "☃ stderr delivered\n");
    Ok(())
}

/// The endpoint `acyclic serve` prints once it serves, announced by the
/// harness server itself: the one entry point a managed host serves through,
/// with nothing of the binary's own between them.
async fn served(server: &mut Running) -> Outcome<Value> {
    let endpoint: Value = serde_json::from_str(&server.line().await?)?;
    assert_eq!(endpoint["server"], acyclic_harness::server::IDENTITY);
    Ok(endpoint)
}

#[tokio::test(flavor = "multi_thread")]
async fn external_client_delivers_attributed_peer_input_to_live_session() -> Outcome {
    use acyclic_harness::local::{
        Connection,
        agent_conversation::{Conversation, Settlement},
        sessions,
    };
    let directory = tempfile::tempdir().unwrap();
    let model = model(directory.path()).await?;
    let home = Home::new(directory.path(), "peer-owner")?;
    let mut server = Running::start(home.command(&model, None, &["serve", "127.0.0.1:0"])?)?;
    let published = served(&mut server).await?;
    let endpoint = published["endpoint"].as_str().unwrap();
    let credential = published["credential"].as_str().unwrap();
    let options = acyclic_harness::client::resolve_connection_options(
        Some(endpoint),
        Some(credential),
        None,
        None,
    )
    .unwrap();
    let connection = Connection::connect(&options).await.unwrap();
    let sender = sessions::create(&connection, "external-sender", None)
        .await
        .unwrap();
    let receiver = sessions::create(&connection, "external-receiver", None)
        .await
        .unwrap();
    sessions::name(&sender, "sender".into(), "name-sender")
        .await
        .unwrap();
    sessions::name(&receiver, "receiver".into(), "name-receiver")
        .await
        .unwrap();
    let conversation = Conversation::open(receiver);
    let mut observer = conversation.observe(None).await.unwrap();
    let client_home = Home::new(directory.path(), "peer-client")?;
    let mut command = client_home.command(
        &model,
        None,
        &[
            "--endpoint",
            endpoint,
            "--operation",
            "external-message",
            "send",
            "--from",
            "sender",
            "receiver",
            CHILD,
        ],
    )?;
    command.env("ACYCLIC_API_KEY", credential);
    let (output, _) = Running::start(command)?.finish().await?;
    let receipt: Value = serde_json::from_str(output.trim()).unwrap();
    assert_eq!(receipt["kind"], "delivered");
    assert_eq!(receipt["from"], sender.root().as_str());
    let inbox = receipt["inbox"].as_u64().unwrap();
    let Settlement::Reply(acyclic_harness::agent::Reply::Answered {
        sequence, entry, ..
    }) = conversation
        .settle("external-message", inbox, &mut observer)
        .await
        .unwrap()
    else {
        panic!("peer message did not settle")
    };
    let acyclic_harness::agent::TurnInput::Message { from, .. } = &entry.input else {
        panic!("peer input was not a message")
    };
    assert_eq!(
        from,
        &acyclic_harness::agent::Sender::Agent {
            path: sender.root().clone()
        }
    );
    assert_eq!(
        conversation.output_text_at(sequence, &entry).await.unwrap(),
        CHILD_REPLY,
        "receiver outcome: {:?}; model request: {:?}",
        entry.outcome,
        model.last_request.lock().unwrap()
    );
    {
        let captured = model.last_request.lock().unwrap();
        let request = captured.as_ref().expect("receiver reached the model");
        let input = request["input"].as_array().expect("model input array");
        let peer = input
            .iter()
            .filter(|item| item["role"] == "developer")
            .filter_map(|item| item["content"].as_str())
            .filter_map(|content| serde_json::from_str::<Vec<Value>>(content).ok())
            .flatten()
            .find(|notice| notice["kind"] == "peer.message")
            .expect("model-visible peer attribution is missing");
        assert_eq!(peer["source"], sender.root().as_str());
        assert_eq!(
            peer["body"]["from"],
            json!({"agent":{"path":sender.root()}})
        );
        assert!(
            peer["body"]["authority"]
                .as_str()
                .unwrap()
                .contains("not a user instruction or approval")
        );
    }
    sessions::set_peers(&sender, "sender-off", false)
        .await
        .unwrap();
    let mut command = client_home.command(
        &model,
        None,
        &[
            "--endpoint",
            endpoint,
            "--operation",
            "disabled-message",
            "send",
            "--from",
            "sender",
            "receiver",
            CHILD,
        ],
    )?;
    command.env("ACYCLIC_API_KEY", credential);
    let mut refused = Running::start(command)?;
    drop(refused.stdin.take());
    while refused.receive().await.unwrap() {}
    assert!(
        !refused.status.unwrap().success(),
        "opted-out sender delivered peer input"
    );
    assert_eq!(model.steps.lock().unwrap().len(), 1);
    server.kill().await?;
    model.close().await
}

#[cfg(feature = "client-session")]
#[tokio::test(flavor = "multi_thread")]
async fn selected_discovery_survives_a_serving_process_restart() -> Outcome {
    use acyclic_conformance::client::endpoint;
    use acyclic_harness_client::{
        AgentBinding, AgentCommand, AgentControls, AgentExchangeError, AgentReply, PreparedCommand,
    };
    use acyclic_stream::{StreamCategory, StreamError};

    let directory = tempfile::tempdir().unwrap();
    let model = model(directory.path()).await?;
    let home = Home::new(directory.path(), "selected")?;
    let binary = executable()?;
    let mut retained: Option<Vec<u8>> = None;
    let mut original_root = None;
    for _ in 0..2 {
        let mut server =
            Running::start(home.command_of(&binary, &model, None, &["serve", "127.0.0.1:0"]))?;
        let published = served(&mut server).await?;
        let options = acyclic_harness_client::resolve_connection_options(
            published["endpoint"].as_str(),
            published["credential"].as_str(),
            None,
            None,
        )
        .unwrap();
        let session = endpoint::connect(&options).await.unwrap();
        let (controls, _) = match &retained {
            Some(binding) => {
                AgentControls::discover_at(session, 0, AgentBinding::new(binding).unwrap()).await
            }
            None => AgentControls::discover(session, 0).await,
        }
        .unwrap();
        let root = controls
            .execute(
                &controls
                    .prepare(PreparedCommand::new(&AgentCommand::Root {}).unwrap())
                    .unwrap(),
            )
            .await
            .unwrap();
        let AgentReply::Root { agent } = root else {
            panic!("serving process did not return its committed root");
        };
        if let Some(original) = &original_root {
            assert_eq!(&agent, original);
            assert_eq!(Some(controls.binding_bytes()), retained);
        } else {
            original_root = Some(agent.clone());
        }
        let (selected, _) = controls.select(agent.clone()).await.unwrap();
        assert_eq!(selected.binding_bytes(), controls.binding_bytes());
        retained = Some(selected.binding_bytes());

        // Join actual physical exit and pipe retirement before reopening the
        // SAME durable home. A live client must terminate when its owner dies.
        server.kill().await?;
        let failure = match selected.select(agent).await {
            Err(failure) => failure,
            Ok(_) => panic!("a retired serving process must terminate discovery"),
        };
        assert!(
            matches!(
                &failure,
                AgentExchangeError::Session(error)
                    if error.category() == StreamCategory::Unavailable
            ),
            "retired serving process returned {failure:?}"
        );
    }
    assert!(model.steps.lock().unwrap().is_empty());
    model.close().await
}

/// Where the scenario runs: this machine's harness, or a served endpoint
/// and the credential it minted.
enum Backend {
    Local,
    Endpoint { url: String, credential: String },
}

async fn scenario_conversation(home: &Home, backend: &Backend) -> Outcome<Arc<Conversation>> {
    let connection = match backend {
        Backend::Local => {
            let owner = Directory::for_workspace(&home.workspace(), Some(&home.owner_state()))?
                .connect()
                .await?;
            acyclic_harness::local::sessions::latest(&owner)
                .await?
                .ok_or("local owner did not select the scenario session")?
        }
        Backend::Endpoint { url, credential } => {
            let options =
                acyclic_harness_client::ConnectionOptions::from_env(Some(url), Some(credential))?;
            Connection::connect(&options).await?
        }
    };
    Ok(Conversation::open(connection))
}

async fn observe_prompt(home: &Home, backend: &Backend) -> Outcome<(Arc<Conversation>, u64)> {
    let conversation = scenario_conversation(home, backend).await?;
    let (inbox, _) = conversation
        .pending_inputs()
        .await?
        .into_iter()
        .find(|(_, prompt)| prompt == SCENARIO)
        .ok_or("owner did not expose the pending scenario prompt")?;
    Ok((conversation, inbox))
}

async fn execution_trace(observation: Outcome<(Arc<Conversation>, u64)>) -> Outcome<Vec<String>> {
    use acyclic_harness::executor::ExecutionEvent;

    let (conversation, input) = observation?;
    let mut after = 0;
    let mut trace = Vec::new();
    while let Some(page) = conversation.execution_page(input, after, false).await?.page {
        for record in page.items {
            match record.event {
                ExecutionEvent::ContextPrepared { step, .. } => {
                    trace.push(format!("{step}: context prepared"));
                }
                ExecutionEvent::ModelStarted { step, purpose, .. } => {
                    trace.push(format!("{step}: {purpose:?} model started"));
                }
                ExecutionEvent::ModelStopped { step, purpose, .. } => {
                    trace.push(format!("{step}: {purpose:?} model stopped"));
                }
                ExecutionEvent::ModelInterrupted {
                    step,
                    purpose,
                    provider_code,
                    ..
                } => {
                    trace.push(format!(
                        "{step}: {purpose:?} model interrupted ({provider_code:?})"
                    ));
                }
                ExecutionEvent::Model {
                    step,
                    usage: Some(_),
                    ..
                }
                | ExecutionEvent::ModelInline {
                    step,
                    usage: Some(_),
                    ..
                } => {
                    trace.push(format!("{step}: model completed"));
                }
                ExecutionEvent::ToolStarted { step, call_id, .. } => {
                    trace.push(format!("{step}: {call_id} started"));
                }
                ExecutionEvent::ToolCompleted { step, call_id, .. } => {
                    trace.push(format!("{step}: {call_id} completed"));
                }
                ExecutionEvent::ToolFailed {
                    step,
                    call_id,
                    reason,
                    cause,
                } => {
                    trace.push(format!(
                        "{step}: {call_id} failed ({reason:?}, {:?})",
                        cause.class
                    ));
                }
                _ => {}
            }
        }
        let Some(next) = page.next else {
            break;
        };
        after = next;
    }
    Ok(trace)
}

/// The scenario through `backend`: the prompt's turn is interrupted while
/// its shell call awaits approval, and a fresh run of the same operation
/// approves it and prints the answer.
async fn scenario(
    name: &str,
    home: &Home,
    model: &Model,
    policy: &Path,
    backend: &Backend,
) -> Outcome<String> {
    let local_owner = match backend {
        Backend::Local => Some(home.local_owner(model, policy).await?),
        Backend::Endpoint { .. } => None,
    };
    let binary = executable()?;
    let run = |prompt: &str, operation: &str, agent: Option<&str>| {
        let mut command = home.client(&binary);
        let mut args = vec!["-p", prompt, "--operation", operation];
        if let Some(agent) = agent {
            args.extend(["--agent", agent]);
        }
        match backend {
            Backend::Local => {
                command
                    .arg("--host-state-directory")
                    .arg(home.owner_state());
            }
            Backend::Endpoint { url, credential } => {
                args.extend(["--endpoint", url.as_str()]);
                command.env("ACYCLIC_API_KEY", credential);
            }
        }
        command.args(args);
        Running::start(command)
    };
    // Reconnect mid-turn: the first client leaves while the approval is
    // pending; the second sees the same pending turn and settles it. The
    // first client's departure must not decide the approval.
    let mut first = run(SCENARIO, "conformance", None)?;
    first.wait_for("approve request").await?;
    eprintln!("GraphCoder {name} reached approval");
    let observation = observe_prompt(home, backend).await;
    first.kill().await?;
    let mut second = run(SCENARIO, "conformance", None)?;
    second.answer("y\n")?;
    let (answer, stderr) = match second.finish().await {
        Ok(output) => output,
        Err(error) => {
            let trace = execution_trace(observation).await;
            return Err(format!("{error}\nOriginal execution events: {trace:?}").into());
        }
    };
    eprintln!("GraphCoder {name} settled the approved turn");
    assert!(stderr.contains("approve request"), "{stderr}");
    // The original agent wait resumes only once the child's own reply settled.
    let spawned: Value = serde_json::from_str(
        model
            .outputs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get("spawn")
            .ok_or("missing spawn result")?,
    )?;
    let agent = spawned
        .get("agent")
        .and_then(Value::as_str)
        .ok_or("spawn named no agent")?;
    let root = agent
        .split_once("/children/")
        .map_or(agent, |(root, _)| root);
    let followup = run(FOLLOWUP, "conformance-follow-up", Some(root))?
        .finish()
        .await;
    let (followup, _) = match followup {
        Ok(output) => output,
        Err(error) => {
            let trace = async {
                let conversation = scenario_conversation(home, backend).await?;
                let settled = conversation.settled().await?;
                let mut traces = Vec::new();
                for entry in conversation.entries(0, settled).await? {
                    let events = execution_trace(Ok((conversation.clone(), entry.inbox))).await;
                    traces.push((entry.inbox, entry.outcome, events));
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(traces)
            }
            .await;
            return Err(format!("{error}\nOriginal follow-up execution events: {trace:?}").into());
        }
    };
    eprintln!("GraphCoder {name} settled the child follow-up");
    if let Some(mut owner) = local_owner {
        tokio::task::spawn_blocking(move || owner.terminate(None)).await??;
    }
    Ok(format!("{}\n{}", answer.trim(), followup.trim()))
}

/// The committed history shape of one backend's run: its answer, each
/// tool call's result less the backend's own identities, and every model
/// step its agents took.
#[derive(Debug, PartialEq)]
struct Shape {
    answer: String,
    results: Vec<Value>,
    steps: BTreeSet<Step>,
}

#[derive(Debug)]
struct UnexpectedChildSettlement(Value);

impl std::fmt::Display for UnexpectedChildSettlement {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            output,
            "child did not settle with its original reply: {}",
            self.0
        )
    }
}

impl std::error::Error for UnexpectedChildSettlement {}

impl Shape {
    fn of(answer: &str, outputs: &Outputs, steps: BTreeSet<Step>) -> Outcome<Self> {
        let parsed = |call: &str| -> Outcome<Value> {
            let output = outputs
                .get(call)
                .ok_or_else(|| format!("missing {call} result"))?;
            Ok(serde_json::from_str(output).unwrap_or_else(|_| output.as_str().into()))
        };
        let written = parsed("write")?;
        assert!(
            written["version"]
                .as_str()
                .is_some_and(|version| !version.is_empty()),
            "write named no immutable version: {written}"
        );
        let read = parsed("read")?;
        assert_eq!(read["path"], written["path"], "read selected another path");
        assert_eq!(
            read["version"], written["version"],
            "read selected another immutable publication"
        );
        assert_eq!(
            read["text"], WRITTEN,
            "immutable read differs from the requested write"
        );
        assert!(
            read["page"].is_null()
                || (read["page"]["start"] == 0
                    && read["page"]["end"] == read["page"]["total"]
                    && read["page"]["eof"] == true),
            "read returned a partial view: {read}"
        );
        // The child's path under the root, less the backend's agent ids.
        let spawned = parsed("spawn")?;
        let waited = parsed("wait")?;
        let settled = waited["settled"]
            .as_array()
            .ok_or("wait named no settlement")?;
        assert_eq!(settled.len(), 1, "{waited}");
        assert_eq!(settled[0]["agent"], spawned["agent"]);
        let completed = json!([
            settled[0]["phase"],
            settled[0]["answer"],
            waited["unsettled"]
        ]);
        if completed != json!(["idle", CHILD_REPLY, []]) {
            return Err(UnexpectedChildSettlement(completed).into());
        }
        let child: Vec<&str> = spawned["agent"]
            .as_str()
            .ok_or("spawn named no child")?
            .split('/')
            .map(|segment| {
                if segment.len() == 32 && segment.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    "<agent>"
                } else {
                    segment
                }
            })
            .collect();
        Ok(Self {
            answer: answer.trim().to_owned(),
            results: vec![
                parsed("shell")?,
                written["path"].clone(),
                read["text"].clone(),
                child.into(),
                completed,
            ],
            steps,
        })
    }
}

async fn backend_shape(directory: &Path, policy: &Path, name: &str) -> Outcome<Shape> {
    let model = model(&directory.join(name)).await?;
    let home = Home::new(directory, name)?;
    let (backend, server) = if name == "local" {
        (Backend::Local, None)
    } else {
        // The seams' server uses the same entry point with an injected verifier.
        let binary = if name == "seams" {
            acyclic_test_support::cargo_binary!("graphcoder-seams")
        } else {
            executable()?
        };
        let command = home.command_of(&binary, &model, Some(policy), &["serve", "127.0.0.1:0"]);
        let mut server = Running::start(command)?;
        let endpoint = served(&mut server).await?;
        let field = |name| {
            endpoint
                .get(name)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or("invalid endpoint announcement")
        };
        let backend = Backend::Endpoint {
            url: field("endpoint")?,
            credential: field("credential")?,
        };
        (backend, Some(server))
    };
    eprintln!("GraphCoder {name} started the scenario");
    let answer = scenario(name, &home, &model, policy, &backend)
        .await
        .map_err(|error| {
            let steps = model.steps.lock().unwrap_or_else(PoisonError::into_inner);
            format!("{name} GraphCoder scenario stopped at model steps {steps:?}: {error}")
        })?;
    if let Some(mut server) = server {
        if name == "seams" {
            server.wait_for("seams: verified the caller").await?;
        }
        server.kill().await?;
    }
    let steps = model
        .steps
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    let outputs = model
        .outputs
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    // A real completed process without the original child reply is no
    // success. Plant that omission in its actual retained tool result.
    let mut missing = outputs.clone();
    let original = missing.get_mut("wait").ok_or("missing wait result")?;
    let mut waited: Value = serde_json::from_str(original)?;
    *waited
        .pointer_mut("/settled/0/answer")
        .ok_or("wait named no child answer")? = Value::Null;
    *original = waited.to_string();
    assert!(matches!(
        Shape::of(&answer, &missing, steps.clone()),
        Err(error) if error.is::<UnexpectedChildSettlement>()
    ));
    let shape = Shape::of(&answer, &outputs, steps)
        .map_err(|error| format!("{name} GraphCoder history: {error}"))?;
    model.close().await?;
    Ok(shape)
}

#[tokio::test(flavor = "multi_thread")]
async fn graphcoder_runs_one_scenario_identically_on_every_backend() -> Outcome {
    if acyclic_native_runtime::execution::skip_if_windows_psec_unavailable() {
        return Ok(());
    }
    let directory = tempfile::tempdir()?;
    // All three backends use the local host's human approval stage. Its
    // principal is distinct from the credential admitted by the endpoint.
    let policy = directory.path().join("policy.json");
    std::fs::write(
        &policy,
        serde_json::to_vec(&json!({"approvers": [
            {"kind": "rules", "principal": "policy:conformance",
             "rules": [{"tool": "shell", "arguments": null, "verdict": "ask"}],
             "fallback": "allow"},
            {"kind": "human", "principal": acyclic_harness::approvals::HUMAN_APPROVER},
        ]}))?,
    )?;
    // Each backend owns its service, files and model endpoint.
    let (local, endpoint, seams) = tokio::try_join!(
        backend_shape(directory.path(), &policy, "local"),
        backend_shape(directory.path(), &policy, "endpoint"),
        backend_shape(directory.path(), &policy, "seams"),
    )?;
    assert_eq!(local.answer, "root answer\nfollowup answer");
    assert!(
        local.results[0]
            .as_str()
            .is_some_and(|text| text.lines().eq(["Exit code: 0", "conformance-shell"])),
        "shell did not present a successful command and its output: {:?}",
        local.results[0]
    );
    assert_eq!(local.results[1], "conformance.txt");
    assert!(local.steps.contains(&("child", 0)), "{:?}", local.steps);
    assert_eq!(endpoint, local, "served endpoint committed another history");
    assert_eq!(seams, local, "injected verifier committed another history");
    Ok(())
}
