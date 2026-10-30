//! One `GraphCoder` binary, built once, run unchanged against each backend:
//! this machine's harness, the self-hosted endpoint (`graphcoder serve`)
//! over gRPC, and the same server entry point with injected seams
//! (`graphcoder-seams serve`, its own verifier). Only the endpoint
//! differs for the client, and only the seams for the server. Each runs one
//! scenario on the loopback fixture model, a shell call held for one human
//! approval, a file write, a spawned child's turn and a reconnect while the
//! approval is pending, and must commit the same history shape.
//!
//! Build the binary first, with fixture signing custody:
//! `cargo build -p graphcoder --features acyclic-native-runtime/test-support`.
//! `GRAPHCODER` names it elsewhere than the shared target directory.
use acyclic_native_runtime::{
    CaptureHandler, CaptureSchedule, ProcessCaptureStream, ProcessInput, ProcessOutput,
    ProcessTree, TaskSpawner,
};
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    io::{self, Write as _},
    path::{Path, PathBuf},
    process::{ChildStdin, Command, ExitStatus, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

const SCENARIO: &str = "conformance scenario";
const CHILD: &str = "conformance child";
/// The child's answer, which its result carries back to the root.
const CHILD_REPLY: &str = "child reply";
const FOLLOWUP: &str = "conformance follow-up";
const WRITTEN: &str = "conformance write";
/// How long the follow-up waits for the child before answering anyway.
const CHILD_WAIT: Duration = Duration::from_secs(10);

/// The binary every backend runs.
fn executable() -> PathBuf {
    if let Some(path) = std::env::var_os("GRAPHCODER") {
        return path.into();
    }
    let deps = std::env::current_exe().unwrap();
    let target = deps.parent().and_then(Path::parent).unwrap();
    let path = target.join(format!("graphcoder{}", std::env::consts::EXE_SUFFIX));
    assert!(
        path.exists(),
        "build the graphcoder binary first: cargo build -p graphcoder          --features acyclic-native-runtime/test-support ({})",
        path.display()
    );
    path
}

/// A Server-Sent Events body of one Responses generation.
fn events(items: &[Value]) -> String {
    let mut body = String::new();
    for (index, item) in items.iter().enumerate() {
        let event = |kind: &str| json!({"type": kind, "output_index": index, "item": item});
        if item["type"] == "message" {
            let text = item["content"][0]["text"].as_str().unwrap();
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
            "usage": {"input_tokens": 64, "output_tokens": 2}}})
    );
    body
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
fn write_arguments(request: &Value) -> Value {
    let tool = request["tools"]
        .as_array()
        .and_then(|tools| tools.iter().find(|tool| tool["name"] == "write"))
        .expect("the turn offers the write tool");
    let mut arguments = serde_json::Map::new();
    for name in tool["parameters"]["required"].as_array().unwrap() {
        let name = name.as_str().unwrap();
        let value = match name {
            "path" | "display_name" => "conformance.txt",
            "media_type" => "text/plain",
            _ => WRITTEN,
        };
        arguments.insert(name.to_owned(), value.into());
    }
    Value::Object(arguments)
}

/// Each tool call's committed result, by call.
type Outputs = std::collections::BTreeMap<String, String>;

/// One step a backend asked the model for: whose turn, and how many tool
/// results it already held.
type Step = (&'static str, usize);

/// One scripted generation, sent once `after_child` holds if it is set.
struct Reply {
    events: String,
    after_child: bool,
}

/// The scripted turns, told apart by their newest user message: the root's
/// scenario turn calls shell, write and spawn, then answers; the child
/// answers at once; the root acknowledges its child's report; the root's
/// follow-up answers once the child ran.
fn answer(
    request: &Value,
    steps: &Mutex<BTreeSet<Step>>,
    outputs: &Mutex<Outputs>,
    child: &tokio::sync::watch::Sender<bool>,
) -> Reply {
    let input = request["input"].as_array().cloned().unwrap_or_default();
    for item in &input {
        if item["type"] == "function_call_output"
            && let (Some(call), Some(output)) = (item["call_id"].as_str(), item["output"].as_str())
        {
            outputs
                .lock()
                .unwrap()
                .insert(call.to_owned(), output.to_owned());
        }
    }
    let newest = input
        .iter()
        .rposition(|item| item["role"] == "user")
        .unwrap_or_default();
    let prompt = input.get(newest).map(Value::to_string).unwrap_or_default();
    let results = input[newest..]
        .iter()
        .filter(|item| item["type"] == "function_call_output")
        .count();
    let reply = |events| Reply {
        events,
        after_child: false,
    };
    if prompt.contains(CHILD) {
        steps.lock().unwrap().insert(("child", results));
        child.send_replace(true);
        return reply(events(&[text(CHILD_REPLY)]));
    }
    // The root's report of its child's result, which carries the child's
    // reply. The child decides when it arrives, before or after the
    // follow-up, so it records no step.
    if prompt.contains(CHILD_REPLY) {
        return reply(events(&[text("child noted")]));
    }
    if prompt.contains(FOLLOWUP) {
        steps.lock().unwrap().insert(("followup", results));
        return Reply {
            events: events(&[text("followup answer")]),
            after_child: true,
        };
    }
    steps.lock().unwrap().insert(("root", results));
    reply(events(&[match results {
        0 => call(
            "shell",
            "shell",
            &json!({"command": "echo conformance-shell"}),
        ),
        1 => call("write", "write", &write_arguments(request)),
        2 => call("spawn", "spawn", &json!({"task": CHILD, "name": "child"})),
        _ => text("root answer"),
    }]))
}

/// The loopback Responses endpoint and its catalog.
struct Model {
    catalog: PathBuf,
    steps: Arc<Mutex<BTreeSet<Step>>>,
    outputs: Arc<Mutex<Outputs>>,
}

async fn model(directory: &Path) -> Model {
    std::fs::create_dir_all(directory).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let steps = Arc::new(Mutex::new(BTreeSet::new()));
    let recorded = Arc::clone(&steps);
    let outputs = Arc::new(Mutex::new(Outputs::new()));
    let seen = Arc::clone(&outputs);
    let (child, ran) = tokio::sync::watch::channel(false);
    let app = Router::new()
        .route(
            "/count",
            post(|| async { Json(json!({"input_tokens": 64})) }),
        )
        .route(
            "/responses",
            post(move |Json(body): Json<Value>| {
                let reply = answer(&body, &recorded, &seen, &child);
                let mut ran = ran.clone();
                async move {
                    if reply.after_child {
                        let _ = tokio::time::timeout(CHILD_WAIT, ran.wait_for(|ran| *ran)).await;
                    }
                    ([("content-type", "text/event-stream")], reply.events)
                }
            }),
        );
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let catalog = directory.join("catalog.json");
    std::fs::write(&catalog, serde_json::to_vec(&json!({"conformance":{
        "adapter":"responses","authentication":"optional","base_url":format!("http://{address}"),
        "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}}},"max_event_bytes":65536,
        "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
    }})).unwrap()).unwrap();
    Model {
        catalog,
        steps,
        outputs,
    }
}

/// One isolated harness home: custody, workspace and settings.
struct Home {
    custody: acyclic_native_runtime::signing_key::FixtureCustody,
    root: PathBuf,
}

impl Home {
    fn new(directory: &Path, name: &str) -> Self {
        let root = directory.join(name);
        for path in ["workspace", "settings/.codex"] {
            std::fs::create_dir_all(root.join(path)).unwrap();
        }
        Self {
            custody: acyclic_native_runtime::signing_key::FixtureCustody::new(
                &root.join("custody"),
            )
            .unwrap(),
            root,
        }
    }

    fn workspace(&self) -> PathBuf {
        self.root.join("workspace")
    }

    /// The binary with this home's harness arguments, then `args`.
    fn command(&self, model: &Model, policy: Option<&Path>, args: &[&str]) -> Command {
        self.command_of(&executable(), model, policy, args)
    }

    /// `binary`, a build of `GraphCoder`, with this home's harness arguments,
    /// then `args`.
    fn command_of(
        &self,
        binary: &Path,
        model: &Model,
        policy: Option<&Path>,
        args: &[&str],
    ) -> Command {
        let settings = self.root.join("settings");
        let mut command = Command::new(binary);
        self.custody.configure(&mut command);
        command
            .env("CODEX_HOME", settings.join(".codex"))
            .env("HOME", &settings)
            .env("USERPROFILE", &settings)
            .env("LOCALAPPDATA", &settings)
            .env("XDG_DATA_HOME", &settings)
            .env("TERM", "dumb")
            .env("GRAPHCODER_LOG", "warn")
            .env_remove("GRAPHCODER_MODEL")
            .env_remove("ACYCLIC_ENDPOINT")
            .env_remove("ACYCLIC_API_KEY")
            .current_dir(self.workspace())
            .arg("--workspace")
            .arg(self.workspace())
            .arg("--settings-root")
            .arg(&settings)
            .arg("--catalog")
            .arg(&model.catalog)
            .args(["--model", "conformance/tiny"]);
        if let Some(policy) = policy {
            command.arg("--approval-policy").arg(policy);
        }
        command
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
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
    fn start(mut command: Command) -> Self {
        let mut child = ProcessTree::spawn(&mut command).unwrap();
        let stdin = child.take_stdin();
        let output = child.stream_capture(
            &TaskSpawner::current(),
            CaptureSchedule::Unbounded,
            usize::MAX,
            None,
            OutputHandler,
        );
        Self {
            output: Some(output),
            stdin,
            stdout: Vec::new(),
            stderr: Vec::new(),
            line_start: 0,
            status: None,
        }
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
        let completed = self.output.take().unwrap().finish().await?;
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
            if let Some(offset) = self.stdout[scanned..]
                .iter()
                .position(|byte| *byte == b'\n')
            {
                let end = scanned + offset;
                let line = self.stdout[self.line_start..end]
                    .strip_suffix(b"\r")
                    .unwrap_or(&self.stdout[self.line_start..end]);
                let line = String::from_utf8(line.to_vec()).map_err(io::Error::other)?;
                self.line_start = end + 1;
                return Ok(line);
            }
            scanned = self.stdout.len();
            if !self.receive().await? {
                if self.line_start == self.stdout.len() {
                    return Err(self.closed("a stdout line"));
                }
                let line = String::from_utf8(self.stdout[self.line_start..].to_vec())
                    .map_err(io::Error::other)?;
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
            if self.stderr[scanned..]
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

    fn answer(&mut self, input: &str) {
        self.stdin
            .as_mut()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
    }

    /// Original exit and pipe EOF settle before either output is inspected.
    async fn finish(mut self) -> (String, String) {
        drop(self.stdin.take());
        while self.receive().await.unwrap() {}
        let stdout = String::from_utf8(self.stdout[self.line_start..].to_vec()).unwrap();
        let stderr = String::from_utf8(self.stderr).unwrap();
        let status = self.status.unwrap();
        assert!(status.success(), "exit {status:?}\n{stdout}\n{stderr}");
        (stdout, stderr)
    }

    /// Closing output credit wakes even a quiet or backpressured child. Join
    /// the same native cleanup owner before starting the replacement client.
    async fn kill(mut self) {
        drop(self.stdin.take());
        if let Some(output) = self.output.take() {
            let completed = output.finish().await.unwrap();
            if let Err(failure) = completed.result {
                assert!(failure.cleanup_completed, "{failure:?}");
            }
        }
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
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args(["--exact", "interactive_output_child", "--nocapture"])
        .env("ACYCLIC_CONFORMANCE_OUTPUT_CHILD", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = Running::start(command);
    // Consume the test executable's own banner, then the original child's
    // readiness line. Closing that child before the line is a typed EOF.
    while !child.line().await?.ends_with("child-ready") {}
    while child.stderr.is_empty() {
        assert!(child.receive().await?);
    }
    assert_eq!(child.stderr, [0xe2]);
    child.answer("!");
    child.wait_for("☃ stderr delivered").await?;
    let missing = child
        .wait_for("never written by the child")
        .await
        .unwrap_err();
    assert_eq!(missing.kind(), io::ErrorKind::UnexpectedEof);
    let (stdout, stderr) = child.finish().await;
    assert!(stdout.contains("final output"));
    assert_eq!(stderr, "☃ stderr delivered\n");
    Ok(())
}

/// The endpoint `graphcoder serve` prints once it serves, announced by the
/// harness server itself: the one entry point a managed host serves through,
/// with nothing of the binary's own between them.
async fn served(server: &mut Running) -> Value {
    let endpoint: Value = serde_json::from_str(&server.line().await.unwrap())
        .expect("the server prints its endpoint as JSON");
    assert_eq!(endpoint["server"], acyclic_harness::server::IDENTITY);
    endpoint
}

/// Where the scenario runs: this machine's harness, or a served endpoint
/// and the credential it minted.
enum Backend {
    Local,
    Endpoint { url: String, credential: String },
}

/// The scenario through `backend`: the prompt's turn is interrupted while
/// its shell call awaits approval, and a fresh run of the same operation
/// approves it and prints the answer.
async fn scenario(home: &Home, model: &Model, policy: &Path, backend: &Backend) -> String {
    let run = |prompt: &str, operation: &str| {
        let mut args = vec!["-p", prompt, "--operation", operation];
        Running::start(match backend {
            Backend::Local => home.command(model, Some(policy), &args),
            Backend::Endpoint { url, credential } => {
                args.extend(["--endpoint", url.as_str()]);
                let mut command = home.command(model, None, &args);
                command.env("ACYCLIC_API_KEY", credential);
                command
            }
        })
    };
    // Reconnect mid-turn: the first client leaves while the approval is
    // pending; the second sees the same pending turn and settles it.
    let mut first = run(SCENARIO, "conformance");
    first.wait_for("approve request").await.unwrap();
    first.kill().await;
    let mut second = run(SCENARIO, "conformance");
    second.answer("y\n");
    let (answer, stderr) = second.finish().await;
    assert!(stderr.contains("approve request"), "{stderr}");
    // The spawned child runs once its parent's turn settled; the follow-up
    // answers after it did.
    let (followup, _) = run(FOLLOWUP, "conformance-follow-up").finish().await;
    format!("{}\n{}", answer.trim(), followup.trim())
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

impl Shape {
    fn of(answer: String, outputs: &Outputs, steps: BTreeSet<Step>) -> Self {
        let parsed = |call: &str| -> Value {
            let output = outputs
                .get(call)
                .unwrap_or_else(|| panic!("no {call} result: {outputs:?}"));
            serde_json::from_str(output).unwrap_or_else(|_| output.as_str().into())
        };
        let pick = |value: &Value, pointers: &[&str]| -> Value {
            pointers
                .iter()
                .map(|pointer| value.pointer(pointer).cloned().unwrap_or_default())
                .collect()
        };
        // The child's path under the root, less the backend's agent ids.
        let spawned = parsed("spawn");
        let child: Vec<&str> = spawned["agent"]
            .as_str()
            .unwrap_or_else(|| panic!("spawn named no child: {spawned}"))
            .split('/')
            .map(|segment| {
                if segment.len() == 32 && segment.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    "<agent>"
                } else {
                    segment
                }
            })
            .collect();
        Self {
            answer: answer.trim().to_owned(),
            results: vec![
                pick(&parsed("shell"), &["/exit_code", "/stdout"]),
                pick(
                    &parsed("write"),
                    &["/file/descriptor", "/file/path", "/file/volume/class"],
                ),
                child.into(),
            ],
            steps,
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn graphcoder_runs_one_scenario_identically_on_every_backend() {
    let directory = tempfile::tempdir().unwrap();
    // The approver is the principal the endpoint credential authenticates,
    // read from a minted credential rather than assumed.
    let principal = {
        let model = model(directory.path()).await;
        let home = Home::new(directory.path(), "probe");
        let mut server = Running::start(home.command(&model, None, &["serve", "127.0.0.1:0"]));
        let endpoint = served(&mut server).await;
        server.kill().await;
        endpoint["principal"].as_str().unwrap().to_owned()
    };
    let policy = directory.path().join("policy.json");
    std::fs::write(
        &policy,
        serde_json::to_vec(&json!({"approvers": [
            {"kind": "rules", "principal": "policy:conformance",
             "rules": [{"tool": "shell", "arguments": null, "verdict": "ask"}],
             "fallback": "allow"},
            {"kind": "human", "principal": principal},
        ]}))
        .unwrap(),
    )
    .unwrap();
    let mut shapes = Vec::new();
    // The seams' server: the same entry point with this package's own
    // verifier injected.
    let seams = PathBuf::from(env!("CARGO_BIN_EXE_graphcoder-seams"));
    for name in ["local", "endpoint", "seams"] {
        let model = model(&directory.path().join(name)).await;
        let home = Home::new(directory.path(), name);
        let (backend, server) = if name == "local" {
            (Backend::Local, None)
        } else {
            let binary = if name == "seams" {
                seams.clone()
            } else {
                executable()
            };
            let mut server = Running::start(home.command_of(
                &binary,
                &model,
                Some(&policy),
                &["serve", "127.0.0.1:0"],
            ));
            let endpoint = served(&mut server).await;
            assert_eq!(endpoint["principal"], principal.as_str());
            let backend = Backend::Endpoint {
                url: endpoint["endpoint"].as_str().unwrap().to_owned(),
                credential: endpoint["credential"].as_str().unwrap().to_owned(),
            };
            (backend, Some(server))
        };
        let answer = scenario(&home, &model, &policy, &backend).await;
        let backend_url = match &backend {
            Backend::Endpoint { url, .. } => url.clone(),
            Backend::Local => String::new(),
        };
        if let Some(mut server) = server {
            // The injected seams admitted the tree and every client.
            if name == "seams" {
                server.wait_for("seams: verified the caller").await.unwrap();
                server
                    .wait_for("seams: verified a session credential")
                    .await
                    .unwrap();
                // A credential in a gateway's own format, a dotted JWS, is
                // carried opaque to the verifier, not refused by the client.
                let url = backend_url.as_str();
                let mut command = home.command(
                    &model,
                    None,
                    &["-p", "hi", "--operation", "opaque", "--endpoint", url],
                );
                command.env(
                    "ACYCLIC_API_KEY",
                    "eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJzZWFtcyJ9.c2lnbmF0dXJl",
                );
                let opaque = Running::start(command);
                server
                    .wait_for("seams: verified an opaque credential")
                    .await
                    .unwrap();
                opaque.kill().await;
            }
            server.kill().await;
        }
        let steps = model.steps.lock().unwrap().clone();
        let outputs = model.outputs.lock().unwrap().clone();
        shapes.push((name, Shape::of(answer, &outputs, steps)));
    }
    let (_, local) = &shapes[0];
    assert_eq!(local.answer, "root answer\nfollowup answer");
    assert_eq!(local.results[0], json!([0, "conformance-shell\n"]));
    assert_eq!(
        local.results[1][0]["byte_length"],
        WRITTEN.len(),
        "{:?}",
        local.results
    );
    assert!(local.steps.contains(&("child", 0)), "{:?}", local.steps);
    for (name, shape) in &shapes[1..] {
        assert_eq!(shape.answer, local.answer, "{name} answered otherwise");
        assert_eq!(shape.steps, local.steps, "{name} took other steps");
        for (index, (theirs, ours)) in shape.results.iter().zip(&local.results).enumerate() {
            assert_eq!(
                theirs.to_string(),
                ours.to_string(),
                "{name} result {index}"
            );
        }
        assert_eq!(shape, local, "{name} committed another history");
    }
}
