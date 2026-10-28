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
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    io::{BufRead as _, BufReader, Write as _},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant},
};

const SCENARIO: &str = "conformance scenario";
const CHILD: &str = "conformance child";
/// The child's answer, which its result carries back to the root.
const CHILD_REPLY: &str = "child reply";
const FOLLOWUP: &str = "conformance follow-up";
const WRITTEN: &str = "conformance write";
const DEADLINE: Duration = Duration::from_secs(60);
/// How long an exited binary's output may still be arriving: a helper it
/// started may hold its pipes open.
const DRAIN: Duration = Duration::from_millis(500);
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

/// A running binary whose output arrives as it is written: stdout by line,
/// stderr as written, since `graphcoder` prompts there without a newline.
struct Running {
    child: Child,
    stdout: mpsc::Receiver<String>,
    stderr: mpsc::Receiver<String>,
    /// Stderr received so far.
    seen: String,
}

fn lines(source: impl std::io::Read + Send + 'static) -> mpsc::Receiver<String> {
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(source).lines() {
            let Ok(line) = line else { return };
            if sender.send(line).is_err() {
                return;
            }
        }
    });
    receiver
}

fn chunks(mut source: impl std::io::Read + Send + 'static) -> mpsc::Receiver<String> {
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut buffer = [0; 4096];
        while let Ok(read @ 1..) = source.read(&mut buffer) {
            let chunk = String::from_utf8_lossy(&buffer[..read]).into_owned();
            if sender.send(chunk).is_err() {
                return;
            }
        }
    });
    receiver
}

impl Running {
    fn start(mut command: Command) -> Self {
        let mut child = command.spawn().unwrap();
        let stdout = lines(child.stdout.take().unwrap());
        let stderr = chunks(child.stderr.take().unwrap());
        Self {
            child,
            stdout,
            stderr,
            seen: String::new(),
        }
    }

    /// The first stdout line.
    fn line(&mut self) -> String {
        match self.stdout.recv_timeout(DEADLINE) {
            Ok(line) => line,
            Err(error) => {
                std::thread::sleep(Duration::from_millis(50));
                self.seen.extend(self.stderr.try_iter());
                panic!("the binary printed no line ({error}): {}", self.seen)
            }
        }
    }

    /// Waits until stderr shows `needle`.
    fn wait_for(&mut self, needle: &str) {
        let deadline = Instant::now() + DEADLINE;
        while !self.seen.contains(needle) {
            assert!(
                Instant::now() < deadline,
                "no {needle:?} on stderr: {}",
                self.seen
            );
            if let Ok(chunk) = self.stderr.recv_timeout(Duration::from_millis(100)) {
                self.seen += &chunk;
            }
        }
    }

    fn answer(&mut self, input: &str) {
        let stdin = self.child.stdin.as_mut().unwrap();
        stdin.write_all(input.as_bytes()).unwrap();
        stdin.flush().unwrap();
    }

    /// Its stdout and stderr once it exits successfully.
    fn finish(mut self) -> (String, String) {
        let deadline = Instant::now() + DEADLINE;
        let status = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() > deadline {
                self.seen.extend(self.stderr.try_iter());
                panic!("the binary did not exit: {}", self.seen);
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        // The readers drain the closed pipes, then hang up.
        let stdout: Vec<_> = std::iter::from_fn(|| self.stdout.recv_timeout(DRAIN).ok()).collect();
        self.seen
            .extend(std::iter::from_fn(|| self.stderr.recv_timeout(DRAIN).ok()));
        assert!(
            status.success(),
            "exit {status:?}\n{stdout:?}\n{}",
            self.seen
        );
        (stdout.join("\n"), std::mem::take(&mut self.seen))
    }

    /// Stops the binary now; dropping it does the same.
    fn kill(self) {
        drop(self);
    }
}

/// A failed assertion leaves no binary running.
impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The endpoint `graphcoder serve` prints once it serves, announced by the
/// harness server itself: the one entry point a managed host serves through,
/// with nothing of the binary's own between them.
fn served(server: &mut Running) -> Value {
    let endpoint: Value =
        serde_json::from_str(&server.line()).expect("the server prints its endpoint as JSON");
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
fn scenario(home: &Home, model: &Model, policy: &Path, backend: &Backend) -> String {
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
    first.wait_for("approve request");
    first.kill();
    let mut second = run(SCENARIO, "conformance");
    second.answer("y\n");
    let (answer, stderr) = second.finish();
    assert!(stderr.contains("approve request"), "{stderr}");
    // The spawned child runs once its parent's turn settled; the follow-up
    // answers after it did.
    let (followup, _) = run(FOLLOWUP, "conformance-follow-up").finish();
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
        let endpoint = served(&mut server);
        server.kill();
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
            let endpoint = served(&mut server);
            assert_eq!(endpoint["principal"], principal.as_str());
            let backend = Backend::Endpoint {
                url: endpoint["endpoint"].as_str().unwrap().to_owned(),
                credential: endpoint["credential"].as_str().unwrap().to_owned(),
            };
            (backend, Some(server))
        };
        let answer = tokio::task::block_in_place(|| scenario(&home, &model, &policy, &backend));
        let backend_url = match &backend {
            Backend::Endpoint { url, .. } => url.clone(),
            Backend::Local => String::new(),
        };
        if let Some(mut server) = server {
            // The injected seams admitted the tree and every client.
            if name == "seams" {
                server.wait_for("seams: verified the caller");
                server.wait_for("seams: verified a session credential");
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
                server.wait_for("seams: verified an opaque credential");
                opaque.kill();
            }
            server.kill();
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
