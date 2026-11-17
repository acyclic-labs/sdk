//! The built `acyclic` binary against the self-hosted endpoint (`acyclic
//! serve`): an external client delivers attributed peer input to a live
//! session, and selected discovery survives a serving process restart.
//!
//! The binary is a declared test tool: Cargo builds it with fixture signing custody
//! (`cargo build -p acyclic-plugin --features acyclic-native-runtime/test-support`), and
//! `ACYCLIC_TEST_BINARY` names it elsewhere than the shared target directory.
use acyclic_native_runtime::{
    CaptureHandler, CaptureSchedule, ProcessCaptureStream, ProcessCommand, ProcessInput,
    ProcessOutput, ProcessStdio, ProcessTree, TaskSpawner,
};
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    io::{self, Write as _},
    path::{Path, PathBuf},
    process::{ChildStdin, ExitStatus},
    sync::{Arc, Mutex},
};

const CHILD: &str = "conformance child";
/// The child's answer, which its result carries back to the root.
const CHILD_REPLY: &str = "child reply";
/// The harness's out-of-turn context review; its reply is a JSON decision,
/// never a scenario step.
const REVIEW: &str = "You manage this worker's context outside its turns.";
/// The harness's compaction request; its reply is a summary, not a step.
const COMPACT: &str = "Summarize this conversation for continued work.";
/// The settlement hook asks every agent for feedback after its turn; it is
/// outside the scenario, so the model declines it and it records no step.
const FEEDBACK: &str = "What slowed you down or blocked you?";

/// The binary every backend runs.
fn executable() -> PathBuf {
    let declared = std::env::var_os("ACYCLIC_TEST_BINARY")
        .or_else(|| std::env::var_os("CARGO_BIN_EXE_acyclic"))
        .or_else(|| option_env!("CARGO_BIN_EXE_acyclic").map(Into::into));
    if let Some(path) = declared {
        return acyclic_test_support::cargo_binary!("acyclic", Some(path));
    }
    // Under Cargo the plugin package's binary sits in the shared target
    // directory beside the conformance test's own directory.
    let deps = std::env::current_exe().unwrap();
    let target = deps.parent().and_then(Path::parent).unwrap();
    let path = target.join(format!("acyclic{}", std::env::consts::EXE_SUFFIX));
    assert!(
        path.exists(),
        "build the acyclic binary first: cargo build -p acyclic-plugin --features acyclic-native-runtime/test-support ({})",
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

fn text(answer: &str) -> Value {
    json!({"type": "message", "content": [{"type": "output_text", "text": answer}]})
}

/// One step a backend asked the model for: whose turn, and how many tool
/// results it already held.
type Step = (&'static str, usize);

/// The scripted turns: the child answers at once, and the harness's
/// out-of-turn requests are answered as data.
fn answer(request: &Value, steps: &Mutex<BTreeSet<Step>>) -> String {
    if request.to_string().contains(REVIEW) {
        let decision = json!({"version": 1, "summary": null, "review_at_input_tokens": 8000});
        return events(&[text(&decision.to_string())]);
    }
    if request.to_string().contains(COMPACT) {
        return events(&[text("The scenario ran.")]);
    }
    let input = request["input"].as_array().cloned().unwrap_or_default();
    let newest = input
        .iter()
        .rposition(|item| item["role"] == "user")
        .unwrap_or_default();
    let prompt = input
        .get(newest)
        .and_then(|item| item["content"][0]["text"].as_str())
        .unwrap_or_default();
    let results = input[newest..]
        .iter()
        .filter(|item| item["type"] == "function_call_output")
        .count();
    if prompt.contains(FEEDBACK) {
        return events(&[text("none")]);
    }
    if prompt.contains(CHILD) {
        steps.lock().unwrap().insert(("child", results));
        return events(&[text(CHILD_REPLY)]);
    }
    if prompt.contains(CHILD_REPLY) {
        return events(&[text("child noted")]);
    }
    events(&[text("none")])
}

/// The loopback Responses endpoint and its catalog.
struct Model {
    catalog: PathBuf,
    steps: Arc<Mutex<BTreeSet<Step>>>,
    last_request: Arc<Mutex<Option<Value>>>,
}

async fn model(directory: &Path) -> Model {
    std::fs::create_dir_all(directory).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let steps = Arc::new(Mutex::new(BTreeSet::new()));
    let recorded = Arc::clone(&steps);
    let last_request = Arc::new(Mutex::new(None));
    let captured = Arc::clone(&last_request);
    let app = Router::new()
        .route(
            "/count",
            post(|| async { Json(json!({"input_tokens": 64})) }),
        )
        .route(
            "/responses",
            post(move |Json(body): Json<Value>| {
                let reply = answer(&body, &recorded);
                *captured.lock().unwrap() = Some(body);
                async move { ([("content-type", "text/event-stream")], reply) }
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
        last_request,
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
            custody: acyclic_native_runtime::signing_key::FixtureCustody::new().unwrap(),
            root,
        }
    }

    fn workspace(&self) -> PathBuf {
        self.root.join("workspace")
    }

    /// The binary with this home's harness arguments, then `args`.
    fn command(&self, model: &Model, policy: Option<&Path>, args: &[&str]) -> ProcessCommand {
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
    ) -> ProcessCommand {
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
            .arg("--settings-root")
            .arg(&settings)
            .arg("--catalog")
            .arg(&model.catalog)
            .args(["--model", "conformance/tiny"]);
        if let Some(policy) = policy {
            command.arg("--approval-policy").arg(policy);
        }
        // The scenario is small; automatic context management only adds
        // out-of-turn model requests.
        if args.contains(&"serve") {
            command.args(["--no-compaction", "--workspace-mounting", "physical"]);
        }
        command
            .args(args)
            .stdin(ProcessStdio::piped())
            .stdout(ProcessStdio::piped())
            .stderr(ProcessStdio::piped());
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
    fn start(mut command: ProcessCommand) -> Self {
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
    let mut command = ProcessCommand::new(std::env::current_exe()?);
    command
        .args(["--exact", "interactive_output_child", "--nocapture"])
        .env("ACYCLIC_CONFORMANCE_OUTPUT_CHILD", "1")
        .stdin(ProcessStdio::piped())
        .stdout(ProcessStdio::piped())
        .stderr(ProcessStdio::piped());
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

/// The endpoint `acyclic serve` prints once it serves, announced by the
/// harness server itself: the one entry point a managed host serves through,
/// with nothing of the binary's own between them.
async fn served(server: &mut Running) -> Value {
    let endpoint: Value = serde_json::from_str(&server.line().await.unwrap())
        .expect("the server prints its endpoint as JSON");
    assert_eq!(endpoint["server"], acyclic_harness::server::IDENTITY);
    endpoint
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_client_delivers_attributed_peer_input_to_live_session() {
    use acyclic_harness::local::{
        Connection,
        agent_conversation::{Conversation, Settlement},
        sessions,
    };
    let directory = tempfile::tempdir().unwrap();
    let model = model(directory.path()).await;
    let home = Home::new(directory.path(), "peer-owner");
    let mut server = Running::start(home.command(&model, None, &["serve", "127.0.0.1:0"]));
    let published = served(&mut server).await;
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
    let client_home = Home::new(directory.path(), "peer-client");
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
    );
    command.env("ACYCLIC_API_KEY", credential);
    let (output, _) = Running::start(command).finish().await;
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
    );
    command.env("ACYCLIC_API_KEY", credential);
    let mut refused = Running::start(command);
    drop(refused.stdin.take());
    while refused.receive().await.unwrap() {}
    assert!(
        !refused.status.unwrap().success(),
        "opted-out sender delivered peer input"
    );
    assert_eq!(model.steps.lock().unwrap().len(), 1);
    server.kill().await;
}

#[cfg(feature = "client-session")]
#[tokio::test(flavor = "multi_thread")]
async fn selected_discovery_survives_a_serving_process_restart() {
    use acyclic_conformance::client::endpoint;
    use acyclic_harness_client::{
        AgentBinding, AgentCommand, AgentControls, AgentExchangeError, AgentReply, PreparedCommand,
    };
    use acyclic_stream::StreamCategory;

    let directory = tempfile::tempdir().unwrap();
    let model = model(directory.path()).await;
    let home = Home::new(directory.path(), "selected");
    let binary = executable();
    let mut retained: Option<Vec<u8>> = None;
    let mut original_root = None;
    for _ in 0..2 {
        let mut server =
            Running::start(home.command_of(&binary, &model, None, &["serve", "127.0.0.1:0"]));
        let published = served(&mut server).await;
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
        server.kill().await;
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
}
