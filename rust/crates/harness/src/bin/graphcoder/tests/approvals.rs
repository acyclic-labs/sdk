//! The shipped CLI consumes policy data and follows the real approval files.
use super::*;
use acyclic_harness::{
    interaction::ApprovalDecisionFile,
    task_volume::{Component, FileValue},
};
use std::io::{BufRead as _, BufReader};
mod recovery;

#[derive(Clone, Copy, Debug)]
enum Case {
    Default,
    RuleAllow,
    RuleDeny,
    LlmAllow,
    LlmDeny,
    LlmForged,
    HumanAllow,
    HumanDeny,
    ReadHumanAllow,
    ReadHumanDeny,
    ReadHumanForged,
    ForgedPrincipal,
    ForgedSubject,
    Chain,
    ParentAllow,
    ParentDeny,
    ParentForged,
    ParentCompletes,
    ParentError,
    ParentHumanWait,
    ChildHumanAllow,
    ChildHumanDeny,
    ChildHumanForged,
    ChildHumanDisconnected,
}
impl Case {
    fn read_file(self) -> bool {
        matches!(
            self,
            Self::ReadHumanAllow | Self::ReadHumanDeny | Self::ReadHumanForged
        )
    }
    fn child_human(self) -> bool {
        matches!(
            self,
            Self::ChildHumanAllow
                | Self::ChildHumanDeny
                | Self::ChildHumanForged
                | Self::ChildHumanDisconnected
        )
    }
    fn parent(self) -> bool {
        matches!(
            self,
            Self::ParentAllow
                | Self::ParentDeny
                | Self::ParentForged
                | Self::ParentCompletes
                | Self::ParentError
                | Self::ParentHumanWait
        )
    }
    fn human(self) -> bool {
        self.child_human()
            || matches!(
                self,
                Self::HumanAllow
                    | Self::HumanDeny
                    | Self::ReadHumanAllow
                    | Self::ReadHumanDeny
                    | Self::ReadHumanForged
                    | Self::ForgedPrincipal
                    | Self::ForgedSubject
                    | Self::LlmForged
                    | Self::Chain
                    | Self::ParentHumanWait
            )
    }
    fn allowed(self) -> bool {
        matches!(
            self,
            Self::Default
                | Self::RuleAllow
                | Self::LlmAllow
                | Self::HumanAllow
                | Self::ReadHumanAllow
                | Self::Chain
                | Self::ParentAllow
                | Self::ParentHumanWait
                | Self::ChildHumanAllow
        )
    }
    fn forged(self) -> bool {
        matches!(
            self,
            Self::ForgedPrincipal
                | Self::ForgedSubject
                | Self::ReadHumanForged
                | Self::ParentForged
                | Self::ChildHumanForged
                | Self::LlmForged
        )
    }
}
fn rule(fallback: &str) -> Value {
    json!({"kind":"rules","principal":"rules","rules":[],"fallback":fallback})
}
fn policy(case: Case, address: std::net::SocketAddr) -> Value {
    let human = json!({"kind":"human","principal":"human"});
    let llm = json!({"kind":"llm","principal":"reviewer","model":"graphcoder-qual/reviewer","prompt":"REVIEW_FIXTURE"});
    let stages = match case {
        Case::RuleAllow | Case::RuleDeny => vec![json!({"kind":"rules","principal":"rules",
            "rules":[{"tool":"url_fetch","arguments":{"url":format!("http://{address}/effect")},
                "selectors":[{"kind":"pattern","pointer":"/url","pattern":if matches!(case,Case::RuleDeny) {
                    format!("http://{address}/changed*")
                } else { format!("http://{address}/e*") }},
                {"kind":"equal","pointer":"/url","value":format!("http://{address}/effect")}],
                "verdict":"allow"}],"fallback":"deny"})],
        Case::Default => vec![rule("allow")],
        Case::LlmAllow | Case::LlmDeny => vec![llm],
        Case::LlmForged => vec![llm, human],
        Case::ReadHumanAllow | Case::ReadHumanDeny | Case::ReadHumanForged => vec![
            json!({"kind":"rules","principal":"rules","rules":[{"tool":"read","arguments":null,"verdict":"ask"}],"fallback":"allow"}),
            human,
        ],
        Case::Chain => vec![rule("ask"), llm, human],
        Case::ForgedPrincipal => vec![rule("ask"), human],
        Case::ChildHumanAllow
        | Case::ChildHumanDeny
        | Case::ChildHumanForged
        | Case::ChildHumanDisconnected => vec![
            json!({"kind":"rules","principal":"rules","rules":[{"tool":"url_fetch","arguments":null,"verdict":"ask"}],"fallback":"allow"}),
            human,
        ],
        Case::ParentAllow
        | Case::ParentDeny
        | Case::ParentForged
        | Case::ParentCompletes
        | Case::ParentError
        | Case::ParentHumanWait => vec![
            json!({"kind":"rules","principal":"rules","rules":[{"tool":"url_fetch","arguments":null,"verdict":"ask"}],"fallback":"allow"}),
            json!({"kind":"parent"}),
            human,
        ],
        _ => vec![human],
    };
    let mut policy = json!({"approvers":stages});
    if matches!(case, Case::ParentHumanWait) {
        policy
            .get_mut("approvers")
            .unwrap()
            .as_array_mut()
            .unwrap()
            .first_mut()
            .unwrap()
            .get_mut("rules")
            .unwrap()
            .as_array_mut()
            .unwrap()
            .push(json!({"tool":"wait","arguments":null,"verdict":"ask"}));
    }
    policy
}
fn response_text(text: &str, streamed: &str) -> String {
    let step = harness_mock_model::Step {
        text: vec![streamed.to_owned()],
        ..Default::default()
    };
    harness_mock_model::responses_with_completed_text(&step, 64, text)
        .unwrap()
        .concat()
}
fn call(name: &str, arguments: &Value, sequence: usize) -> String {
    let step = harness_mock_model::Step {
        tools: vec![harness_mock_model::Tool {
            id: format!("approval-{sequence}"),
            name: name.to_owned(),
            namespace: None,
            arguments: arguments.clone(),
        }],
        ..Default::default()
    };
    harness_mock_model::events("responses", &step)
        .unwrap()
        .concat()
}
fn tool_result(body: &Value) -> Value {
    let item = body["input"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|item| item["type"] == "function_call_output")
        .unwrap();
    serde_json::from_str(item["output"].as_str().unwrap()).unwrap()
}
fn decision(request: &FileValue, principal: &str, approved: bool) -> FileValue {
    let FileKey::Task {
        task,
        file: TaskFile::ApprovalRequest(id),
    } = &request.key
    else {
        panic!("request key")
    };
    let hash = acyclic_harness::conversation::FileDescriptor::from_bytes(
        &request.bytes,
        "application/json",
        &mut None,
    )
    .unwrap();
    FileValue::new(
        FileKey::Task {
            task: task.clone(),
            file: TaskFile::ApprovalDecision {
                request: id.clone(),
                principal: Component::new(ApprovalDecisionFile::principal_file(principal)).unwrap(),
            },
        },
        serde_json::to_vec(
            &ApprovalDecisionFile::new(principal, hash.sha256(), 1, approved, None).unwrap(),
        )
        .unwrap(),
    )
}

fn executable() -> std::path::PathBuf {
    let compiled = Path::new(build_metadata::EXECUTABLE);
    // Cross-built integration fixtures are exported beside the executable
    // identified by Cargo JSON, preserving Cargo's binary filename.
    let exported = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .join(compiled.file_name().unwrap());
    select_executable(
        acyclic_harness::test_support::environment("ACYCLIC_TEST_BINARY")
            .map(std::path::PathBuf::from),
        compiled,
        &exported,
    )
}

fn select_executable(
    explicit: Option<std::path::PathBuf>,
    compiled: &Path,
    exported: &Path,
) -> std::path::PathBuf {
    let selected = explicit.unwrap_or_else(|| {
        if compiled.is_file() {
            compiled.to_owned()
        } else {
            exported.to_owned()
        }
    });
    assert!(
        selected.is_absolute() && selected.is_file(),
        "missing shipping binary: {}",
        selected.display()
    );
    selected
}

#[test]
fn approvals_use_the_declared_binary_and_refuse_a_missing_override() -> std::io::Result<()> {
    let root = tempfile::tempdir()?;
    let compiled = root.path().join("compiled");
    let shipping = root.path().join("shipping");
    let exported = root.path().join("exported");
    for path in [&compiled, &shipping, &exported] {
        std::fs::write(path, b"fixture")?;
    }
    assert_eq!(
        select_executable(Some(shipping.clone()), &compiled, &exported),
        shipping
    );
    assert_ne!(
        compiled, shipping,
        "the old compile-time-first selector ignores the declared binary"
    );
    assert_eq!(select_executable(None, &compiled, &exported), compiled);
    std::fs::remove_file(&compiled)?;
    assert_eq!(select_executable(None, &compiled, &exported), exported);
    std::fs::write(&compiled, b"fixture")?;
    let missing = root.path().join("missing");
    assert!(
        std::panic::catch_unwind(|| select_executable(Some(missing), &compiled, &exported))
            .is_err()
    );
    Ok(())
}

#[derive(Clone)]
struct Fixture {
    case: Case,
    address: std::net::SocketAddr,
    parent_task: acyclic_harness::TaskId,
    effects: Arc<AtomicUsize>,
    root_calls: Arc<AtomicUsize>,
    child_calls: Arc<AtomicUsize>,
    reviews: Arc<AtomicUsize>,
    review_started: Option<Arc<tokio::sync::Notify>>,
}
impl Fixture {
    fn review(&self, body: &Value) -> String {
        self.reviews.fetch_add(1, Ordering::SeqCst);
        assert!(
            body["tools"].as_array().is_none_or(Vec::is_empty),
            "approver inherited tools: {body}"
        );
        assert!(body.to_string().contains("REVIEW_FIXTURE"));
        let verdict = match self.case {
            Case::LlmDeny => "deny",
            Case::Chain | Case::LlmForged => "ask",
            _ => "allow",
        };
        let final_text = json!({"verdict":verdict,"reason":"independent review"}).to_string();
        // The denial scenario must block the real effect even if streamed
        // text provisionally allowed it before the authoritative final message.
        let streamed = if matches!(self.case, Case::LlmDeny) {
            json!({"verdict":"allow","reason":"provisional"}).to_string()
        } else {
            final_text.clone()
        };
        response_text(&final_text, &streamed)
    }
    fn model(&self, body: &Value) -> String {
        if body.get("model").unwrap() == "reviewer" {
            return self.review(body);
        }
        // Each child turn retains its receive instruction, while a later inbox
        // turn need not repeat the initial CHILD_APPROVAL_FIXTURE message.
        let child = body["input"].as_array().unwrap().iter().any(|item| {
            item["role"] == "user"
                && item["content"].as_array().is_some_and(|content| {
                    content
                        .iter()
                        .any(|part| part["text"] == "Follow the received child request.")
                })
        });
        let counter = if child {
            &self.child_calls
        } else {
            &self.root_calls
        };
        let step = counter.fetch_add(1, Ordering::SeqCst);
        if !child && (self.case.parent() || self.case.child_human()) {
            return self.parent(body, step);
        }
        if self.case.read_file() {
            return match step {
                0 => call(
                    "write",
                    &json!({"path":"probe.txt","text":"approval read content\n",
                    "media_type":"text/plain","display_name":"probe.txt"}),
                    step,
                ),
                1 => {
                    let result = tool_result(body);
                    let file = result.get("file").expect("setup write file").clone();
                    assert!(
                        file.is_object(),
                        "setup write did not return its immutable file: {result}"
                    );
                    call("read", &json!({"file":file}), step)
                }
                2 => {
                    assert_eq!(tool_result(body), json!("approval read content\n"));
                    self.effects.fetch_add(1, Ordering::SeqCst);
                    response_text("fixture answer", "fixture answer")
                }
                _ => panic!("read approval repeated the effect"),
            };
        }
        if step == 0 {
            call(
                "url_fetch",
                &json!({"url":format!("http://{}/effect",self.address)}),
                step,
            )
        } else {
            response_text("fixture answer", "fixture answer")
        }
    }
    fn parent(&self, body: &Value, step: usize) -> String {
        match step {
            0 => call(
                "spawn",
                &json!({"task":"CHILD_APPROVAL_FIXTURE","name":"worker","grants":null}),
                step,
            ),
            1 => {
                let result = tool_result(body);
                let reference = result.get("agent").unwrap().as_str().unwrap();
                call("wait", &json!({"agents":[reference],"mode":"all"}), step)
            }
            2 => {
                if self.case.child_human() {
                    return response_text("fixture answer", "fixture answer");
                }
                let result = tool_result(body);
                let agent = result
                    .get("agents")
                    .unwrap()
                    .as_array()
                    .unwrap()
                    .first()
                    .unwrap();
                let approval = &agent["approval"];
                assert!(
                    !approval.is_null(),
                    "parent was not woken by the approval: {result}"
                );
                if matches!(self.case, Case::ParentCompletes) {
                    return response_text("fixture answer", "fixture answer");
                }
                if matches!(self.case, Case::ParentError) {
                    return "data: malformed\n\n".into();
                }
                let principal = format!("parent:{}", self.parent_task);
                let hash = if matches!(self.case, Case::ParentForged) {
                    "11".repeat(32)
                } else {
                    approval["request_sha256"].as_str().unwrap().to_owned()
                };
                let file = FileValue::new(FileKey::Task { task:Component::new(approval["task"].as_str().unwrap()).unwrap(),
                    file:TaskFile::ApprovalDecision { request:Component::new(approval["request"].as_str().unwrap()).unwrap(),
                        principal:Component::new(ApprovalDecisionFile::principal_file(&principal)).unwrap() } },
                    serde_json::to_vec(&json!({"principal":principal,"request_sha256":hash,"expected_version":1,
                        "verdict":if matches!(self.case,Case::ParentDeny) {"deny"} else {"approve"},"reason":null})).unwrap());
                call(
                    "send",
                    &json!({"agent":agent["reference"],"message":json!({"approval_decision":file}).to_string()}),
                    step,
                )
            }
            3 if matches!(self.case, Case::ParentHumanWait) => {
                response_text("fixture answer", "fixture answer")
            }
            3 => call("wait", &json!({"agents":["worker"],"mode":"all"}), step),
            _ => response_text("fixture answer", "fixture answer"),
        }
    }
    fn router(self) -> Router {
        let fetched = self.effects.clone();
        Router::new()
            .route(
                "/count",
                post(|| async { Json(json!({"input_tokens":64})) }),
            )
            .route(
                "/effect",
                axum::routing::get(move || {
                    let fetched = fetched.clone();
                    async move {
                        fetched.fetch_add(1, Ordering::SeqCst);
                        "approval side effect"
                    }
                }),
            )
            .route(
                "/responses",
                post(move |Json(body): Json<Value>| {
                    let fixture = self.clone();
                    async move {
                        if body.get("model").unwrap() == "reviewer"
                            && let Some(started) = &fixture.review_started
                        {
                            fixture.reviews.fetch_add(1, Ordering::SeqCst);
                            started.notify_one();
                            std::future::pending::<()>().await;
                        }
                        (
                            [("content-type", "text/event-stream")],
                            fixture.model(&body),
                        )
                    }
                }),
            )
    }
    fn verify(&self, output: &str) {
        assert_eq!(
            self.effects.load(Ordering::SeqCst),
            usize::from(self.case.allowed()),
            "{:?}: {output}",
            self.case
        );
        assert_eq!(
            self.reviews.load(Ordering::SeqCst),
            usize::from(matches!(
                self.case,
                Case::LlmAllow | Case::LlmDeny | Case::LlmForged | Case::Chain
            ))
        );
        if self.case.parent() {
            assert!(
                self.root_calls.load(Ordering::SeqCst) >= 3,
                "parent never answered the request: {output}"
            );
            assert!(
                self.child_calls.load(Ordering::SeqCst) >= 1,
                "child never requested a tool: {output}"
            );
        }
    }
}
struct Files {
    _directory: tempfile::TempDir,
    workspace: std::path::PathBuf,
    settings: std::path::PathBuf,
    catalog: std::path::PathBuf,
    policy: std::path::PathBuf,
}
impl Files {
    fn new(case: Case, address: std::net::SocketAddr) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let workspace = directory.path().join("workspace");
        let settings = directory.path().join("settings");
        std::fs::create_dir(&workspace).unwrap();
        for name in ["codex-home", ".codex", "AppData/Local", "AppData/Roaming"] {
            std::fs::create_dir_all(settings.join(name)).unwrap();
        }
        std::fs::write(settings.join(".codex/config.toml"), "").unwrap();
        let catalog = catalog(directory.path(), address);
        let mut data: Value = serde_json::from_slice(&std::fs::read(&catalog).unwrap()).unwrap();
        let models = data
            .pointer_mut("/graphcoder-qual/models")
            .unwrap()
            .as_object_mut()
            .unwrap();
        let reviewer = models.get("tiny").unwrap().clone();
        models.insert("reviewer".into(), reviewer);
        std::fs::write(&catalog, serde_json::to_vec(&data).unwrap()).unwrap();
        let policy_path = directory.path().join("policy.json");
        std::fs::write(&policy_path, policy(case, address).to_string()).unwrap();
        Self {
            _directory: directory,
            workspace,
            settings,
            catalog,
            policy: policy_path,
        }
    }
    fn command(&self, case: Case, operation: OperationId) -> Command {
        let mut command = self.base_command(case);
        command.args([
            "--operation",
            &operation.to_string(),
            "--input-format",
            "jsonl",
        ]);
        command
    }
    fn base_command(&self, case: Case) -> Command {
        let mut command = Command::new(executable());
        command
            .current_dir(&self.workspace)
            .env("HOME", &self.settings)
            .env("USERPROFILE", &self.settings)
            .env("LOCALAPPDATA", &self.settings)
            .env("APPDATA", &self.settings)
            .env("XDG_DATA_HOME", &self.settings)
            .env("CODEX_HOME", self.settings.join("codex-home"))
            .env_remove("OPENAI_API_KEY")
            .env_remove("GRAPHCODER_MODEL")
            .arg("--workspace")
            .arg(&self.workspace)
            .arg("--settings-root")
            .arg(&self.settings)
            .arg("--catalog")
            .arg(&self.catalog)
            .args(["--model", "graphcoder-qual/tiny"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if !matches!(case, Case::Default) {
            command.arg("--approval-policy").arg(&self.policy);
        }
        command
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn approvals_prompt_mode_drives_admission_and_default_policy() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let fixture = Fixture {
        case: Case::Default,
        address,
        parent_task: acyclic_harness::TaskId::from_bytes([1; 16]),
        effects: Arc::new(AtomicUsize::new(0)),
        root_calls: Arc::new(AtomicUsize::new(0)),
        child_calls: Arc::new(AtomicUsize::new(0)),
        reviews: Arc::new(AtomicUsize::new(0)),
        review_started: None,
    };
    let app = fixture.clone().router();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let files = Files::new(Case::Default, address);
    let mut command = files.base_command(Case::Default);
    command.args(["-p", "hello"]);
    let output = tokio::task::spawn_blocking(move || command.output().unwrap())
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    fixture.verify(&String::from_utf8_lossy(&output.stdout));
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 2);
    server.abort();
    let _ = server.await;
}
struct Followed {
    root: Component,
    requests: std::collections::BTreeMap<(Component, Component), FileValue>,
    submitted: bool,
    saw_decision: bool,
}

fn read_setup_approval(case: Case, file: &FileValue) -> bool {
    if !case.read_file() {
        return false;
    }
    let interaction: acyclic_harness::interaction::Interaction =
        serde_json::from_slice(&file.bytes).unwrap();
    let acyclic_harness::interaction::Interaction::Approval {
        subject:
            acyclic_harness::interaction::ApprovalSubject::Tool {
                tool, arguments, ..
            },
        ..
    } = interaction
    else {
        panic!("read call lost its exact approval subject")
    };
    if tool == "read" {
        let input: acyclic_harness::tool::files::ReadFileInput =
            serde_json::from_value(arguments).unwrap();
        assert_eq!(input.file.path(), "probe.txt");
        assert!(input.range.is_none());
        false
    } else {
        assert_eq!(tool, "write", "unexpected setup approval");
        true
    }
}

impl Followed {
    fn observe(&mut self, case: Case, file: FileValue, stdin: &mut impl std::io::Write) {
        match &file.key {
            FileKey::Task {
                task,
                file: TaskFile::ApprovalRequest(request),
            } => {
                read_setup_approval(case, &file);
                self.requests.insert((task.clone(), request.clone()), file);
            }
            FileKey::Task {
                task,
                file: TaskFile::ApprovalReview { request, .. },
            } if case.human() && !self.submitted => {
                if String::from_utf8_lossy(&file.bytes).contains("awaiting authenticated decision")
                {
                    if matches!(case, Case::ChildHumanDisconnected) {
                        self.submitted = true;
                        return;
                    }
                    let principal = if matches!(case, Case::ForgedPrincipal) {
                        "rules"
                    } else if matches!(case, Case::LlmForged) {
                        "reviewer"
                    } else {
                        "human"
                    };
                    let mut decision = decision(
                        self.requests.get(&(task.clone(), request.clone())).unwrap(),
                        principal,
                        !matches!(
                            case,
                            Case::HumanDeny | Case::ChildHumanDeny | Case::ReadHumanDeny
                        ),
                    );
                    if matches!(
                        case,
                        Case::ForgedSubject | Case::ChildHumanForged | Case::ReadHumanForged
                    ) {
                        let mut data: Value = serde_json::from_slice(&decision.bytes).unwrap();
                        *data.get_mut("request_sha256").unwrap() = json!("11".repeat(32));
                        decision.bytes = serde_json::to_vec(&data).unwrap().into();
                    }
                    writeln!(stdin, "{}", jsonl::request(&decision).unwrap()).unwrap();
                    self.submitted = true;
                }
            }
            FileKey::Task {
                task,
                file: TaskFile::ApprovalDecision { request, .. },
            } => {
                let request = self.requests.get(&(task.clone(), request.clone())).unwrap();
                let value: ApprovalDecisionFile = serde_json::from_slice(&file.bytes).unwrap();
                let descriptor = acyclic_harness::conversation::FileDescriptor::from_bytes(
                    &request.bytes,
                    "application/json",
                    &mut None,
                )
                .unwrap();
                assert!(
                    value.binds(descriptor.sha256(), 1),
                    "{case:?}: {:?}",
                    file.key
                );
                let setup_write = read_setup_approval(case, request);
                let root_tool =
                    setup_write || ((case.parent() || case.child_human()) && task == &self.root);
                if !root_tool {
                    self.saw_decision = true;
                }
                assert_eq!(
                    value.approved(),
                    root_tool || case.allowed(),
                    "{case:?}: {:?}",
                    file.key
                );
            }
            _ => {}
        }
    }
}
fn drive(mut command: Command, case: Case, operation: OperationId) -> String {
    let mut process = command.spawn().unwrap();
    let mut stdin = process.stdin.take();
    writeln!(
        stdin.as_mut().unwrap(),
        "{}",
        jsonl::request(&prepare_local_prompt(operation, "hello").unwrap()).unwrap()
    )
    .unwrap();
    let mut reader = BufReader::new(process.stdout.take().unwrap());
    let mut followed = Followed {
        root: Component::new(
            acyclic_harness::runtime::task_id_for_admission(operation).to_string(),
        )
        .unwrap(),
        requests: std::collections::BTreeMap::new(),
        submitted: false,
        saw_decision: false,
    };
    let mut lines = Vec::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap() == 0 {
            break;
        }
        let files = if let Ok(change) = jsonl::parse_change(&line) {
            change
                .file
                .into_iter()
                .chain(change.files)
                .collect::<Vec<_>>()
        } else if let Ok((_, snapshot)) = jsonl::parse_snapshot(&line) {
            snapshot
                .files
                .into_iter()
                .filter_map(|file| {
                    if file.segments.len() != 1 {
                        return None;
                    }
                    Some(FileValue::new(file.key, file.segments.into_iter().next()?))
                })
                .collect()
        } else {
            Vec::new()
        };
        for file in files {
            lines.push(format!(
                "{:?}: {}\n",
                file.key,
                String::from_utf8_lossy(&file.bytes)
            ));
            if let Some(stdin) = stdin.as_mut() {
                followed.observe(case, file, stdin);
            }
        }
        if matches!(case, Case::ChildHumanDisconnected) && followed.submitted {
            drop(stdin.take());
        }
    }
    drop(stdin);
    let output = process.wait_with_output().unwrap();
    assert!(
        output.status.code().is_some(),
        "shipping binary was terminated: {case:?}"
    );
    assert_eq!(
        output.status.success(),
        case.allowed()
            || matches!(
                case,
                Case::ParentDeny | Case::ParentCompletes | Case::ChildHumanDeny
            ),
        "{case:?}: {:?}\n{}\n{}",
        output.status.code(),
        lines.concat(),
        String::from_utf8_lossy(&output.stderr)
    );
    if case.human() {
        assert!(
            followed.submitted,
            "human stage never exercised: {case:?}: {}",
            lines.concat()
        );
    }
    if matches!(case, Case::ChildHumanDisconnected) {
        assert_eq!(
            output.status.code(),
            Some(4),
            "disconnected descendant must remain indeterminate"
        );
    }
    if !matches!(case, Case::ChildHumanDisconnected) {
        assert_eq!(
            followed.saw_decision,
            !case.forged(),
            "{case:?}: {}\n{}",
            lines.concat(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    lines.concat()
}
async fn scenario(case: Case) {
    let started = std::time::Instant::now();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let operation = OperationId::new();
    let fixture = Fixture {
        case,
        address,
        parent_task: acyclic_harness::runtime::task_id_for_admission(operation),
        effects: Arc::new(AtomicUsize::new(0)),
        root_calls: Arc::new(AtomicUsize::new(0)),
        child_calls: Arc::new(AtomicUsize::new(0)),
        reviews: Arc::new(AtomicUsize::new(0)),
        review_started: None,
    };
    let app = fixture.clone().router();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let files = Files::new(case, address);
    let command = files.command(case, operation);
    let output = tokio::task::spawn_blocking(move || drive(command, case, operation))
        .await
        .unwrap();
    fixture.verify(&output);
    server.abort();
    let _ = server.await;
    eprintln!(
        "approval scenario {case:?}: {:.3}s",
        started.elapsed().as_secs_f64()
    );
}
macro_rules! cases { ($($name:ident => $case:ident),* $(,)?) => { $(
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn $name() { scenario(Case::$case).await; }
)* }; }
cases! {
    approvals_graphcoder_default_is_policy_data => Default,
    approvals_rule_allows => RuleAllow,
    approvals_rule_denies_before_effect => RuleDeny,
    approvals_llm_allows => LlmAllow,
    approvals_llm_denies_before_effect => LlmDeny,
    approvals_llm_forged_decision_is_refused => LlmForged,
    approvals_human_allows_live => HumanAllow,
    approvals_human_denies_before_effect => HumanDeny,
    approvals_read_human_allows_live => ReadHumanAllow,
    approvals_read_human_denies_before_read => ReadHumanDeny,
    approvals_read_human_forged_file_is_refused => ReadHumanForged,
    approvals_forged_principal_is_refused => ForgedPrincipal,
    approvals_forged_subject_is_refused => ForgedSubject,
    approvals_rules_llm_human_chain => Chain,
    approvals_parent_allows => ParentAllow,
    approvals_human_wait_and_parent_approver_compose => ParentHumanWait,
    approvals_parent_denies_before_effect => ParentDeny,
    approvals_parent_forgery_is_refused => ParentForged,
    approvals_parent_completion_denies_unanswered_request => ParentCompletes,
    approvals_parent_failure_denies_unanswered_request => ParentError,
    approvals_child_human_allows_live => ChildHumanAllow,
    approvals_child_human_denies_before_effect => ChildHumanDeny,
    approvals_child_human_forged_file_is_refused => ChildHumanForged,
    approvals_child_human_disconnect_is_indeterminate => ChildHumanDisconnected,
}
