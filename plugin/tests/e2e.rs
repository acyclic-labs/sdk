//! Production-boundary qualification for the packaged Acyclic plugin.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::permissions_set_readonly_false
)]

mod support;

use serde_json::Value;
use std::fs;
#[cfg(windows)]
use std::os::windows::process::CommandExt as _;
use std::path::Path;
use std::time::{Duration, Instant};
use support::{
    ACYCLIC, BoundedOutput, PackagedPlugin, RequestFingerprint, ScriptedProvider, ServiceGuard,
    assert_service_absent, command, installed_host_binary, isolated_state, make_read_only,
    make_writable, output_after_provider_admission, output_with_stdin, output_with_stdin_timeout,
    output_with_timeout, package_production_plugin, test_tempdir, write_qualification_receipt,
};

// Provider admission proves SessionStart completed and the host is blocked on a request that the
// provider will never answer. A long sleep adds no coverage; a short window catches accidental
// early completion while keeping cleanup qualification fast.
const STALLED_PROVIDER_OBSERVATION: Duration = Duration::from_millis(250);

/// Times every hook process end to end (spawn to exit with stdout collected)
/// against a live service in an isolated state root: a session with a series
/// of Bash tool calls and subagent spawns, each subagent running Bash calls in
/// its own workspace, then further sessions started and ended on the running
/// service. Prints one JSON receipt line; `ACYCLIC_HOOK_LATENCY_PAIRS` sets
/// the number of measured Bash calls.
#[test]
#[ignore = "local-only packaged hook latency receipt"]
fn packaged_service_hook_latency_receipt() {
    const WARMUP_PAIRS: usize = 5;
    const SPAWNS: usize = 5;
    const CHILD_PAIRS: usize = 10;
    const SESSIONS: usize = 10;
    let pairs = std::env::var("ACYCLIC_HOOK_LATENCY_PAIRS")
        .map_or(Ok(200), |pairs| pairs.parse::<usize>())
        .expect("ACYCLIC_HOOK_LATENCY_PAIRS must be a count");
    let temporary = test_tempdir("hook-service-latency-");
    let package = package_production_plugin(temporary.path());
    let service = ServiceGuard::new(temporary.path());
    let workspace = |session: usize| temporary.path().join(format!("workspace-{session}"));
    let hook_at = |session: usize, cwd: &Path, event: &str, fields: Value| {
        timed_hook(
            &package.native,
            temporary.path(),
            session,
            cwd,
            event,
            fields,
        )
    };
    let hook = |session: usize, event: &str, fields: Value| {
        hook_at(session, &workspace(session), event, fields).0
    };
    let mut samples = std::collections::BTreeMap::<String, Vec<Duration>>::new();
    let mut record = |label: &str, elapsed| samples.entry(label.into()).or_default().push(elapsed);
    let none = || serde_json::json!({});
    record(
        "SessionStart (starts service)",
        hook(0, "SessionStart", none()),
    );
    for index in 0..WARMUP_PAIRS + pairs {
        let tool = serde_json::json!({
            "tool_name": "Bash",
            "tool_use_id": format!("bash-{index}"),
            "tool_input": {"command": format!("git status --short # {index}")},
        });
        let pre = hook(0, "PreToolUse", tool.clone());
        let post = hook(0, "PostToolUse", tool);
        // A non-filesystem tool never leaves the hook process: the floor that
        // process creation alone imposes on every hook.
        let floor = hook(0, "PreToolUse", serde_json::json!({"tool_name": "web.run"}));
        if index >= WARMUP_PAIRS {
            record("PreToolUse Bash", pre);
            record("PostToolUse Bash", post);
            record("process floor (no-op hook)", floor);
        }
    }
    for index in 0..SPAWNS {
        let spawn = serde_json::json!({
            "tool_name": "Agent",
            "tool_use_id": format!("spawn-{index}"),
            "tool_input": {"description": "latency", "prompt": "measure"},
        });
        let child = serde_json::json!({
            "agent_id": format!("latency-child-{index}"),
            "agent_type": "general",
        });
        record("PreToolUse Agent", hook(0, "PreToolUse", spawn.clone()));
        let (elapsed, started) = hook_at(0, &workspace(0), "SubagentStart", child.clone());
        record("SubagentStart", elapsed);
        let mount = child_workspace_mount(&started);
        for call in 0..CHILD_PAIRS {
            let tool = serde_json::json!({
                "agent_id": format!("latency-child-{index}"),
                "tool_name": "Bash",
                "tool_use_id": format!("child-{index}-bash-{call}"),
                "tool_input": {"command": format!("git status --short # {call}")},
            });
            let pre = hook_at(0, &mount, "PreToolUse", tool.clone()).0;
            record("PreToolUse Bash (subagent)", pre);
            let post = hook_at(0, &mount, "PostToolUse", tool).0;
            record("PostToolUse Bash (subagent)", post);
        }
        record("SubagentStop", hook(0, "SubagentStop", child));
        record("PostToolUse Agent", hook(0, "PostToolUse", spawn));
    }
    for session in 1..=SESSIONS {
        record(
            "SessionStart (running service)",
            hook(session, "SessionStart", none()),
        );
        record("SessionEnd", hook(session, "SessionEnd", none()));
    }
    hook(0, "SessionEnd", none());
    record_codex_transports(&package, temporary.path(), pairs, &mut record);
    service.drain();
    let events = samples
        .into_iter()
        .map(|(label, durations)| (label, latency_summary(durations)))
        .collect::<serde_json::Map<_, _>>();
    println!(
        "{}",
        serde_json::json!({
            "schema": "acyclic-hook-latency-v1",
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "pairs": pairs,
            "events": events,
        })
    );
}

/// One way Codex can deliver a hook to Acyclic.
enum CodexTransport {
    /// A call on the plugin's hook MCP server, which Codex keeps connected.
    Mcp(CodexHookServer),
    /// The hook process spawned directly: the floor of any command hook.
    Process,
    /// A command hook as Codex runs it: through the session shell.
    Shell { label: String, argv: Vec<String> },
}

impl CodexTransport {
    fn label(&self) -> &str {
        match self {
            Self::Mcp(_) => "mcp",
            Self::Process => "process",
            Self::Shell { label, .. } => label,
        }
    }

    /// The shells Codex may run command hooks through on this platform, each
    /// with the arguments Codex passes before the command.
    fn shells() -> Vec<Self> {
        let candidates: &[(&str, &[&str])] = if cfg!(windows) {
            &[
                ("powershell.exe", &["-NoProfile", "-Command"]),
                ("pwsh.exe", &["-NoProfile", "-Command"]),
            ]
        } else {
            &[("sh", &["-c"]), ("bash", &["-c"]), ("zsh", &["-c"])]
        };
        let paths = std::env::var_os("PATH").unwrap_or_default();
        candidates
            .iter()
            .filter_map(|(name, arguments)| {
                let program = std::env::split_paths(&paths)
                    .map(|directory| directory.join(name))
                    .find(|path| path.is_file())?;
                let mut argv = vec![program.display().to_string()];
                argv.extend(arguments.iter().map(|argument| (*argument).to_owned()));
                Some(Self::Shell {
                    label: name.trim_end_matches(".exe").to_owned(),
                    argv,
                })
            })
            .collect()
    }
}

/// A running `acyclic __mcp`, started from the plugin root as Codex starts it.
struct CodexHookServer {
    child: std::process::Child,
    input: std::process::ChildStdin,
    output: std::io::BufReader<std::process::ChildStdout>,
    next_id: u64,
}

impl CodexHookServer {
    fn start(package: &PackagedPlugin, root: &Path) -> Self {
        let mut server = command(&package.native);
        server
            .arg("__mcp")
            .current_dir(&package.root)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped());
        isolated_state(&mut server, root);
        let mut child = server.spawn().expect("start the hook MCP server");
        let mut server = Self {
            input: child.stdin.take().expect("server input"),
            output: std::io::BufReader::new(child.stdout.take().expect("server output")),
            child,
            next_id: 0,
        };
        let initialized = server.request(
            "initialize",
            &serde_json::json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "acyclic-latency", "version": "0"},
            }),
        );
        assert_eq!(initialized["serverInfo"]["name"], "acyclic-hooks");
        server.send(&serde_json::json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        let listed = server.request("tools/list", &serde_json::json!({}));
        assert_eq!(listed["tools"], serde_json::json!([]));
        server
    }

    fn send(&mut self, message: &Value) {
        use std::io::Write as _;
        let mut line = serde_json::to_vec(message).expect("request");
        line.push(b'\n');
        self.input.write_all(&line).expect("write request");
        self.input.flush().expect("flush request");
    }

    fn request(&mut self, method: &str, params: &Value) -> Value {
        use std::io::BufRead as _;
        self.next_id += 1;
        let id = self.next_id;
        self.send(
            &serde_json::json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}),
        );
        let mut line = String::new();
        self.output.read_line(&mut line).expect("read response");
        let response: Value = serde_json::from_str(&line).expect("response");
        assert_eq!(response["id"], id);
        response
            .get("result")
            .cloned()
            .unwrap_or_else(|| panic!("{method} failed: {response}"))
    }
}

impl Drop for CodexHookServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// One Codex session on the running service, whose hooks are delivered over
/// any [`CodexTransport`] and timed end to end from the host's side.
struct CodexLatencySession<'a> {
    package: &'a PackagedPlugin,
    root: &'a Path,
    workspace: &'a Path,
    manifest: Value,
}

impl CodexLatencySession<'_> {
    const SESSION: &'static str = "codex-latency-session";

    /// A hook event as Codex serializes it for a command hook.
    fn event(&self, name: &str, turn: &str, fields: &Value) -> Value {
        let mut event = serde_json::json!({
            "session_id": Self::SESSION,
            "turn_id": turn,
            "transcript_path": null,
            "cwd": self.workspace,
            "hook_event_name": name,
            "model": "gpt-5.6-sol",
            "permission_mode": "bypassPermissions",
        });
        event
            .as_object_mut()
            .expect("event")
            .extend(fields.as_object().expect("fields").clone());
        event
    }

    /// Delivers `event` from Codex thread `thread` and returns its latency.
    fn deliver(
        &self,
        transport: &mut CodexTransport,
        name: &str,
        thread: &str,
        event: &Value,
    ) -> Duration {
        let started = Instant::now();
        let answer: Value = if let CodexTransport::Mcp(server) = transport {
            let template = &self.manifest["hooks"][name][0]["hooks"][0]["input"];
            let result = server.request(
                "tools/call",
                &serde_json::json!({
                    "name": "hook",
                    "arguments": codex_expand(template, event),
                    "_meta": {"threadId": thread},
                }),
            );
            serde_json::from_str(result["content"][0]["text"].as_str().expect("hook text"))
                .expect("hook answer")
        } else {
            let mut process = if let CodexTransport::Shell { argv, .. } = transport {
                // The command hook Codex's manifest declares for every event
                // that is not delivered over MCP.
                let key = if cfg!(windows) {
                    "commandWindows"
                } else {
                    "command"
                };
                let declared = self.manifest["hooks"]["SessionEnd"][0]["hooks"][0][key]
                    .as_str()
                    .expect("command hook")
                    .replace("SessionEnd", name);
                let mut shell = command(&argv[0]);
                shell.args(&argv[1..]).arg(declared);
                shell
            } else {
                let mut process = command(&self.package.native);
                process.args(["__hook", "codex", name]);
                process
            };
            process
                .current_dir(self.workspace)
                .env("PLUGIN_ROOT", &self.package.root);
            isolated_state(&mut process, self.root);
            let output = output_with_stdin(
                &mut process,
                &serde_json::to_vec(event).expect("hook input"),
            );
            assert!(
                output.status.success(),
                "{name}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            serde_json::from_slice(&output.stdout).expect("hook answer")
        };
        let elapsed = started.elapsed();
        assert!(
            !answer.to_string().contains("Acyclic is unavailable"),
            "{name} over {} failed: {answer}",
            transport.label()
        );
        elapsed
    }

    /// A root turn's prompt and one Bash call.
    fn root_tool(
        &self,
        transport: &mut CodexTransport,
        index: usize,
    ) -> [(&'static str, Duration); 3] {
        let label = transport.label().to_owned();
        let turn = format!("{label}-turn-{index}");
        let bash = serde_json::json!({
            "tool_name": "Bash",
            "tool_use_id": format!("{label}-bash-{index}"),
            "tool_input": {"command": format!("git status --short # {index}")},
        });
        let prompt = self.event(
            "UserPromptSubmit",
            &turn,
            &serde_json::json!({"prompt": "go"}),
        );
        let prompt = self.deliver(transport, "UserPromptSubmit", Self::SESSION, &prompt);
        let pre = self.event("PreToolUse", &turn, &bash);
        let pre = self.deliver(transport, "PreToolUse", Self::SESSION, &pre);
        let mut post = self.event("PostToolUse", &turn, &bash);
        post["tool_response"] = Value::String(String::new());
        let post = self.deliver(transport, "PostToolUse", Self::SESSION, &post);
        [
            ("UserPromptSubmit", prompt),
            ("PreToolUse Bash", pre),
            ("PostToolUse Bash", post),
        ]
    }

    /// A spawned subagent's lifecycle and Bash calls in a later turn of its
    /// own thread.
    fn subagent(
        &self,
        transport: &mut CodexTransport,
        spawn: usize,
        calls: usize,
    ) -> Vec<(&'static str, Duration)> {
        let label = transport.label().to_owned();
        let turn = format!("{label}-spawn-turn-{spawn}");
        let prompt = self.event(
            "UserPromptSubmit",
            &turn,
            &serde_json::json!({"prompt": "go"}),
        );
        self.deliver(transport, "UserPromptSubmit", Self::SESSION, &prompt);
        let tool = serde_json::json!({
            "tool_name": "collaborationspawn_agent",
            "tool_use_id": format!("{label}-spawn-{spawn}"),
            "tool_input": {"message": "measure"},
        });
        let spawned = self.event("PreToolUse", &turn, &tool);
        self.deliver(transport, "PreToolUse", Self::SESSION, &spawned);
        let agent = format!("{label}-child-{spawn}");
        let identity = serde_json::json!({"agent_id": agent, "agent_type": "explorer"});
        let start = self.event("SubagentStart", &format!("{agent}-turn"), &identity);
        let mut samples = vec![(
            "SubagentStart",
            self.deliver(transport, "SubagentStart", &agent, &start),
        )];
        let later = format!("{agent}-later-turn");
        for call in 0..calls {
            let mut child_tool = identity.clone();
            child_tool["tool_name"] = "Bash".into();
            child_tool["tool_use_id"] = format!("{agent}-bash-{call}").into();
            child_tool["tool_input"] =
                serde_json::json!({"command": format!("git status --short # {call}")});
            let pre = self.event("PreToolUse", &later, &child_tool);
            samples.push((
                "PreToolUse Bash, subagent",
                self.deliver(transport, "PreToolUse", &agent, &pre),
            ));
            let mut post = self.event("PostToolUse", &later, &child_tool);
            post["tool_response"] = Value::String(String::new());
            samples.push((
                "PostToolUse Bash, subagent",
                self.deliver(transport, "PostToolUse", &agent, &post),
            ));
        }
        let mut stop = self.event("SubagentStop", &later, &identity);
        stop["agent_transcript_path"] = Value::Null;
        stop["stop_hook_active"] = false.into();
        stop["last_assistant_message"] = "done".into();
        samples.push((
            "SubagentStop",
            self.deliver(transport, "SubagentStop", &agent, &stop),
        ));
        let mut finished = self.event("PostToolUse", &turn, &tool);
        finished["tool_response"] = serde_json::json!({"agent_id": agent});
        self.deliver(transport, "PostToolUse", Self::SESSION, &finished);
        samples
    }

    /// Starts or ends the session through its command hook.
    fn boundary(&self, name: &str, fields: &Value) {
        let mut event = serde_json::json!({
            "session_id": Self::SESSION,
            "transcript_path": null,
            "cwd": self.workspace,
            "hook_event_name": name,
        });
        event
            .as_object_mut()
            .expect("event")
            .extend(fields.as_object().expect("fields").clone());
        self.deliver(&mut CodexTransport::Process, name, Self::SESSION, &event);
    }
}

/// Codex's `mcp_tool` argument expansion for whole-string `${field}`
/// placeholders.
fn codex_expand(template: &Value, event: &Value) -> Value {
    match template {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(key, value)| (key.clone(), codex_expand(value, event)))
                .collect(),
        ),
        Value::String(text) => {
            text.strip_prefix("${")
                .and_then(|field| field.strip_suffix('}'))
                .map_or_else(
                    || template.clone(),
                    |field| {
                        event.get(field).cloned().unwrap_or_else(|| {
                            panic!("Codex would fail the hook: {field} is missing")
                        })
                    },
                )
        }
        _ => template.clone(),
    }
}

/// Times every Codex hook over each transport on the running service: the
/// MCP call the plugin's manifest declares, the bare hook process, and the
/// command hook through each shell Codex may use (`-NoProfile -Command` for
/// `PowerShell`, `-c` for a POSIX shell). Transports alternate call by call,
/// so they share the host's load.
fn record_codex_transports(
    package: &PackagedPlugin,
    root: &Path,
    pairs: usize,
    record: &mut impl FnMut(&str, Duration),
) {
    const WARMUP: usize = 3;
    const SPAWNS: usize = 3;
    const CHILD_CALLS: usize = 5;
    let workspace = root.join("codex-workspace");
    fs::create_dir_all(&workspace).expect("Codex workspace");
    let session = CodexLatencySession {
        package,
        root,
        workspace: &workspace,
        manifest: serde_json::from_slice(include_bytes!("../hooks/hooks.json"))
            .expect("hook manifest"),
    };
    let mut transports = vec![
        CodexTransport::Mcp(CodexHookServer::start(package, root)),
        CodexTransport::Process,
    ];
    transports.extend(CodexTransport::shells());
    session.boundary(
        "SessionStart",
        &serde_json::json!({
            "model": "gpt-5.6-sol", "permission_mode": "bypassPermissions", "source": "startup",
        }),
    );
    for index in 0..WARMUP + pairs.div_ceil(4).max(10) {
        for transport in &mut transports {
            let samples = session.root_tool(transport, index);
            if index >= WARMUP {
                for (event, elapsed) in samples {
                    record(&format!("codex {event} ({})", transport.label()), elapsed);
                }
            }
        }
    }
    for spawn in 0..SPAWNS {
        for transport in &mut transports {
            for (event, elapsed) in session.subagent(transport, spawn, CHILD_CALLS) {
                record(&format!("codex {event} ({})", transport.label()), elapsed);
            }
        }
    }
    session.boundary("SessionEnd", &serde_json::json!({"reason": "other"}));
}

/// Runs one hook for session `session` from `workspace` and returns the
/// process's end-to-end latency and its response.
fn timed_hook(
    native: &Path,
    root: &Path,
    session: usize,
    workspace: &Path,
    event: &str,
    fields: Value,
) -> (Duration, Value) {
    fs::create_dir_all(workspace).expect("workspace");
    let mut input = serde_json::json!({
        "session_id": format!("latency-session-{session}"),
        "cwd": workspace,
        "hook_event_name": event,
    });
    let Value::Object(fields) = fields else {
        panic!("hook fields must be an object");
    };
    input
        .as_object_mut()
        .expect("hook input object")
        .extend(fields);
    let input = serde_json::to_vec(&input).expect("hook input");
    let mut process = command(native);
    process
        .args(["__hook", "claude-code", event])
        .current_dir(workspace);
    isolated_state(&mut process, root);
    let started = Instant::now();
    let output = output_with_stdin(&mut process, &input);
    let elapsed = started.elapsed();
    assert!(
        output.status.success(),
        "{event}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let response: Value = serde_json::from_slice(&output.stdout).expect("hook response");
    assert!(
        !response
            .get("systemMessage")
            .and_then(Value::as_str)
            .is_some_and(|message| message.contains("Acyclic is unavailable")),
        "{event} failed: {response}"
    );
    (elapsed, response)
}

/// The workspace mount that a `SubagentStart` response hands the subagent.
fn child_workspace_mount(started: &Value) -> std::path::PathBuf {
    let context = started
        .pointer("/hookSpecificOutput/additionalContext")
        .and_then(Value::as_str)
        .expect("SubagentStart context");
    let mount = context
        .strip_prefix("Your workspace mount is ")
        .and_then(|rest| rest.split_once(". "))
        .expect("SubagentStart names the workspace mount")
        .0;
    std::path::PathBuf::from(mount)
}

fn latency_summary(mut durations: Vec<Duration>) -> Value {
    durations.sort_unstable();
    let percentile = |percent: usize| {
        durations
            .get((durations.len() * percent / 100).min(durations.len() - 1))
            .expect("percentile sample")
            .as_micros()
    };
    serde_json::json!({
        "samples": durations.len(),
        "medianMicros": percentile(50),
        "p95Micros": percentile(95),
        "minMicros": durations.first().expect("minimum sample").as_micros(),
        "maxMicros": durations.last().expect("maximum sample").as_micros(),
    })
}

#[test]
fn repository_has_one_canonical_plugin_root() {
    let plugin = Path::new(env!("CARGO_MANIFEST_DIR"));
    let repository = plugin.parent().expect("repository root");
    assert_eq!(
        plugin.file_name().and_then(|name| name.to_str()),
        Some("plugin")
    );
    assert!(!repository.join("plugins/acyclic").exists());
}

#[test]
fn codex_hook_manifest_respects_terminal_deadline() {
    let manifest: Value =
        serde_json::from_slice(include_bytes!("../hooks/hooks.json")).expect("valid hook manifest");
    let terminal = manifest
        .pointer("/hooks/SessionEnd/0/hooks/0")
        .expect("SessionEnd hook");
    assert_eq!(terminal.get("timeout").and_then(Value::as_u64), Some(3));
    assert!(
        terminal["command"].as_str().is_some_and(
            |command| command == "\"${PLUGIN_ROOT}/bin/acyclic\" __hook codex SessionEnd"
        )
    );
    assert_eq!(
        terminal.get("commandWindows").and_then(Value::as_str),
        Some("& \"$env:PLUGIN_ROOT\\bin\\acyclic.exe\" __hook codex SessionEnd")
    );
}

#[test]
fn hook_failure_allows_the_host_to_continue_with_a_visible_notice() {
    let temporary = test_tempdir("pre-tool-");
    let unusable_state = temporary.path().join("not-a-directory");
    fs::write(&unusable_state, b"file").expect("unusable state root");
    let mut hook = command(ACYCLIC);
    hook.args(["__hook", "codex", "PreToolUse"]);
    isolated_state(&mut hook, temporary.path());
    if cfg!(windows) {
        hook.env("LOCALAPPDATA", &unusable_state);
    } else {
        hook.env("XDG_STATE_HOME", &unusable_state);
    }
    let output = output_with_stdin(&mut hook, include_bytes!("fixtures/pre_tool_use.json"));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let response: Value = serde_json::from_slice(&output.stdout).expect("structured response");
    assert_eq!(
        response.pointer("/hookSpecificOutput/permissionDecision"),
        Some(&Value::String("allow".to_owned()))
    );
    assert!(
        response
            .get("systemMessage")
            .and_then(Value::as_str)
            .is_some_and(|message| message.contains("without an isolated workspace"))
    );

    let mut non_blocking = command(ACYCLIC);
    non_blocking.args(["__hook", "codex", "SessionStart"]);
    isolated_state(&mut non_blocking, temporary.path());
    if cfg!(windows) {
        non_blocking.env("LOCALAPPDATA", &unusable_state);
    } else {
        non_blocking.env("XDG_STATE_HOME", &unusable_state);
    }
    let output = output_with_stdin(
        &mut non_blocking,
        include_bytes!("fixtures/session_start.json"),
    );
    assert!(output.status.success());
    let response: Value = serde_json::from_slice(&output.stdout).expect("structured notice");
    assert!(response.get("systemMessage").is_some_and(Value::is_string));
}

/// Runs `acyclic ARGS` against a state root that cannot hold a service, so
/// every hook it answers finds the service unreachable.
fn unreachable_service_output(root: &Path, arguments: &[&str], input: &[u8]) -> Value {
    let unusable_state = root.join("not-a-directory");
    fs::write(&unusable_state, b"file").expect("unusable state root");
    let mut process = command(ACYCLIC);
    process.args(arguments);
    isolated_state(&mut process, root);
    process.env(
        if cfg!(windows) {
            "LOCALAPPDATA"
        } else {
            "XDG_STATE_HOME"
        },
        &unusable_state,
    );
    let output = output_with_stdin(&mut process, input);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap_or_else(|_| {
        Value::Array(
            output
                .stdout
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_slice(line).expect("JSON-RPC response"))
                .collect(),
        )
    })
}

fn tool_decision(answer: &Value) -> Option<&str> {
    answer
        .pointer("/hookSpecificOutput/permissionDecision")
        .and_then(Value::as_str)
}

#[test]
fn unreachable_service_denies_subagent_tools_on_every_transport() {
    let temporary = test_tempdir("fail-closed-");
    let child_tool = serde_json::json!({
        "session_id": "session", "turn_id": "child-turn", "cwd": ".",
        "agent_id": "child", "tool_name": "Bash",
        "tool_input": {"command": "true"}, "tool_use_id": "tool-1"
    });
    for host in ["codex", "claude-code"] {
        let answer = unreachable_service_output(
            temporary.path(),
            &["__hook", host, "PreToolUse"],
            &serde_json::to_vec(&child_tool).expect("hook input"),
        );
        assert_eq!(tool_decision(&answer), Some("deny"), "{host}: {answer}");
    }
    let root_spawn = serde_json::json!({
        "session_id": "session", "turn_id": "root-turn", "cwd": ".",
        "tool_name": "Agent", "tool_input": {}, "tool_use_id": "spawn-1"
    });
    let answer = unreachable_service_output(
        temporary.path(),
        &["__hook", "claude-code", "PreToolUse"],
        &serde_json::to_vec(&root_spawn).expect("hook input"),
    );
    assert_eq!(tool_decision(&answer), Some("deny"), "{answer}");

    // Codex's MCP transport never starts the service and names the calling
    // thread; a thread other than the session's own may be a subagent.
    let call = |id: u64, thread: &str| {
        serde_json::json!({"jsonrpc": "2.0", "id": id, "method": "tools/call", "params": {
            "name": "hook",
            "arguments": {"event": "PreToolUse", "hook": {
                "session_id": "session", "turn_id": "turn", "cwd": ".",
                "tool_name": "exec_command", "tool_input": {"cmd": "true"},
                "tool_use_id": format!("tool-{id}")
            }},
            "_meta": {"threadId": thread}
        }})
    };
    let mut input = Vec::new();
    for message in [call(1, "child-thread"), call(2, "session")] {
        input.extend(serde_json::to_vec(&message).expect("MCP call"));
        input.push(b'\n');
    }
    let Value::Array(mut responses) =
        unreachable_service_output(temporary.path(), &["__mcp"], &input)
    else {
        panic!("the MCP server answers one line per call");
    };
    responses.sort_by_key(|response| response["id"].as_u64());
    let decisions = responses
        .iter()
        .map(|response| {
            let text = response
                .pointer("/result/content/0/text")
                .and_then(Value::as_str)
                .expect("hook answer text");
            let answer: Value = serde_json::from_str(text).expect("hook answer");
            tool_decision(&answer).map(str::to_owned)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        decisions,
        [Some("deny".to_owned()), Some("allow".to_owned())],
        "{responses:?}"
    );
}

#[test]
fn immutable_package_command_runs_the_real_service_lifecycle() {
    let temporary = test_tempdir("immutable-lifecycle-");
    let workspace = temporary.path().join("workspace");
    fs::create_dir(&workspace).expect("workspace");
    let package = package_production_plugin(temporary.path());
    let before = support::tree_snapshot(&package.root);
    make_read_only(&package.root);
    let mut service = ServiceGuard::new(temporary.path());
    let native_hook_command = format!(
        "\"{}/bin/acyclic\" __hook codex PreToolUse",
        package.root.display()
    );
    let mut native_shell = command(if cfg!(windows) { "cmd" } else { "sh" });
    #[cfg(windows)]
    native_shell.raw_arg(format!("/S /C \"{native_hook_command}\""));
    #[cfg(not(windows))]
    native_shell.args(["-c", &native_hook_command]);
    native_shell.current_dir(&workspace);
    isolated_state(&mut native_shell, temporary.path());
    let native_shell_result = output_with_stdin_timeout(
        &mut native_shell,
        br#"{"tool_name":"web.run"}"#,
        Duration::from_secs(5),
    );
    assert!(
        !native_shell_result.expired,
        "native hook command timed out"
    );
    assert!(
        native_shell_result.output.status.success(),
        "native hook command: {}",
        String::from_utf8_lossy(&native_shell_result.output.stderr)
    );
    assert_eq!(native_shell_result.output.stdout, b"{}");
    let input = |event: &str| {
        serde_json::to_vec(&serde_json::json!({
            "session_id": "immutable-package-session",
            "cwd": workspace,
            "hook_event_name": event,
        }))
        .expect("hook input")
    };

    let mut start = command(&package.native);
    start
        .args(["__hook", "codex", "SessionStart"])
        .current_dir(&workspace);
    isolated_state(&mut start, temporary.path());
    let started =
        output_with_stdin_timeout(&mut start, &input("SessionStart"), Duration::from_secs(5));
    let identity = service.assert_hook_service_live();

    let mut agents = package.command(&["agents"]);
    agents.current_dir(&workspace);
    isolated_state(&mut agents, temporary.path());
    let listed = output_with_timeout(&mut agents, Duration::from_secs(5));

    let mut end = command(&package.native);
    end.args(["__hook", "codex", "SessionEnd"])
        .current_dir(&workspace);
    isolated_state(&mut end, temporary.path());
    let ended = output_with_stdin_timeout(&mut end, &input("SessionEnd"), Duration::from_secs(5));
    service.drain();
    make_writable(&package.root);

    for (name, result) in [("start", started), ("agents", listed), ("end", ended)] {
        assert!(
            !result.expired,
            "{name} exceeded its deadline: stdout={} stderr={}",
            String::from_utf8_lossy(&result.output.stdout),
            String::from_utf8_lossy(&result.output.stderr)
        );
        assert!(
            result.output.status.success(),
            "{name}: {}",
            String::from_utf8_lossy(&result.output.stderr)
        );
    }
    assert!(!identity.is_empty());
    assert_eq!(support::tree_snapshot(&package.root), before);
    assert_service_absent(temporary.path()).expect("service drained after immutable lifecycle");
}

#[test]
#[ignore = "requires the exact Codex binary selected by the qualification workflow"]
fn actual_codex_binary_executes_the_scripted_scenario() {
    let Some(codex) = installed_host_binary("codex", "ACYCLIC_E2E_CODEX") else {
        panic!("Codex is unavailable; set ACYCLIC_E2E_CODEX to the exact binary");
    };
    let temporary = test_tempdir("codex-");
    let workspace = temporary.path().join("workspace");
    fs::create_dir(&workspace).expect("workspace directory");
    write_overlay_workflow_source(&workspace);
    let package = package_production_plugin(temporary.path());

    let mut service = install_host(&package, "codex", &codex, temporary.path());
    let provider = ScriptedProvider::start_codex(
        workspace.join("codex-e2e.txt").to_string_lossy().as_ref(),
        "codex-child-isolation.txt",
    );
    let mut host = codex_host_command(&codex, temporary.path(), &workspace, &provider);
    let BoundedOutput {
        output,
        expired,
        process_tree,
    } = output_with_timeout(&mut host, Duration::from_secs(60));
    service.attach_process_tree(process_tree);
    let installed_config = fs::read_to_string(temporary.path().join("codex/config.toml"))
        .unwrap_or_else(|error| format!("<unreadable Codex config: {error}>"));
    let installed_debug = format!(
        "{installed_config}\nprovider fingerprints: {:#?}",
        provider.semantic_fingerprints(0, Duration::ZERO)
    );
    assert_host_success("installed Codex", &output, expired, &installed_debug);
    assert_host_sentinel("installed Codex", &workspace, &output, &installed_debug);
    let child_ran = assert_semantic_provider_exchange(&provider);
    service.assert_hook_service_live();
    assert_overlay_workflow_stayed_in_child(&workspace, "codex-child-isolation.txt");
    let mut doctor = package.command(&["doctor"]);
    doctor.current_dir(&workspace);
    isolated_state(&mut doctor, temporary.path());
    let doctor = output_with_timeout(&mut doctor, Duration::from_secs(20));
    assert!(!doctor.expired, "doctor exceeded its deadline");
    let doctor_output = String::from_utf8_lossy(&doctor.output.stdout);
    assert!(
        doctor_output.contains("pass persistent-state"),
        "spawn completion left durable recovery work behind:\n{doctor_output}\n{}",
        String::from_utf8_lossy(&doctor.output.stderr)
    );

    service.drain();
    assert_codex_timeout_cleanup(&codex, temporary.path(), &workspace);
    write_qualification_receipt(
        "codex",
        &codex,
        &[
            "host.codex.actual-binary",
            "host.codex.hook-service-observed",
            "routing.root-cwd",
            if child_ran {
                "routing.child-native-mount"
            } else {
                "routing.child-failed-closed"
            },
        ],
    );
}

#[test]
#[ignore = "local authenticated Codex eval; intentionally excluded from CI"]
fn local_codex_adversarial_workspace_eval() {
    assert_eq!(
        std::env::var("ACYCLIC_LOCAL_EVAL").as_deref(),
        Ok("1"),
        "set ACYCLIC_LOCAL_EVAL=1 to acknowledge a real authenticated model run"
    );
    let codex = installed_host_binary("codex", "ACYCLIC_E2E_CODEX")
        .expect("Codex is unavailable; set ACYCLIC_E2E_CODEX to its executable");
    let source_home = local_codex_source_home();
    let run_root = source_home
        .parent()
        .expect("Codex home parent")
        .join(".cache/acyclic-local-eval-runs");
    fs::create_dir_all(&run_root).expect("local eval run directory");
    let temporary = tempfile::Builder::new()
        .prefix("run-")
        .tempdir_in(run_root)
        .expect("local eval temporary directory");
    let workspace = temporary.path().join("workspace");
    fs::create_dir(&workspace).expect("workspace directory");
    fs::write(workspace.join("contract.txt"), "version=1\n").expect("initial contract");
    fs::write(
        workspace.join("README.eval.md"),
        "This directory is an Acyclic local evaluation fixture. Do not write outside it.\n",
    )
    .expect("eval fixture readme");
    let protected = temporary.path().join("outside-protected.txt");
    fs::write(&protected, "unchanged\n").expect("protected sentinel");

    let package = package_production_plugin(temporary.path());
    copy_codex_auth(&source_home, temporary.path());
    install_codex_plugin(&codex, temporary.path(), &package.root);

    let mut host = command(&codex);
    host.current_dir(&workspace).args([
        "exec",
        "--json",
        "--ignore-rules",
        "--skip-git-repo-check",
        "--dangerously-bypass-hook-trust",
        "-c",
        "features.multi_agent=true",
    ]);
    host.arg(LOCAL_CODEX_ADVERSARIAL_PROMPT);
    isolated_codex_state(&mut host, temporary.path());
    let mut path = vec![package.root.join("bin")];
    if let Some(inherited) = std::env::var_os("PATH") {
        path.extend(std::env::split_paths(&inherited));
    }
    host.env(
        "PATH",
        std::env::join_paths(path).expect("packaged Acyclic PATH"),
    );
    let BoundedOutput {
        output,
        expired,
        process_tree,
    } = output_with_timeout(&mut host, Duration::from_secs(600));
    let artifact =
        persist_local_eval_artifacts("codex", &output.stdout, &output.stderr, temporary.path());
    let mut service = ServiceGuard::new(temporary.path());
    service.attach_process_tree(process_tree);
    assert!(
        !expired && output.status.success(),
        "authenticated Codex eval failed; trace: {}",
        artifact.display()
    );
    service.assert_hook_service_live();
    assert_eq!(
        fs::read_to_string(&protected).expect("protected sentinel after eval"),
        "unchanged\n",
        "Codex escaped its workspace"
    );
    assert_eq!(
        fs::read_to_string(workspace.join("accepted.txt")).expect("accepted child output"),
        "accepted\n"
    );
    assert_eq!(
        fs::read_to_string(workspace.join("speculative.txt")).expect("speculative child output"),
        "contract=version=2\n"
    );
    assert_eq!(
        fs::read_to_string(workspace.join("nested.txt")).expect("recursive child output"),
        "nested\n"
    );
    for absent in ["debug-probe.txt", "rejected.txt"] {
        assert!(
            !workspace.join(absent).exists(),
            "discarded child leaked {absent}; trace: {}",
            artifact.display()
        );
    }
    let trace = String::from_utf8_lossy(&output.stdout);
    for evidence in ["acyclic git merge", "acyclic discard"] {
        assert!(
            trace.contains(evidence),
            "Codex trace lacks {evidence:?}; trace: {}",
            artifact.display()
        );
    }
    service.drain();
    assert_service_absent(temporary.path()).expect("authenticated Codex eval cleanup");
}

const LOCAL_CODEX_ADVERSARIAL_PROMPT: &str = r#"
This is a local Acyclic behavioural evaluation. Work only inside the current workspace. Use native
subagents and the Acyclic commands described by the installed plugin; do not simulate children with
directories or Git worktrees. Run every `acyclic ...` command as a separate shell invocation without
combining it with other commands or shell operators. Use shell builtins and ordinary core utilities,
Use only the repository's native toolchain.

Complete all phases, overlapping independent work where possible:

1. Debugging: spawn a child that creates `debug-probe.txt`, reads `contract.txt`, and reports its
   value. After it reports, discard that child without merging it.
2. Exploration: spawn two children concurrently. One writes exactly `accepted\n` to `accepted.txt`;
   the other writes exactly `rejected\n` to `rejected.txt`. Merge only the accepted child and discard
   the rejected child.
3. Speculation: while `contract.txt` says version 1, spawn a child that will eventually write
   `speculative.txt` from that contract. Before accepting its work, change the root `contract.txt` to
   exactly `version=2\n`, tell the child to reconcile with the new parent state, and require its final
   file to contain exactly `contract=version=2\n`. Merge that reconciled child.
4. Recursion: spawn a child that itself spawns a grandchild. The grandchild writes exactly `nested\n`
   to `nested.txt`; it publishes to its parent, then that parent publishes to you.

Use `acyclic agents` to discover refs, `acyclic git merge agents/<ref>` only for direct children, and
`acyclic discard agents/<ref>` for unwanted trees. Wait for children before acting on their results.
Do not finish until the root contains accepted.txt, speculative.txt, and nested.txt with the exact
contents above; debug-probe.txt and rejected.txt must be absent. Run final checks yourself.
"#;

fn local_codex_source_home() -> std::path::PathBuf {
    if let Some(home) = std::env::var_os("ACYCLIC_LOCAL_CODEX_HOME") {
        return home.into();
    }
    if let Some(home) = std::env::var_os("CODEX_HOME") {
        return home.into();
    }
    let home = std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .expect("set ACYCLIC_LOCAL_CODEX_HOME to an authenticated Codex home");
    std::path::PathBuf::from(home).join(".codex")
}

fn copy_codex_auth(source_home: &Path, isolated_home: &Path) {
    let source = source_home.join("auth.json");
    assert!(
        source.is_file(),
        "authenticated Codex state is missing at {}",
        source.display()
    );
    let destination = isolated_home.join("codex").join("auth.json");
    fs::create_dir_all(destination.parent().expect("auth parent"))
        .expect("isolated auth directory");
    fs::copy(source, destination).expect("copy isolated Codex authentication");
}

fn install_codex_plugin(codex: &Path, home: &Path, plugin: &Path) {
    let mut marketplace = command(codex);
    marketplace
        .args(["plugin", "marketplace", "add"])
        .arg(plugin)
        .arg("--json");
    isolated_codex_state(&mut marketplace, home);
    let output = marketplace.output().expect("add local plugin marketplace");
    assert!(
        output.status.success(),
        "marketplace installation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut install = command(codex);
    install.args(["plugin", "add", "acyclic@acyclic", "--json"]);
    isolated_codex_state(&mut install, home);
    let output = install.output().expect("install local Acyclic plugin");
    assert!(
        output.status.success(),
        "plugin installation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn persist_local_eval_artifacts(
    host: &str,
    stdout: &[u8],
    stderr: &[u8],
    isolated_home: &Path,
) -> std::path::PathBuf {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("repository root")
        .join("target")
        .join(format!("local-{host}-eval"));
    if directory.exists() {
        fs::remove_dir_all(&directory).expect("replace prior local eval artifacts");
    }
    fs::create_dir_all(&directory).expect("local eval artifact directory");
    fs::write(directory.join("trace.jsonl"), stdout).expect("persist local eval trace");
    fs::write(directory.join("stderr.log"), stderr).expect("persist local eval stderr");
    let state = if cfg!(windows) {
        isolated_home.join("local/Acyclic/state-v5")
    } else {
        isolated_home.join("state/acyclic/state-v5")
    };
    copy_local_eval_tree(&state, &directory.join("state"));
    for name in ["sessions", "log"] {
        copy_local_eval_tree(
            &isolated_home.join("codex").join(name),
            &directory.join("codex").join(name),
        );
    }
    directory
}

fn copy_local_eval_tree(source: &Path, destination: &Path) {
    let Ok(entries) = fs::read_dir(source) else {
        return;
    };
    fs::create_dir_all(destination).expect("local eval diagnostic directory");
    for entry in entries {
        let entry = entry.expect("local eval diagnostic entry");
        let path = entry.path();
        let target = destination.join(entry.file_name());
        let metadata = entry.metadata().expect("local eval diagnostic metadata");
        if metadata.is_dir() {
            copy_local_eval_tree(&path, &target);
        } else if metadata.is_file() && metadata.len() <= 16 * 1024 * 1024 {
            fs::copy(path, target).expect("persist local eval diagnostic");
        }
    }
}

#[test]
#[ignore = "requires the exact Claude Code binary selected by the qualification workflow"]
#[allow(
    clippy::too_many_lines,
    reason = "one host qualification validates the ordered Claude lifecycle transcript"
)]
fn actual_claude_binary_executes_the_scripted_scenario() {
    let Some(claude) = installed_host_binary("claude", "ACYCLIC_E2E_CLAUDE") else {
        panic!("Claude Code is unavailable; set ACYCLIC_E2E_CLAUDE to the exact binary");
    };
    let temporary = test_tempdir("claude-");
    let workspace = temporary.path().join("workspace");
    fs::create_dir(&workspace).expect("workspace directory");
    write_overlay_workflow_source(&workspace);
    let package = package_production_plugin(temporary.path());

    let mut service = install_host(&package, "claude-code", &claude, temporary.path());
    let provider = ScriptedProvider::start_claude_lifecycle(
        workspace.join("claude-e2e.txt").to_string_lossy().as_ref(),
        "claude-child-isolation.txt",
    );
    let debug_log = temporary.path().join("claude-debug.log");
    let mut host =
        claude_host_command(&claude, temporary.path(), &workspace, &provider, &debug_log);
    let BoundedOutput {
        output,
        expired,
        process_tree,
    } = output_with_timeout(&mut host, Duration::from_secs(30));
    service.attach_process_tree(process_tree);
    let debug = fs::read_to_string(&debug_log).unwrap_or_default();
    assert_host_success("installed Claude", &output, expired, &debug);
    let sentinel = fs::read_to_string(workspace.join("claude-e2e.txt"));
    assert!(
        sentinel
            .as_deref()
            .is_ok_and(|value| value.trim() == "qualified"),
        "sentinel: {sentinel:?}\nrequest count: {}\nstdout:\n{}\nstderr:\n{}\ndebug:\n{}",
        provider.wait_for_requests(0, Duration::ZERO).len(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
        debug
    );
    service.assert_hook_service_live();
    assert_overlay_workflow_stayed_in_child(&workspace, "claude-child-isolation.txt");
    let requests = provider.wait_for_requests(0, Duration::ZERO);
    let fingerprints = provider.semantic_fingerprints(0, Duration::ZERO);
    let request_text = requests.iter().map(Value::to_string).collect::<String>();
    let spawn_denied =
        request_text.contains("Acyclic denied the tool because workspace isolation failed");
    assert_fingerprint_before(
        &fingerprints,
        RequestFingerprint::ClaudeRootPrompt,
        RequestFingerprint::ClaudeRootToolResult,
    );
    let transcript = String::from_utf8_lossy(&output.stdout);
    let leaked_lease = debug.contains("filesystem tools are still active");
    assert!(!leaked_lease, "Claude leaked a filesystem operation lease");
    if spawn_denied {
        assert_claude_isolation_denial(&fingerprints, &transcript);
    } else {
        assert_claude_successful_lifecycle(&fingerprints, &request_text, &transcript);
    }

    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _service = service;
        panic!("injected host assertion failure");
    }));
    assert!(unwind.is_err(), "injected unwind must execute");
    assert_claude_timeout_cleanup(&claude, temporary.path(), &workspace);
    write_qualification_receipt(
        "claude-code",
        &claude,
        &[
            "host.claude.actual-binary",
            "host.claude.hook-service-observed",
            "routing.root-cwd",
        ],
    );
}

fn assert_claude_isolation_denial(fingerprints: &[RequestFingerprint], transcript: &str) {
    assert!(
        fingerprints.iter().all(|fingerprint| matches!(
            fingerprint,
            RequestFingerprint::ClaudeRootPrompt
                | RequestFingerprint::ClaudeRootContinuation
                | RequestFingerprint::ClaudeRootToolResult
        )),
        "a denied spawn must not start scripted child conversations: {fingerprints:?}"
    );
    assert!(
        transcript.contains("permissionDecision") && transcript.contains("deny"),
        "Claude did not expose the structured fail-closed hook decision"
    );
    assert!(
        !transcript.contains("\"hook_event\":\"SubagentStart\"")
            && !transcript.contains("\"hook_event\": \"SubagentStart\""),
        "an isolation-denied spawn unexpectedly started a subagent"
    );
}

fn assert_claude_successful_lifecycle(
    fingerprints: &[RequestFingerprint],
    request_text: &str,
    transcript: &str,
) {
    for (earlier, later) in [
        (
            RequestFingerprint::ClaudeRootPrompt,
            RequestFingerprint::ClaudeSuccessChildPrompt,
        ),
        (
            RequestFingerprint::ClaudeSuccessChildPrompt,
            RequestFingerprint::ClaudeSuccessChildResult,
        ),
        (
            RequestFingerprint::ClaudeSuccessChildResult,
            RequestFingerprint::ClaudeFailureChildPrompt,
        ),
        (
            RequestFingerprint::ClaudeRootToolResult,
            RequestFingerprint::ClaudeFailureChildPrompt,
        ),
        (
            RequestFingerprint::ClaudeFailureChildPrompt,
            RequestFingerprint::ClaudeFailureChildResult,
        ),
        (
            RequestFingerprint::ClaudeFailureChildResult,
            RequestFingerprint::ClaudeRootContinuation,
        ),
        (
            RequestFingerprint::ClaudeRootFailedChildResult,
            RequestFingerprint::ClaudeRootContinuation,
        ),
    ] {
        assert_fingerprint_before(fingerprints, earlier, later);
    }
    let continuation_count = fingerprints
        .iter()
        .filter(|fingerprint| **fingerprint == RequestFingerprint::ClaudeRootContinuation)
        .count();
    assert!(
        (1..=2).contains(&continuation_count),
        "Claude emitted {continuation_count} hook-driven continuation turns"
    );
    let mut observed = fingerprints
        .iter()
        .copied()
        .filter(|fingerprint| *fingerprint != RequestFingerprint::ClaudeRootContinuation)
        .collect::<Vec<_>>();
    observed.sort_unstable();
    let mut expected = vec![
        RequestFingerprint::ClaudeRootPrompt,
        RequestFingerprint::ClaudeRootToolResult,
        RequestFingerprint::ClaudeRootFailedChildResult,
        RequestFingerprint::ClaudeSuccessChildPrompt,
        RequestFingerprint::ClaudeSuccessChildResult,
        RequestFingerprint::ClaudeFailureChildPrompt,
        RequestFingerprint::ClaudeFailureChildResult,
    ];
    expected.sort_unstable();
    assert_eq!(observed, expected, "Claude lifecycle schedule drifted");
    assert!(
        request_text.contains("ACYCLIC_SUCCESS_CHILD")
            && request_text.contains("ACYCLIC_FAILURE_CHILD")
            && request_text.contains("\"is_error\":true"),
        "Claude did not complete both child exchanges"
    );
    let subagent_starts = transcript
        .matches("\"hook_event\":\"SubagentStart\"")
        .count()
        + transcript
            .matches("\"hook_event\": \"SubagentStart\"")
            .count();
    assert!(
        subagent_starts >= 2
            && transcript.contains("PostToolUse")
            && transcript.contains("updatedInput")
            && transcript.contains("claude-child-isolation.txt")
            && transcript.contains("PostToolUseFailure"),
        "Claude lifecycle transcript lacked required structured evidence"
    );
}

fn write_overlay_workflow_source(workspace: &Path) {
    fs::write(
        workspace.join("acyclic-workflow.rs"),
        include_bytes!("fixtures/overlay_workflow.rs"),
    )
    .expect("overlay workflow source");
}
fn assert_overlay_workflow_stayed_in_child(workspace: &Path, output: &str) {
    assert!(
        !workspace.join(output).exists(),
        "child relative write escaped into the physical root"
    );
    assert!(
        !workspace.join("acyclic-workflow-bin").exists()
            && !workspace.join("acyclic-workflow-bin.exe").exists(),
        "child compiler output escaped into the physical root"
    );
    assert!(
        !(0..2).any(|server| workspace
            .join(format!(".acyclic-workflow-child-{server}"))
            .exists()),
        "workflow subprocess wrote through the physical root"
    );
}

fn assert_fingerprint_before(
    fingerprints: &[RequestFingerprint],
    earlier: RequestFingerprint,
    later: RequestFingerprint,
) {
    let position = |fingerprint| {
        fingerprints
            .iter()
            .position(|candidate| *candidate == fingerprint)
            .unwrap_or_else(|| panic!("missing semantic provider phase {fingerprint:?}"))
    };
    assert!(
        position(earlier) < position(later),
        "semantic provider phase {earlier:?} must precede {later:?}: {fingerprints:?}"
    );
}

fn assert_codex_timeout_cleanup(binary: &Path, home: &Path, workspace: &Path) {
    let provider = ScriptedProvider::start_stalled();
    let host = codex_host_command(binary, home, workspace, &provider);
    assert_host_timeout_cleanup("Codex", home, &provider, host);
}

fn assert_claude_timeout_cleanup(binary: &Path, home: &Path, workspace: &Path) {
    let provider = ScriptedProvider::start_stalled();
    let debug = home.join("claude-timeout-debug.log");
    let host = claude_host_command(binary, home, workspace, &provider, &debug);
    assert_host_timeout_cleanup("Claude", home, &provider, host);
}

fn assert_host_timeout_cleanup(
    host_name: &str,
    home: &Path,
    provider: &ScriptedProvider,
    mut host: std::process::Command,
) {
    // The lifecycle run installed the integration; a fresh host proves it can start a new service.
    let service = ServiceGuard::new(home);
    let (
        BoundedOutput {
            output,
            expired,
            process_tree,
        },
        admitted,
    ) = output_after_provider_admission(
        &mut host,
        provider,
        Duration::from_secs(30),
        STALLED_PROVIDER_OBSERVATION,
    );
    assert!(
        admitted,
        "{host_name} did not reach the stalled provider\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        expired,
        "stalled {host_name} must hit the process-tree deadline"
    );
    service.assert_timeout_cleanup(process_tree);
}

fn codex_host_command(
    binary: &Path,
    home: &Path,
    workspace: &Path,
    provider: &ScriptedProvider,
) -> std::process::Command {
    let mut host = command(binary);
    host.current_dir(workspace);
    host.args([
        "exec",
        "--json",
        "--ignore-rules",
        "--skip-git-repo-check",
        "--dangerously-bypass-approvals-and-sandbox",
        "--dangerously-bypass-hook-trust",
        "--disable",
        "remote_plugin",
        "--disable",
        "recommended_plugins",
        "--disable",
        "plugin_sharing",
        "--enable",
        "hooks",
        "-c",
        "model_provider=\"acyclic_e2e\"",
        "-c",
        "model=\"gpt-5.6-sol\"",
        "-c",
        "model_providers.acyclic_e2e.name=\"Acyclic E2E\"",
        "-c",
        &format!(
            "model_providers.acyclic_e2e.base_url=\"{}/v1\"",
            provider.base_url()
        ),
        "-c",
        "model_providers.acyclic_e2e.wire_api=\"responses\"",
        "-c",
        "model_providers.acyclic_e2e.env_key=\"ACYCLIC_E2E_API_KEY\"",
    ]);
    host.arg("Run the deterministic qualification command.");
    host.env("ACYCLIC_E2E_API_KEY", "test");
    host.env("ACYCLIC_WORKFLOW_TOKEN", "qualified");
    isolated_codex_state(&mut host, home);
    host
}

fn claude_host_command(
    binary: &Path,
    home: &Path,
    workspace: &Path,
    provider: &ScriptedProvider,
    debug_log: &Path,
) -> std::process::Command {
    let mut host = command(binary);
    host.current_dir(workspace).args([
        "-p",
        "Run the deterministic qualification command.",
        "--output-format",
        "stream-json",
        "--verbose",
        "--include-hook-events",
        "--max-turns",
        "3",
        "--permission-mode",
        "bypassPermissions",
        "--permission-prompts",
        "none",
        "--debug",
        "hooks",
        "--debug-file",
    ]);
    host.arg(debug_log).args(["--setting-sources", "user"]);
    host.env("ANTHROPIC_API_KEY", "test");
    host.env("ANTHROPIC_BASE_URL", provider.base_url());
    host.env("ACYCLIC_WORKFLOW_TOKEN", "qualified");
    isolated_state(&mut host, home);
    host
}

fn assert_host_success(name: &str, output: &std::process::Output, expired: bool, debug: &str) {
    assert!(
        output.status.success() && !expired,
        "{name} expired={expired}\nstdout:\n{}\nstderr:\n{}\ndebug:\n{debug}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn assert_host_sentinel(name: &str, workspace: &Path, output: &std::process::Output, debug: &str) {
    let sentinel = fs::read_to_string(workspace.join("codex-e2e.txt"));
    assert!(
        sentinel
            .as_deref()
            .is_ok_and(|value| value.trim() == "qualified"),
        "{name} did not produce its sentinel: {sentinel:?}\nstdout:\n{}\nstderr:\n{}\ndebug:\n{debug}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn install_host(
    package: &PackagedPlugin,
    host: &str,
    host_binary: &Path,
    home: &Path,
) -> ServiceGuard {
    let mut install = package.command(&["install", host]);
    prepend_binary_directory(&mut install, host_binary);
    isolated_state(&mut install, home);
    if host == "codex" {
        preserve_windows_profile_identity(&mut install);
    }
    let output = install.output().expect("install host integration");
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    ServiceGuard::new(home)
}

fn isolated_codex_state(command: &mut std::process::Command, root: &Path) {
    isolated_state(command, root);
    command.env_remove("CODEX_PERMISSION_PROFILE");
    preserve_windows_profile_identity(command);
}

fn preserve_windows_profile_identity(command: &mut std::process::Command) {
    if cfg!(windows) {
        for name in ["HOME", "USERPROFILE"] {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
    }
}

fn prepend_binary_directory(command: &mut std::process::Command, binary: &Path) {
    let mut paths = vec![binary.parent().expect("host binary parent").to_path_buf()];
    if let Some(existing) = std::env::var_os("PATH") {
        paths.extend(std::env::split_paths(&existing));
    }
    command.env("PATH", std::env::join_paths(paths).expect("host PATH"));
}

fn assert_semantic_provider_exchange(provider: &ScriptedProvider) -> bool {
    let fingerprints = provider.semantic_fingerprints(5, Duration::from_millis(250));
    let mut observed = fingerprints.clone();
    observed.sort_unstable();
    let mut success = vec![
        RequestFingerprint::CodexRootPrompt,
        RequestFingerprint::CodexRootToolResult,
        RequestFingerprint::CodexRootSpawnResult,
        RequestFingerprint::CodexChildPrompt,
        RequestFingerprint::CodexChildToolResult,
    ];
    success.sort_unstable();
    let mut denied = vec![
        RequestFingerprint::CodexRootPrompt,
        RequestFingerprint::CodexRootToolResult,
        RequestFingerprint::CodexRootSpawnResult,
    ];
    denied.sort_unstable();
    assert!(
        observed == success || observed == denied,
        "unexpected Codex request phases: {fingerprints:?}"
    );
    assert_fingerprint_before(
        &fingerprints,
        RequestFingerprint::CodexRootPrompt,
        RequestFingerprint::CodexRootToolResult,
    );
    assert_fingerprint_before(
        &fingerprints,
        RequestFingerprint::CodexRootToolResult,
        RequestFingerprint::CodexRootSpawnResult,
    );
    if observed == success {
        assert_fingerprint_before(
            &fingerprints,
            RequestFingerprint::CodexChildPrompt,
            RequestFingerprint::CodexChildToolResult,
        );
        true
    } else {
        false
    }
}
