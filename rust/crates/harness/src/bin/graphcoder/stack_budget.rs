//! A whole `graphcoder` turn fits the process main thread's stack.
//!
//! `main` polls every turn on the process main thread, whose stack is fixed by
//! the platform: 1 MiB on Windows, the smallest of the release platforms. The
//! budget is measured on the target being built, through the same `serve` that
//! `main` calls, so every poll frame of the turn, the tools it runs, and the
//! filesystem work under them is counted. Each entry point `serve` selects is
//! measured: the headless prompt, JSONL over standard input, and the TUI on a
//! real pseudo-terminal (script(1) on Unix, the console pseudo-terminal on
//! Windows).
//!
//! This qualification is its own process's `main`, not a libtest test, so
//! each step reports failure as an error instead of panicking.
use crate::{args::Args, entry::serve};
#[path = "stack_budget/terminal.rs"]
mod terminal;
#[path = "stack_budget/unboxed.rs"]
mod unboxed;
use acyclic_harness::native_environment::variable;
use acyclic_test_support::stack::deepest_stack;
use axum::{Router, body::Bytes, http::StatusCode, routing::post};
use clap::Parser;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

/// Why a qualification step failed.
pub(crate) type Failure = Box<dyn std::error::Error + Send + Sync>;
/// The result of one qualification step.
pub(crate) type Outcome<T = ()> = Result<T, Failure>;

/// The smallest process main-thread stack of a release platform (Windows).
const MAIN_THREAD_STACK_KIB: usize = 1024;
/// Every measured turn leaves at least this many times its own depth unused.
const MARGIN: usize = 4;
const BUDGET_KIB: usize = MAIN_THREAD_STACK_KIB / MARGIN;

/// The task every scripted spawn gives its child. Quoted, so the child's own
/// request (where it is one JSON string) is told apart from the parent's
/// (where it is nested in the spawn call's argument string).
const CHILD_TASK: &str = r#"stack "child" task"#;
const CHILD_TASK_IN_REQUEST: &str = r#"stack \"child\" task"#;

static NULL: Value = Value::Null;

/// The value at the JSON `pointer` in `value`; null where there is none.
fn at<'a>(value: &'a Value, pointer: &str) -> &'a Value {
    value.pointer(pointer).unwrap_or(&NULL)
}

/// The `index`th value; null past the end.
fn nth(values: &[Value], index: usize) -> &Value {
    values.get(index).unwrap_or(&NULL)
}

/// Sets `key` on the JSON object `value`.
fn set(value: &mut Value, key: &str, field: Value) -> Outcome {
    value
        .as_object_mut()
        .ok_or_else(|| format!("{key}: not set on a non-object"))?
        .insert(key.to_owned(), field);
    Ok(())
}

/// The strings at `pointer` in every element of the array at `array`.
fn names(value: &Value, array: &str, pointer: &str) -> Outcome<Vec<String>> {
    at(value, array)
        .as_array()
        .ok_or_else(|| format!("{array} is not an array: {value}"))?
        .iter()
        .map(|item| {
            at(item, pointer)
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("{pointer} is not a string: {item}").into())
        })
        .collect()
}

/// Substitutes the file the latest tool result names for a `"$FILE"` argument.
fn with_file(arguments: &Value, body: &str) -> Outcome<String> {
    let arguments = arguments.as_str().ok_or("tool arguments are a string")?;
    if !arguments.contains("\"$FILE\"") {
        return Ok(arguments.to_owned());
    }
    let body: Value = serde_json::from_str(body)?;
    let file = at(&body, "/input")
        .as_array()
        .ok_or("the request carries its input")?
        .iter()
        .rev()
        .filter(|item| *at(item, "/type") == "function_call_output")
        .find_map(|item| {
            let output: Value = serde_json::from_str(at(item, "/output").as_str()?).ok()?;
            output.get("file").cloned()
        })
        .ok_or("a prior tool result names a file")?;
    Ok(arguments.replace("\"$FILE\"", &file.to_string()))
}

/// The scripted provider: where it listens, the parent requests it served,
/// and how to stop it.
struct Provider {
    address: std::net::SocketAddr,
    requests: Arc<Mutex<Vec<String>>>,
    stop: tokio::sync::oneshot::Sender<()>,
    server: std::thread::JoinHandle<Outcome>,
}

impl Provider {
    /// Stops the provider and returns the parent requests it served.
    fn finish(self) -> Outcome<Vec<String>> {
        let _ = self.stop.send(());
        self.server
            .join()
            .map_err(|_| "the scripted provider panicked")??;
        let requests = self
            .requests
            .lock()
            .map_err(|_| "the request log is poisoned")?;
        Ok(requests.clone())
    }
}

/// The script's state, shared by every request the provider serves.
struct Script {
    turns: Vec<Value>,
    child_writes: bool,
    address: std::net::SocketAddr,
    requests: Arc<Mutex<Vec<String>>>,
}

impl Script {
    /// The turn a child's request receives. A writing child leaves a change
    /// on its own branch, so the parent's merge joins two diverged trees.
    fn child_turn(&self, body: &str) -> Outcome<Value> {
        if self.child_writes && tool_outputs(body)?.is_empty() {
            let write = json!({"path": "child.txt", "text": "child\n",
                "media_type": "text/plain", "display_name": "child.txt"});
            return Ok(call("write", &write, "child-write"));
        }
        Ok(text("child done"))
    }

    /// The turn the existing-child fixture's own invocation receives.
    fn fixture_turn(body: &str) -> Outcome<Value> {
        let outputs = tool_outputs(body)?;
        match outputs.len() {
            0 => child_call("spawn"),
            1 => {
                let mut wait = child_call("wait")?;
                set(&mut wait, "call_id", json!("fixture-wait"))?;
                Ok(wait)
            }
            _ => {
                let waited = at(nth(&outputs, 1), "/agents/0/result/outcome/Succeeded/text");
                if *waited != "child done" {
                    return Err(format!("the fixture child did not finish: {waited}").into());
                }
                Ok(text("prepared"))
            }
        }
    }

    /// The next scripted parent turn; the request log advances the script.
    fn parent_turn(&self, body: String) -> Outcome<Value> {
        let mut requests = self
            .requests
            .lock()
            .map_err(|_| "the request log is poisoned")?;
        let served = requests.len();
        let mut turn = self
            .turns
            .get(served)
            .cloned()
            .unwrap_or_else(|| json!({"type": "message", "text": "unscripted"}));
        // A terminal driver quits the TUI once the script is served.
        if served + 1 == self.turns.len()
            && let Some(settled) = variable(SETTLED)
        {
            std::fs::write(settled, b"settled")?;
        }
        if *at(&turn, "/type") == "function_call" {
            set(&mut turn, "call_id", json!(format!("call{served}")))?;
            let arguments = with_file(at(&turn, "/arguments"), &body)?
                .replace("$ORIGIN", &format!("http://{}", self.address));
            set(&mut turn, "arguments", json!(arguments))?;
            // Actual SDK rejection: popping before any push
            // must not count as successful deep tool coverage.
            if variable(EARLY_ERROR).is_some()
                && *at(&turn, "/name") == "fs"
                && at(&turn, "/arguments") == &json!(json!({"command": "stash push"}).to_string())
            {
                let pop = json!({"command": "stash pop"}).to_string();
                set(&mut turn, "arguments", json!(pop))?;
            }
        }
        requests.push(body);
        Ok(turn)
    }

    /// The streamed response to one provider request.
    fn respond(&self, body: &[u8]) -> Outcome<String> {
        let body = String::from_utf8(body.to_vec())?;
        // Children run their own turns against the same provider;
        // only the parent's requests advance the script.
        let turn = if body.contains(CHILD_TASK_IN_REQUEST) {
            self.child_turn(&body)?
        } else if body.contains("stack fixture") {
            Self::fixture_turn(&body)?
        } else {
            self.parent_turn(body)?
        };
        eprintln!(
            "stack provider response: {} {}",
            at(&turn, "/name"),
            at(&turn, "/arguments")
        );
        let event = if *at(&turn, "/type") == "message" {
            json!({"type": "response.output_text.delta", "delta": at(&turn, "/text")})
        } else {
            json!({"type": "response.output_item.done", "output_index": 0, "item": turn})
        };
        let events = [
            event,
            json!({"type": "response.completed", "response": {"status": "completed",
                "usage": {"input_tokens": 64, "output_tokens": 2}}}),
        ];
        Ok(events
            .iter()
            .map(|event| format!("data: {event}\n\n"))
            .collect())
    }
}

/// The provider answers each request with the next scripted turn.
fn scripted(turns: Vec<Value>, child_writes: bool) -> Outcome<Provider> {
    // The request log is the only fixture lock; no other lock is acquired under it.
    let requests = Arc::new(Mutex::new(Vec::new()));
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let address = listener.local_addr()?;
    let script = Arc::new(Script {
        turns,
        child_writes,
        address,
        requests: Arc::clone(&requests),
    });
    let app = Router::new()
        .route("/source", axum::routing::get(|| async { SOURCE }))
        .route(
            "/count",
            post(|| async { axum::Json(json!({"input_tokens": 64})) }),
        )
        .route(
            "/responses",
            post(move |body: Bytes| {
                let script = Arc::clone(&script);
                async move {
                    let headers = [("content-type", "text/event-stream")];
                    match script.respond(&body) {
                        Ok(stream) => (StatusCode::OK, headers, stream),
                        Err(error) => {
                            eprintln!("stack provider failed: {error}");
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                headers,
                                error.to_string(),
                            )
                        }
                    }
                }
            }),
        );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = std::thread::spawn(move || -> Outcome {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener)?;
                axum::serve(listener, app)
                    .with_graceful_shutdown(async {
                        let _ = stopped.await;
                    })
                    .await?;
                Ok(())
            })
    });
    Ok(Provider {
        address,
        requests,
        stop,
        server,
    })
}

/// How the measured invocation receives its prompt: each is a distinct `serve` path.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Entry {
    /// `-p`: the headless client.
    Prompt,
    /// `--input-format jsonl`: the JSONL client over standard input.
    Jsonl,
    /// No prompt, on a terminal: the TUI and its local command loop.
    Tui,
}

/// Names the file the scripted provider writes once its last turn is served.
const SETTLED: &str = "GRAPHCODER_STACK_SETTLED";
/// Selects the case in a process whose argv a terminal host fixes.
const CASE: &str = "GRAPHCODER_STACK_CASE";
/// Fixture-only counterexample to accepting an early tool error as coverage.
const EARLY_ERROR: &str = "GRAPHCODER_STACK_EARLY_ERROR";

/// The directories and catalog one measured invocation runs against.
struct Fixture {
    directory: tempfile::TempDir,
    workspace: std::path::PathBuf,
    settings: std::path::PathBuf,
    catalog: std::path::PathBuf,
}

impl Fixture {
    /// A workspace with one file, and a catalog naming the provider at `address`.
    fn new(address: std::net::SocketAddr) -> Outcome<Self> {
        let directory = tempfile::tempdir()?;
        let workspace = directory.path().join("workspace");
        let settings = directory.path().join("settings");
        std::fs::create_dir(&workspace)?;
        std::fs::create_dir(&settings)?;
        std::fs::write(workspace.join("calc.py"), "x = 1\n")?;
        let catalog = directory.path().join("catalog.json");
        std::fs::write(
            &catalog,
            serde_json::to_vec(&json!({"stack": {
                "adapter": "responses", "authentication": "optional", "base_url": format!("http://{address}"),
                "models": {"tiny": {"capacity": {"context_tokens": 8192, "output_tokens": 1024},
                    "features": {"tools": true, "images": false}}},
                "max_event_bytes": 65536,
                "token_counter": {"path": "count", "adapter": "responses",
                    "response_field": "input_tokens", "framing_tokens": 0}
            }}))?,
        )?;
        Ok(Self {
            directory,
            workspace,
            settings,
            catalog,
        })
    }

    fn state(&self) -> std::path::PathBuf {
        self.directory.path().join("state")
    }

    /// The invocation's arguments for `entry`, before its prompt.
    fn argv(&self, entry: Entry) -> Outcome<Vec<std::ffi::OsString>> {
        let mut argv: Vec<std::ffi::OsString> = vec![
            "graphcoder".into(),
            "--workspace".into(),
            self.workspace.clone().into(),
            "--catalog".into(),
            self.catalog.clone().into(),
            "--settings-root".into(),
            self.settings.clone().into(),
            "--model".into(),
            "stack/tiny".into(),
        ];
        match entry {
            Entry::Prompt => argv.extend(["-p".into(), "go".into()]),
            Entry::Jsonl => {
                let operation = std::env::args()
                    .find_map(|arg| arg.strip_prefix("--stack-operation=").map(str::to_owned))
                    .ok_or("a JSONL case names its operation")?;
                argv.extend(["--input-format".into(), "jsonl".into()]);
                argv.extend(["--operation".into(), operation.into()]);
            }
            Entry::Tui => {}
        }
        Ok(argv)
    }

    /// Prepares an admitted child through the shipping entrypoint, with its
    /// ordinary child admission, for the measured invocation to find.
    fn prepare_child(&self, mut argv: Vec<std::ffi::OsString>) -> Outcome {
        if let Some(prompt) = argv.last_mut() {
            *prompt = "stack fixture".into();
        }
        let code = serve(
            Args::try_parse_from(argv)?,
            Some(acyclic_harness_providers::Credentials::explicit(
                Default::default(),
            )),
            Some(self.state()),
        )
        .map_err(|error| error.to_string())?;
        if code != 0 {
            return Err(format!("child fixture exited with {code}").into());
        }
        admitted(
            &self.workspace,
            &self.catalog,
            &self.settings,
            &self.state(),
        )
    }
}

/// One measured invocation: its exit code, depth in bytes, and the parent
/// requests the provider served.
struct Measured {
    code: u8,
    used: usize,
    requests: Vec<String>,
}

/// Runs one invocation through `serve` and measures its depth.
fn measured_turn<const UNBOXED: bool>(
    turns: Vec<Value>,
    entry: Entry,
    child_writes: bool,
) -> Outcome<Measured> {
    let first = turns.first().map(|turn| at(turn, "/name").clone());
    let spawn = first.as_ref().is_some_and(|name| *name == "spawn");
    let existing_child = child_writes
        || first
            .as_ref()
            .is_some_and(|name| matches!(name.as_str(), Some("send" | "wait" | "archive")));
    let provider = scripted(turns, child_writes)?;
    let fixture = Fixture::new(provider.address)?;
    let argv = fixture.argv(entry)?;
    if existing_child {
        fixture.prepare_child(argv.clone())?;
    }
    let args = Args::try_parse_from(argv)?;
    let serve = if UNBOXED { unboxed::serve } else { serve };
    let callback = || {
        serve(
            args,
            Some(acyclic_harness_providers::Credentials::explicit(
                Default::default(),
            )),
            Some(fixture.state()),
        )
        .map_err(|error| error.to_string())
    };
    let (code, used) = deepest_stack(callback);
    if spawn {
        admitted(
            &fixture.workspace,
            &fixture.catalog,
            &fixture.settings,
            &fixture.state(),
        )?;
    }
    let requests = provider.finish()?;
    if let Some(first) = requests.first() {
        let offered = names(&serde_json::from_str(first)?, "/tools", "/name")?;
        eprintln!("offered tools: {offered:?}");
    }
    eprintln!(
        "GraphCoder turn: {} KiB, {} requests, unboxed CLI boundary: {UNBOXED}",
        used.div_ceil(1024),
        requests.len()
    );
    Ok(Measured {
        code: code?,
        used,
        requests,
    })
}

/// Every case, each run in a process of its own.
const CASES: &[&str] = &[
    "diff",
    "commit",
    "tools-files",
    "tools-history",
    "tools-branches",
    "tools-recovery",
    "tools-rejected",
    "merge-continue",
    "merge-abort",
    "child-merge",
    "spawn",
    "send",
    "wait",
    "archive",
    "jsonl",
    "tui",
    "unboxed",
];

/// Runs every case in its own process on the platform's process main thread.
pub(crate) fn qualify() -> Outcome {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "--list") {
        if !args.iter().any(|arg| arg == "--ignored") {
            for case in CASES {
                println!("stack_budget::{case}: test");
            }
        }
        return Ok(());
    }
    let case = variable(CASE)
        .and_then(|case| case.into_string().ok())
        .or_else(|| {
            std::env::args().find_map(|arg| arg.strip_prefix("--stack-case=").map(str::to_owned))
        });
    if let Some(case) = case {
        return run_case(&case);
    }
    if args.iter().any(|arg| arg == "--ignored") {
        return Ok(());
    }
    let filter = args.iter().find(|arg| !arg.starts_with('-'));
    let exact = args.iter().any(|arg| arg == "--exact");
    assert_process_stack()?;
    for case in CASES {
        let name = format!("stack_budget::{case}");
        let selected = filter.is_none_or(|filter| {
            if exact {
                &name == filter
            } else {
                name.contains(filter.as_str())
            }
        });
        if selected {
            run_process(case)?;
        }
    }
    Ok(())
}

/// The scripted turns of `case`.
fn case_turns(case: &str) -> Outcome<Vec<Value>> {
    Ok(match case {
        "diff" | "jsonl" | "tui" => {
            vec![fs("diff"), fs("commit -m seed"), fs("diff"), text("done")]
        }
        "commit" | "unboxed" => vec![fs("commit -m stack"), text("done")],
        "spawn" => vec![child_call("spawn")?, text("done")],
        "tools-rejected" => tool_group("tools-recovery")?,
        case if case.starts_with("tools-") => tool_group(case)?,
        "merge-continue" | "merge-abort" => merge_recovery(case)?,
        "child-merge" => child_merge()?,
        "send" | "wait" | "archive" => vec![child_call(case)?, text("done")],
        _ => return Err(format!("unknown stack case {case}").into()),
    })
}

/// Measures `case` in this process and checks what its turn proved.
fn run_case(case: &str) -> Outcome {
    let turns = case_turns(case)?;
    let entry = match case {
        "jsonl" => Entry::Jsonl,
        "tui" => Entry::Tui,
        _ => Entry::Prompt,
    };
    let child_writes = case == "child-merge";
    let measured = if case == "unboxed" {
        measured_turn::<true>(turns, entry, child_writes)?
    } else {
        measured_turn::<false>(turns, entry, child_writes)?
    };
    match case {
        "tools-rejected" => covers_every_entry_point("tools-recovery", &measured),
        case if case.starts_with("tools-") => covers_every_entry_point(case, &measured),
        "merge-continue" | "merge-abort" => covers_merge_recovery(case, &measured),
        "child-merge" => covers_child_merge(&measured),
        "unboxed" => {
            let Measured {
                code,
                used,
                requests,
            } = &measured;
            assert_eq!(*code, 0);
            let response = requests.get(1).ok_or("tool response request")?;
            assert!(response.contains("Committed"));
            assert!(
                *used >= BUDGET_KIB * 1024,
                "unboxed boundary escaped measurement: {used} bytes"
            );
            Ok(())
        }
        _ => covers_turn(case, &measured),
    }
}

/// Checks a single-tool turn: its depth, and the result its tool returned.
fn covers_turn(case: &str, measured: &Measured) -> Outcome {
    let Measured {
        code,
        used,
        requests,
    } = measured;
    assert!(
        *used < BUDGET_KIB * 1024,
        "{case}: {used} bytes exceeds the strict {BUDGET_KIB} KiB budget"
    );
    assert_eq!(*code, 0, "{case}: the turn did not complete");
    if CHILD_TOOLS.contains(&case) {
        // The scripted call is turn 0, so its result is in request 1;
        // later requests may follow child notifications.
        let outputs = tool_outputs(requests.get(1).ok_or("child result request")?)?;
        let output = outputs.last().ok_or("child result reached model")?;
        match case {
            "wait" => {
                assert_eq!(*at(output, "/agents/0/reference"), "kid", "{output}");
                assert_eq!(at(output, "/agents/0/result/turn"), 1, "{output}");
                assert_eq!(
                    *at(output, "/agents/0/result/outcome/Succeeded/text"),
                    "child done",
                    "{output}"
                );
            }
            "send" => assert_eq!(output, &json!({"agent":"kid", "queued":true})),
            _ => assert_eq!(output, &json!({"agent":"kid"})),
        }
        return Ok(());
    }
    // The tool result is a JSON string inside the request body.
    let expected = if matches!(case, "diff" | "jsonl" | "tui") {
        r#"\"kind\":\"diff\""#
    } else {
        "Committed"
    };
    let response = requests.get(1).ok_or("tool result was not returned")?;
    assert!(
        response.contains(expected),
        "{case}: tool response lacks {expected:?}: {response}"
    );
    Ok(())
}

/// Runs `case` in a child process on its own main thread.
fn run_process(case: &str) -> Outcome {
    let executable = std::env::current_exe()?;
    if case == "tui" {
        return terminal::run(&executable);
    }
    #[cfg(unix)]
    let mut command = {
        let mut command = std::process::Command::new("sh");
        command
            .args(["-c", "ulimit -s 1024; exec \"$@\"", "stack-main"])
            .arg(executable);
        command
    };
    #[cfg(windows)]
    let mut command = std::process::Command::new(executable);
    command.arg(format!("--stack-case={case}"));
    if case == "tools-rejected" {
        let output = command.env(EARLY_ERROR, "1").output()?;
        let error = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.code(),
            Some(101),
            "early-error control: {error}"
        );
        assert!(
            error.contains("expected the successful operation") && error.contains("stash push"),
            "early-error control failed for another reason: {error}"
        );
        return Ok(());
    }
    let status = if case == "jsonl" {
        use std::io::Write as _;
        let operation = acyclic_harness::OperationId::new();
        let request = acyclic_harness::client::jsonl::request(
            &acyclic_harness::client::prepare_local_prompt(operation, "go")?,
        )?;
        let mut child = command
            .arg(format!("--stack-operation={operation}"))
            .stdin(std::process::Stdio::piped())
            .spawn()?;
        let mut stdin = child.stdin.take().ok_or("the case's standard input")?;
        writeln!(stdin, "{request}")?;
        drop(stdin);
        child.wait()?
    } else {
        command.status()?
    };
    assert!(status.success(), "main-thread {case}");
    Ok(())
}

fn call(name: &str, arguments: &Value, call_id: &str) -> Value {
    json!({"type": "function_call", "call_id": call_id, "name": name,
        "arguments": arguments.to_string()})
}

/// Every model tool `graphcoder` offers. A tool the provider sees that is not
/// listed here fails the `tools` case, so a new tool cannot ship unmeasured.
#[cfg(not(windows))]
const TOOLS: &[&str] = &[
    "archive",
    "bash",
    "edit",
    "fs",
    "read",
    "send",
    "spawn",
    "url_fetch",
    "wait",
    "write",
];
#[cfg(windows)]
const TOOLS: &[&str] = &[
    "archive",
    "edit",
    "fs",
    "powershell",
    "read",
    "send",
    "spawn",
    "url_fetch",
    "wait",
    "write",
];

/// The platform shell tool `graphcoder` offers: `bash` on Unix, `powershell`
/// on Windows.
const SHELL: &str = if cfg!(windows) { "powershell" } else { "bash" };

/// Writes `text` byte for byte to `path` through the platform shell. The
/// fixture texts never contain a single quote.
fn write_file(path: &str, text: &str) -> Outcome<Value> {
    if text.contains('\'') {
        return Err(format!("fixture text {text:?} contains a single quote").into());
    }
    let command = if cfg!(windows) {
        format!("[IO.File]::WriteAllText((Join-Path $PWD '{path}'), '{text}')")
    } else {
        format!("printf '%s' '{text}' > {path}")
    };
    Ok(call(SHELL, &json!({ "command": command }), "call"))
}

/// Prints the files' exact bytes, in order, through the platform shell.
fn print_files(paths: &[&str]) -> Value {
    let command = if cfg!(windows) {
        let paths = paths
            .iter()
            .map(|path| format!("'{path}'"))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "foreach ($path in {paths}) {{ [Console]::Out.Write([IO.File]::ReadAllText((Join-Path $PWD $path))) }}"
        )
    } else {
        format!("cat {}", paths.join(" "))
    };
    call(SHELL, &json!({ "command": command }), "call")
}

/// The body the fixture serves to `url_fetch`, a real credential-free GET.
const SOURCE: &str = "stack source\n";

/// One `fs` command per Git-compatible command variant, in an order where each
/// finds the history it acts on. `apply` takes the typed patch path, so it is
/// the one spelling `parse_git_argv` does not admit.
const FS_COMMANDS: &[&str] = &[
    "status",
    "add -A",
    "commit -m first",
    "log -n 5",
    "show HEAD",
    "branch topic",
    "switch topic",
    "commit -m topic",
    "switch main",
    "merge topic",
    "merge --continue",
    "merge --abort",
    "rebase topic",
    "cherry-pick topic",
    "revert HEAD",
    "stash push",
    "stash pop",
    "tag v1",
    "blame notes.txt",
    "grep alpha",
    "clean -n",
    "archive HEAD",
    "apply change.patch",
    "commit -m patched",
    "bisect start",
    "rev-parse HEAD",
    "symbolic-ref HEAD",
    "merge-base HEAD topic",
    "discard topic",
    "ls-files",
    "check-ignore notes.txt",
    "restore --source HEAD notes.txt",
    "reset --soft HEAD",
    "diff",
];

/// The tools that act on child agents. Each settles the headless turn, so each
/// is measured in a process of its own.
const CHILD_TOOLS: &[&str] = &["spawn", "send", "wait", "archive"];

fn child_call(name: &str) -> Outcome<Value> {
    let arguments = match name {
        "spawn" => json!({"task": CHILD_TASK, "name": "kid"}),
        "send" => json!({"agent": "kid", "message": "hello"}),
        "wait" => json!({"agents": ["kid"], "mode": "any"}),
        "archive" => json!({"agent": "kid"}),
        _ => return Err(format!("{name} is not a child tool").into()),
    };
    Ok(call(name, &arguments, "call"))
}

/// A session that calls every other tool and every `fs` command on the main thread.
fn tool_group(case: &str) -> Outcome<Vec<Value>> {
    let notes = json!({"path": "notes.txt", "text": "alpha beta\n",
        "media_type": "text/plain", "display_name": "notes.txt"});
    let mut turns = vec![
        call("write", &notes, "call"),
        call("read", &json!({"file": "$FILE"}), "call"),
        call("write", &notes, "call"),
        call(
            "edit",
            &json!({"file": "$FILE", "old_text": "beta", "new_text": "gamma"}),
            "call",
        ),
        call("url_fetch", &json!({"url": "$ORIGIN/source"}), "call"),
        write_file(
            "change.patch",
            "diff --git a/calc.py b/calc.py\n--- a/calc.py\n+++ b/calc.py\n@@ -1 +1 @@\n-x = 1\n+x = 2\n",
        )?,
    ];
    // Every group starts with the same file tools and its own complete history.
    // Group by public operation family so each semantic suite stays bounded.
    if case != "tools-branches" {
        turns.push(fs("commit -m seed"));
        turns.push(fs("branch topic"));
    }
    for command in tool_commands(case)? {
        // A commit must capture a real change. An early NothingToCommit reply
        // never reaches the filesystem work whose stack this suite measures.
        if matches!(*command, "commit -m first" | "commit -m topic") {
            turns.push(write_file("notes.txt", &format!("alpha {command}\n"))?);
        }
        turns.push(fs(command));
    }
    if case == "tools-recovery" {
        turns.push(print_files(&["calc.py"]));
    }
    turns.push(text("done"));
    Ok(turns)
}

/// The tool groups that together call every `fs` command.
const TOOL_GROUPS: &[&str] = &[
    "tools-files",
    "tools-history",
    "tools-branches",
    "tools-recovery",
];

fn tool_commands(case: &str) -> Outcome<&'static [&'static str]> {
    Ok(match case {
        "tools-files" => &[
            "status",
            "add -A",
            "clean -n",
            "ls-files",
            "check-ignore notes.txt",
            "restore --source HEAD notes.txt",
            "reset --soft HEAD",
            "diff",
        ],
        "tools-history" => &[
            "commit -m first",
            "log -n 5",
            "show HEAD",
            "tag v1",
            "blame notes.txt",
            "grep alpha",
            "archive HEAD",
            "rev-parse HEAD",
            "symbolic-ref HEAD",
            "merge-base HEAD topic",
            "discard topic",
        ],
        "tools-branches" => &[
            "commit -m first",
            "branch topic",
            "switch topic",
            "commit -m topic",
            "switch main",
            "merge topic",
            "rebase topic",
            "cherry-pick topic",
            "revert HEAD",
        ],
        "tools-recovery" => &[
            "stash push",
            "stash pop",
            "apply change.patch",
            "commit -m patched",
            "bisect start",
        ],
        _ => return Err(format!("unknown tool group {case}").into()),
    })
}

/// Every offered tool and every `fs` command variant has a measured call.
fn covers_every_tool(case: &str, first_request: &str) -> Outcome {
    let mut offered = names(&serde_json::from_str(first_request)?, "/tools", "/name")?;
    offered.sort();
    assert_eq!(offered, TOOLS, "every offered tool has a measured call");
    let called: std::collections::BTreeSet<_> = tool_group(case)?
        .iter()
        .filter_map(|turn| at(turn, "/name").as_str().map(str::to_owned))
        .chain(CHILD_TOOLS.iter().map(|tool| (*tool).to_owned()))
        .collect();
    assert_eq!(called.into_iter().collect::<Vec<_>>(), TOOLS);
    let mut grouped = std::collections::BTreeSet::from(["merge --continue", "merge --abort"]);
    for group in TOOL_GROUPS {
        grouped.extend(tool_commands(group)?.iter().copied());
    }
    assert_eq!(
        grouped,
        FS_COMMANDS.iter().copied().collect(),
        "all command paths remain measured"
    );
    let mut variants = std::collections::BTreeSet::new();
    for command in grouped {
        let argv: Vec<String> = command.split(' ').map(str::to_owned).collect();
        let variant = if command.starts_with("apply ") {
            "Apply"
        } else {
            acyclic_fs::git_compat::parse_git_argv::<std::io::Error>(&argv, "stack", 0)
                .map_err(|error| format!("{command}: {error}"))?
                .variant_name()
        };
        variants.insert(variant);
    }
    let all: std::collections::BTreeSet<_> = acyclic_fs::git_compat::GIT_COMPAT_COMMAND_VARIANTS
        .iter()
        .copied()
        .collect();
    assert_eq!(
        variants, all,
        "every fs command variant has a measured call"
    );
    Ok(())
}

fn covers_every_entry_point(case: &str, measured: &Measured) -> Outcome {
    let Measured {
        code,
        used,
        requests,
    } = measured;
    assert!(
        *used < BUDGET_KIB * 1024,
        "tools: {used} bytes exceeds the strict {BUDGET_KIB} KiB budget"
    );
    assert_eq!(*code, 0, "tools: the session did not complete");
    covers_every_tool(case, requests.first().ok_or("the session's first request")?)?;
    // Each result must prove the successful operation, not just a reply. The
    // conflict-only merge recovery variants have their own successful suites.
    let outputs = tool_outputs(requests.last().ok_or("the session's last request")?)?;
    let turns = tool_group(case)?;
    assert_eq!(outputs.len() + 1, turns.len());
    for (turn, output) in turns.iter().zip(&outputs) {
        successful_tool_result(turn, output)?;
    }
    if case == "tools-recovery" {
        let printed = outputs.last().map(|output| at(output, "/stdout"));
        assert_eq!(printed, Some(&json!("x = 2\n")));
    }
    Ok(())
}

fn successful_tool_result(turn: &Value, output: &Value) -> Outcome {
    assert!(
        tool_result_succeeded(turn, output)?,
        "{turn}: expected the successful operation, received {output}"
    );
    // The former reply-count oracle accepted this same early error for every
    // entry point. Keep that counterexample beside each successful result.
    assert!(
        !tool_result_succeeded(turn, &json!({"error": "rejected before filesystem work"}))?,
        "{turn}: an early error passed the semantic oracle"
    );
    Ok(())
}

fn tool_result_succeeded(turn: &Value, output: &Value) -> Outcome<bool> {
    let name = at(turn, "/name")
        .as_str()
        .ok_or("the turn names its tool")?;
    Ok(match name {
        "read" => *output == "alpha beta\n",
        "write" | "edit" => at(output, "/file").is_object(),
        name if name == SHELL => {
            at(output, "/exit_code").as_i64() == Some(0)
                && at(output, "/success").as_bool() == Some(true)
        }
        "url_fetch" => *at(output, "/content") == SOURCE,
        "fs" => {
            let arguments = at(turn, "/arguments").as_str().ok_or("fs arguments")?;
            let arguments: Value = serde_json::from_str(arguments)?;
            let command = at(&arguments, "/command").as_str().ok_or("fs command")?;
            let result = at(output, "/result").clone();
            match serde_json::from_value::<acyclic_fs::GitCommandOutput>(result) {
                Ok(result) => fs_succeeded(command, &result)?,
                Err(_) => false,
            }
        }
        name => return Err(format!("missing semantic oracle for {name}").into()),
    })
}

/// Whether `result` proves the successful operation of the `fs` `command`.
fn fs_succeeded(command: &str, result: &acyclic_fs::GitCommandOutput) -> Outcome<bool> {
    use acyclic_fs::{GitCommandOutput as O, GitFilesystemResult as F};
    let program = command.split_whitespace().next().unwrap_or_default();
    Ok(match program {
        "commit" | "merge" | "rebase" | "cherry-pick" | "revert" => {
            matches!(result, O::Committed(_))
        }
        "status" => matches!(result, O::Status(_)),
        "log" | "show" => matches!(result, O::Commits(commits) if !commits.is_empty()),
        "tag" => matches!(result, O::Tags(tags) if tags.contains_key("v1")),
        "branch" => {
            matches!(result, O::Filesystem(F::Forked { workspace_id })
                if workspace_id.into_bytes() != [0; 16])
        }
        // A transition that moves the tree reports the applied tree;
        // one that leaves it unchanged reports no filesystem effect.
        "switch" | "stash" => {
            *result == O::NoOp || matches!(result, O::Filesystem(F::Applied { .. }))
        }
        "reset" | "add" => *result == O::NoOp,
        "rev-parse" | "symbolic-ref" | "merge-base" => {
            matches!(result, O::Text(text) if !text.is_empty())
        }
        "ls-files" => {
            matches!(result, O::Paths(paths) if paths.iter().any(|path| path == "notes.txt"))
        }
        "bisect" => matches!(result, O::Bisect(_)),
        "discard" => {
            matches!(result, O::Branches { current, branches }
                if current == "main" && !branches.iter().any(|branch| branch.name == "topic"))
        }
        "apply" | "restore" => matches!(result, O::Filesystem(F::Applied { .. })),
        inspection @ ("blame" | "grep" | "clean" | "archive" | "check-ignore" | "diff") => {
            matches!(result, O::Filesystem(F::Data { kind, .. }) if kind == inspection)
        }
        _ => return Err(format!("missing semantic oracle for {command}").into()),
    })
}

fn fs(command: &str) -> Value {
    call("fs", &json!({"command": command}), "call")
}
fn text(text: &str) -> Value {
    json!({"type": "message", "text": text})
}

fn tool_outputs(request: &str) -> Outcome<Vec<Value>> {
    let request: Value = serde_json::from_str(request)?;
    at(&request, "/input")
        .as_array()
        .ok_or("the request carries its input")?
        .iter()
        .filter(|item| *at(item, "/type") == "function_call_output")
        .map(|item| {
            let output = at(item, "/output").as_str().ok_or("a tool output string")?;
            Ok(serde_json::from_str(output)?)
        })
        .collect()
}

fn merge_recovery(case: &str) -> Outcome<Vec<Value>> {
    let mut turns = vec![
        fs("commit -m base"),
        fs("branch topic"),
        fs("switch topic"),
        write_file("calc.py", "theirs\n")?,
        fs("commit -m theirs"),
        fs("switch main"),
        write_file("calc.py", "ours\n")?,
        fs("commit -m ours"),
        fs("merge topic"),
    ];
    if case == "merge-continue" {
        turns.push(write_file("calc.py", "resolved\n")?);
        turns.push(fs("merge --continue"));
    } else {
        turns.push(fs("merge --abort"));
    }
    turns.push(print_files(&["calc.py"]));
    turns.push(fs("status"));
    turns.push(text("done"));
    Ok(turns)
}

fn covers_merge_recovery(case: &str, measured: &Measured) -> Outcome {
    let Measured {
        code,
        used,
        requests,
    } = measured;
    assert_eq!(*code, 0, "{case}");
    assert!(*used < BUDGET_KIB * 1024, "{case}: {used} bytes");
    let outputs = tool_outputs(requests.last().ok_or("the session's last request")?)?;
    assert_eq!(outputs.len() + 1, merge_recovery(case)?.len());
    let merged = nth(&outputs, 8);
    assert!(
        merged.to_string().contains("unresolved conflicts"),
        "merge did not conflict: {merged}"
    );
    let recovery = if case == "merge-continue" { 10 } else { 9 };
    let recovered = nth(&outputs, recovery);
    assert!(
        at(recovered, "/result").is_object(),
        "recovery failed: {recovered}"
    );
    let expected = if case == "merge-continue" {
        "resolved\n"
    } else {
        "ours\n"
    };
    let printed = nth(&outputs, recovery + 1);
    assert_eq!(
        *at(printed, "/stdout"),
        expected,
        "wrong recovered bytes: {printed}"
    );
    let status = outputs.last().unwrap_or(&NULL);
    assert!(
        at(status, "/result/Status").is_object(),
        "status failed: {status}"
    );
    Ok(())
}

/// The parent joins a finished child's branch into its own diverged tree:
/// the `fs merge agents/<child>` path of a spawned agent's work.
fn child_merge() -> Outcome<Vec<Value>> {
    let parent = json!({"path": "parent.txt", "text": "parent\n",
        "media_type": "text/plain", "display_name": "parent.txt"});
    Ok(vec![
        call("write", &parent, "call"),
        child_call("archive")?,
        fs("merge agents/kid"),
        print_files(&["child.txt", "parent.txt"]),
        text("done"),
    ])
}

fn covers_child_merge(measured: &Measured) -> Outcome {
    let Measured {
        code,
        used,
        requests,
    } = measured;
    assert_eq!(*code, 0, "child-merge: the session did not complete");
    assert!(
        *used < BUDGET_KIB * 1024,
        "child-merge: {used} bytes exceeds the strict {BUDGET_KIB} KiB budget"
    );
    let outputs = tool_outputs(requests.last().ok_or("the session's last request")?)?;
    assert_eq!(outputs.len() + 1, child_merge()?.len(), "{outputs:?}");
    let written = nth(&outputs, 0);
    assert!(at(written, "/file").is_object(), "parent write: {written}");
    let archived = nth(&outputs, 1);
    assert_eq!(archived, &json!({"agent": "kid"}), "archive: {archived}");
    let merge = nth(&outputs, 2);
    let merged =
        serde_json::from_value::<acyclic_fs::GitCommandOutput>(at(merge, "/result").clone());
    assert!(
        matches!(merged, Ok(acyclic_fs::GitCommandOutput::Committed(_))),
        "child merge did not commit a join: {merge}"
    );
    let printed = nth(&outputs, 3);
    assert_eq!(
        *at(printed, "/stdout"),
        "child\nparent\n",
        "merged tree lacks either side: {printed}"
    );
    Ok(())
}

fn admitted(
    workspace: &std::path::Path,
    catalog: &std::path::Path,
    settings: &std::path::Path,
    state: &std::path::Path,
) -> Outcome {
    use acyclic_harness::local::{LocalHarness, LocalHarnessConfig};
    use acyclic_harness::task_volume::{ChildFile, ChildStatus, Component, FileKey, Query};
    use acyclic_harness_providers::{Credentials, Registry};
    let mut registry = Registry::new(Credentials::explicit(Default::default()))?;
    registry.load_catalog(catalog)?;
    let provider = Arc::new(registry.resolve("stack/tiny")?);
    let config = LocalHarnessConfig::new(workspace, (provider.model().clone(), provider))
        .state_directory(state)
        .settings(
            acyclic_harness::native_settings::SettingsConfig::new(workspace, settings)
                .codex_home(settings.join(".codex")),
        );
    let inputs = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(acyclic_fs::in_heap(|| async move {
            let local = LocalHarness::open(config).await?;
            let snapshot = local
                .task_files()
                .snapshot(&Query::Child(Component::new("kid")?), 16)
                .await?;
            let status = snapshot
                .files
                .iter()
                .find(|file| {
                    matches!(
                        &file.key,
                        FileKey::Child {
                            file: ChildFile::Status,
                            ..
                        }
                    )
                })
                .map(|file| ChildStatus::parse(&file.bytes))
                .transpose()?;
            local.disconnect().await?;
            Ok::<_, Failure>(status.map(|status| status.inputs))
        }))?;
    let inputs = inputs.ok_or("spawn must publish its admitted child status")?;
    assert_eq!(inputs, 1);
    Ok(())
}

#[cfg(windows)]
fn assert_process_stack() -> Outcome {
    let bytes = std::fs::read(std::env::current_exe()?)?;
    let word = |offset: usize| -> Outcome<usize> {
        let word = bytes.get(offset..offset + 4).ok_or("a truncated image")?;
        Ok(usize::try_from(u32::from_le_bytes(word.try_into()?))?)
    };
    let pe = word(0x3c)?;
    assert_eq!(bytes.get(pe..pe + 4), Some(b"PE\0\0".as_slice()));
    let offset = pe + 24 + 72;
    let reserved = bytes.get(offset..offset + 8).ok_or("a truncated image")?;
    let reserved = u64::from_le_bytes(reserved.try_into()?);
    assert_eq!(
        reserved,
        1024 * 1024,
        "the executable must retain the default 1 MiB stack"
    );
    Ok(())
}
#[cfg(unix)]
fn assert_process_stack() -> Outcome {
    Ok(())
}
