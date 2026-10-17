//! Native hook/MCP processes share LocalHarness request and result files.
use futures::FutureExt as _;
use serde_json::{Value, json};
use std::path::Path;
use tokio::{
    io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout},
};

#[path = "support/native_hooks.rs"]
mod native_hooks;
use native_hooks::*;

#[path = "native_tools/commands.rs"]
mod commands;

#[path = "native_tools/parallel.rs"]
mod parallel;

struct ResultClient {
    child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
    next_id: u64,
}

impl ResultClient {
    async fn open(directory: &Path, host: &str, binding: Option<&Path>) -> Result<Self> {
        let mut launch = command(directory);
        if let Some(binding) = binding {
            launch.env("ACYCLIC_HARNESS_BINDING", binding);
        }
        if host == "codex" {
            // Codex gives MCP a small standard environment plus the manifest's
            // explicit env_vars. Hooks inherit the calling host's environment.
            launch.env("HOME", directory.parent().ok_or("MCP home missing")?);
            let overrides: Vec<_> = launch
                .as_std()
                .get_envs()
                .map(|(key, value)| (key.to_owned(), value.map(std::ffi::OsStr::to_owned)))
                .collect();
            launch.env_clear();
            let manifest: Value = serde_json::from_str(include_str!("../mcp.json"))?;
            let declared = manifest["mcpServers"]["acyclic"]["env_vars"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str);
            for name in [
                "HOME",
                "PATH",
                "USERPROFILE",
                "SYSTEMROOT",
                "COMSPEC",
                "PATHEXT",
                "TEMP",
                "TMP",
            ]
            .into_iter()
            .chain(declared)
            {
                let value = match overrides.iter().find(|(key, _)| key == name) {
                    Some((_, value)) => value.clone(),
                    None => std::env::var_os(name),
                };
                if let Some(value) = value {
                    launch.env(name, value);
                }
            }
        }
        let mut child: Child = launch.arg("__tools").spawn()?;
        eprintln!(
            "{host} MCP {:?} starting (explicit binding: {})",
            child.id(),
            binding.is_some()
        );
        let mut input = child.stdin.take().ok_or("MCP stdin missing")?;
        let mut output = BufReader::new(child.stdout.take().ok_or("MCP stdout missing")?).lines();
        let write = |value: Value| format!("{value}\n").into_bytes();
        input.write_all(&write(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
        "protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"native-probe","version":"1"}}}))).await?;
        let initialized: Value = serde_json::from_str(
            &output
                .next_line()
                .await?
                .ok_or("MCP initialization ended")?,
        )?;
        assert_eq!(initialized["id"], 1);
        assert!(initialized.get("error").is_none(), "{initialized}");
        eprintln!("{host} MCP {:?} initialized", child.id());
        input
            .write_all(&write(
                json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            ))
            .await?;
        Ok(Self {
            child,
            input,
            output,
            next_id: 2,
        })
    }

    async fn result(&mut self, arguments: Value) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        eprintln!("MCP {:?} request {id} started", self.child.id());
        self.input.write_all(format!("{}\n", json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"write","arguments":arguments}})).as_bytes()).await?;
        let response: Value =
            serde_json::from_str(&self.output.next_line().await?.ok_or("MCP result ended")?)?;
        assert_eq!(response["id"], id);
        assert!(response.get("error").is_none(), "{response}");
        eprintln!("MCP {:?} request {id} answered", self.child.id());
        Ok(response["result"].clone())
    }

    async fn close(mut self) -> Result {
        let id = self.child.id();
        eprintln!("MCP {id:?} closing stdin");
        drop(self.input);
        assert!(self.child.wait().await?.success());
        eprintln!("MCP {id:?} exited");
        Ok(())
    }
}

#[tokio::test]
async fn native_hooks_publish_once_and_mcp_only_returns_the_stored_result() -> Result {
    let started = std::time::Instant::now();
    let fixture = Fixture::new()?;
    let result = std::panic::AssertUnwindSafe(async {
        // The final authenticated SessionStart after immediate restart is the
        // readiness boundary, not a delay or an optimistic process-spawn time.
        tokio::time::timeout(
            std::time::Duration::from_secs(8),
            parallel::overlapping_hook_processes_publish_two_then_eight_writes(&fixture),
        )
        .await??;
        eprintln!(
            "restarted owner authenticated and ready after {:?}",
            started.elapsed()
        );
        tokio::time::timeout(std::time::Duration::from_secs(8), native_cases(&fixture)).await?
    })
    .catch_unwind()
    .await;
    // Even a failed assertion or deadline must settle this user's providers
    // before removing any of its checkouts. Report readiness and cleanup too.
    let stopped = fixture.close().await;
    if let Err(error) = &stopped {
        eprintln!("native fixture shutdown: {error}");
    }
    match result {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    stopped?;
    eprintln!(
        "native suite including readiness and shutdown: {:?}",
        started.elapsed()
    );
    Ok(())
}

async fn native_cases(fixture: &Fixture) -> Result {
    let root = fixture.path().parent().unwrap();
    let address = root.join("owner/address.json");
    let instance: Value = serde_json::from_slice(&std::fs::read(&address)?)?;
    let mut projects = Vec::new();
    for host in ["codex", "claude-code", "pi"] {
        let project = root.join(host);
        std::fs::create_dir(&project)?;
        projects.push((host, project));
    }
    let shell = root.join("shell");
    std::fs::create_dir(&shell)?;
    let (results, shell_result) = futures::join!(
        futures::future::join_all(projects.iter().map(|(host, project)| async move {
            let started = std::time::Instant::now();
            let result = host_result_client(project, host).await;
            eprintln!("{host} hook/MCP case: {:?}", started.elapsed());
            result
        })),
        commands::native_shell_and_git_commands_use_the_fenced_owner(&shell)
    );
    let retained: Value = serde_json::from_slice(&std::fs::read(&address)?)?;
    assert_eq!(retained["instance"], instance["instance"]);
    let registrations = std::fs::read_dir(root.join("owner/workspaces"))?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
    let second = root.join("second");
    let expected = projects
        .iter()
        .map(|(_, project)| project.as_path())
        .chain([fixture.path(), second.as_path(), shell.as_path()]);
    assert_eq!(registrations.len(), projects.len() + 3);
    for project in expected {
        let canonical = std::fs::canonicalize(project)?;
        let principal = blake3::hash(canonical.as_os_str().as_encoded_bytes()).to_hex();
        assert!(registrations.contains(&std::ffi::OsString::from(format!("{principal}.json"))));
    }
    for result in results {
        result?;
    }
    shell_result
}

async fn host_result_client(directory: &Path, host: &str) -> Result {
    let session = uuid::Uuid::new_v4().to_string();
    let cwd = directory.to_str().ok_or("non-UTF8 test directory")?;
    let pi = host == "pi";
    hook(
        directory,
        host,
        if pi { "session_start" } else { "SessionStart" },
        json!({"session_id":session,"cwd":cwd}),
    )
    .await?;
    eprintln!("{host} session attached");
    let arguments = json!({"path":"native.txt","text":"one harness write","media_type":"text/plain","display_name":"native.txt"});
    let request = json!({"session_id":session,"cwd":cwd,"tool_use_id":"call","tool_name":if pi {"write"} else {"mcp__acyclic__write"},"tool_input":arguments});
    let answer = hook(
        directory,
        host,
        if pi { "tool_call" } else { "PreToolUse" },
        request.clone(),
    )
    .await?;
    eprintln!("{host} original write settled");
    assert_eq!(
        std::fs::read_to_string(directory.join("native.txt"))?,
        "one harness write"
    );
    let published = std::fs::metadata(directory.join("native.txt"))?.modified()?;
    if pi {
        assert_eq!(answer["isError"], false);
        assert!(answer["details"]["file"].is_object());
    } else {
        assert_eq!(answer["hookSpecificOutput"]["permissionDecision"], "allow");
        let arguments = answer["hookSpecificOutput"]["updatedInput"].clone();
        assert!(arguments["_acyclic"].is_string());
        // Standard plugins start MCP at the package root, independent
        // of the user's selected checkout. The file names its binding.
        let mcp = directory.parent().unwrap().join(format!("mcp-{host}"));
        std::fs::create_dir(&mcp)?;
        let binding = directory
            .parent()
            .unwrap()
            .join(format!("binding-{host}.json"));
        std::fs::write(
            &binding,
            serde_json::to_vec(&json!({"provider":"local","workspace":directory}))?,
        )?;
        let mut original = None;
        for binding in [None, Some(binding.as_path())] {
            let mut results = ResultClient::open(&mcp, host, binding).await?;
            let first = results.result(arguments.clone()).await?;
            assert!(first["structuredContent"]["file"].is_object(), "{first}");
            if let Some(original) = &original {
                assert_eq!(&first, original);
            }
            assert_eq!(results.result(arguments.clone()).await?, first);
            let mut changed = arguments.clone();
            changed["text"] = json!("forged");
            assert_eq!(results.result(changed).await?["isError"], true);
            if let Some(binding) = binding {
                std::fs::write(
                    binding,
                    serde_json::to_vec(&json!({"provider":"local","workspace":mcp}))?,
                )?;
                assert_eq!(results.result(arguments.clone()).await?["isError"], true);
            }
            results.close().await?;
            original = Some(first);
        }
    }
    assert_eq!(
        hook(
            directory,
            host,
            if pi { "tool_call" } else { "PreToolUse" },
            request
        )
        .await?,
        answer
    );
    eprintln!("{host} retry settled");
    assert_eq!(
        std::fs::metadata(directory.join("native.txt"))?.modified()?,
        published
    );
    hook(
        directory,
        host,
        if pi { "session_shutdown" } else { "SessionEnd" },
        json!({"session_id":session,"cwd":cwd}),
    )
    .await?;
    eprintln!("{host} session detached");
    Ok(())
}
