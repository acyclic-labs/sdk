//! Native PowerShell exercises the stock scripted-provider path on real Windows.
use super::*;

fn call(id: &str, name: &str, arguments: Value) -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
            id.into(),
            name.into(),
            arguments,
        ))),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({}),
        }),
    ])
}

#[tokio::test]
async fn scripted_powershell_reads_writes_runs_and_captures_exit_code() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::create_dir(directory.path().join("src")).map_err(super::super::storage)?;
    std::fs::write(directory.path().join("src/input.txt"), "projected input")
        .map_err(super::super::storage)?;
    let provider = scripted([
        call(
            "write",
            "write",
            json!({"path":"from-write.txt", "text":"file tool input", "media_type":"text/plain", "display_name":"from-write.txt"}),
        ),
        call(
            "shell",
            "powershell",
            json!({"command": "[Console]::Write((Get-Content -Raw input.txt)); [Console]::Write((Get-Content -Raw ../from-write.txt)); [IO.File]::WriteAllText((Join-Path (Get-Location) 'captured.txt'), 'native write'); [IO.File]::WriteAllText((Join-Path $env:ACYCLIC_VOLUME_SCRATCH 'scratch.txt'), 'volume write'); [Console]::Error.Write('diagnostic'); exit 7", "cwd":"src"}),
        ),
        call(
            "scratch",
            "powershell",
            json!({"command":"[Console]::Write((Get-Content -Raw scratch.txt))", "cwd":".volumes/scratch"}),
        ),
        call("diff", "fs", json!({"command":"diff"})),
        ScriptedTurn::Text("done".into()),
    ]);
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider.clone()))?
            .state_directory(state.path()),
    )
    .await?;
    super::checkout_reopen::run(&harness).await?;
    let requests = provider.requests();
    assert_eq!(requests.len(), 5);
    let result = |id: &str| {
        requests
            .last()
            .unwrap()
            .messages
            .iter()
            .flat_map(|message| message.content.parts())
            .find_map(|part| match part {
                crate::model::ModelContentPart::ToolResult {
                    call_id,
                    content: crate::model::ToolResultContent::Json { value },
                    ..
                } if call_id == id => Some(value),
                _ => None,
            })
            .unwrap_or_else(|| panic!("missing result {id}"))
    };
    assert_eq!(result("shell")["success"], false);
    assert_eq!(result("shell")["exit_code"], 7);
    assert_eq!(result("shell")["stdout"], "projected inputfile tool input");
    assert_eq!(result("shell")["stderr"], "diagnostic");
    assert_eq!(result("scratch")["stdout"], "volume write");
    assert_eq!(result("scratch")["exit_code"], 0);
    assert!(
        result("diff").to_string().contains("captured.txt"),
        "{}",
        result("diff")
    );
    // Read the immutable head, so a host-only write cannot satisfy capture.
    let head = harness
        .workspace()
        .unwrap()
        .view()
        .workspace()
        .head(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(super::super::storage)?
        .value;
    let bytes = head
        .read(
            "/src/captured.txt",
            64,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(super::super::storage)?
        .value;
    assert_eq!(bytes.as_ref(), b"native write");
    harness.close().await?;
    Ok(())
}

#[tokio::test]
async fn powershell_cancellation_kills_parent_and_descendant() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let marker = state.path().join("pids");
    let executable = super::super::checkout_tools::shell::executable()?;
    let script = format!(
        "$child = Start-Process -FilePath (Get-Process -Id $PID).Path -ArgumentList @('-NoProfile', '-NonInteractive', '-Command', 'Start-Sleep -Seconds 60') -PassThru; [IO.File]::WriteAllText('{}', \"$PID $($child.Id)\"); Start-Sleep -Seconds 60",
        marker.display().to_string().replace('\'', "''")
    );
    let provider = scripted([
        call(
            "running",
            "powershell",
            json!({"command":script, "timeout":5}),
        ),
        ScriptedTurn::Text("unexpected completion".into()),
    ]);
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider))?
            .state_directory(state.path().join("state")),
    )
    .await?;
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id: task, .. }) = harness
        .submit(OperationId::new(), "cancel the running shell")
        .await?
    else {
        panic!("task admission")
    };
    let cancel = async {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !marker.is_file() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("PowerShell descendant started");
        harness.runtime.task_host().cancel(task).await
    };
    let (run, cancel) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(harness.resume(task), cancel)
    })
    .await
    .expect("cancelled tree drained");
    cancel?;
    assert!(run.is_err(), "cancelled command completed a model turn");
    let pids = std::fs::read_to_string(marker).map_err(super::super::storage)?;
    let ids: Vec<u32> = pids
        .split_whitespace()
        .map(|pid| pid.parse().unwrap())
        .collect();
    assert_eq!(ids.len(), 2);
    let check = format!(
        "if (Get-Process -Id {} -ErrorAction SilentlyContinue) {{ exit 9 }}; exit 0",
        ids.iter().map(u32::to_string).collect::<Vec<_>>().join(",")
    );
    let status = std::process::Command::new(executable)
        .args(crate::tools::shell::PLATFORM.arguments)
        .arg(check)
        .status()
        .map_err(super::super::storage)?;
    assert!(status.success(), "cancelled process survived: {ids:?}");
    harness.close().await?;
    Ok(())
}

#[tokio::test]
async fn powershell_approval_patterns_block_execution() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let home = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::create_dir(home.path().join(".claude")).map_err(super::super::storage)?;
    let executable = super::super::checkout_tools::shell::executable()?;
    let deny = json!({"hookSpecificOutput":{"permissionDecision":"deny", "permissionDecisionReason":"PowerShell policy denied"}});
    let settings = json!({"hooks":{"PreToolUse":[{"matcher":"^(powershell|PowerShell)$", "hooks":[{
        "type":"command", "command":executable,
        "args":["-NoProfile", "-NonInteractive", "-Command", format!("[Console]::Write('{}')", deny)],
        "onFailure":"block"
    }]}]}});
    std::fs::write(
        home.path().join(".claude/settings.json"),
        settings.to_string(),
    )
    .map_err(super::super::storage)?;
    let provider = scripted([
        call(
            "denied",
            "powershell",
            json!({"command":"Set-Content -LiteralPath forbidden.txt -Value executed"}),
        ),
        ScriptedTurn::Text("done".into()),
    ]);
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider.clone()))?
            .state_directory(state.path())
            .settings(
                crate::native_settings::SettingsConfig::new(directory.path(), home.path())
                    .codex_home(home.path().join(".codex")),
            ),
    )
    .await?;
    super::checkout_reopen::run(&harness).await?;
    assert!(!directory.path().join("forbidden.txt").exists());
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        serde_json::to_string(&requests[1])
            .map_err(super::super::storage)?
            .contains("PowerShell policy denied")
    );
    harness.close().await?;
    Ok(())
}
