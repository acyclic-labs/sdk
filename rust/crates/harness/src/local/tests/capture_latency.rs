//! A command publishes external edits and its own edits in one capture.
use super::*;
use acyclic_fs::{CancellationToken, WorkBudget};

#[derive(Debug, Clone, Copy)]
enum Entry {
    Direct,
    Model,
    Hook,
}

#[tokio::test]
async fn direct_shell_uses_one_capture() -> Result<()> {
    exercise(Entry::Direct).await
}

#[tokio::test]
async fn model_shell_uses_one_capture() -> Result<()> {
    exercise(Entry::Model).await
}

#[tokio::test]
async fn configured_hook_uses_one_capture() -> Result<()> {
    exercise(Entry::Hook).await
}

async fn exercise(entry: Entry) -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let command = if cfg!(windows) {
        "[IO.File]::WriteAllText('command.txt', 'command'); [Console]::Write('ready')"
    } else {
        "printf command > command.txt; printf ready"
    };
    let provider = if matches!(entry, Entry::Hook) {
        text()
    } else {
        scripted([
            checked_commands::call(0, crate::tools::shell::PLATFORM.name, command),
            ScriptedTurn::Text("done".into()),
        ])
    };
    let mut config =
        checkout_config(directory.path(), (model()?, provider))?.state_directory(state.path());
    if matches!(entry, Entry::Hook) {
        let home = state.path().join("home");
        std::fs::create_dir_all(home.join(".claude")).map_err(super::super::storage)?;
        let (executable, args) = if cfg!(windows) {
            (
                super::super::checkout_tools::shell::executable()?.to_path_buf(),
                vec![
                    "-NoLogo",
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    command,
                ],
            )
        } else {
            (std::path::PathBuf::from("sh"), vec!["-c", command])
        };
        let settings = json!({"hooks":{"SessionStart":[{"matcher":"startup","hooks":[{
            "type":"command", "command": executable, "args": args, "onFailure":"block"
        }]}]}});
        std::fs::write(home.join(".claude/settings.json"), settings.to_string())
            .map_err(super::super::storage)?;
        config = config.settings(
            crate::native_settings::SettingsConfig::new(directory.path(), &home)
                .codex_home(home.join(".codex")),
        );
    }
    let harness = LocalHarness::open(config).await?;
    let workspace = harness
        .workspace()
        .ok_or_else(|| Error::NotFound("checkout".into()))?;
    let cancellation = CancellationToken::new();
    let before = workspace
        .view()
        .workspace()
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(super::super::storage)?
        .value;
    // An external author edits the physical checkout after its initial cut.
    std::fs::write(directory.path().join("external.txt"), b"external")
        .map_err(super::super::storage)?;
    let started = std::time::Instant::now();
    if matches!(entry, Entry::Direct) {
        let operation = OperationId::new();
        let resolution = harness
            .resolve_input(operation, &format!("/shell {command}"))
            .await?;
        let result = harness
            .dispatch_command(operation, None, resolution)
            .await?;
        assert_eq!(result["exit_code"], 0, "{result}");
        assert_eq!(result["stdout"], "ready");
    } else {
        assert_eq!(harness.run("run command").await?.text, "done");
    }
    let elapsed = started.elapsed();
    let after = workspace
        .view()
        .workspace()
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(super::super::storage)?
        .value;
    let mut generation = after.clone();
    let mut captures = 0;
    while generation.id() != before.id() {
        let parents = generation
            .parents(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(super::super::storage)?
            .value;
        let [parent] = parents.as_slice() else {
            return Err(Error::Conflict("capture must have one parent".into()));
        };
        generation = workspace
            .view()
            .workspace()
            .generation(*parent, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(super::super::storage)?
            .value;
        captures += 1;
        assert!(
            captures <= 4,
            "capture ancestry must reach the original head"
        );
    }
    eprintln!(
        "CAPTURE_COST entry={entry:?} captures={captures} elapsed_us={}",
        elapsed.as_micros()
    );
    for (path, expected) in [
        ("/external.txt", b"external".as_slice()),
        ("/command.txt", b"command"),
    ] {
        let bytes = after
            .read(path, 32, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(super::super::storage)?
            .value;
        assert_eq!(bytes.as_ref(), expected);
    }
    assert_eq!(
        captures, 1,
        "external and command edits must share one post-command capture"
    );
    harness.close().await?;
    Ok(())
}
