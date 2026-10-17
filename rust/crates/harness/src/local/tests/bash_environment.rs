//! Stock Windows bash inherits host settings and overlays task mount hints.
use super::*;

#[tokio::test]
async fn stock_bash_preserves_host_path_and_home_and_mounts_win() -> Result<()> {
    const CHILD: &str = "ACYCLIC_TEST_WINDOWS_BASH_ENVIRONMENT";
    const HOME: &str = "/acyclic-stock-bash-home";
    const TOOL: &str = "acyclic-host-path-only";
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    if std::env::var_os(CHILD).is_none() {
        let bin = directory.path().join("bin");
        std::fs::create_dir(&bin).map_err(super::super::storage)?;
        std::fs::write(
            bin.join(TOOL),
            b"#!/bin/sh\nprintf '%s\\n' 'host PATH tool' \"$HOME\" \"$ACYCLIC_VOLUME_PROJECT\" \"$ACYCLIC_VOLUME_PROJECT_MODE\" \"$ACYCLIC_VOLUME_SCRATCH\" \"$ACYCLIC_VOLUME_SCRATCH_MODE\"\n",
        )
        .map_err(super::super::storage)?;
        let bash = which::which("bash").map_err(super::super::storage)?;
        let path = std::env::join_paths([bin.as_path(), bash.parent().unwrap()])
            .map_err(super::super::storage)?;
        // A child isolates host settings from parallel tests. The private tool
        // exists only in PATH; bash is still located and invoked by stock tools.
        let output = std::process::Command::new(
            std::env::current_exe().map_err(super::super::storage)?,
        )
        .args([
            "local::tests::bash_environment::stock_bash_preserves_host_path_and_home_and_mounts_win",
            "--exact",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .env("PATH", path)
        .env("HOME", HOME)
        .env("ACYCLIC_VOLUME_PROJECT", "stale project")
        .env("ACYCLIC_VOLUME_PROJECT_MODE", "ro")
        .env("ACYCLIC_VOLUME_SCRATCH", "stale scratch")
        .env("ACYCLIC_VOLUME_SCRATCH_MODE", "ro")
        .env_remove("BASH_ENV")
        .output()
        .map_err(super::super::storage)?;
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed;"));
        return Ok(());
    }

    assert_eq!(std::env::var("HOME").unwrap(), HOME);
    assert_eq!(std::env::var("ACYCLIC_VOLUME_SCRATCH_MODE").unwrap(), "ro");
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted([
        super::checked_commands::call(0, "bash", TOOL),
        ScriptedTurn::Text("done".into()),
    ]);
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider.clone()))?
            .state_directory(state.path()),
    )
    .await?;
    let mounts = harness.workspace().unwrap().mounts.environment()?;
    let expected = format!(
        "host PATH tool\n{HOME}\n{}\nrw\n{}\nrw\n",
        mounts["ACYCLIC_VOLUME_PROJECT"], mounts["ACYCLIC_VOLUME_SCRATCH"],
    );
    super::checkout_reopen::run(&harness).await?;
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    let result = requests[1]
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .find_map(|part| match part {
            crate::model::ModelContentPart::ToolResult {
                call_id,
                content: crate::model::ToolResultContent::Json { value },
                ..
            } if call_id == "checkout-0" => Some(value),
            _ => None,
        })
        .expect("stock bash result");
    assert_eq!(result["success"], true, "{result}");
    assert_eq!(result["exit_code"], 0, "{result}");
    assert_eq!(result["stdout"], expected, "{result}");
    assert_eq!(result["stderr"], "", "{result}");
    harness.close().await?;
    Ok(())
}
