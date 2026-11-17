//! Task-binding environment facts, framed like Codex's `<environment_context>`.
//!
//! Facts are recorded once per context epoch. They hold nothing that changes
//! without a file edit (no Git status), so the prefix they render in is stable
//! until compaction.
use std::path::Path;

/// The bound shell's `HOME` and `TMPDIR`: private writable scratch that is
/// never the project (see `workspace_environment`).
const SCRATCH: &str = "$HOME and $TMPDIR are your private writable scratch, outside the project";

/// The bound view's facts at `now_unix_millis`, the epoch's session date.
pub(super) fn gather(
    cwd: Option<&Path>,
    now_unix_millis: u64,
    platform: &crate::native_environment::PlatformPolicy,
) -> crate::Result<crate::context::EnvironmentFacts> {
    // Tools resolve task-relative paths. Keep the host directory only for
    // native executable discovery, never as a path to copy into another task.
    let task_cwd = cwd.map(|_| Path::new("."));
    let mut facts: crate::context::EnvironmentFacts =
        crate::native_environment::checkout(task_cwd, now_unix_millis, platform).into();
    if cwd.is_some() {
        // The seed prompt promises private scratch wherever a bound checkout
        // exists; the fact does not depend on native process discovery.
        facts.scratch = Some(SCRATCH.into());
    }
    if let (Some(cwd), crate::native_environment::PlatformPolicy::Native) = (cwd, platform) {
        let environment =
            crate::native_environment::tool_environment(acyclic_native_runtime::platform_shell)
                .map_err(crate::Error::process_io)?;
        let search = acyclic_native_runtime::ExecutableSearch::from_environment(cwd, |name| {
            environment.get(name).map(std::ffi::OsString::from)
        });
        facts.tool_path = environment.get("PATH").cloned();
        facts.tooling = Some(
            crate::context::TASK_TOOLS
                .iter()
                .copied()
                .chain(["python", "python3", "node", "cargo"])
                .map(|name| {
                    format!(
                        "{name}: {}",
                        if search.command(name).is_ok() {
                            "available"
                        } else {
                            "absent"
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    Ok(facts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn volume_environment_discovers_tools_from_its_admitted_directory() -> crate::Result<()> {
        let directory =
            tempfile::tempdir().map_err(|error| crate::Error::Storage(error.to_string()))?;
        let provider = std::sync::Arc::new(
            crate::test_support::ScriptedModelProvider::new([
                crate::test_support::ScriptedTurn::Text("ENVIRONMENT_REPLY".into()),
            ])
            .with_context_capacity(crate::context::ModelContextCapacity {
                context_tokens: 1_000_000,
                output_tokens: Some(4096),
                output_shares_context: false,
            }),
        );
        let project = crate::local::tests::volume(directory.path()).await?;
        let mut config =
            crate::local::tests::volume_config(directory.path(), project, provider.clone())?;
        config.environment_platform = crate::native_environment::PlatformPolicy::Native;
        let harness = std::sync::Arc::new(crate::local::tests::open_scripted(config).await?);
        harness.agents().await?;
        match crate::test_support::prompt_settled(&harness, "environment", "show environment")
            .await?
        {
            crate::test_support::PromptOutcome::Settled(_, _) => {}
            crate::test_support::PromptOutcome::Failed(message) => {
                return Err(crate::Error::Conflict(format!(
                    "environment prompt failed: {message}"
                )));
            }
        }
        let request = provider
            .request(0)
            .ok_or_else(|| crate::Error::NotFound("model request".into()))?;
        let text = crate::local::tests::request_text(&harness, &request).await?;
        assert!(text.contains("<cwd>.</cwd>"), "{text}");
        assert!(text.contains("git: available"), "{text}");
        // Negative control: a relative directory names no executable search root.
        let relative = gather(
            Some(Path::new(".")),
            0,
            &crate::native_environment::PlatformPolicy::Native,
        )?;
        assert!(
            relative
                .tooling
                .as_deref()
                .is_some_and(|tooling| tooling.contains("git: absent")),
            "{:?}",
            relative.tooling
        );
        Ok(())
    }
}
