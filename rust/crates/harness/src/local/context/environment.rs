//! Task-binding environment facts, framed like Codex's `<environment_context>`.
//!
//! Facts are recorded once per context epoch. They hold nothing that changes
//! without a file edit (no Git status), so the prefix they render in is stable
//! until compaction.
use std::path::Path;

/// The bound view's facts at `now_unix_millis`, the epoch's session date.
pub(super) fn gather(
    cwd: Option<&Path>,
    now_unix_millis: u64,
    model: &str,
    platform: &crate::native_environment::PlatformPolicy,
) -> crate::Result<crate::context::EnvironmentFacts> {
    // Tools resolve task-relative paths. Keep the host directory only for
    // native executable discovery, never as a path to copy into another task.
    let task_cwd = cwd.map(|_| Path::new("."));
    let mut facts: crate::context::EnvironmentFacts =
        crate::native_environment::checkout(task_cwd, now_unix_millis, model, platform).into();
    if let (Some(cwd), crate::native_environment::PlatformPolicy::Native) = (cwd, platform) {
        let environment =
            crate::native_environment::tool_environment(acyclic_native_runtime::platform_shell)
                .map_err(crate::Error::process_io)?;
        let search = acyclic_native_runtime::ExecutableSearch::from_environment(cwd, |name| {
            environment.get(name).map(std::ffi::OsString::from)
        });
        facts.tool_path = environment.get("PATH").cloned();
        facts.tooling = Some(
            ["git", "rg", "python", "python3", "node", "cargo"]
                .into_iter()
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
