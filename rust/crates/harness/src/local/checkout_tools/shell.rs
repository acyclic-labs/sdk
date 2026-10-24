//! Task shell execution through the shared native process capture engine.
use super::*;
use crate::filesystem::native_process::{self, Invocation};

impl CheckoutTools {
    pub(super) async fn shell(
        &self,
        input: crate::tools::ShellInput,
        maximum_output: usize,
        state: OwnedMutexGuard<State>,
        files: output::Output,
        claims: workspace::volumes::Claims,
    ) -> Result<(
        Result<ToolResult>,
        OwnedMutexGuard<State>,
        output::Output,
        workspace::volumes::Claims,
    )> {
        let workspace = state.workspace.as_ref().map_or_else(
            || Arc::clone(&self.workspace),
            |(_, workspace)| workspace.clone(),
        );
        let invocation = Invocation {
            executable: executable()?.to_path_buf(),
            argv: Vec::new(),
            cwd: workspace.path.clone(),
            environment: Default::default(),
            inherit_environment: false,
            timeout_ms: 300_000,
            control_timeout_ms: output::CONTROL_TIMEOUT_MS,
            cancellation_poll_ms: 25,
            maximum_output_bytes: maximum_output,
            standard_input: None,
            mcp_stdio: None,
            process_custody: workspace.processes.clone(),
        };
        let invocation = native_process::shell::prepare(
            input,
            &state.mounts,
            invocation,
            self.clock.now_unix_millis() / 1000,
        )?;
        let check_owner = Arc::clone(&files.owner);
        let runtime = tokio::runtime::Handle::current();
        let (output, (state, files, claims)) = native_process::capture::capture_with_custody(
            invocation,
            |(_, files, _): &mut (
                OwnedMutexGuard<State>,
                output::Output,
                workspace::volumes::Claims,
            ),
             chunk| files.observe(chunk),
            move || {
                if acyclic_fs::capture_root_identity(&workspace.path)
                    .map_err(std::io::Error::other)?
                    != workspace.identity
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "checkout root was replaced",
                    ));
                }
                // Completes when the journal confirms the fence or reports
                // it lost; no wall-clock deadline applies.
                output::await_fence(&runtime, || async {
                    check_owner
                        .verify(false, &mut None)
                        .await
                        .map_err(native_process::capture::control_error)
                })
            },
            (state, files, claims),
        )
        .await?;
        let result = output
            .ok_or_else(|| Error::Conflict("process capture was interrupted".into()))
            .map(|output| ToolResult {
                value: serde_json::json!({
                    "success": output.success, "exit_code": output.exit_code,
                    "stdout": String::from_utf8_lossy(&output.stdout),
                    "stderr": String::from_utf8_lossy(&output.stderr),
                    "stop": output.stop,
                }),
            });
        Ok((result, state, files, claims))
    }
}

// Pin the native executable at startup; later PATH changes cannot retarget calls.
static EXECUTABLE: std::sync::OnceLock<std::result::Result<std::path::PathBuf, String>> =
    std::sync::OnceLock::new();

pub(in crate::local) fn executable() -> Result<&'static std::path::Path> {
    EXECUTABLE
        .get_or_init(|| resolve(lookup))
        .as_deref()
        .map_err(|cause| Error::Invalid(cause.clone()))
}

#[cfg(not(windows))]
fn lookup(name: &str) -> Option<std::path::PathBuf> {
    which::which(name).ok()
}

#[cfg(windows)]
fn lookup(name: &str) -> Option<std::path::PathBuf> {
    direct_candidate(which::which_all(name).ok()?)
}

#[cfg(windows)]
fn direct_candidate(
    candidates: impl IntoIterator<Item = std::path::PathBuf>,
) -> Option<std::path::PathBuf> {
    candidates
        .into_iter()
        .find_map(|candidate| acyclic_native_runtime::resolve_native_executable(&candidate).ok())
}

fn resolve(
    mut lookup: impl FnMut(&str) -> Option<std::path::PathBuf>,
) -> std::result::Result<std::path::PathBuf, String> {
    crate::tools::shell::PLATFORM
        .executables
        .iter()
        .find_map(|name| lookup(name))
        .ok_or_else(|| {
            format!(
                "native {} executable is unavailable",
                crate::tools::shell::PLATFORM.name
            )
        })
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    #[test]
    fn selection_requires_a_direct_image_and_considers_later_path_candidates() {
        let root = tempfile::tempdir().expect("shell candidates");
        let alias = root.path().join("activation.exe");
        std::fs::write(&alias, []).expect("activation placeholder");
        let native = std::env::current_exe().expect("direct test image");
        let candidates = [alias.clone(), native.clone()];
        // The old first-existing selection admits the non-image placeholder.
        assert_eq!(candidates.first(), Some(&alias));
        assert_eq!(
            direct_candidate(candidates),
            Some(native.canonicalize().unwrap())
        );
        assert_eq!(direct_candidate([alias]), None);
    }

    #[test]
    fn shell_fallback_requires_an_admitted_image_for_each_shell_name() {
        let root = tempfile::tempdir().expect("shell candidates");
        let invalid = root.path().join("activation.exe");
        std::fs::write(&invalid, []).expect("activation placeholder");
        let native = std::env::current_exe().expect("direct image");
        let canonical = native.canonicalize().expect("physical image");
        let choices = [vec![], vec![invalid.clone()], vec![invalid, native]];
        for first in 0..choices.len() {
            for second in 0..choices.len() {
                let mut calls = Vec::new();
                let result = resolve(|name| {
                    calls.push(name.to_owned());
                    let index = if name == "pwsh.exe" { first } else { second };
                    direct_candidate(choices[index].clone())
                });
                assert_eq!(
                    result.ok(),
                    (first == 2 || second == 2).then(|| canonical.clone())
                );
                assert_eq!(
                    calls,
                    if first == 2 {
                        vec!["pwsh.exe"]
                    } else {
                        vec!["pwsh.exe", "powershell.exe"]
                    }
                );
            }
        }
    }

    #[test]
    fn selection_prefers_pwsh_then_windows_powershell_and_never_bash() {
        for (available, expected) in [
            (
                vec!["pwsh.exe", "powershell.exe", "bash.exe"],
                Some("pwsh.exe"),
            ),
            (vec!["powershell.exe", "bash.exe"], Some("powershell.exe")),
            (vec!["bash.exe"], None),
        ] {
            let selected = resolve(|name| available.contains(&name).then(|| name.into()));
            assert_eq!(selected.ok(), expected.map(std::path::PathBuf::from));
        }
    }
}
