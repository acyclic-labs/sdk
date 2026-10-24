//! Task-binding environment facts, framed like Codex's `<environment_context>`.
//!
//! Facts are recorded once at admission. Resumed turns use the retained manifest.
use std::path::Path;
mod porcelain;
use porcelain::summarize;
use std::time::Duration;

/// Porcelain bytes summarized; larger repositories report a partial count.
const GIT_STATUS_BYTES: u64 = 64 * 1024;
/// A slow `git status` only drops the summary, never delays the turn further.
const GIT_STATUS_TIMEOUT: Duration = Duration::from_secs(2);

// Git's `rev-parse --local-env-vars`, plus repository-discovery controls.
// A caller's repository selection must never replace the task binding.
const REPOSITORY_ENVIRONMENT: &[&str] = &[
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_CONFIG",
    "GIT_CONFIG_PARAMETERS",
    "GIT_CONFIG_COUNT",
    "GIT_OBJECT_DIRECTORY",
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_IMPLICIT_WORK_TREE",
    "GIT_GRAFT_FILE",
    "GIT_INDEX_FILE",
    "GIT_NO_REPLACE_OBJECTS",
    "GIT_REPLACE_REF_BASE",
    "GIT_PREFIX",
    "GIT_SHALLOW_FILE",
    "GIT_COMMON_DIR",
    "GIT_CEILING_DIRECTORIES",
    "GIT_DISCOVERY_ACROSS_FILESYSTEM",
];

/// Resolves only physical Git facts that still describe the task's bound view.
pub(super) async fn gather(
    cwd: Option<&Path>,
    now_unix_millis: u64,
    model: &str,
    platform: &crate::native_environment::PlatformPolicy,
    physical_git: bool,
) -> crate::context::EnvironmentFacts {
    let git = if let Some(cwd) = cwd.filter(|_| physical_git) {
        git_status(cwd).await
    } else {
        None
    };
    crate::native_environment::checkout(cwd, now_unix_millis, model, platform, git).into()
}

/// One-line summary of `git status`, or `None` outside a work tree.
async fn git_status(cwd: &Path) -> Option<String> {
    let command = tokio::process::Command::new(which::which("git").ok()?);
    git_status_from(cwd, command).await
}

async fn git_status_from(cwd: &Path, mut command: tokio::process::Command) -> Option<String> {
    use tokio::io::AsyncReadExt as _;
    for name in REPOSITORY_ENVIRONMENT {
        command.env_remove(name);
    }
    let mut child = command
        .current_dir(cwd)
        .args([
            "status",
            "--porcelain=v1",
            "--branch",
            "-z",
            "--untracked-files=normal",
        ])
        // Read-only: never take the index lock another git process may need.
        .env("GIT_OPTIONAL_LOCKS", "0")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .ok()?;
    let stdout = child.stdout.take()?;
    let collected = tokio::time::timeout(GIT_STATUS_TIMEOUT, async {
        let mut bytes = Vec::new();
        stdout
            .take(GIT_STATUS_BYTES + 1)
            .read_to_end(&mut bytes)
            .await
            .ok()?;
        let truncated = bytes.len() as u64 > GIT_STATUS_BYTES;
        if truncated {
            child.start_kill().ok()?;
            child.wait().await.ok()?;
        } else if !child.wait().await.ok()?.success() {
            return None;
        }
        Some((bytes, truncated))
    })
    .await
    .ok()??;
    Some(summarize(&collected.0, collected.1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn inherited_repository_selection_cannot_replace_the_bound_checkout() {
        let bound = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let git = which::which("git").unwrap();
        for (root, branch) in [
            (bound.path(), "bound-fixture"),
            (other.path(), "other-fixture"),
        ] {
            assert!(
                std::process::Command::new(&git)
                    .current_dir(root)
                    .env("GIT_CONFIG_GLOBAL", root.join("missing-config"))
                    .env("GIT_CONFIG_NOSYSTEM", "1")
                    .args(["init", "--quiet", "--template=", "--initial-branch", branch])
                    .status()
                    .unwrap()
                    .success()
            );
        }
        std::fs::write(bound.path().join("bound.txt"), "bound").unwrap();
        std::fs::write(other.path().join("other.txt"), "other").unwrap();
        std::fs::write(other.path().join("second.txt"), "other").unwrap();
        let mut command = tokio::process::Command::new(git);
        command
            .env("GIT_DIR", other.path().join(".git"))
            .env("GIT_WORK_TREE", other.path())
            .env("GIT_COMMON_DIR", other.path().join(".git"))
            .env("GIT_INDEX_FILE", other.path().join(".git/index"));
        assert_eq!(
            git_status_from(bound.path(), command).await.as_deref(),
            Some("branch No commits yet on bound-fixture; 1 untracked")
        );
    }

    #[test]
    fn porcelain_records_summarize_by_state() {
        let porcelain =
            b"## main...origin/main [ahead 1]\0M  staged.rs\0 M edited.rs\0MM both.rs\0R  new.rs\0old.rs\0?? scratch.txt\0UU conflict.rs\0!! ignored\0";
        assert_eq!(
            summarize(porcelain, false),
            "branch main...origin/main [ahead 1]; 3 staged, 2 modified, 1 untracked, 1 conflicted"
        );
        assert_eq!(summarize(b"## main\0", false), "branch main; clean");
        assert_eq!(
            summarize(b"## main\0?? a\0?? b", true),
            "branch main; 1 untracked, more changes not counted"
        );
        // A rename's source path is not a change, even when it reads like one.
        assert_eq!(summarize(b"R  new.rs\0?? old\0", false), "1 staged");
        assert_eq!(summarize(b"?? old\0", false), "1 untracked");
    }
}
