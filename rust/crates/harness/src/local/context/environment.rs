//! Task-binding environment facts, framed like Codex's `<environment_context>`.
//!
//! Facts are recorded once at admission. Resumed turns use the retained manifest.
use std::path::Path;
mod porcelain;
use porcelain::Summary;
pub(super) fn sdk_status(
    state: &acyclic_fs::GitCompatState,
    tree: acyclic_fs::GitTreeRef,
) -> crate::Result<String> {
    let status = state.status(tree).ok_or_else(|| {
        crate::Error::Invalid(
            "Git status has no current branch; recovery: open a valid agent workspace".into(),
        )
    })?;
    let dirty = match status.dirty {
        acyclic_fs::GitDirtyState::Clean => "clean",
        acyclic_fs::GitDirtyState::Dirty => "modified",
        acyclic_fs::GitDirtyState::Unknown => "unresolved source state",
    };
    Ok(format!("branch {}; {dirty}", status.branch))
}

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
) -> crate::Result<crate::context::EnvironmentFacts> {
    let git = if let Some(cwd) = cwd.filter(|_| physical_git) {
        git_status(cwd).await
    } else {
        None
    };
    let mut facts: crate::context::EnvironmentFacts = crate::native_environment::checkout(
        cwd,
        now_unix_millis,
        model,
        platform,
        Some(git.unwrap_or_else(|| "unavailable for this workspace".into())),
    )
    .into();
    if let (Some(cwd), crate::native_environment::PlatformPolicy::Native) = (cwd, platform) {
        let environment =
            crate::native_environment::tool_environment().map_err(crate::Error::process_io)?;
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

/// One-line summary of `git status`, or `None` outside a work tree.
async fn git_status(cwd: &Path) -> Option<String> {
    git_status_from(cwd, || {
        Some(tokio::process::Command::new(which::which("git").ok()?))
    })
    .await
}

// Git's launcher can be expensive even when there is no repository to inspect.
// Discover markers without starting it; Git still validates any marker found.
struct WorktreeDirectory(std::path::PathBuf);

impl WorktreeDirectory {
    fn discover(cwd: &Path) -> std::io::Result<Option<Self>> {
        let cwd = cwd.canonicalize()?;
        for ancestor in cwd.ancestors() {
            match std::fs::symlink_metadata(ancestor.join(".git")) {
                Ok(_) => return Ok(Some(Self(cwd))),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }
}

async fn git_status_from(
    cwd: &Path,
    command: impl FnOnce() -> Option<tokio::process::Command>,
) -> Option<String> {
    use tokio::io::AsyncReadExt as _;
    let cwd = cwd.to_owned();
    let directory =
        acyclic_native_runtime::spawn_blocking(move || WorktreeDirectory::discover(&cwd))
            .await
            .ok()?
            .ok()??;
    let mut command = command()?;
    for name in REPOSITORY_ENVIRONMENT {
        command.env_remove(name);
    }
    let mut child = command
        .current_dir(directory.0)
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
    let mut stdout = child.stdout.take()?;
    let mut summary = Summary::new();
    let mut chunk = [0_u8; 16 * 1024];
    loop {
        let count = stdout.read(&mut chunk).await.ok()?;
        if count == 0 {
            break;
        }
        summary.push(&chunk[..count]);
    }
    if !child.wait().await.ok()?.success() {
        return None;
    }
    Some(summary.finish())
}

#[cfg(test)]
mod tests {
    use super::porcelain::summarize;
    use super::*;

    #[tokio::test]
    async fn git_environment_outside_worktree_never_starts_a_command() -> std::io::Result<()> {
        let root = tempfile::tempdir()?;
        let started = std::cell::Cell::new(false);
        let status = git_status_from(root.path(), || {
            started.set(true);
            None
        })
        .await;
        assert!(status.is_none());
        assert!(
            !started.get(),
            "non-repository context started a Git command"
        );
        Ok(())
    }

    #[test]
    fn git_environment_admission_requires_a_worktree_marker() -> std::io::Result<()> {
        let root = tempfile::tempdir()?;
        let nested = root.path().join("nested");
        std::fs::create_dir(&nested)?;
        assert!(WorktreeDirectory::discover(&nested)?.is_none());
        let marker = root.path().join(".git");
        std::fs::create_dir(&marker)?;
        let directory = WorktreeDirectory::discover(&nested)?;
        assert_eq!(directory.map(|value| value.0), Some(nested.canonicalize()?));
        std::fs::remove_dir(&marker)?;
        std::fs::write(&marker, "gitdir: /fixture/worktree\n")?;
        assert!(WorktreeDirectory::discover(&nested)?.is_some());
        std::fs::remove_file(marker)?;
        assert!(WorktreeDirectory::discover(&nested)?.is_none());
        Ok(())
    }

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
            git_status_from(bound.path(), || Some(command))
                .await
                .as_deref(),
            Some("branch No commits yet on bound-fixture; 1 untracked")
        );
    }

    #[test]
    fn porcelain_records_summarize_by_state() {
        let porcelain =
            b"## main...origin/main [ahead 1]\0M  staged.rs\0 M edited.rs\0MM both.rs\0R  new.rs\0old.rs\0?? scratch.txt\0UU conflict.rs\0!! ignored\0";
        assert_eq!(
            summarize(porcelain),
            "branch main...origin/main [ahead 1]; 3 staged, 2 modified, 1 untracked, 1 conflicted"
        );
        assert_eq!(summarize(b"## main\0"), "branch main; clean");
        assert_eq!(
            summarize(b"## main\0?? a\0?? b"),
            "branch main; 1 untracked, more changes not counted"
        );
        // A rename's source path is not a change, even when it reads like one.
        assert_eq!(summarize(b"R  new.rs\0?? old\0"), "1 staged");
        assert_eq!(summarize(b"?? old\0"), "1 untracked");
    }
}
