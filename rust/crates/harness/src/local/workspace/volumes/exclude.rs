//! The mount point is harness state, never a user change: Git ignores it
//! through the repository's own `info/exclude`, not the user's `.gitignore`.
use std::path::{Path, PathBuf};

const MARKER: &str = "# acyclic harness volume mount point";

/// The `info/exclude` that governs `root`, and `root` relative to the
/// working tree's top, or `None` outside a Git working tree.
fn governing(root: &Path) -> std::io::Result<Option<(PathBuf, PathBuf)>> {
    let mut top = root;
    loop {
        let dot_git = top.join(".git");
        let git_dir = match std::fs::symlink_metadata(&dot_git) {
            Ok(entry) if entry.is_dir() => dot_git,
            Ok(entry) if entry.is_file() => {
                // A linked worktree or submodule: `gitdir: <path>`.
                let text = std::fs::read_to_string(&dot_git)?;
                let Some(path) = text.trim_end().strip_prefix("gitdir: ") else {
                    return Ok(None);
                };
                top.join(path)
            }
            Ok(_) => return Ok(None),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let Some(parent) = top.parent() else {
                    return Ok(None);
                };
                top = parent;
                continue;
            }
            Err(error) => return Err(error),
        };
        // Exclusions live in the common directory shared by all worktrees.
        let common = match std::fs::read_to_string(git_dir.join("commondir")) {
            Ok(path) => git_dir.join(path.trim_end()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => git_dir,
            Err(error) => return Err(error),
        };
        let relative = root
            .strip_prefix(top)
            .unwrap_or(Path::new(""))
            .to_path_buf();
        return Ok(Some((common.join("info").join("exclude"), relative)));
    }
}

mod pattern;

pub(super) fn exclude_mount_paths(root: &Path, paths: &[PathBuf]) -> std::io::Result<()> {
    let Some((exclude, relative)) = governing(root)? else {
        return Ok(());
    };
    let patterns = paths
        .iter()
        .map(|path| {
            let path = path.strip_prefix(root).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "mount exclusion lies outside the checkout",
                )
            })?;
            pattern::pattern(&relative.join(path))
        })
        .collect::<std::io::Result<Vec<_>>>()?;
    let existing = match std::fs::read_to_string(&exclude) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error),
    };
    let mut appended = String::new();
    for pattern in patterns {
        if !existing.lines().any(|line| line == pattern) {
            appended.push_str(MARKER);
            appended.push('\n');
            appended.push_str(&pattern);
            appended.push('\n');
        }
    }
    if appended.is_empty() {
        return Ok(());
    }
    if let Some(parent) = exclude.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if !existing.is_empty() && !existing.ends_with('\n') {
        appended.insert(0, '\n');
    }
    use std::io::Write as _;
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&exclude)?
        .write_all(appended.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn git(root: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("HOME", root)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        String::from_utf8(output.stdout).unwrap()
    }

    #[test]
    fn the_mount_point_is_excluded_once_without_touching_user_ignores() {
        let directory = tempfile::tempdir().unwrap();
        let repository = directory.path().join("repo");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-q"]);
        let exclude = repository.join(".git/info/exclude");
        std::fs::write(&exclude, "user-pattern").unwrap();
        let nested = repository.join("sub dir");
        for root in [&repository, &nested] {
            std::fs::create_dir_all(root.join(".volumes/scratch")).unwrap();
            std::fs::write(root.join(".volumes/scratch/note"), "x").unwrap();
            exclude_mount_paths(root, &[root.join(".volumes/scratch")]).unwrap();
            exclude_mount_paths(root, &[root.join(".volumes/scratch")]).unwrap();
        }
        std::fs::write(nested.join("tracked"), "x").unwrap();
        let text = std::fs::read_to_string(&exclude).unwrap();
        assert_eq!(
            text,
            concat!(
                "user-pattern\n",
                "# acyclic harness volume mount point\n/.volumes/scratch\n",
                "# acyclic harness volume mount point\n/sub\\ dir/.volumes/scratch\n",
            )
        );
        assert!(!repository.join(".gitignore").exists());
        let status = git(
            &repository,
            &["status", "--porcelain", "--untracked-files=all"],
        );
        assert_eq!(status, "?? \"sub dir/tracked\"\n");
        // Negative control: without the exclusion both mount points show.
        std::fs::write(&exclude, "user-pattern\n").unwrap();
        let status = git(
            &repository,
            &["status", "--porcelain", "--untracked-files=all"],
        );
        assert!(status.contains(".volumes/scratch/note"), "{status}");
    }

    #[test]
    fn linked_worktrees_share_the_common_exclude() {
        let directory = tempfile::tempdir().unwrap();
        let repository = directory.path().join("repo");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-q"]);
        git(
            &repository,
            &[
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@t",
                "commit",
                "-q",
                "--allow-empty",
                "-m",
                "c",
            ],
        );
        let linked = directory.path().join("linked");
        git(
            &repository,
            &["worktree", "add", "-q", linked.to_str().unwrap()],
        );
        std::fs::create_dir_all(linked.join(".volumes/scratch")).unwrap();
        std::fs::write(linked.join(".volumes/scratch/note"), "x").unwrap();
        exclude_mount_paths(&linked, &[linked.join(".volumes/scratch")]).unwrap();
        assert!(
            std::fs::read_to_string(repository.join(".git/info/exclude"))
                .unwrap()
                .contains("\n/.volumes/scratch\n")
        );
        assert_eq!(git(&linked, &["status", "--porcelain"]), "");
    }

    #[test]
    fn a_directory_outside_git_is_left_alone() {
        let directory = tempfile::tempdir().unwrap();
        // The temporary root may sit inside a repository on a developer box;
        // only assert that nothing is created inside the directory itself.
        exclude_mount_paths(
            directory.path(),
            &[directory.path().join(".volumes/scratch")],
        )
        .unwrap();
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    fn ignored(root: &Path, path: &Path) -> bool {
        let status = std::process::Command::new("git")
            .args(["check-ignore", "--no-index", "-q", "--"])
            .arg(path)
            .current_dir(root)
            .status()
            .unwrap();
        match status.code() {
            Some(0) => true,
            Some(1) => false,
            other => panic!("git: {other:?}"),
        }
    }

    fn native_component() -> impl Strategy<Value = String> {
        let character = any::<char>().prop_filter("representable native component", |character| {
            if matches!(character, '/' | '\0' | '\r' | '\n') {
                return false;
            }
            #[cfg(windows)]
            if *character < ' ' || "<>:\"\\|?*".contains(*character) {
                return false;
            }
            true
        });
        // Letters exclude dot/device names and Windows trailing-name trimming.
        prop::collection::vec(character, 0..6)
            .prop_map(|middle| format!("a{}z", middle.into_iter().collect::<String>()))
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(16))]
        #[test]
        fn exact_rules_preserve_authored_siblings(
            parts in prop::collection::vec(native_component(), 0..3),
        ) {
            let directory = tempfile::tempdir().unwrap();
            let repository = directory.path();
            git(repository, &["init", "-q"]);
            let mut root = repository.to_path_buf();
            for part in parts { root.push(part); }
            let mount = root.join(".volumes/scratch");
            std::fs::create_dir_all(&mount).unwrap();
            let authored = root.join(".volumes/user-owned");
            std::fs::write(&authored, "keep").unwrap();
            let control = root.join(".volumes/.acyclic-fs-mount-test.lock");
            std::fs::write(&control, "owned").unwrap();
            exclude_mount_paths(&root, &[mount.clone(), control.clone()]).unwrap();
            let mounted = mount.join("data");
            std::fs::write(&mounted, "volume").unwrap();
            prop_assert!(ignored(repository, &mounted));
            prop_assert!(ignored(repository, &control));
            prop_assert!(!ignored(repository, &authored));
            std::fs::remove_file(&mounted).unwrap();
            std::fs::remove_dir(&mount).unwrap();
            prop_assert!(!ignored(repository, &authored));
            // Reinstating the broad rule must fail the sibling oracle.
            let broad = pattern::pattern(&root.strip_prefix(repository).unwrap().join(".volumes")).unwrap();
            std::fs::write(repository.join(".git/info/exclude"), format!("{broad}\n")).unwrap();
            prop_assert!(ignored(repository, &authored));
        }
    }

    #[cfg(unix)]
    #[test]
    fn unrepresentable_paths_leave_existing_exclusions_unchanged() {
        let directory = tempfile::tempdir().unwrap();
        git(directory.path(), &["init", "-q"]);
        let exclude = directory.path().join(".git/info/exclude");
        std::fs::write(&exclude, "user-pattern\n").unwrap();
        for name in ["line\nbreak", "line\rbreak"] {
            let root = directory.path().join(name);
            std::fs::create_dir_all(&root).unwrap();
            let error = exclude_mount_paths(&root, &[root.join(".volumes/scratch")]).unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
            assert_eq!(std::fs::read_to_string(&exclude).unwrap(), "user-pattern\n");
            if name.contains('\n') {
                // Restore the old encoder, which escaped whitespace without
                // rejecting physical line separators. Git must expose the
                // projected data despite that malformed exclusion.
                let mount = root.join(".volumes/scratch");
                std::fs::create_dir_all(&mount).unwrap();
                let projected = mount.join("data");
                std::fs::write(&projected, "volume").unwrap();
                let mut malformed = String::from("/");
                for character in name.chars() {
                    if character.is_whitespace() {
                        malformed.push('\\');
                    }
                    malformed.push(character);
                }
                malformed.push_str("/.volumes/scratch\n");
                std::fs::write(&exclude, malformed).unwrap();
                assert!(!ignored(directory.path(), &projected));
                std::fs::write(&exclude, "user-pattern\n").unwrap();
            }
        }
        use std::os::unix::ffi::OsStrExt as _;
        let root = directory
            .path()
            .join(std::ffi::OsStr::from_bytes(b"non-\xff"));
        std::fs::create_dir(&root).unwrap();
        let error = exclude_mount_paths(&root, &[root.join(".volumes/scratch")]).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
        assert_eq!(std::fs::read_to_string(&exclude).unwrap(), "user-pattern\n");
    }
}
