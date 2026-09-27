//! Black-box fork/join conformance on live native mounts.
//!
//! Every assertion goes through the public surface only: host hook events,
//! `acyclic git merge`, `acyclic discard`, `acyclic agents --json`, and host file
//! operations on the mounts the engine hands out. Nothing here knows how the
//! filesystem layer is built, so these hold for whatever implements it.
//!
//! Run with `cargo test -p acyclic-plugin --test fork_join -- --ignored
//! --test-threads=1 --skip support::` (the shared support module carries
//! ignored tests of its own).

#![allow(clippy::expect_used, clippy::panic)]

#[allow(dead_code, unused_imports)]
mod support;

use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};
use support::{ACYCLIC, ServiceGuard, command, isolated_state, output_with_stdin, test_tempdir};

/// The host protocol these scenarios drive; its hook contract matches every
/// host that forks subagents.
const HOST: &str = "claude-code";

/// One session over a fresh repository, with its own isolated service state.
struct Session {
    home: tempfile::TempDir,
    repo: PathBuf,
    id: String,
    service: Option<ServiceGuard>,
}

/// A workspace the engine manages: the repository root, or a forked child.
#[derive(Clone)]
struct Workspace {
    path: PathBuf,
    agent: Option<String>,
    parent: Option<PathBuf>,
}

impl Workspace {
    fn reference(&self) -> String {
        format!(
            "agents/{}",
            self.agent.as_deref().expect("a child workspace")
        )
    }
}

impl Session {
    fn open(name: &str) -> Self {
        let home = test_tempdir("fork-join-");
        let repo = home.path().join("repo");
        fs::create_dir_all(repo.join("pkg")).expect("repository");
        fs::write(repo.join("README.md"), "base\n").expect("readme");
        fs::write(repo.join("pkg/shared.rs"), "VALUE = 1\n").expect("shared");
        let initialized = std::process::Command::new("git")
            .args(["init", "-q"])
            .current_dir(&repo)
            .status()
            .expect("git init");
        assert!(initialized.success());
        let service = ServiceGuard::new(home.path());
        let session = Self {
            home,
            repo,
            id: format!("fork-join-{name}"),
            service: Some(service),
        };
        session.hook(
            "SessionStart",
            &json!({"session_id": session.id, "cwd": session.repo}),
            &session.repo,
        );
        session.hook(
            "UserPromptSubmit",
            &json!({
                "session_id": session.id,
                "cwd": session.repo,
                "turn_id": format!("{}:root", session.id),
            }),
            &session.repo,
        );
        session
    }

    fn root(&self) -> Workspace {
        Workspace {
            path: self.repo.clone(),
            agent: None,
            parent: None,
        }
    }

    fn run(&self, arguments: &[&str], cwd: &Path, input: &[u8]) -> (bool, String) {
        let mut run = command(ACYCLIC);
        run.args(arguments).current_dir(cwd);
        isolated_state(&mut run, self.home.path());
        let output = output_with_stdin(&mut run, input);
        let text = if output.status.success() {
            String::from_utf8_lossy(&output.stdout).into_owned()
        } else {
            String::from_utf8_lossy(&output.stderr).into_owned()
        };
        (output.status.success(), text)
    }

    fn hook(&self, event: &str, payload: &Value, cwd: &Path) -> Value {
        let input = serde_json::to_vec(payload).expect("hook payload");
        let (ok, text) = self.run(&["__hook", HOST, event], cwd, &input);
        assert!(ok, "{event} hook failed: {text}");
        if text.trim().is_empty() {
            json!({})
        } else {
            serde_json::from_str(&text).expect("hook output is JSON")
        }
    }

    fn cli_json(&self, arguments: &[&str], cwd: &Path) -> Value {
        let (ok, text) = self.run(arguments, cwd, b"");
        assert!(ok, "acyclic {} failed: {text}", arguments.join(" "));
        serde_json::from_str(&text).expect("command output is JSON")
    }

    fn agents(&self) -> Vec<Value> {
        self.cli_json(&["agents", "--json"], &self.repo)
            .get("agents")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    }

    fn spawn(&self, parent: &Workspace, name: &str) -> Workspace {
        let agent = format!("{name}-agent");
        let mut spawn = json!({
            "session_id": self.id,
            "cwd": parent.path,
            "tool_name": "Agent",
            "tool_use_id": format!("spawn-{agent}"),
            "tool_input": {},
        });
        if let (Some(caller), Some(fields)) = (&parent.agent, spawn.as_object_mut()) {
            fields.insert("agent_id".to_owned(), json!(caller));
        }
        self.hook("PreToolUse", &spawn, &parent.path);
        self.hook(
            "SubagentStart",
            &json!({"session_id": self.id, "cwd": parent.path, "agent_id": agent, "agent_type": "sdk"}),
            &parent.path,
        );
        let reference = format!("agents/{agent}");
        let mount = self
            .agents()
            .into_iter()
            .find(|entry| entry.get("ref").and_then(Value::as_str) == Some(reference.as_str()))
            .and_then(|entry| {
                entry
                    .get("mount")
                    .and_then(Value::as_str)
                    .map(PathBuf::from)
            })
            .unwrap_or_else(|| panic!("{reference} reported no mount"));
        Workspace {
            path: mount,
            agent: Some(agent),
            parent: Some(parent.path.clone()),
        }
    }

    /// Stops a child from its parent's directory: a hook whose cwd is inside
    /// the child's own mount would hold it busy while it unmounts.
    fn stop(&self, child: &Workspace) {
        let parent = child.parent.as_deref().expect("a child workspace");
        self.hook(
            "SubagentStop",
            &json!({"session_id": self.id, "cwd": parent, "agent_id": child.agent}),
            parent,
        );
    }

    /// Stops and merges `child` into its parent; on a conflict, aborts the
    /// pending merge and returns the conflicted paths.
    fn merge(&self, child: &Workspace) -> Result<(), Vec<String>> {
        let parent = child.parent.as_deref().expect("a child workspace");
        self.stop(child);
        let result = self.cli_json(&["git", "merge", &child.reference()], parent);
        match result.get("status").and_then(Value::as_str) {
            Some("applied" | "no-changes") => Ok(()),
            Some("conflicted") => {
                let paths = result
                    .get("conflicts")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|conflict| conflict.get("path").and_then(Value::as_str))
                    .map(str::to_owned)
                    .collect();
                self.cli_json(&["git", "merge", "--abort"], parent);
                Err(paths)
            }
            _ => panic!("merging {} returned {result}", child.reference()),
        }
    }

    fn discard(&self, child: &Workspace) {
        let parent = child.parent.as_deref().expect("a child workspace");
        self.stop(child);
        let (ok, text) = self.run(&["discard", &child.reference()], parent, b"");
        assert!(ok, "discarding {} failed: {text}", child.reference());
    }

    fn files(&self) -> Vec<String> {
        files(&self.repo)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let end = serde_json::to_vec(&json!({"session_id": self.id, "cwd": self.repo}))
            .expect("session end");
        let _ = self.run(&["__hook", HOST, "SessionEnd"], &self.repo, &end);
        if let Some(service) = self.service.take()
            && !std::thread::panicking()
        {
            service.drain();
        }
    }
}

/// Every regular file under `root`, outside `.git`, as sorted relative paths.
fn files(root: &Path) -> Vec<String> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).expect("list directory") {
            let entry = entry.expect("directory entry");
            let path = entry.path();
            if entry.file_name() == ".git" {
                continue;
            }
            if entry.file_type().expect("entry type").is_dir() {
                pending.push(path);
            } else {
                found.push(
                    path.strip_prefix(root)
                        .expect("under root")
                        .to_string_lossy()
                        .replace('\\', "/"),
                );
            }
        }
    }
    found.sort();
    found
}

fn read(path: &Path) -> String {
    fs::read_to_string(path).expect("read file")
}

fn sorted_names(directory: &Path) -> Vec<String> {
    let mut names = fs::read_dir(directory)
        .expect("list directory")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[test]
#[ignore = "requires live native mounts"]
fn posix_operations_inside_a_fork_reach_the_parent_on_merge() {
    let session = Session::open("posix");
    let fork = session.spawn(&session.root(), "posix");
    fs::create_dir_all(fork.path.join("new/deeper")).expect("mkdir");
    fs::write(fork.path.join("new/deeper/a.rs"), "A = 1\n").expect("write");
    assert_eq!(sorted_names(&fork.path.join("new/deeper")), ["a.rs"]);
    fs::rename(
        fork.path.join("new/deeper/a.rs"),
        fork.path.join("new/b.rs"),
    )
    .expect("rename");
    assert_eq!(sorted_names(&fork.path.join("new")), ["b.rs", "deeper"]);
    fs::remove_dir(fork.path.join("new/deeper")).expect("rmdir");
    fs::remove_file(fork.path.join("README.md")).expect("unlink");
    fs::write(fork.path.join("pkg/shared.rs"), "VALUE = 2\n").expect("edit");
    assert_eq!(read(&fork.path.join("pkg/shared.rs")), "VALUE = 2\n");
    assert!(
        !session.repo.join("new").exists(),
        "a fork's writes must not reach its parent before merge"
    );
    session.merge(&fork).expect("merge");
    assert_eq!(session.files(), ["new/b.rs", "pkg/shared.rs"]);
    assert_eq!(read(&session.repo.join("pkg/shared.rs")), "VALUE = 2\n");
}

#[test]
#[ignore = "requires live native mounts"]
fn renaming_a_source_file_moves_it_in_the_parent() {
    let session = Session::open("rename");
    let fork = session.spawn(&session.root(), "mover");
    fs::rename(fork.path.join("README.md"), fork.path.join("GUIDE.md")).expect("rename");
    assert_eq!(sorted_names(&fork.path), ["GUIDE.md", "pkg"]);
    session.merge(&fork).expect("merge");
    assert_eq!(session.files(), ["GUIDE.md", "pkg/shared.rs"]);
    assert_eq!(read(&session.repo.join("GUIDE.md")), "base\n");
}

#[test]
#[ignore = "requires live native mounts"]
fn a_deleted_directory_keeps_what_the_parent_changed_in_it() {
    let session = Session::open("directory-deletion");
    let fork = session.spawn(&session.root(), "deleter");
    fs::remove_dir_all(fork.path.join("pkg")).expect("delete directory");
    // An edit in place leaves the directory itself as the child saw it.
    fs::write(session.repo.join("pkg/shared.rs"), "VALUE = 9\n").expect("parent edit");
    session.merge(&fork).expect("merge");
    assert_eq!(session.files(), ["README.md", "pkg/shared.rs"]);
    assert_eq!(read(&session.repo.join("pkg/shared.rs")), "VALUE = 9\n");
}

#[test]
#[ignore = "requires live native mounts"]
fn a_deletion_travels_up_one_parent_at_a_time() {
    let session = Session::open("deletion");
    let child = session.spawn(&session.root(), "child");
    let grandchild = session.spawn(&child, "grandchild");
    fs::remove_file(grandchild.path.join("pkg/shared.rs")).expect("unlink");
    session.merge(&grandchild).expect("merge grandchild");
    assert!(!child.path.join("pkg/shared.rs").exists());
    assert!(session.repo.join("pkg/shared.rs").exists());
    session.merge(&child).expect("merge child");
    assert_eq!(session.files(), ["README.md"]);
}

#[test]
#[ignore = "requires live native mounts"]
fn a_deletion_does_not_discard_a_siblings_newer_edit() {
    let session = Session::open("deletion-race");
    let editor = session.spawn(&session.root(), "editor");
    let deleter = session.spawn(&session.root(), "deleter");
    fs::write(editor.path.join("README.md"), "edited\n").expect("edit");
    fs::remove_file(deleter.path.join("README.md")).expect("unlink");
    session.merge(&editor).expect("merge editor");
    session.merge(&deleter).expect("merge deleter");
    assert_eq!(read(&session.repo.join("README.md")), "edited\n");
}

#[test]
#[ignore = "requires live native mounts"]
fn parallel_siblings_adding_to_one_directory_both_land() {
    let session = Session::open("siblings");
    let a = session.spawn(&session.root(), "a");
    let b = session.spawn(&session.root(), "b");
    fs::write(a.path.join("pkg/a.rs"), "a\n").expect("write a");
    fs::write(b.path.join("pkg/b.rs"), "b\n").expect("write b");
    session.merge(&a).expect("merge a");
    session.merge(&b).expect("merge b");
    assert_eq!(
        session.files(),
        ["README.md", "pkg/a.rs", "pkg/b.rs", "pkg/shared.rs"]
    );
}

#[test]
#[ignore = "requires live native mounts"]
fn a_sibling_edit_is_not_reverted_by_a_fork_that_never_touched_the_file() {
    let session = Session::open("bystander");
    let editor = session.spawn(&session.root(), "editor");
    let bystander = session.spawn(&session.root(), "bystander");
    fs::write(editor.path.join("pkg/shared.rs"), "VALUE = 2\n").expect("edit");
    fs::write(bystander.path.join("other.rs"), "x\n").expect("write");
    session.merge(&editor).expect("merge editor");
    session.merge(&bystander).expect("merge bystander");
    assert_eq!(read(&session.repo.join("pkg/shared.rs")), "VALUE = 2\n");
    assert_eq!(session.files(), ["README.md", "other.rs", "pkg/shared.rs"]);
}

#[test]
#[ignore = "requires live native mounts"]
fn grandchildren_merge_into_their_parent_only() {
    let session = Session::open("grandchildren");
    let child = session.spawn(&session.root(), "child");
    let grandchild = session.spawn(&child, "grandchild");
    fs::write(grandchild.path.join("deep.rs"), "deep\n").expect("write");
    session.merge(&grandchild).expect("merge grandchild");
    assert_eq!(read(&child.path.join("deep.rs")), "deep\n");
    assert!(!session.repo.join("deep.rs").exists());
    session.merge(&child).expect("merge child");
    assert_eq!(read(&session.repo.join("deep.rs")), "deep\n");
}

#[test]
#[ignore = "requires live native mounts"]
fn discard_drops_a_whole_subtree() {
    let session = Session::open("discard");
    let child = session.spawn(&session.root(), "doomed");
    let grandchild = session.spawn(&child, "doomed-child");
    fs::write(child.path.join("c.rs"), "c\n").expect("write child");
    fs::write(grandchild.path.join("g.rs"), "g\n").expect("write grandchild");
    session.discard(&child);
    let references = session
        .agents()
        .into_iter()
        .filter_map(|entry| entry.get("ref").and_then(Value::as_str).map(str::to_owned))
        .collect::<Vec<_>>();
    assert!(!references.contains(&child.reference()));
    assert!(!references.contains(&grandchild.reference()));
    assert_eq!(session.files(), ["README.md", "pkg/shared.rs"]);
}

#[test]
#[ignore = "requires live native mounts"]
fn a_conflict_is_reported_aborted_and_does_not_block_later_merges() {
    let session = Session::open("conflict");
    let first = session.spawn(&session.root(), "first");
    let second = session.spawn(&session.root(), "second");
    let third = session.spawn(&session.root(), "third");
    fs::write(first.path.join("README.md"), "from first\n").expect("first");
    fs::write(second.path.join("README.md"), "from second\n").expect("second");
    fs::write(third.path.join("third.rs"), "t\n").expect("third");
    session.merge(&first).expect("merge first");
    let conflicts = session
        .merge(&second)
        .expect_err("the second edit conflicts");
    assert_eq!(conflicts, ["/README.md"]);
    session.discard(&second);
    session.merge(&third).expect("merge third");
    assert_eq!(read(&session.repo.join("README.md")), "from first\n");
    assert_eq!(session.files(), ["README.md", "pkg/shared.rs", "third.rs"]);
}

#[test]
#[ignore = "requires live native mounts"]
fn a_deletion_survives_a_conflict_resolved_with_continue() {
    let session = Session::open("continue");
    let editor = session.spawn(&session.root(), "editor");
    let other = session.spawn(&session.root(), "other");
    fs::write(editor.path.join("pkg/shared.rs"), "VALUE = 2\n").expect("edit");
    fs::write(other.path.join("pkg/shared.rs"), "VALUE = 3\n").expect("conflicting edit");
    fs::remove_file(other.path.join("README.md")).expect("unlink");
    session.merge(&editor).expect("merge editor");
    session.stop(&other);
    let conflicted = session.cli_json(&["git", "merge", &other.reference()], &session.repo);
    assert_eq!(
        conflicted.get("status").and_then(Value::as_str),
        Some("conflicted"),
        "{conflicted}"
    );
    fs::write(session.repo.join("pkg/shared.rs"), "VALUE = 3\n").expect("resolve");
    let (added, text) = session.run(&["git", "add", "pkg/shared.rs"], &session.repo, b"");
    assert!(added, "acyclic git add failed: {text}");
    let continued = session.cli_json(&["git", "merge", "--continue"], &session.repo);
    assert_eq!(
        continued.get("status").and_then(Value::as_str),
        Some("applied"),
        "{continued}"
    );
    assert_eq!(read(&session.repo.join("pkg/shared.rs")), "VALUE = 3\n");
    assert_eq!(session.files(), ["pkg/shared.rs"]);
}

#[test]
#[ignore = "requires live native mounts"]
fn a_fork_keeps_creating_files_after_the_root_changes() {
    let session = Session::open("late");
    let waiting = session.spawn(&session.root(), "waiting");
    for name in ["one", "two"] {
        let fork = session.spawn(&session.root(), name);
        fs::write(fork.path.join(format!("{name}.rs")), format!("{name}\n")).expect("write");
        session.merge(&fork).expect("merge");
    }
    fs::write(waiting.path.join("late.rs"), "late\n").expect("late file");
    fs::create_dir(waiting.path.join("late-dir")).expect("late dir");
    fs::write(waiting.path.join("late-dir/x.rs"), "x\n").expect("late nested");
    session.merge(&waiting).expect("merge waiting");
    assert_eq!(
        session.files(),
        [
            "README.md",
            "late-dir/x.rs",
            "late.rs",
            "one.rs",
            "pkg/shared.rs",
            "two.rs"
        ]
    );
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires live native mounts"]
fn extended_attributes_set_in_a_fork_reach_the_parent() {
    let session = Session::open("xattr");
    let fork = session.spawn(&session.root(), "meta");
    let tagged = fork.path.join("tagged.rs");
    fs::write(&tagged, "x\n").expect("write");
    let set = std::process::Command::new("setfattr")
        .args(["-n", "user.acyclic.test", "-v", "1"])
        .arg(&tagged)
        .status();
    let attribute_set = set.is_ok_and(|status| status.success());
    // Read-only, which denies setting attributes once the mode is applied.
    make_read_only(&tagged);
    session.merge(&fork).expect("merge");
    assert!(session.files().contains(&"tagged.rs".to_owned()));
    assert!(read_only(&session.repo.join("tagged.rs")));
    if attribute_set {
        let read = std::process::Command::new("getfattr")
            .args(["--only-values", "-n", "user.acyclic.test"])
            .arg(session.repo.join("tagged.rs"))
            .output()
            .expect("getfattr");
        assert_eq!(read.stdout, b"1");
    }
}

#[cfg(unix)]
fn make_read_only(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(path, fs::Permissions::from_mode(0o444)).expect("read-only");
}

#[cfg(unix)]
fn read_only(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    fs::metadata(path).expect("metadata").permissions().mode() & 0o777 == 0o444
}

#[cfg(target_os = "macos")]
#[test]
#[ignore = "requires live native mounts"]
fn no_host_metadata_files_are_merged() {
    let session = Session::open("appledouble");
    let fork = session.spawn(&session.root(), "meta");
    let tagged = fork.path.join("tagged.rs");
    fs::write(&tagged, "x\n").expect("write");
    let attribute_set = std::process::Command::new("xattr")
        .args(["-w", "com.acyclic.test", "1"])
        .arg(&tagged)
        .status()
        .is_ok_and(|status| status.success());
    // Read-only, which denies setting attributes once the mode is applied.
    make_read_only(&tagged);
    session.merge(&fork).expect("merge");
    assert!(
        !session.files().iter().any(|path| Path::new(path)
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with("._"))),
        "macOS host metadata files reached the parent"
    );
    assert!(read_only(&session.repo.join("tagged.rs")));
    if attribute_set {
        let read = std::process::Command::new("xattr")
            .args(["-p", "com.acyclic.test"])
            .arg(session.repo.join("tagged.rs"))
            .output()
            .expect("xattr");
        assert_eq!(String::from_utf8_lossy(&read.stdout).trim(), "1");
    }
}
