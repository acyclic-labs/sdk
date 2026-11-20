use super::{CheckResult, harness, once};

use harness::native_settings::{NativeSettings, SettingsConfig};
use std::path::{Path, PathBuf};
#[path = "settings/mcp.rs"]
mod mcp;
#[path = "settings/model.rs"]
mod model;
#[path = "settings/selection.rs"]
mod selection;

struct Files {
    _root: tempfile::TempDir,
    home: PathBuf,
    workspace: PathBuf,
    codex: PathBuf,
}

impl Files {
    fn new() -> CheckResult<Self> {
        let root = tempfile::tempdir()?;
        let home = root.path().join("home");
        let workspace = root.path().join("workspace");
        let codex = root.path().join("codex");
        for path in [&home, &workspace, &codex] {
            std::fs::create_dir(path)?;
        }
        Ok(Self {
            _root: root,
            home,
            workspace,
            codex,
        })
    }

    fn source(&self, role: u8) -> PathBuf {
        match role {
            0 => self.home.join(".claude.json"),
            1 => self.codex.join("config.toml"),
            2 => self.workspace.join(".mcp.json"),
            3 => self.workspace.join(".codex/config.toml"),
            4 => self.home.join(".claude/settings.json"),
            5 => self.codex.join("hooks.json"),
            6 => self.workspace.join(".claude/settings.json"),
            7 => self.workspace.join(".claude/settings.local.json"),
            _ => self.workspace.join(".codex/hooks.json"),
        }
    }

    fn load(&self, trusted: bool) -> harness::Result<NativeSettings> {
        self.load_bounded(trusted, 512)
    }

    fn load_bounded(&self, trusted: bool, maximum: u64) -> harness::Result<NativeSettings> {
        self.load_trust(trusted, trusted, maximum)
    }

    fn load_trust(&self, hooks: bool, mcp: bool, maximum: u64) -> harness::Result<NativeSettings> {
        // Use the shipped entry and its original native IO completion channel.
        // No test-only constructor or ambient user configuration is selected.
        futures::executor::block_on(NativeSettings::load(self.config(hooks, mcp, maximum)))
    }

    fn config(&self, hooks: bool, mcp: bool, maximum: u64) -> SettingsConfig {
        SettingsConfig::new(&self.workspace, &self.home)
            .codex_home(&self.codex)
            .trust_project_hooks(hooks)
            .trust_project_mcp(mcp)
            .maximum_file_bytes(Some(maximum))
    }
}

fn native_failure<T>(result: harness::Result<T>, error: std::io::Error) -> CheckResult {
    let harness::Error::Process(expected) = harness::Error::process_io(error) else {
        return Err("native I/O must retain its process classification".into());
    };
    let Err(harness::Error::Process(actual)) = result else {
        return Err("selected file I/O must not be admitted or reclassified".into());
    };
    assert_eq!(
        actual, expected,
        "native I/O must retain its original kind and cause"
    );
    Ok(())
}

fn write(path: &Path, bytes: &[u8]) -> CheckResult {
    std::fs::create_dir_all(path.parent().ok_or("settings path has no parent")?)?;
    std::fs::write(path, bytes)?;
    Ok(())
}

/// Exercises the shared native settings admission contract.
///
/// # Panics
/// Panics if a settings admission invariant fails.
pub fn exercise(bytes: &[u8]) {
    let result = check_generated(bytes);
    assert!(
        result.is_ok(),
        "settings admission check failed: {result:?}"
    );
}

fn check_generated(bytes: &[u8]) -> CheckResult {
    mcp::check(bytes)?;
    model::check(bytes)?;
    selection::inactive(bytes)?;
    #[cfg(not(target_arch = "wasm32"))]
    selection::partial(bytes)?;
    let (&role, document) = bytes.split_first().unwrap_or((&0, &[]));
    let files = Files::new()?;
    let role = role % 9;
    let path = files.source(role);
    write(&path, document)?;
    if matches!(role, 2 | 3 | 6..=8) {
        let untrusted = files.load(false)?;
        assert!(
            untrusted.mcp.server_names().next().is_none() && untrusted.hooks.is_empty(),
            "untrusted project declarations must not enter the snapshot"
        );
    }
    let admitted = files.load(true);
    if document.len() > 512 {
        assert!(
            admitted.is_err(),
            "selected settings exceed their explicit file bound"
        );
    }

    static ADMISSION: std::sync::Once = std::sync::Once::new();
    once(&ADMISSION, check_admission);
    Ok(())
}

fn check_admission() -> CheckResult {
    selection::check()?;
    check_file_errors()?;
    check_hook_sources()?;
    check_reservations()
}

fn check_file_errors() -> CheckResult {
    // A real selected directory makes open or read fail on every platform.
    // Compare the same OS failure, including its original classification/cause.
    for role in 0..9 {
        let files = Files::new()?;
        let path = files.source(role);
        std::fs::create_dir_all(&path)?;
        let error = std::fs::read(&path)
            .err()
            .ok_or("a directory was read as a settings file")?;
        if matches!(role, 2 | 3 | 6..=8) {
            files.load(false)?;
        }
        native_failure(files.load(true), error)?;
        if role == 1 {
            native_failure(
                SettingsConfig::new(&files.workspace, &files.home)
                    .codex_home(&files.codex)
                    .configured_model(),
                std::fs::read(&path)
                    .err()
                    .ok_or("a directory was read as a settings file")?,
            )?;
        }
    }
    let files = Files::new()?;
    std::fs::remove_dir(&files.workspace)?;
    native_failure(
        files.load(false),
        std::fs::canonicalize(&files.workspace)
            .err()
            .ok_or("removed workspace resolved")?,
    )?;
    // A real non-directory parent exercises the OS canonicalization error.
    // Some platforms report absence here; only NotFound may be skipped.
    for (role, project) in [(4, false), (5, false), (3, true), (6, true)] {
        let files = Files::new()?;
        let path = files.source(role);
        let parent = path.parent().ok_or("settings path has no parent")?;
        if role == 5 {
            std::fs::remove_dir(parent)?;
        }
        std::fs::write(parent, b"")?;
        let error = std::fs::canonicalize(&path)
            .err()
            .ok_or("settings resolved under a non-directory parent")?;
        if project {
            files.load(false)?;
        }
        if error.kind() == std::io::ErrorKind::NotFound {
            files.load(true)?;
        } else {
            native_failure(files.load(true), error)?;
        }
    }
    Ok(())
}

fn check_hook_sources() -> CheckResult {
    // Caller-authored settings, through the original file loader and policy.
    // No hook command is executed by these admission checks.
    for role in 0..7 {
        let files = Files::new()?;
        let (path, project) = match role {
            0 => (files.home.join(".claude/settings.json"), false),
            1 => (files.codex.join("hooks.json"), false),
            2 => (files.codex.join("config.toml"), false),
            3 => (files.workspace.join(".claude/settings.json"), true),
            4 => (files.workspace.join(".claude/settings.local.json"), true),
            5 => (files.workspace.join(".codex/hooks.json"), true),
            _ => (files.workspace.join(".codex/config.toml"), true),
        };
        let toml = path
            .extension()
            .is_some_and(|extension| extension == "toml");
        let valid = if toml {
            &b"[[hooks.Stop]]\n[[hooks.Stop.hooks]]\ntype='command'\ncommand='guard'\n"[..]
        } else {
            &br#"{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"guard"}]}]}}"#[..]
        };
        for hooks in [false, true] {
            for mcp in [false, true] {
                write(&path, valid)?;
                let snapshot = files.load_trust(hooks, mcp, 512)?;
                assert_eq!(
                    !snapshot.hooks.is_empty(),
                    !project || hooks,
                    "hook sources must follow their independent trust role"
                );
                assert!(snapshot.mcp.server_names().next().is_none());
                write(
                    &path,
                    if toml {
                        b"hooks=42"
                    } else {
                        br#"{"hooks":42}"#
                    },
                )?;
                assert_eq!(
                    files.load_trust(hooks, mcp, 512).is_err(),
                    !project || hooks,
                    "untrusted hook sections must remain untyped"
                );
                // Invalid syntax must remain unread when its whole source is unselected.
                write(&path, b"{")?;
                assert_eq!(
                    files.load_trust(hooks, mcp, 512).is_err(),
                    !project || hooks || toml && mcp,
                    "only selected source syntax may block admission"
                );
            }
        }
    }
    let files = Files::new()?;
    let path = files.workspace.join(".codex/config.toml");
    for (document, invalid_hooks, invalid_mcp) in [
        (
            "[mcp_servers.project]\ncommand='server'\n[[hooks.PreToolUse]]\nmatcher='['\n[[hooks.PreToolUse.hooks]]\ntype='command'\ncommand='guard'\n",
            true,
            false,
        ),
        (
            "[mcp_servers.project]\ncommand=42\n[[hooks.Stop]]\n[[hooks.Stop.hooks]]\ntype='command'\ncommand='guard'\n",
            false,
            true,
        ),
        (
            "hooks=42\n[mcp_servers.project]\ncommand='server'\n",
            true,
            false,
        ),
    ] {
        write(&path, document.as_bytes())?;
        for hooks in [false, true] {
            for mcp in [false, true] {
                let result = files.load_trust(hooks, mcp, 512);
                if hooks && invalid_hooks || mcp && invalid_mcp {
                    assert!(result.is_err(), "trusted invalid sections must be refused");
                } else {
                    let snapshot = result?;
                    assert_eq!(!snapshot.hooks.is_empty(), hooks);
                    assert_eq!(
                        snapshot.mcp.server_names().collect::<Vec<_>>(),
                        if mcp { vec!["project"] } else { vec![] }
                    );
                }
            }
        }
    }
    let files = Files::new()?;
    write(
        &files.home.join(".claude.json"),
        br#"{"mcpServers":{"selected":{"command":"user","args":["discard"]}}}"#,
    )?;
    write(
        &files.workspace.join(".mcp.json"),
        br#"{"mcpServers":{"selected":{"command":"project"}}}"#,
    )?;
    for (trusted, expected, expected_args) in [
        (false, "user", &["discard"][..]),
        (true, "project", &[][..]),
    ] {
        let snapshot = files.load(trusted)?;
        let mut servers = snapshot.mcp.servers();
        let (name, server) = servers
            .next()
            .ok_or("selected declaration must survive admission")?;
        assert_eq!(name, "selected");
        assert!(servers.next().is_none());
        let harness::mcp::McpTransport::Stdio { command, args, .. } = &server.transport else {
            return Err("native process declaration changed transport".into());
        };
        assert_eq!(
            command, expected,
            "scope selection must retain the original command"
        );
        assert_eq!(
            args, expected_args,
            "scope overrides replace the whole declaration"
        );
    }
    Ok(())
}

fn check_reservations() -> CheckResult {
    // Valid syntax on both sides makes a missing read bound observable.
    for (role, start) in [(0, &b"{}"[..]), (1, &b""[..])] {
        let files = Files::new()?;
        let path = if role == 0 {
            files.home.join(".claude.json")
        } else {
            files.codex.join("config.toml")
        };
        let mut document = start.to_vec();
        document.resize(512, b' ');
        write(&path, &document)?;
        files.load(true)?;
        document.push(b' ');
        write(&path, &document)?;
        assert!(
            files.load(true).is_err(),
            "valid over-bound settings must be refused"
        );
        write(&path, &[0xff])?;
        assert!(
            files.load(true).is_err(),
            "selected invalid UTF-8 must be refused"
        );
        write(&path, start)?;
        assert!(
            files.load_bounded(true, 0).is_err(),
            "a selected zero reservation is invalid"
        );
        files.load_bounded(true, u64::MAX)?;
    }
    Ok(())
}
