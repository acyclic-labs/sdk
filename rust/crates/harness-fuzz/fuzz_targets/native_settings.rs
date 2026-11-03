#![no_main]

use acyclic_harness_core::native_settings::{NativeSettings, SettingsConfig};
use std::path::{Path, PathBuf};

struct Files {
    _root: tempfile::TempDir,
    home: PathBuf,
    workspace: PathBuf,
    codex: PathBuf,
}

impl Files {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        let workspace = root.path().join("workspace");
        let codex = root.path().join("codex");
        for path in [&home, &workspace, &codex] {
            std::fs::create_dir(path).unwrap();
        }
        Self {
            _root: root,
            home,
            workspace,
            codex,
        }
    }

    fn load(&self, trusted: bool) -> acyclic_harness_core::Result<NativeSettings> {
        self.load_bounded(trusted, 512)
    }

    fn load_bounded(
        &self,
        trusted: bool,
        maximum: u64,
    ) -> acyclic_harness_core::Result<NativeSettings> {
        self.load_trust(trusted, trusted, maximum)
    }

    fn load_trust(
        &self,
        hooks: bool,
        mcp: bool,
        maximum: u64,
    ) -> acyclic_harness_core::Result<NativeSettings> {
        // Use the shipped entry and its original native IO completion channel.
        // No test-only constructor or ambient user configuration is selected.
        futures::executor::block_on(NativeSettings::load(
            SettingsConfig::new(&self.workspace, &self.home)
                .codex_home(&self.codex)
                .trust_project_hooks(hooks)
                .trust_project_mcp(mcp)
                .maximum_file_bytes(maximum),
        ))
    }
}

fn write(path: &Path, bytes: &[u8]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let (&role, document) = bytes.split_first().unwrap_or((&0, &[]));
    let files = Files::new();
    let role = role % 9;
    let path = match role {
        0 => files.home.join(".claude.json"),
        1 => files.codex.join("config.toml"),
        2 => files.workspace.join(".mcp.json"),
        3 => files.workspace.join(".codex/config.toml"),
        4 => files.home.join(".claude/settings.json"),
        5 => files.codex.join("hooks.json"),
        6 => files.workspace.join(".claude/settings.json"),
        7 => files.workspace.join(".claude/settings.local.json"),
        _ => files.workspace.join(".codex/hooks.json"),
    };
    write(&path, document);
    if matches!(role, 2 | 3 | 6..=8) {
        let untrusted = files
            .load(false)
            .expect("untrusted project bytes must stay unread");
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
    ADMISSION.call_once(|| {
        // Caller-authored settings, through the original file loader and policy.
        // No hook command is executed by these admission checks.
        for role in 0..7 {
            let files = Files::new();
            let (path, project) = match role {
                0 => (files.home.join(".claude/settings.json"), false),
                1 => (files.codex.join("hooks.json"), false),
                2 => (files.codex.join("config.toml"), false),
                3 => (files.workspace.join(".claude/settings.json"), true),
                4 => (files.workspace.join(".claude/settings.local.json"), true),
                5 => (files.workspace.join(".codex/hooks.json"), true),
                _ => (files.workspace.join(".codex/config.toml"), true),
            };
            let toml = path.extension().unwrap() == "toml";
            let valid = if toml {
                &b"[[hooks.Stop]]\n[[hooks.Stop.hooks]]\ntype='command'\ncommand='guard'\n"[..]
            } else {
                &br#"{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"guard"}]}]}}"#[..]
            };
            for hooks in [false, true] {
                for mcp in [false, true] {
                    write(&path, valid);
                    let snapshot = files.load_trust(hooks, mcp, 512).unwrap();
                    assert_eq!(
                        !snapshot.hooks.is_empty(),
                        !project || hooks,
                        "hook sources must follow their independent trust role"
                    );
                    assert!(snapshot.mcp.server_names().next().is_none());
                    write(&path, if toml { b"hooks=42" } else { br#"{"hooks":42}"# });
                    assert_eq!(
                        files.load_trust(hooks, mcp, 512).is_err(),
                        !project || hooks,
                        "untrusted hook sections must remain untyped"
                    );
                    // Invalid syntax must remain unread when its whole source is unselected.
                    write(&path, b"{");
                    assert_eq!(
                        files.load_trust(hooks, mcp, 512).is_err(),
                        !project || hooks || toml && mcp,
                        "only selected source syntax may block admission"
                    );
                }
            }
        }
        let files = Files::new();
        let path = files.workspace.join(".codex/config.toml");
        for (document, invalid_hooks, invalid_mcp) in [
            ("[mcp_servers.project]\ncommand='server'\n[[hooks.PreToolUse]]\nmatcher='['\n[[hooks.PreToolUse.hooks]]\ntype='command'\ncommand='guard'\n", true, false),
            ("[mcp_servers.project]\ncommand=42\n[[hooks.Stop]]\n[[hooks.Stop.hooks]]\ntype='command'\ncommand='guard'\n", false, true),
            ("hooks=42\n[mcp_servers.project]\ncommand='server'\n", true, false),
        ] {
            write(&path, document.as_bytes());
            for hooks in [false, true] {
                for mcp in [false, true] {
                    let result = files.load_trust(hooks, mcp, 512);
                    if hooks && invalid_hooks || mcp && invalid_mcp {
                        assert!(result.is_err(), "trusted invalid sections must be refused");
                    } else {
                        let snapshot = result.expect("untrusted sections must not block admission");
                        assert_eq!(!snapshot.hooks.is_empty(), hooks);
                        assert_eq!(snapshot.mcp.server_names().collect::<Vec<_>>(), if mcp { vec!["project"] } else { vec![] });
                    }
                }
            }
        }
        let files = Files::new();
        write(
            &files.home.join(".claude.json"),
            br#"{"mcpServers":{"selected":{"command":"user","args":["discard"]}}}"#,
        );
        write(
            &files.workspace.join(".mcp.json"),
            br#"{"mcpServers":{"selected":{"command":"project"}}}"#,
        );
        for (trusted, expected, expected_args) in [
            (false, "user", &["discard"][..]),
            (true, "project", &[][..]),
        ] {
            let snapshot = files.load(trusted).unwrap();
            let mut servers = snapshot.mcp.servers();
            let (name, server) = servers
                .next()
                .expect("selected declaration must survive admission");
            assert_eq!(name, "selected");
            assert!(servers.next().is_none());
            let acyclic_harness_core::mcp::McpTransport::Stdio { command, args, .. } =
                &server.transport
            else {
                panic!("native process declaration changed transport");
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
        // Valid syntax on both sides makes a missing read bound observable.
        for (role, start) in [(0, &b"{}"[..]), (1, &b""[..])] {
            let files = Files::new();
            let path = if role == 0 {
                files.home.join(".claude.json")
            } else {
                files.codex.join("config.toml")
            };
            let mut document = start.to_vec();
            document.resize(512, b' ');
            write(&path, &document);
            files
                .load(true)
                .expect("in-bound valid settings must be admitted");
            document.push(b' ');
            write(&path, &document);
            assert!(
                files.load(true).is_err(),
                "valid over-bound settings must be refused"
            );
            write(&path, &[0xff]);
            assert!(
                files.load(true).is_err(),
                "selected invalid UTF-8 must be refused"
            );
            write(&path, start);
            for maximum in [0, u64::MAX] {
                assert!(
                    files.load_bounded(true, maximum).is_err(),
                    "settings bounds must be positive and finite"
                );
            }
        }
    });
});
