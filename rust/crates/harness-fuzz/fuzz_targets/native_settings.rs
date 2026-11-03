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
        // Use the shipped entry and its original native IO completion channel.
        // No test-only constructor or ambient user configuration is selected.
        futures::executor::block_on(NativeSettings::load(
            SettingsConfig::new(&self.workspace, &self.home)
                .codex_home(&self.codex)
                .trust_project_hooks(trusted)
                .trust_project_mcp(trusted)
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
    let role = role % 4;
    let path = match role {
        0 => files.home.join(".claude.json"),
        1 => files.codex.join("config.toml"),
        2 => files.workspace.join(".mcp.json"),
        _ => files.workspace.join(".codex/config.toml"),
    };
    write(&path, document);
    if role >= 2 {
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
