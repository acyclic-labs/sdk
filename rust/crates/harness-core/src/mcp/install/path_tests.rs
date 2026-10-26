use super::tests::{Remote, advertised, server};
use super::*;
use crate::mcp::config::Dialect;
use proptest::prelude::*;
use serde_json::json;

#[cfg(windows)]
#[test]
fn both_mcp_dialects_pin_batch_launchers_from_the_configured_search_environment() -> Result<()> {
    let root = tempfile::tempdir().map_err(Error::process_io)?;
    let bin = root.path().join("mcp & % launchers");
    std::fs::create_dir(&bin).map_err(Error::process_io)?;
    for extension in ["cmd", "bat"] {
        let name = format!("mcp-fixture-{extension}");
        let path = bin.join(format!("{name}.{extension}"));
        std::fs::write(&path, "@echo off\r\n@echo mcp-launcher-ran\r\n")
            .map_err(Error::process_io)?;
        for dialect in [Dialect::Codex, Dialect::Claude] {
            let config = server(
                json!({"command":name,"env":{
                    "PATH": bin, "PATHEXT":".EXE;.BAT;.CMD"
                }}),
                dialect,
            )?;
            let mut command = native::command_for(&config, root.path(), |key| {
                (key == "SYSTEMROOT")
                    .then(|| std::env::var_os(key))
                    .flatten()
            })?;
            assert_eq!(Path::new(command.get_program()), path);
            command
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            let mut tree = acyclic_native_runtime::ProcessTree::spawn(&mut command)
                .map_err(Error::process_io)?;
            let output = tree
                .wait_with_output(
                    std::time::Duration::from_secs(1),
                    1_024,
                    None,
                    || Ok(()),
                    |_| Ok(acyclic_native_runtime::ProcessInput::default()),
                    None,
                    None,
                )
                .map_err(|error| Error::process_io(error.error))?;
            assert!(output.status.success());
            assert_eq!(
                String::from_utf8_lossy(&output.stdout).trim(),
                "mcp-launcher-ran"
            );
        }
    }
    Ok(())
}

// Property: changing only ambient variables preserves revisions; resolving the same
// configured name to a different absolute executable changes them. Resolution is idempotent.
proptest! {
    #[test]
    fn explicit_binding_ignores_ambient_environment_and_pins_path(
        first_term in "[a-z]{0,20}", second_term in "[a-z]{0,20}", argument in "[a-z ]{0,24}",
    ) {
        let executable = std::env::current_exe().unwrap();
        let root = tempfile::tempdir_in(executable.parent().unwrap()).unwrap();
        let a = root.path().join("a");
        let b = root.path().join("b");
        std::fs::create_dir(&a).unwrap();
        std::fs::create_dir(&b).unwrap();
        let name = if cfg!(windows) { "mcp-fixture.exe" } else { "mcp-fixture" };
        for directory in [&a, &b] {
            #[cfg(unix)]
            std::os::unix::fs::symlink(&executable, directory.join(name)).unwrap();
            #[cfg(windows)]
            std::fs::hard_link(&executable, directory.join(name)).unwrap();
        }
        let config = server(json!({"command":"mcp-fixture", "args":[argument], "env":{"KEY":"explicit"}}), Dialect::Codex).unwrap();
        let command = |directory: &Path, term: &str| native::command_for(&config, root.path(), |key| match key {
            "PATH" => Some(directory.as_os_str().to_owned()), "TERM" => Some(term.into()), _ => None,
        }).unwrap();
        let original = command(&a, &first_term);
        let ambient = command(&a, &second_term);
        let replaced = command(&b, &first_term);
        prop_assert!(Path::new(original.get_program()).is_absolute());
        prop_assert_eq!(Path::new(original.get_program()), a.join(name));
        prop_assert_eq!(Path::new(replaced.get_program()), b.join(name));
        prop_assert_eq!(acyclic_native_runtime::ExecutableSearch::from_environment(root.path(), |_| None).resolve(original.get_program()).unwrap(), Path::new(original.get_program()));
        let revision = |command| {
            let binding = native::command_binding(command, &config);
            let (tools, _) = register("fixture".into(), &config, Connected {
                transport: Arc::new(Remote::default()), tools: advertised(&["read"]), binding,
            }, McpClientLimits::default(), &|| Ok(())).unwrap();
            tools.get("mcp.fixture.read", None, None).unwrap().unwrap().definition.revision.clone()
        };
        prop_assert_eq!(revision(&original), revision(&ambient));
        prop_assert_ne!(revision(&original), revision(&replaced));
    }
}

#[cfg(unix)]
#[test]
fn symlinked_launchers_preserve_invocation_path_and_binding() -> Result<()> {
    use std::os::unix::fs::symlink;
    let root = tempfile::tempdir().map_err(Error::process_io)?;
    let target = root.path().join("launcher");
    // Use an existing executable: a concurrent fork can retain a freshly written
    // script's write descriptor until exec, making its execution fail with ETXTBSY.
    symlink("/bin/sh", &target).map_err(Error::process_io)?;
    let bin = root.path().join(".venv/bin");
    std::fs::create_dir_all(&bin).map_err(Error::process_io)?;
    let launcher = bin.join("python");
    symlink(&target, &launcher).map_err(Error::process_io)?;
    let selected = |program| {
        server(
            json!({"command":program, "args":["-c", "printf '%s\\n' \"$0\""]}),
            Dialect::Claude,
        )
    };
    let mut command = native::command_for(&selected(".venv/bin/python")?, root.path(), |_| None)?;
    assert_eq!(Path::new(command.get_program()), launcher);
    let output = command.output().map_err(Error::process_io)?;
    assert!(output.status.success());
    assert_eq!(
        output.stdout,
        format!("{}\n", launcher.display()).as_bytes()
    );

    let searched = selected("python")?;
    let absolute =
        native::command_for(&selected(launcher.to_str().unwrap())?, root.path(), |_| {
            None
        })?;
    assert_eq!(Path::new(absolute.get_program()), launcher);
    assert_eq!(
        native::command_binding(&command, &searched),
        native::command_binding(&absolute, &searched)
    );
    for path in [bin.as_os_str(), std::ffi::OsStr::new(".venv/bin")] {
        let found = native::command_for(&searched, root.path(), |key| {
            (key == "PATH").then(|| path.to_owned())
        })?;
        assert_eq!(Path::new(found.get_program()), launcher);
        assert_eq!(
            native::command_binding(&command, &searched),
            native::command_binding(&found, &searched)
        );
    }
    let real = native::command_for(&selected(target.to_str().unwrap())?, root.path(), |_| None)?;
    assert_ne!(
        native::command_binding(&command, &searched),
        native::command_binding(&real, &searched)
    );
    Ok(())
}
