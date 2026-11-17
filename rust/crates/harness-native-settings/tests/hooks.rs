#![cfg(feature = "test-support")]
#![cfg(not(target_arch = "wasm32"))]
//! Native hook trust and project-settings admission over the shared script.
use acyclic_harness_core::{
    Error, Result,
    hooks::{HookEvent, HookFailure, HookOutput, HookRule, HookRun, HookSpec, Hooks},
    tool::ToolRegistry,
};
use acyclic_harness_native_settings::{NativeSettings, SettingsConfig};
use serde_json::{Value, json};

fn command(hook: &HookSpec) -> &str {
    match &hook.run {
        HookRun::Executable { executable, .. } => executable,
        HookRun::Shell { command, .. } => command,
        HookRun::Plugin { instance, .. } => instance,
    }
}

fn registry() -> Result<ToolRegistry> {
    let mut registry = acyclic_harness_core::tools::defaults()?;
    let mut notebook = acyclic_harness_core::tool::files::read()?;
    notebook.definition.name = "NotebookEdit".into();
    notebook.hook_names = &[];
    registry.register((notebook, None), &mut None, None)?;
    Ok(registry)
}

#[tokio::test]
async fn native_hooks_default_user_guards_and_project_trust_use_one_shared_script() -> Result<()> {
    let process_error = Error::process_io;
    if let Some(address) =
        std::env::args().find_map(|arg| arg.strip_prefix("hook-child:").map(str::to_owned))
    {
        use std::io::{Read, Write};
        let mut stream = std::net::TcpStream::connect(address).map_err(process_error)?;
        stream.write_all(&[1]).map_err(process_error)?;
        let mut byte = [0];
        stream.read_exact(&mut byte).map_err(process_error)?;
        return Err(Error::Invalid(
            "hook fixture was released instead of cancelled".into(),
        ));
    }
    let fixture = tempfile::tempdir().map_err(process_error)?;
    let home = fixture.path().join("home");
    let workspace = fixture.path().join("project");
    let cwd = workspace.join("nested");
    std::fs::create_dir_all(&cwd).map_err(process_error)?;
    #[cfg(windows)]
    let (script_name, script, shell) = (
        "fixture.cmd",
        r#"@echo off
setlocal
set /p payload=
if not defined payload exit /b 3
if not "%CD%"=="%CLAUDE_PROJECT_DIR%\nested" exit /b 4
> "%CLAUDE_PROJECT_DIR%\hook-ran" echo %payload%
1>&2 echo %payload%
exit /b 2
"#,
        "cmd",
    );
    #[cfg(not(windows))]
    let (script_name, script, shell) = (
        "fixture.sh",
        r#"IFS= read -r input
test -n "$input" || exit 3
test "$(pwd -P)" = "$(cd "$CLAUDE_PROJECT_DIR/nested" && pwd -P)" || exit 4
printf '%s' "$input" > "$CLAUDE_PROJECT_DIR/hook-ran"
printf '%s' "$input" >&2
exit 2
"#,
        "sh",
    );
    let script_path = fixture.path().join(script_name);
    std::fs::write(&script_path, script).map_err(process_error)?;
    #[cfg(windows)]
    let command = format!("call \"{}\"", script_path.display());
    #[cfg(not(windows))]
    let command = format!("sh \"{}\"", script_path.display());
    let hook = json!({"type":"command","command":command,"shell":shell});
    let write = |root: &std::path::Path, path: &str, value: &Value| -> Result<()> {
        let path = root.join(path);
        std::fs::create_dir_all(
            path.parent()
                .ok_or_else(|| Error::Invalid("fixture parent missing".into()))?,
        )
        .map_err(process_error)?;
        let bytes = serde_json::to_vec(value).map_err(|error| Error::Invalid(error.to_string()))?;
        acyclic_harness_native_settings::codec_fuzz::check(&bytes)?;
        std::fs::write(path, bytes).map_err(process_error)
    };
    write(
        &home,
        ".claude/settings.json",
        &json!({"hooks":{"PreToolUse":[{"matcher":acyclic_harness_core::tools::shell::PLATFORM.name,"hooks":[hook]}]}}),
    )?;
    let project = json!({"hooks":{"Stop":[{"hooks":[hook]}]}});
    // Configuration input, not a fabricated command-output fixture: the second
    // selector must refuse the whole import before publishing the first group.
    acyclic_harness_native_settings::codec_fuzz::check(
        &serde_json::to_vec(&json!({"hooks":{"PreToolUse":[
            {"matcher":"shell","hooks":[hook]},
            {"matcher":"[","hooks":[hook]}
        ]}}))
        .map_err(|error| Error::Invalid(error.to_string()))?,
    )?;
    for path in [
        ".claude/settings.json",
        ".claude/settings.local.json",
        ".codex/hooks.json",
    ] {
        write(&workspace, path, &project)?;
    }
    let toml = format!(
        "[[hooks.Stop]]\n[[hooks.Stop.hooks]]\ntype = 'command'\ncommand = {command:?}\nshell = '{shell}'\n"
    );
    acyclic_harness_native_settings::codec_fuzz::check(toml.as_bytes())?;
    std::fs::write(workspace.join(".codex/config.toml"), toml).map_err(process_error)?;
    let registry = registry()?;
    let hooks = NativeSettings::load(
        SettingsConfig::new(&workspace, &home).codex_home(home.join(".codex")),
    )
    .await?
    .hooks;
    let input =
        json!({"tool_name":acyclic_harness_core::tools::shell::PLATFORM.name,"session_id":"test"});
    assert_eq!(
        hooks
            .run(HookEvent::Stop, &workspace, &cwd, &input, &registry)
            .await?,
        HookOutput::default()
    );
    assert!(
        !workspace.join("hook-ran").exists(),
        "untrusted project commands never execute"
    );
    let output = hooks
        .run(HookEvent::PreToolUse, &workspace, &cwd, &input, &registry)
        .await?;
    assert!(
        output.blocks(),
        "user shell guard fires on the platform tool: {output:?}"
    );
    assert!(output.diagnostics.is_empty(), "{output:?}");
    assert!(
        workspace.join("hook-ran").exists(),
        "script receives the project root and per-call cwd"
    );
    let capture = |output: &HookOutput| -> Result<Value> {
        let bytes = std::fs::read(workspace.join("hook-ran")).map_err(process_error)?;
        assert_eq!(
            output.blocked.as_deref(),
            Some(std::str::from_utf8(&bytes).map_err(|error| Error::Invalid(error.to_string()))?),
            "native hook lost its original refusal bytes"
        );
        serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))
    };
    let captured = capture(&output)?;
    assert_eq!(
        captured,
        json!({"tool_name":acyclic_harness_core::tools::shell::PLATFORM.name, "session_id":"test", "hook_event_name":"PreToolUse", "cwd":cwd.to_string_lossy()})
    );
    let failure = hooks
        .run(
            HookEvent::PreToolUse,
            &workspace,
            &workspace.join("missing"),
            &input,
            &registry,
        )
        .await?;
    assert!(failure.blocked.is_none());
    assert_eq!(failure.diagnostics.len(), 1);
    let trusted = NativeSettings::load(
        SettingsConfig::new(&workspace, &home)
            .codex_home(home.join(".codex"))
            .trust_project_hooks(true),
    )
    .await?
    .hooks;
    assert_eq!(
        trusted
            .commands(HookEvent::Stop, &json!({}), &registry)?
            .count(),
        4
    );
    let trusted_output = trusted
        .run(HookEvent::Stop, &workspace, &cwd, &input, &registry)
        .await?;
    let captured = capture(&trusted_output)?;
    assert_eq!(captured["hook_event_name"], "Stop");
    std::fs::remove_file(home.join(".claude/settings.json")).map_err(process_error)?;
    std::fs::create_dir(home.join(".claude/settings.json")).map_err(process_error)?;
    assert!(matches!(
        NativeSettings::load(
            SettingsConfig::new(&workspace, &home).codex_home(home.join(".codex"))
        )
        .await,
        Err(Error::Process(_))
    ));
    Ok(())
}

#[tokio::test]
async fn relocated_codex_configuration_and_duplicate_paths_are_loaded_once() -> Result<()> {
    let process_error = Error::process_io;
    let root = tempfile::tempdir().map_err(process_error)?;
    let home = root.path();
    let relocated = home.join("custom-codex");
    std::fs::create_dir_all(home.join(".claude")).map_err(process_error)?;
    std::fs::create_dir(&relocated).map_err(process_error)?;
    let document = br#"{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"guard"}]}]}}"#;
    std::fs::write(home.join(".claude/settings.json"), document).map_err(process_error)?;
    std::fs::write(relocated.join("hooks.json"), document).map_err(process_error)?;
    let hooks = NativeSettings::load(
        SettingsConfig::new(home, home)
            .codex_home(&relocated)
            .trust_project_hooks(true),
    )
    .await?
    .hooks;
    let registry = registry()?;
    assert_eq!(
        hooks
            .commands(HookEvent::Stop, &json!({}), &registry)?
            .count(),
        2
    );
    #[cfg(unix)]
    {
        let alias = home.join("alias");
        std::os::unix::fs::symlink(home, &alias).map_err(process_error)?;
        let hooks = NativeSettings::load(
            SettingsConfig::new(&alias, home)
                .codex_home(&relocated)
                .trust_project_hooks(true),
        )
        .await?
        .hooks;
        assert_eq!(
            hooks
                .commands(HookEvent::Stop, &json!({}), &registry)?
                .count(),
            2
        );
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_settings_keep_the_format_of_the_configured_path() -> Result<()> {
    let process_error = Error::process_io;
    let root = tempfile::tempdir().map_err(process_error)?;
    let home = root.path().join("home");
    let codex = home.join(".codex");
    let claude = home.join(".claude");
    let workspace = root.path().join("workspace");
    for directory in [&codex, &claude, &workspace] {
        std::fs::create_dir_all(directory).map_err(process_error)?;
    }
    let toml = root.path().join("codex-dotfile");
    std::fs::write(
        &toml,
        "[[hooks.Stop]]\n[[hooks.Stop.hooks]]\ntype = 'command'\ncommand = 'toml-guard'\n",
    )
    .map_err(process_error)?;
    let json = root.path().join("claude-dotfile.toml");
    std::fs::write(
        &json,
        br#"{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"json-guard"}]}]}}"#,
    )
    .map_err(process_error)?;
    std::os::unix::fs::symlink(&toml, codex.join("config.toml")).map_err(process_error)?;
    std::os::unix::fs::symlink(&json, claude.join("settings.json")).map_err(process_error)?;
    // The trusted project alias resolves to the same JSON file and still runs once.
    std::fs::create_dir(workspace.join(".claude")).map_err(process_error)?;
    std::os::unix::fs::symlink(&json, workspace.join(".claude/settings.json"))
        .map_err(process_error)?;
    let hooks = NativeSettings::load(
        SettingsConfig::new(&workspace, &home)
            .codex_home(&codex)
            .trust_project_hooks(true),
    )
    .await?
    .hooks;
    assert_eq!(
        hooks
            .commands(HookEvent::Stop, &json!({}), &registry()?)?
            .map(|hook| command(hook))
            .collect::<Vec<_>>(),
        ["json-guard", "toml-guard"]
    );
    Ok(())
}
