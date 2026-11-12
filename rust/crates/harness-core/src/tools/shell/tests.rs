use super::*;
use proptest::prelude::*;

proptest! {
    // Decoding/encoding preserves every field; every accepted cwd is relative
    // and contains no parent component, regardless of host path syntax.
    #[test]
    fn command_round_trip_preserves_the_selected_directory(
        command in prop_oneof![".{0,200}".prop_map(ShellCommand::Source), proptest::collection::vec(".{0,80}", 0..8).prop_map(ShellCommand::Argv)], cwd in proptest::option::of(".{0,100}"),
        timeout_ms in proptest::option::of(any::<u32>()),
        background in any::<bool>(),
    ) {
        let input = ShellInput { command, cwd, timeout_ms, background, target: None };
        let encoded = serde_json::to_vec(&input).map_err(|error| TestCaseError::fail(error.to_string()))?;
        let restored: ShellInput = serde_json::from_slice(&encoded).map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(&restored, &input);
        if input.validate().is_ok() {
            let cwd = Path::new(input.cwd.as_deref().unwrap_or(""));
            prop_assert!(!cwd.is_absolute());
            prop_assert!(cwd.components().all(|part| matches!(part, Component::CurDir | Component::Normal(_))));
            prop_assert!(input.command.validate().is_ok());
            let timeout = input.timeout_ms().map_err(|error| TestCaseError::fail(error.to_string()))?;
            prop_assert!(timeout.is_none_or(|timeout| timeout > 0));
            prop_assert_eq!(timeout, input.timeout_ms);
        }
    }
}

#[test]
fn shell_refusals_name_the_field_and_accept_its_correction() -> Result<()> {
    // An absolute working directory is rejected before execution.
    let mut input: ShellInput = serde_json::from_str(
        r#"{
        "command":"pwd && ls -la",
        "cwd":"/absolute/workspace",
        "timeout_ms":20000
    }"#,
    )
    .expect("recorded shell call");
    let error = input.validate().unwrap_err().to_string();
    assert!(
        error.contains("cwd: use a path relative to the task view"),
        "{error}"
    );
    input.cwd = None;
    input.validate()?;
    for cwd in ["../sibling", "src/../sibling", "/absolute", "dir\0name"] {
        input.cwd = Some(cwd.into());
        assert!(input.validate().unwrap_err().to_string().contains("cwd:"));
        input.cwd = Some("src".into());
        input.validate()?;
    }
    input.cwd = None;
    input.timeout_ms = Some(0);
    assert!(
        input
            .validate()
            .unwrap_err()
            .to_string()
            .contains("timeout_ms: supply a positive duration in milliseconds")
    );
    input.timeout_ms = None;
    input.validate()?;
    for command in [
        ShellCommand::Source(String::new()),
        ShellCommand::Source("a\0b".into()),
        ShellCommand::Argv(Vec::new()),
        ShellCommand::Argv(vec![String::new()]),
        ShellCommand::Argv(vec!["echo".into(), "a\0b".into()]),
    ] {
        input.command = command.clone();
        let error = input.validate().unwrap_err().to_string();
        assert!(
            error.contains("command: supply nonempty shell source or argv"),
            "{error}"
        );
        assert_eq!(command.prepare(&PLATFORM).unwrap_err().to_string(), error);
    }
    input.command = ShellCommand::Argv(vec!["echo".into(), String::new()]);
    input.validate()?;
    assert!(serde_json::from_str::<ShellInput>(r#"{"command":"pwd","workspace":"A"}"#).is_err());
    Ok(())
}

proptest! {
    // Every facade argv has a canonical quoted representation that parses back
    // without shell expansion, including empty words and embedded quotes.
    #[test]
    fn facade_quoting_preserves_every_argument(arguments in proptest::collection::vec("[^\\x00]{0,80}", 1..10)) {
        let quoted = arguments.iter().map(|argument| format!("'{}'", argument.replace('\'', "'\\''")))
            .collect::<Vec<_>>().join(" ");
        let parsed = crate::tools::parse_command(&quoted).map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(parsed, arguments);
    }
}

// Check the actual model-visible registry, independently of PLATFORM.name.
#[test]
fn model_visible_shell_name_is_platform_independent() -> crate::Result<()> {
    let registry = crate::tools::defaults()?;
    let definitions = registry.definitions(None)?;
    assert!(definitions.iter().any(|tool| tool.name == "shell"));
    assert!(
        !definitions
            .iter()
            .any(|tool| matches!(tool.name.as_str(), "bash" | "powershell"))
    );
    for alias in ["bash", "powershell"] {
        assert!(registry.get(alias, None, None)?.is_none());
    }
    Ok(())
}

#[cfg(windows)]
#[test]
fn windows_shell_name_keeps_powershell_execution() -> crate::Result<()> {
    model_visible_shell_name_is_platform_independent()?;
    assert_eq!(PLATFORM.executables, &["pwsh.exe", "powershell.exe"]);
    assert_eq!(PLATFORM.language, "powershell");
    assert_eq!(
        crate::native_environment::PlatformPolicy::Native.facts().1,
        "PowerShell"
    );
    Ok(())
}
