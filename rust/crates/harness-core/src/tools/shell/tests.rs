use super::*;
use proptest::prelude::*;

proptest! {
    // Decoding/encoding preserves every field; every accepted cwd is relative
    // and contains no parent component, regardless of host path syntax.
    #[test]
    fn command_round_trip_preserves_the_selected_directory(
        command in ".{0,200}", cwd in proptest::option::of(".{0,100}"),
        timeout in proptest::option::of(any::<u32>()),
    ) {
        let input = ShellInput { command, cwd, timeout_ms: timeout, target: None };
        let encoded = serde_json::to_vec(&input).map_err(|error| TestCaseError::fail(error.to_string()))?;
        let restored: ShellInput = serde_json::from_slice(&encoded).map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(&restored, &input);
        if input.validate().is_ok() {
            let cwd = Path::new(input.cwd.as_deref().unwrap_or(""));
            prop_assert!(!cwd.is_absolute());
            prop_assert!(cwd.components().all(|part| matches!(part, Component::CurDir | Component::Normal(_))));
            prop_assert!(!input.command.is_empty());
            prop_assert!(!input.command.contains('\0'));
            let timeout = input.timeout_ms().map_err(|error| TestCaseError::fail(error.to_string()))?;
            prop_assert!(timeout.is_none_or(|timeout| timeout > 0));
            prop_assert_eq!(timeout, input.timeout_ms);
        }
    }
}

#[test]
fn model_cannot_select_a_workspace_or_escape_through_cwd() {
    assert!(serde_json::from_str::<ShellInput>(r#"{"command":"pwd","workspace":"A"}"#).is_err());
    for cwd in ["../sibling", "src/../sibling", "/absolute", "dir\0name"] {
        assert!(
            ShellInput {
                command: "pwd".into(),
                cwd: Some(cwd.into()),
                timeout_ms: None,
                target: None
            }
            .validate()
            .is_err()
        );
    }
    assert!(
        ShellInput {
            command: "pwd".into(),
            cwd: None,
            timeout_ms: Some(0),
            target: None,
        }
        .validate()
        .is_err()
    );
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
    let definitions = crate::tools::defaults()?.definitions(None)?;
    assert!(definitions.iter().any(|tool| tool.name == "shell"));
    assert!(
        !definitions
            .iter()
            .any(|tool| matches!(tool.name.as_str(), "bash" | "powershell"))
    );
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
