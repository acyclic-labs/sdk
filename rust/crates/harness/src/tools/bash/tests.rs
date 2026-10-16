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
        let input = BashInput { command, cwd, timeout };
        let encoded = serde_json::to_vec(&input).map_err(|error| TestCaseError::fail(error.to_string()))?;
        let restored: BashInput = serde_json::from_slice(&encoded).map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(&restored, &input);
        if input.validate().is_ok() {
            let cwd = Path::new(input.cwd.as_deref().unwrap_or(""));
            prop_assert!(!cwd.is_absolute());
            prop_assert!(cwd.components().all(|part| matches!(part, Component::CurDir | Component::Normal(_))));
            prop_assert!(!input.command.is_empty());
            prop_assert!(!input.command.contains('\0'));
            let timeout = input.timeout_ms().map_err(|error| TestCaseError::fail(error.to_string()))?;
            prop_assert!(timeout > 0);
            prop_assert_eq!(u64::from(timeout), u64::from(input.timeout.unwrap_or(300)) * 1000);
        }
    }
}

#[test]
fn model_cannot_select_a_workspace_or_escape_through_cwd() {
    assert!(serde_json::from_str::<BashInput>(r#"{"command":"pwd","workspace":"A"}"#).is_err());
    for cwd in ["../sibling", "src/../sibling", "/absolute", "dir\0name"] {
        assert!(
            BashInput {
                command: "pwd".into(),
                cwd: Some(cwd.into()),
                timeout: None
            }
            .validate()
            .is_err()
        );
    }
    for timeout in [0, 4_294_968, u32::MAX] {
        assert!(
            BashInput {
                command: "pwd".into(),
                cwd: None,
                timeout: Some(timeout)
            }
            .validate()
            .is_err()
        );
    }
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
