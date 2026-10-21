use super::*;

proptest::proptest! {
    // Property: retention and propagation preserve the process classification,
    // exit code and message. Stop ignores failure policy, but retains the data.
    #[test]
    fn retained_failures_keep_their_kind_code_and_policy(
        kind in 0usize..8, code in proptest::option::of(proptest::num::i32::ANY),
        message in ".{0,48}", blocking in proptest::bool::ANY, stop in proptest::bool::ANY,
    ) {
        let cause = match kind {
            0 => crate::ProcessError::NotFound { exit_code: code, message },
            1 => crate::ProcessError::Timeout { exit_code: code, message },
            2 => crate::ProcessError::PermissionDenied { exit_code: code, message },
            3 => crate::ProcessError::Io { exit_code: code, message },
            4 => crate::ProcessError::Exit { exit_code: code, message },
            5 => crate::ProcessError::InvalidOutput { exit_code: code, message },
            6 => crate::ProcessError::Cancelled { exit_code: code, message },
            _ => crate::ProcessError::OutputLimit { exit_code: code, message },
        };
        let mut output = HookOutput::default();
        output.apply(if stop { HookEvent::Stop } else { HookEvent::SessionStart },
            if blocking { HookFailure::Block } else { HookFailure::Report },
            Err(Error::Process(cause.clone())));
        let bytes = serde_json::to_vec(&output)?;
        let decoded: HookOutput = serde_json::from_slice(&bytes)?;
        proptest::prop_assert_eq!(&decoded, &output);
        proptest::prop_assert_eq!(decoded.diagnostics[0].blocking, blocking && !stop);
        proptest::prop_assert_eq!(&decoded.diagnostics[0].cause, &cause);
        match decoded.into_result() {
            Err(Error::Process(retained)) => {
                proptest::prop_assert!(blocking && !stop);
                proptest::prop_assert_eq!(&retained, &cause);
                // Propagate, retain again, and compare the complete enum. In
                // particular InvalidOutput must never become Io.
                let mut again = HookOutput::default();
                again.apply(HookEvent::PreToolUse, HookFailure::Block, Err(Error::Process(retained)));
                proptest::prop_assert_eq!(&again.diagnostics[0].cause, &cause);
                // Negative control: the old lossy conversion is caught by the
                // same comparison for every non-IO variant.
                let broken = crate::ProcessError::Io { exit_code: cause.exit_code(), message: cause.message().clone() };
                proptest::prop_assert_eq!(broken == cause, matches!(cause, crate::ProcessError::Io { .. }));
            }
            Ok(_) => proptest::prop_assert!(!blocking || stop),
            Err(error) => proptest::prop_assert!(false, "failure became {error:?}"),
        }
    }
}

#[tokio::test]
async fn imported_on_failure_controls_a_missing_guard() -> Result<()> {
    for event in [HookEvent::PreToolUse, HookEvent::Stop] {
        for (setting, expected) in [
            (Some("block"), true),
            (Some("continue"), false),
            (None, false),
        ] {
            let mut guard = json!({"type":"command", "command":"missing-guard"});
            if let Some(setting) = setting {
                guard["onFailure"] = setting.into();
            }
            let mut hooks = Hooks::default();
            hooks.add_json(
                json!({"hooks":{event.name():[{"hooks":[guard]}]}})
                    .to_string()
                    .as_bytes(),
            )?;
            let output = hooks
                .run_commands(
                    event,
                    &json!({"tool_name":"bash"}),
                    &registry()?,
                    65_536,
                    |_, _| {
                        std::future::ready(Err(Error::Process(crate::ProcessError::NotFound {
                            exit_code: None,
                            message: "missing guard".into(),
                        })))
                    },
                )
                .await?;
            assert_eq!(
                output.blocks(),
                expected && event != HookEvent::Stop,
                "{event:?} {setting:?}"
            );
            assert_eq!(output.diagnostics.len(), 1);
        }
    }
    Ok(())
}

#[test]
fn stop_failures_remain_diagnostics_while_explicit_denials_still_block() {
    for policy in [HookFailure::Report, HookFailure::Block] {
        for result in [
            Err(Error::Process(crate::ProcessError::NotFound {
                exit_code: None,
                message: "guard missing".into(),
            })),
            Err(Error::Process(crate::ProcessError::Timeout {
                exit_code: None,
                message: "guard timed out".into(),
            })),
            Ok((Some(3), vec![], b"guard failed".to_vec())),
            Ok((None, vec![], b"guard terminated".to_vec())),
            Ok((Some(0), br#"{"decision":"block""#.to_vec(), vec![])),
            Ok((
                Some(0),
                br#"{"hookSpecificOutput":{"permissionDecision":false}}"#.to_vec(),
                vec![],
            )),
        ] {
            let mut output = HookOutput::default();
            output.apply(HookEvent::Stop, policy, result);
            assert!(output.blocked.is_none(), "{output:?}");
            assert_eq!(output.diagnostics.len(), 1);
        }
        for result in [
            (Some(2), vec![], b"explicit denial".to_vec()),
            (
                Some(0),
                br#"{"decision":"block","reason":"explicit denial"}"#.to_vec(),
                vec![],
            ),
            (
                Some(0),
                br#"{"continue":false,"stopReason":"explicit denial"}"#.to_vec(),
                vec![],
            ),
        ] {
            let mut output = HookOutput::default();
            output.apply(HookEvent::Stop, policy, Ok(result));
            assert_eq!(output.blocked.as_deref(), Some("explicit denial"));
            assert!(output.diagnostics.is_empty(), "{output:?}");
        }
    }
}

#[test]
fn post_tool_block_preserves_feedback_alongside_additional_context() {
    let mut output = HookOutput::default();
    output.apply(HookEvent::PostToolUse, HookFailure::Report, Ok((Some(0),
        br#"{"decision":"block","reason":"Fix lint errors","hookSpecificOutput":{"additionalContext":"Run lint again"}}"#.to_vec(), vec![])));
    assert_eq!(
        output.additional_context,
        ["Fix lint errors", "Run lint again"]
    );
    assert!(output.blocked.is_none());
    assert!(output.diagnostics.is_empty());
}
