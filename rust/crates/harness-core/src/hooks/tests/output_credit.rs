use super::*;

#[tokio::test]
async fn native_output_has_no_implicit_cap_and_preserves_explicit_credit() -> Result<()> {
    use std::io::Write;

    let payload = "x".repeat(131_072);
    if std::env::args().any(|argument| argument == "hook-output-credit-child") {
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(payload.as_bytes()).unwrap();
        stdout.flush().unwrap();
        std::process::exit(0);
    }
    let mut hooks = Hooks::default();
    hooks.add_rules([HookRule {
        on: HookEvent::SessionStart,
        matcher: String::new(),
        hooks: vec![HookSpec {
            run: HookRun::Executable {
                executable: std::env::current_exe()
                    .map_err(Error::process_io)?
                    .to_string_lossy()
                    .into_owned(),
                argv: vec![
                    "--exact".into(),
                    "hooks::tests::output_credit::native_output_has_no_implicit_cap_and_preserves_explicit_credit".into(),
                    "--nocapture".into(),
                    "--skip".into(),
                    "hook-output-credit-child".into(),
                ],
            },
            timeout_ms: None,
            failure: HookFailure::Report,
        }],
    }])?;
    let root = std::env::current_dir().map_err(Error::process_io)?;
    let registry = ToolRegistry::default();
    let input = json!({});
    // Exercise the public default and the real native capture, not a simulated
    // callback that could ignore the quota passed by the dispatcher.
    let output = hooks
        .run(HookEvent::SessionStart, &root, &root, &input, &registry)
        .await?;
    assert!(output.diagnostics.is_empty(), "{output:?}");
    assert_eq!(output.additional_context.len(), 1);
    assert!(output.additional_context[0].ends_with(&payload));

    for credit in [262_144, 4096] {
        let output = hooks
            .run_checked(
                HookEvent::SessionStart,
                (&root, &root),
                (&input).into(),
                &registry,
                Some(credit),
                (Arc::new(|| Ok(())), None, None),
            )
            .await?;
        if credit == 262_144 {
            assert!(output.diagnostics.is_empty(), "{output:?}");
            assert_eq!(output.additional_context.len(), 1);
            assert!(output.additional_context[0].ends_with(&payload));
        } else {
            assert!(output.additional_context.is_empty());
            assert!(matches!(
                output.diagnostics.as_slice(),
                [HookDiagnostic {
                    cause: crate::ProcessError::OutputLimit { .. },
                    blocking: false,
                }]
            ));
            assert!(!output.blocks());
        }
    }
    Ok(())
}
