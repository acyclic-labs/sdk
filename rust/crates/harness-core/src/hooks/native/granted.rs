use super::*;
use acyclic_native_runtime::execution::{ExecutionAccess, ExecutionGrant, ExecutionNetwork};

#[tokio::test]
async fn hook_launch_keeps_the_host_grant_and_explicit_environment() -> Result<()> {
    // Isolate the ambient marker without mutating this test process's environment.
    const AMBIENT: &str = "HOOK_AMBIENT_VALUE";
    if crate::native_environment::variable(AMBIENT).as_deref()
        != Some(std::ffi::OsStr::new("present"))
    {
        acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
        let mut command = ProcessCommand::new(std::env::current_exe().map_err(Error::process_io)?);
        command
            .args([
                "--exact",
                "hooks::native::granted::hook_launch_keeps_the_host_grant_and_explicit_environment",
                "--nocapture",
            ])
            .env(AMBIENT, "present")
            .env_remove("HOOK_ORDINARY_VALUE")
            .stdin(ProcessStdio::null());
        let status = ProcessTree::spawn(&mut command)
            .map_err(Error::process_io)?
            .wait(CaptureSchedule::Unbounded)
            .map_err(Error::process_io)?;
        assert!(status.success(), "isolated hook control failed: {status}");
        return Ok(());
    }
    let root = tempfile::tempdir().map_err(Error::process_io)?;
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).map_err(Error::process_io)?;
    let workspace = std::fs::canonicalize(workspace).map_err(Error::process_io)?;
    let outside = root.path().join("outside");
    let inside = workspace.join("inside");
    let read: Vec<_> = ["/usr", "/bin", "/lib", "/lib64", "/etc/ld.so.cache"]
        .into_iter()
        .map(PathBuf::from)
        .filter(|path| path.exists())
        .collect();
    let program = r#"test "$CLAUDE_PROJECT_DIR" = "$PWD" || exit 9; { printf changed > ../outside; printf created > inside; } 2>/dev/null; printf '%s:%s:complete' "$HOOK_ORDINARY_VALUE" "${HOOK_AMBIENT_VALUE+x}""#;
    for run in [
        HookRun::Shell {
            command: program.into(),
            shell: Some("sh".into()),
        },
        HookRun::Executable {
            executable: "/bin/sh".into(),
            argv: vec!["-c".into(), program.into()],
        },
    ] {
        let mut hooks = Hooks::default();
        hooks.add_rules([HookRule {
            on: HookEvent::SessionStart,
            matcher: String::new(),
            hooks: vec![HookSpec {
                run,
                timeout_ms: 1_000,
                failure: HookFailure::Block,
            }],
        }])?;
        // The same ordinary launch is the outside-effect negative control.
        for access in [
            None,
            Some(ExecutionAccess::ReadOnly),
            Some(ExecutionAccess::WorkspaceWrite),
        ] {
            std::fs::write(&outside, "original").map_err(Error::process_io)?;
            match std::fs::remove_file(&inside) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(Error::process_io(error)),
            }
            let execution = access
                .map(|access| {
                    ExecutionGrant::new(&workspace, access, read.clone(), ExecutionNetwork::Off)
                        .map(|grant| {
                            Arc::new(HookExecution {
                                grant,
                                environment: BTreeMap::from([
                                    ("HOOK_ORDINARY_VALUE".into(), "selected".into()),
                                    (
                                        "CLAUDE_PROJECT_DIR".into(),
                                        "cannot-override-workspace".into(),
                                    ),
                                ]),
                            })
                        })
                })
                .transpose()
                .map_err(Error::process_io)?;
            let output = hooks
                .run_checked(
                    HookEvent::SessionStart,
                    (&workspace, &workspace),
                    HookInput::Fields(&serde_json::json!({})),
                    &ToolRegistry::default(),
                    1_024,
                    (Arc::new(|| Ok(())), None, execution),
                )
                .await?;
            // The checked runner retains native unavailability in its original
            // diagnostic envelope, without launching a fallback command.
            if access.is_some()
                && matches!(output.diagnostics.as_slice(), [HookDiagnostic {
                    cause: crate::ProcessError::Io { exit_code: None, message },
                    blocking: true,
                }] if message.starts_with("unsupported capability: Landlock "))
            {
                assert!(output.additional_context.is_empty());
                assert_eq!(
                    std::fs::read_to_string(&outside).map_err(Error::process_io)?,
                    "original"
                );
                assert!(!inside.exists());
                continue;
            }
            assert!(output.diagnostics.is_empty(), "{output:?}");
            assert!(!output.blocks(), "{output:?}");
            assert_eq!(
                output.additional_context,
                [if access.is_some() {
                    "selected::complete"
                } else {
                    ":x:complete"
                }]
            );
            assert_eq!(
                std::fs::read_to_string(&outside).map_err(Error::process_io)?,
                if access.is_none() {
                    "changed"
                } else {
                    "original"
                }
            );
            assert_eq!(inside.exists(), access != Some(ExecutionAccess::ReadOnly));
        }
    }
    Ok(())
}
