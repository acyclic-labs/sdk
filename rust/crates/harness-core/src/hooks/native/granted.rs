use super::*;
use acyclic_native_runtime::execution::{ExecutionAccess, ExecutionGrant, ExecutionNetwork};

#[tokio::test]
async fn hook_launch_keeps_the_host_grant_and_explicit_environment() -> Result<()> {
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
    let program = r#"test "$CLAUDE_PROJECT_DIR" = "$PWD" || exit 9; { printf changed > ../outside; printf created > inside; } 2>/dev/null; printf '%s:complete' "$HOOK_ORDINARY_VALUE""#;
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
        let hook = Arc::new(HookSpec {
            run,
            timeout_ms: 1_000,
            failure: HookFailure::Block,
        });
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
            let result = run_hook(
                Arc::clone(&hook),
                Arc::new((workspace.clone(), workspace.clone(), vec![])),
                Arc::new(|| Ok(())),
                1_024,
                None,
                execution,
            )
            .await;
            let (code, stdout, stderr) = match result {
                Err(Error::Unsupported(_)) if access.is_some() => {
                    assert_eq!(
                        std::fs::read_to_string(&outside).map_err(Error::process_io)?,
                        "original"
                    );
                    assert!(!inside.exists());
                    continue;
                }
                other => other?,
            };
            assert_eq!(code, Some(0), "{stderr:?}");
            assert!(stdout.ends_with(b":complete"));
            if access.is_some() {
                assert_eq!(stdout, b"selected:complete");
            }
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
