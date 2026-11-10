//! Real checkout dispatch retains the host ceiling and definitive refusals.
use super::*;
use acyclic_native_runtime::execution::{ExecutionAccess, ExecutionGrant, ExecutionNetwork};

#[tokio::test]
async fn host_executor_grants_reach_shells_and_hooks_without_erasing_refusals() -> Result<()> {
    let root = temporary_directory()?;
    let directory = root.path().join("workspace");
    let foreign = root.path().join("foreign");
    std::fs::create_dir(&directory).map_err(storage)?;
    std::fs::create_dir(&foreign).map_err(storage)?;
    let state = temporary_directory()?;
    let fixture = LocalHarness::open(
        LocalHarnessConfig::native_host(&directory).state_directory(state.path()),
    )
    .await?;
    let workspace = fixture.workspace.as_ref().expect("checkout");
    let private = workspace.mounts.named(Some("home"))?.1.view.volume.clone();
    let maximum = 65_536;
    let signed = &fixture.signed;
    let task = crate::runtime::Bindings {
        scope: RuntimeScope::new(
            Capabilities::new(signed.capabilities().iter().map(str::to_owned).chain([
                crate::contract::capability::tool_call(crate::tools::shell::PLATFORM.name),
            ])),
            Default::default(),
        )?,
        ..crate::runtime::Bindings::local()
    }
    .build()?
    .agent_context(
        &Capabilities::default(),
        crate::runtime::ContentBindings {
            volume: private.clone(),
            reader: crate::filesystem::FilesystemContentVerifier::new(
                fixture.files.clone(),
                &fixture.verifier,
                signed,
                maximum,
                None,
            )?,
            writer: Some(crate::filesystem::FilesystemContentPublisher::new(
                fixture.files.clone(),
                &private,
                &fixture.verifier,
                signed,
                maximum,
                None,
            )?),
        },
    )?;
    let task = Arc::new(task);
    let read: Vec<_> = ["/usr", "/bin", "/lib", "/lib64", "/etc/ld.so.cache"]
        .into_iter()
        .map(PathBuf::from)
        .filter(|path| path.exists())
        .collect();
    let program =
        "{ printf changed > ../outside; printf created > inside; } 2>/dev/null; printf complete";
    let mut hooks = crate::hooks::Hooks::default();
    hooks.add_rules([crate::hooks::HookRule {
        on: crate::hooks::HookEvent::SessionStart,
        matcher: String::new(),
        hooks: vec![crate::hooks::HookSpec {
            run: crate::hooks::HookRun::Shell {
                command: program.into(),
                shell: Some("sh".into()),
            },
            timeout_ms: 1_000,
            failure: crate::hooks::HookFailure::Block,
        }],
    }])?;
    let outside = root.path().join("outside");
    let inside = directory.join("inside");
    for hook in [false, true] {
        for (access, wrong_workspace) in [
            (None, false),
            (Some(ExecutionAccess::ReadOnly), false),
            (Some(ExecutionAccess::WorkspaceWrite), false),
            (Some(ExecutionAccess::WorkspaceWrite), true),
        ] {
            let grant = access
                .map(|access| {
                    ExecutionGrant::new(
                        if wrong_workspace {
                            &foreign
                        } else {
                            &directory
                        },
                        access,
                        read.clone(),
                        ExecutionNetwork::Off,
                    )
                })
                .transpose()
                .map_err(Error::process_io)?;
            let (executor, _writer) = workspace
                .executor(
                    (&fixture.project, &fixture.files, maximum),
                    Arc::new(acyclic_stream::SystemUnixMillisClock),
                    signed,
                    &fixture.verifier,
                    grant,
                )
                .await?;
            // Reuse this same installed grant through two independent child calls.
            for _ in 0..2 {
                std::fs::write(&outside, "original").map_err(storage)?;
                match std::fs::remove_file(&inside) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(storage(error)),
                }
                let context = ToolContext::new(task.clone(), OperationId::new(), "host-grant")?;
                let refused = if hook {
                    let output = executor
                        .run_hooks(
                            (&hooks, [1; 32]),
                            crate::hooks::HookEvent::SessionStart,
                            crate::hooks::HookInput::Fields(&serde_json::json!({})),
                            &ToolRegistry::default(),
                            context,
                            false,
                        )
                        .await?
                        .expect("hook outcome");
                    if wrong_workspace {
                        assert_eq!(output.diagnostics.len(), 1, "{output:?}");
                        assert!(output.diagnostics[0].cause.message().ends_with(
                            "execution working directory escapes the granted workspace"
                        ));
                        assert!(output.blocks());
                        true
                    } else {
                        assert!(output.diagnostics.is_empty(), "{output:?}");
                        assert!(!output.blocks(), "{output:?}");
                        assert_eq!(output.additional_context, ["complete"]);
                        false
                    }
                } else {
                    let invocation = ToolInvocation {
                        operation_id: context.operation_id(),
                        call_id: context.call_id().into(),
                        name: crate::tools::shell::PLATFORM.name.into(),
                        arguments: serde_json::json!({"command": program}),
                    };
                    match executor.execute(invocation, Some(context)).await {
                        Err(Error::Unauthorized(reason)) if wrong_workspace => {
                            assert_eq!(
                                reason,
                                "execution working directory escapes the granted workspace"
                            );
                            true
                        }
                        other => {
                            let result = other?;
                            assert!(!wrong_workspace, "{result:?}");
                            assert_eq!(result.value["success"], true, "{result:?}");
                            false
                        }
                    }
                };
                assert_eq!(
                    std::fs::read_to_string(&outside).map_err(storage)?,
                    if access.is_none() {
                        "changed"
                    } else {
                        "original"
                    }
                );
                assert_eq!(
                    inside.exists(),
                    !refused && access != Some(ExecutionAccess::ReadOnly)
                );
            }
        }
    }
    Ok(())
}
