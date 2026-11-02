use super::*;
use crate::{Capabilities, OperationId, conversation::Limits};

#[test]
fn context_free_gate_rejects_even_valid_source_grants() -> Result<()> {
    let file = crate::tool::tests::file(b"exact")?;
    let missing = RuntimeScope::new(Capabilities::default(), Limits::default())?;
    let grants = Capabilities::new([file.read_capability(&mut None)?]);
    let granted = RuntimeScope::new(grants.clone(), Limits::default())?;
    let mut invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "preflight".into(),
        name: String::new(),
        arguments: Value::Null,
    };
    for (tool, arguments) in [
        (
            read()?,
            json!({"path":file.path(),"version":serde_json::to_string(&file).unwrap()}),
        ),
        (
            edit()?,
            json!({"path":file.path(),"version":serde_json::to_string(&file).unwrap(),"old_text":"exact","new_text":"changed"}),
        ),
        (
            patch_file(4096, 16)?,
            json!({"file":file,"diff":"@@\n-exact\n+changed\n"}),
        ),
    ] {
        invocation.name = tool.definition.name.clone();
        invocation.arguments = arguments;
        for scope in [None, Some(&missing), Some(&granted)] {
            assert!(matches!(
                tool.executor.authorize(scope, None, &invocation, None),
                Err(Error::Unauthorized(_))
            ));
        }
    }
    invocation.name = "read".into();
    invocation.arguments =
        json!({"path":file.path(),"version":serde_json::to_string(&file).unwrap()});
    let narrow = RuntimeScope::new(
        grants,
        Limits {
            render_bytes: 4,
            ..Limits::default()
        },
    )?;
    assert!(matches!(
        read()?
            .executor
            .authorize(Some(&narrow), None, &invocation, None),
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}

#[test]
fn publication_template_rejects_missing_scope_and_invalid_path_before_dispatch() -> Result<()> {
    let tool = write()?;
    let scope = RuntimeScope::new(Capabilities::default(), Limits::default())?;
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "write".into(),
        name: tool.definition.name.clone(),
        arguments: json!({"path":"../escape.txt","content":"body"}),
    };
    assert!(matches!(
        tool.executor.authorize(None, None, &invocation, None),
        Err(Error::Unauthorized(_))
    ));
    assert!(
        tool.executor
            .authorize(Some(&scope), None, &invocation, None)
            .is_err()
    );
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn native_write_recovery_observes_the_original_receipt_without_republishing() -> Result<()> {
    use crate::{conversation::VolumeOperation, runtime::Bindings};

    let project =
        crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65536).await?;
    let home = crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65536).await?;
    let mut grants = vec!["tool:call:read".into(), "tool:call:write".into()];
    for storage in [&project, &home] {
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            grants.push(storage.volume().capability(operation, &mut None)?);
        }
    }
    let limits = Limits {
        file_bytes: 65536,
        render_bytes: 4096,
        ..Limits::default()
    };
    let task = Arc::new(
        Bindings {
            scope: RuntimeScope::new(Capabilities::new(grants), limits)?,
            content: Some(project.content_bindings()),
            artifacts: Some(home.content_bindings()),
            ..Bindings::local()
        }
        .build()?
        .agent_context(&Capabilities::default(), home.content_bindings())?,
    );
    for (prefix, storage) in [("", &project), (crate::layers::HOME_PATH, &home)] {
        for mode in ["omitted", "any", "absent", "content", "authored"] {
            let relative = format!("{mode}.txt");
            let path = format!("{prefix}{relative}");
            let mut arguments = json!({"path":path,"content":"committed"});
            if matches!(mode, "content" | "authored") {
                let source = storage
                    .stage(
                        OperationId::new(),
                        &relative,
                        b"original",
                        "text/plain",
                        &relative,
                    )
                    .await?;
                let version = if mode == "content" {
                    encode_version(&source, None)?.0
                } else {
                    let invocation = ToolInvocation {
                        operation_id: OperationId::new(),
                        call_id: "source".into(),
                        name: "read".into(),
                        arguments: json!({"path":path}),
                    };
                    let context = ToolContext::new(
                        task.clone(),
                        invocation.operation_id,
                        &invocation.call_id,
                    )?;
                    let result = read()?.executor.execute(invocation, Some(context)).await?;
                    let source: ReadFileResult = serde_json::from_value(result.value)
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                    assert!(matches!(source.file, FileReadSource::Authored(_)));
                    super::version::encode_source(&source.file, None)?.0
                };
                arguments["ifVersion"] = json!(version);
            } else if mode != "omitted" {
                arguments["ifVersion"] = json!(mode);
            }
            let invocation = ToolInvocation {
                operation_id: OperationId::new(),
                call_id: format!("write-{mode}"),
                name: "write".into(),
                arguments,
            };
            // Each original invocation owns its ledger; all of its recovery
            // attempts below retain that same ledger and cancellation scope.
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(100_000, 16_000_000),
                &acyclic_fs::CancellationToken::new(),
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            eprintln!("native recovery case: {path} ({mode})");
            let context =
                ToolContext::new(task.clone(), invocation.operation_id, &invocation.call_id)?;
            // An admitted source is not evidence that the write committed.
            assert!(
                write()?
                    .executor
                    .reconcile_with_native_context(&context, &invocation, &work)?
                    .await?
                    .0
                    .is_none()
            );
            let expected = {
                let (result, _custody) = write()?
                    .executor
                    .execute_with_native_context(&context, &invocation, &work)?
                    .await?;
                result
            };
            // Lose the caller's completion, and let an unrelated publication win.
            drop(context);
            let later = storage
                .stage(
                    OperationId::new(),
                    &relative,
                    b"later",
                    "text/plain",
                    &relative,
                )
                .await?;
            let before = storage.list_private_directory("", None, None, 1).await?.0;
            let restarted =
                ToolContext::new(task.clone(), invocation.operation_id, &invocation.call_id)?;
            if matches!(mode, "content" | "authored") {
                // The former unconditional lookup cannot recover this write.
                assert!(
                    restarted
                        .observe_tool_file_once(invocation.operation_id, None)
                        .await?
                        .0
                        .is_none()
                );
            }
            let (recovered, _custody) = write()?
                .executor
                .reconcile_with_native_context(&restarted, &invocation, &work)?
                .await?;
            assert_eq!(recovered, Some(expected), "{path}");
            assert_eq!(
                storage.list_private_directory("", None, None, 1).await?.0,
                before
            );
            assert_eq!(storage.read(&later).await?.as_ref(), b"later");

            let denied = ToolContext::new(
                Arc::new(task.scoped(Capabilities::new(["tool:call:write"]), limits)?),
                invocation.operation_id,
                &invocation.call_id,
            )?;
            assert_eq!(
                write()?
                    .executor
                    .reconcile_with_native_context(&denied, &invocation, &work)?
                    .await
                    .unwrap_err()
                    .code(),
                crate::wire::ErrorCode::Unauthorized
            );
            if matches!(mode, "content" | "authored") {
                let mut forged = invocation.clone();
                forged.arguments["path"] = json!(format!("{prefix}other.txt"));
                assert!(
                    write()?
                        .executor
                        .reconcile_with_native_context(&restarted, &forged, &work)?
                        .await
                        .is_err()
                );
            }
        }
    }
    Ok(())
}
