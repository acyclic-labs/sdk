use super::*;
use crate::{Capabilities, OperationId, conversation::Limits};

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn effective_reads_retain_the_original_layer_and_generation() -> Result<()> {
    use crate::{
        conversation::VolumeOperation,
        runtime::{Bindings, TaskContext},
    };

    async fn read_at(task: Arc<TaskContext>, arguments: Value) -> Result<ReadFileResult> {
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "layer-read".into(),
            name: "read".into(),
            arguments,
        };
        let context = ToolContext::new(task, invocation.operation_id, &invocation.call_id)?;
        let result = read()?.executor.execute(invocation, Some(context)).await?;
        serde_json::from_value(result.value).map_err(|error| Error::Invalid(error.to_string()))
    }

    let seed = crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65536).await?;
    let home = crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65536).await?;
    let mut grants = vec!["tool:call:read".into()];
    for storage in [&seed, &home] {
        grants.push(
            storage
                .volume()
                .capability(VolumeOperation::Read, &mut None)?,
        );
    }
    let task = Arc::new(
        Bindings {
            scope: RuntimeScope::new(
                Capabilities::new(grants),
                Limits {
                    file_bytes: 65536,
                    render_bytes: 4096,
                    ..Limits::default()
                },
            )?,
            content: Some(seed.content_bindings()),
            artifacts: Some(home.content_bindings()),
            seed: Some(seed.volume().clone()),
            ..Bindings::local()
        }
        .build()?
        .agent_context(&Capabilities::default(), home.content_bindings())?,
    );

    seed.stage(
        OperationId::new(),
        "shared.md",
        b"seed\n",
        "text/plain",
        "shared.md",
    )
    .await?;
    let original_content = home
        .stage(
            OperationId::new(),
            "mine.md",
            b"first\nsecond\n",
            "text/plain",
            "mine.md",
        )
        .await?;
    let effective = ".volumes/effective/mine.md";
    let original = read_at(task.clone(), json!({"path": effective})).await?;
    assert_eq!(original.text, "first\nsecond\n");
    let version = super::version::encode_source(&original.file, None)?.0;
    super::version::check_version(version.as_bytes());
    let content_version = super::version::encode_version(&original_content, None)?.0;
    super::version::check_version(content_version.as_bytes());
    home.stage(
        OperationId::new(),
        "mine.md",
        b"off: true",
        "text/plain",
        "mine.md",
    )
    .await?;
    assert_eq!(
        read_at(task.clone(), json!({"path":effective}))
            .await
            .unwrap_err()
            .code(),
        crate::wire::ErrorCode::NotFound
    );
    // A resumed byte or line projection must not re-resolve today's off marker.
    for selection in [
        json!({"kind":"text"}),
        json!({"kind":"lines","offset":1,"limit":1}),
    ] {
        let result = read_at(
            task.clone(),
            json!({"path":effective,"version":version,"selection":selection}),
        )
        .await?;
        assert_eq!(result.file, original.file);
        assert_eq!(
            result.text,
            if selection["kind"] == "lines" {
                "second\n"
            } else {
                "first\nsecond\n"
            }
        );
    }

    let shared = ".volumes/effective/shared.md";
    let original_seed = read_at(task.clone(), json!({"path":shared})).await?;
    assert_eq!(original_seed.text, "seed\n");
    let seed_version = super::version::encode_source(&original_seed.file, None)?.0;
    home.stage(
        OperationId::new(),
        "shared.md",
        b"home\n",
        "text/plain",
        "shared.md",
    )
    .await?;
    assert_eq!(
        read_at(task.clone(), json!({"path":shared})).await?.text,
        "home\n"
    );
    let retained = read_at(task.clone(), json!({"path":shared,"version":seed_version})).await?;
    assert_eq!(retained, original_seed);
    assert!(
        read_at(task.clone(), json!({"path":shared,"version":version}))
            .await
            .is_err()
    );
    let denied =
        Arc::new(task.scoped(Capabilities::new(["tool:call:read"]), task.scope().limits())?);
    assert_eq!(
        read_at(denied, json!({"path":effective,"version":version}))
            .await
            .unwrap_err()
            .code(),
        crate::wire::ErrorCode::Unauthorized
    );

    // The first projection also keeps the probe's cut when home advances before it reads.
    let context = ToolContext::new(task, OperationId::new(), "retained-probe")?;
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(100_000, 16_000_000),
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::executor::PreparationIo::from_native_work(&work)?;
    let (layer, selected, hold) = context.effective_tool_path(shared, Some(&io)).await?;
    let selected = selected.expect("home supplied the original selection");
    home.stage(
        OperationId::new(),
        "shared.md",
        b"later\n",
        "text/plain",
        "shared.md",
    )
    .await?;
    let input = ReadFileInput {
        path: layer.into_owned(),
        version: None,
        range: None,
        selection: None,
    };
    let window = super::window::Window::first(
        &context,
        &input,
        Some((
            super::version::SelectedVersion::Authored(crate::conversation::PathSelection {
                volume: selected.volume,
                path: selected.path,
                generation: selected.generation,
            }),
            hold,
        )),
        acyclic_fs::ByteRange {
            offset: 0,
            length: 32,
        },
        Some(&work),
        Some(&io),
    )
    .await?;
    assert_eq!(&*window.bytes, b"home\n");
    Ok(())
}

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
    // A single-volume publisher must never interpret a read-only public view
    // as a literal writable path, regardless of the conditional-write mode.
    for prefix in [crate::layers::EFFECTIVE_PATH, crate::layers::SEED_PATH] {
        for mode in ["omitted", "any", "absent"] {
            let mut arguments =
                json!({"path":format!("{prefix}example.txt"),"content":"forbidden"});
            if mode != "omitted" {
                arguments["ifVersion"] = json!(mode);
            }
            let invocation = ToolInvocation {
                operation_id: OperationId::new(),
                call_id: format!("read-only-{mode}"),
                name: "write".into(),
                arguments,
            };
            let context =
                ToolContext::new(task.clone(), invocation.operation_id, &invocation.call_id)?;
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(100_000, 16_000_000),
                &acyclic_fs::CancellationToken::new(),
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let before = work.receipt();
            assert_eq!(
                write()?
                    .executor
                    .authorize(
                        Some(context.effective_scope()),
                        Some(&context),
                        &invocation,
                        Some(&work)
                    )
                    .unwrap_err()
                    .code(),
                crate::wire::ErrorCode::Unauthorized,
            );
            assert_eq!(
                write()?
                    .executor
                    .execute(invocation, Some(context))
                    .await
                    .unwrap_err()
                    .code(),
                crate::wire::ErrorCode::Unauthorized,
            );
            let after = work.receipt();
            assert_eq!(
                after.backend_write_operations,
                before.backend_write_operations
            );
            assert_eq!(
                after.authority_records_appended,
                before.authority_records_appended
            );
            assert_eq!(after.durability_operations, before.durability_operations);
        }
    }
    for storage in [&project, &home] {
        assert!(
            storage
                .list_private_directory("", None, None, 1)
                .await?
                .1
                .entries
                .is_empty()
        );
    }
    for (prefix, storage) in [("", &project), (crate::layers::HOME_PATH, &home)] {
        for mode in [
            "omitted",
            "any",
            "absent",
            "content",
            "authored",
            "namespace",
        ] {
            let relative = format!("{mode}.txt");
            let path = format!("{prefix}{relative}");
            let mut arguments = json!({"path":path,"content":"committed"});
            if matches!(mode, "content" | "authored" | "namespace") {
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
                } else if mode == "namespace" {
                    // The existing directory producer supplies the generation;
                    // the version needs no file-body read, length or digest.
                    let (generation, page) =
                        storage.list_private_directory("", None, None, 128).await?;
                    assert!(
                        page.entries
                            .iter()
                            .any(|entry| entry.name.as_bytes() == relative.as_bytes())
                    );
                    encode_path_version(storage.volume(), &relative, &generation, None)?.0
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
            if !prefix.is_empty() {
                // Project authority never authorizes the same public home path.
                let project_only = ToolContext::new(
                    Arc::new(
                        task.scoped(
                            Capabilities::new([
                                "tool:call:write".into(),
                                project
                                    .volume()
                                    .capability(VolumeOperation::Read, &mut None)?,
                                project
                                    .volume()
                                    .capability(VolumeOperation::Write, &mut None)?,
                            ]),
                            limits,
                        )?,
                    ),
                    invocation.operation_id,
                    &invocation.call_id,
                )?;
                assert_eq!(
                    write()?
                        .executor
                        .authorize(
                            Some(project_only.effective_scope()),
                            Some(&project_only),
                            &invocation,
                            None,
                        )
                        .unwrap_err()
                        .code(),
                    crate::wire::ErrorCode::Unauthorized,
                );
            }
            // Recovery owns one ledger across its receipt checks. Publication
            // setup uses the real executor without consuming this read budget.
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
                    .await
                    .map_err(|error| {
                        eprintln!(
                            "missing-receipt observation failed: {error:?}; work={:?}",
                            work.receipt()
                        );
                        error
                    })?
                    .0
                    .is_none()
            );
            let expected = write()?
                .executor
                .execute(invocation.clone(), Some(context))
                .await?;
            let published: FileResult = serde_json::from_value(expected.value.clone())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            assert_eq!(published.file.volume(), storage.volume());
            assert_eq!(published.file.path(), relative);
            assert_eq!(storage.read(&published.file).await?.as_ref(), b"committed");
            let read_invocation = ToolInvocation {
                operation_id: OperationId::new(),
                call_id: "read-after-write".into(),
                name: "read".into(),
                arguments: json!({"path":path}),
            };
            let read_context = ToolContext::new(
                task.clone(),
                read_invocation.operation_id,
                &read_invocation.call_id,
            )?;
            assert_eq!(
                read()?
                    .executor
                    .execute(read_invocation, Some(read_context))
                    .await?
                    .value["text"],
                "committed"
            );
            if mode == "absent" {
                let duplicate = ToolInvocation {
                    operation_id: OperationId::new(),
                    call_id: "duplicate".into(),
                    ..invocation.clone()
                };
                let duplicate_context =
                    ToolContext::new(task.clone(), duplicate.operation_id, &duplicate.call_id)?;
                assert_eq!(
                    write()?
                        .executor
                        .execute(duplicate, Some(duplicate_context))
                        .await
                        .unwrap_err()
                        .code(),
                    crate::wire::ErrorCode::Conflict,
                );
            }
            // Lose the caller's completion, and let an unrelated publication win.
            let later = storage
                .stage(
                    OperationId::new(),
                    &relative,
                    b"later",
                    "text/plain",
                    &relative,
                )
                .await?;
            if mode == "namespace" {
                // A token from a directory page must keep the original file,
                // even after both replacement publications changed its head.
                let pinned = ToolInvocation {
                    operation_id: OperationId::new(),
                    call_id: "namespace-pinned-read".into(),
                    name: "read".into(),
                    arguments: json!({"path":path,"version":invocation.arguments["ifVersion"]}),
                };
                let pinned_context =
                    ToolContext::new(task.clone(), pinned.operation_id, &pinned.call_id)?;
                assert_eq!(
                    read()?
                        .executor
                        .execute(pinned, Some(pinned_context))
                        .await?
                        .value["text"],
                    "original"
                );
            }
            let before = storage.list_private_directory("", None, None, 1).await?.0;
            let restarted =
                ToolContext::new(task.clone(), invocation.operation_id, &invocation.call_id)?;
            if matches!(mode, "content" | "authored" | "namespace") {
                // The former unconditional lookup cannot recover a replacement.
                assert!(
                    restarted
                        .observe_tool_file_once(invocation.operation_id, None)
                        .await?
                        .0
                        .is_none()
                );
            }
            let work_before = work.receipt();
            let (recovered, _custody) = write()?
                .executor
                .reconcile_with_native_context(&restarted, &invocation, &work)?
                .await
                .map_err(|error| {
                    eprintln!(
                        "original-receipt observation failed: {error:?}; work={:?}",
                        work.receipt()
                    );
                    error
                })?;
            assert_eq!(recovered, Some(expected), "{path}");
            let work_after = work.receipt();
            assert_eq!(
                work_after.backend_write_operations,
                work_before.backend_write_operations
            );
            assert_eq!(
                work_after.authority_records_appended,
                work_before.authority_records_appended
            );
            assert_eq!(
                work_after.durability_operations,
                work_before.durability_operations
            );
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
            if matches!(mode, "content" | "authored" | "namespace") {
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
