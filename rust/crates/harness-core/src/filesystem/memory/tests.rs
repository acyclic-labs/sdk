use super::*;
use crate::conversation::ContentGrant;

const FINITE_CONTENT_BUDGET: acyclic_fs::WorkBudget =
    acyclic_fs::performance::work_budget(100_000, 16_000_000);

mod access;
mod approvals;
mod content;
mod publication_cost;
mod publication_evidence;

async fn run_admitted_file_case<F, Fut>(
    storage: &MemoryHarnessStorage,
    limits: Limits,
    handler: F,
) -> Result<()>
where
    F: Fn(crate::runtime::TaskContext) -> Fut + acyclic_stream::ProviderPlatform + 'static,
    Fut: std::future::Future<Output = Result<()>> + acyclic_stream::ProviderTask + 'static,
{
    use crate::runtime::Bindings;
    let mut bindings = Bindings::local();
    bindings.scope = RuntimeScope::new(
        Capabilities::new([
            storage.read_capability.clone(),
            storage.write_capability.clone(),
            "tool:call:read".into(),
            "tool:call:write".into(),
            "tool:call:edit".into(),
            "tool:call:acyclic.list_files".into(),
        ]),
        limits,
    )?;
    let content = storage.content_bindings();
    bindings.content = Some(content.clone());
    let context = bindings
        .build()?
        .agent_context(&Capabilities::new(Vec::<String>::new()), content)?;
    handler(context).await
}

#[tokio::test]
async fn default_read_file_tool_checks_owner_and_render_bound() -> Result<()> {
    let owner = MemoryHarnessStorage::new(AgentId::new(), 4_096).await?;
    let other = MemoryHarnessStorage::new(AgentId::new(), 4_096).await?;
    let file = owner
        .stage(
            OperationId::new(),
            "notes/one.txt",
            b"pinned text",
            "text/plain",
            "one.txt",
        )
        .await?;
    let expected_projection = json!({"kind":"json","value":{"path":file.path(),"version":serde_json::to_string(&file).unwrap(),"text":"pinned text"}});
    let render_bytes = serde_json::to_vec(&expected_projection)
        .map_err(|error| Error::Invalid(error.to_string()))?
        .len() as u64;
    let limits = Limits {
        file_bytes: 4_096,
        render_bytes,
        ..Limits::default()
    };
    let tool = crate::tools::defaults()?
        .get("read", None, None)?
        .ok_or_else(|| Error::NotFound("read tool".into()))?
        .clone();
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "read-1".into(),
        name: "read".into(),
        arguments: json!({"path":file.path(),"version":serde_json::to_string(&file).unwrap()}),
    };
    let own_tool = tool.clone();
    let own_invocation = invocation.clone();
    run_admitted_file_case(&owner, limits, move |task| {
        let tool = own_tool.clone();
        let invocation = own_invocation.clone();
        let expected_projection = expected_projection.clone();
        async move {
            let context = crate::runtime::ToolContext::new(
                Arc::new(task.clone()),
                invocation.operation_id,
                &invocation.call_id,
            )?;
            let result = tool
                .executor
                .execute(invocation.clone(), Some(context.clone()))
                .await?;
            assert_eq!(result.value["text"], "pinned text");
            assert_eq!(result.value["file"]["path"], "notes/one.txt");
            let projection = tool.projection.project(&invocation, &result, None)?.0;
            assert_eq!(projection, expected_projection);
            let content = tool
                .definition
                .validate_projection(&projection, limits, None)?;
            content.validate_limits(limits, &mut None)?;
            assert!(matches!(
                content.validate_limits(
                    Limits {
                        render_bytes: render_bytes - 1,
                        ..limits
                    },
                    &mut None
                ),
                Err(Error::Invalid(_))
            ));
            assert_eq!(
                tool.executor
                    .reconcile(invocation.clone(), Some(context))
                    .await?,
                Some(result)
            );
            let narrowed = task.scoped(
                task.scope().grants().clone(),
                Limits {
                    render_bytes: 4,
                    ..limits
                },
            )?;
            let context = crate::runtime::ToolContext::new(
                Arc::new(narrowed),
                invocation.operation_id,
                &invocation.call_id,
            )?;
            assert!(matches!(
                tool.executor.execute(invocation, Some(context)).await,
                Err(Error::Invalid(_))
            ));
            Ok(())
        }
    })
    .await?;
    run_admitted_file_case(&other, limits, move |task| {
        let tool = tool.clone();
        let invocation = invocation.clone();
        async move {
            let context = crate::runtime::ToolContext::new(
                Arc::new(task),
                invocation.operation_id,
                &invocation.call_id,
            )?;
            assert!(matches!(
                tool.executor.execute(invocation, Some(context)).await,
                Err(Error::Unauthorized(_)) | Err(Error::NotFound(_))
            ));
            Ok(())
        }
    })
    .await
}

#[tokio::test]
async fn default_write_reconciles_original_receipt_and_rejects_changed_retry() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 4_096).await?;
    let limits = Limits {
        file_bytes: 4_096,
        render_bytes: 4_096,
        ..Limits::default()
    };
    let tool = crate::tools::defaults()?
        .get("write", None, None)?
        .ok_or_else(|| Error::NotFound("write tool".into()))?
        .clone();
    run_admitted_file_case(&storage, limits, move |task| {
        let tool = tool.clone();
        async move {
            let invocation = ToolInvocation {
                operation_id: OperationId::new(),
                call_id: "write-1".into(),
                name: "write".into(),
                arguments: json!({"path":"notes/one.txt","content":"saved text"}),
            };
            let context = crate::runtime::ToolContext::new(
                Arc::new(task.clone()),
                invocation.operation_id,
                &invocation.call_id,
            )?;
            let first = tool
                .executor
                .execute(invocation.clone(), Some(context.clone()))
                .await?;
            let file: FileRef = serde_json::from_value(first.value["file"].clone())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let user = task
                .stage_file(
                    None,
                    OperationId::new(),
                    "notes/one.txt",
                    b"later user edit",
                    "text/plain",
                    "one.txt",
                )
                .await?;
            assert_eq!(
                tool.executor
                    .reconcile(invocation.clone(), Some(context.clone()))
                    .await?,
                Some(first)
            );
            assert_eq!(
                task.read_file(&file, None).await?.as_ref(),
                b"saved text".as_ref()
            );
            assert_eq!(
                task.read_file(&user, None).await?.as_ref(),
                b"later user edit".as_ref()
            );
            for (field, value) in [("content", "different text"), ("path", "notes/another.txt")] {
                let mut changed = invocation.clone();
                changed.arguments[field] = json!(value);
                assert!(matches!(
                    tool.executor
                        .reconcile(changed, Some(context.clone()))
                        .await,
                    Err(Error::Conflict(_))
                ));
            }
            let ordinary = OperationId::new();
            for path in ["multi/a.txt", "multi/b.txt"] {
                task.stage_file(
                    None,
                    ordinary,
                    path,
                    b"ordinary",
                    "text/plain",
                    "ordinary.txt",
                )
                .await?;
            }
            Ok(())
        }
    })
    .await
}

#[tokio::test]
async fn owner_private_directory_is_lazy_paged_and_generation_pinned() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 4_096).await?;
    storage
        .stage(
            OperationId::new(),
            "notes/one.txt",
            b"one",
            "text/plain",
            "one.txt",
        )
        .await?;
    let (generation, root) = storage.list_private_directory("", None, None, 8).await?;
    assert!(root.entries.iter().any(|entry| {
        entry
            .name
            .unicode_text(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .ok()
            .map(|receipt| receipt.value)
            .as_deref()
            == Some("notes")
    }));
    assert!(!root.entries.iter().any(|entry| {
        entry
            .name
            .unicode_text(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .ok()
            .map(|receipt| receipt.value)
            .as_deref()
            == Some(".system")
    }));
    let (_, notes) = storage
        .list_private_directory("notes", Some(&generation), None, 8)
        .await?;
    assert!(notes.entries.iter().any(|entry| {
        entry
            .name
            .unicode_text(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .ok()
            .map(|receipt| receipt.value)
            .as_deref()
            == Some("one.txt")
    }));
    let limits = Limits {
        file_bytes: 4_096,
        render_bytes: 1_024,
        ..Limits::default()
    };
    let tool = storage.list_files_tool(limits)?;
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "list-1".into(),
        name: "acyclic.list_files".into(),
        arguments: json!({"path": "notes", "maximum_entries": 65}),
    };
    let page = tool.executor.execute(invocation.clone(), None).await?;
    assert_eq!(page.value["entries"][0]["name"], "one.txt");
    storage
        .stage(
            OperationId::new(),
            "notes/two.txt",
            b"two",
            "text/plain",
            "two.txt",
        )
        .await?;
    assert_eq!(tool.executor.reconcile(invocation, None).await?, Some(page));
    let (_, pinned_notes) = storage
        .list_private_directory("notes", Some(&generation), None, 8)
        .await?;
    assert_eq!(pinned_notes.entries.len(), 1);
    assert_eq!(
        pinned_notes.entries[0]
            .name
            .unicode_text(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new()
            )
            .ok()
            .map(|receipt| receipt.value)
            .as_deref(),
        Some("one.txt")
    );
    let (_, current_notes) = storage
        .list_private_directory("notes", None, None, 8)
        .await?;
    assert_eq!(current_notes.entries.len(), 2);
    Ok(())
}

#[tokio::test]
async fn local_private_volume_identity_is_unique_per_conversation() -> Result<()> {
    let agent = AgentId::new();
    let first = MemoryHarnessStorage::new(agent, 4_096).await?;
    let second = MemoryHarnessStorage::new(agent, 4_096).await?;
    assert_ne!(first.volume(), second.volume());
    let file = first
        .stage(
            OperationId::new(),
            "notes/one.txt",
            b"private",
            "text/plain",
            "one.txt",
        )
        .await?;
    assert!(matches!(
        second.read(&file).await,
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}

#[tokio::test]
async fn default_file_tools_require_caller_volume_grants() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 4_096).await?;
    let limits = Limits {
        file_bytes: 4_096,
        render_bytes: 4_096,
        ..Limits::default()
    };
    let mut tools = crate::tools::defaults()?;
    tools.register((storage.list_files_tool(limits)?, None), &mut None, None)?;
    let file = storage
        .stage(
            OperationId::new(),
            "notes/one.txt",
            b"one",
            "text/plain",
            "one.txt",
        )
        .await?;
    run_admitted_file_case(&storage, limits, move |task| {
        let tools = tools.clone();
        let file = file.clone();
        async move {
            let task = task.scoped(
                Capabilities::new([
                    "tool:call:read",
                    "tool:call:write",
                    "tool:call:edit",
                    "tool:call:acyclic.list_files",
                ]),
                limits,
            )?;
            for (name, arguments) in [
                ("read", json!({"path":file.path(),"version":serde_json::to_string(&file).unwrap()})),
                ("write", json!({"path":"notes/two.txt","content":"two"})),
                ("edit", json!({"path":file.path(),"version":serde_json::to_string(&file).unwrap(),"old_text":"one","new_text":"two"})),
            ] {
                let invocation = ToolInvocation {
                    operation_id: OperationId::new(),
                    call_id: name.into(),
                    name: name.into(),
                    arguments,
                };
                let tool = tools
                    .get(name, None, None)?
                    .ok_or_else(|| Error::NotFound(name.into()))?;
                let context = crate::runtime::ToolContext::new(
                    Arc::new(task.clone()),
                    invocation.operation_id,
                    &invocation.call_id,
                )?;
                assert!(matches!(
                    tool.executor.execute(invocation, Some(context)).await,
                    Err(Error::Unauthorized(_))
                ));
            }
            let invocation = ToolInvocation {
                operation_id: OperationId::new(),
                call_id: "list".into(),
                name: "acyclic.list_files".into(),
                arguments: json!({"path":"","maximum_entries":8}),
            };
            assert!(matches!(
                tools
                    .get("acyclic.list_files", None, None)?
                    .ok_or_else(|| Error::NotFound("list tool".into()))?
                    .executor
                    .authorize(Some(task.scope()), None, &invocation, None),
                Err(Error::Unauthorized(_))
            ));
            Ok(())
        }
    })
    .await
}

#[tokio::test]
async fn path_reads_and_edits_keep_original_grants_and_pinned_replay() -> Result<()> {
    async fn call(
        task: &crate::runtime::TaskContext,
        tool: &crate::tool::Tool,
        arguments: Value,
    ) -> Result<(ToolInvocation, crate::tool::ToolResult)> {
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "path-file".into(),
            name: tool.definition.name.clone(),
            arguments,
        };
        let context = crate::runtime::ToolContext::new(
            Arc::new(task.clone()),
            invocation.operation_id,
            &invocation.call_id,
        )?;
        let result = tool
            .executor
            .execute(invocation.clone(), Some(context))
            .await?;
        Ok((invocation, result))
    }
    let storage = MemoryHarnessStorage::new(AgentId::new(), 4_096).await?;
    storage
        .stage(
            OperationId::new(),
            "notes/one.txt",
            b"one",
            "text/plain",
            "one.txt",
        )
        .await?;
    let limits = Limits {
        file_bytes: 4_096,
        render_bytes: 4_096,
        ..Limits::default()
    };
    let mut readonly_project = storage.content_bindings();
    readonly_project.writer = None;
    let other_private = MemoryHarnessStorage::new(AgentId::new(), 4_096)
        .await?
        .content_bindings();
    run_admitted_file_case(&storage, limits, move |task| {
        let readonly_project = readonly_project.clone();
        let other_private = other_private.clone();
        async move {
        let read = crate::tool::files::read()?;
        let edit = crate::tool::files::edit()?;
        let readonly = crate::runtime::Bindings {
            scope: task.scope().clone(),
            content: Some(readonly_project),
            ..crate::runtime::Bindings::local()
        }.build()?.agent_context(&Capabilities::default(), other_private)?;
        assert_eq!(call(&readonly, &read, json!({"path":"notes/one.txt"})).await?.1.value["text"], "one");
        assert_eq!(call(&readonly, &edit, json!({"path":"notes/one.txt","old_text":"one","new_text":"denied"})).await.unwrap_err().code(), crate::wire::ErrorCode::Unauthorized);
        let (invocation, result) = call(&task, &read, json!({"path":"notes/one.txt"})).await?;
        assert_eq!(result.value["text"], "one");
        let projected = read.projection.project(&invocation, &result, None)?.0;
        let version = projected["value"]["version"].as_str().expect("opaque version");
        let source: FileRef = serde_json::from_str(version).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(source.path(), "notes/one.txt");
        assert_eq!(task.read_file(&source, None).await?.as_ref(), b"one");
        assert_eq!(call(&task, &read, json!({"path":"notes/one.txt","version":version})).await?.1.value, result.value);

        for arguments in [
            json!({"path":"notes/other.txt","version":version}),
            json!({"path":"notes/one.txt","version":"invented"}),
            json!({"path":".system/private"}),
        ] {
            assert!(call(&task, &read, arguments).await.is_err());
        }
        let denied = task.scoped(Capabilities::new(["tool:call:read", "tool:call:edit"]), limits)?;
        for arguments in [json!({"path":"notes/one.txt"}), json!({"path":"notes/one.txt","version":version})] {
            assert_eq!(call(&denied, &read, arguments).await.unwrap_err().code(), crate::wire::ErrorCode::Unauthorized);
        }
        assert_eq!(call(&denied, &edit, json!({"path":"notes/one.txt","version":version,"old_text":"one","new_text":"denied"})).await.unwrap_err().code(), crate::wire::ErrorCode::Unauthorized);

        task.stage_file(None, OperationId::new(), "notes/one.txt", b"two", "text/plain", "one.txt").await?;
        assert_eq!(call(&task, &edit, json!({"path":"notes/one.txt","version":version,"old_text":"one","new_text":"stale"})).await.unwrap_err().code(), crate::wire::ErrorCode::Conflict);
        let (invocation, result) = call(&task, &edit, json!({"path":"notes/one.txt","old_text":"two","new_text":"three"})).await?;
        assert_eq!(call(&task, &read, json!({"path":"notes/one.txt"})).await?.1.value["text"], "three");
        // Replay observes the original publication, even after the path moves.
        task.stage_file(None, OperationId::new(), "notes/one.txt", b"later", "text/plain", "one.txt").await?;
        let context = crate::runtime::ToolContext::new(Arc::new(task.clone()), invocation.operation_id, &invocation.call_id)?;
        assert_eq!(edit.executor.reconcile(invocation, Some(context)).await?, Some(result));
        assert_eq!(call(&task, &read, json!({"path":"notes/one.txt"})).await?.1.value["text"], "later");
        Ok(())
    }}).await
}

#[tokio::test]
async fn context_files_stage_and_resolve_only_with_owner_bound_grants() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::from_bytes([91; 16]), 4_096).await?;
    run_admitted_file_case(&storage, Limits::default(), |context| async move {
        let file = context
            .stage_file(
                None,
                OperationId::from_bytes([92; 16]),
                "task/result.txt",
                b"pinned bytes",
                "text/plain",
                "result.txt",
            )
            .await?;
        assert_eq!(
            context.read_file(&file, None).await?.as_ref(),
            b"pinned bytes"
        );
        let narrowed =
            context.scoped(Capabilities::new(Vec::<String>::new()), Limits::default())?;
        assert!(narrowed.read_file(&file, None).await.is_err());
        assert!(
            narrowed
                .stage_file(
                    None,
                    OperationId::from_bytes([93; 16]),
                    "task/denied.txt",
                    b"denied",
                    "text/plain",
                    "denied.txt"
                )
                .await
                .is_err()
        );
        Ok(())
    })
    .await
}
