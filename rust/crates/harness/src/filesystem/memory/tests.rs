use super::*;
use crate::conversation::ContentGrant;
use crate::{
    Outcome,
    model::{Model, ModelEvent},
    runtime::{TaskDefinition, TaskRegistry},
};
use serde_json::Value;

const FINITE_CONTENT_BUDGET: acyclic_fs::WorkBudget =
    acyclic_fs::performance::work_budget(100_000, 16_000_000);

use crate::test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn};

mod content;

fn synthetic_token_count(
    request: &crate::model::PreparedModelRequest,
) -> Result<crate::context::ModelTokenCount> {
    let message_tokens = request
        .request()
        .messages
        .iter()
        .map(|message| {
            let bytes = message.content.file_refs().try_fold(
                crate::contract::canonical_json_bytes(message)?.len() as u64,
                |total, file| {
                    total
                        .checked_add(file.descriptor().byte_length())
                        .ok_or_else(|| Error::Invalid("synthetic token count overflows".into()))
                },
            )?;
            u32::try_from(bytes)
                .map_err(|_| Error::Invalid("synthetic token count exceeds u32".into()))
        })
        .collect::<Result<Vec<_>>>()?;
    let fixed = crate::contract::canonical_json_bytes(&request.request().tools)?.len();
    Ok(crate::context::ModelTokenCount::from_upper_bounds(
        request.request_digest(),
        u32::try_from(fixed).map_err(|_| Error::Invalid("synthetic framing exceeds u32".into()))?,
        message_tokens,
    ))
}

fn text_model(turns: usize) -> ScriptedModelProvider {
    ScriptedModelProvider::new(std::iter::repeat_n(
        ScriptedTurn::Text("local response".into()),
        turns,
    ))
    .with_token_counter(synthetic_token_count)
}

async fn run_admitted_file_case<F, Fut>(
    storage: &MemoryHarnessStorage,
    limits: Limits,
    handler: F,
) -> Result<()>
where
    F: Fn(crate::runtime::TaskContext) -> Fut + acyclic_stream::ProviderPlatform + 'static,
    Fut: std::future::Future<Output = Result<()>> + acyclic_stream::ProviderTask + 'static,
{
    use crate::runtime::{Bindings, ContentBindings};
    let mut bindings = Bindings::local();
    bindings.scope = RuntimeScope::new(
        Capabilities::new([
            storage.read_capability.clone(),
            storage.write_capability.clone(),
            "task:spawn:file_tool_case@1".into(),
            "tool:call:read".into(),
            "tool:call:write".into(),
            "tool:call:edit".into(),
            "tool:call:acyclic.list_files".into(),
            "tool:call:acyclic.fork".into(),
            "tool:call:acyclic.merge".into(),
        ]),
        limits,
    )?;
    bindings.content = Some(ContentBindings {
        reader: storage.content_verifier.clone(),
        writer: Some(storage.publisher.clone()),
    });
    bindings.tasks.register(TaskDefinition::live(
        "file_tool_case",
        "1",
        move |context, (): ()| handler(context),
    )?)?;
    let runtime = bindings.build(None)?;
    let task = runtime.task::<(), ()>("file_tool_case")?;
    assert_eq!(
        runtime.spawn(&task, ()).await?.result().await?,
        Outcome::Succeeded(())
    );
    Ok(())
}

#[tokio::test]
async fn memory_builder_replaces_or_disables_compaction() -> Result<()> {
    let provider = Arc::new(text_model(1));
    let unaccounted = Arc::new(text_model(1).without_accounting());
    let policy = crate::context::CompactionPolicy::Threshold(crate::context::ThresholdCompaction {
        response_reserve_tokens: 1_024,
        recent_tokens: 2_048,
        ..crate::context::ThresholdCompaction::default()
    });
    let storage = MemoryHarnessStorage::new(AgentId::new(), Limits::default().file_bytes).await?;
    let replacement = storage
        .builder()
        .model(
            Model::new("test".into(), "text".into(), "1".into(), Value::Null)?,
            provider.clone(),
        )
        .tool(storage.list_files_tool(Limits::default())?)
        .grant("tool:call:read")
        .grant("tool:call:write")
        .grant("tool:call:edit")
        .grant("tool:call:acyclic.list_files")
        .grant(capability::MODEL_GENERATE)
        .compaction(policy)
        .build()?;
    assert_eq!(
        storage
            .run_prompt(&replacement, "replacement", Vec::new())
            .await?
            .text,
        "local response"
    );
    let disabled_storage =
        MemoryHarnessStorage::new(AgentId::new(), Limits::default().file_bytes).await?;
    let disabled = disabled_storage
        .builder()
        .model(
            Model::new("test".into(), "text".into(), "1".into(), Value::Null)?,
            unaccounted.clone(),
        )
        .grant(capability::MODEL_GENERATE)
        .compaction(crate::context::CompactionPolicy::Disabled)
        .build()?;
    assert_eq!(
        disabled_storage
            .run_prompt(&disabled, "disabled", Vec::new())
            .await?
            .text,
        "local response"
    );
    let requests = provider
        .requests()
        .into_iter()
        .chain(unaccounted.requests())
        .collect::<Vec<_>>();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].max_output_tokens, Some(1_024));
    assert_eq!(requests[1].max_output_tokens, None);
    Ok(())
}

#[tokio::test]
async fn default_recursive_tools_are_present_and_unbound_routes_are_unavailable() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 65536).await?;
    let limits = Limits {
        file_bytes: 65536,
        ..Default::default()
    };
    let mut tools = crate::tools::defaults()?;
    tools.extend(crate::filesystem::recursive_tools::<
        MemoryStream,
        MemoryAuthorityBackend,
        MemoryObjectBackend,
    >(None, limits.file_bytes)?)?;
    tools.register((storage.list_files_tool(limits)?, None), &mut None, None)?;
    for name in ["acyclic.fork", "acyclic.merge"] {
        let tool = tools.get(name, None, None)?.unwrap().clone();
        run_admitted_file_case(&storage, limits, move |task| {
            let tool = tool.clone();
            async move {
                let invocation = ToolInvocation {
                    operation_id: OperationId::new(),
                    call_id: "unbound".into(),
                    name: tool.definition.name.clone(),
                    arguments: json!({}),
                };
                let context = crate::runtime::ToolContext::new(
                    Arc::new(task),
                    invocation.operation_id,
                    &invocation.call_id,
                    None,
                )?;
                assert!(matches!(
                    tool.executor
                        .authorize(None, Some(&context), &invocation, None),
                    Err(Error::Unsupported(_))
                ));
                assert!(matches!(
                    tool.executor.execute(invocation, Some(context)).await,
                    Err(Error::Unsupported(_))
                ));
                Ok(())
            }
        })
        .await?;
    }
    Ok(())
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
    let expected_projection = json!({"kind":"json","value":"pinned text"});
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
        arguments: json!({"file":file}),
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
                None,
            )?;
            let result = tool
                .executor
                .execute(invocation.clone(), Some(context.clone()))
                .await?;
            assert_eq!(result.value, json!("pinned text"));
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
                None,
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
                None,
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
                operation_id: OperationId::new(), call_id: "write-1".into(), name: "write".into(),
                arguments: json!({"path":"notes/one.txt","text":"saved text","media_type":"text/plain","display_name":"one.txt"}),
            };
            let context = crate::runtime::ToolContext::new(Arc::new(task.clone()), invocation.operation_id, &invocation.call_id, None)?;
            let first = tool.executor.execute(invocation.clone(), Some(context.clone())).await?;
            let file: FileRef = serde_json::from_value(first.value["file"].clone()).map_err(|error| Error::Invalid(error.to_string()))?;
            let user = task.stage_file(None, OperationId::new(), "notes/one.txt", b"later user edit", "text/plain", "one.txt").await?;
            assert_eq!(tool.executor.reconcile(invocation.clone(), Some(context.clone())).await?, Some(first));
            assert_eq!(task.read_file(&file, None).await?.as_ref(), b"saved text".as_ref());
            assert_eq!(task.read_file(&user, None).await?.as_ref(), b"later user edit".as_ref());
            for (field, value) in [("text", "different text"), ("path", "notes/another.txt"), ("media_type", "text/markdown"), ("display_name", "other.txt")] {
                let mut changed = invocation.clone(); changed.arguments[field] = json!(value);
                assert!(matches!(tool.executor.reconcile(changed, Some(context.clone())).await, Err(Error::Conflict(_))));
            }
            let ordinary = OperationId::new();
            for path in ["multi/a.txt", "multi/b.txt"] {
                task.stage_file(None, ordinary, path, b"ordinary", "text/plain", "ordinary.txt").await?;
            }
            Ok(())
        }
    }).await
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
    tools.extend(crate::filesystem::recursive_tools::<
        MemoryStream,
        MemoryAuthorityBackend,
        MemoryObjectBackend,
    >(None, limits.file_bytes)?)?;
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
        let tools = tools.clone(); let file = file.clone();
        async move {
            let task = task.scoped(Capabilities::new([
                "tool:call:read", "tool:call:write", "tool:call:edit", "tool:call:acyclic.list_files",
            ]), limits)?;
            for (name, arguments) in [
                ("read", json!({"file":file})),
                ("write", json!({"path":"notes/two.txt","text":"two","media_type":"text/plain","display_name":"two.txt"})),
                ("edit", json!({"file":file,"old_text":"one","new_text":"two"})),
            ] {
                let invocation = ToolInvocation { operation_id: OperationId::new(), call_id: name.into(), name: name.into(), arguments };
                let tool = tools.get(name, None, None)?.ok_or_else(|| Error::NotFound(name.into()))?;
                let context = crate::runtime::ToolContext::new(Arc::new(task.clone()), invocation.operation_id, &invocation.call_id, None)?;
                assert!(matches!(tool.executor.execute(invocation, Some(context)).await, Err(Error::Unauthorized(_))));
            }
            let invocation = ToolInvocation { operation_id: OperationId::new(), call_id: "list".into(), name: "acyclic.list_files".into(), arguments: json!({"path":"","maximum_entries":8}) };
            assert!(matches!(tools.get("acyclic.list_files", None, None)?.ok_or_else(|| Error::NotFound("list tool".into()))?.executor.authorize(Some(task.scope()), None, &invocation, None), Err(Error::Unauthorized(_))));
            Ok(())
        }
    }).await
}

#[tokio::test]
async fn memory_storage_advertises_and_executes_default_read_file() -> Result<()> {
    let limits = Limits::default();
    let storage = MemoryHarnessStorage::new(AgentId::new(), limits.file_bytes).await?;
    let staged = storage
        .stage(
            OperationId::new(),
            "notes/read.txt",
            b"read me",
            "text/plain",
            "read.txt",
        )
        .await?;
    let model = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                    "read".into(),
                    "read".into(),
                    json!({"file": staged}),
                ))),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]),
            ScriptedTurn::Text("read complete".into()),
        ])
        .with_token_counter(synthetic_token_count),
    );
    let bundle = storage
        .builder()
        .model(
            Model::new("test".into(), "file".into(), "1".into(), Value::Null)?,
            model.clone(),
        )
        .grant(capability::MODEL_GENERATE)
        .tool(storage.list_files_tool(limits)?)
        .grant("tool:call:read")
        .grant("tool:call:write")
        .grant("tool:call:edit")
        .grant("tool:call:acyclic.list_files")
        .compaction(crate::context::CompactionPolicy::default())
        .limits(limits)
        .build()?;
    assert_eq!(
        storage
            .run_prompt(&bundle, "read the file", Vec::new())
            .await?
            .text,
        "read complete"
    );
    let requests = model.requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].tools.iter().any(|tool| tool.name == "read"));
    assert!(requests[1].messages.iter().any(|message| matches!(
        &message.content,
        crate::model::ModelContent::Part(crate::model::ModelContentPart::ToolResult { .. })
    )));
    Ok(())
}

#[tokio::test]
async fn memory_prompt_runs_with_attachments() -> Result<()> {
    let provider = Arc::new(text_model(2));
    let storage = MemoryHarnessStorage::new(AgentId::new(), Limits::default().file_bytes).await?;
    let bundle = storage
        .builder()
        .model(
            Model::new("test".into(), "text".into(), "1".into(), Value::Null)?,
            provider.clone(),
        )
        .tool(storage.list_files_tool(Limits::default())?)
        .grant("tool:call:read")
        .grant("tool:call:write")
        .grant("tool:call:edit")
        .grant("tool:call:acyclic.list_files")
        .grant(capability::MODEL_GENERATE)
        .compaction(crate::context::CompactionPolicy::default())
        .build()?;
    let output = storage.run_prompt(&bundle, "hello", Vec::new()).await?;
    assert_eq!(output.text, "local response");
    let file = storage
        .stage(
            OperationId::new(),
            "input/note.txt",
            b"attached text",
            "text/plain",
            "note.txt",
        )
        .await?;
    let output = storage
        .run_prompt(
            &bundle,
            "read this",
            vec![Attachment {
                file,
                label: Some("note".into()),
            }],
        )
        .await?;
    assert_eq!(output.text, "local response");
    assert_eq!(provider.requests().len(), 2);
    assert!(matches!(storage.volume().owner(), VolumeOwner::Agent(_)));
    Ok(())
}

#[tokio::test]
async fn local_builder_runs_through_ref_only_storage() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::from_bytes([81; 16]), 4_096).await?;
    let provider = Arc::new(text_model(2));
    let bundle = storage
        .builder()
        .without_default_tools()
        .compaction(crate::context::CompactionPolicy::Disabled)
        .model(
            Model::new("test".into(), "text".into(), "1".into(), Value::Null)?,
            provider.clone(),
        )
        .grant("model:generate")
        .build()?;
    let operation_id = OperationId::from_bytes([82; 16]);
    let attachment = storage
        .stage(
            operation_id,
            "input/note.txt",
            b"private attachment body",
            "text/plain",
            "note.txt",
        )
        .await?;
    let user = storage
        .stage(
            operation_id,
            "input/prompt.txt",
            b"hello",
            "text/plain",
            "prompt.txt",
        )
        .await?;
    assert_eq!(
        storage.read(&attachment).await?.as_slice(),
        b"private attachment body"
    );
    let reader = AgentId::from_bytes([83; 16]);
    let delegated = storage
        .delegate_file_read(reader, "attached-reader", &attachment)
        .await?;
    assert_eq!(delegated.agent(), Some(reader));
    assert_eq!(
        storage
            .read_delegated(&delegated, &attachment)
            .await?
            .as_slice(),
        b"private attachment body"
    );
    let root_directory = storage.delegate_directory_read(reader, "attached-root", "")?;
    assert_eq!(
        storage
            .read_delegated(&root_directory, &attachment)
            .await?
            .as_slice(),
        b"private attachment body"
    );
    assert!(
        ContentGrant::verify(
            &storage.verifier(),
            &delegated,
            storage.volume(),
            VolumeOperation::Write,
            u64::MAX,
            &mut None
        )
        .is_err()
    );
    let directory = storage.delegate_directory_read(reader, "attached-directory", "input")?;
    let (generation, listed) = storage
        .list_delegated(&directory, "input", "input", None, None, 16)
        .await?;
    assert!(listed.entries.iter().any(|entry| {
        entry
            .name
            .unicode_text(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .ok()
            .map(|receipt| receipt.value)
            .as_deref()
            == Some("note.txt")
    }));
    let (from_directory, body) = storage
        .read_delegated_path(&directory, "input", "input/note.txt", Some(&generation))
        .await?;
    assert_eq!(body.as_ref(), b"private attachment body");
    assert_eq!(from_directory.path(), attachment.path());
    assert!(
        storage
            .read_delegated_path(&directory, "input", "inputs/note.txt", Some(&generation))
            .await
            .is_err()
    );
    storage
        .stage(
            OperationId::from_bytes([84; 16]),
            "input/new.txt",
            b"new",
            "text/plain",
            "new.txt",
        )
        .await?;
    assert!(
        storage
            .read_delegated_path(&directory, "input", "input/note.txt", Some(&generation))
            .await
            .is_ok()
    );
    assert!(
        ContentGrant::verify(
            &storage.verifier(),
            &directory,
            storage.volume(),
            VolumeOperation::Write,
            u64::MAX,
            &mut None
        )
        .is_err()
    );
    let output = storage
        .run_conversation(
            &bundle,
            operation_id,
            user.clone(),
            vec![Attachment {
                file: attachment.clone(),
                label: None,
            }],
            2,
        )
        .await?;
    assert_eq!(output.text, "local response");
    let first_aggregate = storage
        .journal
        .conversation_projection(bundle.limits())
        .await?;
    let first_user = first_aggregate
        .reducer()
        .conversation()
        .and_then(|conversation| conversation.messages.first())
        .ok_or_else(|| Error::Invalid("conversation user message is missing".into()))?;
    assert_eq!(
        first_user.id,
        crate::turn::derived_uuid(operation_id, "user")
    );
    drop(first_aggregate);
    let replayed = storage
        .run_conversation(
            &bundle,
            operation_id,
            user.clone(),
            vec![Attachment {
                file: attachment.clone(),
                label: None,
            }],
            2,
        )
        .await?;
    assert_eq!(replayed, output);
    let mut aggregate = storage
        .journal
        .conversation_projection(bundle.limits())
        .await?;
    assert_eq!(
        aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| crate::Error::Invalid("conversation not bound".into()))?
            .messages
            .len(),
        2
    );
    assert_eq!(aggregate.reducer().resident_context_selections().count(), 1);
    let expected_selection = aggregate
        .context_selection_for_operation(operation_id)
        .await?
        .ok_or_else(|| Error::Invalid("original context selection missing".into()))?;
    aggregate.set_resident_event_limit(1)?;
    assert_eq!(aggregate.reducer().resident_context_selections().count(), 0);
    assert!(
        aggregate
            .reducer()
            .context_selection_for_operation(operation_id)
            .is_none()
    );
    assert_eq!(
        aggregate
            .context_selection_for_operation(operation_id)
            .await?,
        Some(expected_selection)
    );
    drop(aggregate);
    storage.run_prompt(&bundle, "follow-up", Vec::new()).await?;
    assert_eq!(
        storage
            .run_conversation(
                &bundle,
                operation_id,
                user,
                vec![Attachment {
                    file: attachment,
                    label: None
                }],
                2
            )
            .await?,
        output
    );
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| request.tools.is_empty()));
    assert_eq!(requests[0].messages.len(), 1);
    assert_eq!(requests[1].messages.len(), 3);
    Ok(())
}

#[tokio::test]
async fn typed_tasks_stage_and_resolve_only_with_owner_bound_grants() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::from_bytes([91; 16]), 4_096).await?;
    let mut tasks = TaskRegistry::default();
    tasks.register(
        TaskDefinition::live("file_task", "1", |context, (): ()| async move {
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
            let bytes = context.read_file(&file, None).await?;
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
            String::from_utf8(bytes.to_vec()).map_err(|error| Error::Invalid(error.to_string()))
        })?
        .requires("content:write")?,
    )?;
    let bundle = storage
        .builder()
        .compaction(crate::context::CompactionPolicy::Disabled)
        .tasks(tasks)
        .model(
            Model::new("test".into(), "text".into(), "1".into(), Value::Null)?,
            Arc::new(text_model(1)),
        )
        .grant("model:generate")
        .build()?;
    let runtime = bundle.runtime();
    let task = runtime.task::<(), String>("file_task")?;
    assert_eq!(
        runtime.spawn(&task, ()).await?.result().await?,
        Outcome::Succeeded("pinned bytes".to_owned())
    );
    Ok(())
}
