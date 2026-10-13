use super::*;
use crate::conversation::ContentGrant;
use crate::{
    Outcome,
    model::{Model, ModelEvent, ModelProvider, ModelRequest},
    runtime::{TaskDefinition, TaskRegistry},
};
use futures::{
    future::BoxFuture,
    stream::{self, BoxStream},
};
use serde_json::Value;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

const FINITE_CONTENT_BUDGET: acyclic_fs::WorkBudget =
    acyclic_fs::performance::work_budget(100_000, 16_000_000);

mod content;

struct TextModel(Arc<Mutex<Vec<ModelRequest>>>);

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
    Ok(crate::context::ModelTokenCount {
        request_digest: request.request_digest(),
        fixed_tokens: u32::try_from(fixed)
            .map_err(|_| Error::Invalid("synthetic framing exceeds u32".into()))?,
        message_tokens,
    })
}

impl ModelProvider for TextModel {
    fn context_capacity(&self, _: &Model) -> Result<crate::context::ModelContextCapacity> {
        Ok(crate::context::ModelContextCapacity {
            context_tokens: 131_072,
            output_tokens: 4_096,
        })
    }
    fn count_tokens(
        &self,
        request: &crate::model::PreparedModelRequest,
    ) -> Result<crate::context::ModelTokenCount> {
        synthetic_token_count(request)
    }
    fn generate<'a>(
        &'a self,
        request: crate::model::PreparedModelRequest,
        _dispatch: crate::model::ModelDispatch,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request.request().clone());
        Box::pin(stream::iter(vec![
            Ok(ModelEvent::Content {
                delta: "local response".into(),
            }),
            Ok(ModelEvent::Completed {
                metadata: Value::Null,
            }),
        ]))
    }
}

async fn run_admitted_file_case<F, Fut>(
    storage: &MemoryHarnessStorage,
    limits: Limits,
    handler: F,
) -> Result<()>
where
    F: Fn(crate::runtime::TaskContext) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    use crate::runtime::{Bindings, ContentBindings};
    let mut bindings = Bindings::local();
    bindings.scope = RuntimeScope::new(
        Capabilities::new([
            storage.read_capability.clone(),
            storage.write_capability.clone(),
            "task:spawn:file_tool_case@1".into(),
            "tool:call:acyclic.read_file".into(),
            "tool:call:acyclic.write_file".into(),
            "tool:call:acyclic.edit_file".into(),
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
    let runtime = bindings.build()?;
    let task = runtime.task::<(), ()>("file_tool_case")?;
    assert_eq!(
        runtime.spawn(&task, ()).await?.result().await?,
        Outcome::Succeeded(())
    );
    Ok(())
}

struct ModelWithoutAccounting(TextModel);

impl ModelProvider for ModelWithoutAccounting {
    fn generate<'a>(
        &'a self,
        request: crate::model::PreparedModelRequest,
        dispatch: crate::model::ModelDispatch,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        self.0.generate(request, dispatch)
    }

    fn reconcile<'a>(
        &'a self,
        dispatch: crate::model::ModelDispatch,
        observed: &'a [crate::model::ModelEvent],
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        self.0.reconcile(dispatch, observed)
    }
}

#[tokio::test]
async fn memory_builder_replaces_or_disables_compaction() -> Result<()> {
    let requests = Arc::new(Mutex::new(Vec::new()));
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
            Arc::new(TextModel(requests.clone())),
        )
        .tools(storage.default_tools(Limits::default(), None)?)
        .grant("tool:call:acyclic.read_file")
        .grant("tool:call:acyclic.write_file")
        .grant("tool:call:acyclic.edit_file")
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
            Arc::new(ModelWithoutAccounting(TextModel(requests.clone()))),
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
    let requests = requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
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
    let tools = storage.default_tools(limits, None)?;
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
                    task,
                    invocation.operation_id,
                    &invocation.call_id,
                )?;
                assert!(matches!(
                    tool.executor.authorize_with_context(&context, &invocation),
                    Err(Error::Unsupported(_))
                ));
                assert!(matches!(
                    tool.executor
                        .execute_with_context(context, invocation)
                        .await,
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
    let tool = owner
        .default_tools(limits, None)?
        .get("acyclic.read_file", None, None)?
        .ok_or_else(|| Error::NotFound("read tool".into()))?
        .clone();
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "read-1".into(),
        name: "acyclic.read_file".into(),
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
                task.clone(),
                invocation.operation_id,
                &invocation.call_id,
            )?;
            let result = tool
                .executor
                .execute_with_context(context.clone(), invocation.clone())
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
                    .reconcile_with_context(context, invocation.clone())
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
            assert!(matches!(
                tool.executor
                    .execute_with_context(
                        crate::runtime::ToolContext::new(
                            narrowed,
                            invocation.operation_id,
                            &invocation.call_id
                        )?,
                        invocation
                    )
                    .await,
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
            assert!(matches!(
                tool.executor
                    .execute_with_context(
                        crate::runtime::ToolContext::new(
                            task,
                            invocation.operation_id,
                            &invocation.call_id
                        )?,
                        invocation
                    )
                    .await,
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
    let tool = storage
        .default_tools(limits, None)?
        .get("acyclic.write_file", None, None)?
        .ok_or_else(|| Error::NotFound("write tool".into()))?
        .clone();
    run_admitted_file_case(&storage, limits, move |task| {
        let tool = tool.clone();
        async move {
            let invocation = ToolInvocation {
                operation_id: OperationId::new(), call_id: "write-1".into(), name: "acyclic.write_file".into(),
                arguments: json!({"path":"notes/one.txt","text":"saved text","media_type":"text/plain","display_name":"one.txt"}),
            };
            let context = crate::runtime::ToolContext::new(task.clone(), invocation.operation_id, &invocation.call_id)?;
            let first = tool.executor.execute_with_context(context.clone(), invocation.clone()).await?;
            let file: FileRef = serde_json::from_value(first.value["file"].clone()).map_err(|error| Error::Invalid(error.to_string()))?;
            let user = task.stage_file(None, OperationId::new(), "notes/one.txt", b"later user edit", "text/plain", "one.txt").await?;
            assert_eq!(tool.executor.reconcile_with_context(context.clone(), invocation.clone()).await?, Some(first));
            assert_eq!(task.read_file(&file, None).await?.as_ref(), b"saved text".as_ref());
            assert_eq!(task.read_file(&user, None).await?.as_ref(), b"later user edit".as_ref());
            for (field, value) in [("text", "different text"), ("path", "notes/another.txt"), ("media_type", "text/markdown"), ("display_name", "other.txt")] {
                let mut changed = invocation.clone(); changed.arguments[field] = json!(value);
                assert!(matches!(tool.executor.reconcile_with_context(context.clone(), changed).await, Err(Error::Conflict(_))));
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
    assert!(
        root.entries
            .iter()
            .any(|entry| entry.name.unicode_text().as_deref() == Some("notes"))
    );
    assert!(
        !root
            .entries
            .iter()
            .any(|entry| entry.name.unicode_text().as_deref() == Some(".system"))
    );
    let (_, notes) = storage
        .list_private_directory("notes", Some(&generation), None, 8)
        .await?;
    assert!(
        notes
            .entries
            .iter()
            .any(|entry| entry.name.unicode_text().as_deref() == Some("one.txt"))
    );
    let limits = Limits {
        file_bytes: 4_096,
        render_bytes: 1_024,
        ..Limits::default()
    };
    let tool = storage
        .default_tools(limits, None)?
        .get("acyclic.list_files", None, None)?
        .ok_or_else(|| Error::NotFound("default list tool".into()))?
        .clone();
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "list-1".into(),
        name: "acyclic.list_files".into(),
        arguments: json!({"path": "notes", "maximum_entries": 65}),
    };
    let page = tool.executor.execute(invocation.clone()).await?;
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
    assert_eq!(tool.executor.reconcile(invocation).await?, Some(page));
    let (_, pinned_notes) = storage
        .list_private_directory("notes", Some(&generation), None, 8)
        .await?;
    assert_eq!(pinned_notes.entries.len(), 1);
    assert_eq!(
        pinned_notes.entries[0].name.unicode_text().as_deref(),
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
    let tools = storage.default_tools(limits, None)?;
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
                "tool:call:acyclic.read_file", "tool:call:acyclic.write_file", "tool:call:acyclic.edit_file", "tool:call:acyclic.list_files",
            ]), limits)?;
            for (name, arguments) in [
                ("acyclic.read_file", json!({"file":file})),
                ("acyclic.write_file", json!({"path":"notes/two.txt","text":"two","media_type":"text/plain","display_name":"two.txt"})),
                ("acyclic.edit_file", json!({"file":file,"old_text":"one","new_text":"two"})),
            ] {
                let invocation = ToolInvocation { operation_id: OperationId::new(), call_id: name.into(), name: name.into(), arguments };
                let tool = tools.get(name, None, None)?.ok_or_else(|| Error::NotFound(name.into()))?;
                assert!(matches!(tool.executor.execute_with_context(crate::runtime::ToolContext::new(task.clone(), invocation.operation_id, &invocation.call_id)?, invocation).await, Err(Error::Unauthorized(_))));
            }
            let invocation = ToolInvocation { operation_id: OperationId::new(), call_id: "list".into(), name: "acyclic.list_files".into(), arguments: json!({"path":"","maximum_entries":8}) };
            assert!(matches!(tools.get("acyclic.list_files", None, None)?.ok_or_else(|| Error::NotFound("list tool".into()))?.executor.authorize(Some(task.scope()), &invocation), Err(Error::Unauthorized(_))));
            Ok(())
        }
    }).await
}

struct ReadFileModel {
    file: Arc<Mutex<Option<FileRef>>>,
    calls: AtomicUsize,
}

impl ModelProvider for ReadFileModel {
    fn context_capacity(&self, _: &Model) -> Result<crate::context::ModelContextCapacity> {
        Ok(crate::context::ModelContextCapacity {
            context_tokens: 131_072,
            output_tokens: 4_096,
        })
    }
    fn count_tokens(
        &self,
        request: &crate::model::PreparedModelRequest,
    ) -> Result<crate::context::ModelTokenCount> {
        synthetic_token_count(request)
    }
    fn generate<'a>(
        &'a self,
        request: crate::model::PreparedModelRequest,
        _dispatch: crate::model::ModelDispatch,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        let request = request.request();
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            let Some(file) = self
                .file
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
            else {
                return Box::pin(stream::iter(vec![Err(Error::Invalid(
                    "file was not staged before model generation".into(),
                ))]));
            };
            assert!(
                request
                    .tools
                    .iter()
                    .any(|tool| tool.name == "acyclic.read_file")
            );
            Box::pin(stream::iter(vec![
                Ok(ModelEvent::ToolCall {
                    call_id: "read".into(),
                    name: "acyclic.read_file".into(),
                    arguments: json!({"file": file}),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]))
        } else {
            assert!(request.messages.iter().any(|message| {
                matches!(
                    &message.content,
                    crate::model::ModelContent::Part(
                        crate::model::ModelContentPart::ToolResult { .. }
                    )
                )
            }));
            Box::pin(stream::iter(vec![
                Ok(ModelEvent::Content {
                    delta: "read complete".into(),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]))
        }
    }
}

#[tokio::test]
async fn memory_storage_advertises_and_executes_default_read_file() -> Result<()> {
    let file = Arc::new(Mutex::new(None));
    let model = Arc::new(ReadFileModel {
        file: file.clone(),
        calls: AtomicUsize::new(0),
    });
    let limits = Limits::default();
    let storage = MemoryHarnessStorage::new(AgentId::new(), limits.file_bytes).await?;
    let bundle = storage
        .builder()
        .model(
            Model::new("test".into(), "file".into(), "1".into(), Value::Null)?,
            model.clone(),
        )
        .grant(capability::MODEL_GENERATE)
        .tools(storage.default_tools(limits, None)?)
        .grant("tool:call:acyclic.read_file")
        .grant("tool:call:acyclic.write_file")
        .grant("tool:call:acyclic.edit_file")
        .grant("tool:call:acyclic.list_files")
        .compaction(crate::context::CompactionPolicy::default())
        .limits(limits)
        .build()?;
    let staged = storage
        .stage(
            OperationId::new(),
            "notes/read.txt",
            b"read me",
            "text/plain",
            "read.txt",
        )
        .await?;
    *file
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(staged);
    assert_eq!(
        storage
            .run_prompt(&bundle, "read the file", Vec::new())
            .await?
            .text,
        "read complete"
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn memory_prompt_runs_with_attachments() -> Result<()> {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let storage = MemoryHarnessStorage::new(AgentId::new(), Limits::default().file_bytes).await?;
    let bundle = storage
        .builder()
        .model(
            Model::new("test".into(), "text".into(), "1".into(), Value::Null)?,
            Arc::new(TextModel(requests.clone())),
        )
        .tools(storage.default_tools(Limits::default(), None)?)
        .grant("tool:call:acyclic.read_file")
        .grant("tool:call:acyclic.write_file")
        .grant("tool:call:acyclic.edit_file")
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
    assert_eq!(
        requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        2
    );
    assert!(matches!(storage.volume().owner(), VolumeOwner::Agent(_)));
    Ok(())
}

#[tokio::test]
async fn local_builder_runs_through_ref_only_storage() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::from_bytes([81; 16]), 4_096).await?;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let bundle = storage
        .builder()
        .compaction(crate::context::CompactionPolicy::Disabled)
        .model(
            Model::new("test".into(), "text".into(), "1".into(), Value::Null)?,
            Arc::new(TextModel(requests.clone())),
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
    assert!(
        listed
            .entries
            .iter()
            .any(|entry| entry.name.unicode_text().as_deref() == Some("note.txt"))
    );
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
        crate::turn::canonical_user_message_id(operation_id)
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
    let requests = requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(requests.len(), 2);
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
            Arc::new(TextModel(Arc::new(Mutex::new(Vec::new())))),
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
