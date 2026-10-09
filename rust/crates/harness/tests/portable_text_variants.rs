//! Real live task admission and authenticated FS reads for explicit text variants.
//! Durable restart/fork/cancellation and stock model/browser execution are separate gates.
#![cfg(feature = "filesystem")]

use acyclic_fs::Fs;
use acyclic_harness::{
    AgentId, Capabilities, Error, OperationId, Outcome, Result,
    conversation::{
        ContentPublisher, ContentResidencyVerifier, FileRef, Limits, VolumeClass, VolumeOperation,
        VolumeOwner, VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer},
    filesystem::{FilesystemContentPublisher, FilesystemContentVerifier, FilesystemHost},
    model::{FileProjectionPolicy, ModelDataPart, ToolResultContent},
    resources::ProviderRef,
    runtime::{Bindings, ContentBindings, RuntimeScope, TaskDefinition, TaskRegistry, ToolContext},
    tool::{
        ToolInvocation, ToolRegistry, ToolResult,
        files::{self, EditFileInput, FileResult, PatchFileInput, ReadFileInput, WriteFileInput},
        schema::ProjectionMode,
        text::{ReadOptions, SearchOptions, TextRange},
        text_files::{self, ReadInput, ReadResult, SearchInput, SearchResult},
    },
};
use acyclic_stream::BoxProviderFuture;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct ReaderSpy {
    inner: Arc<dyn ContentResidencyVerifier>,
    reads: AtomicUsize,
    corrupt: AtomicBool,
}

impl ContentResidencyVerifier for ReaderSpy {
    fn verify<'a>(&'a self, file: &'a FileRef) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move { self.inner.verify(file).await })
    }
    fn read<'a>(&'a self, file: &'a FileRef) -> BoxProviderFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let mut bytes = self.inner.read(file).await?;
            if self.corrupt.load(Ordering::SeqCst)
                && let Some(first) = bytes.first_mut()
            {
                *first ^= 1;
            }
            Ok(bytes)
        })
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one actual admitted live task retains positive and pre-I/O negative controls"
)]
async fn admitted_live_task_reads_pinned_text_and_preserves_projection_identity() -> Result<()> {
    let provider = ProviderRef::new("text-variants", "filesystem", "1")?;
    let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
    let agent = AgentId::new();
    let volume = VolumeRef::new(
        provider,
        "text-variants",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    host.create_volume(&volume).await?;
    let issuer = AuthorityIssuer::new(
        "text-variants",
        [41; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "text-variants".into(),
        },
    );
    let read = volume.capability(VolumeOperation::Read)?;
    let write = volume.capability(VolumeOperation::Write)?;
    let owner = issuer.root_for_agent(
        agent,
        "owner",
        Capabilities::new([read.clone(), write.clone()]),
    );
    let writer = Arc::new(FilesystemContentPublisher::new(
        host.clone(),
        volume,
        &issuer.verifier(),
        &owner,
        4096,
    )?);
    let source = writer
        .stage(
            OperationId::new(),
            "source.txt",
            "one\r\n🦀 abaaba\r\n".as_bytes(),
            "text/plain",
            "source.txt",
        )
        .await?;
    // A later workspace generation must not change the immutable source selected by the task.
    let later = writer
        .stage(
            OperationId::new(),
            "source.txt",
            b"later workspace content",
            "text/plain",
            "source.txt",
        )
        .await?;
    assert_ne!(source, later);
    let reader = Arc::new(ReaderSpy {
        inner: Arc::new(FilesystemContentVerifier::new(
            host.clone(),
            issuer.verifier(),
            owner,
            4096,
        )?),
        reads: AtomicUsize::new(0),
        corrupt: AtomicBool::new(false),
    });
    let read_tool = text_files::read_file_range(
        ReadOptions {
            maximum_input_bytes: 4096,
            maximum_text_bytes: 32,
        },
        4096,
        ProjectionMode::Reference,
    )?;
    let search_tool = text_files::search_file(
        SearchOptions {
            maximum_input_bytes: 4096,
            maximum_query_bytes: 16,
            maximum_work: 4096,
            maximum_matches: 1,
        },
        4096,
        ProjectionMode::Full,
    )?;
    let read_executor = read_tool.executor.clone();
    let read_projector = read_tool.projection.clone();
    let read_definition = read_tool.definition.clone();
    let search_projector = search_tool.projection.clone();
    let mut tools = ToolRegistry::new();
    tools.register(read_tool)?;
    tools.register(search_tool)?;
    tools.register(files::read_file()?)?;
    tools.register(files::write_file()?)?;
    tools.register(files::edit_file()?)?;
    tools.register(files::patch_file(65_536, 16)?)?;
    let captured_reader = reader.clone();
    let task = TaskDefinition::live("fixture.text_variants", "1", move |context, (): ()| {
        let source = source.clone();
        let host = host.clone();
        let reader = captured_reader.clone();
        let executor = read_executor.clone();
        let projector = read_projector.clone();
        let definition = read_definition.clone();
        let search_projector = search_projector.clone();
        async move {
            let read = context.tool::<ReadInput, ReadResult>("acyclic.read_file_range")?;
            let input = ReadInput {
                file: source.clone(),
                range: TextRange { start: 5, end: 9 },
            };
            let result = context.call(&read, input.clone()).await?;
            assert_eq!(result.file, source);
            assert_eq!(result.selection.text, "🦀");
            assert_eq!(result.selection.omitted_before, 5);
            assert_eq!(result.selection.omitted_after, 9);
            let invocation = ToolInvocation {
                operation_id: OperationId::new(),
                call_id: "retained-read".into(),
                name: definition.name.clone(),
                arguments: json!(input),
            };
            let canonical = ToolResult {
                value: json!(result),
            };
            let unchanged = canonical.clone();
            let projection = projector.project(&invocation, &canonical)?;
            let content: ToolResultContent = serde_json::from_value(projection)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let ToolResultContent::Parts { parts } = content else {
                panic!("reference parts")
            };
            assert!(
                matches!(parts.get(1),Some(ModelDataPart::File { file,policy:FileProjectionPolicy::Reference }) if file==&source)
            );
            assert!(
                matches!(parts.first(),Some(ModelDataPart::Text { text }) if text.contains("omitted"))
            );
            assert_eq!(canonical, unchanged);
            let search = context.tool::<SearchInput, SearchResult>("acyclic.search_file")?;
            let result = context
                .call(
                    &search,
                    SearchInput {
                        file: source.clone(),
                        query: "aba".into(),
                    },
                )
                .await?;
            assert_eq!(result.file, source);
            assert_eq!(result.matches.total_matches, 2);
            assert_eq!(result.matches.omitted_matches, 1);
            assert_eq!(
                result.matches.matches,
                vec![TextRange { start: 10, end: 13 }]
            );
            let canonical = ToolResult {
                value: json!(result),
            };
            assert_eq!(
                search_projector.project(&invocation, &canonical)?,
                json!({"kind":"json","value":canonical.value})
            );
            // Public typed calls use the actual admitted live context and writer.
            let write = context.tool::<WriteFileInput, FileResult>("acyclic.write_file")?;
            let whole = context.tool::<ReadFileInput, String>("acyclic.read_file")?;
            let edit = context.tool::<EditFileInput, FileResult>("acyclic.edit_file")?;
            let patch = context.tool::<PatchFileInput, FileResult>("acyclic.patch_file")?;
            let written = context
                .call(
                    &write,
                    WriteFileInput {
                        path: "live.txt".into(),
                        text: "alpha\r\n🦀 omega\r\n".into(),
                        media_type: "text/plain".into(),
                        display_name: "live.txt".into(),
                    },
                )
                .await?;
            assert_eq!(
                context
                    .call(
                        &whole,
                        ReadFileInput {
                            file: written.file.clone()
                        }
                    )
                    .await?,
                "alpha\r\n🦀 omega\r\n"
            );
            let edited = context
                .call(
                    &edit,
                    EditFileInput {
                        file: written.file.clone(),
                        old_text: "omega".into(),
                        new_text: "done".into(),
                    },
                )
                .await?;
            assert_eq!(
                context.read_file(&edited.file).await?,
                "alpha\r\n🦀 done\r\n".as_bytes()
            );
            assert_eq!(
                context.read_file(&written.file).await?,
                "alpha\r\n🦀 omega\r\n".as_bytes()
            );
            let patched = context
                .call(
                    &patch,
                    PatchFileInput {
                        file: edited.file.clone(),
                        diff: "@@\n-alpha\n+beta\n".into(),
                    },
                )
                .await?;
            assert_eq!(
                context.read_file(&patched.file).await?,
                "beta\r\n🦀 done\r\n".as_bytes()
            );
            assert_eq!(
                context.read_file(&edited.file).await?,
                "alpha\r\n🦀 done\r\n".as_bytes()
            );
            let workspace = acyclic_harness::filesystem::workspace_ref(
                patched.file.volume().provider().clone(),
                &patched.file.volume().storage_name()?,
            )?;
            let before_failed_writes = host.resolve(&workspace).await?.generation;
            assert!(matches!(
                context
                    .call(
                        &edit,
                        EditFileInput {
                            file: written.file.clone(),
                            old_text: "omega".into(),
                            new_text: "stale".into(),
                        }
                    )
                    .await,
                Err(Error::Conflict(_))
            ));
            assert_eq!(
                host.resolve(&workspace).await?.generation,
                before_failed_writes
            );
            let read_only = context.scoped(
                context.scope().grants().without(&Capabilities::new([written
                    .file
                    .volume()
                    .capability(VolumeOperation::Write)?])),
                context.scope().limits(),
            )?;
            assert!(matches!(
                read_only
                    .call(
                        &write,
                        WriteFileInput {
                            path: "denied.txt".into(),
                            text: "denied".into(),
                            media_type: "text/plain".into(),
                            display_name: "denied.txt".into(),
                        }
                    )
                    .await,
                Err(Error::Unauthorized(_))
            ));
            assert_eq!(
                context.read_file(&patched.file).await?,
                "beta\r\n🦀 done\r\n".as_bytes()
            );
            assert_eq!(
                host.resolve(&workspace).await?.generation,
                before_failed_writes
            );
            // The original tool-call grants survive attenuation; file access does not.
            let restricted = context.scoped(
                Capabilities::new([
                    "tool:call:acyclic.read_file_range",
                    "tool:call:acyclic.search_file",
                ]),
                Limits::default(),
            )?;
            let before = reader.reads.load(Ordering::SeqCst);
            assert!(before >= 2);
            assert!(matches!(
                restricted.call(&read, input.clone()).await,
                Err(Error::Unauthorized(_))
            ));
            assert!(
                context
                    .call(
                        &read,
                        ReadInput {
                            file: source.clone(),
                            range: TextRange { start: 0, end: 19 }
                        }
                    )
                    .await
                    .is_err()
            );
            assert!(
                context
                    .call(
                        &search,
                        SearchInput {
                            file: source.clone(),
                            query: String::new()
                        }
                    )
                    .await
                    .is_err()
            );
            assert_eq!(reader.reads.load(Ordering::SeqCst), before);
            // Copying a valid invocation does not authorize a different operation or call.
            for wrong in [
                ToolContext::new(context.clone(), OperationId::new(), "retained-read")?,
                ToolContext::new(context.clone(), invocation.operation_id, "different-call")?,
            ] {
                assert!(matches!(
                    executor
                        .execute_with_context(wrong, invocation.clone())
                        .await,
                    Err(Error::Unauthorized(_))
                ));
            }
            assert_eq!(reader.reads.load(Ordering::SeqCst), before);
            // An authenticated provider still cannot substitute bytes for the pinned descriptor.
            reader.corrupt.store(true, Ordering::SeqCst);
            assert!(context.call(&read, input).await.is_err());
            assert_eq!(reader.reads.load(Ordering::SeqCst), before + 1);
            reader.corrupt.store(false, Ordering::SeqCst);
            Ok(())
        }
    })?;
    let mut tasks = TaskRegistry::default();
    tasks.register(task)?;
    let mut bindings = Bindings::local();
    bindings.tasks = tasks;
    bindings.tools = tools;
    bindings.content = Some(ContentBindings {
        reader,
        writer: Some(writer),
    });
    bindings.scope = RuntimeScope::new(
        Capabilities::new([
            "task:spawn:fixture.text_variants@1".to_owned(),
            "tool:call:acyclic.read_file_range".to_owned(),
            "tool:call:acyclic.search_file".to_owned(),
            read,
            write,
            "tool:call:acyclic.read_file".to_owned(),
            "tool:call:acyclic.write_file".to_owned(),
            "tool:call:acyclic.edit_file".to_owned(),
            "tool:call:acyclic.patch_file".to_owned(),
        ]),
        Limits::default(),
    )?;
    let runtime = bindings.build()?;
    let task = runtime.task::<(), ()>("fixture.text_variants")?;
    assert_eq!(
        runtime.spawn(&task, ()).await?.result().await?,
        Outcome::Succeeded(())
    );
    Ok(())
}
