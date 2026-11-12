use super::*;
use crate::conversation::{
    BoundedContentFuture, ContentAccess, ContentHold, ContentResidencyVerifier, FilePathSelection,
    VolumeRef,
};
use crate::filesystem::MemoryHarnessStorage;
use crate::resources::GenerationRef;
use std::sync::Mutex;

struct ObservedReader {
    inner: Arc<dyn ContentResidencyVerifier>,
    ranges: Mutex<Vec<acyclic_fs::ByteRange>>,
}

impl ContentResidencyVerifier for ObservedReader {
    fn read_private_path<'a, 'io>(
        &'a self,
        volume: &'a VolumeRef,
        prefix: &'a str,
        path: &'a str,
        generation: Option<&'a GenerationRef>,
        maximum_bytes: u64,
        access: ContentAccess<'a, 'io>,
    ) -> Result<
        BoundedContentFuture<
            'a,
            Option<(
                crate::conversation::FileRef,
                acyclic_stream::OwnedBytes,
                Option<ContentHold<'io>>,
            )>,
        >,
    >
    where
        'io: 'a,
    {
        self.inner
            .read_private_path(volume, prefix, path, generation, maximum_bytes, access)
    }

    fn read_range<'a>(
        &'a self,
        reference: &'a crate::conversation::FileRef,
        range: acyclic_fs::ByteRange,
        ceiling: Option<&'a Capabilities>,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        self.ranges.lock().unwrap().push(range);
        self.inner.read_range(reference, range, ceiling, io)
    }

    fn read_private_range<'a, 'io>(
        &'a self,
        volume: &'a VolumeRef,
        prefix: &'a str,
        path: &'a str,
        generation: Option<&'a GenerationRef>,
        range: acyclic_fs::ByteRange,
        access: ContentAccess<'a, 'io>,
    ) -> Result<
        BoundedContentFuture<
            'a,
            (
                FilePathSelection,
                acyclic_stream::OwnedBytes,
                Option<ContentHold<'io>>,
            ),
        >,
    >
    where
        'io: 'a,
    {
        self.ranges.lock().unwrap().push(range);
        self.inner
            .read_private_range(volume, prefix, path, generation, range, access)
    }
}

#[tokio::test]
async fn own_ui_requires_explicit_body_and_never_shadows_project_source() -> Result<()> {
    for native in [false, true] {
        let own = MemoryHarnessStorage::new(crate::AgentId::new(), 4096).await?;
        let project = MemoryHarnessStorage::new(crate::AgentId::new(), 4096).await?;
        let mut original = None;
        for (storage, is_own) in [(&own, true), (&project, false)] {
            let file = storage
                .stage(
                    OperationId::new(),
                    "ui/index.md",
                    b"SOURCE_BODY",
                    "text/markdown",
                    "view",
                )
                .await?;
            if is_own {
                original = Some(file);
            }
        }
        let original = original.unwrap();
        let version = super::super::version::encode_version(&original, None)?.0;
        let mut own_content = own.content_bindings();
        let project_content = project.content_bindings();
        let reader = Arc::new(ObservedReader {
            inner: own_content.reader.clone(),
            ranges: Mutex::new(Vec::new()),
        });
        own_content.reader = reader.clone();
        let build = |render_bytes| -> Result<_> {
            Ok(Arc::new(
                crate::runtime::Bindings {
                    scope: RuntimeScope::new(
                        Capabilities::new([
                            own_content
                                .volume
                                .directory_read_capability("", &mut None)?,
                            project_content
                                .volume
                                .directory_read_capability("", &mut None)?,
                            "tool:call:read".into(),
                            original.read_capability(&mut None)?,
                        ]),
                        Limits {
                            file_bytes: 4096,
                            render_bytes: Some(render_bytes),
                            attachments: std::num::NonZeroUsize::new(1),
                            ..Limits::default()
                        },
                    )?,
                    content: Some(project_content.clone()),
                    ..crate::runtime::Bindings::local()
                }
                .build()?
                .agent_context(&Capabilities::default(), own_content.clone())?,
            ))
        };
        let task = build(4096)?;
        let tool = read()?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(100_000, 16_000_000),
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        for (path, version) in [
            ("volume:///private/ui/index.md", None),
            ("volume:///output/ui/index.md", None),
            ("volume:///home/ui/index.md", None),
            ("volume:///private/ui/index.md", Some(version.as_str())),
            ("ui/index.md", None),
        ] {
            let own_path = path.starts_with("volume:");
            for include_body in [false, true] {
                reader.ranges.lock().unwrap().clear();
                let invocation = ToolInvocation {
                    operation_id: OperationId::new(),
                    call_id: "read-view".into(),
                    name: "read".into(),
                    arguments: json!({"path":path,"include_body":include_body,"version":version}),
                };
                let context =
                    ToolContext::new(task.clone(), invocation.operation_id, &invocation.call_id)?;
                let (result, _custody) = if native {
                    let (result, custody) = tool
                        .executor
                        .execute_with_native_context(&context, &invocation, &work)?
                        .await?;
                    (result, Some(custody))
                } else {
                    (
                        tool.executor
                            .execute(invocation.clone(), Some(context))
                            .await?,
                        None,
                    )
                };
                let (projected, _projection_custody) = tool.projection.project(
                    &invocation,
                    &result,
                    if native { Some(&work) } else { None },
                )?;
                let encoded = serde_json::to_string(&projected)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                tool.definition
                    .validate_projection(&projected, Limits::default(), None)?;
                let reference = super::super::projection::FileProjection::Read(
                    crate::tool::schema::ProjectionMode::Reference,
                );
                let (reference_value, _reference_custody) = reference.project(
                    &invocation,
                    &result,
                    if native { Some(&work) } else { None },
                )?;
                let mut definition = tool.definition.clone();
                definition.projection_schema = reference.schema()?;
                definition.validate_projection(&reference_value, Limits::default(), None)?;
                if own_path {
                    assert_eq!(result.value["self-projection"], true);
                    assert!(encoded.contains("self-projection"));
                    assert!(reference_value.to_string().contains("self-projection"));
                } else {
                    assert!(result.value.get("self-projection").is_none());
                }
                if own_path && !include_body {
                    assert_eq!(result.value["text"], "");
                    assert!(!encoded.contains("SOURCE_BODY"));
                    assert!(encoded.contains("include_body: true"));
                    let ranges = reader.ranges.lock().unwrap();
                    assert_eq!(ranges.len(), 1);
                    assert_eq!(ranges[0].length, 0, "implicit self read fetched a body");
                } else {
                    assert_eq!(result.value["text"], "SOURCE_BODY");
                    assert!(encoded.contains("SOURCE_BODY"));
                }
            }
        }
        own.stage(
            OperationId::new(),
            "ui/boundary.txt",
            &vec![b'x'; 2048],
            "text/plain",
            "text",
        )
        .await?;
        for (path, selection, media) in [
            (
                "ui/image.png",
                Some(json!({"kind":"image","detail":"auto"})),
                "image/png",
            ),
            (
                "ui/document.pdf",
                Some(json!({"kind":"pdf"})),
                "application/pdf",
            ),
            ("ui/boundary.txt", None, "text/plain"),
        ] {
            if selection.is_some() {
                own.stage(OperationId::new(), path, b"media", media, "media")
                    .await?;
            }
            let invocation = ToolInvocation {
                operation_id: OperationId::new(),
                call_id: "bounded-view".into(),
                name: "read".into(),
                arguments: json!({"path":format!("volume:///private/{path}"),"include_body":true,"selection":selection}),
            };
            let execute = |task: Arc<crate::runtime::TaskContext>, invocation: &ToolInvocation| {
                let invocation = invocation.clone();
                let tool = &tool;
                let work = &work;
                async move {
                    let context =
                        ToolContext::new(task, invocation.operation_id, &invocation.call_id)?;
                    let (result, custody) = if native {
                        let (result, custody) = tool
                            .executor
                            .execute_with_native_context(&context, &invocation, work)?
                            .await?;
                        (result, Some(custody))
                    } else {
                        (
                            tool.executor
                                .execute(invocation.clone(), Some(context))
                                .await?,
                            None,
                        )
                    };
                    let (projection, projection_custody) = tool.projection.project(
                        &invocation,
                        &result,
                        if native { Some(work) } else { None },
                    )?;
                    Ok::<_, Error>((projection, custody, projection_custody))
                }
            };
            let (projection, _custody, _projection_custody) =
                execute(task.clone(), &invocation).await?;
            // A complete ordinary page fits exactly; the self-view marker must
            // cause pagination rather than rejection at the same credit.
            let credit = if selection.is_none() {
                let mut without_marker = projection.clone();
                without_marker["value"]
                    .as_object_mut()
                    .unwrap()
                    .remove("self-projection");
                serde_json::to_vec(&without_marker).unwrap().len() as u64
            } else {
                4096
            };
            let (projection, _custody, _projection_custody) =
                execute(build(credit)?, &invocation).await?;
            tool.definition.validate_projection(
                &projection,
                Limits {
                    render_bytes: Some(credit),
                    attachments: std::num::NonZeroUsize::new(1),
                    ..Limits::default()
                },
                None,
            )?;
            if selection.is_some() {
                assert_eq!(projection["parts"].as_array().unwrap().len(), 2);
                assert!(
                    projection["parts"][0]["text"]
                        .as_str()
                        .unwrap()
                        .starts_with("self-projection: true\n")
                );
            } else {
                let shown = projection["value"]["text"].as_str().unwrap();
                assert!(!shown.is_empty());
                assert!(shown.len() < 2048);
                assert_eq!(projection["value"]["page"]["next"], shown.len());
                assert!(serde_json::to_vec(&projection).unwrap().len() as u64 <= credit);
                let version = projection["value"]["version"].as_str().unwrap();
                let mut assembled = shown.to_owned();
                while assembled.len() < 2048 {
                    let mut next = invocation.clone();
                    next.operation_id = OperationId::new();
                    next.arguments["version"] = json!(version);
                    next.arguments["range"] = json!({"start":assembled.len(),"end":2048});
                    let (page, _page_custody, _page_projection_custody) =
                        execute(build(credit)?, &next).await?;
                    tool.definition.validate_projection(
                        &page,
                        Limits {
                            render_bytes: Some(credit),
                            attachments: std::num::NonZeroUsize::new(1),
                            ..Limits::default()
                        },
                        None,
                    )?;
                    assert_eq!(page["value"]["version"], version);
                    assert_eq!(page["value"]["self-projection"], true);
                    assert_eq!(page["value"]["page"]["start"], assembled.len());
                    let text = page["value"]["text"].as_str().unwrap();
                    assert!(!text.is_empty(), "continuation made no source progress");
                    assembled.push_str(text);
                    assert_eq!(page["value"]["page"]["end"], assembled.len());
                    assert_eq!(page["value"]["page"]["eof"], assembled.len() == 2048);
                }
                assert_eq!(assembled.as_bytes(), vec![b'x'; 2048]);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn selected_private_pages_fit_credit_and_preserve_pinned_bytes() -> Result<()> {
    for native in [false, true] {
        let own = MemoryHarnessStorage::new(crate::AgentId::new(), 16384).await?;
        let project = MemoryHarnessStorage::new(crate::AgentId::new(), 16384).await?;
        let artifacts = MemoryHarnessStorage::new(crate::AgentId::new(), 16384).await?;
        let own_content = own.content_bindings();
        let project_content = project.content_bindings();
        let artifact_content = artifacts.content_bindings();
        let limits = Limits {
            file_bytes: 16384,
            render_bytes: Some(2048),
            attachments: std::num::NonZeroUsize::new(1),
            ..Limits::default()
        };
        let task = Arc::new(
            crate::runtime::Bindings {
                scope: RuntimeScope::new(
                    Capabilities::new([
                        own_content
                            .volume
                            .directory_read_capability("", &mut None)?,
                        project_content
                            .volume
                            .directory_read_capability("", &mut None)?,
                        artifact_content
                            .volume
                            .directory_read_capability("", &mut None)?,
                        "tool:call:read".into(),
                    ]),
                    limits,
                )?,
                content: Some(project_content),
                artifacts: Some(artifact_content),
                ..crate::runtime::Bindings::local()
            }
            .build()?
            .agent_context(&Capabilities::default(), own_content)?,
        );
        let tool = read()?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, 64_000_000),
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let execute = |invocation: &ToolInvocation| {
            let invocation = invocation.clone();
            let task = task.clone();
            let tool = &tool;
            let work = &work;
            async move {
                let context = ToolContext::new(task, invocation.operation_id, &invocation.call_id)?;
                let (result, custody) = if native {
                    let (result, custody) = tool
                        .executor
                        .execute_with_native_context(&context, &invocation, work)?
                        .await?;
                    (result, Some(custody))
                } else {
                    (
                        tool.executor
                            .execute(invocation.clone(), Some(context))
                            .await?,
                        None,
                    )
                };
                let (projection, projection_custody) = tool.projection.project(
                    &invocation,
                    &result,
                    if native { Some(work) } else { None },
                )?;
                tool.definition
                    .validate_projection(&projection, limits, None)?;
                assert!(serde_json::to_vec(&projection).unwrap().len() <= 2048);
                assert_eq!(result.value["self-projection"], true);
                assert!(projection.to_string().contains("self-projection"));
                Ok::<_, Error>((result, projection, custody, projection_custody))
            }
        };
        let lines = format!("{}\n{}\n", "first".repeat(400), "last".repeat(400));
        for (name, selection, output) in [
            ("lines.txt", json!({"kind":"lines","offset":0}), None),
            ("cells.ipynb", json!({"kind":"cells","offset":0}), None),
            (
                "outputs.ipynb",
                json!({"kind":"cells","offset":0}),
                Some(
                    json!({"output_type":"display_data","data":{"application/pdf":"JVBERi0xLjQK"}}),
                ),
            ),
        ] {
            let cell = json!({"cell_type":"code","source":["cell".repeat(900)],"outputs":output.clone().into_iter().collect::<Vec<_>>()}).to_string();
            let (source, expected) = if name == "lines.txt" {
                (lines.clone(), lines.clone())
            } else {
                (format!("{{\"nbformat\":4,\"cells\":[{cell}]}}"), cell)
            };
            let origin = source.find(&expected).unwrap();
            own.stage(
                OperationId::new(),
                &format!("ui/{name}"),
                source.as_bytes(),
                "text/plain",
                "selected source",
            )
            .await?;
            let mut invocation = ToolInvocation {
                operation_id: OperationId::new(),
                call_id: "selected-view".into(),
                name: "read".into(),
                arguments: json!({"path":format!("volume:///private/ui/{name}"),"include_body":true,"selection":selection}),
            };
            let (result, projection, _custody, _projection_custody) = execute(&invocation).await?;
            if output.is_some() {
                assert_eq!(projection["parts"].as_array().unwrap().len(), 2);
                assert_eq!(result.value["outputs"].as_array().unwrap().len(), 1);
            }
            let selected: ReadFileResult = serde_json::from_value(result.value.clone()).unwrap();
            let version = super::super::version::encode_source(&selected.file, None)?.0;
            assert_eq!(result.value["page"]["start"], origin);
            let mut assembled = result.value["text"].as_str().unwrap().to_owned();
            assert!(!assembled.is_empty());
            assert!(assembled.len() < expected.len());
            assert_eq!(result.value["page"]["next"], origin + assembled.len());
            while assembled.len() < expected.len() {
                invocation.operation_id = OperationId::new();
                invocation.arguments["selection"] = Value::Null;
                invocation.arguments["version"] = json!(version);
                invocation.arguments["range"] =
                    json!({"start":origin+assembled.len(),"end":origin+expected.len()});
                let (result, _, _page_custody, _page_projection_custody) =
                    execute(&invocation).await?;
                assert_eq!(result.value["page"]["start"], origin + assembled.len());
                let text = result.value["text"].as_str().unwrap();
                assert!(!text.is_empty(), "continuation made no source progress");
                assembled.push_str(text);
                assert_eq!(result.value["page"]["end"], origin + assembled.len());
            }
            assert_eq!(assembled, expected);
        }
    }
    Ok(())
}
