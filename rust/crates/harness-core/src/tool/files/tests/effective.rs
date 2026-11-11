use super::*;
use crate::conversation::{
    BoundedContentFuture, ContentAccess, ContentHold, ContentIo, ContentResidencyVerifier,
    FilePathSelection, VolumeRef,
};
use crate::filesystem::MemoryHarnessStorage;
use crate::resources::GenerationRef;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[tokio::test]
async fn read_only_directory_pages_keep_the_run_generation() -> Result<()> {
    async fn select(
        task: Arc<crate::runtime::TaskContext>,
        arguments: Value,
    ) -> Result<ToolResult> {
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "pinned-directory".into(),
            name: "read".into(),
            arguments,
        };
        let context = ToolContext::new(task, invocation.operation_id, &invocation.call_id)?;
        read()?.executor.execute(invocation, Some(context)).await
    }
    for (prefix, directory) in [
        (crate::layers::SEED_PATH, "skills"),
        (crate::layers::SYSTEM_PATH, "sys/models"),
    ] {
        let storage = MemoryHarnessStorage::new(crate::AgentId::new(), 4096).await?;
        for name in ["a", "b"] {
            storage
                .stage(
                    OperationId::new(),
                    &format!("{directory}/{name}"),
                    b"original",
                    "text/plain",
                    name,
                )
                .await?;
        }
        let (generation, _) = storage
            .list_private_directory(directory, None, None, 1)
            .await?;
        let content = storage.content_bindings();
        let layer = crate::layers::PinnedLayer {
            volume: content.volume.clone(),
            generation: generation.clone(),
        };
        let task = Arc::new(
            crate::runtime::Bindings {
                content: Some(content.clone()),
                seed: (prefix == crate::layers::SEED_PATH).then(|| layer.clone()),
                system: (prefix == crate::layers::SYSTEM_PATH).then_some(layer),
                scope: RuntimeScope::new(
                    Capabilities::new([
                        content.volume.directory_read_capability("", &mut None)?,
                        "tool:call:read".into(),
                    ]),
                    Limits {
                        file_bytes: 4096,
                        render_bytes: 4096,
                        ..Limits::default()
                    },
                )?,
                ..crate::runtime::Bindings::local()
            }
            .build()?
            .agent_context(&Capabilities::default(), content.clone())?,
        );
        // Host publication advances this same volume after the run pins its cut.
        storage
            .stage(
                OperationId::new(),
                &format!("{directory}/0-later"),
                b"later",
                "text/plain",
                "later",
            )
            .await?;
        let (current, _) = storage
            .list_private_directory(directory, None, None, 1)
            .await?;
        assert_ne!(generation, current);
        let path = format!("{prefix}{directory}");
        let first = select(
            task.clone(),
            json!({"path":path, "selection":{"kind":"directory","limit":1}}),
        )
        .await?;
        assert_eq!(first.value["entries"][0]["name"], "a");
        let cursor = first.value["entries"][0]["version"]
            .as_str()
            .expect("original cursor");
        let ordinary = select(
            task.clone(),
            json!({"path":format!("{path}/a"), "version":cursor}),
        )
        .await?;
        assert_eq!(ordinary.value["text"], "original");
        let second = select(
            task.clone(),
            json!({"path":path, "selection":{"kind":"directory","limit":1,"after":cursor}}),
        )
        .await?;
        assert_eq!(second.value["entries"][0]["name"], "b");
        assert_eq!(second.value["hasMore"], false);
        let foreign = crate::tool::files::version::encode_path_version(
            &content.volume,
            &format!("{directory}/a"),
            &current,
            None,
        )?
        .0;
        assert!(
            select(
                task,
                json!({"path":path, "selection":{"kind":"directory","limit":1,"after":foreign}})
            )
            .await
            .is_err(),
            "a later generation's cursor escaped the run's pinned layer"
        );
    }
    Ok(())
}

#[tokio::test]
async fn waiting_reads_cover_media_and_effective_paths_and_can_be_dropped() -> Result<()> {
    use futures::FutureExt as _;
    for effective in [false, true] {
        for (selection, media) in [
            (None, "text/plain"),
            (Some(json!({"kind":"pdf"})), "application/pdf"),
            (Some(json!({"kind":"image","detail":"auto"})), "image/png"),
        ] {
            let fs = acyclic_fs::Fs::memory();
            let storage = MemoryHarnessStorage::from_providers(
                crate::AgentId::new(),
                4096,
                fs.clone(),
                fs.stream_provider(),
            )
            .await?;
            let content = storage.content_bindings();
            let task = crate::runtime::Bindings {
                scope: RuntimeScope::new(
                    Capabilities::new([
                        content.volume.directory_read_capability("", &mut None)?,
                        "tool:call:read".into(),
                    ]),
                    Limits {
                        file_bytes: 4096,
                        render_bytes: 4096,
                        ..Limits::default()
                    },
                )?,
                content: Some(content.clone()),
                ..crate::runtime::Bindings::local()
            }
            .build()?
            .agent_context(&Capabilities::default(), content)?;
            let path = if effective {
                ".volumes/effective/pending"
            } else {
                "pending"
            };
            let mut invocation = ToolInvocation {
                operation_id: OperationId::new(),
                call_id: "waiting".into(),
                name: "read".into(),
                arguments: json!({"path":path,"selection":selection}),
            };
            let context =
                ToolContext::new(Arc::new(task), invocation.operation_id, &invocation.call_id)?;
            let tool = read()?;
            assert!(
                tool.executor
                    .execute(invocation.clone(), Some(context.clone()))
                    .await
                    .is_err()
            );
            invocation.arguments["wait"] = json!(true);
            // Explicitly drop an unfinished read, as an interrupted turn does.
            let mut abandoned = tool
                .executor
                .execute(invocation.clone(), Some(context.clone()));
            assert!(abandoned.as_mut().now_or_never().is_none());
            drop(abandoned);
            if effective {
                // A suppression file is present physically but absent in the
                // effective view; waiting must require a later publication.
                storage
                    .stage(
                        OperationId::new(),
                        "pending",
                        b"off: true",
                        "text/plain",
                        "off",
                    )
                    .await?;
            }
            let mut pending = tool.executor.execute(invocation, Some(context));
            assert!(pending.as_mut().now_or_never().is_none());
            storage
                .stage(
                    OperationId::new(),
                    "unrelated",
                    b"other",
                    "text/plain",
                    "other",
                )
                .await?;
            assert!(pending.as_mut().now_or_never().is_none());
            storage
                .stage(OperationId::new(), "pending", b"published", media, "ready")
                .await?;
            let result = pending.await?;
            if selection.is_none() {
                assert_eq!(result.value["text"], "published");
            } else {
                assert_eq!(result.value["media"], selection.unwrap());
            }
        }
    }
    Ok(())
}

// Interpose one actual publication after the original filesystem range has
// selected its generation. All reads still use the authenticated storage reader.
struct AdvanceAfterSelection {
    storage: Arc<MemoryHarnessStorage>,
    inner: Arc<dyn ContentResidencyVerifier>,
    armed: AtomicBool,
    heads: AtomicUsize,
    replacement: &'static [u8],
    media_type: &'static str,
}

impl ContentResidencyVerifier for AdvanceAfterSelection {
    fn supports_read(&self, volume: &VolumeRef, io: Option<&ContentIo>) -> Result<bool> {
        self.inner.supports_read(volume, io)
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
        BoundedContentFuture::new(access.io, || async move {
            if generation.is_none() {
                self.heads.fetch_add(1, Ordering::SeqCst);
            }
            let selected = self
                .inner
                .read_private_range(volume, prefix, path, generation, range, access)?
                .await?;
            if generation.is_none() && self.armed.swap(false, Ordering::SeqCst) {
                self.storage
                    .stage(
                        OperationId::new(),
                        path,
                        self.replacement,
                        self.media_type,
                        "replacement",
                    )
                    .await?;
            }
            Ok(selected)
        })
    }

    fn read_private_path<'a, 'io>(
        &'a self,
        volume: &'a VolumeRef,
        prefix: &'a str,
        path: &'a str,
        generation: Option<&'a GenerationRef>,
        maximum: u64,
        access: ContentAccess<'a, 'io>,
    ) -> Result<
        BoundedContentFuture<
            'a,
            Option<(
                FileRef,
                acyclic_stream::OwnedBytes,
                Option<ContentHold<'io>>,
            )>,
        >,
    >
    where
        'io: 'a,
    {
        if generation.is_none() {
            self.heads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner
            .read_private_path(volume, prefix, path, generation, maximum, access)
    }
}

#[tokio::test]
async fn effective_projections_keep_the_original_selection_after_head_advances() -> Result<()> {
    let cases: [(&str, &[u8], &[u8], &str, Option<Value>, Option<&str>); 6] = [
        ("skills/text", b"original", b"changed!", "text/plain", None, Some("original")),
        ("skills/lines", b"original\nsecond\n", b"changed!\nsecond\n", "text/plain",
            Some(json!({"kind":"lines","offset":0,"limit":1})), Some("original\n")),
        ("skills/cells", br#"{"nbformat":4,"nbformat_minor":5,"metadata":{},"cells":[{"cell_type":"markdown","id":"one","metadata":{},"source":["original"]}]}"#,
            br#"{"nbformat":4,"nbformat_minor":5,"metadata":{},"cells":[{"cell_type":"markdown","id":"one","metadata":{},"source":["changed!"]}]}"#, "application/json",
            Some(json!({"kind":"cells","offset":0,"limit":1})), None),
        ("skills/auto.ipynb", br#"{"nbformat":4,"nbformat_minor":5,"metadata":{},"cells":[{"cell_type":"markdown","id":"one","metadata":{},"source":["original"]}]}"#,
            br#"{"nbformat":4,"nbformat_minor":5,"metadata":{},"cells":[{"cell_type":"markdown","id":"one","metadata":{},"source":["changed!"]}]}"#, "application/json", None, None),
        // Authored byte inputs exercise selection, not PDF parsing or rendering.
        ("skills/media", b"original", b"changed!", "application/pdf", Some(json!({"kind":"pdf"})), None),
        ("skills/auto", b"%PDF-original", b"%PDF-changed!", "application/pdf", None, None),
    ];
    for native in [false, true] {
        for (path, before, after, media_type, projection, text) in &cases {
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(100_000, 16_000_000),
                &cancellation,
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            {
                let storage =
                    Arc::new(MemoryHarnessStorage::new(crate::AgentId::new(), 4096).await?);
                let original = storage
                    .stage(OperationId::new(), path, before, media_type, "original")
                    .await?;
                let mut content = storage.content_bindings();
                let reader = Arc::new(AdvanceAfterSelection {
                    storage: storage.clone(),
                    inner: content.reader.clone(),
                    armed: AtomicBool::new(true),
                    heads: AtomicUsize::new(0),
                    replacement: after,
                    media_type,
                });
                content.reader = reader.clone();
                let limits = Limits {
                    file_bytes: 4096,
                    render_bytes: 4096,
                    ..Limits::default()
                };
                let task = crate::runtime::Bindings {
                    scope: RuntimeScope::new(
                        Capabilities::new([
                            content.volume.directory_read_capability("", &mut None)?,
                            "tool:call:read".into(),
                        ]),
                        limits,
                    )?,
                    content: Some(content.clone()),
                    ..crate::runtime::Bindings::local()
                }
                .build()?
                .agent_context(&Capabilities::default(), content)?;
                let invocation = ToolInvocation {
                    operation_id: OperationId::new(),
                    call_id: "effective".into(),
                    name: "read".into(),
                    arguments: json!({"path":format!("{}{path}", crate::layers::EFFECTIVE_PATH)}),
                };
                let mut invocation = invocation;
                if let Some(projection) = projection {
                    invocation.arguments["selection"] = projection.clone();
                }
                let context =
                    ToolContext::new(Arc::new(task), invocation.operation_id, &invocation.call_id)?;
                let tool = read()?;
                let (result, _custody) = if native {
                    let (result, custody) = tool
                        .executor
                        .execute_with_native_context(&context, &invocation, &work)?
                        .await?;
                    (result, Some(custody))
                } else {
                    (
                        tool.executor
                            .execute(invocation.clone(), Some(context.clone()))
                            .await?,
                        None,
                    )
                };
                let selected: FileReadSource = serde_json::from_value(result.value["file"].clone())
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                assert!(
                    selected.same_source(&FileReadSource::Content(original)),
                    "{path}: {selected:?}"
                );
                if let Some(text) = text {
                    assert_eq!(result.value["text"], *text);
                }
                assert_eq!(
                    reader.heads.load(Ordering::SeqCst),
                    1,
                    "{path}: reselected head"
                );

                // The real current head now differs. Restoring a current-head
                // read in any projection fails both identity and head counts.
                let (current, _, _) = context
                    .read_tool_path_range(
                        &format!("{}{path}", crate::layers::HOME_PATH),
                        None,
                        acyclic_fs::ByteRange {
                            offset: 0,
                            length: 32,
                        },
                        None,
                    )
                    .await?;
                assert!(!selected.same_source(&FileReadSource::Authored(current)));
                assert_eq!(reader.heads.load(Ordering::SeqCst), 2);
                if *path == "skills/text" {
                    // A returned version names its concrete layer. A later
                    // effective-view change cannot substitute that source.
                    let version = super::super::version::encode_source(&selected, None)?.0;
                    let pinned = ToolInvocation {
                        arguments: json!({"path":format!("{}{path}", crate::layers::HOME_PATH), "version":version}),
                        ..invocation.clone()
                    };
                    assert_eq!(
                        tool.executor
                            .execute(pinned, Some(context.clone()))
                            .await?
                            .value["text"],
                        "original"
                    );
                    assert_eq!(reader.heads.load(Ordering::SeqCst), 2);
                    let denied = ToolContext::new(
                        Arc::new(context.task().scoped(
                            Capabilities::default(),
                            context.effective_scope().limits().clone(),
                        )?),
                        invocation.operation_id,
                        &invocation.call_id,
                    )?;
                    assert_eq!(
                        tool.executor
                            .execute(invocation.clone(), Some(denied))
                            .await
                            .unwrap_err()
                            .code(),
                        crate::wire::ErrorCode::Unauthorized
                    );
                    assert_eq!(reader.heads.load(Ordering::SeqCst), 2);
                    reader
                        .storage
                        .stage(
                            OperationId::new(),
                            path,
                            b"off: true",
                            "text/plain",
                            "disabled",
                        )
                        .await?;
                    {
                        let io = if native {
                            crate::executor::PreparationIo::from_native_work(&work)?
                        } else {
                            crate::executor::PreparationIo::from_budget(
                                acyclic_fs::performance::work_budget(100_000, 16_000_000),
                                &cancellation,
                            )?
                        };
                        let before = io.owned_bytes()?;
                        let error = context
                            .effective_tool_path(
                                invocation.arguments["path"].as_str().unwrap(),
                                None,
                                Some(&io),
                            )
                            .await
                            .unwrap_err();
                        assert_eq!(error.code(), crate::wire::ErrorCode::NotFound);
                        assert!(
                            error.to_string().contains("is in no layer"),
                            "disabled selection lost its refusal: {error}"
                        );
                        assert_eq!(
                            io.owned_bytes()?,
                            before,
                            "disabled selection retained metadata"
                        );
                        let heads = reader.heads.load(Ordering::SeqCst);
                        cancellation.cancel();
                        let error = context
                            .effective_tool_path(
                                invocation.arguments["path"].as_str().unwrap(),
                                None,
                                Some(&io),
                            )
                            .await
                            .unwrap_err();
                        assert_eq!(error.code(), crate::wire::ErrorCode::Cancelled);
                        if native {
                            assert!(error.is_native_failure());
                        }
                        assert_eq!(
                            reader.heads.load(Ordering::SeqCst),
                            heads,
                            "cancelled producer was called"
                        );
                    }
                }
            }
            assert_eq!(
                work.live_allocation_bytes(),
                acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
            );
        }
    }
    Ok(())
}
