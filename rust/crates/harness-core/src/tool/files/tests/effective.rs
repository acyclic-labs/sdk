use super::*;
use crate::conversation::{
    BoundedContentFuture, ContentAccess, ContentHold, ContentIo, ContentResidencyVerifier,
    FilePathSelection, VolumeRef,
};
use crate::filesystem::MemoryHarnessStorage;
use crate::resources::GenerationRef;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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
            (
                FileRef,
                acyclic_stream::OwnedBytes,
                Option<ContentHold<'io>>,
            ),
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
                    if native {
                        let io = crate::executor::PreparationIo::from_native_work(&work)?;
                        let before = io.owned_bytes()?;
                        let error = context
                            .effective_tool_path(
                                invocation.arguments["path"].as_str().unwrap(),
                                Some(&io),
                            )
                            .await
                            .unwrap_err();
                        assert_eq!(error.code(), crate::wire::ErrorCode::NotFound);
                        assert_eq!(
                            io.owned_bytes()?,
                            before,
                            "disabled selection retained metadata"
                        );
                        let heads = reader.heads.load(Ordering::SeqCst);
                        cancellation.cancel();
                        assert!(
                            context
                                .effective_tool_path(
                                    invocation.arguments["path"].as_str().unwrap(),
                                    Some(&io)
                                )
                                .await
                                .unwrap_err()
                                .is_native_failure()
                        );
                        assert_eq!(
                            reader.heads.load(Ordering::SeqCst),
                            heads,
                            "cancelled producer was called"
                        );
                    } else {
                        assert_eq!(
                            tool.executor
                                .execute(invocation, Some(context))
                                .await
                                .unwrap_err()
                                .code(),
                            crate::wire::ErrorCode::NotFound
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
