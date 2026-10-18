use super::*;
use acyclic_harness::{
    AgentId, Capabilities, IdempotencyKey,
    conversation::{
        BoundedContentFuture, ContentGrant, ContentIo, ContentPublisher, ContentResidencyVerifier,
        FileRef, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer},
    filesystem::{FilesystemContentPublisher, FilesystemContentVerifier, FilesystemHost},
    model::{
        FileProjectionPolicy, ImageDetail, ModelContentPart, ModelDataPart, NativeMediaIntent,
        NativeMediaPolicy, ToolResultContent,
    },
    resources::ProviderRef,
};
use base64::Engine as _;
use std::sync::atomic::{AtomicUsize, Ordering};

#[path = "image_anchor_tests.rs"]
mod anchors;
#[path = "image_catalog_tests.rs"]
mod catalog;
#[path = "image_dedup_tests.rs"]
mod dedup;
#[path = "image_native_tests.rs"]
mod native;
#[path = "image_projection_tests.rs"]
mod projection_performance;

fn reasoning_blocks(adapter: Adapter) -> usize {
    match adapter {
        Adapter::Responses => 0,
        Adapter::Gemini => 2,
        _ => 1,
    }
}

const PNG: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR4nGP4/x8AAwAB//wl3FEAAAAASUVORK5CYII=";

struct Reads {
    inner: Arc<dyn ContentResidencyVerifier>,
    calls: AtomicUsize,
}

impl ContentResidencyVerifier for Reads {
    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        maximum: u64,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.read(file, maximum, io)
    }
}

async fn file(
    bytes: &[u8],
    mime: &str,
    denied: bool,
    io: Option<&ContentIo>,
) -> Result<(FileRef, Arc<Reads>, Arc<dyn ContentPublisher>)> {
    let agent = AgentId::from_bytes([81; 16]);
    let provider = ProviderRef::new("images".into(), "filesystem".into(), "1".into(), &mut None)?;
    let volume = VolumeRef::new(
        provider.clone(),
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
    host.create_volume(&volume).await?;
    let issuer = AuthorityIssuer::new(
        "images",
        [81; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "images".into(),
        },
    );
    let capabilities = Capabilities::new([
        volume.capability(VolumeOperation::Read, &mut None)?,
        volume.capability(VolumeOperation::Write, &mut None)?,
    ]);
    let scope = issuer.issue("images", &capabilities, Some(agent), None, &mut None)?;
    let verifier = issuer.verifier(&mut None)?;
    let write = ContentGrant::verify(
        &verifier,
        &scope,
        &volume,
        VolumeOperation::Write,
        65_536,
        &mut None,
    )?;
    let file = host
        .put_content(
            &volume,
            &write,
            "attachments/image.png",
            bytes,
            mime,
            "image.png",
            65_536,
            &IdempotencyKey::new("image")?,
            None,
            None,
        )
        .await?;
    let publisher =
        FilesystemContentPublisher::new(host.clone(), &volume, &verifier, &scope, 65_536, None)?;
    let scope = if denied {
        issuer.issue(
            "denied",
            &Capabilities::default(),
            Some(agent),
            None,
            &mut None,
        )?
    } else {
        scope
    };
    let reader = FilesystemContentVerifier::new(host, &verifier, &scope, 65_536, io)?;
    Ok((
        file,
        Arc::new(Reads {
            inner: reader,
            calls: AtomicUsize::new(0),
        }),
        publisher,
    ))
}

fn image(file: &FileRef) -> ModelContentPart {
    ModelContentPart::File {
        file: file.clone(),
        policy: policy(),
    }
}

fn policy() -> FileProjectionPolicy {
    FileProjectionPolicy::Native(Box::new(NativeMediaPolicy {
        intent: NativeMediaIntent::Image {
            detail: ImageDetail::High,
        },
        maximum_bytes: 4096,
        maximum_work: 1024,
        configuration: None,
    }))
}

fn enable(provider: &mut ResolvedModel) {
    Arc::make_mut(&mut provider.config).images.insert(
        provider.model.name.clone(),
        ImageInput {
            maximum_bytes: 4096,
            maximum_total_bytes: 8192,
            maximum_images: 4,
            maximum_dimension: 1024,
            maximum_tokens: 1024,
        },
    );
}

fn prepare(
    provider: &ResolvedModel,
    messages: Vec<ModelMessage>,
) -> Result<(PreparedModelRequest, ModelDispatch)> {
    let tools = messages
        .iter()
        .flat_map(|message| message.content.parts())
        .filter_map(|part| {
            let ModelContentPart::ToolCall(call) = part else {
                return None;
            };
            Some(acyclic_harness::tool::ToolDefinition {
                name: call.name.clone(),
                revision: "1".into(),
                description: "Image fixture tool".into(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({}),
                projection_schema: json!({}),
            })
        })
        .collect();
    let request = PreparedModelRequest::prepare(
        ModelRequest {
            model: provider.model.clone(),
            messages,
            tools,
            max_output_tokens: Some(64),
        },
        Limits::default(),
        None,
    )?;
    let dispatch = ModelDispatch {
        operation_id: acyclic_harness::OperationId::new(),
        step: 0,
        request_digest: request.request_digest(),
    };
    Ok((request, dispatch))
}

fn user(parts: Vec<ModelContentPart>) -> ModelMessage {
    ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Parts(parts),
    }
}

fn text(value: &str) -> ModelContentPart {
    ModelContentPart::Text { text: value.into() }
}

fn checked_body(adapter: Adapter, bytes: &[u8]) -> Result<serde_json::Value> {
    let body = json(bytes)?;
    assert!(
        crate::validate_input(adapter, &body).is_empty(),
        "{adapter:?} {body}"
    );
    Ok(body)
}

fn expected_image(adapter: Adapter) -> serde_json::Value {
    let fixtures = json(include_bytes!("fixtures/image-parts.json")).expect("image wire fixtures");
    let name = serde_json::to_value(adapter).expect("adapter");
    fixtures[name.as_str().expect("adapter name")].clone()
}

fn message_parts<'a>(
    adapter: Adapter,
    body: &'a serde_json::Value,
    index: usize,
) -> &'a serde_json::Value {
    match adapter {
        Adapter::Chat | Adapter::Anthropic => &body["messages"][index]["content"],
        Adapter::Responses => &body["input"][index]["content"],
        Adapter::Gemini => &body["contents"][index]["parts"],
    }
}

#[tokio::test]
async fn image_planning_excludes_file_identity_metadata() -> Result<()> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(PNG)
        .expect("PNG");
    let (file, reads, _) = file(&bytes, "image/png", false, None).await?;
    let mut renamed = serde_json::to_value(&file).expect("file");
    renamed["display_name"] = json!("a much longer display name for the same image.png");
    let renamed: FileRef = serde_json::from_value(renamed).expect("renamed image");
    for (adapter, _) in fixtures() {
        let (mut provider, _) = provider(adapter, 200);
        enable(&mut provider);
        let (first, _) = prepare(&provider, vec![user(vec![image(&file)])])?;
        let (second, _) = prepare(&provider, vec![user(vec![image(&renamed)])])?;
        assert_ne!(first.request_digest(), second.request_digest());
        assert_ne!(
            first.bytes().len(),
            second.bytes().len(),
            "canonical-byte negative control"
        );
        let first = crate::projection::plan(&provider, &first, 1234)?;
        let second = crate::projection::plan(&provider, &second, 1234)?;
        assert_eq!(first.message_tokens, second.message_tokens);
        assert_eq!(first.fixed_tokens, second.fixed_tokens);
        assert_eq!(
            first.input_tokens, 1234,
            "verified admission count stays authoritative"
        );
    }
    assert_eq!(reads.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn image_adapters_send_ordered_verified_parts_from_files_and_history() -> Result<()> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(PNG)
        .expect("PNG fixture");
    for (adapter, _) in fixtures() {
        let (file, reads, _) = file(&bytes, "image/png", false, None).await?;
        let (mut provider, script) = provider(adapter, 200);
        enable(&mut provider);
        provider.files.maximum_bytes = 1;
        let provider = provider.with_content_reader(reads.clone());
        let messages = vec![
            user(vec![
                text("before"),
                image(&file),
                text("between"),
                image(&file),
                text("after"),
            ]),
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("earlier answer".into()),
            },
            user(vec![text("again"), image(&file)]),
        ];
        let (request, dispatch) = prepare(&provider, messages)?;
        let count = provider.count_tokens(&request).await?;
        assert!(count.message_tokens[0] >= 2048);
        assert!(count.message_tokens[0] < 2600);
        assert_eq!(
            reads.calls.load(Ordering::SeqCst),
            1,
            "verified counting resolves each distinct immutable image once"
        );
        let digest = request.request_digest();
        let events = provider
            .generate(request.clone(), dispatch)
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(events.len(), 4 + reasoning_blocks(adapter));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ModelEvent::ReasoningBlock { .. }))
                .count(),
            reasoning_blocks(adapter)
        );
        assert_eq!(digest, request.request_digest());
        let recounted = provider.count_tokens(&request).await?;
        assert_eq!(count.message_tokens, recounted.message_tokens);
        assert_eq!(count.fixed_tokens, recounted.fixed_tokens);
        assert_eq!(reads.calls.load(Ordering::SeqCst), 3);
        let sent = script.sent.lock().expect("capture");
        assert_eq!(sent.len(), 1);
        let body = checked_body(adapter, &sent[0].body)?;
        let parts = message_parts(adapter, &body, 0).as_array().expect("parts");
        assert_eq!(parts.len(), 5, "{adapter:?}");
        for (index, value) in [(0, "before"), (2, "between"), (4, "after")] {
            assert_eq!(parts[index]["text"], value);
        }
        assert_eq!(parts[1], expected_image(adapter));
        assert_eq!(parts[3], expected_image(adapter));
        assert_eq!(message_parts(adapter, &body, 2)[1], expected_image(adapter));
    }
    Ok(())
}

#[tokio::test]
async fn image_tool_results_keep_each_provider_wire_shape() -> Result<()> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(PNG)
        .expect("PNG fixture");
    for (adapter, _) in fixtures() {
        let (file, reads, _) = file(&bytes, "image/png", false, None).await?;
        let (mut provider, script) = provider(adapter, 200);
        enable(&mut provider);
        let provider = provider.with_content_reader(reads);
        let messages = vec![
            user(vec![text("take a screenshot")]),
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Parts(vec![ModelContentPart::ToolCall(
                    acyclic_harness::model::ToolCall::new(
                        "shot".into(),
                        "screenshot".into(),
                        json!({}),
                    ),
                )]),
            },
            ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Parts(vec![ModelContentPart::ToolResult {
                    call_id: "shot".into(),
                    name: "screenshot".into(),
                    content: ToolResultContent::Parts {
                        parts: vec![
                            ModelDataPart::Text {
                                text: "caption".into(),
                            },
                            ModelDataPart::File {
                                file,
                                policy: policy(),
                            },
                        ],
                    },
                }]),
            },
        ];
        let (request, dispatch) = prepare(&provider, messages)?;
        provider
            .generate(request, dispatch)
            .try_collect::<Vec<_>>()
            .await?;
        let sent = script.sent.lock().expect("capture");
        let body = checked_body(adapter, &sent[0].body)?;
        match adapter {
            Adapter::Chat => {
                assert_eq!(body["messages"][2]["role"], "tool");
                assert_eq!(body["messages"][2]["content"], "caption");
                assert_eq!(body["messages"][3]["role"], "user");
                assert_eq!(body["messages"][3]["content"][1], expected_image(adapter));
            }
            Adapter::Responses => {
                assert_eq!(body["input"][2]["output"][1], expected_image(adapter))
            }
            Adapter::Anthropic => assert_eq!(
                body["messages"][2]["content"][0]["content"][1],
                expected_image(adapter)
            ),
            Adapter::Gemini => {
                assert_eq!(
                    body["contents"][2]["parts"][0]["functionResponse"]["response"]["output"],
                    "caption"
                );
                assert_eq!(body["contents"][2]["parts"][2], expected_image(adapter));
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn image_fallbacks_never_read_files() -> Result<()> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(PNG)
        .expect("PNG fixture");
    for (adapter, _) in fixtures() {
        for case in ["model", "format", "system", "assistant"] {
            let mime = if case == "format" {
                "image/svg+xml"
            } else {
                "image/png"
            };
            let (file, reads, _) = file(&bytes, mime, true, None).await?;
            let (mut provider, script) = provider(adapter, 200);
            if case != "model" {
                enable(&mut provider);
            }
            let provider = provider.with_content_reader(reads.clone());
            let role = match case {
                "system" => ModelRole::System,
                "assistant" => ModelRole::Assistant,
                _ => ModelRole::User,
            };
            let mut messages = vec![ModelMessage {
                role,
                content: ModelContent::Parts(vec![image(&file)]),
            }];
            if role != ModelRole::User {
                messages.push(user(vec![text("question")]));
            }
            let (request, dispatch) = prepare(&provider, messages)?;
            provider
                .generate(request, dispatch)
                .try_collect::<Vec<_>>()
                .await?;
            assert_eq!(reads.calls.load(Ordering::SeqCst), 0, "{adapter:?} {case}");
            let sent = script.sent.lock().expect("capture");
            let body = checked_body(adapter, &sent[0].body)?;
            assert!(body.to_string().contains("omitted"));
            assert!(!body.to_string().contains(PNG));
        }
    }
    Ok(())
}

#[tokio::test]
async fn image_prompt_files_resolve_utf8_on_the_same_authorized_path() -> Result<()> {
    for (adapter, _) in fixtures() {
        for bytes in ["hé\n\0".as_bytes(), &[0xff]] {
            let (file, reads, _) = file(bytes, "text/plain", false, None).await?;
            let (provider, script) = provider(adapter, 200);
            let provider = provider.with_content_reader(reads.clone());
            let (request, dispatch) = prepare(
                &provider,
                vec![user(vec![ModelContentPart::File {
                    file,
                    policy: FileProjectionPolicy::BoundedFull,
                }])],
            )?;
            assert_eq!(
                crate::images::encoded_bytes(&provider, request.request())?,
                0,
                "text files use the shared text projection admission"
            );
            let counted = provider.count_tokens(&request).await;
            if std::str::from_utf8(bytes).is_ok() {
                assert!(counted?.message_tokens[0] >= bytes.len() as u32);
            } else {
                assert!(counted.is_err());
            }
            assert_eq!(reads.calls.load(Ordering::SeqCst), 1);
            let result = provider
                .generate(request, dispatch)
                .try_collect::<Vec<_>>()
                .await;
            let sent = script.sent.lock().expect("capture");
            match std::str::from_utf8(bytes) {
                Err(_) => {
                    assert!(result.is_err());
                    assert!(sent.is_empty());
                }
                Ok(text) => {
                    result?;
                    let body = checked_body(adapter, &sent[0].body)?;
                    let actual = match adapter {
                        Adapter::Chat => &body["messages"][0]["content"],
                        Adapter::Responses => &body["input"][0]["content"][0]["text"],
                        _ => &message_parts(adapter, &body, 0)[0]["text"],
                    };
                    assert_eq!(actual, text);
                }
            }
            assert_eq!(reads.calls.load(Ordering::SeqCst), 2);
        }
    }
    Ok(())
}

#[tokio::test]
async fn image_bounds_and_authority_fail_before_http() -> Result<()> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(PNG)
        .expect("PNG fixture");
    for (adapter, _) in fixtures() {
        for case in [
            "count",
            "aggregate",
            "bytes",
            "work",
            "header",
            "mime",
            "dimensions",
            "denied",
            "integrity",
            "reader",
        ] {
            let mut bytes = bytes.clone();
            let mime = if case == "mime" {
                "image/jpeg"
            } else {
                "image/png"
            };
            if case == "header" {
                bytes.fill(0);
            }
            if case == "dimensions" {
                bytes[16..20].copy_from_slice(&4096u32.to_be_bytes());
            }
            let (mut file, reads, _) = file(&bytes, mime, case == "denied", None).await?;
            if case == "integrity" {
                file = FileRef::new(
                    file.volume().clone(),
                    file.path().into(),
                    file.version().into(),
                    acyclic_harness::conversation::FileDescriptor::new(
                        [0; 32],
                        bytes.len() as u64,
                        mime.into(),
                    )?,
                    file.display_name().into(),
                    &mut None,
                )?;
            }
            let (mut provider, script) = provider(adapter, 200);
            enable(&mut provider);
            let limits = Arc::make_mut(&mut provider.config)
                .images
                .get_mut("model")
                .expect("limits");
            match case {
                "count" => limits.maximum_images = 1,
                "aggregate" => limits.maximum_total_bytes = file.descriptor().byte_length(),
                "bytes" => limits.maximum_bytes = file.descriptor().byte_length() - 1,
                _ => {}
            }
            let mut parts = vec![image(&file)];
            if matches!(case, "count" | "aggregate") {
                parts.push(image(&file));
            }
            if case == "work" {
                let ModelContentPart::File {
                    policy: FileProjectionPolicy::Native(policy),
                    ..
                } = &mut parts[0]
                else {
                    unreachable!()
                };
                policy.maximum_work = 1;
            }
            if case != "reader" {
                provider = provider.with_content_reader(reads.clone());
            }
            let (request, dispatch) = prepare(&provider, vec![user(parts)])?;
            assert!(
                provider
                    .generate(request, dispatch)
                    .try_collect::<Vec<_>>()
                    .await
                    .is_err(),
                "{adapter:?} {case}"
            );
            assert!(script.sent.lock().expect("capture").is_empty());
            let expected = usize::from(matches!(
                case,
                "header" | "mime" | "dimensions" | "denied" | "integrity"
            ));
            assert_eq!(
                reads.calls.load(Ordering::SeqCst),
                expected,
                "{adapter:?} {case}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn image_attach_and_paste_survive_manifest_reopen_and_model_projection() -> Result<()> {
    use acyclic_harness::{
        client::{attach_image, paste_image},
        conversation::{
            ConversationMessage, ConversationState, MessageKind, ModelContextSelection,
            ReferencedAttachments, encode_attachment_manifest, load_manifest,
        },
        projection::select_model_context,
        tui::{ClipboardContent, prepare_image},
    };

    let bytes = base64::engine::general_purpose::STANDARD
        .decode(PNG)
        .expect("PNG fixture");
    for (adapter, _) in fixtures() {
        let (original, reads, publisher) = file(&bytes, "image/png", false, None).await?;
        let attached = attach_image(original.clone(), reads.as_ref(), 4096).await?;
        assert_eq!(
            attached.file, original,
            "attachment retains the selected version"
        );
        let clipboard = prepare_image(2, 2, vec![255; 16]).expect("clipboard image");
        let ClipboardContent::Image { jpeg, .. } = &clipboard else {
            unreachable!()
        };
        let paste = acyclic_harness::OperationId::new();
        let pasted = paste_image(publisher.as_ref(), paste, &clipboard, 4096).await?;
        assert_eq!(
            paste_image(publisher.as_ref(), paste, &clipboard, 4096).await?,
            pasted
        );
        assert_eq!(pasted.file.volume(), original.volume());
        assert_eq!(pasted.file.descriptor().media_type(), "image/jpeg");
        let attachments = vec![attached, pasted];
        let manifest_bytes = encode_attachment_manifest(&attachments)?;
        let (manifest, hold) = publisher
            .stage_once(
                acyclic_harness::OperationId::new(),
                "drafts/input/attachments.json",
                &manifest_bytes,
                "application/vnd.acyclic.harness.attachments+json",
                "attachments.json",
                None,
            )?
            .await?;
        drop(hold);
        let (text, hold) = publisher
            .stage_once(
                acyclic_harness::OperationId::new(),
                "drafts/input/prompt.txt",
                b"describe both images",
                "text/plain",
                "prompt.txt",
                None,
            )?
            .await?;
        drop(hold);
        let (restored, hold) = load_manifest(reads.as_ref(), &manifest, 2, None).await?;
        assert_eq!(restored, attachments);
        drop((restored, hold));
        let message = ConversationMessage {
            id: "10000000-0000-0000-0000-000000000001".parse().expect("id"),
            sequence: 1,
            kind: MessageKind::User,
            content: text,
            attachments: ReferencedAttachments::Manifest {
                manifest,
                item_count: 2,
            },
            reply_to: None,
            tool_call_id: None,
            extensions: BTreeMap::new(),
        };
        let mut history = ConversationState::default();
        history.bind(AgentId::from_bytes([81; 16]))?;
        history.append(message.clone(), &mut None)?;
        // Reopen only the canonical files and references, not transient provider parts.
        let history: ConversationState =
            serde_json::from_slice(&serde_json::to_vec(&history).expect("history"))
                .expect("reopen history");
        let selected = select_model_context(
            &history,
            1,
            ModelContextSelection {
                checkpoint: None,
                conversation_revision: 1,
                message_ids: vec![message.id],
            },
            reads.as_ref(),
            Limits::default(),
            2,
            None,
        )
        .await?;
        let (mut provider, script) = provider(adapter, 200);
        enable(&mut provider);
        let provider = provider.with_content_reader(reads);
        let (request, dispatch) = prepare(&provider, selected.messages)?;
        provider
            .generate(request, dispatch)
            .try_collect::<Vec<_>>()
            .await?;
        let sent = script.sent.lock().expect("capture");
        let body = checked_body(adapter, &sent[0].body)?;
        let parts = message_parts(adapter, &body, 0).as_array().expect("parts");
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0]["text"], "describe both images");
        let encoded_jpeg = base64::engine::general_purpose::STANDARD.encode(jpeg);
        match adapter {
            Adapter::Chat => {
                assert_eq!(
                    parts[1]["image_url"]["url"],
                    format!("data:image/png;base64,{PNG}")
                );
                assert_eq!(
                    parts[2]["image_url"]["url"],
                    format!("data:image/jpeg;base64,{encoded_jpeg}")
                );
            }
            Adapter::Responses => {
                assert_eq!(
                    parts[1]["image_url"],
                    format!("data:image/png;base64,{PNG}")
                );
                assert_eq!(
                    parts[2]["image_url"],
                    format!("data:image/jpeg;base64,{encoded_jpeg}")
                );
            }
            Adapter::Anthropic => {
                assert_eq!(parts[1]["source"]["data"], PNG);
                assert_eq!(parts[2]["source"]["data"], encoded_jpeg);
            }
            Adapter::Gemini => {
                assert_eq!(parts[1]["inlineData"]["data"], PNG);
                assert_eq!(parts[2]["inlineData"]["data"], encoded_jpeg);
            }
        }
    }
    Ok(())
}
