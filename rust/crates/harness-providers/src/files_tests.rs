use super::*;
use acyclic_harness_core::{
    conversation::{
        BoundedContentFuture, ContentIo, ContentResidencyVerifier, FileDescriptor, FileRef,
    },
    model::{FileProjectionPolicy, ModelContentPart, ModelDataPart, ToolResultContent},
};
use proptest::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
#[path = "native_image_tests.rs"]
mod native_image_tests;
#[path = "reference_tests.rs"]
mod reference_tests;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn file_estimates_bound_unicode_wire_expansion_for_every_adapter(
        characters in prop::collection::vec(any::<char>(), 0..128),
        tool_result in any::<bool>(),
    ) {
        let text: String = characters.into_iter().collect();
        let file = file(text.as_bytes(), "text/plain").unwrap();
        for (adapter, _) in fixtures() {
            let (provider, _) = provider(adapter, 200);
            let part = if tool_result {
                ModelContentPart::ToolResult {
                    call_id: "call".into(), name: "echo".into(),
                    content: ToolResultContent::Parts { parts: vec![ModelDataPart::File {
                        file: file.clone(), policy: FileProjectionPolicy::BoundedFull,
                    }] },
                }
            } else {
                ModelContentPart::File { file: file.clone(), policy: FileProjectionPolicy::BoundedFull }
            };
            let mut messages = Vec::new();
            if tool_result {
                messages.push(ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(ModelContentPart::ToolCall(
                        acyclic_harness_core::model::ToolCall::new("call".into(), "echo".into(), json!({})),
                    )),
                });
            }
            messages.push(ModelMessage {
                role: if tool_result { ModelRole::Tool } else { ModelRole::User },
                content: ModelContent::Part(part),
            });
            let tools = if tool_result {
                vec![acyclic_harness_core::tool::ToolDefinition {
                    name: "echo".into(), revision: "1".into(), description: "Echo".into(),
                    input_schema: json!({"type":"object"}),
                    output_schema: json!({}), projection_schema: json!({}),
                }]
            } else { vec![] };
            let request = PreparedModelRequest::prepare(ModelRequest {
                model: provider.model().clone(), messages, tools, max_output_tokens: Some(64),
            }, Limits::default(), None).unwrap();
            let extra = crate::files::extra_bytes(&request.request().messages.last().unwrap().content, u64::MAX).unwrap();
            let mut resolved = crate::files::FileText(vec![text.clone().into()].into_iter());
            let after = crate::projection::body_with_files(&provider, request.request(), &mut resolved).unwrap();
            prop_assert_eq!(extra, text.len() * 6);
            assert_expansion(&text, extra);
            // Planning starts with the complete canonical request, including the
            // file reference and framing; an omitted-file wire body is not a baseline.
            prop_assert!(crate::projection::encoded_len(&after).unwrap()
                <= request.bytes().len() + extra);
        }
    }
}

struct Reader {
    data: Vec<u8>,
    reads: AtomicUsize,
}
impl ContentResidencyVerifier for Reader {
    fn read<'a>(
        &'a self,
        _file: &'a FileRef,
        _maximum: u64,
        _ceiling: Option<&'a acyclic_harness::Capabilities>,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        BoundedContentFuture::new(io, || async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.data.clone().into())
        })
    }
}
fn file(bytes: &[u8], media: &str) -> Result<FileRef> {
    FileRef::new(
        serde_json::from_value(json!({
            "provider":{"namespace":"test","family":"filesystem","version":"2"},
            "id":"project","class":"project","owner":{"kind":"project","id":"project"},
        }))
        .map_err(|_| invalid("fixture volume"))?,
        "file.txt".into(),
        "1".into(),
        FileDescriptor::from_bytes(bytes, media, &mut None)?,
        "file.txt".into(),
        &mut None,
    )
}
fn with_parts(
    provider: &ResolvedModel,
    parts: Vec<ModelContentPart>,
) -> Result<PreparedModelRequest> {
    PreparedModelRequest::prepare(
        ModelRequest {
            model: provider.model().clone(),
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Parts(parts),
            }],
            tools: vec![],
            max_output_tokens: Some(64),
        },
        Limits::default(),
        None,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn fix_live_composition_reader_resolves_exact_file_bytes(chars in prop::collection::vec(any::<char>(), 0..64)) {
        futures::executor::block_on(async {
            let text: String = chars.into_iter().collect();
            let (provider, script) = provider(Adapter::Responses, 200);
            let request = with_parts(&provider, vec![ModelContentPart::File {
                file: file(text.as_bytes(), "text/plain").unwrap(),
                policy: FileProjectionPolicy::BoundedFull,
            }]).unwrap();
            let dispatch = ModelDispatch { request_digest: request.request_digest(), ..parser_dispatch() };
            // Negative control: the original unbound composition cannot issue HTTP.
            let error = provider.generate(request.clone(), dispatch).try_collect::<Vec<_>>().await.unwrap_err();
            assert!(error.to_string().contains("model file byte resolution is unavailable"));
            assert!(script.sent.lock().unwrap().is_empty());
            let reader = Arc::new(Reader { data: text.as_bytes().to_vec(), reads: AtomicUsize::new(0) });
            let bound = provider.bind_content(reader.clone(), 4096).unwrap();
            bound.generate(request, dispatch).try_collect::<Vec<_>>().await.unwrap();
            assert_eq!(reader.reads.load(Ordering::SeqCst), 1);
            let sent = script.sent.lock().unwrap();
            let body = crate::json(&sent[0].body).unwrap();
            let expected = if text.is_empty() { json!([]) } else { json!([
                {"type":"message","role":"user","content":[{"type":"input_text","text":text}]}
            ]) };
            assert_eq!(body["input"], expected);
        });
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn shared_preparation_matches_independent_requests_without_repeated_reads(chars in prop::collection::vec(any::<char>(), 0..96)) {
        futures::executor::block_on(async {
            let data: String = chars.into_iter().collect();
            for (adapter, _) in fixtures() {
                let (mut provider, _) = provider(adapter, 200);
                let reader = Arc::new(Reader {data:data.as_bytes().to_vec(), reads:AtomicUsize::new(0)});
                provider.files = crate::files::Files {reader:Some(reader.clone()),maximum_bytes:4096};
                let request = with_parts(&provider, vec![ModelContentPart::File {
                    file: file(data.as_bytes(), "text/plain").unwrap(), policy: FileProjectionPolicy::BoundedFull,
                }]).unwrap();
                let prepared = crate::prepared::PreparedInput::new(&provider, request.clone()).await.unwrap();
                let count = prepared.count(&provider).await.unwrap();
                // Reusing the retained HTTP bytes for generation performs no file I/O.
                let auth = crate::dispatch::authenticate(&provider, None).await.unwrap();
                let dispatch = ModelDispatch {request_digest:request.request_digest(),..parser_dispatch()};
                let names = crate::tool_names::Names::new(request.request());
                crate::dispatch::generate_encoded(&provider, &request, dispatch, prepared.generation.clone(), auth, names).try_collect::<Vec<_>>().await.unwrap();
                assert_eq!(reader.reads.load(Ordering::SeqCst), 1);
                let baseline_count = provider.count_tokens(&request).await.unwrap();
                let baseline_generation = crate::dispatch::encode(&provider, &request, &crate::tool_names::Names::new(request.request()), None, &provider.reasoning_origin(provider.key.as_deref(),None)).await.unwrap();
                assert_eq!(baseline_count, count);
                assert_eq!(baseline_generation.as_slice(), prepared.generation.as_ref());
                // Negative control: the independent old paths require two additional reads.
                assert_eq!(reader.reads.load(Ordering::SeqCst), 3);
            }
        });
    }
}

#[tokio::test]
async fn bounded_text_and_reference_files_project_in_order_with_verified_counting() -> Result<()> {
    for (adapter, _) in fixtures() {
        let (mut provider, _) = provider(adapter, 200);
        let data = "hé\n\u{0000}\"".as_bytes();
        let file = file(data, "text/plain")?;
        let reader = Arc::new(Reader {
            data: data.to_vec(),
            reads: AtomicUsize::new(0),
        });
        provider.files = crate::files::Files {
            reader: Some(reader.clone()),
            maximum_bytes: 64,
        };
        let prepared = with_parts(
            &provider,
            vec![
                ModelContentPart::Text {
                    text: "first".into(),
                },
                ModelContentPart::File {
                    file: file.clone(),
                    policy: FileProjectionPolicy::BoundedFull,
                },
                ModelContentPart::File {
                    file: file.clone(),
                    policy: FileProjectionPolicy::Reference,
                },
                ModelContentPart::Text {
                    text: "last".into(),
                },
            ],
        )?;
        let count = provider.count_tokens(&prepared).await?;
        assert_eq!(reader.reads.load(Ordering::SeqCst), 1);
        let wire = crate::dispatch::encode(
            &provider,
            &prepared,
            &crate::tool_names::Names::new(prepared.request()),
            None,
            &provider.reasoning_origin(provider.key.as_deref(), None),
        )
        .await?;
        assert_eq!(reader.reads.load(Ordering::SeqCst), 2);
        assert!(
            (u64::from(count.fixed_tokens)
                + count
                    .message_tokens
                    .iter()
                    .map(|n| u64::from(*n))
                    .sum::<u64>())
                * 2
                >= wire.len() as u64
        );
        let body = json(&wire)?;
        let marker = format!("[file: {}]", serde_json::to_string(&file).unwrap());
        let texts = |parts: &Value| {
            parts
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|part| part["text"].as_str())
                .collect::<String>()
        };
        let text = match adapter {
            Adapter::Chat => body["messages"][0]["content"].as_str().unwrap().to_owned(),
            Adapter::Anthropic => texts(&body["messages"][0]["content"]),
            Adapter::Gemini => texts(&body["contents"][0]["parts"]),
            Adapter::Responses => body["input"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| texts(&item["content"]))
                .collect(),
        };
        assert_eq!(
            text,
            format!("first{}{marker}last", std::str::from_utf8(data).unwrap())
        );
    }
    Ok(())
}

#[tokio::test]
async fn invalid_file_policies_lengths_utf8_and_digests_fail_before_http() -> Result<()> {
    let (mut provider, script) = provider(Adapter::Chat, 200);
    let reader = Arc::new(Reader {
        data: b"ok".to_vec(),
        reads: AtomicUsize::new(0),
    });
    provider.files = crate::files::Files {
        reader: Some(reader.clone()),
        maximum_bytes: 4,
    };
    for bad in [
        (
            file(b"ok", "application/zip")?,
            FileProjectionPolicy::BoundedFull,
        ),
        (
            file(b"12345", "text/plain")?,
            FileProjectionPolicy::BoundedFull,
        ),
        (
            file(b"ok", "application/zip")?,
            FileProjectionPolicy::Native(Box::new(
                acyclic_harness_core::model::NativeMediaPolicy {
                    intent: acyclic_harness_core::model::NativeMediaIntent::Image {
                        detail: acyclic_harness_core::model::ImageDetail::Auto,
                    },
                    maximum_bytes: 4,
                    maximum_work: 4,
                    configuration: None,
                },
            )),
        ),
    ] {
        let prepared = with_parts(
            &provider,
            vec![
                ModelContentPart::File {
                    file: file(b"ok", "text/plain")?,
                    policy: FileProjectionPolicy::BoundedFull,
                },
                ModelContentPart::File {
                    file: bad.0,
                    policy: bad.1,
                },
            ],
        )?;
        assert!(
            crate::dispatch::encode(
                &provider,
                &prepared,
                &crate::tool_names::Names::new(prepared.request()),
                None,
                &provider.reasoning_origin(provider.key.as_deref(), None),
            )
            .await
            .is_err()
        );
        assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
    }
    for (expected, actual) in [
        (b"other".as_slice(), b"ok".as_slice()),
        (b"\xff".as_slice(), b"\xff".as_slice()),
    ] {
        let reader = Arc::new(Reader {
            data: actual.to_vec(),
            reads: AtomicUsize::new(0),
        });
        provider.files = crate::files::Files {
            reader: Some(reader.clone()),
            maximum_bytes: 64,
        };
        let prepared = with_parts(
            &provider,
            vec![ModelContentPart::File {
                file: file(expected, "text/plain")?,
                policy: FileProjectionPolicy::BoundedFull,
            }],
        )?;
        let dispatch = ModelDispatch {
            operation_id: acyclic_harness_core::OperationId::new(),
            step: 0,
            request_digest: prepared.request_digest(),
        };
        assert!(
            provider
                .generate(prepared, dispatch)
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
        assert_eq!(reader.reads.load(Ordering::SeqCst), 1);
    }
    assert!(script.sent.lock().expect("fixture").is_empty());
    Ok(())
}

#[tokio::test]
async fn tool_result_file_parts_preserve_order_and_custom_headers_reach_the_transport() -> Result<()>
{
    let (mut provider, script) = provider(Adapter::Chat, 200);
    let reader = Arc::new(Reader {
        data: b"body".to_vec(),
        reads: AtomicUsize::new(0),
    });
    provider.files = crate::files::Files {
        reader: Some(reader.clone()),
        maximum_bytes: 64,
    };
    let content = ToolResultContent::Parts {
        parts: vec![
            ModelDataPart::Text {
                text: "first".into(),
            },
            ModelDataPart::File {
                file: file(b"body", "text/plain")?,
                policy: FileProjectionPolicy::BoundedFull,
            },
            ModelDataPart::Text {
                text: "last".into(),
            },
        ],
    };
    let mut files = crate::files::FileText(vec![String::from("body").into()].into_iter());
    let result = crate::projection::body_with_files(
        &provider,
        &ModelRequest {
            model: provider.model().clone(),
            tools: vec![],
            max_output_tokens: Some(64),
            messages: vec![ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: "call".into(),
                    name: "echo".into(),
                    content,
                }),
            }],
        },
        &mut files,
    )?;
    assert_eq!(result["messages"][0]["content"], json!("first\nbody\nlast"));
    Arc::make_mut(&mut provider.config)
        .headers
        .insert("x-owner".into(), "project".into());
    let (prepared, dispatch) = request(&provider)?;
    provider
        .generate(prepared, dispatch)
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        script.sent.lock().expect("fixture")[0].headers["x-owner"],
        "project"
    );
    let mut registry = Registry::with_transport(Credentials::explicit(BTreeMap::new()), script);
    for header in [
        "Authorization",
        "x-api-key",
        "cookie",
        "host",
        "bad\r\nname",
    ] {
        let mut spec = provider.config.as_ref().clone();
        spec.headers.insert(header.into(), "value".into());
        assert!(registry.register("custom".into(), spec).is_err());
    }
    Ok(())
}

fn assert_expansion(text: &str, bound: usize) {
    assert!(
        serde_json::to_vec(text).unwrap().len() - 2 <= bound,
        "JSON escaping exceeded file expansion bound"
    );
}

#[test]
#[should_panic(expected = "JSON escaping exceeded file expansion bound")]
fn negative_control_unescaped_file_lengths_understate_json_expansion() {
    assert_expansion("\0", 1);
}

#[test]
fn unresolved_fulltext_is_never_silently_omitted() -> Result<()> {
    assert!(
        crate::files::FileText::default()
            .project(
                &file(b"full", "text/plain")?,
                &FileProjectionPolicy::BoundedFull
            )
            .is_err()
    );
    Ok(())
}

async fn resolved_instruction(body: &str, path: &str, broken: bool) -> Result<String> {
    let (provider, _) = provider(Adapter::Chat, 200);
    let file = file(body.as_bytes(), "text/plain")?;
    let policy = FileProjectionPolicy::ProjectInstructions(Box::new(
        acyclic_harness::model::InstructionIdentity::new(file.volume().clone(), path.into())?,
    ));
    let request = with_parts(
        &provider,
        vec![ModelContentPart::File {
            file: file.clone(),
            policy: if broken {
                FileProjectionPolicy::BoundedFull
            } else {
                policy.clone()
            },
        }],
    )?;
    let reader = Arc::new(Reader {
        data: body.as_bytes().to_vec(),
        reads: AtomicUsize::new(0),
    });
    let mut resolved = crate::files::resolve(
        &crate::files::Files {
            reader: Some(reader.clone()),
            maximum_bytes: 4096,
        },
        request.request(),
        None,
    )
    .await?;
    assert_eq!(reader.reads.load(Ordering::SeqCst), 1);
    resolved.project(&file, &policy)
}

fn check_instruction_frame(actual: &str, path: &str, body: &str) {
    let (directory, filename) = path.rsplit_once('/').unwrap_or((".", path));
    assert_eq!(
        actual,
        format!(
            "# {filename} instructions for {directory}\n\n<INSTRUCTIONS>\n{body}\n</INSTRUCTIONS>"
        ),
        "instruction framing was lost"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn retained_instruction_files_keep_their_source_frame_without_rereading(
        body in prop::collection::vec(any::<char>(), 0..32), nested in any::<bool>(),
    ) {
        let body: String = body.into_iter().collect();
        let path = if nested { "nested/AGENTS.md" } else { "CLAUDE.md" };
        let actual = futures::executor::block_on(resolved_instruction(&body, path, false)).unwrap();
        check_instruction_frame(&actual, path, &body);
    }
}

#[test]
#[should_panic(expected = "instruction framing was lost")]
fn raw_instruction_projection_negative_control() {
    let actual =
        futures::executor::block_on(resolved_instruction("body", "AGENTS.md", true)).unwrap();
    check_instruction_frame(&actual, "AGENTS.md", "body");
}
