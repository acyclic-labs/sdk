use super::*;
use acyclic_harness::{
    conversation::{
        BoundedContentFuture, ContentIo, ContentResidencyVerifier, FileDescriptor, FileRef,
    },
    model::{FileProjectionPolicy, ModelContentPart, ModelDataPart, ToolResultContent},
};
use proptest::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
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
                        acyclic_harness::model::ToolCall::new("call".into(), "echo".into(), json!({})),
                    )),
                });
            }
            messages.push(ModelMessage {
                role: if tool_result { ModelRole::Tool } else { ModelRole::User },
                content: ModelContent::Part(part),
            });
            let tools = if tool_result {
                vec![acyclic_harness::tool::ToolDefinition {
                    name: "echo".into(), revision: "1".into(), description: "Echo".into(),
                    input_schema: json!({"type":"object"}),
                    output_schema: json!({}), projection_schema: json!({}),
                }]
            } else { vec![] };
            let request = PreparedModelRequest::prepare(ModelRequest {
                model: provider.model().clone(), messages, tools, max_output_tokens: Some(64),
            }, Limits::default(), None).unwrap();
            let extra = crate::files::extra_bytes(&request.request().messages.last().unwrap().content, u64::MAX).unwrap();
            let mut estimate = crate::files::FileText::Estimate;
            let before = crate::projection::body_with_files(&provider, request.request(), &mut estimate).unwrap();
            let mut resolved = crate::files::FileText::Ready(vec![text.clone()].into_iter());
            let after = crate::projection::body_with_files(&provider, request.request(), &mut resolved).unwrap();
            prop_assert_eq!(extra, text.len() * 6);
            prop_assert!(crate::projection::encoded_len(&after).unwrap()
                <= crate::projection::encoded_len(&before).unwrap() + extra);
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
        let marker = format!("[file: {}]", serde_json::to_string(&file)?);
        if adapter == Adapter::Chat {
            assert_eq!(
                body["messages"][0]["content"],
                format!("first{}{marker}last", std::str::from_utf8(data).unwrap())
            );
        } else {
            let parts = match adapter {
                Adapter::Anthropic => &body["messages"][0]["content"],
                Adapter::Responses => &body["input"][0]["content"],
                Adapter::Gemini => &body["contents"][0]["parts"],
                Adapter::Chat => unreachable!(),
            };
            assert_eq!(parts[0]["text"], "first");
            assert_eq!(parts[1]["text"], std::str::from_utf8(data).unwrap());
            assert_eq!(parts[2]["text"], marker);
            assert_eq!(parts[3]["text"], "last");
        }
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
            file(b"ok", "image/png")?,
            FileProjectionPolicy::Native(Box::new(acyclic_harness::model::NativeMediaPolicy {
                intent: acyclic_harness::model::NativeMediaIntent::Image {
                    detail: acyclic_harness::model::ImageDetail::Auto,
                },
                maximum_bytes: 4,
                maximum_work: 4,
                configuration: None,
            })),
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
            operation_id: acyclic_harness::OperationId::new(),
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
    let mut files = crate::files::FileText::Ready(vec!["body".into()].into_iter());
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
