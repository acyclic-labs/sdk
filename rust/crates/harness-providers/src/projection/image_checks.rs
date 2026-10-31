//! Shared shrinking/fuzz probe through verified resolution and request encoding.
use crate::{Adapter, Credentials, HttpRequest, HttpResponse, HttpTransport, Registry};
use acyclic_harness_core::{
    Result,
    conversation::{
        BoundedContentFuture, ContentIo, ContentResidencyVerifier, FileDescriptor, FileRef, Limits,
    },
    model::{
        FileProjectionPolicy, ImageDetail, ModelContent, ModelContentPart, ModelDataPart,
        ModelMessage, ModelRequest, ModelRole, NativeMediaIntent, NativeMediaPolicy,
        PreparedModelRequest, ToolCall, ToolResultContent, image_media_type,
    },
};
use acyclic_stream::{BoxProviderFuture, OwnedBytes};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct NoHttp;
impl HttpTransport for NoHttp {
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        panic!("encoding must not send HTTP")
    }
    fn delay(&self, _: Duration) -> BoxProviderFuture<'static, ()> {
        panic!("encoding must not wait")
    }
    fn now(&self) -> Duration {
        Duration::ZERO
    }
}
struct Reader {
    bytes: Vec<Vec<u8>>,
    reads: AtomicUsize,
}
impl ContentResidencyVerifier for Reader {
    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        _: u64,
        ceiling: Option<&'a acyclic_harness_core::Capabilities>,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, OwnedBytes>> {
        if ceiling.is_some() {
            return Err(acyclic_harness_core::Error::Unsupported(
                "fixture image reader cannot enforce a caller ceiling".into(),
            ));
        }
        BoundedContentFuture::new(io, || async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(self
                .bytes
                .iter()
                .find(|bytes| bytes.len() as u64 == file.descriptor().byte_length())
                .expect("generated image length")
                .clone()
                .into())
        })
    }
}

pub(super) fn assert_roundtrip(input: &[u8]) {
    futures::executor::block_on(async {
        let images: Vec<Vec<u8>> = [
            b"\x89PNG\r\n\x1a\n".as_slice(),
            b"\xff\xd8\xff",
            b"GIF89a",
            b"RIFF0000WEBP",
            b"%PDF-1.7\n",
        ]
        .into_iter()
        .map(|header| [header, input].concat())
        .collect();
        let maximum_bytes = images.iter().map(Vec::len).max().unwrap() as u64;
        for adapter in [
            Adapter::Responses,
            Adapter::Chat,
            Adapter::Anthropic,
            Adapter::Gemini,
        ] {
            let reader = Arc::new(Reader {
                bytes: images.clone(),
                reads: AtomicUsize::new(0),
            });
            let mut registry = Registry::with_transport(
                Credentials::explicit(Default::default()),
                Arc::new(NoHttp),
            )
            .with_content(reader.clone(), maximum_bytes * 2);
            registry.register("image-test".into(), serde_json::from_value(json!({
                "adapter":adapter,"authentication":"optional","base_url":"http://127.0.0.1:1/v1",
                "models":{"tiny":{"capacity":{"context_tokens":32768,"output_tokens":1024},"features":{"images":true,"tools":true}}},
                "max_event_bytes":4096,
                "token_counter":{"path":"count_tokens","adapter":adapter,"response_field":"input_tokens","framing_tokens":0}
            })).unwrap()).unwrap();
            let provider = registry.resolve("image-test/tiny").unwrap();
            for bytes in &images {
                reader.reads.store(0, Ordering::SeqCst);
                let media = image_media_type(bytes).unwrap_or("application/pdf");
                let encoded_work = bytes.len() as u64;
                let file = FileRef::new(
                    serde_json::from_value(json!({
                        "provider":{"namespace":"test","family":"filesystem","version":"2"},
                        "id":"project","class":"project","owner":{"kind":"project","id":"project"}
                    }))
                    .unwrap(),
                    format!("image-{}", bytes.len()),
                    "1".into(),
                    FileDescriptor::from_bytes(&bytes, media, &mut None).unwrap(),
                    "image".into(),
                    &mut None,
                )
                .unwrap();
                for work in [1, encoded_work - 1, encoded_work] {
                    check_projection(&provider, &reader, &file, bytes, adapter, work).await;
                }
            }
        }
    });
    let _ = image_media_type(input);
}

async fn check_projection(
    provider: &crate::ResolvedModel,
    reader: &Reader,
    file: &FileRef,
    bytes: &[u8],
    adapter: Adapter,
    work: u64,
) {
    let encoded_work = bytes.len() as u64;
    let request = PreparedModelRequest::prepare(
        ModelRequest {
            model: provider.model().clone(),
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Parts(vec![
                    ModelContentPart::Text {
                        text: "before".into(),
                    },
                    ModelContentPart::File {
                        file: file.clone(),
                        policy: FileProjectionPolicy::Native(Box::new(NativeMediaPolicy {
                            intent: if file.descriptor().media_type() == "application/pdf" {
                                NativeMediaIntent::Document
                            } else {
                                NativeMediaIntent::Image {
                                    detail: ImageDetail::High,
                                }
                            },
                            maximum_bytes: bytes.len() as u64,
                            maximum_work: work,
                            configuration: None,
                        })),
                    },
                    ModelContentPart::Text {
                        text: "after".into(),
                    },
                ]),
            }],
            tools: vec![],
            max_output_tokens: Some(64),
        },
        Limits::default(),
        None,
    )
    .unwrap();
    let encoded = crate::dispatch::encode(
        provider,
        &request,
        &crate::tool_names::Names::new(request.request()),
        None,
        &provider.reasoning_origin(None, None),
    )
    .await;
    if work < encoded_work {
        assert!(
            encoded
                .unwrap_err()
                .to_string()
                .contains("projection work limit")
        );
        assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
        return;
    }
    let body: Value = serde_json::from_slice(&encoded.unwrap()).unwrap();
    assert!(crate::validate_input(adapter, &body).is_empty(), "{body}");
    let parts: Vec<_> = match adapter {
        Adapter::Responses => body["input"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|item| item["content"].as_array().unwrap())
            .collect(),
        Adapter::Chat | Adapter::Anthropic => body["messages"][0]["content"]
            .as_array()
            .unwrap()
            .iter()
            .collect(),
        Adapter::Gemini => body["contents"][0]["parts"]
            .as_array()
            .unwrap()
            .iter()
            .collect(),
    };
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0]["text"], "before");
    assert_eq!(parts[2]["text"], "after");
    let data = match adapter {
        Adapter::Responses if file.descriptor().media_type() == "application/pdf" => {
            parts[1]["file_data"]
                .as_str()
                .unwrap()
                .split_once(',')
                .unwrap()
                .1
        }
        Adapter::Chat if file.descriptor().media_type() == "application/pdf" => {
            parts[1]["file"]["file_data"]
                .as_str()
                .unwrap()
                .split_once(',')
                .unwrap()
                .1
        }
        Adapter::Responses => {
            parts[1]["image_url"]
                .as_str()
                .unwrap()
                .split_once(',')
                .unwrap()
                .1
        }
        Adapter::Chat => {
            parts[1]["image_url"]["url"]
                .as_str()
                .unwrap()
                .split_once(',')
                .unwrap()
                .1
        }
        Adapter::Anthropic => parts[1]["source"]["data"].as_str().unwrap(),
        Adapter::Gemini => parts[1]["inlineData"]["data"].as_str().unwrap(),
    };
    assert_eq!(STANDARD.decode(data).unwrap().as_slice(), bytes);
    assert_eq!(reader.reads.load(Ordering::SeqCst), 1);
    check_result_media(provider, reader, request.request(), file, bytes, adapter).await;
}

async fn check_result_media(
    provider: &crate::ResolvedModel,
    reader: &Reader,
    request: &ModelRequest,
    file: &FileRef,
    bytes: &[u8],
    adapter: Adapter,
) {
    let ModelContentPart::File { policy, .. } = &request.messages[0].content.parts()[1] else {
        panic!("generated native input");
    };
    let mut request = request.clone();
    request
        .tools
        .push(acyclic_harness_core::tool::ToolDefinition {
            name: "read".into(),
            revision: "1".into(),
            description: "fixture".into(),
            input_schema: json!({"type":"object"}),
            output_schema: json!({}),
            projection_schema: json!({}),
        });
    request.messages = vec![
        ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("read both".into()),
        },
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Parts(
                (0..2)
                    .map(|index| {
                        ModelContentPart::ToolCall(ToolCall::new(
                            format!("read-{index}"),
                            "read".into(),
                            json!({}),
                        ))
                    })
                    .collect(),
            ),
        },
    ];
    for index in (0..2).rev() {
        request.messages.push(ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Part(ModelContentPart::ToolResult {
                call_id: format!("read-{index}"),
                name: "read".into(),
                content: ToolResultContent::Parts {
                    parts: vec![
                        ModelDataPart::Text {
                            text: format!("source-{index}"),
                        },
                        ModelDataPart::File {
                            file: file.clone(),
                            policy: policy.clone(),
                        },
                    ],
                },
            }),
        });
    }
    let prepared = PreparedModelRequest::prepare(request, Limits::default(), None).unwrap();
    reader.reads.store(0, Ordering::SeqCst);
    let encoded = crate::dispatch::encode(
        provider,
        &prepared,
        &crate::tool_names::Names::new(prepared.request()),
        None,
        &provider.reasoning_origin(None, None),
    )
    .await
    .unwrap();
    let body: Value = serde_json::from_slice(&encoded).unwrap();
    assert!(
        crate::validate_input(adapter, &body).is_empty(),
        "{adapter:?}: {body}"
    );
    let mut found = Vec::new();
    collect_media(&body, &mut found);
    assert_eq!(found, vec![bytes.to_vec(), bytes.to_vec()]);
    assert_eq!(reader.reads.load(Ordering::SeqCst), 2);
    if matches!(adapter, Adapter::Chat | Adapter::Responses) {
        let items = body[if adapter == Adapter::Chat {
            "messages"
        } else {
            "input"
        }]
        .as_array()
        .unwrap();
        let last_result = items
            .iter()
            .rposition(|item| item["role"] == "tool" || item["type"] == "function_call_output")
            .unwrap();
        let first_media = items
            .iter()
            .position(|item| {
                let mut media = Vec::new();
                collect_media(item, &mut media);
                !media.is_empty()
            })
            .unwrap();
        assert!(last_result < first_media);
    }
}

fn collect_media(value: &Value, found: &mut Vec<Vec<u8>>) {
    match value {
        Value::String(text) if text.starts_with("data:") => {
            found.push(STANDARD.decode(text.split_once(',').unwrap().1).unwrap())
        }
        Value::Object(fields) => {
            for (key, value) in fields {
                if key == "data" {
                    found.push(STANDARD.decode(value.as_str().unwrap()).unwrap());
                } else {
                    collect_media(value, found);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_media(item, found);
            }
        }
        _ => {}
    }
}
