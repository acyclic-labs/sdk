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

type ProbeResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Default)]
struct NoHttp {
    attempts: AtomicUsize,
}
impl NoHttp {
    fn rejected(&self) -> acyclic_harness_core::Error {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        crate::invalid("image encoding attempted transport I/O")
    }
}
impl HttpTransport for NoHttp {
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(std::future::ready(Err(self.rejected())))
    }
    fn delay(&self, _: Duration) -> BoxProviderFuture<'static, Result<()>> {
        Box::pin(std::future::ready(Err(self.rejected())))
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
                .ok_or_else(|| crate::invalid("generated image length has no verified bytes"))?
                .clone()
                .into())
        })
    }
}

pub(super) fn assert_roundtrip(input: &[u8]) -> ProbeResult {
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
        let maximum_bytes = images
            .iter()
            .map(Vec::len)
            .max()
            .ok_or_else(|| crate::invalid("image probe has no formats"))?
            as u64;
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
            let transport = Arc::new(NoHttp::default());
            let mut registry = Registry::with_transport(
                Credentials::explicit(Default::default()),
                transport.clone(),
            )?
            .with_content(reader.clone(), Some(maximum_bytes * 2));
            registry.register("image-test".into(), serde_json::from_value(json!({
                "adapter":adapter,"authentication":"optional","base_url":"http://127.0.0.1:1/v1",
                "models":{"tiny":{"capacity":{"context_tokens":32768,"output_tokens":1024},"features":{"images":true,"tools":true}}},
                "max_event_bytes":4096,
                "token_counter":{"path":"count_tokens","adapter":adapter,"response_field":"input_tokens","framing_tokens":0}
            }))?)?;
            let provider = registry.resolve("image-test/tiny")?;
            for bytes in &images {
                reader.reads.store(0, Ordering::SeqCst);
                let media = image_media_type(bytes).unwrap_or("application/pdf");
                let encoded_work = bytes.len() as u64;
                let file = FileRef::new(
                    serde_json::from_value(json!({
                        "provider":{"namespace":"test","family":"filesystem","version":"2"},
                        "id":"project","class":"project","owner":{"kind":"project","id":"project"}
                    }))?,
                    format!("image-{}", bytes.len()),
                    "1".into(),
                    FileDescriptor::from_bytes(bytes, media, &mut None)?,
                    "image".into(),
                    &mut None,
                )?;
                for work in [1, encoded_work - 1, encoded_work] {
                    check_projection(&provider, &reader, &file, bytes, adapter, work).await?;
                }
            }
            assert_eq!(transport.attempts.load(Ordering::Relaxed), 0);
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;
    let _ = image_media_type(input);
    Ok(())
}

async fn check_projection(
    provider: &crate::ResolvedModel,
    reader: &Reader,
    file: &FileRef,
    bytes: &[u8],
    adapter: Adapter,
    work: u64,
) -> ProbeResult {
    let encoded_work = bytes.len() as u64;
    let request = PreparedModelRequest::prepare(
        ModelRequest {
            summary: false,
            compose_tools: false,
            model: provider.model().clone(),
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Parts(vec![
                    ModelContentPart::text("before".into()),
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
                    ModelContentPart::text("after".into()),
                ]),
            }],
            tools: vec![],
            sampling: Default::default(),
        },
        Limits::default(),
        None,
    )?;
    let encoded = crate::dispatch::encode(
        provider,
        &request,
        &crate::tool_names::Names::new(request.request()),
        None,
        &provider.reasoning_origin(None, None),
    )
    .await;
    if work < encoded_work {
        let Err(error) = encoded else {
            return Err(crate::invalid("image projection exceeded its admitted work").into());
        };
        assert!(error.to_string().contains("projection work limit"));
        assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
        return Ok(());
    }
    let body: Value = serde_json::from_slice(&encoded?.bytes)?;
    assert!(crate::validate_input(adapter, &body).is_empty(), "{body}");
    let parts = projection_parts(&body, adapter)?;
    let [before, media, after] = parts.as_slice() else {
        return Err(
            crate::invalid("projected image must retain exactly three ordered parts").into(),
        );
    };
    assert_eq!(before["text"], "before");
    assert_eq!(after["text"], "after");
    let data = projection_data(
        media,
        adapter,
        file.descriptor().media_type() == "application/pdf",
    )?;
    assert_eq!(STANDARD.decode(data)?.as_slice(), bytes);
    assert_eq!(reader.reads.load(Ordering::SeqCst), 1);
    check_result_media(provider, reader, request.request(), file, bytes, adapter).await?;
    Ok(())
}

async fn check_result_media(
    provider: &crate::ResolvedModel,
    reader: &Reader,
    request: &ModelRequest,
    file: &FileRef,
    bytes: &[u8],
    adapter: Adapter,
) -> ProbeResult {
    let request = result_request(request, file)?;
    let prepared = PreparedModelRequest::prepare(request, Limits::default(), None)?;
    check_prepared_media(provider, reader, &prepared, file, bytes, adapter).await
}

fn result_request(request: &ModelRequest, file: &FileRef) -> ProbeResult<ModelRequest> {
    let policy = request
        .messages
        .first()
        .and_then(|message| message.content.parts().get(1))
        .and_then(|part| match part {
            ModelContentPart::File { policy, .. } => Some(policy.clone()),
            _ => None,
        })
        .ok_or_else(|| crate::invalid("generated native input is missing"))?;
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
    Ok(request)
}

async fn check_prepared_media(
    provider: &crate::ResolvedModel,
    reader: &Reader,
    prepared: &PreparedModelRequest,
    file: &FileRef,
    bytes: &[u8],
    adapter: Adapter,
) -> ProbeResult {
    reader.reads.store(0, Ordering::SeqCst);
    let encoded = crate::dispatch::encode(
        provider,
        prepared,
        &crate::tool_names::Names::new(prepared.request()),
        None,
        &provider.reasoning_origin(None, None),
    )
    .await?;
    let body: Value = serde_json::from_slice(&encoded.bytes)?;
    assert!(
        crate::validate_input(adapter, &body).is_empty(),
        "{adapter:?}: {body}"
    );
    let mut found = Vec::new();
    collect_media(&body, &mut found)?;
    assert_eq!(found, vec![bytes.to_vec(), bytes.to_vec()]);
    assert_eq!(reader.reads.load(Ordering::SeqCst), 2);
    let marker = |file: &FileRef| {
        crate::files::FileText::default().project(file, &FileProjectionPolicy::Reference)
    };
    assert!(contains_text(&body, &marker(file)?));
    let mut relocated = serde_json::to_value(file)?;
    relocated["path"] = json!(format!("moved/{}", file.path()));
    let relocated: FileRef = serde_json::from_value(relocated)?;
    let changed = relocate_result(prepared, &relocated)?;
    assert_ne!(
        prepared.replay_key()?,
        changed.replay_key()?,
        "a changed visible tool reference must invalidate replay"
    );
    let changed_encoded = crate::dispatch::encode(
        provider,
        &changed,
        &crate::tool_names::Names::new(changed.request()),
        None,
        &provider.reasoning_origin(None, None),
    )
    .await?;
    let changed_body: Value = serde_json::from_slice(&changed_encoded.bytes)?;
    assert!(crate::validate_input(adapter, &changed_body).is_empty());
    assert_eq!(reader.reads.load(Ordering::SeqCst), 4);
    assert!(contains_text(&changed_body, &marker(&relocated)?));
    let mut changed_media = Vec::new();
    collect_media(&changed_body, &mut changed_media)?;
    assert_eq!(changed_media, found, "only the visible reference changed");
    check_result_order(&body, adapter)?;
    Ok(())
}

fn relocate_result(
    prepared: &PreparedModelRequest,
    relocated: &FileRef,
) -> ProbeResult<PreparedModelRequest> {
    let mut changed = prepared.request().clone();
    let message = changed
        .messages
        .get_mut(2)
        .ok_or_else(|| crate::invalid("generated tool result is missing"))?;
    let ModelContent::Part(ModelContentPart::ToolResult {
        content: ToolResultContent::Parts { parts },
        ..
    }) = &mut message.content
    else {
        return Err(crate::invalid("generated native tool result is missing").into());
    };
    let Some(ModelDataPart::File { file, .. }) = parts.get_mut(1) else {
        return Err(crate::invalid("generated native file is missing").into());
    };
    *file = relocated.clone();
    Ok(PreparedModelRequest::prepare(
        changed,
        Limits::default(),
        None,
    )?)
}

fn check_result_order(body: &Value, adapter: Adapter) -> ProbeResult {
    if matches!(adapter, Adapter::Chat | Adapter::Responses) {
        let items = json_array(
            &body[if adapter == Adapter::Chat {
                "messages"
            } else {
                "input"
            }],
        )?;
        let last_result = items
            .iter()
            .rposition(|item| item["role"] == "tool" || item["type"] == "function_call_output")
            .ok_or_else(|| crate::invalid("encoded tool result is missing"))?;
        let mut first_media = None;
        for (index, item) in items.iter().enumerate() {
            let mut media = Vec::new();
            collect_media(item, &mut media)?;
            if !media.is_empty() {
                first_media = Some(index);
                break;
            }
        }
        let first_media = first_media.ok_or_else(|| crate::invalid("encoded media is missing"))?;
        assert!(last_result < first_media);
    }
    Ok(())
}

fn json_array(value: &Value) -> ProbeResult<&Vec<Value>> {
    value
        .as_array()
        .ok_or_else(|| crate::invalid("encoded projection is not an array").into())
}

fn projection_parts(body: &Value, adapter: Adapter) -> ProbeResult<Vec<&Value>> {
    match adapter {
        Adapter::Responses => {
            let mut parts = Vec::new();
            for item in json_array(&body["input"])? {
                parts.extend(json_array(&item["content"])?);
            }
            Ok(parts)
        }
        Adapter::Chat | Adapter::Anthropic => Ok(json_array(&body["messages"][0]["content"])?
            .iter()
            .collect()),
        Adapter::Gemini => Ok(json_array(&body["contents"][0]["parts"])?.iter().collect()),
    }
}

fn data_uri(text: &str) -> ProbeResult<&str> {
    text.split_once(',')
        .map(|(_, data)| data)
        .ok_or_else(|| crate::invalid("encoded data URI has no payload").into())
}

fn projection_data(media: &Value, adapter: Adapter, pdf: bool) -> ProbeResult<&str> {
    let value = match adapter {
        Adapter::Responses if pdf => &media["file_data"],
        Adapter::Chat if pdf => &media["file"]["file_data"],
        Adapter::Responses => &media["image_url"],
        Adapter::Chat => &media["image_url"]["url"],
        Adapter::Anthropic => &media["source"]["data"],
        Adapter::Gemini => &media["inlineData"]["data"],
    };
    let text = value
        .as_str()
        .ok_or_else(|| crate::invalid("encoded media payload is not text"))?;
    if matches!(adapter, Adapter::Responses | Adapter::Chat) {
        data_uri(text)
    } else {
        Ok(text)
    }
}
fn contains_text(value: &Value, needle: &str) -> bool {
    match value {
        Value::String(text) => text.contains(needle),
        Value::Array(items) => items.iter().any(|item| contains_text(item, needle)),
        Value::Object(fields) => fields.values().any(|value| contains_text(value, needle)),
        _ => false,
    }
}

fn collect_media(value: &Value, found: &mut Vec<Vec<u8>>) -> ProbeResult {
    match value {
        Value::String(text) if text.starts_with("data:") => {
            found.push(STANDARD.decode(data_uri(text)?)?)
        }
        Value::Object(fields) => {
            for (key, value) in fields {
                if key == "data" {
                    found.push(
                        STANDARD.decode(
                            value
                                .as_str()
                                .ok_or_else(|| crate::invalid("encoded media data is not text"))?,
                        )?,
                    );
                } else {
                    collect_media(value, found)?;
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_media(item, found)?;
            }
        }
        _ => {}
    }
    Ok(())
}
