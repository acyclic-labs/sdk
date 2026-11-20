//! Shared shrinking/fuzz probe through verified resolution and request encoding.
use crate::{Adapter, Credentials, Registry, fixtures::ScriptedHttp};
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
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

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
    ) -> Result<BoundedContentFuture<'a, acyclic_harness_core::conversation::Custody>> {
        if ceiling.is_some() {
            return Err(acyclic_harness_core::Error::Unsupported(
                "fixture image reader cannot enforce a caller ceiling".into(),
            ));
        }
        BoundedContentFuture::new(io, || async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            // A resident fixture copy is outside the preparation ledger.
            Ok(acyclic_harness_core::conversation::Custody::Unmetered(
                self.bytes
                    .iter()
                    .find(|bytes| bytes.len() as u64 == file.descriptor().byte_length())
                    .ok_or_else(|| crate::invalid("image length is absent from the fixture"))?
                    .clone()
                    .into(),
            ))
        })
    }
}

pub(super) fn assert_roundtrip(input: &[u8]) {
    let result = futures::executor::block_on(check_roundtrip(input));
    assert!(result.is_ok(), "native media round trip failed: {result:?}");
}

async fn check_roundtrip(input: &[u8]) -> crate::fuzzing::CheckResult {
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
        .ok_or("missing image fixtures")? as u64;
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
        let attempted = Arc::new(AtomicBool::new(false));
        let send = attempted.clone();
        let delay = attempted.clone();
        let transport = Arc::new(
            ScriptedHttp::respond_owned(move |_| {
                send.store(true, Ordering::Relaxed);
                Box::pin(std::future::ready(Err(crate::invalid(
                    "encoding must not send HTTP",
                ))))
            })
            .with_delay(move |_| {
                delay.store(true, Ordering::Relaxed);
                Box::pin(std::future::ready(Err(crate::invalid(
                    "encoding must not wait",
                ))))
            }),
        );
        let mut registry =
            Registry::with_transport(Credentials::explicit(Default::default()), transport.clone())
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
        assert!(
            !attempted.load(Ordering::Relaxed),
            "encoding performed HTTP or waited"
        );
    }
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
) -> crate::fuzzing::CheckResult {
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
            return Err("projection accepted work beyond its limit".into());
        };
        assert!(error.to_string().contains("projection work limit"));
        assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
        return Ok(());
    }
    let body: Value = serde_json::from_slice(&encoded?.bytes)?;
    assert!(crate::validate_input(adapter, &body).is_empty(), "{body}");
    let mut parts = Vec::new();
    match adapter {
        Adapter::Responses => {
            for item in body
                .get("input")
                .and_then(Value::as_array)
                .ok_or("missing Responses input")?
            {
                parts.extend(
                    item.get("content")
                        .and_then(Value::as_array)
                        .ok_or("missing Responses content")?,
                );
            }
        }
        Adapter::Chat | Adapter::Anthropic => {
            parts.extend(
                body.pointer("/messages/0/content")
                    .and_then(Value::as_array)
                    .ok_or("missing message content")?,
            );
        }
        Adapter::Gemini => {
            parts.extend(
                body.pointer("/contents/0/parts")
                    .and_then(Value::as_array)
                    .ok_or("missing Gemini parts")?,
            );
        }
    };
    let &[before, media, after] = parts.as_slice() else {
        return Err("expected text, media and trailing text in projection".into());
    };
    assert_eq!(before.get("text").and_then(Value::as_str), Some("before"));
    assert_eq!(after.get("text").and_then(Value::as_str), Some("after"));
    let pointer = match adapter {
        Adapter::Responses if file.descriptor().media_type() == "application/pdf" => "/file_data",
        Adapter::Chat if file.descriptor().media_type() == "application/pdf" => "/file/file_data",
        Adapter::Responses => "/image_url",
        Adapter::Chat => "/image_url/url",
        Adapter::Anthropic => "/source/data",
        Adapter::Gemini => "/inlineData/data",
    };
    let media = media.pointer(pointer).ok_or("missing projected media")?;
    let data = match adapter {
        Adapter::Responses | Adapter::Chat => data_uri(media)?,
        Adapter::Anthropic | Adapter::Gemini => media.as_str().ok_or("invalid projected media")?,
    };
    assert_eq!(STANDARD.decode(data)?.as_slice(), bytes);
    assert_eq!(reader.reads.load(Ordering::SeqCst), 1);
    check_result_media(provider, reader, request.request(), file, bytes, adapter).await
}

fn data_uri(value: &Value) -> std::result::Result<&str, &'static str> {
    value
        .as_str()
        .and_then(|uri| uri.split_once(','))
        .map(|(_, data)| data)
        .ok_or("missing data URI payload")
}

async fn check_result_media(
    provider: &crate::ResolvedModel,
    reader: &Reader,
    request: &ModelRequest,
    file: &FileRef,
    bytes: &[u8],
    adapter: Adapter,
) -> crate::fuzzing::CheckResult {
    let Some(ModelContentPart::File { policy, .. }) = request
        .messages
        .first()
        .and_then(|message| message.content.parts().get(1))
    else {
        return Err("fixture request lacks its native file input".into());
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
    let prepared = PreparedModelRequest::prepare(request, Limits::default(), None)?;
    reader.reads.store(0, Ordering::SeqCst);
    let encoded = crate::dispatch::encode(
        provider,
        &prepared,
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
    let marker =
        |file| crate::files::FileText::default().project(file, &FileProjectionPolicy::Reference);
    assert!(contains_text(&body, &marker(file)?));
    let mut relocated = serde_json::to_value(file)?;
    relocated["path"] = json!(format!("moved/{}", file.path()));
    let relocated: FileRef = serde_json::from_value(relocated)?;
    let mut changed = prepared.request().clone();
    let Some(ModelContent::Part(ModelContentPart::ToolResult {
        content: ToolResultContent::Parts { parts },
        ..
    })) = changed
        .messages
        .get_mut(2)
        .map(|message| &mut message.content)
    else {
        return Err("fixture request lacks its native tool result".into());
    };
    let Some(ModelDataPart::File { file, .. }) = parts.get_mut(1) else {
        return Err("fixture tool result lacks its native file".into());
    };
    *file = relocated.clone();
    let changed = PreparedModelRequest::prepare(changed, Limits::default(), None)?;
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
    if matches!(adapter, Adapter::Chat | Adapter::Responses) {
        let items = body[if adapter == Adapter::Chat {
            "messages"
        } else {
            "input"
        }]
        .as_array()
        .ok_or("missing replayed messages")?;
        let last_result = items
            .iter()
            .rposition(|item| item["role"] == "tool" || item["type"] == "function_call_output")
            .ok_or("missing tool result")?;
        let mut first_media = None;
        for (index, item) in items.iter().enumerate() {
            let mut media = Vec::new();
            collect_media(item, &mut media)?;
            if !media.is_empty() {
                first_media = Some(index);
                break;
            }
        }
        assert!(last_result < first_media.ok_or("missing tool result media")?);
    }
    Ok(())
}

fn contains_text(value: &Value, needle: &str) -> bool {
    match value {
        Value::String(text) => text.contains(needle),
        Value::Array(items) => items.iter().any(|item| contains_text(item, needle)),
        Value::Object(fields) => fields.values().any(|value| contains_text(value, needle)),
        _ => false,
    }
}

fn collect_media(value: &Value, found: &mut Vec<Vec<u8>>) -> crate::fuzzing::CheckResult {
    match value {
        Value::String(text) if text.starts_with("data:") => {
            found.push(STANDARD.decode(data_uri(value)?)?);
        }
        Value::Object(fields) => {
            for (key, value) in fields {
                if key == "data" {
                    found.push(STANDARD.decode(value.as_str().ok_or("missing base64 media")?)?);
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
