pub(crate) mod arguments;
mod call;
pub(crate) mod gemini;
mod gemini_results;
mod json_string;
pub(crate) mod native_reasoning;
mod sequence;
pub(crate) mod text;
pub(crate) mod wire;
use crate::{Adapter, ResolvedModel, files::FileText, invalid, registry::validate_url};
use acyclic_harness::{
    Result,
    context::ModelTokenCount,
    model::{
        ModelContent, ModelContentPart, ModelDataPart, ModelMessage, ModelRequest, ModelRole,
        PreparedModelRequest, ToolResultContent,
    },
};
use serde_json::{Value, json};
use wire::{Body, Item, Message};

pub(crate) fn endpoint(provider: &ResolvedModel) -> Result<String> {
    let mut base = validate_url(&provider.config.base_url)?;
    let path = format!("{}/", base.path().trim_end_matches('/'));
    base.set_path(&path);
    let suffix = match provider.config.adapter {
        Adapter::Anthropic => "messages".into(),
        Adapter::Responses => "responses".into(),
        Adapter::Chat => "chat/completions".into(),
        Adapter::Gemini => {
            let name = &provider.model.name;
            if name.contains('/') || name.contains(':') || name.contains('?') || name.contains('#')
            {
                return Err(invalid("invalid Gemini model name"));
            }
            format!("models/{name}:streamGenerateContent?alt=sse")
        }
    };
    base.join(&suffix)
        .map(String::from)
        .map_err(|_| invalid("invalid model endpoint"))
}

pub(crate) fn plan(
    provider: &ResolvedModel,
    prepared: &PreparedModelRequest,
    input_tokens: u64,
) -> Result<ModelTokenCount> {
    // These weights only choose a compaction cut. Admission always uses the
    // independently verified total, never a usage-calibrated ratio.
    let tokens = |bytes| {
        provider
            .estimator
            .tokens(bytes)
            .ok_or_else(|| invalid("token planning weight exceeds u32"))
    };
    let mut message_bytes = 0usize;
    let message_tokens = prepared
        .request()
        .messages
        .iter()
        .map(|message| {
            let bytes = encoded_len(message)?
                .checked_add(crate::files::extra_bytes(
                    &message.content,
                    provider.files.maximum_bytes,
                )?)
                .ok_or_else(|| invalid("token planning size overflow"))?;
            message_bytes = message_bytes
                .checked_add(bytes)
                .ok_or_else(|| invalid("token planning size overflow"))?;
            tokens(bytes)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ModelTokenCount {
        request_digest: prepared.request_digest(),
        input_tokens,
        fixed_tokens: tokens(prepared.bytes().len().saturating_sub(message_bytes))?,
        message_tokens,
    })
}

pub(crate) fn encoded_len<T: serde::Serialize>(value: &T) -> Result<usize> {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("token size overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Counter(0);
    serde_json::to_writer(&mut count, value).map_err(|error| invalid(error.to_string()))?;
    Ok(count.0)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) fn body(provider: &ResolvedModel, request: &ModelRequest) -> Result<Value> {
    body_with_files(provider, request, &mut FileText::default())
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) fn body_with_files(
    provider: &ResolvedModel,
    request: &ModelRequest,
    files: &mut FileText,
) -> Result<Value> {
    serde_json::to_value(body_named(
        provider,
        request,
        &crate::tool_names::Names::new(request),
        &provider.reasoning_origin(provider.key.as_deref(), None),
        files,
    )?)
    .map_err(|_| invalid("cannot encode projected request"))
}

pub(crate) fn body_named<'a>(
    provider: &ResolvedModel,
    request: &'a ModelRequest,
    names: &'a crate::tool_names::Names<'_>,
    target: &acyclic_harness::model::ReasoningOrigin,
    files: &mut FileText,
) -> Result<Body<'a>> {
    let adapter = provider.config.adapter;
    let call_ids = crate::call_ids::CallIds::new(request, adapter);
    let mut messages: Vec<Message<'a>> = Vec::new();
    let mut system = Vec::new();
    let mut gemini = gemini::Replay::default();
    for message in &request.messages {
        #[cfg(not(target_arch = "wasm32"))]
        if provider.codex_login.is_some() && message.role == ModelRole::System {
            system.push(text_content(&message.content, files)?);
            continue;
        }
        if message.role == ModelRole::System
            && matches!(adapter, Adapter::Anthropic | Adapter::Gemini)
        {
            system.push(text_content(&message.content, files)?);
        } else {
            for projected in project_message(
                adapter,
                message,
                names,
                &call_ids,
                target,
                &mut gemini,
                files,
            )? {
                sequence::push(adapter, &mut messages, projected)?;
            }
        }
    }
    sequence::finish(adapter, &mut messages);
    let tools: Vec<Value> = request.tools.iter().map(|tool| { let name = names.wire(&tool.name)?; Ok(match adapter {
        Adapter::Anthropic => json!({"name":name,"description":tool.description,"input_schema":tool.input_schema}),
        Adapter::Responses => json!({"type":"function","name":name,"description":tool.description,
            "parameters":tool.input_schema,"strict":false}),
        Adapter::Chat => json!({"type":"function","function":{"name":name,
            "description":tool.description,"parameters":tool.input_schema}}),
        Adapter::Gemini => json!({"name":name,"description":tool.description,"parametersJsonSchema":tool.input_schema}),
    }) }).collect::<Result<_>>()?;
    system.retain(|text| !text.trim().is_empty());
    let max = request
        .max_output_tokens
        .unwrap_or(provider.context_output());
    if max == 0 || max > provider.context_output() {
        return Err(invalid("requested output exceeds selected model capacity"));
    }
    let mut body = request.model.options.clone();
    names.controls(adapter, &mut body)?;
    let map = body
        .as_object_mut()
        .ok_or_else(|| invalid("model options must be an object"))?;
    map.insert("model".into(), json!(request.model.name));
    match adapter {
        Adapter::Chat => {
            map.insert("stream".into(), json!(true));
            map.insert("stream_options".into(), json!({"include_usage":true}));
            map.insert("max_completion_tokens".into(), json!(max));
            if !tools.is_empty() {
                map.insert("tools".into(), json!(tools));
            }
        }
        Adapter::Responses => {
            let include = map
                .entry("include")
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .ok_or_else(|| invalid("Responses include must be an array"))?;
            if !include
                .iter()
                .any(|item| item == "reasoning.encrypted_content")
            {
                include.push(json!("reasoning.encrypted_content"));
            }

            map.insert("stream".into(), json!(true));
            map.insert("store".into(), json!(false));
            map.insert("max_output_tokens".into(), json!(max));
            map.insert("tools".into(), json!(tools));
            #[cfg(not(target_arch = "wasm32"))]
            if provider.codex_login.is_some() {
                map.remove("max_output_tokens");
                map.insert("instructions".into(), json!(system.join("\n\n")));
                map.entry("tool_choice").or_insert_with(|| json!("auto"));
                map.entry("parallel_tool_calls")
                    .or_insert_with(|| json!(true));
                if map.get("reasoning").is_some_and(Value::is_null) {
                    map.remove("reasoning");
                }
            }
        }
        Adapter::Anthropic => {
            map.insert("stream".into(), json!(true));
            map.insert("max_tokens".into(), json!(max));
            if !system.is_empty() {
                map.insert("system".into(), json!(system.join("\n\n")));
            }
            if !tools.is_empty() {
                map.insert("tools".into(), json!(tools));
            }
        }
        Adapter::Gemini => {
            map.remove("model");

            map.entry("generationConfig")
                .or_insert_with(|| json!({}))
                .as_object_mut()
                .ok_or_else(|| invalid("Gemini generationConfig must be an object"))?
                .insert("maxOutputTokens".into(), json!(max));
            if !system.is_empty() {
                map.insert(
                    "systemInstruction".into(),
                    json!({"parts":[{"text":system.join("\n\n")}]}),
                );
            }
            if !tools.is_empty() {
                map.insert("tools".into(), json!([{"functionDeclarations":tools}]));
            }
        }
    }
    let fields = body
        .as_object_mut()
        .ok_or_else(|| invalid("invalid request body"))?;
    let messages_field = match adapter {
        Adapter::Responses => "input",
        Adapter::Gemini => "contents",
        _ => "messages",
    };
    // Options cannot override transcript projection or inject a second input path.
    for name in ["messages", "input", "contents"] {
        fields.remove(name);
    }
    Ok(Body {
        fields: std::mem::take(fields),
        messages_field,
        messages,
    })
}

fn project_message<'a>(
    adapter: Adapter,
    message: &'a ModelMessage,
    names: &'a crate::tool_names::Names<'_>,
    call_ids: &crate::call_ids::CallIds,
    target: &acyclic_harness::model::ReasoningOrigin,
    gemini: &mut gemini::Replay<'a>,
    files: &mut FileText,
) -> Result<Vec<Message<'a>>> {
    let role: &'static str = match message.role {
        ModelRole::System => "system",
        ModelRole::User => "user",
        ModelRole::Assistant => "assistant",
        ModelRole::Tool => "tool",
    };
    let mut texts = Vec::new();
    let mut calls = Vec::new();
    let mut results = Vec::new();
    let mut ordered = Vec::new();
    gemini.begin(message.role);
    let mut chat_reasoning = String::new();
    if let ModelContent::Text(text) = &message.content {
        project_visible_text(adapter, role, text, &mut texts, &mut ordered, gemini);
    }
    for part in message.content.parts() {
        match part {
            ModelContentPart::Text { text } => {
                project_visible_text(adapter, role, text, &mut texts, &mut ordered, gemini);
            }
            ModelContentPart::File { file, policy } => {
                project_text(
                    adapter,
                    role,
                    files.project(file, policy)?,
                    &mut texts,
                    &mut ordered,
                );
            }
            ModelContentPart::ToolCall(acyclic_harness::model::ToolCall {
                call_id,
                name,
                arguments,
                metadata: _,
            }) => {
                let name = names.wire(name)?;
                let call_id = call_ids.wire(call_id)?;
                let call = call::Call {
                    adapter,
                    id: call_id.into_owned(),
                    name,
                    arguments,
                    signature: if adapter == Adapter::Gemini {
                        gemini.signature.take()
                    } else {
                        None
                    },
                };
                if adapter == Adapter::Chat {
                    calls.push(call);
                } else {
                    ordered.push(Item::Call(call));
                }
            }
            ModelContentPart::Reasoning { block } => {
                use crate::ProjectedReasoning;
                match crate::project_reasoning(block, target) {
                    ProjectedReasoning::Native { continuity, .. }
                        if matches!(adapter, Adapter::Responses | Adapter::Anthropic) =>
                    {
                        // Invalid native shapes fall back to ordinary readable context.
                        if let Some(raw) = native_reasoning::validated(adapter, continuity) {
                            ordered.push(Item::Native(raw));
                        } else if let Some(text) = block.text.as_deref() {
                            project_text(
                                adapter,
                                role,
                                foreign_text(&block.origin, text),
                                &mut texts,
                                &mut ordered,
                            );
                        }
                    }
                    ProjectedReasoning::Native { continuity, .. } if adapter == Adapter::Gemini => {
                        if let Some(part) = gemini.native(
                            continuity,
                            block.outcome
                                == acyclic_harness::model::ReasoningOutcome::SuppressedDisplay,
                        ) {
                            ordered.push(part);
                        }
                    }
                    ProjectedReasoning::Native { .. } => {
                        if let Some(text) = &block.text {
                            chat_reasoning.push_str(text);
                        }
                    }
                    ProjectedReasoning::Readable { text, origin, .. } => {
                        project_text(
                            adapter,
                            role,
                            foreign_text(origin, text),
                            &mut texts,
                            &mut ordered,
                        );
                    }
                    ProjectedReasoning::Dropped { .. } => {}
                }
            }
            ModelContentPart::ToolResult {
                call_id,
                name,
                content,
            } => {
                let name = names.wire(name)?;
                let call_id = call_ids.wire(call_id)?;
                let text = result_text(content, files)?;
                let error = matches!(content, ToolResultContent::Error { .. });
                let result = match adapter {
                    Adapter::Anthropic if error => json!(
                        {"type":"tool_result","tool_use_id":call_id,"content":text,"is_error":true}
                    ),
                    Adapter::Gemini if error => json!(
                        {"functionResponse":{"id":call_id,"name":name,"response":{"error":text}}}
                    ),
                    Adapter::Anthropic => {
                        json!({"type":"tool_result","tool_use_id":call_id,"content":text})
                    }
                    Adapter::Responses => {
                        json!({"type":"function_call_output","call_id":call_id,"output":text})
                    }
                    Adapter::Chat => json!({"role":"tool","tool_call_id":call_id,"content":text}),
                    Adapter::Gemini => {
                        json!({"functionResponse":{"id":call_id,"name":name,"response":{"output":text}}})
                    }
                };
                if adapter == Adapter::Chat {
                    results.push(result);
                } else {
                    ordered.push(Item::Json(result));
                }
            }
        }
    }
    match adapter {
        Adapter::Chat => {
            let mut output = Vec::new();
            if !texts.is_empty() || !calls.is_empty() || !chat_reasoning.is_empty() {
                output.push(Message::Chat(wire::Chat {
                    role,
                    content: if texts.is_empty() {
                        calls.is_empty().then(String::new)
                    } else {
                        Some(texts.concat())
                    },
                    tool_calls: calls,
                    reasoning_content: (!chat_reasoning.is_empty()).then_some(chat_reasoning),
                }));
            }
            output.extend(
                results
                    .into_iter()
                    .map(|value| Message::Item(Item::Json(value))),
            );
            Ok(output)
        }
        Adapter::Responses => Ok(ordered.into_iter().map(Message::Item).collect()),
        Adapter::Anthropic => Ok(if ordered.is_empty() {
            Vec::new()
        } else {
            vec![Message::Anthropic {
                role: if role == "tool" { "user" } else { role },
                content: ordered,
            }]
        }),
        Adapter::Gemini => Ok(if ordered.is_empty() {
            Vec::new()
        } else {
            vec![Message::Gemini {
                role: if role == "assistant" { "model" } else { "user" },
                parts: ordered,
            }]
        }),
    }
}

fn project_visible_text(
    adapter: Adapter,
    role: &str,
    text: &str,
    texts: &mut Vec<String>,
    ordered: &mut Vec<Item<'_>>,
    gemini: &mut gemini::Replay<'_>,
) {
    let text = if adapter == Adapter::Gemini && role == "assistant" {
        gemini.remaining(text)
    } else {
        text
    };
    if !text.is_empty() || adapter != Adapter::Gemini {
        project_text(adapter, role, text.to_owned(), texts, ordered);
    }
}

fn project_text(
    adapter: Adapter,
    role: &str,
    text: String,
    texts: &mut Vec<String>,
    ordered: &mut Vec<Item<'_>>,
) {
    if text.is_empty() {
        return;
    }
    match adapter {
        Adapter::Chat => texts.push(text),
        Adapter::Responses => ordered.push(Item::Json(json!({
            "type":"message", "role":if role == "tool" { "user" } else { role },
            "content":[{"type":if role=="assistant" {"output_text"} else {"input_text"},"text":text}],
        }))),
        Adapter::Anthropic => ordered.push(Item::Json(json!({"type":"text","text":text}))),
        Adapter::Gemini => ordered.push(Item::Json(json!({"text":text}))),
    }
}

fn result_text(content: &ToolResultContent, files: &mut FileText) -> Result<String> {
    match content {
        ToolResultContent::Json { value } => {
            serde_json::to_string(value).map_err(|error| invalid(error.to_string()))
        }
        ToolResultContent::Parts { parts } => parts
            .iter()
            .map(|part| match part {
                ModelDataPart::Text { text } => Ok(text.clone()),
                ModelDataPart::File { file, policy } => files.project(file, policy),
            })
            .collect::<Result<Vec<_>>>()
            .map(|parts| parts.join("\n")),
        ToolResultContent::Error { message, .. } => Ok(message.clone()),
    }
}

fn text_content(content: &ModelContent, files: &mut FileText) -> Result<String> {
    if let ModelContent::Text(text) = content {
        return Ok(text.clone());
    }
    content
        .parts()
        .iter()
        .map(|part| match part {
            ModelContentPart::Text { text } => Ok(text.clone()),
            ModelContentPart::File { file, policy } => files.project(file, policy),
            _ => Err(invalid("system messages cannot contain tool exchanges")),
        })
        .collect::<Result<Vec<_>>>()
        .map(|parts| parts.join("\n"))
}

fn foreign_text(origin: &acyclic_harness::model::ReasoningOrigin, text: &str) -> String {
    format!(
        "<prior-reasoning provider={:?} model={:?}>\n{text}\n</prior-reasoning>",
        origin.provider, origin.model
    )
}
