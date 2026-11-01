pub(crate) mod arguments;
mod call;
pub(crate) mod gemini;
mod gemini_results;
pub(crate) mod image;
mod json_string;
pub(crate) mod native_reasoning;
mod profile;
pub(crate) mod result;
mod sequence;
pub(crate) mod text;
pub(crate) mod wire;
use crate::{Adapter, ResolvedModel, files::FileText, invalid, registry::validate_url};
use acyclic_harness_core::{
    Result,
    context::ModelTokenCount,
    model::{
        ModelContent, ModelContentPart, ModelDataPart, ModelMessage, ModelRequest, ModelRole,
        PreparedModelRequest, ToolResultContent,
    },
};
pub(crate) use profile::validate as validate_profile;
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
            #[cfg(all(test, not(target_arch = "wasm32")))]
            let name = if crate::tests::catalog_identity_tests::SUBSTITUTE_WIRE_MODEL
                .with(std::cell::Cell::get)
            {
                "unselected-model"
            } else {
                name
            };
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
    usize::try_from(acyclic_harness_core::executor::json::json_byte_length(
        value,
        u64::MAX,
        &mut None,
    )?)
    .map_err(|_| invalid("token size overflow"))
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn body(provider: &ResolvedModel, request: &ModelRequest) -> Result<Value> {
    body_with_files(provider, request, &mut FileText::default())
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
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
    target: &acyclic_harness_core::model::ReasoningOrigin,
    files: &mut FileText,
) -> Result<Body<'a>> {
    let adapter = provider.config.adapter;
    if provider
        .profile
        .as_ref()
        .is_some_and(|profile| profile.max_output_tokens() != request.max_output_tokens)
    {
        return Err(invalid(
            "request output reservation differs from its admitted profile",
        ));
    }
    let call_ids = crate::call_ids::CallIds::new(request, adapter)?;
    let mut messages: Vec<Message<'a>> = Vec::new();
    let mut deferred_media = Vec::new();
    let mut system = Vec::new();
    let mut gemini = gemini::Replay::default();
    let mut effective_effort = provider
        .profile
        .as_ref()
        .and_then(|profile| profile.baseline_effort());
    for message in &request.messages {
        // Chat and Responses put native result media in user input items.
        // Keep every result of a parallel call batch ahead of those items.
        if message.role != ModelRole::Tool {
            for media in deferred_media.drain(..) {
                sequence::push(adapter, &mut messages, media)?;
            }
        }
        let event = message.content.notice_parts();
        if let Some((notices, attachments)) = event {
            // A notice is its own turn; retained Gemini parts cannot span it.
            gemini.begin(message.role)?;
            let notice = acyclic_model_render::notice_block(
                acyclic_model_render::Provenance {
                    family: adapter,
                    provider: target.provider.clone(),
                    model: target.model.clone(),
                    endpoint: endpoint(provider)?,
                    credential_scope: target.credential_scope,
                },
                notices,
            )
            .map_err(|error| invalid(error.to_string()))?;
            sequence::push(adapter, &mut messages, Message::Notice(notice))?;
            if attachments.is_empty() {
                continue;
            }
        }
        if let ModelContent::Part(ModelContentPart::Configuration { prefix, effort }) =
            &message.content
        {
            let profile = provider.profile.as_ref().ok_or_else(|| {
                invalid("configuration update requires an admitted model profile")
            })?;
            if prefix != &profile.prefix_digest() {
                continue;
            }
            if adapter != Adapter::Responses
                || profile.reasoning() == acyclic_model_render::Reasoning::Disabled
                || *effort == acyclic_model_render::Effort::Adaptive
                || message.role != ModelRole::User
            {
                return Err(invalid(
                    "configuration update is not admitted by this protocol and profile",
                ));
            }
            sequence::push(
                adapter,
                &mut messages,
                Message::Item(Item::Json(json!({
                    "type":"configuration_update", "reasoning":{"effort":effort},
                }))),
            )?;
            effective_effort = Some(*effort);
            continue;
        }
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
                if message.role == ModelRole::Tool
                    && matches!(adapter, Adapter::Chat | Adapter::Responses)
                    && matches!(&projected, Message::Item(Item::Json(_)))
                {
                    deferred_media.push(projected);
                } else {
                    sequence::push(adapter, &mut messages, projected)?;
                }
            }
        }
    }
    for media in deferred_media {
        sequence::push(adapter, &mut messages, media)?;
    }
    if let Some(profile) = &provider.profile {
        let desired = profile.fixed_effort().or(profile.baseline_effort());
        if effective_effort != desired {
            return Err(invalid(
                "request is missing its original-position role effort update",
            ));
        }
    }
    gemini.finish()?;
    sequence::finish(adapter, &mut messages)?;
    let tools = request
        .model_tools()
        .map(|tool| Ok(tool.model_declaration(names.wire(&tool.name)?)))
        .collect::<Result<Vec<_>>>()?;
    let tools = acyclic_model_render::ToolDeclarations::new(adapter, tools)
        .map_err(|error| invalid(error.to_string()))?;
    let tools = (!tools.is_empty() || adapter == Adapter::Responses).then_some(tools);
    system.retain(|text| !text.trim().is_empty());
    let max = provider
        .admitted_capacity()
        .output_reservation(request.max_output_tokens)?;
    let mut fields = serde_json::Map::new();
    let map = &mut fields;
    let model = request.model.name.as_str();
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let model =
        if crate::tests::catalog_identity_tests::SUBSTITUTE_WIRE_MODEL.with(std::cell::Cell::get) {
            "unselected-model"
        } else {
            model
        };
    map.insert("model".into(), json!(model));
    match adapter {
        Adapter::Chat => {
            map.insert("stream".into(), json!(true));
            map.insert("stream_options".into(), json!({"include_usage":true}));
            map.insert("max_completion_tokens".into(), json!(max));
        }
        Adapter::Responses => {
            map.insert("include".into(), json!(["reasoning.encrypted_content"]));
            map.insert("stream".into(), json!(true));
            map.insert("store".into(), json!(false));
            map.insert("max_output_tokens".into(), json!(max));
            #[cfg(not(target_arch = "wasm32"))]
            if provider.codex_login.is_some() {
                map.remove("max_output_tokens");
                map.insert(
                    "instructions".into(),
                    serde_json::Value::String(text::join(system, "\n\n")),
                );
                map.insert("tool_choice".into(), json!("auto"));
                map.insert("parallel_tool_calls".into(), json!(true));
            }
        }
        Adapter::Anthropic => {
            map.insert("stream".into(), json!(true));
            map.insert("max_tokens".into(), json!(max));
            if !system.is_empty() {
                map.insert(
                    "system".into(),
                    serde_json::Value::String(text::join(system, "\n\n")),
                );
            }
        }
        Adapter::Gemini => {
            map.remove("model");

            map.insert("generationConfig".into(), json!({"maxOutputTokens":max}));
            if !system.is_empty() {
                let mut part = serde_json::Map::new();
                part.insert(
                    "text".into(),
                    serde_json::Value::String(text::join(system, "\n\n")),
                );
                let mut instruction = serde_json::Map::new();
                instruction.insert(
                    "parts".into(),
                    serde_json::Value::Array(vec![serde_json::Value::Object(part)]),
                );
                map.insert(
                    "systemInstruction".into(),
                    serde_json::Value::Object(instruction),
                );
            }
        }
    }
    if let Some(profile) = &provider.profile {
        profile::project(adapter, profile, &mut fields);
    }
    let messages_field = match adapter {
        Adapter::Responses => "input",
        Adapter::Gemini => "contents",
        _ => "messages",
    };
    Ok(Body {
        fields,
        tools,
        messages_field,
        messages,
    })
}

fn project_message<'a>(
    adapter: Adapter,
    message: &'a ModelMessage,
    names: &'a crate::tool_names::Names<'_>,
    call_ids: &crate::call_ids::CallIds,
    target: &acyclic_harness_core::model::ReasoningOrigin,
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
    let mut tool_media = Vec::new();
    gemini.begin(message.role)?;
    let mut chat_reasoning = None;
    if let ModelContent::Text(text) = &message.content {
        gemini.finish()?;
        project_visible_text(adapter, role, text, &mut texts, &mut ordered, gemini);
    }
    let parts = message
        .content
        .notice_parts()
        .map_or(message.content.parts(), |(_, files)| files);
    for part in parts {
        if !matches!(
            part,
            ModelContentPart::ToolCall(_) | ModelContentPart::Reasoning { .. }
        ) {
            gemini.finish()?;
        }
        match part {
            ModelContentPart::Notices { .. } => {
                return Err(invalid("notice batch must occupy one message"));
            }
            ModelContentPart::Configuration { .. } => {
                return Err(invalid("configuration update must occupy one message"));
            }
            ModelContentPart::Text { text } => {
                project_visible_text(adapter, role, text, &mut texts, &mut ordered, gemini);
            }
            ModelContentPart::File { file, policy } => {
                if let acyclic_harness_core::model::FileProjectionPolicy::Native(policy) = policy {
                    if role != "user" {
                        return Err(invalid("native media requires a user message"));
                    }
                    let image = image::part(
                        adapter,
                        file.descriptor().media_type(),
                        policy,
                        files.native()?,
                    )?;
                    if adapter == Adapter::Chat {
                        ordered.extend(
                            texts
                                .drain(..)
                                .map(|text| Item::Json(json!({"type":"text","text":text}))),
                        );
                    }
                    ordered.push(Item::Json(image));
                    continue;
                }
                project_text(
                    adapter,
                    role,
                    files.project(file, policy)?,
                    &mut texts,
                    &mut ordered,
                );
            }
            ModelContentPart::ToolCall(acyclic_harness_core::model::ToolCall {
                call_id,
                name,
                arguments,
                metadata: _,
            }) => {
                let name = names.wire(name)?;
                let (call_id, freeform, omit_gemini_id) = call_ids.binding(call_id)?;
                let native = if adapter == Adapter::Gemini {
                    gemini.take_call(
                        (!omit_gemini_id).then_some(call_id.as_ref()),
                        &name,
                        arguments,
                    )?
                } else {
                    None
                };
                let call = call::Call {
                    adapter,
                    id: call_id.into_owned(),
                    name,
                    arguments,
                    freeform,
                    omit_gemini_id,
                    native,
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
                        let raw = native_reasoning::validated(adapter, continuity)
                            .ok_or_else(|| invalid("invalid retained native reasoning"))?;
                        ordered.push(Item::Native(raw));
                    }
                    ProjectedReasoning::Native { continuity, .. } if adapter == Adapter::Gemini => {
                        if let Some(part) = gemini.native(
                            continuity,
                            block.outcome
                                == acyclic_harness_core::model::ReasoningOutcome::SuppressedDisplay,
                        )? {
                            ordered.push(Item::Native(part));
                        }
                    }
                    ProjectedReasoning::Native { continuity, .. } => {
                        let raw = native_reasoning::chat(continuity)
                            .ok_or_else(|| invalid("invalid retained native Chat reasoning"))?;
                        if chat_reasoning.replace(raw).is_some() {
                            return Err(invalid("multiple native Chat reasoning fields"));
                        }
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
                let (call_id, freeform, omit_gemini_id) = call_ids.binding(call_id)?;
                let result = Item::Result(result::ToolResult {
                    adapter,
                    id: call_id.into_owned(),
                    name,
                    payload: result_text(adapter, content, files, &mut tool_media)?,
                    error: matches!(content, ToolResultContent::Error { .. }),
                    freeform,
                    omit_gemini_id,
                });
                if adapter == Adapter::Chat {
                    results.push(result);
                } else {
                    ordered.push(result);
                }
            }
        }
    }
    if adapter != Adapter::Chat {
        ordered.extend(tool_media.drain(..).map(Item::Json));
    }
    match adapter {
        Adapter::Chat => {
            let mut output = Vec::new();
            if !ordered.is_empty() {
                output.push(Message::Item(Item::Json(
                    json!({"role":role,"content":ordered}),
                )));
            }
            if !texts.is_empty() || !calls.is_empty() || chat_reasoning.is_some() {
                output.push(Message::Chat(wire::Chat {
                    role,
                    content: if texts.is_empty() {
                        calls.is_empty().then(String::new)
                    } else {
                        Some(texts.concat())
                    },
                    tool_calls: calls,
                    reasoning_content: chat_reasoning,
                }));
            }
            output.extend(results.into_iter().map(Message::Item));
            if !tool_media.is_empty() {
                output.push(Message::Item(Item::Json(
                    json!({"role":"user","content":tool_media}),
                )));
            }
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
        Adapter::Chat if ordered.is_empty() => texts.push(text),
        Adapter::Chat => ordered.push(Item::Json(json!({"type":"text","text":text}))),
        Adapter::Responses => ordered.push(Item::Json(json!({
            "type":"message", "role":if role == "tool" { "user" } else { role },
            "content":[{"type":if role=="assistant" {"output_text"} else {"input_text"},"text":text}],
        }))),
        Adapter::Anthropic => ordered.push(Item::Json(json!({"type":"text","text":text}))),
        Adapter::Gemini => ordered.push(Item::Json(json!({"text":text}))),
    }
}

fn result_text<'a>(
    adapter: Adapter,
    content: &'a ToolResultContent,
    files: &mut FileText,
    media: &mut Vec<Value>,
) -> Result<result::Payload<'a>> {
    match content {
        ToolResultContent::Json { value } => Ok(result::Payload::Json(value)),
        ToolResultContent::Parts { parts } => parts
            .iter()
            .map(|part| match part {
                ModelDataPart::Text { text } => Ok(text.clone()),
                ModelDataPart::File {
                    file,
                    policy: acyclic_harness_core::model::FileProjectionPolicy::Native(policy),
                } => {
                    media.push(image::part(
                        adapter,
                        file.descriptor().media_type(),
                        policy,
                        files.native()?,
                    )?);
                    files.project(
                        file,
                        &acyclic_harness_core::model::FileProjectionPolicy::Reference,
                    )
                }
                ModelDataPart::File { file, policy } => files.project(file, policy),
            })
            .collect::<Result<Vec<_>>>()
            .map(|parts| result::Payload::Text(parts.join("\n").into())),
        ToolResultContent::Error { message, .. } => {
            Ok(result::Payload::Text(message.as_str().into()))
        }
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
