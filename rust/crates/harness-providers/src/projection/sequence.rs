//! Portable role boundaries and stateless reasoning-item adjacency.
use super::{
    merge_chat_text,
    wire::{Item, Message},
};
use crate::{Adapter, invalid};
use acyclic_harness::Result;
use serde_json::{Value, json};

pub(super) fn push<'a>(
    adapter: Adapter,
    messages: &mut Vec<Message<'a>>,
    mut next: Message<'a>,
) -> Result<()> {
    if matches!(adapter, Adapter::Anthropic | Adapter::Gemini)
        && messages.is_empty()
        && role(&next) != "user"
    {
        messages.push(continuation(adapter));
    }
    if adapter == Adapter::Responses
        && matches!(messages.last(), Some(Message::Item(Item::Native(_))))
        && !produced(&next)
    {
        messages.push(reasoning_output());
    }
    match (messages.last_mut(), &mut next) {
        (
            Some(Message::Anthropic { role: old, content }),
            Message::Anthropic {
                role: new,
                content: parts,
            },
        ) if old == new => content.append(parts),
        (
            Some(Message::Gemini { role: old, parts }),
            Message::Gemini {
                role: new,
                parts: more,
            },
        ) if old == new => parts.append(more),
        (Some(Message::Item(Item::Json(old))), Message::Item(Item::Json(new)))
            if adapter == Adapter::Chat
                && old["role"] == "assistant"
                && new["role"] == "assistant" =>
        {
            if let Some(Value::Array(more)) = new.get_mut("tool_calls") {
                let calls = old
                    .as_object_mut()
                    .ok_or_else(|| invalid("invalid Chat turn"))?
                    .entry("tool_calls")
                    .or_insert_with(|| json!([]));
                calls
                    .as_array_mut()
                    .ok_or_else(|| invalid("invalid Chat calls"))?
                    .append(more);
            }
            if let Some(reasoning) = new["reasoning_content"].as_str() {
                let previous = old
                    .as_object_mut()
                    .ok_or_else(|| invalid("invalid Chat turn"))?
                    .entry("reasoning_content")
                    .or_insert_with(|| json!(""));
                let mut text = previous.as_str().unwrap_or_default().to_owned();
                text.push_str(reasoning);
                *previous = json!(text);
            }
            merge_chat_text(&mut old["content"], new["content"].take())?;
        }
        _ => messages.push(next),
    }
    Ok(())
}
pub(super) fn finish(adapter: Adapter, messages: &mut Vec<Message<'_>>) {
    match adapter {
        Adapter::Anthropic | Adapter::Gemini => {
            // N-01: role merging establishes the whole text run. Native parts
            // remain separate items and are never trimmed or re-encoded.
            messages.retain_mut(|message| match message {
                Message::Anthropic { content, .. } | Message::Gemini { parts: content, .. } => {
                    super::text::normalize(adapter, content);
                    !content.is_empty()
                }
                _ => true,
            });
            messages.dedup_by(|next, previous| match (previous, next) {
                (
                    Message::Anthropic { role: old, content },
                    Message::Anthropic {
                        role: new,
                        content: more,
                    },
                )
                | (
                    Message::Gemini {
                        role: old,
                        parts: content,
                    },
                    Message::Gemini {
                        role: new,
                        parts: more,
                    },
                ) if old == new => {
                    content.append(more);
                    true
                }
                _ => false,
            });
            if messages.first().is_some_and(|first| role(first) != "user") {
                messages.insert(0, continuation(adapter));
            }
            if adapter == Adapter::Gemini {
                super::gemini_results::order(messages);
            }
            if messages.is_empty() || messages.last().is_some_and(|last| role(last) != "user") {
                messages.push(continuation(adapter));
            }
        }
        Adapter::Responses if matches!(messages.last(), Some(Message::Item(Item::Native(_)))) => {
            messages.push(reasoning_output())
        }
        _ => {}
    }
}
fn role(message: &Message<'_>) -> &'static str {
    match message {
        Message::Anthropic { role, .. } | Message::Gemini { role, .. } => role,
        _ => "",
    }
}
fn produced(message: &Message<'_>) -> bool {
    matches!(message,Message::Item(Item::Json(item)) if item["type"]=="function_call" || (item["type"]=="message" && item["role"]=="assistant"))
}
fn continuation<'a>(adapter: Adapter) -> Message<'a> {
    match adapter {
        Adapter::Anthropic => Message::Anthropic {
            role: "user",
            content: vec![Item::Json(json!({"type":"text","text":"(continue)"}))],
        },
        _ => Message::Gemini {
            role: "user",
            parts: vec![Item::Json(json!({"text":"(continue)"}))],
        },
    }
}
fn reasoning_output<'a>() -> Message<'a> {
    Message::Item(Item::Json(
        json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"(no visible response)","annotations":[]}]}),
    ))
}
