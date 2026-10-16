//! Portable role boundaries and stateless reasoning-item adjacency.
use super::wire::{Item, Message};
use crate::Adapter;
use acyclic_harness::Result;
use serde_json::json;

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
        (Some(Message::Chat(old)), Message::Chat(new))
            if old.role == "assistant" && new.role == "assistant" =>
        {
            old.tool_calls.append(&mut new.tool_calls);
            if let Some(more) = new.reasoning_content.take() {
                old.reasoning_content
                    .get_or_insert_with(String::new)
                    .push_str(&more);
            }
            if let Some(more) = new.content.take() {
                match &mut old.content {
                    Some(text) => {
                        text.push('\n');
                        text.push_str(&more);
                    }
                    None => old.content = Some(more),
                }
            }
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
    matches!(message, Message::Item(Item::Call(_)))
        || matches!(message,Message::Item(Item::Json(item)) if item["type"]=="function_call" || (item["type"]=="message" && item["role"]=="assistant"))
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
