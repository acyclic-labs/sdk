//! Portable role boundaries and stateless reasoning-item adjacency.
use super::wire::{Item, Message};
use crate::{
    Adapter,
    protocol::sequence::{self, ResponseItem},
};
use acyclic_harness_core::Result;
use serde_json::json;

pub(super) fn push<'a>(
    adapter: Adapter,
    messages: &mut Vec<Message<'a>>,
    next: Message<'a>,
) -> Result<()> {
    push_with_successor(adapter, messages, next, ResponseItem::allows)
}

/// `allows` is always `ResponseItem::allows` outside negative controls.
pub(super) fn push_with_successor<'a>(
    adapter: Adapter,
    messages: &mut Vec<Message<'a>>,
    mut next: Message<'a>,
    allows: impl Fn(ResponseItem, ResponseItem) -> bool,
) -> Result<()> {
    if adapter == Adapter::Responses
        && messages
            .last()
            .is_some_and(|last| !allows(response_item(last), response_item(&next)))
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
/// The shared protocol rules `finish` applies; negative controls substitute one.
#[derive(Clone, Copy)]
pub(super) struct Rules {
    pub boundary: fn(Option<&str>) -> bool,
    pub order: fn(&mut [Message<'_>]),
    pub normalize: fn(Adapter, &mut Vec<Item<'_>>),
}
pub(super) const RULES: Rules = Rules {
    boundary: sequence::user_boundary,
    order,
    normalize: super::text::normalize,
};

pub(super) fn finish(adapter: Adapter, messages: &mut Vec<Message<'_>>) {
    finish_with(adapter, messages, RULES)
}

pub(super) fn finish_with(adapter: Adapter, messages: &mut Vec<Message<'_>>, rules: Rules) {
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
            if !(rules.boundary)(messages.first().map(role)) {
                messages.insert(0, continuation(adapter));
            }
            (rules.order)(messages);
            // Joining turns around a removed empty one, or moving results
            // ahead of text, can leave a blank fragment beside text: normalize
            // the final runs once so a single pass is the fixed point. Both
            // only join or permute non-empty contents, so none empties here.
            for message in messages.iter_mut() {
                if let Message::Anthropic { content, .. } | Message::Gemini { parts: content, .. } =
                    message
                {
                    (rules.normalize)(adapter, content);
                }
            }
            if !(rules.boundary)(messages.last().map(role)) {
                messages.push(continuation(adapter));
            }
        }
        Adapter::Responses
            if messages
                .last()
                .is_some_and(|last| !response_item(last).allows(ResponseItem::End)) =>
        {
            messages.push(reasoning_output())
        }
        _ => {}
    }
}
/// ANT-TOOL-05 and GEM-TOOL-05/06: results answer their calls ahead of any
/// other content in the turn. Anthropic results move to the front, keeping
/// the relative order of both groups; Gemini results also follow call order.
fn order(messages: &mut [Message<'_>]) {
    for message in messages.iter_mut() {
        if let Message::Anthropic { content, .. } = message
            && !sequence::results_lead(content.iter().map(tool_result))
        {
            content.sort_by_key(|item| !tool_result(item));
        }
    }
    super::gemini_results::order(messages);
}
fn tool_result(item: &Item<'_>) -> bool {
    matches!(item, Item::Json(value) if sequence::tool_result(value))
}
fn role(message: &Message<'_>) -> &'static str {
    match message {
        Message::Anthropic { role, .. } | Message::Gemini { role, .. } => role,
        _ => "",
    }
}
fn response_item(message: &Message<'_>) -> ResponseItem {
    match message {
        // Native replay admits only the Responses reasoning shape.
        Message::Item(Item::Native(_)) => ResponseItem::Reasoning,
        Message::Item(Item::Call(_)) => ResponseItem::Output,
        Message::Item(Item::Json(item)) => ResponseItem::of(item),
        _ => ResponseItem::Other,
    }
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
