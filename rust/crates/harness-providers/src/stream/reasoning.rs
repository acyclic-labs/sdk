use super::{Adapter, Result, State, Value, budget, invalid};
use crate::protocol::present;
use acyclic_harness_core::model::{
    ReasoningBlock, ReasoningKind, ReasoningOrigin, ReasoningOutcome,
};
use std::mem::size_of;

pub(super) struct Thinking {
    native: String,
    text: String,
    signature: String,
    kind: ReasoningKind,
}
impl Default for Thinking {
    fn default() -> Self {
        Self {
            native: String::new(),
            text: String::new(),
            signature: String::new(),
            kind: ReasoningKind::Readable,
        }
    }
}
impl Thinking {
    pub(super) fn bytes(&self) -> usize {
        self.native.capacity() + self.text.capacity() + self.signature.capacity()
    }
}

impl<'work> State<'work> {
    fn thinking(&mut self, index: u64) -> Result<(&mut Thinking, &mut budget::Budget<'work>)> {
        let target = match self.thinking.entry(index) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                self.budget.add(size_of::<(u64, Thinking)>())?;
                entry.insert(Thinking::default())
            }
        };
        Ok((target, &mut self.budget))
    }

    #[cfg(test)]
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn retain_thinking(&mut self, index: u64, value: &Value) -> Result<()> {
        self.raw_part(index, value, None)
    }

    pub(super) fn raw_part(&mut self, index: u64, value: &Value, raw: Option<&str>) -> Result<()> {
        let (target, budget) = self.thinking(index)?;
        match raw {
            Some(raw) => budget.string(&mut target.native, raw)?,
            None => budget.encoded(&mut target.native, value)?,
        }
        target.kind = match value["type"].as_str() {
            Some("redacted_thinking") => ReasoningKind::Redacted,
            Some("reasoning") if value["encrypted_content"].is_string() => ReasoningKind::Encrypted,
            Some("reasoning") => ReasoningKind::Summary,
            _ if present(value.get("thoughtSignature")).is_some() => ReasoningKind::Signature,
            _ => ReasoningKind::Readable,
        };
        budget.string(
            &mut target.signature,
            value["signature"].as_str().unwrap_or_default(),
        )?;
        let mut first = true;
        let summaries = value["summary"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|part| part["text"].as_str());
        let direct = present(value.get("thinking"))
            .or_else(|| present(value.get("text")))
            .and_then(Value::as_str);
        for text in direct.into_iter().chain(summaries) {
            if first {
                budget.string(&mut target.text, text)?;
                first = false;
            } else {
                budget.append_str(&mut target.text, "\n")?;
                budget.append_str(&mut target.text, text)?;
            }
        }
        if first {
            budget.string(&mut target.text, "")?;
        }
        Ok(())
    }
    pub(super) fn thinking_delta(&mut self, index: u64, key: &str, value: &Value) -> Result<()> {
        let Some(value) = present(Some(value)) else {
            return Ok(());
        };
        let (target, budget) = self.thinking(index)?;
        budget.string(&mut target.native, "")?;
        let field = match key {
            "thinking" | "text" => &mut target.text,
            "signature" => &mut target.signature,
            _ => return Err(invalid("invalid reasoning delta field")),
        };
        budget.append(field, Some(value))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn reasoning_shape(&self) -> Result<(u64, u64)> {
        let bytes = self.thinking.values().try_fold(0u64, |bytes, block| {
            bytes
                .checked_add(block.bytes() as u64)
                .ok_or_else(|| invalid("reasoning size overflow"))
        })?;
        let origins =
            self.origin
                .as_ref()
                .map_or(0, |origin| origin.provider.len() + origin.model.len()) as u64;
        let bytes = (self.thinking.len() as u64)
            .checked_mul(origins)
            .and_then(|origins| bytes.checked_add(origins))
            .ok_or_else(|| invalid("reasoning size overflow"))?;
        Ok((bytes, self.thinking.len() as u64 * 8))
    }
    pub(crate) fn set_origin(&mut self, origin: Option<ReasoningOrigin>) -> Result<()> {
        if let Some(origin) = &origin {
            self.budget
                .add(origin.provider.capacity() + origin.model.capacity())?;
        }
        self.origin = origin;
        Ok(())
    }
}

pub(super) fn block(
    adapter: Adapter,
    mut value: Thinking,
    origin: Option<&ReasoningOrigin>,
    budget: &mut budget::Budget<'_>,
) -> Result<ReasoningBlock> {
    if value.native.is_empty() {
        #[derive(serde::Serialize)]
        struct Anthropic<'a> {
            r#type: &'static str,
            thinking: &'a str,
            signature: &'a str,
        }
        #[derive(serde::Serialize)]
        struct Chat<'a> {
            reasoning_content: &'a str,
        }
        if adapter == Adapter::Anthropic {
            budget.encoded(
                &mut value.native,
                &Anthropic {
                    r#type: "thinking",
                    thinking: &value.text,
                    signature: &value.signature,
                },
            )?;
        } else {
            budget.encoded(
                &mut value.native,
                &Chat {
                    reasoning_content: &value.text,
                },
            )?;
        }
    }
    let origin = native_origin(adapter, origin, budget)?;
    let text = (!value.text.is_empty()).then_some(value.text);
    let outcome = if text.is_some() {
        ReasoningOutcome::PreservedReadable
    } else {
        ReasoningOutcome::PreservedOpaque
    };
    Ok(ReasoningBlock {
        kind: value.kind,
        text,
        origin,
        continuity: value.native.into_bytes(),
        outcome,
    })
}

/// The origin every native part of this response carries.
pub(super) fn native_origin(
    adapter: Adapter,
    origin: Option<&ReasoningOrigin>,
    budget: &mut budget::Budget<'_>,
) -> Result<ReasoningOrigin> {
    Ok(if let Some(origin) = origin {
        budget.add(origin.provider.len() + origin.model.len())?;
        origin.clone()
    } else {
        budget.add(family(adapter).len())?;
        ReasoningOrigin {
            provider: family(adapter).into(),
            model: String::new(),
            credential_scope: None,
        }
    })
}

pub(crate) fn family(adapter: Adapter) -> &'static str {
    match adapter {
        Adapter::Anthropic => "anthropic",
        Adapter::Responses => "responses",
        Adapter::Chat => "chat",
        Adapter::Gemini => "gemini",
    }
}
