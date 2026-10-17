//! Local wire acceptance rules from provider-input-rules.md.
use crate::Adapter;
use serde_json::Value;

mod anthropic;
mod chat;
mod gemini;
mod responses;

/// Checks the projected wire grammar locally, without I/O or credential access.
pub fn validate_input(adapter: Adapter, body: &Value) -> Vec<&'static str> {
    let mut errors = Vec::new();
    match adapter {
        Adapter::Anthropic => anthropic::validate(body, &mut errors),
        Adapter::Responses => responses::validate(body, &mut errors),
        Adapter::Chat => chat::validate(body, &mut errors),
        Adapter::Gemini => gemini::validate(body, &mut errors),
    }
    errors
}

fn check(ok: bool, rule: &'static str, errors: &mut Vec<&'static str>) {
    if !ok {
        errors.push(rule);
    }
}

fn name(value: &Value, maximum: usize, leading_letter: bool, punctuation: &'static [u8]) -> bool {
    value.as_str().is_some_and(|text| {
        crate::protocol::name::Name {
            maximum,
            leading_letter,
            punctuation,
        }
        .accepts(text)
    })
}
fn id(value: &Value, maximum: usize) -> bool {
    name(value, maximum, false, b"_-")
}
fn text(value: &Value) -> bool {
    value.as_str().is_some_and(|text| !text.trim().is_empty())
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
