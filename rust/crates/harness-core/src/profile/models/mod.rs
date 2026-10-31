//! Built-in profile files. These are the same ordinary files a project may pin;
//! they do not discover providers, invent capacities or bypass model admission.

use crate::{Error, Result};

/// Borrows a built-in family's and selected model's exact source files.
/// Unknown keys require explicit files instead of a guessed family or model.
pub fn sources(key: &str) -> Result<(&'static str, &'static str)> {
    let sources = match key {
        "openai/gpt-6.1-sol" => (OPENAI, include_str!("openai/gpt-6.1-sol.md")),
        "openai/gpt-6-astra" => (OPENAI, include_str!("openai/gpt-6-astra.md")),
        "openai/gpt-6.1-terra" => (OPENAI, include_str!("openai/gpt-6.1-terra.md")),
        "openai/gpt-6-luna" => (OPENAI, include_str!("openai/gpt-6-luna.md")),
        "anthropic/claude-opus-5-5" => (ANTHROPIC, include_str!("anthropic/claude-opus-5-5.md")),
        "anthropic/claude-sonnet-5-5" => {
            (ANTHROPIC, include_str!("anthropic/claude-sonnet-5-5.md"))
        }
        "anthropic/claude-haiku-5-5" => (ANTHROPIC, include_str!("anthropic/claude-haiku-5-5.md")),
        "anthropic/claude-fable-5-1" => (ANTHROPIC, include_str!("anthropic/claude-fable-5-1.md")),
        "gemini/gemini-2.5-pro" => (GEMINI, include_str!("gemini/gemini-2.5-pro.md")),
        "gemini/gemini-2.5-flash" => (GEMINI, include_str!("gemini/gemini-2.5-flash.md")),
        _ => {
            return Err(Error::Invalid(
                "profile: model has no built-in profile files".into(),
            ));
        }
    };
    Ok(sources)
}

const OPENAI: &str = include_str!("openai/_family.md");
const ANTHROPIC: &str = include_str!("anthropic/_family.md");
const GEMINI: &str = include_str!("gemini/_family.md");
