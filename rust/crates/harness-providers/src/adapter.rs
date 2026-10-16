use serde::{Deserialize, Serialize};

/// Wire protocol implemented by an endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Adapter {
    /// Anthropic Messages with API keys only.
    Anthropic,
    /// OpenAI Responses.
    Responses,
    /// OpenAI-compatible Chat Completions.
    Chat,
    /// Gemini generateContent.
    Gemini,
}
