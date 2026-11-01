use serde::{Deserialize, Serialize};

/// Pinned tool contract selected by the model catalog. This identity grants no
/// authority; every declaration binds the original capability executor.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum NativeToolProfile {
    /// Claude Code's public tool contracts.
    Anthropic,
    /// Codex CLI's command-array and freeform patch contracts.
    Openai,
    /// Gemini CLI's public tool contracts.
    Google,
}
