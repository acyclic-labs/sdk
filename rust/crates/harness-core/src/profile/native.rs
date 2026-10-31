//! The lab-trained tool layers. Selection belongs to the model catalog.
use super::{File, ModelLayer, ToolRevision};
use crate::Result;
pub use crate::model::NativeToolProfile;

impl NativeToolProfile {
    /// Versioned family file consumed by the existing profile resolver.
    pub const fn family_layer(self) -> &'static str {
        match self {
            Self::Anthropic => include_str!("native/anthropic.md"),
            Self::Openai => include_str!("native/openai.md"),
            Self::Google => include_str!("native/google.md"),
        }
    }

    /// Resolves the pinned declarations through the original family decoder.
    pub fn tools(self) -> Result<Vec<ToolRevision>> {
        Ok(File::<ModelLayer>::parse(self.family_layer())?
            .metadata
            .tools
            .unwrap_or_default())
    }
}
