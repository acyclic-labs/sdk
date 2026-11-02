//! The lab-trained tool layers. Selection belongs to the model catalog.
use super::{File, ModelLayer, ToolRevision};
use crate::Result;
pub use crate::model::NativeToolProfile;

impl NativeToolProfile {
    /// Versioned family file consumed by the existing profile resolver: the
    /// seed's `sys/native/<family>/profile.md`.
    pub const fn family_layer(self) -> &'static str {
        match self {
            Self::Anthropic => crate::seed::ANTHROPIC_PROFILE,
            Self::Openai => crate::seed::OPENAI_PROFILE,
            Self::Google => crate::seed::GOOGLE_PROFILE,
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
