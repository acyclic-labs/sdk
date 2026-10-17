use super::super::{Error, Result};
use serde::{Deserialize, Serialize};

/// Canonical command, skill, config or workspace input source path.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct InputPath(pub(super) String);

impl InputPath {
    /// Canonical input source spelling.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for InputPath {
    type Error = Error;
    fn try_from(path: String) -> Result<Self> {
        let mut parts = path.split('/');
        if path.len() > 4096
            || !matches!(
                parts.next(),
                Some("commands" | "workspace" | "config" | ".claude" | ".codex" | "skills")
            )
            || !path.contains('/')
            || parts.any(|p| {
                p.is_empty()
                    || p == "."
                    || p == ".."
                    || p.chars().any(|c| c.is_control() || matches!(c, '\\' | ':'))
            })
        {
            return Err(Error::InvalidPath);
        }
        Ok(Self(path))
    }
}
impl From<InputPath> for String {
    fn from(path: InputPath) -> Self {
        path.0
    }
}
