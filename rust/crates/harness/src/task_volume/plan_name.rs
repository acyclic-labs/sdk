use super::{Error, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Markdown filename stem, with the same path rules as ordinary file tools.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PlanName(pub(super) Arc<str>);

impl PlanName {
    /// Validates one filename in plans/, preserving spaces and Unicode.
    pub fn new(value: impl AsRef<str>) -> Result<Self> {
        let value = value.as_ref();
        let path = format!("plans/{value}.md");
        if value.contains('/') || path.len() > 4096 {
            return Err(Error::InvalidPath);
        }
        crate::conversation::validate_content_path(&path, &mut None)
            .map_err(|_| Error::InvalidPath)?;
        Ok(Self(Arc::from(value)))
    }

    /// Filename text before its .md extension.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for PlanName {
    type Error = Error;
    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

impl From<PlanName> for String {
    fn from(value: PlanName) -> Self {
        value.0.to_string()
    }
}
