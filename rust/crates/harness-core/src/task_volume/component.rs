use super::super::{Error, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A portable, canonical namespace component.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct Component(#[cfg_attr(feature = "wasm", tsify(type = "string"))] pub(super) Arc<str>);

impl Component {
    /// Validates a task, request, tool, principal or mount identity.
    pub fn new(value: impl AsRef<str>) -> Result<Self> {
        let value = value.as_ref();
        if value.is_empty()
            || value.len() > 255
            || value == "."
            || value == ".."
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        {
            return Err(Error::InvalidPath);
        }
        Ok(Self(Arc::from(value)))
    }

    /// Canonical component text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Component {
    type Error = Error;
    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}
impl From<Component> for String {
    fn from(value: Component) -> Self {
        value.0.to_string()
    }
}
