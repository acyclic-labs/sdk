use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct DiffCase {
    pub before: Option<Input>,
    pub after: Option<Input>,
    pub render: usize,
    pub work: usize,
    pub expect: Value,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub(super) enum Input {
    Text(String),
    Bytes(Vec<u8>),
}

impl Input {
    pub fn bytes(&self) -> &[u8] {
        match self {
            Self::Text(text) => text.as_bytes(),
            Self::Bytes(bytes) => bytes,
        }
    }
}
