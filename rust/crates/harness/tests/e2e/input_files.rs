use super::file_bytes::FileBytes;
use acyclic_harness::model::FileProjectionPolicy;
use serde::{Deserialize, Serialize};

/// An uploaded input, published once before its reference enters the prompt.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputFile {
    pub path: String,
    pub contents: FileBytes,
    #[serde(default = "binary_media_type")]
    pub media_type: String,
    #[serde(default = "reference")]
    pub policy: FileProjectionPolicy,
}

fn binary_media_type() -> String {
    "application/octet-stream".into()
}

fn reference() -> FileProjectionPolicy {
    FileProjectionPolicy::Reference
}
