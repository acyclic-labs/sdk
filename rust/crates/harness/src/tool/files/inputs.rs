use super::*;

/// Exact immutable UTF-8 file read arguments.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReadFileInput {
    /// Owner-authorized immutable source.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
}

/// Single-operation UTF-8 publication arguments.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WriteFileInput {
    /// Valid public destination path.
    pub path: String,
    /// Exact content bytes encoded as UTF-8.
    pub text: String,
    /// Stored MIME type.
    pub media_type: String,
    /// Human-readable file name.
    pub display_name: String,
}

/// Exact replacement against one pinned file generation.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EditFileInput {
    /// Owner-authorized immutable source.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
    /// One exact nonempty unambiguous needle.
    #[schemars(length(min = 1))]
    pub old_text: String,
    /// Exact replacement, including an empty deletion.
    pub new_text: String,
}

/// Explicit single-file V4A update arguments.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PatchFileInput {
    /// Owner-authorized immutable source generation.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
    /// Nonempty exact V4A update diff fragment.
    #[schemars(length(min = 1))]
    pub diff: String,
}

/// Canonical successful write/edit result, independent of model projection.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FileResult {
    /// Exact retained publication result with workspace and content identity.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
}
