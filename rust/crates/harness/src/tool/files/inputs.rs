use super::*;

/// Workspace-relative text read arguments; the host resolves the authenticated file.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReadFileInput {
    /// File path relative to the task workspace root.
    #[schemars(length(min = 1))]
    pub path: String,
    /// One-based line number to start reading from; omission starts at line 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub offset: Option<u64>,
    /// Maximum number of lines to read; omission reads to the end of the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub limit: Option<u64>,
}

/// Canonical read result: the selected lines and the exact file they came from.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReadFileResult {
    /// Authenticated file generation that was read.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
    /// One-based number of the first selected line.
    pub offset: u64,
    /// Exact selected UTF-8 lines, including their line terminators.
    pub text: String,
}

/// Single-operation UTF-8 publication arguments.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WriteFileInput {
    /// File path relative to the task workspace root.
    pub path: String,
    /// Exact content bytes encoded as UTF-8.
    pub text: String,
    /// Stored MIME type; omission stores `text/plain`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// Human-readable file name; omission uses the path's final component.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
}

impl WriteFileInput {
    /// Stored MIME type after applying the default.
    #[must_use]
    pub fn media_type(&self) -> &str {
        self.media_type.as_deref().unwrap_or("text/plain")
    }

    /// Display name after applying the default.
    #[must_use]
    pub fn display_name(&self) -> &str {
        self.display_name
            .as_deref()
            .unwrap_or_else(|| self.path.rsplit('/').next().unwrap_or(&self.path))
    }
}

/// Exact replacement in the file currently at a workspace-relative path.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EditFileInput {
    /// File path relative to the task workspace root.
    #[schemars(length(min = 1))]
    pub path: String,
    /// Exact nonempty text to replace; it must match exactly once unless `replace_all` is set.
    #[schemars(length(min = 1))]
    pub old_string: String,
    /// Exact replacement, including an empty deletion.
    pub new_string: String,
    /// Replaces every non-overlapping match instead of requiring a unique one.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub replace_all: bool,
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod codec_tests {
    use super::*;
    use crate::{
        conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
        resources::ProviderRef,
    };
    use proptest::prelude::*;

    fn round_trip<T: Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug>(
        value: T,
    ) {
        let encoded = serde_json::to_vec(&value).unwrap();
        assert_eq!(serde_json::from_slice::<T>(&encoded).unwrap(), value);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        // Every accepted structured tool argument has a lossless wire codec,
        // including arbitrary Unicode text and the full range endpoint domain.
        #[test]
        fn file_arguments_round_trip(
            path in "[a-z]{1,16}/[a-z]{1,16}", text in ".{0,128}",
            replacement in ".{0,128}", endpoints in any::<Option<(u64, u64)>>(),
        ) {
            let volume = VolumeRef::new(
                ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None).unwrap(),
                "volume".into(), VolumeClass::Project, VolumeOwner::Project("project".into()), &mut None,
            ).unwrap();
            let file = FileRef::new(volume, path.clone(), "version".into(),
                FileDescriptor::new([1; 32], 128, "text/plain".into()).unwrap(), "file".into(), &mut None).unwrap();
            round_trip(ReadFileInput { path: path.clone(), offset: endpoints.map(|(start, _)| start), limit: endpoints.map(|(_, end)| end) });
            round_trip(ReadFileResult { file, offset: endpoints.map_or(1, |(start, _)| start), text: text.clone() });
            round_trip(EditFileInput { path: path.clone(), old_string: text.clone(), new_string: replacement, replace_all: endpoints.is_some() });
            round_trip(WriteFileInput { path, text, media_type: endpoints.map(|_| "text/plain".into()), display_name: None });
        }
    }
}
