use super::*;

/// Exact immutable UTF-8 file read arguments.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReadFileInput {
    /// Owner-authorized immutable source.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
    /// Optional exact UTF-8 byte interval; omission reads the complete file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<super::text::TextRange>,
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
            round_trip(ReadFileInput { file: file.clone(), range: endpoints.map(|(start, end)| text::TextRange { start, end }) });
            round_trip(EditFileInput { file, old_text: text.clone(), new_text: replacement });
            round_trip(WriteFileInput { path, text, media_type: "text/plain".into(), display_name: "file".into() });
        }
    }
}
