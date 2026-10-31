use super::*;

/// Exact immutable UTF-8 file read arguments.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(deny_unknown_fields)]
pub struct ReadFileInput {
    /// Public path relative to the agent's project volume.
    pub path: String,
    /// Opaque version from a prior read or publication. Omission reads the
    /// current path under the original directory read grant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Optional exact UTF-8 byte interval; omission reads the complete file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<super::text::TextRange>,
}

/// Single-operation UTF-8 publication arguments.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
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
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(deny_unknown_fields)]
pub struct EditFileInput {
    /// Public path relative to the agent's project volume.
    pub path: String,
    /// Opaque version from a prior read or publication. Omission resolves the
    /// current path under the original directory read grant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// One exact nonempty unambiguous needle.
    #[schemars(length(min = 1))]
    pub old_text: String,
    /// Exact replacement, including an empty deletion.
    pub new_text: String,
}

/// Explicit single-file V4A update arguments.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
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
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(deny_unknown_fields)]
pub struct FileResult {
    /// Exact retained publication result with workspace and content identity.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
}

/// Canonical read result retains its verified source independently of the
/// compact model projection.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(deny_unknown_fields)]
pub struct ReadFileResult {
    /// Original immutable source, used for grant and replay validation.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
    /// Exact selected UTF-8 text.
    pub text: String,
}

/// Model-visible file identity; the version is opaque and grants no authority.
#[derive(Serialize, schemars::JsonSchema)]
pub(crate) struct FileView<'a> {
    pub path: &'a str,
    pub version: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<&'a str>,
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
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
            let version = super::super::version::encode_version(&file, None).unwrap().0;
            round_trip(ReadFileInput { path: path.clone(), version: Some(version.clone()), range: endpoints.map(|(start, end)| text::TextRange { start, end }) });
            round_trip(EditFileInput { path: path.clone(), version: Some(version), old_text: text.clone(), new_text: replacement });
            round_trip(WriteFileInput { path, text, media_type: "text/plain".into(), display_name: "file".into() });
        }
    }
}
