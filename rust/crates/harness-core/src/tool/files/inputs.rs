use super::*;

/// Version-pinned file/media read arguments shared by native tool profiles.
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
    /// Optional UTF-8 byte interval. Large intervals return a continuation page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<super::text::TextRange>,
    /// A line, cell or native media projection. Mutually exclusive with `range`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<ReadSelection>,
}

/// Requested projection of the authoritative source file. Offsets are zero based;
/// omitted counts have no count ceiling. Model views remain byte paged.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReadSelection {
    /// Text lines, with an optional byte cursor returned by a previous page.
    Lines {
        /// First line to return.
        #[serde(default)]
        offset: u64,
        /// Number of lines; omission means no count ceiling.
        #[serde(default)]
        limit: Option<u64>,
    },
    /// Notebook cells, including their outputs, from the same JSON file.
    Cells {
        /// First cell to return.
        #[serde(default)]
        offset: u64,
        /// Number of cells; omission means no count ceiling.
        #[serde(default)]
        limit: Option<u64>,
    },
    /// Native image input, preserving the requested provider detail.
    Image {
        /// Provider image detail.
        detail: crate::model::ImageDetail,
    },
    /// Native PDF input.
    Pdf,
}

/// Exact provenance and continuation for a projected read.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(deny_unknown_fields)]
pub struct ReadPage {
    /// Selected byte interval in the original source, including notebook JSON.
    pub start: u64,
    /// Exclusive end of the selected interval.
    pub end: u64,
    /// Next byte offset for `range`, absent at end of source.
    pub next: Option<u64>,
    /// First zero-based line/cell in this view.
    pub offset: u64,
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
    pub content: String,
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
    /// Exact nonempty needle; one unambiguous match unless `replace_all` is set.
    #[serde(default)]
    pub old_text: String,
    /// Exact replacement, including an empty deletion.
    #[serde(default)]
    pub new_text: String,
    /// Replace every non-overlapping occurrence; omission requires one unique match.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub replace_all: bool,
    /// Cell edit against this same versioned file, exclusive with text replacement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notebook: Option<NotebookEdit>,
}

/// A cell operation on an nbformat v4 notebook; metadata and other cells survive.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NotebookEdit {
    /// Replace the source of a cell identified by index and optionally stable ID.
    Replace {
        /// Zero-based position.
        index: u64,
        /// Expected stable ID, when the caller has one.
        id: Option<String>,
        /// New cell source; code outputs and execution count are cleared.
        source: String,
    },
    /// Insert before `index` (the cell count appends).
    Insert {
        /// Zero-based insertion position; the cell count appends.
        index: u64,
        /// New nbformat cell ID, unique within this notebook.
        id: String,
        /// One of `code`, `markdown`, or `raw`.
        cell_type: String,
        /// Initial cell source.
        source: String,
    },
    /// Delete exactly the selected cell.
    Delete {
        /// Zero-based position.
        index: u64,
        /// Expected stable ID, when the caller has one.
        id: Option<String>,
    },
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
    pub file: FileReadSource,
    /// Exact selected UTF-8 text.
    pub text: String,
    /// Provenance of a bounded text/cell view; absent for exact complete reads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page: Option<ReadPage>,
    /// Native media selection; the immutable file supplies its bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<ReadSelection>,
    /// Native MIME outputs from selected notebook cells. Payload intervals
    /// refer to this result's original notebook, never the derived file.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<NotebookOutput>,
}

/// A retained native notebook output and its exact original JSON provenance.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(deny_unknown_fields)]
pub struct NotebookOutput {
    /// Immutable decoded media, retained by the original private-volume writer.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
    /// Zero-based source cell.
    pub cell_index: u64,
    /// Zero-based output within that cell.
    pub output_index: u64,
    /// First byte of the original MIME value, including JSON framing.
    pub start: u64,
    /// Exclusive end of that original MIME value.
    pub end: u64,
}

/// Model-visible file identity; the version is opaque and grants no authority.
#[derive(Serialize, schemars::JsonSchema)]
pub(crate) struct FileView<'a> {
    pub path: &'a str,
    pub version: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<&'a ReadPage>,
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
            round_trip(ReadFileInput { path: path.clone(), version: Some(version.clone()), range: endpoints.map(|(start, end)| text::TextRange { start, end }), selection: None });
            round_trip(EditFileInput { path: path.clone(), version: Some(version), old_text: text.clone(), new_text: replacement, replace_all: endpoints.is_some(), notebook: None });
            round_trip(WriteFileInput { path, content: text });
        }
    }
}
