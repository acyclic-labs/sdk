use super::*;

/// Immutable identity retained by a read. A bounded authored selection does not
/// claim a whole-file content digest.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(rename_all = "snake_case")]
pub enum FileReadSource {
    /// A verified complete content descriptor.
    Content(#[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))] FileRef),
    /// The exact authored generation selected by the original range reader.
    Authored(crate::conversation::FilePathSelection),
}

impl FileReadSource {
    pub(super) fn same_source(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Authored(selected), Self::Content(file))
            | (Self::Content(file), Self::Authored(selected)) => selected.matches_content(file),
            _ => self == other,
        }
    }

    pub(super) fn volume(&self) -> &crate::conversation::VolumeRef {
        match self {
            Self::Content(file) => file.volume(),
            Self::Authored(file) => &file.volume,
        }
    }

    pub(super) fn path(&self) -> &str {
        match self {
            Self::Content(file) => file.path(),
            Self::Authored(file) => &file.path,
        }
    }

    pub(super) fn byte_length(&self) -> u64 {
        match self {
            Self::Content(file) => file.descriptor().byte_length(),
            Self::Authored(file) => file.byte_length,
        }
    }

    pub(super) fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        match self {
            Self::Content(file) => file.validate(work),
            Self::Authored(file) => file.validate(work),
        }
    }

    #[cfg(feature = "filesystem")]
    pub(super) fn logical_owned_bytes(&self) -> Result<u64> {
        match self {
            Self::Content(file) => file.logical_owned_bytes(),
            Self::Authored(file) => file.logical_owned_bytes(),
        }
    }
}
