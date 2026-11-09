use super::*;

/// An immutable namespace selection. The original producer supplies its
/// generation and path; this does not assert a file length or content digest.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(deny_unknown_fields)]
pub struct PathSelection {
    /// Original selected volume.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmVolumeRefWire"))]
    pub volume: VolumeRef,
    /// Validated path relative to that volume.
    pub path: String,
    /// Generation retained by the original producer.
    #[cfg_attr(
        feature = "wasm",
        tsify(type = "WasmResourceRefWire & { kind: 'generation' }")
    )]
    pub generation: crate::resources::GenerationRef,
}

/// Borrowed wire projection: encoding a path version copies no retained
/// volume, generation or path before writing the original output buffer.
#[derive(Serialize)]
pub(crate) struct PathSelectionView<'a> {
    pub volume: &'a VolumeRef,
    pub path: &'a str,
    pub generation: &'a crate::resources::GenerationRef,
}

impl PathSelection {
    pub(crate) fn view(&self) -> PathSelectionView<'_> {
        PathSelectionView {
            volume: &self.volume,
            path: &self.path,
            generation: &self.generation,
        }
    }

    pub(crate) fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.view().validate(work)
    }

    pub(crate) fn matches_source(&self, file: &FilePathSelection) -> bool {
        self.volume == file.volume && self.path == file.path && self.generation == file.generation
    }

    pub(crate) fn matches_content(&self, file: &FileRef) -> bool {
        self.view().matches_content(file)
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        self.view().logical_owned_bytes(self.path.capacity())
    }
}

impl PathSelectionView<'_> {
    fn logical_owned_bytes(&self, path_capacity: usize) -> Result<u64> {
        crate::core::owned_sum([
            self.volume.logical_owned_bytes()?,
            path_capacity as u64,
            self.generation.logical_owned_bytes()?,
        ])
    }

    pub(crate) fn matches_content(&self, file: &FileRef) -> bool {
        self.volume == file.volume()
            && self.path == file.path()
            && names_generation(file, self.generation)
    }

    fn validate(&self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
        self.volume.validate(work)?;
        self.generation.validate(work)?;
        validate_content_path(self.path, work)?;
        if self.generation.as_resource().provider() != self.volume.provider()
            || is_internal_path(self.path)
        {
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "path selection belongs to another provider or an internal path",
            ));
        }
        Ok(())
    }
}
/// Original allocation ownership for an authored file selection.
pub(crate) type FileSelectionCustody = Option<crate::filesystem::PreparationCustody>;

/// An immutable authored file selection. This identifies the selected generation
/// and path; it makes no whole-file digest or native snapshot completeness claim.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(deny_unknown_fields)]
pub struct FilePathSelection {
    /// Original selected volume.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmVolumeRefWire"))]
    pub volume: VolumeRef,
    /// Validated path relative to that volume.
    pub path: String,
    /// Generation retained by the original read producer.
    #[cfg_attr(
        feature = "wasm",
        tsify(type = "WasmResourceRefWire & { kind: 'generation' }")
    )]
    pub generation: crate::resources::GenerationRef,
    /// Length obtained by that producer's original stat.
    pub byte_length: u64,
}

impl FilePathSelection {
    /// Whether a complete descriptor names this authored generation and path.
    /// Authored path producers encode the generation key as lowercase hex in
    /// the complete descriptor's version. This compares identity only; the
    /// original content reader still authenticates its bytes and descriptor.
    pub fn matches_content(&self, file: &FileRef) -> bool {
        self.path_selection().matches_content(file)
            && self.byte_length == file.descriptor().byte_length()
    }

    pub(crate) fn path_selection(&self) -> PathSelectionView<'_> {
        PathSelectionView {
            volume: &self.volume,
            path: &self.path,
            generation: &self.generation,
        }
    }
}

/// Whether a complete descriptor's version is `generation`'s key, as authored
/// path producers encode it (lowercase hex).
pub(crate) fn names_generation(
    file: &FileRef,
    generation: &crate::resources::GenerationRef,
) -> bool {
    let key = generation.as_resource().key();
    const HEX: &[u8; 16] = b"0123456789abcdef";
    Some(file.version().len()) == key.len().checked_mul(2)
        && file.version().bytes().eq(key
            .iter()
            .flat_map(|byte| [HEX[(byte >> 4) as usize], HEX[(byte & 15) as usize]]))
}

impl FilePathSelection {
    pub(crate) fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.path_selection().validate(work)
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        self.path_selection()
            .logical_owned_bytes(self.path.capacity())
    }
}
