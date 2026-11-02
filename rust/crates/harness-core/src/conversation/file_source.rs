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

    #[cfg(feature = "filesystem")]
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        crate::core::owned_sum([
            self.volume.logical_owned_bytes()?,
            self.path.capacity() as u64,
            self.generation.logical_owned_bytes()?,
        ])
    }
}

impl PathSelectionView<'_> {
    /// Authored content references encode the generation key as lowercase hex.
    /// Identity comparison does not substitute for the original content check.
    pub(crate) fn matches_content(&self, file: &FileRef) -> bool {
        let key = self.generation.as_resource().key();
        const HEX: &[u8; 16] = b"0123456789abcdef";
        self.volume == file.volume()
            && self.path == file.path()
            && Some(file.version().len()) == key.len().checked_mul(2)
            && file.version().bytes().eq(key
                .iter()
                .flat_map(|byte| [HEX[(byte >> 4) as usize], HEX[(byte & 15) as usize]]))
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
    pub(crate) fn path_selection(&self) -> PathSelectionView<'_> {
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
        self.path_selection().validate(work)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        crate::core::owned_sum([
            self.volume.logical_owned_bytes()?,
            self.path.capacity() as u64,
            self.generation.logical_owned_bytes()?,
        ])
    }
}
