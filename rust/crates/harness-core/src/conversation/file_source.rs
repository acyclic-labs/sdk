use super::*;

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
    pub(crate) fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.volume.validate(work)?;
        self.generation.validate(work)?;
        validate_content_path(&self.path, work)?;
        if self.generation.as_resource().provider() != self.volume.provider()
            || is_internal_path(&self.path)
        {
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "file selection belongs to another provider or an internal path",
            ));
        }
        Ok(())
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
