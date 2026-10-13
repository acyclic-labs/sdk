//! Selected immutable task volume metadata.
use super::*;

/// Exact volume and generation selected by authenticated allocation metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct TaskVolumeSnapshot {
    /// Selected volume identity.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmVolumeRefWire"))]
    pub volume: crate::conversation::VolumeRef,
    /// Selected immutable generation.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmResourceRefWire"))]
    pub generation: crate::resources::GenerationRef,
}
