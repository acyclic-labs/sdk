use acyclic_harness_core::context::ModelContextCapacity;
use serde::{Deserialize, Serialize};

/// Features explicitly advertised by a model's catalog entry.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
pub struct ModelFeatures {
    /// Accepts tool declarations, calls and results.
    pub tools: bool,
    /// Accepts images in input.
    pub images: bool,
}

impl ModelFeatures {
    pub(crate) fn contains(self, required: Self) -> bool {
        (!required.tools || self.tools) && (!required.images || self.images)
    }
}

/// One model's exact configured capacity and declared input capabilities.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
pub struct ModelSpec {
    /// Maximum context accepted by an explicit context override, when advertised.
    #[serde(default)]
    pub max_context_tokens: Option<u32>,
    /// Selected context limit and any verified generated-output limit.
    pub capacity: ModelContextCapacity,
    /// Undeclared features fail closed during alloy resolution.
    #[serde(default)]
    pub features: ModelFeatures,
}
