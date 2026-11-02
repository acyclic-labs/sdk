use acyclic_harness_core::context::ModelContextCapacity;
use serde::{Deserialize, Serialize};

fn is_false(value: &bool) -> bool {
    !*value
}

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
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
pub struct ModelSpec {
    /// Publisher-selected immutable price for this exact provider/model.
    /// Absence means unknown, never zero or a rate inferred from its name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price: Option<acyclic_harness_core::executor::ModelPriceRecord>,
    /// Lab-native tool contracts for this exact model. Custom entries without
    /// a declared profile do not acquire one from their name or endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_profile: Option<acyclic_harness_core::model::NativeToolProfile>,
    /// Accepts all three profile thinking levels: low, medium and high.
    /// This is an exact catalog fact, never inferred from the model name.
    /// Omission admits provider defaults but no explicit level control.
    #[serde(default, skip_serializing_if = "is_false")]
    pub thinking_levels: bool,
    /// Maximum context accepted by an explicit context override, when advertised.
    #[serde(default)]
    pub max_context_tokens: Option<u32>,
    /// Selected context limit and any verified generated-output limit.
    pub capacity: ModelContextCapacity,
    /// Undeclared features fail closed during alloy resolution.
    #[serde(default)]
    pub features: ModelFeatures,
}

/// Public facts for one catalogue member. This borrows the original model
/// specification without exposing its endpoint, headers or authentication.
/// Presence is discovery only; the caller's signed grant authorizes selection.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct CatalogModel<'a> {
    /// Exact provider ID.
    pub provider: &'a str,
    /// Exact provider-owned name.
    pub model: &'a str,
    /// Protocol dialect, independent of the native tool family.
    pub wire_family: crate::Adapter,
    /// The same declared model facts used by the original resolver.
    #[serde(flatten)]
    pub specification: &'a ModelSpec,
}

impl<'a> CatalogModel<'a> {
    pub(crate) fn new(
        provider: &'a str,
        model: &'a str,
        endpoint: &crate::ProviderSpec,
        specification: &'a ModelSpec,
    ) -> Self {
        Self {
            provider,
            model,
            wire_family: endpoint.adapter,
            specification,
        }
    }
}
