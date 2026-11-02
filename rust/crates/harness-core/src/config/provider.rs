//! Provider settings are data. Protocol encoding belongs to model-render;
//! executable tool authority remains with the selected capability.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use ts_rs::TS;

/// The result of folding `plugins/<provider>/provider.ts` over its base.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderProfile {
    /// Model-visible declarations, freely replaceable by a settings layer.
    pub tools: Vec<NativeTool>,
    /// Where the protocol may retain prompt prefixes.
    pub cache: ProviderCache,
    /// Which summary representation the selected provider may use.
    pub compaction: Compaction,
    /// Whether summary calls retain provider-executed builtins.
    pub summary_header: SummaryHeader,
    /// Per-call settings for each reasoning effort.
    pub effort: ProviderEffort,
    /// Defaults used when the agent leaves sampling unspecified.
    pub sampling: ProviderSampling,
}

/// A declaration changes model input, without granting its executable authority.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct NativeTool {
    /// The declaration's model-visible name.
    pub name: String,
    /// The provider's declaration object; provider-specific keys are preserved.
    #[ts(type = "object")]
    pub definition: Map<String, Value>,
}

/// Cache settings interpreted by the selected protocol.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct ProviderCache {
    /// Ordered locations at which a protocol may mark reusable bytes.
    pub breakpoints: Vec<CacheBreakpoint>,
    /// Retention requested from the provider.
    pub ttl: CacheTtl,
}

/// A cache breakpoint's location in the shared renderer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
pub enum CacheBreakpoint {
    /// The frozen request header.
    Header,
    /// Completed conversation segments.
    Segments,
}

/// The provider's supported cache retention choices.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum CacheTtl {
    /// Five minutes.
    #[serde(rename = "5m")]
    FiveMinutes,
    /// One hour.
    #[serde(rename = "1h")]
    Hour,
}

/// Summary representation selection; a portable summary is always retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "kebab-case")]
pub enum Compaction {
    /// Use native artifacts only with their admitted producer.
    NativeWhenAvailable,
    /// Render the portable summary on every provider.
    Portable,
}

/// Header treatment for a summary call through the same wire renderer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "kebab-case")]
pub enum SummaryHeader {
    /// Remove builtins that the provider itself can execute.
    StripExecutableBuiltins,
    /// Preserve the admitted header.
    Keep,
}

/// Protocol fields selected by the agent's per-call effort.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct ProviderEffort {
    /// Least reasoning.
    #[ts(type = "object")]
    pub low: Map<String, Value>,
    /// The middle setting.
    #[ts(type = "object")]
    pub medium: Map<String, Value>,
    /// Most reasoning.
    #[ts(type = "object")]
    pub high: Map<String, Value>,
}

/// Provider sampling defaults. Null defers to the selected model.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderSampling {
    /// Temperature in the selected protocol's units.
    pub temperature: Option<f64>,
    /// Probability mass retained by nucleus sampling.
    pub top_p: Option<f64>,
    /// Output reservation, absent when the user has not set one.
    pub max_output_tokens: Option<u32>,
}

/// Fold provider settings with the shared evaluator and admit the result.
/// The returned links are the evaluator's original settings provenance.
///
/// # Errors
/// A module evaluation or provider settings admission refusal.
#[cfg(not(target_arch = "wasm32"))]
pub async fn admit(
    path: &str,
    layers: &[crate::layers::Layer<'_>],
    tree: &dyn super::Tree,
    base: Value,
) -> Result<(ProviderProfile, Vec<super::Link>)> {
    let (value, links) = super::compose(path, layers, tree, base).await?;
    Ok((decode(value)?, links))
}

/// Decode the shared evaluator's result once, at settings admission.
///
/// # Errors
/// [`Error::Invalid`] when a setting has an unknown field or wrong shape.
pub fn decode(value: Value) -> Result<ProviderProfile> {
    serde_json::from_value(value).map_err(|error| Error::Invalid(format!("provider.ts: {error}")))
}
