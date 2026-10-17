use super::*;

/// Literal search arguments; there is no implicit regex, directory or shell search.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchInput {
    /// Owner-authorized immutable source.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
    /// Nonempty case-sensitive UTF-8 literal, including overlapping matches.
    #[schemars(length(min = 1))]
    pub query: String,
}

/// Durable literal-search result, independent of its model projection.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchResult {
    /// Original reference, including provider, volume, generation and content identity.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
    /// Exact literal searched in that source.
    pub query: String,
    /// Complete match accounting with explicitly bounded retained positions.
    pub matches: SearchMatches,
}
