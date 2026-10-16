use crate::Adapter;
use acyclic_harness::context::ModelContextCapacity;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Authentication required by a registered endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Authentication {
    /// A provider API key is required.
    ApiKey,
    /// The endpoint accepts anonymous requests; use an API key when configured.
    Optional,
}

/// Endpoint configuration independent of immutable model identity.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderSpec {
    /// Protocol dialect.
    pub adapter: Adapter,
    /// Credential policy supplied by the catalog, independent of its provider ID.
    pub authentication: Authentication,
    /// HTTPS API root, or loopback HTTP API root.
    pub base_url: String,
    /// Exact model capacities, including local server configurations.
    #[serde(default)]
    pub models: BTreeMap<String, ModelContextCapacity>,
    /// Verified admission counter. Required for automatic compaction; generic
    /// compatible endpoints must configure their own tokenizer contract.
    #[serde(default)]
    pub token_counter: Option<crate::TokenCounterSpec>,
    /// Maximum bytes in one SSE event and aggregate retained protocol state.
    pub max_event_bytes: usize,
}
