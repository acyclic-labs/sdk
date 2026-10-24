#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        clippy::too_many_lines,
        clippy::cognitive_complexity,
        reason = "unit tests run ordered scenarios over fixtures they built"
    )
)]
//! Provider-neutral web search, with credentials confined to the host.
//!
//! Depend on `acyclic-harness`; it re-exports this crate as `search`.

/// Credential-free, bounded HTTP source retrieval.
pub mod url_fetch;
#[cfg(all(
    feature = "filesystem",
    feature = "search-http",
    not(target_arch = "wasm32")
))]
#[doc(hidden)]
pub mod web;

// The core's items keep their `crate::` paths here, as in the facade.
use acyclic_harness_core::*;

use crate::{Error, Result};
use acyclic_stream::BoxProviderFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub mod files;
#[cfg(all(feature = "search-http", not(target_arch = "wasm32")))]
pub(crate) mod http;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) mod native;
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod test_support;
#[cfg(test)]
mod tests;
#[cfg(all(
    feature = "search-http",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
pub use http::decode_search_http_response;
#[cfg(all(feature = "search-http", not(target_arch = "wasm32")))]
pub use http::{Exa, TinyFish};
#[cfg(all(feature = "search-http", not(target_arch = "wasm32")))]
mod config;
#[cfg(all(feature = "search-http", not(target_arch = "wasm32")))]
pub use config::{SearchBackend, SearchConfig};

/// A bounded query, independent of a provider's wire protocol.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchRequest {
    /// Natural-language query, at most 4,096 Unicode characters, sent verbatim.
    #[schemars(length(min = 1, max = 4096))]
    pub query: String,
    /// Maximum number of ranked results to return.
    #[serde(default = "default_results")]
    #[schemars(range(min = 1, max = 20))]
    pub max_results: u32,
}

fn default_results() -> u32 {
    5
}

impl SearchRequest {
    /// Validates a request file before provider dispatch.
    pub fn validate(&self) -> Result<()> {
        self.validate_with_work(None)
    }

    pub(crate) fn validate_with_work(&self, work: crate::NativeWork<'_>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: self.query.len() as u64,
                ..Default::default()
            })?;
        }
        let mut length = 0;
        let mut non_whitespace = false;
        for character in self.query.chars().take(4097) {
            length += 1;
            non_whitespace |= !character.is_whitespace();
        }
        if !non_whitespace || length > 4096 || !(1..=20).contains(&self.max_results) {
            return Err(rejection(work, "invalid web search query or result limit"));
        }
        Ok(())
    }
}

/// One ranked source. Provider-specific metadata and credentials stay private.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchResult {
    /// Source URL for citations.
    pub url: String,
    /// Source title, empty when the provider has none.
    pub title: String,
    /// Provider-supplied excerpt.
    pub snippet: String,
}

/// Canonical successful search output.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchResponse {
    /// Sources in provider relevance order.
    pub results: Vec<SearchResult>,
}

/// Replaceable search capability. Dropping its future cancels the request.
pub trait SearchProvider: acyclic_stream::ProviderPlatform {
    /// Searches once. Durable replay belongs to the task request/result files.
    fn search<'a>(
        &'a self,
        request: &'a SearchRequest,
    ) -> BoxProviderFuture<'a, Result<SearchResponse>>;

    /// Checks for an original-ledger implementation before admitting native dispatch.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_search(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(false)
    }

    /// Executes without falling back to ordinary, unaccounted execution.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn search_with_native_work<'a>(
        &'a self,
        _: &'a SearchRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (SearchResponse, acyclic_fs::NativeAllocation)>> {
        work.remaining()?;
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "search provider has no native producer",
        })
    }
}

pub(crate) fn error(work: crate::NativeWork<'_>, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        return Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason,
        };
    }
    let _ = work;
    Error::Invalid(reason.into())
}

pub(crate) fn rejection(work: crate::NativeWork<'_>, reason: &'static str) -> Error {
    let _ = work;
    Error::ToolRejected {
        code: crate::wire::ErrorCode::Invalid,
        reason,
    }
}
