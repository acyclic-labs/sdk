//! Credential-free, bounded retrieval of one HTTP(S) source, distinct from search.

use crate::{Error, Result};
use acyclic_stream::BoxProviderFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg(all(feature = "search-http", not(target_arch = "wasm32")))]
pub(crate) mod http;
#[cfg(all(feature = "search-http", not(target_arch = "wasm32")))]
pub use http::HttpFetch;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native;
#[cfg(all(
    feature = "filesystem",
    feature = "search-http",
    not(target_arch = "wasm32")
))]
pub(crate) use native::response_bytes;

pub(crate) const MAXIMUM_URL_BYTES: usize = 16_384;

/// One absolute HTTP(S) URL. User information and fragments are not accepted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FetchRequest {
    /// Absolute source URL, at most 16,384 UTF-8 bytes, without user information.
    #[schemars(length(min = 1, max = 16384))]
    pub url: String,
}

impl FetchRequest {
    /// Validates a request file before provider dispatch.
    pub fn validate(&self) -> Result<()> {
        self.validate_with_work(None)
    }

    pub(crate) fn validate_with_work(&self, work: crate::NativeWork<'_>) -> Result<()> {
        #[cfg(all(feature = "search-http", not(target_arch = "wasm32")))]
        {
            http::validate_url(&self.url, work)
        }
        #[cfg(not(all(feature = "search-http", not(target_arch = "wasm32"))))]
        {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(work) = work {
                work.charge(acyclic_fs::WorkCounters {
                    items_examined: self.url.len() as u64,
                    ..Default::default()
                })?;
            }
            validate_url_text(&self.url, work)
        }
    }
}

pub(crate) fn validate_url_text(url: &str, work: crate::NativeWork<'_>) -> Result<()> {
    if url.is_empty()
        || url.len() > MAXIMUM_URL_BYTES
        || url.chars().any(|c| c.is_whitespace() || c.is_control())
        || url.contains(['#', '\\'])
    {
        return Err(rejection(work, "invalid fetch URL"));
    }
    let Some((scheme, rest)) = url.split_once("://") else {
        return Err(rejection(work, "fetch URL must use HTTP or HTTPS"));
    };
    let authority = rest.split(['/', '?']).next().unwrap_or_default();
    if !(scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https"))
        || authority.is_empty()
        || authority.contains('@')
    {
        return Err(rejection(
            work,
            "invalid fetch URL scheme, host, or user information",
        ));
    }
    Ok(())
}

/// Exact UTF-8 response bytes, with original and final source provenance.
/// Content is not a search excerpt, rendered page, or synthesized fallback.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FetchResponse {
    /// Original requested URL, preserved verbatim for source attribution.
    pub source_url: String,
    /// Final absolute source URL after bounded, validated redirects.
    pub final_url: String,
    /// Successful HTTP status returned by the final source.
    pub status: u16,
    /// Final source's Content-Type header, when present.
    pub content_type: Option<String>,
    /// Exact UTF-8 response body, bounded to one MiB by the HTTP adapter.
    pub content: String,
}

/// Replaceable URL retrieval capability. Dropping its future cancels its I/O.
pub trait FetchProvider: acyclic_stream::ProviderPlatform {
    /// Fetches once. Durable replay belongs to the host execution journal.
    fn fetch<'a>(
        &'a self,
        request: &'a FetchRequest,
    ) -> BoxProviderFuture<'a, Result<FetchResponse>>;

    /// Checks for an original-ledger implementation before admitting native dispatch.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_fetch(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(false)
    }

    /// Executes without falling back to ordinary, unaccounted execution.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn fetch_with_native_work<'a>(
        &'a self,
        _: &'a FetchRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FetchResponse, acyclic_fs::NativeAllocation)>> {
        work.remaining()?;
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "fetch provider has no native producer",
        })
    }
}

pub(crate) fn rejection(work: crate::NativeWork<'_>, reason: &'static str) -> Error {
    let _ = work;
    Error::ToolRejected {
        code: crate::wire::ErrorCode::Invalid,
        reason,
    }
}
