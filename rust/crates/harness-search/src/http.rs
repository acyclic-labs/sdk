use super::{SearchProvider, SearchRequest, SearchResponse};
use crate::{Error, Result};
use acyclic_stream::BoxProviderFuture;
use reqwest::{Url, header::HeaderValue};
use serde::Deserialize;
use std::time::Duration;
mod codec;
pub(crate) mod transport;

const MAXIMUM_RESPONSE_BYTES: usize = 1_048_576;

/// Exa's typed POST /search adapter.
pub struct Exa(HttpSearch);

/// TinyFish's typed GET / adapter.
pub struct TinyFish(HttpSearch);

struct HttpSearch {
    tls: Option<std::sync::Arc<rustls::ClientConfig>>,
    endpoint: Url,
    key: HeaderValue,
}

impl HttpSearch {
    fn new(key: &str, endpoint: &str) -> Result<Self> {
        if key.trim().is_empty() {
            return Err(Error::Invalid("search API key is empty".into()));
        }
        let mut key = HeaderValue::from_str(key)
            .map_err(|_| Error::Invalid("invalid search API key".into()))?;
        key.set_sensitive(true);
        let endpoint =
            Url::parse(endpoint).map_err(|_| Error::Invalid("invalid search endpoint".into()))?;
        if !matches!(endpoint.scheme(), "https" | "http")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(Error::Invalid("invalid search endpoint".into()));
        }
        let tls = transport::tls(&endpoint)?;
        Ok(Self { tls, endpoint, key })
    }

    async fn send(
        &self,
        method: http::Method,
        endpoint: &Url,
        body: bytes::Bytes,
        work: crate::NativeWork<'_>,
    ) -> Result<(Vec<u8>, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.admit(acyclic_fs::WorkCounters {
                source_bytes_read: 1,
                ..Default::default()
            })?;
            work.charge(acyclic_fs::WorkCounters {
                backend_read_operations: 1,
                bytes_copied: self.key.as_bytes().len() as u64,
                items_examined: 1,
                ..Default::default()
            })?;
        }
        let mut failure = None;
        let exchange = transport::send(
            method,
            endpoint,
            &self.key,
            body,
            self.tls.as_ref(),
            work,
            &mut failure,
        );
        let result = await_io(
            tokio::time::timeout(Duration::from_secs(30), exchange),
            work,
        )
        .await?;
        if let Some(error) = failure {
            return Err(error);
        }
        result.map_err(|_| transport_error(work, "search HTTP request timed out"))?
    }
}

impl Exa {
    /// Binds a host-resolved key. It is never written to task files.
    pub fn new(key: &str) -> Result<Self> {
        Self::with_endpoint(key, "https://api.exa.ai/search")
    }

    /// Uses an explicit endpoint, for private gateways and local protocol tests.
    pub fn with_endpoint(key: &str, endpoint: &str) -> Result<Self> {
        HttpSearch::new(key, endpoint).map(Self)
    }

    async fn run(
        &self,
        request: &SearchRequest,
        work: crate::NativeWork<'_>,
    ) -> Result<(SearchResponse, crate::NativeCustody)> {
        request.validate_with_work(work)?;
        let _endpoint = crate::tool::edit::allocation(work, self.0.endpoint.as_str().len())?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.charge(acyclic_fs::WorkCounters {
                allocation_operations: 1,
                ..Default::default()
            })?;
        }
        crate::tool::edit::charge(&_endpoint, self.0.endpoint.as_str().len() as u64, 1)?;
        let body = ExaQuery {
            query: &request.query,
            num_results: request.max_results,
            r#type: "auto",
            contents: Contents { highlights: true },
        };
        let body = request_body(&body, work)?;
        let bytes = self
            .0
            .send(http::Method::POST, &self.0.endpoint, body, work)
            .await?;
        codec::exa(&bytes.0, request.max_results, work)
    }
}

impl TinyFish {
    /// Binds a host-resolved key.
    pub fn new(key: &str) -> Result<Self> {
        Self::with_endpoint(key, "https://api.search.tinyfish.ai/")
    }

    /// Uses an explicit endpoint, for private gateways and local protocol tests.
    pub fn with_endpoint(key: &str, endpoint: &str) -> Result<Self> {
        HttpSearch::new(key, endpoint).map(Self)
    }

    async fn run(
        &self,
        request: &SearchRequest,
        work: crate::NativeWork<'_>,
    ) -> Result<(SearchResponse, crate::NativeCustody)> {
        request.validate_with_work(work)?;
        // Reserve both old and replacement URL buffers while the URL serializer
        // grows its percent-encoded query. This remains separate from response IO.
        let _url = crate::tool::edit::allocation(
            work,
            (self.0.endpoint.as_str().len() + 7 + request.query.len() * 3) * 3,
        )?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            // Cloning and geometric growth of the serializer's private String.
            let bound = self.0.endpoint.as_str().len() + 7 + request.query.len() * 3;
            work.charge(acyclic_fs::WorkCounters {
                allocation_operations: u64::from(usize::BITS - bound.leading_zeros()) + 1,
                ..Default::default()
            })?;
        }
        crate::tool::edit::charge(
            &_url,
            (self.0.endpoint.as_str().len() + 7 + request.query.len() * 3) as u64,
            request.query.len() as u64,
        )?;
        let mut endpoint = self.0.endpoint.clone();
        endpoint
            .query_pairs_mut()
            .append_pair("query", &request.query);
        let bytes = self
            .0
            .send(http::Method::GET, &endpoint, bytes::Bytes::new(), work)
            .await?;
        codec::tinyfish(&bytes.0, request.max_results, work)
    }
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct ExaQuery<'a> {
    query: &'a str,
    num_results: u32,
    r#type: &'static str,
    contents: Contents,
}
#[derive(serde::Serialize)]
struct Contents {
    highlights: bool,
}

fn request_body(
    value: &impl serde::Serialize,
    work: crate::NativeWork<'_>,
) -> Result<bytes::Bytes> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        let bytes = io.encode(value, MAXIMUM_RESPONSE_BYTES as u64, false)?;
        return Ok(bytes::Bytes::from_owner(bytes));
    }
    let _ = work;
    serde_json::to_vec(value)
        .map(bytes::Bytes::from)
        .map_err(|_| super::error(work, "cannot encode search request"))
}

async fn await_io<F: std::future::Future>(
    future: F,
    work: crate::NativeWork<'_>,
) -> Result<F::Output> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.remaining()?;
        return tokio::select! {
            biased;
            _ = work.cancellation().cancelled() => Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into()),
            result = future => Ok(result),
        };
    }
    let _ = work;
    Ok(future.await)
}

fn transport_error(work: crate::NativeWork<'_>, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        return Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason,
        };
    }
    let _ = work;
    Error::Storage(reason.into())
}

#[derive(Deserialize)]
struct ExaResponse {
    results: Vec<ExaResult>,
}

#[derive(Deserialize)]
struct ExaResult {
    url: String,
    title: Option<String>,
    #[serde(default)]
    highlights: Vec<String>,
}

impl SearchProvider for Exa {
    fn search<'a>(
        &'a self,
        request: &'a SearchRequest,
    ) -> BoxProviderFuture<'a, Result<SearchResponse>> {
        Box::pin(async move { self.run(request, None).await.map(|r| r.0) })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_search(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(true)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn search_with_native_work<'a>(
        &'a self,
        request: &'a SearchRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (SearchResponse, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let (response, held) = self.run(request, Some(work)).await?;
            Ok((
                response,
                held.ok_or_else(|| {
                    super::error(Some(work), "native search response lacks custody")
                })?,
            ))
        })
    }
}

#[derive(Deserialize)]
struct TinyFishResponse {
    results: Vec<TinyFishResult>,
}

#[derive(Deserialize)]
struct TinyFishResult {
    url: String,
    title: String,
    snippet: String,
}

/// Exercises the actual response codec without credentials or network I/O.
#[cfg(feature = "test-support")]
pub fn decode_search_http_response(
    bytes: &[u8],
    maximum: u32,
    exa: bool,
    work: crate::NativeWork<'_>,
) -> Result<(SearchResponse, crate::NativeCustody)> {
    if exa {
        codec::exa(bytes, maximum, work)
    } else {
        codec::tinyfish(bytes, maximum, work)
    }
}

impl SearchProvider for TinyFish {
    fn search<'a>(
        &'a self,
        request: &'a SearchRequest,
    ) -> BoxProviderFuture<'a, Result<SearchResponse>> {
        Box::pin(async move { self.run(request, None).await.map(|r| r.0) })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_search(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(true)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn search_with_native_work<'a>(
        &'a self,
        request: &'a SearchRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (SearchResponse, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let (response, held) = self.run(request, Some(work)).await?;
            Ok((
                response,
                held.ok_or_else(|| {
                    super::error(Some(work), "native search response lacks custody")
                })?,
            ))
        })
    }
}

#[cfg(test)]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native_tests;
#[cfg(test)]
mod tests;
