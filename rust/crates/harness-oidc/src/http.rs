//! Native JWKS transport: one GET and a streamed body. Policy is in the core.
use super::{JwksSource, Result, refused};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use bytes::Bytes;
use futures::StreamExt as _;

/// Native HTTPS/explicit loopback source. Redirects and transport retries are
/// disabled; the shared refresher owns all bounds, timing and key replacement.
pub struct HttpSource(reqwest::Client);

impl HttpSource {
    /// Creates a pooled transport without loading identity credentials.
    pub fn new() -> Result<Self> {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map(Self)
            .map_err(|_| refused("OIDC HTTP transport initialization failed"))
    }
}

impl JwksSource for HttpSource {
    fn fetch<'a>(
        &'a self,
        uri: &'a str,
    ) -> BoxProviderFuture<'a, Result<BoxProviderStream<'static, Result<Bytes>>>> {
        Box::pin(async move {
            let response = self
                .0
                .get(uri)
                .send()
                .await
                .map_err(|_| refused("OIDC JWKS GET failed"))?;
            if response.status() != reqwest::StatusCode::OK {
                return Err(refused("OIDC JWKS GET did not return 200"));
            }
            let body = response
                .bytes_stream()
                .map(|chunk| chunk.map_err(|_| refused("OIDC JWKS response body failed")));
            Ok(Box::pin(body) as BoxProviderStream<'static, Result<Bytes>>)
        })
    }
}
