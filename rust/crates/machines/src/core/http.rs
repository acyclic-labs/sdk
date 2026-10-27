//! HTTP transport admission and error classification shared by provider adapters.
use crate::ProviderError;
use reqwest::{
    Client, Method, Response, Url,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use std::time::Duration;

/// Shared single-dispatch transport. Neither redirects nor protocol NACKs may
/// silently redispatch a provider mutation. Adapters select request deadlines.
pub fn http_transport_builder() -> reqwest::ClientBuilder {
    Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
}

/// Dispatches exactly once; a lost response remains an unknown mutation outcome.
pub async fn dispatch(request: reqwest::RequestBuilder) -> Result<Response, ProviderError> {
    request.send().await.map_err(transport_error)
}

fn transport_error(error: reqwest::Error) -> ProviderError {
    tracing::warn!(
        timeout = error.is_timeout(),
        connect = error.is_connect(),
        "provider HTTP transport failed; recovery: inspect the endpoint before a fresh operation"
    );
    ProviderError::Unavailable
}

/// Shared body ownership and call outcome accounting. Status meaning belongs to
/// the wire adapter, including native definite-refusal details used by fork.
pub async fn send_body<T>(
    request: reqwest::RequestBuilder,
    decode: impl FnOnce(Vec<u8>) -> Result<T, ProviderError> + Send,
    classify: impl FnOnce(reqwest::StatusCode, &str) -> ProviderError + Send,
) -> Result<T, ProviderError> {
    let result = async {
        let response = dispatch(request).await?;
        let status = response.status();
        tracing::Span::current().record("http.status", status.as_u16());
        let body = response.bytes().await.map_err(transport_error)?;
        if status.is_success() {
            decode(body.into())
        } else {
            Err(classify(status, &String::from_utf8_lossy(&body)))
        }
    }
    .await;
    let span = tracing::Span::current();
    match &result {
        Ok(_) => span.record("outcome", "ok"),
        Err(error) => span
            .record("outcome", "err")
            .record("error.kind", error.kind()),
    };
    result
}

#[derive(Clone)]
pub struct Http {
    pub endpoint: Url,
    pub client: Client,
    headers: HeaderMap,
}

pub fn secret(value: &str) -> Result<HeaderValue, ProviderError> {
    let mut value = HeaderValue::from_str(value).map_err(|_| {
        ProviderError::Invalid(
            "provider credential admission failed; recovery: supply a valid header credential"
                .into(),
        )
    })?;
    value.set_sensitive(true);
    Ok(value)
}

/// Credentials are validated and marked sensitive once for HTTP and RPC alike.
/// Names are static adapter protocol constants, never provider response input.
pub fn credential_headers<'a>(
    values: impl IntoIterator<Item = (&'static str, &'a str)>,
) -> Result<HeaderMap, ProviderError> {
    values
        .into_iter()
        .map(|(name, value)| Ok((HeaderName::from_static(name), secret(value)?)))
        .collect()
}

pub fn endpoint_url(endpoint: &str) -> Result<Url, ProviderError> {
    let endpoint = Url::parse(endpoint).map_err(|_| ProviderError::Invalid(
            "provider endpoint parsing failed; recovery: configure an absolute HTTPS URL or loopback HTTP URL".into()))?;
    if (endpoint.scheme() != "https"
        && !(endpoint.scheme() == "http"
            && matches!(
                endpoint.host_str(),
                Some("localhost" | "127.0.0.1" | "[::1]")
            )))
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(ProviderError::Invalid("provider endpoint admission failed; recovery: use HTTPS or loopback HTTP without URL credentials, query or fragment".into()));
    }
    Ok(endpoint)
}

impl Http {
    pub fn new(endpoint: &str, headers: HeaderMap) -> Result<Self, ProviderError> {
        let endpoint = endpoint_url(endpoint)?;
        let client = http_transport_builder()
            .build()
            .map_err(|_| ProviderError::Unavailable)?;
        Ok(Self {
            endpoint,
            client,
            headers,
        })
    }

    pub fn request(
        &self,
        method: Method,
        path: &[&str],
    ) -> Result<reqwest::RequestBuilder, ProviderError> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut().map_err(|()| ProviderError::Invalid(
            "provider URL path admission failed; recovery: configure a hierarchical HTTP URL".into()))?
            .pop_if_empty().extend(path);
        Ok(self
            .client
            .request(method, url)
            .headers(self.headers.clone()))
    }

    /// One status check and body decode. Unknown mutation outcomes never retry.
    pub async fn json<T: serde::de::DeserializeOwned>(
        request: reqwest::RequestBuilder,
    ) -> Result<T, ProviderError> {
        Self::checked(dispatch(request).await)
            .await?
            .json()
            .await
            .map_err(|_| ProviderError::Unavailable)
    }

    pub async fn bytes(request: reqwest::RequestBuilder) -> Result<Vec<u8>, ProviderError> {
        Self::checked(dispatch(request).await)
            .await?
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|_| ProviderError::Unavailable)
    }

    pub async fn empty(request: reqwest::RequestBuilder) -> Result<(), ProviderError> {
        Self::checked(dispatch(request).await).await.map(|_| ())
    }

    pub async fn json_stream<T: serde::de::DeserializeOwned + Send + 'static>(
        request: reqwest::RequestBuilder,
        framing: super::Framing,
    ) -> Result<futures::stream::BoxStream<'static, Result<T, ProviderError>>, ProviderError> {
        let response = Self::checked(dispatch(request).await).await?;
        Ok(super::framing::json_frames(
            response.bytes_stream(),
            framing,
        ))
    }

    pub async fn connect<T: serde::de::DeserializeOwned + Send + 'static>(
        request: reqwest::RequestBuilder,
        message: &serde_json::Value,
    ) -> Result<futures::stream::BoxStream<'static, Result<T, ProviderError>>, ProviderError> {
        Self::json_stream(
            request
                .header("content-type", "application/connect+json")
                .header("connect-protocol-version", "1")
                .body(super::connect_message(message)?),
            super::Framing::Connect,
        )
        .await
    }

    pub async fn checked(
        response: Result<Response, ProviderError>,
    ) -> Result<Response, ProviderError> {
        let response = response?;
        match response.status().as_u16() {
            200..=299 => Ok(response),
            404 => Err(ProviderError::NotFound("provider resource".into())),
            409 => Err(ProviderError::Conflict("provider state conflict; recovery: inspect the resource before a new mutation".into())),
            400 | 401 | 403 | 422 => Err(ProviderError::Rejected("provider request rejected; recovery: inspect transport configuration and access policy".into())),
            _ => {
                tracing::warn!(status = response.status().as_u16(), url = %response.url(), "provider HTTP response failed; recovery: inspect provider availability before a fresh operation");
                Err(ProviderError::Unavailable)
            },
        }
    }
}
