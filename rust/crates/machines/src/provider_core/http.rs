//! HTTP transport admission and error classification shared by provider adapters.
use crate::ProviderError;
use reqwest::{
    Client, Method, Response, Url,
    header::{HeaderMap, HeaderValue},
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

#[derive(Clone)]
pub(super) struct Http {
    pub endpoint: Url,
    pub client: Client,
    headers: HeaderMap,
}

pub(super) fn secret(value: &str) -> Result<HeaderValue, ProviderError> {
    let mut value = HeaderValue::from_str(value).map_err(|_| {
        ProviderError::Invalid(
            "provider credential admission failed; recovery: supply a valid header credential"
                .into(),
        )
    })?;
    value.set_sensitive(true);
    Ok(value)
}

pub(super) fn endpoint_url(endpoint: &str) -> Result<Url, ProviderError> {
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

    pub async fn checked(
        response: Result<Response, reqwest::Error>,
    ) -> Result<Response, ProviderError> {
        let response = response.map_err(|error| {
            tracing::warn!(%error, "provider HTTP request failed; recovery: inspect the transport endpoint before a fresh operation");
            ProviderError::Unavailable
        })?;
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
