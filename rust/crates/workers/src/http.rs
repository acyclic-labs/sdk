//! Authenticated HTTP client using the canonical descriptor's Protobuf JSON mapping.
use crate::{FILE_DESCRIPTOR_SET, HTTP_ROUTES, wire};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use reqwest::{
    Client as Transport, Url,
    header::{AUTHORIZATION, HeaderValue},
};

/// HTTP configuration, encoding, or remote service failure.
#[derive(Debug, thiserror::Error)]
pub enum Error<E = wire::Error> {
    /// Invalid endpoint, credential, bound, or request path.
    #[error("invalid HTTP client configuration or request")]
    InvalidArgument,
    /// Network failure.
    #[error(transparent)]
    Transport(#[from] reqwest::Error),
    /// Response exceeded the configured bound.
    #[error("HTTP response exceeds configured bound")]
    ResponseTooLarge,
    /// Malformed canonical response.
    #[error("malformed Protobuf JSON response")]
    MalformedResponse,
    /// Service rejected the request, with canonical detail when available.
    #[error("HTTP service returned status {status}")]
    Service {
        /// HTTP response status.
        status: u16,
        /// Decoded wire error, including service codes unknown to this SDK.
        detail: Option<E>,
        /// Exact response body, including empty or malformed error payloads.
        raw_details: Vec<u8>,
    },
}

impl<E> Error<E> {
    /// Stable classification for tracing's `error.kind`.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid_argument",
            Self::Transport(_) => "transport",
            Self::ResponseTooLarge => "response_too_large",
            Self::MalformedResponse => "malformed_response",
            Self::Service { .. } => "service",
        }
    }
}

/// Plain HTTP is admitted only for `localhost` and loopback IP literals.
#[doc(hidden)]
pub fn loopback(endpoint: &Url) -> bool {
    endpoint.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    })
}

/// Typed HTTP operations with bearer authentication and bounded responses.
#[derive(Clone)]
pub struct Client {
    transport: Transport,
    endpoint: Url,
    authorization: HeaderValue,
    maximum: usize,
    descriptors: DescriptorPool,
}

/// Collects a response within `maximum`, returning `None` when it exceeds the bound.
#[doc(hidden)]
pub async fn response_body(response: &mut reqwest::Response, maximum: usize) -> Result<Option<Vec<u8>>, reqwest::Error> {
    if response
        .content_length()
        .is_some_and(|length| length > maximum as u64)
    {
        return Ok(None);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if chunk.len() > maximum.saturating_sub(bytes.len()) {
            return Ok(None);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(Some(bytes))
}

impl Client {
    /// Create a client. HTTPS or loopback HTTP is required.
    pub fn new(endpoint: &str, token: &str, maximum_response_bytes: usize) -> Result<Self, Error> {
        let mut endpoint = Url::parse(endpoint).map_err(|_| Error::InvalidArgument)?;
        if !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && loopback(&endpoint))
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !crate::valid_token(token)
            || maximum_response_bytes == 0
        {
            return Err(Error::InvalidArgument);
        }
        let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| Error::InvalidArgument)?;
        authorization.set_sensitive(true);
        if !endpoint.path().ends_with('/') {
            endpoint.set_path(&format!("{}/", endpoint.path()));
        }
        Ok(Self {
            transport: Transport::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            endpoint,
            authorization,
            maximum: maximum_response_bytes,
            descriptors: DescriptorPool::decode(FILE_DESCRIPTOR_SET)
                .map_err(|_| Error::MalformedResponse)?,
        })
    }

    /// Calls `route`, an instance of the static `template` that spans record.
    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.workers.http.call",
            level = "info",
            skip_all,
            fields(
                route = template,
                http.status = tracing::field::Empty,
                outcome = tracing::field::Empty,
                error.kind = tracing::field::Empty,
            )
        )
    )]
    async fn call<I: Message, O: Message + Default>(
        &self,
        template: &'static str,
        route: &str,
        input: &str,
        output: &str,
        request: &I,
    ) -> Result<O, Error> {
        let result = async {
            let descriptor = self
                .descriptors
                .get_message_by_name(input)
                .ok_or(Error::MalformedResponse)?;
            let message = DynamicMessage::decode(descriptor, request.encode_to_vec().as_slice())
                .map_err(|_| Error::InvalidArgument)?;
            let body = serde_json::to_vec(&message).map_err(|_| Error::InvalidArgument)?;
            let mut response = self
                .transport
                .post(
                    self.endpoint
                        .join(route)
                        .map_err(|_| Error::InvalidArgument)?,
                )
                .header(AUTHORIZATION, self.authorization.clone())
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await?;
            let status = response.status();
            #[cfg(not(target_arch = "wasm32"))]
            tracing::Span::current().record("http.status", status.as_u16());
            let bytes = response_body(&mut response, self.maximum).await?.ok_or(Error::ResponseTooLarge)?;
            if !status.is_success() {
                let detail = self
                    .decode::<wire::Error>("acyclic.workers.v1.Error", &bytes)
                    .ok()
                    .filter(|detail| detail.code != wire::ErrorCode::Unspecified as i32);
                return Err(Error::Service {
                    status: status.as_u16(),
                    detail,
                    raw_details: bytes,
                });
            }
            self.decode(output, &bytes)
        }.await;
        #[cfg(not(target_arch = "wasm32"))]
        {
            let span = tracing::Span::current();
            match &result {
                Ok(_) => span.record("outcome", "ok"),
                Err(error) => span
                    .record("outcome", "err")
                    .record("error.kind", error.kind()),
            };
        }
        result
    }

    fn decode<O: Message + Default>(&self, name: &str, bytes: &[u8]) -> Result<O, Error> {
        let descriptor = self
            .descriptors
            .get_message_by_name(name)
            .ok_or(Error::MalformedResponse)?;
        let mut json = serde_json::Deserializer::from_slice(bytes);
        let message = DynamicMessage::deserialize(descriptor, &mut json)
            .map_err(|_| Error::MalformedResponse)?;
        json.end().map_err(|_| Error::MalformedResponse)?;
        message.transcode_to().map_err(|_| Error::MalformedResponse)
    }

    /// Execute the canonical `PublishVersion` operation.
    pub async fn publish_version(
        &self,
        request: &wire::PublishVersionRequest,
    ) -> Result<wire::PublishVersionResponse, Error> {
        let route = HTTP_ROUTES.first().ok_or(Error::InvalidArgument)?.1;
        self.call(
            route, route,
            "acyclic.workers.v1.PublishVersionRequest",
            "acyclic.workers.v1.PublishVersionResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `SelectDeployment` operation.
    pub async fn select_deployment(
        &self,
        request: &wire::SelectDeploymentRequest,
    ) -> Result<wire::SelectDeploymentResponse, Error> {
        let route = HTTP_ROUTES.get(1).ok_or(Error::InvalidArgument)?.1;
        self.call(
            route, route,
            "acyclic.workers.v1.SelectDeploymentRequest",
            "acyclic.workers.v1.SelectDeploymentResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `SubmitJob` operation.
    pub async fn submit_job(
        &self,
        request: &wire::SubmitJobRequest,
    ) -> Result<wire::SubmitJobResponse, Error> {
        let route = HTTP_ROUTES.get(2).ok_or(Error::InvalidArgument)?.1;
        self.call(
            route, route,
            "acyclic.workers.v1.SubmitJobRequest",
            "acyclic.workers.v1.SubmitJobResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `InspectJob` operation.
    pub async fn inspect_job(
        &self,
        request: &wire::InspectJobRequest,
    ) -> Result<wire::InspectJobResponse, Error> {
        let route = HTTP_ROUTES.get(3).ok_or(Error::InvalidArgument)?.1;
        self.call(
            route, route,
            "acyclic.workers.v1.InspectJobRequest",
            "acyclic.workers.v1.InspectJobResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `CancelJob` operation.
    pub async fn cancel_job(
        &self,
        request: &wire::CancelJobRequest,
    ) -> Result<wire::CancelJobResponse, Error> {
        let route = HTTP_ROUTES.get(4).ok_or(Error::InvalidArgument)?.1;
        self.call(
            route, route,
            "acyclic.workers.v1.CancelJobRequest",
            "acyclic.workers.v1.CancelJobResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `InvokeVersion` operation.
    pub async fn invoke_version(
        &self,
        request: &wire::InvokeVersionRequest,
    ) -> Result<wire::InvokeResponse, Error> {
        crate::validate_invoke_version(request).map_err(|_| Error::InvalidArgument)?;
        let digest: String = request
            .version_sha256
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let template = HTTP_ROUTES.get(5).ok_or(Error::InvalidArgument)?.1;
        self.call(
            template,
            &template.replace("{sha256hex}", &digest),
            "acyclic.workers.v1.InvokeVersionRequest",
            "acyclic.workers.v1.InvokeResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `InvokeDeployment` operation.
    pub async fn invoke_deployment(
        &self,
        request: &wire::InvokeDeploymentRequest,
    ) -> Result<wire::InvokeResponse, Error> {
        crate::validate_invoke_deployment(request).map_err(|_| Error::InvalidArgument)?;
        let template = HTTP_ROUTES.get(6).ok_or(Error::InvalidArgument)?.1;
        self.call(
            template,
            &template.replace("{alias}", &request.alias),
            "acyclic.workers.v1.InvokeDeploymentRequest",
            "acyclic.workers.v1.InvokeResponse",
            request,
        )
        .await
    }
}
