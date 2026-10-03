//! Authenticated HTTP client using the canonical descriptor's Protobuf JSON mapping.
use crate::{FILE_DESCRIPTOR_SET, HTTP_ROUTES, wire};
use acyclic_sdk_contract_wire::{BEARER_NO_CRLF, credential};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use reqwest::{Client as Transport, Url};

/// HTTP configuration, encoding, or remote service failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
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
        /// Canonical semantic error detail.
        detail: Option<wire::Error>,
    },
}

/// Typed HTTP operations with bearer authentication and bounded responses.
#[derive(Clone)]
pub struct Client {
    transport: Transport,
    endpoint: Url,
    token: String,
    maximum: usize,
    descriptors: DescriptorPool,
}

impl Client {
    /// Create a client. HTTPS or loopback HTTP is required.
    ///
    /// # Errors
    /// Rejects unsafe endpoints, invalid credentials, or a zero response bound.
    pub fn new(endpoint: &str, token: &str, maximum_response_bytes: usize) -> Result<Self, Error> {
        let mut endpoint = Url::parse(endpoint).map_err(|_| Error::InvalidArgument)?;
        let loopback = matches!(
            endpoint.host_str(),
            Some("localhost" | "127.0.0.1" | "[::1]")
        );
        if !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && loopback)
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !credential::validate(BEARER_NO_CRLF, token)
            || maximum_response_bytes == 0
        {
            return Err(Error::InvalidArgument);
        }
        reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| Error::InvalidArgument)?;
        if !endpoint.path().ends_with('/') {
            endpoint.set_path(&format!("{}/", endpoint.path()));
        }
        Ok(Self {
            transport: Transport::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            endpoint,
            token: token.to_owned(),
            maximum: maximum_response_bytes,
            descriptors: DescriptorPool::decode(FILE_DESCRIPTOR_SET)
                .map_err(|_| Error::MalformedResponse)?,
        })
    }

    async fn call<I: Message, O: Message + Default>(
        &self,
        route: &str,
        input: &str,
        output: &str,
        request: &I,
    ) -> Result<O, Error> {
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
            .bearer_auth(&self.token)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await?;
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|length| length > self.maximum as u64)
        {
            return Err(Error::ResponseTooLarge);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if chunk.len() > self.maximum.saturating_sub(bytes.len()) {
                return Err(Error::ResponseTooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            let detail = self
                .decode::<wire::Error>("acyclic.workers.v1.Error", &bytes)
                .ok()
                .filter(|detail| {
                    wire::ErrorCode::try_from(detail.code)
                        .is_ok_and(|code| code != wire::ErrorCode::Unspecified)
                });
            return Err(Error::Service {
                status: status.as_u16(),
                detail,
            });
        }
        self.decode(output, &bytes)
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
    ///
    /// # Errors
    /// Returns configuration, transport, decoding, bound, or canonical service errors.
    pub async fn publish_version(
        &self,
        request: &wire::PublishVersionRequest,
    ) -> Result<wire::PublishVersionResponse, Error> {
        self.call(
            HTTP_ROUTES.first().ok_or(Error::InvalidArgument)?.1,
            "acyclic.workers.v1.PublishVersionRequest",
            "acyclic.workers.v1.PublishVersionResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `SelectDeployment` operation.
    ///
    /// # Errors
    /// Returns configuration, transport, decoding, bound, or canonical service errors.
    pub async fn select_deployment(
        &self,
        request: &wire::SelectDeploymentRequest,
    ) -> Result<wire::SelectDeploymentResponse, Error> {
        self.call(
            HTTP_ROUTES.get(1).ok_or(Error::InvalidArgument)?.1,
            "acyclic.workers.v1.SelectDeploymentRequest",
            "acyclic.workers.v1.SelectDeploymentResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `SubmitJob` operation.
    ///
    /// # Errors
    /// Returns configuration, transport, decoding, bound, or canonical service errors.
    pub async fn submit_job(
        &self,
        request: &wire::SubmitJobRequest,
    ) -> Result<wire::SubmitJobResponse, Error> {
        self.call(
            HTTP_ROUTES.get(2).ok_or(Error::InvalidArgument)?.1,
            "acyclic.workers.v1.SubmitJobRequest",
            "acyclic.workers.v1.SubmitJobResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `InspectJob` operation.
    ///
    /// # Errors
    /// Returns configuration, transport, decoding, bound, or canonical service errors.
    pub async fn inspect_job(
        &self,
        request: &wire::InspectJobRequest,
    ) -> Result<wire::InspectJobResponse, Error> {
        self.call(
            HTTP_ROUTES.get(3).ok_or(Error::InvalidArgument)?.1,
            "acyclic.workers.v1.InspectJobRequest",
            "acyclic.workers.v1.InspectJobResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `CancelJob` operation.
    ///
    /// # Errors
    /// Returns configuration, transport, decoding, bound, or canonical service errors.
    pub async fn cancel_job(
        &self,
        request: &wire::CancelJobRequest,
    ) -> Result<wire::CancelJobResponse, Error> {
        self.call(
            HTTP_ROUTES.get(4).ok_or(Error::InvalidArgument)?.1,
            "acyclic.workers.v1.CancelJobRequest",
            "acyclic.workers.v1.CancelJobResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `InvokeVersion` operation.
    ///
    /// # Errors
    /// Returns configuration, transport, decoding, bound, or canonical service errors.
    pub async fn invoke_version(
        &self,
        request: &wire::InvokeVersionRequest,
    ) -> Result<wire::InvokeResponse, Error> {
        if request.version_sha256.len() != 32 {
            return Err(Error::InvalidArgument);
        }
        let digest: String = request
            .version_sha256
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let route = HTTP_ROUTES
            .get(5)
            .ok_or(Error::InvalidArgument)?
            .1
            .replace("{sha256hex}", &digest);
        self.call(
            &route,
            "acyclic.workers.v1.InvokeVersionRequest",
            "acyclic.workers.v1.InvokeResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `InvokeDeployment` operation.
    ///
    /// # Errors
    /// Returns configuration, transport, decoding, bound, or canonical service errors.
    pub async fn invoke_deployment(
        &self,
        request: &wire::InvokeDeploymentRequest,
    ) -> Result<wire::InvokeResponse, Error> {
        if request.alias.is_empty()
            || request.alias.len() > 256
            || request.alias == "."
            || request.alias == ".."
            || !request
                .alias
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(Error::InvalidArgument);
        }
        let route = HTTP_ROUTES
            .get(6)
            .ok_or(Error::InvalidArgument)?
            .1
            .replace("{alias}", &request.alias);
        self.call(
            &route,
            "acyclic.workers.v1.InvokeDeploymentRequest",
            "acyclic.workers.v1.InvokeResponse",
            request,
        )
        .await
    }
}
