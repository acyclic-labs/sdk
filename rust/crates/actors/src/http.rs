//! Authenticated HTTP client using the canonical descriptor's Protobuf JSON mapping.
use crate::{FILE_DESCRIPTOR_SET, HTTP_ROUTES, wire};
use acyclic_sdk_contract_wire::{BEARER_NO_CRLF, credential};
use futures::StreamExt;
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
        let transport = Transport::builder();
        #[cfg(not(target_arch = "wasm32"))]
        let transport = transport
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(30));
        Ok(Self {
            transport: transport.build()?,
            endpoint,
            token: token.to_owned(),
            maximum: maximum_response_bytes,
            descriptors: DescriptorPool::decode(FILE_DESCRIPTOR_SET)
                .map_err(|_| Error::MalformedResponse)?,
        })
    }

    /// Verify the family identity using an authenticated, non-mutating GET.
    ///
    /// # Errors
    /// Rejects failed authentication, malformed responses, and incompatible identities.
    pub async fn verify_handshake(&self) -> Result<bool, Error> {
        use acyclic_sdk_contract_wire::{BindingFamily, transport_control as control};
        let family = BindingFamily::Actors;
        let version = control::control_protocol_version(family);
        let route = control::handshake_http_route(family.name()).ok_or(Error::InvalidArgument)?;
        let url = self
            .endpoint
            .join(route.trim_start_matches('/'))
            .map_err(|_| Error::InvalidArgument)?;
        let response = self
            .transport
            .get(url.clone())
            .timeout(std::time::Duration::from_secs(10))
            .bearer_auth(&self.token)
            .header("accept", "application/json")
            .send()
            .await?;
        if response.url() != &url {
            return Err(Error::MalformedResponse);
        }
        let status = response.status();
        if matches!(status.as_u16(), 404 | 405) {
            return Ok(false);
        }
        if !status.is_success() {
            return Err(Error::Service {
                status: status.as_u16(),
                detail: None,
            });
        }
        if !response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .split(';')
                    .next()
                    .is_some_and(|kind| kind.trim().eq_ignore_ascii_case("application/json"))
            })
        {
            return Err(Error::MalformedResponse);
        }
        let maximum = self.maximum.min(control::MAXIMUM_HANDSHAKE_RESPONSE_BYTES);
        if response
            .content_length()
            .is_some_and(|length| length > maximum as u64)
        {
            return Err(Error::ResponseTooLarge);
        }
        let mut bytes = Vec::new();
        let mut chunks = response.bytes_stream();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            if chunk.len() > maximum.saturating_sub(bytes.len()) {
                return Err(Error::ResponseTooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        let pool = DescriptorPool::decode(
            acyclic_sdk_contract_wire::protocol::protocol_descriptor().as_slice(),
        )
        .map_err(|_| Error::MalformedResponse)?;
        let descriptor = pool
            .get_message_by_name("acyclic.protocol.v1.HandshakeResponse")
            .ok_or(Error::MalformedResponse)?;
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        let decoded = DynamicMessage::deserialize(descriptor, &mut deserializer)
            .map_err(|_| Error::MalformedResponse)?;
        deserializer.end().map_err(|_| Error::MalformedResponse)?;
        control::validate_handshake_response(
            family,
            version,
            &[control::RequiredCapability {
                name: family.name(),
                version,
            }],
            &decoded.encode_to_vec(),
            maximum,
        )
        .map_err(|_| Error::MalformedResponse)?;
        Ok(true)
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
        let response = self
            .transport
            .post(
                self.endpoint
                    .join(route)
                    .map_err(|_| Error::InvalidArgument)?,
            )
            .timeout(std::time::Duration::from_secs(30))
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
        let mut chunks = response.bytes_stream();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            if chunk.len() > self.maximum.saturating_sub(bytes.len()) {
                return Err(Error::ResponseTooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            let detail = self
                .decode::<wire::Error>("acyclic.actors.v1.Error", &bytes)
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

    /// Execute the canonical `CreateActor` operation.
    ///
    /// # Errors
    /// Returns configuration, transport, decoding, bound, or canonical service errors.
    pub async fn create_actor(
        &self,
        request: &wire::CreateActorRequest,
    ) -> Result<wire::CreateActorResponse, Error> {
        self.call(
            HTTP_ROUTES.first().ok_or(Error::InvalidArgument)?.1,
            "acyclic.actors.v1.CreateActorRequest",
            "acyclic.actors.v1.CreateActorResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `UpdateActor` operation.
    ///
    /// # Errors
    /// Returns configuration, transport, decoding, bound, or canonical service errors.
    pub async fn update_actor(
        &self,
        request: &wire::UpdateActorRequest,
    ) -> Result<wire::UpdateActorResponse, Error> {
        self.call(
            HTTP_ROUTES.get(1).ok_or(Error::InvalidArgument)?.1,
            "acyclic.actors.v1.UpdateActorRequest",
            "acyclic.actors.v1.UpdateActorResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `InspectActor` operation.
    ///
    /// # Errors
    /// Returns configuration, transport, decoding, bound, or canonical service errors.
    pub async fn inspect_actor(
        &self,
        request: &wire::InspectActorRequest,
    ) -> Result<wire::InspectActorResponse, Error> {
        self.call(
            HTTP_ROUTES.get(2).ok_or(Error::InvalidArgument)?.1,
            "acyclic.actors.v1.InspectActorRequest",
            "acyclic.actors.v1.InspectActorResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `AddSubscription` operation.
    ///
    /// # Errors
    /// Returns configuration, transport, decoding, bound, or canonical service errors.
    pub async fn add_subscription(
        &self,
        request: &wire::AddSubscriptionRequest,
    ) -> Result<wire::AddSubscriptionResponse, Error> {
        self.call(
            HTTP_ROUTES.get(3).ok_or(Error::InvalidArgument)?.1,
            "acyclic.actors.v1.AddSubscriptionRequest",
            "acyclic.actors.v1.AddSubscriptionResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `RemoveSubscription` operation.
    ///
    /// # Errors
    /// Returns configuration, transport, decoding, bound, or canonical service errors.
    pub async fn remove_subscription(
        &self,
        request: &wire::RemoveSubscriptionRequest,
    ) -> Result<wire::RemoveSubscriptionResponse, Error> {
        self.call(
            HTTP_ROUTES.get(4).ok_or(Error::InvalidArgument)?.1,
            "acyclic.actors.v1.RemoveSubscriptionRequest",
            "acyclic.actors.v1.RemoveSubscriptionResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `ResumeSubscription` operation.
    ///
    /// # Errors
    /// Returns configuration, transport, decoding, bound, or canonical service errors.
    pub async fn resume_subscription(
        &self,
        request: &wire::ResumeSubscriptionRequest,
    ) -> Result<wire::ResumeSubscriptionResponse, Error> {
        self.call(
            HTTP_ROUTES.get(5).ok_or(Error::InvalidArgument)?.1,
            "acyclic.actors.v1.ResumeSubscriptionRequest",
            "acyclic.actors.v1.ResumeSubscriptionResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `CheckpointActor` operation.
    ///
    /// # Errors
    /// Returns configuration, transport, decoding, bound, or canonical service errors.
    pub async fn checkpoint_actor(
        &self,
        request: &wire::CheckpointActorRequest,
    ) -> Result<wire::CheckpointActorResponse, Error> {
        self.call(
            HTTP_ROUTES.get(6).ok_or(Error::InvalidArgument)?.1,
            "acyclic.actors.v1.CheckpointActorRequest",
            "acyclic.actors.v1.CheckpointActorResponse",
            request,
        )
        .await
    }

    /// Execute the canonical `InvokeActor` operation.
    ///
    /// # Errors
    /// Returns configuration, transport, decoding, bound, or canonical service errors.
    pub async fn invoke_actor(
        &self,
        request: &wire::InvokeActorRequest,
    ) -> Result<wire::InvokeActorResponse, Error> {
        self.call(
            HTTP_ROUTES.get(7).ok_or(Error::InvalidArgument)?.1,
            "acyclic.actors.v1.InvokeActorRequest",
            "acyclic.actors.v1.InvokeActorResponse",
            request,
        )
        .await
    }
}
