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

impl Error {
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
fn loopback(endpoint: &Url) -> bool {
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

impl Client {
    /// Create a client. HTTPS or loopback HTTP is required.
    ///
    /// # Errors
    /// Rejects unsafe endpoints, invalid credentials, or a zero response bound.
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

    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.actors.http.call",
            level = "info",
            skip_all,
            fields(
                route = route,
                http.status = tracing::field::Empty,
                outcome = tracing::field::Empty,
                error.kind = tracing::field::Empty,
            )
        )
    )]
    async fn call<I: Message, O: Message + Default>(
        &self,
        route: &'static str,
        input: &str,
        output: &str,
        request: &I,
    ) -> Result<O, Error> {
        let result = self.exchange(route, input, output, request).await;
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

    async fn exchange<I: Message, O: Message + Default>(
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
            .header(AUTHORIZATION, self.authorization.clone())
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await?;
        let status = response.status();
        #[cfg(not(target_arch = "wasm32"))]
        tracing::Span::current().record("http.status", status.as_u16());
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
