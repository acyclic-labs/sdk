//! Authenticated HTTP client using the canonical descriptor's Protobuf JSON mapping.
use crate::{FILE_DESCRIPTOR_SET, HTTP_ROUTES, wire};
pub use acyclic_auth::credential::CredentialProvider;
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
    /// The current callback failed; a fixed credential is never substituted.
    #[error("Actors credential renewal failed")]
    Credential(#[from] acyclic_auth::credential::CredentialError),
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
        detail: Option<wire::Error>,
        /// Exact response body, including empty or malformed error payloads.
        raw_details: Vec<u8>,
    },
}

impl Error {
    /// Stable classification for tracing's `error.kind`.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid_argument",
            Self::Credential(_) => "unauthenticated",
            Self::Transport(_) => "transport",
            Self::ResponseTooLarge => "response_too_large",
            Self::MalformedResponse => "malformed_response",
            Self::Service { .. } => "service",
        }
    }
}

fn credential_header(token: &str) -> Result<HeaderValue, Error> {
    if !crate::valid_token(token) {
        return Err(Error::InvalidArgument);
    }
    let mut value =
        HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| Error::InvalidArgument)?;
    value.set_sensitive(true);
    Ok(value)
}

/// Configures bounded authenticated HTTP without redirects.
fn configure(
    endpoint: &str,
    token: Option<&str>,
    maximum: usize,
) -> Result<(Transport, Url, Option<HeaderValue>), Error> {
    let mut endpoint = Url::parse(endpoint).map_err(|_| Error::InvalidArgument)?;
    let loopback = endpoint.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && loopback)
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || maximum == 0
    {
        return Err(Error::InvalidArgument);
    }
    let authorization = token.map(credential_header).transpose()?;
    if !endpoint.path().ends_with('/') {
        endpoint.set_path(&format!("{}/", endpoint.path()));
    }
    let transport = Transport::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    Ok((transport, endpoint, authorization))
}

/// Encodes a request through its canonical JSON descriptor.
fn encode(
    descriptors: &DescriptorPool,
    name: &str,
    request: &impl Message,
) -> Result<Vec<u8>, Error> {
    let descriptor = descriptors
        .get_message_by_name(name)
        .ok_or(Error::MalformedResponse)?;
    let message = DynamicMessage::decode(descriptor, request.encode_to_vec().as_slice())
        .map_err(|_| Error::InvalidArgument)?;
    serde_json::to_vec(&message).map_err(|_| Error::InvalidArgument)
}

/// Decodes a complete response through its canonical JSON descriptor.
fn decode<O: Message + Default>(
    descriptors: &DescriptorPool,
    name: &str,
    bytes: &[u8],
) -> Result<O, Error> {
    let descriptor = descriptors
        .get_message_by_name(name)
        .ok_or(Error::MalformedResponse)?;
    let mut json = serde_json::Deserializer::from_slice(bytes);
    let message =
        DynamicMessage::deserialize(descriptor, &mut json).map_err(|_| Error::MalformedResponse)?;
    json.end().map_err(|_| Error::MalformedResponse)?;
    message.transcode_to().map_err(|_| Error::MalformedResponse)
}

/// Collects a response within `maximum`, returning `None` when it exceeds the bound.
async fn response_body(
    response: &mut reqwest::Response,
    maximum: usize,
) -> Result<Option<Vec<u8>>, reqwest::Error> {
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

/// Typed HTTP operations with bearer authentication and bounded responses.
#[derive(Clone)]
pub struct Client {
    transport: Transport,
    endpoint: Url,
    authorization: Option<HeaderValue>,
    credential: Option<CredentialProvider>,
    maximum: usize,
    descriptors: DescriptorPool,
}

#[cfg(test)]
mod credential_tests;

macro_rules! operations {
    ($(($method:ident, $index:literal, $input:ident, $output:ident)),+ $(,)?) => {
        $(/// Executes the canonical HTTP operation.
        pub async fn $method(&self, request: &wire::$input) -> Result<wire::$output, Error> {
            let route = HTTP_ROUTES.get($index).ok_or(Error::InvalidArgument)?.1;
            self.call(
                route,
                concat!("acyclic.actors.v1.", stringify!($input)),
                concat!("acyclic.actors.v1.", stringify!($output)),
                request,
            ).await
        })+
    };
}

impl Client {
    /// Create a client. HTTPS or loopback HTTP is required.
    pub fn new(endpoint: &str, token: &str, maximum_response_bytes: usize) -> Result<Self, Error> {
        Self::build(endpoint, Some(token), None, maximum_response_bytes)
    }

    /// Resolve the current callback before each operation without caching its result.
    pub fn with_credential_provider(
        endpoint: &str,
        credential: CredentialProvider,
        maximum: usize,
    ) -> Result<Self, Error> {
        Self::build(endpoint, None, Some(credential), maximum)
    }

    fn build(
        endpoint: &str,
        token: Option<&str>,
        credential: Option<CredentialProvider>,
        maximum_response_bytes: usize,
    ) -> Result<Self, Error> {
        let (transport, endpoint, authorization) =
            configure(endpoint, token, maximum_response_bytes)?;
        Ok(Self {
            transport,
            endpoint,
            authorization,
            credential,
            maximum: maximum_response_bytes,
            descriptors: DescriptorPool::decode(FILE_DESCRIPTOR_SET)
                .map_err(|_| Error::MalformedResponse)?,
        })
    }

    async fn authorization(&self) -> Result<HeaderValue, Error> {
        if let Some(provider) = &self.credential {
            let token = provider.resolve().await?;
            credential_header(&token)
        } else {
            self.authorization.clone().ok_or(Error::InvalidArgument)
        }
    }

    #[tracing::instrument(
            name = "acyclic.actors.http.call",
            level = "info",
            skip_all,
            fields(
                route = route,
                http.status = tracing::field::Empty,
                outcome = tracing::field::Empty,
                error.kind = tracing::field::Empty,
            )
    )]
    async fn call<I: Message, O: Message + Default>(
        &self,
        route: &'static str,
        input: &str,
        output: &str,
        request: &I,
    ) -> Result<O, Error> {
        let result = async {
            let body = encode(&self.descriptors, input, request)?;
            let mut response = self
                .transport
                .post(
                    self.endpoint
                        .join(route)
                        .map_err(|_| Error::InvalidArgument)?,
                )
                .header(AUTHORIZATION, self.authorization().await?)
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await?;
            let status = response.status();
            tracing::Span::current().record("http.status", status.as_u16());
            let bytes = response_body(&mut response, self.maximum)
                .await?
                .ok_or(Error::ResponseTooLarge)?;
            if !status.is_success() {
                let detail =
                    decode::<wire::Error>(&self.descriptors, "acyclic.actors.v1.Error", &bytes)
                        .ok()
                        .filter(|detail| {
                            wire::ErrorCode::try_from(detail.code)
                                .is_ok_and(|code| code != wire::ErrorCode::Unspecified)
                        });
                return Err(Error::Service {
                    status: status.as_u16(),
                    detail,
                    raw_details: bytes,
                });
            }
            decode(&self.descriptors, output, &bytes)
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

    operations! {
        (create_actor, 0, CreateActorRequest, CreateActorResponse),
        (update_actor, 1, UpdateActorRequest, UpdateActorResponse),
        (inspect_actor, 2, InspectActorRequest, InspectActorResponse),
        (add_subscription, 3, AddSubscriptionRequest, AddSubscriptionResponse),
        (remove_subscription, 4, RemoveSubscriptionRequest, RemoveSubscriptionResponse),
        (resume_subscription, 5, ResumeSubscriptionRequest, ResumeSubscriptionResponse),
        (checkpoint_actor, 6, CheckpointActorRequest, CheckpointActorResponse),
        (invoke_actor, 7, InvokeActorRequest, InvokeActorResponse),
        (delete_actor, 8, DeleteActorRequest, DeleteActorResponse),
    }
}
