//! Authenticated HTTP client using the canonical descriptor's Protobuf JSON mapping.
use crate::{FILE_DESCRIPTOR_SET, HTTP_ROUTES, wire};
use acyclic_sdk_contract_wire::{BEARER_NO_CRLF, credential};
use futures::FutureExt;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use reqwest::{Client as Transport, Url};
use std::sync::{Arc, Mutex};

#[cfg(not(target_arch = "wasm32"))]
type SharedHandshake =
    futures::future::Shared<futures::future::BoxFuture<'static, Result<bool, Arc<Error>>>>;
#[cfg(target_arch = "wasm32")]
type SharedHandshake = futures::future::Shared<
    futures::future::LocalBoxFuture<'static, Result<bool, Arc<Error>>>,
>;

#[derive(Default)]
struct HandshakeSlot {
    next_generation: u64,
    current: Option<(u64, SharedHandshake)>,
}

fn clear_failed_handshake(slot: &mut HandshakeSlot, generation: u64) {
    if slot
        .current
        .as_ref()
        .is_some_and(|(current, _)| *current == generation)
    {
        slot.current = None;
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn share_handshake<F>(future: F) -> SharedHandshake
where
    F: std::future::Future<Output = Result<bool, Arc<Error>>> + Send + 'static,
{
    future.boxed().shared()
}

#[cfg(target_arch = "wasm32")]
fn share_handshake<F>(future: F) -> SharedHandshake
where
    F: std::future::Future<Output = Result<bool, Arc<Error>>> + 'static,
{
    future.boxed_local().shared()
}

/// Error reported by the Actors HTTP client while configuring, encoding, or
/// sending a canonical request.
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    /// The endpoint, credential, response bound, request path, or request body is invalid.
    #[error("invalid HTTP client configuration or request")]
    InvalidArgument,
    /// Network failure.
    #[error(transparent)]
    Transport(Arc<reqwest::Error>),
    /// The response exceeded the configured byte bound before decoding.
    #[error("HTTP response exceeds configured bound")]
    ResponseTooLarge,
    /// The response content type or Protobuf JSON body is malformed.
    #[error("malformed Protobuf JSON response")]
    MalformedResponse,
    /// The service rejected the request, with Rust-owned detail when available.
    #[error("HTTP service returned status {status}")]
    Service {
        /// HTTP response status.
        status: u16,
        /// Rust-owned semantic error detail decoded from the response body.
        detail: Option<wire::Error>,
    },
}

impl From<reqwest::Error> for Error {
    fn from(error: reqwest::Error) -> Self {
        Self::Transport(Arc::new(error))
    }
}

/// Typed Actors v1 HTTP operations with bearer authentication and bounded responses.
///
/// Requests and responses are encoded from [`crate::FILE_DESCRIPTOR_SET`], so
/// the HTTP transport uses the same field names, presence rules, and semantic
/// error values as the gRPC transport.
#[derive(Clone)]
pub struct Client {
    transport: Transport,
    endpoint: Url,
    token: String,
    maximum: usize,
    descriptors: DescriptorPool,
    handshake: Arc<Mutex<HandshakeSlot>>,
}

impl Client {
    /// Create a client for the canonical Actors HTTP service.
    ///
    /// HTTPS is accepted for any host; HTTP is accepted only for localhost,
    /// `127.0.0.1`, or `[::1]`. The endpoint has no user information, query,
    /// or fragment, and the bearer token must be valid HTTP metadata.
    ///
    /// # Errors
    /// Rejects unsafe endpoints, invalid credentials, or a zero response bound.
    pub fn new(endpoint: &str, token: &str, maximum_response_bytes: usize) -> Result<Self, Error> {
        Self::new_with_ca(endpoint, token, maximum_response_bytes, None)
    }

    /// Create a client with an optional additional native trust anchor.
    ///
    /// Browser builds cannot install caller-provided trust anchors and reject
    /// a nonempty CA instead of silently ignoring it.
    pub fn new_with_ca(
        endpoint: &str,
        token: &str,
        maximum_response_bytes: usize,
        ca_pem: Option<&[u8]>,
    ) -> Result<Self, Error> {
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
        let builder = Transport::builder().timeout(std::time::Duration::from_secs(30));
        #[cfg(not(target_arch = "wasm32"))]
        let builder = {
            let mut builder = builder.redirect(reqwest::redirect::Policy::none());
            if let Some(ca_pem) = ca_pem {
                if ca_pem.is_empty() || ca_pem.len() > 64 * 1024 {
                    return Err(Error::InvalidArgument);
                }
                use rustls::pki_types::pem::PemObject;
                let parsed_certificates =
                    rustls::pki_types::CertificateDer::pem_slice_iter(ca_pem)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| Error::InvalidArgument)?;
                if parsed_certificates.is_empty() {
                    return Err(Error::InvalidArgument);
                }
                let mut roots = rustls::RootCertStore::empty();
                for certificate in &parsed_certificates {
                    roots
                        .add(certificate.clone())
                        .map_err(|_| Error::InvalidArgument)?;
                }
                let certificates = reqwest::Certificate::from_pem_bundle(ca_pem)
                    .map_err(|_| Error::InvalidArgument)?;
                if certificates.len() != parsed_certificates.len() {
                    return Err(Error::InvalidArgument);
                }
                for certificate in certificates {
                    builder = builder.add_root_certificate(certificate);
                }
            }
            builder
        };
        #[cfg(target_arch = "wasm32")]
        {
            if ca_pem.is_some() {
                return Err(Error::InvalidArgument);
            }
        }
        Ok(Self {
            transport: builder.build()?,
            endpoint,
            token: token.to_owned(),
            maximum: maximum_response_bytes,
            descriptors: DescriptorPool::decode(FILE_DESCRIPTOR_SET)
                .map_err(|_| Error::MalformedResponse)?,
            handshake: Arc::new(Mutex::new(HandshakeSlot::default())),
        })
    }

    /// Verify the authenticated Rust-owned Actors identity using the HTTP control route.
    ///
    /// A missing route reports `false`, allowing the platform facade to reject
    /// the endpoint without sending an application operation. Authentication
    /// failures and identity mismatches remain terminal errors.
    pub async fn verify_handshake(&self) -> Result<bool, Error> {
        let (generation, shared) = {
            let mut slot = self
                .handshake
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some((generation, shared)) = slot.current.as_ref() {
                (*generation, shared.clone())
            } else {
                let generation = slot.next_generation;
                slot.next_generation = slot.next_generation.wrapping_add(1);
                let client = self.clone();
                let shared = share_handshake(async move {
                    client.perform_handshake().await.map_err(Arc::new)
                });
                slot.current = Some((generation, shared.clone()));
                (generation, shared)
            }
        };
        match shared.await {
            Ok(result) => Ok(result),
            Err(error) => {
                let mut slot = self
                    .handshake
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                clear_failed_handshake(&mut slot, generation);
                Err(error.as_ref().clone())
            }
        }
    }

    async fn perform_handshake(&self) -> Result<bool, Error> {
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
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"));
        if !content_type {
            return Err(Error::MalformedResponse);
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.maximum.min(control::MAXIMUM_HANDSHAKE_RESPONSE_BYTES) as u64)
        {
            return Err(Error::ResponseTooLarge);
        }
        let mut bytes = Vec::new();
        let mut response = response;
        while let Some(chunk) = response.chunk().await? {
            if chunk.len()
                > self
                    .maximum
                    .min(control::MAXIMUM_HANDSHAKE_RESPONSE_BYTES)
                    .saturating_sub(bytes.len())
            {
                return Err(Error::ResponseTooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        let pool = DescriptorPool::decode(control::control_descriptor().as_slice())
            .map_err(|_| Error::MalformedResponse)?;
        let descriptor = pool
            .get_message_by_name("acyclic.protocol.v1.HandshakeResponse")
            .ok_or(Error::MalformedResponse)?;
        let mut json = serde_json::Deserializer::from_slice(&bytes);
        let decoded = DynamicMessage::deserialize(descriptor, &mut json)
            .map_err(|_| Error::MalformedResponse)?;
        json.end().map_err(|_| Error::MalformedResponse)?;
        control::validate_handshake_response(
            family,
            version,
            &[control::RequiredCapability {
                name: family.name(),
                version,
            }],
            &decoded.encode_to_vec(),
            control::MAXIMUM_HANDSHAKE_RESPONSE_BYTES.min(self.maximum),
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

    /// Execute `POST v1/actors/create` with canonical Actors JSON mapping.
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

    /// Execute `POST v1/actors/update` with canonical Actors JSON mapping.
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

    /// Execute `POST v1/actors/inspect` with canonical Actors JSON mapping.
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

    /// Execute `POST v1/actors/subscriptions/add` with canonical Actors JSON mapping.
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

    /// Execute `POST v1/actors/subscriptions/remove` with canonical Actors JSON mapping.
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

    /// Execute `POST v1/actors/subscriptions/resume` with canonical Actors JSON mapping.
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

    /// Execute `POST v1/actors/checkpoint` with canonical Actors JSON mapping.
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

    /// Execute `POST v1/actors/invoke` with canonical Actors JSON mapping.
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

#[cfg(test)]
mod tests {
    use super::{clear_failed_handshake, share_handshake, Client, Error, HandshakeSlot};

    #[test]
    fn caller_ca_is_validated_instead_of_ignored() {
        for (name, ca) in [
            ("plain garbage", Some(b"not a PEM certificate".as_slice())),
            (
                "malformed certificate",
                Some(b"-----BEGIN CERTIFICATE-----\nZ2FyYmFnZQ==\n-----END CERTIFICATE-----".as_slice()),
            ),
            ("empty", Some(b"".as_slice())),
        ] {
            let result = Client::new_with_ca("https://localhost", "fixture-token", 1024, ca);
            assert!(
                matches!(result, Err(Error::InvalidArgument)),
                "invalid CA fixture: {name}"
            );
        }
    }

    #[test]
    fn oversized_caller_ca_is_rejected() {
        let ca = vec![b'X'; 64 * 1024 + 1];
        let result = Client::new_with_ca("https://localhost", "fixture-token", 1024, Some(&ca));
        assert!(matches!(result, Err(Error::InvalidArgument)), "oversized CA fixture");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_client_accepts_a_valid_caller_ca() -> Result<(), Box<dyn std::error::Error>> {
        let fixture_result = rcgen::generate_simple_self_signed(["localhost".to_owned()]);
        let certificate = fixture_result?;
        let pem = certificate.cert.pem();
        let _client = Client::new_with_ca(
            "https://localhost",
            "fixture-token",
            1024,
            Some(pem.as_bytes()),
        )?;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_handshake_waiter_does_not_cancel_shared_probe() -> Result<(), Box<dyn std::error::Error>> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        use tokio::sync::oneshot;
        use tokio::time::{timeout, Duration};

        const FIXTURE_TIMEOUT: Duration = Duration::from_secs(5);

        tokio::task::LocalSet::new()
            .run_until(async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (started, started_rx) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        let server = tokio::task::spawn_local(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = [0_u8; 1024];
            let _ = socket.read(&mut request).await?;
            started
                .send(())
                .map_err(|_| std::io::Error::other("probe waiter dropped"))?;
            timeout(FIXTURE_TIMEOUT, release_rx)
                .await
                .map_err(|_| std::io::Error::other("probe release timed out"))?
                .map_err(|_| std::io::Error::other("probe release dropped"))?;
            socket
                .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
                .await?;
            Ok::<_, Box<dyn std::error::Error>>(())
        });
        let client = Client::new(&endpoint, "fixture-token", 1024)?;
        let first = tokio::task::spawn_local({
            let client = client.clone();
            async move { client.verify_handshake().await }
        });
        timeout(FIXTURE_TIMEOUT, started_rx)
            .await
            .map_err(|_| std::io::Error::other("probe did not start before timeout"))?
            .map_err(|_| std::io::Error::other("probe did not start"))?;
        first.abort();
        assert!(first.await.is_err(), "first waiter should be cancelled");

        let second = tokio::task::spawn_local(async move { client.verify_handshake().await });
        release
            .send(())
            .map_err(|_| std::io::Error::other("probe release failed"))?;
        let second = timeout(FIXTURE_TIMEOUT, second)
            .await
            .map_err(|_| std::io::Error::other("second probe waiter timed out"))?
            .map_err(|_| std::io::Error::other("second probe waiter task failed"))??;
        assert!(!second, "second waiter should observe HTTP fallback");
        timeout(FIXTURE_TIMEOUT, server)
            .await
            .map_err(|_| std::io::Error::other("fixture server timed out"))???;
        Ok(())
            })
            .await
    }

    #[test]
    fn stale_handshake_failure_cannot_clear_new_probe() {
        let mut slot = HandshakeSlot::default();
        let shared = share_handshake(async { Ok(true) });
        slot.current = Some((1, shared));

        clear_failed_handshake(&mut slot, 0);
        assert_eq!(slot.current.as_ref().map(|(generation, _)| *generation), Some(1));

        clear_failed_handshake(&mut slot, 1);
        assert!(slot.current.is_none());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_handshake_failure_is_terminal_and_shared() -> Result<(), Box<dyn std::error::Error>> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        use tokio::sync::oneshot;
        use tokio::time::{timeout, Duration};

        const FIXTURE_TIMEOUT: Duration = Duration::from_secs(5);

        tokio::task::LocalSet::new()
            .run_until(async {
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                let endpoint = format!("http://{}", listener.local_addr()?);
                let (started, started_rx) = oneshot::channel();
                let (release, release_rx) = oneshot::channel();
                let server = tokio::task::spawn_local(async move {
                    let (mut socket, _) = listener.accept().await?;
                    let mut request = [0_u8; 1024];
                    let _ = socket.read(&mut request).await?;
                    started
                        .send(())
                        .map_err(|_| std::io::Error::other("probe waiter dropped"))?;
                    timeout(FIXTURE_TIMEOUT, release_rx)
                        .await
                        .map_err(|_| std::io::Error::other("probe release timed out"))?
                        .map_err(|_| std::io::Error::other("probe release dropped"))?;
                    socket
                        .write_all(
                            b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n",
                        )
                        .await?;
                    Ok::<_, Box<dyn std::error::Error>>(())
                });
                let client = Client::new(&endpoint, "fixture-token", 1024)?;
                let first = tokio::task::spawn_local({
                    let client = client.clone();
                    async move { client.verify_handshake().await }
                });
                timeout(FIXTURE_TIMEOUT, started_rx)
                    .await
                    .map_err(|_| std::io::Error::other("probe did not start before timeout"))?
                    .map_err(|_| std::io::Error::other("probe did not start"))?;
                let second = tokio::task::spawn_local(async move { client.verify_handshake().await });
                release
                    .send(())
                    .map_err(|_| std::io::Error::other("probe release failed"))?;
                let first = timeout(FIXTURE_TIMEOUT, first)
                    .await
                    .map_err(|_| std::io::Error::other("first probe waiter timed out"))?
                    .map_err(|_| std::io::Error::other("first probe waiter task failed"))?;
                let second = timeout(FIXTURE_TIMEOUT, second)
                    .await
                    .map_err(|_| std::io::Error::other("second probe waiter timed out"))?
                    .map_err(|_| std::io::Error::other("second probe waiter task failed"))?;
                assert!(matches!(first, Err(Error::Service { status: 401, .. })));
                assert!(matches!(second, Err(Error::Service { status: 401, .. })));
                timeout(FIXTURE_TIMEOUT, server)
                    .await
                    .map_err(|_| std::io::Error::other("fixture server timed out"))???;
                Ok(())
            })
            .await
    }
}
