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

/// Error reported by the Workers HTTP client while configuring, encoding, or
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

/// Typed Workers v1 HTTP operations with bearer authentication and bounded responses.
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
    /// Create a client for the canonical Workers HTTP service.
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

    /// Verify the authenticated Rust-owned Workers identity using the HTTP control route.
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
                if slot
                    .current
                    .as_ref()
                    .is_some_and(|(current, _)| *current == generation)
                {
                    slot.current = None;
                }
                Err(error.as_ref().clone())
            }
        }
    }

    async fn perform_handshake(&self) -> Result<bool, Error> {
        use acyclic_sdk_contract_wire::{BindingFamily, transport_control as control};
        let family = BindingFamily::Workers;
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

    /// Execute `POST v1/workers/versions/publish` with canonical Workers JSON mapping.
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

    /// Execute `POST v1/workers/deployments/select` with canonical Workers JSON mapping.
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

    /// Execute `POST v1/workers/jobs/submit` with canonical Workers JSON mapping.
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

    /// Execute `POST v1/workers/jobs/inspect` with canonical Workers JSON mapping.
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

    /// Execute `POST v1/workers/jobs/cancel` with canonical Workers JSON mapping.
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

    /// Execute `POST v1/workers/versions/{sha256hex}/invoke` with canonical
    /// Workers JSON mapping. The path digest is the lowercase hexadecimal form
    /// of `request.version_sha256`.
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

    /// Execute `POST v1/workers/deployments/{alias}/invoke` with canonical
    /// Workers JSON mapping. The alias is validated as a path-safe v1 name
    /// before interpolation.
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

#[cfg(test)]
mod tests {
    use super::{Client, Error};

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
}
