//! Idiomatic handles over the canonical hosted Filesystem transport.
//!
//! This module deliberately contains no filesystem semantics. It retains exact
//! workspace/generation identities and delegates every operation to the v2
//! service implemented by the same canonical engine used by embedded profiles.

use crate::model::FilesystemProfile as EmbeddedProfile;
use acyclic_fs_wire::wire::filesystem::v2 as wire;
use acyclic_fs_wire::wire::protocol::v1 as protocol;
use crate::{
    DEFAULT_HOSTED_MAXIMUM_RESPONSE_BYTES, Digest, FILESYSTEM_PROTOCOL_VERSION, GenerationId,
    HostedSourceInvalidation, HostedSourceResult, HostedSourceState, IdempotencyKey,
};
pub use acyclic_auth::credential::CredentialProvider;
use bytes::Bytes;
use futures::Stream;
#[cfg(test)]
use prost::Message;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

type Client = wire::filesystem_service_client::FilesystemServiceClient<Channel>;

/// Largest caller-supplied PEM trust bundle accepted by the hosted constructor.
pub const MAX_CA_CERTIFICATE_BYTES: usize = 64 * 1024;
/// Smallest encoded response that can contain a valid protocol handshake.
pub const MINIMUM_HANDSHAKE_RESPONSE_BYTES: usize = 512;
/// Largest encoded byte-bearing envelope: an eight-byte cursor, 33-byte typed
/// object ID, contents, terminal flag, tags, and length varints.
pub const MAX_BYTE_RESPONSE_ENVELOPE_BYTES: u64 = 10 + 35 + 11 + 2;

/// Connection and client-side response bounds for [`HostedFs::connect`].
#[derive(Clone, Eq, PartialEq)]
pub struct HostedFsOptions {
    /// HTTPS endpoint of the hosted Filesystem service.
    pub endpoint: String,
    /// Opaque account-scoped bearer credential.
    pub bearer_token: String,
    /// Optional PEM CA bundle added to, rather than replacing, ambient roots.
    pub ca_certificate_pem: Option<Vec<u8>>,
    /// Maximum accepted encoded response size.
    pub maximum_response_bytes: usize,
    /// Maximum encoded request size sent by this client.
    pub maximum_request_bytes: usize,
    /// Connection deadline.
    pub connect_timeout: Duration,
    /// Per-request transport deadline.
    pub request_timeout: Duration,
}

impl std::fmt::Debug for HostedFsOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostedFsOptions")
            .field("endpoint", &self.endpoint)
            .field("bearer_token", &"[REDACTED]")
            .field(
                "ca_certificate_bytes",
                &self.ca_certificate_pem.as_ref().map(Vec::len),
            )
            .field("maximum_response_bytes", &self.maximum_response_bytes)
            .field("maximum_request_bytes", &self.maximum_request_bytes)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

impl HostedFsOptions {
    /// Creates bounded hosted options with conservative transport defaults.
    #[must_use]
    pub fn new(endpoint: impl Into<String>, bearer_token: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            bearer_token: bearer_token.into(),
            ca_certificate_pem: None,
            maximum_response_bytes: usize::try_from(DEFAULT_HOSTED_MAXIMUM_RESPONSE_BYTES)
                .unwrap_or(usize::MAX),
            maximum_request_bytes: 16 * 1024 * 1024,
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
        }
    }

    /// Adds a caller-supplied private CA while retaining ambient `WebPKI` roots.
    #[must_use]
    pub fn with_ca_certificate(mut self, certificate_pem: impl Into<Vec<u8>>) -> Self {
        self.ca_certificate_pem = Some(certificate_pem.into());
        self
    }
}

impl Default for HostedFsOptions {
    fn default() -> Self {
        Self::new(
            std::env::var("ACYCLIC_FILESYSTEM_ENDPOINT").unwrap_or_default(),
            std::env::var("ACYCLIC_API_KEY").unwrap_or_default(),
        )
    }
}

/// A local validation, transport, or malformed-server failure.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum HostedFsError {
    /// Hosted options are empty, unbounded, or use an unsupported endpoint.
    #[error("invalid hosted filesystem options: {0}")]
    InvalidOptions(&'static str),
    /// A caller-supplied semantic bound exceeds the negotiated service limit.
    #[error("hosted filesystem request exceeds negotiated limit: {0}")]
    LimitExceeded(&'static str),
    /// The endpoint could not be parsed or connected.
    #[error("hosted filesystem transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// The remote service rejected the operation.
    #[error("hosted filesystem operation failed: {0}")]
    Status(#[from] Status),
    /// A successful response omitted or substituted required identity state.
    #[error("hosted filesystem returned an invalid response: {0}")]
    InvalidResponse(&'static str),
}

/// Validated short-lived S3 access scoped to one hosted workspace generation.
#[derive(Clone, Eq, PartialEq)]
pub struct HostedS3Access {
    /// S3-compatible service endpoint selected by the deployment.
    pub endpoint: String,
    /// Unix timestamp after which these credentials must not be used.
    pub expires_at_unix_seconds: i64,
    /// Deployment-selected bucket.
    pub bucket: String,
    /// Deployment-selected region.
    pub region: String,
    /// Temporary access-key identifier.
    pub access_key_id: String,
    /// Temporary secret access key.
    pub secret_access_key: String,
    /// Temporary session token.
    pub session_token: String,
}

impl fmt::Debug for HostedS3Access {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HostedS3Access")
            .field("endpoint", &self.endpoint)
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("access_key_id", &"[REDACTED]")
            .field("secret_access_key", &"[REDACTED]")
            .field("session_token", &"[REDACTED]")
            .finish()
    }
}

/// Explicit controls for [`HostedWorkspace::s3_access_with_options`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostedS3AccessOptions {
    /// Whether the scoped credentials may author mutations.
    pub writable: bool,
    /// Requested credential lifetime. The service applies its negotiated bound.
    pub expires_after_seconds: u64,
    /// Stable retry identity for ambiguous credential-issuance outcomes.
    pub idempotency_key: IdempotencyKey,
}

impl Default for HostedS3AccessOptions {
    fn default() -> Self {
        Self {
            writable: false,
            expires_after_seconds: 900,
            idempotency_key: IdempotencyKey::new(),
        }
    }
}

/// Cloneable hosted Filesystem owner. Handles produced by another owner cannot
/// be substituted because each operation retains its exact wire reference.
#[derive(Clone)]
pub struct HostedFs {
    client: Client,
    authorization: Option<MetadataValue<Ascii>>,
    credential: Option<CredentialProvider>,
    capabilities: wire::Capabilities,
    owner: Arc<()>,
}

/// Rejects empty, unbounded, or unsupported hosted connection options before
/// any transport or credential is touched.
fn validate_hosted_options(options: &HostedFsOptions) -> Result<(), HostedFsError> {
    if options.maximum_request_bytes == 0
        || options.maximum_response_bytes < MINIMUM_HANDSHAKE_RESPONSE_BYTES
    {
        return Err(HostedFsError::InvalidOptions(
            "request bound must be nonzero and response bound must admit the handshake",
        ));
    }
    if options
        .ca_certificate_pem
        .as_ref()
        .is_some_and(|certificate| {
            certificate.is_empty() || certificate.len() > MAX_CA_CERTIFICATE_BYTES
        })
    {
        return Err(HostedFsError::InvalidOptions(
            "CA certificate must contain 1 to 65536 bytes",
        ));
    }
    if !(options.endpoint.starts_with("https://")
        || options.endpoint.starts_with("http://127.0.0.1:")
        || options.endpoint.starts_with("http://[::1]:")
        || options.endpoint.starts_with("http://localhost:"))
    {
        return Err(HostedFsError::InvalidOptions(
            "endpoint must use HTTPS or loopback HTTP",
        ));
    }
    Ok(())
}

impl HostedFs {
    /// Connects a bounded client and validates the canonical service handshake.
    pub async fn connect(options: HostedFsOptions) -> Result<Self, HostedFsError> {
        Self::connect_inner(options, None).await
    }

    /// Resolve the callback for the handshake and every subsequent request.
    pub async fn connect_with_credential(
        options: HostedFsOptions,
        credential: CredentialProvider,
    ) -> Result<Self, HostedFsError> {
        Self::connect_inner(options, Some(credential)).await
    }

    async fn connect_inner(
        options: HostedFsOptions,
        credential: Option<CredentialProvider>,
    ) -> Result<Self, HostedFsError> {
        validate_hosted_options(&options)?;
        let authorization = if credential.is_some() {
            None
        } else {
            Some(credential_header(&options.bearer_token)?)
        };
        let is_https = options.endpoint.starts_with("https://");
        let mut endpoint = Endpoint::new(options.endpoint)?
            .connect_timeout(options.connect_timeout)
            .timeout(options.request_timeout);
        if let Some(certificate) = options.ca_certificate_pem {
            if !is_https {
                return Err(HostedFsError::InvalidOptions(
                    "CA certificate requires an HTTPS endpoint",
                ));
            }
            endpoint = endpoint.tls_config(
                ClientTlsConfig::new()
                    .with_enabled_roots()
                    .ca_certificate(Certificate::from_pem(certificate)),
            )?;
        }
        let channel = endpoint.connect().await?;
        let mut client = Client::new(channel)
            .max_decoding_message_size(options.maximum_response_bytes)
            .max_encoding_message_size(options.maximum_request_bytes);
        let mut handshake = Request::new(wire::HandshakeRequest {
            protocol: Some(protocol::HandshakeRequest {
                protocol: Some(protocol::ProtocolIdentity {
                    version: FILESYSTEM_PROTOCOL_VERSION.to_owned(),
                    descriptor_digest: crate::descriptor_digest(),
                }),
                required: Some(protocol::CapabilitySet {
                    capabilities: vec![protocol::Capability {
                        name: "filesystem".to_owned(),
                        version: FILESYSTEM_PROTOCOL_VERSION.to_owned(),
                    }],
                }),
            }),
        });
        handshake.metadata_mut().insert(
            "authorization",
            request_authorization(authorization.as_ref(), credential.as_ref()).await?,
        );
        let mut capabilities = validate_handshake(client.handshake(handshake).await?.into_inner())?;
        let configured_request_bytes =
            u64::try_from(options.maximum_request_bytes).map_err(|_| {
                HostedFsError::InvalidOptions("request bound does not fit the protocol")
            })?;
        let configured_response_bytes =
            u64::try_from(options.maximum_response_bytes).map_err(|_| {
                HostedFsError::InvalidOptions("response bound does not fit the protocol")
            })?;
        capabilities.maximum_request_bytes = capabilities
            .maximum_request_bytes
            .min(configured_request_bytes);
        capabilities.maximum_response_bytes = capabilities
            .maximum_response_bytes
            .min(configured_response_bytes - MAX_BYTE_RESPONSE_ENVELOPE_BYTES);
        let maximum_request_bytes =
            usize::try_from(capabilities.maximum_request_bytes).map_err(|_| {
                HostedFsError::InvalidResponse("request bound does not fit this platform")
            })?;
        // The advertised limit is application payload, while Tonic bounds the complete
        // protobuf message. Retain the caller's encoded-frame ceiling and expose only
        // the payload bytes that fit below the largest byte-bearing response envelope.
        client = client
            .max_decoding_message_size(options.maximum_response_bytes)
            .max_encoding_message_size(maximum_request_bytes);
        Ok(Self {
            client,
            authorization,
            credential,
            capabilities,
            owner: Arc::new(()),
        })
    }

    /// Returns the authenticated server capabilities retained by this client.
    #[must_use]
    pub const fn capabilities(&self) -> &wire::Capabilities {
        &self.capabilities
    }

    async fn request<T>(&self, value: T) -> Result<Request<T>, HostedFsError> {
        let authorization =
            request_authorization(self.authorization.as_ref(), self.credential.as_ref()).await?;
        let mut request = Request::new(value);
        request
            .metadata_mut()
            .insert("authorization", authorization);
        Ok(request)
    }

    fn require_page_bound(&self, value: u32, name: &'static str) -> Result<(), HostedFsError> {
        if value == 0 || value > self.capabilities.maximum_page_items {
            Err(HostedFsError::LimitExceeded(name))
        } else {
            Ok(())
        }
    }

    fn require_response_bound(&self, value: u64, name: &'static str) -> Result<(), HostedFsError> {
        if value == 0 || value > self.capabilities.maximum_response_bytes {
            Err(HostedFsError::LimitExceeded(name))
        } else {
            Ok(())
        }
    }

    fn require_transaction_bound(&self, value: usize) -> Result<(), HostedFsError> {
        if value == 0 || value > self.capabilities.maximum_transaction_mutations as usize {
            Err(HostedFsError::LimitExceeded("transaction mutations"))
        } else {
            Ok(())
        }
    }

    /// Creates one named workspace with a caller-owned retry identity.
    pub async fn create_workspace(
        &self,
        name: impl Into<String>,
        profile: EmbeddedProfile,
        idempotency_key: IdempotencyKey,
    ) -> Result<HostedWorkspace, HostedFsError> {
        let mut client = self.client.clone();
        let response = client
            .create_workspace(
                self.request(wire::CreateWorkspaceRequest {
                    name: name.into(),
                    profile: profile_to_wire(profile) as i32,
                    operation: Some(operation(idempotency_key)),
                })
                .await?,
            )
            .await?
            .into_inner();
        self.workspace(response.workspace)
    }

    /// Opens one workspace by its canonical name.
    pub async fn open_workspace(
        &self,
        name: impl Into<String>,
    ) -> Result<HostedWorkspace, HostedFsError> {
        let mut client = self.client.clone();
        let response = client
            .open_workspace(
                self.request(wire::OpenWorkspaceRequest {
                    selector: Some(wire::open_workspace_request::Selector::Name(name.into())),
                })
                .await?,
            )
            .await?
            .into_inner();
        self.workspace(response.workspace)
    }

    /// Resolves a previously submitted mutation by its caller-owned identity.
    pub async fn observe(
        &self,
        workspace: &HostedWorkspace,
        idempotency_key: IdempotencyKey,
    ) -> Result<wire::ObserveResponse, HostedFsError> {
        self.require_owner(workspace)?;
        let mut client = self.client.clone();
        Ok(client
            .observe(
                self.request(wire::ObserveRequest {
                    workspace: Some(workspace.reference.clone()),
                    operation_id: idempotency_key.into_bytes().to_vec(),
                })
                .await?,
            )
            .await?
            .into_inner())
    }

    /// Requests cancellation and returns the service's latest operation state.
    pub async fn cancel(
        &self,
        workspace: &HostedWorkspace,
        idempotency_key: IdempotencyKey,
    ) -> Result<wire::CancelResponse, HostedFsError> {
        self.require_owner(workspace)?;
        let mut client = self.client.clone();
        Ok(client
            .cancel(
                self.request(wire::CancelRequest {
                    workspace: Some(workspace.reference.clone()),
                    operation_id: idempotency_key.into_bytes().to_vec(),
                })
                .await?,
            )
            .await?
            .into_inner())
    }

    /// Applies one server-authenticated immutable join plan.
    pub async fn apply_join(
        &self,
        plan: wire::JoinPlan,
        idempotency_key: IdempotencyKey,
    ) -> Result<wire::JoinResponse, HostedFsError> {
        let mut client = self.client.clone();
        Ok(client
            .apply_join(
                self.request(wire::ApplyJoinRequest {
                    plan: Some(plan),
                    operation: Some(operation(idempotency_key)),
                })
                .await?,
            )
            .await?
            .into_inner())
    }

    /// Imports a bounded canonical object stream into one hosted workspace.
    pub async fn import<S>(&self, chunks: S) -> Result<wire::ImportResponse, HostedFsError>
    where
        S: Stream<Item = wire::ImportChunk> + Send + 'static,
    {
        let mut client = self.client.clone();
        Ok(client
            .import(self.request(chunks).await?)
            .await?
            .into_inner())
    }

    fn require_owner(&self, workspace: &HostedWorkspace) -> Result<(), HostedFsError> {
        if Arc::ptr_eq(&self.owner, &workspace.filesystem.owner) {
            Ok(())
        } else {
            Err(HostedFsError::InvalidOptions(
                "workspace belongs to another hosted client",
            ))
        }
    }

    fn workspace(&self, value: Option<wire::Workspace>) -> Result<HostedWorkspace, HostedFsError> {
        let value = value.ok_or(HostedFsError::InvalidResponse("workspace is absent"))?;
        let reference = value.workspace.ok_or(HostedFsError::InvalidResponse(
            "workspace reference is absent",
        ))?;
        exact_len(
            &reference.workspace_id,
            16,
            "workspace identity has the wrong length",
        )?;
        let head = value
            .head
            .ok_or(HostedFsError::InvalidResponse("workspace head is absent"))?;
        validate_generation(&head, &reference)?;
        Ok(HostedWorkspace {
            filesystem: self.clone(),
            reference,
            profile: wire::FilesystemProfile::try_from(value.profile)
                .map_err(|_| HostedFsError::InvalidResponse("workspace profile is invalid"))?,
            head,
        })
    }
}

fn credential_header(token: &str) -> Result<MetadataValue<Ascii>, HostedFsError> {
    if !acyclic_auth::credential::valid_credential(token) {
        return Err(HostedFsError::InvalidOptions("invalid bearer credential"));
    }
    let mut value: MetadataValue<Ascii> = format!("Bearer {token}")
        .parse()
        .map_err(|_| HostedFsError::InvalidOptions("bearer credential is not HTTP metadata"))?;
    value.set_sensitive(true);
    Ok(value)
}

async fn request_authorization(
    fixed: Option<&MetadataValue<Ascii>>,
    provider: Option<&CredentialProvider>,
) -> Result<MetadataValue<Ascii>, HostedFsError> {
    if let Some(provider) = provider {
        let token = provider
            .resolve()
            .await
            .map_err(|_| Status::unauthenticated("filesystem credential renewal failed"))?;
        credential_header(&token)
            .map_err(|_| Status::unauthenticated("invalid filesystem credential").into())
    } else {
        fixed
            .cloned()
            .ok_or_else(|| Status::unauthenticated("missing filesystem credential").into())
    }
}

#[cfg(test)]
mod credential_tests;

include!("hosted/workspace.rs");
include!("hosted/generation.rs");
include!("hosted/transaction.rs");
include!("hosted/validation.rs");
include!("hosted/tests.rs");
