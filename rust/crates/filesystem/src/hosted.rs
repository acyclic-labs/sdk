//! Idiomatic handles over the canonical hosted Filesystem transport.
//!
//! This module deliberately contains no filesystem semantics. It retains exact
//! workspace/generation identities and delegates every operation to the v2
//! service implemented by the same canonical engine used by embedded profiles.

use crate::model::FilesystemProfile as EmbeddedProfile;
use crate::wire::filesystem::v2 as wire;
use crate::wire::harness::v2 as harness;
use crate::{
    Digest, Fs, GenerationId, HostedSourceInvalidation, HostedSourceResult, HostedSourceState,
    IdempotencyKey,
};
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
const MINIMUM_HANDSHAKE_RESPONSE_BYTES: usize = 512;
// ExportChunk is the largest byte-bearing envelope: an eight-byte cursor,
// 33-byte typed object ID, contents, terminal flag, tags, and length varints.
const MAX_BYTE_RESPONSE_ENVELOPE_BYTES: u64 = 10 + 35 + 11 + 2;

/// Connection and client-side response bounds for [`Fs::hosted`].
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
            maximum_response_bytes: 16 * 1024 * 1024,
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
#[derive(Debug, Error)]
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

/// Marker selecting the transport-only `Fs::hosted` constructor.
#[doc(hidden)]
pub struct HostedAuthority;
/// Marker selecting the transport-only `Fs::hosted` constructor.
#[doc(hidden)]
pub struct HostedObjects;

impl Fs<HostedAuthority, HostedObjects> {
    /// Connects the canonical high-level Filesystem handles to a hosted service.
    ///
    /// # Errors
    ///
    /// Rejects invalid bounds, credentials, non-HTTP endpoints, and failed
    /// transport setup before returning a usable handle.
    pub async fn hosted(options: HostedFsOptions) -> Result<HostedFs, HostedFsError> {
        HostedFs::connect(options).await
    }
}

/// Cloneable hosted Filesystem owner. Handles produced by another owner cannot
/// be substituted because each operation retains its exact wire reference.
#[derive(Clone)]
pub struct HostedFs {
    client: Client,
    authorization: MetadataValue<Ascii>,
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
    if options.bearer_token.is_empty() {
        return Err(HostedFsError::InvalidOptions(
            "bearer credential must be nonempty",
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
    async fn connect(options: HostedFsOptions) -> Result<Self, HostedFsError> {
        validate_hosted_options(&options)?;
        let authorization: MetadataValue<Ascii> = format!("Bearer {}", options.bearer_token)
            .parse()
            .map_err(|_| HostedFsError::InvalidOptions("bearer credential is not HTTP metadata"))?;
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
            harness: Some(harness::HandshakeRequest {
                protocol: Some(harness::ProtocolIdentity {
                    version: "1".to_owned(),
                    descriptor_digest: crate::descriptor_digest(),
                }),
                required: Some(harness::CapabilitySet {
                    capabilities: vec![harness::Capability {
                        name: "filesystem".to_owned(),
                        version: "1".to_owned(),
                    }],
                }),
            }),
        });
        handshake
            .metadata_mut()
            .insert("authorization", authorization.clone());
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
            capabilities,
            owner: Arc::new(()),
        })
    }

    /// Returns the authenticated server capabilities retained by this client.
    #[must_use]
    pub const fn capabilities(&self) -> &wire::Capabilities {
        &self.capabilities
    }

    fn request<T>(&self, value: T) -> Request<T> {
        let mut request = Request::new(value);
        request
            .metadata_mut()
            .insert("authorization", self.authorization.clone());
        request
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
            .create_workspace(self.request(wire::CreateWorkspaceRequest {
                name: name.into(),
                profile: profile_to_wire(profile) as i32,
                operation: Some(operation(idempotency_key)),
            }))
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
            .open_workspace(self.request(wire::OpenWorkspaceRequest {
                selector: Some(wire::open_workspace_request::Selector::Name(name.into())),
            }))
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
            .observe(self.request(wire::ObserveRequest {
                workspace: Some(workspace.reference.clone()),
                operation_id: idempotency_key.into_bytes().to_vec(),
            }))
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
            .cancel(self.request(wire::CancelRequest {
                workspace: Some(workspace.reference.clone()),
                operation_id: idempotency_key.into_bytes().to_vec(),
            }))
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
            .apply_join(self.request(wire::ApplyJoinRequest {
                plan: Some(plan),
                operation: Some(operation(idempotency_key)),
            }))
            .await?
            .into_inner())
    }

    /// Imports a bounded canonical object stream into one hosted workspace.
    pub async fn import<S>(&self, chunks: S) -> Result<wire::ImportResponse, HostedFsError>
    where
        S: Stream<Item = wire::ImportChunk> + Send + 'static,
    {
        let mut client = self.client.clone();
        Ok(client.import(self.request(chunks)).await?.into_inner())
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

/// One hosted named workspace with an exact last-observed head.
#[derive(Clone)]
pub struct HostedWorkspace {
    filesystem: HostedFs,
    reference: wire::WorkspaceRef,
    profile: wire::FilesystemProfile,
    head: wire::GenerationRef,
}

impl HostedWorkspace {
    /// Stable opaque workspace identity.
    #[must_use]
    pub fn id(&self) -> &[u8] {
        &self.reference.workspace_id
    }

    /// Canonical workspace name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.reference.name
    }

    /// Exact profile selected at creation.
    #[must_use]
    pub const fn profile(&self) -> wire::FilesystemProfile {
        self.profile
    }

    /// Resolves the current exact immutable head.
    pub async fn head(&self) -> Result<HostedGeneration, HostedFsError> {
        let mut client = self.filesystem.client.clone();
        let response = client
            .get_head(self.filesystem.request(wire::GetHeadRequest {
                workspace: Some(self.reference.clone()),
            }))
            .await?
            .into_inner();
        self.generation_handle(response.generation)
    }

    /// Returns the current provider-backed source lifecycle state.
    pub async fn source_state(&self) -> Result<HostedSourceResult, HostedFsError> {
        self.require_source_reconciliation()?;
        let mut client = self.filesystem.client.clone();
        let response = client
            .get_source_state(self.filesystem.request(wire::SourceStateRequest {
                workspace: Some(self.reference.clone()),
            }))
            .await?
            .into_inner();
        self.source_result(&response)
    }

    /// Reconciles pending provider events with exactly idempotent retry.
    pub async fn reconcile_source(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<HostedSourceResult, HostedFsError> {
        self.source_operation(idempotency_key, SourceOperation::Reconcile)
            .await
    }

    /// Rebuilds provider state from an authoritative source scan.
    pub async fn rescan_source(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<HostedSourceResult, HostedFsError> {
        self.source_operation(idempotency_key, SourceOperation::Rescan)
            .await
    }

    /// Seals the provider-backed source and returns its immutable generation.
    pub async fn seal(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<HostedGeneration, HostedFsError> {
        self.require_source_reconciliation()?;
        let mut client = self.filesystem.client.clone();
        let response = client
            .seal_source(self.filesystem.request(wire::SourceOperationRequest {
                workspace: Some(self.reference.clone()),
                operation: Some(operation(idempotency_key)),
            }))
            .await?
            .into_inner();
        let result = self.source_result(&response)?;
        if result.state != HostedSourceState::Sealed {
            return Err(HostedFsError::InvalidResponse(
                "seal did not return a sealed generation",
            ));
        }
        self.generation_handle(response.generation)
    }

    fn require_source_reconciliation(&self) -> Result<(), HostedFsError> {
        if self.filesystem.capabilities.source_reconciliation {
            Ok(())
        } else {
            Err(HostedFsError::InvalidOptions(
                "hosted deployment does not support source reconciliation",
            ))
        }
    }

    async fn source_operation(
        &self,
        idempotency_key: IdempotencyKey,
        kind: SourceOperation,
    ) -> Result<HostedSourceResult, HostedFsError> {
        self.require_source_reconciliation()?;
        let request = self.filesystem.request(wire::SourceOperationRequest {
            workspace: Some(self.reference.clone()),
            operation: Some(operation(idempotency_key)),
        });
        let mut client = self.filesystem.client.clone();
        let response = match kind {
            SourceOperation::Reconcile => client.reconcile_source(request).await?.into_inner(),
            SourceOperation::Rescan => client.rescan_source(request).await?.into_inner(),
        };
        self.source_result(&response)
    }

    fn source_result(
        &self,
        response: &wire::SourceResponse,
    ) -> Result<HostedSourceResult, HostedFsError> {
        let invalidation = match wire::SourceInvalidationReason::try_from(response.reason) {
            Ok(wire::SourceInvalidationReason::Unspecified) => None,
            Ok(wire::SourceInvalidationReason::InitialSnapshotRequired) => {
                Some(HostedSourceInvalidation::InitialSnapshotRequired)
            }
            Ok(wire::SourceInvalidationReason::QueueOverflow) => {
                Some(HostedSourceInvalidation::QueueOverflow)
            }
            Ok(wire::SourceInvalidationReason::NativeRescanRequired) => {
                Some(HostedSourceInvalidation::NativeRescanRequired)
            }
            Ok(wire::SourceInvalidationReason::BackendError) => {
                Some(HostedSourceInvalidation::BackendError)
            }
            Ok(wire::SourceInvalidationReason::UnrepresentablePath) => {
                Some(HostedSourceInvalidation::UnrepresentablePath)
            }
            Ok(wire::SourceInvalidationReason::AmbiguousRename) => {
                Some(HostedSourceInvalidation::AmbiguousRename)
            }
            Ok(wire::SourceInvalidationReason::RootChanged) => {
                Some(HostedSourceInvalidation::RootChanged)
            }
            Err(_) => {
                return Err(HostedFsError::InvalidResponse(
                    "source invalidation reason is invalid",
                ));
            }
        };
        let state = match (wire::SourceState::try_from(response.state), invalidation) {
            (Ok(wire::SourceState::Clean), None) => HostedSourceState::Clean,
            (Ok(wire::SourceState::PendingCapture), None) => HostedSourceState::PendingCapture,
            (Ok(wire::SourceState::NeedsRescan), Some(reason)) => {
                HostedSourceState::NeedsRescan(reason)
            }
            (Ok(wire::SourceState::Conflict), None) => HostedSourceState::Conflict,
            (Ok(wire::SourceState::Sealed), None) => HostedSourceState::Sealed,
            _ => return Err(HostedFsError::InvalidResponse("source state is invalid")),
        };
        let has_generation = response.generation.is_some();
        if matches!(state, HostedSourceState::Clean | HostedSourceState::Sealed) != has_generation {
            return Err(HostedFsError::InvalidResponse(
                "source generation does not match its state",
            ));
        }
        let generation_id = response
            .generation
            .as_ref()
            .map(|generation| {
                validate_generation(generation, &self.reference)?;
                let bytes: [u8; 32] =
                    generation
                        .generation_id
                        .as_slice()
                        .try_into()
                        .map_err(|_| {
                            HostedFsError::InvalidResponse(
                                "generation identity has the wrong length",
                            )
                        })?;
                Ok::<GenerationId, HostedFsError>(GenerationId::new(Digest::from_bytes(bytes)))
            })
            .transpose()?;
        Ok(HostedSourceResult {
            state,
            generation_id,
        })
    }

    /// Reopens and authenticates one exact generation.
    pub async fn generation(
        &self,
        generation_id: impl Into<Vec<u8>>,
    ) -> Result<HostedGeneration, HostedFsError> {
        let selected = wire::GenerationRef {
            workspace: Some(self.reference.clone()),
            generation_id: generation_id.into(),
        };
        validate_generation(&selected, &self.reference)?;
        let mut client = self.filesystem.client.clone();
        let response = client
            .get_generation(self.filesystem.request(wire::GetGenerationRequest {
                generation: Some(selected),
            }))
            .await?
            .into_inner();
        self.generation_handle(response.generation)
    }

    fn generation_handle(
        &self,
        value: Option<wire::GenerationRef>,
    ) -> Result<HostedGeneration, HostedFsError> {
        let reference = value.ok_or(HostedFsError::InvalidResponse("generation is absent"))?;
        validate_generation(&reference, &self.reference)?;
        Ok(HostedGeneration {
            workspace: self.clone(),
            reference,
        })
    }

    /// Begins a sparse transaction against the current observed head.
    pub fn begin_transaction(&self, idempotency_key: IdempotencyKey) -> HostedTransaction {
        HostedTransaction {
            workspace: self.clone(),
            base: self.head.clone(),
            idempotency_key,
            mutations: Vec::new(),
        }
    }

    /// Deletes this workspace head with exactly idempotent retry.
    pub async fn delete(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<wire::MutationResponse, HostedFsError> {
        let mut client = self.filesystem.client.clone();
        Ok(client
            .delete_workspace(self.filesystem.request(wire::DeleteWorkspaceRequest {
                workspace: Some(self.reference.clone()),
                operation: Some(operation(idempotency_key)),
            }))
            .await?
            .into_inner())
    }

    /// Observation-safe rebases this fork using bounded history/change/conflict work.
    pub async fn rebase(
        &self,
        maximum_generations: u32,
        maximum_changes: u32,
        maximum_conflicts: u32,
        idempotency_key: IdempotencyKey,
    ) -> Result<wire::RebaseResponse, HostedFsError> {
        self.filesystem
            .require_page_bound(maximum_generations, "rebase generations")?;
        self.filesystem
            .require_page_bound(maximum_changes, "rebase changes")?;
        self.filesystem
            .require_page_bound(maximum_conflicts, "rebase conflicts")?;
        let mut client = self.filesystem.client.clone();
        Ok(client
            .rebase(self.filesystem.request(wire::RebaseRequest {
                workspace: Some(self.reference.clone()),
                maximum_conflicts,
                operation: Some(operation(idempotency_key)),
                maximum_generations,
                maximum_changes,
            }))
            .await?
            .into_inner())
    }

    /// Issues a short-lived mount capability scoped to one exact generation.
    pub async fn issue_mount_credential(
        &self,
        generation: &HostedGeneration,
        writable: bool,
        expires_after_seconds: u64,
        idempotency_key: IdempotencyKey,
    ) -> Result<wire::CredentialResponse, HostedFsError> {
        self.issue_credential(
            generation,
            writable,
            expires_after_seconds,
            idempotency_key,
            false,
        )
        .await
    }

    /// Issues short-lived S3 credentials scoped to one exact generation.
    pub async fn issue_s3_credential(
        &self,
        generation: &HostedGeneration,
        writable: bool,
        expires_after_seconds: u64,
        idempotency_key: IdempotencyKey,
    ) -> Result<wire::CredentialResponse, HostedFsError> {
        self.issue_credential(
            generation,
            writable,
            expires_after_seconds,
            idempotency_key,
            true,
        )
        .await
    }

    /// Discovers the deployment endpoint and region and issues read-only S3 access
    /// for the workspace's current immutable head.
    pub async fn s3_access(&self) -> Result<HostedS3Access, HostedFsError> {
        self.s3_access_with_options(HostedS3AccessOptions::default())
            .await
    }

    /// Discovers and validates S3 access using explicit scope, lifetime, and retry identity.
    pub async fn s3_access_with_options(
        &self,
        options: HostedS3AccessOptions,
    ) -> Result<HostedS3Access, HostedFsError> {
        if !self.filesystem.capabilities.s3_credentials {
            return Err(HostedFsError::InvalidOptions(
                "hosted deployment does not support S3 credentials",
            ));
        }
        let generation = self.head().await?;
        let response = self
            .issue_s3_credential(
                &generation,
                options.writable,
                options.expires_after_seconds,
                options.idempotency_key,
            )
            .await?;
        hosted_s3_access(response)
    }

    async fn issue_credential(
        &self,
        generation: &HostedGeneration,
        writable: bool,
        expires_after_seconds: u64,
        idempotency_key: IdempotencyKey,
        s3: bool,
    ) -> Result<wire::CredentialResponse, HostedFsError> {
        self.filesystem.require_owner(&generation.workspace)?;
        let request = self.filesystem.request(wire::CredentialRequest {
            workspace: Some(self.reference.clone()),
            generation: Some(generation.reference.clone()),
            writable,
            expires_after_seconds,
            operation: Some(operation(idempotency_key)),
        });
        let mut client = self.filesystem.client.clone();
        if s3 {
            Ok(client.issue_s3_credential(request).await?.into_inner())
        } else {
            Ok(client.issue_mount_credential(request).await?.into_inner())
        }
    }
}

#[derive(Clone, Copy)]
enum SourceOperation {
    Reconcile,
    Rescan,
}

fn hosted_s3_access(response: wire::CredentialResponse) -> Result<HostedS3Access, HostedFsError> {
    let Some(wire::credential_response::Credential::S3(credential)) = response.credential else {
        return Err(HostedFsError::InvalidResponse(
            "S3 credential response has the wrong credential kind",
        ));
    };
    if response.endpoint.is_empty()
        || response.expires_at_unix_seconds <= 0
        || credential.bucket.is_empty()
        || credential.region.is_empty()
        || credential.access_key_id.is_empty()
        || credential.secret_access_key.is_empty()
    {
        return Err(HostedFsError::InvalidResponse(
            "S3 credential response is incomplete",
        ));
    }
    Ok(HostedS3Access {
        endpoint: response.endpoint,
        expires_at_unix_seconds: response.expires_at_unix_seconds,
        bucket: credential.bucket,
        region: credential.region,
        access_key_id: credential.access_key_id,
        secret_access_key: credential.secret_access_key,
        session_token: credential.session_token,
    })
}

/// One exact immutable hosted generation.
#[derive(Clone)]
pub struct HostedGeneration {
    workspace: HostedWorkspace,
    reference: wire::GenerationRef,
}

impl HostedGeneration {
    /// Exact 32-byte generation identity.
    #[must_use]
    pub fn id(&self) -> &[u8] {
        &self.reference.generation_id
    }

    /// Reads a bounded byte range without materializing unrelated extents.
    pub async fn read_range(
        &self,
        path: impl Into<String>,
        offset: u64,
        length: u64,
        maximum_bytes: u64,
    ) -> Result<Bytes, HostedFsError> {
        self.workspace
            .filesystem
            .require_response_bound(maximum_bytes, "read bytes")?;
        let mut client = self.workspace.filesystem.client.clone();
        let response = client
            .read(self.workspace.filesystem.request(wire::ReadRequest {
                generation: Some(self.reference.clone()),
                path: path.into(),
                range: Some(wire::ByteRange { offset, length }),
                maximum_bytes,
            }))
            .await?
            .into_inner();
        Ok(Bytes::from(response.contents))
    }

    /// Reads one complete regular file up to an explicit maximum.
    pub async fn read(
        &self,
        path: impl Into<String>,
        maximum_bytes: u64,
    ) -> Result<Bytes, HostedFsError> {
        self.workspace
            .filesystem
            .require_response_bound(maximum_bytes, "read bytes")?;
        let mut client = self.workspace.filesystem.client.clone();
        let response = client
            .read(self.workspace.filesystem.request(wire::ReadRequest {
                generation: Some(self.reference.clone()),
                path: path.into(),
                range: None,
                maximum_bytes,
            }))
            .await?
            .into_inner();
        Ok(Bytes::from(response.contents))
    }

    /// Returns exact metadata for one path.
    pub async fn stat(&self, path: impl Into<String>) -> Result<wire::FileStat, HostedFsError> {
        let mut client = self.workspace.filesystem.client.clone();
        client
            .stat(self.workspace.filesystem.request(wire::StatRequest {
                generation: Some(self.reference.clone()),
                path: path.into(),
            }))
            .await?
            .into_inner()
            .stat
            .ok_or(HostedFsError::InvalidResponse("file stat is absent"))
    }

    /// Lists one bounded directory page.
    pub async fn list_directory(
        &self,
        path: impl Into<String>,
        after: Option<wire::LogicalName>,
        maximum_items: u32,
    ) -> Result<wire::DirectoryPage, HostedFsError> {
        self.workspace
            .filesystem
            .require_page_bound(maximum_items, "directory items")?;
        let mut client = self.workspace.filesystem.client.clone();
        client
            .list_directory(
                self.workspace
                    .filesystem
                    .request(wire::ListDirectoryRequest {
                        generation: Some(self.reference.clone()),
                        path: path.into(),
                        page: Some(wire::PageOptions {
                            maximum_items,
                            after,
                        }),
                    }),
            )
            .await?
            .into_inner()
            .page
            .ok_or(HostedFsError::InvalidResponse("directory page is absent"))
    }

    /// Reads a bounded symbolic-link target.
    pub async fn read_symbolic_link(
        &self,
        path: impl Into<String>,
        maximum_bytes: u64,
    ) -> Result<Bytes, HostedFsError> {
        self.workspace
            .filesystem
            .require_response_bound(maximum_bytes, "symbolic-link bytes")?;
        let mut client = self.workspace.filesystem.client.clone();
        let response = client
            .read_link(self.workspace.filesystem.request(wire::ReadLinkRequest {
                generation: Some(self.reference.clone()),
                path: path.into(),
                maximum_bytes,
            }))
            .await?
            .into_inner();
        Ok(Bytes::from(response.contents))
    }

    /// Plans only the sparse extents intersecting one requested range.
    pub async fn plan_extents(
        &self,
        path: impl Into<String>,
        offset: u64,
        length: u64,
        maximum_extents: u32,
    ) -> Result<wire::PlanExtentsResponse, HostedFsError> {
        self.workspace
            .filesystem
            .require_page_bound(maximum_extents, "extent items")?;
        let mut client = self.workspace.filesystem.client.clone();
        Ok(client
            .plan_extents(self.workspace.filesystem.request(wire::PlanExtentsRequest {
                generation: Some(self.reference.clone()),
                path: path.into(),
                range: Some(wire::ByteRange { offset, length }),
                maximum_extents,
            }))
            .await?
            .into_inner())
    }

    /// Computes a bounded semantic diff to another generation in this client.
    pub async fn diff(
        &self,
        to: &HostedGeneration,
        maximum_changes: u32,
    ) -> Result<wire::DiffResponse, HostedFsError> {
        self.workspace.filesystem.require_owner(&to.workspace)?;
        self.workspace
            .filesystem
            .require_page_bound(maximum_changes, "diff changes")?;
        let mut client = self.workspace.filesystem.client.clone();
        Ok(client
            .diff(self.workspace.filesystem.request(wire::DiffRequest {
                from: Some(self.reference.clone()),
                to: Some(to.reference.clone()),
                maximum_changes,
            }))
            .await?
            .into_inner())
    }

    /// Plans a bounded merge/rebase/squash/cherry-pick into an exact target.
    pub async fn plan_join(
        &self,
        target: &HostedGeneration,
        history: wire::JoinHistory,
        maximum_generations: u32,
        maximum_changes: u32,
        maximum_conflicts: u32,
    ) -> Result<wire::JoinPlan, HostedFsError> {
        self.workspace.filesystem.require_owner(&target.workspace)?;
        self.workspace
            .filesystem
            .require_page_bound(maximum_generations, "join generations")?;
        self.workspace
            .filesystem
            .require_page_bound(maximum_changes, "join changes")?;
        self.workspace
            .filesystem
            .require_page_bound(maximum_conflicts, "join conflicts")?;
        let mut client = self.workspace.filesystem.client.clone();
        Ok(client
            .plan_join(self.workspace.filesystem.request(wire::PlanJoinRequest {
                source: Some(self.reference.clone()),
                target: Some(target.reference.clone()),
                maximum_changes,
                maximum_conflicts,
                maximum_generations,
                history: history as i32,
            }))
            .await?
            .into_inner())
    }

    /// Streams one bounded canonical export without whole-generation buffering.
    pub async fn export(
        &self,
        after: Vec<u8>,
        maximum_objects: u32,
        maximum_bytes: u64,
    ) -> Result<tonic::Streaming<wire::ExportChunk>, HostedFsError> {
        self.workspace
            .filesystem
            .require_page_bound(maximum_objects, "export objects")?;
        self.workspace
            .filesystem
            .require_response_bound(maximum_bytes, "export bytes")?;
        let mut client = self.workspace.filesystem.client.clone();
        Ok(client
            .export(self.workspace.filesystem.request(wire::ExportRequest {
                generation: Some(self.reference.clone()),
                after,
                maximum_objects,
                maximum_bytes,
            }))
            .await?
            .into_inner())
    }

    /// Forks this exact generation without copying its immutable closure.
    pub async fn fork(
        &self,
        destination_name: impl Into<String>,
        idempotency_key: IdempotencyKey,
    ) -> Result<HostedWorkspace, HostedFsError> {
        let mut client = self.workspace.filesystem.client.clone();
        let response = client
            .fork_workspace(
                self.workspace
                    .filesystem
                    .request(wire::ForkWorkspaceRequest {
                        source: Some(self.reference.clone()),
                        destination_name: destination_name.into(),
                        operation: Some(operation(idempotency_key)),
                    }),
            )
            .await?
            .into_inner();
        self.workspace.filesystem.workspace(response.workspace)
    }

    /// Retains this exact generation as a named checkpoint.
    pub async fn checkpoint(
        &self,
        identity: impl Into<String>,
        idempotency_key: IdempotencyKey,
    ) -> Result<wire::RetainGenerationResponse, HostedFsError> {
        self.retain(identity.into(), idempotency_key, false).await
    }

    /// Retains this exact generation as an opaque pin.
    pub async fn pin(
        &self,
        identity: impl Into<String>,
        idempotency_key: IdempotencyKey,
    ) -> Result<wire::RetainGenerationResponse, HostedFsError> {
        self.retain(identity.into(), idempotency_key, true).await
    }

    async fn retain(
        &self,
        identity: String,
        idempotency_key: IdempotencyKey,
        pin: bool,
    ) -> Result<wire::RetainGenerationResponse, HostedFsError> {
        let request = self
            .workspace
            .filesystem
            .request(wire::RetainGenerationRequest {
                generation: Some(self.reference.clone()),
                identity,
                operation: Some(operation(idempotency_key)),
            });
        let mut client = self.workspace.filesystem.client.clone();
        if pin {
            Ok(client.pin(request).await?.into_inner())
        } else {
            Ok(client.checkpoint(request).await?.into_inner())
        }
    }
}

/// Sparse hosted transaction. Mutations remain private until one atomic commit.
pub struct HostedTransaction {
    workspace: HostedWorkspace,
    base: wire::GenerationRef,
    idempotency_key: IdempotencyKey,
    mutations: Vec<wire::Mutation>,
}

impl HostedTransaction {
    /// Adds one canonical protocol mutation. Generated mutation variants are
    /// intentionally reused so Rust and TypeScript cannot drift from the wire.
    pub fn push(&mut self, mutation: wire::mutation::Mutation) {
        self.mutations.push(wire::Mutation {
            mutation: Some(mutation),
        });
    }

    /// Creates parent directories as one transaction mutation.
    pub fn create_directories(&mut self, path: impl Into<String>) {
        self.push(wire::mutation::Mutation::CreateDirectories(
            wire::CreateDirectories { path: path.into() },
        ));
    }

    /// Creates one regular file with exact metadata.
    pub fn create_file(
        &mut self,
        path: impl Into<String>,
        contents: impl Into<Vec<u8>>,
        metadata: Option<wire::Metadata>,
    ) {
        self.push(wire::mutation::Mutation::CreateFile(wire::CreateFile {
            path: path.into(),
            contents: contents.into(),
            metadata,
        }));
    }

    /// Creates one directory with exact metadata.
    pub fn create_directory(&mut self, path: impl Into<String>, metadata: Option<wire::Metadata>) {
        self.push(wire::mutation::Mutation::CreateDirectory(
            wire::CreateDirectory {
                path: path.into(),
                metadata,
            },
        ));
    }

    /// Creates one symbolic link without interpreting its target bytes.
    pub fn create_symbolic_link(
        &mut self,
        path: impl Into<String>,
        target: impl Into<Vec<u8>>,
        metadata: Option<wire::Metadata>,
    ) {
        self.push(wire::mutation::Mutation::CreateSymbolicLink(
            wire::CreateSymbolicLink {
                path: path.into(),
                target: target.into(),
                metadata,
            },
        ));
    }

    /// Creates or replaces one complete regular file.
    pub fn put_file(&mut self, path: impl Into<String>, contents: impl Into<Vec<u8>>) {
        self.push(wire::mutation::Mutation::PutFile(wire::PutFile {
            path: path.into(),
            contents: contents.into(),
        }));
    }

    /// Removes one path.
    pub fn remove(&mut self, path: impl Into<String>) {
        self.push(wire::mutation::Mutation::Remove(wire::Remove {
            path: path.into(),
        }));
    }

    /// Atomically renames one path.
    pub fn rename(
        &mut self,
        source: impl Into<String>,
        destination: impl Into<String>,
        replace: bool,
    ) {
        self.push(wire::mutation::Mutation::Rename(wire::Rename {
            source: source.into(),
            destination: destination.into(),
            replace,
        }));
    }

    /// Creates a hard link without copying file contents.
    pub fn hard_link(&mut self, source: impl Into<String>, destination: impl Into<String>) {
        self.push(wire::mutation::Mutation::HardLink(wire::HardLink {
            source: source.into(),
            destination: destination.into(),
        }));
    }

    /// Copies one complete file through shared immutable extent references.
    pub fn copy_file(&mut self, source: impl Into<String>, destination: impl Into<String>) {
        self.push(wire::mutation::Mutation::CopyFile(wire::CopyFile {
            source: source.into(),
            destination: destination.into(),
        }));
    }

    /// Writes one sparse range.
    pub fn write(&mut self, path: impl Into<String>, offset: u64, contents: impl Into<Vec<u8>>) {
        self.push(wire::mutation::Mutation::Write(wire::Write {
            path: path.into(),
            offset,
            contents: contents.into(),
        }));
    }

    /// Changes logical file length without materializing holes.
    pub fn resize(&mut self, path: impl Into<String>, logical_bytes: u64) {
        self.push(wire::mutation::Mutation::Resize(wire::Resize {
            path: path.into(),
            logical_bytes,
        }));
    }

    /// Converts one range to a hole or allocated zeros.
    pub fn zero_range(
        &mut self,
        path: impl Into<String>,
        offset: u64,
        length: u64,
        allocated: bool,
        extend: bool,
    ) {
        self.push(wire::mutation::Mutation::ZeroRange(wire::ZeroRange {
            path: path.into(),
            range: Some(wire::ByteRange { offset, length }),
            allocated,
            extend,
        }));
    }

    /// Preallocates one sparse file range.
    pub fn preallocate(
        &mut self,
        path: impl Into<String>,
        offset: u64,
        length: u64,
        keep_size: bool,
    ) {
        self.push(wire::mutation::Mutation::Preallocate(wire::Preallocate {
            path: path.into(),
            range: Some(wire::ByteRange { offset, length }),
            keep_size,
        }));
    }

    /// Clones one content range without copying unchanged object bodies.
    pub fn clone_range(
        &mut self,
        source: impl Into<String>,
        source_offset: u64,
        destination: impl Into<String>,
        destination_offset: u64,
        length: u64,
    ) {
        self.push(wire::mutation::Mutation::CloneRange(wire::CloneRange {
            source: source.into(),
            source_offset,
            destination: destination.into(),
            destination_offset,
            length,
        }));
    }

    /// Replaces explicitly represented metadata fields.
    pub fn set_metadata(&mut self, path: impl Into<String>, metadata: wire::Metadata) {
        self.push(wire::mutation::Mutation::SetMetadata(wire::SetMetadata {
            path: path.into(),
            metadata: Some(metadata),
        }));
    }

    /// Publishes every accumulated mutation atomically.
    pub async fn commit(
        self,
        maximum_conflicts: u32,
    ) -> Result<wire::MutationResponse, HostedFsError> {
        self.workspace
            .filesystem
            .require_transaction_bound(self.mutations.len())?;
        self.workspace
            .filesystem
            .require_page_bound(maximum_conflicts, "transaction conflicts")?;
        let mut client = self.workspace.filesystem.client.clone();
        Ok(client
            .apply_transaction(
                self.workspace
                    .filesystem
                    .request(wire::ApplyTransactionRequest {
                        base: Some(self.base),
                        mutations: self.mutations,
                        operation: Some(operation(self.idempotency_key)),
                        maximum_conflicts,
                    }),
            )
            .await?
            .into_inner())
    }

    /// Validates replay against the current head and returns exact conflicts
    /// without publishing the transaction.
    pub async fn rebase(
        &self,
        maximum_conflicts: u32,
    ) -> Result<wire::RebaseTransactionResponse, HostedFsError> {
        self.workspace
            .filesystem
            .require_transaction_bound(self.mutations.len())?;
        self.workspace
            .filesystem
            .require_page_bound(maximum_conflicts, "transaction conflicts")?;
        let mut client = self.workspace.filesystem.client.clone();
        Ok(client
            .rebase_transaction(
                self.workspace
                    .filesystem
                    .request(wire::RebaseTransactionRequest {
                        base: Some(self.base.clone()),
                        mutations: self.mutations.clone(),
                        maximum_conflicts,
                        operation: Some(operation(self.idempotency_key)),
                    }),
            )
            .await?
            .into_inner())
    }
}

fn validate_handshake(
    response: wire::HandshakeResponse,
) -> Result<wire::Capabilities, HostedFsError> {
    let handshake = response.harness.ok_or(HostedFsError::InvalidResponse(
        "handshake response is absent",
    ))?;
    let protocol = handshake.protocol.ok_or(HostedFsError::InvalidResponse(
        "handshake protocol is absent",
    ))?;
    if protocol.version != "1" {
        return Err(HostedFsError::InvalidResponse(
            "filesystem protocol version is unsupported",
        ));
    }
    if protocol.descriptor_digest != crate::descriptor_digest() {
        return Err(HostedFsError::InvalidResponse(
            "filesystem descriptor digest does not match",
        ));
    }
    let supported = handshake.supported.ok_or(HostedFsError::InvalidResponse(
        "supported capabilities are absent",
    ))?;
    if !supported
        .capabilities
        .iter()
        .any(|capability| capability.name == "filesystem" && capability.version == "1")
    {
        return Err(HostedFsError::InvalidResponse(
            "filesystem capability version is unsupported",
        ));
    }
    let capabilities = response.capabilities.ok_or(HostedFsError::InvalidResponse(
        "filesystem capabilities are absent",
    ))?;
    if capabilities.contract_version != "1" {
        return Err(HostedFsError::InvalidResponse(
            "filesystem contract version is unsupported",
        ));
    }
    if capabilities.maximum_request_bytes == 0
        || capabilities.maximum_response_bytes == 0
        || capabilities.maximum_transaction_mutations == 0
        || capabilities.maximum_page_items == 0
    {
        return Err(HostedFsError::InvalidResponse(
            "filesystem capabilities contain an unbounded limit",
        ));
    }
    Ok(capabilities)
}

fn operation(idempotency_key: IdempotencyKey) -> wire::OperationOptions {
    wire::OperationOptions {
        idempotency_key: idempotency_key.into_bytes().to_vec(),
    }
}

fn profile_to_wire(profile: EmbeddedProfile) -> wire::FilesystemProfile {
    match profile {
        EmbeddedProfile::Portable => wire::FilesystemProfile::Portable,
        EmbeddedProfile::Posix => wire::FilesystemProfile::Posix,
        EmbeddedProfile::Windows => wire::FilesystemProfile::Windows,
        EmbeddedProfile::Browser => wire::FilesystemProfile::Browser,
    }
}

fn validate_generation(
    generation: &wire::GenerationRef,
    workspace: &wire::WorkspaceRef,
) -> Result<(), HostedFsError> {
    exact_len(
        &generation.generation_id,
        32,
        "generation identity has the wrong length",
    )?;
    let owner = generation
        .workspace
        .as_ref()
        .ok_or(HostedFsError::InvalidResponse(
            "generation workspace is absent",
        ))?;
    if owner != workspace {
        return Err(HostedFsError::InvalidResponse(
            "generation belongs to another workspace",
        ));
    }
    Ok(())
}

fn exact_len(value: &[u8], expected: usize, message: &'static str) -> Result<(), HostedFsError> {
    if value.len() == expected {
        Ok(())
    } else {
        Err(HostedFsError::InvalidResponse(message))
    }
}

#[cfg(all(test, feature = "memory", feature = "distributed"))]
mod tests {
    use super::*;
    use crate::wire::filesystem::v2::filesystem_service_server::FilesystemServiceServer;
    use crate::{
        EmbeddedCapabilities, FilesystemSourceProvider, FilesystemWireLimits,
        FilesystemWireService, HostedSourceOperation, HostedSourceScope,
    };
    use rcgen::generate_simple_self_signed;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::{Identity, Server, ServerTlsConfig};

    struct ClientTestSourceProvider {
        generation: std::sync::Mutex<Option<GenerationId>>,
    }

    #[tonic::async_trait]
    impl FilesystemSourceProvider for ClientTestSourceProvider {
        async fn state(&self, _scope: HostedSourceScope) -> Result<HostedSourceResult, Status> {
            Ok(HostedSourceResult {
                state: HostedSourceState::NeedsRescan(HostedSourceInvalidation::QueueOverflow),
                generation_id: None,
            })
        }

        async fn reconcile(
            &self,
            _operation: HostedSourceOperation,
        ) -> Result<HostedSourceResult, Status> {
            Ok(HostedSourceResult {
                state: HostedSourceState::Clean,
                generation_id: *self
                    .generation
                    .lock()
                    .map_err(|_| Status::internal("lock"))?,
            })
        }

        async fn rescan(
            &self,
            operation: HostedSourceOperation,
        ) -> Result<HostedSourceResult, Status> {
            self.reconcile(operation).await
        }

        async fn seal(
            &self,
            _operation: HostedSourceOperation,
        ) -> Result<HostedSourceResult, Status> {
            Ok(HostedSourceResult {
                state: HostedSourceState::Sealed,
                generation_id: *self
                    .generation
                    .lock()
                    .map_err(|_| Status::internal("lock"))?,
            })
        }
    }

    #[tokio::test]
    async fn hosted_constructor_uses_the_same_canonical_engine_over_real_grpc()
    -> Result<(), Box<dyn std::error::Error>> {
        let embedded = crate::MemoryFs::memory();
        let limits = FilesystemWireLimits {
            maximum_response_bytes: 1_024,
            maximum_transaction_mutations: 2,
            maximum_page_items: 2,
            ..FilesystemWireLimits::default()
        };
        let source_provider = Arc::new(ClientTestSourceProvider {
            generation: std::sync::Mutex::new(None),
        });
        let service = FilesystemServiceServer::new(
            FilesystemWireService::new(embedded, limits)?
                .with_source_provider(source_provider.clone()),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stop_rx.await;
                })
                .await
        });

        let endpoint = format!("http://{address}");
        let mut minimum_options = HostedFsOptions::new(&endpoint, "test-account-token");
        minimum_options.maximum_response_bytes = MINIMUM_HANDSHAKE_RESPONSE_BYTES;
        let minimum_hosted = Fs::hosted(minimum_options).await?;
        assert_eq!(
            minimum_hosted.capabilities().maximum_response_bytes,
            u64::try_from(MINIMUM_HANDSHAKE_RESPONSE_BYTES)? - MAX_BYTE_RESPONSE_ENVELOPE_BYTES
        );

        let mut options = HostedFsOptions::new(endpoint, "test-account-token");
        options.maximum_response_bytes = 1_024 + usize::try_from(MAX_BYTE_RESPONSE_ENVELOPE_BYTES)?;
        let hosted = Fs::hosted(options).await?;
        assert_eq!(hosted.capabilities().maximum_response_bytes, 1_024);
        assert_eq!(hosted.capabilities().maximum_transaction_mutations, 2);
        assert_eq!(hosted.capabilities().maximum_page_items, 2);
        let workspace = hosted
            .create_workspace(
                "hosted",
                EmbeddedProfile::Portable,
                IdempotencyKey::from_bytes([1; 16]),
            )
            .await?;
        let head_bytes: [u8; 32] = workspace.head.generation_id.as_slice().try_into()?;
        *source_provider
            .generation
            .lock()
            .map_err(|_| "source provider lock poisoned")? =
            Some(GenerationId::new(Digest::from_bytes(head_bytes)));
        assert_eq!(
            workspace.source_state().await?.state,
            HostedSourceState::NeedsRescan(HostedSourceInvalidation::QueueOverflow)
        );
        let reconciled = workspace
            .reconcile_source(IdempotencyKey::from_bytes([15; 16]))
            .await?;
        assert_eq!(reconciled.state, HostedSourceState::Clean);
        assert_eq!(
            reconciled.generation_id,
            *source_provider
                .generation
                .lock()
                .map_err(|_| "source provider lock poisoned")?
        );
        let rescanned = workspace
            .rescan_source(IdempotencyKey::from_bytes([16; 16]))
            .await?;
        assert_eq!(rescanned.state, HostedSourceState::Clean);
        let sealed = workspace.seal(IdempotencyKey::from_bytes([17; 16])).await?;
        assert_eq!(sealed.id(), workspace.head.generation_id);
        let mut transaction = workspace.begin_transaction(IdempotencyKey::from_bytes([2; 16]));
        transaction.put_file("/value", vec![b'x'; 1_024]);
        let outcome = transaction.commit(1).await?;
        assert!(matches!(
            wire::MutationStatus::try_from(outcome.status),
            Ok(wire::MutationStatus::Committed)
        ));
        let workspace = hosted.open_workspace("hosted").await?;
        let head = workspace.head().await?;
        assert_eq!(head.read("/value", 1_024).await?.len(), 1_024);
        let child = head
            .fork("child", IdempotencyKey::from_bytes([3; 16]))
            .await?;
        let sibling = head
            .fork("sibling", IdempotencyKey::from_bytes([6; 16]))
            .await?;
        let stale_target = head
            .fork("stale-target", IdempotencyKey::from_bytes([14; 16]))
            .await?;
        assert_eq!(
            child.head().await?.read("/value", 1_024).await?.len(),
            1_024
        );
        let child_before = child.head().await?;
        let mut child_change = child.begin_transaction(IdempotencyKey::from_bytes([4; 16]));
        child_change.write("/value", 0, b"joined".to_vec());
        let child_change = child_change.commit(1).await?;
        assert_eq!(child_change.status, wire::MutationStatus::Committed as i32);
        let child = hosted.open_workspace("child").await?;
        let child_after = child.head().await?;
        let child_diff = child_before.diff(&child_after, 2).await?;
        assert_eq!(child_diff.from.as_ref(), Some(&child_before.reference));
        assert_eq!(child_diff.to.as_ref(), Some(&child_after.reference));
        assert_eq!(child_diff.files.len(), 1);
        assert!(child_diff.bindings.is_empty());
        assert!(!child_diff.truncated);
        let plan = child_after
            .plan_join(&workspace.head().await?, wire::JoinHistory::Merge, 2, 2, 2)
            .await?;
        let mut tampered = plan.clone();
        let Some(common_ancestor) = tampered.common_ancestor.as_mut() else {
            return Err("join plan has no common ancestor".into());
        };
        let Some(identity_byte) = common_ancestor.generation_id.first_mut() else {
            return Err("generation identity is empty".into());
        };
        *identity_byte ^= 0x01;
        assert!(matches!(
            hosted
                .apply_join(tampered, IdempotencyKey::from_bytes([7; 16]))
                .await,
            Err(HostedFsError::Status(status)) if status.code() == tonic::Code::InvalidArgument
        ));
        let stale_plan = child_after
            .plan_join(
                &stale_target.head().await?,
                wire::JoinHistory::Merge,
                2,
                2,
                2,
            )
            .await?;
        let mut target_change =
            stale_target.begin_transaction(IdempotencyKey::from_bytes([12; 16]));
        target_change.create_directories("/target-only");
        let target_change = target_change.commit(1).await?;
        assert_eq!(target_change.status, wire::MutationStatus::Committed as i32);
        let target_before_stale_apply = stale_target.head().await?;
        let stale = hosted
            .apply_join(stale_plan, IdempotencyKey::from_bytes([8; 16]))
            .await?;
        assert_eq!(stale.status, wire::JoinStatus::StaleTarget as i32);
        assert_eq!(
            stale.generation.as_ref(),
            Some(&target_before_stale_apply.reference)
        );
        let target_after_stale_apply = stale_target.head().await?;
        assert_eq!(
            target_after_stale_apply.reference,
            target_before_stale_apply.reference
        );
        assert_eq!(
            target_after_stale_apply.read("/value", 1_024).await?,
            vec![b'x'; 1_024]
        );
        target_after_stale_apply.stat("/target-only").await?;
        let joined = hosted
            .apply_join(plan, IdempotencyKey::from_bytes([13; 16]))
            .await?;
        assert_eq!(joined.status, wire::MutationStatus::Committed as i32);
        let workspace = hosted.open_workspace("hosted").await?;
        assert!(
            workspace
                .head()
                .await?
                .read("/value", 1_024)
                .await?
                .starts_with(b"joined")
        );
        let sibling_plan = child_after
            .plan_join(&sibling.head().await?, wire::JoinHistory::Merge, 2, 2, 2)
            .await?;
        let Some(sibling_ancestor) = sibling_plan.common_ancestor.as_ref() else {
            return Err("sibling join plan has no common ancestor".into());
        };
        assert!(sibling_ancestor.workspace.is_none());
        let sibling_joined = hosted
            .apply_join(sibling_plan, IdempotencyKey::from_bytes([9; 16]))
            .await?;
        assert_eq!(
            sibling_joined.status,
            wire::MutationStatus::Committed as i32
        );
        let sibling = hosted.open_workspace("sibling").await?;
        assert!(
            sibling
                .head()
                .await?
                .read("/value", 1_024)
                .await?
                .starts_with(b"joined")
        );
        assert!(matches!(
            child.head().await?.read("/value", 1_025).await,
            Err(HostedFsError::LimitExceeded("read bytes"))
        ));
        assert!(matches!(
            child.head().await?.list_directory("/", None, 3).await,
            Err(HostedFsError::LimitExceeded("directory items"))
        ));
        let mut oversized = child.begin_transaction(IdempotencyKey::from_bytes([10; 16]));
        oversized.create_directories("/one");
        oversized.create_directories("/two");
        oversized.create_directories("/three");
        assert!(matches!(
            oversized.commit(1).await,
            Err(HostedFsError::LimitExceeded("transaction mutations"))
        ));
        let empty = child.begin_transaction(IdempotencyKey::from_bytes([11; 16]));
        assert!(matches!(
            empty.commit(1).await,
            Err(HostedFsError::LimitExceeded("transaction mutations"))
        ));

        stop_tx
            .send(())
            .map_err(|()| "hosted test server disappeared")?;
        server.await??;
        let _ = EmbeddedCapabilities::MEMORY;
        Ok(())
    }

    #[tokio::test]
    async fn private_ca_https_constructor_requires_handshake_and_exact_bearer()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let certified = generate_simple_self_signed(["localhost".to_owned()])?;
        let certificate_pem = certified.cert.pem();
        let private_key_pem = certified.signing_key.serialize_pem();
        let embedded = crate::MemoryFs::memory();
        let service = FilesystemServiceServer::with_interceptor(
            FilesystemWireService::new(embedded, FilesystemWireLimits::default())?,
            |request: Request<()>| {
                if request
                    .metadata()
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    != Some("Bearer exact-token")
                {
                    return Err(Status::unauthenticated("missing exact bearer credential"));
                }
                Ok(request)
            },
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let server_certificate_pem = certificate_pem.clone();
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(
                    ServerTlsConfig::new()
                        .identity(Identity::from_pem(server_certificate_pem, private_key_pem)),
                )?
                .add_service(service)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stop_rx.await;
                })
                .await
        });
        let endpoint = format!("https://localhost:{}", address.port());

        assert!(
            Fs::hosted(HostedFsOptions::new(&endpoint, "exact-token"))
                .await
                .is_err()
        );
        assert!(matches!(
            Fs::hosted(
                HostedFsOptions::new(&endpoint, "wrong-token")
                    .with_ca_certificate(certificate_pem.clone())
            )
            .await,
            Err(HostedFsError::Status(status)) if status.code() == tonic::Code::Unauthenticated
        ));
        let hosted = Fs::hosted(
            HostedFsOptions::new(endpoint, "exact-token").with_ca_certificate(certificate_pem),
        )
        .await?;
        assert_eq!(hosted.capabilities().contract_version, "1");

        stop_tx
            .send(())
            .map_err(|()| "hosted TLS test server disappeared")?;
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn hosted_constructor_rejects_unbounded_ca_and_malformed_handshake()
    -> Result<(), HostedFsError> {
        let debug = format!(
            "{:?}",
            HostedFsOptions::new("https://localhost", "secret-token")
        );
        assert!(!debug.contains("secret-token"));
        assert!(debug.contains("[REDACTED]"));
        let oversized = HostedFsOptions::new("https://localhost", "token")
            .with_ca_certificate(vec![b'x'; MAX_CA_CERTIFICATE_BYTES + 1]);
        assert!(matches!(
            Fs::hosted(oversized).await,
            Err(HostedFsError::InvalidOptions(
                "CA certificate must contain 1 to 65536 bytes"
            ))
        ));

        let mut undersized = HostedFsOptions::new("https://localhost", "token");
        undersized.maximum_response_bytes = MINIMUM_HANDSHAKE_RESPONSE_BYTES - 1;
        assert!(matches!(
            Fs::hosted(undersized).await,
            Err(HostedFsError::InvalidOptions(
                "request bound must be nonzero and response bound must admit the handshake"
            ))
        ));

        let largest_byte_envelope = wire::ExportChunk {
            cursor: vec![0; 8],
            object_id: vec![0; 33],
            contents: vec![0; 1_024],
            terminal: true,
        };
        assert!(
            u64::try_from(largest_byte_envelope.encoded_len()).unwrap_or(u64::MAX)
                <= 1_024 + MAX_BYTE_RESPONSE_ENVELOPE_BYTES
        );

        let malformed = wire::HandshakeResponse {
            harness: Some(harness::HandshakeResponse {
                protocol: Some(harness::ProtocolIdentity {
                    version: "1".to_owned(),
                    descriptor_digest: "substituted".to_owned(),
                }),
                supported: Some(harness::CapabilitySet {
                    capabilities: vec![harness::Capability {
                        name: "filesystem".to_owned(),
                        version: "1".to_owned(),
                    }],
                }),
            }),
            capabilities: Some(wire::Capabilities {
                contract_version: "1".to_owned(),
                profiles: Vec::new(),
                maximum_request_bytes: 1,
                maximum_response_bytes: 1,
                maximum_transaction_mutations: 1,
                maximum_page_items: 1,
                native_mount_credentials: false,
                s3_credentials: false,
                source_reconciliation: false,
            }),
        };
        assert!(matches!(
            validate_handshake(malformed),
            Err(HostedFsError::InvalidResponse(
                "filesystem descriptor digest does not match"
            ))
        ));

        let access = hosted_s3_access(wire::CredentialResponse {
            endpoint: "https://s3.example.test".to_owned(),
            expires_at_unix_seconds: 1_900_000_000,
            credential: Some(wire::credential_response::Credential::S3(
                wire::S3Credential {
                    bucket: "workspace".to_owned(),
                    region: "eu-west-2".to_owned(),
                    access_key_id: "visible-only-to-caller".to_owned(),
                    secret_access_key: "never-log-this-secret".to_owned(),
                    session_token: String::new(),
                },
            )),
        })?;
        assert_eq!(access.region, "eu-west-2");
        assert!(access.session_token.is_empty());
        let debug = format!("{access:?}");
        assert!(!debug.contains("visible-only-to-caller"));
        assert!(!debug.contains("never-log-this-secret"));
        assert!(debug.matches("[REDACTED]").count() >= 3);

        assert!(matches!(
            hosted_s3_access(wire::CredentialResponse {
                endpoint: "https://s3.example.test".to_owned(),
                expires_at_unix_seconds: 1_900_000_000,
                credential: Some(wire::credential_response::Credential::BearerToken(
                    "wrong-kind".to_owned()
                )),
            }),
            Err(HostedFsError::InvalidResponse(
                "S3 credential response has the wrong credential kind"
            ))
        ));
        Ok(())
    }
}
