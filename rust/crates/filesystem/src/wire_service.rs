//! Canonical generated-wire translation for every remote filesystem deployment.

use crate::hosted_contract;
use crate::kernel::{
    FileKind as EngineFileKind, FileMetadata, FilePayload, LogicalName, MetadataField, TreeEntry,
};
use crate::model::{FilesystemProfile as EngineProfile, Lifecycle, VolumeConfig};
use acyclic_fs_wire::wire::{filesystem::v2 as wire, protocol::v1 as protocol};
use crate::{
    ApplyOptions, AsyncAuthorityStore, AsyncObjectStore, ByteRange, CancellationToken,
    DependencyRegion, DependencyState, Digest, DurableCommit, ForkOptions, Fs, Generation,
    GenerationId, IdempotencyKey, JoinHistory, JoinOutcome, MergeConflict, ObjectId,
    RebaseConflict, Sequence, Transaction, TransactionCommit, TransactionRebase, WorkBudget,
    Workspace, WorkspaceDelete, WorkspaceError, WorkspaceMetadata, WorkspaceRebase,
};
use bytes::Bytes;
use futures::stream;
use prost::Message;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status};

type WireStream<T> = Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Version of the public Filesystem protocol identity.
pub const FILESYSTEM_PROTOCOL_VERSION: &str = "1";
/// Default maximum encoded response accepted by the hosted Filesystem service.
pub const DEFAULT_HOSTED_MAXIMUM_RESPONSE_BYTES: u64 = 16 * 1024 * 1024;
/// Default maximum page size advertised by the hosted Filesystem service.
pub const DEFAULT_HOSTED_MAXIMUM_PAGE_ITEMS: u32 = 1_024;

enum JoinTargetHead<A, O> {
    Expected {
        head: crate::Head,
        existing_operation: bool,
    },
    Stale(Generation<A, O>),
}

/// Hard bounds and explicitly available optional hosted capabilities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FilesystemWireLimits {
    /// Largest accepted encoded request.
    pub maximum_request_bytes: u64,
    /// Largest response body or aggregate page.
    pub maximum_response_bytes: u64,
    /// Largest atomic transaction.
    pub maximum_transaction_mutations: u32,
    /// Largest directory, diff, or transfer page.
    pub maximum_page_items: u32,
    /// Longest accepted lifetime for a scoped deployment credential.
    pub maximum_credential_seconds: u64,
}

impl Default for FilesystemWireLimits {
    fn default() -> Self {
        Self {
            maximum_request_bytes: 16 * 1024 * 1024,
            maximum_response_bytes: DEFAULT_HOSTED_MAXIMUM_RESPONSE_BYTES,
            maximum_transaction_mutations: 2_048,
            maximum_page_items: DEFAULT_HOSTED_MAXIMUM_PAGE_ITEMS,
            maximum_credential_seconds: 3_600,
        }
    }
}

/// Deployment-owned credential surface selected after SDK validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialKind {
    /// Native mount transport credential.
    Mount,
    /// Filesystem S3 compatibility credential.
    S3,
}

/// Fully validated scope passed to the authenticated deployment edge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialGrantRequest {
    /// Requested surface.
    pub kind: CredentialKind,
    /// Stable workspace identity.
    pub workspace_id: crate::WorkspaceId,
    /// Canonical workspace name.
    pub workspace_name: crate::WorkspaceName,
    /// Exact generation visible through the credential.
    pub generation_id: GenerationId,
    /// Whether mutation is requested.
    pub writable: bool,
    /// Requested bounded lifetime.
    pub expires_after_seconds: u64,
    /// Absolute ceiling intersected with policy and the requested lifetime.
    pub expires_at_unix_seconds: i64,
    /// Stable caller retry identity.
    pub idempotency_key: IdempotencyKey,
}

/// Deployment-produced scoped credential material.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CredentialGrant {
    /// Opaque bearer capability for the native mount transport.
    Bearer {
        /// Customer endpoint for the mount transport.
        endpoint: String,
        /// Opaque bearer capability.
        token: String,
        /// Absolute Unix expiry enforced by the endpoint.
        expires_at_unix_seconds: i64,
    },
    /// Standard S3 `SigV4` coordinates scoped to one workspace.
    S3 {
        /// Customer S3 endpoint.
        endpoint: String,
        /// Opaque bucket coordinate.
        bucket: String,
        /// Signing region.
        region: String,
        /// Scoped access-key identity.
        access_key_id: String,
        /// Scoped secret access key.
        secret_access_key: String,
        /// Optional STS-compatible session token; empty when unused.
        session_token: String,
        /// Absolute Unix expiry enforced by the endpoint.
        expires_at_unix_seconds: i64,
    },
}

/// Authenticated deployment extension for private credential issuance.
#[tonic::async_trait]
pub trait FilesystemCredentialIssuer: Send + Sync + 'static {
    /// Whether native mount credentials are available.
    fn mount_enabled(&self) -> bool;

    /// Whether S3 credentials are available.
    fn s3_enabled(&self) -> bool;

    /// Issues one opaque credential for an already validated exact scope.
    async fn issue(&self, request: CredentialGrantRequest) -> Result<CredentialGrant, Status>;
}

/// Deployment-owned source identity after the SDK authenticates its workspace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostedSourceScope {
    /// Stable workspace identity.
    pub workspace_id: crate::WorkspaceId,
    /// Canonical workspace name.
    pub workspace_name: crate::WorkspaceName,
}

/// Stable operation identity for a hosted source mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostedSourceOperation {
    /// Authenticated source workspace.
    pub scope: HostedSourceScope,
    /// Caller retry identity retained across ambiguous outcomes.
    pub idempotency_key: IdempotencyKey,
}

/// Why a hosted source can no longer prove a contiguous change interval.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostedSourceInvalidation {
    /// No authenticated baseline has been established.
    InitialSnapshotRequired,
    /// The bounded change queue overflowed.
    QueueOverflow,
    /// The provider explicitly requires a full rescan.
    NativeRescanRequired,
    /// The provider reported an asynchronous backend failure.
    BackendError,
    /// A source path cannot be represented by filesystem semantics.
    UnrepresentablePath,
    /// A rename could not be paired exactly.
    AmbiguousRename,
    /// The source root changed identity.
    RootChanged,
}

/// Canonical state returned by a deployment source provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostedSourceState {
    /// Source and workspace agree.
    Clean,
    /// A bounded capture is in progress.
    PendingCapture,
    /// Continuity was lost.
    NeedsRescan(HostedSourceInvalidation),
    /// Source and workspace publications overlap.
    Conflict,
    /// The generation is independent of the source.
    Sealed,
}

/// One provider result, optionally naming the exact resulting generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostedSourceResult {
    /// Resulting semantic state.
    pub state: HostedSourceState,
    /// Exact workspace generation for clean or sealed outcomes.
    pub generation_id: Option<GenerationId>,
}

/// Deployment extension for a remotely owned attached source.
#[tonic::async_trait]
pub trait FilesystemSourceProvider: Send + Sync + 'static {
    /// Returns the current source state without mutation.
    async fn state(&self, scope: HostedSourceScope) -> Result<HostedSourceResult, Status>;
    /// Captures one bounded contiguous source interval.
    async fn reconcile(
        &self,
        operation: HostedSourceOperation,
    ) -> Result<HostedSourceResult, Status>;
    /// Rebuilds a complete authenticated source baseline.
    async fn rescan(&self, operation: HostedSourceOperation) -> Result<HostedSourceResult, Status>;
    /// Produces a generation independent of the attached source.
    async fn seal(&self, operation: HostedSourceOperation) -> Result<HostedSourceResult, Status>;
}

/// One SDK-owned protocol adapter over the canonical filesystem engine.
///
/// Deployments wrap this adapter with authentication, quota, credential
/// issuance, and transport policy. Filesystem semantics stay here.
#[derive(Clone)]
pub struct FilesystemWireService<A, O> {
    filesystem: Fs<A, O>,
    limits: FilesystemWireLimits,
    credential_issuer: Option<Arc<dyn FilesystemCredentialIssuer>>,
    source_provider: Option<Arc<dyn FilesystemSourceProvider>>,
}

impl<A, O> FilesystemWireService<A, O> {
    /// Binds one canonical engine and validates finite protocol bounds.
    ///
    /// # Errors
    ///
    /// Rejects a zero bound.
    pub fn new(filesystem: Fs<A, O>, limits: FilesystemWireLimits) -> Result<Self, Status> {
        if limits.maximum_request_bytes == 0
            || limits.maximum_response_bytes == 0
            || limits.maximum_transaction_mutations == 0
            || limits.maximum_page_items == 0
            || limits.maximum_credential_seconds == 0
        {
            return Err(Status::invalid_argument(
                "filesystem wire bounds must be nonzero",
            ));
        }
        Ok(Self {
            filesystem,
            limits,
            credential_issuer: None,
            source_provider: None,
        })
    }

    /// Borrows the exact engine served by this adapter.
    #[must_use]
    pub const fn filesystem(&self) -> &Fs<A, O> {
        &self.filesystem
    }

    /// Installs the authenticated deployment's scoped credential issuer.
    #[must_use]
    pub fn with_credential_issuer(mut self, issuer: Arc<dyn FilesystemCredentialIssuer>) -> Self {
        self.credential_issuer = Some(issuer);
        self
    }

    /// Installs the deployment's authenticated hosted-source provider.
    #[must_use]
    pub fn with_source_provider(mut self, provider: Arc<dyn FilesystemSourceProvider>) -> Self {
        self.source_provider = Some(provider);
        self
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemWireService<A, O> {
    async fn workspace(
        &self,
        reference: Option<wire::WorkspaceRef>,
    ) -> Result<Workspace<A, O>, Status> {
        let reference = required(reference, "workspace")?;
        let workspace = self
            .filesystem
            .open_workspace(
                &reference.name,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| status(&failure.error))?
            .value;
        if workspace.id().into_bytes().as_slice() != reference.workspace_id {
            return Err(Status::failed_precondition(
                "workspace name and identity disagree",
            ));
        }
        workspace
            .bind_coarse_access(&crate::access::Action::ALL, &CancellationToken::new())
            .await
            .map_err(|error| status(&error))
    }

    async fn source_scope(
        &self,
        reference: Option<wire::WorkspaceRef>,
    ) -> Result<(Workspace<A, O>, HostedSourceScope), Status> {
        let workspace = self.workspace(reference).await?;
        let scope = HostedSourceScope {
            workspace_id: workspace.id(),
            workspace_name: workspace.name().clone(),
        };
        Ok((workspace, scope))
    }

    fn source_provider(&self) -> Result<&Arc<dyn FilesystemSourceProvider>, Status> {
        self.source_provider
            .as_ref()
            .ok_or_else(|| Status::unimplemented("hosted source reconciliation is unavailable"))
    }

    async fn source_response(
        &self,
        workspace: &Workspace<A, O>,
        result: HostedSourceResult,
    ) -> Result<wire::SourceResponse, Status> {
        let generation = match result.generation_id {
            Some(id) => Some(generation_ref(
                &workspace
                    .generation(id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(|failure| status(&failure.error))?
                    .value,
            )),
            None => None,
        };
        let (state, reason) = source_state_message(result.state);
        let requires_generation = matches!(
            result.state,
            HostedSourceState::Clean | HostedSourceState::Sealed
        );
        if requires_generation != generation.is_some() {
            return Err(Status::data_loss(
                "hosted source result generation does not match its state",
            ));
        }
        Ok(wire::SourceResponse {
            state: state as i32,
            reason: reason as i32,
            generation,
        })
    }

    async fn generation(
        &self,
        reference: Option<wire::GenerationRef>,
    ) -> Result<Generation<A, O>, Status> {
        let reference = required(reference, "generation")?;
        let workspace = self.workspace(reference.workspace).await?;
        let id = generation_id(&reference.generation_id)?;
        workspace
            .generation(id, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| status(&failure.error))
            .map(|receipt| receipt.value)
    }

    async fn join_target_head(
        &self,
        target: &Workspace<A, O>,
        expected: GenerationId,
        operation: IdempotencyKey,
    ) -> Result<JoinTargetHead<A, O>, Status> {
        let committed = self
            .filesystem
            .observe_volume_operation(
                target.id().volume_id(),
                operation.operation_id(),
                crate::WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|error| status(&WorkspaceError::engine(error)))?;
        if let Some(commit) = committed.value {
            let sequence = commit
                .sequence
                .get()
                .checked_sub(1)
                .ok_or_else(|| Status::data_loss("operation has invalid authority sequence"))?;
            return Ok(JoinTargetHead::Expected {
                head: crate::Head {
                    epoch: commit.epoch,
                    sequence: Sequence::new(sequence),
                    digest: commit.previous_digest,
                },
                existing_operation: true,
            });
        }
        let cancellation = CancellationToken::new();
        let mut accounting =
            crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let (current, head) = self
            .filesystem
            .workspace_head_state(&target.volume, &mut accounting, &cancellation)
            .await
            .map_err(|error| status(&error))?;
        if current == expected {
            Ok(JoinTargetHead::Expected {
                head,
                existing_operation: false,
            })
        } else {
            Ok(JoinTargetHead::Stale(
                target
                    .generation(current, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(|failure| status(&failure.error))?
                    .value,
            ))
        }
    }

    fn admit<M: Message>(&self, request: &Request<M>) -> Result<(), Status> {
        self.admit_message(request.get_ref())
    }

    fn admit_message<M: Message>(&self, message: &M) -> Result<(), Status> {
        let bytes = u64::try_from(message.encoded_len()).unwrap_or(u64::MAX);
        if bytes > self.limits.maximum_request_bytes {
            return Err(Status::resource_exhausted(
                "encoded request exceeds service bound",
            ));
        }
        Ok(())
    }
}

include!("wire_service/operations.rs");
include!("wire_service/rpc_join.rs");
include!("wire_service/rpc_transfer.rs");
include!("wire_service/rpc_transaction.rs");
include!("wire_service/credentials.rs");
include!("wire_service/mutations.rs");
include!("wire_service/input.rs");
include!("wire_service/references.rs");
include!("wire_service/changes.rs");
include!("wire_service/join_encoding.rs");
include!("wire_service/output.rs");

#[cfg(test)]
mod tests;
