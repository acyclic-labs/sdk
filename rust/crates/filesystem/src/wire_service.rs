//! Canonical generated-wire translation for every remote filesystem deployment.

use crate::kernel::{FileKind as EngineFileKind, FileMetadata, MetadataField, NameEncoding};
use crate::model::{FilesystemProfile as EngineProfile, Lifecycle, VolumeConfig};
use crate::wire::{filesystem::v1 as wire, harness::v1 as harness};
use crate::{
    ApplyOptions, AsyncAuthorityStore, AsyncObjectStore, ByteRange, CancellationToken, Digest,
    DurableCommit, ForkOptions, Fs, Generation, GenerationId, IdempotencyKey, JoinHistory,
    JoinOutcome, ObjectId, ObjectKind, Transaction, TransactionCommit, Workspace, WorkspaceDelete,
    WorkspaceError, WorkspaceExtentKind, WorkspaceMetadata, WorkspaceRebase,
};
use bytes::Bytes;
use futures::{StreamExt, stream};
use prost::Message;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status};

type WireStream<T> = Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send + 'static>>;

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
            maximum_response_bytes: 16 * 1024 * 1024,
            maximum_transaction_mutations: 2_048,
            maximum_page_items: 1_024,
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
    /// Stable caller retry identity.
    pub idempotency_key: IdempotencyKey,
}

/// Deployment-produced opaque scoped credential.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialGrant {
    /// Customer endpoint for the selected surface.
    pub endpoint: String,
    /// Opaque bearer capability.
    pub token: String,
    /// Absolute Unix expiry enforced by the endpoint.
    pub expires_at_unix_seconds: i64,
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

/// One SDK-owned protocol adapter over the canonical filesystem engine.
///
/// Deployments wrap this adapter with authentication, quota, credential
/// issuance, and transport policy. Filesystem semantics stay here.
#[derive(Clone)]
pub struct FilesystemWireService<A, O> {
    filesystem: Fs<A, O>,
    limits: FilesystemWireLimits,
    credential_issuer: Option<Arc<dyn FilesystemCredentialIssuer>>,
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
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemWireService<A, O> {
    async fn workspace(
        &self,
        reference: Option<wire::WorkspaceRef>,
    ) -> Result<Workspace<A, O>, Status> {
        let reference = required(reference, "workspace")?;
        let workspace = self
            .filesystem
            .open_workspace(&reference.name)
            .await
            .map_err(status)?;
        if workspace.id().into_bytes().as_slice() != reference.workspace_id {
            return Err(Status::failed_precondition(
                "workspace name and identity disagree",
            ));
        }
        Ok(workspace)
    }

    async fn generation(
        &self,
        reference: Option<wire::GenerationRef>,
    ) -> Result<Generation<A, O>, Status> {
        let reference = required(reference, "generation")?;
        let workspace = self.workspace(reference.workspace).await?;
        let id = generation_id(&reference.generation_id)?;
        workspace.generation(id).await.map_err(status)
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

#[tonic::async_trait]
impl<A, O> wire::filesystem_service_server::FilesystemService for FilesystemWireService<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    async fn handshake(
        &self,
        request: Request<wire::HandshakeRequest>,
    ) -> Result<Response<wire::HandshakeResponse>, Status> {
        self.admit(&request)?;
        if let Some(requested) = request
            .into_inner()
            .harness
            .and_then(|value| value.protocol)
        {
            if !requested.version.is_empty() && requested.version != "1" {
                return Err(Status::failed_precondition(
                    "unsupported filesystem contract version",
                ));
            }
            let digest = descriptor_digest();
            if !requested.descriptor_digest.is_empty() && requested.descriptor_digest != digest {
                return Err(Status::failed_precondition(
                    "filesystem descriptor digest mismatch",
                ));
            }
        }
        let protocol = harness::ProtocolIdentity {
            version: "1".to_owned(),
            descriptor_digest: descriptor_digest(),
        };
        Ok(Response::new(wire::HandshakeResponse {
            harness: Some(harness::HandshakeResponse {
                protocol: Some(protocol),
                supported: Some(harness::CapabilitySet {
                    capabilities: vec![harness::Capability {
                        name: "filesystem".to_owned(),
                        version: "1".to_owned(),
                    }],
                }),
            }),
            capabilities: Some(wire::Capabilities {
                contract_version: "1".to_owned(),
                profiles: vec![
                    wire::FilesystemProfile::Portable as i32,
                    wire::FilesystemProfile::Posix as i32,
                    wire::FilesystemProfile::Windows as i32,
                    wire::FilesystemProfile::Browser as i32,
                ],
                maximum_request_bytes: self.limits.maximum_request_bytes,
                maximum_response_bytes: self.limits.maximum_response_bytes,
                maximum_transaction_mutations: self.limits.maximum_transaction_mutations,
                maximum_page_items: self.limits.maximum_page_items,
                native_mount_credentials: self
                    .credential_issuer
                    .as_ref()
                    .is_some_and(|issuer| issuer.mount_enabled()),
                s3_credentials: self
                    .credential_issuer
                    .as_ref()
                    .is_some_and(|issuer| issuer.s3_enabled()),
                source_reconciliation: false,
            }),
        }))
    }

    async fn create_workspace(
        &self,
        request: Request<wire::CreateWorkspaceRequest>,
    ) -> Result<Response<wire::WorkspaceResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let operation_id = operation(request.operation)?.operation_id();
        let profile = profile(request.profile)?;
        let lifecycle = if self.filesystem.capabilities().durable {
            Lifecycle::Durable
        } else {
            Lifecycle::Ephemeral
        };
        let mut config = VolumeConfig::portable(lifecycle);
        config.profile = profile;
        let workspace = self
            .filesystem
            .create_workspace_with_config_operation(request.name, config, Some(operation_id))
            .await
            .map_err(status)?;
        Ok(Response::new(wire::WorkspaceResponse {
            workspace: Some(workspace_message(&workspace).await?),
            status: wire::MutationStatus::Committed as i32,
        }))
    }

    async fn open_workspace(
        &self,
        request: Request<wire::OpenWorkspaceRequest>,
    ) -> Result<Response<wire::WorkspaceResponse>, Status> {
        self.admit(&request)?;
        let workspace = match required(request.into_inner().selector, "workspace selector")? {
            wire::open_workspace_request::Selector::Workspace(reference) => {
                self.workspace(Some(reference)).await?
            }
            wire::open_workspace_request::Selector::Name(name) => {
                self.filesystem.open_workspace(name).await.map_err(status)?
            }
        };
        Ok(Response::new(wire::WorkspaceResponse {
            workspace: Some(workspace_message(&workspace).await?),
            status: wire::MutationStatus::Committed as i32,
        }))
    }

    async fn delete_workspace(
        &self,
        request: Request<wire::DeleteWorkspaceRequest>,
    ) -> Result<Response<wire::MutationResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let workspace = self.workspace(request.workspace).await?;
        let outcome = workspace
            .delete(operation(request.operation)?)
            .await
            .map_err(status)?;
        let status = match outcome {
            WorkspaceDelete::Deleted => wire::MutationStatus::Committed,
            WorkspaceDelete::AlreadyDeleted => wire::MutationStatus::AlreadyCommitted,
            WorkspaceDelete::Conflict => wire::MutationStatus::Conflict,
            WorkspaceDelete::IdempotencyConflict => wire::MutationStatus::IdempotencyConflict,
        };
        Ok(Response::new(mutation(status, None, None)))
    }

    async fn get_head(
        &self,
        request: Request<wire::GetHeadRequest>,
    ) -> Result<Response<wire::GenerationResponse>, Status> {
        self.admit(&request)?;
        let workspace = self.workspace(request.into_inner().workspace).await?;
        let generation = workspace.head().await.map_err(status)?;
        Ok(Response::new(generation_message(&generation).await?))
    }

    async fn get_generation(
        &self,
        request: Request<wire::GetGenerationRequest>,
    ) -> Result<Response<wire::GenerationResponse>, Status> {
        self.admit(&request)?;
        let generation = self.generation(request.into_inner().generation).await?;
        Ok(Response::new(generation_message(&generation).await?))
    }

    async fn read(
        &self,
        request: Request<wire::ReadRequest>,
    ) -> Result<Response<wire::ReadResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        if request.maximum_bytes == 0 || request.maximum_bytes > self.limits.maximum_response_bytes
        {
            return Err(Status::invalid_argument(
                "maximum_bytes is outside service bounds",
            ));
        }
        let generation = self.generation(request.generation).await?;
        let bytes = match request.range {
            Some(range) => {
                if range.length > request.maximum_bytes {
                    return Err(Status::invalid_argument("range exceeds maximum_bytes"));
                }
                generation
                    .read_range(&request.path, range.offset, range.length)
                    .await
            }
            None => generation.read(&request.path, request.maximum_bytes).await,
        }
        .map_err(status)?;
        Ok(Response::new(wire::ReadResponse {
            contents: bytes.to_vec(),
        }))
    }

    async fn stat(
        &self,
        request: Request<wire::StatRequest>,
    ) -> Result<Response<wire::StatResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let value = self
            .generation(request.generation)
            .await?
            .stat(&request.path)
            .await
            .map_err(status)?;
        Ok(Response::new(wire::StatResponse {
            stat: Some(stat_message(value)),
        }))
    }

    async fn list_directory(
        &self,
        request: Request<wire::ListDirectoryRequest>,
    ) -> Result<Response<wire::ListDirectoryResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let page = required(request.page, "page")?;
        if page.maximum_items == 0 || page.maximum_items > self.limits.maximum_page_items {
            return Err(Status::invalid_argument("directory page bound is invalid"));
        }
        let generation = self.generation(request.generation).await?;
        let after = if page.after.is_empty() {
            None
        } else {
            Some(
                crate::kernel::LogicalName::new(NameEncoding::Utf8, page.after, 255)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?,
            )
        };
        let listed = generation
            .list_directory(&request.path, after.as_ref(), page.maximum_items)
            .await
            .map_err(status)?;
        let next = if listed.has_more {
            listed
                .entries
                .last()
                .map_or_else(Vec::new, |entry| entry.name.as_bytes().to_vec())
        } else {
            Vec::new()
        };
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(listed.entries.len())
            .map_err(|_| Status::resource_exhausted("directory page allocation failed"))?;
        for entry in listed.entries {
            let name = std::str::from_utf8(entry.name.as_bytes())
                .map_err(|_| Status::data_loss("portable directory name is not UTF-8"))?;
            let child = if request.path == "/" {
                format!("/{name}")
            } else {
                format!("{}/{name}", request.path.trim_end_matches('/'))
            };
            let stat = generation.stat(&child).await.map_err(status)?;
            if stat.file_id != entry.file_id || stat.kind != entry.kind {
                return Err(Status::data_loss(
                    "directory binding changed within immutable generation",
                ));
            }
            entries.push(wire::DirectoryEntry {
                name: entry.name.as_bytes().to_vec(),
                stat: Some(stat_message(stat)),
            });
        }
        Ok(Response::new(wire::ListDirectoryResponse {
            page: Some(wire::DirectoryPage { entries, next }),
        }))
    }

    async fn read_link(
        &self,
        request: Request<wire::ReadLinkRequest>,
    ) -> Result<Response<wire::ReadResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let contents = self
            .generation(request.generation)
            .await?
            .read_symbolic_link(&request.path)
            .await
            .map_err(status)?;
        if u64::try_from(contents.len()).unwrap_or(u64::MAX) > request.maximum_bytes
            || request.maximum_bytes > self.limits.maximum_response_bytes
        {
            return Err(Status::resource_exhausted(
                "symbolic link exceeds response bound",
            ));
        }
        Ok(Response::new(wire::ReadResponse {
            contents: contents.to_vec(),
        }))
    }

    async fn plan_extents(
        &self,
        request: Request<wire::PlanExtentsRequest>,
    ) -> Result<Response<wire::PlanExtentsResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        if request.maximum_extents == 0 || request.maximum_extents > self.limits.maximum_page_items
        {
            return Err(Status::invalid_argument("extent bound is invalid"));
        }
        let range = required(request.range, "range")?;
        let plan = self
            .generation(request.generation)
            .await?
            .plan_extents(
                &request.path,
                range.offset,
                range.length,
                request.maximum_extents,
            )
            .await
            .map_err(status)?;
        let truncated =
            u32::try_from(plan.spans.len()).unwrap_or(u32::MAX) == request.maximum_extents;
        Ok(Response::new(wire::PlanExtentsResponse {
            extents: plan
                .spans
                .into_iter()
                .map(|span| wire::Extent {
                    range: Some(wire::ByteRange {
                        offset: span.offset,
                        length: span.length,
                    }),
                    kind: match span.kind {
                        WorkspaceExtentKind::Hole => wire::ExtentKind::Hole as i32,
                        WorkspaceExtentKind::AllocatedZero => {
                            wire::ExtentKind::AllocatedZero as i32
                        }
                        WorkspaceExtentKind::Content => wire::ExtentKind::Content as i32,
                    },
                })
                .collect(),
            truncated,
        }))
    }

    async fn apply_transaction(
        &self,
        request: Request<wire::ApplyTransactionRequest>,
    ) -> Result<Response<wire::MutationResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        if request.mutations.is_empty()
            || u32::try_from(request.mutations.len()).unwrap_or(u32::MAX)
                > self.limits.maximum_transaction_mutations
        {
            return Err(Status::invalid_argument(
                "transaction mutation count is invalid",
            ));
        }
        let expected = required(request.expected, "expected generation")?;
        let workspace = self.workspace(expected.workspace.clone()).await?;
        let expected_id = generation_id(&expected.generation_id)?;
        let actual = workspace.head().await.map_err(status)?;
        if actual.id() != expected_id {
            return Ok(Response::new(mutation(
                wire::MutationStatus::Conflict,
                None,
                Some(generation_ref(&actual)),
            )));
        }
        let mut transaction = workspace
            .begin_transaction(operation(request.operation)?)
            .await
            .map_err(status)?;
        for mutation in request.mutations {
            apply_mutation(&mut transaction, mutation).await?;
        }
        Ok(Response::new(commit_message(
            transaction.commit().await.map_err(status)?,
        )))
    }

    async fn fork_workspace(
        &self,
        request: Request<wire::ForkWorkspaceRequest>,
    ) -> Result<Response<wire::WorkspaceResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let idempotency_key = operation(request.operation)?;
        let source = self.generation(request.source).await?;
        let source_workspace = self
            .workspace(Some(
                generation_ref(&source)
                    .workspace
                    .ok_or_else(|| Status::internal("missing workspace"))?,
            ))
            .await?;
        let destination = source_workspace
            .fork(
                request.destination_name,
                ForkOptions::from_generation(source, idempotency_key),
            )
            .await
            .map_err(status)?;
        Ok(Response::new(wire::WorkspaceResponse {
            workspace: Some(workspace_message(&destination).await?),
            status: wire::MutationStatus::Committed as i32,
        }))
    }

    async fn diff(
        &self,
        request: Request<wire::DiffRequest>,
    ) -> Result<Response<wire::DiffResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        if request.maximum_changes == 0 || request.maximum_changes > self.limits.maximum_page_items
        {
            return Err(Status::invalid_argument("diff bound is invalid"));
        }
        let from = self.generation(request.from).await?;
        let to = self.generation(request.to).await?;
        let workspace = self.workspace(generation_ref(&from).workspace).await?;
        let changes = workspace
            .diff(&from, &to, request.maximum_changes)
            .await
            .map_err(status)?;
        let (values, truncated) = diff_changes(changes.changes(), request.maximum_changes);
        Ok(Response::new(wire::DiffResponse {
            changes: values,
            truncated,
        }))
    }

    async fn rebase(
        &self,
        request: Request<wire::RebaseRequest>,
    ) -> Result<Response<wire::RebaseResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let workspace = self.workspace(request.workspace).await?;
        let onto = self.generation(request.onto).await?;
        let source_workspace = self.workspace(generation_ref(&onto).workspace).await?;
        if source_workspace.id() == workspace.id() {
            return Err(Status::invalid_argument(
                "rebase source must be the fork source",
            ));
        }
        if source_workspace.head().await.map_err(status)?.id() != onto.id() {
            return Err(Status::failed_precondition(
                "rebase source generation is not current",
            ));
        }
        let outcome = workspace
            .live_rebase(
                operation(request.operation)?,
                self.limits.maximum_page_items,
                self.limits.maximum_page_items,
                request.maximum_conflicts,
            )
            .await
            .map_err(status)?;
        Ok(Response::new(rebase_message(outcome)))
    }

    async fn plan_join(
        &self,
        request: Request<wire::PlanJoinRequest>,
    ) -> Result<Response<wire::JoinPlan>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        if request.maximum_changes == 0
            || request.maximum_changes > self.limits.maximum_page_items
            || request.maximum_conflicts == 0
            || request.maximum_conflicts > self.limits.maximum_page_items
        {
            return Err(Status::invalid_argument("join bounds are invalid"));
        }
        let source = self.generation(request.source).await?;
        let target = self.generation(request.target).await?;
        let source_workspace = self.workspace(generation_ref(&source).workspace).await?;
        let target_workspace = self.workspace(generation_ref(&target).workspace).await?;
        if source_workspace.head().await.map_err(status)?.id() != source.id()
            || target_workspace.head().await.map_err(status)?.id() != target.id()
        {
            return Err(Status::failed_precondition(
                "join endpoints must be current workspace heads",
            ));
        }
        let plan = source_workspace
            .join_into(&target_workspace)
            .history(JoinHistory::Merge)
            .bounds(
                self.limits.maximum_page_items,
                request.maximum_changes,
                request.maximum_conflicts,
            )
            .plan()
            .await
            .map_err(status)?;
        let source_ref = generation_ref(&source);
        let target_ref = generation_ref(&target);
        let plan_id = join_plan_id(&source_ref, &target_ref);
        let base = source_workspace
            .generation(plan.common_ancestor())
            .await
            .map_err(status)?;
        let changes = source_workspace
            .diff(&base, &source, request.maximum_changes)
            .await
            .map_err(status)?;
        let (changes, truncated) = diff_changes(changes.changes(), request.maximum_changes);
        Ok(Response::new(wire::JoinPlan {
            plan_id: plan_id.to_vec(),
            source: Some(source_ref),
            expected_target: Some(target_ref),
            changes,
            conflicts: Vec::new(),
            truncated,
        }))
    }

    async fn apply_join(
        &self,
        request: Request<wire::ApplyJoinRequest>,
    ) -> Result<Response<wire::MutationResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let supplied = required(request.plan, "join plan")?;
        let source_ref = required(supplied.source, "join source")?;
        let target_ref = required(supplied.expected_target, "join target")?;
        if supplied.plan_id != join_plan_id(&source_ref, &target_ref) {
            return Err(Status::invalid_argument("join plan identity mismatch"));
        }
        let source = self.generation(Some(source_ref)).await?;
        let target = self.generation(Some(target_ref)).await?;
        let source_workspace = self.workspace(generation_ref(&source).workspace).await?;
        let target_workspace = self.workspace(generation_ref(&target).workspace).await?;
        let plan = source_workspace
            .join_into(&target_workspace)
            .history(JoinHistory::Merge)
            .bounds(
                self.limits.maximum_page_items,
                self.limits.maximum_page_items,
                self.limits.maximum_page_items,
            )
            .plan()
            .await
            .map_err(status)?;
        if plan.source_head() != source.id() || plan.target_head() != target.id() {
            let actual = target_workspace.head().await.map_err(status)?;
            return Ok(Response::new(mutation(
                wire::MutationStatus::Conflict,
                None,
                Some(generation_ref(&actual)),
            )));
        }
        let outcome = plan
            .apply(ApplyOptions {
                if_target: target.id(),
                idempotency_key: operation(request.operation)?,
            })
            .await
            .map_err(status)?;
        Ok(Response::new(join_message(outcome)))
    }

    async fn checkpoint(
        &self,
        request: Request<wire::RetainGenerationRequest>,
    ) -> Result<Response<wire::RetainGenerationResponse>, Status> {
        self.retain(request, true).await
    }

    async fn pin(
        &self,
        request: Request<wire::RetainGenerationRequest>,
    ) -> Result<Response<wire::RetainGenerationResponse>, Status> {
        self.retain(request, false).await
    }

    async fn attach_source(
        &self,
        _request: Request<wire::AttachSourceRequest>,
    ) -> Result<Response<wire::WorkspaceResponse>, Status> {
        Err(Status::unimplemented(
            "host source attachment is an embedded native capability",
        ))
    }

    async fn get_source_state(
        &self,
        _request: Request<wire::SourceStateRequest>,
    ) -> Result<Response<wire::SourceStateResponse>, Status> {
        Err(Status::unimplemented(
            "host source attachment is an embedded native capability",
        ))
    }

    async fn reconcile_source(
        &self,
        _request: Request<wire::ReconcileSourceRequest>,
    ) -> Result<Response<wire::MutationResponse>, Status> {
        Err(Status::unimplemented(
            "host source attachment is an embedded native capability",
        ))
    }

    async fn seal_source(
        &self,
        _request: Request<wire::SealSourceRequest>,
    ) -> Result<Response<wire::MutationResponse>, Status> {
        Err(Status::unimplemented(
            "host source attachment is an embedded native capability",
        ))
    }

    type ExportStream = WireStream<wire::ExportChunk>;

    async fn export(
        &self,
        request: Request<wire::ExportRequest>,
    ) -> Result<Response<Self::ExportStream>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        if request.maximum_objects == 0
            || request.maximum_objects > self.limits.maximum_page_items
            || request.maximum_bytes == 0
            || request.maximum_bytes > self.limits.maximum_response_bytes
        {
            return Err(Status::invalid_argument("export bounds are invalid"));
        }
        let generation = self.generation(request.generation).await?;
        let checkout = generation
            .workspace
            .engine_checkout(
                crate::model::GenerationSelector::Exact(generation.id()),
                crate::model::CheckoutMode::read_only_pinned(),
            )
            .await
            .map_err(status)?;
        let manifest = checkout
            .export_manifest(crate::WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| Status::unavailable(failure.error.to_string()))?
            .value;
        let encoded = crate::encode_generation_export_manifest(&manifest)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        let retained = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
        if retained > request.maximum_bytes {
            return Err(Status::resource_exhausted(
                "export manifest exceeds byte bound",
            ));
        }
        let start = transfer_cursor(&request.after)?;
        let start_index = usize::try_from(start)
            .map_err(|_| Status::invalid_argument("export cursor is out of range"))?;
        if start_index > manifest.objects.len() {
            return Err(Status::invalid_argument("export cursor is out of range"));
        }
        let manifest_chunk = (start == 0).then(|| wire::ExportChunk {
            cursor: 0_u64.to_le_bytes().to_vec(),
            object_id: Vec::new(),
            contents: encoded,
            terminal: manifest.objects.is_empty(),
        });
        let filesystem = self.filesystem.clone();
        let objects = manifest.objects;
        let maximum_objects = request.maximum_objects;
        let maximum_bytes = request.maximum_bytes;
        let object_stream = stream::try_unfold(
            (filesystem, objects, start_index, start, retained, 0_u32),
            move |(filesystem, objects, index, cursor, retained, emitted)| async move {
                if index == objects.len() || emitted == maximum_objects || retained == maximum_bytes
                {
                    return Ok(None);
                }
                let object = objects[index];
                let remaining = maximum_bytes.saturating_sub(retained);
                let body = filesystem
                    .export_object(
                        object,
                        remaining,
                        crate::WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(|failure| Status::resource_exhausted(failure.error.to_string()))?
                    .value
                    .bytes;
                let retained = retained
                    .checked_add(u64::try_from(body.len()).unwrap_or(u64::MAX))
                    .ok_or_else(|| Status::resource_exhausted("export byte count overflow"))?;
                let next_index = index.saturating_add(1);
                let next_cursor = cursor.saturating_add(1);
                let chunk = wire::ExportChunk {
                    cursor: next_cursor.to_le_bytes().to_vec(),
                    object_id: encode_object_id(object),
                    contents: body.to_vec(),
                    terminal: next_index == objects.len(),
                };
                Ok(Some((
                    chunk,
                    (
                        filesystem,
                        objects,
                        next_index,
                        next_cursor,
                        retained,
                        emitted.saturating_add(1),
                    ),
                )))
            },
        );
        let output = stream::iter(manifest_chunk.map(Ok)).chain(object_stream);
        Ok(Response::new(Box::pin(output)))
    }

    async fn import(
        &self,
        request: Request<tonic::Streaming<wire::ImportChunk>>,
    ) -> Result<Response<wire::ImportResponse>, Status> {
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("import stream is empty"))?;
        self.admit_message(&first)?;
        if !first.object_id.is_empty() || first.contents.is_empty() {
            return Err(Status::invalid_argument(
                "import must begin with a manifest chunk",
            ));
        }
        let workspace_ref = required(first.workspace, "workspace")?;
        let operation_bytes = first.operation_id.clone();
        let operation_id = operation(Some(wire::OperationOptions {
            idempotency_key: operation_bytes.clone(),
        }))?
        .operation_id();
        let manifest = crate::decode_generation_export_manifest(
            &first.contents,
            self.limits.maximum_response_bytes,
            u64::from(self.limits.maximum_page_items),
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let expected_workspace = self
            .filesystem
            .workspace_id(&workspace_ref.name)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if expected_workspace.into_bytes().as_slice() != workspace_ref.workspace_id
            || expected_workspace.volume_id() != manifest.volume_id
        {
            return Err(Status::failed_precondition(
                "import workspace identity mismatch",
            ));
        }
        let mut expected_index = 0_usize;
        let mut terminal = first.terminal;
        while let Some(chunk) = stream.message().await? {
            self.admit_message(&chunk)?;
            if expected_index >= self.limits.maximum_page_items as usize {
                return Err(Status::resource_exhausted(
                    "import object count exceeds service bound",
                ));
            }
            if chunk.workspace.as_ref() != Some(&workspace_ref)
                || chunk.operation_id != operation_bytes
                || terminal
            {
                return Err(Status::invalid_argument(
                    "import stream continuity mismatch",
                ));
            }
            let object = decode_object_id(&chunk.object_id)?;
            if manifest.objects.get(expected_index).copied() != Some(object) {
                return Err(Status::invalid_argument("import object order mismatch"));
            }
            self.filesystem
                .import_object(
                    object,
                    Bytes::from(chunk.contents),
                    crate::WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|failure| Status::unavailable(failure.error.to_string()))?;
            expected_index = expected_index.saturating_add(1);
            if transfer_cursor(&chunk.cursor)? != expected_index as u64 {
                return Err(Status::invalid_argument("import cursor mismatch"));
            }
            terminal = chunk.terminal;
        }
        if !terminal || expected_index != manifest.objects.len() {
            return Err(Status::invalid_argument(
                "import stream ended before its closure",
            ));
        }
        self.filesystem
            .restore_volume(
                &manifest,
                operation_id,
                crate::WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| Status::unavailable(failure.error.to_string()))?;
        let workspace = self
            .filesystem
            .open_workspace(&workspace_ref.name)
            .await
            .map_err(status)?;
        Ok(Response::new(wire::ImportResponse {
            outcome: Some(mutation(
                wire::MutationStatus::Committed,
                Some(generation_ref(&workspace.head().await.map_err(status)?)),
                None,
            )),
        }))
    }

    async fn issue_mount_credential(
        &self,
        request: Request<wire::CredentialRequest>,
    ) -> Result<Response<wire::CredentialResponse>, Status> {
        self.issue_credential(request, CredentialKind::Mount).await
    }

    async fn issue_s3_credential(
        &self,
        request: Request<wire::CredentialRequest>,
    ) -> Result<Response<wire::CredentialResponse>, Status> {
        self.issue_credential(request, CredentialKind::S3).await
    }

    async fn observe(
        &self,
        request: Request<wire::ObserveRequest>,
    ) -> Result<Response<wire::ObserveResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let workspace = self.workspace(request.workspace).await?;
        let operation_id = raw_operation(&request.operation_id)?;
        let observed = self
            .filesystem
            .observe_volume_operation(
                workspace.id().volume_id(),
                operation_id,
                crate::WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| Status::unavailable(failure.error.to_string()))?;
        let response = match observed.value {
            Some(commit) => wire::ObserveResponse {
                state: "completed".to_owned(),
                outcome: Some(observed_mutation(&workspace, &commit)?),
            },
            None => wire::ObserveResponse {
                state: "unknown".to_owned(),
                outcome: None,
            },
        };
        Ok(Response::new(response))
    }

    async fn cancel(
        &self,
        request: Request<wire::CancelRequest>,
    ) -> Result<Response<wire::CancelResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let observed = self
            .observe(Request::new(wire::ObserveRequest {
                workspace: request.workspace,
                operation_id: request.operation_id,
            }))
            .await?
            .into_inner();
        Ok(Response::new(wire::CancelResponse {
            operation: Some(observed),
        }))
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemWireService<A, O> {
    async fn issue_credential(
        &self,
        request: Request<wire::CredentialRequest>,
        kind: CredentialKind,
    ) -> Result<Response<wire::CredentialResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        if request.expires_after_seconds == 0
            || request.expires_after_seconds > self.limits.maximum_credential_seconds
        {
            return Err(Status::invalid_argument(
                "credential lifetime exceeds the configured bound",
            ));
        }
        let workspace = self.workspace(request.workspace).await?;
        let generation = match request.generation {
            Some(reference) => {
                let generation = self.generation(Some(reference)).await?;
                if generation.workspace.id() != workspace.id() {
                    return Err(Status::failed_precondition(
                        "credential generation belongs to another workspace",
                    ));
                }
                generation
            }
            None => workspace.head().await.map_err(status)?,
        };
        let issuer = self.credential_issuer.as_ref().ok_or_else(|| {
            Status::unimplemented("credential issuance is unavailable in this deployment")
        })?;
        let enabled = match kind {
            CredentialKind::Mount => issuer.mount_enabled(),
            CredentialKind::S3 => issuer.s3_enabled(),
        };
        if !enabled {
            return Err(Status::unimplemented(
                "requested credential surface is unavailable",
            ));
        }
        let grant = issuer
            .issue(CredentialGrantRequest {
                kind,
                workspace_id: workspace.id(),
                workspace_name: workspace.name().clone(),
                generation_id: generation.id(),
                writable: request.writable,
                expires_after_seconds: request.expires_after_seconds,
                idempotency_key: operation(request.operation)?,
            })
            .await?;
        if grant.endpoint.is_empty() || grant.token.is_empty() || grant.expires_at_unix_seconds <= 0
        {
            return Err(Status::internal(
                "credential issuer returned an invalid grant",
            ));
        }
        Ok(Response::new(wire::CredentialResponse {
            endpoint: grant.endpoint,
            token: grant.token,
            expires_at_unix_seconds: grant.expires_at_unix_seconds,
        }))
    }

    async fn retain(
        &self,
        request: Request<wire::RetainGenerationRequest>,
        checkpoint: bool,
    ) -> Result<Response<wire::RetainGenerationResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let _ = operation(request.operation)?;
        let generation = self.generation(request.generation).await?;
        if checkpoint {
            let workspace = self
                .workspace(generation_ref(&generation).workspace)
                .await?;
            let head = workspace.head().await.map_err(status)?;
            if head.id() != generation.id() {
                return Err(Status::failed_precondition(
                    "checkpoint requires the current head",
                ));
            }
            workspace
                .checkpoint(&request.identity)
                .await
                .map_err(status)?;
        } else {
            generation.pin(&request.identity).await.map_err(status)?;
        }
        Ok(Response::new(wire::RetainGenerationResponse {
            generation: Some(generation_ref(&generation)),
            identity: request.identity,
            status: wire::MutationStatus::Committed as i32,
        }))
    }
}

async fn apply_mutation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    transaction: &mut Transaction<A, O>,
    value: wire::Mutation,
) -> Result<(), Status> {
    use wire::mutation::Mutation;
    match required(value.mutation, "mutation")? {
        Mutation::CreateFile(value) => {
            transaction
                .create_file(
                    &value.path,
                    Bytes::from(value.contents),
                    metadata(value.metadata)?,
                )
                .await
        }
        Mutation::CreateDirectory(value) => {
            transaction
                .create_directory(&value.path)
                .await
                .map_err(status)?;
            transaction
                .set_metadata(&value.path, metadata(value.metadata)?)
                .await
        }
        Mutation::CreateSymbolicLink(value) => {
            transaction
                .create_symbolic_link(&value.path, Bytes::from(value.target))
                .await
                .map_err(status)?;
            transaction
                .set_metadata(&value.path, metadata(value.metadata)?)
                .await
        }
        Mutation::Remove(value) => transaction.remove(&value.path).await,
        Mutation::Rename(value) => {
            transaction
                .rename_with_replace(&value.source, &value.destination, value.replace)
                .await
        }
        Mutation::HardLink(value) => {
            transaction
                .hard_link(&value.source, &value.destination)
                .await
        }
        Mutation::Write(value) => {
            transaction
                .write_range(&value.path, value.offset, Bytes::from(value.contents))
                .await
        }
        Mutation::Resize(value) => transaction.resize(&value.path, value.logical_bytes).await,
        Mutation::ZeroRange(value) => {
            let range = required(value.range, "range")?;
            transaction
                .zero_range(
                    &value.path,
                    ByteRange {
                        offset: range.offset,
                        length: range.length,
                    },
                    value.allocated,
                    value.extend,
                )
                .await
        }
        Mutation::Preallocate(value) => {
            let range = required(value.range, "range")?;
            transaction
                .preallocate(
                    &value.path,
                    ByteRange {
                        offset: range.offset,
                        length: range.length,
                    },
                    value.keep_size,
                )
                .await
        }
        Mutation::CloneRange(value) => {
            transaction
                .clone_range(
                    &value.source,
                    value.source_offset,
                    &value.destination,
                    value.destination_offset,
                    value.length,
                )
                .await
        }
        Mutation::SetMetadata(value) => {
            transaction
                .set_metadata(&value.path, metadata(value.metadata)?)
                .await
        }
    }
    .map_err(status)
}

fn metadata(value: Option<wire::Metadata>) -> Result<FileMetadata, Status> {
    let value = value.unwrap_or_default();
    if value.has_named_attributes || value.has_acl || value.has_security_descriptor {
        return Err(Status::invalid_argument(
            "opaque metadata bodies are required separately",
        ));
    }
    Ok(FileMetadata {
        posix_mode: input_u32(value.posix_mode)?,
        posix_uid: input_u32(value.posix_uid)?,
        posix_gid: input_u32(value.posix_gid)?,
        posix_flags: input_u64(value.posix_flags)?,
        windows_attributes: input_u32(value.windows_attributes)?,
        created_ns: input_i64(value.created_ns)?,
        modified_ns: input_i64(value.modified_ns)?,
        accessed_ns: input_i64(value.accessed_ns)?,
        changed_ns: input_i64(value.changed_ns)?,
        named_attributes: MetadataField::Unavailable,
        acl: MetadataField::Unavailable,
        security_descriptor: MetadataField::Unavailable,
    })
}

fn input_u32(value: Option<wire::OptionalU32>) -> Result<MetadataField<u32>, Status> {
    match value.and_then(|value| value.value) {
        None | Some(wire::optional_u32::Value::Unavailable(true)) => Ok(MetadataField::Unavailable),
        Some(wire::optional_u32::Value::Present(value)) => Ok(MetadataField::Value(value)),
        Some(wire::optional_u32::Value::Unavailable(false)) => {
            Err(Status::invalid_argument("unavailable marker must be true"))
        }
    }
}

fn input_u64(value: Option<wire::OptionalU64>) -> Result<MetadataField<u64>, Status> {
    match value.and_then(|value| value.value) {
        None | Some(wire::optional_u64::Value::Unavailable(true)) => Ok(MetadataField::Unavailable),
        Some(wire::optional_u64::Value::Present(value)) => Ok(MetadataField::Value(value)),
        Some(wire::optional_u64::Value::Unavailable(false)) => {
            Err(Status::invalid_argument("unavailable marker must be true"))
        }
    }
}

fn input_i64(value: Option<wire::OptionalI64>) -> Result<MetadataField<i64>, Status> {
    match value.and_then(|value| value.value) {
        None | Some(wire::optional_i64::Value::Unavailable(true)) => Ok(MetadataField::Unavailable),
        Some(wire::optional_i64::Value::Present(value)) => Ok(MetadataField::Value(value)),
        Some(wire::optional_i64::Value::Unavailable(false)) => {
            Err(Status::invalid_argument("unavailable marker must be true"))
        }
    }
}

fn profile(value: i32) -> Result<EngineProfile, Status> {
    match wire::FilesystemProfile::try_from(value).ok() {
        Some(wire::FilesystemProfile::Portable) => Ok(EngineProfile::Portable),
        Some(wire::FilesystemProfile::Posix) => Ok(EngineProfile::Posix),
        Some(wire::FilesystemProfile::Windows) => Ok(EngineProfile::Windows),
        Some(wire::FilesystemProfile::Browser) => Ok(EngineProfile::Browser),
        Some(wire::FilesystemProfile::Unspecified) | None => {
            Err(Status::invalid_argument("filesystem profile is required"))
        }
    }
}

fn profile_message(value: EngineProfile) -> i32 {
    match value {
        EngineProfile::Portable => wire::FilesystemProfile::Portable as i32,
        EngineProfile::Posix => wire::FilesystemProfile::Posix as i32,
        EngineProfile::Windows => wire::FilesystemProfile::Windows as i32,
        EngineProfile::Browser => wire::FilesystemProfile::Browser as i32,
    }
}

fn operation(value: Option<wire::OperationOptions>) -> Result<IdempotencyKey, Status> {
    let value = required(value, "operation")?;
    Ok(IdempotencyKey::from_bytes(raw_operation_bytes(
        &value.idempotency_key,
    )?))
}

fn raw_operation(value: &[u8]) -> Result<crate::OperationId, Status> {
    Ok(crate::OperationId::from_bytes(raw_operation_bytes(value)?))
}

fn raw_operation_bytes(value: &[u8]) -> Result<[u8; 16], Status> {
    value
        .try_into()
        .map_err(|_| Status::invalid_argument("idempotency_key must contain 16 bytes"))
}

fn generation_id(value: &[u8]) -> Result<GenerationId, Status> {
    let bytes: [u8; 32] = value
        .try_into()
        .map_err(|_| Status::invalid_argument("generation_id must contain 32 bytes"))?;
    Ok(GenerationId::new(Digest::from_bytes(bytes)))
}

fn workspace_ref<A, O>(workspace: &Workspace<A, O>) -> wire::WorkspaceRef {
    wire::WorkspaceRef {
        workspace_id: workspace.id().into_bytes().to_vec(),
        name: workspace.name().as_str().to_owned(),
    }
}

fn generation_ref<A, O>(generation: &Generation<A, O>) -> wire::GenerationRef {
    wire::GenerationRef {
        workspace: Some(workspace_ref(&generation.workspace)),
        generation_id: generation.id().digest().into_bytes().to_vec(),
    }
}

fn observed_mutation<A, O>(
    workspace: &Workspace<A, O>,
    commit: &DurableCommit,
) -> Result<wire::MutationResponse, Status> {
    const MAXIMUM_EVENT_BYTES: u64 = 4 * 1024;
    let expected = workspace.id().volume_id();
    if let Ok(deleted) =
        crate::kernel::decode_workspace_deleted(&commit.payload, MAXIMUM_EVENT_BYTES)
    {
        if deleted != expected {
            return Err(Status::data_loss("operation workspace identity mismatch"));
        }
        return Ok(mutation(wire::MutationStatus::Committed, None, None));
    }
    let root = if let Ok(publication) =
        crate::kernel::decode_published_generation(&commit.payload, MAXIMUM_EVENT_BYTES)
    {
        if publication.volume_id != expected {
            return Err(Status::data_loss("operation workspace identity mismatch"));
        }
        publication.generation_root
    } else if let Ok(created) =
        crate::kernel::decode_volume_created(&commit.payload, MAXIMUM_EVENT_BYTES)
    {
        if created.volume_id != expected {
            return Err(Status::data_loss("operation workspace identity mismatch"));
        }
        created.initial_generation_root
    } else {
        return Err(Status::data_loss(
            "operation does not contain a workspace mutation",
        ));
    };
    Ok(mutation(
        wire::MutationStatus::Committed,
        Some(wire::GenerationRef {
            workspace: Some(workspace_ref(workspace)),
            generation_id: root.digest.into_bytes().to_vec(),
        }),
        None,
    ))
}

async fn workspace_message<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
) -> Result<wire::Workspace, Status> {
    let head = workspace.head().await.map_err(status)?;
    Ok(wire::Workspace {
        workspace: Some(workspace_ref(workspace)),
        name: workspace.name().as_str().to_owned(),
        profile: profile_message(workspace.profile()),
        head: Some(generation_ref(&head)),
        deleted: false,
    })
}

async fn generation_message<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &Generation<A, O>,
) -> Result<wire::GenerationResponse, Status> {
    let reference = generation_ref(generation);
    let workspace = reference.workspace.clone();
    let parents = generation
        .parents()
        .await
        .map_err(status)?
        .into_iter()
        .map(|id| wire::GenerationRef {
            workspace: workspace.clone(),
            generation_id: id.digest().into_bytes().to_vec(),
        })
        .collect();
    Ok(wire::GenerationResponse {
        generation: Some(reference),
        parents,
    })
}

fn commit_message<A, O>(value: TransactionCommit<A, O>) -> wire::MutationResponse {
    match value {
        TransactionCommit::Committed(generation) => mutation(
            wire::MutationStatus::Committed,
            Some(generation_ref(&generation)),
            None,
        ),
        TransactionCommit::AlreadyCommitted(generation) => mutation(
            wire::MutationStatus::AlreadyCommitted,
            Some(generation_ref(&generation)),
            None,
        ),
        TransactionCommit::Conflict { actual } => mutation(
            wire::MutationStatus::Conflict,
            None,
            Some(generation_ref(&actual)),
        ),
        TransactionCommit::Fenced => mutation(wire::MutationStatus::Fenced, None, None),
        TransactionCommit::IdempotencyConflict => {
            mutation(wire::MutationStatus::IdempotencyConflict, None, None)
        }
    }
}

fn join_message<A, O>(value: JoinOutcome<A, O>) -> wire::MutationResponse {
    match value {
        JoinOutcome::Applied(generation) => mutation(
            wire::MutationStatus::Committed,
            Some(generation_ref(&generation)),
            None,
        ),
        JoinOutcome::AlreadyApplied(generation) | JoinOutcome::NoChanges(generation) => mutation(
            wire::MutationStatus::AlreadyCommitted,
            Some(generation_ref(&generation)),
            None,
        ),
        JoinOutcome::StaleTarget(actual) => mutation(
            wire::MutationStatus::Conflict,
            None,
            Some(generation_ref(&actual)),
        ),
        JoinOutcome::Conflicted { .. } => mutation(wire::MutationStatus::Conflict, None, None),
        JoinOutcome::Fenced => mutation(wire::MutationStatus::Fenced, None, None),
        JoinOutcome::IdempotencyConflict => {
            mutation(wire::MutationStatus::IdempotencyConflict, None, None)
        }
    }
}

fn diff_changes(
    changes: &crate::GenerationDiff,
    maximum_changes: u32,
) -> (Vec<wire::ChangedPath>, bool) {
    let mut values = Vec::new();
    for change in &changes.files {
        values.push(wire::ChangedPath {
            path: format!("@file/{}", hex::encode(change.file_id.into_bytes())),
            file_id: change.file_id.into_bytes().to_vec(),
        });
    }
    for change in &changes.bindings {
        values.push(wire::ChangedPath {
            path: format!(
                "@directory/{}/{}",
                hex::encode(change.directory_id.into_bytes()),
                hex::encode(change.name.as_bytes())
            ),
            file_id: change
                .after
                .as_ref()
                .or(change.before.as_ref())
                .map_or_else(Vec::new, |entry| entry.file_id.into_bytes().to_vec()),
        });
    }
    let over_bound = values.len() > maximum_changes as usize;
    values.truncate(maximum_changes as usize);
    (values, changes.truncated || over_bound)
}

fn join_plan_id(source: &wire::GenerationRef, target: &wire::GenerationRef) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-wire-join-plan-v1\0");
    for reference in [source, target] {
        if let Some(workspace) = &reference.workspace {
            hasher.update(&(workspace.workspace_id.len() as u64).to_le_bytes());
            hasher.update(&workspace.workspace_id);
            hasher.update(&(workspace.name.len() as u64).to_le_bytes());
            hasher.update(workspace.name.as_bytes());
        } else {
            hasher.update(&0_u64.to_le_bytes());
            hasher.update(&0_u64.to_le_bytes());
        }
        hasher.update(&(reference.generation_id.len() as u64).to_le_bytes());
        hasher.update(&reference.generation_id);
    }
    *hasher.finalize().as_bytes()
}

fn encode_object_id(object: ObjectId) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(33);
    bytes.push(object.kind.canonical_tag());
    bytes.extend_from_slice(object.digest.as_bytes());
    bytes
}

fn decode_object_id(bytes: &[u8]) -> Result<ObjectId, Status> {
    if bytes.len() != 33 {
        return Err(Status::invalid_argument(
            "object identity must contain kind and digest",
        ));
    }
    let kind = ObjectKind::from_canonical_tag(bytes[0])
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let digest: [u8; 32] = bytes[1..]
        .try_into()
        .map_err(|_| Status::invalid_argument("object digest is malformed"))?;
    Ok(ObjectId {
        kind,
        digest: Digest::from_bytes(digest),
    })
}

fn transfer_cursor(bytes: &[u8]) -> Result<u64, Status> {
    if bytes.is_empty() {
        return Ok(0);
    }
    let cursor: [u8; 8] = bytes
        .try_into()
        .map_err(|_| Status::invalid_argument("transfer cursor must contain 8 bytes"))?;
    Ok(u64::from_le_bytes(cursor))
}

fn mutation(
    status: wire::MutationStatus,
    generation: Option<wire::GenerationRef>,
    actual_head: Option<wire::GenerationRef>,
) -> wire::MutationResponse {
    wire::MutationResponse {
        status: status as i32,
        generation,
        actual_head,
    }
}

fn rebase_message<A, O>(value: WorkspaceRebase<A, O>) -> wire::RebaseResponse {
    match value {
        WorkspaceRebase::Rebased(generation) => wire::RebaseResponse {
            outcome: Some(mutation(
                wire::MutationStatus::Committed,
                Some(generation_ref(&generation)),
                None,
            )),
            conflicts: Vec::new(),
            truncated: false,
        },
        WorkspaceRebase::AlreadyRebased(generation) | WorkspaceRebase::Current(generation) => {
            wire::RebaseResponse {
                outcome: Some(mutation(
                    wire::MutationStatus::AlreadyCommitted,
                    Some(generation_ref(&generation)),
                    None,
                )),
                conflicts: Vec::new(),
                truncated: false,
            }
        }
        WorkspaceRebase::Stale(generation) => wire::RebaseResponse {
            outcome: Some(mutation(
                wire::MutationStatus::Conflict,
                None,
                Some(generation_ref(&generation)),
            )),
            conflicts: Vec::new(),
            truncated: false,
        },
        WorkspaceRebase::Conflicted {
            conflicts,
            truncated,
        } => wire::RebaseResponse {
            outcome: Some(mutation(wire::MutationStatus::Conflict, None, None)),
            conflicts: conflicts
                .into_iter()
                .map(|conflict| wire::Conflict {
                    path: String::new(),
                    range: None,
                    reason: format!("{conflict:?}"),
                })
                .collect(),
            truncated,
        },
        WorkspaceRebase::Fenced => wire::RebaseResponse {
            outcome: Some(mutation(wire::MutationStatus::Fenced, None, None)),
            conflicts: Vec::new(),
            truncated: false,
        },
        WorkspaceRebase::IdempotencyConflict => wire::RebaseResponse {
            outcome: Some(mutation(
                wire::MutationStatus::IdempotencyConflict,
                None,
                None,
            )),
            conflicts: Vec::new(),
            truncated: false,
        },
    }
}

fn stat_message(value: crate::WorkspaceStat) -> wire::FileStat {
    wire::FileStat {
        file_id: value.file_id.into_bytes().to_vec(),
        kind: file_kind(value.kind),
        link_count: value.link_count,
        logical_bytes: Some(output_u64(value.logical_bytes)),
        metadata: Some(metadata_message(value.metadata)),
    }
}

fn metadata_message(value: WorkspaceMetadata) -> wire::Metadata {
    wire::Metadata {
        posix_mode: Some(output_u32(value.posix_mode)),
        posix_uid: Some(output_u32(value.posix_uid)),
        posix_gid: Some(output_u32(value.posix_gid)),
        posix_flags: Some(output_u64(value.posix_flags)),
        windows_attributes: Some(output_u32(value.windows_attributes)),
        created_ns: Some(output_i64(value.created_ns)),
        modified_ns: Some(output_i64(value.modified_ns)),
        accessed_ns: Some(output_i64(value.accessed_ns)),
        changed_ns: Some(output_i64(value.changed_ns)),
        has_named_attributes: value.has_named_attributes,
        has_acl: value.has_acl,
        has_security_descriptor: value.has_security_descriptor,
    }
}

fn output_u32(value: Option<u32>) -> wire::OptionalU32 {
    wire::OptionalU32 {
        value: Some(value.map_or(
            wire::optional_u32::Value::Unavailable(true),
            wire::optional_u32::Value::Present,
        )),
    }
}

fn output_u64(value: Option<u64>) -> wire::OptionalU64 {
    wire::OptionalU64 {
        value: Some(value.map_or(
            wire::optional_u64::Value::Unavailable(true),
            wire::optional_u64::Value::Present,
        )),
    }
}

fn output_i64(value: Option<i64>) -> wire::OptionalI64 {
    wire::OptionalI64 {
        value: Some(value.map_or(
            wire::optional_i64::Value::Unavailable(true),
            wire::optional_i64::Value::Present,
        )),
    }
}

fn file_kind(value: EngineFileKind) -> i32 {
    (match value {
        EngineFileKind::Regular => wire::FileKind::Regular,
        EngineFileKind::Directory => wire::FileKind::Directory,
        EngineFileKind::SymbolicLink => wire::FileKind::SymbolicLink,
        EngineFileKind::Fifo => wire::FileKind::Fifo,
        EngineFileKind::Socket => wire::FileKind::Socket,
        EngineFileKind::CharacterDevice => wire::FileKind::CharacterDevice,
        EngineFileKind::BlockDevice => wire::FileKind::BlockDevice,
        EngineFileKind::ReparsePoint => wire::FileKind::ReparsePoint,
        EngineFileKind::MountBoundary => wire::FileKind::MountBoundary,
    }) as i32
}

fn descriptor_digest() -> String {
    blake3::hash(crate::FILE_DESCRIPTOR_SET)
        .to_hex()
        .to_string()
}

fn required<T>(value: Option<T>, name: &'static str) -> Result<T, Status> {
    value.ok_or_else(|| Status::invalid_argument(format!("{name} is required")))
}

fn status(error: WorkspaceError) -> Status {
    match error {
        WorkspaceError::NotFound => Status::not_found(error.to_string()),
        WorkspaceError::Name(_) | WorkspaceError::Path(_) | WorkspaceError::ReadLimitExceeded => {
            Status::invalid_argument(error.to_string())
        }
        WorkspaceError::ForeignGeneration
        | WorkspaceError::IncompatibleWorkspace
        | WorkspaceError::ChangeSetContinuity => Status::failed_precondition(error.to_string()),
        WorkspaceError::RetentionConflict
        | WorkspaceError::NoCommonAncestor
        | WorkspaceError::LineageLimit
        | WorkspaceError::JoinLimit
        | WorkspaceError::NotFork => Status::failed_precondition(error.to_string()),
        WorkspaceError::Engine(_) => Status::unavailable(error.to_string()),
        WorkspaceError::NotRegularFile
        | WorkspaceError::NotDirectory
        | WorkspaceError::EmptyContentSet
        | WorkspaceError::ContentLengthOverflow => Status::invalid_argument(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::filesystem::v1::filesystem_service_server::FilesystemService as _;

    struct TestIssuer;

    #[tonic::async_trait]
    impl FilesystemCredentialIssuer for TestIssuer {
        fn mount_enabled(&self) -> bool {
            true
        }

        fn s3_enabled(&self) -> bool {
            true
        }

        async fn issue(&self, request: CredentialGrantRequest) -> Result<CredentialGrant, Status> {
            Ok(CredentialGrant {
                endpoint: match request.kind {
                    CredentialKind::Mount => "mount://test",
                    CredentialKind::S3 => "https://s3.test",
                }
                .to_owned(),
                token: hex::encode(request.workspace_id.into_bytes()),
                expires_at_unix_seconds: 1_900_000_000,
            })
        }
    }

    fn operation(value: u8) -> Option<wire::OperationOptions> {
        Some(wire::OperationOptions {
            idempotency_key: operation_bytes(value),
        })
    }

    fn operation_bytes(value: u8) -> Vec<u8> {
        let mut id = [0_u8; 16];
        id[15] = value;
        id.to_vec()
    }

    fn mutation(value: wire::mutation::Mutation) -> wire::Mutation {
        wire::Mutation {
            mutation: Some(value),
        }
    }

    #[tokio::test]
    async fn handshake_and_creation_expose_every_canonical_profile()
    -> Result<(), Box<dyn std::error::Error>> {
        let service = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?;
        let advertised = service
            .handshake(Request::new(wire::HandshakeRequest { harness: None }))
            .await?
            .into_inner()
            .capabilities
            .ok_or("missing capabilities")?;
        let profiles = [
            wire::FilesystemProfile::Portable,
            wire::FilesystemProfile::Posix,
            wire::FilesystemProfile::Windows,
            wire::FilesystemProfile::Browser,
        ];
        assert_eq!(advertised.profiles, profiles.map(|profile| profile as i32));
        for (index, selected) in profiles.into_iter().enumerate() {
            let created = service
                .create_workspace(Request::new(wire::CreateWorkspaceRequest {
                    name: format!("profile-{index}"),
                    profile: selected as i32,
                    operation: operation(u8::try_from(index + 1)?),
                }))
                .await?
                .into_inner()
                .workspace
                .ok_or("missing workspace")?;
            assert_eq!(created.profile, selected as i32);
            let observed = service
                .observe(Request::new(wire::ObserveRequest {
                    workspace: created.workspace.clone(),
                    operation_id: operation_bytes(u8::try_from(index + 1)?),
                }))
                .await?
                .into_inner();
            assert_eq!(observed.state, "completed");
            assert_eq!(
                observed.outcome.and_then(|outcome| outcome.generation),
                created.head
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn wire_adapter_uses_one_engine_for_bounded_customer_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let service = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?
            .with_credential_issuer(Arc::new(TestIssuer));
        let created = service
            .create_workspace(Request::new(wire::CreateWorkspaceRequest {
                name: "main".to_owned(),
                profile: wire::FilesystemProfile::Portable as i32,
                operation: operation(1),
            }))
            .await?
            .into_inner()
            .workspace
            .ok_or("missing workspace")?;
        let initial = created.head.ok_or("missing head")?;
        let workspace = created.workspace.ok_or("missing workspace ref")?;

        let credential = service
            .issue_mount_credential(Request::new(wire::CredentialRequest {
                workspace: Some(workspace.clone()),
                generation: Some(initial.clone()),
                writable: false,
                expires_after_seconds: 60,
                operation: operation(8),
            }))
            .await?
            .into_inner();
        assert_eq!(credential.endpoint, "mount://test");
        assert!(!credential.token.is_empty());

        let committed = service
            .apply_transaction(Request::new(wire::ApplyTransactionRequest {
                expected: Some(initial),
                mutations: vec![
                    mutation(wire::mutation::Mutation::CreateDirectory(
                        wire::CreateDirectory {
                            path: "/dir".to_owned(),
                            metadata: None,
                        },
                    )),
                    mutation(wire::mutation::Mutation::CreateFile(wire::CreateFile {
                        path: "/dir/value".to_owned(),
                        contents: b"value".to_vec(),
                        metadata: None,
                    })),
                ],
                operation: operation(2),
            }))
            .await?
            .into_inner();
        assert_eq!(committed.status, wire::MutationStatus::Committed as i32);
        let generation = committed.generation.ok_or("missing generation")?;
        let observed = service
            .observe(Request::new(wire::ObserveRequest {
                workspace: Some(workspace.clone()),
                operation_id: operation_bytes(2),
            }))
            .await?
            .into_inner();
        assert_eq!(observed.state, "completed");
        assert_eq!(
            observed
                .outcome
                .as_ref()
                .and_then(|outcome| outcome.generation.as_ref()),
            Some(&generation)
        );
        let unknown = service
            .cancel(Request::new(wire::CancelRequest {
                workspace: Some(workspace.clone()),
                operation_id: operation_bytes(99),
            }))
            .await?
            .into_inner()
            .operation
            .ok_or("missing cancellation observation")?;
        assert_eq!(unknown.state, "unknown");
        assert!(unknown.outcome.is_none());
        let read = service
            .read(Request::new(wire::ReadRequest {
                generation: Some(generation.clone()),
                path: "/dir/value".to_owned(),
                range: None,
                maximum_bytes: 5,
            }))
            .await?
            .into_inner();
        assert_eq!(read.contents, b"value");

        let listed = service
            .list_directory(Request::new(wire::ListDirectoryRequest {
                generation: Some(generation.clone()),
                path: "/dir".to_owned(),
                page: Some(wire::PageOptions {
                    maximum_items: 8,
                    after: Vec::new(),
                }),
            }))
            .await?
            .into_inner()
            .page
            .ok_or("missing directory page")?;
        assert_eq!(listed.entries.len(), 1);
        assert_eq!(listed.entries[0].name, b"value");

        let pinned = service
            .pin(Request::new(wire::RetainGenerationRequest {
                generation: Some(generation.clone()),
                identity: "release".to_owned(),
                operation: operation(3),
            }))
            .await?
            .into_inner();
        assert_eq!(pinned.status, wire::MutationStatus::Committed as i32);

        let forked = service
            .fork_workspace(Request::new(wire::ForkWorkspaceRequest {
                source: Some(generation),
                destination_name: "agent".to_owned(),
                operation: operation(4),
            }))
            .await?
            .into_inner()
            .workspace
            .ok_or("missing fork")?;
        assert_ne!(
            forked
                .workspace
                .as_ref()
                .ok_or("missing fork ref")?
                .workspace_id,
            workspace.workspace_id
        );

        let main_head = service
            .get_head(Request::new(wire::GetHeadRequest {
                workspace: Some(workspace.clone()),
            }))
            .await?
            .into_inner()
            .generation
            .ok_or("missing main head")?;
        let changed = service
            .apply_transaction(Request::new(wire::ApplyTransactionRequest {
                expected: Some(main_head),
                mutations: vec![mutation(wire::mutation::Mutation::Write(wire::Write {
                    path: "/dir/value".to_owned(),
                    offset: 0,
                    contents: b"new!!".to_vec(),
                }))],
                operation: operation(5),
            }))
            .await?
            .into_inner()
            .generation
            .ok_or("missing changed generation")?;
        let fork_head = forked.head.clone().ok_or("missing fork head")?;
        let plan = service
            .plan_join(Request::new(wire::PlanJoinRequest {
                source: Some(changed.clone()),
                target: Some(fork_head),
                maximum_changes: 32,
                maximum_conflicts: 8,
            }))
            .await?
            .into_inner();
        let joined = service
            .apply_join(Request::new(wire::ApplyJoinRequest {
                plan: Some(plan),
                operation: operation(6),
            }))
            .await?
            .into_inner();
        assert_eq!(joined.status, wire::MutationStatus::Committed as i32);

        let mut exported = service
            .export(Request::new(wire::ExportRequest {
                generation: Some(changed),
                after: Vec::new(),
                maximum_objects: 64,
                maximum_bytes: 1024 * 1024,
            }))
            .await?
            .into_inner();
        let mut export_chunks = Vec::new();
        while let Some(chunk) = exported.next().await {
            export_chunks.push(chunk?);
        }
        assert!(export_chunks.len() > 1);
        assert!(export_chunks[0].object_id.is_empty());
        assert!(
            export_chunks
                .last()
                .ok_or("missing export terminal")?
                .terminal
        );

        let deleted = service
            .delete_workspace(Request::new(wire::DeleteWorkspaceRequest {
                workspace: Some(workspace),
                operation: operation(7),
            }))
            .await?
            .into_inner();
        assert_eq!(deleted.status, wire::MutationStatus::Committed as i32);
        Ok(())
    }
}
