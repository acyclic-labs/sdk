//! Idiomatic handles over the canonical hosted Filesystem transport.
//!
//! This module deliberately contains no filesystem semantics. It retains exact
//! workspace/generation identities and delegates every operation to the v2
//! service implemented by the same canonical engine used by embedded profiles.

use crate::model::FilesystemProfile as EmbeddedProfile;
use crate::wire::filesystem::v2 as wire;
use crate::{Fs, IdempotencyKey};
use bytes::Bytes;
use futures::Stream;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

type Client = wire::filesystem_service_client::FilesystemServiceClient<Channel>;

/// Connection and client-side response bounds for [`Fs::hosted`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostedFsOptions {
    /// HTTPS endpoint of the hosted Filesystem service.
    pub endpoint: String,
    /// Opaque account-scoped bearer credential.
    pub bearer_token: String,
    /// Maximum accepted encoded response size.
    pub maximum_response_bytes: usize,
    /// Maximum encoded request size sent by this client.
    pub maximum_request_bytes: usize,
    /// Connection deadline.
    pub connect_timeout: Duration,
    /// Per-request transport deadline.
    pub request_timeout: Duration,
}

impl HostedFsOptions {
    /// Creates bounded hosted options with conservative transport defaults.
    #[must_use]
    pub fn new(endpoint: impl Into<String>, bearer_token: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            bearer_token: bearer_token.into(),
            maximum_response_bytes: 16 * 1024 * 1024,
            maximum_request_bytes: 16 * 1024 * 1024,
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
        }
    }
}

/// A local validation, transport, or malformed-server failure.
#[derive(Debug, Error)]
pub enum HostedFsError {
    /// Hosted options are empty, unbounded, or use an unsupported endpoint.
    #[error("invalid hosted filesystem options: {0}")]
    InvalidOptions(&'static str),
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
    owner: Arc<()>,
}

impl HostedFs {
    async fn connect(options: HostedFsOptions) -> Result<Self, HostedFsError> {
        if options.maximum_request_bytes == 0 || options.maximum_response_bytes == 0 {
            return Err(HostedFsError::InvalidOptions(
                "request and response bounds must be nonzero",
            ));
        }
        if options.bearer_token.is_empty() {
            return Err(HostedFsError::InvalidOptions(
                "bearer credential must be nonempty",
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
        let authorization = format!("Bearer {}", options.bearer_token)
            .parse()
            .map_err(|_| HostedFsError::InvalidOptions("bearer credential is not HTTP metadata"))?;
        let endpoint = Endpoint::from_shared(options.endpoint)?
            .connect_timeout(options.connect_timeout)
            .timeout(options.request_timeout);
        let channel = endpoint.connect().await?;
        let client = Client::new(channel)
            .max_decoding_message_size(options.maximum_response_bytes)
            .max_encoding_message_size(options.maximum_request_bytes);
        Ok(Self {
            client,
            authorization,
            owner: Arc::new(()),
        })
    }

    fn request<T>(&self, value: T) -> Request<T> {
        let mut request = Request::new(value);
        request
            .metadata_mut()
            .insert("authorization", self.authorization.clone());
        request
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
    use crate::{EmbeddedCapabilities, FilesystemWireLimits, FilesystemWireService};
    use tokio_stream::wrappers::TcpListenerStream;

    #[tokio::test]
    async fn hosted_constructor_uses_the_same_canonical_engine_over_real_grpc()
    -> Result<(), Box<dyn std::error::Error>> {
        let embedded = crate::MemoryFs::memory();
        let service = FilesystemServiceServer::new(FilesystemWireService::new(
            embedded,
            FilesystemWireLimits::default(),
        )?);
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

        let hosted = Fs::hosted(HostedFsOptions::new(
            format!("http://{address}"),
            "test-account-token",
        ))
        .await?;
        let workspace = hosted
            .create_workspace(
                "hosted",
                EmbeddedProfile::Portable,
                IdempotencyKey::from_bytes([1; 16]),
            )
            .await?;
        let mut transaction = workspace.begin_transaction(IdempotencyKey::from_bytes([2; 16]));
        transaction.create_directories("/tree");
        transaction.put_file("/tree/value", b"canonical".to_vec());
        let outcome = transaction.commit(32).await?;
        assert!(matches!(
            wire::MutationStatus::try_from(outcome.status),
            Ok(wire::MutationStatus::Committed)
        ));
        let head = workspace.head().await?;
        assert_eq!(
            head.read("/tree/value", 64).await?,
            Bytes::from_static(b"canonical")
        );
        let child = head
            .fork("child", IdempotencyKey::from_bytes([3; 16]))
            .await?;
        assert_eq!(
            child.head().await?.read("/tree/value", 64).await?,
            b"canonical".as_slice()
        );

        stop_tx
            .send(())
            .map_err(|()| "hosted test server disappeared")?;
        server.await??;
        let _ = EmbeddedCapabilities::MEMORY;
        Ok(())
    }
}
