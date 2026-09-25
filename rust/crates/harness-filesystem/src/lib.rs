#![deny(unsafe_code)]
#![doc = include_str!("../README.md")]

use acyclic_fs::kernel::{FileKind, LogicalName};
use acyclic_fs::{
    ApplyOptions, AsyncAuthorityStore, AsyncObjectStore, ConflictSide, Digest, ForkOptions, Fs,
    Generation, GenerationId, IdempotencyKey as FilesystemKey, JoinCommitWitness, JoinHistory,
    JoinOutcome, JoinPlan, MergeConflict, MergeDriverRegistry, MergePlan, MergeResolutionCache,
    PublicationReservation, TransactionCommit, Workspace, WorkspaceDirectoryPage, WorkspaceError,
    WorkspaceStat,
};
use acyclic_harness::{
    AgentId, Error, IdempotencyKey, Result,
    conversation::{
        Attachment, ContentGrant, ContentResidencyVerifier, FileDescriptor, FileRef, Limits,
        ReferencedAttachments, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
        decode_complete_attachment_manifest,
    },
    core::{Authority, AuthorityVerifier, Reducer, Scope},
    distributed::SchedulerPayloadStore,
    fork::{
        ForkPublicationGuard, ForkSeed, ForkSeedVerifier, InheritedConversationPrefix,
        ReferenceGrant, ResourceRevision,
    },
    merge::{ProjectMergeReceipt, ProjectMergeVerifier, ProviderJoinProof},
    resources::{GenerationRef, ProviderRef, WorkspaceRef},
};
use bytes::Bytes;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

const FILESYSTEM_JOIN_PROOF_FORMAT: &str = "acyclic.filesystem.join-commit.v2";

mod execution_journal;
pub use execution_journal::FilesystemExecutionJournal;
mod interaction_host;
pub use interaction_host::FilesystemInteractionHost;
mod workflow_journal;
pub use workflow_journal::FilesystemWorkflowJournal;
#[cfg(feature = "memory")]
mod memory;
#[cfg(feature = "memory")]
pub use memory::MemoryHarnessStorage;

/// Owner-scoped scheduler result staging into one agent-private Filesystem volume.
pub struct FilesystemSchedulerPayloadStore<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    grant: ContentGrant,
    maximum_bytes: u64,
}

impl<A, O> FilesystemSchedulerPayloadStore<A, O> {
    /// Binds the exact private volume and authenticated write capability.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        maximum_bytes: u64,
    ) -> Result<Self> {
        if volume.class() != VolumeClass::AgentPrivate
            || volume.provider() != &host.provider
            || maximum_bytes == 0
        {
            return Err(Error::Invalid(
                "scheduler payload store requires a private volume and positive limit".into(),
            ));
        }
        let grant = ContentGrant::verify(verifier, scope, &volume, VolumeOperation::Write)?;
        Ok(Self {
            host,
            volume,
            grant,
            maximum_bytes,
        })
    }
}

impl<A, O> SchedulerPayloadStore for FilesystemSchedulerPayloadStore<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn stage<'a>(
        &'a self,
        operation_id: acyclic_harness::OperationId,
        idempotency_key: &'a str,
        bytes: &'a [u8],
    ) -> BoxFuture<'a, Result<FileRef>> {
        Box::pin(async move {
            let key = blake3::hash(idempotency_key.as_bytes())
                .to_hex()
                .to_string();
            let path = format!("scheduler/results/{operation_id}/{key}.json");
            let retry = IdempotencyKey::new(format!("scheduler-result:{operation_id}:{key}"))?;
            self.host
                .put_content(
                    &self.volume,
                    &self.grant,
                    &path,
                    bytes,
                    "application/json",
                    "result.json",
                    self.maximum_bytes,
                    &retry,
                )
                .await
        })
    }
}

/// Filesystem-backed admission barrier for version-pinned conversation content.
pub struct FilesystemContentVerifier<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    verifier: AuthorityVerifier,
    scope: Scope,
    maximum_bytes: u64,
}

impl<A, O> FilesystemContentVerifier<A, O> {
    /// Binds a verified caller scope and an upper bound for each admitted file.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        verifier: AuthorityVerifier,
        scope: Scope,
        maximum_bytes: u64,
    ) -> Result<Self> {
        if maximum_bytes == 0 {
            return Err(Error::Invalid(
                "content admission limit must be positive".into(),
            ));
        }
        verifier.verify(&scope)?;
        Ok(Self {
            host,
            verifier,
            scope,
            maximum_bytes,
        })
    }
}

impl<A, O> ContentResidencyVerifier for FilesystemContentVerifier<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn verify<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let grant = self.read_grant(reference)?;
            self.host
                .read_content(reference, &grant, self.maximum_bytes)
                .await?;
            Ok(())
        })
    }

    fn read<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            let grant = self.read_grant(reference)?;
            Ok(self
                .host
                .read_content(reference, &grant, self.maximum_bytes)
                .await?
                .to_vec())
        })
    }

    fn verify_manifest<'a>(
        &'a self,
        reference: &'a FileRef,
        item_count: u32,
        limits: &'a Limits,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            limits.validate_file(reference)?;
            for item in
                FilesystemContentVerifier::load_manifest(self, reference, item_count).await?
            {
                limits.validate_file(&item.file)?;
                self.verify(&item.file).await?;
            }
            Ok(())
        })
    }

    fn load_manifest<'a>(
        &'a self,
        reference: &'a FileRef,
        item_count: u32,
    ) -> BoxFuture<'a, Result<Vec<Attachment>>> {
        Box::pin(async move {
            FilesystemContentVerifier::load_manifest(self, reference, item_count).await
        })
    }
}

impl<A, O> FilesystemContentVerifier<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Lazily lists another agent's private directory under this reader's
    /// signed, owner-delegated subtree grant. Pagination pins one generation.
    pub async fn list_private_directory(
        &self,
        volume: &VolumeRef,
        granted_prefix: &str,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        after: Option<&LogicalName>,
        maximum_entries: u32,
    ) -> Result<(GenerationRef, WorkspaceDirectoryPage)> {
        let grant = ContentGrant::verify_directory_read(
            &self.verifier,
            &self.scope,
            volume,
            granted_prefix,
        )?;
        self.host
            .list_private_directory(
                volume,
                &grant,
                path,
                expected_generation,
                after,
                maximum_entries,
            )
            .await
    }

    /// Lazily reads a named private file and returns its pinned ref and bytes;
    /// the exact version can subsequently be shared independently of lineage.
    pub async fn read_private_path(
        &self,
        volume: &VolumeRef,
        granted_prefix: &str,
        path: &str,
        expected_generation: Option<&GenerationRef>,
    ) -> Result<(FileRef, Vec<u8>)> {
        let grant = ContentGrant::verify_directory_read(
            &self.verifier,
            &self.scope,
            volume,
            granted_prefix,
        )?;
        let (reference, bytes) = self
            .host
            .read_private_path(
                volume,
                &grant,
                path,
                expected_generation,
                self.maximum_bytes,
            )
            .await?;
        Ok((reference, bytes.to_vec()))
    }

    fn read_grant(&self, reference: &FileRef) -> Result<ContentGrant> {
        ContentGrant::verify_read(&self.verifier, &self.scope, reference)
    }

    async fn load_manifest(&self, reference: &FileRef, item_count: u32) -> Result<Vec<Attachment>> {
        let grant = self.read_grant(reference)?;
        let bytes = self
            .host
            .read_content(reference, &grant, self.maximum_bytes)
            .await?;
        acyclic_harness::conversation::decode_attachment_manifest(reference, &bytes, item_count)
    }
}

/// Filesystem portion of the fork-publication admission barrier.
pub struct FilesystemForkVerifier<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    maximum_inherited_bytes: u64,
    maximum_total_reference_bytes: u64,
}

struct FilesystemForkPublicationGuard<A, O> {
    workspace: Workspace<A, O>,
    reservation: PublicationReservation,
}

impl<A, O> ForkPublicationGuard for FilesystemForkPublicationGuard<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn release<'a>(&'a self) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.workspace
                .release_publication(self.reservation)
                .await
                .map_err(map_error)
        })
    }
}

impl<A, O> FilesystemForkVerifier<A, O> {
    /// Binds one Filesystem provider and a per-file fork-reference limit.
    pub fn new(host: Arc<FilesystemHost<A, O>>, maximum_inherited_bytes: u64) -> Result<Self> {
        if maximum_inherited_bytes == 0 {
            return Err(Error::Invalid(
                "fork inherited-content limit must be positive".into(),
            ));
        }
        Ok(Self {
            host,
            maximum_inherited_bytes,
            maximum_total_reference_bytes: maximum_inherited_bytes.saturating_mul(1_024),
        })
    }

    /// Narrows the aggregate bytes inspected during one fork publication.
    pub fn with_total_reference_bytes(mut self, maximum_bytes: u64) -> Result<Self> {
        if maximum_bytes == 0 {
            return Err(Error::Invalid(
                "fork total reference limit must be positive".into(),
            ));
        }
        self.maximum_total_reference_bytes = maximum_bytes;
        Ok(self)
    }
}

impl<A, O> ForkSeedVerifier for FilesystemForkVerifier<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn provider(&self) -> &ProviderRef {
        &self.host.provider
    }

    fn acquire_private_fence<'a>(
        &'a self,
        seed: &'a ForkSeed,
    ) -> BoxFuture<'a, Result<Box<dyn ForkPublicationGuard>>> {
        Box::pin(async move {
            if seed.child_private_volume.provider() != &self.host.provider {
                return Err(Error::Unauthorized(
                    "fork private volume belongs to another provider".into(),
                ));
            }
            let workspace = self
                .host
                .open(&workspace_ref(
                    self.host.provider.clone(),
                    &seed.child_private_volume.storage_name()?,
                )?)
                .await?;
            let reservation = workspace
                .reserve_publication(acyclic_fs::OperationId::from_bytes(
                    seed.operation_id.into_bytes(),
                ))
                .await
                .map_err(map_error)?;
            Ok(Box::new(FilesystemForkPublicationGuard {
                workspace,
                reservation,
            }) as Box<dyn ForkPublicationGuard>)
        })
    }

    fn read_manifest<'a>(&'a self, manifest: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            if manifest.volume().provider() != &self.host.provider {
                return Err(Error::Unauthorized(
                    "fork manifest belongs to another provider".into(),
                ));
            }
            let bytes = self
                .host
                .read_pinned(manifest, self.maximum_inherited_bytes)
                .await?;
            self.host.retain_file_generation(manifest).await?;
            Ok(bytes.to_vec())
        })
    }

    fn verify_file<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if file.volume().provider() != &self.host.provider {
                return Err(Error::Unauthorized(
                    "fork file belongs to another provider".into(),
                ));
            }
            self.host
                .read_pinned(file, self.maximum_inherited_bytes)
                .await?;
            self.host.retain_file_generation(file).await?;
            Ok(())
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one fork admission barrier validates every selected provider-owned revision before publication"
    )]
    fn verify<'a>(&'a self, seed: &'a ForkSeed) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            seed.validate()?;
            for resource in &seed.resources {
                if let (
                    ResourceRevision::Project {
                        volume: source,
                        generation: fork_point,
                    },
                    ResourceRevision::Project {
                        volume: child,
                        generation: initial,
                    },
                ) = (&resource.source, &resource.revision)
                    && source.provider() == &self.host.provider
                {
                    let source_workspace = self
                        .host
                        .open(&workspace_ref(
                            self.host.provider.clone(),
                            &source.storage_name()?,
                        )?)
                        .await?;
                    let child_workspace = self
                        .host
                        .open(&workspace_ref(
                            self.host.provider.clone(),
                            &child.storage_name()?,
                        )?)
                        .await?;
                    let expected_child = source_workspace
                        .fork_workspace_id(child_workspace.name().as_str())
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                    if child_workspace.id() != expected_child {
                        return Err(Error::Invalid(
                            "project capture is not a direct child workspace".into(),
                        ));
                    }
                    let source_generation =
                        self.host.generation(&source_workspace, fork_point).await?;
                    let child_generation = self.host.generation(&child_workspace, initial).await?;
                    if child_generation
                        .parents()
                        .await
                        .map_err(map_error)?
                        .as_slice()
                        != [source_generation.id()]
                    {
                        return Err(Error::Invalid(
                            "project capture is not forked from the selected generation".into(),
                        ));
                    }
                }
            }
            let mut unique_files = BTreeMap::new();
            for file in seed
                .inherited_context
                .iter()
                .chain(seed.reference_grants.iter().map(|grant| &grant.file))
                .chain(seed.attachment_manifests.iter())
            {
                if file.volume().provider() == &self.host.provider {
                    unique_files.insert(file.read_capability()?, file);
                }
            }
            let mut total = 0_u64;
            for file in unique_files.values() {
                total = total
                    .checked_add(file.descriptor().byte_length())
                    .ok_or_else(|| Error::Invalid("fork reference bytes overflow".into()))?;
                if total > self.maximum_total_reference_bytes {
                    return Err(Error::Invalid(
                        "fork reference bytes exceed aggregate limit".into(),
                    ));
                }
            }
            if seed.child_private_volume.provider() == &self.host.provider {
                let prefix = seed
                    .inherited_context
                    .iter()
                    .find(|file| file.path() == ".system/inherited-conversation/prefix.json");
                if seed.inherited_through_sequence > 0 && prefix.is_none() {
                    return Err(Error::Invalid(
                        "fork omitted its materialized conversation prefix".into(),
                    ));
                }
                if let Some(file) = prefix {
                    if file.descriptor().media_type()
                        != "application/vnd.acyclic.harness.inherited-conversation+json"
                    {
                        return Err(Error::Invalid(
                            "inherited conversation has the wrong media type".into(),
                        ));
                    }
                    let bytes = self
                        .host
                        .read_pinned(file, self.maximum_inherited_bytes)
                        .await?;
                    let inherited: InheritedConversationPrefix = serde_json::from_slice(&bytes)
                        .map_err(|_| {
                            Error::Invalid("inherited conversation is malformed".into())
                        })?;
                    if inherited.canonical_bytes()?.as_slice() != bytes.as_ref()
                        || file.display_name() != "inherited-conversation.json"
                    {
                        return Err(Error::Invalid(
                            "inherited conversation is not canonical".into(),
                        ));
                    }
                    if inherited.parent != seed.parent
                        || inherited.parent_revision != seed.parent_revision
                        || inherited.parent_agent == seed.child_agent
                        || inherited.through_sequence != seed.inherited_through_sequence
                        || inherited.attached_agents != seed.attached_agents
                        || inherited.messages.len() as u64 != seed.inherited_through_sequence
                    {
                        return Err(Error::Invalid(
                            "inherited conversation does not match the fork boundary".into(),
                        ));
                    }
                    for (index, message) in inherited.messages.iter().enumerate() {
                        message.validate()?;
                        if message.sequence != index as u64 + 1 {
                            return Err(Error::Invalid(
                                "inherited conversation is not a prefix".into(),
                            ));
                        }
                        let mut references = vec![&message.content];
                        match &message.attachments {
                            ReferencedAttachments::Inline { items } => {
                                references.extend(items.iter().map(|item| &item.file));
                            }
                            ReferencedAttachments::Manifest { manifest, .. } => {
                                references.push(manifest);
                            }
                        }
                        references.extend(message.extensions.values());
                        for reference in references {
                            for reader in std::iter::once(&seed.child_agent)
                                .chain(seed.attached_agents.iter())
                            {
                                let owner_read = reference.volume().class()
                                    == VolumeClass::AgentPrivate
                                    && reference.volume().owner() == &VolumeOwner::Agent(*reader);
                                let shared_read = reference.volume().class()
                                    == VolumeClass::SessionShared
                                    && seed.shared_grants.iter().any(|grant| {
                                        grant.child_agent == *reader
                                            && &grant.volume == reference.volume()
                                            && grant.operations.contains(&VolumeOperation::Read)
                                    });
                                let exact_read = seed.reference_grants.iter().any(|grant| {
                                    grant.reader == *reader
                                        && &grant.file == reference
                                        && grant.attachment_manifest.is_none()
                                });
                                if !owner_read && !shared_read && !exact_read {
                                    return Err(Error::Unauthorized(
                                        "inherited conversation reference lacks reader authority"
                                            .into(),
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            if seed.child_private_volume.provider() == &self.host.provider {
                let private = workspace_ref(
                    self.host.provider.clone(),
                    &seed.child_private_volume.storage_name()?,
                )?;
                let private_head = self.host.resolve(&private).await?.generation;
                if private_head != seed.child_private_generation {
                    return Err(Error::Conflict(
                        "child private generation changed before fork admission".into(),
                    ));
                }
                // A child-private volume may contain only the explicitly materialized
                // inherited prefix at publication. Existing scratch is never adopted.
                let expected = seed
                    .inherited_context
                    .iter()
                    .map(|file| {
                        if file.version() != hex::encode(private_head.as_resource().key()) {
                            return Err(Error::Invalid(
                                "inherited file is not pinned to the selected private generation"
                                    .into(),
                            ));
                        }
                        Ok(format!("/{}", file.path()))
                    })
                    .collect::<Result<BTreeSet<_>>>()?;
                if expected.len() != seed.inherited_context.len() {
                    return Err(Error::Invalid(
                        "inherited context has duplicate paths".into(),
                    ));
                }
                let mut expected_directories = BTreeSet::from([String::from("/")]);
                for path in &expected {
                    let mut parent = path.as_str();
                    while let Some((directory, _)) = parent.rsplit_once('/') {
                        if directory.is_empty() {
                            break;
                        }
                        expected_directories.insert(directory.to_owned());
                        parent = directory;
                    }
                }
                let mut observed = BTreeSet::new();
                let mut pending = vec![String::from("/")];
                let mut visited = 0_usize;
                while let Some(directory) = pending.pop() {
                    visited += 1;
                    if visited > 65_536 {
                        return Err(Error::Invalid(
                            "private volume directory limit exceeded".into(),
                        ));
                    }
                    let mut cursor = None;
                    loop {
                        let page = self
                            .host
                            .list_after(
                                &private,
                                Some(&private_head),
                                &directory,
                                cursor.as_ref(),
                                1_024,
                            )
                            .await?;
                        let next = page.entries.last().map(|entry| entry.name.clone());
                        for entry in page.entries {
                            let name =
                                std::str::from_utf8(entry.name.as_bytes()).map_err(|_| {
                                    Error::Invalid("private volume name is not UTF-8".into())
                                })?;
                            let path = if directory == "/" {
                                format!("/{name}")
                            } else {
                                format!("{directory}/{name}")
                            };
                            match entry.kind {
                                FileKind::Directory if expected_directories.contains(&path) => {
                                    pending.push(path)
                                }
                                FileKind::Regular if expected.contains(&path) => {
                                    observed.insert(path);
                                }
                                _ => {
                                    return Err(Error::Invalid(
                                        "child private volume contains unselected state".into(),
                                    ));
                                }
                            }
                        }
                        if !page.has_more {
                            break;
                        }
                        cursor = Some(next.ok_or_else(|| {
                            Error::Storage("private directory pagination did not advance".into())
                        })?);
                    }
                }
                if observed != expected {
                    return Err(Error::Invalid(
                        "child private volume lacks selected inherited context".into(),
                    ));
                }
                if self.host.resolve(&private).await?.generation != private_head {
                    return Err(Error::Conflict(
                        "child private volume changed during fork admission".into(),
                    ));
                }
                // The Stream fork event retains this exact immutable private view,
                // not whatever mutable head the child may later publish.
                self.host.retain_generation(&private, &private_head).await?;
            }
            for resource in &seed.resources {
                for revision in [&resource.source, &resource.revision] {
                    if revision.provider() != &self.host.provider {
                        continue;
                    }
                    match revision {
                        ResourceRevision::Project { volume, generation } => {
                            let workspace_ref =
                                workspace_ref(self.host.provider.clone(), &volume.storage_name()?)?;
                            let workspace = self.host.open(&workspace_ref).await?;
                            self.host.generation(&workspace, generation).await?;
                            self.host
                                .retain_generation(&workspace_ref, generation)
                                .await?;
                        }
                        ResourceRevision::SharedVolume(volume) => {
                            self.host
                                .resolve(&workspace_ref(
                                    self.host.provider.clone(),
                                    &volume.storage_name()?,
                                )?)
                                .await?;
                        }
                        ResourceRevision::History(_) => {
                            return Err(Error::Unsupported(
                                "Filesystem cannot prove Stream history".into(),
                            ));
                        }
                        ResourceRevision::Context(_)
                        | ResourceRevision::Process(_)
                        | ResourceRevision::Artifact(_)
                        | ResourceRevision::Extension { .. } => {
                            return Err(Error::Unsupported(
                                "fork resource requires its owning provider verifier".into(),
                            ));
                        }
                    }
                }
            }
            for reference in unique_files.values() {
                self.verify_file(reference).await?;
            }
            Ok(())
        })
    }
}

/// Resolved mutable workspace head and immutable generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceObservation {
    /// Provider-owned mutable workspace identity.
    pub workspace: WorkspaceRef,
    /// Exact immutable head observed by this operation.
    pub generation: GenerationRef,
}

/// One code-defined member of an atomic workspace mutation batch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceMutation {
    /// Creates every missing directory on an absolute path.
    CreateDirectory {
        /// Canonical absolute path.
        path: String,
    },
    /// Creates or replaces one complete file.
    PutFile {
        /// Canonical absolute path.
        path: String,
        /// Complete file content.
        bytes: Vec<u8>,
    },
    /// Removes one file or empty directory.
    Remove {
        /// Canonical absolute path.
        path: String,
    },
}

/// Reserved host-owned file families. These are never accepted by the public
/// agent upload API, even when the agent owns the private volume.
#[derive(Clone, Copy)]
pub(crate) enum InternalContentClass {
    Interaction,
    Workflow,
    Execution,
}

impl InternalContentClass {
    fn prefix(self) -> &'static str {
        match self {
            Self::Interaction => ".system/interactions/",
            Self::Workflow => ".system/workflows/",
            Self::Execution => ".system/execution/",
        }
    }
}

/// Adapter over any embedded, local, or distributed Filesystem provider pair.
#[derive(Clone)]
pub struct FilesystemHost<A, O> {
    filesystem: Fs<A, O>,
    provider: ProviderRef,
}

/// Provider-side proof for a parent-published project merge receipt.
pub struct FilesystemProjectMergeVerifier<A, O> {
    host: Arc<FilesystemHost<A, O>>,
}

impl<A, O> FilesystemProjectMergeVerifier<A, O> {
    /// Binds verification to one exact Filesystem provider identity.
    #[must_use]
    pub fn new(host: Arc<FilesystemHost<A, O>>) -> Self {
        Self { host }
    }
}

impl<A, O> ProjectMergeVerifier for FilesystemProjectMergeVerifier<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn verify<'a>(&'a self, receipt: &'a ProjectMergeReceipt) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if receipt.source_project.provider() != &self.host.provider
                || receipt.target_project.provider() != &self.host.provider
                || receipt.provider_proof.provider != self.host.provider
            {
                return Err(Error::Invalid(
                    "merge receipt belongs to another provider or retry identity".into(),
                ));
            }
            receipt.provider_proof.validate()?;
            let source = workspace_ref(
                self.host.provider.clone(),
                &receipt.source_project.storage_name()?,
            )?;
            let target = workspace_ref(
                self.host.provider.clone(),
                &receipt.target_project.storage_name()?,
            )?;
            let source_workspace = self.host.open(&source).await?;
            let target_workspace = self.host.open(&target).await?;
            if receipt.provider_proof.format != FILESYSTEM_JOIN_PROOF_FORMAT {
                return Err(Error::Invalid(
                    "merge receipt has an unsupported provider proof".into(),
                ));
            }
            let witness: JoinCommitWitness =
                serde_json::from_value(receipt.provider_proof.statement.clone())
                    .map_err(|error| Error::Invalid(error.to_string()))?;
            let source_generation = self
                .host
                .generation(&source_workspace, &receipt.source_generation)
                .await?;
            let expected = self
                .host
                .generation(&target_workspace, &receipt.expected_target_generation)
                .await?;
            let result = self
                .host
                .generation(&target_workspace, &receipt.result_generation)
                .await?;
            if witness.source_workspace() != source_workspace.id().volume_id()
                || witness.target_workspace() != target_workspace.id().volume_id()
                || witness.source_generation() != source_generation.id()
                || witness.expected_target() != expected.id()
                || witness.result_generation() != result.id()
                || witness.operation_id().into_bytes() != receipt.filesystem_operation_id
                || witness.history() != JoinHistory::Merge
                || !target_workspace
                    .verify_join_commit(&witness)
                    .await
                    .map_err(map_error)?
            {
                return Err(Error::Invalid(
                    "merge receipt does not prove the committed Filesystem join".into(),
                ));
            }
            let normalized_source = source_generation
                .normalized_join_parent_for(&expected)
                .await
                .map_err(map_error)?;
            let parents = result.parents().await.map_err(map_error)?;
            if parents.as_slice() != [expected.id(), normalized_source] {
                return Err(Error::Invalid(
                    "merge result does not bind the inspected child and target generations".into(),
                ));
            }
            self.host
                .retain_generation(&source, &receipt.source_generation)
                .await?;
            self.host
                .retain_generation(&target, &receipt.expected_target_generation)
                .await?;
            self.host
                .retain_generation(&target, &receipt.result_generation)
                .await?;
            Ok(())
        })
    }
}

/// Parent-owned project operations. A child may report its changes, but cannot
/// fork or prepare promotion using a scope issued for its own conversation.
pub struct ParentProjectController<'a, A, O> {
    host: &'a FilesystemHost<A, O>,
    parent: Authority,
    project: VolumeRef,
    verifier: AuthorityVerifier,
    scope: Scope,
}

/// Inspected project changes whose publication remains bound to the parent controller.
/// The underlying Filesystem plan is deliberately not exposed.
pub struct ParentMergePlan<A, O> {
    plan: JoinPlan<A, O>,
    child_project: VolumeRef,
    parent_project: VolumeRef,
    parent_scope: Scope,
}

/// Prepared child-owned prefix and the exact read grants needed by every
/// attached reader. Feed these fields into the parent-controlled `ForkReport`.
pub struct InheritedContextCapture {
    pub file: FileRef,
    pub generation: GenerationRef,
    pub reference_grants: Vec<ReferenceGrant>,
    pub attachment_manifests: Vec<FileRef>,
}

impl<A, O> ParentMergePlan<A, O> {
    /// Exact parent generation observed while planning; publication uses this CAS target.
    #[must_use]
    pub const fn target_head(&self) -> GenerationId {
        self.plan.target_head()
    }

    /// Exact child generation whose changes were inspected.
    #[must_use]
    pub const fn source_head(&self) -> GenerationId {
        self.plan.source_head()
    }

    /// Exact common ancestor used for the three-way merge.
    #[must_use]
    pub const fn common_ancestor(&self) -> GenerationId {
        self.plan.common_ancestor()
    }
}

impl<'a, A, O> ParentProjectController<'a, A, O> {
    /// Binds an authenticated parent conversation to its exact project volume.
    pub fn new(
        host: &'a FilesystemHost<A, O>,
        parent: &Authority,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        project: VolumeRef,
    ) -> Result<Self> {
        if parent.kind != acyclic_harness::core::AggregateKind::Conversation {
            return Err(Error::Invalid(
                "project controller requires a parent conversation".into(),
            ));
        }
        verifier.verify_audience(parent)?;
        verifier.verify(scope)?;
        project.validate()?;
        if project.class() != VolumeClass::Project || project.provider() != &host.provider {
            return Err(Error::Invalid(
                "parent project belongs to another provider or class".into(),
            ));
        }
        Ok(Self {
            host,
            parent: parent.clone(),
            project,
            verifier: verifier.clone(),
            scope: scope.clone(),
        })
    }

    fn require(&self, capability: &str, operation: VolumeOperation) -> Result<()> {
        ContentGrant::verify(&self.verifier, &self.scope, &self.project, operation)?;
        if !self.scope.capabilities().contains(capability) {
            return Err(Error::Unauthorized(
                "parent project operation is not granted".into(),
            ));
        }
        Ok(())
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> ParentProjectController<'_, A, O> {
    /// Materializes an exact bounded parent prefix into a fresh child-private
    /// workspace. Only the parent controller can write this reserved path;
    /// ordinary agent file staging rejects every `.system` path.
    pub async fn materialize_inherited_conversation(
        &self,
        parent: &Reducer,
        child_private: &VolumeRef,
        child_agent: AgentId,
        attached_agents: &[AgentId],
        through_sequence: u64,
        maximum_messages: usize,
        maximum_bytes: u64,
        maximum_references: usize,
        resolver: &dyn ContentResidencyVerifier,
        idempotency_key: &IdempotencyKey,
    ) -> Result<InheritedContextCapture> {
        self.require("fork:publish", VolumeOperation::Read)?;
        if parent.authority() != &self.parent
            || parent.conversation().and_then(|state| state.agent) != self.scope.agent()
            || child_private.provider() != &self.host.provider
            || child_private.class() != VolumeClass::AgentPrivate
            || child_private.owner() != &VolumeOwner::Agent(child_agent)
        {
            return Err(Error::Unauthorized(
                "inherited context is not parent-controlled and child-owned".into(),
            ));
        }
        child_private.validate()?;
        let history = parent
            .conversation()
            .ok_or_else(|| Error::Invalid("parent is not a conversation".into()))?;
        let count = usize::try_from(through_sequence)
            .map_err(|_| Error::Invalid("inherited prefix is too large".into()))?;
        if count > maximum_messages || count > history.messages.len() {
            return Err(Error::Invalid(
                "inherited prefix exceeds the selected history or limit".into(),
            ));
        }
        let prefix = InheritedConversationPrefix::select(
            self.parent.clone(),
            parent.revision(),
            history
                .agent
                .ok_or_else(|| Error::Conflict("parent conversation is unbound".into()))?,
            through_sequence,
            attached_agents,
            &history.messages,
        )?;
        let mut direct = BTreeMap::<String, FileRef>::new();
        let mut member = BTreeMap::<(String, String), (FileRef, FileRef)>::new();
        let mut manifests = BTreeMap::<String, FileRef>::new();
        let mut manifest_bytes_total = 0_u64;
        for message in &prefix.messages {
            direct.insert(message.content.read_capability()?, message.content.clone());
            for reference in message.extensions.values() {
                direct.insert(reference.read_capability()?, reference.clone());
            }
            match &message.attachments {
                ReferencedAttachments::Inline { items } => {
                    for item in items {
                        direct.insert(item.file.read_capability()?, item.file.clone());
                    }
                }
                ReferencedAttachments::Manifest {
                    manifest,
                    item_count,
                } => {
                    manifest_bytes_total = manifest_bytes_total
                        .checked_add(manifest.descriptor().byte_length())
                        .ok_or_else(|| {
                            Error::Invalid("inherited manifests exceed byte limit".into())
                        })?;
                    if manifest_bytes_total > maximum_bytes {
                        return Err(Error::Invalid(
                            "inherited attachment manifest exceeds limit".into(),
                        ));
                    }
                    let manifest_bytes = resolver.read(manifest).await?;
                    let items = decode_complete_attachment_manifest(manifest, &manifest_bytes)?;
                    if items.len() != *item_count as usize {
                        return Err(Error::Invalid(
                            "inherited attachment manifest count mismatch".into(),
                        ));
                    }
                    let key = manifest.read_capability()?;
                    direct.insert(key.clone(), manifest.clone());
                    manifests.insert(key.clone(), manifest.clone());
                    for item in items {
                        member.insert(
                            (key.clone(), item.file.read_capability()?),
                            (manifest.clone(), item.file),
                        );
                    }
                }
            }
            if direct.len().saturating_add(member.len()) > maximum_references {
                return Err(Error::Invalid(
                    "inherited reference count exceeds limit".into(),
                ));
            }
        }
        let mut readers = BTreeSet::from([child_agent]);
        readers.extend(attached_agents.iter().copied());
        if readers.len() != attached_agents.len().saturating_add(1) {
            return Err(Error::Invalid("inherited readers are duplicated".into()));
        }
        let total_grants = direct
            .len()
            .saturating_add(member.len())
            .saturating_mul(readers.len());
        if total_grants > maximum_references {
            return Err(Error::Invalid("inherited read grants exceed limit".into()));
        }
        let mut reference_grants = Vec::with_capacity(total_grants);
        for reader in readers {
            for file in direct.values() {
                if file.volume().class() == VolumeClass::AgentPrivate
                    && file.volume().owner() == &VolumeOwner::Agent(reader)
                {
                    continue;
                }
                reference_grants.push(ReferenceGrant {
                    file: file.clone(),
                    reader,
                    attachment_manifest: None,
                });
            }
            for (manifest, file) in member.values() {
                if file.volume().class() == VolumeClass::AgentPrivate
                    && file.volume().owner() == &VolumeOwner::Agent(reader)
                {
                    continue;
                }
                reference_grants.push(ReferenceGrant {
                    file: file.clone(),
                    reader,
                    attachment_manifest: Some(manifest.clone()),
                });
            }
        }
        let bytes = prefix.canonical_bytes()?;
        if bytes.len() as u64 > maximum_bytes {
            return Err(Error::Invalid(
                "inherited context exceeds byte limit".into(),
            ));
        }
        let path = ".system/inherited-conversation/prefix.json";
        let descriptor = FileDescriptor::from_bytes(
            &bytes,
            "application/vnd.acyclic.harness.inherited-conversation+json",
        )?;
        let workspace = workspace_ref(self.host.provider.clone(), &child_private.storage_name()?)?;
        let generation = if let Some(prior) = self
            .host
            .open(&workspace)
            .await?
            .operation_generation(filesystem_key(idempotency_key))
            .await
            .map_err(map_error)?
        {
            let prior = self.host.generation_ref(&prior)?;
            if self.host.resolve(&workspace).await?.generation != prior {
                return Err(Error::Conflict(
                    "child private volume changed after inherited-context staging".into(),
                ));
            }
            prior
        } else {
            let empty = self.host.resolve(&workspace).await?.generation;
            let first_page = self
                .host
                .list_after(&workspace, Some(&empty), "/", None, 1)
                .await?;
            if !first_page.entries.is_empty() {
                return Err(Error::Conflict("child private volume is not fresh".into()));
            }
            self.host
                .apply(
                    &workspace,
                    Some(&empty),
                    &[
                        WorkspaceMutation::CreateDirectory {
                            path: "/.system/inherited-conversation".into(),
                        },
                        WorkspaceMutation::PutFile {
                            path: format!("/{path}"),
                            bytes: bytes.clone(),
                        },
                    ],
                    idempotency_key,
                )
                .await?
        };
        self.host.retain_generation(&workspace, &generation).await?;
        let reference = FileRef::new(
            child_private.clone(),
            path,
            hex::encode(generation.as_resource().key()),
            descriptor,
            "inherited-conversation.json",
        )?;
        let committed = self
            .host
            .read_pinned(&reference, maximum_bytes)
            .await
            .map_err(|_| {
                Error::Conflict(
                    "inherited-context retry identity belongs to different bytes".into(),
                )
            })?;
        if committed.as_ref() != bytes {
            return Err(Error::Conflict(
                "inherited-context retry identity belongs to different prefix".into(),
            ));
        }
        Ok(InheritedContextCapture {
            file: reference,
            generation,
            reference_grants,
            attachment_manifests: manifests.into_values().collect(),
        })
    }

    /// Forks the parent's exact project generation into a child project.
    pub async fn fork_project(
        &self,
        source_generation: &GenerationRef,
        child: &VolumeRef,
        idempotency_key: &IdempotencyKey,
    ) -> Result<WorkspaceObservation> {
        self.require("fork:publish", VolumeOperation::Read)?;
        self.host
            .fork_project(&self.project, source_generation, child, idempotency_key)
            .await
    }

    /// Inspects a child's changes for an explicit parent-authorized promotion.
    pub async fn prepare_project_merge(&self, child: &VolumeRef) -> Result<ParentMergePlan<A, O>> {
        self.require("project:merge", VolumeOperation::Write)?;
        Ok(ParentMergePlan {
            plan: self
                .host
                .prepare_project_merge(child, &self.project)
                .await?,
            child_project: child.clone(),
            parent_project: self.project.clone(),
            parent_scope: self.scope.clone(),
        })
    }

    /// Publishes only a plan prepared under this same authenticated parent scope.
    /// The exact inspected target generation is always the Filesystem CAS precondition.
    pub async fn apply_project_merge(
        &self,
        plan: &ParentMergePlan<A, O>,
        idempotency_key: FilesystemKey,
    ) -> Result<JoinOutcome<A, O>> {
        self.require_merge_plan(plan)?;
        plan.plan
            .apply(Self::merge_options(plan, idempotency_key))
            .await
            .map_err(map_error)
    }

    /// Converts only a durable successful join into an immutable Harness
    /// receipt. Stale/conflicted/fenced outcomes cannot be published as merges.
    pub fn merge_receipt(
        &self,
        plan: &ParentMergePlan<A, O>,
        outcome: &JoinOutcome<A, O>,
        child: Authority,
        operation_id: acyclic_harness::OperationId,
        filesystem_key: FilesystemKey,
        notice: acyclic_harness::conversation::ConversationMessage,
    ) -> Result<ProjectMergeReceipt> {
        self.require_merge_plan(plan)?;
        let application = match outcome {
            JoinOutcome::Applied(generation) | JoinOutcome::AlreadyApplied(generation) => {
                generation
            }
            _ => {
                return Err(Error::Conflict(
                    "project join did not publish a result".into(),
                ));
            }
        };
        let witness = plan
            .plan
            .commit_witness(application, filesystem_key)
            .map_err(map_error)?;
        let receipt = ProjectMergeReceipt {
            operation_id,
            child,
            source_project: plan.child_project.clone(),
            source_generation: self.host.generation_ref_id(plan.source_head())?,
            target_project: self.project.clone(),
            expected_target_generation: self.host.generation_ref_id(plan.target_head())?,
            result_generation: self.host.generation_ref(application.generation())?,
            filesystem_operation_id: filesystem_key.into_bytes(),
            provider_proof: ProviderJoinProof {
                provider: self.host.provider.clone(),
                format: FILESYSTEM_JOIN_PROOF_FORMAT.into(),
                statement: serde_json::to_value(witness)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            },
            notice,
        };
        Ok(receipt)
    }

    /// Describes exact three-way conflicts without yielding the publishable plan.
    pub async fn describe_project_merge_conflicts(
        &self,
        plan: &ParentMergePlan<A, O>,
        conflicts: &[MergeConflict],
        truncated: bool,
    ) -> Result<MergePlan> {
        self.require_merge_plan(plan)?;
        plan.plan
            .describe_conflicts(conflicts, truncated)
            .await
            .map_err(map_error)
    }

    /// Resolves conflicts with registered immutable-input drivers under parent authority.
    pub async fn apply_project_merge_with_drivers<C: MergeResolutionCache>(
        &self,
        plan: &ParentMergePlan<A, O>,
        idempotency_key: FilesystemKey,
        registry: &MergeDriverRegistry,
        cache: &mut C,
        replanning: bool,
    ) -> Result<JoinOutcome<A, O>> {
        self.require_merge_plan(plan)?;
        plan.plan
            .apply_with_drivers(
                Self::merge_options(plan, idempotency_key),
                registry,
                cache,
                replanning,
            )
            .await
            .map_err(|error| Error::Storage(error.to_string()))
    }

    /// Applies an explicit, exact conflict-side selection under parent authority.
    pub async fn apply_project_merge_sides(
        &self,
        plan: &ParentMergePlan<A, O>,
        idempotency_key: FilesystemKey,
        selections: BTreeMap<MergeConflict, ConflictSide>,
    ) -> Result<JoinOutcome<A, O>> {
        self.require_merge_plan(plan)?;
        plan.plan
            .apply_sides(Self::merge_options(plan, idempotency_key), selections)
            .await
            .map_err(|error| Error::Storage(error.to_string()))
    }

    fn require_merge_plan(&self, plan: &ParentMergePlan<A, O>) -> Result<()> {
        self.require("project:merge", VolumeOperation::Write)?;
        if plan.parent_project != self.project || plan.parent_scope != self.scope {
            return Err(Error::Unauthorized(
                "merge plan belongs to another parent controller".into(),
            ));
        }
        Ok(())
    }

    fn merge_options(plan: &ParentMergePlan<A, O>, idempotency_key: FilesystemKey) -> ApplyOptions {
        ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key,
        }
    }
}

impl<A, O> FilesystemHost<A, O> {
    /// Binds one Filesystem deployment and immutable provider identity.
    pub fn new(filesystem: Fs<A, O>, provider: ProviderRef) -> Result<Self> {
        provider.validate()?;
        if provider.family() != "filesystem" {
            return Err(Error::Invalid(
                "Filesystem adapter requires the filesystem provider family".into(),
            ));
        }
        Ok(Self {
            filesystem,
            provider,
        })
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Forks a project workspace from one exact generation into a new project volume.
    async fn fork_project(
        &self,
        source: &VolumeRef,
        source_generation: &GenerationRef,
        child: &VolumeRef,
        idempotency_key: &IdempotencyKey,
    ) -> Result<WorkspaceObservation> {
        self.require_project_pair(source, child)?;
        if source == child {
            return Err(Error::Invalid(
                "project fork requires a distinct child volume".into(),
            ));
        }
        let source_name = source.storage_name()?;
        let source_workspace = self
            .open(&workspace_ref(self.provider.clone(), &source_name)?)
            .await?;
        let generation = self
            .generation(&source_workspace, source_generation)
            .await?;
        self.retain_generation(
            &workspace_ref(self.provider.clone(), &source_name)?,
            source_generation,
        )
        .await?;
        let child_name = child.storage_name()?;
        let forked = source_workspace
            .fork(
                &child_name,
                ForkOptions::from_generation(generation, filesystem_key(idempotency_key)),
            )
            .await
            .map_err(map_error)?;
        let child_workspace = workspace_ref(self.provider.clone(), &child_name)?;
        let child_generation = self.generation_ref(&forked.head().await.map_err(map_error)?)?;
        self.retain_generation(&child_workspace, &child_generation)
            .await?;
        Ok(WorkspaceObservation {
            workspace: child_workspace,
            generation: child_generation,
        })
    }

    /// Prepares an inspected, conflict-aware child-to-parent project merge.
    /// Publication remains an explicit Filesystem `JoinPlan` application.
    async fn prepare_project_merge(
        &self,
        child: &VolumeRef,
        parent: &VolumeRef,
    ) -> Result<JoinPlan<A, O>> {
        self.require_project_pair(child, parent)?;
        let child_workspace = self
            .open(&workspace_ref(
                self.provider.clone(),
                &child.storage_name()?,
            )?)
            .await?;
        let parent_workspace = self
            .open(&workspace_ref(
                self.provider.clone(),
                &parent.storage_name()?,
            )?)
            .await?;
        let expected_child = parent_workspace
            .fork_workspace_id(child_workspace.name().as_str())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        if child_workspace.id() != expected_child {
            return Err(Error::Unauthorized(
                "project merge target is not the child's direct parent".into(),
            ));
        }
        child_workspace
            .join_into(&parent_workspace)
            .plan()
            .await
            .map_err(map_error)
    }

    fn require_project_pair(&self, source: &VolumeRef, target: &VolumeRef) -> Result<()> {
        source.validate()?;
        target.validate()?;
        if source.class() != VolumeClass::Project
            || target.class() != VolumeClass::Project
            || source.provider() != &self.provider
            || target.provider() != &self.provider
            || source.owner() != target.owner()
        {
            return Err(Error::Invalid(
                "project fork or merge requires two project volumes".into(),
            ));
        }
        Ok(())
    }

    /// Creates the unique physical workspace for a logical volume.
    pub async fn create_volume(&self, volume: &VolumeRef) -> Result<WorkspaceObservation> {
        volume.validate()?;
        if volume.provider() != &self.provider {
            return Err(Error::Invalid(
                "volume belongs to another Filesystem provider".into(),
            ));
        }
        let name = volume.storage_name()?;
        self.filesystem
            .create_workspace(&name)
            .await
            .map_err(map_error)?;
        self.resolve(&workspace_ref(self.provider.clone(), &name)?)
            .await
    }

    /// Stages an immutable version of one volume file before conversation admission.
    ///
    /// The caller must append the returned reference only after this operation succeeds.
    /// A failed later append may leave an unreferenced pinned generation. Retention
    /// remains until the provider offers an explicit, safe release operation.
    #[allow(
        clippy::too_many_arguments,
        reason = "public upload names every identity, descriptor field, bound, and retry key explicitly"
    )]
    pub async fn put_content(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        maximum_bytes: u64,
        idempotency_key: &IdempotencyKey,
    ) -> Result<FileRef> {
        self.put_content_impl(
            volume,
            grant,
            path,
            bytes,
            media_type,
            display_name,
            maximum_bytes,
            idempotency_key,
            None,
        )
        .await
    }

    /// Trusted journal staging retains the same atomic metadata and retry
    /// checks as public uploads, but may write only its assigned subtree.
    #[allow(
        clippy::too_many_arguments,
        reason = "internal stage mirrors the complete file contract"
    )]
    pub(crate) async fn put_internal_content(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        maximum_bytes: u64,
        idempotency_key: &IdempotencyKey,
        class: InternalContentClass,
    ) -> Result<FileRef> {
        self.put_content_impl(
            volume,
            grant,
            path,
            bytes,
            media_type,
            display_name,
            maximum_bytes,
            idempotency_key,
            Some(class),
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one implementation pins the complete staged file contract"
    )]
    async fn put_content_impl(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        maximum_bytes: u64,
        idempotency_key: &IdempotencyKey,
        internal_class: Option<InternalContentClass>,
    ) -> Result<FileRef> {
        grant.require(volume, VolumeOperation::Write)?;
        if volume.provider() != &self.provider || bytes.len() as u64 > maximum_bytes {
            return Err(Error::Invalid(
                "content provider or size limit mismatch".into(),
            ));
        }
        let descriptor = FileDescriptor::from_bytes(bytes, media_type)?;
        // Validate the path before opening a workspace or writing bytes.
        let provisional = FileRef::new(
            volume.clone(),
            path,
            "pending",
            descriptor.clone(),
            display_name,
        )?;
        // The same operation ID must pin the entire file contract, not just
        // the byte payload. A sidecar in the atomic generation proves path,
        // media type, display name, and descriptor on every retry.
        match internal_class {
            Some(class)
                if volume.class() == VolumeClass::AgentPrivate
                    && provisional.path().starts_with(class.prefix()) => {}
            Some(_) => {
                return Err(Error::Unauthorized(
                    "journal path or volume is not assigned to this host".into(),
                ));
            }
            None if provisional.path() == ".system"
                || provisional.path().starts_with(".system/") =>
            {
                return Err(Error::Invalid("internal storage paths are reserved".into()));
            }
            None => {}
        }
        let receipt_path = format!(
            ".system/harness-uploads/{}.json",
            blake3::hash(idempotency_key.as_str().as_bytes()).to_hex()
        );
        let metadata_path = content_metadata_path(provisional.path());
        let receipt_bytes =
            serde_json::to_vec(&provisional).map_err(|error| Error::Invalid(error.to_string()))?;
        let workspace = workspace_ref(self.provider.clone(), &volume.storage_name()?)?;
        if let Some(prior) = self
            .open(&workspace)
            .await?
            .operation_generation(filesystem_key(idempotency_key))
            .await
            .map_err(map_error)?
        {
            let generation = self.generation_ref(&prior)?;
            self.retain_generation(&workspace, &generation).await?;
            let version = hex::encode(generation.as_resource().key());
            let receipt = FileRef::new(
                volume.clone(),
                receipt_path.clone(),
                version.clone(),
                FileDescriptor::from_bytes(&receipt_bytes, "application/json")?,
                "receipt.json",
            )?;
            self.read_pinned(&receipt, receipt_bytes.len() as u64)
                .await
                .map_err(|_| {
                    Error::Conflict("upload identity belongs to different metadata".into())
                })?;
            let indexed = self
                .read(
                    &workspace,
                    Some(&generation),
                    &format!("/{metadata_path}"),
                    64 * 1024,
                )
                .await
                .map_err(|_| Error::Conflict("upload metadata index is missing".into()))?;
            if indexed.as_ref() != receipt_bytes.as_slice() {
                return Err(Error::Conflict(
                    "upload metadata index does not match".into(),
                ));
            }
            let reference = FileRef::new(
                volume.clone(),
                path,
                version,
                descriptor.clone(),
                display_name,
            )?;
            let committed = self
                .read_pinned(&reference, maximum_bytes)
                .await
                .map_err(|_| {
                    Error::Conflict("upload identity belongs to different content".into())
                })?;
            if committed.as_ref() != bytes {
                return Err(Error::Conflict(
                    "upload identity belongs to different content".into(),
                ));
            }
            return Ok(reference);
        }
        let absolute = format!("/{}", provisional.path());
        let mut mutations = Vec::new();
        if let Some((parent, _)) = absolute.rsplit_once('/')
            && !parent.is_empty()
        {
            mutations.push(WorkspaceMutation::CreateDirectory {
                path: parent.to_owned(),
            });
        }
        mutations.push(WorkspaceMutation::PutFile {
            path: absolute,
            bytes: bytes.to_vec(),
        });
        mutations.push(WorkspaceMutation::CreateDirectory {
            path: "/.system/harness-uploads".into(),
        });
        mutations.push(WorkspaceMutation::CreateDirectory {
            path: "/.system/harness-file-metadata".into(),
        });
        mutations.push(WorkspaceMutation::PutFile {
            path: format!("/{metadata_path}"),
            bytes: receipt_bytes.clone(),
        });
        mutations.push(WorkspaceMutation::PutFile {
            path: format!("/{receipt_path}"),
            bytes: receipt_bytes,
        });
        let generation = self
            .apply(&workspace, None, &mutations, idempotency_key)
            .await?;
        self.retain_generation(&workspace, &generation).await?;
        let version = hex::encode(generation.as_resource().key());
        let reference = FileRef::new(volume.clone(), path, version, descriptor, display_name)?;
        // Read back the exact committed generation, not the mutable workspace head.
        let committed = self.read_pinned(&reference, maximum_bytes).await?;
        reference.descriptor().verify(&committed)?;
        Ok(reference)
    }

    /// Stages a complete canonical attachment list for a manifest-backed event.
    pub async fn put_attachment_manifest(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        items: &[Attachment],
        maximum_bytes: u64,
        idempotency_key: &IdempotencyKey,
    ) -> Result<ReferencedAttachments> {
        let bytes = acyclic_harness::conversation::encode_attachment_manifest(items)?;
        let manifest = self
            .put_content(
                volume,
                grant,
                path,
                &bytes,
                "application/vnd.acyclic.harness.attachments+json",
                "attachments.json",
                maximum_bytes,
                idempotency_key,
            )
            .await?;
        Ok(ReferencedAttachments::Manifest {
            manifest,
            item_count: u32::try_from(items.len())
                .map_err(|_| Error::Invalid("attachment count exceeds limit".into()))?,
        })
    }

    /// Reads a pinned version after exact-volume access verification.
    pub async fn read_content(
        &self,
        reference: &FileRef,
        grant: &ContentGrant,
        maximum_bytes: u64,
    ) -> Result<Bytes> {
        reference.validate()?;
        grant.require_file_read(reference)?;
        let bytes = self.read_pinned(reference, maximum_bytes).await?;
        let generation = self.file_generation(reference)?;
        let workspace = workspace_ref(self.provider.clone(), &reference.volume().storage_name()?)?;
        // Admission cannot publish a ref whose verified generation can be
        // reclaimed before (or immediately after) the Stream append.
        self.retain_generation(&workspace, &generation).await?;
        Ok(bytes)
    }

    /// Discovers a granted private directory at the owner's current head.
    /// The physical workspace is opened only when this method is called;
    /// callers need no fork or pre-mounted copy of the owner's tree.
    pub async fn list_private_directory(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        after: Option<&LogicalName>,
        maximum_entries: u32,
    ) -> Result<(GenerationRef, WorkspaceDirectoryPage)> {
        self.require_private_directory(volume, grant, path)?;
        if maximum_entries == 0 || maximum_entries > 4096 {
            return Err(Error::Invalid(
                "private directory page limit is invalid".into(),
            ));
        }
        let workspace = workspace_ref(self.provider.clone(), &volume.storage_name()?)?;
        let observation = self.resolve(&workspace).await?;
        if expected_generation.is_some_and(|expected| expected != &observation.generation) {
            return Err(Error::Conflict(
                "private directory changed during pagination".into(),
            ));
        }
        let requested = if path.is_empty() {
            "/".to_owned()
        } else {
            format!("/{path}")
        };
        let mut page = self
            .list_after(
                &workspace,
                Some(&observation.generation),
                &requested,
                after,
                maximum_entries + u32::from(path.is_empty()),
            )
            .await?;
        if path.is_empty() {
            page.entries
                .retain(|entry| entry.name.unicode_text().as_deref() != Some(".system"));
            if page.entries.len() > maximum_entries as usize {
                page.entries.truncate(maximum_entries as usize);
                page.has_more = true;
            }
        }
        Ok((observation.generation, page))
    }

    /// Lazily resolves a named private file and returns an immutable ref plus
    /// verified bytes. This is directory authority, never write authority.
    pub async fn read_private_path(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        maximum_bytes: u64,
    ) -> Result<(FileRef, Bytes)> {
        self.require_private_directory(volume, grant, path)?;
        let name = path.rsplit('/').next().unwrap_or(path);
        let provisional = FileRef::new(
            volume.clone(),
            path,
            "pending",
            FileDescriptor::from_bytes(&[], "application/octet-stream")?,
            name,
        )?;
        let workspace = workspace_ref(self.provider.clone(), &volume.storage_name()?)?;
        let observation = self.resolve(&workspace).await?;
        if expected_generation.is_some_and(|expected| expected != &observation.generation) {
            return Err(Error::Conflict(
                "private file changed since directory listing".into(),
            ));
        }
        let bytes = self
            .read(
                &workspace,
                Some(&observation.generation),
                &format!("/{}", provisional.path()),
                maximum_bytes,
            )
            .await?;
        let metadata = self
            .read(
                &workspace,
                Some(&observation.generation),
                &format!("/{}", content_metadata_path(provisional.path())),
                64 * 1024,
            )
            .await;
        let (descriptor, display_name) = match metadata {
            Ok(metadata) => {
                let staged: FileRef = serde_json::from_slice(&metadata)
                    .map_err(|_| Error::Storage("private file metadata is corrupt".into()))?;
                if staged.volume() != volume
                    || staged.path() != path
                    || staged.version() != "pending"
                {
                    return Err(Error::Storage(
                        "private file metadata does not match the file".into(),
                    ));
                }
                staged.descriptor().verify(&bytes)?;
                (
                    staged.descriptor().clone(),
                    staged.display_name().to_owned(),
                )
            }
            Err(Error::NotFound(_)) => (
                FileDescriptor::from_bytes(&bytes, "application/octet-stream")?,
                name.to_owned(),
            ),
            Err(error) => return Err(error),
        };
        let reference = FileRef::new(
            volume.clone(),
            path,
            hex::encode(observation.generation.as_resource().key()),
            descriptor,
            display_name,
        )?;
        grant.require_file_read(&reference)?;
        self.retain_generation(&workspace, &observation.generation)
            .await?;
        Ok((reference, bytes))
    }

    fn require_private_directory(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
    ) -> Result<()> {
        volume.validate()?;
        volume.directory_read_capability(path)?;
        if volume.provider() != &self.provider || volume.class() != VolumeClass::AgentPrivate {
            return Err(Error::Invalid(
                "private directory belongs to another provider or class".into(),
            ));
        }
        grant.require_directory_path(volume, path)
    }

    async fn read_pinned(&self, reference: &FileRef, maximum_bytes: u64) -> Result<Bytes> {
        if reference.volume().provider() != &self.provider
            || reference.descriptor().byte_length() > maximum_bytes
        {
            return Err(Error::Invalid(
                "content provider or size limit mismatch".into(),
            ));
        }
        let generation = self.file_generation(reference)?;
        let workspace = workspace_ref(self.provider.clone(), &reference.volume().storage_name()?)?;
        let bytes = self
            .read(
                &workspace,
                Some(&generation),
                &format!("/{}", reference.path()),
                maximum_bytes,
            )
            .await?;
        reference.descriptor().verify(&bytes)?;
        Ok(bytes)
    }

    fn file_generation(&self, reference: &FileRef) -> Result<GenerationRef> {
        let raw = hex::decode(reference.version())
            .map_err(|_| Error::Invalid("file version is not a Filesystem generation".into()))?;
        GenerationRef::new(self.provider.clone(), raw, None)
    }

    /// Resolves the current immutable head of a named workspace.
    pub async fn resolve(&self, reference: &WorkspaceRef) -> Result<WorkspaceObservation> {
        let workspace = self.open(reference).await?;
        let head = workspace.head().await.map_err(map_error)?;
        Ok(WorkspaceObservation {
            workspace: reference.clone(),
            generation: self.generation_ref(&head)?,
        })
    }

    /// Reads one bounded file from the current head or an exact generation.
    pub async fn read(
        &self,
        workspace: &WorkspaceRef,
        generation: Option<&GenerationRef>,
        path: &str,
        maximum_bytes: u64,
    ) -> Result<Bytes> {
        let workspace = self.open(workspace).await?;
        match generation {
            Some(reference) => self
                .generation(&workspace, reference)
                .await?
                .read(path, maximum_bytes)
                .await
                .map_err(map_error),
            None => workspace.read(path, maximum_bytes).await.map_err(map_error),
        }
    }

    /// Stats one path at the current head or an exact generation.
    pub async fn stat(
        &self,
        workspace: &WorkspaceRef,
        generation: Option<&GenerationRef>,
        path: &str,
    ) -> Result<WorkspaceStat> {
        let workspace = self.open(workspace).await?;
        match generation {
            Some(reference) => self
                .generation(&workspace, reference)
                .await?
                .stat(path)
                .await
                .map_err(map_error),
            None => workspace.stat(path).await.map_err(map_error),
        }
    }

    /// Lists one bounded directory page at the current head or an exact generation.
    pub async fn list(
        &self,
        workspace: &WorkspaceRef,
        generation: Option<&GenerationRef>,
        path: &str,
        maximum_entries: u32,
    ) -> Result<WorkspaceDirectoryPage> {
        self.list_after(workspace, generation, path, None, maximum_entries)
            .await
    }

    /// Lists one bounded page after an exact child name cursor.
    pub async fn list_after(
        &self,
        workspace: &WorkspaceRef,
        generation: Option<&GenerationRef>,
        path: &str,
        after: Option<&LogicalName>,
        maximum_entries: u32,
    ) -> Result<WorkspaceDirectoryPage> {
        let workspace = self.open(workspace).await?;
        match generation {
            Some(reference) => self
                .generation(&workspace, reference)
                .await?
                .list_directory(path, after, maximum_entries)
                .await
                .map_err(map_error),
            None => workspace
                .list_directory(path, after, maximum_entries)
                .await
                .map_err(map_error),
        }
    }

    /// Applies one atomic, idempotent mutation batch against an optional exact generation.
    pub async fn apply(
        &self,
        workspace: &WorkspaceRef,
        expected_generation: Option<&GenerationRef>,
        mutations: &[WorkspaceMutation],
        idempotency_key: &IdempotencyKey,
    ) -> Result<GenerationRef> {
        let workspace = self.open(workspace).await?;
        let key = filesystem_key(idempotency_key);
        let mut transaction = match expected_generation {
            Some(reference) => {
                let generation = self.generation(&workspace, reference).await?;
                workspace
                    .begin_transaction_at(&generation, key)
                    .await
                    .map_err(map_error)?
            }
            None => workspace.begin_transaction(key).await.map_err(map_error)?,
        };
        for mutation in mutations {
            match mutation {
                WorkspaceMutation::CreateDirectory { path } => {
                    transaction.create_dir_all(path).await.map_err(map_error)?;
                }
                WorkspaceMutation::PutFile { path, bytes } => {
                    transaction
                        .write(path, Bytes::copy_from_slice(bytes))
                        .await
                        .map_err(map_error)?;
                }
                WorkspaceMutation::Remove { path } => {
                    transaction.remove(path).await.map_err(map_error)?;
                }
            }
        }
        match transaction.commit().await.map_err(map_error)? {
            TransactionCommit::Committed(generation)
            | TransactionCommit::AlreadyCommitted(generation) => self.generation_ref(&generation),
            TransactionCommit::Conflict { .. } => {
                Err(Error::Conflict("workspace generation changed".into()))
            }
            TransactionCommit::Fenced => Err(Error::Conflict("workspace writer was fenced".into())),
            TransactionCommit::IdempotencyConflict => Err(Error::Conflict(
                "filesystem idempotency key belongs to another mutation".into(),
            )),
        }
    }

    async fn open(&self, reference: &WorkspaceRef) -> Result<Workspace<A, O>> {
        reference.validate()?;
        self.validate_provider(reference.as_resource().provider())?;
        let name = std::str::from_utf8(reference.as_resource().key())
            .map_err(|_| Error::Invalid("workspace reference key must be UTF-8".into()))?;
        self.filesystem
            .open_workspace(name)
            .await
            .map_err(map_error)
    }

    async fn generation(
        &self,
        workspace: &Workspace<A, O>,
        reference: &GenerationRef,
    ) -> Result<Generation<A, O>> {
        reference.validate()?;
        self.validate_provider(reference.as_resource().provider())?;
        let bytes: [u8; 32] = reference
            .as_resource()
            .key()
            .try_into()
            .map_err(|_| Error::Invalid("generation reference key must be 32 bytes".into()))?;
        workspace
            .generation(GenerationId::new(Digest::from_bytes(bytes)))
            .await
            .map_err(map_error)
    }

    async fn retain_generation(
        &self,
        workspace: &WorkspaceRef,
        reference: &GenerationRef,
    ) -> Result<()> {
        let workspace = self.open(workspace).await?;
        let generation = self.generation(&workspace, reference).await?;
        let identity = format!(
            "harness-generation-{}",
            hex::encode(reference.as_resource().key())
        );
        generation.pin(identity).await.map_err(map_error)?;
        Ok(())
    }

    async fn retain_file_generation(&self, file: &FileRef) -> Result<()> {
        file.validate()?;
        if file.volume().provider() != &self.provider {
            return Err(Error::Unauthorized(
                "file belongs to another Filesystem provider".into(),
            ));
        }
        let workspace = workspace_ref(self.provider.clone(), &file.volume().storage_name()?)?;
        self.retain_generation(&workspace, &self.file_generation(file)?)
            .await
    }

    fn generation_ref(&self, generation: &Generation<A, O>) -> Result<GenerationRef> {
        GenerationRef::new(
            self.provider.clone(),
            generation.id().digest().into_bytes(),
            None,
        )
    }

    fn generation_ref_id(&self, id: GenerationId) -> Result<GenerationRef> {
        GenerationRef::new(self.provider.clone(), id.digest().into_bytes(), None)
    }

    fn validate_provider(&self, provider: &ProviderRef) -> Result<()> {
        provider.validate()?;
        if provider != &self.provider {
            return Err(Error::Invalid(
                "resource belongs to another Filesystem provider".into(),
            ));
        }
        Ok(())
    }
}

/// Creates the canonical provider-owned reference for a named workspace.
pub fn workspace_ref(provider: ProviderRef, name: &str) -> Result<WorkspaceRef> {
    acyclic_fs::WorkspaceName::new(name).map_err(|error| Error::Invalid(error.to_string()))?;
    WorkspaceRef::new(provider, name.as_bytes(), None)
}

fn filesystem_key(key: &IdempotencyKey) -> FilesystemKey {
    let digest = blake3::hash(key.as_str().as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    if bytes == [0; 16] {
        bytes[0] = 1;
    }
    FilesystemKey::from_bytes(bytes)
}

fn content_metadata_path(path: &str) -> String {
    format!(
        ".system/harness-file-metadata/{}.json",
        blake3::hash(path.as_bytes()).to_hex()
    )
}

fn map_error(error: WorkspaceError) -> Error {
    match error {
        WorkspaceError::NotFound => Error::NotFound("workspace path".into()),
        WorkspaceError::RetentionConflict
        | WorkspaceError::StaleGeneration
        | WorkspaceError::StaleIdentity => Error::Conflict(error.to_string()),
        WorkspaceError::Name(_)
        | WorkspaceError::Path(_)
        | WorkspaceError::ReadLimitExceeded
        | WorkspaceError::NotRegularFile
        | WorkspaceError::NotDirectory
        | WorkspaceError::ForeignGeneration
        | WorkspaceError::IncompatibleWorkspace
        | WorkspaceError::NoCommonAncestor
        | WorkspaceError::LineageLimit
        | WorkspaceError::JoinLimit
        | WorkspaceError::InvalidMergeResolution
        | WorkspaceError::ChangeSetContinuity
        | WorkspaceError::ChangedPathLimit
        | WorkspaceError::EmptyContentSet
        | WorkspaceError::NotFork
        | WorkspaceError::ContentLengthOverflow
        | WorkspaceError::Work(_) => Error::Invalid(error.to_string()),
        WorkspaceError::Cancelled(error) => Error::Storage(error.to_string()),
        WorkspaceError::Engine(value) => Error::Storage(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use acyclic_harness::core::{AggregateKind, Authority, AuthorityIssuer};
    use acyclic_harness::{
        AgentId, Capabilities,
        conversation::{VolumeClass, VolumeOwner},
    };

    #[tokio::test]
    async fn resolves_exact_memory_generation_and_rejects_foreign_provider() -> Result<()> {
        let filesystem = Fs::memory();
        let workspace = filesystem
            .create_workspace("adapter-test")
            .await
            .map_err(map_error)?;
        let expected = workspace.head().await.map_err(map_error)?;
        let provider = ProviderRef::new("example", "filesystem", "1")?;
        let host = FilesystemHost::new(filesystem, provider.clone())?;
        let reference = workspace_ref(provider, "adapter-test")?;
        let observed = host.resolve(&reference).await?;
        assert_eq!(
            observed.generation.as_resource().key(),
            expected.id().digest().into_bytes()
        );

        let foreign = workspace_ref(
            ProviderRef::new("other", "filesystem", "1")?,
            "adapter-test",
        )?;
        assert!(matches!(
            host.resolve(&foreign).await,
            Err(Error::Invalid(_))
        ));

        let updated = host
            .apply(
                &reference,
                Some(&observed.generation),
                &[WorkspaceMutation::PutFile {
                    path: "/hello.txt".into(),
                    bytes: b"hello".to_vec(),
                }],
                &IdempotencyKey::new("write-hello")?,
            )
            .await?;
        assert_eq!(
            host.read(&reference, Some(&updated), "/hello.txt", 16)
                .await?,
            Bytes::from_static(b"hello")
        );
        assert!(matches!(
            host.apply(
                &reference,
                Some(&observed.generation),
                &[WorkspaceMutation::PutFile {
                    path: "/hello.txt".into(),
                    bytes: b"changed".to_vec(),
                }],
                &IdempotencyKey::new("stale-write")?,
            )
            .await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn staged_content_is_version_pinned_and_requires_exact_grants() -> Result<()> {
        let filesystem = Fs::memory();
        let provider = ProviderRef::new("example", "filesystem", "2")?;
        let host = Arc::new(FilesystemHost::new(filesystem, provider.clone())?);
        let owner = AgentId::from_bytes([3; 16]);
        let volume = VolumeRef::new(
            provider,
            "agent-private-a",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(owner),
        )?;
        host.create_volume(&volume).await?;
        let issuer = AuthorityIssuer::new(
            "test",
            [4; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "test".into(),
            },
        );
        let scope = issuer.root_for_agent(
            owner,
            "owner",
            Capabilities::new([
                volume.capability(VolumeOperation::Read)?,
                volume.capability(VolumeOperation::Write)?,
            ]),
        );
        let verifier = issuer.verifier();
        let read = ContentGrant::verify(&verifier, &scope, &volume, VolumeOperation::Read)?;
        let write = ContentGrant::verify(&verifier, &scope, &volume, VolumeOperation::Write)?;
        let first = host
            .put_content(
                &volume,
                &write,
                "messages/first.txt",
                b"one",
                "text/plain",
                "first.txt",
                16,
                &IdempotencyKey::new("stage-first")?,
            )
            .await?;
        let second = host
            .put_content(
                &volume,
                &write,
                "messages/first.txt",
                b"two",
                "text/plain",
                "first.txt",
                16,
                &IdempotencyKey::new("stage-second")?,
            )
            .await?;
        assert_eq!(
            host.put_content(
                &volume,
                &write,
                "messages/first.txt",
                b"one",
                "text/plain",
                "first.txt",
                16,
                &IdempotencyKey::new("stage-first")?
            )
            .await?,
            first
        );
        for (path, media_type, display_name) in [
            (
                "messages/first.txt",
                "application/octet-stream",
                "first.txt",
            ),
            ("messages/first.txt", "text/plain", "renamed.txt"),
            ("messages/other.txt", "text/plain", "first.txt"),
        ] {
            assert!(
                host.put_content(
                    &volume,
                    &write,
                    path,
                    b"one",
                    media_type,
                    display_name,
                    16,
                    &IdempotencyKey::new("stage-first")?
                )
                .await
                .is_err()
            );
        }
        assert_ne!(first.version(), second.version());
        assert_eq!(
            host.read_content(&first, &read, 16).await?,
            Bytes::from_static(b"one")
        );
        assert_eq!(
            host.read_content(&second, &read, 16).await?,
            Bytes::from_static(b"two")
        );
        let delegated_scope = issuer.delegate_private_directory_read(
            &scope,
            AgentId::from_bytes([8; 16]),
            "reader",
            &volume,
            "messages",
        )?;
        let delegated_reader = FilesystemContentVerifier::new(
            host.clone(),
            verifier.clone(),
            delegated_scope.clone(),
            16,
        )?;
        let (listed_generation, page) = delegated_reader
            .list_private_directory(&volume, "messages", "messages", None, None, 10)
            .await?;
        assert_eq!(page.entries.len(), 1);
        let (pinned, delegated_bytes) = delegated_reader
            .read_private_path(
                &volume,
                "messages",
                "messages/first.txt",
                Some(&listed_generation),
            )
            .await?;
        assert_eq!(pinned, second);
        assert_eq!(delegated_bytes, b"two");
        assert!(
            delegated_reader
                .list_private_directory(&volume, "messages", "elsewhere", None, None, 10)
                .await
                .is_err()
        );
        let delegated =
            ContentGrant::verify_directory_read(&verifier, &delegated_scope, &volume, "messages")?;
        let (discovered, bytes) = host
            .read_private_path(&volume, &delegated, "messages/first.txt", None, 16)
            .await?;
        assert_eq!(bytes, Bytes::from_static(b"two"));
        assert_eq!(discovered, second);
        assert!(
            host.put_content(
                &volume,
                &write,
                ".system/forged.json",
                b"bad",
                "application/json",
                "forged.json",
                16,
                &IdempotencyKey::new("internal-write")?
            )
            .await
            .is_err()
        );
        let empty = host
            .put_content(
                &volume,
                &write,
                "messages/empty.txt",
                b"",
                "text/plain",
                "empty.txt",
                16,
                &IdempotencyKey::new("stage-empty")?,
            )
            .await?;
        assert!(matches!(
            delegated_reader
                .read_private_path(
                    &volume,
                    "messages",
                    "messages/empty.txt",
                    Some(&listed_generation)
                )
                .await,
            Err(Error::Conflict(_))
        ));
        assert!(host.read_content(&empty, &read, 16).await?.is_empty());
        let wrong_digest = FileRef::new(
            volume.clone(),
            first.path(),
            first.version(),
            FileDescriptor::new([0; 32], 3, "text/plain")?,
            first.display_name(),
        )?;
        assert!(host.read_content(&wrong_digest, &read, 16).await.is_err());
        assert!(
            host.put_content(
                &volume,
                &write,
                "../escape",
                b"bad",
                "text/plain",
                "escape",
                16,
                &IdempotencyKey::new("invalid-path")?,
            )
            .await
            .is_err()
        );
        assert!(host.read_content(&first, &write, 16).await.is_err());
        let other = VolumeRef::new(
            volume.provider().clone(),
            "agent-private-b",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([5; 16])),
        )?;
        assert!(ContentGrant::verify(&verifier, &scope, &other, VolumeOperation::Read).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn conversation_stream_admission_requires_resident_files() -> Result<()> {
        use acyclic_harness::conversation::{Attachment, ConversationMessage, MessageKind};
        use acyclic_harness::core::{Action, Command, SchemaRegistry};
        use acyclic_harness::store::StreamAggregate;
        use acyclic_harness::{IdempotencyKey, OperationId};
        use acyclic_stream::{MemoryStream, StreamClient};
        use std::{collections::BTreeMap, sync::Arc};

        let filesystem = Fs::memory();
        let provider = ProviderRef::new("example", "filesystem", "2")?;
        let host = FilesystemHost::new(filesystem, provider.clone())?;
        let agent = AgentId::from_bytes([7; 16]);
        let volume = VolumeRef::new(
            provider,
            "scratch",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
        )?;
        host.create_volume(&volume).await?;
        let authority = Authority {
            kind: AggregateKind::Conversation,
            id: "integration".into(),
        };
        let issuer = AuthorityIssuer::new("integration", [11; 32], authority.clone());
        let scope = issuer.root_for_agent(
            agent,
            "agent",
            Capabilities::new([
                "conversation:bind".to_owned(),
                "conversation:append".to_owned(),
                volume.capability(VolumeOperation::Read)?,
                volume.capability(VolumeOperation::Write)?,
            ]),
        );
        let read =
            ContentGrant::verify(&issuer.verifier(), &scope, &volume, VolumeOperation::Read)?;
        let write =
            ContentGrant::verify(&issuer.verifier(), &scope, &volume, VolumeOperation::Write)?;
        let file = host
            .put_content(
                &volume,
                &write,
                "messages/input.txt",
                b"hello",
                "text/plain",
                "input.txt",
                32,
                &IdempotencyKey::new("input-upload")?,
            )
            .await?;
        let manifest = host
            .put_attachment_manifest(
                &volume,
                &write,
                "attachments/list.json",
                &[Attachment {
                    file: file.clone(),
                    label: Some("primary copy".into()),
                }],
                4_096,
                &IdempotencyKey::new("attachment-list")?,
            )
            .await?;
        let verifier = Arc::new(FilesystemContentVerifier::new(
            Arc::new(host),
            issuer.verifier(),
            scope.clone(),
            4_096,
        )?);
        let stream = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut aggregate =
            StreamAggregate::open(&stream, authority, issuer.verifier(), SchemaRegistry::new())
                .await?
                .with_content_verifier(verifier);
        aggregate
            .execute(Command {
                operation_id: OperationId::from_bytes([1; 16]),
                idempotency_key: IdempotencyKey::new("bind")?,
                expected_revision: 0,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::BindConversation { agent },
            })
            .await?;
        let message = ConversationMessage {
            id: uuid::Uuid::from_bytes([2; 16]),
            sequence: 1,
            kind: MessageKind::User,
            content: file.clone(),
            attachments: Vec::new().into(),
            reply_to: None,
            tool_call_id: None,
            extensions: BTreeMap::new(),
        };
        let mut missing = message.clone();
        missing.content = FileRef::new(
            volume,
            "messages/missing.txt",
            file.version(),
            file.descriptor().clone(),
            "missing.txt",
        )?;
        let command = |id: u8, value: ConversationMessage| -> Result<Command> {
            Ok(Command {
                operation_id: OperationId::from_bytes([id; 16]),
                idempotency_key: IdempotencyKey::new(format!("append-{id}"))?,
                expected_revision: 1,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::AppendConversationMessage {
                    message: Box::new(value),
                },
            })
        };
        assert!(aggregate.execute(command(3, missing)?).await.is_err());
        assert_eq!(aggregate.reducer().revision(), 1);
        let mut missing_attachment = message.clone();
        missing_attachment.attachments = vec![Attachment {
            file: FileRef::new(
                file.volume().clone(),
                "attachments/missing.bin",
                file.version(),
                file.descriptor().clone(),
                "missing.bin",
            )?,
            label: None,
        }]
        .into();
        assert!(
            aggregate
                .execute(command(5, missing_attachment)?)
                .await
                .is_err()
        );
        assert_eq!(aggregate.reducer().revision(), 1);
        aggregate.execute(command(4, message.clone())?).await?;
        assert_eq!(
            aggregate
                .reducer()
                .conversation()
                .map(|state| state.messages.as_slice()),
            Some(&[message][..])
        );
        let manifest_message = ConversationMessage {
            id: uuid::Uuid::from_bytes([6; 16]),
            sequence: 2,
            kind: MessageKind::User,
            content: file.clone(),
            attachments: manifest.clone(),
            reply_to: None,
            tool_call_id: None,
            extensions: BTreeMap::new(),
        };
        let mut bad_count = manifest_message.clone();
        if let ReferencedAttachments::Manifest { item_count, .. } = &mut bad_count.attachments {
            *item_count += 1;
        }
        assert!(
            aggregate
                .execute(Command {
                    operation_id: OperationId::from_bytes([7; 16]),
                    idempotency_key: IdempotencyKey::new("manifest-bad-count")?,
                    expected_revision: 2,
                    scope: scope.clone(),
                    causal_parent: None,
                    action: Action::AppendConversationMessage {
                        message: Box::new(bad_count)
                    },
                })
                .await
                .is_err()
        );
        assert_eq!(aggregate.reducer().revision(), 2);
        aggregate
            .execute(Command {
                operation_id: OperationId::from_bytes([8; 16]),
                idempotency_key: IdempotencyKey::new("manifest-valid")?,
                expected_revision: 2,
                scope,
                causal_parent: None,
                action: Action::AppendConversationMessage {
                    message: Box::new(manifest_message),
                },
            })
            .await?;
        assert_eq!(
            aggregate
                .reducer()
                .conversation()
                .map(|state| state.messages.len()),
            Some(2)
        );
        assert!(read.require(file.volume(), VolumeOperation::Read).is_ok());
        Ok(())
    }
}
