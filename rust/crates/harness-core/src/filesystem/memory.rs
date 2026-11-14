//! Infrastructure-free Harness storage with the same Stream/Filesystem contracts.

use crate::contract::capability;
use crate::filesystem::{
    FilesystemContentPublisher, FilesystemContentVerifier, FilesystemExecutionJournal,
    FilesystemHost,
    agent_internal::{AgentInternalFiles, AgentTurnOutputs},
};
use crate::{
    AgentId, Capabilities, ConversationId, Error, InteractionId, OperationId, Result,
    approvals::{
        AgentApprovalPolicy, AgentApprovals, ApprovalFiles, ApprovalPolicy, ApprovalResponder,
    },
    conversation::{
        ContentPublisher, ContentResidencyVerifier, FileRef, Limits, VolumeClass, VolumeOperation,
        VolumeOwner, VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityRoot, Scope},
    executor::ExecutionJournal,
    interaction::InteractionOutcome,
    resources::{GenerationRef, ProviderRef},
    runtime::{ContentBindings, RuntimeScope},
    tool::{Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolResult},
};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, DirectoryPage, Fs, MemoryAuthorityBackend,
    MemoryObjectBackend,
    kernel::{FileKind, LogicalName, NameEncoding},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::{MemoryStream, StreamProvider};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};

#[cfg(test)]
thread_local! {
    static MEMORY_VOLUME_KEY: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
}

async fn new_memory_content<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    agent: AgentId,
    filesystem: Fs<A, O>,
    changes: Arc<dyn StreamProvider>,
) -> Result<(Arc<FilesystemHost<A, O>>, VolumeRef)> {
    let provider = ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None)?;
    let shared_owner = filesystem.operation_claims().owner_identity()
        == changes.context().operation_claims().owner_identity();
    let host = FilesystemHost::new(filesystem, &provider, None)?;
    let host = if shared_owner {
        host.with_changes(changes)?
    } else {
        host
    };
    let random_key = || crate::identity::fresh_uuid().to_string();
    #[cfg(test)]
    let key = MEMORY_VOLUME_KEY
        .get()
        .map_or_else(random_key, str::to_owned);
    #[cfg(not(test))]
    let key = random_key();
    let volume = VolumeRef::new(
        provider,
        key,
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    #[cfg(not(target_arch = "wasm32"))]
    let host = {
        let reference = super::bounded_io::content_workspace(&host.provider, &volume, None)?;
        let workspace = host.open(&reference, None).await?;
        host.for_workspace(&volume, workspace)?
    };
    Ok((host, volume))
}

/// The one harness approval owner over `files` under `policy`, for an agent
/// with no parent.
fn agent_approvals(
    volume: &VolumeRef,
    files: Arc<dyn ApprovalFiles>,
    policy: ApprovalPolicy,
    maximum_file_bytes: Option<u64>,
) -> Result<Arc<AgentApprovals>> {
    let limits = Limits {
        file_bytes: maximum_file_bytes,
        ..Limits::default()
    };
    let VolumeOwner::Agent(owner) = volume.owner() else {
        return Err(Error::Invalid("approval storage has no agent owner".into()));
    };
    let policy = AgentApprovalPolicy::new(
        crate::agent::AgentId::from_bytes(owner.into_bytes()),
        policy,
        None,
        Default::default(),
        limits,
    )?;
    Ok(Arc::new(AgentApprovals::new(files, Arc::new(policy))))
}

/// A harness-owned journal and an independent owner-private file volume.
///
/// The journal runs on any [`StreamProvider`]: [`MemoryStream`] by default, or a
/// remote Streams service through [`MemoryHarnessStorage::with_stream`]. Both
/// filesystem backends are selected through [`MemoryHarnessStorage::from_providers`].
pub struct MemoryHarnessStorage<
    P = MemoryStream,
    A: 'static = MemoryAuthorityBackend,
    O: 'static = MemoryObjectBackend,
> {
    journal: Arc<FilesystemExecutionJournal<P, A, O>>,
    internal_files: Arc<AgentInternalFiles<A, O>>,
    outputs: AgentTurnOutputs<A, O>,
    approvals: Arc<AgentApprovals>,
    content_verifier: Arc<FilesystemContentVerifier<A, O>>,
    publisher: Arc<FilesystemContentPublisher<A, O>>,
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    #[cfg(test)]
    read_capability: String,
    #[cfg(test)]
    write_capability: String,
    scope: Scope,
    issuer: AuthorityRoot,
    stream: Arc<P>,
    conversation: Authority,
    maximum_file_bytes: Option<u64>,
}

mod listing;
use listing::LocalListFilesTool;

impl MemoryHarnessStorage {
    /// Creates an agent-owned private volume and a bound conversation before any turn or interaction can publish a reference.
    pub async fn new(agent: AgentId, maximum_file_bytes: impl Into<Option<u64>>) -> Result<Self> {
        Self::with_stream(
            agent,
            maximum_file_bytes.into(),
            Arc::new(MemoryStream::default()),
        )
        .await
    }
}

impl<P: crate::store::JournalProvider> MemoryHarnessStorage<P> {
    /// Binds the journal and conversation to `stream`; files stay in process.
    pub async fn with_stream(
        agent: AgentId,
        maximum_file_bytes: impl Into<Option<u64>>,
        stream: Arc<P>,
    ) -> Result<Self> {
        Self::from_providers(agent, maximum_file_bytes.into(), Fs::memory(), stream).await
    }
}

impl<
    P: crate::store::JournalProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
> MemoryHarnessStorage<P, A, O>
{
    /// Builds an optional directory listing tool for explicit custom registration.
    pub fn list_files_tool(&self, limits: Limits) -> Result<Tool> {
        limits.validate(&mut None)?;
        if self
            .maximum_file_bytes
            .is_some_and(|maximum| limits.file_bytes.is_none_or(|limit| limit > maximum))
        {
            return Err(Error::Invalid(
                "tool file limit exceeds the bound storage volume".into(),
            ));
        }
        let implementation = Arc::new(LocalListFilesTool {
            verifier: self.content_verifier.clone(),
            volume: self.volume.clone(),
            maximum_bytes: limits.render_bytes,
            observed: tokio::sync::Mutex::new(BTreeMap::new()),
        });
        Ok(Tool {
            hook_names: &[],
            definition: ToolDefinition {
                name: "acyclic.list_files".into(),
                revision: "3".into(),
                description: "List one bounded, generation-pinned page of the agent-private volume"
                    .into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "expected_generation": {"type": ["object", "null"]},
                        "after": {"type": ["array", "null"], "items": {"type":"integer", "minimum":0, "maximum":255}},
                        "maximum_entries": {"type": "integer", "minimum": 1, "maximum": u32::MAX}
                    },
                    "required": ["path", "maximum_entries"],
                    "additionalProperties": false
                }),
                output_schema: json!({
                    "type": "object",
                    "properties": {
                        "generation": {"type": "object"},
                        "entries": {"type": "array", "items": {"type": "object"}},
                        "next": {"type": ["array", "null"], "items":{"type":"integer","minimum":0,"maximum":255}}
                    },
                    "required": ["generation", "entries", "next"],
                    "additionalProperties": false
                }),
                projection_schema: crate::tool::json_projection_schema(json!({
                    "type": "object",
                    "properties": {
                        "generation": {"type": "object"},
                        "entries": {"type": "array", "items": {"type": "object"}},
                        "next": {"type": ["array", "null"], "items":{"type":"integer","minimum":0,"maximum":255}}
                    },
                    "required": ["generation", "entries", "next"],
                    "additionalProperties": false
                })),
            },
            executor: implementation.clone(),
            projection: implementation,
        })
    }

    /// Binds the canonical journal and files to caller-selected service providers.
    #[inline(never)]
    pub fn from_providers<'a>(
        agent: AgentId,
        maximum_file_bytes: impl Into<Option<u64>>,
        filesystem: Fs<A, O>,
        stream: Arc<P>,
    ) -> BoxFuture<'a, Result<Self>>
    where
        P: 'a,
    {
        // One cold owner keeps filesystem initialization out of every caller's
        // future layout. Established operations need no such allocation.
        let maximum_file_bytes = maximum_file_bytes.into();
        Box::pin(async move {
            Limits {
                file_bytes: maximum_file_bytes,
                ..Limits::default()
            }
            .validate(&mut None)?;
            let (host, volume) = new_memory_content(agent, filesystem, stream.clone()).await?;
            let conversation = Authority {
                kind: AggregateKind::Conversation,
                id: ConversationId::new().to_string(),
            };
            let key = *blake3::hash(&OperationId::new().into_bytes()).as_bytes();
            let issuer = AuthorityRoot::new("local-harness", key, conversation.clone());
            let read_capability = volume.capability(VolumeOperation::Read, &mut None)?;
            let write_capability = volume.capability(VolumeOperation::Write, &mut None)?;
            let control_volume = crate::conversation::journal_volume(
                host.provider.clone(),
                &crate::agent::AgentId::from_bytes(agent.into_bytes()),
            )?;
            host.create_volume(&control_volume).await?;
            let scope = issuer.bootstrap_scope(
                "owner",
                &Capabilities::new([
                    capability::CONVERSATION_BIND.to_owned(),
                    capability::CONVERSATION_APPEND.to_owned(),
                    capability::CONVERSATION_SELECT_CONTEXT.to_owned(),
                    capability::INTERACTION_OPEN.to_owned(),
                    read_capability.clone(),
                    write_capability.clone(),
                    control_volume.capability(VolumeOperation::Read, &mut None)?,
                ]),
                Some(agent),
                &mut None,
            )?;
            let verifier = issuer.verifier(&mut None)?;
            let input = FilesystemContentVerifier::new(
                Arc::clone(&host),
                &verifier,
                &scope,
                maximum_file_bytes,
                None,
            )?;
            let control_scope = issuer.bootstrap_scope(
                "host-journal",
                &super::agent_turns::journal_grants(&control_volume, &Capabilities::default())?,
                Some(agent),
                &mut None,
            )?;
            let output_grant = crate::conversation::ContentGrant::verify(
                &verifier,
                &control_scope,
                &control_volume,
                VolumeOperation::Write,
                &mut None,
            )?;
            let internal_files = Arc::new(AgentInternalFiles::new(
                Arc::clone(&host),
                control_volume.clone(),
                output_grant.clone(),
                volume.clone(),
                crate::conversation::ContentGrant::verify(
                    &verifier,
                    &scope,
                    &volume,
                    VolumeOperation::Write,
                    &mut None,
                )?,
            ));
            let outputs = AgentTurnOutputs {
                ui: internal_files.clone(),
                journal: Arc::new(AgentInternalFiles::new(
                    host.clone(),
                    control_volume.clone(),
                    output_grant.clone(),
                    control_volume.clone(),
                    output_grant,
                )),
            };
            let approvals = agent_approvals(
                &volume,
                internal_files.clone(),
                ApprovalPolicy::default(),
                maximum_file_bytes,
            )?;
            let journal = Arc::new(
                FilesystemExecutionJournal::new(
                    stream.clone(),
                    Arc::clone(&host),
                    crate::filesystem::FilesystemJournalBinding {
                        volume: &control_volume,
                        verifier: &verifier,
                        scope: &control_scope,
                        io: None,
                    },
                )?
                .with_input_verifier(input.clone())
                .with_agent_approvals(Arc::clone(&approvals)),
            );
            let publisher = FilesystemContentPublisher::new(
                Arc::clone(&host),
                &volume,
                &verifier,
                &scope,
                maximum_file_bytes,
                None,
            )?;
            Ok(Self {
                journal,
                internal_files,
                outputs,
                approvals,
                content_verifier: input,
                publisher,
                host,
                volume,
                #[cfg(test)]
                read_capability,
                #[cfg(test)]
                write_capability,
                scope,
                issuer,
                stream,
                conversation,
                maximum_file_bytes,
            })
        })
    }

    /// Runs every approval under `policy`, normally the caller's preset
    /// approval policy (`Preset::approval_policy`). Without it the storage
    /// uses the harness default policy chain. Binds before the journal is
    /// shared, so every approval of this storage sees one policy.
    pub fn with_approval_policy(mut self, policy: ApprovalPolicy) -> Result<Self> {
        let approvals = agent_approvals(
            &self.volume,
            self.internal_files.clone(),
            policy,
            self.maximum_file_bytes,
        )?;
        let journal = Arc::try_unwrap(self.journal).map_err(|_| {
            Error::Conflict("approval policy must be bound before the journal is shared".into())
        })?;
        self.journal = Arc::new(journal.with_agent_approvals(Arc::clone(&approvals)));
        self.approvals = approvals;
        Ok(self)
    }

    /// Records `responder`'s decision on approval `id` through the bound
    /// approval owner, then returns its settled outcome: `None` while a later
    /// stage of the policy chain still waits on its decider.
    pub async fn decide_approval(
        &self,
        id: InteractionId,
        responder: ApprovalResponder<'_>,
        verdict: crate::interaction::ApprovalVerdict,
        reason: Option<String>,
    ) -> Result<Option<InteractionOutcome>> {
        self.approvals
            .decide(id, responder, verdict, reason)
            .await?;
        self.approvals
            .outcome(id, self.journal.as_ref(), None)
            .await
    }

    /// Supplies the exact ref-only journal required by `HarnessBuilder`.
    #[must_use]
    pub fn journal(&self) -> Arc<dyn ExecutionJournal> {
        self.journal.clone()
    }

    /// Keeps authored UI separate from journal-owned settled and streamed outputs.
    #[must_use]
    pub fn turn_outputs(&self) -> &dyn crate::agent::TurnOutputs {
        &self.outputs
    }

    /// Globally distinct identity of this local agent-private volume.
    #[must_use]
    pub const fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    /// Composes the authenticated private volume with a caller-selected runtime.
    #[must_use]
    pub fn content_bindings(&self) -> ContentBindings {
        ContentBindings {
            volume: self.volume.clone(),
            reader: self.content_verifier.clone(),
            writer: Some(self.publisher.clone()),
        }
    }

    /// Borrows the authenticated reader for context and client adapters.
    #[must_use]
    pub fn content_reader(&self) -> &dyn ContentResidencyVerifier {
        self.content_verifier.as_ref()
    }

    /// Stages user content or an attachment before it is admitted to a turn.
    pub async fn stage(
        &self,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
    ) -> Result<FileRef> {
        let (file, _hold) = self
            .publisher
            .stage(
                operation_id,
                path,
                bytes,
                media_type,
                display_name,
                crate::conversation::ContentAccess::default(),
            )?
            .await?;
        Ok(file)
    }

    /// Reads a pinned owner-private version after verifying its descriptor.
    pub async fn read(&self, file: &FileRef) -> Result<acyclic_stream::OwnedBytes> {
        self.content_verifier
            .read(file, file.descriptor().byte_length(), None, None)?
            .await
    }

    /// Lazily discovers this agent's private files under its existing signed volume-read grant.
    pub async fn list_private_directory(
        &self,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        after: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<crate::conversation::PrivateDirectoryPage> {
        self.content_verifier
            .list_private_directory(
                &self.volume,
                "",
                path,
                crate::filesystem::DirectoryOptions {
                    generation: expected_generation,
                    after,
                    maximum_entries,
                    ceiling: None,
                },
            )
            .await?
            .ok_or_else(|| Error::NotFound("selected directory does not exist".into()))
    }

    /// Publishes one owner-mediated exact-file read scope to another agent, independent of any fork relationship.
    pub async fn delegate_file_read(
        &self,
        reader: AgentId,
        id: impl Into<String>,
        file: &FileRef,
    ) -> Result<Scope> {
        self.read(file).await?;
        self.issuer
            .delegate_private_file_read(&self.scope, reader, id, file)
    }

    /// Resolves a pinned reference under an owner-issued signed read scope.
    pub async fn read_delegated(
        &self,
        scope: &Scope,
        file: &FileRef,
    ) -> Result<acyclic_stream::OwnedBytes> {
        self.delegated_reader(scope)?
            .read(file, file.descriptor().byte_length(), None, None)?
            .await
    }

    /// Shares a private subtree with a named agent.
    pub fn delegate_directory_read(
        &self,
        reader: AgentId,
        id: impl Into<String>,
        prefix: &str,
    ) -> Result<Scope> {
        self.issuer
            .delegate_private_directory_read(&self.scope, reader, id, &self.volume, prefix)
    }

    /// Lists a lazily opened owner-private directory under a signed grant.
    pub async fn list_delegated(
        &self,
        scope: &Scope,
        granted_prefix: &str,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        after: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<crate::conversation::PrivateDirectoryPage> {
        self.delegated_reader(scope)?
            .list_private_directory(
                &self.volume,
                granted_prefix,
                path,
                crate::filesystem::DirectoryOptions {
                    generation: expected_generation,
                    after,
                    maximum_entries,
                    ceiling: None,
                },
            )
            .await?
            .ok_or_else(|| Error::NotFound("selected directory does not exist".into()))
    }

    /// Reads a named file from a delegated private directory and pins its observed version for later exact-reference use.
    pub async fn read_delegated_path(
        &self,
        scope: &Scope,
        granted_prefix: &str,
        path: &str,
        expected_generation: Option<&GenerationRef>,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        self.delegated_reader(scope)?
            .read_private_path(
                &self.volume,
                granted_prefix,
                path,
                expected_generation,
                None,
                crate::conversation::ContentAccess::default(),
            )?
            .await
            .and_then(|selected| {
                selected
                    .map(|(file, bytes, _)| (file, bytes))
                    .ok_or_else(|| {
                        Error::NotFound(crate::conversation::SELECTED_FILE_MISSING.into())
                    })
            })
    }

    fn delegated_reader(&self, scope: &Scope) -> Result<Arc<FilesystemContentVerifier<A, O>>> {
        FilesystemContentVerifier::new(
            self.host.clone(),
            self.content_verifier.authority.verifier(),
            scope,
            self.maximum_file_bytes,
            None,
        )
    }

    /// Opens the bound conversation's exact Stream history to host adapters.
    #[must_use]
    pub fn stream(&self) -> &Arc<P> {
        &self.stream
    }

    /// Returns the bound conversation aggregate identity.
    #[must_use]
    pub const fn conversation(&self) -> &Authority {
        &self.conversation
    }

    /// Returns the signed owner scope for composing additional local adapters.
    #[must_use]
    pub const fn owner_scope(&self) -> &Scope {
        &self.scope
    }

    /// Returns the trusted verifier without disclosing the signing key.
    #[must_use]
    pub fn verifier(&self) -> crate::core::AuthorityVerifier {
        self.content_verifier.authority.verifier().clone()
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[path = "memory/workspace_binding_properties.rs"]
mod workspace_binding_properties;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[path = "memory/binding_differential.rs"]
mod binding_differential;
