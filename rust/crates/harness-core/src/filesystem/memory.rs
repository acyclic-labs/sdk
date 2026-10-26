//! Infrastructure-free Harness storage with the same Stream/Filesystem contracts.

use crate::contract::capability;
use crate::filesystem::{
    FilesystemContentPublisher, FilesystemContentVerifier, FilesystemExecutionJournal,
    FilesystemHost,
};
use crate::{
    AgentId, Capabilities, ConversationId, Error, IdempotencyKey, InteractionId, OperationId,
    Result,
    conversation::{
        BoundedContentFuture, ContentHold, ContentPublisher, ContentResidencyVerifier, FileRef,
        Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{Action, AggregateKind, Authority, AuthorityRoot, Command, SchemaRegistry, Scope},
    executor::ExecutionJournal,
    interaction::{InteractionOutcome, InteractionResponse},
    resources::{GenerationRef, ProviderRef},
    runtime::RuntimeScope,
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
    maximum_file_bytes: u64,
    filesystem: Fs<A, O>,
) -> Result<(
    Arc<FilesystemHost<A, O>>,
    VolumeRef,
    Arc<std::sync::Mutex<crate::memory_store::MemoryStore>>,
)> {
    if maximum_file_bytes == 0 {
        return Err(Error::Invalid(
            "memory Harness file limit must be positive".into(),
        ));
    }
    let provider = ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(filesystem, &provider, None)?;
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
    let memory_store = Arc::new(std::sync::Mutex::new(
        crate::memory_store::MemoryStore::new(
            volume.clone(),
            maximum_file_bytes,
            crate::conversation::MAX_PATH_BYTES as u64,
            u64::MAX,
            u64::MAX,
        )?,
    ));
    host.create_volume(&volume).await?;
    #[cfg(not(target_arch = "wasm32"))]
    let host = {
        let reference = super::bounded_io::content_workspace(&host.provider, &volume, None)?;
        let workspace = host.open(&reference, None).await?;
        host.for_workspace(&volume, workspace)?
    };
    Ok((host, volume, memory_store))
}

async fn bind_memory_conversation<P: StreamProvider>(
    stream: &Arc<P>,
    conversation: &Authority,
    verifier: &crate::core::AuthorityVerifier,
    scope: &Scope,
    agent: AgentId,
) -> Result<()> {
    crate::store::StreamAggregate::open(
        stream,
        (
            conversation.clone(),
            verifier.clone(),
            SchemaRegistry::new(),
        ),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .execute(
        Command {
            operation_id: OperationId::new(),
            idempotency_key: IdempotencyKey::new("local-conversation-bind")?,
            expected_revision: 0,
            scope: scope.clone(),
            causal_parent: None,
            action: Action::BindConversation { agent },
        },
        &mut None,
    )
    .await?;
    Ok(())
}

/// A fully bound journal and owner-private file volume.
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
    content_verifier: Arc<FilesystemContentVerifier<A, O>>,
    publisher: Arc<MemoryContentPublisher<A, O>>,
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    read_capability: String,
    write_capability: String,
    scope: Scope,
    issuer: AuthorityRoot,
    stream: Arc<P>,
    conversation: Authority,
    maximum_file_bytes: u64,
    memory_store: Arc<std::sync::Mutex<crate::memory_store::MemoryStore>>,
}

mod listing;
mod publisher;
use listing::LocalListFilesTool;
use publisher::MemoryContentPublisher;

impl MemoryHarnessStorage {
    /// Creates an agent-owned private volume and a bound conversation before any turn or interaction can publish a reference.
    pub async fn new(agent: AgentId, maximum_file_bytes: u64) -> Result<Self> {
        Self::with_stream(agent, maximum_file_bytes, Arc::new(MemoryStream::default())).await
    }
}

impl<P: StreamProvider> MemoryHarnessStorage<P> {
    /// Binds the journal and conversation to `stream`; files stay in process.
    pub async fn with_stream(
        agent: AgentId,
        maximum_file_bytes: u64,
        stream: Arc<P>,
    ) -> Result<Self> {
        Self::from_providers(agent, maximum_file_bytes, Fs::memory(), stream).await
    }
}

impl<P: StreamProvider, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    MemoryHarnessStorage<P, A, O>
{
    /// Builds an optional directory listing tool for explicit custom registration.
    pub fn list_files_tool(&self, limits: Limits) -> Result<Tool> {
        limits.validate(&mut None)?;
        if limits.file_bytes > self.maximum_file_bytes {
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
                revision: "2".into(),
                description: "List one bounded, generation-pinned page of the agent-private volume"
                    .into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "expected_generation": {"type": ["object", "null"]},
                        "after": {"type": ["string", "null"]},
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
                        "has_more": {"type": "boolean"},
                        "next_after": {"type": ["string", "null"]}
                    },
                    "required": ["generation", "entries", "has_more", "next_after"],
                    "additionalProperties": false
                }),
                projection_schema: crate::tool::json_projection_schema(json!({
                    "type": "object",
                    "properties": {
                        "generation": {"type": "object"},
                        "entries": {"type": "array", "items": {"type": "object"}},
                        "has_more": {"type": "boolean"},
                        "next_after": {"type": ["string", "null"]}
                    },
                    "required": ["generation", "entries", "has_more", "next_after"],
                    "additionalProperties": false
                })),
            },
            executor: implementation.clone(),
            projection: implementation,
        })
    }

    /// Binds the canonical journal and files to caller-selected service providers.
    pub async fn from_providers(
        agent: AgentId,
        maximum_file_bytes: u64,
        filesystem: Fs<A, O>,
        stream: Arc<P>,
    ) -> Result<Self> {
        let (host, volume, memory_store) =
            new_memory_content(agent, maximum_file_bytes, filesystem).await?;
        let conversation = Authority {
            kind: AggregateKind::Conversation,
            id: ConversationId::new().to_string(),
        };
        let key = *blake3::hash(&OperationId::new().into_bytes()).as_bytes();
        let issuer = AuthorityRoot::new("local-harness", key, conversation.clone());
        let read_capability = volume.capability(VolumeOperation::Read, &mut None)?;
        let write_capability = volume.capability(VolumeOperation::Write, &mut None)?;
        let scope = issuer.bootstrap_scope(
            "owner",
            &Capabilities::new([
                capability::CONVERSATION_BIND.to_owned(),
                capability::CONVERSATION_APPEND.to_owned(),
                capability::CONVERSATION_SELECT_CONTEXT.to_owned(),
                capability::INTERACTION_OPEN.to_owned(),
                read_capability.clone(),
                write_capability.clone(),
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
        let journal = Arc::new(
            FilesystemExecutionJournal::new(
                stream.clone(),
                Arc::clone(&host),
                &SchemaRegistry::new(),
                crate::filesystem::FilesystemJournalBinding {
                    volume: &volume,
                    verifier: &verifier,
                    scope: &scope,
                    maximum_bytes: maximum_file_bytes,
                    io: None,
                },
            )?
            .with_input_verifier(input.clone()),
        );
        bind_memory_conversation(&stream, &conversation, &verifier, &scope, agent).await?;
        let publisher = Arc::new(MemoryContentPublisher {
            files: FilesystemContentPublisher::new(
                Arc::clone(&host),
                &volume,
                &verifier,
                &scope,
                maximum_file_bytes,
                None,
            )?,
            memory_store: memory_store.clone(),
        });
        Ok(Self {
            journal,
            content_verifier: input,
            publisher,
            host,
            volume,
            read_capability,
            write_capability,
            scope,
            issuer,
            stream,
            conversation,
            maximum_file_bytes,
            memory_store,
        })
    }

    /// Supplies the exact ref-only journal required by `HarnessBuilder`.
    #[must_use]
    pub fn journal(&self) -> Arc<dyn ExecutionJournal> {
        self.journal.clone()
    }

    /// Globally distinct identity of this local agent-private volume.
    #[must_use]
    pub const fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    /// Borrows the authenticated reader for context and client adapters.
    #[must_use]
    pub fn content_reader(&self) -> &dyn ContentResidencyVerifier {
        self.content_verifier.as_ref()
    }

    /// Shares the authenticated reader with a store's admission checks.
    #[cfg(test)]
    pub(crate) fn content_verifier(&self) -> Arc<dyn ContentResidencyVerifier> {
        self.content_verifier.clone()
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
            .read(file, self.maximum_file_bytes, None, None)?
            .await
    }

    /// Lazily discovers this agent's private files under its existing signed volume-read grant.
    pub async fn list_private_directory(
        &self,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        after: Option<&LogicalName>,
        maximum_entries: u32,
    ) -> Result<(GenerationRef, DirectoryPage)> {
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
            .await
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
            .read(file, self.maximum_file_bytes, None, None)?
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
        after: Option<&LogicalName>,
        maximum_entries: u32,
    ) -> Result<(GenerationRef, DirectoryPage)> {
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
            .await
    }

    /// Reads a named file from a delegated private directory and pins its observed version for later exact-reference use.
    pub async fn read_delegated_path(
        &self,
        scope: &Scope,
        granted_prefix: &str,
        path: &str,
        expected_generation: Option<&GenerationRef>,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        let resolved = self
            .delegated_reader(scope)?
            .read_private_path(
                &self.volume,
                granted_prefix,
                path,
                expected_generation,
                u64::MAX,
                crate::conversation::ContentAccess::default(),
            )?
            .await
            .map(|(file, bytes, _)| (file, bytes))?;
        let core = self
            .memory_store
            .lock()
            .map_err(|_| Error::Storage("memory content core lock is poisoned".into()))?;
        let shared = core.read_path(path, None);
        match shared {
            Ok(reference) if reference == resolved.0 => {
                let bytes = core.read(&reference)?;
                Ok((reference, bytes.into()))
            }
            _ => Ok(resolved),
        }
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

    /// Resolves an addressable local interaction under a host-issued exact responder grant.
    pub async fn resolve_interaction(
        &self,
        id: InteractionId,
        response: InteractionResponse,
    ) -> Result<InteractionOutcome> {
        let responder = self.issuer.bootstrap_scope(
            &format!("local-responder:{id}"),
            &Capabilities::new([
                capability::INTERACTION_RESOLVE.to_owned(),
                capability::interaction_respond(id),
            ]),
            None,
            &mut None,
        )?;
        self.journal
            .resolve_interaction(id, response, &responder)
            .await?;
        self.journal
            .interaction_outcome(id, None)?
            .await?
            .ok_or_else(|| Error::Storage("resolved interaction has no committed outcome".into()))
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
