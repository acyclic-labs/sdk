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
        Attachment, BoundedContentFuture, ContentHold, ContentPublisher, ContentResidencyVerifier,
        FileRef, Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, SchemaRegistry, Scope},
    executor::{ExecutionJournal, TurnOutput},
    interaction::{InteractionOutcome, InteractionResponse},
    resources::{GenerationRef, ProviderRef},
    runtime::{ContentBindings, RuntimeScope},
    store::StreamAggregate,
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry,
        ToolResult,
    },
};
#[cfg(test)]
use crate::{
    conversation::{ConversationMessage, MessageKind},
    executor::TurnInput,
};
use acyclic_fs::{
    DirectoryPage, Fs, MemoryAuthorityBackend, MemoryObjectBackend,
    kernel::{FileKind, LogicalName, NameEncoding},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::MemoryStream;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

type MemoryHost = FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>;
type MemoryJournal =
    FilesystemExecutionJournal<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>;

fn new_memory_store(
    volume: &VolumeRef,
    maximum_file_bytes: u64,
) -> Result<Arc<std::sync::Mutex<crate::memory_store::MemoryStore>>> {
    Ok(Arc::new(std::sync::Mutex::new(
        crate::memory_store::MemoryStore::new(
            volume.clone(),
            maximum_file_bytes,
            crate::conversation::MAX_PATH_BYTES as u64,
            u64::MAX,
            u64::MAX,
        )?,
    )))
}

/// A fully bound in-process journal and owner-private file volume.
pub struct MemoryHarnessStorage {
    journal: Arc<MemoryJournal>,
    content_verifier: Arc<FilesystemContentVerifier<MemoryAuthorityBackend, MemoryObjectBackend>>,
    publisher: Arc<MemoryContentPublisher>,
    host: Arc<MemoryHost>,
    volume: VolumeRef,
    read_capability: String,
    write_capability: String,
    scope: Scope,
    issuer: AuthorityIssuer,
    stream: Arc<MemoryStream>,
    conversation: Authority,
    maximum_file_bytes: u64,
    memory_store: Arc<std::sync::Mutex<crate::memory_store::MemoryStore>>,
}

mod listing;
mod publisher;
use listing::LocalListFilesTool;
use publisher::MemoryContentPublisher;

impl MemoryHarnessStorage {
    /// Assembles portable defaults with an optional explicitly bound recursive provider.
    pub fn default_tools(
        &self,
        limits: Limits,
        source: Option<
            Arc<
                super::ConfiguredRecursiveToolSource<
                        MemoryStream,
                        MemoryAuthorityBackend,
                        MemoryObjectBackend,
                    >,
            >,
        >,
    ) -> Result<ToolRegistry> {
        limits.validate(&mut None)?;
        if limits.file_bytes > self.maximum_file_bytes {
            return Err(Error::Invalid(
                "tool file limit exceeds the bound storage volume".into(),
            ));
        }
        let mut tools = super::recursive_default_tools(source, limits.file_bytes)?.tools;
        tools.register((self.list_files_tool(limits), None), &mut None, None)?;
        Ok(tools)
    }

    fn list_files_tool(&self, limits: Limits) -> Tool {
        let implementation = Arc::new(LocalListFilesTool {
            verifier: self.content_verifier.clone(),
            volume: self.volume.clone(),
            maximum_bytes: limits.render_bytes,
            observed: tokio::sync::Mutex::new(BTreeMap::new()),
        });
        Tool {
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
        }
    }

    /// Creates an agent-owned private volume and a bound conversation before any turn or interaction can publish a reference.
    pub async fn new(agent: AgentId, maximum_file_bytes: u64) -> Result<Self> {
        if maximum_file_bytes == 0 {
            return Err(Error::Invalid(
                "memory Harness file limit must be positive".into(),
            ));
        }
        let provider =
            ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None)?;
        let host: Arc<MemoryHost> = FilesystemHost::new(Fs::memory(), &provider, None)?;
        let volume = VolumeRef::new(
            provider,
            Uuid::new_v4().to_string(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        let memory_store = new_memory_store(&volume, maximum_file_bytes)?;
        host.create_volume(&volume).await?;
        let conversation = Authority {
            kind: AggregateKind::Conversation,
            id: ConversationId::new().to_string(),
        };
        let key = *blake3::hash(&OperationId::new().into_bytes()).as_bytes();
        let issuer = AuthorityIssuer::new("local-harness", key, conversation.clone());
        let read_capability = volume.capability(VolumeOperation::Read, &mut None)?;
        let write_capability = volume.capability(VolumeOperation::Write, &mut None)?;
        let scope = issuer
            .issue(
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
                None,
                &mut None,
            )
            .expect("scope proof");
        let stream = Arc::new(MemoryStream::default());
        let mut aggregate = StreamAggregate::open(
            &stream,
            conversation.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
            None,
            crate::store::default_projection_read_limits(),
            crate::store::DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut None,
        )
        .await?;
        aggregate
            .execute(
                Command {
                    operation_id: OperationId::new(),
                    idempotency_key: IdempotencyKey::new("local-conversation-bind")?,
                    expected_revision: 0,
                    scope: scope.clone(),
                    causal_parent: None,
                    action: Action::BindConversation { agent },
                },
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
        let input = FilesystemContentVerifier::new(
            Arc::clone(&host),
            &issuer.verifier(&mut None).expect("verifier copy"),
            &scope,
            maximum_file_bytes,
            None,
        )?;
        let journal = Arc::new(
            FilesystemExecutionJournal::new(
                stream.clone(),
                Arc::clone(&host),
                &volume,
                &issuer.verifier(&mut None).expect("verifier copy"),
                &crate::core::SchemaRegistry::new(),
                &scope,
                maximum_file_bytes,
                None,
            )?
            .with_input_verifier(input.clone()),
        );
        let publisher = Arc::new(MemoryContentPublisher {
            files: FilesystemContentPublisher::new(
                Arc::clone(&host),
                &volume,
                &issuer.verifier(&mut None).expect("verifier copy"),
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

    /// Starts a runnable local composition with this owner-controlled journal.
    #[must_use]
    pub fn builder(&self) -> crate::bundle::HarnessBuilder {
        crate::bundle::HarnessBuilder::new()
            .journal(self.journal())
            .content(ContentBindings {
                reader: self.content_verifier.clone(),
                writer: Some(self.publisher.clone()),
            })
            .grant(self.read_capability.clone())
            .grant(self.write_capability.clone())
    }

    /// Runs the shared canonical loop with this memory-backed journal.
    pub async fn run_conversation(
        &self,
        bundle: &crate::bundle::HarnessBundle,
        operation_id: OperationId,
        content: FileRef,
        attachments: Vec<Attachment>,
        max_steps: u32,
    ) -> Result<TurnOutput> {
        self.journal
            .run_conversation(bundle, operation_id, content, attachments, max_steps)
            .await
    }

    /// Stages text and ordered attachments for the canonical turn loop.
    pub async fn run_prompt(
        &self,
        bundle: &crate::bundle::HarnessBundle,
        prompt: &str,
        attachments: Vec<Attachment>,
    ) -> Result<TurnOutput> {
        let operation_id = OperationId::new();
        let content = self
            .stage(
                operation_id,
                &format!("turns/{operation_id}/user.txt"),
                prompt.as_bytes(),
                "text/plain",
                "prompt.txt",
            )
            .await?;
        let max_steps = u32::try_from(bundle.limits().model_steps)
            .map_err(|_| Error::Invalid("model step limit exceeds u32".into()))?;
        self.run_conversation(bundle, operation_id, content, attachments, max_steps)
            .await
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
            .stage(operation_id, path, bytes, media_type, display_name, None)?
            .await?;
        Ok(file)
    }

    /// Reads a pinned owner-private version after verifying its descriptor.
    pub async fn read(&self, file: &FileRef) -> Result<acyclic_stream::OwnedBytes> {
        if let Ok(bytes) = self
            .memory_store
            .lock()
            .map_err(|_| Error::Storage("memory content core lock is poisoned".into()))?
            .read(file)
        {
            return Ok(bytes.into());
        }
        self.content_verifier
            .read(file, self.maximum_file_bytes, None)?
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
                expected_generation,
                after,
                maximum_entries,
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
            .read(file, self.maximum_file_bytes, None)?
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
                expected_generation,
                after,
                maximum_entries,
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
                None,
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

    fn delegated_reader(
        &self,
        scope: &Scope,
    ) -> Result<Arc<FilesystemContentVerifier<MemoryAuthorityBackend, MemoryObjectBackend>>> {
        FilesystemContentVerifier::new(
            self.host.clone(),
            &self.issuer.verifier(&mut None).expect("verifier copy"),
            &scope,
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
        let responder = self
            .issuer
            .issue(
                &format!("local-responder:{id}"),
                &Capabilities::new([
                    capability::INTERACTION_RESOLVE.to_owned(),
                    capability::interaction_respond(id),
                ]),
                None,
                None,
                &mut None,
            )
            .expect("scope proof");
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
    pub fn stream(&self) -> &Arc<MemoryStream> {
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
        self.issuer.verifier(&mut None).expect("verifier copy")
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "memory_summary_tests.rs"]
mod summary_tests;
