//! Durable local Harness composition using public Stream and Filesystem providers.
use super::{FilesystemForkVerifier, FilesystemHost, HarnessStorage};
use crate::{
    AgentId, ConversationId, Error, OperationId, Result,
    conversation::{Limits, VolumeClass, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    executor::TurnOutput,
    fork::{CompositeForkVerifier, ForkSeed, ForkSeedVerifier, StreamHistoryForkVerifier},
    model::{Model, ModelProvider},
    tool::ToolRegistry,
    resources::ProviderRef,
    store::StreamAggregate,
};
use acyclic_fs::{LocalAuthorityBackend, LocalFs, LocalObjectBackend, LocalOptions};
use acyclic_stream::{AppendOutcome, LocalStream, LocalStreamLimits, StreamClient, StreamError};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};

/// Persistent providers own durability; storage semantics are shared with memory.
pub type DurableHarnessStorage =
    HarnessStorage<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionDescriptor {
    version: u32,
    agent: AgentId,
    conversation: Authority,
    private_volume: VolumeRef,
    signing_key: [u8; 32],
    model: Model,
    limits: Limits,
}

impl SessionDescriptor {
    fn fresh(model: Model, limits: Limits) -> Result<Self> {
        let agent = AgentId::new();
        let descriptor = Self {
            version: 1,
            agent,
            conversation: Authority {
                kind: AggregateKind::Conversation,
                id: ConversationId::new().to_string(),
            },
            private_volume: VolumeRef::new(
                ProviderRef::new("local", "filesystem", "2")?,
                OperationId::new().to_string(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(agent),
            )?,
            signing_key: *blake3::hash(&OperationId::new().into_bytes()).as_bytes(),
            model,
            limits,
        };
        Ok(descriptor)
    }
}

fn validate_descriptor(
    descriptor: &SessionDescriptor,
    model: &Model,
    limits: Limits,
) -> Result<()> {
    if descriptor.version != 1
        || &descriptor.model != model
        || crate::contract::canonical_json_digest(&descriptor.limits)?
            != crate::contract::canonical_json_digest(&limits)?
    {
        return Err(Error::Conflict(
            "local session composition differs from its pinned descriptor".into(),
        ));
    }
    Ok(())
}

/// Ready-to-run, reopenable local agent with pinned composition.
pub struct PersistentLocalHarness {
    storage: DurableHarnessStorage,
    bundle: crate::Harness,
}

/// Optional owner supplied tools shared by every session in one local swarm.
///
/// The registry is copied into each immutable Harness composition. Executors
/// retain their authenticated host and wait sources; model input only sees
/// the version pinned definitions selected by the registry.
#[derive(Clone, Default)]
pub struct LocalHarnessTools {
    tools: ToolRegistry,
}

impl LocalHarnessTools {
    /// Creates an empty extension set.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            tools: ToolRegistry::new(),
        }
    }

    /// Retains one explicitly assembled tool registry.
    #[must_use]
    pub fn from_registry(tools: ToolRegistry) -> Self {
        Self { tools }
    }

    pub(crate) fn install_into(
        &self,
        mut builder: crate::bundle::HarnessBuilder,
    ) -> Result<crate::bundle::HarnessBuilder> {
        for definition in self.tools.definitions()? {
            let tool = self
                .tools
                .get_version(&definition.name, &definition.revision)
                .cloned()
                .ok_or_else(|| Error::Storage("local tool registry lost selected revision".into()))?;
            builder = builder.tool(tool)?;
            builder = builder.grant(format!("tool:call:{}", definition.name));
        }
        Ok(builder)
    }
}

impl PersistentLocalHarness {
    /// Composes a durable harness from provider and identity descriptors that
    /// the application has already persisted.
    pub async fn from_providers(
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
        host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
        stream: StreamClient<LocalStream>,
        agent: AgentId,
        volume: VolumeRef,
        conversation: Authority,
        issuer: AuthorityIssuer,
    ) -> Result<Self> {
        Self::from_providers_with_tools(
            model,
            provider,
            limits,
            host,
            stream,
            agent,
            volume,
            conversation,
            issuer,
            LocalHarnessTools::new(),
        )
        .await
    }

    /// Composes a durable local agent with owner supplied model tools.
    pub async fn from_providers_with_tools(
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
        host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
        stream: StreamClient<LocalStream>,
        agent: AgentId,
        volume: VolumeRef,
        conversation: Authority,
        issuer: AuthorityIssuer,
        extension: LocalHarnessTools,
    ) -> Result<Self> {
        limits.validate()?;
        let storage = DurableHarnessStorage::from_providers(
            agent,
            limits.file_bytes,
            host.clone(),
            stream,
            volume,
            conversation,
            issuer,
        )
        .await?
        .with_fork_verifier(local_fork_verifier(host.clone(), limits.file_bytes)?);
        let tools = storage.default_tools(limits)?;
        let builder = storage
            .builder()
            .model(model, provider)
            .tools(tools)
            .grant("model:generate")
            .grant("tool:call:acyclic.read_file")
            .grant("tool:call:acyclic.stage_file")
            .grant("tool:call:acyclic.list_files")
            .limits(limits);
        let bundle = extension.install_into(builder)?.build()?;
        Ok(Self { storage, bundle })
    }

    /// Composes a child harness from an already published typed fork. The
    /// caller owns the shared local providers and must retain the parent
    /// aggregate used by `spawn_from_report`; this constructor only binds the
    /// exact child seed and its immutable reference grants.
    pub async fn from_published_fork(
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
        host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
        stream: StreamClient<LocalStream>,
        issuer: AuthorityIssuer,
        parent: &StreamAggregate<LocalStream>,
        seed: &ForkSeed,
    ) -> Result<Self> {
        Self::from_published_fork_with_tools(
            model,
            provider,
            limits,
            host,
            stream,
            issuer,
            parent,
            seed,
            LocalHarnessTools::new(),
        )
        .await
    }

    /// Composes a child from a published fork while retaining owner supplied
    /// model tools from the parent swarm.
    pub async fn from_published_fork_with_tools(
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
        host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
        stream: StreamClient<LocalStream>,
        issuer: AuthorityIssuer,
        parent: &StreamAggregate<LocalStream>,
        seed: &ForkSeed,
        extension: LocalHarnessTools,
    ) -> Result<Self> {
        limits.validate()?;
        let storage = DurableHarnessStorage::from_published_fork(
            limits.file_bytes,
            host.clone(),
            stream,
            issuer,
            parent,
            seed,
        )
        .await?
        .with_fork_verifier(local_fork_verifier(host.clone(), limits.file_bytes)?);
        let tools = storage.default_tools(limits)?;
        let builder = storage
            .builder()
            .model(model, provider)
            .tools(tools)
            .grant("model:generate")
            .grant("tool:call:acyclic.read_file")
            .grant("tool:call:acyclic.stage_file")
            .grant("tool:call:acyclic.list_files")
            .limits(limits);
        let bundle = extension.install_into(builder)?.build()?;
        Ok(Self { storage, bundle })
    }

    /// Opens an exclusive session root, preserving stable identities and configuration.
    pub async fn open(
        root: impl AsRef<Path>,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
    ) -> Result<Self> {
        Self::open_with_tools(root, model, provider, limits, LocalHarnessTools::new()).await
    }

    /// Reopens a local session with the same owner supplied tool registry.
    pub async fn open_with_tools(
        root: impl AsRef<Path>,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
        extension: LocalHarnessTools,
    ) -> Result<Self> {
        limits.validate()?;
        let root = root.as_ref();
        let stream = StreamClient::new(Arc::new(
            LocalStream::open(root.join("history"), LocalStreamLimits::default())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        ));
        let metadata = stream
            .stream("harness/session")
            .map_err(|error| Error::Storage(error.to_string()))?;
        let missing = match metadata.tail().await {
            Ok(1) => false,
            Ok(0) | Err(StreamError::NotFound) => true,
            Ok(_) => {
                return Err(Error::Storage(
                    "invalid local session descriptor tail".into(),
                ));
            }
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        if missing {
            let descriptor = SessionDescriptor::fresh(model.clone(), limits)?;
            match metadata
                .append_at(crate::contract::canonical_json_bytes(&descriptor)?, 0)
                .await
                .map_err(|error| Error::Storage(error.to_string()))?
            {
                AppendOutcome::Committed(_) => {}
                AppendOutcome::TailConflict { .. } => {
                    return Err(Error::Conflict(
                        "local initialization lost ownership".into(),
                    ));
                }
            }
        }
        let mut records = metadata
            .read(0, 1)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let record = records
            .next()
            .await
            .ok_or_else(|| Error::Storage("local descriptor is missing".into()))?
            .map_err(|error| Error::Storage(error.to_string()))?;
        let descriptor: SessionDescriptor = serde_json::from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        validate_descriptor(&descriptor, &model, limits)?;
        let fs = LocalFs::local(LocalOptions::new(root.join("filesystem")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let host = Arc::new(FilesystemHost::new(
            fs,
            descriptor.private_volume.provider().clone(),
        )?);
        host.create_volume(&descriptor.private_volume).await?;
        let issuer = AuthorityIssuer::new(
            "local-harness",
            descriptor.signing_key,
            descriptor.conversation.clone(),
        );
        let storage = DurableHarnessStorage::from_providers(
            descriptor.agent,
            limits.file_bytes,
            host.clone(),
            stream,
            descriptor.private_volume,
            descriptor.conversation,
            issuer,
        )
        .await?
        .with_fork_verifier(local_fork_verifier(host, limits.file_bytes)?);
        let tools = storage.default_tools(limits)?;
        let builder = storage
            .builder()
            .model(model, provider)
            .tools(tools)
            .grant("model:generate")
            .grant("tool:call:acyclic.read_file")
            .grant("tool:call:acyclic.stage_file")
            .grant("tool:call:acyclic.list_files")
            .limits(limits);
        let bundle = extension.install_into(builder)?.build()?;
        Ok(Self { storage, bundle })
    }
    /// Runs or recovers an exact prompt with a caller-retained operation identity.
    pub async fn run(&self, operation: OperationId, prompt: &str) -> Result<TurnOutput> {
        let content = self
            .storage
            .stage(
                operation,
                &format!("turns/{operation}/user.txt"),
                prompt.as_bytes(),
                "text/plain",
                "prompt.txt",
            )
            .await?;
        let max_steps = u32::try_from(self.bundle.limits().model_steps)
            .map_err(|_| Error::Invalid("model step limit exceeds u32".into()))?;
        self.storage
            .run_conversation(&self.bundle, operation, content, vec![], max_steps)
            .await
    }
    /// Provider-bound storage for tools and recovery.
    #[must_use]
    pub fn storage(&self) -> &DurableHarnessStorage {
        &self.storage
    }
    /// Runtime shared with other local host compositions.
    #[must_use]
    pub fn bundle(&self) -> &crate::Harness {
        &self.bundle
    }
}

fn local_fork_verifier(
    host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
    maximum_bytes: u64,
) -> Result<Arc<CompositeForkVerifier>> {
    let filesystem = Arc::new(FilesystemForkVerifier::new(host, maximum_bytes)?);
    let stream = Arc::new(StreamHistoryForkVerifier::new(ProviderRef::new(
        "local", "stream", "2",
    )?)?);
    Ok(Arc::new(CompositeForkVerifier::new(vec![
        filesystem as Arc<dyn ForkSeedVerifier>,
        stream as Arc<dyn ForkSeedVerifier>,
    ])?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ModelAttempt, ModelEvent, ModelRequest};
    use futures::{future::BoxFuture, stream::BoxStream};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Mock(AtomicUsize);
    impl ModelProvider for Mock {
        fn generate<'a>(&'a self, _: ModelRequest) -> BoxStream<'a, Result<ModelEvent>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::iter([
                Ok(ModelEvent::Content {
                    delta: "persisted".into(),
                }),
                Ok(ModelEvent::Completed {
                    metadata: serde_json::Value::Null,
                }),
            ]))
        }
        fn reconcile<'a>(
            &'a self,
            _: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            Box::pin(async { Ok(None) })
        }
    }
    #[tokio::test]
    async fn authoritative_history_overflow_refuses_dispatch_after_restart() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = Arc::new(Mock(AtomicUsize::new(0)));
        let model = Model::new("mock", "context-overflow", "1", serde_json::json!({}))?;
        let limits = Limits {
            context_messages: 2,
            ..Limits::default()
        };
        let operation = OperationId::new();
        {
            let session =
                PersistentLocalHarness::open(root.path(), model.clone(), provider.clone(), limits)
                    .await?;
            assert_eq!(
                session.run(OperationId::new(), "first").await?.text,
                "persisted"
            );
            assert!(matches!(
                session.run(operation, "second").await,
                Err(Error::Invalid(message)) if message.contains("no history was omitted")
            ));
            assert_eq!(provider.0.load(Ordering::SeqCst), 1);
        }
        let reopened =
            PersistentLocalHarness::open(root.path(), model, provider.clone(), limits).await?;
        assert!(matches!(
            reopened.run(operation, "second").await,
            Err(Error::Invalid(message)) if message.contains("no history was omitted")
        ));
        assert_eq!(provider.0.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn reopen_recovers_completed_turn_without_dispatch() -> Result<()> {
        let root = std::env::temp_dir().join(format!("harness-reopen-{}", OperationId::new()));
        let provider = Arc::new(Mock(AtomicUsize::new(0)));
        let model = Model::new("mock", "durable", "1", serde_json::json!({}))?;
        let operation = OperationId::new();
        let file;
        {
            let session = PersistentLocalHarness::open(
                &root,
                model.clone(),
                provider.clone(),
                Limits::default(),
            )
            .await?;
            file = session
                .storage()
                .stage(
                    OperationId::new(),
                    "example.txt",
                    b"original",
                    "text/plain",
                    "example.txt",
                )
                .await?;
            assert_eq!(
                session.run(operation, "exact prompt\r\n").await?.text,
                "persisted"
            );
        }
        {
            let session = PersistentLocalHarness::open(
                &root,
                model.clone(),
                provider.clone(),
                Limits::default(),
            )
            .await?;
            assert_eq!(session.storage().read(&file).await?, b"original");
            assert_eq!(
                session.run(operation, "exact prompt\r\n").await?.text,
                "persisted"
            );
            assert_eq!(provider.0.load(Ordering::SeqCst), 1);
            assert!(session.run(operation, "different").await.is_err());
        }
        let changed = Model::new("mock", "changed", "1", serde_json::json!({}))?;
        assert!(
            PersistentLocalHarness::open(&root, changed, provider, Limits::default())
                .await
                .is_err()
        );
        std::fs::remove_dir_all(root).map_err(|error| Error::Storage(error.to_string()))?;
        Ok(())
    }
}
