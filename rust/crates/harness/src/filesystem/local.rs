//! Durable local Harness composition using public Stream and Filesystem providers.
use super::{FilesystemForkVerifier, FilesystemHost, HarnessStorage};
use crate::{
    AgentId, ConversationId, Error, OperationId, Result,
    conversation::{
        ContentGrant, FileRef, Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer},
    executor::TurnOutput,
    fork::{CompositeForkVerifier, ForkSeed, ForkSeedVerifier, StreamHistoryForkVerifier},
    host_execution::{
        ExecutionClaim, ExecutionReceipt, ExecutionReceiptKey, ExecutionReceiptRecord,
        ExecutionReceiptStore,
    },
    model::{Model, ModelProvider},
    tool::ToolRegistry,
    resources::ProviderRef,
    store::StreamAggregate,
};
use acyclic_fs::{LocalAuthorityBackend, LocalFs, LocalObjectBackend, LocalOptions};
use acyclic_stream::{
    AppendOutcome, LocalStream, LocalStreamLimits, Stream, StreamClient, StreamError,
};
use futures::StreamExt as _;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};

/// Persistent providers own durability; storage semantics are shared with memory.
pub type DurableHarnessStorage =
    HarnessStorage<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;

const EXECUTION_RECEIPT_STREAM: &str = "harness/system/execution-receipts";
const EXECUTION_RECEIPT_MAX_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ExecutionReceiptEvent {
    Pending { key: ExecutionReceiptKey },
    Completed { record: ExecutionReceiptRecord },
}

/// Host-owned receipt journal for approved local process execution.
///
/// The stream is the durable compare-and-swap boundary. A caller first
/// appends a `Pending` event at the observed tail, then publishes a terminal
/// receipt at a later tail. Reopened providers therefore observe the pending
/// claim and refuse to spawn the command again until an operator resolves it.
/// The receipt content is staged under the reserved internal path and never
/// resolved from a model-supplied workspace reference.
pub struct FilesystemExecutionReceiptStore<A, O> {
    stream: Stream<LocalStream>,
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    write: ContentGrant,
    maximum_bytes: u64,
}

impl<A, O> FilesystemExecutionReceiptStore<A, O> {
    /// Binds one local stream and owner-authorized private volume.
    pub fn new(
        stream: StreamClient<LocalStream>,
        host: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef,
        write: ContentGrant,
        maximum_bytes: u64,
    ) -> Result<Self> {
        if volume.class() != VolumeClass::AgentPrivate
            || volume.provider() != &host.provider
            || maximum_bytes == 0
        {
            return Err(Error::Invalid(
                "execution receipts require a local agent-private volume".into(),
            ));
        }
        write.require(&volume, VolumeOperation::Write)?;
        let stream = stream
            .stream(EXECUTION_RECEIPT_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        Ok(Self {
            stream,
            host,
            volume,
            write,
            maximum_bytes: maximum_bytes.min(EXECUTION_RECEIP_MAX_BYTES),
        })
    }

    async fn events(&self) -> Result<(u64, Vec<ExecutionReceiptEvent>)> {
        let mut replay = self.stream.replay(0);
        let mut events = Vec::new();
        while let Some(page) = replay
            .next_page()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
        {
            for record in page {
                let event: ExecutionReceiptEvent =
                    serde_json::from_slice(&record.value).map_err(|error| {
                        Error::Storage(format!("invalid execution receipt journal: {error}"))
                    })?;
                events.push(event);
            }
        }
        Ok((replay.cursor(), events))
    }

    async fn append_at_tail(&self, tail: u64, event: &ExecutionReceiptEvent) -> Result<bool> {
        let bytes = crate::contract::canonical_json_bytes(event)?;
        match self
            .stream
            .append_at(bytes, tail)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
        {
            AppendOutcome::Committed(_) => Ok(true),
            AppendOutcome::TailConflict { .. } => Ok(false),
        }
    }

    /// Lists claims that remain unresolved after a provider restart.
    pub async fn pending_claims(&self) -> Result<Vec<ExecutionReceiptKey>> {
        let (_, events) = self.events().await?;
        let mut pending = Vec::new();
        for event in events {
            match event {
                ExecutionReceiptEvent::Pending { key } => {
                    if !pending.iter().any(|candidate| candidate == &key) {
                        pending.push(key);
                    }
                }
                ExecutionReceiptEvent::Completed { record } => {
                    record.validate()?;
                    pending.retain(|candidate| candidate != &record.key);
                }
            }
        }
        Ok(pending)
    }

    /// Explicitly resolves a pending claim as unknown after operator review.
    /// This is the only supported path for clearing uncertainty; retrying the
    /// command itself is never inferred from a process or storage failure.
    pub async fn resolve_unknown(
        &self,
        key: &ExecutionReceiptKey,
        reason: impl Into<String>,
    ) -> Result<FileRef> {
        self.publish(
            key,
            &ExecutionReceipt::Unknown {
                reason: reason.into(),
            },
        )
        .await
    }

    fn terminal_for(
        events: &[ExecutionReceiptEvent],
        key: &ExecutionReceiptKey,
    ) -> Result<Option<ExecutionReceiptRecord>> {
        let mut pending = false;
        let mut terminal = None;
        for event in events {
            match event {
                ExecutionReceiptEvent::Pending { key: candidate } if candidate == key => {
                    pending = true;
                }
                ExecutionReceiptEvent::Completed { record } if record.key == *key => {
                    record.validate()?;
                    if !pending {
                        return Err(Error::Storage(
                            "execution receipt completed without a durable claim".into(),
                        ));
                    }
                    if terminal
                        .as_ref()
                        .is_some_and(|prior: &ExecutionReceiptRecord| prior != record)
                    {
                        return Err(Error::Conflict(
                            "execution receipt journal contains conflicting terminal records"
                                .into(),
                        ));
                    }
                    terminal = Some(record.clone());
                }
                _ => {}
            }
        }
        Ok(terminal)
    }
}

impl<A, O> ExecutionReceiptStore for FilesystemExecutionReceiptStore<A, O>
where
    A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
    O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
{
    fn claim<'a>(&'a self, key: &'a ExecutionReceiptKey) -> BoxFuture<'a, Result<ExecutionClaim>> {
        Box::pin(async move {
            if key.provider.is_empty() || key.effect_kind.is_empty() {
                return Err(Error::Invalid("execution receipt key is incomplete".into()));
            }
            loop {
                let (tail, events) = self.events().await?;
                if let Some(record) = Self::terminal_for(&events, key)? {
                    return Ok(ExecutionClaim::Completed(record));
                }
                if events.iter().any(|event| {
                    matches!(event, ExecutionReceiptEvent::Pending { key: candidate } if candidate == key)
                }) {
                    return Ok(ExecutionClaim::Pending);
                }
                if self
                    .append_at_tail(tail, &ExecutionReceiptEvent::Pending { key: key.clone() })
                    .await?
                {
                    return Ok(ExecutionClaim::Acquired);
                }
            }
        })
    }

    fn load<'a>(
        &'a self,
        key: &'a ExecutionReceiptKey,
    ) -> BoxFuture<'a, Result<Option<ExecutionReceiptRecord>>> {
        Box::pin(async move {
            let (_, events) = self.events().await?;
            Self::terminal_for(&events, key)
        })
    }

    fn load_attempt<'a>(
        &'a self,
        attempt_id: crate::EffectAttemptId,
    ) -> BoxFuture<'a, Result<Option<ExecutionReceiptRecord>>> {
        Box::pin(async move {
            let (_, events) = self.events().await?;
            let mut record = None;
            for event in events {
                if let ExecutionReceiptEvent::Completed { record: candidate } = event
                    && candidate.key.attempt_id == attempt_id
                {
                    candidate.validate()?;
                    if record
                        .as_ref()
                        .is_some_and(|prior: &ExecutionReceiptRecord| prior != &candidate)
                    {
                        return Err(Error::Conflict(
                            "execution receipt attempt has conflicting terminal records".into(),
                        ));
                    }
                    record = Some(candidate);
                }
            }
            Ok(record)
        })
    }

    fn publish<'a>(
        &'a self,
        key: &'a ExecutionReceiptKey,
        receipt: &'a ExecutionReceipt,
    ) -> BoxFuture<'a, Result<FileRef>> {
        Box::pin(async move {
            receipt.validate()?;
            let bytes =
                serde_json::to_vec(receipt).map_err(|error| Error::Invalid(error.to_string()))?;
            if bytes.len() as u64 > self.maximum_bytes {
                return Err(Error::Invalid("execution receipt exceeds its bound".into()));
            }
            loop {
                let (tail, events) = self.events().await?;
                if let Some(record) = Self::terminal_for(&events, key)? {
                    if record.receipt != *receipt {
                        return Err(Error::Conflict(
                            "execution receipt publication conflicts with the durable result"
                                .into(),
                        ));
                    }
                    return Ok(record.result);
                }
                if !events.iter().any(|event| {
                    matches!(event, ExecutionReceiptEvent::Pending { key: candidate } if candidate == key)
                }) {
                    return Err(Error::Conflict(
                        "execution receipt publication has no durable claim".into(),
                    ));
                }
                let result = self
                    .host
                    .put_internal_content(
                        &self.volume,
                        &self.write,
                        &format!(".system/execution/{}.json", key.attempt_id),
                        &bytes,
                        "application/json",
                        "execution-result.json",
                        self.maximum_bytes,
                        &crate::IdempotencyKey::new(format!(
                            "execution-receipt:{}",
                            key.attempt_id
                        ))?,
                        super::InternalContentClass::Execution,
                    )
                    .await?;
                let record = ExecutionReceiptRecord {
                    key: key.clone(),
                    result,
                    receipt: receipt.clone(),
                };
                record.validate()?;
                let published = record.result.clone();
                if self
                    .append_at_tail(tail, &ExecutionReceiptEvent::Completed { record })
                    .await?
                {
                    return Ok(published);
                }
            }
        })
    }
}

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
    batch_publisher: Option<Arc<dyn crate::batch_publication::ModelBatchPublisher>>,
}

impl LocalHarnessTools {
    /// Creates an empty extension set.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            tools: ToolRegistry::new(),
            batch_publisher: None,
        }
    }

    /// Retains one explicitly assembled tool registry.
    #[must_use]
    pub fn from_registry(tools: ToolRegistry) -> Self {
        Self {
            tools,
            batch_publisher: None,
        }
    }

    /// Binds the owner mediated completed batch publisher used by model turns.
    #[must_use]
    pub fn with_batch_publisher(
        mut self,
        publisher: Arc<dyn crate::batch_publication::ModelBatchPublisher>,
    ) -> Self {
        self.batch_publisher = Some(publisher);
        self
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
        if let Some(publisher) = &self.batch_publisher {
            builder = builder.batch_publisher(publisher.clone());
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

    /// Returns a host-owned receipt store bound to this reopened session.
    /// The store uses the session's private owner grant and system stream;
    /// callers cannot redirect it to a model-visible workspace path.
    pub fn execution_receipt_store(
        &self,
    ) -> Result<Arc<FilesystemExecutionReceiptStore<LocalAuthorityBackend, LocalObjectBackend>>> {
        let (host, stream, write, maximum_bytes) = self.storage.execution_binding();
        Ok(Arc::new(FilesystemExecutionReceiptStore::new(
            stream,
            host,
            self.storage.volume().clone(),
            write,
            maximum_bytes,
        )?))
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
    use crate::{EffectAttemptId, EffectGuarantee, EffectId};
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

    #[tokio::test]
    async fn reopened_receipt_providers_share_atomic_claim_and_terminal_record() -> Result<()> {
        let root =
            std::env::temp_dir().join(format!("harness-receipt-reopen-{}", OperationId::new()));
        let model = Model::new("mock", "durable", "1", serde_json::json!({}))?;
        let key = ExecutionReceiptKey {
            operation_id: OperationId::from_bytes([61; 16]),
            effect_id: EffectId::from_bytes([62; 16]),
            attempt_id: EffectAttemptId::from_bytes([63; 16]),
            provider: "harness.native-execution.v1".into(),
            effect_kind: "host.process".into(),
            guarantee: EffectGuarantee::AtMostOnce,
            request_digest: [64; 32],
        };
        let receipt = ExecutionReceipt::Succeeded {
            status_code: 0,
            stdout: b"ok".to_vec(),
            stderr: Vec::new(),
        };
        {
            let session = PersistentLocalHarness::open(
                &root,
                model.clone(),
                Arc::new(Mock(AtomicUsize::new(0))),
                Limits::default(),
            )
            .await?;
            let store = session.execution_receipt_store()?;
            assert_eq!(store.claim(&key).await?, ExecutionClaim::Acquired);
        }
        let first = {
            let session = PersistentLocalHarness::open(
                &root,
                model.clone(),
                Arc::new(Mock(AtomicUsize::new(0))),
                Limits::default(),
            )
            .await?;
            let store = session.execution_receipt_store()?;
            assert_eq!(store.claim(&key).await?, ExecutionClaim::Pending);
            assert_eq!(store.pending_claims().await?, vec![key.clone()]);
            store.publish(&key, &receipt).await?
        };
        {
            let session = PersistentLocalHarness::open(
                &root,
                model,
                Arc::new(Mock(AtomicUsize::new(0))),
                Limits::default(),
            )
            .await?;
            let store = session.execution_receipt_store()?;
            let claim = store.claim(&key).await?;
            let ExecutionClaim::Completed(record) = claim else {
                return Err(Error::Storage("reopened receipt was not terminal".into()));
            };
            assert_eq!(record.result, first);
            assert_eq!(record.receipt, receipt);
            assert_eq!(store.load_attempt(key.attempt_id).await?, Some(record));
            assert!(session.storage().content_verifier().read(&first).await.is_err());
        }
        std::fs::remove_dir_all(root).map_err(|error| Error::Storage(error.to_string()))?;
        Ok(())
    }
}
