//! Durable local Harness composition using public Stream and Filesystem providers.
use super::{FilesystemHost, HarnessStorage};
use crate::{
    AgentId, ConversationId, Error, OperationId, Result, SessionId,
    conversation::{
        ContentGrant, FileRef, Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer},
    executor::TurnOutput,
    host_execution::{
        ExecutionClaim, ExecutionClaimHandle, ExecutionReceipt, ExecutionReceiptKey,
        ExecutionReceiptRecord, ExecutionReceiptStore, ExecutionResolutionCapability,
        NativeExecutionProvider,
    },
    model::{Model, ModelProvider},
    resources::ProviderRef,
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
    Pending {
        key: ExecutionReceiptKey,
        owner_token: [u8; 32],
        generation: u64,
    },
    Completed {
        key: ExecutionReceiptKey,
        result: FileRef,
        owner_token: [u8; 32],
        generation: u64,
        operator_resolution: bool,
    },
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
    read: ContentGrant,
    write: ContentGrant,
    session_id: SessionId,
    resolution_token: [u8; 32],
    maximum_bytes: u64,
}

impl<A, O> FilesystemExecutionReceiptStore<A, O>
where
    A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
    O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
{
    /// Binds one local stream and owner-authorized private volume.
    pub fn new(
        stream: StreamClient<LocalStream>,
        host: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef,
        session_id: SessionId,
        resolution: &ExecutionResolutionCapability,
        read: ContentGrant,
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
        read.require(&volume, VolumeOperation::Read)?;
        let expected_resolution = ExecutionResolutionCapability::issue(session_id, &volume)?;
        if resolution != &expected_resolution {
            return Err(Error::Unauthorized(
                "execution receipt resolver is not bound to this session and volume".into(),
            ));
        }
        let stream_name = format!(
            "{EXECUTION_RECEIPT_STREAM}/{}/{}",
            volume.storage_name()?,
            session_id
        );
        let stream = stream
            .stream(stream_name)
            .map_err(|error| Error::Storage(error.to_string()))?;
        Ok(Self {
            stream,
            host,
            volume,
            read,
            write,
            session_id,
            resolution_token: *resolution.token(),
            maximum_bytes: maximum_bytes.min(EXECUTION_RECEIPT_MAX_BYTES),
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
        Self::validate_events(&events)?;
        Ok((replay.cursor(), events))
    }

    fn validate_events(events: &[ExecutionReceiptEvent]) -> Result<()> {
        let mut pending = Vec::<(ExecutionReceiptKey, [u8; 32], u64)>::new();
        let mut finalized = Vec::<ExecutionReceiptKey>::new();
        for event in events {
            match event {
                ExecutionReceiptEvent::Pending {
                    key,
                    owner_token,
                    generation,
                } => {
                    if finalized.iter().any(|candidate| candidate == key)
                        || pending.iter().any(|(candidate, _, _)| candidate == key)
                    {
                        return Err(Error::Storage(
                            "execution receipt journal contains a duplicate claim".into(),
                        ));
                    }
                    pending.push((key.clone(), *owner_token, *generation));
                }
                ExecutionReceiptEvent::Completed {
                    key,
                    result,
                    owner_token,
                    generation,
                    operator_resolution: _,
                } => {
                    result.validate()?;
                    if result.descriptor().media_type() != "application/json" {
                        return Err(Error::Storage(
                            "execution receipt result is not JSON content".into(),
                        ));
                    }
                    if finalized.iter().any(|candidate| candidate == key) {
                        return Err(Error::Storage(
                            "execution receipt terminal record is orphaned or duplicated".into(),
                        ));
                    }
                    let Some((_, pending_token, pending_generation)) =
                        pending.iter().find(|(candidate, _, _)| candidate == key)
                    else {
                        return Err(Error::Storage(
                            "execution receipt terminal record is orphaned or duplicated".into(),
                        ));
                    };
                    if pending_token != owner_token || pending_generation != generation {
                        return Err(Error::Conflict(
                            "execution receipt terminal owner does not match its claim".into(),
                        ));
                    }
                    pending.retain(|(candidate, _, _)| candidate != key);
                    finalized.push(key.clone());
                }
            }
        }
        Ok(())
    }

    fn validate_result_ref(&self, key: &ExecutionReceiptKey, result: &FileRef) -> Result<()> {
        result.validate()?;
        if result.volume() != &self.volume
            || result.descriptor().media_type() != "application/json"
            || !result
                .path()
                .starts_with(&format!(".system/execution/{}/", key.attempt_id))
        {
            return Err(Error::Unauthorized(
                "execution receipt result is outside its host-owned attempt path".into(),
            ));
        }
        Ok(())
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
    pub async fn pending_claims(
        &self,
        resolution: &ExecutionResolutionCapability,
        resolver: &ContentGrant,
    ) -> Result<Vec<(ExecutionReceiptKey, ExecutionClaimHandle)>> {
        if !resolution.matches(self.session_id, &self.resolution_token) {
            return Err(Error::Unauthorized(
                "execution receipt resolver is not authenticated for this session".into(),
            ));
        }
        resolver.require(&self.volume, VolumeOperation::Write)?;
        let (_, events) = self.events().await?;
        let mut pending: Vec<(ExecutionReceiptKey, ExecutionClaimHandle)> = Vec::new();
        for event in events {
            match event {
                ExecutionReceiptEvent::Pending {
                    key,
                    owner_token,
                    generation,
                } => {
                    if !pending.iter().any(|(candidate, _)| candidate == &key) {
                        pending.push((
                            key.clone(),
                            ExecutionClaimHandle::from_parts(key, owner_token, generation, true),
                        ));
                    }
                }
                ExecutionReceiptEvent::Completed { key, result, .. } => {
                    result.validate()?;
                    pending.retain(|(candidate, _)| candidate != key);
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
        resolution: &ExecutionResolutionCapability,
        resolver: &ContentGrant,
        handle: &ExecutionClaimHandle,
        reason: impl Into<String>,
    ) -> Result<FileRef> {
        if !resolution.matches(self.session_id, &self.resolution_token) {
            return Err(Error::Unauthorized(
                "execution receipt resolver is not authenticated for this session".into(),
            ));
        }
        resolver.require(&self.volume, VolumeOperation::Write)?;
        if !handle.is_operator() {
            return Err(Error::Unauthorized(
                "pending execution resolution requires an operator handle".into(),
            ));
        }
        self.publish(
            key,
            handle,
            &ExecutionReceipt::Unknown {
                reason: reason.into(),
            },
        )
        .await
    }

    async fn terminal_for(
        &self,
        events: &[ExecutionReceiptEvent],
        key: &ExecutionReceiptKey,
    ) -> Result<Option<ExecutionReceiptRecord>> {
        let mut pending = false;
        let mut terminal = None;
        for event in events {
            match event {
                ExecutionReceiptEvent::Pending { key: candidate, .. } if candidate == key => {
                    pending = true;
                }
                ExecutionReceiptEvent::Completed {
                    key: candidate,
                    result,
                    ..
                } if candidate == key => {
                    self.validate_result_ref(candidate, result)?;
                    let bytes = self
                        .host
                        .read_content(result, &self.read, self.maximum_bytes)
                        .await?
                        .to_vec();
                    result.descriptor().verify(&bytes)?;
                    let receipt: ExecutionReceipt = serde_json::from_slice(&bytes)
                        .map_err(|error| Error::Storage(error.to_string()))?;
                    receipt.validate()?;
                    let record = ExecutionReceiptRecord {
                        key: candidate.clone(),
                        result: result.clone(),
                        receipt,
                    };
                    if !pending {
                        return Err(Error::Storage(
                            "execution receipt completed without a durable claim".into(),
                        ));
                    }
                    if terminal
                        .as_ref()
                        .is_some_and(|prior: &ExecutionReceiptRecord| prior != &record)
                    {
                        return Err(Error::Conflict(
                            "execution receipt journal contains conflicting terminal records"
                                .into(),
                        ));
                    }
                    terminal = Some(record);
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
                if let Some(record) = self.terminal_for(&events, key).await? {
                    return Ok(ExecutionClaim::Completed(record));
                }
                if events.iter().any(|event| {
                    matches!(event, ExecutionReceiptEvent::Pending { key: candidate, .. } if candidate == key)
                }) {
                    return Ok(ExecutionClaim::Pending);
                }
                let handle = ExecutionClaimHandle::issue(key.clone(), tail, false);
                if self
                    .append_at_tail(
                        tail,
                        &ExecutionReceiptEvent::Pending {
                            key: key.clone(),
                            owner_token: *handle.token(),
                            generation: handle.generation(),
                        },
                    )
                    .await?
                {
                    return Ok(ExecutionClaim::Acquired { handle });
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
            self.terminal_for(&events, key).await
        })
    }

    fn load_attempt<'a>(
        &'a self,
        attempt_id: crate::EffectAttemptId,
    ) -> BoxFuture<'a, Result<Option<ExecutionReceiptRecord>>> {
        Box::pin(async move {
            let (_, events) = self.events().await?;
            let mut record = None;
            for event in &events {
                if let ExecutionReceiptEvent::Completed { key: candidate, .. } = event
                    && candidate.attempt_id == attempt_id
                {
                    let Some(candidate) = self.terminal_for(&events, candidate).await? else {
                        continue;
                    };
                    if record.as_ref().is_some_and(|prior| prior != &candidate) {
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
        handle: &'a ExecutionClaimHandle,
        receipt: &'a ExecutionReceipt,
    ) -> BoxFuture<'a, Result<FileRef>> {
        Box::pin(async move {
            receipt.validate()?;
            let bytes =
                serde_json::to_vec(receipt).map_err(|error| Error::Invalid(error.to_string()))?;
            if bytes.len() as u64 > self.maximum_bytes {
                return Err(Error::Invalid("execution receipt exceeds its bound".into()));
            }
            if !handle.matches(key) {
                return Err(Error::Unauthorized(
                    "execution receipt handle is bound to another key".into(),
                ));
            }
            if handle.is_operator() && !matches!(receipt, ExecutionReceipt::Unknown { .. }) {
                return Err(Error::Unauthorized(
                    "operator handles may only resolve execution as unknown".into(),
                ));
            }
            loop {
                let (tail, events) = self.events().await?;
                if let Some(record) = self.terminal_for(&events, key).await? {
                    if record.receipt == *receipt {
                        return Ok(record.result);
                    }
                    return Err(Error::Conflict(
                        "execution receipt claim was already finalized".into(),
                    ));
                }
                let Some((owner_token, generation)) = events.iter().find_map(|event| {
                    let ExecutionReceiptEvent::Pending {
                        key: candidate,
                        owner_token,
                        generation,
                    } = event
                    else {
                        return None;
                    };
                    (candidate == key).then_some((*owner_token, *generation))
                }) else {
                    return Err(Error::Conflict(
                        "execution receipt publication has no durable claim".into(),
                    ));
                };
                if owner_token != *handle.token() || generation != handle.generation() {
                    return Err(Error::Conflict(
                        "execution receipt handle is stale or owned by another dispatcher".into(),
                    ));
                }
                let result = self
                    .host
                    .put_internal_content(
                        &self.volume,
                        &self.write,
                        &format!(
                            ".system/execution/{}/{}.json",
                            key.attempt_id,
                            if handle.is_operator() {
                                format!("resolve-{}", hex::encode(handle.token()))
                            } else {
                                "dispatch".into()
                            }
                        ),
                        &bytes,
                        "application/json",
                        "execution-result.json",
                        self.maximum_bytes,
                        &crate::IdempotencyKey::new(format!(
                            "execution-receipt:{}:{}",
                            key.attempt_id,
                            hex::encode(handle.token())
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
                    .append_at_tail(
                        tail,
                        &ExecutionReceiptEvent::Completed {
                            key: record.key,
                            result: record.result,
                            owner_token: *handle.token(),
                            generation: handle.generation(),
                            operator_resolution: handle.is_operator(),
                        },
                    )
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
    #[serde(default)]
    session_id: Option<SessionId>,
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
            session_id: Some(SessionId::new()),
            model,
            limits,
        };
        Ok(descriptor)
    }
}

fn descriptor_session_id(descriptor: &SessionDescriptor) -> Result<SessionId> {
    let session_id = if let Some(session_id) = descriptor.session_id {
        session_id
    } else {
        let mut input = Vec::with_capacity(40);
        input.extend_from_slice(b"acyclic:harness:legacy-session:v1");
        input.extend_from_slice(&descriptor.signing_key);
        let bytes: [u8; 16] = blake3::hash(&input).as_bytes()[..16]
            .try_into()
            .map_err(|_| Error::Invalid("legacy session identity has invalid length".into()))?;
        SessionId::from_bytes(bytes)
    };
    if session_id.into_bytes() == [0; 16] {
        return Err(Error::Invalid("session identity cannot be zero".into()));
    }
    Ok(session_id)
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
impl PersistentLocalHarness {
    /// Opens an exclusive session root, preserving stable identities and configuration.
    pub async fn open(
        root: impl AsRef<Path>,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
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
        let session_id = descriptor_session_id(&descriptor)?;
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
        let storage = DurableHarnessStorage::from_providers_with_session(
            descriptor.agent,
            limits.file_bytes,
            host,
            stream,
            descriptor.private_volume,
            descriptor.conversation,
            issuer,
            session_id,
        )
        .await?;
        let tools = storage.default_tools(limits)?;
        let bundle = storage
            .builder()
            .model(model, provider)
            .tools(tools)
            .grant("model:generate")
            .grant("tool:call:acyclic.read_file")
            .grant("tool:call:acyclic.stage_file")
            .grant("tool:call:acyclic.list_files")
            .limits(limits)
            .build()?;
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
    ) -> Result<Arc<FilesystemExecutionReceiptStore<LocalAuthorityBackend, LocalObjectBackend>>>
    {
        let (host, stream, read, write, maximum_bytes) = self.storage.execution_binding();
        let resolution = self.execution_resolution_capability()?;
        Ok(Arc::new(FilesystemExecutionReceiptStore::new(
            stream,
            host,
            self.storage.volume().clone(),
            self.storage.session_id(),
            &resolution,
            read,
            write,
            maximum_bytes,
        )?))
    }

    /// Returns the host application's separately authenticated authority for
    /// resolving uncertain process attempts after review.
    pub fn execution_resolution_capability(&self) -> Result<ExecutionResolutionCapability> {
        ExecutionResolutionCapability::issue(self.storage.session_id(), self.storage.volume())
    }

    /// Composes the production native provider around this session's
    /// host-owned receipt journal and authenticated interaction verifier.
    /// The provider uses the native runner and never falls back to a
    /// model-writable receipt path.
    pub fn native_execution_provider(&self) -> Result<NativeExecutionProvider> {
        NativeExecutionProvider::native_with_receipt_store(
            self.storage.content_verifier(),
            self.execution_receipt_store()?,
            self.storage.execution_approval_verifier(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ModelAttempt, ModelEvent, ModelRequest};
    use crate::{EffectAttemptId, EffectId, core::EffectGuarantee};
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
        {
            let session = PersistentLocalHarness::open(
                &root,
                model.clone(),
                Arc::new(Mock(AtomicUsize::new(0))),
                Limits::default(),
            )
            .await?;
            let store = session.execution_receipt_store()?;
            assert!(matches!(
                store.claim(&key).await?,
                ExecutionClaim::Acquired { .. }
            ));
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
            let (_, _, _, resolver, _) = session.storage().execution_binding();
            let resolution = session.execution_resolution_capability()?;
            let pending = store.pending_claims(&resolution, &resolver).await?;
            assert_eq!(pending.len(), 1);
            let (_, operator) = pending.into_iter().next().unwrap();
            store
                .resolve_unknown(
                    &key,
                    &resolution,
                    &resolver,
                    &operator,
                    "operator resolved after restart",
                )
                .await?
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
            assert!(matches!(record.receipt, ExecutionReceipt::Unknown { .. }));
            assert_eq!(store.load_attempt(key.attempt_id).await?, Some(record));
            assert!(
                session
                    .storage()
                    .content_verifier()
                    .read(&first)
                    .await
                    .is_err()
            );
        }
        std::fs::remove_dir_all(root).map_err(|error| Error::Storage(error.to_string()))?;
        Ok(())
    }

    #[tokio::test]
    async fn reopened_local_receipt_provider_replays_success_ack() -> Result<()> {
        let root =
            std::env::temp_dir().join(format!("harness-receipt-success-{}", OperationId::new()));
        let model = Model::new("mock", "durable", "1", serde_json::json!({}))?;
        let key = ExecutionReceiptKey {
            operation_id: OperationId::from_bytes([71; 16]),
            effect_id: EffectId::from_bytes([72; 16]),
            attempt_id: EffectAttemptId::from_bytes([73; 16]),
            provider: "harness.native-execution.v1".into(),
            effect_kind: "host.process".into(),
            guarantee: EffectGuarantee::AtMostOnce,
            request_digest: [74; 32],
        };
        let result = {
            let session = PersistentLocalHarness::open(
                &root,
                model.clone(),
                Arc::new(Mock(AtomicUsize::new(0))),
                Limits::default(),
            )
            .await?;
            let store = session.execution_receipt_store()?;
            let ExecutionClaim::Acquired { handle } = store.claim(&key).await? else {
                return Err(Error::Conflict(
                    "local receipt claim was not acquired".into(),
                ));
            };
            let receipt = ExecutionReceipt::Succeeded {
                status_code: 0,
                stdout: b"ack".to_vec(),
                stderr: Vec::new(),
            };
            let result = store.publish(&key, &handle, &receipt).await?;
            // A caller may lose the publication acknowledgement after the
            // terminal event is durable. Retrying the exact protected
            // receipt is an idempotent replay and returns the retained ref.
            assert_eq!(store.publish(&key, &handle, &receipt).await?, result);
            result
        };
        let session = PersistentLocalHarness::open(
            &root,
            model,
            Arc::new(Mock(AtomicUsize::new(0))),
            Limits::default(),
        )
        .await?;
        let store = session.execution_receipt_store()?;
        let ExecutionClaim::Completed(record) = store.claim(&key).await? else {
            return Err(Error::Storage(
                "reopened success receipt was not terminal".into(),
            ));
        };
        assert_eq!(record.result, result);
        assert!(matches!(record.receipt, ExecutionReceipt::Succeeded { .. }));
        std::fs::remove_dir_all(root).map_err(|error| Error::Storage(error.to_string()))?;
        Ok(())
    }
}
