use super::report::{Result, emit};
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock};
use acyclic_fs::{
    CancellationToken, EmbeddedCapabilities, Fs, LogicalObjectStore, StreamAuthorityStore,
    WorkBudget,
};
use acyclic_harness_core::{
    OperationId,
    agent::{AgentId, AgentState, ForkAdopted, IntentId, VolumeBinding},
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityRoot},
    executor::Executor,
    filesystem::{
        PRIVATE_VOLUME, TurnAuthority, TurnExecutors, TurnLimits,
        hosting::{Bound, Placement, Turns},
    },
    model::ModelContent,
    resources::ProviderRef,
};
use acyclic_objects::{LocalMedium, LocalObjects, Store, local::LocalObjectsLimits};
use acyclic_stream::{
    BoxProviderFuture, CommitRequest, IdempotencyKey, LocalStream, LocalStreamLimits, MemoryLimits,
};
use std::{path::Path, sync::Arc, time::Instant};

pub(super) type Objects = LogicalObjectStore<LocalObjects>;
pub(super) type Filesystem = Fs<StreamAuthorityStore<LocalStream>, Objects>;
pub(super) type Binding = Bound<LocalStream, Objects>;

/// Creation-only executors refuse accidental execution rather than making up a
/// model completion. Population roots remain dormant throughout measurement.
struct CreationOnly;
impl TurnExecutors for CreationOnly {
    fn executor<'a>(
        &'a self,
        _: &'a AgentState,
        _: OperationId,
        _: &'a ModelContent,
        _: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<Arc<dyn Executor>>> {
        Box::pin(async {
            Err(acyclic_harness_core::Error::Unsupported(
                "creation-only benchmark fixture".into(),
            ))
        })
    }
}

pub(super) struct Fixture {
    pub(super) stream: Arc<LocalStream>,
    pub(super) objects: Arc<LocalObjects>,
    pub(super) fs: Filesystem,
    pub(super) bound: Binding,
    pub(super) provider: ProviderRef,
}

pub(super) fn key(value: impl Into<String>) -> Result<IdempotencyKey> {
    Ok(IdempotencyKey::new(value.into().into_bytes())?)
}

pub(super) fn id(value: usize) -> Result<AgentId> {
    Ok(AgentId::from_bytes(u128::try_from(value)?.to_le_bytes()))
}

impl Fixture {
    pub(super) fn volume(&self, agent: &AgentId) -> Result<VolumeBinding> {
        Ok(VolumeBinding {
            name: PRIVATE_VOLUME.into(),
            volume: VolumeRef::new(
                self.provider.clone(),
                format!("private-{agent}"),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(acyclic_harness_core::AgentId::from_bytes(agent.to_bytes())),
                &mut None,
            )?,
        })
    }

    pub(super) async fn open(root: &Path, agents: usize) -> Result<Self> {
        let start = Instant::now();
        let limits = LocalStreamLimits {
            memory: MemoryLimits {
                paths: usize::MAX,
                path_bytes: usize::MAX,
                records: usize::MAX,
                payload_bytes: usize::MAX,
                commits: usize::MAX,
                idempotency_results: usize::MAX,
            },
            journal_operations: u64::MAX,
            journal_bytes: u64::MAX,
            ..LocalStreamLimits::default()
        };
        let stream = Arc::new(LocalStream::open(root.join("stream"), limits).await?);
        let medium =
            LocalMedium::open(root.join("objects"), LocalObjectsLimits::default(), None).await?;
        let objects = Arc::new(Store::new(medium).account(b"benchmark")?);
        let fs = Fs::new(
            StreamAuthorityStore::new(stream.clone()),
            LogicalObjectStore::new(objects.clone()),
            EmbeddedCapabilities { durable: true },
        );
        let provider = ProviderRef::new(
            "benchmark".into(),
            "filesystem".into(),
            "1".into(),
            &mut None,
        )?;
        let bound = Bound::local_root(
            AuthorityRoot::new(
                "benchmark",
                [7; 32],
                Authority {
                    kind: AggregateKind::Conversation,
                    id: "benchmark".into(),
                },
            ),
            Placement {
                filesystem: fs.clone(),
                provider: provider.clone(),
                within: ActorReference::new(b"benchmark", b"agents", b"owner")?,
            },
            Turns {
                executors: Arc::new(CreationOnly),
                limits: TurnLimits {
                    maximum_bytes: 1 << 20,
                    max_steps: 1,
                    history_turns: 1,
                },
                actors: ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
                clock: Arc::new(SystemClock),
                approval_policy: Default::default(),
            },
        )?;
        let fixture = Self {
            stream,
            objects,
            fs,
            bound,
            provider,
        };
        // Chunking bounds setup memory and command size; it never bounds the
        // population. Each root is an actual actor, inbox subtree and volume.
        for first in (0..agents).step_by(256) {
            fixture.chunk(first, (agents - first).min(256)).await?;
        }
        emit(
            serde_json::json!({"schema": 1, "kind": "setup", "agents": agents, "elapsed_seconds": start.elapsed().as_secs_f64(), "dormant": agents, "active": 1}),
        )?;
        Ok(fixture)
    }

    async fn chunk(&self, first: usize, count: usize) -> Result {
        use acyclic_fs::{
            CreateSpec, CreationOperation, Lifecycle, VolumeConfig, VolumeInitialization,
            WorkspaceName,
            distributed::{FilesystemCommit, FilesystemCommitOutcome},
        };
        let cancellation = CancellationToken::new();
        let mut request = CommitRequest {
            deadline_unix_millis: None,
            conditions: Vec::new(),
            mutations: Vec::new(),
            idempotency_key: key(format!("population/{first}"))?,
        };
        let mut creations = Vec::with_capacity(count);
        let mut children = Vec::with_capacity(count);
        for index in first..first + count {
            let agent = id(index)?;
            let volume = self.volume(&agent)?;
            let name = volume.volume.storage_name(&mut None)?;
            let created = self.bound.agents().prepare_root(
                &IntentId::try_from(format!("population-{index}").as_str())?,
                ForkAdopted {
                    agent,
                    parent: None,
                    forked_at: 0,
                    conversation_tail: 0,
                    checkpoint: None,
                    volumes: vec![volume],
                    brief: None,
                    policy: None,
                },
            )?;
            request.conditions.extend(created.request.conditions);
            request.mutations.extend(created.request.mutations);
            children.push(created.child);
            creations.push(CreateSpec {
                name: WorkspaceName::new(name, &WorkBudget::UNBOUNDED, &cancellation)
                    .map_err(|failure| failure.error)?
                    .value,
                initialization: VolumeInitialization::new(
                    VolumeConfig::portable(Lifecycle::Durable),
                    None,
                ),
                creation: CreationOperation::populated(
                    acyclic_fs::OperationId::from_bytes(u128::try_from(index)?.to_le_bytes()),
                    &[],
                ),
            });
        }
        let mut commit = FilesystemCommit::new(request);
        Box::pin(self.fs.prepare_create_many(
            &creations,
            &mut commit,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
        .map_err(|failure| failure.error)?;
        self.fs
            .seal_commit(&mut commit, WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|failure| failure.error)?;
        let committed = Box::pin(self.fs.submit_commit(
            commit,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
        .map_err(|failure| failure.error)?
        .value;
        if !matches!(committed, FilesystemCommitOutcome::Committed(_)) {
            return Err("population creation was not committed".into());
        }
        for child in children {
            self.bound.agents().host(&child)?;
        }
        Ok(())
    }
}
