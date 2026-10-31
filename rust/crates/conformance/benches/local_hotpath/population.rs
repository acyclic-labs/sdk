use super::{
    actor_delivery::{Entries, Observed},
    report::{Result, emit},
};
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock};
use acyclic_fs::{EmbeddedCapabilities, Fs, LogicalObjectStore, StreamAuthorityStore};
use acyclic_harness_core::{
    OperationId,
    agent::{AgentId, AgentPath, AgentState, IntentId, VolumeBinding},
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityRoot},
    executor::Executor,
    filesystem::{
        PRIVATE_VOLUME, TurnAuthority, TurnExecutors, TurnLimits,
        hosting::{Bound, Placement, RootRequest, RootSubmission, Turns},
    },
    model::ModelContent,
    resources::ProviderRef,
};
use acyclic_objects::{LocalMedium, LocalObjects, Store, local::LocalObjectsLimits};
use acyclic_stream::testing::{Counted, RequestLedger};
use acyclic_stream::{
    BoxProviderFuture, CommitRequest, IdempotencyKey, LocalStream, LocalStreamLimits, MemoryLimits,
};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

pub(super) type Objects = LogicalObjectStore<Counted<LocalObjects>>;
pub(super) type Streams = Counted<LocalStream>;
pub(super) type Filesystem = Fs<StreamAuthorityStore<Streams>, Objects>;
pub(super) type Binding = Bound<Streams, Objects>;

/// Creation-only executors refuse accidental execution rather than making up a
/// model completion. Population roots remain dormant throughout measurement.
struct CreationOnly(Arc<AtomicU64>);
impl TurnExecutors for CreationOnly {
    fn executor<'a>(
        &'a self,
        _: &'a AgentState,
        _: OperationId,
        _: &'a ModelContent,
        _: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<Arc<dyn Executor>>> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Box::pin(async {
            Err(acyclic_harness_core::Error::Unsupported(
                "creation-only benchmark fixture".into(),
            ))
        })
    }
}

pub(super) struct Fixture {
    pub(super) stream: Arc<Streams>,
    pub(super) requests: RequestLedger,
    pub(super) objects: Arc<Counted<LocalObjects>>,
    pub(super) fs: Filesystem,
    pub(super) bound: Binding,
    pub(super) provider: ProviderRef,
    pub(super) entries: Arc<Entries>,
    pub(super) model_calls: Arc<AtomicU64>,
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
        let requests = RequestLedger::new();
        let stream = Arc::new(Counted::with_ledger(
            LocalStream::open(root.join("stream"), limits).await?,
            requests.clone(),
        ));
        let medium =
            LocalMedium::open(root.join("objects"), LocalObjectsLimits::default(), None).await?;
        let objects = Arc::new(Counted::with_ledger(
            Store::new(medium).account(b"benchmark")?,
            requests.clone(),
        ));
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
        let model_calls = Arc::new(AtomicU64::new(0));
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
                executors: Arc::new(CreationOnly(model_calls.clone())),
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
        let entries = Arc::new(Entries::default());
        let handler = Arc::new(Observed {
            handler: bound.handler(),
            entries: entries.clone(),
        });
        let bound = bound.hosting(handler)?;
        let fixture = Self {
            stream,
            requests,
            objects,
            fs,
            bound,
            provider,
            entries,
            model_calls: model_calls.clone(),
        };
        // Use the same publication owner as session creation. Concatenating
        // independent root requests duplicates their shared activation stream.
        let mut first_request: Option<CommitRequest> = None;
        for index in 0..agents {
            let agent = id(index)?;
            let expected = AgentPath::root(&agent);
            let fresh = vec![fixture.volume(&agent)?];
            let prepared = Box::pin(fixture.bound.prepare_root(
                &IntentId::try_from(format!("population-{index}").as_str())?,
                RootRequest {
                    agent,
                    fresh,
                    retained: Vec::new(),
                    brief: None,
                    policy: None,
                },
            ))
            .await?;
            if index == 0 && agents > 1 {
                first_request = Some(prepared.request().clone());
            } else if let Some(mut invalid) = first_request.take() {
                invalid
                    .conditions
                    .extend(prepared.request().conditions.iter().cloned());
                invalid
                    .mutations
                    .extend(prepared.request().mutations.iter().cloned());
                if acyclic_stream::request::validate_commit(&mut invalid)
                    != Err(acyclic_stream::StreamError::InvalidArgument)
                {
                    return Err("root composition oracle accepted concatenated publications".into());
                }
                emit(serde_json::json!({
                    "schema": 1, "kind": "negative_control",
                    "control": "concatenated real root publications",
                    "agents": agents, "result": "typed invalid argument before submission",
                }))?;
            }
            match Box::pin(fixture.bound.submit_root(prepared)).await? {
                RootSubmission::Created(child) if child == expected => {}
                outcome => {
                    return Err(format!("population root {index} publication: {outcome:?}").into());
                }
            }
        }
        let runtime = fixture.bound.agents().runtime();
        // Registration queues initial replay. Prove that the idle oracle
        // refuses this real unsettled population before consuming that work.
        if require_idle(agents, runtime.counts()).is_ok() {
            return Err("idle safety oracle accepted initial queued replay".into());
        }
        emit(
            serde_json::json!({"schema": 1, "kind": "negative_control", "control": "real population before initial replay settles", "agents": agents, "result": "rejected"}),
        )?;
        // The runtime consumes its real ready events and commits adoption.
        // Empty briefs request no model turn; no readiness retry is needed.
        if let Some((actor, error)) = Box::pin(runtime.drain()).await.into_iter().next() {
            return Err(format!("initial agent replay {actor:?} failed: {error}").into());
        }
        require_idle(agents, runtime.counts())?;
        if model_calls.load(Ordering::Relaxed) != 0 {
            return Err("dormant population requested model execution".into());
        }
        emit(
            serde_json::json!({"schema": 1, "kind": "setup", "agents": agents, "elapsed_seconds": start.elapsed().as_secs_f64(), "dormant": agents, "ready": runtime.counts().1, "model_calls": 0, "active": 1}),
        )?;
        Ok(fixture)
    }
}

fn require_idle(agents: usize, observed: (usize, usize)) -> Result {
    if observed != (agents, 0) {
        return Err(format!(
            "idle population differs: expected=({agents}, 0), observed={observed:?}"
        )
        .into());
    }
    Ok(())
}
