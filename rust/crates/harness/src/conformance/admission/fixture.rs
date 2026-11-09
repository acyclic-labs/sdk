//! Shared private host setup; identity vectors enter the same production host.
use super::*;
use acyclic_harness_core::{
    context::CompactionPolicy,
    conversation::Limits,
    filesystem::{
        Grants, StockExecutors,
        hosting::{Bound, Placement, RootRequest, RootSubmission, Turns},
    },
    model::Model,
    test_support::{ScriptedModelProvider, ScriptedTurn},
    tool::ToolRegistry,
};

pub(crate) struct Fixture {
    pub path: AgentPath,
    pub binding: VolumeBinding,
    pub provider: Arc<Observed>,
    scheduler: Arc<Observed>,
    filesystem: Arc<
        FilesystemHost<acyclic_fs::StreamAuthorityStore<Observed>, crate::conformance::ObjectStore>,
    >,
    _storage: Box<dyn crate::conformance::Storage>,
}

impl Fixture {
    pub async fn new(backend: &dyn Backend) -> Result<Self> {
        let storage = backend.fresh().await?;
        let raw = storage.open().await?;
        let scheduler = Arc::new(Observed::new(raw.clone()));
        let provider = Arc::new(Observed::new(raw));
        let agent = crate::conformance::fixtures::adopted().agent;
        let binding = crate::conformance::roots::volume(PRIVATE_VOLUME, agent.clone())?;
        let objects = storage.objects().await?;
        let capabilities = objects.capabilities;
        let executors = StockExecutors::new(
            (
                Model::new("conformance".into(), "scripted".into(), "1".into())?,
                Arc::new(ScriptedModelProvider::new([ScriptedTurn::Text(
                    "history".into(),
                )])),
            ),
            ToolRegistry::default(),
            (Limits::default(), CompactionPolicy::Disabled),
            Arc::new(Grants(Vec::new())),
        );
        let bound = Bound::local_root(
            AuthorityRoot::new(
                "conformance",
                [7; 32],
                Authority {
                    kind: AggregateKind::Conversation,
                    id: "workload".into(),
                },
            ),
            Placement {
                filesystem: acyclic_fs::Fs::new(
                    acyclic_fs::StreamAuthorityStore::new(scheduler.clone()),
                    objects.into_store(storage.as_ref())?,
                    capabilities,
                ),
                provider: binding.volume.provider().clone(),
                within: acyclic_actors::addressing::ActorReference::new(
                    b"conformance",
                    b"agents",
                    b"host",
                )?,
            },
            Turns {
                executors: Arc::new(executors),
                limits: TurnLimits {
                    maximum_bytes: 64 * 1024,
                    history_turns: 8,
                },
                actors: vectors::limits()?,
                clock: Arc::new(acyclic_actors::local::SystemClock),
                approval_policy: Default::default(),
            },
        )?;
        let handler = backend.handler(bound.handler())?;
        let bound = bound.hosting(handler)?;
        let RootSubmission::Created(path) = bound
            .create_root(
                &vectors::intent("create")?,
                RootRequest {
                    agent,
                    fresh: vec![binding.clone()],
                    retained: Vec::new(),
                    brief: None,
                    policy: None,
                },
            )
            .await?
        else {
            return Err("admission fixture root creation was uncertain".into());
        };
        let sender = crate::conformance::fixtures::sender();
        let id = vectors::intent("history")?;
        let body = bound
            .stage_text(&path, "message", &sender, &id, ("history", "history"))
            .await?;
        bound
            .agents()
            .send(
                &path,
                &sender,
                &InboxRecord::Message(Message {
                    from: sender.clone(),
                    id,
                    body: Box::new(body),
                    attachments: None,
                }),
            )
            .await?;
        // Seed the same indexed history and filesystem checkpoint a deployed
        // turn commits, rather than arbitrary bytes in its conversation stream.
        vectors::drain(bound.agents()).await?;
        let state = bound.agents().state(&path).await?;
        if !matches!(state.phase, Phase::Idle) || state.conversation_tail == 0 {
            return Err(
                format!("admission fixture did not commit its initial turn: {state:?}").into(),
            );
        }
        let filesystem = bound.volumes().clone();
        drop(bound);
        Ok(Self {
            path,
            binding,
            provider,
            scheduler,
            filesystem,
            _storage: storage,
        })
    }

    pub fn no_turn_io(&self) -> bool {
        self.provider.requests.requests().data() == 0
            && self.provider.requests.requests().control() == 0
    }

    pub async fn check(
        &self,
        backend: &dyn Backend,
        root: AuthorityRoot,
        admission: TurnAdmission,
        allowed: bool,
        label: &str,
        expected: &str,
    ) -> Result {
        let executors = Arc::new(Executors(AtomicUsize::new(0)));
        let driver = ExecutorDriver::new(FilesystemTurnHost::new(
            self.provider.clone(),
            acyclic_actors::addressing::ActorReference::new(b"conformance", b"agents", b"host")?,
            self.filesystem.clone(),
            root,
            admission,
            executors.clone(),
            TurnLimits {
                maximum_bytes: 64 * 1024,
                history_turns: 8,
            },
            Default::default(),
        ));
        let handler = backend.handler(Arc::new(AgentHandler::new(
            actor_store(&self.scheduler)?,
            driver,
            vectors::limits()?,
        )?))?;
        let host = vectors::hosting(self.scheduler.clone(), handler, None)?;
        host.host(&self.path)?;
        host.send(
            &self.path,
            &crate::conformance::fixtures::sender(),
            &crate::conformance::fixtures::request("next")?,
        )
        .await?;
        vectors::drain(&host).await?;
        // Decided turn failures settle through the original agent commit.
        // Runtime errors represent unsettled deliveries, not admission refusals.
        let Phase::Done {
            outcome: AgentOutcome::Failed { message },
        } = host.state(&self.path).await?.phase
        else {
            return Err(format!("{label} did not publish its terminal failure").into());
        };
        if message != expected {
            return Err(format!("{label}: unexpected terminal failure: {message}").into());
        }
        if allowed && self.provider.history_reads.load(Ordering::SeqCst) == 0 {
            return Err(format!("{label}: valid admission did not read nonempty history").into());
        }
        if !allowed && (!self.no_turn_io() || executors.0.load(Ordering::SeqCst) != 0) {
            return Err(
                format!("{label}: refusal performed turn IO or invoked its executor").into(),
            );
        }
        Ok(())
    }
}
