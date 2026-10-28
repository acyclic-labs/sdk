//! The wired path: stock executor and a filesystem journal on the agent's private volume.

use super::{Result, adopted, id, intent};
use acyclic_fs::{Fs, MemoryAuthorityBackend, MemoryObjectBackend};
use acyclic_harness_core::{
    OperationId,
    agent::*,
    context::{CompactionPolicy, ContextPipeline},
    contract::{Capabilities, capability::INTERACTION_ROUTE},
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityRoot},
    executor::Executor,
    executor::StockExecutor,
    filesystem::{FilesystemHost, FilesystemTurnHost, PRIVATE_VOLUME, TurnLimits},
    filesystem::{TurnAdmission, TurnAuthority, TurnExecutors},
    model::{Model, ModelContent, ModelRole},
    resources::ProviderRef,
    test_support::{ScriptedModelProvider, ScriptedTurn},
    tool::ToolRegistry,
};
use acyclic_stream::{
    AppendOutcome, AppendRequest, BoxProviderFuture, CommitCondition, CommitId, CommitMutation,
    CommitOutcome, CommitRequest, CommittedEnvelope, ForkReceipt, ForkRequest,
    IdempotencyKey, IdempotencyObservation, MemoryStream, ReadRequest, RecordStream,
    StreamError, StreamPath, StreamProvider,
};
use std::sync::Arc;

const LIMITS: TurnLimits = TurnLimits {
    maximum_bytes: 64 * 1024,
    max_steps: 4,
    history_turns: 8,
};

/// A memory filesystem host with agent `path`'s private volume.
type Host = Arc<FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>>;

/// The private-volume binding of agent `path`, and its filesystem host.
async fn private(path: &AgentPath) -> Result<(Host, VolumeBinding)> {
    let provider = ProviderRef::new("agents".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::<MemoryAuthorityBackend, MemoryObjectBackend>::new(
        Fs::memory(),
        &provider,
        None,
    )?;
    let owner = acyclic_harness_core::AgentId::from_bytes(path.id().to_bytes());
    let volume = VolumeRef::new(
        provider,
        "agent-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    Ok((
        host,
        VolumeBinding {
            name: PRIVATE_VOLUME.into(),
            volume,
        },
    ))
}

/// The stock loop over a scripted model, built once per turn.
struct Stock {
    selection: Model,
    model: Arc<ScriptedModelProvider>,
}

impl TurnExecutors for Stock {
    fn executor<'a>(
        &'a self,
        _: &'a AgentState,
        _: OperationId,
        _: &'a ModelContent,
        _: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<Arc<dyn Executor>>> {
        Box::pin(async move {
            Ok(Arc::new(
                StockExecutor::new(
                    self.selection.clone(),
                    self.model.clone(),
                    ContextPipeline::default(),
                    ToolRegistry::default(),
                )
                .with_compaction_policy(CompactionPolicy::Disabled),
            ) as Arc<dyn Executor>)
        })
    }
}

fn executors(model: Arc<ScriptedModelProvider>) -> Result<Arc<dyn TurnExecutors>> {
    let selection = Model::new(
        "test".into(),
        "text".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    Ok(Arc::new(Stock { selection, model }))
}

fn requested(id: &str, child: &AgentPath) -> Wake {
    Wake::Inbox {
        sequence: 1,
        record: InboxRecord::ApprovalRequested(ApprovalRequested {
            child: child.clone(),
            id: intent(id),
        }),
    }
}

#[tokio::test]
async fn a_child_turn_runs_the_stock_executor_and_replays_without_redispatch() -> Result {
    let parent = AgentPath::root(&id(1));
    let path = parent.child(&id(2))?;
    let model = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Text(
        "done".into(),
    )]));
    let (host, binding) = private(&path).await?;
    let driver = ExecutorDriver::new(FilesystemTurnHost::new(
        Arc::new(MemoryStream::default()),
        host,
        AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "agents".into(),
            },
        ),
        TurnAdmission::LocalRoot,
        executors(model.clone())?,
        LIMITS,
        Default::default(),
    ));
    let mut adoption = adopted(id(2));
    adoption.parent = Some(parent.clone());
    adoption.volumes = vec![binding];
    let state = AgentState::adopted(path.clone(), &adoption);
    let wake = || requested("r1", &parent);
    let first = step(
        &driver,
        &path,
        Some(state.clone()),
        2,
        wake(),
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?;
    let Phase::Done {
        outcome: AgentOutcome::Succeeded { result: Some(_) },
    } = &first.state.phase
    else {
        return Err(format!("unexpected phase {:?}", first.state.phase).into());
    };
    let entries: Vec<_> = first
        .outs
        .iter()
        .filter_map(|out| match out {
            Out::Conversation(bytes) => Some(ConversationEntry::decode(bytes)),
            _ => None,
        })
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(entries.len(), 1);
    // A crash before the step commit re-runs the step; the journal replays it.
    let again = step(
        &driver,
        &path,
        Some(state),
        2,
        wake(),
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?;
    assert_eq!(again.outs, first.outs);
    assert_eq!(model.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn a_later_turn_sees_the_prompt_and_settled_conversation_and_nothing_else() -> Result {
    let path = AgentPath::root(&id(3));
    let stream = Arc::new(MemoryStream::default());
    let model = Arc::new(ScriptedModelProvider::new([
        ScriptedTurn::Text("one".into()),
        ScriptedTurn::Text("two".into()),
    ]));
    let (host, binding) = private(&path).await?;
    let driver = ExecutorDriver::new(
        FilesystemTurnHost::new(
            stream.clone(),
            host,
            AuthorityRoot::new(
                "agents",
                [7; 32],
                Authority {
                    kind: AggregateKind::Conversation,
                    id: "agents".into(),
                },
            ),
            TurnAdmission::LocalRoot,
            executors(model.clone())?,
            LIMITS,
            Default::default(),
        )
        .with_prompt(Some("You write code.")),
    );
    let mut adoption = adopted(id(3));
    adoption.volumes = vec![binding];
    let state = AgentState::adopted(path.clone(), &adoption);
    // Settling the conversation creates the subtree; its inbox stays empty,
    // so these directly driven turns retain cursor 0.
    let first = step(
        &driver,
        &path,
        Some(state),
        0,
        requested("r1", &path),
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?;
    // Settle the first turn's conversation record as its step commit would.
    let records = first
        .outs
        .iter()
        .filter_map(|out| match out {
            Out::Conversation(bytes) => Some(bytes.clone().into()),
            _ => None,
        })
        .collect();
    stream
        .commit(CommitRequest {
            deadline_unix_millis: None,
            conditions: vec![CommitCondition::Absent {
                path: path.conversation()?,
            }],
            mutations: vec![CommitMutation::Append {
                path: path.conversation()?,
                records,
            }],
            idempotency_key: intent_key("settle", &[path.as_str()])?,
        })
        .await?;
    assert_eq!(first.state.conversation_tail, 1);
    step(
        &driver,
        &path,
        Some(first.state),
        0,
        requested("r2", &path),
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?;
    let requests = model.requests();
    assert_eq!(requests.len(), 2);
    let roles = |index: usize| -> Vec<ModelRole> {
        requests[index]
            .messages
            .iter()
            .map(|message| message.role)
            .collect()
    };
    let last = |index: usize| {
        roles(index)
            .into_iter()
            .filter(|r| *r != ModelRole::System)
            .count()
    };
    assert_eq!(last(1), last(0) + 2);
    let assistant = requests[1]
        .messages
        .iter()
        .find(|message| message.role == ModelRole::Assistant)
        .ok_or("no assistant history")?;
    assert_eq!(assistant.content, ModelContent::Text("one".into()));
    // The host's prompt opens every turn's context, with or without history.
    for request in &requests {
        let first = request.messages.first().ok_or("empty request")?;
        assert_eq!(first.role, ModelRole::System);
        assert_eq!(first.content, ModelContent::Text("You write code.".into()));
    }
    Ok(())
}

/// A spawned child is a clone of its parent at the fork cut: its first model
/// request holds the parent's settled turn, whose output the parent staged on
/// its own private volume.
#[tokio::test]
async fn a_spawned_childs_first_request_holds_its_parents_conversation() -> Result {
    let parent = AgentPath::root(&id(5));
    let child = parent.child(&id(6))?;
    let stream = Arc::new(MemoryStream::default());
    let model = Arc::new(ScriptedModelProvider::new([
        ScriptedTurn::Text("parent answer".into()),
        ScriptedTurn::Text("child answer".into()),
    ]));
    let (host, binding) = private(&parent).await?;
    let own = VolumeRef::new(
        binding.volume.provider().clone(),
        "agent-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(acyclic_harness_core::AgentId::from_bytes(
            child.id().to_bytes(),
        )),
        &mut None,
    )?;
    host.create_volume(&own).await?;
    let driver = ExecutorDriver::new(FilesystemTurnHost::new(
        stream.clone(),
        host,
        AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "agents".into(),
            },
        ),
        TurnAdmission::LocalRoot,
        executors(model.clone())?,
        LIMITS,
        Default::default(),
    ));
    let mut adoption = adopted(id(5));
    adoption.volumes = vec![binding];
    let state = AgentState::adopted(parent.clone(), &adoption);
    let first = step(
        &driver,
        &parent,
        Some(state),
        0,
        requested("p1", &parent),
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?;
    let entry: Vec<_> = first
        .outs
        .iter()
        .filter_map(|out| match out {
            Out::Conversation(bytes) => Some(bytes.clone()),
            _ => None,
        })
        .collect();
    // The parent's settled turn, and the child's conversation as the spawn
    // fork leaves it: the same entry, inherited at the cut.
    for path in [parent.conversation()?, child.conversation()?] {
        stream
            .commit(CommitRequest {
                conditions: vec![CommitCondition::Absent { path: path.clone() }],
                mutations: vec![CommitMutation::Append {
                    path: path.clone(),
                    records: entry.iter().cloned().map(Into::into).collect(),
                }],
                idempotency_key: intent_key("settle", &[path.as_str()])?,
            })
            .await?;
    }
    let forked = ForkAdopted {
        agent: id(6),
        parent: Some(parent.clone()),
        forked_at: 0,
        conversation_tail: 1,
        volumes: vec![VolumeBinding {
            name: PRIVATE_VOLUME.into(),
            volume: own,
        }],
        brief: None,
        policy: None,
    };
    let state = AgentState::adopted(child.clone(), &forked);
    step(
        &driver,
        &child,
        Some(state),
        0,
        requested("c1", &child),
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?;
    let requests = model.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[1]
            .messages
            .iter()
            .any(|message| message.role == ModelRole::Assistant
                && message.content == ModelContent::Text("parent answer".into())),
        "the child's first request lacks its parent's settled turn: {:?}",
        requests[1].messages
    );
    Ok(())
}

#[tokio::test]
async fn an_admitted_turn_runs_within_its_admission_and_a_read_only_one_cannot_write() -> Result {
    let parent = AgentPath::root(&id(4));
    let path = parent.child(&id(5))?;
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: "workload".into(),
    };
    let root = AuthorityRoot::new("agents", [7; 32], audience.clone());
    let run = |operations: &'static [VolumeOperation]| {
        let (parent, path, audience, root) =
            (parent.clone(), path.clone(), audience.clone(), root.clone());
        async move {
            let model = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Text(
                "done".into(),
            )]));
            let (host, binding) = private(&path).await?;
            let mut grants = vec![INTERACTION_ROUTE.to_owned()];
            for operation in operations {
                grants.push(binding.volume.capability(*operation, &mut None)?);
            }
            let scope =
                root.bootstrap_scope("admitted", &Capabilities::new(grants), None, &mut None)?;
            let driver = ExecutorDriver::new(FilesystemTurnHost::new(
                Arc::new(MemoryStream::default()),
                host,
                root,
                TurnAdmission::Admitted { audience, scope },
                executors(model.clone())?,
                LIMITS,
                Default::default(),
            ));
            let mut adoption = adopted(id(5));
            adoption.parent = Some(parent.clone());
            adoption.volumes = vec![binding];
            let state = AgentState::adopted(path.clone(), &adoption);
            let stepped = step(
                &driver,
                &path,
                Some(state),
                2,
                requested("r1", &parent),
                acyclic_actors::guest::CancellationToken::new(),
            )
            .await;
            Ok::<_, Box<dyn std::error::Error>>((
                stepped.map(|stepped| stepped.state.phase),
                model.requests().len(),
            ))
        }
    };
    let (admitted, calls) = run(&[VolumeOperation::Read, VolumeOperation::Write]).await?;
    assert!(
        matches!(
            admitted?,
            Phase::Done {
                outcome: AgentOutcome::Succeeded { .. }
            }
        ),
        "a read-write admission runs the turn"
    );
    assert_eq!(calls, 1);
    let (read_only, calls) = run(&[VolumeOperation::Read]).await?;
    assert!(read_only.is_err(), "a read-only admission cannot write");
    assert_eq!(calls, 0, "refused before the model runs");
    Ok(())
}

/// A stream that refuses, and counts, every call.
#[derive(Default)]
pub(super) struct Refusing {
    claims: acyclic_stream::OperationClaims,
    pub(super) calls: std::sync::atomic::AtomicUsize,
}

impl Refusing {
    fn refuse<T>(&self) -> std::result::Result<T, StreamError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(StreamError::Unsupported)
    }
}

#[async_trait::async_trait]
impl StreamProvider for Refusing {
    async fn head(
        &self,
        _: StreamPath,
    ) -> std::result::Result<acyclic_stream::PathHead, StreamError> {
        self.refuse()
    }

    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        &self.claims
    }
    async fn inspect_idempotency(
        &self,
        _: IdempotencyKey,
    ) -> std::result::Result<Option<IdempotencyObservation>, StreamError> {
        self.refuse()
    }
    async fn tail(&self, _: StreamPath) -> std::result::Result<u64, StreamError> {
        self.refuse()
    }
    async fn append(&self, _: AppendRequest) -> std::result::Result<AppendOutcome, StreamError> {
        self.refuse()
    }
    async fn fork(&self, _: ForkRequest) -> std::result::Result<ForkReceipt, StreamError> {
        self.refuse()
    }
    async fn read(&self, _: ReadRequest) -> std::result::Result<RecordStream, StreamError> {
        self.refuse()
    }
    async fn follow(
        &self,
        _: acyclic_stream::FollowRequest,
    ) -> std::result::Result<acyclic_stream::FollowStream, StreamError> {
        self.refuse()
    }
    async fn commit(&self, _: CommitRequest) -> std::result::Result<CommitOutcome, StreamError> {
        self.refuse()
    }
    async fn read_commit(
        &self,
        _: CommitId,
    ) -> std::result::Result<CommittedEnvelope, StreamError> {
        self.refuse()
    }
}

/// An admission's private-volume grant: exact volume labels, or one
/// structured `filesystem:` grant over the whole volume.
#[derive(Clone, Copy)]
enum Grant {
    Exact(&'static [VolumeOperation]),
    Structured(acyclic_fs::access::ActionSet),
}

#[tokio::test]
async fn a_refused_admission_reads_no_history_and_has_no_effect() -> Result {
    let parent = AgentPath::root(&id(6));
    let path = parent.child(&id(7))?;
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: "workload".into(),
    };
    let other = Authority {
        kind: AggregateKind::Conversation,
        id: "other".into(),
    };
    let stranger = acyclic_harness_core::AgentId::from_bytes(id(8).to_bytes());
    // Each case: the audience the admission is issued for, the one the host
    // is given, its bound agent, and its private-volume grant.
    let run = |issued: Authority,
               given: Authority,
               agent: Option<acyclic_harness_core::AgentId>,
               grant: Grant| {
        let (parent, path) = (parent.clone(), path.clone());
        async move {
            let root = AuthorityRoot::new("agents", [7; 32], issued);
            let model = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Text(
                "done".into(),
            )]));
            let (host, binding) = private(&path).await?;
            let mut grants = vec![INTERACTION_ROUTE.to_owned()];
            match grant {
                Grant::Exact(operations) => {
                    for operation in operations {
                        grants.push(binding.volume.capability(*operation, &mut None)?);
                    }
                }
                Grant::Structured(actions) => grants.push(
                    acyclic_fs::access::Capability::whole_volume(None, actions.actions())
                        .to_scope_label()?,
                ),
            }
            let scope =
                root.bootstrap_scope("admitted", &Capabilities::new(grants), agent, &mut None)?;
            let stream = Arc::new(Refusing::default());
            let driver = ExecutorDriver::new(FilesystemTurnHost::new(
                stream.clone(),
                host,
                root,
                TurnAdmission::Admitted {
                    audience: given,
                    scope,
                },
                executors(model.clone())?,
                LIMITS,
                Default::default(),
            ));
            let mut adoption = adopted(id(7));
            adoption.parent = Some(parent.clone());
            adoption.volumes = vec![binding];
            let mut state = AgentState::adopted(path.clone(), &adoption);
            // Settled history the turn would read first.
            state.conversation_tail = 3;
            let stepped = step(
                &driver,
                &path,
                Some(state),
                2,
                requested("r1", &parent),
                acyclic_actors::guest::CancellationToken::new(),
            )
            .await;
            assert!(stepped.is_err());
            assert_eq!(model.requests().len(), 0);
            Ok::<_, Box<dyn std::error::Error>>(
                stream.calls.load(std::sync::atomic::Ordering::SeqCst),
            )
        }
    };
    use acyclic_fs::access::ActionSet;
    let both = Grant::Exact(&[VolumeOperation::Read, VolumeOperation::Write]);
    // Controls: a valid exact or structured admission reaches the stream (and
    // fails there).
    for grant in [both, Grant::Structured(ActionSet::MountWrite)] {
        let reached = run(audience.clone(), audience.clone(), None, grant).await?;
        assert!(reached > 0, "a valid admission reads its history");
    }
    for (case, issued, given, agent, grant) in [
        (
            "read-only",
            audience.clone(),
            audience.clone(),
            None,
            Grant::Exact(&[VolumeOperation::Read]),
        ),
        (
            "structured read-only",
            audience.clone(),
            audience.clone(),
            None,
            Grant::Structured(ActionSet::MountRead),
        ),
        (
            "wrong audience",
            other.clone(),
            audience.clone(),
            None,
            both,
        ),
        (
            "other agent",
            audience.clone(),
            audience.clone(),
            Some(stranger),
            both,
        ),
    ] {
        let calls = run(issued, given, agent, grant).await?;
        assert_eq!(calls, 0, "{case}: refused before any stream IO");
    }
    Ok(())
}

/// Effects recorded in memory, once per call.
#[derive(Default)]
struct Recorded(std::sync::Mutex<std::collections::BTreeMap<String, TurnEffect>>);

impl TurnEffects for Recorded {
    fn record<'a>(
        &'a self,
        call: &'a str,
        effect: &'a TurnEffect,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<TurnEffect>> {
        let mut recorded = self.0.lock().expect("effects");
        let first = recorded
            .entry(call.to_owned())
            .or_insert_with(|| effect.clone());
        let first = first.clone();
        Box::pin(async move { Ok(first) })
    }

    fn recorded(&self) -> BoxProviderFuture<'_, acyclic_harness_core::Result<Vec<TurnEffect>>> {
        let recorded = self.0.lock().expect("effects").values().cloned().collect();
        Box::pin(async move { Ok(recorded) })
    }
}

/// A writer that names the agent's private volume; spawn stages nothing.
struct Writer(VolumeRef);

impl acyclic_harness_core::conversation::ContentPublisher for Writer {
    fn volume(&self) -> &VolumeRef {
        &self.0
    }
}

#[tokio::test]
async fn spawn_refuses_a_child_beyond_the_open_children_limit() -> Result {
    let path = AgentPath::root(&id(9));
    let (_, binding) = private(&path).await?;
    let mut state = AgentState::adopted(path.clone(), &adopted(id(9)));
    state.open_children.insert(path.child(&id(10))?);
    // Each spawn call in order, under `limit`: which ones the tool refused.
    let refused = |state: AgentState, limit: Option<u32>| {
        let volume = binding.volume.clone();
        async move {
            let effects = Arc::new(Recorded::default());
            let mut tools = ToolRegistry::default();
            acyclic_harness_core::tools::bind_agent_tools(
                &mut tools,
                &state,
                effects.clone(),
                Arc::new(Writer(volume)),
                1024,
                (&acyclic_harness_core::tools::default_names(), limit),
            )?;
            let spawn = tools.get("spawn", None, None)?.ok_or("no spawn tool")?;
            let mut refused = Vec::new();
            for (call, name) in [(1, "b"), (2, "b"), (3, "c")] {
                let result = spawn
                    .executor
                    .execute(
                        acyclic_harness_core::tool::ToolInvocation {
                            operation_id: OperationId::from_bytes([call; 16]),
                            call_id: format!("call-{call}"),
                            name: "spawn".into(),
                            arguments: serde_json::json!({"task": "work", "name": name}),
                        },
                        None,
                    )
                    .await?;
                refused.push(result.value.get("error").is_some());
            }
            Ok::<_, Box<dyn std::error::Error>>((refused, effects.recorded().await?.len()))
        }
    };
    // One open child and a limit of two: `b` fits (twice, as one child), `c` does not.
    assert_eq!(
        refused(state.clone(), Some(2)).await?,
        (vec![false, false, true], 2)
    );
    // Control: without a configured limit every spawn is recorded.
    assert_eq!(
        refused(state.clone(), None).await?,
        (vec![false, false, false], 3)
    );
    // Without one the agent's hard limit still holds: one below it, `b` fits
    // and `c` is refused to the model rather than dropped by the step.
    let mut full = state;
    for n in 0..MAX_OPEN_CHILDREN - 2 {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&(n as u64 + 1_000).to_le_bytes());
        full.open_children
            .insert(path.child(&AgentId::from_bytes(bytes))?);
    }
    assert_eq!(full.open_children.len(), MAX_OPEN_CHILDREN - 1);
    assert_eq!(refused(full, None).await?, (vec![false, false, true], 2));
    Ok(())
}
