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
    BoxProviderFuture, CommitCondition, CommitMutation, CommitRequest, MemoryStream, StreamProvider,
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
    ));
    let mut adoption = adopted(id(2));
    adoption.parent = Some(parent.clone());
    adoption.volumes = vec![binding];
    let state = AgentState::adopted(path.clone(), &adoption);
    let wake = || requested("r1", &parent);
    let first = step(&driver, &path, Some(state.clone()), 2, wake()).await?;
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
    let again = step(&driver, &path, Some(state), 2, wake()).await?;
    assert_eq!(again.outs, first.outs);
    assert_eq!(model.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn a_later_turn_sees_the_settled_conversation_and_nothing_else() -> Result {
    let path = AgentPath::root(&id(3));
    let stream = Arc::new(MemoryStream::default());
    let model = Arc::new(ScriptedModelProvider::new([
        ScriptedTurn::Text("one".into()),
        ScriptedTurn::Text("two".into()),
    ]));
    let (host, binding) = private(&path).await?;
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
    ));
    let mut adoption = adopted(id(3));
    adoption.volumes = vec![binding];
    let state = AgentState::adopted(path.clone(), &adoption);
    // Settling the conversation creates the subtree; its inbox stays empty, so
    // each turn's cursor (where its stop follow starts) is 0.
    let first = step(&driver, &path, Some(state), 0, requested("r1", &path)).await?;
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
    step(&driver, &path, Some(first.state), 0, requested("r2", &path)).await?;
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
            ));
            let mut adoption = adopted(id(5));
            adoption.parent = Some(parent.clone());
            adoption.volumes = vec![binding];
            let state = AgentState::adopted(path.clone(), &adoption);
            let stepped = step(&driver, &path, Some(state), 2, requested("r1", &parent)).await;
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
