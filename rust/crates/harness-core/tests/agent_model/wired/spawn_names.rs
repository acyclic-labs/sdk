//! Real effects, actor commits and bindings: names survive a setup failure.
use super::*;
use acyclic_harness_core::{
    filesystem::StockExecutors,
    model::{ModelEvent, ToolCall, ToolResultContent},
    test_support::ScriptedEvent,
};
use acyclic_stream::testing::{Counted, RequestClass};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};

fn call(tool: &str, arguments: serde_json::Value) -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
            tool.into(),
            tool.into(),
            arguments,
        ))),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({}),
        }),
    ])
}

fn spawn_call(name: &str, peers: &[&str]) -> ScriptedTurn {
    call(
        "spawn",
        json!({"name": name, "task": "Work on the assigned brief", "role": "worker", "peers": peers}),
    )
}

async fn fixture(
    turns: Vec<ScriptedTurn>,
    setup_failures: usize,
) -> Result<(
    AgentHost<MemoryStream>,
    Arc<MemoryStream>,
    AgentPath,
    Arc<ScriptedModelProvider>,
)> {
    let root = AgentPath::root(&id(81));
    let stream = Arc::new(MemoryStream::default());
    let model = Arc::new(ScriptedModelProvider::new(turns));
    let (files, binding) = private_in(&stream, &root).await?;
    files.seed_layer().await?;
    let limits = acyclic_harness_core::conversation::Limits::default();
    let harness = acyclic_harness_core::runtime::Bindings {
        scope: acyclic_harness_core::runtime::RuntimeScope::new(
            Capabilities::new([acyclic_harness_core::contract::capability::MODEL_GENERATE]),
            limits,
        )?,
        ..Default::default()
    }
    .build()?;
    let executors = StockExecutors::new(
        (
            Model::new("test".into(), "text".into(), "1".into())?,
            model.clone(),
        ),
        ToolRegistry::default(),
        (limits, CompactionPolicy::Disabled),
        Arc::new(Tasks(harness)),
    )
    .with_agent_tools(&[
        "spawn".into(),
        "send".into(),
        "wait".into(),
        "archive".into(),
    ])
    .with_templates();
    let remaining = AtomicUsize::new(setup_failures);
    let turns = FilesystemTurnHost::new(
        stream.clone(),
        within(),
        files.clone(),
        AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "agents".into(),
            },
        ),
        TurnAdmission::LocalRoot,
        Arc::new(executors),
        LIMITS,
        acyclic_harness_core::approvals::ApprovalPolicy::accept_all("test"),
    )
    .with_binding_fault(move |agent| {
        if agent.parent.is_some()
            && remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        {
            return Err(acyclic_harness_core::Error::Invalid(
                "injected child binding failure".into(),
            ));
        }
        Ok(())
    });
    let templates = turns.templates().ok_or("missing creation templates")?;
    let agents = committing(&stream, &files, ExecutorDriver::new(turns))?.with_templates(templates);
    let mut adoption = adopted(root.id());
    adoption.volumes = vec![binding];
    agents.create(&intent("root"), adoption).await?;
    Ok((agents, stream, root, model))
}

async fn named(
    agents: &AgentHost<MemoryStream>,
    parent: &AgentPath,
    name: &str,
) -> Result<AgentPath> {
    agents
        .resolve_spawn_name(parent, name)
        .await?
        .live()
        .ok_or_else(|| "missing named child".into())
}

#[tokio::test]
async fn atomic_spawn_retries_setup_and_preserves_references_and_sibling_grants() -> Result {
    let text = || ScriptedTurn::Text("ready".into());
    let (agents, stream, root, model) = fixture(
        vec![
            spawn_call("w", &[]),
            spawn_call("s", &["w"]),
            text(),
            spawn_call("w", &[]),
            text(),
            text(),
            call("wait", json!({"agents": ["w"]})),
            call("send", json!({"agent": "w", "message": "parent message"})),
            text(),
            text(),
            call("send", json!({"agent": "w", "message": "sibling message"})),
            text(),
            text(),
            call("archive", json!({"agent": "w"})),
            text(),
        ],
        1,
    )
    .await?;
    deliver(&agents, &root, requested("first", &root)).await?;
    let failed = named(&agents, &root, "w").await?;
    let sibling = named(&agents, &root, "s").await?;
    // This grant predates the failure and survives the sibling's new identity.
    assert!(
        agents
            .state(&sibling)
            .await?
            .profile
            .unwrap()
            .peers
            .contains("agent.send-sibling:w")
    );
    let counted = Arc::new(Counted::new((*stream).clone()));
    // Newly adopted and checkpointed live agents both cost one head, no data IO.
    for name in ["w", "absent"] {
        let before = counted.requests();
        resolve_spawn_name(&counted, &within(), &root, name).await?;
        let reads = counted.requests().since(before);
        assert_eq!(reads.class(RequestClass::Head), 1);
        assert_eq!(reads.control(), 1);
        assert_eq!(reads.data(), 0);
    }
    agents.drain(&failed).await?;
    let state = agents.state(&failed).await?;
    assert!(state.released());
    assert_eq!(state.conversation_tail, state.conversation_base);
    // Negative control: the old pure derivation still finds the occupied inbox
    // and its admission refuses the retry with exactly the old name-exists error.
    let old = TurnEffect::child(&root, &intent("spawn:w"))?;
    assert_eq!(old, failed);
    let old_admission = match inbox_tail(&*stream, &old).await {
        Ok(_) => Err(acyclic_harness_core::Error::Conflict(
            "an agent with this name exists".into(),
        )),
        Err(error) => Ok(error),
    };
    assert!(
        matches!(old_admission, Err(acyclic_harness_core::Error::Conflict(ref reason)) if reason == "an agent with this name exists")
    );
    // ChildDone wakes the parent, whose scripted next turn retries the same name.
    agents.drain(&root).await?;
    let retry = named(&agents, &root, "w").await?;
    assert_ne!(retry, failed);
    assert_eq!(
        agents.resolve_spawn_name(&root, "w").await?,
        SpawnResolution::Live(1, retry.clone())
    );
    agents.drain(&retry).await?;
    assert_eq!(agents.state(&retry).await?.phase, Phase::Idle);
    assert!(!agents.state(&retry).await?.released());
    deliver(&agents, &root, requested("references", &root)).await?;
    assert!(
        model
            .requests()
            .iter()
            .flat_map(|request| &request.messages)
            .flat_map(|message| message.content.parts())
            .any(|part| matches!(part,
            ModelContentPart::ToolResult { name, content: ToolResultContent::Json { value }, .. }
            if name == "wait" && value["settled"][0]["agent"] == retry.as_str()))
    );
    agents.drain(&retry).await?;
    agents.drain(&sibling).await?;
    agents.drain(&retry).await?;
    let messages = super::super::values(&*stream, retry.inbox()?)
        .await?
        .into_iter()
        .map(|bytes| InboxRecord::decode(&bytes))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert!(messages.iter().any(|record| matches!(record, InboxRecord::Message(message) if message.from == Sender::Agent { path: sibling.clone() })));
    let before = counted.requests();
    assert_eq!(
        resolve_spawn_name(&counted, &within(), &root, "s")
            .await?
            .live(),
        Some(sibling)
    );
    let reads = counted.requests().since(before);
    assert_eq!(reads.class(RequestClass::Head), 1);
    assert_eq!(reads.control(), 1);
    assert_eq!(reads.data(), 0);
    deliver(&agents, &root, requested("archive", &root)).await?;
    agents.drain(&retry).await?;
    assert_eq!(
        agents.state(&retry).await?.phase,
        Phase::Done {
            outcome: AgentOutcome::Archived
        }
    );
    assert_eq!(
        agents.resolve_spawn_name(&root, "w").await?.live(),
        Some(retry)
    );
    Ok(())
}

#[tokio::test]
async fn spawn_name_resolver_follows_a_six_deep_chain() -> Result {
    let mut turns = Vec::new();
    for _ in 0..6 {
        turns.extend([spawn_call("w", &[]), ScriptedTurn::Text("delegated".into())]);
    }
    turns.push(ScriptedTurn::Text("leaf".into()));
    let (agents, _, root, _) = fixture(turns, 0).await?;
    deliver(&agents, &root, requested("start", &root)).await?;
    let mut parent = root;
    for _ in 0..6 {
        let child = named(&agents, &parent, "w").await?;
        agents.drain(&child).await?;
        assert_eq!(agents.state(&child).await?.phase, Phase::Idle);
        parent = child;
    }
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(8))]
    #[test]
    fn resolver_never_skips_a_live_generation(failures in 0usize..4, fails_after_turn in proptest::bool::ANY) {
        let runtime = acyclic_native_runtime::testing::seeded_runtime(81).unwrap();
        runtime.block_on(async {
            let mut turns = Vec::new();
            for _ in 0..=failures {
                turns.extend([spawn_call("w", &[]), ScriptedTurn::Text("spawned".into())]);
            }
            turns.push(ScriptedTurn::Text("committed turn".into()));
            turns.push(ScriptedTurn::Events(vec![ScriptedEvent::Interrupted { error: "failure after work".into() }]));
            let (agents, stream, root, _) = fixture(turns, failures).await?;
            deliver(&agents, &root, requested("start", &root)).await?;
            for _ in 0..failures {
                let failed = named(&agents, &root, "w").await?;
                agents.drain(&failed).await?;
                assert!(agents.state(&failed).await?.released());
                agents.drain(&root).await?;
            }
            let live = named(&agents, &root, "w").await?;
            agents.drain(&live).await?;
            if fails_after_turn {
                deliver(&agents, &live, requested("fail-after-work", &live)).await?;
                assert!(matches!(agents.state(&live).await?.phase, Phase::Done { outcome: AgentOutcome::Failed { message } } if message.contains("failure after work")));
            }
            assert!(!agents.state(&live).await?.released());
            let counted = Arc::new(Counted::new((*stream).clone()));
            assert_eq!(resolve_spawn_name(&counted, &within(), &root, "w").await?, SpawnResolution::Live(u64::try_from(failures)?, live));
            assert_eq!(counted.requests().class(RequestClass::Head), u64::try_from(failures)? + 1);
            assert_eq!(counted.requests().data(), 0);
            Ok::<_, Box<dyn std::error::Error>>(())
        }).unwrap();
    }
}
