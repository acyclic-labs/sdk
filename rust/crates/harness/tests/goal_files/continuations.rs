use super::*;
use acyclic_harness::{
    AgentId, Harness,
    context::CompactionPolicy,
    filesystem::MemoryHarnessStorage,
    model::Model,
    test_support::{ScriptedModelProvider, ScriptedTurn},
};
use serde_json::Value;
use std::{collections::BTreeSet, sync::Arc};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    name: String,
    goal: String,
    #[serde(default)]
    policy: GoalContinuationPolicy,
    settles: u32,
    continuations: u32,
    status: GoalStatus,
    reason: Option<String>,
}

#[tokio::test]
async fn continuation_scenarios() -> Result<()> {
    let cases: Vec<Case> = serde_json::from_str(include_str!("../goal_continuations.json"))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    futures::future::try_join_all(cases.into_iter().map(run)).await?;
    Ok(())
}

async fn read_goal(storage: &MemoryHarnessStorage) -> Result<GoalView> {
    let (source, bytes) = storage
        .read_delegated_path(storage.owner_scope(), "", "GOAL.md", None)
        .await?;
    GoalView::from_file(source, &bytes, 4096)
}

async fn run_turn(
    storage: &MemoryHarnessStorage,
    bundle: &Harness,
    provider: &ScriptedModelProvider,
    operation: OperationId,
) -> Result<()> {
    let input = storage
        .stage(
            operation,
            &format!("turns/{operation}/prompt.txt"),
            b"Continue working toward GOAL.md.",
            "text/plain",
            "prompt.txt",
        )
        .await?;
    let output = storage
        .run_conversation(bundle, operation, input.clone(), Vec::new(), 1)
        .await?;
    assert_eq!(output.text, "Settled");
    let requests = provider.requests().len();
    let replay = storage
        .run_conversation(bundle, operation, input, Vec::new(), 1)
        .await?;
    assert_eq!(replay, output);
    assert_eq!(
        provider.requests().len(),
        requests,
        "replayed admission dispatched again"
    );
    Ok(())
}

async fn run(case: Case) -> Result<()> {
    let storage = MemoryHarnessStorage::new(
        AgentId::new(),
        acyclic_harness::conversation::Limits::default().file_bytes,
    )
    .await?;
    let seed = storage
        .stage(
            OperationId::new(),
            "GOAL.md",
            case.goal.as_bytes(),
            "text/markdown",
            "GOAL.md",
        )
        .await?;
    let original = read_goal(&storage).await?;
    let provider = Arc::new(ScriptedModelProvider::new(
        (0..=case.continuations).map(|_| ScriptedTurn::Text("Settled".into())),
    ));
    let bundle = storage
        .builder()
        .model(
            Model::new("scripted".into(), "goal".into(), "1".into(), Value::Null)?,
            provider.clone(),
        )
        .grant("model:generate")
        .compaction(CompactionPolicy::Disabled)
        .build()?;
    let task = TaskId::new();
    let mut settled_turn = OperationId::new();
    run_turn(&storage, &bundle, &provider, settled_turn).await?;
    let mut admitted = BTreeSet::new();
    let mut count = 0;
    let mut observed_settles = 0;
    for _ in 0..case.settles {
        observed_settles += 1;
        let view = read_goal(&storage).await?;
        assert_eq!(
            view.revision(),
            original.revision(),
            "{}: agent edited the goal",
            case.name
        );
        let action = case.policy.on_settle(&view, task, settled_turn, count);
        assert_eq!(
            action,
            case.policy.on_settle(&view, task, settled_turn, count)
        );
        match action {
            Some(GoalContinuation::Continue(operation)) => {
                assert!(
                    admitted.insert(operation),
                    "{}: repeated continuation identity",
                    case.name
                );
                run_turn(&storage, &bundle, &provider, operation).await?;
                count += 1;
                settled_turn = operation;
            }
            Some(GoalContinuation::Block(contents)) => {
                let operation = view
                    .continuation_operation(task, settled_turn)
                    .ok_or_else(|| Error::Invalid("missing cap write identity".into()))?;
                storage
                    .stage(
                        operation,
                        "GOAL.md",
                        contents.as_bytes(),
                        "text/markdown",
                        "GOAL.md",
                    )
                    .await?;
                let blocked = read_goal(&storage).await?;
                assert_eq!(
                    case.policy.on_settle(&blocked, task, settled_turn, count),
                    None
                );
                break;
            }
            None => break,
        }
    }
    let final_goal = read_goal(&storage).await?;
    assert_eq!(observed_settles, case.settles, "{}", case.name);
    assert_eq!(count, case.continuations, "{}", case.name);
    assert_eq!(final_goal.status, case.status, "{}", case.name);
    assert_eq!(final_goal.reason, case.reason, "{}", case.name);
    assert_eq!(final_goal.markdown, original.markdown, "{}", case.name);
    assert_eq!(provider.remaining(), 0, "{}", case.name);
    assert_eq!(
        provider.requests().len(),
        case.continuations as usize + 1,
        "{}",
        case.name
    );
    assert_eq!(storage.read(&seed).await?.as_ref(), case.goal.as_bytes());
    Ok(())
}
