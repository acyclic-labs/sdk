use super::*;
use acyclic_harness::{
    AgentId,
    context::CompactionPolicy,
    filesystem::MemoryHarnessStorage,
    model::{Model, ModelEvent, ToolCall},
    test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
};
use serde_json::{Value, json};
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    name: String,
    seed: Option<String>,
    prompt: String,
    writes: Vec<String>,
    status: GoalStatus,
    markdown: String,
}

#[tokio::test]
async fn ordinary_goal_file_scenarios() -> Result<()> {
    let cases: Vec<Case> = serde_json::from_str(include_str!("../goal_files.json"))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    futures::future::try_join_all(cases.into_iter().map(run)).await?;
    Ok(())
}

async fn run(case: Case) -> Result<()> {
    let storage = MemoryHarnessStorage::new(
        AgentId::new(),
        acyclic_harness::conversation::Limits::default().file_bytes,
    )
    .await?;
    let seed = match &case.seed {
        Some(seed) => Some(
            storage
                .stage(
                    OperationId::new(),
                    "GOAL.md",
                    seed.as_bytes(),
                    "text/markdown",
                    "GOAL.md",
                )
                .await?,
        ),
        None => None,
    };
    let mut turns = Vec::new();
    for (index, text) in case.writes.iter().enumerate() {
        turns.push(ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                format!("write-{index}"),
                "write".into(),
                json!({
                    "path":"GOAL.md", "text":text,
                    "media_type":"text/markdown", "display_name":"GOAL.md",
                }),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: Value::Null,
            }),
        ]));
    }
    turns.push(ScriptedTurn::Text("Settled".into()));
    let provider = Arc::new(ScriptedModelProvider::new(turns));
    let bundle = storage
        .builder()
        .model(
            Model::new("scripted".into(), "goal".into(), "1".into(), Value::Null)?,
            provider.clone(),
        )
        .grant("model:generate")
        .compaction(CompactionPolicy::Disabled)
        .build()?;
    let output = storage
        .run_prompt(&bundle, &case.prompt, Vec::new())
        .await?;
    assert_eq!(output.text, "Settled", "{}", case.name);
    assert_eq!(provider.remaining(), 0, "{}", case.name);
    assert_eq!(
        provider.requests().len(),
        case.writes.len() + 1,
        "{}",
        case.name
    );
    let (source, bytes) = storage
        .read_delegated_path(storage.owner_scope(), "", "GOAL.md", None)
        .await?;
    let view = GoalView::from_file(source, &bytes, 4096)?;
    assert_eq!(view.status, case.status, "{}", case.name);
    assert_eq!(view.markdown, case.markdown, "{}", case.name);
    let expected_file = case
        .writes
        .last()
        .or(case.seed.as_ref())
        .ok_or_else(|| Error::Invalid("scenario has no goal".into()))?;
    assert_eq!(bytes.as_ref(), expected_file.as_bytes(), "{}", case.name);
    if let Some(seed) = seed {
        assert_eq!(
            storage.read(&seed).await?.as_ref(),
            case.seed.as_deref().unwrap_or_default().as_bytes()
        );
    }
    Ok(())
}
