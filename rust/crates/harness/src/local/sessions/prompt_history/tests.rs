use super::*;
use crate::{
    LocalHarness,
    local::WorkspaceMounting,
    model::Model,
    test_support::{ScriptedModelProvider, ScriptedTurn, local_harness_config},
};
use std::sync::Arc;

#[tokio::test]
async fn prompt_history_reconnects_across_sessions_without_copying_inherited_prompts()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let workspace = directory.path().join("workspace");
    std::fs::create_dir(&workspace)?;
    let model = Arc::new(ScriptedModelProvider::new((0..5).flat_map(|_| {
        [
            ScriptedTurn::Text("answer".into()),
            ScriptedTurn::Text("none".into()),
        ]
    })));
    let local = Arc::new(
        LocalHarness::open(
            local_harness_config(
                &workspace,
                (
                    Model::new("scripted".into(), "test".into(), "1".into())?,
                    model,
                ),
            )?
            .without_user_settings()
            .state_directory(directory.path().join("state"))
            .workspace_mounting(WorkspaceMounting::Physical),
        )
        .await?,
    );
    let owner = local.connect_sessions().await?;
    let first = super::super::create(&owner, "first-history-session", None).await?;
    let first_view = Conversation::open(first.clone());
    first_view.send("first-prompt", "repeated prompt").await?;
    first_view.send("second-prompt", "repeated prompt").await?;
    first_view.wait_current_input().await?;
    let second = super::super::create(&owner, "other-history-session", None).await?;
    let second_view = Conversation::open(second);
    second_view
        .send("other-prompt", "other session prompt")
        .await?;
    second_view.wait_current_input().await?;
    let fork = super::super::fork(&owner, first.root().clone(), 0, "history-fork").await?;
    let fork_view = Conversation::open(fork);
    fork_view.send("fork-prompt", "fork own prompt").await?;

    let mut history = PromptHistory::new(first.clone());
    assert_eq!(history.next().await?.as_deref(), Some("repeated prompt"));
    first_view
        .send("later-prompt", "later selected prompt")
        .await?;
    assert_eq!(history.next().await?.as_deref(), Some("repeated prompt"));
    let mut other = Vec::new();
    while let Some(text) = history.next().await? {
        other.push(text);
    }
    other.sort();
    assert_eq!(other, ["fork own prompt", "other session prompt"]);
    let mut reconnected = PromptHistory::new(first);
    assert_eq!(
        reconnected.next().await?.as_deref(),
        Some("later selected prompt")
    );
    first_view.wait_current_input().await?;
    Ok(())
}
