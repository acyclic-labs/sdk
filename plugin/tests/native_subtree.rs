//! Subtree cancellation through the shared hook handler and task files.
#[path = "support/native_actions.rs"]
mod native_actions;
#[path = "support/native_hooks.rs"]
mod native_hooks;
use native_actions::Tools;
use native_hooks::{Fixture, Result};
use serde_json::json;

#[tokio::test]
async fn hooks_cancel_only_the_selected_child_subtree() -> Result {
    let fixture = Fixture::new()?;
    let owner = fixture.start_owner().await?;
    let result = async {
        let [codex, claude, pi] = Tools::hosts(fixture.path()).await?;
        tokio::try_join!(subtree(codex), subtree(claude), subtree(pi))
    }
    .await;
    fixture.close_owner(owner).await?;
    result?;
    Ok(())
}

async fn subtree(parent: Tools<'_>) -> Result {
    let child = parent.child("branch").await?;
    let leaf = child.child("leaf").await?;
    let sibling = parent.child("sibling").await?;
    let args = json!({"child":child.task});
    let cancelled = parent
        .call("cancel", "acyclic_cancel", args.clone())
        .await?;
    assert_eq!(
        parent.call("cancel", "acyclic_cancel", args).await?,
        cancelled
    );
    for task in [child.task, leaf.task] {
        let inspected = parent
            .call(
                &format!("inspect-{task}"),
                "acyclic_inspect",
                json!({"task":task}),
            )
            .await?;
        assert_eq!(inspected["outcome"], "Cancelled", "{inspected}");
    }
    let inspected = parent
        .call("sibling", "acyclic_inspect", json!({"task":sibling.task}))
        .await?;
    assert!(inspected["outcome"].is_null(), "{inspected}");
    let inspected = parent
        .call("parent", "acyclic_inspect", json!({"task":parent.task}))
        .await?;
    assert!(inspected["outcome"].is_null(), "{inspected}");
    // A sibling is not a direct child of this child. Denial must be terminal,
    // and the rejected selection cannot cancel a different task.
    sibling
        .reject("not-my-child", "acyclic_wait", json!({"child":parent.task}))
        .await?;
    Ok(())
}
