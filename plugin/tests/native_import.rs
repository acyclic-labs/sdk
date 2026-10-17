//! Native hooks import selected published child files through the fenced fs tool.
#[path = "support/native_hooks.rs"]
mod native_hooks;

use futures::FutureExt as _;
use native_hooks::*;
use serde_json::json;

#[tokio::test]
async fn native_child_shell_and_selected_import_preserve_the_parent_checkout() -> Result {
    let fixture = Fixture::new()?;
    let cwd = fixture.path();
    std::fs::write(cwd.join("preserved.txt"), "parent contents")?;
    let owner = fixture.start_owner().await?;
    let result = std::panic::AssertUnwindSafe(async {
        hook(cwd, "pi", "session_start", json!({"sessionId":"import","cwd":cwd})).await?;
        let spawned = hook(cwd, "pi", "subagent_start", json!({"sessionId":"import","agentId":"child"})).await?;
        let child = spawned["Succeeded"].as_str().ok_or("missing child identity")?;
        // Native subagent hooks send SubagentStart followed directly by tools;
        // they need no additional child SessionStart before their first effect.
        let shell = hook(cwd, "pi", "tool_call", json!({
            "sessionId":"import","agentId":"child","toolCallId":"shell",
            "toolName":"bash","input":{"command":"printf selected > selected.txt; printf unselected > unselected.txt; cat preserved.txt"},
        })).await?;
        assert_eq!(shell["isError"], false, "{shell}");
        assert!(shell.to_string().contains("parent contents"), "{shell}");
        assert!(!cwd.join("selected.txt").exists());
        assert!(!cwd.join("unselected.txt").exists());
        assert_eq!(std::fs::read_to_string(cwd.join("preserved.txt"))?, "parent contents");
        let input = json!({"sessionId":"import","toolCallId":"selected-import",
            "toolName":"fs","input":{"command":format!("restore --source agents/{child} -- selected.txt")}});
        let imported = hook(cwd, "pi", "tool_call", input.clone()).await?;
        assert_eq!(imported["isError"], false, "{imported}");
        assert_eq!(std::fs::read_to_string(cwd.join("selected.txt"))?, "selected");
        assert!(!cwd.join("unselected.txt").exists());
        assert_eq!(std::fs::read_to_string(cwd.join("preserved.txt"))?, "parent contents");
        let modified = std::fs::metadata(cwd.join("selected.txt"))?.modified()?;
        assert_eq!(hook(cwd, "pi", "tool_call", input).await?, imported);
        assert_eq!(std::fs::metadata(cwd.join("selected.txt"))?.modified()?, modified);
        Ok::<(), Box<dyn std::error::Error>>(())
    }).catch_unwind().await;
    let closed = fixture.close_owner(owner).await;
    match result {
        Ok(result) => {
            result?;
            closed
        }
        Err(panic) => {
            closed?;
            std::panic::resume_unwind(panic)
        }
    }
}
