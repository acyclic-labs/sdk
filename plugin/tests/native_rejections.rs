//! A definite rejection settles once without poisoning the task's file feed.
#[path = "support/native_hooks.rs"]
mod native_hooks;
use acyclic_harness::{
    external_executor::{ExternalHost, local_hook_volumes, request},
    task_volume::{Component, Query},
};
use futures::FutureExt as _;
use native_hooks::*;
use serde_json::{Value, json};
use std::{path::Path, sync::Arc};

async fn rejected_stop(cwd: &Path, input: &Value) -> Result<Value> {
    let output = start_hook(cwd, "pi", "subagent_stop", input.clone())
        .await?
        .wait_with_output()
        .await?;
    assert_eq!(output.status.code(), Some(2));
    let answer: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(
        answer["systemMessage"],
        acyclic_harness::Error::Invalid("native child has not been admitted".into()).to_string()
    );
    Ok(answer)
}

#[tokio::test]
async fn rejected_stop_keeps_consuming_and_cannot_replay_against_a_later_child() -> Result {
    let fixture = Fixture::new()?;
    let mut owner = Some(fixture.start_owner().await?);
    let result = std::panic::AssertUnwindSafe(async {
        let cwd = fixture.path();
        let session = "rejection-restart";
        hook(
            cwd,
            "pi",
            "session_start",
            json!({"sessionId":session,"cwd":cwd}),
        )
        .await?;
        // The eight-second operation budget starts at authenticated readiness.
        tokio::time::timeout(std::time::Duration::from_secs(8), async {
            let stop = json!({"sessionId":session,"agentId":"later-child"});
            let first = rejected_stop(cwd, &stop).await?;
            let write = |call: &str, path: &str| {
                json!({"sessionId":session,"toolCallId":call,
                "toolName":"write","input":{"path":path,"content":call}})
            };
            assert_eq!(
                hook(
                    cwd,
                    "pi",
                    "tool_call",
                    write("after-rejection", "parent.txt")
                )
                .await?["isError"],
                false
            );
            assert_eq!(
                std::fs::read_to_string(cwd.join("parent.txt"))?,
                "after-rejection"
            );
            let attached = request(
                ExternalHost::Pi,
                "session_start",
                json!({"sessionId":session,"cwd":cwd}),
            )?;
            let task = Component::new(attached.task_id().to_string())?;
            let rejected = request(ExternalHost::Pi, "subagent_stop", stop.clone())?;
            let (_, results) =
                local_hook_volumes(Arc::new(client(cwd).await?), attached.task_id()).await?;
            let key = rejected.result_key(task.clone())?;
            let stored = results.snapshot(&Query::File(key.clone()), 1).await?.files;
            assert_eq!(stored.len(), 1);
            assert_eq!(
                serde_json::from_slice::<Value>(&stored[0].bytes)?,
                json!({"Failed":{"message":"native child has not been admitted"}})
            );
            let fork = hook(cwd, "pi", "subagent_start", stop.clone()).await?;
            let child = fork["Succeeded"]
                .as_str()
                .ok_or("missing child admission")?;
            hook(
                cwd,
                "pi",
                "session_start",
                json!({"sessionId":session,"agentId":"later-child","cwd":cwd}),
            )
            .await?;
            let mut child_write = write("before-restart", "child-before.txt");
            child_write["agentId"] = json!("later-child");
            assert_eq!(
                hook(cwd, "pi", "tool_call", child_write).await?["isError"],
                false
            );
            let before = results.snapshot(&Query::Root, 64).await?.generation;
            fixture.close_owner(owner.take().unwrap()).await?;
            owner = Some(fixture.start_owner().await?);
            // No SessionStart can repair the registration or mask a replay.
            let (_, reopened) =
                local_hook_volumes(Arc::new(client(cwd).await?), attached.task_id()).await?;
            assert_eq!(rejected_stop(cwd, &stop).await?, first);
            assert_eq!(reopened.snapshot(&Query::File(key), 1).await?.files, stored);
            assert_eq!(
                reopened.snapshot(&Query::Root, 64).await?.generation,
                before
            );
            let mut child_write = write("after-rejected-replay", "child-after.txt");
            child_write["agentId"] = json!("later-child");
            assert_eq!(
                hook(cwd, "pi", "tool_call", child_write).await?["isError"],
                false
            );
            assert!(!cwd.join("child-before.txt").exists());
            assert!(!cwd.join("child-after.txt").exists());
            let inspected = hook(
                cwd,
                "pi",
                "tool_call",
                json!({"sessionId":session,"toolCallId":"inspect-after-replay",
                "toolName":"acyclic_inspect","input":{"task":child}}),
            )
            .await?;
            assert_eq!(inspected["isError"], false);
            let text = inspected["content"]
                .as_array()
                .ok_or("inspection has no content")?
                .iter()
                .filter_map(|part| part["text"].as_str())
                .collect::<String>();
            let inspected: Value = serde_json::from_str(&text)?;
            assert_eq!(inspected["task"], child);
            assert_eq!(
                inspected["admission"]["parent"],
                attached.task_id().to_string()
            );
            assert_eq!(
                hook(
                    cwd,
                    "pi",
                    "tool_call",
                    write("still-live", "parent-after.txt")
                )
                .await?["isError"],
                false
            );
            assert_eq!(
                std::fs::read_to_string(cwd.join("parent-after.txt"))?,
                "still-live"
            );
            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await?
    })
    .catch_unwind()
    .await;
    let stopped = if let Some(owner) = owner {
        fixture.close_owner(owner).await
    } else {
        Ok(())
    };
    if let Err(error) = &stopped {
        eprintln!("rejection fixture shutdown: {error}");
    }
    match result {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    stopped
}
