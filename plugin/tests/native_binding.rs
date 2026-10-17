//! A native child cwd cannot select a different provider or checkout owner.
#[path = "support/native_hooks.rs"]
mod native_hooks;

use acyclic_harness::{
    external_executor::{ExternalHost, request},
    task_volume::{Component, Query},
};
use native_hooks::*;
use serde_json::{Value, json};
use std::path::Path;

#[tokio::test]
async fn separate_hook_working_views_retain_the_original_workspace_and_child_binding() -> Result {
    let fixture = Fixture::new()?;
    let owner = fixture.start_owner().await?;
    let result = async {
        let binding = fixture.path().parent().unwrap().join("binding.json");
        std::fs::write(
            &binding,
            serde_json::to_vec(&json!({
                "provider":"local","workspace":fixture.path(),
            }))?,
        )?;
        futures::future::try_join_all(
            [
                ExternalHost::Codex,
                ExternalHost::ClaudeCode,
                ExternalHost::Pi,
            ]
            .into_iter()
            .map(|host| case(fixture.path(), &binding, host)),
        )
        .await?;
        assert_eq!(
            std::fs::read_dir(fixture.path().parent().unwrap().join("owner/workspaces"))?.count(),
            1,
            "a native working view created another workspace owner",
        );
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    fixture.close_owner(owner).await?;
    result
}

async fn case(root: &Path, binding: &Path, host: ExternalHost) -> Result {
    let pi = host == ExternalHost::Pi;
    let attach = if pi { "session_start" } else { "SessionStart" };
    let event = if pi { "tool_call" } else { "PreToolUse" };
    let session = uuid::Uuid::new_v4().to_string();
    hook(
        root,
        host.name(),
        attach,
        json!({"sessionId":session,"cwd":root}),
    )
    .await?;
    let child = hook(
        root,
        host.name(),
        if pi {
            "subagent_start"
        } else {
            "SubagentStart"
        },
        json!({"sessionId":session,"agentId":"bound-child"}),
    )
    .await?;
    let child = child["Succeeded"].as_str().ok_or("child task missing")?;
    let view = root
        .parent()
        .unwrap()
        .join(format!("{}-native-view", host.name()));
    std::fs::create_dir(&view)?;
    in_view(
        root,
        &view,
        binding,
        host,
        attach,
        json!({
            "sessionId":session,"agentId":"bound-child","cwd":view,
        }),
    )
    .await?;
    let filename = format!("{}-bound-child.txt", host.name());
    let answer = in_view(root,&view,binding,host,event,json!({
        "sessionId":session,"agentId":"bound-child","cwd":view,
        "toolCallId":"bound-write","toolName":if pi {"write"} else {"mcp__acyclic__write"},
        "input":{"path":filename,"text":"bound child","media_type":"text/plain","display_name":filename},
    })).await?;
    permitted(pi, &answer);
    assert!(
        !root.join(&filename).exists(),
        "child write reached its parent checkout"
    );
    assert!(
        !view.join(&filename).exists(),
        "cwd substituted an unrelated checkout"
    );

    let arguments = json!({"command":format!("diff agents/{child}")});
    let diff = request(
        host,
        event,
        json!({"sessionId":session,"toolCallId":"bound-diff",
        "toolName":"fs","input":arguments}),
    )?;
    let answer = hook(
        root,
        host.name(),
        event,
        json!({"sessionId":session,"toolCallId":"bound-diff",
        "toolName":if pi {"fs"} else {"mcp__acyclic__fs"},"input":arguments}),
    )
    .await?;
    permitted(pi, &answer);
    let task = Component::new(diff.task_id().to_string())?;
    let result = files(root, &format!("external/results/{}", diff.task_id()))
        .await?
        .snapshot(&Query::File(diff.result_key(task)?), 1)
        .await?;
    assert_eq!(result.files.len(), 1);
    let answer = acyclic_harness::external_executor::tool_result(serde_json::from_slice(
        &result.files[0].bytes,
    )?)?;
    let diff: acyclic_fs::GitCommandOutput =
        serde_json::from_value(answer["details"]["result"].clone())?;
    let acyclic_fs::GitCommandOutput::Filesystem(acyclic_fs::GitFilesystemResult::Data {
        kind,
        value,
    }) = diff
    else {
        panic!("bound child diff missing: {answer}");
    };
    assert_eq!(kind, "diff");
    assert!(value["fileChanges"].as_u64().unwrap() > 0, "{answer}");
    hook(
        root,
        host.name(),
        if pi { "subagent_stop" } else { "SubagentStop" },
        json!({"sessionId":session,"agentId":"bound-child"}),
    )
    .await?;
    Ok(())
}

async fn in_view(
    root: &Path,
    view: &Path,
    binding: &Path,
    host: ExternalHost,
    event: &str,
    input: Value,
) -> Result<Value> {
    let mut launch = command(root);
    launch
        .current_dir(view)
        .env("ACYCLIC_HARNESS_BINDING", binding);
    hook_using(launch, host.name(), event, input).await
}

fn permitted(pi: bool, answer: &Value) {
    if pi {
        assert_eq!(answer["isError"], false, "{answer}");
    } else {
        assert_eq!(
            answer["hookSpecificOutput"]["permissionDecision"], "allow",
            "{answer}"
        );
    }
}
