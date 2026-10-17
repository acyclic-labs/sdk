//! Open provider subscriptions cannot retain the process owner during shutdown.
#[path = "support/native_hooks.rs"]
mod native_hooks;

use acyclic_stream::{ChildrenRequest, StreamPath, StreamProvider as _};
use futures::StreamExt as _;
use native_hooks::*;
use serde_json::json;
use acyclic_harness::{
    external_executor::{ExternalHost, local_hook_volumes, request},
    task_volume::{Component, Query},
};
use std::sync::Arc;

#[tokio::test]
async fn owner_shutdown_completes_while_clients_retain_their_follows() -> Result {
    let fixture = Fixture::new()?;
    let owner = fixture.start_owner().await?;
    let attached = hook(fixture.path(), "pi", "session_start", json!({
        "sessionId":"open-follows","cwd":fixture.path(),
    })).await?;
    let task = attached["task"].as_str().ok_or("missing attached task")?;
    let client = client(fixture.path()).await?;
    let mut followed = client.follow(StreamPath::new(format!("external/results/{task}"))?, 0).await?;
    followed.next().await.ok_or("missing result prefix")??;
    let mut children = client.children(ChildrenRequest {
        parent:Some(StreamPath::new("external/requests")?), limit:64,
    }).await?;
    children.next().await.ok_or("missing admitted task child")??;
    // Both SDK follows stay alive until the owner has drained and released its lock.
    tokio::time::timeout(std::time::Duration::from_secs(3), fixture.close_owner(owner)).await??;
    drop((followed, children, client));
    Ok(())
}

#[tokio::test]
async fn malformed_task_files_leave_other_tasks_and_workspaces_live_after_restart() -> Result {
    let fixture = Fixture::new()?;
    let other = fixture.path().parent().ok_or("workspace parent missing")?.join("other");
    std::fs::create_dir(&other)?;
    let canonical = std::fs::canonicalize(fixture.path())?;
    let principal = blake3::hash(canonical.as_os_str().as_encoded_bytes()).to_hex();
    let registration = fixture.path().parent().unwrap().join("owner/workspaces")
        .join(format!("{principal}.json"));
    assert!(!registration.exists());
    let mut retained_binding = None;
    for round in 0..2 {
        let owner = fixture.start_owner().await?;
        if round == 0 {
            // Use the production hook bootstrap before making a raw SDK client.
            // On restart, open the retained stream before any hook can repair it.
            hook(fixture.path(), "pi", "session_start", json!({
                "sessionId":"workspace-binding","cwd":fixture.path(),
            })).await?;
        }
        let binding = std::fs::read(&registration)
            .map_err(|error| format!("round {round}: workspace {principal} has no registration: {error}"))?;
        if let Some(previous) = &retained_binding {
            assert_eq!(&binding, previous, "restart changed the authenticated workspace binding");
        }
        retained_binding = Some(binding);
        eprintln!("round {round}: opening task files as registered workspace {principal}");
        let client = Arc::new(client(fixture.path()).await?);
        let rejected = request(ExternalHost::Pi, "session_start", json!({
            "sessionId":"malformed","cwd":fixture.path(),
        }))?;
        let (files, _) = local_hook_volumes(client, rejected.task_id()).await?;
        let mut file = rejected.to_file(Component::new(rejected.task_id().to_string())?)?;
        file.bytes = b"{malformed".as_slice().into();
        if round == 0 {
            files.append(files.head().await?, &file).await?;
        }
        for (index, workspace) in [fixture.path(), other.as_path()].into_iter().enumerate() {
            let session = format!("healthy-{round}-{index}");
            hook(workspace, "pi", "session_start", json!({"sessionId":session,"cwd":workspace})).await?;
            let written = hook(workspace, "pi", "tool_call", json!({
                "sessionId":session,"cwd":workspace,"toolCallId":"write","toolName":"write",
                "input":{"path":format!("healthy-{round}.txt"),"text":"present","media_type":"text/plain","display_name":"healthy.txt"},
            })).await?;
            assert_eq!(written["isError"], false, "{written}");
            assert_eq!(std::fs::read_to_string(workspace.join(format!("healthy-{round}.txt")))?, "present");
        }
        let retained = files.snapshot(&Query::File(file.key.clone()), 1).await?;
        assert_eq!(retained.files.as_slice(), &[file]);
        drop(files);
        tokio::time::timeout(std::time::Duration::from_secs(3), fixture.close_owner(owner)).await??;
    }
    Ok(())
}
