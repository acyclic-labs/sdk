//! Separate native hook processes progress while their parent awaits a child.
#[path = "support/native_hooks.rs"]
mod native_hooks;
use acyclic_harness::{
    external_executor::{ExternalHost, ExternalRequest, request},
    task_volume::{Component, FileKey, Query, TaskFile, TaskVolume},
};
use acyclic_stream::grpc::Client;
use futures::StreamExt as _;
use native_hooks::*;
use serde_json::json;

#[tokio::test]
async fn codex_transcript_ancestry_routes_nested_forks_and_stops() -> Result {
    let fixture = Fixture::new()?;
    let cwd = fixture.path();
    let owner = fixture.start_owner().await?;
    let session = uuid::Uuid::from_u128(1).to_string();
    let child = uuid::Uuid::from_u128(2).to_string();
    let grandchild = uuid::Uuid::from_u128(3).to_string();
    hook(
        cwd,
        "codex",
        "SessionStart",
        json!({"session_id":session,"cwd":cwd}),
    )
    .await?;
    let mut parent_path = None;
    for (current, parent) in [(&child, &session), (&grandchild, &child)] {
        let transcript = cwd.join(format!("{current}.jsonl"));
        std::fs::write(
            &transcript,
            serde_json::to_vec(&json!({"type":"session_meta","payload":{
                "session_id":session,"id":current,"parent_thread_id":parent,
            }}))?,
        )?;
        let input = json!({"session_id":session,"agent_id":current,"transcript_path":transcript});
        let fork = hook(cwd, "codex", "SubagentStart", input.clone()).await?;
        assert!(fork["Succeeded"].is_string(), "{fork}");
        let mut expected = input;
        expected["parent_agent_id"] = if parent == &session {
            serde_json::Value::Null
        } else {
            json!(parent)
        };
        let request = request(ExternalHost::Codex, "SubagentStart", expected)?;
        let task = Component::new(request.task_id().to_string())?;
        let accepted = files(cwd, &format!("external/requests/{}", task.as_str())).await?;
        let key = FileKey::Task {
            task: task.clone(),
            file: TaskFile::Request(Component::new(request.operation_id.to_string())?),
        };
        let snapshot = accepted.snapshot(&Query::File(key), 1).await?;
        assert_eq!(snapshot.files.len(), 1);
        assert_eq!(
            ExternalRequest::from_file(&task, &snapshot.files[0])?,
            request
        );
        hook(
            cwd,
            "codex",
            "SessionStart",
            json!({"session_id":session,"agent_id":current,"cwd":cwd}),
        )
        .await?;
        if parent == &child {
            let stopped = hook(
                cwd,
                "codex",
                "SubagentStop",
                json!({
                    "session_id":session,"agent_id":current,"agent_transcript_path":transcript,
                    "transcript_path":parent_path,
                }),
            )
            .await?;
            assert!(stopped.get("Failed").is_none(), "{stopped}");
        }
        parent_path = Some(transcript);
    }
    let stopped = hook(cwd,"codex","SubagentStop",json!({
        "session_id":session,"agent_id":child,"agent_transcript_path":cwd.join(format!("{child}.jsonl")),
    })).await?;
    assert!(stopped.get("Failed").is_none(), "{stopped}");
    hook(cwd, "codex", "SessionEnd", json!({"session_id":session})).await?;
    fixture.close_owner(owner).await
}

async fn started(files: &TaskVolume<Client>, call: &ExternalRequest) -> Result {
    let key = FileKey::Task {
        task: Component::new(call.task_id().to_string())?,
        file: TaskFile::RequestStarted(Component::new(call.operation_id.to_string())?),
    };
    let mut observed = files.observe(Query::File(key.clone()), None, 1).await?;
    if observed
        .snapshot
        .is_some_and(|snapshot| snapshot.files.iter().any(|file| file.key == key))
    {
        return Ok(());
    }
    while let Some(change) = observed.changes.next().await {
        let change = change?;
        if change.file.as_ref().is_some_and(|file| file.key == key) {
            return Ok(());
        }
    }
    Err("command-start follow ended".into())
}

#[tokio::test]
async fn exposed_wait_uses_the_returned_child_handle_for_every_host() -> Result {
    let fixture = Fixture::new()?;
    let cwd = fixture.path();
    for host in [
        ExternalHost::Codex,
        ExternalHost::ClaudeCode,
        ExternalHost::Pi,
    ] {
        let pi = host == ExternalHost::Pi;
        let session = host.name();
        let attach = if pi { "session_start" } else { "SessionStart" };
        hook(
            cwd,
            host.name(),
            attach,
            json!({"session_id":session,"cwd":cwd}),
        )
        .await?;
        let spawned = hook(
            cwd,
            host.name(),
            if pi {
                "subagent_start"
            } else {
                "SubagentStart"
            },
            json!({"session_id":session,"agent_id":"child"}),
        )
        .await?;
        let child = spawned["Succeeded"]
            .as_str()
            .ok_or("spawn did not return a child handle")?;
        hook(
            cwd,
            host.name(),
            attach,
            json!({"session_id":session,"agent_id":"child","cwd":cwd}),
        )
        .await?;
        let event = if pi { "tool_call" } else { "PreToolUse" };
        let arguments = json!({"child":child});
        let input = json!({"session_id":session,"tool_use_id":"wait","tool_name":
            if pi {"acyclic_wait"} else {"mcp__acyclic__acyclic_wait"},"tool_input":arguments});
        let call = request(
            host,
            event,
            json!({"session_id":session,"tool_use_id":"wait",
            "tool_name":"acyclic_wait","tool_input":arguments}),
        )?;
        let results = files(cwd, &format!("external/results/{}", call.task_id())).await?;
        let mut waiting = Box::pin(hook(cwd, host.name(), event, input));
        tokio::select! {
            result = &mut waiting => panic!("wait returned before child completion: {result:?}"),
            result = started(&results,&call) => result?,
        }
        let (answer, ended) = tokio::join!(
            waiting,
            hook(
                cwd,
                host.name(),
                if pi { "session_shutdown" } else { "SessionEnd" },
                json!({"session_id":session,"agent_id":"child"})
            )
        );
        let answer = answer?;
        ended?;
        if pi {
            assert_eq!(answer["isError"], false);
            assert!(answer["details"].is_null());
        } else {
            assert_eq!(answer["hookSpecificOutput"]["permissionDecision"], "allow");
        }
        let settled = results
            .snapshot(
                &Query::File(call.result_key(Component::new(call.task_id().to_string())?)?),
                1,
            )
            .await?;
        assert_eq!(settled.files.len(), 1);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&settled.files[0].bytes)?,
            json!({"Succeeded":null})
        );
    }
    fixture.close().await
}

#[tokio::test]
async fn native_subagent_stop_completes_the_child_before_parent_continuation() -> Result {
    let fixture = Fixture::new()?;
    let cwd = fixture.path();
    tokio::try_join!(
        Box::pin(native_stop_lifecycle(cwd, ExternalHost::Codex)),
        Box::pin(native_stop_lifecycle(cwd, ExternalHost::ClaudeCode)),
        Box::pin(native_stop_lifecycle(cwd, ExternalHost::Pi)),
    )?;
    fixture.close().await
}

async fn native_stop_lifecycle(cwd: &std::path::Path, host: ExternalHost) -> Result {
    let pi = host == ExternalHost::Pi;
    let session = host.name();
    let attach = if pi { "session_start" } else { "SessionStart" };
    hook(
        cwd,
        host.name(),
        attach,
        json!({"sessionId":session,"cwd":cwd}),
    )
    .await?;
    let spawned = hook(
        cwd,
        host.name(),
        if pi {
            "subagent_start"
        } else {
            "SubagentStart"
        },
        json!({"sessionId":session,"agentId":"child","agentType":"worker"}),
    )
    .await?;
    let child = spawned["Succeeded"]
        .as_str()
        .ok_or("native child task missing")?;
    hook(
        cwd,
        host.name(),
        attach,
        json!({"sessionId":session,"agentId":"child","cwd":cwd}),
    )
    .await?;
    let filename = format!("{}-child-only.txt", host.name());
    let writing = hook(cwd,host.name(),if pi {"tool_call"} else {"PreToolUse"},json!({
            "sessionId":session,"agentId":"child","toolCallId":"child-write",
            "toolName":if pi {"write"} else {"mcp__acyclic__write"},
            "input":{"path":filename,"text":"isolated child contents","media_type":"text/plain","display_name":"child.txt"},
        })).await?;
    if pi {
        assert_eq!(writing["isError"], false, "{writing}");
    } else {
        assert_eq!(
            writing["hookSpecificOutput"]["permissionDecision"], "allow",
            "{writing}"
        );
    }
    assert!(
        !cwd.join(&filename).exists(),
        "child write reached the parent checkout"
    );
    let event = if pi { "subagent_stop" } else { "SubagentStop" };
    let input = json!({"sessionId":session,"agentId":"child"});
    let join = request(host, event, input.clone())?;
    // The native host sends no child SessionEnd before continuing its
    // parent. Stop itself must settle the child, and retries must agree.
    assert_eq!(
        hook(cwd, host.name(), event, input.clone()).await?,
        json!({"Succeeded":null})
    );
    assert_eq!(
        hook(cwd, host.name(), event, input).await?,
        json!({"Succeeded":null})
    );
    let continued = hook(
        cwd,
        host.name(),
        if pi { "tool_call" } else { "PreToolUse" },
        json!({"session_id":session,"tool_use_id":"continued",
                "tool_name":if pi {"write"} else {"mcp__acyclic__write"},
                "tool_input":{"path":format!("{}.txt",host.name()),"text":"parent continued",
                    "media_type":"text/plain","display_name":"continued.txt"}}),
    )
    .await?;
    if pi {
        assert_eq!(continued["isError"], false, "{continued}");
    } else {
        assert_eq!(
            continued["hookSpecificOutput"]["permissionDecision"], "allow",
            "{continued}"
        );
    }
    assert_eq!(
        std::fs::read_to_string(cwd.join(format!("{}.txt", host.name())))?,
        "parent continued"
    );
    let event = if pi { "tool_call" } else { "PreToolUse" };
    let arguments = json!({"command":format!("diff agents/{child}")});
    let diff = request(
        host,
        event,
        json!({"sessionId":session,"toolCallId":"child-diff",
            "toolName":"fs","input":arguments}),
    )?;
    let answer = hook(
        cwd,
        host.name(),
        event,
        json!({"sessionId":session,"toolCallId":"child-diff",
            "toolName":if pi {"fs"} else {"mcp__acyclic__fs"},"input":arguments}),
    )
    .await?;
    if pi {
        assert_eq!(answer["isError"], false, "{answer}");
    } else {
        assert_eq!(
            answer["hookSpecificOutput"]["permissionDecision"], "allow",
            "{answer}"
        );
    }
    let published = files(cwd, &format!("external/results/{}", diff.task_id()))
        .await?
        .snapshot(
            &Query::File(diff.result_key(Component::new(diff.task_id().to_string())?)?),
            1,
        )
        .await?;
    assert_eq!(published.files.len(), 1);
    let actual = acyclic_harness::external_executor::tool_result(serde_json::from_slice(
        &published.files[0].bytes,
    )?)?;
    let command: acyclic_fs::GitCommandOutput =
        serde_json::from_value(actual["details"]["result"].clone())?;
    let acyclic_fs::GitCommandOutput::Filesystem(acyclic_fs::GitFilesystemResult::Data {
        kind,
        value,
    }) = command
    else {
        panic!("expected the native child's published diff: {actual}");
    };
    assert_eq!(kind, "diff");
    assert!(value["fileChanges"].as_u64().unwrap() > 0, "{actual}");
    let results = files(cwd, &format!("external/results/{}", join.task_id())).await?;
    assert_eq!(
        results
            .snapshot(
                &Query::File(join.result_key(Component::new(join.task_id().to_string())?)?),
                1,
            )
            .await?
            .files
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn cancelling_a_passive_native_tool_settles_it_and_keeps_the_session_usable() -> Result {
    let fixture = Fixture::new()?;
    let cwd = fixture.path();
    hook(
        cwd,
        "pi",
        "session_start",
        json!({"sessionId":"cancel-wait","cwd":cwd}),
    )
    .await?;
    let input = json!({"sessionId":"cancel-wait","toolCallId":"receive",
        "toolName":"acyclic_receive","input":{"after":0}});
    let call = request(ExternalHost::Pi, "tool_call", input.clone())?;
    let results = files(cwd, &format!("external/results/{}", call.task_id())).await?;
    let waiting = start_hook(cwd, "pi", "tool_call", input).await?;
    started(&results, &call).await?;
    assert!(
        results
            .snapshot(
                &Query::File(call.result_key(Component::new(call.task_id().to_string())?)?),
                1
            )
            .await?
            .files
            .is_empty()
    );
    // A cancellation is published independently of the occupied command slot.
    let original = start_hook(
        cwd,
        "pi",
        "tool_cancel",
        json!({"sessionId":"cancel-wait","toolCallId":"receive"}),
    )
    .await?;
    // Pi's raw tool-call process reports a failed tool as a nonzero exit. Use
    // the request result file to assert settlement without treating it as success.
    let settled = async {
        let key = call.result_key(Component::new(call.task_id().to_string())?)?;
        let mut observed = results.observe(Query::File(key), None, 1).await?;
        if let Some(snapshot) = observed.snapshot {
            if let Some(file) = snapshot.files.first() {
                return Ok::<_, Box<dyn std::error::Error>>(serde_json::from_slice::<
                    serde_json::Value,
                >(&file.bytes)?);
            }
        }
        while let Some(change) = observed.changes.next().await {
            if let Some(file) = change?.file {
                return Ok(serde_json::from_slice(&file.bytes)?);
            }
        }
        Err("result follow ended".into())
    };
    let answer = settled.await?;
    assert!(answer.get("Failed").is_some(), "{answer}");
    let cancelled = original.wait_with_output().await?;
    assert!(
        cancelled.status.success(),
        "{}",
        String::from_utf8_lossy(&cancelled.stderr)
    );
    assert!(!waiting.wait_with_output().await?.status.success());
    hook(
        cwd,
        "pi",
        "tool_call",
        json!({"sessionId":"cancel-wait","toolCallId":"next",
        "toolName":"write","input":{"path":"after-cancel.txt","content":"usable"}}),
    )
    .await?;
    assert_eq!(
        std::fs::read_to_string(cwd.join("after-cancel.txt"))?,
        "usable"
    );
    fixture.close().await
}
