//! The native binding must exercise LocalHarness's actual project publisher.
use super::*;
use acyclic_harness::{LocalHarness, LocalHarnessConfig};
fn failure(error: impl std::fmt::Display) -> Error {
    Error::Invalid(error.to_string())
}

#[tokio::test]
async fn native_sessions_execute_once_in_the_selected_checkout_after_reopen() -> Result<()> {
    for host in [
        ExternalHost::Codex,
        ExternalHost::ClaudeCode,
        ExternalHost::Pi,
    ] {
        let directory = tempfile::tempdir().map_err(|e| failure(e))?;
        let cwd = directory
            .path()
            .to_str()
            .ok_or_else(|| Error::Invalid("non-UTF8 test path".into()))?;
        let session = uuid::Uuid::new_v4().to_string();
        let (attach_event, tool_event, detach_event) = if host == ExternalHost::Pi {
            ("session_start", "tool_call", "session_shutdown")
        } else {
            ("SessionStart", "PreToolUse", "SessionEnd")
        };
        let attach = request(host, attach_event, json!({"session_id":session,"cwd":cwd}))?;
        let write = request(
            host,
            tool_event,
            json!({"session_id":session,"tool_use_id":"write",
            "tool_name":"write","tool_input":{"path":"note.txt","text":"from harness","media_type":"text/plain","display_name":"note.txt"}}),
        )?;
        assert_eq!(attach.task_id(), write.task_id());
        let harness = LocalHarness::open(LocalHarnessConfig::external(directory.path())).await?;
        let future = harness.external_request(&attach);
        eprintln!(
            "LocalHarness external request future: {} bytes",
            std::mem::size_of_val(&future)
        );
        assert!(std::mem::size_of_val(&future) < 16 * 1024);
        let attachment = future.await?;
        let result = harness.external_request(&write).await?;
        let native = tool_result(result.clone())?;
        assert_eq!(native["isError"], false);
        assert!(native["details"]["file"].is_object());
        assert_eq!(
            std::fs::read_to_string(directory.path().join("note.txt")).map_err(|e| failure(e))?,
            "from harness"
        );
        assert_eq!(harness.external_request(&write).await?, result);
        let mut changed = write.clone();
        if let ExternalAction::RunTool { arguments, .. } = &mut changed.action {
            arguments["text"] = json!("different");
        }
        assert!(harness.external_request(&changed).await.is_err());
        drop(harness);
        let reopened = LocalHarness::open(LocalHarnessConfig::external(directory.path())).await?;
        assert_eq!(reopened.external_request(&write).await?, result);
        if host != ExternalHost::Pi {
            let ExternalAction::RunTool { arguments, .. } = &write.action else {
                unreachable!()
            };
            let mut arguments = arguments.clone();
            arguments["_acyclic"] = json!(
                write
                    .request_key(
                        acyclic_harness::task_volume::Component::new(write.task_id().to_string())
                            .map_err(failure)?
                    )
                    .map_err(failure)?
                    .path()
            );
            assert_eq!(
                reopened
                    .external_tool_result("write", arguments.clone())
                    .await?,
                result
            );
            arguments["text"] = json!("untrusted replacement");
            assert!(
                reopened
                    .external_tool_result("write", arguments)
                    .await
                    .is_err()
            );
        }
        assert_eq!(
            std::fs::read_to_string(directory.path().join("note.txt")).map_err(|e| failure(e))?,
            "from harness"
        );
        let detach = request(host, detach_event, json!({"session_id":session}))?;
        reopened.external_request(&detach).await?;
        drop(reopened);
        let resumed = LocalHarness::open(LocalHarnessConfig::external(directory.path())).await?;
        assert_eq!(resumed.external_request(&attach).await?, attachment);
        let next = request(
            host,
            tool_event,
            json!({"session_id":session,"tool_use_id":"after-reconnect",
            "tool_name":"write","tool_input":{"path":"note.txt","text":"after reconnect","media_type":"text/plain","display_name":"note.txt"}}),
        )?;
        tool_result(resumed.external_request(&next).await?)?;
        assert_eq!(
            std::fs::read_to_string(directory.path().join("note.txt")).map_err(failure)?,
            "after reconnect"
        );
        assert!(
            resumed
                .submit(acyclic_harness::OperationId::new(), "no model bound")
                .await
                .is_err()
        );
    }
    Ok(())
}
