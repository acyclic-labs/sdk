use super::*;
use crate::{agent::TurnOutputs, executor::ExecutionEvent};

#[tokio::test]
async fn ui_agent_override_and_turn_receipts_ignore_old_asks() -> Result<()> {
    let agent = AgentId::new();
    let storage = MemoryHarnessStorage::new(agent, 1_048_576).await?;
    let control_volume = crate::conversation::journal_volume(
        storage.volume.provider().clone(),
        &crate::agent::AgentId::from_bytes(agent.into_bytes()),
    )?;
    let files = storage.internal_files.clone();
    let journal = storage.journal();
    let turn = OperationId::new();
    journal
        .append(
            turn,
            "start",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    storage
        .stage(
            OperationId::new(),
            "ui/old.md",
            b"```input text id=old\nold\n```",
            "text/plain",
            "old.md",
        )
        .await?;
    assert_eq!(files.waiting_user(turn, &*journal).await?, None);
    let ask = storage
        .stage(
            OperationId::new(),
            "ui/ask.md",
            b"```input choose id=pick\n- a\n- b\n```",
            "text/plain",
            "ask.md",
        )
        .await?;
    let bytes = serde_json::to_vec(&ToolResult {
        value: serde_json::to_value(crate::tool::files::FileResult { file: ask.clone() })
            .map_err(|e| Error::Invalid(e.to_string()))?,
    })
    .map_err(|e| Error::Invalid(e.to_string()))?;
    let result = journal
        .stage(turn, "write-result", &bytes, "application/json", None)?
        .await?;
    let invocation = journal
        .stage(turn, "write-invocation", b"{}", "application/json", None)?
        .await?;
    assert_eq!(result.volume(), &control_volume);
    assert_eq!(invocation.volume(), &control_volume);
    assert_eq!(journal.load(&invocation, None)?.await?.as_ref(), b"{}");
    assert_eq!(
        storage
            .read(&result)
            .await
            .expect_err("private owner read journal payload")
            .code(),
        crate::wire::ErrorCode::Unauthorized,
    );
    journal
        .append(
            turn,
            "write-started",
            &ExecutionEvent::ToolStarted {
                step: 0,
                call_id: "write-ui".into(),
                invocation,
                publication: None,
            },
            None,
        )?
        .await?;
    journal
        .append(
            turn,
            "write-completed",
            &ExecutionEvent::ToolCompleted {
                step: 0,
                call_id: "write-ui".into(),
                result: result.clone(),
                projection: result,
                record_kind: None,
            },
            None,
        )?
        .await?;
    assert_eq!(
        files.waiting_user(turn, &*journal).await?,
        Some("ask.md".into())
    );
    // Recovery reads the same receipts and pinned content, without rewriting it.
    assert_eq!(
        files.waiting_user(turn, &*journal).await?,
        Some("ask.md".into())
    );
    storage
        .stage(
            OperationId::new(),
            "ui/ask.md",
            b"Resolved",
            "text/plain",
            "ask.md",
        )
        .await?;
    assert_eq!(files.waiting_user(turn, &*journal).await?, None);
    assert_eq!(
        storage.read(&ask).await?.as_ref(),
        b"```input choose id=pick\n- a\n- b\n```"
    );
    Ok(())
}
