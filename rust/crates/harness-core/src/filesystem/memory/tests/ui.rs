use super::*;
use crate::{agent::TurnOutputs, executor::ExecutionEvent};

#[tokio::test]
async fn ui_agent_override_and_turn_receipts_ignore_old_asks() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 1_048_576).await?;
    let grant = ContentGrant::verify(
        &storage.issuer.verifier(&mut None)?,
        &storage.scope,
        &storage.volume,
        VolumeOperation::Write,
        &mut None,
    )?;
    let files = AgentInternalFiles::new(storage.host.clone(), storage.volume.clone(), grant);
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
