use super::*;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one real-journal admission, retry and operation-isolation timeline"
)]
pub(super) async fn local_journal_admits_compaction_only_after_one_settled_summary() -> Result<()> {
    use acyclic_harness::executor::{ExecutionEvent, ModelPurpose};
    let storage = MemoryHarnessStorage::new(AgentId::new(), 131_072).await?;
    let journal = storage.journal();
    let operation = OperationId::new();
    let request = PreparedModelRequest::prepare(
        ModelRequest {
            model: Model::new(
                "synthetic".into(),
                "journal-test".into(),
                "1".into(),
                serde_json::json!({}),
            )?,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("source".into()),
            }],
            tools: Vec::new(),
            max_output_tokens: Some(32),
        },
        Limits::default(),
        None,
    )?;
    let request_ref = journal
        .stage(
            operation,
            "summary-request",
            request.bytes(),
            "application/json",
            None,
        )?
        .await?;
    let projection_bytes = serde_json::to_vec(&serde_json::json!({
        "canonical": null, "checkpoint": null,
        "context": {"current_input_index": 0, "metadata": {}, "messages": request.request().messages},
    }))
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let context = journal
        .stage(
            operation,
            "temporal-context",
            &projection_bytes,
            "application/json",
            None,
        )?
        .await?;
    let compacted = ExecutionEvent::ContextCompacted {
        step: 0,
        projection: context.clone(),
        compaction: context.clone(),
        accounting: context.clone(),
    };
    assert!(
        journal
            .append_if_tail(operation, 0, "premature", &compacted, None)?
            .await
            .is_err()
    );
    journal
        .append(
            operation,
            "started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    journal
        .append(
            operation,
            "prepared",
            &ExecutionEvent::ContextPrepared {
                step: 0,
                projection: context.clone(),
                accounting: None,
            },
            None,
        )?
        .await?;
    assert!(
        journal
            .append(operation, "no-summary", &compacted, None)?
            .await
            .is_err()
    );
    journal
        .append(
            operation,
            "summary-started",
            &ExecutionEvent::ModelStarted {
                step: 0,
                purpose: ModelPurpose::Summary,
                request_digest: request.request_digest(),
                request: request_ref,
            },
            None,
        )?
        .await?;
    assert!(
        journal
            .append(operation, "unsettled-summary", &compacted, None)?
            .await
            .is_err()
    );
    let completed = journal
        .stage(
            operation,
            "summary-completed",
            b"{\"kind\":\"completed\",\"metadata\":{}}",
            "application/json",
            None,
        )?
        .await?;
    journal
        .append(
            operation,
            "summary-completed",
            &ExecutionEvent::Model {
                step: 0,
                purpose: ModelPurpose::Summary,
                event: completed,
            },
            None,
        )?
        .await?;
    assert!(
        !journal
            .append_if_tail(operation, 3, "stale-tail", &compacted, None)?
            .await?
    );
    assert!(
        journal
            .append_if_tail(operation, 4, "compacted", &compacted, None)?
            .await?
    );
    assert!(
        journal
            .append_if_tail(operation, 4, "compacted", &compacted, None)?
            .await?
    );
    assert!(
        journal
            .append(operation, "duplicate-compacted", &compacted, None)?
            .await
            .is_err()
    );
    let other = OperationId::new();
    journal
        .append(
            other,
            "started",
            &ExecutionEvent::Started {
                request_digest: [2; 32],
            },
            None,
        )?
        .await?;
    assert_eq!(journal.replay(operation, 0, 64, None)?.await?.len(), 5);
    assert_eq!(journal.replay(other, 0, 64, None)?.await?.len(), 1);
    assert!(
        journal
            .append_if_tail(
                operation,
                4,
                "compacted",
                &journal
                    .replay(operation, 4, 1, None)?
                    .await?
                    .first()
                    .ok_or_else(|| Error::Invalid("compaction record missing".into()))?
                    .event,
                None
            )?
            .await?
    );
    Ok(())
}
