use super::*;
use crate::executor::{ModelPurpose, ToolErrorClass, ToolFailureCause, ToolFailureKind};

fn failure(step: u32, call_id: &str, reason: ToolFailureKind) -> ExecutionEvent {
    ExecutionEvent::ToolFailed {
        step,
        call_id: call_id.into(),
        reason,
        cause: ToolFailureCause {
            class: ToolErrorClass::Invalid,
            message: "unavailable".into(),
        },
    }
}

async fn observed(
    journal: &impl ExecutionJournal,
    operation: OperationId,
    key: &str,
    event: &ModelEvent,
) -> Result<ExecutionEvent> {
    Ok(ExecutionEvent::Model {
        usage: None,
        step: 0,
        purpose: ModelPurpose::Response,
        event: crate::executor::stage_json(journal, operation, key, event, None).await?,
    })
}
#[tokio::test]
async fn evicted_dispatches_keep_admission_and_old_retries_keep_identity() -> Result<()> {
    for native in [false, true] {
        let (journal, _issuer) =
            crate::filesystem::execution_journal::test_support::journal().await?;
        let operation = OperationId::new();
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(16 * 1024 * 1024, 4 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let native_baseline = work.live_allocation_bytes();
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let started = ExecutionEvent::Started {
            request_digest: [1; 32],
        };
        journal
            .append(operation, "start", &started, Some(&io))?
            .await?;
        let invocation = journal
            .stage_with_io(operation, "invocation", b"{}", "application/json", None)
            .await?;
        journal
            .append(
                operation,
                "tool",
                &ExecutionEvent::ToolStarted {
                    step: 0,
                    call_id: "pending".into(),
                    invocation,
                },
                Some(&io),
            )?
            .await?;
        let prepared = crate::model::PreparedModelRequest::prepare(
            crate::model::ModelRequest {
                model: crate::model::Model::new("fixture".into(), "fixture".into(), "1".into())?,
                messages: vec![crate::model::ModelMessage {
                    role: crate::model::ModelRole::User,
                    content: crate::model::ModelContent::Text("input".into()),
                }],
                tools: vec![],
                max_output_tokens: None,
            },
            Limits::default(),
            None,
        )?;
        let request = journal
            .stage_with_io(
                operation,
                "request",
                prepared.bytes(),
                "application/json",
                None,
            )
            .await?;
        journal
            .append(
                operation,
                "model",
                &ExecutionEvent::ModelStarted {
                    step: 0,
                    purpose: ModelPurpose::Response,
                    request_digest: prepared.request_digest(),
                    request,
                },
                Some(&io),
            )?
            .await?;
        let call = observed(
            &journal,
            operation,
            "call",
            &ModelEvent::ToolCall(crate::model::ToolCall::new(
                "seen".into(),
                "fixture".into(),
                serde_json::json!({}),
            )),
        )
        .await?;
        journal.append(operation, "call", &call, Some(&io))?.await?;
        for step in 1..=100 {
            journal
                .append(
                    operation,
                    &format!("failed:{step}"),
                    &failure(step, "refused", ToolFailureKind::Unavailable),
                    Some(&io),
                )?
                .await?;
            let cache = journal.verified.lock().await;
            let summary = cache.summary();
            assert!(
                summary.retries.len()
                    + summary.contexts.len()
                    + summary.models.len()
                    + summary.tools.len()
                    <= crate::filesystem::execution_journal::summary_cache::HOT_SUMMARY_ENTRIES + 3,
                "unbounded cache negative control retained historical keys"
            );
            assert_eq!(summary.pending_dispatches, 2);
        }
        assert!(
            journal.verified.lock().await.summary().partial,
            "no eviction was exercised"
        );
        // Retry lookup must reload the old authenticated key, not accept a new start.
        journal
            .append(operation, "start", &started, Some(&io))?
            .await?;
        assert!(
            journal
                .append(
                    operation,
                    "start",
                    &ExecutionEvent::Started {
                        request_digest: [2; 32]
                    },
                    Some(&io)
                )?
                .await
                .is_err()
        );
        assert!(
            journal
                .append(operation, "duplicate-call", &call, Some(&io))?
                .await
                .is_err(),
            "eviction forgot the pending model's observed call IDs"
        );
        journal
            .append(
                operation,
                "completed-model",
                &observed(
                    &journal,
                    operation,
                    "completed",
                    &ModelEvent::Completed {
                        metadata: serde_json::Value::Null,
                    },
                )
                .await?,
                Some(&io),
            )?
            .await?;
        journal
            .append(
                operation,
                "completed-tool",
                &failure(0, "pending", ToolFailureKind::ExecutorRejected),
                Some(&io),
            )?
            .await?;
        assert_eq!(
            journal.verified.lock().await.summary().pending_dispatches,
            0
        );
        assert!(
            journal
                .append(
                    operation,
                    "second-completion",
                    &failure(0, "pending", ToolFailureKind::ExecutorRejected),
                    Some(&io)
                )?
                .await
                .is_err()
        );
        drop(journal);
        assert_eq!(io.owned_bytes()?, 0, "evicted metadata retained custody");
        if native {
            assert_eq!(work.live_allocation_bytes(), native_baseline);
        }
    }
    Ok(())
}
