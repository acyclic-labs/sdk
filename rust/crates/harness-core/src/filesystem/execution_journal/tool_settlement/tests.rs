use super::*;
use crate::executor::{ToolErrorClass, ToolFailureCause, ToolFailureKind};

fn refusal(step: u32) -> ExecutionEvent {
    ExecutionEvent::ToolFailed {
        step,
        call_id: "call".into(),
        reason: ToolFailureKind::Unavailable,
        cause: ToolFailureCause {
            class: ToolErrorClass::Invalid,
            message: "unavailable".into(),
        },
    }
}

#[tokio::test]
async fn terminal_lookup_survives_reconstruction_without_replaying_history() -> Result<()> {
    for native in [false, true] {
        let (journal, _issuer) = test_support::journal().await?;
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
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(256 * 1024 * 1024, 4 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let query = ToolSettlementQuery {
            turn,
            step: 0,
            call_id: "call",
            effect: crate::tool::ToolInvocation::model_call_operation(turn, 0, "call", None)?,
        };
        // Dropping the producer never creates a settlement or a pending wait.
        let event = refusal(0);
        let key = tool_settlement_key(0, "call", ToolTerminalOutcome::Failed, None)?;
        drop(journal.append(turn, &key, &event, None)?);
        assert_eq!(journal.tool_settlement(query, Some(&io))?.await?, None);
        let before = io.receipt();
        let foreign = ToolSettlementQuery {
            effect: OperationId::new(),
            ..query
        };
        let error = journal
            .tool_settlement(foreign, Some(&io))?
            .await
            .expect_err("foreign effect accepted");
        assert_eq!(error.code(), crate::wire::ErrorCode::Conflict);
        assert_eq!(
            io.receipt().authority_records_read,
            before.authority_records_read
        );
        for step in 0..64 {
            let key = tool_settlement_key(step, "call", ToolTerminalOutcome::Failed, None)?;
            journal.append(turn, &key, &refusal(step), None)?.await?;
            if ![0, 63].contains(&step) {
                continue;
            }
            let reopened = FilesystemExecutionJournal::new(
                journal.stream.clone(),
                journal.host.clone(),
                crate::filesystem::FilesystemJournalBinding {
                    volume: &journal.volume,
                    verifier: &journal.verifier,
                    scope: &journal.scope,
                    maximum_bytes: journal.maximum_payload_bytes,
                    io: None,
                },
            )?;
            let before = io.receipt();
            let owned = io.owned_bytes()?;
            let settled = reopened.tool_settlement(query, Some(&io))?.await?;
            assert_eq!(
                settled,
                Some(ToolSettlement {
                    cut: u64::from(step) + 2,
                    sequence: 2,
                    outcome: ToolTerminalOutcome::Failed
                })
            );
            let reads = io.receipt().authority_records_read - before.authority_records_read;
            assert!(
                reads <= 518,
                "terminal lookup exceeded two fixed-depth paths: {reads}"
            );
            eprintln!(
                "terminal lookup native={native} steps={} reads={reads}",
                step + 1
            );
            assert_eq!(io.owned_bytes()?, owned);
            let rebound = reopened.with_turn(OperationId::new(), turn);
            assert_eq!(
                rebound
                    .tool_settlement(query, Some(&io))?
                    .await
                    .expect_err("foreign journal accepted")
                    .code(),
                crate::wire::ErrorCode::Unauthorized
            );
        }
        if native {
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
        let cancelled = acyclic_fs::CancellationToken::new();
        let cancelled_io = PreparationIo::from_budget(budget, &cancelled)?;
        cancelled.cancel();
        assert!(
            journal
                .tool_settlement(query, Some(&cancelled_io))?
                .await
                .is_err()
        );
        assert_eq!(cancelled_io.receipt().authority_records_read, 0);
    }
    Ok(())
}

#[tokio::test]
async fn raw_terminal_observation_cannot_authorize_request_retirement() -> Result<()> {
    let (journal, _issuer) = test_support::journal().await?;
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
    let key = tool_settlement_key(0, "call", ToolTerminalOutcome::Failed, None)?;
    let retry = content::operation_digest(turn, &key, None)?
        .to_hex()
        .to_string();
    let body = serde_json::to_vec(&Observation {
        version: OBSERVATION_VERSION,
        operation_id: turn,
        turn_operation: turn,
        retry_digest: retry,
        event: refusal(0),
    })
    .map_err(|error| Error::Invalid(error.to_string()))?;
    journal
        .stream
        .append(acyclic_stream::AppendRequest {
            path: crate::distributed::execution_path(turn, &mut None)?,
            records: vec![body.into()],
            if_tail: Some(1),
            idempotency_key: StreamKey::unique(),
        })
        .await?;
    let query = ToolSettlementQuery {
        turn,
        step: 0,
        call_id: "call",
        effect: crate::tool::ToolInvocation::model_call_operation(turn, 0, "call", None)?,
    };
    assert_eq!(journal.tool_settlement(query, None)?.await?, None);
    Ok(())
}
