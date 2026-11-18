use super::*;
use crate::executor::{ToolErrorClass, ToolFailureCause, ToolFailureKind, tool_settlement_key};
use acyclic_stream::StreamProviderExt as _;

fn refusal(step: u32, call_id: &str) -> ExecutionEvent {
    ExecutionEvent::ToolFailed {
        step,
        call_id: call_id.into(),
        reason: ToolFailureKind::Unavailable,
        cause: ToolFailureCause {
            class: ToolErrorClass::Invalid,
            message: "unavailable".into(),
        },
    }
}

fn legacy_tool_settlement_key(
    step: u32,
    call_id: &str,
    outcome: ToolTerminalOutcome,
) -> Result<String> {
    let kind = match outcome {
        ToolTerminalOutcome::Completed => "completed",
        ToolTerminalOutcome::Failed => "failed",
    };
    crate::executor::with_history_io(None, |work| {
        crate::executor::preparation_format(format_args!("tool:{step}:{call_id}:{kind}"), work)
    })
}

#[tokio::test]
async fn terminal_lookup_reads_settlements_written_before_the_shared_identity() -> Result<()> {
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
    let key = legacy_tool_settlement_key(0, "call", ToolTerminalOutcome::Failed)?;
    journal
        .append(turn, &key, &refusal(0, "call"), None)?
        .await?;
    let query = ToolSettlementQuery {
        turn,
        step: 0,
        call_id: "call",
        effect: crate::tool::ToolInvocation::model_call_operation(turn, 0, "call", None)?,
    };
    assert_eq!(
        journal.tool_settlement(query, None)?.await?,
        Some(ToolSettlement {
            cut: 2,
            sequence: 2,
            outcome: ToolTerminalOutcome::Failed,
        })
    );
    Ok(())
}

#[tokio::test]
async fn a_colon_in_a_call_id_cannot_collide_with_a_legacy_settlement() -> Result<()> {
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
    // Before the shared identity, `call` failed under the outcome-suffixed key.
    let legacy = legacy_tool_settlement_key(0, "call", ToolTerminalOutcome::Failed)?;
    journal
        .append(turn, &legacy, &refusal(0, "call"), None)?
        .await?;
    // A pending call whose ID is that key's suffix must not reuse the key.
    let pending = "call:failed";
    let key = tool_settlement_key(0, pending, None)?;
    assert_ne!(key, legacy);
    journal
        .append(turn, &key, &refusal(0, pending), None)?
        .await?;
    for (call_id, sequence) in [("call", 2), (pending, 3)] {
        let query = ToolSettlementQuery {
            turn,
            step: 0,
            call_id,
            effect: crate::tool::ToolInvocation::model_call_operation(turn, 0, call_id, None)?,
        };
        assert_eq!(
            journal.tool_settlement(query, None)?.await?,
            Some(ToolSettlement {
                cut: 3,
                sequence,
                outcome: ToolTerminalOutcome::Failed,
            })
        );
    }
    Ok(())
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
        let budget = acyclic_fs_work::performance::work_budget(256 * 1024 * 1024, 4 * 1024 * 1024);
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
        let event = refusal(0, "call");
        let key = tool_settlement_key(0, "call", None)?;
        drop(journal.append(turn, &key, &event, None)?);
        assert_eq!(journal.tool_settlement(query, Some(&io))?.await?, None);
        let before = io.receipt()?;
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
            io.receipt()?.authority_records_read,
            before.authority_records_read
        );
        for step in 0..64 {
            let key = tool_settlement_key(step, "call", None)?;
            journal
                .append(turn, &key, &refusal(step, "call"), None)?
                .await?;
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
                    io: None,
                },
            )?;
            let before = io.receipt()?;
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
            let reads = io.receipt()?.authority_records_read - before.authority_records_read;
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
        let payload = journal
            .stage_with_io(turn, "payload", b"{}", "application/json", None)
            .await?;
        journal
            .append(
                turn,
                "dispatch",
                &ExecutionEvent::ToolStarted {
                    step: 64,
                    call_id: "call".into(),
                    invocation: payload.clone(),
                    publication: None,
                },
                None,
            )?
            .await?;
        let key = tool_settlement_key(64, "call", None)?;
        journal
            .append(
                turn,
                &key,
                &ExecutionEvent::ToolCompleted {
                    step: 64,
                    call_id: "call".into(),
                    result: payload.clone(),
                    projection: payload,
                    record_kind: None,
                },
                None,
            )?
            .await?;
        let completed = ToolSettlementQuery {
            step: 64,
            effect: crate::tool::ToolInvocation::model_call_operation(turn, 64, "call", None)?,
            ..query
        };
        assert!(
            journal
                .append(turn, &key, &refusal(64, "call"), None)?
                .await
                .is_err(),
            "a second settlement entered the authenticated prefix"
        );
        assert_eq!(
            journal.tool_settlement(completed, Some(&io))?.await?,
            Some(ToolSettlement {
                cut: 67,
                sequence: 67,
                outcome: ToolTerminalOutcome::Completed,
            })
        );
        let usage = crate::executor::usage_page(&journal, turn, "unbound", 65).await?;
        assert!(usage.items.is_empty());
        assert_eq!((usage.tools.started, usage.tools.completed), (1, 1));
        assert!(usage.tools.failures.is_empty());
        assert!(
            usage.tools.feedback.is_empty(),
            "unmarked opaque results are not decoded as feedback"
        );
        if native {
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
        let mut finite = budget;
        finite.backend_read_operations = 0;
        let finite_io = PreparationIo::from_budget(finite, &cancellation)?;
        let denied = match journal.tool_settlement(completed, Some(&finite_io)) {
            Ok(future) => future.await,
            Err(error) => Err(error),
        };
        assert!(
            denied.is_err(),
            "finite read refusal became an absent result"
        );
        assert_eq!(finite_io.receipt()?.backend_read_operations, 0);
        assert_eq!(finite_io.receipt()?.authority_records_read, 0);
        assert_eq!(finite_io.owned_bytes()?, 0);
        let cancelled = acyclic_fs::CancellationToken::new();
        let cancelled_io = PreparationIo::from_budget(budget, &cancelled)?;
        cancelled.cancel();
        let cancelled_result = match journal.tool_settlement(query, Some(&cancelled_io)) {
            Ok(future) => future.await,
            Err(error) => Err(error),
        };
        assert_eq!(
            cancelled_result
                .expect_err("cancelled read accepted")
                .code(),
            crate::wire::ErrorCode::Cancelled
        );
        assert_eq!(cancelled_io.receipt()?.authority_records_read, 0);
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
    let key = tool_settlement_key(0, "call", None)?;
    let retry = content::operation_digest(turn, &key, None)?
        .to_hex()
        .to_string();
    let body = serde_json::to_vec(&Observation {
        version: OBSERVATION_VERSION,
        operation_id: turn,
        turn_operation: turn,
        retry_digest: retry,
        event: refusal(0, "call"),
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
    let state = journal
        .load_index_checkpoint(turn, None)
        .await?
        .expect("original checkpoint")
        .0;
    let head = journal.index_path(turn, "head", None)?;
    // A genuine signed root copied into a different commit loses its anchor.
    journal
        .stream
        .append(acyclic_stream::AppendRequest {
            path: (*head).clone(),
            records: vec![journal.encode_index_checkpoint(state, None).await?.into()],
            if_tail: Some(1),
            idempotency_key: StreamKey::unique(),
        })
        .await?;
    assert!(
        journal.tool_settlement(query, None)?.await.is_err(),
        "detached checkpoint became an absent or terminal result"
    );
    Ok(())
}
