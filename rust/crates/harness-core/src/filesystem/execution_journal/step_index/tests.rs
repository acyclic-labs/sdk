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
async fn fresh_step_selection_avoids_historical_replay_and_releases_custody() -> Result<()> {
    for native in [false, true] {
        check_selection(native).await?;
    }
    Ok(())
}

async fn check_selection(native: bool) -> Result<()> {
    let (journal, _issuer) = crate::filesystem::execution_journal::test_support::journal().await?;
    let operation = OperationId::new();
    journal
        .append(
            operation,
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
    for step in 0..1_000 {
        journal
            .append(operation, &format!("call:{step}"), &refusal(step), None)?
            .await?;
        if ![0, 99, 999].contains(&step) {
            continue;
        }
        // This journal has no cached summary or driver state from the writer.
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
        let start = std::time::Instant::now();
        let result = reopened
            .indexed_records(
                crate::executor::ExecutionRecordQuery {
                    operation,
                    step,
                    include_started: true,
                    maximum_step_records: 1,
                    maximum: 2,
                    select: &|_| true,
                },
                Some(&io),
            )?
            .ok_or_else(|| invalid(Some(&io)))?
            .await?
            .ok_or_else(|| invalid(Some(&io)))?;
        let hold = io.guard_owned(io.owned_since(owned)?)?;
        let reads = io.receipt().authority_records_read - before.authority_records_read;
        eprintln!(
            "fresh step native={native} steps={} reads={reads} elapsed_us={}",
            step + 1,
            start.elapsed().as_micros()
        );
        assert!(reads < 128, "step selection replayed history");
        assert_eq!(result.0, u64::from(step) + 2);
        assert_eq!(result.1.len(), 2);
        assert_eq!(result.1.first().map(|record| record.sequence), Some(1));
        assert_eq!(
            result.1.last().map(|record| record.sequence),
            Some(u64::from(step) + 2)
        );
        drop(result);
        drop(hold);
        drop(reopened);
        assert_eq!(io.owned_bytes()?, owned);
        if native {
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }
    // The old public replay entry point is the negative control, independently
    // validated by ExecutionReplay rather than trusting the indexed selection.
    let before = io.receipt();
    let owned = io.owned_bytes()?;
    let (_, records, hold) = crate::executor::replay_execution(
        &journal,
        operation,
        2,
        |event| event.step().is_none() || event.step() == Some(999),
        Some(&io),
    )
    .await?;
    let finite_hold = (!native)
        .then(|| io.guard_owned(io.owned_since(owned)?))
        .transpose()?;
    assert!(io.receipt().authority_records_read - before.authority_records_read >= 1_001);
    assert_eq!(records.len(), 2);
    drop(records);
    drop(hold);
    drop(finite_hold);
    drop(journal);
    assert_eq!(io.owned_bytes()?, 0);
    if native {
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}

#[test]
fn step_links_reject_bad_lengths_tags_padding_and_forward_references() -> Result<()> {
    let entry = Entry {
        step: Some(1),
        sequence: 2,
        retry: [3; 32],
        digest: [4; 32],
        tool: None,
    };
    let reference = NodeRef {
        sequence: 10,
        digest: [5; 32],
    };
    let bytes = encode(&entry, None);
    let bytes = bytes.as_slice();
    for length in 0..bytes.len() {
        assert!(decode(&bytes[..length], reference, entry.step, None).is_err());
    }
    for (offset, value) in [(72, 2), (72, 4), (72, 8), (73, 1), (0, 0)] {
        let mut malformed = bytes.to_vec();
        malformed[offset] = value;
        assert!(decode(&malformed, reference, entry.step, None).is_err());
    }
    let forward = encode(&entry, Some(reference));
    assert!(decode(forward.as_slice(), reference, entry.step, None).is_err());
    assert_eq!(decode(bytes, reference, entry.step, None)?.0.sequence, 2);
    for settled in [false, true] {
        let tool = Some(([9; 32], settled));
        let bytes = encode(&Entry { tool, ..entry }, None);
        assert_eq!(bytes.as_slice().len(), 145);
        assert_eq!(
            decode(bytes.as_slice(), reference, entry.step, None)?
                .0
                .tool,
            tool
        );
        for length in 113..145 {
            assert!(decode(&bytes.as_slice()[..length], reference, entry.step, None).is_err());
        }
    }
    Ok(())
}

#[tokio::test]
async fn cold_tool_admission_uses_the_step_chain_without_a_duplicate_state_tree() -> Result<()> {
    for (native, compare) in [(false, false), (false, true), (true, false), (true, true)] {
        let (journal, _issuer) =
            crate::filesystem::execution_journal::test_support::journal().await?;
        let operation = OperationId::new();
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(16 * 1024 * 1024, 4 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        journal
            .append(
                operation,
                "start",
                &ExecutionEvent::Started {
                    request_digest: [1; 32],
                },
                None,
            )?
            .await?;
        let payload = journal
            .stage_with_io(operation, "payload", b"{}", "application/json", None)
            .await?;
        let started = ExecutionEvent::ToolStarted {
            step: 7,
            call_id: "call".into(),
            invocation: payload.clone(),
        };
        journal.append(operation, "tool", &started, None)?.await?;
        *journal.verified.lock().await = SummaryCache::default();
        let completed = ExecutionEvent::ToolCompleted {
            step: 7,
            call_id: "call".into(),
            result: payload.clone(),
            projection: payload,
        };
        // Without the recovered false state a completion has no dispatch.
        if compare {
            assert!(
                journal
                    .append_if_tail(operation, 2, "complete", &completed, Some(&io))?
                    .await?
            );
        } else {
            journal
                .append(operation, "complete", &completed, Some(&io))?
                .await?;
        }
        {
            let cache = journal.verified.lock().await;
            let state = cache
                .summary()
                .recovery
                .ok_or_else(|| invalid(Some(&io)))?
                .state;
            assert!(state.roots[3].is_none(), "tool state was persisted twice");
        }
        *journal.verified.lock().await = SummaryCache::default();
        if compare {
            assert!(
                journal
                    .append_if_tail(operation, 3, "repeated-start", &started, Some(&io))?
                    .await
                    .is_err()
            );
        } else {
            assert!(
                journal
                    .append(operation, "repeated-start", &started, Some(&io))?
                    .await
                    .is_err()
            );
        }
        *journal.verified.lock().await = SummaryCache::default();
        if compare {
            assert!(
                journal
                    .append_if_tail(operation, 3, "refusal", &refusal(8), Some(&io))?
                    .await?
            );
        } else {
            journal
                .append(operation, "refusal", &refusal(8), Some(&io))?
                .await?;
        }
        *journal.verified.lock().await = SummaryCache::default();
        // A refusal settled without a dispatch still excludes another refusal.
        if compare {
            assert!(
                journal
                    .append_if_tail(operation, 4, "repeated-refusal", &refusal(8), Some(&io))?
                    .await
                    .is_err()
            );
        } else {
            assert!(
                journal
                    .append(operation, "repeated-refusal", &refusal(8), Some(&io))?
                    .await
                    .is_err()
            );
        }
        drop(journal);
        assert_eq!(io.owned_bytes()?, 0);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}

#[tokio::test]
async fn indexed_selection_includes_only_fully_validated_raw_tail() -> Result<()> {
    let (journal, _issuer) = crate::filesystem::execution_journal::test_support::journal().await?;
    let operation = OperationId::new();
    journal
        .append(
            operation,
            "start",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    let path = acyclic_stream::StreamPath::new(format!("harness/v2/execution/{operation}"))?;
    let record = serde_json::to_vec(&Observation {
        version: OBSERVATION_VERSION,
        operation_id: operation,
        turn_operation: journal.turn_operation(operation),
        retry_digest: "raw-tail".into(),
        event: refusal(0),
    })
    .map_err(|_| invalid(None))?;
    journal
        .stream
        .append(acyclic_stream::AppendRequest {
            path: path.clone(),
            records: vec![record.into()],
            if_tail: Some(1),
            idempotency_key: acyclic_stream::IdempotencyKey::unique(),
        })
        .await?;
    let selection = journal
        .indexed_records(
            ExecutionRecordQuery {
                operation,
                step: 0,
                include_started: true,
                maximum_step_records: 1,
                maximum: 2,
                select: &|_| true,
            },
            None,
        )?
        .ok_or_else(|| invalid(None))?
        .await?;
    assert!(
        selection.is_some(),
        "validated raw tail disabled indexed selection"
    );
    let (tail, selected) = selection.ok_or_else(|| invalid(None))?;
    assert_eq!(tail, 2);
    assert_eq!(
        selected.len(),
        2,
        "indexed selection omitted the validated raw tail"
    );
    assert!(matches!(selected[0].event, ExecutionEvent::Started { .. }));
    assert!(matches!(
        selected[1].event,
        ExecutionEvent::ToolFailed { step: 0, .. }
    ));
    let records = journal.replay(operation, 0, 10, None)?.await?;
    assert_eq!(records.len(), 2);
    // A record at the same storage path is not sufficient authority. It must
    // belong to this operation before it can enter the pending step chain.
    let forged = serde_json::to_vec(&Observation {
        version: OBSERVATION_VERSION,
        operation_id: OperationId::new(),
        turn_operation: journal.turn_operation(operation),
        retry_digest: "forged-tail".into(),
        event: refusal(0),
    })
    .map_err(|_| invalid(None))?;
    journal
        .stream
        .append(acyclic_stream::AppendRequest {
            path,
            records: vec![forged.into()],
            if_tail: Some(2),
            idempotency_key: acyclic_stream::IdempotencyKey::unique(),
        })
        .await?;
    for cold in [false, true] {
        if cold {
            *journal.verified.lock().await = SummaryCache::default();
        }
        assert!(
            journal
                .indexed_records(
                    ExecutionRecordQuery {
                        operation,
                        step: 0,
                        include_started: true,
                        maximum_step_records: 2,
                        maximum: 3,
                        select: &|_| true,
                    },
                    None
                )?
                .ok_or_else(|| invalid(None))?
                .await
                .is_err(),
            "indexed selection accepted a raw tail from another operation"
        );
    }
    Ok(())
}

#[tokio::test]
async fn refused_selections_release_partial_output_on_both_ledgers() -> Result<()> {
    for native in [false, true] {
        let (journal, _issuer) =
            crate::filesystem::execution_journal::test_support::journal().await?;
        let operation = OperationId::new();
        journal
            .append(
                operation,
                "start",
                &ExecutionEvent::Started {
                    request_digest: [1; 32],
                },
                None,
            )?
            .await?;
        journal
            .append(operation, "refusal", &refusal(0), None)?
            .await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(16 * 1024 * 1024, 4 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        for protected in [false, true] {
            let temporary = custody::QueryIo::new(Some(&io))?;
            let original = temporary.io().ok_or_else(|| invalid(Some(&io)))?;
            let query = ExecutionRecordQuery {
                operation,
                step: 0,
                include_started: true,
                maximum_step_records: 1,
                maximum: 1,
                select: &|_| true,
            };
            let result = if protected {
                journal
                    .indexed_records(query, Some(original))?
                    .ok_or_else(|| invalid(Some(original)))?
                    .await
            } else {
                // The former unguarded entry point abandons a copied Started
                // record when the second selected record exceeds the bound.
                journal.indexed_step_inner(query, Some(original)).await
            };
            assert!(result.is_err());
            if protected {
                assert_eq!(
                    original.owned_bytes()?,
                    0,
                    "rejected query retained partial output"
                );
            } else {
                assert!(
                    original.owned_bytes()? > 0,
                    "unguarded negative control did not retain partial output"
                );
            }
            drop(temporary);
        }
        drop(journal);
        assert_eq!(io.owned_bytes()?, 0);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}
