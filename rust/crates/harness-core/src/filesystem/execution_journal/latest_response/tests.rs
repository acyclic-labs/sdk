//! The passive approval cursor is authenticated with the same cut as its step index.
use super::*;
use crate::executor::{
    ExecutionReplay, ModelPurpose, ToolErrorClass, ToolFailureCause, ToolFailureKind,
};

async fn replay_latest<P, A, O>(
    journal: &FilesystemExecutionJournal<P, A, O>,
    operation: OperationId,
    io: Option<&PreparationIo>,
) -> Result<Option<u32>>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    let scratch = crate::filesystem::PreparationScope::new(io)?;
    let mut replay = ExecutionReplay::new(operation);
    let mut latest = None;
    while let Some(page) = replay.next_page(journal, scratch.io()).await? {
        for record in page {
            if let ExecutionEvent::ModelStarted {
                step,
                purpose: ModelPurpose::Response,
                ..
            } = record.event
            {
                latest = Some(latest.map_or(step, |previous: u32| previous.max(step)));
            }
        }
    }
    drop(replay);
    scratch.finish(0)?;
    Ok(latest)
}

#[tokio::test]
async fn latest_response_cursor_has_flat_reads_and_cold_recovery_through_one_thousand_steps()
-> Result<()> {
    let (journal, _issuer) = test_support::journal().await?;
    let operation = OperationId::new();
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
    assert_eq!(journal.latest_response_step(operation, None).await?, None);
    let request = crate::model::PreparedModelRequest::prepare(
        crate::model::ModelRequest {
            model: crate::model::Model::new(
                "fixture".into(),
                "cursor".into(),
                "1".into(),
                serde_json::Value::Null,
            )?,
            messages: vec![crate::model::ModelMessage {
                role: crate::model::ModelRole::User,
                content: ModelContent::Text("question".into()),
            }],
            tools: Vec::new(),
            max_output_tokens: None,
        },
        Limits::default(),
        None,
    )?;
    let reference = journal
        .stage_prepared_model_request(operation, "request", &request, None, None)?
        .ok_or_else(|| Error::Storage("fixture request manifest is unavailable".into()))?
        .await?;
    assert!(!journal.model_was_dispatched(operation, None).await?);
    journal
        .append(
            operation,
            "initial-summary",
            &ExecutionEvent::ModelStarted {
                step: 2_000,
                purpose: ModelPurpose::Summary,
                request_digest: request.request_digest(),
                request: reference.clone(),
            },
            None,
        )?
        .await?;
    *journal.verified.lock().await = SummaryCache::default();
    assert!(
        journal.model_was_dispatched(operation, None).await?,
        "summary dispatch was invisible to reconciliation"
    );
    assert_eq!(
        journal.latest_response_step(operation, None).await?,
        None,
        "summary dispatch advanced the response cursor"
    );
    journal
        .append(
            operation,
            "stop-initial-summary",
            &ExecutionEvent::ModelStopped {
                step: 2_000,
                purpose: ModelPurpose::Summary,
            },
            None,
        )?
        .await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(256 * 1024 * 1024, 4 * 1024 * 1024);
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    let finite = PreparationIo::from_budget(budget, &cancellation)?;
    let native = PreparationIo::from_native_work(&work)?;
    for step in 0..1_000 {
        journal
            .append(
                operation,
                &format!("tool:{step}"),
                &ExecutionEvent::ToolFailed {
                    step,
                    call_id: "call".into(),
                    reason: ToolFailureKind::Unavailable,
                    cause: ToolFailureCause {
                        class: ToolErrorClass::Invalid,
                        message: "unavailable".into(),
                    },
                },
                None,
            )?
            .await?;
        if ![0, 9, 99, 999].contains(&step) {
            continue;
        }
        for purpose in [ModelPurpose::Response, ModelPurpose::Summary] {
            // Summary ordinals deliberately exceed the response cursor.
            let ordinal = if purpose == ModelPurpose::Summary {
                step + 1_000
            } else {
                step
            };
            journal
                .append(
                    operation,
                    &format!("model:{ordinal}"),
                    &ExecutionEvent::ModelStarted {
                        step: ordinal,
                        purpose,
                        request_digest: request.request_digest(),
                        request: reference.clone(),
                    },
                    None,
                )?
                .await?;
            journal
                .append(
                    operation,
                    &format!("stopped:{ordinal}"),
                    &ExecutionEvent::ModelStopped {
                        step: ordinal,
                        purpose,
                    },
                    None,
                )?
                .await?;
        }
        for io in [&finite, &native] {
            for cold in [true, false] {
                if cold {
                    *journal.verified.lock().await = SummaryCache::default();
                }
                let owned = io.owned_bytes()?;
                let before = io.receipt();
                let started = std::time::Instant::now();
                assert_eq!(
                    journal.latest_response_step(operation, Some(io)).await?,
                    Some(step)
                );
                let elapsed = started.elapsed().as_micros();
                let after = io.receipt();
                let reads = after.authority_records_read - before.authority_records_read;
                eprintln!(
                    "latest_response steps={} native={} cold={cold} reads={reads} payload_bytes={} elapsed_us={elapsed}",
                    step + 1,
                    io.original_native_work().is_some(),
                    after.object_bytes_read - before.object_bytes_read
                );
                assert!(
                    reads <= 2,
                    "latest response cursor replayed historical records: {reads}"
                );
                assert_eq!(
                    after.object_bytes_read - before.object_bytes_read,
                    0,
                    "latest response cursor loaded historical payloads"
                );
                assert_eq!(
                    io.owned_bytes()?,
                    owned,
                    "latest response cursor retained query scratch"
                );
                let before = io.receipt();
                assert!(journal.model_was_dispatched(operation, Some(io)).await?);
                assert_eq!(
                    io.receipt().authority_records_read - before.authority_records_read,
                    0,
                    "reconciliation replayed the cached dispatch prefix"
                );
            }
            // The former scan is an actual negative control using the public replay entry point.
            // Each replay is a separate public operation. Its historical
            // work must not consume the positive cursor operation's budget.
            let control_cancellation = acyclic_fs::CancellationToken::new();
            let control_work =
                acyclic_fs::NativeMaterializationWork::new(budget, &control_cancellation)
                    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let control_baseline = control_work.live_allocation_bytes();
            let control_io = if io.original_native_work().is_some() {
                PreparationIo::from_native_work(&control_work)?
            } else {
                PreparationIo::from_budget(budget, &control_cancellation)?
            };
            let before = control_io.receipt();
            let started = std::time::Instant::now();
            assert_eq!(
                replay_latest(&journal, operation, Some(&control_io)).await?,
                Some(step)
            );
            let reads = control_io.receipt().authority_records_read - before.authority_records_read;
            eprintln!(
                "latest_response_replay_control steps={} reads={reads} elapsed_us={}",
                step + 1,
                started.elapsed().as_micros()
            );
            assert!(
                reads >= u64::from(step) + 6,
                "negative control failed to read the historical prefix"
            );
            assert!(
                reads > 2,
                "the flat read oracle accepted the replay control"
            );
            assert_eq!(control_io.owned_bytes()?, 0);
            assert_eq!(control_work.live_allocation_bytes(), control_baseline);
        }
    }
    // A later publication at a lower ordinal cannot move the maximum backwards.
    journal
        .append(
            operation,
            "older-response",
            &ExecutionEvent::ModelStarted {
                step: 3,
                purpose: ModelPurpose::Response,
                request_digest: request.request_digest(),
                request: reference.clone(),
            },
            None,
        )?
        .await?;
    assert_eq!(
        journal.latest_response_step(operation, None).await?,
        Some(999)
    );
    // A raw append beyond the signed cut must be validated and included, never
    // silently hidden by the cached ordinal or cold recovery checkpoint.
    let path = crate::distributed::execution_path(operation, &mut None)?;
    let tail = journal.execution_tail(operation, None).await?;
    let value = serde_json::to_vec(&Observation {
        version: OBSERVATION_VERSION,
        operation_id: operation,
        turn_operation: operation,
        retry_digest: "raw-response".into(),
        event: ExecutionEvent::ModelStarted {
            step: 1_001,
            purpose: ModelPurpose::Response,
            request_digest: request.request_digest(),
            request: reference,
        },
    })
    .map_err(|error| Error::Invalid(error.to_string()))?;
    journal
        .stream
        .append(acyclic_stream::AppendRequest {
            path,
            records: vec![value.into()],
            if_tail: Some(tail),
            idempotency_key: None,
        })
        .await?;
    for io in [&finite, &native] {
        *journal.verified.lock().await = SummaryCache::default();
        let before = io.receipt();
        assert_eq!(
            journal.latest_response_step(operation, Some(io)).await?,
            Some(1_001),
            "unindexed response tail was omitted"
        );
        let reads = io.receipt().authority_records_read - before.authority_records_read;
        assert!(
            reads < 128,
            "raw-tail recovery replayed the authenticated prefix: {reads}"
        );
        let control_cancellation = acyclic_fs::CancellationToken::new();
        let control_work =
            acyclic_fs::NativeMaterializationWork::new(budget, &control_cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let control_baseline = control_work.live_allocation_bytes();
        let control_io = if io.original_native_work().is_some() {
            PreparationIo::from_native_work(&control_work)?
        } else {
            PreparationIo::from_budget(budget, &control_cancellation)?
        };
        let before = control_io.receipt();
        let control_scope = crate::filesystem::PreparationScope::new(Some(&control_io))?;
        let (_, replayed, replay_hold) = crate::executor::replay_execution(
            &journal,
            operation,
            1,
            |event| event.step() == Some(1_001),
            control_scope.io(),
        )
        .await?;
        assert_eq!(replayed.len(), 1);
        assert!(
            control_io.receipt().authority_records_read - before.authority_records_read > 1_000,
            "raw-tail replay control did not traverse the historical prefix"
        );
        drop(replayed);
        drop(replay_hold);
        control_scope.finish(0)?;
        assert_eq!(control_io.owned_bytes()?, 0);
        assert_eq!(control_work.live_allocation_bytes(), control_baseline);
        let before = io.receipt();
        let query_scope = crate::filesystem::PreparationScope::new(Some(io))?;
        let (selected_tail, records, hold) = crate::executor::selected_step_records(
            &journal,
            crate::executor::ExecutionRecordQuery {
                operation,
                step: 1_001,
                include_started: false,
                maximum_step_records: 1,
                maximum: 1,
                select: &|_| true,
            },
            query_scope.io(),
        )
        .await?;
        assert_eq!(selected_tail, tail + 1);
        assert_eq!(records.len(), 1);
        assert!(matches!(
            records[0].event,
            ExecutionEvent::ModelStarted {
                step: 1_001,
                purpose: ModelPurpose::Response,
                ..
            }
        ));
        let reads = io.receipt().authority_records_read - before.authority_records_read;
        assert!(
            reads < 128,
            "step selection replayed the authenticated prefix behind a raw tail: {reads}"
        );
        drop(records);
        drop(hold);
        query_scope.finish(0)?;
    }
    journal
        .append(
            operation,
            "stop-raw-response",
            &ExecutionEvent::ModelStopped {
                step: 1_001,
                purpose: ModelPurpose::Response,
            },
            Some(&finite),
        )?
        .await?;
    assert!(
        journal
            .append_if_tail(
                operation,
                tail + 2,
                "after-raw-tail",
                &ExecutionEvent::ToolFailed {
                    step: 1_002,
                    call_id: "next".into(),
                    reason: ToolFailureKind::Unavailable,
                    cause: ToolFailureCause {
                        class: ToolErrorClass::Invalid,
                        message: "unavailable".into()
                    },
                },
                Some(&native)
            )?
            .await?
    );
    let (_, records, hold) = crate::executor::selected_step_records(
        &journal,
        crate::executor::ExecutionRecordQuery {
            operation,
            step: 1_001,
            include_started: false,
            maximum_step_records: 2,
            maximum: 2,
            select: &|_| true,
        },
        Some(&native),
    )
    .await?;
    assert_eq!(
        records.len(),
        2,
        "catch-up publication omitted the raw tail from its step chain"
    );
    drop(records);
    drop(hold);
    let value = serde_json::to_vec(&Observation {
        version: OBSERVATION_VERSION,
        operation_id: operation,
        turn_operation: operation,
        retry_digest: "raw-response".into(),
        event: ExecutionEvent::ToolFailed {
            step: 1_003,
            call_id: "duplicate".into(),
            reason: ToolFailureKind::Unavailable,
            cause: ToolFailureCause {
                class: ToolErrorClass::Invalid,
                message: "unavailable".into(),
            },
        },
    })
    .map_err(|error| Error::Invalid(error.to_string()))?;
    journal
        .stream
        .append(acyclic_stream::AppendRequest {
            path: crate::distributed::execution_path(operation, &mut None)?,
            records: vec![value.into()],
            if_tail: Some(tail + 3),
            idempotency_key: None,
        })
        .await?;
    for io in [&finite, &native] {
        *journal.verified.lock().await = SummaryCache::default();
        assert!(
            journal
                .latest_response_step(operation, Some(io))
                .await
                .is_err(),
            "catch-up publication omitted a raw retry identity"
        );
    }
    drop(journal);
    assert_eq!(finite.owned_bytes()?, 0);
    assert_eq!(native.owned_bytes()?, 0);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}
