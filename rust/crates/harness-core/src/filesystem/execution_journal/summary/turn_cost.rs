use super::*;
use crate::executor::{ToolErrorClass, ToolFailureCause, ToolFailureKind};
use crate::filesystem::execution_journal::test_support::journal;

#[tokio::test]
async fn authenticated_replay_defers_payload_loading_until_consumption() -> Result<()> {
    for native in [false, true] {
        check_replay_payload_cost(native).await?;
    }
    Ok(())
}

async fn check_replay_payload_cost(native: bool) -> Result<()> {
    let (journal, _issuer) = journal().await?;
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
    let payload = vec![b'x'; 16_384];
    let reference = journal
        .stage_with_io(operation, "invocation", &payload, "text/plain", None)
        .await?;
    journal
        .append(
            operation,
            "tool",
            &ExecutionEvent::ToolStarted {
                step: 0,
                call_id: "call".into(),
                invocation: reference.clone(),
                publication: None,
            },
            None,
        )?
        .await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(16 * 1024 * 1024, 4 * 1024 * 1024);
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = if native {
        PreparationIo::from_native_work(&work)?
    } else {
        PreparationIo::from_budget(budget, &cancellation)?
    };
    for (cold, eager) in [(false, false), (true, false), (true, true)] {
        if cold {
            *journal.verified.lock().await = SummaryCache::default();
        }
        let before = io.receipt();
        let owned = io.owned_bytes()?;
        let count = if eager {
            // The former replay path consumes every referenced payload.
            let records = journal
                .replay_page(operation, 1, 1, true, Some(&io))
                .await?;
            let guard = io.guard_owned(io.owned_since(owned)?)?;
            let count = records.len();
            drop(records);
            drop(guard);
            count
        } else {
            let records = journal.replay(operation, 1, 1, Some(&io))?.await?;
            let guard = io.guard_owned(io.owned_since(owned)?)?;
            let count = records.len();
            drop(records);
            drop(guard);
            count
        };
        assert_eq!(count, 1);
        let after = io.receipt();
        eprintln!(
            "replay native={native} cold={cold} eager={eager} source_bytes={} authority_bytes={} object_bytes={} hashed_bytes={}",
            after.source_bytes_read - before.source_bytes_read,
            after.authority_bytes_read - before.authority_bytes_read,
            after.object_bytes_read - before.object_bytes_read,
            after.bytes_hashed - before.bytes_hashed,
        );
        let loaded = after.object_bytes_read - before.object_bytes_read;
        if eager {
            assert!(
                loaded >= payload.len() as u64,
                "the old replay path must detect the eager payload negative control"
            );
        } else {
            assert_eq!(loaded, 0, "authenticated replay reloaded a payload");
        }
    }
    let bytes = journal.load(&reference, Some(&io))?.await?;
    let guard = io.guard_bytes(&bytes)?;
    assert_eq!(bytes.as_ref(), payload);
    drop(bytes);
    drop(guard);
    Ok(())
}

#[tokio::test]
async fn a_second_writer_refreshes_the_authenticated_cut_and_preserves_retry_and_compare_semantics()
-> Result<()> {
    let (first, _issuer) = journal().await?;
    let second = FilesystemExecutionJournal::new(
        first.stream.clone(),
        first.host.clone(),
        crate::filesystem::FilesystemJournalBinding {
            volume: &first.volume,
            verifier: &first.verifier,
            scope: &first.scope,
            io: None,
        },
    )?;
    let operation = OperationId::new();
    let io = PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(16 * 1024 * 1024, 4 * 1024 * 1024),
        &acyclic_fs::CancellationToken::new(),
    )?;
    let failure = |step, message: &str| ExecutionEvent::ToolFailed {
        step,
        call_id: "call".into(),
        reason: ToolFailureKind::Unavailable,
        cause: ToolFailureCause {
            class: ToolErrorClass::Invalid,
            message: message.into(),
        },
    };
    first
        .append(
            operation,
            "started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            Some(&io),
        )?
        .await?;
    assert!(
        first
            .append_if_tail(operation, 1, "first", &failure(0, "unavailable"), Some(&io))?
            .await?
    );
    assert!(
        second
            .append_if_tail(
                operation,
                2,
                "second",
                &failure(1, "unavailable"),
                Some(&io)
            )?
            .await?
    );
    let before = io.receipt();
    assert!(
        !first
            .append_if_tail(operation, 2, "third", &failure(2, "unavailable"), Some(&io))?
            .await?
    );
    // The checkpoint and anchor are two records. Each of the two validation
    // keys traverses at most three pages in these three-entry maps.
    let reads = io.receipt().authority_records_read - before.authority_records_read;
    assert!(
        (2..=8).contains(&reads),
        "writer refreshed beyond the authenticated cut: {reads}"
    );
    assert!(
        first
            .append_if_tail(operation, 1, "first", &failure(0, "unavailable"), Some(&io))?
            .await?
    );
    assert!(
        !first
            .append_if_tail(operation, 2, "first", &failure(0, "unavailable"), Some(&io))?
            .await?
    );
    let Err(error) = first
        .append(operation, "first", &failure(0, "different"), Some(&io))?
        .await
    else {
        return Err(Error::Invalid("changed retry was accepted".into()));
    };
    assert_eq!(error.code(), crate::wire::ErrorCode::Conflict);
    Ok(())
}

#[test]
fn quiescence_work_is_flat_for_a_thousand_settled_dispatches() -> Result<()> {
    let mut summary = ExecutionSummary::default();
    let io = PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(16 * 1024 * 1024, 4 * 1024 * 1024),
        &acyclic_fs::CancellationToken::new(),
    )?;
    super::accept(
        &mut summary,
        ExecutionEvent::Started {
            request_digest: [1; 32],
        },
    )?;
    for step in 0..1_000 {
        super::accept(
            &mut summary,
            ExecutionEvent::ToolFailed {
                step,
                call_id: "call".into(),
                reason: ToolFailureKind::Unavailable,
                cause: ToolFailureCause {
                    class: ToolErrorClass::Invalid,
                    message: "unavailable".into(),
                },
            },
        )?;
        let before = io.receipt().items_examined;
        assert!(summary.quiescent(Some(&io))?);
        assert_eq!(io.receipt().items_examined - before, 1);
    }
    Ok(())
}
