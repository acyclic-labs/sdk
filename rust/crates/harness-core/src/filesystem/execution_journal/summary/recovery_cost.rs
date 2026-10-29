//! A reopened append loads the authenticated cut and only its relevant keys.
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
async fn cold_append_recovery_measurements() -> Result<()> {
    for native in [false, true] {
        check_cold_recovery(native).await?;
    }
    Ok(())
}

async fn check_cold_recovery(native: bool) -> Result<()> {
    for count in [1, 100, 1_000] {
        let (original, _issuer) =
            crate::filesystem::execution_journal::test_support::journal().await?;
        let operation = OperationId::new();
        original
            .append(
                operation,
                "start",
                &ExecutionEvent::Started {
                    request_digest: [1; 32],
                },
                None,
            )?
            .await?;
        for step in 0..count {
            original
                .append(operation, &format!("failed:{step}"), &refusal(step), None)?
                .await?;
        }
        let reopened = FilesystemExecutionJournal::new(
            original.stream.clone(),
            original.host.clone(),
            crate::filesystem::FilesystemJournalBinding {
                volume: &original.volume,
                verifier: &original.verifier,
                scope: &original.scope,
                io: None,
            },
        )?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(256 * 1024 * 1024, 4 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let native_baseline = work.live_allocation_bytes();
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let before = io.receipt();
        let started = std::time::Instant::now();
        reopened
            .append(operation, "after-reopen", &refusal(count), Some(&io))?
            .await?;
        let after = io.receipt();
        eprintln!(
            "cold append native={native} steps={count} elapsed_us={} records_received={} authority_bytes={} hashed_bytes={} owned_bytes={}",
            started.elapsed().as_micros(),
            after.authority_records_read - before.authority_records_read,
            after.authority_bytes_read - before.authority_bytes_read,
            after.bytes_hashed - before.bytes_hashed,
            io.owned_bytes()?,
        );
        let history_reads = (after.authority_records_read - before.authority_records_read)
            - (after.authority_records_appended - before.authority_records_appended);
        // A head and its anchor, then at most four fixed-width key traversals:
        // preload retry/tool state and stage their replacements. A traversal
        // has at most 256 branches and one leaf, regardless of history length.
        assert!(
            history_reads <= 2 + 4 * 257,
            "recovery read beyond its authenticated key paths"
        );
        {
            let cache = reopened.verified.lock().await;
            let summary = cache.summary();
            assert!(
                summary.partial,
                "recovery rebuilt the full in-memory journal"
            );
            assert_eq!(summary.tail, u64::from(count) + 2);
            assert_eq!(summary.retries.entries().count(), 1);
            assert_eq!(summary.tools.entries().count(), 1);
            assert_eq!(summary.contexts.entries().count(), 0);
            assert_eq!(summary.models.entries().count(), 0);
        }
        check_full_fold(&reopened, operation, count, &io).await?;
        drop(reopened);
        drop(original);
        assert_eq!(
            io.owned_bytes()?,
            0,
            "recovered index retained custody after drop"
        );
        if native {
            assert_eq!(
                work.live_allocation_bytes(),
                native_baseline,
                "recovered index retained original native allocations"
            );
        }
    }
    Ok(())
}

async fn check_full_fold<P, A, O>(
    journal: &FilesystemExecutionJournal<P, A, O>,
    operation: OperationId,
    count: u32,
    io: &PreparationIo,
) -> Result<()>
where
    P: StreamProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    // Explicitly exercise the old full-fold path as the recovery control.
    let before = io.receipt();
    let owned = io.owned_bytes()?;
    let mut summary = ExecutionSummary {
        operation: Some(operation),
        ..Default::default()
    };
    journal
        .refresh_summary(operation, &mut summary, Some(io))
        .await?;
    let hold = io.guard_owned(io.owned_since(owned)?)?;
    assert_eq!(summary.tail, u64::from(count) + 2);
    assert!(
        summary.retries.entries().count() > 1,
        "full-fold negative control did not violate the recovered-state bound"
    );
    assert_eq!(
        io.receipt().authority_records_read - before.authority_records_read,
        u64::from(count) + 2,
        "full-fold negative control did not read the entire history"
    );
    drop(summary);
    drop(hold);
    Ok(())
}
