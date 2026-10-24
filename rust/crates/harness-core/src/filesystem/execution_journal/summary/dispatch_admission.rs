//! A funded cache update is part of admission, before the durable claim.
use super::*;

#[tokio::test]
async fn allocation_cuts_cannot_commit_an_unreported_tool_claim() -> Result<()> {
    for native in [false, true] {
        let (success, allocations) = run_cut(native, 1_000_000).await?;
        assert!(success);
        let mut refused = 0;
        let mut admitted = 0;
        for cutoff in 0..=allocations {
            if run_cut(native, cutoff).await?.0 {
                admitted += 1;
            } else {
                refused += 1;
            }
        }
        assert!(
            refused > 0 && admitted > 0,
            "allocation cuts never crossed admission"
        );
        eprintln!("tool claim admission native={native}: refused={refused} admitted={admitted}");
    }
    Ok(())
}

async fn run_cut(native: bool, cutoff: u64) -> Result<(bool, u64)> {
    let (journal, _issuer) = crate::filesystem::execution_journal::test_support::journal().await?;
    let operation = OperationId::from_bytes([83; 16]);
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
    let invocation = journal
        .stage_with_io(operation, "invocation", b"{}", "application/json", None)
        .await?;
    let mut budget = acyclic_fs::performance::work_budget(1_000_000, 4 * 1024 * 1024);
    budget.allocation_operations = cutoff;
    let cancellation = acyclic_fs::CancellationToken::new();
    let io = if native {
        acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
            .and_then(|work| PreparationIo::from_native_work(&work))
    } else {
        PreparationIo::from_budget(budget, &cancellation)
    };
    let (outcome, allocations) = match io {
        Ok(io) => {
            let event = ExecutionEvent::ToolStarted {
                step: 0,
                call_id: "call".into(),
                invocation,
            };
            let outcome = match journal.append(operation, "tool", &event, Some(&io)) {
                Ok(future) => future.await,
                Err(error) => Err(error),
            };
            (outcome, io.receipt().allocation_operations)
        }
        Err(error) => (Err(error), 0),
    };
    let records = journal.replay(operation, 0, 3, None)?.await?;
    let claims = records
        .iter()
        .filter(|record| matches!(record.event, ExecutionEvent::ToolStarted { .. }))
        .count();
    match outcome {
        Ok(()) => {
            assert_eq!(claims, 1);
            Ok((true, allocations))
        }
        Err(error) => {
            assert_eq!(
                claims, 0,
                "native={native} cutoff={cutoff}: claim committed before admission error {error}"
            );
            Ok((false, allocations))
        }
    }
}
