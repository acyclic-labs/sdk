use super::*;

use acyclic_fs::{CancellationToken, WorkBudget, WorkCounters};

use std::sync::atomic::Ordering;

type Provider = crate::test_stream::LostSessionAck<MemoryStream>;

async fn open_history(
    client: &Arc<Provider>,
    authority: Authority,
    verifier: AuthorityVerifier,
    schemas: SchemaRegistry,
    revision: Option<u64>,
    limits: HistoryReadLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<StreamAggregate<Provider>>, Error> {
    let mut work = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?,
    );
    let result = async {
        work.preflight()?;
        StreamAggregate::open(
            client,
            authority,
            verifier,
            schemas,
            None,
            limits,
            DEFAULT_TERMINAL_EFFECTS,
            revision,
            &mut Some(&mut work),
        )
        .await
    }
    .await;
    work.finish(result)
}

fn budget() -> WorkBudget {
    let bytes = 64 * acyclic_stream::MAX_COMMAND_BYTES as u64;
    WorkBudget {
        authority_records_read: 128,
        authority_bytes_read: bytes,
        backend_read_operations: 128,
        source_bytes_read: bytes,
        bytes_copied: bytes,
        bytes_encoded: bytes,
        bytes_hashed: bytes,
        output_bytes: bytes,
        items_examined: 1_000_000,
        items_returned: 128,
        allocation_operations: 1_000_000,
        peak_allocation_bytes: bytes,
        ..WorkBudget::default()
    }
}

async fn fixture() -> Result<Arc<Provider>> {
    let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let mut writer = with_content(&provider).await?;
    for revision in 0..69 {
        writer
            .execute(
                projection_command(u128::from(revision + 1), revision)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    provider.forbid_writes.store(true, Ordering::SeqCst);
    provider.reset_io();
    Ok(provider)
}

mod checkpoints;
mod effects;
mod historical;
mod reader;
