use super::*;

mod output;
mod prepared;
pub use crate::memory::NativeStreamOutput;
pub use output::{NativeProviderFuture, NativeRecordStream, NativeStreamResult};
pub(crate) use prepared::NativeCommitRecordSlot;
pub use prepared::PreparedNativeCommit;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn capture_commit(
    source: &CommitRequest,
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> std::io::Result<crate::memory::NativeStreamOutput<CommitRequest>> {
    let mut funding = crate::memory::NativeStreamOutput::fund(Some(meter));
    let mut conditions = funding.vector(source.conditions.len())?;
    for condition in &source.conditions {
        funding.examine(1)?;
        funding.copy_admission(std::mem::size_of::<CommitCondition>())?;
        conditions.push(condition.clone());
        funding.copy_completed(std::mem::size_of::<CommitCondition>())?;
    }
    let mut mutations = funding.vector(source.mutations.len())?;
    for mutation in &source.mutations {
        funding.examine(1)?;
        let mutation = match mutation {
            CommitMutation::Append { path, records } => CommitMutation::Append {
                path: path.clone(),
                records: funding.records(records)?,
            },
            CommitMutation::Fork {
                source,
                destination,
                at_tail,
                records,
            } => CommitMutation::Fork {
                source: source.clone(),
                destination: destination.clone(),
                at_tail: *at_tail,
                records: funding.records(records)?,
            },
        };
        funding.copy_admission(std::mem::size_of::<CommitMutation>())?;
        mutations.push(mutation);
        funding.copy_completed(std::mem::size_of::<CommitMutation>())?;
    }
    Ok(funding.map(|_| CommitRequest {
        conditions,
        mutations,
        idempotency_key: source.idempotency_key.clone(),
    }))
}
