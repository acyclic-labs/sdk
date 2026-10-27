use super::*;

mod output;
mod prepared;
pub use output::NativeStreamOutput;
pub use output::{NativeProviderFuture, NativeRecordStream, NativeStreamResult};
pub(crate) use prepared::NativeCommitRecordSlot;
pub use prepared::PreparedNativeCommit;

pub(crate) fn vector<T>(
    length: usize,
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    allocation: &mut acyclic_native_runtime::ReadAllocation,
) -> std::io::Result<Vec<T>> {
    let bytes = length
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(std::io::ErrorKind::OutOfMemory)?;
    let mut claim = acyclic_native_runtime::ReadAllocation::new(meter, bytes)?;
    let mut value = Vec::new();
    value
        .try_reserve_exact(length)
        .map_err(|error| meter.allocation_failed(error))?;
    let excess = value
        .capacity()
        .saturating_sub(length)
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(std::io::ErrorKind::OutOfMemory)?;
    claim.reconcile_capacity(
        bytes
            .checked_add(excess)
            .ok_or(std::io::ErrorKind::OutOfMemory)?,
    )?;
    allocation.absorb(&mut claim)?;
    Ok(value)
}

pub(crate) fn clone_commit(
    value: &CommitOutcome,
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> std::io::Result<NativeStreamOutput<CommitOutcome>> {
    fn copy<T: Clone>(
        source: &[T],
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        allocation: &mut acyclic_native_runtime::ReadAllocation,
    ) -> std::io::Result<Vec<T>> {
        let mut output = vector(source.len(), meter, allocation)?;
        for value in source {
            meter.admit_examination(1, std::mem::size_of::<T>() as u64)?;
            output.push(value.clone());
            meter.record_examination(1, std::mem::size_of::<T>() as u64)?;
        }
        Ok(output)
    }
    let mut allocation = acyclic_native_runtime::ReadAllocation::empty(meter);
    let value = match value {
        CommitOutcome::Committed(receipt) => CommitOutcome::Committed(CommitReceipt {
            commit_id: receipt.commit_id,
            committed_at_micros: receipt.committed_at_micros,
            appends: copy(&receipt.appends, meter, &mut allocation)?,
            forks: copy(&receipt.forks, meter, &mut allocation)?,
        }),
        CommitOutcome::Conflict(source) => {
            CommitOutcome::Conflict(copy(source, meter, &mut allocation)?)
        }
    };
    meter.check()?;
    Ok(NativeStreamOutput::new(value, allocation))
}
pub(crate) fn clone_outcome(
    value: &IdempotencyOutcome,
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> std::io::Result<NativeStreamOutput<IdempotencyOutcome>> {
    match value {
        IdempotencyOutcome::Commit(value) => clone_commit(value, meter)
        .map(|output| output.map(IdempotencyOutcome::Commit)),
        IdempotencyOutcome::Append(value) => Ok(NativeStreamOutput::new(
            IdempotencyOutcome::Append(value.clone()),
            acyclic_native_runtime::ReadAllocation::empty(meter),
        )),
        IdempotencyOutcome::Fork(value) => Ok(NativeStreamOutput::new(
            IdempotencyOutcome::Fork(value.clone()),
            acyclic_native_runtime::ReadAllocation::empty(meter),
        )),
    }
}
