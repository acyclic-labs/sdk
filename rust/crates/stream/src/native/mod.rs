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
    kind: acyclic_native_runtime::NativeOperationKind,
) -> std::io::Result<NativeStreamOutput<CommitOutcome>> {
    let mut allocation = acyclic_native_runtime::ReadAllocation::empty(meter);
    let value = match value {
        CommitOutcome::Committed(envelope) => {
            let mut mutations = vector(envelope.mutations.len(), meter, &mut allocation)?;
            for mutation in &envelope.mutations {
                meter.admit_examination(1, 0)?;
                let source = match mutation {
                    CommittedMutation::Append(append) => &append.records,
                    CommittedMutation::Fork(fork) => &fork.records,
                };
                let mut records = vector(source.len(), meter, &mut allocation)?;
                for record in source {
                    let mut bytes = if kind == acyclic_native_runtime::NativeOperationKind::Read {
                        Some(acyclic_native_runtime::NativeByteReservation::new(
                            Arc::clone(meter),
                            kind,
                            record.value.len(),
                        )?)
                    } else {
                        None
                    };
                    meter.admit_examination(1, std::mem::size_of::<Record>() as u64)?;
                    records.push(record.clone());
                    let read = bytes
                        .as_mut()
                        .map_or(Ok(()), |bytes| bytes.record(record.value.len()));
                    let examined =
                        meter.record_examination(1, std::mem::size_of::<Record>() as u64);
                    read?;
                    examined?;
                }
                mutations.push(match mutation {
                    CommittedMutation::Append(append) => {
                        CommittedMutation::Append(CommittedAppend {
                            path: append.path.clone(),
                            start: append.start,
                            end: append.end,
                            tail: append.tail,
                            records,
                        })
                    }
                    CommittedMutation::Fork(fork) => CommittedMutation::Fork(CommittedFork {
                        source: fork.source.clone(),
                        destination: fork.destination.clone(),
                        forked_at: fork.forked_at,
                        tail: fork.tail,
                        records,
                    }),
                });
                meter.record_examination(1, 0)?;
            }
            CommitOutcome::Committed(CommittedEnvelope {
                commit_id: envelope.commit_id,
                mutations,
            })
        }
        CommitOutcome::Conflict(source) => {
            let mut conflicts = vector(source.len(), meter, &mut allocation)?;
            for conflict in source {
                meter.admit_examination(1, 0)?;
                conflicts.push(conflict.clone());
                meter.record_examination(1, 0)?;
            }
            CommitOutcome::Conflict(conflicts)
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
        IdempotencyOutcome::Commit(value) => clone_commit(
            value,
            meter,
            acyclic_native_runtime::NativeOperationKind::Read,
        )
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
