use super::*;

mod failure;
mod follow;
mod output;
mod prepared;
mod request;
mod work;
pub use failure::LocalStreamFailure;
pub use follow::{boxed_stream, follow_records, validate_follow};
pub use output::NativeStreamOutput;
pub use output::{NativeProviderFuture, NativeRecordStream, NativeStreamResult};
pub use prepared::NativeCommitRecordSlot;
pub use prepared::PreparedNativeCommit;
pub use work::Work;

pub use crate::alloc::{copy_vector, vector};

#[macro_export]
macro_rules! span {
    ($name:literal) => {
        acyclic_native_runtime::obs::OperationSpan::new(tracing::info_span!(
            $name,
            outcome = tracing::field::Empty,
            error.kind = tracing::field::Empty,
        ))
    };
}
pub use crate::span;

use acyclic_native_runtime::obs::kind as io_kind;

pub fn outcome<T, E>(
    result: &NativeStreamResult<T, E>,
    kind: impl FnOnce(&E) -> &'static str,
) -> Option<&'static str> {
    match result {
        Err(error) => Some(io_kind(error)),
        Ok(Err(error)) => Some(kind(error)),
        Ok(Ok(_)) => None,
    }
}

pub fn clone_envelope(
    envelope: &CommittedEnvelope,
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    kind: acyclic_native_runtime::NativeOperationKind,
) -> std::io::Result<NativeStreamOutput<CommittedEnvelope>> {
    let mut allocation = acyclic_native_runtime::ReadAllocation::empty(meter);
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
                    record.payload_bytes(),
                )?)
            } else {
                None
            };
            meter.admit_examination(1, std::mem::size_of::<Record>() as u64)?;
            records.push(record.clone());
            let read = bytes
                .as_mut()
                .map_or(Ok(()), |bytes| bytes.record(record.payload_bytes()));
            let examined = meter.record_examination(1, std::mem::size_of::<Record>() as u64);
            read?;
            examined?;
        }
        mutations.push(match mutation {
            CommittedMutation::Append(append) => CommittedMutation::Append(CommittedAppend {
                path: append.path.clone(),
                start: append.start,
                end: append.end,
                tail: append.tail,
                records,
            }),
            CommittedMutation::Fork(fork) => {
                let mut exclude = vector(fork.exclude.len(), meter, &mut allocation)?;
                for path in &fork.exclude {
                    let copied = std::mem::size_of::<crate::RelativePath>() as u64;
                    meter.admit_examination(1, copied)?;
                    exclude.push(path.clone());
                    meter.record_examination(1, copied)?;
                }
                CommittedMutation::Fork(CommittedFork {
                    exclude,
                    source: fork.source.clone(),
                    destination: fork.destination.clone(),
                    forked_at: fork.forked_at,
                    tail: fork.tail,
                    records,
                })
            }
        });
        meter.record_examination(1, 0)?;
    }
    let value = CommittedEnvelope {
        commit_id: envelope.commit_id,
        committed_at_micros: envelope.committed_at_micros,
        mutations,
    };
    meter.check()?;
    Ok(NativeStreamOutput::new(value, allocation))
}
pub fn clone_outcome(
    value: &IdempotencyOutcome,
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> std::io::Result<NativeStreamOutput<IdempotencyOutcome>> {
    match value {
        IdempotencyOutcome::Commit(value) => clone_envelope(
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
