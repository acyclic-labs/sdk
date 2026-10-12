use super::*;

pub(super) fn clone_records(
    records: &[Record],
    meter: WireMeter<'_>,
    output_read: bool,
) -> MemoryResult<MemoryOutput<Vec<Record>>> {
    let mut output = vec(meter, records.len())?;
    for record in records {
        admit_item(meter)?;
        let mut read = if output_read { crate::memory::bytes(meter, false, record.value.len())? } else { None };
        output.value.push(record.clone());
        let examined = record_item(meter);
        let read = record_bytes(&mut read, record.value.len());
        examined.and(read)?;
        check(meter)?;
    }
    Ok(output)
}

pub(super) fn clone_envelope(
    envelope: &CommittedEnvelope,
    meter: WireMeter<'_>,
    output_read: bool,
) -> MemoryResult<MemoryOutput<CommittedEnvelope>> {
    let mut output = vec(meter, envelope.mutations.len())?;
    for mutation in &envelope.mutations {
        admit_item(meter)?;
        let cloned = match mutation {
            CommittedMutation::Append(append) => {
                let records = clone_records(&append.records, meter, output_read)?;
                absorb(&mut output.allocation, records.allocation)?;
                CommittedMutation::Append(crate::CommittedAppend {
                    path: append.path.clone(),
                    start: append.start,
                    end: append.end,
                    tail: append.tail,
                    records: records.value,
                })
            }
            CommittedMutation::Fork(fork) => {
                let records = clone_records(&fork.records, meter, output_read)?;
                absorb(&mut output.allocation, records.allocation)?;
                CommittedMutation::Fork(CommittedFork {
                    source: fork.source.clone(),
                    destination: fork.destination.clone(),
                    forked_at: fork.forked_at,
                    tail: fork.tail,
                    records: records.value,
                })
            }
        };
        output.value.push(cloned);
        record_item(meter)?;
    }
    Ok(output.map(|mutations| CommittedEnvelope { commit_id: envelope.commit_id, mutations }))
}

pub(super) fn clone_commit_outcome(
    outcome: &CommitOutcome,
    meter: WireMeter<'_>,
    output_read: bool,
) -> MemoryResult<MemoryOutput<CommitOutcome>> {
    match outcome {
        CommitOutcome::Committed(envelope) => {
            let output = clone_envelope(envelope, meter, output_read)?;
            Ok(output.map(CommitOutcome::Committed))
        }
        CommitOutcome::Conflict(conflicts) => {
            let mut output = vec(meter, conflicts.len())?;
            for conflict in conflicts {
                admit_item(meter)?;
                output.value.push(conflict.clone());
                record_item(meter)?;
            }
            Ok(output.map(CommitOutcome::Conflict))
        }
    }
}

pub(super) fn clone_idempotency_outcome(
    outcome: &IdempotencyOutcome,
    meter: WireMeter<'_>,
) -> MemoryResult<MemoryOutput<IdempotencyOutcome>> {
    match outcome {
        IdempotencyOutcome::Commit(outcome) => {
            let output = clone_commit_outcome(outcome, meter, true)?;
            Ok(output.map(IdempotencyOutcome::Commit))
        }
        IdempotencyOutcome::Append(outcome) => Ok(MemoryOutput {
            value: IdempotencyOutcome::Append(outcome.clone()),
            allocation: allocate(meter, 0)?,
        }),
        IdempotencyOutcome::Fork(outcome) => Ok(MemoryOutput {
            value: IdempotencyOutcome::Fork(outcome.clone()),
            allocation: allocate(meter, 0)?,
        }),
    }
}

pub(super) fn record_iterator(
    (records, mut terminal): (MemoryOutput<Vec<Record>>, Option<std::io::Error>),
) -> impl Iterator<Item = std::io::Result<Record>> {
    let mut records = records.map(Vec::into_iter);
    std::iter::from_fn(move || {
        // Capture the whole carrier so vector storage drops before its claim.
        let _retained = &records;
        records.value.next().map(Ok).or_else(|| terminal.take().map(Err))
    })
}
