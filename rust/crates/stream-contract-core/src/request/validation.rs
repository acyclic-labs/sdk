//! Admission of retained facts before they are encoded or after they are decoded.

use crate::{
    AppendOutcome, CommitId, CommittedEnvelope, CommittedMutation, IdempotencyObservation,
    IdempotencyOutcome, MAX_ITEMS, MAX_RECORD_BYTES, Record, StreamError, StreamPath,
};

/// Admits an observation's outcome facts.
pub fn validate_observation(value: &IdempotencyObservation) -> Result<(), StreamError> {
    match &value.outcome {
        IdempotencyOutcome::Append(AppendOutcome::Committed(receipt)) => {
            if receipt.start >= receipt.end
                || receipt.end != receipt.tail
                || receipt.end - receipt.start > MAX_ITEMS as u64
            {
                return Err(StreamError::InvalidArgument);
            }
        }
        IdempotencyOutcome::Fork(receipt) => {
            if receipt.source == receipt.destination || receipt.forked_at != receipt.tail {
                return Err(StreamError::InvalidArgument);
            }
        }
        IdempotencyOutcome::Commit(envelope) => validate_envelope(envelope)?,
        IdempotencyOutcome::Append(AppendOutcome::TailConflict { .. }) => {}
    }
    Ok(())
}

/// Admits an envelope's effect count and per-effect facts.
pub fn validate_envelope(value: &CommittedEnvelope) -> Result<(), StreamError> {
    // A commit that skipped every effect retains an envelope without facts.
    if value.mutations.len() > MAX_ITEMS {
        return Err(StreamError::InvalidArgument);
    }
    let mut previous: Option<&StreamPath> = None;
    let mut time = Some(value.committed_at_micros);
    for mutation in &value.mutations {
        let (path, start, end, records) = match mutation {
            CommittedMutation::Append(append) => {
                if append.start >= append.end || append.end != append.tail {
                    return Err(StreamError::InvalidArgument);
                }
                (&append.path, append.start, append.end, &append.records)
            }
            CommittedMutation::Fork(fork) => {
                if fork.source == fork.destination || fork.forked_at > fork.tail {
                    return Err(StreamError::InvalidArgument);
                }
                (&fork.destination, fork.forked_at, fork.tail, &fork.records)
            }
        };
        if previous.is_some_and(|previous| previous >= path) {
            return Err(StreamError::InvalidArgument);
        }
        previous = Some(path);
        validate_records(records, start, end, value.commit_id, &mut time)?;
    }
    Ok(())
}

fn validate_records(
    records: &[Record],
    start: u64,
    end: u64,
    commit_id: CommitId,
    time: &mut Option<u64>,
) -> Result<(), StreamError> {
    if records.len() > MAX_ITEMS || end - start != records.len() as u64 {
        return Err(StreamError::InvalidArgument);
    }
    for (index, record) in records.iter().enumerate() {
        if record.sequence != start + index as u64
            || record.commit_id != commit_id
            || record.payload_bytes() > MAX_RECORD_BYTES
            || time.is_some_and(|time| time != record.committed_at_micros)
        {
            return Err(StreamError::InvalidArgument);
        }
        crate::changes::validate(&record.value, &record.headers)?;
        *time = Some(record.committed_at_micros);
    }
    Ok(())
}
