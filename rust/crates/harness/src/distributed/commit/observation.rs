use super::*;

fn retain_received_owned(
    bytes: u64,
    custody: &mut CodecQuota<'_, '_>,
    failure: &mut Option<Error>,
) -> Result<()> {
    let Some(work) = custody.work().as_deref_mut() else {
        return Ok(());
    };
    if operations::original_native_decode_mode(work) {
        return Ok(());
    }
    match work.retain_owned(bytes) {
        Ok(()) => custody.adopt(bytes)?,
        Err(error) => {
            failure.get_or_insert(error);
        }
    }
    Ok(())
}

pub(super) fn observe_commit(
    outcome: &acyclic_stream::CommitOutcome,
    direct_acknowledgment: bool,
    custody: &mut CodecQuota<'_, '_>,
) -> Result<()> {
    use acyclic_stream::{CommitConflict, CommitOutcome, CommittedMutation};
    if custody.work().is_none() {
        return Ok(());
    }
    let native = custody
        .work()
        .as_deref()
        .is_some_and(operations::original_native_decode_mode);
    let mut failure = None;
    match outcome {
        CommitOutcome::Committed(envelope) => {
            retain_received_owned(
                (envelope.mutations.capacity() * std::mem::size_of::<CommittedMutation>()) as u64,
                custody,
                &mut failure,
            )?;
            for mutation in &envelope.mutations {
                let (records, path_bytes) = match mutation {
                    CommittedMutation::Append(append) => {
                        (&append.records, append.path.as_str().len())
                    }
                    CommittedMutation::Fork(fork) => (
                        &fork.records,
                        fork.source.as_str().len() + fork.destination.as_str().len(),
                    ),
                };
                retain_received_owned(path_bytes as u64, custody, &mut failure)?;
                retain_received_owned(
                    (records.capacity() * std::mem::size_of::<acyclic_stream::Record>()) as u64,
                    custody,
                    &mut failure,
                )?;
                for record in records {
                    let bytes = record.value.len() as u64;
                    let observer = custody.work().as_deref_mut().expect("observed envelope");
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    let received = if native {
                        observer.receive_native_record(bytes)
                    } else {
                        observer.receive_record(bytes)
                    };
                    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                    let received = observer.receive_record(bytes);
                    match received {
                        Ok(()) if !native => custody.adopt(bytes)?,
                        Ok(()) => {}
                        Err(error) => {
                            failure.get_or_insert(error);
                        }
                    }
                    if direct_acknowledgment {
                        let acknowledged = custody
                            .work()
                            .as_deref_mut()
                            .expect("observed envelope")
                            .acknowledged(bytes, 1);
                        if let Err(error) = acknowledged
                            && failure.is_none()
                        {
                            failure = Some(error);
                        }
                    }
                }
            }
        }
        CommitOutcome::Conflict(conflicts) => {
            retain_received_owned(
                (conflicts.capacity() * std::mem::size_of::<CommitConflict>()) as u64,
                custody,
                &mut failure,
            )?;
            for conflict in conflicts {
                let (CommitConflict::Tail { path, .. } | CommitConflict::Exists { path }) =
                    conflict;
                retain_received_owned(path.as_str().len() as u64, custody, &mut failure)?;
            }
        }
    }
    failure.map_or(Ok(()), Err)
}

pub(super) fn observe_recovery(
    observation: &acyclic_stream::IdempotencyObservation,
    phase: Option<&mut CommitPhase>,
    custody: &mut CodecQuota<'_, '_>,
) -> Result<()> {
    if let (Some(phase), IdempotencyOutcome::Commit(outcome)) = (phase, &observation.outcome) {
        *phase = match outcome {
            acyclic_stream::CommitOutcome::Committed(_) => CommitPhase::Committed,
            acyclic_stream::CommitOutcome::Conflict(_) => CommitPhase::Uncommitted,
        };
    }
    let mut failure = None;
    retain_received_owned(
        observation.idempotency_key.as_bytes().len() as u64,
        custody,
        &mut failure,
    )?;
    match &observation.outcome {
        IdempotencyOutcome::Commit(outcome) => {
            if let Err(error) = observe_commit(outcome, false, custody) {
                failure.get_or_insert(error);
            }
        }
        IdempotencyOutcome::Fork(receipt) => {
            retain_received_owned(
                (receipt.source.as_str().len() + receipt.destination.as_str().len()) as u64,
                custody,
                &mut failure,
            )?;
        }
        IdempotencyOutcome::Append(_) => {}
    }
    failure.map_or(Ok(()), Err)
}
