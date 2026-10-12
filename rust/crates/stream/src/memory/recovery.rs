use super::prepared::{
    fill_single_append, prepared_failure, record_prepared_work, retain_prepared_error,
};
use super::*;

#[cfg(not(target_arch = "wasm32"))]
impl MemoryStream {
    pub(crate) fn prepare_recovery(
        &self,
        prepared: &NativeMemoryPreparedCommit,
    ) -> MemoryResult<NativeProviderFuture<'static, Option<IdempotencyObservation>>> {
        let meter = Arc::clone(&prepared.meter);
        let mut output = clone_envelope(&prepared.output.value, Some(&meter), false)?;
        let mut conflicts = vec(Some(&meter), 2)?;
        let mut read_operation =
            NativeOperationReservation::new(Arc::clone(&meter), NativeOperationKind::Read, 1)?;
        let mut read_bytes = NativeByteReservation::new(
            Arc::clone(&meter),
            NativeOperationKind::Read,
            prepared.maximum,
        )?;
        meter.admit_examination(1 + 2 + 2 * 2, 0)?;
        let key = IdempotencyKey(prepared.key.clone());
        let conditions = prepared.observations.clone();
        let maximum = prepared.maximum;
        check(Some(&meter))?;
        let provider = self.clone();
        let original = Arc::clone(&meter);
        NativeProviderFuture::try_new(&original, async move {
            let mut late = None;
            let ((), recorded) = read_operation.attempt(|| ());
            retain_prepared_error(&mut late, recorded);
            let state = provider.state.read().await;
            let replay = state.replays.get(key.as_bytes());
            record_prepared_work(&meter, &mut late, 1, 0);
            let Some(replay) = replay else {
                return match late {
                    Some(error) => Err(error),
                    None => Ok(Ok(NativeStreamOutput::new(
                        None,
                        ReadAllocation::empty(&meter),
                    ))),
                };
            };
            let IdempotencyOutcome::Commit(outcome) = &replay.result else {
                return prepared_failure(late, StreamError::IdempotencyMismatch);
            };
            let request_digest = replay.digest;
            let mut outcome = match outcome {
                CommitOutcome::Committed(envelope) => {
                    if envelope.mutations.len() != 1 {
                        return prepared_failure(late, StreamError::LimitExceeded);
                    }
                    let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
                        return prepared_failure(late, StreamError::IdempotencyMismatch);
                    };
                    record_prepared_work(&meter, &mut late, 1, 0);
                    if append.records.len() != 1 {
                        return prepared_failure(late, StreamError::LimitExceeded);
                    }
                    let [record] = append.records.as_slice() else {
                        unreachable!("the record count was checked")
                    };
                    record_prepared_work(&meter, &mut late, 1, 0);
                    if record.value.len() > maximum {
                        return prepared_failure(late, StreamError::LimitExceeded);
                    }
                    let [CommittedMutation::Append(expected)] = output.value.mutations.as_slice()
                    else {
                        unreachable!("prepared recovery has one Append template")
                    };
                    if append.path != expected.path
                        || append.start != expected.start
                        || append.end != expected.start + 1
                        || append.tail != append.end
                        || record.sequence != expected.start
                        || record.commit_id != envelope.commit_id
                        || record.value.is_empty()
                    {
                        return prepared_failure(late, StreamError::IdempotencyMismatch);
                    }
                    fill_single_append(&mut output.value, record, &meter, &mut late)
                        .expect("the protected recovery output has one preallocated record");
                    retain_prepared_error(&mut late, read_bytes.record(record.value.len()));
                    output.map(CommitOutcome::Committed)
                }
                CommitOutcome::Conflict(stored_conflicts) => {
                    if stored_conflicts.len() > 2 {
                        return prepared_failure(late, StreamError::LimitExceeded);
                    }
                    for conflict in stored_conflicts {
                        record_prepared_work(&meter, &mut late, 1, 0);
                        let CommitConflict::Tail { path, expected, .. } = conflict else {
                            return prepared_failure(late, StreamError::IdempotencyMismatch);
                        };
                        let mut matched = false;
                        for (known, tail) in &conditions {
                            record_prepared_work(&meter, &mut late, 1, 0);
                            if path == known && expected == tail {
                                matched = true;
                                break;
                            }
                        }
                        if !matched {
                            return prepared_failure(late, StreamError::IdempotencyMismatch);
                        }
                        conflicts.value.push(conflict.clone());
                    }
                    conflicts.map(CommitOutcome::Conflict)
                }
            };
            drop(state);
            if let Some(error) = late {
                return Err(error);
            }
            Ok(Ok(NativeStreamOutput::new(
                Some(IdempotencyObservation {
                    idempotency_key: key,
                    request_digest,
                    outcome: IdempotencyOutcome::Commit(outcome.value),
                }),
                outcome
                    .allocation
                    .take()
                    .unwrap_or_else(|| ReadAllocation::empty(&meter)),
            )))
        })
        .map_err(MemoryFailure::Observer)
    }
}
