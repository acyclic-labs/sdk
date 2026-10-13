use super::*;
use crate::NativeProviderFuture;
use acyclic_native_runtime::{
    NativeByteReservation, NativeOperationKind, NativeOperationReservation,
};

struct CapacityReservation {
    counters: Arc<std::sync::Mutex<(usize, usize)>>,
    bytes: usize,
}
impl Drop for CapacityReservation {
    fn drop(&mut self) {
        let mut counts = self
            .counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        counts.0 -= 1;
        counts.1 -= self.bytes;
    }
}

pub(crate) struct PreparedCommit {
    provider: MemoryStream,
    conditions: [CommitCondition; 2],
    key: IdempotencyKey,
    plan: CommitPlan,
    conflicts: [(Vec<CommitConflict>, Allocation); 2],
    capacity: Option<CapacityReservation>,
    inventory: Option<(VecDeque<Retained>, Allocation)>,
}

impl MemoryStream {
    pub(crate) async fn prepare(
        &self,
        mut request: CommitRequest,
        maximum: usize,
    ) -> Result<
        (
            PreparedCommit,
            NativeProviderFuture<'static, Option<IdempotencyObservation>>,
        ),
        StreamError,
    > {
        if maximum > MAX_RECORD_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        normalize_commit(&mut request)?;
        validate_commit_shape(&request)?;
        let [CommitMutation::Append { path, records }] = request.mutations.as_slice() else {
            return Err(StreamError::InvalidArgument);
        };
        if request.conditions.len() != 2
            || request
                .conditions
                .iter()
                .any(|condition| !matches!(condition, CommitCondition::Tail { .. }))
            || records.len() != 1
            || !records[0].is_empty()
        {
            return Err(StreamError::InvalidArgument);
        }
        let expected = request
            .conditions
            .iter()
            .find_map(|condition| match condition {
                CommitCondition::Tail {
                    path: guarded,
                    expected,
                } if guarded == path => Some(*expected),
                _ => None,
            })
            .expect("target guard");
        let work = self.work.as_ref().expect("prepared context");
        let meter = Arc::clone(&work.meter);
        let input = self.request_claim(&request)?;
        let maximum_hash = commit_hash_bytes(&request) + maximum as u64;
        let mut state = self.state.write().await;
        let capacity = if let Some(stored) = state.replay(request.idempotency_key.as_bytes()) {
            if !matches!(stored.result, IdempotencyOutcome::Commit(_)) {
                return Err(StreamError::IdempotencyMismatch);
            }
            None
        } else {
            for condition in &request.conditions {
                if !state.paths.contains_key(condition_path(condition)) {
                    return Err(StreamError::NotFound);
                }
                if condition_conflict(&state, condition).is_some() {
                    return Err(StreamError::OutOfRange);
                }
            }
            admit_replay(&state, Some(&request.idempotency_key), self.limits)?;
            reserve_commit(&state, self.limits)?;
            reserve_records(&state, 1, maximum, self.limits)?;
            let counters = Arc::clone(&state.reserved);
            {
                let mut held = counters
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                held.0 += 1;
                held.1 += maximum;
            }
            Some(CapacityReservation {
                counters,
                bytes: maximum,
            })
        };
        let needed = state.retained.len() + reserved(&state).0 + 1;
        let inventory = if needed > state.retained.capacity() {
            Some(self.retention_buffer(needed.next_power_of_two())?)
        } else {
            None
        };
        let mut plan = self.plan_commit(&mut state, &request, CommitId::default(), 0)?;
        for value in [&mut plan.result, &mut plan.cached] {
            let append = append_mut(value);
            append.start = expected;
            append.end = expected.checked_add(1).ok_or(StreamError::LimitExceeded)?;
            append.tail = append.end;
            append.records[0].sequence = expected;
        }
        history_record(&mut plan.histories[0]).sequence = expected;
        let conflict = self.vector(2)?;
        let (cached_conflicts, mut conflict_claim) = self.vector(2)?;
        self.absorb(&mut conflict_claim, self.retained_claim(true, false)?)?;
        let prototype = self.copy_commit(&plan.result)?;
        let recovery_conflicts = self.vector(2)?;
        let conditions: [CommitCondition; 2] = request
            .conditions
            .try_into()
            .map_err(|_| StreamError::InvalidArgument)?;
        drop(state);
        drop(request.mutations);
        drop(input);
        let key = request.idempotency_key;
        let recovery_key = key.clone();
        let recovery_conditions = conditions.clone();
        let mut read = work.record(NativeOperationReservation::new(
            Arc::clone(&meter),
            NativeOperationKind::Read,
            1,
        ))?;
        let mut bytes = work.record(NativeByteReservation::new(
            Arc::clone(&meter),
            NativeOperationKind::Read,
            maximum,
        ))?;
        work.record(meter.admit_codec(
            0,
            3 * std::mem::size_of::<Record>() as u64,
            maximum_hash + identity_bytes(),
        ))?;
        work.record(meter.admit_examination(16, 0))?;
        work.record(meter.admit_codec(
            0,
            2 * std::mem::size_of::<CommitConflict>() as u64,
            maximum_hash,
        ))?;
        work.record(meter.admit_examination(8, 0))?;
        let state = Arc::clone(&self.state);
        let recovery_meter = Arc::clone(&meter);
        let recovery = work.record(NativeProviderFuture::try_new(&meter, async move {
            let state = state.read().await;
            let (stored, mut observed) = read.attempt(|| state.replay(recovery_key.as_bytes()));
            let result = match stored {
                None => Ok((None, None)),
                Some(stored) => match copy_outcome(
                    &stored.result,
                    &recovery_conditions,
                    maximum,
                    prototype,
                    recovery_conflicts,
                ) {
                    Err(error) => Err(error),
                    Ok((value, claim)) => {
                        observed = observed.and(bytes.record(envelope_payload_bytes(&value)));
                        observed =
                            observed.and(recovery_meter.record_codec(0, copied_bytes(&value), 0));
                        let authenticated = if let CommitOutcome::Committed(_) = &value {
                            let append = append(&value);
                            let (digest, hashed) = digest(
                                &recovery_conditions,
                                &append.path,
                                &append.records[0].value,
                            );
                            observed = observed.and(recovery_meter.record_codec(0, 0, hashed));
                            digest == stored.digest
                        } else {
                            true
                        };
                        if authenticated {
                            Ok((
                                Some(IdempotencyObservation {
                                    idempotency_key: recovery_key,
                                    request_digest: stored.digest,
                                    outcome: IdempotencyOutcome::Commit(value),
                                }),
                                claim,
                            ))
                        } else {
                            Err(StreamError::IdempotencyMismatch)
                        }
                    }
                },
            };
            observed.and(recovery_meter.record_examination(8, 0))?;
            Ok(result.map(|(value, claim)| {
                crate::NativeStreamOutput::new(
                    value,
                    claim.unwrap_or_else(|| {
                        acyclic_native_runtime::ReadAllocation::empty(&recovery_meter)
                    }),
                )
            }))
        }))?;
        work.record(meter.check())?;
        Ok((
            PreparedCommit {
                provider: self.clone(),
                conditions,
                key,
                plan,
                conflicts: [conflict, (cached_conflicts, conflict_claim)],
                capacity,
                inventory,
            },
            recovery,
        ))
    }
}

impl PreparedCommit {
    pub(crate) async fn settle(
        mut self,
        body: Bytes,
    ) -> Result<(CommitOutcome, Allocation), StreamError> {
        let work = Arc::clone(self.provider.work.as_ref().expect("prepared context"));
        let (digest, hashed) = digest(&self.conditions, &append(&self.plan.result).path, &body);
        let _ = work.record(work.meter.record_codec(0, 0, hashed));
        let mut state = self.provider.state.write().await;
        let [(mut conflicts, output_claim), (mut cached, resident)] = self.conflicts;
        let result = (|| {
            if let Some(stored) = state.replay(self.key.as_bytes()) {
                if stored.digest != digest {
                    return Err(StreamError::IdempotencyMismatch);
                }
                let result = copy_outcome(
                    &stored.result,
                    &self.conditions,
                    body.len(),
                    (self.plan.result, self.plan.output_claim),
                    (conflicts, output_claim),
                )?;
                let _ = work.record(work.meter.record_codec(0, copied_bytes(&result.0), 0));
                return Ok(result);
            }
            if self
                .capacity
                .as_ref()
                .is_none_or(|held| !Arc::ptr_eq(&held.counters, &state.reserved))
            {
                return Err(StreamError::PrefixNotRetained);
            }
            for condition in &self.conditions {
                if let Some(conflict) = condition_conflict(&state, condition) {
                    cached.push(conflict.clone());
                    conflicts.push(conflict);
                }
            }
            drop(self.capacity.take());
            if state.retained.len() == state.retained.capacity() {
                let (mut buffer, claim) = self.inventory.take().expect("reserved inventory");
                buffer.append(&mut state.retained);
                state.retained = buffer;
                state.retained_allocation = claim;
            }
            let bytes = if conflicts.is_empty() { body.len() } else { 0 };
            self.provider.publish(bytes, || {
                if conflicts.is_empty() {
                    let decision = state.decision.checked_add(1).ok_or(StreamError::Capacity)?;
                    let time = state.last_committed_at_micros.max(
                        self.provider
                            .commit_clock
                            .now_unix_millis()
                            .saturating_mul(1_000),
                    );
                    let id = commit_identity(decision, digest);
                    for value in [&mut self.plan.result, &mut self.plan.cached] {
                        append_mut(value).records[0].value = body.clone();
                    }
                    history_record(&mut self.plan.histories[0]).value = body;
                    self.plan.bind(id, time);
                    let _ = work.record(work.meter.record_codec(
                        0,
                        3 * std::mem::size_of::<Record>() as u64,
                        identity_bytes(),
                    ));
                    Ok(self.plan.publish(&mut state, self.key, digest, time))
                } else {
                    let value = CommitOutcome::Conflict(conflicts);
                    let _ = work.record(work.meter.record_codec(0, 2 * copied_bytes(&value), 0));
                    retain(
                        &mut state,
                        None,
                        Some(self.key),
                        digest,
                        IdempotencyOutcome::Commit(CommitOutcome::Conflict(cached)),
                    );
                    state
                        .retained
                        .back_mut()
                        .expect("retained conflict")
                        ._allocation = resident;
                    Ok((value, output_claim))
                }
            })
        })();
        let _ = work.record(work.meter.record_examination(16, 0));
        work.write
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        result
    }
}

fn append(value: &CommitOutcome) -> &crate::CommittedAppend {
    let CommitOutcome::Committed(envelope) = value else {
        unreachable!()
    };
    let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
        unreachable!()
    };
    append
}
fn append_mut(value: &mut CommitOutcome) -> &mut crate::CommittedAppend {
    let CommitOutcome::Committed(envelope) = value else {
        unreachable!()
    };
    let [CommittedMutation::Append(append)] = envelope.mutations.as_mut_slice() else {
        unreachable!()
    };
    append
}
fn history_record(history: &mut Arc<History>) -> &mut Record {
    let History::Batch { records, .. } = Arc::get_mut(history).expect("unpublished history") else {
        unreachable!()
    };
    &mut Arc::get_mut(records).expect("unpublished records").records[0]
}
fn digest(conditions: &[CommitCondition; 2], path: &StreamPath, body: &Bytes) -> ([u8; 32], u64) {
    let mut hash = request_hasher(b"commit");
    hash_conditions(&mut hash, conditions);
    hash.update([1]);
    hash_path(&mut hash, path);
    hash_records(&mut hash, std::slice::from_ref(body));
    let bytes = b"acyclic.stream.request.v1\0".len() as u64
        + 6
        + conditions
            .iter()
            .map(|condition| 17 + condition_path(condition).as_str().len() as u64)
            .sum::<u64>()
        + 25
        + path.as_str().len() as u64
        + body.len() as u64;
    (hash.finalize().into(), bytes)
}
fn copy_outcome(
    source: &IdempotencyOutcome,
    conditions: &[CommitCondition; 2],
    maximum: usize,
    (mut output, output_claim): (CommitOutcome, Allocation),
    (mut conflicts, conflict_claim): (Vec<CommitConflict>, Allocation),
) -> Result<(CommitOutcome, Allocation), StreamError> {
    match source {
        IdempotencyOutcome::Commit(CommitOutcome::Committed(envelope)) => {
            if envelope.mutations.len() > 1 {
                return Err(StreamError::LimitExceeded);
            }
            let [CommittedMutation::Append(source)] = envelope.mutations.as_slice() else {
                return Err(StreamError::IdempotencyMismatch);
            };
            if source.records.len() > 1 {
                return Err(StreamError::LimitExceeded);
            }
            let [record] = source.records.as_slice() else {
                return Err(StreamError::IdempotencyMismatch);
            };
            let target = append_mut(&mut output);
            if source.path != target.path
                || source.start != target.start
                || source.end != target.end
                || source.tail != target.tail
                || record.sequence != target.start
            {
                return Err(StreamError::IdempotencyMismatch);
            }
            if record.value.len() > maximum {
                return Err(StreamError::LimitExceeded);
            }
            target.records[0] = record.clone();
            let CommitOutcome::Committed(output_envelope) = &mut output else {
                unreachable!()
            };
            output_envelope.commit_id = envelope.commit_id;
            Ok((output, output_claim))
        }
        IdempotencyOutcome::Commit(CommitOutcome::Conflict(source)) => {
            if source.len() > 2 {
                return Err(StreamError::LimitExceeded);
            }
            for conflict in source {
                let CommitConflict::Tail { path, expected, .. } = conflict else {
                    return Err(StreamError::IdempotencyMismatch);
                };
                if !conditions.iter().any(|condition| matches!(condition,
                    CommitCondition::Tail { path: guarded, expected: tail } if guarded == path && tail == expected)) {
                    return Err(StreamError::IdempotencyMismatch);
                }
                conflicts.push(conflict.clone());
            }
            Ok((CommitOutcome::Conflict(conflicts), conflict_claim))
        }
        _ => Err(StreamError::IdempotencyMismatch),
    }
}
fn copied_bytes(value: &CommitOutcome) -> u64 {
    match value {
        CommitOutcome::Committed(_) => std::mem::size_of::<Record>() as u64,
        CommitOutcome::Conflict(conflicts) => {
            (conflicts.len() * std::mem::size_of::<CommitConflict>()) as u64
        }
    }
}
