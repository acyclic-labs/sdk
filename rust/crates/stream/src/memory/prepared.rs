use super::*;
use crate::NativeProviderFuture;
use acyclic_native_runtime::{
    NativeByteReservation, NativeOperationKind, NativeOperationReservation,
};

struct CapacityReservation {
    counters: Arc<crate::query_sync::SyncMutex<(usize, usize)>>,
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
    probes: ReplayProbes,
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
        let expected = prepared_tail(&mut request, maximum)?;
        let work = self.work.as_ref().ok_or(StreamError::Unavailable)?;
        let input = self.request_claim(&request)?;
        let maximum_hash = commit_hash_bytes(&request) + maximum as u64;
        let mut state = self.state.write().await;
        let capacity =
            if let Some(stored) = self.replay(&state, request.idempotency_key.as_bytes())? {
                if !matches!(stored.result, IdempotencyOutcome::Commit(_)) {
                    return Err(StreamError::IdempotencyMismatch);
                }
                None
            } else {
                for condition in &request.conditions {
                    if !state.paths.contains_key(condition_path(condition)) {
                        return Err(StreamError::NotFound);
                    }
                    if self.condition_conflict(&state, condition)?.is_some() {
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
        self.reserve_retained(&mut state)?;
        // The tree can grow up to the admitted replay limit while this completion is pending.
        let probes = self.reserve_replay_probes(2 * self.maximum_replay_height() + 16)?;
        let mut plan = self.plan_commit(&mut state, &request, CommitId::default(), 0)?;
        for value in [&mut plan.result, &mut plan.cached] {
            let append = append_mut(value);
            append.start = expected;
            append.end = expected.checked_add(1).ok_or(StreamError::LimitExceeded)?;
            append.tail = append.end;
            append
                .records
                .first_mut()
                .ok_or(StreamError::Unavailable)?
                .sequence = expected;
        }
        history_record(plan.histories.first_mut().ok_or(StreamError::Unavailable)?)?.sequence =
            expected;
        let conflict = self.vector(2)?;
        let (cached_conflicts, conflict_claim) = self.vector(2)?;
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
        let recovery = self.prepare_recovery(
            key.clone(),
            conditions.clone(),
            maximum,
            maximum_hash,
            prototype,
            recovery_conflicts,
        )?;
        work.record(work.meter.check())?;
        Ok((
            PreparedCommit {
                provider: self.clone(),
                conditions,
                key,
                plan,
                conflicts: [conflict, (cached_conflicts, conflict_claim)],
                capacity,
                probes,
            },
            recovery,
        ))
    }
    fn prepare_recovery(
        &self,
        recovery_key: IdempotencyKey,
        recovery_conditions: [CommitCondition; 2],
        maximum: usize,
        maximum_hash: u64,
        prototype: (CommitOutcome, Allocation),
        recovery_conflicts: (Vec<CommitConflict>, Allocation),
    ) -> Result<NativeProviderFuture<'static, Option<IdempotencyObservation>>, StreamError> {
        let work = self.work.as_ref().ok_or(StreamError::Unavailable)?;
        let meter = Arc::clone(&work.meter);
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
        work.record(meter.admit_codec(
            0,
            2 * std::mem::size_of::<CommitConflict>() as u64,
            maximum_hash,
        ))?;
        let state = Arc::clone(&self.state);
        let mut probes = self.reserve_replay_probes(self.maximum_replay_height() + 8)?;
        let recovery_meter = Arc::clone(&meter);
        let recovery = work.record(NativeProviderFuture::try_new(&meter, async move {
            let state = state.read().await;
            let (stored, observed) =
                read.attempt(|| state.replay(recovery_key.as_bytes(), || probes.record(1)));
            let stored = stored?;
            let mut observed = observed.err();
            let result = (|| {
                let Some(stored) = stored else {
                    return Ok((None, None));
                };
                let (value, claim) = copy_outcome(
                    &stored.result,
                    &recovery_conditions,
                    maximum,
                    prototype,
                    recovery_conflicts,
                )?;
                observed = observed
                    .take()
                    .or(bytes.record(envelope_payload_bytes(&value)).err());
                observed = observed.take().or(recovery_meter
                    .record_codec(0, copied_bytes(&value), 0)
                    .err());
                if let CommitOutcome::Committed(_) = &value {
                    let append = append(&value);
                    let (digest, hashed) = digest(
                        &recovery_conditions,
                        &append.path,
                        &append
                            .records
                            .first()
                            .ok_or(StreamError::Unavailable)?
                            .value,
                    );
                    observed = observed
                        .take()
                        .or(recovery_meter.record_codec(0, 0, hashed).err());
                    if digest != stored.digest {
                        return Err(StreamError::IdempotencyMismatch);
                    }
                }
                Ok((
                    Some(IdempotencyObservation {
                        idempotency_key: recovery_key,
                        request_digest: stored.digest,
                        outcome: IdempotencyOutcome::Commit(value),
                    }),
                    claim,
                ))
            })();
            if let Some(error) = observed.or(probes.record(8).err()) {
                return Err(error);
            }
            Ok(result.map(|(value, claim)| {
                crate::NativeStreamOutput::new(
                    value,
                    claim.unwrap_or_else(|| {
                        acyclic_native_runtime::ReadAllocation::empty(&recovery_meter)
                    }),
                )
            }))
        }))?;
        Ok(recovery)
    }
}

impl PreparedCommit {
    pub(crate) async fn settle(
        mut self,
        body: Bytes,
    ) -> Result<(CommitOutcome, Allocation), StreamError> {
        let work = Arc::clone(
            self.provider
                .work
                .as_ref()
                .ok_or(StreamError::Unavailable)?,
        );
        let (digest, hashed) = digest(&self.conditions, &append(&self.plan.result).path, &body);
        let _ = work.record(work.meter.record_codec(0, 0, hashed));
        let mut state = self.provider.state.write().await;
        let [(mut conflicts, output_claim), (mut cached, resident)] = self.conflicts;
        let result = (|| {
            if let Some(stored) = state.replay(self.key.as_bytes(), || {
                self.provider.record_replay_probe(&mut self.probes);
                Ok::<_, StreamError>(())
            })? {
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
                if let Some(conflict) = self.provider.condition_conflict(&state, condition)? {
                    cached.push(conflict.clone());
                    conflicts.push(conflict);
                }
            }
            drop(self.capacity.take());
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
                    bind_body(&mut self.plan, body)?;
                    self.plan.bind(id, time)?;
                    let _ = work.record(work.meter.record_codec(
                        0,
                        3 * std::mem::size_of::<Record>() as u64,
                        identity_bytes(),
                    ));
                    self.plan.publish(
                        &mut state,
                        &mut || self.provider.record_replay_probe(&mut self.probes),
                        self.key,
                        digest,
                        time,
                    )
                } else {
                    let value = CommitOutcome::Conflict(conflicts);
                    let _ = work.record(work.meter.record_codec(0, 2 * copied_bytes(&value), 0));
                    retain(
                        &mut state,
                        &mut || self.provider.record_replay_probe(&mut self.probes),
                        None,
                        Some(self.key),
                        digest,
                        IdempotencyOutcome::Commit(CommitOutcome::Conflict(cached)),
                        resident,
                    );
                    Ok((value, output_claim))
                }
            })
        })();
        let _ = work.record(self.probes.record(16));
        work.write
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        result
    }
}

fn bind_body(plan: &mut CommitPlan, body: Bytes) -> Result<(), StreamError> {
    for value in [&mut plan.result, &mut plan.cached] {
        append_mut(value)
            .records
            .first_mut()
            .ok_or(StreamError::Unavailable)?
            .value = body.clone();
    }
    history_record(plan.histories.first_mut().ok_or(StreamError::Unavailable)?)?.value = body;
    Ok(())
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
fn history_record(history: &mut Arc<History>) -> Result<&mut Record, StreamError> {
    let Some(History::Batch { records, .. }) = Arc::get_mut(history) else {
        return Err(StreamError::Unavailable);
    };
    Arc::get_mut(records)
        .and_then(|batch| batch.records.first_mut())
        .ok_or(StreamError::Unavailable)
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
            if record.payload_bytes() > maximum {
                return Err(StreamError::LimitExceeded);
            }
            *target.records.first_mut().ok_or(StreamError::Unavailable)? = record.clone();
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

fn prepared_tail(request: &mut CommitRequest, maximum: usize) -> Result<u64, StreamError> {
    if maximum > MAX_RECORD_BYTES {
        return Err(StreamError::LimitExceeded);
    }
    normalize_commit(request)?;
    validate_commit_shape(request)?;
    let [CommitMutation::Append { path, records }] = request.mutations.as_slice() else {
        return Err(StreamError::InvalidArgument);
    };
    if request.conditions.len() != 2
        || request
            .conditions
            .iter()
            .any(|condition| !matches!(condition, CommitCondition::Tail { .. }))
        || records.len() != 1
        || !records.first().is_some_and(Bytes::is_empty)
    {
        return Err(StreamError::InvalidArgument);
    }
    request
        .conditions
        .iter()
        .find_map(|condition| match condition {
            CommitCondition::Tail {
                path: guarded,
                expected,
            } if guarded == path => Some(*expected),
            _ => None,
        })
        .ok_or(StreamError::InvalidArgument)
}
