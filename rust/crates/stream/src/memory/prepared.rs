use super::*;
use crate::NativeProviderFuture;
use acyclic_native_runtime::{
    NativeByteReservation, NativeCodecReservation, NativeCodecWork, NativeOperationKind,
    NativeOperationReservation,
};
use sha2::Digest as _;

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
    namespace: NamespaceReserve,
    conflicts: (Vec<CommitConflict>, Allocation),
    capacity: Option<CapacityReservation>,
    probes: ReplayProbes,
    codec: NativeCodecReservation<Arc<dyn acyclic_native_runtime::NativeReadMeter>>,
    headers: crate::record_headers::PreparedHeaders,
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
        let maximum_hash = prepared_hash_bytes(&request, maximum);
        let headers = work.record(crate::record_headers::PreparedHeaders::new(
            &work.meter,
            maximum,
        ))?;
        let mut state = self.state.write().await;
        let capacity = self.reserve_prepared_capacity(&state, &request, maximum)?;
        self.reserve_retained(&mut state)?;
        // The tree can grow up to the admitted replay limit while this completion is pending.
        let mut probes = self.reserve_replay_probes(2 * self.maximum_replay_height() + 16)?;
        self.reserve_commit_index(&mut probes)?;
        let mut plan = self.plan_commit(&mut state, &request, CommitId::default(), 0)?;
        let range = append_mut(&mut plan.result);
        range.start = expected;
        range.tail = expected.checked_add(1).ok_or(StreamError::LimitExceeded)?;
        let fact = envelope_append_mut(&mut plan.envelope);
        fact.start = expected;
        fact.end = expected.checked_add(1).ok_or(StreamError::LimitExceeded)?;
        fact.tail = fact.end;
        fact.records.first_mut().ok_or(StreamError::Unavailable)?.sequence = expected;
        history_record(plan.histories.first_mut().ok_or(StreamError::Unavailable)?)?.sequence =
            expected;
        let namespace = self.namespace_reserve(&append(&plan.result).path)?;
        let conflict = self.vector(2)?;
        let prototype = self.copy_envelope(&plan.envelope)?;
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
        )?;
        let codec = work.record(NativeCodecReservation::new(
            Arc::clone(&work.meter),
            NativeCodecWork {
                copied: (2 * std::mem::size_of::<Record>())
                    .max(4 * std::mem::size_of::<CommitConflict>()) as u64,
                hashed: maximum_hash + identity_bytes(),
                ..NativeCodecWork::default()
            },
        ))?;
        work.record(work.meter.check())?;
        Ok((
            PreparedCommit {
                provider: self.clone(),
                conditions,
                key,
                plan,
                namespace,
                conflicts: conflict,
                capacity,
                probes,
                codec,
                headers,
            },
            recovery,
        ))
    }
    fn reserve_prepared_capacity(
        &self,
        state: &State,
        request: &CommitRequest,
        maximum: usize,
    ) -> Result<Option<CapacityReservation>, StreamError> {
        if let Some(stored) = self.replay(state, request.idempotency_key.as_bytes())? {
            if !matches!(stored.result, IdempotencyOutcome::Commit(_)) {
                return Err(StreamError::IdempotencyMismatch);
            }
            return Ok(None);
        }
        for condition in &request.conditions {
            if !state.paths.contains_key(condition_path(condition)) {
                return Err(StreamError::NotFound);
            }
            if self.condition_conflict(state, condition)?.is_some() {
                return Err(StreamError::OutOfRange);
            }
        }
        admit_replay(state, Some(&request.idempotency_key), self.limits)?;
        reserve_commit(state, self.limits)?;
        reserve_records(state, 1, maximum, self.limits)?;
        let counters = Arc::clone(&state.reserved);
        {
            let mut held = counters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            held.0 += 1;
            held.1 += maximum;
        }
        Ok(Some(CapacityReservation {
            counters,
            bytes: maximum,
        }))
    }

    fn prepare_recovery(
        &self,
        recovery_key: IdempotencyKey,
        recovery_conditions: [CommitCondition; 2],
        maximum: usize,
        maximum_hash: u64,
        prototype: (CommittedEnvelope, Allocation),
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
        let mut codec = work.record(NativeCodecReservation::new(
            Arc::clone(&meter),
            NativeCodecWork {
                copied: std::mem::size_of::<Record>() as u64,
                hashed: maximum_hash,
                ..NativeCodecWork::default()
            },
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
                let (mut value, claim) = prototype;
                let target = envelope_append(&value);
                let envelope = prepared_envelope(
                    stored, maximum, &target.path, target.start, target.tail,
                )?;
                let source = envelope_append(envelope);
                let record = source.records.first().ok_or(StreamError::Unavailable)?;
                *envelope_append_mut(&mut value).records.first_mut()
                    .ok_or(StreamError::Unavailable)? = record.clone();
                value.commit_id = envelope.commit_id;
                value.committed_at_micros = envelope.committed_at_micros;
                observed = observed
                    .take()
                    .or(record_codec(&mut codec, std::mem::size_of::<Record>() as u64, 0).err());
                observed = observed.take().or(bytes.record(envelope_payload_bytes(envelope)).err());
                let (digest, hashed) = digest(
                    &recovery_conditions,
                    &source.path,
                    &crate::AppendRecord {
                        value: record.value.clone(),
                        headers: record.headers.clone(),
                    },
                );
                observed = observed
                    .take()
                    .or(record_codec(&mut codec, 0, hashed).err());
                if digest != stored.digest {
                    return Err(StreamError::IdempotencyMismatch);
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
        mut body: crate::AppendRecord,
    ) -> Result<(CommitOutcome, Allocation), StreamError> {
        let work = Arc::clone(
            self.provider
                .work
                .as_ref()
                .ok_or(StreamError::Unavailable)?,
        );
        body.headers = work.record(self.headers.finish(&body.headers))?;
        let (digest, hashed) = digest(&self.conditions, &append(&self.plan.result).path, &body);
        let _ = work.record(record_codec(&mut self.codec, 0, hashed));
        let mut state = self.provider.state.write().await;
        let (mut conflicts, output_claim) = self.conflicts;
        let result = (|| {
            if let Some(stored) = state.replay(self.key.as_bytes(), || {
                self.provider.record_replay_probe(&mut self.probes);
                Ok::<_, StreamError>(())
            })? {
                if stored.digest != digest {
                    return Err(StreamError::IdempotencyMismatch);
                }
                let result = copy_outcome(
                    stored,
                    body.payload_bytes(),
                    (self.plan.result, self.plan.output_claim),
                )?;
                let _ = work.record(record_codec(&mut self.codec, copied_bytes(&result.0), 0));
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
                    conflicts.push(conflict);
                }
            }
            drop(self.capacity.take());
            let bytes = if conflicts.is_empty() {
                body.payload_bytes()
            } else {
                0
            };
            self.provider.publish(bytes, || {
                if conflicts.is_empty() {
                    let decision = state.decision.checked_add(1).ok_or(StreamError::Capacity)?;
                    let time = crate::preparation::committed_time(
                        state.last_committed_at_micros,
                        self.provider.commit_clock.now_unix_millis(),
                    );
                    let id = commit_identity(decision, digest);
                    bind_body(&mut self.plan, body)?;
                    self.plan.bind(id, time)?;
                    let _ = work.record(record_codec(
                        &mut self.codec,
                        2 * std::mem::size_of::<Record>() as u64,
                        identity_bytes(),
                    ));
                    self.plan.publish(
                        &mut state,
                        &mut NamespaceBuilder {
                            allocate: &|_| Err(StreamError::Capacity),
                            reserve: Some(&mut self.namespace),
                        },
                        &mut || self.provider.record_replay_probe(&mut self.probes),
                        self.key,
                        digest,
                        time,
                    )
                } else {
                    let value = CommitOutcome::Conflict(conflicts);
                    let _ = work.record(record_codec(&mut self.codec, 2 * copied_bytes(&value), 0));
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

fn record_codec(
    reservation: &mut NativeCodecReservation<Arc<dyn acyclic_native_runtime::NativeReadMeter>>,
    copied: u64,
    hashed: u64,
) -> std::io::Result<()> {
    reservation.record(NativeCodecWork {
        copied,
        hashed,
        ..NativeCodecWork::default()
    })
}

fn prepared_hash_bytes(request: &CommitRequest, maximum: usize) -> u64 {
    // Header payload is included in `maximum`; its canonical length words
    // are additional digest input, even for empty keys and attribute values.
    commit_hash_bytes(request)
        + maximum as u64
        + 8 * crate::MAX_RECORD_KEYS as u64
        + 16 * crate::MAX_RECORD_ATTRIBUTES as u64
}

fn bind_body(plan: &mut CommitPlan, body: crate::AppendRecord) -> Result<(), StreamError> {
    let record = envelope_append_mut(&mut plan.envelope).records.first_mut()
        .ok_or(StreamError::Unavailable)?;
    record.value = body.value.clone();
    record.headers = body.headers.clone();
    let record = history_record(plan.histories.first_mut().ok_or(StreamError::Unavailable)?)?;
    record.value = body.value;
    record.headers = body.headers;
    Ok(())
}

fn append(value: &CommitOutcome) -> &crate::AppendRange {
    let CommitOutcome::Committed(receipt) = value else {
        unreachable!()
    };
    let [append] = receipt.appends.as_slice() else {
        unreachable!()
    };
    append
}
fn append_mut(value: &mut CommitOutcome) -> &mut crate::AppendRange {
    let CommitOutcome::Committed(receipt) = value else {
        unreachable!()
    };
    let [append] = receipt.appends.as_mut_slice() else {
        unreachable!()
    };
    append
}
fn envelope_append(envelope: &CommittedEnvelope) -> &crate::CommittedAppend {
    let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
        unreachable!()
    };
    append
}
fn envelope_append_mut(envelope: &mut CommittedEnvelope) -> &mut crate::CommittedAppend {
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
fn digest(
    conditions: &[CommitCondition; 2],
    path: &StreamPath,
    body: &crate::AppendRecord,
) -> ([u8; 32], u64) {
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
        + body.hash_bytes()
        - 8;
    (hash.finalize().into(), bytes)
}
fn copy_outcome(
    stored: &Retained,
    maximum: usize,
    (mut output, output_claim): (CommitOutcome, Allocation),
) -> Result<(CommitOutcome, Allocation), StreamError> {
    let target = append(&output);
    let envelope = prepared_envelope(stored, maximum, &target.path, target.start, target.tail)?;
    let CommitOutcome::Committed(receipt) = &mut output else { unreachable!() };
    receipt.commit_id = envelope.commit_id;
    receipt.committed_at_micros = envelope.committed_at_micros;
    Ok((output, output_claim))
}

fn prepared_envelope<'a>(
    stored: &'a Retained,
    maximum: usize,
    path: &StreamPath,
    start: u64,
    tail: u64,
) -> Result<&'a CommittedEnvelope, StreamError> {
    let IdempotencyOutcome::Commit(envelope) = &stored.result else {
        return Err(StreamError::IdempotencyMismatch);
    };
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
    if &source.path != path || source.start != start || source.end != tail || source.tail != tail
        || record.sequence != start || record.commit_id != envelope.commit_id
        || record.committed_at_micros != envelope.committed_at_micros
    {
        return Err(StreamError::IdempotencyMismatch);
    }
    if record.payload_bytes() > maximum {
        return Err(StreamError::LimitExceeded);
    }
    Ok(envelope)
}
fn copied_bytes(value: &CommitOutcome) -> u64 {
    match value {
        CommitOutcome::Committed(_) => std::mem::size_of::<crate::CommitReceipt>() as u64,
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
        || !records
            .first()
            .is_some_and(|record| record.payload_bytes() == 0)
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
