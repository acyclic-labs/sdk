use super::*;

/// Actual canonical controls, created before an external source action.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeMemoryPreparedCommit {
    pub(super) output: NativeStreamOutput<CommittedEnvelope>,
    pub(super) cached: Arc<Replay>,
    pub(super) mutations: NativeStreamOutput<Vec<PreparedMutation>>,
    pub(super) conflicts: NativeStreamOutput<Vec<CommitConflict>>,
    pub(super) conflict_cache: Arc<Replay>,
    pub(super) observations: [(StreamPath, u64); 2],
    pub(super) key: OwnedBytes,
    pub(super) reservation: Option<PreparedCapacityReservation>,
    pub(super) write_operation: NativeOperationReservation<Arc<dyn NativeReadMeter>>,
    pub(super) write_bytes: NativeByteReservation<Arc<dyn NativeReadMeter>>,
    pub(super) meter: Arc<dyn NativeReadMeter>,
    pub(super) maximum: usize,
    pub(super) replay_entry: Allocation,
    pub(super) commit_entry: Allocation,
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn prepared_hash_bytes(request: &CommitRequest, body_length: usize) -> u64 {
    // Exactly the byte slices consumed by the canonical Commit digest writer.
    b"acyclic.stream.request.v1\0".len() as u64
        + b"commit".len() as u64
        + request
            .conditions
            .iter()
            .map(|condition| 1 + 8 + condition_path(condition).as_str().len() as u64 + 8)
            .sum::<u64>()
        + 1
        + 8
        + mutation_path(&request.mutations[0]).as_str().len() as u64
        + 8
        + 8
        + body_length as u64
}

#[cfg(not(target_arch = "wasm32"))]
impl MemoryStream {
    pub(crate) async fn prepare_commit(
        &self,
        request: &mut CommitRequest,
        maximum: usize,
        meter: Arc<dyn NativeReadMeter>,
    ) -> std::io::Result<Result<NativeMemoryPreparedCommit, StreamError>> {
        let result: MemoryResult<NativeMemoryPreparedCommit> = async {
            check(Some(&meter))?;
            if maximum > MAX_RECORD_BYTES {
                return Err(StreamError::LimitExceeded.into());
            }
            normalize_commit(request, Some(&meter))?;
            validate_commit_shape(request, Some(&meter))?;
            let (_, start) = crate::preparation::single_append_template(request)?;
            let command_bytes = crate::wire_codec::CanonicalWire::Commit(request)
                .encoded_len(Some(maximum), None)?;
            if command_bytes > MAX_COMMAND_BYTES {
                return Err(StreamError::LimitExceeded.into());
            }
            let mut state = self.state.write().await;
            admit_item(Some(&meter))?;
            let replay = state.replays.get(request.idempotency_key.as_bytes());
            record_item(Some(&meter))?;
            if replay.is_some_and(|replay| !matches!(replay.result, IdempotencyOutcome::Commit(_)))
            {
                return Err(StreamError::IdempotencyMismatch.into());
            }
            let already_retained = replay.is_some();
            let observe = |condition: &CommitCondition| -> MemoryResult<(StreamPath, u64)> {
                let CommitCondition::Tail { path, expected } = condition else {
                    unreachable!()
                };
                admit_item(Some(&meter))?;
                let stream = state.paths.get(path);
                record_item(Some(&meter))?;
                if !already_retained {
                    let current = stream.ok_or(StreamError::NotFound)?;
                    if current.tail != *expected {
                        return Err(StreamError::OutOfRange.into());
                    }
                }
                Ok((retain_path(Some(&meter), path)?, *expected))
            };
            let observations = [
                observe(&request.conditions[0])?,
                observe(&request.conditions[1])?,
            ];
            let reservation = if already_retained {
                None
            } else {
                admit_replay(&state, Some(&request.idempotency_key), self.limits)?;
                reserve(&state, &BTreeSet::new(), 1, 0, self.limits)?;
                if state
                    .payload_bytes
                    .checked_add(
                        state
                            .prepared_capacity
                            .payload_bytes
                            .load(Ordering::Acquire),
                    )
                    .and_then(|bytes| bytes.checked_add(maximum))
                    .is_none_or(|bytes| bytes > self.limits.payload_bytes)
                {
                    return Err(StreamError::Capacity.into());
                }
                // Every mutation honors these reservations.
                prepare_retained_capacity(&mut state, Some(&meter))?;
                let capacity = Arc::clone(&state.prepared_capacity);
                capacity.advance_revision();
                capacity.pending.fetch_add(1, Ordering::AcqRel);
                capacity.payload_bytes.fetch_add(maximum, Ordering::AcqRel);
                Some(PreparedCapacityReservation {
                    capacity,
                    bytes: maximum,
                })
            };
            let facts = crate::preparation::mutations(
                &request.mutations,
                |_| Some(start),
                CommitId::default(),
                0,
                Some(&meter),
            )?;
            let output = facts.map(|mutations| CommittedEnvelope {
                commit_id: CommitId::default(),
                mutations,
            });
            let mutations = prepare_mutations(&state, &output.value, Some(&meter))?;
            let replay = clone_envelope(&output.value, Some(&meter), false)?;
            let cached = cache_replay(
                Some(&meter),
                None,
                IdempotencyOutcome::Commit(CommitOutcome::Committed(replay.value)),
                replay.allocation,
            )?;
            let conflicts = vec(Some(&meter), request.conditions.len())?;
            let replay_conflicts = vec(Some(&meter), request.conditions.len())?;
            let conflict_cache = cache_replay(
                Some(&meter),
                None,
                IdempotencyOutcome::Commit(CommitOutcome::Conflict(replay_conflicts.value)),
                replay_conflicts.allocation,
            )?;
            let key = request.idempotency_key.0.clone();
            let replay_entry = adopt(
                Some(&meter),
                std::mem::size_of::<(OwnedBytes, Arc<Replay>)>(),
            )?;
            let commit_entry = adopt(Some(&meter), std::mem::size_of::<(CommitId, Arc<Replay>)>())?;
            let write_operation =
                NativeOperationReservation::new(Arc::clone(&meter), NativeOperationKind::Write, 1)?;
            let write_bytes = NativeByteReservation::new(
                Arc::clone(&meter),
                NativeOperationKind::Write,
                maximum,
            )?;
            // Bound request, state, and preallocated output visits.
            meter.admit_examination(5 + 4 + 1 + 2 + 1 + 2 + 3, 0)?;
            meter.admit_codec(
                0,
                0,
                prepared_hash_bytes(request, maximum)
                    + b"acyclic.stream.commit.v1\0".len() as u64
                    + 8
                    + 32,
            )?;
            check(Some(&meter))?;
            Ok(NativeMemoryPreparedCommit {
                output,
                cached,
                mutations,
                conflicts,
                conflict_cache,
                observations,
                key,
                reservation,
                write_operation,
                write_bytes,
                meter,
                maximum,
                replay_entry,
                commit_entry,
            })
        }
        .await;
        match result {
            Ok(prepared) => Ok(Ok(prepared)),
            Err(MemoryFailure::Provider(error)) => Ok(Err(error)),
            Err(MemoryFailure::Observer(error)) => Err(error),
        }
    }

    pub(crate) fn finish_commit(
        &self,
        state: &mut State,
        mut prepared: NativeMemoryPreparedCommit,
        request: &mut CommitRequest,
        committed_at: Option<u64>,
    ) -> std::io::Result<
        Result<(NativeStreamOutput<CommitOutcome>, Option<std::io::Error>), StreamError>,
    > {
        let mut late = None;
        let meter = Arc::clone(&prepared.meter);
        let [CommitMutation::Append { path, records }] = request.mutations.as_slice() else {
            return Ok(Err(StreamError::InvalidArgument));
        };
        record_prepared_work(&meter, &mut late, 1, 0);
        let [body] = records.as_slice() else {
            return prepared_failure(late, StreamError::InvalidArgument);
        };
        record_prepared_work(&meter, &mut late, 1, 0);
        if request.conditions.len() != 2 || body.len() > prepared.maximum {
            return prepared_failure(late, StreamError::LimitExceeded);
        }
        let unchanged_key = request.idempotency_key.as_bytes() == prepared.key.as_ref();
        record_prepared_work(&meter, &mut late, 1, 0);
        if !unchanged_key {
            return prepared_failure(late, StreamError::IdempotencyMismatch);
        }
        let [CommittedMutation::Append(append)] = prepared.output.value.mutations.as_slice() else {
            unreachable!("preparation creates the exact single Append fact")
        };
        if path != &append.path {
            return prepared_failure(late, StreamError::InvalidArgument);
        }
        for (condition, (observed_path, observed_tail)) in
            request.conditions.iter().zip(&prepared.observations)
        {
            let unchanged = matches!(condition, CommitCondition::Tail { path, expected }
                if path == observed_path && expected == observed_tail);
            record_prepared_work(&meter, &mut late, 1, 0);
            if !unchanged {
                return prepared_failure(late, StreamError::InvalidArgument);
            }
        }
        // Record prepaid work without fresh admission.
        let digest = commit_digest(request, Some(&meter), Some(&mut late))
            .expect("the prepared writer retains late causes instead of abandoning settlement");
        let ((), recorded) = prepared.write_operation.attempt(|| ());
        retain_prepared_error(&mut late, recorded);
        let replay = state.replays.get(prepared.key.as_ref());
        record_prepared_work(&meter, &mut late, 1, 0);
        if let Some(replay) = replay {
            if replay.digest != digest {
                return prepared_failure(late, StreamError::IdempotencyMismatch);
            }
            let IdempotencyOutcome::Commit(outcome) = &replay.result else {
                return prepared_failure(late, StreamError::IdempotencyMismatch);
            };
            let outcome = match outcome {
                CommitOutcome::Committed(envelope) => {
                    let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
                        return prepared_failure(late, StreamError::IdempotencyMismatch);
                    };
                    let [record] = append.records.as_slice() else {
                        return prepared_failure(late, StreamError::IdempotencyMismatch);
                    };
                    fill_single_append(&mut prepared.output.value, record, &meter, &mut late)
                        .expect("the original canonical replay has one preallocated record");
                    prepared.output.map(CommitOutcome::Committed)
                }
                CommitOutcome::Conflict(conflicts) => {
                    if conflicts.len() > prepared.conflicts.value.capacity() {
                        return prepared_failure(late, StreamError::IdempotencyMismatch);
                    }
                    for conflict in conflicts {
                        prepared.conflicts.value.push(conflict.clone());
                        record_prepared_work(&meter, &mut late, 1, 0);
                    }
                    prepared.conflicts.map(CommitOutcome::Conflict)
                }
            };
            return Ok(Ok((outcome, late)));
        }
        let Some(reservation) = prepared.reservation.as_ref() else {
            return prepared_failure(late, StreamError::Unavailable);
        };
        if !Arc::ptr_eq(&reservation.capacity, &state.prepared_capacity) {
            return prepared_failure(late, StreamError::PrefixNotRetained);
        }
        let IdempotencyOutcome::Commit(CommitOutcome::Conflict(replay_conflicts)) =
            &mut Arc::get_mut(&mut prepared.conflict_cache)
                .expect("an unpublished conflict cache has one owner")
                .result
        else {
            unreachable!("preparation creates a conflict cache")
        };
        for (path, expected) in &prepared.observations {
            let actual = state.paths.get(path).map(|stream| stream.tail);
            if actual != Some(*expected) {
                let conflict = CommitConflict::Tail {
                    path: path.clone(),
                    expected: *expected,
                    actual,
                };
                replay_conflicts.push(conflict.clone());
                prepared.conflicts.value.push(conflict);
            }
            record_prepared_work(&meter, &mut late, 1, 0);
        }
        if !prepared.conflicts.value.is_empty() {
            let publication = PreparedPublication {
                cached: prepared.conflict_cache,
                key: Some(prepared.key),
                paths: NativeStreamOutput::default(),
                mutations: NativeStreamOutput::default(),
                resident: prepared.replay_entry,
            };
            drop(prepared.reservation);
            publish(state, publication, digest);
            return Ok(Ok((prepared.conflicts.map(CommitOutcome::Conflict), late)));
        }
        let decision = match state
            .commits
            .len()
            .checked_add(1)
            .and_then(|count| u64::try_from(count).ok())
        {
            Some(decision) => decision,
            None => return prepared_failure(late, StreamError::Capacity),
        };
        let commit_id = planned_commit_id(decision, digest, None)
            .expect("the canonical scalar Commit ID writer cannot fail without an meter");
        record_prepared_work(
            &meter,
            &mut late,
            0,
            b"acyclic.stream.commit.v1\0".len() as u64 + 8 + 32,
        );
        let committed_at_micros = state.last_committed_at_micros.max(
            committed_at
                .unwrap_or_else(|| self.clock.now_unix_millis())
                .saturating_mul(1_000),
        );
        let record = Record {
            sequence: append.start,
            value: body.clone(),
            commit_id,
            committed_at_micros,
        };
        let mutation = &mut prepared.mutations.value[0];
        let Some(history) = Arc::get_mut(&mut mutation.history) else {
            return prepared_failure(late, StreamError::Unavailable);
        };
        let History::Batch { records, .. } = history else {
            return prepared_failure(late, StreamError::Unavailable);
        };
        let Some(records) = Arc::get_mut(records) else {
            return prepared_failure(late, StreamError::Unavailable);
        };
        record_prepared_work(&meter, &mut late, 1, 0);
        records.value[0] = record.clone();
        record_prepared_work(&meter, &mut late, 1, 0);
        let cached =
            Arc::get_mut(&mut prepared.cached).expect("an unpublished cache has one owner");
        let envelope = cached
            .envelope_mut()
            .expect("preparation creates the canonical Commit cache");
        for envelope in [&mut prepared.output.value, envelope] {
            fill_single_append(envelope, &record, &meter, &mut late)
                .expect("preparation created both canonical envelope slots");
        }
        mutation.bytes = body.len();
        absorb(&mut prepared.replay_entry, prepared.commit_entry)?;
        let publication = PreparedPublication {
            cached: prepared.cached,
            key: Some(prepared.key),
            paths: NativeStreamOutput::default(),
            mutations: prepared.mutations,
            resident: prepared.replay_entry,
        };
        drop(prepared.reservation);
        publish(state, publication, digest);
        state.last_committed_at_micros = committed_at_micros;
        retain_prepared_error(&mut late, prepared.write_bytes.record(body.len()));
        Ok(Ok((prepared.output.map(CommitOutcome::Committed), late)))
    }
}

/// Rebinds only scalar metadata and an immutable body alias in the real fact vector allocated by `mutations`.
#[cfg(not(target_arch = "wasm32"))]
pub(super) fn fill_single_append(
    envelope: &mut CommittedEnvelope,
    record: &Record,
    meter: &Arc<dyn NativeReadMeter>,
    late: &mut Option<std::io::Error>,
) -> Result<(), StreamError> {
    let [CommittedMutation::Append(append)] = envelope.mutations.as_mut_slice() else {
        return Err(StreamError::InvalidArgument);
    };
    record_prepared_work(meter, late, 1, 0);
    let [slot] = append.records.as_mut_slice() else {
        return Err(StreamError::InvalidArgument);
    };
    record_prepared_work(meter, late, 1, 0);
    let end = record
        .sequence
        .checked_add(1)
        .ok_or(StreamError::LimitExceeded)?;
    *slot = record.clone();
    append.start = record.sequence;
    append.end = end;
    append.tail = end;
    envelope.commit_id = record.commit_id;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn retain_prepared_error(
    late: &mut Option<std::io::Error>,
    result: std::io::Result<()>,
) {
    if let Err(error) = result {
        if late.is_none() {
            *late = Some(error);
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn record_prepared_work(
    meter: &Arc<dyn NativeReadMeter>,
    late: &mut Option<std::io::Error>,
    items: u64,
    hashed: u64,
) {
    if items != 0 {
        retain_prepared_error(late, meter.record_examination(items, 0));
    }
    if hashed != 0 {
        retain_prepared_error(late, meter.record_codec(0, 0, hashed));
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn prepared_failure<T>(
    late: Option<std::io::Error>,
    error: StreamError,
) -> std::io::Result<Result<T, StreamError>> {
    match late {
        Some(error) => Err(error),
        None => Ok(Err(error)),
    }
}
