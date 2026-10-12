use super::*;

pub(super) fn reservation_identity(
    authority: AuthorityId,
    operation: OperationId,
    tail: u64,
) -> [u8; 40] {
    let mut identity = [0_u8; 40];
    identity[..16].copy_from_slice(&authority.into_bytes());
    identity[16..32].copy_from_slice(&operation.into_bytes());
    identity[32..].copy_from_slice(&tail.to_le_bytes());
    identity
}

/// The original acquisition/release request, with caller-transferred gate custody.
pub(super) fn reservation_request(
    authority: AuthorityId,
    snapshot: Option<AuthoritySnapshot>,
    operation: OperationId,
    gate_tail: u64,
    gate: Option<acyclic_stream::StreamPath>,
    meter: &mut AuthorityWork<'_>,
) -> Result<acyclic_stream::CommitRequest, AuthorityStoreError> {
    use acyclic_stream::{CommitCondition, CommitMutation};
    let gate = match gate {
        Some(gate) => gate,
        None => meter.path(authority, "/publication-gate", None)?,
    };
    let mut conditions = meter.vec::<CommitCondition>(if snapshot.is_some() { 4 } else { 2 })?;
    if let Some(snapshot) = snapshot {
        let epochs = meter.path(authority, "/epochs", None)?;
        meter.push(
            &mut conditions,
            CommitCondition::Tail {
                path: epochs,
                expected: snapshot.epoch_tail,
            },
        )?;
        let gate_condition = gate.clone();
        meter.push(
            &mut conditions,
            CommitCondition::Tail {
                path: gate_condition,
                expected: gate_tail,
            },
        )?;
        let records = meter.path(authority, "/records", None)?;
        meter.push(
            &mut conditions,
            CommitCondition::Tail {
                path: records,
                expected: (snapshot.head.sequence.get() + 1),
            },
        )?;
    } else {
        let gate_condition = gate.clone();
        meter.push(
            &mut conditions,
            CommitCondition::Tail {
                path: gate_condition,
                expected: gate_tail,
            },
        )?;
    }
    let retirement = meter.path(authority, "/retirement", None)?;
    meter.push(
        &mut conditions,
        CommitCondition::Absent { path: retirement },
    )?;
    let body = if snapshot.is_none() {
        acyclic_stream::OwnedBytes::from_static(PUBLICATION_GATE_FREE)
    } else {
        meter.stream_bytes(PUBLICATION_GATE_ACTIVE, &[&operation.into_bytes()])?
    };
    let mut mutations = meter.vec::<CommitMutation>(1)?;
    meter.append_record(&mut mutations, gate, [body])?;
    let copies = WorkCounters {
        bytes_copied: 40,
        ..WorkCounters::UNCHARGED
    };
    if meter.is_native() {
        meter.admit(copies)?;
    }
    let identity = reservation_identity(authority, operation, gate_tail);
    if meter.is_native() {
        meter.charge(copies)?;
    }
    let key = meter.stream_key(
        if snapshot.is_none() {
            b"release-publication"
        } else {
            b"reserve-publication"
        },
        &identity,
    )?;
    Ok(acyclic_stream::CommitRequest {
        conditions,
        mutations,
        idempotency_key: key,
    })
}

/// Hashes the already canonical, fixed-shape request using the Stream producer.
pub(super) fn reservation_request_digest(
    request: &acyclic_stream::CommitRequest,
    meter: &mut AuthorityWork<'_>,
) -> Result<[u8; 32], AuthorityStoreError> {
    let mut bytes = (b"acyclic.stream.request.v1\0".len() + b"commit".len()) as u64;
    for condition in &request.conditions {
        let (path, scalar) = match condition {
            acyclic_stream::CommitCondition::Tail { path, .. } => (path, 8),
            acyclic_stream::CommitCondition::Absent { path } => (path, 0),
        };
        bytes = bytes
            .checked_add(1 + 8 + path.as_str().len() as u64 + scalar)
            .ok_or(crate::WorkError::Overflow)?;
    }
    for mutation in &request.mutations {
        let acyclic_stream::CommitMutation::Append { path, records } = mutation else {
            return Err(authority_corrupt(
                "reservation request contains a non-append mutation",
                meter,
            ));
        };
        bytes = bytes
            .checked_add(1 + 8 + path.as_str().len() as u64 + 8)
            .ok_or(crate::WorkError::Overflow)?;
        for record in records {
            bytes = bytes
                .checked_add(8 + record.len() as u64)
                .ok_or(crate::WorkError::Overflow)?;
        }
    }
    let hashing = WorkCounters {
        bytes_hashed: bytes,
        ..WorkCounters::default()
    };
    meter.admit(hashing)?;
    let digest = acyclic_stream::request::commit_digest(request);
    meter.charge(hashing)?;
    Ok(digest)
}

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    pub(super) async fn reservation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        meter: &mut AuthorityWork<'_>,
    ) -> Result<Option<PublicationReservation>, AuthorityStoreError> {
        meter.check()?;
        let (gate, gate_tail, active) = self.reservation_gate(authority_id, meter).await?;
        let gate_bytes = gate.as_str().len() as u64;
        drop(gate);
        meter.release(gate_bytes)?;
        if active != Some(operation_id) {
            return meter.finish(None);
        }
        let unavailable = || AuthorityStoreError::ReservationOriginalHeadUnavailable {
            authority_id,
            operation_id,
            gate_tail,
        };
        let prior_gate = gate_tail.checked_sub(1).ok_or_else(|| unavailable())?;
        let (observed, _retained_key_bytes) = self
            .reservation_observation(authority_id, operation_id, prior_gate, meter)
            .await?;
        let observation = observed.0.as_ref().ok_or_else(|| unavailable())?;
        if !self.qualified_reservation_observation(
            observation,
            authority_id,
            operation_id,
            gate_tail,
            meter,
        )? {
            return Err(unavailable());
        }
        let snapshot = self.fork_snapshot(authority_id, meter).await?;
        let mut request = reservation_request(
            authority_id,
            Some(snapshot),
            operation_id,
            prior_gate,
            None,
            meter,
        )?;
        let mut original_tails = None;
        'candidate: for record_tail in (1..=(snapshot.head.sequence.get() + 1)).rev() {
            for epoch_tail in (1..=snapshot.epoch_tail).rev() {
                let candidate_work = WorkCounters {
                    items_examined: 1,
                    bytes_copied: if meter.is_native() { 16 } else { 0 },
                    ..WorkCounters::UNCHARGED
                };
                meter.admit(candidate_work)?;
                meter.charge(candidate_work)?;
                for condition in &mut request.conditions {
                    if let acyclic_stream::CommitCondition::Tail { path, expected } = condition {
                        if path.as_str().ends_with("/records") {
                            *expected = record_tail;
                        } else if path.as_str().ends_with("/epochs") {
                            *expected = epoch_tail;
                        }
                    }
                }
                if reservation_request_digest(&request, meter)? == observation.request_digest {
                    original_tails = Some((record_tail, epoch_tail));
                    break 'candidate;
                }
            }
        }
        let (record_tail, epoch_tail) = original_tails.ok_or_else(|| unavailable())?;
        drop(request);
        drop(observed);
        meter.release_opaque(acyclic_stream::MAX_COMMAND_BYTES as u64)?;
        meter.release_request()?;
        let epochs = meter.path(authority_id, "/epochs", None)?;
        let epochs_bytes = epochs.as_str().len() as u64;
        let epoch_record = self
            .record(epochs, epoch_tail - 1, meter)
            .await
            .map_err(|failure| {
                if authority_missing(&failure) && !meter.is_native() {
                    unavailable()
                } else {
                    failure
                }
            })?;
        meter.release(epochs_bytes)?;
        let epoch = meter.decode_epoch(&epoch_record.0.value, EPOCH_DOMAIN)?;
        drop(epoch_record);
        let records = meter.path(authority_id, "/records", None)?;
        let records_bytes = records.as_str().len() as u64;
        let record = self
            .record(records, record_tail - 1, meter)
            .await
            .map_err(|failure| {
                if authority_missing(&failure) && !meter.is_native() {
                    unavailable()
                } else {
                    failure
                }
            })?;
        meter.release(records_bytes)?;
        let original_head = if record_tail == 1 {
            meter.decode_epoch(&record.0.value, GENESIS_DOMAIN)?;
            Head::genesis(epoch)
        } else {
            let durable = if meter.is_native() {
                meter.decode_durable(&record.0.value)?
            } else {
                StreamsDurableRecord::decode_borrowed(&record.0.value, STREAM_RECORD_LIMIT)
                    .map_err(|error| AuthorityStoreError::Corrupt(error.to_string()))?
            };
            let digest = meter.durable_digest(authority_id, &durable)?;
            if digest != durable.digest
                || durable.sequence.get().checked_add(1) != Some(record_tail)
            {
                return Err(unavailable());
            }
            Head {
                epoch,
                sequence: durable.sequence,
                digest: durable.digest,
            }
        };
        drop(record);
        // Reconfirm the exact existing gate slot after historical proof reads.
        let (gate, current_tail, active) = self.reservation_gate(authority_id, meter).await?;
        let gate_bytes = gate.as_str().len() as u64;
        drop(gate);
        meter.release(gate_bytes)?;
        if current_tail != gate_tail || active != Some(operation_id) {
            return meter.finish(None);
        }
        meter.finish(Some(PublicationReservation {
            authority_id,
            operation_id,
            expected: original_head,
            gate_tail,
        }))
    }

    pub(super) async fn reserve(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        operation_id: OperationId,
        meter: &mut AuthorityWork<'_>,
    ) -> Result<ReservationOutcome, AuthorityStoreError> {
        meter.check()?;
        let snapshot = self.fork_snapshot(authority_id, meter).await?;
        if snapshot.head != expected {
            return meter.finish(ReservationOutcome::Conflict {
                actual: snapshot.head,
            });
        }
        let (gate, gate_tail, active) = self.reservation_gate(authority_id, meter).await?;
        if active == Some(operation_id) {
            let gate_bytes = gate.as_str().len() as u64;
            drop(gate);
            meter.release(gate_bytes)?;
            if !self
                .reservation_matches_original(
                    authority_id,
                    snapshot,
                    operation_id,
                    gate_tail,
                    meter,
                )
                .await?
            {
                return Err(AuthorityStoreError::ReservationOriginalHeadUnavailable {
                    authority_id,
                    operation_id,
                    gate_tail,
                });
            }
            meter.release_request()?;
            return meter.finish(ReservationOutcome::AlreadyReserved(
                PublicationReservation {
                    authority_id,
                    operation_id,
                    expected,
                    gate_tail,
                },
            ));
        }
        if active.is_some() {
            drop(gate);
            meter.release_request()?;
            return meter.finish(ReservationOutcome::Conflict {
                actual: snapshot.head,
            });
        }
        let next_gate_tail = gate_tail.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        let request = reservation_request(
            authority_id,
            Some(snapshot),
            operation_id,
            gate_tail,
            Some(gate),
            meter,
        )?;
        let result = self.reservation_commit(request, meter).await?;
        match &result.0 {
            acyclic_stream::CommitOutcome::Committed(_) => {
                let value = ReservationOutcome::Reserved(PublicationReservation {
                    authority_id,
                    operation_id,
                    expected,
                    gate_tail: next_gate_tail,
                });
                if meter.is_native() {
                    return Ok(value);
                }
                meter.finish(value)
            }
            acyclic_stream::CommitOutcome::Conflict(_) => {
                let actual = self.fork_snapshot(authority_id, meter).await?.head;
                meter.finish(ReservationOutcome::Conflict { actual })
            }
        }
    }

    pub(super) async fn release(
        &self,
        reservation: PublicationReservation,
        meter: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        meter.check()?;
        let copies = WorkCounters {
            bytes_copied: 40,
            ..WorkCounters::UNCHARGED
        };
        meter.admit(copies)?;
        let identity = reservation_identity(
            reservation.authority_id,
            reservation.operation_id,
            reservation.gate_tail,
        );
        meter.charge(copies)?;
        // Resolve a lost release acknowledgement before consulting a newer gate.
        let (observed, retained_bytes) = self
            .reservation_inspect(b"release-publication", &identity, meter)
            .await?;
        if let Some(observation) = observed.0.as_ref() {
            let retained_key = observation.idempotency_key.as_bytes();
            const DOMAIN: &[u8] = b"release-publication\0";
            if retained_key.len() != DOMAIN.len() + identity.len()
                || !retained_key.starts_with(DOMAIN)
                || retained_key[DOMAIN.len()..] != identity
            {
                return Err(authority_corrupt(
                    "publication release observation has another retry identity",
                    meter,
                ));
            }
            match &observation.outcome {
                acyclic_stream::IdempotencyOutcome::Commit(
                    acyclic_stream::CommitOutcome::Committed(envelope),
                ) => {
                    let request = reservation_request(
                        reservation.authority_id,
                        None,
                        reservation.operation_id,
                        reservation.gate_tail,
                        None,
                        meter,
                    )?;
                    let digest = reservation_request_digest(&request, meter)?;
                    let acyclic_stream::CommitMutation::Append { path: gate, .. } =
                        &request.mutations[0]
                    else {
                        unreachable!("release constructor produces one append");
                    };
                    let end = reservation
                        .gate_tail
                        .checked_add(1)
                        .ok_or(crate::WorkError::Overflow)?;
                    let valid = observation.request_digest == digest
                        && matches!(envelope.mutations.as_slice(), [acyclic_stream::CommittedMutation::Append(append)]
                            if &append.path == gate && append.start == reservation.gate_tail
                                && append.end == end && append.tail == end
                                && matches!(append.records.as_slice(), [record]
                                    if record.sequence == reservation.gate_tail && record.commit_id == envelope.commit_id
                                        && record.value.as_ref() == PUBLICATION_GATE_FREE));
                    if !valid {
                        return Err(authority_corrupt(
                            "publication release observation does not bind the original gate release",
                            meter,
                        ));
                    }
                    drop(request);
                }
                acyclic_stream::IdempotencyOutcome::Commit(
                    acyclic_stream::CommitOutcome::Conflict(_),
                ) => {
                    return Err(AuthorityStoreError::RejectedStatic(
                        "publication reservation is no longer active",
                    ));
                }
                _ => {
                    return Err(authority_corrupt(
                        "publication release identity retained another Stream mutation family",
                        meter,
                    ));
                }
            }
            drop(observed);
            meter.release(retained_bytes)?;
            meter.release_request()?;
            meter.release_opaque(acyclic_stream::MAX_COMMAND_BYTES as u64)?;
            return meter.finish(());
        }
        drop(observed);
        let (gate, current_tail, active) = self
            .reservation_gate(reservation.authority_id, meter)
            .await?;
        if current_tail != reservation.gate_tail || active != Some(reservation.operation_id) {
            return Err(AuthorityStoreError::RejectedStatic(
                "publication reservation is no longer owned by this operation",
            ));
        }
        let request = reservation_request(
            reservation.authority_id,
            None,
            reservation.operation_id,
            reservation.gate_tail,
            Some(gate),
            meter,
        )?;
        let result = self.reservation_commit(request, meter).await?;
        match &result.0 {
            acyclic_stream::CommitOutcome::Committed(_) if meter.is_native() => Ok(()),
            acyclic_stream::CommitOutcome::Committed(_) => meter.finish(()),
            acyclic_stream::CommitOutcome::Conflict(_) => Err(AuthorityStoreError::RejectedStatic(
                "publication reservation is no longer active",
            )),
        }
    }

    pub(super) async fn reservation_commit(
        &self,
        request: acyclic_stream::CommitRequest,
        meter: &mut AuthorityWork<'_>,
    ) -> Result<(acyclic_stream::CommitOutcome, ProviderAllocation), AuthorityStoreError> {
        let writes = meter.request_write_work(&request)?;
        meter.admit(writes)?;
        meter.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if meter.is_native() {
            let result = self
                .provider
                .native_commit(request, meter.native_meter())
                .await;
            meter.release_request()?;
            let result = meter.result(result)?;
            if matches!(&result.0, acyclic_stream::CommitOutcome::Committed(_)) {
                meter.charge(writes)?;
            }
            // Return the acquired proof before any late cancellation/head check.
            return Ok(result);
        }
        let result = self.provider.commit(request).await;
        meter.charge(commit_work(writes, &result))?;
        meter.release_request()?;
        result.map(|value| (value, None)).map_err(map_stream_error)
    }

    pub(super) async fn reservation_gate(
        &self,
        authority: AuthorityId,
        meter: &mut AuthorityWork<'_>,
    ) -> Result<(acyclic_stream::StreamPath, u64, Option<OperationId>), AuthorityStoreError> {
        let gate = meter.path(authority, "/publication-gate", None)?;
        let tail = self.tail(gate.clone(), meter).await?;
        if tail == 0 {
            return Err(authority_corrupt(
                "authority publication gate is empty",
                meter,
            ));
        }
        let record = self.record(gate.clone(), tail - 1, meter).await?;
        let active = meter.decode_gate(&record.0.value)?;
        Ok((gate, tail, active))
    }

    pub(super) async fn reservation_observation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        prior_gate_tail: u64,
        meter: &mut AuthorityWork<'_>,
    ) -> Result<
        (
            (
                Option<acyclic_stream::IdempotencyObservation>,
                ProviderAllocation,
            ),
            u64,
        ),
        AuthorityStoreError,
    > {
        let copies = WorkCounters {
            bytes_copied: 40,
            ..WorkCounters::UNCHARGED
        };
        if meter.is_native() {
            meter.admit(copies)?;
        }
        let identity = reservation_identity(authority, operation, prior_gate_tail);
        if meter.is_native() {
            meter.charge(copies)?;
        }
        self.reservation_inspect(b"reserve-publication", &identity, meter)
            .await
    }

    pub(super) async fn reservation_inspect(
        &self,
        domain: &[u8],
        identity: &[u8],
        meter: &mut AuthorityWork<'_>,
    ) -> Result<
        (
            (
                Option<acyclic_stream::IdempotencyObservation>,
                ProviderAllocation,
            ),
            u64,
        ),
        AuthorityStoreError,
    > {
        let baseline = if meter.is_native() {
            0
        } else {
            meter.allocations.live_bytes()
        };
        let key = meter.stream_key(domain, identity)?;
        let key_bytes = if meter.is_native() {
            0
        } else {
            meter
                .allocations
                .live_bytes()
                .checked_sub(baseline)
                .ok_or(crate::WorkError::Overflow)?
        };
        let key_pointer = key.as_bytes().as_ptr() as usize;
        let ceiling = acyclic_stream::MAX_COMMAND_BYTES as u64;
        let record_ceiling = (acyclic_stream::MAX_ITEMS as u64)
            .checked_mul(acyclic_stream::MAX_ITEMS as u64)
            .ok_or(crate::WorkError::Overflow)?;
        let peak = if meter.is_native() {
            0
        } else {
            meter
                .allocations
                .live_bytes()
                .checked_add(ceiling)
                .ok_or(crate::WorkError::Overflow)?
        };
        meter.admit(WorkCounters {
            backend_read_operations: u64::from(!meter.is_native()),
            authority_records_read: record_ceiling,
            authority_bytes_read: ceiling,
            peak_allocation_bytes: peak,
            ..WorkCounters::UNCHARGED
        })?;
        let result = 'inspect: {
            #[cfg(not(target_arch = "wasm32"))]
            if meter.is_native() {
                let result = self
                    .provider
                    .native_inspect_idempotency(key, meter.native_meter())
                    .await;
                break 'inspect meter.result(result);
            }
            self.provider
                .inspect_idempotency(key)
                .await
                .map(|value| (value, None))
                .map_err(map_stream_error)
        };
        let mut actual = WorkCounters {
            backend_read_operations: u64::from(!meter.is_native()),
            ..WorkCounters::UNCHARGED
        };
        if let Ok(output) = &result
            && let Some(observation) = &output.0
            && let acyclic_stream::IdempotencyOutcome::Commit(
                acyclic_stream::CommitOutcome::Committed(envelope),
            ) = &observation.outcome
        {
            for mutation in &envelope.mutations {
                let records = match mutation {
                    acyclic_stream::CommittedMutation::Append(append) => &append.records,
                    acyclic_stream::CommittedMutation::Fork(fork) => &fork.records,
                };
                actual.authority_records_read = actual
                    .authority_records_read
                    .checked_add(records.len() as u64)
                    .ok_or(crate::WorkError::Overflow)?;
                for record in records {
                    actual.authority_bytes_read = actual
                        .authority_bytes_read
                        .checked_add(record.value.len() as u64)
                        .ok_or(crate::WorkError::Overflow)?;
                }
            }
        }
        meter.charge(actual)?;
        let result = result?;
        let retained_bytes = match &result.0 {
            Some(observation)
                if observation.idempotency_key.as_bytes().as_ptr() as usize == key_pointer =>
            {
                key_bytes
            }
            _ => {
                meter.release(key_bytes)?;
                0
            }
        };
        if result.0.is_some() {
            meter.hold_opaque(ceiling)?;
        }
        Ok((result, retained_bytes))
    }

    pub(super) fn qualified_reservation_observation(
        &self,
        observation: &acyclic_stream::IdempotencyObservation,
        authority: AuthorityId,
        operation: OperationId,
        gate_tail: u64,
        meter: &mut AuthorityWork<'_>,
    ) -> Result<bool, AuthorityStoreError> {
        let Some(prior) = gate_tail.checked_sub(1) else {
            return Ok(false);
        };
        let copies = WorkCounters {
            bytes_copied: 40,
            ..WorkCounters::UNCHARGED
        };
        if meter.is_native() {
            meter.admit(copies)?;
        }
        let identity = reservation_identity(authority, operation, prior);
        if meter.is_native() {
            meter.charge(copies)?;
        }
        let key = observation.idempotency_key.as_bytes();
        let domain = b"reserve-publication";
        if key.len() != domain.len() + 1 + identity.len()
            || !key.starts_with(domain)
            || key[domain.len()] != 0
            || key[domain.len() + 1..] != identity
        {
            return Ok(false);
        }
        let acyclic_stream::IdempotencyOutcome::Commit(acyclic_stream::CommitOutcome::Committed(
            envelope,
        )) = &observation.outcome
        else {
            return Ok(false);
        };
        let [acyclic_stream::CommittedMutation::Append(append)] = envelope.mutations.as_slice()
        else {
            return Ok(false);
        };
        let path = append.path.as_str();
        let prefix = b"fs/authorities/";
        let encoding = WorkCounters {
            bytes_encoded: 32,
            ..WorkCounters::UNCHARGED
        };
        if meter.is_native() {
            meter.admit(encoding)?;
        }
        let mut encoded_authority = [0_u8; 32];
        hex::encode_to_slice(authority.into_bytes(), &mut encoded_authority)
            .expect("fixed authority hex width");
        if meter.is_native() {
            meter.charge(encoding)?;
        }
        if path.len() != prefix.len() + 32 + b"/publication-gate".len()
            || !path.as_bytes().starts_with(prefix)
            || path.as_bytes()[prefix.len()..prefix.len() + 32] != encoded_authority
            || !path.ends_with("/publication-gate")
            || append.start != prior
            || append.end != gate_tail
            || append.tail != gate_tail
        {
            return Ok(false);
        }
        let [record] = append.records.as_slice() else {
            return Ok(false);
        };
        Ok(record.sequence == prior
            && record.commit_id == envelope.commit_id
            && record.value.len() == PUBLICATION_GATE_ACTIVE.len() + 16
            && record.value.starts_with(PUBLICATION_GATE_ACTIVE)
            && record.value[PUBLICATION_GATE_ACTIVE.len()..] == operation.into_bytes())
    }

    pub(super) async fn reservation_matches_original(
        &self,
        authority: AuthorityId,
        snapshot: AuthoritySnapshot,
        operation: OperationId,
        gate_tail: u64,
        meter: &mut AuthorityWork<'_>,
    ) -> Result<bool, AuthorityStoreError> {
        let Some(prior) = gate_tail.checked_sub(1) else {
            return Ok(false);
        };
        let (observed, retained_bytes) = self
            .reservation_observation(authority, operation, prior, meter)
            .await?;
        let Some(observation) = observed.0.as_ref() else {
            return Ok(false);
        };
        if !self.qualified_reservation_observation(
            observation,
            authority,
            operation,
            gate_tail,
            meter,
        )? {
            drop(observed);
            meter.release(retained_bytes)?;
            meter.release_opaque(acyclic_stream::MAX_COMMAND_BYTES as u64)?;
            return Ok(false);
        }
        let baseline = if meter.is_native() {
            0
        } else {
            meter.allocations.live_bytes()
        };
        let request =
            reservation_request(authority, Some(snapshot), operation, prior, None, meter)?;
        let digest = reservation_request_digest(&request, meter)?;
        drop(request);
        if !meter.is_native() {
            let bytes = meter
                .allocations
                .live_bytes()
                .checked_sub(baseline)
                .ok_or(crate::WorkError::Overflow)?;
            meter.release(bytes)?;
        }
        let matches = digest == observation.request_digest;
        drop(observed);
        meter.release(retained_bytes)?;
        meter.release_opaque(acyclic_stream::MAX_COMMAND_BYTES as u64)?;
        Ok(matches)
    }
}
