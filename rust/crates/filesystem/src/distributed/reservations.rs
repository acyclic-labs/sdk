use super::*;

fn identity(
    authority: AuthorityId,
    operation: OperationId,
    tail: u64,
    work: &mut AuthorityWork<'_>,
) -> Result<[u8; 40], AuthorityStoreError> {
    let copy = WorkCounters {
        bytes_copied: 40,
        ..WorkCounters::UNCHARGED
    };
    if work.is_native() {
        work.admit(copy)?;
    }
    let mut bytes = [0; 40];
    bytes[..16].copy_from_slice(&authority.into_bytes());
    bytes[16..32].copy_from_slice(&operation.into_bytes());
    bytes[32..].copy_from_slice(&tail.to_le_bytes());
    if work.is_native() {
        work.charge(copy)?;
    }
    Ok(bytes)
}

fn request(
    authority: AuthorityId,
    snapshot: Option<AuthoritySnapshot>,
    operation: OperationId,
    tail: u64,
    gate: Option<StreamPath>,
    work: &mut AuthorityWork<'_>,
) -> Result<CommitRequest, AuthorityStoreError> {
    let gate = match gate {
        Some(path) => path,
        None => work.path(authority, "/publication-gate", None)?,
    };
    let mut conditions = work.vec(if snapshot.is_some() { 4 } else { 2 })?;
    if let Some(snapshot) = snapshot {
        conditions.push(CommitCondition::Tail {
            path: work.path(authority, "/epochs", None)?,
            expected: snapshot.epoch_tail,
        });
    }
    conditions.push(CommitCondition::Tail {
        path: gate.clone(),
        expected: tail,
    });
    if let Some(snapshot) = snapshot {
        conditions.push(CommitCondition::Tail {
            path: work.path(authority, "/records", None)?,
            expected: snapshot.head.sequence.get() + 1,
        });
    }
    conditions.push(CommitCondition::Absent {
        path: work.path(authority, "/retirement", None)?,
    });
    let body = if snapshot.is_some() {
        work.stream_bytes(PUBLICATION_GATE_ACTIVE, &[&operation.into_bytes()])?
    } else {
        acyclic_stream::OwnedBytes::from_static(PUBLICATION_GATE_FREE)
    };
    let mut mutations = work.vec(1)?;
    work.append_record(&mut mutations, gate, [body])?;
    let id = identity(authority, operation, tail, work)?;
    let domain: &[u8] = if snapshot.is_some() {
        b"reserve-publication"
    } else {
        b"release-publication"
    };
    Ok(CommitRequest {
        conditions,
        mutations,
        idempotency_key: work.stream_key(domain, &id)?,
    })
}

fn digest(
    request: &CommitRequest,
    work: &mut AuthorityWork<'_>,
) -> Result<[u8; 32], AuthorityStoreError> {
    let mut bytes = (b"acyclic.stream.request.v1\0".len() + b"commit".len()) as u64;
    for condition in &request.conditions {
        let (path, scalar) = match condition {
            CommitCondition::Tail { path, .. } => (path, 8),
            CommitCondition::Absent { path } => (path, 0),
        };
        bytes = bytes
            .checked_add(1 + 8 + path.as_str().len() as u64 + scalar)
            .ok_or(crate::WorkError::Overflow)?;
    }
    for mutation in &request.mutations {
        let CommitMutation::Append { path, records } = mutation else {
            return Err(authority_corrupt(
                "reservation request contains a non-append mutation",
                work,
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
        ..WorkCounters::UNCHARGED
    };
    work.admit(hashing)?;
    let digest = acyclic_stream::request::commit_digest(request);
    work.charge(hashing)?;
    Ok(digest)
}

fn key_matches(key: &[u8], domain: &[u8], identity: &[u8]) -> bool {
    key.len() == domain.len() + 1 + identity.len()
        && key.starts_with(domain)
        && key[domain.len()] == 0
        && key[domain.len() + 1..] == *identity
}

fn qualified(
    observation: &acyclic_stream::IdempotencyObservation,
    authority: AuthorityId,
    operation: OperationId,
    tail: u64,
    work: &mut AuthorityWork<'_>,
) -> Result<bool, AuthorityStoreError> {
    let Some(prior) = tail.checked_sub(1) else {
        return Ok(false);
    };
    let id = identity(authority, operation, prior, work)?;
    if !key_matches(
        observation.idempotency_key.as_bytes(),
        b"reserve-publication",
        &id,
    ) {
        return Ok(false);
    }
    let acyclic_stream::IdempotencyOutcome::Commit(CommitOutcome::Committed(envelope)) =
        &observation.outcome
    else {
        return Ok(false);
    };
    let [acyclic_stream::CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
        return Ok(false);
    };
    let encoding = WorkCounters {
        bytes_encoded: 32,
        ..WorkCounters::UNCHARGED
    };
    if work.is_native() {
        work.admit(encoding)?;
    }
    let mut hex = [0; 32];
    hex::encode_to_slice(authority.into_bytes(), &mut hex).expect("authority hex width");
    if work.is_native() {
        work.charge(encoding)?;
    }
    let path = append.path.as_str().as_bytes();
    let prefix = b"fs/authorities/";
    if path.len() != prefix.len() + 32 + b"/publication-gate".len()
        || !path.starts_with(prefix)
        || path[prefix.len()..prefix.len() + 32] != hex
        || !path.ends_with(b"/publication-gate")
        || append.start != prior
        || append.end != tail
        || append.tail != tail
    {
        return Ok(false);
    }
    Ok(
        matches!(append.records.as_slice(), [record] if record.sequence == prior && record.commit_id == envelope.commit_id
        && record.value.len() == PUBLICATION_GATE_ACTIVE.len() + 16 && record.value.starts_with(PUBLICATION_GATE_ACTIVE)
        && record.value[PUBLICATION_GATE_ACTIVE.len()..] == operation.into_bytes()),
    )
}

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    pub(super) async fn reservation_gate(
        &self,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(StreamPath, u64, Option<OperationId>), AuthorityStoreError> {
        let gate = work.path(authority, "/publication-gate", None)?;
        let tail = self.tail(gate.clone(), work).await?;
        let from = tail
            .checked_sub(1)
            .ok_or_else(|| authority_corrupt("authority publication gate is empty", work))?;
        let record = self.record(gate.clone(), from, work).await?;
        let active = work.decode_gate(&record.0.value)?;
        Ok((gate, tail, active))
    }

    async fn inspect_reservation(
        &self,
        domain: &[u8],
        id: &[u8],
        work: &mut AuthorityWork<'_>,
    ) -> Result<
        (
            Option<acyclic_stream::IdempotencyObservation>,
            ProviderAllocation,
        ),
        AuthorityStoreError,
    > {
        let baseline = work.allocations.live_bytes();
        let key = work.stream_key(domain, id)?;
        let key_bytes = work
            .allocations
            .live_bytes()
            .checked_sub(baseline)
            .ok_or(crate::WorkError::Overflow)?;
        let ceiling = acyclic_stream::MAX_COMMAND_BYTES as u64;
        work.admit(WorkCounters {
            backend_read_operations: u64::from(!work.is_native()),
            authority_records_read: (acyclic_stream::MAX_ITEMS as u64)
                .checked_mul(acyclic_stream::MAX_ITEMS as u64)
                .ok_or(crate::WorkError::Overflow)?,
            authority_bytes_read: ceiling,
            peak_allocation_bytes: if work.is_native() {
                0
            } else {
                work.allocations
                    .live_bytes()
                    .checked_add(ceiling)
                    .ok_or(crate::WorkError::Overflow)?
            },
            ..WorkCounters::UNCHARGED
        })?;
        let result = 'inspect: {
            #[cfg(not(target_arch = "wasm32"))]
            if work.is_native() {
                let result = self
                    .provider
                    .native_inspect_idempotency(key, work.native_meter())
                    .await;
                break 'inspect work.result(result);
            }
            self.provider
                .inspect_idempotency(key)
                .await
                .map(|value| (value, None))
                .map_err(map_stream_error)
        };
        let mut actual = WorkCounters {
            backend_read_operations: u64::from(!work.is_native()),
            ..WorkCounters::UNCHARGED
        };
        if let Ok((Some(observation), _)) = &result
            && let acyclic_stream::IdempotencyOutcome::Commit(CommitOutcome::Committed(envelope)) =
                &observation.outcome
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
        work.charge(actual)?;
        let output = result?;
        // The opaque envelope bound includes a retained retry key, regardless of sharing.
        if output.0.is_some() {
            work.hold_opaque(ceiling)?;
        }
        work.release(key_bytes)?;
        Ok(output)
    }

    async fn commit_reservation(
        &self,
        request: CommitRequest,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(CommitOutcome, ProviderAllocation), AuthorityStoreError> {
        let writes = work.request_write_work(&request)?;
        work.admit(writes)?;
        work.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if work.is_native() {
            let result = self
                .provider
                .native_commit(request, work.native_meter())
                .await;
            work.release_request()?;
            let output = work.result(result)?;
            if matches!(output.0, CommitOutcome::Committed(_)) {
                work.charge(writes)?;
            }
            return Ok(output);
        }
        let result = self.provider.commit(request).await;
        work.charge(commit_work(writes, &result))?;
        work.release_request()?;
        result.map(|value| (value, None)).map_err(map_stream_error)
    }

    pub(super) async fn reservation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<PublicationReservation>, AuthorityStoreError> {
        work.check()?;
        let (gate, gate_tail, active) = self.reservation_gate(authority_id, work).await?;
        let bytes = gate.as_str().len() as u64;
        drop(gate);
        work.release(bytes)?;
        if active != Some(operation_id) {
            return work.finish(None);
        }
        let unavailable = || AuthorityStoreError::ReservationOriginalHeadUnavailable {
            authority_id,
            operation_id,
            gate_tail,
        };
        let prior = gate_tail.checked_sub(1).ok_or_else(unavailable)?;
        let id = identity(authority_id, operation_id, prior, work)?;
        let observed = self
            .inspect_reservation(b"reserve-publication", &id, work)
            .await?;
        let observation = observed.0.as_ref().ok_or_else(unavailable)?;
        if !qualified(observation, authority_id, operation_id, gate_tail, work)? {
            return Err(unavailable());
        }
        let snapshot = self.fork_snapshot(authority_id, work).await?;
        let mut candidate = request(
            authority_id,
            Some(snapshot),
            operation_id,
            prior,
            None,
            work,
        )?;
        let mut tails = None;
        'candidate: for records in (1..=snapshot.head.sequence.get() + 1).rev() {
            for epochs in (1..=snapshot.epoch_tail).rev() {
                let examined = WorkCounters {
                    items_examined: 1,
                    bytes_copied: if work.is_native() { 16 } else { 0 },
                    ..WorkCounters::UNCHARGED
                };
                work.admit(examined)?;
                work.charge(examined)?;
                for condition in &mut candidate.conditions {
                    if let CommitCondition::Tail { path, expected } = condition {
                        if path.as_str().ends_with("/records") {
                            *expected = records;
                        } else if path.as_str().ends_with("/epochs") {
                            *expected = epochs;
                        }
                    }
                }
                if digest(&candidate, work)? == observation.request_digest {
                    tails = Some((records, epochs));
                    break 'candidate;
                }
            }
        }
        let (records, epochs) = tails.ok_or_else(unavailable)?;
        drop(candidate);
        drop(observed);
        work.release_opaque(acyclic_stream::MAX_COMMAND_BYTES as u64)?;
        work.release_request()?;
        let epoch = self
            .read_at(authority_id, "/epochs", epochs - 1, work, |record, work| {
                work.decode_epoch(&record.value, EPOCH_DOMAIN)
            })
            .await
            .map_err(|error| {
                if authority_missing(&error) && !work.is_native() {
                    unavailable()
                } else {
                    error
                }
            })?;
        let expected = self
            .read_at(
                authority_id,
                "/records",
                records - 1,
                work,
                |record, work| {
                    if records == 1 {
                        work.decode_epoch(&record.value, GENESIS_DOMAIN)?;
                        return Ok(Head::genesis(epoch));
                    }
                    let durable = if work.is_native() {
                        work.decode_durable(&record.value)?
                    } else {
                        decode_record(record)?
                    };
                    if work.durable_digest(authority_id, &durable)? != durable.digest
                        || durable.sequence.get().checked_add(1) != Some(records)
                    {
                        return Err(unavailable());
                    }
                    Ok(Head {
                        epoch,
                        sequence: durable.sequence,
                        digest: durable.digest,
                    })
                },
            )
            .await
            .map_err(|error| {
                if authority_missing(&error) && !work.is_native() {
                    unavailable()
                } else {
                    error
                }
            })?;
        let (gate, tail, active) = self.reservation_gate(authority_id, work).await?;
        let bytes = gate.as_str().len() as u64;
        drop(gate);
        work.release(bytes)?;
        work.finish(if tail == gate_tail && active == Some(operation_id) {
            Some(PublicationReservation {
                authority_id,
                operation_id,
                expected,
                gate_tail,
            })
        } else {
            None
        })
    }

    pub(super) async fn reserve(
        &self,
        authority: AuthorityId,
        expected: Head,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ReservationOutcome, AuthorityStoreError> {
        work.check()?;
        let snapshot = self.fork_snapshot(authority, work).await?;
        if snapshot.head != expected {
            return work.finish(ReservationOutcome::Conflict {
                actual: snapshot.head,
            });
        }
        let (gate, tail, active) = self.reservation_gate(authority, work).await?;
        if active == Some(operation) {
            let bytes = gate.as_str().len() as u64;
            drop(gate);
            work.release(bytes)?;
            let reservation = self
                .reservation(authority, operation, work)
                .await?
                .filter(|reservation| {
                    reservation.expected == expected && reservation.gate_tail == tail
                })
                .ok_or(AuthorityStoreError::ReservationOriginalHeadUnavailable {
                    authority_id: authority,
                    operation_id: operation,
                    gate_tail: tail,
                })?;
            work.release_request()?;
            return work.finish(ReservationOutcome::AlreadyReserved(reservation));
        }
        if active.is_some() {
            drop(gate);
            work.release_request()?;
            return work.finish(ReservationOutcome::Conflict {
                actual: snapshot.head,
            });
        }
        let gate_tail = tail.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        let request = request(authority, Some(snapshot), operation, tail, Some(gate), work)?;
        let output = self.commit_reservation(request, work).await?;
        match &output.0 {
            CommitOutcome::Committed(_) => {
                let result = ReservationOutcome::Reserved(PublicationReservation {
                    authority_id: authority,
                    operation_id: operation,
                    expected,
                    gate_tail,
                });
                if work.is_native() {
                    Ok(result)
                } else {
                    work.finish(result)
                }
            }
            CommitOutcome::Conflict(_) => {
                let actual = self.fork_snapshot(authority, work).await?.head;
                work.finish(ReservationOutcome::Conflict { actual })
            }
        }
    }

    pub(super) async fn release(
        &self,
        reservation: PublicationReservation,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        work.check()?;
        let copying = WorkCounters {
            bytes_copied: 40,
            ..WorkCounters::UNCHARGED
        };
        if !work.is_native() {
            work.admit(copying)?;
        }
        let id = identity(
            reservation.authority_id,
            reservation.operation_id,
            reservation.gate_tail,
            work,
        )?;
        if !work.is_native() {
            work.charge(copying)?;
        }
        let observed = self
            .inspect_reservation(b"release-publication", &id, work)
            .await?;
        if let Some(observation) = &observed.0 {
            if !key_matches(
                observation.idempotency_key.as_bytes(),
                b"release-publication",
                &id,
            ) {
                return Err(authority_corrupt(
                    "publication release observation has another retry identity",
                    work,
                ));
            }
            let acyclic_stream::IdempotencyOutcome::Commit(outcome) = &observation.outcome else {
                return Err(authority_corrupt(
                    "publication release identity retained another Stream mutation family",
                    work,
                ));
            };
            let CommitOutcome::Committed(envelope) = outcome else {
                return Err(AuthorityStoreError::RejectedStatic(
                    "publication reservation is no longer active",
                ));
            };
            let request = request(
                reservation.authority_id,
                None,
                reservation.operation_id,
                reservation.gate_tail,
                None,
                work,
            )?;
            let CommitMutation::Append { path: gate, .. } = &request.mutations[0] else {
                unreachable!("release is one append");
            };
            let end = reservation
                .gate_tail
                .checked_add(1)
                .ok_or(crate::WorkError::Overflow)?;
            let valid = digest(&request, work)? == observation.request_digest
                && matches!(envelope.mutations.as_slice(), [acyclic_stream::CommittedMutation::Append(append)] if &append.path == gate
                    && append.start == reservation.gate_tail && append.end == end && append.tail == end
                    && matches!(append.records.as_slice(), [record] if record.sequence == reservation.gate_tail
                        && record.commit_id == envelope.commit_id && record.value.as_ref() == PUBLICATION_GATE_FREE));
            if !valid {
                return Err(authority_corrupt(
                    "publication release observation does not bind the original gate release",
                    work,
                ));
            }
            drop(request);
            drop(observed);
            work.release_request()?;
            work.release_opaque(acyclic_stream::MAX_COMMAND_BYTES as u64)?;
            return work.finish(());
        }
        drop(observed);
        let (gate, tail, active) = self
            .reservation_gate(reservation.authority_id, work)
            .await?;
        if tail != reservation.gate_tail || active != Some(reservation.operation_id) {
            return Err(AuthorityStoreError::RejectedStatic(
                "publication reservation is no longer owned by this operation",
            ));
        }
        let request = request(
            reservation.authority_id,
            None,
            reservation.operation_id,
            reservation.gate_tail,
            Some(gate),
            work,
        )?;
        let output = self.commit_reservation(request, work).await?;
        match &output.0 {
            CommitOutcome::Committed(_) if work.is_native() => Ok(()),
            CommitOutcome::Committed(_) => work.finish(()),
            CommitOutcome::Conflict(_) => Err(AuthorityStoreError::RejectedStatic(
                "publication reservation is no longer active",
            )),
        }
    }
}
