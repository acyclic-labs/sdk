use super::*;

fn request(
    placement: &AuthorityPlacement,
    authority: AuthorityId,
    snapshot: Option<(Head, u64)>,
    operation: OperationId,
    tail: u64,
    gate: Option<StreamPath>,
    work: &mut AuthorityWork<'_>,
) -> Result<CommitRequest, AuthorityStoreError> {
    let gate = match gate {
        Some(path) => path,
        None => work.path(placement, authority, "/publication-gate", None)?,
    };
    let mut conditions = work.vec(if snapshot.is_some() { 4 } else { 2 })?;
    if let Some((_, epoch_tail)) = snapshot {
        conditions.push(CommitCondition::Tail {
            path: work.path(placement, authority, "/epochs", None)?,
            expected: epoch_tail,
        });
    }
    conditions.push(CommitCondition::Tail {
        path: gate.clone(),
        expected: tail,
    });
    if let Some((head, _)) = snapshot {
        conditions.push(CommitCondition::Tail {
            path: work.path(placement, authority, "/records", None)?,
            expected: head
                .sequence
                .get()
                .checked_add(1)
                .ok_or(crate::WorkError::Overflow)?,
        });
    }
    conditions.push(CommitCondition::Absent {
        path: work.path(placement, authority, "/retirement", None)?,
    });
    let body = if let Some((head, epoch_tail)) = snapshot {
        work.stream_bytes(
            PUBLICATION_GATE_ACTIVE,
            &[
                &operation.into_bytes(),
                &head.epoch.get().to_le_bytes(),
                &head.sequence.get().to_le_bytes(),
                head.digest.as_bytes(),
                &epoch_tail.to_le_bytes(),
            ],
        )?
    } else {
        acyclic_stream::OwnedBytes::from_static(PUBLICATION_GATE_FREE)
    };
    let mut mutations = work.vec(1)?;
    work.append_record(&mut mutations, gate, [body])?;
    let domain: &[u8] = if snapshot.is_some() {
        b"reserve-publication\0"
    } else {
        b"release-publication\0"
    };
    let idempotency_key = if let Some((head, epoch_tail)) = snapshot {
        work.key(
            placement.key_scope(),
            domain,
            &[
                &authority.into_bytes(),
                &operation.into_bytes(),
                &tail.to_le_bytes(),
                &epoch_tail.to_le_bytes(),
                &head.epoch.get().to_le_bytes(),
                &head.sequence.get().to_le_bytes(),
                head.digest.as_bytes(),
            ],
        )?
    } else {
        work.key(
            placement.key_scope(),
            domain,
            &[
                &authority.into_bytes(),
                &operation.into_bytes(),
                &tail.to_le_bytes(),
            ],
        )?
    };
    Ok(CommitRequest {
        deadline_unix_millis: None,
        conditions,
        mutations,
        idempotency_key,
    })
}

fn digest(
    request: &mut CommitRequest,
    work: &mut AuthorityWork<'_>,
) -> Result<[u8; 32], AuthorityStoreError> {
    let Some(CommitMutation::Append { path, records }) = request.mutations.first() else {
        return Err(AuthorityStoreError::RejectedStatic(
            "publication gates only append",
        ));
    };
    let record = records.first().ok_or(AuthorityStoreError::RejectedStatic(
        "empty publication gate append",
    ))?;
    let mut bytes = (b"acyclic.stream.request.v1\0".len() + b"commit".len()) as u64;
    bytes = bytes
        .checked_add(1 + 8 + path.as_str().len() as u64 + 8 + record.hash_bytes())
        .ok_or(crate::WorkError::Overflow)?;
    for condition in &request.conditions {
        let (path, scalar) = match condition {
            CommitCondition::Head { path, .. } => (path, 40),
            CommitCondition::Tail { path, .. } => (path, 8),
            CommitCondition::Absent { path } | CommitCondition::Present { path } => (path, 0),
        };
        bytes = bytes
            .checked_add(1 + 8 + path.as_str().len() as u64 + scalar)
            .ok_or(crate::WorkError::Overflow)?;
    }
    let hashing = codec_work(0, 0, bytes);
    work.admit(hashing)?;
    let digest =
        acyclic_stream::request::commit_digest(request).map_err(AuthorityStoreError::from)?;
    work.charge(hashing)?;
    Ok(digest)
}

fn verify_release(
    placement: &AuthorityPlacement,
    reservation: PublicationReservation,
    observation: &acyclic_stream::IdempotencyObservation,
    work: &mut AuthorityWork<'_>,
) -> Result<(), AuthorityStoreError> {
    let acyclic_stream::IdempotencyOutcome::Commit(envelope) = &observation.outcome else {
        return Err(authority_corrupt(
            "publication release identity retained another Stream mutation family",
            work,
        ));
    };
    let record = match envelope.mutations.as_slice() {
        [acyclic_stream::CommittedMutation::Append(append)] => match append.records.as_slice() {
            [record] => record,
            _ => {
                return Err(authority_corrupt(
                    "publication release observation must retain one record",
                    work,
                ));
            }
        },
        _ => {
            return Err(authority_corrupt(
                "publication release observation must retain one append",
                work,
            ));
        }
    };
    let mut candidate = request(
        placement,
        reservation.authority_id,
        None,
        reservation.operation_id,
        reservation.gate_tail,
        None,
        work,
    )?;
    let tail = reservation
        .gate_tail
        .checked_add(1)
        .ok_or(crate::WorkError::Overflow)?;
    if record.sequence != reservation.gate_tail
        || !qualified(observation, &mut candidate, record, tail, work)?
    {
        return Err(authority_corrupt(
            "publication release observation does not bind the original gate release",
            work,
        ));
    }
    Ok(())
}

fn qualified(
    observation: &acyclic_stream::IdempotencyObservation,
    candidate: &mut CommitRequest,
    record: &acyclic_stream::Record,
    tail: u64,
    work: &mut AuthorityWork<'_>,
) -> Result<bool, AuthorityStoreError> {
    if observation.idempotency_key != candidate.idempotency_key
        || observation.request_digest != digest(candidate, work)?
    {
        return Ok(false);
    }
    let acyclic_stream::IdempotencyOutcome::Commit(envelope) = &observation.outcome else {
        return Ok(false);
    };
    let [acyclic_stream::CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
        return Ok(false);
    };
    let [retained] = append.records.as_slice() else {
        return Ok(false);
    };
    let [CommitMutation::Append { path, records }] = candidate.mutations.as_slice() else {
        return Ok(false);
    };
    Ok(append.path == *path
        && append.start == record.sequence
        && append.end == tail
        && append.tail == tail
        && record.sequence.checked_add(1) == Some(tail)
        && retained == record
        && record.commit_id == envelope.commit_id
        && record.origin == *path
        && matches!(records.as_slice(), [value] if value.value == record.value && value.headers == record.headers))
}

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    pub(super) async fn reservation_gate(
        &self,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(StreamPath, u64, Option<OperationId>), AuthorityStoreError> {
        let gate = work.path(&self.placement, authority, "/publication-gate", None)?;
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
        parts: &[&[u8]],
        work: &mut AuthorityWork<'_>,
    ) -> Result<
        (
            Option<acyclic_stream::IdempotencyObservation>,
            ProviderAllocation,
        ),
        AuthorityStoreError,
    > {
        let key = work.key(self.placement.key_scope(), domain, parts)?;
        work.admit(WorkCounters {
            backend_read_operations: u64::from(!work.is_native()),
            ..WorkCounters::UNCHARGED
        })?;
        let result = self.authority_inspect(key, work).await;
        let mut actual = WorkCounters::UNCHARGED;
        if let Ok((Some(observation), _)) = &result
            && let acyclic_stream::IdempotencyOutcome::Commit(envelope) = &observation.outcome
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
        if actual.authority_bytes_read > acyclic_stream::MAX_COMMAND_BYTES as u64 {
            return Err(authority_corrupt(
                "publication observation exceeds the command bound",
                work,
            ));
        }
        if output.0.is_some() {
            work.hold_opaque(actual.authority_bytes_read)?;
        }
        Ok(output)
    }

    pub(super) async fn reservation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<PublicationReservation>, AuthorityStoreError> {
        work.check()?;
        let gate = work.path(&self.placement, authority_id, "/publication-gate", None)?;
        let gate_tail = self.tail(gate.clone(), work).await?;
        let unavailable = || AuthorityStoreError::ReservationOriginalHeadUnavailable {
            authority_id,
            operation_id,
            gate_tail,
        };
        let prior = gate_tail.checked_sub(1).ok_or_else(unavailable)?;
        let record = self.record(gate, prior, work).await?;
        let Some(reserved) = work.decode_reserved_gate(&record.0.value)? else {
            return work.finish(None);
        };
        if reserved.operation != operation_id {
            return work.finish(None);
        }
        let snapshot = reserved;
        let observed = self
            .inspect_reservation(
                b"reserve-publication\0",
                &[
                    &authority_id.into_bytes(),
                    &operation_id.into_bytes(),
                    &prior.to_le_bytes(),
                    &snapshot.epoch_tail.to_le_bytes(),
                    &snapshot.head.epoch.get().to_le_bytes(),
                    &snapshot.head.sequence.get().to_le_bytes(),
                    snapshot.head.digest.as_bytes(),
                ],
                work,
            )
            .await?;
        let observation = observed.0.as_ref().ok_or_else(unavailable)?;
        let mut candidate = request(
            &self.placement,
            authority_id,
            Some((snapshot.head, snapshot.epoch_tail)),
            operation_id,
            prior,
            None,
            work,
        )?;
        if !qualified(observation, &mut candidate, &record.0, gate_tail, work)? {
            return Err(unavailable());
        }
        drop(candidate);
        drop(observed);
        drop(record);
        work.release_request()?;
        let records = snapshot
            .head
            .sequence
            .get()
            .checked_add(1)
            .ok_or(crate::WorkError::Overflow)?;
        let epochs = snapshot.epoch_tail;
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
        if expected != snapshot.head {
            return Err(unavailable());
        }
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
        let actor = self.actor(authority, work)?;
        let journal = actor.lease(work).await?;
        let _revision = actor.begin(journal)?;
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
        let request = request(
            &self.placement,
            authority,
            Some((snapshot.head, snapshot.epoch_tail)),
            operation,
            tail,
            Some(gate),
            work,
        )?;
        let output = self.commit(request, None, work, None::<&mut ()>).await;
        work.release_request()?;
        let output = output.map_err(|error| error.into_error(std::convert::identity))?;
        match output.0 {
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
        let actor = self.actor(reservation.authority_id, work)?;
        let journal = actor.lease(work).await?;
        let _revision = actor.begin(journal)?;
        work.check()?;
        let observed = self
            .inspect_reservation(
                b"release-publication\0",
                &[
                    &reservation.authority_id.into_bytes(),
                    &reservation.operation_id.into_bytes(),
                    &reservation.gate_tail.to_le_bytes(),
                ],
                work,
            )
            .await?;
        if let Some(observation) = &observed.0 {
            verify_release(&self.placement, reservation, observation, work)?;
            drop(observed);
            work.release_request()?;
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
            &self.placement,
            reservation.authority_id,
            None,
            reservation.operation_id,
            reservation.gate_tail,
            Some(gate),
            work,
        )?;
        let output = self.commit(request, None, work, None::<&mut ()>).await;
        work.release_request()?;
        let output = output.map_err(|error| error.into_error(std::convert::identity))?;
        match output.0 {
            CommitOutcome::Committed(_) if work.is_native() => Ok(()),
            CommitOutcome::Committed(_) => work.finish(()),
            CommitOutcome::Conflict(_) => Err(AuthorityStoreError::RejectedStatic(
                "publication reservation is no longer active",
            )),
        }
    }
}
