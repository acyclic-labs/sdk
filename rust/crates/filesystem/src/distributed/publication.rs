use super::*;

pub(super) async fn seal<G: AuthorityGuard>(
    meter: &mut AuthorityWork<'_>,
    mut guard: Option<&mut G>,
) -> Result<(), AuthorityError<G::Error>> {
    meter.check()?;
    let mut child = meter.operation_work()?;
    let result = crate::async_storage::seal(guard.as_deref_mut(), &mut child).await;
    meter.absorb(&child)?;
    result?;
    meter.check()?;
    Ok(())
}

impl<P: acyclic_stream::StreamProvider> AsyncAuthorityStore for StreamAuthorityStore<P> {
    fn supports_generation_lineage_prefix(&self) -> bool {
        true
    }
    async fn create_authority<G: AuthorityGuard>(
        &self,
        authority: AuthorityId,
        epoch: Epoch,
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, AuthorityError<G::Error>> {
        let mut meter = AuthorityWork::operation(work)?;
        let result =
            super::creation::creation(self, authority, epoch, &mut meter, guard.as_deref_mut())
                .await;
        work.record(meter.spent())?;
        result
    }
    async fn create_first_record<G: AuthorityGuard>(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<bool, AuthorityError<G::Error>> {
        let mut meter = AuthorityWork::operation(work)?;
        let result = super::creation::first_record(
            self,
            authority,
            commit,
            &mut meter,
            guard.as_deref_mut(),
        )
        .await;
        work.record(meter.spent())?;
        result
    }
    async fn fork_workspace<G: AuthorityGuard>(
        &self,
        fork: WorkspaceForkCommit,
        permit: PublicationPermit,
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<WorkspaceForkOutcome, AuthorityError<G::Error>> {
        let mut meter = AuthorityWork::operation(work)?;
        let result =
            super::creation::workspace_fork(self, fork, permit, &mut meter, guard.as_deref_mut())
                .await;
        work.record(meter.spent())?;
        result
    }
    async fn fork_authority<G: AuthorityGuard>(
        &self,
        source: crate::GenerationForkSource,
        destination_authority: AuthorityId,
        operation_id: OperationId,
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, AuthorityError<G::Error>> {
        if source.lineage == crate::GenerationFork::Independent {
            return self
                .create_authority(
                    destination_authority,
                    Epoch::GENESIS,
                    work,
                    guard.as_deref_mut(),
                )
                .await;
        }
        let mut meter = AuthorityWork::operation(work)?;
        let result = super::creation::generation_fork(
            self,
            source,
            destination_authority,
            operation_id,
            &mut meter,
            guard.as_deref_mut(),
        )
        .await;
        work.record(meter.spent())?;
        result
    }
    async fn compare_and_append<G: AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<FsAppendOutcome, AuthorityError<G::Error>> {
        let mut meter = AuthorityWork::operation(work)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = meter.original {
            original
                .admit(WorkCounters::UNCHARGED)
                .map_err(AuthorityStoreError::from)?;
        }
        let result = async {
            let meter = &mut meter;

            use acyclic_stream::{CommitCondition, CommitOutcome, StreamError};
            let crate::GuardedAppend {
                authority_id,
                epoch,
                expected,
                commit,
                permit,
            } = request;
            if let Some(existing) = self
                .authority_find_operation(authority_id, commit.operation_id, meter)
                .await?
            {
                let value = if existing.fingerprint == commit.fingerprint {
                    FsAppendOutcome::AlreadyCommitted(existing)
                } else {
                    FsAppendOutcome::IdempotencyConflict {
                        committed_fingerprint: existing.fingerprint,
                    }
                };
                seal(meter, guard.as_deref_mut()).await?;
                return meter.finish(value).map_err(Into::into);
            }
            let snapshot = self.fork_snapshot(authority_id, meter).await?;
            if snapshot.head.epoch != epoch {
                return meter
                    .finish(FsAppendOutcome::Fenced {
                        actual_epoch: snapshot.head.epoch,
                    })
                    .map_err(Into::into);
            }
            if snapshot.head != expected {
                return meter
                    .finish(FsAppendOutcome::Conflict {
                        actual: snapshot.head,
                    })
                    .map_err(Into::into);
            }
            let (gate, gate_tail, active_reservation) =
                self.reservation_gate(authority_id, meter).await?;
            let admitted = match permit {
                PublicationPermit::Reservation {
                    operation_id,
                    gate_tail: permitted_tail,
                    expected: reserved_head,
                } => {
                    permitted_tail == gate_tail
                        && active_reservation == Some(OperationId::from_bytes(operation_id))
                        && reserved_head == expected
                }
                PublicationPermit::Unrestricted => active_reservation.is_none(),
                PublicationPermit::Lease {
                    authority_id: permitted_authority,
                    workspace_id,
                    ..
                } => {
                    let workspace = crate::WorkspaceId::from_bytes(workspace_id);
                    active_reservation.is_none()
                        && AuthorityId::from_bytes(permitted_authority) == authority_id
                        && crate::kernel::volume_authority_id(workspace.volume_id()) == authority_id
                }
            };
            if !admitted {
                return meter
                    .finish(FsAppendOutcome::Fenced {
                        actual_epoch: snapshot.head.epoch,
                    })
                    .map_err(Into::into);
            }
            let sequence = expected
                .sequence
                .checked_next()
                .map_err(|_| AuthorityStoreError::SequenceExhausted)?;
            let mut durable = DurableCommit {
                epoch,
                sequence,
                operation_id: commit.operation_id,
                fingerprint: commit.fingerprint,
                previous_digest: expected.digest,
                digest: Digest::ZERO,
                payload: commit.payload,
                settled_at_micros: None,
            };
            let mut borrowed = crate::streams_record::BorrowedStreamsDurableRecord {
                epoch,
                sequence,
                operation_id: durable.operation_id,
                fingerprint: durable.fingerprint,
                previous_digest: durable.previous_digest,
                digest: durable.digest,
                payload: &durable.payload,
            };
            borrowed.digest = meter.durable_digest(authority_id, &borrowed)?;
            durable.digest = borrowed.digest;
            let encoded = StreamsDurableRecord::encode_borrowed_with(
                &borrowed,
                STREAM_RECORD_LIMIT,
                |domain, fields| meter.stream_bytes(domain, fields),
            )
            .map_err(|error| AuthorityStoreError::DurableCodec(error))??;
            let records = meter.path(authority_id, "/records", None)?;
            let epochs = meter.path(authority_id, "/epochs", None)?;
            let retirement = meter.path(authority_id, "/retirement", None)?;
            let mut conditions = meter.vec(7)?;
            for condition in [
                CommitCondition::Absent { path: retirement },
                CommitCondition::Tail {
                    path: records.clone(),
                    expected: (snapshot.head.sequence.get() + 1),
                },
                CommitCondition::Tail {
                    path: epochs,
                    expected: snapshot.epoch_tail,
                },
                CommitCondition::Tail {
                    path: gate.clone(),
                    expected: gate_tail,
                },
            ] {
                meter.push(&mut conditions, condition)?;
            }
            let mut lease_path = None;
            let mut deadline = None;
            if let PublicationPermit::Lease {
                workspace_id,
                lease_id,
                expires_at_millis,
                ..
            } = permit
            {
                let path = meter.lease_path(
                    crate::WorkspaceId::from_bytes(workspace_id),
                    crate::OperationLeaseId::from_bytes(lease_id),
                    expires_at_millis,
                )?;
                meter.push(
                    &mut conditions,
                    CommitCondition::Tail {
                        path: path.clone(),
                        expected: 1,
                    },
                )?;
                lease_path = Some(path);
                deadline = Some(expires_at_millis);
            }
            let mut mutations = meter.vec(3)?;
            meter.append_record(&mut mutations, records.clone(), [encoded])?;
            if let Some(generation) =
                generation_from_payload(authority_id, sequence, &durable.payload, meter)?
            {
                let lineage = meter.path(authority_id, "/lineage", None)?;
                let locator = meter.path(authority_id, "/generations/", Some(generation))?;
                let tail = self.tail(locator.clone(), meter).await;
                match tail {
                    Ok(1) => {
                        let located = self.record(locator, 0, meter).await?;
                        let located_tail = meter
                            .decode_fixed::<8>(
                                &located.0.value,
                                LINEAGE_TAIL_DOMAIN,
                                "invalid Stream generation locator",
                            )
                            .map(u64::from_le_bytes)?;
                        if located_tail == 0 {
                            return Err(authority_corrupt(
                                "generation locator selects an empty lineage",
                                meter,
                            )
                            .into());
                        }
                        let entry = self.record(lineage, located_tail - 1, meter).await?;
                        if meter
                            .decode_fixed::<32>(
                                &entry.0.value,
                                LINEAGE_DOMAIN,
                                "invalid Stream generation lineage record",
                            )
                            .map(|bytes| GenerationId::new(Digest::from_bytes(bytes)))?
                            != generation
                        {
                            return Err(authority_corrupt(
                                "generation locator selected another generation",
                                meter,
                            )
                            .into());
                        }
                    }
                    Err(failure) if authority_missing(&failure) => {
                        let lineage_tail = match self.tail(lineage.clone(), meter).await {
                            Ok(tail) => tail,
                            Err(failure) if authority_missing(&failure) => 0,
                            Err(failure) => return Err(failure.into()),
                        };
                        let condition = if lineage_tail == 0 {
                            CommitCondition::Absent {
                                path: lineage.clone(),
                            }
                        } else {
                            CommitCondition::Tail {
                                path: lineage.clone(),
                                expected: lineage_tail,
                            }
                        };
                        meter.push(&mut conditions, condition)?;
                        meter.push(
                            &mut conditions,
                            CommitCondition::Absent {
                                path: locator.clone(),
                            },
                        )?;
                        let entry = meter
                            .stream_bytes(LINEAGE_DOMAIN, &[generation.digest().as_bytes()])?;
                        meter.append_record(&mut mutations, lineage, [entry])?;
                        let location = meter.stream_bytes(
                            LINEAGE_TAIL_DOMAIN,
                            &[&lineage_tail.saturating_add(1).to_le_bytes()],
                        )?;
                        meter.append_record(&mut mutations, locator, [location])?;
                    }
                    Ok(_) => {
                        return Err(authority_corrupt(
                            "generation locator contains an invalid record count",
                            meter,
                        )
                        .into());
                    }
                    Err(failure) => return Err(failure.into()),
                }
            }
            let key = meter.operation_key(authority_id, durable.operation_id)?;
            let request = acyclic_stream::CommitRequest {
                conditions,
                mutations,
                idempotency_key: key,
            };
            let result = self
                .commit(request, deadline, meter, guard.as_deref_mut())
                .await;
            if !meter.is_native() {
                let retained_paths = (gate.as_str().len() as u64)
                    .checked_add(records.as_str().len() as u64)
                    .and_then(|bytes| {
                        bytes.checked_add(
                            lease_path
                                .as_ref()
                                .map_or(0, |path| path.as_str().len() as u64),
                        )
                    })
                    .ok_or(AuthorityStoreError::from(crate::WorkError::Overflow))?;
                let retired = meter
                    .allocations
                    .live_bytes()
                    .checked_sub(retained_paths)
                    .ok_or_else(|| {
                        authority_corrupt("append path custody exceeds live request claims", meter)
                    })?;
                meter.release(retired)?;
            }
            let value = match result {
                Ok(outcome) => match &outcome.0 {
                    CommitOutcome::Committed(envelope) => {
                        let record = envelope
                            .mutations
                            .iter()
                            .find_map(|mutation| match mutation {
                                acyclic_stream::CommittedMutation::Append(append)
                                    if append.path == records =>
                                {
                                    match append.records.as_slice() {
                                        [record]
                                            if record.sequence == sequence.get()
                                                && record.commit_id == envelope.commit_id =>
                                        {
                                            Some(record)
                                        }
                                        _ => None,
                                    }
                                }
                                _ => None,
                            })
                            .ok_or_else(|| {
                                authority_corrupt(
                                    "commit envelope omits its authority record",
                                    meter,
                                )
                            })?;
                        durable.settled_at_micros =
                            (record.committed_at_micros != 0).then_some(record.committed_at_micros);
                        FsAppendOutcome::Committed(durable)
                    }
                    CommitOutcome::Conflict(conflicts) => {
                        if let Some(existing) = self
                            .authority_find_operation(authority_id, durable.operation_id, meter)
                            .await?
                        {
                            let value = if existing.fingerprint == durable.fingerprint {
                                FsAppendOutcome::AlreadyCommitted(existing)
                            } else {
                                FsAppendOutcome::IdempotencyConflict {
                                    committed_fingerprint: existing.fingerprint,
                                }
                            };
                            seal(meter, guard.as_deref_mut()).await?;
                            return meter.finish(value).map_err(Into::into);
                        }
                        let actual = self.fork_snapshot(authority_id, meter).await?.head;
                        let permit_rejected = conflicts.iter().any(|conflict| {
                            let path = match conflict {
                                acyclic_stream::CommitConflict::Tail { path, .. }
                                | acyclic_stream::CommitConflict::Exists { path } => path,
                            };
                            path == &gate || lease_path.as_ref().is_some_and(|lease| path == lease)
                        });
                        if permit_rejected || actual.epoch != epoch {
                            FsAppendOutcome::Fenced {
                                actual_epoch: actual.epoch,
                            }
                        } else {
                            FsAppendOutcome::Conflict { actual }
                        }
                    }
                },
                Err(AuthorityError::Authority(failure))
                    if matches!(
                        authority_stream_error(&failure),
                        Some(StreamError::IdempotencyMismatch)
                    ) =>
                {
                    let existing = self
                        .authority_find_operation(authority_id, durable.operation_id, meter)
                        .await?
                        .ok_or_else(|| {
                            authority_corrupt(
                                "Stream idempotency mismatch has no operation record",
                                meter,
                            )
                        })?;
                    seal(meter, guard.as_deref_mut()).await?;
                    FsAppendOutcome::IdempotencyConflict {
                        committed_fingerprint: existing.fingerprint,
                    }
                }
                Err(AuthorityError::Authority(failure))
                    if matches!(
                        authority_stream_error(&failure),
                        Some(StreamError::DeadlineElapsed)
                    ) =>
                {
                    FsAppendOutcome::Fenced {
                        actual_epoch: epoch,
                    }
                }
                Err(failure) => return Err(failure),
            };
            meter.finish(value).map_err(Into::into)
        }
        .await;
        work.record(meter.spent())?;
        result
    }
    async fn head(
        &self,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Head, AuthorityStoreError> {
        let mut meter = AuthorityWork::operation(work)?;
        let result = self
            .fork_snapshot(authority, &mut meter)
            .await
            .and_then(|snapshot| {
                if meter.is_native() {
                    Ok(snapshot.head)
                } else {
                    meter.finish(snapshot.head)
                }
            });
        work.record(meter.spent())?;
        result
    }

    async fn replay(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, AuthorityAllocation), AuthorityStoreError> {
        if limit.records == 0 || limit.payload_bytes == 0 {
            return Err(AuthorityStoreError::InvalidReplayLimit);
        }
        let mut meter = AuthorityWork::operation(work)?;
        let result = self
            .authority_replay(authority, after, limit, &mut meter, ReadKind::Replay)
            .await
            .and_then(|output| {
                if meter.is_native() {
                    Ok(output)
                } else {
                    meter.finish(output)
                }
            });
        work.record(meter.spent())?;
        result
    }

    async fn fence(
        &self,
        authority: AuthorityId,
        expected: Head,
        work: &mut AuthorityWork<'_>,
    ) -> Result<FenceOutcome, AuthorityStoreError> {
        let mut meter = AuthorityWork::operation(work)?;
        let result = self
            .authority_fence(authority, expected, &mut meter)
            .await
            .and_then(|value| {
                if meter.is_native() {
                    Ok(value)
                } else {
                    meter.finish(value)
                }
            });
        work.record(meter.spent())?;
        result
    }

    async fn find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<DurableCommit>, AuthorityStoreError> {
        let mut meter = AuthorityWork::operation(work)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = meter.original {
            original
                .admit(WorkCounters::UNCHARGED)
                .map_err(AuthorityStoreError::from)?;
        }
        let result = self
            .authority_find_operation(authority, operation, &mut meter)
            .await
            .and_then(|value| {
                if meter.is_native() {
                    Ok(value)
                } else {
                    meter.finish(value)
                }
            });
        work.record(meter.spent())?;
        result
    }

    async fn publication_reservation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<PublicationReservation>, AuthorityStoreError> {
        let mut meter = AuthorityWork::operation(work)?;
        let result = self.reservation(authority, operation, &mut meter).await;
        work.record(meter.spent())?;
        result
    }
    async fn reserve_publication(
        &self,
        authority: AuthorityId,
        expected: Head,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ReservationOutcome, AuthorityStoreError> {
        let mut meter = AuthorityWork::operation(work)?;
        let result = self
            .reserve(authority, expected, operation, &mut meter)
            .await;
        work.record(meter.spent())?;
        result
    }
    async fn release_publication(
        &self,
        reservation: PublicationReservation,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        let mut meter = AuthorityWork::operation(work)?;
        let result = self.release(reservation, &mut meter).await;
        work.record(meter.spent())?;
        result
    }
    async fn retire_authority(
        &self,
        authority_id: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(AuthorityStoreError::NativeOriginalUnsupported);
        }
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        let root = stream_path(authority_id, "")?;
        let path = stream_path(authority_id, "/retirement")?;
        work.record(authority_read_work(1))?;
        work.remaining()?;
        match self.provider.tail(root).await {
            Err(acyclic_stream::StreamError::NotFound) => return Ok(()),
            Ok(_) => {}
            Err(error) => return Err(map_stream_error(error)),
        }
        work.record(WorkCounters {
            authority_records_appended: 1,
            authority_bytes_written: AUTHORITY_RETIRED.len() as u64,
            backend_write_operations: 1,
            durability_operations: 1,
            ..WorkCounters::default()
        })?;
        work.remaining()?;
        let request = acyclic_stream::AppendRequest {
            path,
            records: vec![acyclic_stream::OwnedBytes::from_static(AUTHORITY_RETIRED)],
            if_tail: Some(0),
            idempotency_key: Some(stream_key(b"retire-authority", &authority_id.into_bytes())?),
        };
        match self.provider.append(request).await {
            Ok(acyclic_stream::AppendOutcome::Committed(_)) => Ok(()),
            Ok(acyclic_stream::AppendOutcome::TailConflict { .. }) => {
                work.record(authority_read_work(2))?;
                work.remaining()?;
                if self.is_retired(authority_id).await? {
                    Ok(())
                } else {
                    Err(AuthorityStoreError::Corrupt(
                        "filesystem retirement marker conflicted without retirement".to_owned(),
                    ))
                }
            }
            Err(error) => Err(map_stream_error(error)),
        }
    }
}
