use super::*;

/// Adds the authority creation and first durable record to one transaction.
pub(super) fn first_record_commit(
    request: &mut acyclic_stream::CommitRequest,
    authority: AuthorityId,
    commit: &ProposedCommit,
    lineage: Option<(acyclic_stream::StreamPath, u64, GenerationId)>,
    meter: &mut AuthorityWork<'_>,
) -> Result<(), AuthorityStoreError> {
    use acyclic_stream::CommitCondition::Absent;
    use acyclic_stream::CommitMutation::Append;
    let epoch = Epoch::GENESIS;
    let sequence = Sequence::new(1);
    let previous = Head::genesis(epoch);
    let mut durable = crate::streams_record::BorrowedStreamsDurableRecord {
        epoch,
        sequence,
        operation_id: commit.operation_id,
        fingerprint: commit.fingerprint,
        previous_digest: previous.digest,
        digest: Digest::from_bytes([0; 32]),
        payload: &commit.payload,
    };
    durable.digest = meter.durable_digest(authority, &durable)?;
    let encoded = StreamsDurableRecord::encode_borrowed_with(
        &durable,
        STREAM_RECORD_LIMIT,
        |domain, fields| meter.stream_bytes(domain, fields),
    )
    .map_err(|error| {
        if meter.is_native() {
            AuthorityStoreError::DurableCodec(error)
        } else {
            AuthorityStoreError::Rejected(error.to_string())
        }
    })??;
    let root = meter.path(authority, "", None)?;
    let records = meter.path(authority, "/records", None)?;
    let epochs = meter.path(authority, "/epochs", None)?;
    let gate = meter.path(authority, "/publication-gate", None)?;
    for path in [&root, &records, &epochs, &gate] {
        let condition = Absent { path: path.clone() };
        meter.push(&mut request.conditions, condition)?;
    }
    meter.append_record(
        &mut request.mutations,
        root,
        acyclic_stream::OwnedBytes::from_static(AUTHORITY_MARKER),
    )?;
    let generation = generation_from_payload(authority, sequence, durable.payload, meter)?;
    lineage_commit(request, authority, lineage, generation, meter)?;
    let mut record_values = meter.vec(2)?;
    let genesis = meter.stream_bytes(GENESIS_DOMAIN, &[&epoch.get().to_le_bytes()])?;
    meter.push(&mut record_values, genesis)?;
    meter.push(&mut record_values, encoded)?;
    meter.push(
        &mut request.mutations,
        Append {
            path: records,
            records: record_values,
        },
    )?;
    let mut epoch_values = meter.vec(1)?;
    let epoch_record = meter.stream_bytes(EPOCH_DOMAIN, &[&epoch.get().to_le_bytes()])?;
    meter.push(&mut epoch_values, epoch_record)?;
    meter.push(
        &mut request.mutations,
        Append {
            path: epochs,
            records: epoch_values,
        },
    )?;
    meter.append_record(
        &mut request.mutations,
        gate,
        acyclic_stream::OwnedBytes::from_static(PUBLICATION_GATE_FREE),
    )?;
    Ok(())
}

/// Adds the forked prefix and newly published generation to the authority lineage.
pub(super) fn lineage_commit(
    request: &mut acyclic_stream::CommitRequest,
    authority: AuthorityId,
    lineage: Option<(acyclic_stream::StreamPath, u64, GenerationId)>,
    generation: Option<GenerationId>,
    meter: &mut AuthorityWork<'_>,
) -> Result<(), AuthorityStoreError> {
    use acyclic_stream::CommitCondition::Absent;
    use acyclic_stream::CommitMutation::{Append, Fork};
    if lineage.is_none() && generation.is_none() {
        return Ok(());
    }
    let lineage_path = meter.path(authority, "/lineage", None)?;
    let condition = Absent {
        path: lineage_path.clone(),
    };
    meter.push(&mut request.conditions, condition)?;
    let mut entries = meter.vec(usize::from(generation.is_some()))?;
    if let Some(generation) = generation {
        let entry = meter.stream_bytes(LINEAGE_DOMAIN, &[generation.digest().as_bytes()])?;
        meter.push(&mut entries, entry)?;
    }
    let mut tail = 0;
    let mut forked_locator = None;
    match lineage {
        Some((source_lineage, forked_at, source_generation)) => {
            let locator = meter.path(authority, "/generations/", Some(source_generation))?;
            let condition = Absent {
                path: locator.clone(),
            };
            meter.push(&mut request.conditions, condition)?;
            meter.push(
                &mut request.mutations,
                Fork {
                    source: source_lineage,
                    destination: lineage_path,
                    at_tail: forked_at,
                    records: entries,
                },
            )?;
            let mut locator_values = meter.vec(1)?;
            let locator_record =
                meter.stream_bytes(LINEAGE_TAIL_DOMAIN, &[&forked_at.to_le_bytes()])?;
            meter.push(&mut locator_values, locator_record)?;
            let append = Append {
                path: locator.clone(),
                records: locator_values,
            };
            meter.push(&mut request.mutations, append)?;
            tail = forked_at;
            forked_locator = Some(locator);
        }
        None => meter.push(
            &mut request.mutations,
            Append {
                path: lineage_path,
                records: entries,
            },
        )?,
    }
    if let Some(generation) = generation {
        let locator = meter.path(authority, "/generations/", Some(generation))?;
        if forked_locator.as_ref() == Some(&locator) {
            return Err(AuthorityStoreError::Rejected(
                "a fork cannot publish its source generation again".to_owned(),
            ));
        }
        let locator_record = meter.stream_bytes(
            LINEAGE_TAIL_DOMAIN,
            &[&tail.saturating_add(1).to_le_bytes()],
        )?;
        let condition = Absent {
            path: locator.clone(),
        };
        meter.push(&mut request.conditions, condition)?;
        meter.append_record(&mut request.mutations, locator, locator_record)?;
    }
    Ok(())
}

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
        let result = async {
            let meter = &mut meter;

            match self.fork_snapshot(authority, meter).await {
                Ok(snapshot) => {
                    self.fork_gate(authority, meter).await?;
                    seal(meter, guard.as_deref_mut()).await?;
                    return Ok(CreateAuthorityOutcome::Existing(snapshot.head));
                }
                Err(failure) if authority_missing(&failure) => {}
                Err(failure) => return Err(failure.into()),
            }
            let idempotency_key = meter.stream_key(b"create", &authority.into_bytes())?;
            let mut conditions = meter.vec(4)?;
            let mut mutations = meter.vec(4)?;
            for (suffix, domain, static_value) in [
                ("", None, Some(AUTHORITY_MARKER)),
                ("/records", Some(GENESIS_DOMAIN), None),
                ("/epochs", Some(EPOCH_DOMAIN), None),
                ("/publication-gate", None, Some(PUBLICATION_GATE_FREE)),
            ] {
                let path = meter.path(authority, suffix, None)?;
                let condition = acyclic_stream::CommitCondition::Absent { path: path.clone() };
                meter.push(&mut conditions, condition)?;
                let mut records = meter.vec(1)?;
                let value = if let Some(value) = static_value {
                    acyclic_stream::OwnedBytes::from_static(value)
                } else {
                    meter.stream_bytes(
                        domain.expect("epoch record domain"),
                        &[&epoch.get().to_le_bytes()],
                    )?
                };
                meter.push(&mut records, value)?;
                meter.push(
                    &mut mutations,
                    acyclic_stream::CommitMutation::Append { path, records },
                )?;
            }
            let request = acyclic_stream::CommitRequest {
                conditions,
                mutations,
                idempotency_key,
            };
            let result = self
                .commit(request, None, meter, guard.as_deref_mut())
                .await?;
            if !meter.is_native() {
                meter.release_request()?;
            }
            let value = match &result.0 {
                acyclic_stream::CommitOutcome::Committed(_) => {
                    seal(meter, guard.as_deref_mut()).await?;
                    CreateAuthorityOutcome::Created(Head::genesis(epoch))
                }
                acyclic_stream::CommitOutcome::Conflict(_) => {
                    let snapshot = self.fork_snapshot(authority, meter).await?;
                    self.fork_gate(authority, meter).await?;
                    seal(meter, guard.as_deref_mut()).await?;
                    CreateAuthorityOutcome::Existing(snapshot.head)
                }
            };
            Ok(value)
        }
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
        let result = async {
            let meter = &mut meter;

            meter.check()?;
            let idempotency_key = meter.operation_key(authority, commit.operation_id)?;
            let mut request = acyclic_stream::CommitRequest {
                conditions: meter.vec(7)?,
                mutations: meter.vec(7)?,
                idempotency_key,
            };
            first_record_commit(&mut request, authority, &commit, None, meter)?;
            let result = self
                .commit(request, None, meter, guard.as_deref_mut())
                .await;
            if !meter.is_native() {
                meter.release_request()?;
            }
            match result {
                Ok(outcome) => match &outcome.0 {
                    acyclic_stream::CommitOutcome::Committed(_) => {
                        // An ACK confirms this operation, including keyed replay.
                        seal(meter, guard.as_deref_mut()).await?;
                        return Ok(true);
                    }
                    acyclic_stream::CommitOutcome::Conflict(_) => {}
                },
                Err(AuthorityError::Authority(failure))
                    if matches!(
                        authority_stream_error(&failure),
                        Some(acyclic_stream::StreamError::IdempotencyMismatch)
                    ) =>
                {
                    return Ok(false);
                }
                Err(AuthorityError::Authority(failure)) => return Err(failure.into()),
                Err(AuthorityError::Owner(error)) => return Err(AuthorityError::Owner(error)),
            }
            let records = meter.path(authority, "/records", None)?;
            let path_bytes = records.as_str().len() as u64;
            let tail = self.tail(records.clone(), meter).await;
            let value = match tail {
                Ok(tail) if tail >= 2 => {
                    let record = self.record(records, 1, meter).await?;
                    let first = meter.decode_durable(&record.0.value)?;
                    let digest = meter.durable_digest(authority, &first)?;
                    if digest != first.digest {
                        return Err(AuthorityStoreError::CorruptStatic(
                            "Stream durable record digest mismatch",
                        )
                        .into());
                    }
                    first.sequence == Sequence::new(1)
                        && first.operation_id == commit.operation_id
                        && first.fingerprint == commit.fingerprint
                }
                Ok(_) => {
                    drop(records);
                    false
                }
                Err(failure) if authority_missing(&failure) => {
                    drop(records);
                    false
                }
                Err(failure) => return Err(failure.into()),
            };
            meter.release(path_bytes)?;
            if value {
                seal(meter, guard.as_deref_mut()).await?;
            }
            Ok(value)
        }
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
        let result = async {
            let meter = &mut meter;

            if !matches!(permit, PublicationPermit::Unrestricted) {
                return Err(AuthorityStoreError::RejectedStatic(
                    "workspace fork does not support a publication reservation",
                )
                .into());
            }
            let request = async {
                let meter = &mut *meter;
                let idempotency_key =
                    meter.operation_key(fork.destination, fork.creation.operation_id)?;
                let mut request = acyclic_stream::CommitRequest {
                    conditions: meter.vec(14)?,
                    mutations: meter.vec(14)?,
                    idempotency_key,
                };
                let lineage = match fork.lineage {
                    Some(source) if source.lineage == crate::GenerationFork::PublishedPrefix => {
                        let (source_lineage, forked_at) = self
                            .resolve_source_fork_point(source.authority, source.generation, meter)
                            .await?;
                        Some((source_lineage, forked_at, source.generation))
                    }
                    Some(_) | None => None,
                };
                first_record_commit(&mut request, fork.retention, &fork.retained, None, meter)?;
                first_record_commit(
                    &mut request,
                    fork.destination,
                    &fork.creation,
                    lineage,
                    meter,
                )?;
                Ok::<_, AuthorityStoreError>(request)
            }
            .await?;
            let result = match self
                .commit(request, None, meter, guard.as_deref_mut())
                .await
            {
                Ok(output) => Some(output),
                Err(AuthorityError::Authority(failure))
                    if matches!(
                        authority_stream_error(&failure),
                        Some(acyclic_stream::StreamError::IdempotencyMismatch)
                    ) =>
                {
                    None
                }
                Err(AuthorityError::Authority(failure)) => return Err(failure.into()),
                Err(AuthorityError::Owner(error)) => return Err(AuthorityError::Owner(error)),
            };
            if !meter.is_native() {
                meter.release_request()?;
            }
            match result.as_ref().map(|output| &output.0) {
                Some(acyclic_stream::CommitOutcome::Committed(_)) => {
                    seal(meter, guard.as_deref_mut()).await?;
                    return Ok(WorkspaceForkOutcome::Committed);
                }
                Some(acyclic_stream::CommitOutcome::Conflict(_)) | None => {}
            }
            {
                let mut child = meter.operation_work()?;
                let result = crate::async_storage::workspace_fork(
                    self,
                    fork,
                    permit,
                    &mut child,
                    guard.as_deref_mut(),
                )
                .await;
                meter.absorb(&child)?;
                result
            }
        }
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
        let result = async {
            let meter = &mut meter;

            meter.check()?;
            let (source_lineage, forked_at) = self
                .resolve_source_fork_point(source.authority, source.generation, meter)
                .await?;
            if let Some(snapshot) = self
                .existing_fork_destination(
                    destination_authority,
                    source.generation,
                    forked_at,
                    meter,
                )
                .await?
            {
                self.fork_gate(destination_authority, meter).await?;
                seal(meter, guard.as_deref_mut()).await?;
                return Ok(CreateAuthorityOutcome::Existing(snapshot.head));
            }
            let request = (|| {
                let meter = &mut *meter;
                let root = meter.path(destination_authority, "", None)?;
                let records_path = meter.path(destination_authority, "/records", None)?;
                let epochs_path = meter.path(destination_authority, "/epochs", None)?;
                let locator_path = meter.path(
                    destination_authority,
                    "/generations/",
                    Some(source.generation),
                )?;
                let lineage = meter.path(destination_authority, "/lineage", None)?;
                let gate_path = meter.path(destination_authority, "/publication-gate", None)?;
                use acyclic_stream::{CommitCondition, CommitMutation};
                let mut conditions = meter.vec::<CommitCondition>(6)?;
                for path in [
                    &root,
                    &records_path,
                    &epochs_path,
                    &locator_path,
                    &lineage,
                    &gate_path,
                ] {
                    let path = path.clone();
                    meter.push(&mut conditions, CommitCondition::Absent { path })?;
                }
                let mut mutations = meter.vec::<CommitMutation>(6)?;
                meter.append_record(
                    &mut mutations,
                    root,
                    acyclic_stream::OwnedBytes::from_static(AUTHORITY_MARKER),
                )?;
                meter.push(
                    &mut mutations,
                    CommitMutation::Fork {
                        source: source_lineage,
                        destination: lineage,
                        at_tail: forked_at,
                        records: Vec::new(),
                    },
                )?;
                let genesis =
                    meter.stream_bytes(GENESIS_DOMAIN, &[&Epoch::GENESIS.get().to_le_bytes()])?;
                meter.append_record(&mut mutations, records_path, genesis)?;
                let epoch =
                    meter.stream_bytes(EPOCH_DOMAIN, &[&Epoch::GENESIS.get().to_le_bytes()])?;
                meter.append_record(&mut mutations, epochs_path, epoch)?;
                let locator =
                    meter.stream_bytes(LINEAGE_TAIL_DOMAIN, &[&forked_at.to_le_bytes()])?;
                meter.append_record(&mut mutations, locator_path, locator)?;
                meter.append_record(
                    &mut mutations,
                    gate_path,
                    acyclic_stream::OwnedBytes::from_static(PUBLICATION_GATE_FREE),
                )?;
                let idempotency_key =
                    meter.stream_key(b"fork-authority", &operation_id.into_bytes())?;
                Ok::<_, AuthorityStoreError>(acyclic_stream::CommitRequest {
                    conditions,
                    mutations,
                    idempotency_key,
                })
            })()?;
            let committed = match self
                .commit(request, None, meter, guard.as_deref_mut())
                .await
            {
                Ok(output) => Some(output),
                Err(AuthorityError::Authority(failure))
                    if matches!(
                        authority_stream_error(&failure),
                        Some(acyclic_stream::StreamError::IdempotencyMismatch)
                    ) =>
                {
                    None
                }
                Err(AuthorityError::Authority(failure)) => return Err(failure.into()),
                Err(AuthorityError::Owner(error)) => return Err(AuthorityError::Owner(error)),
            };
            if !meter.is_native() {
                meter.release_request()?;
            }
            match committed.as_ref().map(|output| &output.0) {
                Some(acyclic_stream::CommitOutcome::Committed(_)) => {
                    seal(meter, guard.as_deref_mut()).await?;
                    Ok(CreateAuthorityOutcome::Created(Head::genesis(
                        Epoch::GENESIS,
                    )))
                }
                Some(acyclic_stream::CommitOutcome::Conflict(_)) | None => {
                    let snapshot = self
                        .existing_fork_destination(
                            destination_authority,
                            source.generation,
                            forked_at,
                            meter,
                        )
                        .await?
                        .ok_or_else(|| {
                            AuthorityStoreError::RejectedStatic(
                                "generation fork conflicted without creating its destination",
                            )
                        })?;
                    self.fork_gate(destination_authority, meter).await?;
                    seal(meter, guard.as_deref_mut()).await?;
                    Ok(CreateAuthorityOutcome::Existing(snapshot.head))
                }
            }
        }
        .await;
        work.record(meter.spent())?;
        result
    }
    async fn compare_and_append<G: AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<(FsAppendOutcome, AuthorityAllocation), AuthorityError<G::Error>> {
        let mut meter = AuthorityWork::operation(work)?;
        #[cfg(not(target_arch = "wasm32"))]
        let allocation = if let Some(original) = meter.original {
            Some(
                original
                    .claim(0, 0, WorkCounters::UNCHARGED)
                    .map_err(AuthorityStoreError::from)?,
            )
        } else {
            None
        };
        #[cfg(target_arch = "wasm32")]
        let allocation = None;
        let result = async {
            let meter = &mut meter;

            use acyclic_stream::{CommitCondition, CommitMutation, CommitOutcome, StreamError};
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
            let mut record_values = meter.vec(1)?;
            meter.push(&mut record_values, encoded)?;
            meter.push(
                &mut mutations,
                CommitMutation::Append {
                    path: records.clone(),
                    records: record_values,
                },
            )?;
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
                        let mut entries = meter.vec(1)?;
                        let entry = meter
                            .stream_bytes(LINEAGE_DOMAIN, &[generation.digest().as_bytes()])?;
                        meter.push(&mut entries, entry)?;
                        meter.push(
                            &mut mutations,
                            CommitMutation::Append {
                                path: lineage,
                                records: entries,
                            },
                        )?;
                        let mut locations = meter.vec(1)?;
                        let location = meter.stream_bytes(
                            LINEAGE_TAIL_DOMAIN,
                            &[&lineage_tail.saturating_add(1).to_le_bytes()],
                        )?;
                        meter.push(&mut locations, location)?;
                        meter.push(
                            &mut mutations,
                            CommitMutation::Append {
                                path: locator,
                                records: locations,
                            },
                        )?;
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
        result.map(|value| (value, allocation))
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
    ) -> Result<(Option<DurableCommit>, AuthorityAllocation), AuthorityStoreError> {
        let mut meter = AuthorityWork::operation(work)?;
        #[cfg(not(target_arch = "wasm32"))]
        let allocation = if let Some(original) = meter.original {
            Some(
                original
                    .claim(0, 0, WorkCounters::UNCHARGED)
                    .map_err(AuthorityStoreError::from)?,
            )
        } else {
            None
        };
        #[cfg(target_arch = "wasm32")]
        let allocation = None;
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
        result.map(|value| (value, allocation))
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
