use super::*;

// Main's authority transaction, with an optional first record and lineage prefix.
fn creation_commit(
    request: &mut acyclic_stream::CommitRequest,
    authority: AuthorityId,
    epoch: Epoch,
    commit: Option<&ProposedCommit>,
    lineage: Option<(acyclic_stream::StreamPath, u64, GenerationId)>,
    meter: &mut AuthorityWork<'_>,
) -> Result<(), AuthorityStoreError> {
    use acyclic_stream::CommitCondition::Absent;
    let mut generation = None;
    let encoded = if let Some(commit) = commit {
        let sequence = Sequence::new(1);
        let mut durable = crate::streams_record::BorrowedStreamsDurableRecord {
            epoch,
            sequence,
            operation_id: commit.operation_id,
            fingerprint: commit.fingerprint,
            previous_digest: Head::genesis(epoch).digest,
            digest: Digest::ZERO,
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
        generation = generation_from_payload(authority, sequence, durable.payload, meter)?;
        Some(encoded)
    } else {
        None
    };
    let root = meter.path(authority, "", None)?;
    let records = meter.path(authority, "/records", None)?;
    let epochs = meter.path(authority, "/epochs", None)?;
    let gate = meter.path(authority, "/publication-gate", None)?;
    for path in [&root, &records, &epochs, &gate] {
        meter.push(&mut request.conditions, Absent { path: path.clone() })?;
    }
    meter.append_record(
        &mut request.mutations,
        root,
        [acyclic_stream::OwnedBytes::from_static(AUTHORITY_MARKER)],
    )?;
    lineage_commit(request, authority, lineage, generation, meter)?;
    let genesis = meter.stream_bytes(GENESIS_DOMAIN, &[&epoch.get().to_le_bytes()])?;
    match encoded {
        Some(encoded) => {
            meter.append_record(&mut request.mutations, records, [genesis, encoded])?
        }
        None => meter.append_record(&mut request.mutations, records, [genesis])?,
    }
    let epoch_record = meter.stream_bytes(EPOCH_DOMAIN, &[&epoch.get().to_le_bytes()])?;
    meter.append_record(&mut request.mutations, epochs, [epoch_record])?;
    meter.append_record(
        &mut request.mutations,
        gate,
        [acyclic_stream::OwnedBytes::from_static(
            PUBLICATION_GATE_FREE,
        )],
    )
}

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
    let tail = lineage.as_ref().map_or(0, |(_, tail, _)| *tail);
    let source_generation = lineage.as_ref().map(|(_, _, generation)| *generation);
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
            let locator_record =
                meter.stream_bytes(LINEAGE_TAIL_DOMAIN, &[&forked_at.to_le_bytes()])?;
            meter.append_record(&mut request.mutations, locator, [locator_record])?;
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
        if source_generation == Some(generation) {
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
        meter.append_record(&mut request.mutations, locator, [locator_record])?;
    }
    Ok(())
}

pub(super) async fn creation<P: acyclic_stream::StreamProvider, G: AuthorityGuard>(
    store: &StreamAuthorityStore<P>,
    authority: AuthorityId,
    epoch: Epoch,
    meter: &mut AuthorityWork<'_>,
    mut guard: Option<&mut G>,
) -> Result<CreateAuthorityOutcome, AuthorityError<G::Error>> {
    match store.fork_snapshot(authority, meter).await {
        Ok(snapshot) => {
            store.fork_gate(authority, meter).await?;
            seal(meter, guard.as_deref_mut()).await?;
            return Ok(CreateAuthorityOutcome::Existing(snapshot.head));
        }
        Err(failure) if authority_missing(&failure) => {}
        Err(failure) => return Err(failure.into()),
    }
    let idempotency_key = meter.stream_key(b"create", &authority.into_bytes())?;
    let mut request = acyclic_stream::CommitRequest {
        conditions: meter.vec(4)?,
        mutations: meter.vec(4)?,
        idempotency_key,
    };
    creation_commit(&mut request, authority, epoch, None, None, meter)?;
    let result = store
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
            let snapshot = store.fork_snapshot(authority, meter).await?;
            store.fork_gate(authority, meter).await?;
            seal(meter, guard.as_deref_mut()).await?;
            CreateAuthorityOutcome::Existing(snapshot.head)
        }
    };
    Ok(value)
}

pub(super) async fn first_record<P: acyclic_stream::StreamProvider, G: AuthorityGuard>(
    store: &StreamAuthorityStore<P>,
    authority: AuthorityId,
    commit: ProposedCommit,
    meter: &mut AuthorityWork<'_>,
    mut guard: Option<&mut G>,
) -> Result<bool, AuthorityError<G::Error>> {
    meter.check()?;
    let idempotency_key = meter.operation_key(authority, commit.operation_id)?;
    let mut request = acyclic_stream::CommitRequest {
        conditions: meter.vec(7)?,
        mutations: meter.vec(7)?,
        idempotency_key,
    };
    creation_commit(
        &mut request,
        authority,
        Epoch::GENESIS,
        Some(&commit),
        None,
        meter,
    )?;
    let result = store
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
    let tail = store.tail(records.clone(), meter).await;
    let value = match tail {
        Ok(tail) if tail >= 2 => {
            let record = store.record(records, 1, meter).await?;
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

pub(super) async fn workspace_fork<P: acyclic_stream::StreamProvider, G: AuthorityGuard>(
    store: &StreamAuthorityStore<P>,
    fork: WorkspaceForkCommit,
    permit: PublicationPermit,
    meter: &mut AuthorityWork<'_>,
    mut guard: Option<&mut G>,
) -> Result<WorkspaceForkOutcome, AuthorityError<G::Error>> {
    if !matches!(permit, PublicationPermit::Unrestricted) {
        return Err(AuthorityStoreError::RejectedStatic(
            "workspace fork does not support a publication reservation",
        )
        .into());
    }
    let request = async {
        let meter = &mut *meter;
        let idempotency_key = meter.operation_key(fork.destination, fork.creation.operation_id)?;
        let mut request = acyclic_stream::CommitRequest {
            conditions: meter.vec(14)?,
            mutations: meter.vec(14)?,
            idempotency_key,
        };
        let lineage = match fork.lineage {
            Some(source) if source.lineage == crate::GenerationFork::PublishedPrefix => {
                let (source_lineage, forked_at) = store
                    .resolve_source_fork_point(source.authority, source.generation, meter)
                    .await?;
                Some((source_lineage, forked_at, source.generation))
            }
            Some(_) | None => None,
        };
        creation_commit(
            &mut request,
            fork.retention,
            Epoch::GENESIS,
            Some(&fork.retained),
            None,
            meter,
        )?;
        creation_commit(
            &mut request,
            fork.destination,
            Epoch::GENESIS,
            Some(&fork.creation),
            lineage,
            meter,
        )?;
        Ok::<_, AuthorityStoreError>(request)
    }
    .await?;
    let result = match store
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
            store,
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

pub(super) async fn generation_fork<P: acyclic_stream::StreamProvider, G: AuthorityGuard>(
    store: &StreamAuthorityStore<P>,
    source: crate::GenerationForkSource,
    destination_authority: AuthorityId,
    operation_id: OperationId,
    meter: &mut AuthorityWork<'_>,
    mut guard: Option<&mut G>,
) -> Result<CreateAuthorityOutcome, AuthorityError<G::Error>> {
    meter.check()?;
    let (source_lineage, forked_at) = store
        .resolve_source_fork_point(source.authority, source.generation, meter)
        .await?;
    if let Some(snapshot) = store
        .existing_fork_destination(destination_authority, source.generation, forked_at, meter)
        .await?
    {
        store.fork_gate(destination_authority, meter).await?;
        seal(meter, guard.as_deref_mut()).await?;
        return Ok(CreateAuthorityOutcome::Existing(snapshot.head));
    }
    let mut request = acyclic_stream::CommitRequest {
        conditions: meter.vec(6)?,
        mutations: meter.vec(6)?,
        idempotency_key: meter.stream_key(b"fork-authority", &operation_id.into_bytes())?,
    };
    creation_commit(
        &mut request,
        destination_authority,
        Epoch::GENESIS,
        None,
        Some((source_lineage, forked_at, source.generation)),
        meter,
    )?;
    let committed = match store
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
            let snapshot = store
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
            store.fork_gate(destination_authority, meter).await?;
            seal(meter, guard.as_deref_mut()).await?;
            Ok(CreateAuthorityOutcome::Existing(snapshot.head))
        }
    }
}
