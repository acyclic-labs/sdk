use super::*;

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    pub(super) fn creation_request(
        &self,
        request: &mut CommitRequest,
        authority: AuthorityId,
        epoch: Epoch,
        proposed: Option<&ProposedCommit>,
        source: Option<(StreamPath, GenerationLocator, GenerationId)>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<super::actor::PreparedProjection, AuthorityStoreError> {
        let mut head = Head::genesis(epoch);
        let lineage_tail = source.as_ref().map_or(0, |(_, at, _)| at.lineage);
        let root = work.path(&self.placement, authority, "", None)?;
        let records = work.path(&self.placement, authority, "/records", None)?;
        let epochs = work.path(&self.placement, authority, "/epochs", None)?;
        let gate = work.path(&self.placement, authority, "/publication-gate", None)?;
        request.conditions.extend(
            [&root, &records, &epochs, &gate]
                .map(|path| CommitCondition::Absent { path: path.clone() }),
        );
        work.append_record(
            &mut request.mutations,
            root,
            [acyclic_stream::OwnedBytes::from_static(AUTHORITY_MARKER)],
        )?;
        let genesis = work.stream_bytes(GENESIS_DOMAIN, &[&epoch.get().to_le_bytes()])?;
        let generation = if let Some(commit) = proposed {
            let mut record = crate::streams_record::BorrowedStreamsDurableRecord {
                epoch,
                sequence: Sequence::new(1),
                operation_id: commit.operation_id,
                fingerprint: commit.fingerprint,
                previous_digest: Head::genesis(epoch).digest,
                digest: Digest::ZERO,
                payload: &commit.payload,
            };
            record.digest = work.durable_digest(authority, &record)?;
            head = Head {
                epoch: record.epoch,
                sequence: record.sequence,
                digest: record.digest,
            };
            let encoded = StreamsDurableRecord::encode_borrowed_with(
                &record,
                STREAM_RECORD_LIMIT,
                |domain, fields| work.stream_bytes(domain, fields),
            )
            .map_err(AuthorityStoreError::DurableCodec)??;
            work.append_record(&mut request.mutations, records, [genesis, encoded])?;
            // Sequence one is intrinsically addressable. Its authenticated
            // record carries the operation identity and atomic commit identity,
            // so it needs no separate operation-locator record.
            generation_from_payload(authority, record.sequence, record.payload, work)?
        } else {
            work.append_record(&mut request.mutations, records, [genesis])?;
            None
        };
        let changes = self.creation_changes(
            request,
            authority,
            proposed,
            generation,
            source.as_ref(),
            work,
        )?;
        let lineage_tail = lineage_tail
            .checked_add(u64::from(generation.is_some()))
            .ok_or(crate::WorkError::Overflow)?;
        let projection = super::actor::PreparedProjection::creation(
            head,
            proposed.map(|commit| commit.operation_id),
            changes,
            lineage_tail,
            proposed.and_then(|commit| commit.publication.as_ref()),
            super::actor::GenerationSeeds {
                inherited: source
                    .as_ref()
                    .map(|(_, at, generation)| (*generation, at.lineage)),
                published: generation,
            },
            work,
        )?;
        let epoch = work.stream_bytes(EPOCH_DOMAIN, &[&epoch.get().to_le_bytes()])?;
        work.append_record(&mut request.mutations, epochs, [epoch])?;
        work.append_record(
            &mut request.mutations,
            gate,
            [acyclic_stream::OwnedBytes::from_static(
                PUBLICATION_GATE_FREE,
            )],
        )?;
        if source.is_none() && generation.is_none() {
            return Ok(projection);
        }
        let inherited = proposed
            .and_then(|commit| commit.publication.as_ref())
            .is_some_and(|seal| seal.inherited().is_some());
        let located = GenerationLocator {
            lineage: lineage_tail,
            changes: changes.map_or(0, |(_, tail)| tail),
        };
        self.creation_lineage(
            request, authority, source, generation, located, inherited, work,
        )?;
        Ok(projection)
    }

    fn creation_lineage(
        &self,
        request: &mut CommitRequest,
        authority: AuthorityId,
        source: Option<(StreamPath, GenerationLocator, GenerationId)>,
        generation: Option<GenerationId>,
        located: GenerationLocator,
        inherited: bool,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        let lineage = work.path(&self.placement, authority, "/lineage", None)?;
        request.conditions.push(CommitCondition::Absent {
            path: lineage.clone(),
        });
        let mut records = work.vec(usize::from(generation.is_some()))?;
        if let Some(generation) = generation {
            records.push(
                work.stream_bytes(
                    LINEAGE_DOMAIN,
                    &[generation.object_id().to_bytes().as_slice()],
                )?
                .into(),
            );
        }
        let prior = source.as_ref().map(|(_, _, generation)| *generation);
        match source {
            Some((source, at, generation)) => {
                let locator = work.path(
                    &self.placement,
                    authority,
                    "/generations/",
                    Some(generation),
                )?;
                request.conditions.push(CommitCondition::Absent {
                    path: locator.clone(),
                });
                request.mutations.push(CommitMutation::Fork {
                    exclude: Vec::new(),
                    source,
                    destination: lineage,
                    at_tail: at.lineage,
                    records,
                });
                // The source generation's changes are this volume's only when
                // its changes stream was forked from the source's.
                let value = GenerationLocator {
                    lineage: at.lineage,
                    changes: if inherited { at.changes } else { 0 },
                }
                .encode(work)?;
                work.append_record(&mut request.mutations, locator, [value])?;
            }
            None => request.mutations.push(CommitMutation::Append {
                path: lineage,
                records,
            }),
        }
        if let Some(generation) = generation {
            if prior == Some(generation) {
                return Err(AuthorityStoreError::RejectedStatic(
                    "a fork cannot publish its source generation again",
                ));
            }
            let locator = work.path(
                &self.placement,
                authority,
                "/generations/",
                Some(generation),
            )?;
            request.conditions.push(CommitCondition::Absent {
                path: locator.clone(),
            });
            let value = located.encode(work)?;
            work.append_record(&mut request.mutations, locator, [value])?;
        }
        Ok(())
    }

    fn creation_changes(
        &self,
        request: &mut CommitRequest,
        authority: AuthorityId,
        proposed: Option<&ProposedCommit>,
        generation: Option<GenerationId>,
        source: Option<&(StreamPath, GenerationLocator, GenerationId)>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<(crate::VolumeId, u64)>, AuthorityStoreError> {
        if let (Some(proposed), Some(generation)) = (proposed, generation) {
            let (path, records) = self.publication_changes(
                authority,
                proposed,
                generation,
                None,
                Sequence::new(1),
                work,
            )?;
            let volume = proposed
                .publication
                .as_ref()
                .ok_or(AuthorityStoreError::RejectedStatic(
                    "creation has no namespace seal",
                ))?
                .volume();
            let mut tail = crate::foundation::usize_to_u64(records.len());
            request
                .conditions
                .push(CommitCondition::Absent { path: path.clone() });
            let inherited = proposed
                .publication
                .as_ref()
                .and_then(crate::PublicationChanges::inherited);
            match (inherited, source) {
                (None, _) => request
                    .mutations
                    .push(CommitMutation::Append { path, records }),
                // An unfiltered fork shares its source's changes up to the
                // source generation, then records its own root: O(1) in the
                // namespace size instead of listing every path.
                // The seal's source must be the volume whose lineage forks.
                (Some(origin), Some((lineage, at, _)))
                    if at.changes != 0
                        && volume_authority_id(origin) != authority
                        && work.path(
                            &self.placement,
                            volume_authority_id(origin),
                            "/lineage",
                            None,
                        )? == *lineage =>
                {
                    tail = at
                        .changes
                        .checked_add(tail)
                        .ok_or(crate::WorkError::Overflow)?;
                    request.mutations.push(CommitMutation::Fork {
                        exclude: Vec::new(),
                        source: super::changes::changes_path(&self.placement, origin, work)?,
                        destination: path,
                        at_tail: at.changes,
                        records,
                    });
                }
                (Some(_), _) => {
                    return Err(AuthorityStoreError::RejectedStatic(
                        "an inherited namespace needs its source's published changes",
                    ));
                }
            }
            return Ok(Some((volume, tail)));
        }
        Ok(None)
    }

    pub(super) async fn create<G: AuthorityGuard>(
        &self,
        authority: AuthorityId,
        epoch: Epoch,
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, AuthorityError<G::Error>> {
        let actor = self.actor(authority, work)?;
        let journal = actor.lease(work).await?;
        let revision = actor.begin(journal)?;
        match self.fork_snapshot(authority, work).await {
            Ok(snapshot) => {
                self.fork_gate(authority, work).await?;
                seal(work, guard.as_deref_mut()).await?;
                return Ok(CreateAuthorityOutcome::Existing(snapshot.head));
            }
            Err(error) if authority_missing(&error) => {}
            Err(error) => return Err(error.into()),
        }
        let mut request = CommitRequest {
            deadline_unix_millis: None,
            conditions: work.vec(4)?,
            mutations: work.vec(4)?,
            idempotency_key: work.key(
                self.placement.key_scope(),
                b"create\0",
                &[&authority.into_bytes()],
            )?,
        };
        let projection = self.creation_request(&mut request, authority, epoch, None, None, work)?;
        let outcome = self
            .commit(request, None, work, guard.as_deref_mut())
            .await?;
        if !work.is_native() {
            work.release_request()?;
        }
        let value = match &outcome.0 {
            CommitOutcome::Committed(envelope) => {
                actor
                    .complete(revision, projection.seal(envelope.commit_id), work)
                    .await?;
                CreateAuthorityOutcome::Created(Head::genesis(epoch))
            }
            CommitOutcome::Conflict(_) => {
                let snapshot = self.fork_snapshot(authority, work).await?;
                self.fork_gate(authority, work).await?;
                CreateAuthorityOutcome::Existing(snapshot.head)
            }
        };
        seal(work, guard).await?;
        Ok(value)
    }

    pub(super) async fn first<G: AuthorityGuard>(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<bool, AuthorityError<G::Error>> {
        let actor = self.actor(authority, work)?;
        let journal = actor.lease(work).await?;
        let revision = actor.begin(journal)?;
        work.check()?;
        let mut request = CommitRequest {
            deadline_unix_millis: None,
            // Initial namespace changes add one stream to the creation CAS.
            conditions: work.vec(9)?,
            mutations: work.vec(9)?,
            idempotency_key: work.key(
                self.placement.key_scope(),
                b"operation\0",
                &[&authority.into_bytes(), &commit.operation_id.into_bytes()],
            )?,
        };
        let projection = self.creation_request(
            &mut request,
            authority,
            Epoch::GENESIS,
            Some(&commit),
            None,
            work,
        )?;
        let outcome = self.commit(request, None, work, guard.as_deref_mut()).await;
        if !work.is_native() {
            work.release_request()?;
        }
        match outcome {
            Ok(output) if matches!(output.0, CommitOutcome::Committed(_)) => {
                if let CommitOutcome::Committed(envelope) = &output.0 {
                    actor
                        .complete(revision, projection.seal(envelope.commit_id), work)
                        .await?;
                }
                seal(work, guard).await?;
                return Ok(true);
            }
            Ok(_)
            | Err(AuthorityError::Authority(AuthorityStoreError::NativeStream(
                acyclic_stream::StreamError::IdempotencyMismatch,
            ))) => {}
            Err(error) => return Err(error),
        }
        let value = match self.tail_of(authority, "/records", work).await {
            Ok(tail) if tail >= 2 => {
                self.read_at(authority, "/records", 1, work, |record, work| {
                    let first = authority_decode_borrowed(authority, &record.value, work)?;
                    Ok(first.sequence == Sequence::new(1)
                        && first.operation_id == commit.operation_id
                        && first.fingerprint == commit.fingerprint)
                })
                .await?
            }
            Ok(_) => false,
            Err(error) if authority_missing(&error) => false,
            Err(error) => return Err(error.into()),
        };
        if value {
            seal(work, guard).await?;
        }
        Ok(value)
    }

    pub(super) async fn fork_generation<G: AuthorityGuard>(
        &self,
        source: crate::GenerationForkSource,
        destination: AuthorityId,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, AuthorityError<G::Error>> {
        let actor = self.actor(destination, work)?;
        let journal = actor.lease(work).await?;
        let revision = actor.begin(journal)?;
        work.check()?;
        let (lineage, at) = self
            .resolve_source_fork_point(source.authority, source.generation, work)
            .await?;
        // A generation fork has no changes stream of its own to inherit into.
        let tail = GenerationLocator {
            lineage: at.lineage,
            changes: 0,
        };
        if let Some(snapshot) = self
            .existing_fork_destination(destination, source.generation, tail, work)
            .await?
        {
            self.fork_gate(destination, work).await?;
            seal(work, guard).await?;
            return Ok(CreateAuthorityOutcome::Existing(snapshot.head));
        }
        let mut request = CommitRequest {
            deadline_unix_millis: None,
            conditions: work.vec(6)?,
            mutations: work.vec(6)?,
            idempotency_key: work.key(
                self.placement.key_scope(),
                b"fork-authority\0",
                &[&operation.into_bytes()],
            )?,
        };
        let projection = self.creation_request(
            &mut request,
            destination,
            Epoch::GENESIS,
            None,
            Some((lineage, tail, source.generation)),
            work,
        )?;
        let result = self.commit(request, None, work, guard.as_deref_mut()).await;
        if !work.is_native() {
            work.release_request()?;
        }
        let value = match result {
            Ok(output) if matches!(output.0, CommitOutcome::Committed(_)) => {
                if let CommitOutcome::Committed(envelope) = &output.0 {
                    actor
                        .complete(revision, projection.seal(envelope.commit_id), work)
                        .await?;
                }
                CreateAuthorityOutcome::Created(Head::genesis(Epoch::GENESIS))
            }
            Ok(_)
            | Err(AuthorityError::Authority(AuthorityStoreError::NativeStream(
                acyclic_stream::StreamError::IdempotencyMismatch,
            ))) => {
                let snapshot = self
                    .existing_fork_destination(destination, source.generation, tail, work)
                    .await?
                    .ok_or(AuthorityStoreError::RejectedStatic(
                        "generation fork conflicted without creating its destination",
                    ))?;
                self.fork_gate(destination, work).await?;
                CreateAuthorityOutcome::Existing(snapshot.head)
            }
            Err(error) => return Err(error),
        };
        seal(work, guard).await?;
        Ok(value)
    }

    pub(super) async fn fork_records<G: AuthorityGuard>(
        &self,
        fork: WorkspaceForkCommit,
        permit: PublicationPermit,
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<WorkspaceForkOutcome, AuthorityError<G::Error>> {
        if fork.destination == fork.retention {
            return Ok(WorkspaceForkOutcome::CreationRejected);
        }
        // Fixed identity order prevents two-volume fork custody from cycling.
        let mut identities = [fork.destination, fork.retention];
        identities.sort_by_key(|identity| identity.into_bytes());
        let first_actor = self.actor(identities[0], work)?;
        let second_actor = self.actor(identities[1], work)?;
        let first_journal = first_actor.lease(work).await?;
        let second_journal = second_actor.lease(work).await?;
        let first_revision = first_actor.begin(first_journal)?;
        let second_revision = second_actor.begin(second_journal)?;
        let lease = match fork_lease(&fork, permit) {
            Ok(lease) => lease,
            Err(outcome) => return Ok(outcome),
        };
        let lineage = match fork.lineage {
            Some(source) if source.lineage == crate::GenerationFork::PublishedPrefix => {
                let (lineage, tail) = self
                    .resolve_source_fork_point(source.authority, source.generation, work)
                    .await?;
                Some((lineage, tail, source.generation))
            }
            _ => None,
        };
        let lease_path = lease
            .map(|lease| work.lease_path(lease.workspace, lease.operation, lease.expires))
            .transpose()?;
        if let Some(path) = &lease_path {
            let active = match self.tail(path.clone(), work).await {
                Ok(1) => true,
                Ok(_) => false,
                Err(error) if authority_missing(&error) => false,
                Err(error) => return Err(error.into()),
            };
            if !active {
                let outcome = self.recover_fork_records(&fork, work).await?;
                if outcome == WorkspaceForkOutcome::Committed {
                    seal(work, guard).await?;
                }
                return Ok(outcome);
            }
        }
        let (mut request, retention_projection, destination_projection) =
            self.prepare_fork_records(&fork, lineage, lease_path.as_ref(), work)?;
        if let Some(path) = lease_path {
            request
                .conditions
                .push(CommitCondition::Tail { path, expected: 1 });
        }
        let outcome = self
            .publish_with_policy(
                request,
                lease.map(|lease| lease.expires),
                work,
                guard.as_deref_mut(),
            )
            .await;
        if !work.is_native() {
            work.release_request()?;
        }
        match outcome {
            Ok(output) if matches!(output.0, CommitOutcome::Committed(_)) => {
                if let CommitOutcome::Committed(envelope) = &output.0 {
                    let retained = retention_projection.seal(envelope.commit_id);
                    let created = destination_projection.seal(envelope.commit_id);
                    let (first, second) = if identities[0] == fork.retention {
                        (retained, created)
                    } else {
                        (created, retained)
                    };
                    first_actor.complete(first_revision, first, work).await?;
                    second_actor.complete(second_revision, second, work).await?;
                }
                seal(work, guard).await?;
                Ok(WorkspaceForkOutcome::Committed)
            }
            Ok(_)
            | Err(AuthorityError::Authority(AuthorityStoreError::NativeStream(
                acyclic_stream::StreamError::IdempotencyMismatch,
            ))) => {
                let outcome = self.recover_fork_records(&fork, work).await?;
                if outcome == WorkspaceForkOutcome::Committed {
                    seal(work, guard).await?;
                }
                Ok(outcome)
            }
            Err(error) => Err(error),
        }
    }

    fn prepare_fork_records(
        &self,
        fork: &WorkspaceForkCommit,
        lineage: Option<(StreamPath, GenerationLocator, GenerationId)>,
        lease_path: Option<&StreamPath>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<
        (
            CommitRequest,
            super::actor::PreparedProjection,
            super::actor::PreparedProjection,
        ),
        AuthorityStoreError,
    > {
        let mut request = CommitRequest {
            deadline_unix_millis: None,
            conditions: work.vec(18)?,
            mutations: work.vec(17)?,
            idempotency_key: work.key(
                self.placement.key_scope(),
                b"workspace-fork-attempt\0",
                &[
                    &fork.destination.into_bytes(),
                    &fork.creation.operation_id.into_bytes(),
                    lease_path.map_or(&[][..], |path| path.as_str().as_bytes()),
                ],
            )?,
        };
        let retention_projection = self.creation_request(
            &mut request,
            fork.retention,
            Epoch::GENESIS,
            Some(&fork.retained),
            None,
            work,
        )?;
        let destination_projection = self.creation_request(
            &mut request,
            fork.destination,
            Epoch::GENESIS,
            Some(&fork.creation),
            lineage,
            work,
        )?;
        Ok((request, retention_projection, destination_projection))
    }

    async fn recover_fork_records(
        &self,
        fork: &WorkspaceForkCommit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<WorkspaceForkOutcome, AuthorityStoreError> {
        let mut retained = None;
        let mut created = None;
        for (authority, proposed, found) in [
            (fork.retention, &fork.retained, &mut retained),
            (fork.destination, &fork.creation, &mut created),
        ] {
            *found = match self
                .located_operation(authority, proposed.operation_id, work)
                .await
            {
                Ok(value) => value,
                Err(error) if authority_missing(&error) => None,
                Err(error) => return Err(error),
            };
        }
        let retention_occupied = if retained.is_none() {
            match self.tail_of(fork.retention, "/records", work).await {
                Ok(tail) => tail > 1,
                Err(error) if authority_missing(&error) => false,
                Err(error) => return Err(error),
            }
        } else {
            false
        };
        match (retained, created) {
            (Some((retained, retained_commit)), Some((created, created_commit)))
                if retained.sequence == Sequence::new(1)
                    && created.sequence == Sequence::new(1)
                    && retained.fingerprint == fork.retained.fingerprint
                    && created.fingerprint == fork.creation.fingerprint
                    && retained_commit == created_commit =>
            {
                Ok(WorkspaceForkOutcome::Committed)
            }
            (Some((retained, _)), _) if retained.fingerprint != fork.retained.fingerprint => {
                Ok(WorkspaceForkOutcome::RetentionConflict)
            }
            _ if retention_occupied => Ok(WorkspaceForkOutcome::RetentionConflict),
            _ => Ok(WorkspaceForkOutcome::CreationRejected),
        }
    }
}

#[derive(Clone, Copy)]
struct ForkLease {
    workspace: crate::WorkspaceId,
    operation: crate::OperationLeaseId,
    expires: u64,
}

fn fork_lease(
    fork: &WorkspaceForkCommit,
    permit: PublicationPermit,
) -> Result<Option<ForkLease>, WorkspaceForkOutcome> {
    match permit {
        PublicationPermit::Unrestricted => Ok(None),
        PublicationPermit::Lease {
            authority_id,
            workspace_id,
            lease_id,
            expires_at_millis,
        } if fork.lineage.is_some_and(|source| {
            source.authority == AuthorityId::from_bytes(authority_id)
                && source.authority
                    == volume_authority_id(crate::WorkspaceId::from_bytes(workspace_id).volume_id())
        }) =>
        {
            Ok(Some(ForkLease {
                workspace: crate::WorkspaceId::from_bytes(workspace_id),
                operation: crate::OperationLeaseId::from_bytes(lease_id),
                expires: expires_at_millis,
            }))
        }
        _ => Err(WorkspaceForkOutcome::CreationRejected),
    }
}
