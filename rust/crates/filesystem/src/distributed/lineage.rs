use super::*;

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    pub(super) fn creation_request(
        &self,
        request: &mut CommitRequest,
        authority: AuthorityId,
        epoch: Epoch,
        proposed: Option<&ProposedCommit>,
        source: Option<(StreamPath, u64, GenerationId)>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        let root = work.path(authority, "", None)?;
        let records = work.path(authority, "/records", None)?;
        let epochs = work.path(authority, "/epochs", None)?;
        let gate = work.path(authority, "/publication-gate", None)?;
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
            let encoded = StreamsDurableRecord::encode_borrowed_with(
                &record,
                STREAM_RECORD_LIMIT,
                |domain, fields| work.stream_bytes(domain, fields),
            )
            .map_err(AuthorityStoreError::DurableCodec)??;
            work.append_record(&mut request.mutations, records, [genesis, encoded])?;
            generation_from_payload(authority, record.sequence, record.payload, work)?
        } else {
            work.append_record(&mut request.mutations, records, [genesis])?;
            None
        };
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
            return Ok(());
        }
        let lineage = work.path(authority, "/lineage", None)?;
        request.conditions.push(CommitCondition::Absent {
            path: lineage.clone(),
        });
        let mut records = work.vec(usize::from(generation.is_some()))?;
        if let Some(generation) = generation {
            records.push(work.stream_bytes(LINEAGE_DOMAIN, &[generation.digest().as_bytes()])?);
        }
        let inherited = source
            .as_ref()
            .map(|(_, tail, generation)| (*tail, *generation));
        match source {
            Some((source, at_tail, generation)) => {
                let locator = work.path(authority, "/generations/", Some(generation))?;
                request.conditions.push(CommitCondition::Absent {
                    path: locator.clone(),
                });
                request.mutations.push(CommitMutation::Fork {
                    source,
                    destination: lineage,
                    at_tail,
                    records,
                });
                let value = work.stream_bytes(LINEAGE_TAIL_DOMAIN, &[&at_tail.to_le_bytes()])?;
                work.append_record(&mut request.mutations, locator, [value])?;
            }
            None => request.mutations.push(CommitMutation::Append {
                path: lineage,
                records,
            }),
        }
        if let Some(generation) = generation {
            if inherited.is_some_and(|(_, prior)| prior == generation) {
                return Err(AuthorityStoreError::RejectedStatic(
                    "a fork cannot publish its source generation again",
                ));
            }
            let locator = work.path(authority, "/generations/", Some(generation))?;
            request.conditions.push(CommitCondition::Absent {
                path: locator.clone(),
            });
            let tail = inherited.map_or(0, |(tail, _)| tail).saturating_add(1);
            let value = work.stream_bytes(LINEAGE_TAIL_DOMAIN, &[&tail.to_le_bytes()])?;
            work.append_record(&mut request.mutations, locator, [value])?;
        }
        Ok(())
    }

    pub(super) async fn create<G: AuthorityGuard>(
        &self,
        authority: AuthorityId,
        epoch: Epoch,
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, AuthorityError<G::Error>> {
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
            conditions: work.vec(4)?,
            mutations: work.vec(4)?,
            idempotency_key: work.stream_key(b"create", &authority.into_bytes())?,
        };
        self.creation_request(&mut request, authority, epoch, None, None, work)?;
        let outcome = self
            .commit(request, None, work, guard.as_deref_mut())
            .await?;
        if !work.is_native() {
            work.release_request()?;
        }
        let value = match &outcome.0 {
            CommitOutcome::Committed(_) => CreateAuthorityOutcome::Created(Head::genesis(epoch)),
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
        work.check()?;
        let mut request = CommitRequest {
            conditions: work.vec(7)?,
            mutations: work.vec(7)?,
            idempotency_key: work.operation_key(authority, commit.operation_id)?,
        };
        self.creation_request(
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
                seal(work, guard).await?;
                return Ok(true);
            }
            Ok(_) => {}
            Err(AuthorityError::Authority(AuthorityStoreError::NativeStream(
                acyclic_stream::StreamError::IdempotencyMismatch,
            ))) => return Ok(false),
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
        work.check()?;
        let (lineage, tail) = self
            .resolve_source_fork_point(source.authority, source.generation, work)
            .await?;
        if let Some(snapshot) = self
            .existing_fork_destination(destination, source.generation, tail, work)
            .await?
        {
            self.fork_gate(destination, work).await?;
            seal(work, guard).await?;
            return Ok(CreateAuthorityOutcome::Existing(snapshot.head));
        }
        let mut request = CommitRequest {
            conditions: work.vec(6)?,
            mutations: work.vec(6)?,
            idempotency_key: work.stream_key(b"fork-authority", &operation.into_bytes())?,
        };
        self.creation_request(
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
        if permit != PublicationPermit::Unrestricted {
            return Err(AuthorityStoreError::RejectedStatic(
                "workspace fork does not support a publication reservation",
            )
            .into());
        }
        let lineage = match fork.lineage {
            Some(source) if source.lineage == crate::GenerationFork::PublishedPrefix => {
                let (lineage, tail) = self
                    .resolve_source_fork_point(source.authority, source.generation, work)
                    .await?;
                Some((lineage, tail, source.generation))
            }
            _ => None,
        };
        let mut request = CommitRequest {
            conditions: work.vec(14)?,
            mutations: work.vec(14)?,
            idempotency_key: work.operation_key(fork.destination, fork.creation.operation_id)?,
        };
        self.creation_request(
            &mut request,
            fork.retention,
            Epoch::GENESIS,
            Some(&fork.retained),
            None,
            work,
        )?;
        self.creation_request(
            &mut request,
            fork.destination,
            Epoch::GENESIS,
            Some(&fork.creation),
            lineage,
            work,
        )?;
        let outcome = self.commit(request, None, work, guard.as_deref_mut()).await;
        if !work.is_native() {
            work.release_request()?;
        }
        match outcome {
            Ok(output) if matches!(output.0, CommitOutcome::Committed(_)) => {
                seal(work, guard).await?;
                Ok(WorkspaceForkOutcome::Committed)
            }
            Ok(_)
            | Err(AuthorityError::Authority(AuthorityStoreError::NativeStream(
                acyclic_stream::StreamError::IdempotencyMismatch,
            ))) => {
                let mut child = work.operation()?;
                let result =
                    crate::async_storage::workspace_fork(self, fork, permit, &mut child, guard)
                        .await;
                work.absorb(&child)?;
                result
            }
            Err(error) => Err(error),
        }
    }
}
