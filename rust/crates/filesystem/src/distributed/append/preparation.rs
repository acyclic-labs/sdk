use super::*;

struct AppendFences {
    cursor: AppendCursor,
    gate: StreamPath,
    gate_tail: u64,
}

struct PublicationAppend<'a> {
    request: &'a crate::GuardedAppend,
    sequence: Sequence,
    conditions: &'a mut Vec<CommitCondition>,
    mutations: &'a mut Vec<CommitMutation>,
    fresh_lineage: bool,
}

struct GenerationAppend<'a> {
    generation: GenerationId,
    authority_id: AuthorityId,
    conditions: &'a mut Vec<CommitCondition>,
    mutations: &'a mut Vec<CommitMutation>,
    fresh_lineage: bool,
}

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    pub(super) async fn prepare_append<'a, G: AuthorityGuard, M: AppendPurpose>(
        &self,
        request: &'a crate::GuardedAppend,
        view: &AppendView<'_>,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
        purpose: M,
    ) -> Result<Preparation<M::Output<'a>>, AuthorityError<G::Error>> {
        let AppendFences {
            cursor,
            gate,
            gate_tail,
        } = match self.append_admission(request, view, work, guard).await? {
            Preparation::Ready(fences) => fences,
            Preparation::Outcome(outcome) => return Ok(Preparation::Outcome(outcome)),
        };
        let authority_id = request.authority_id;
        let epoch = request.epoch;
        let commit = &request.commit;
        let (durable, encoded) = prepare_record(request, work)?;
        let sequence = durable.sequence;
        let records = work.path(&self.placement, authority_id, "/records", None)?;
        let epochs = work.path(&self.placement, authority_id, "/epochs", None)?;
        let retirement = work.path(&self.placement, authority_id, "/retirement", None)?;
        // Four authority fences, lease, operation locator, changes, and two
        // generation lineage fences must all fit the admitted allocation.
        let mut conditions = work.vec(9)?;
        conditions.extend([
            CommitCondition::Absent { path: retirement },
            cursor.record_condition(view, &records, work)?,
            CommitCondition::Tail {
                path: epochs,
                expected: cursor.anchor().epoch_tail,
            },
            CommitCondition::Tail {
                path: gate.clone(),
                expected: gate_tail,
            },
        ]);
        let (lease_path, deadline) = match self
            .prepare_lease(request, view, &mut conditions, work)
            .await?
        {
            Preparation::Ready(lease) => lease,
            Preparation::Outcome(outcome) => return Ok(Preparation::Outcome(outcome)),
        };
        // Authority, operation locator, lineage, generation locator and changes.
        let mut mutations = work.vec(5)?;
        work.append_record(&mut mutations, records.clone(), [encoded])?;
        self.locate_operation_in(
            authority_id,
            &durable,
            &mut conditions,
            &mut mutations,
            work,
        )?;
        self.prepare_publication(
            PublicationAppend {
                request,
                sequence,
                conditions: &mut conditions,
                mutations: &mut mutations,
                fresh_lineage: M::FRESH_LINEAGE,
            },
            view,
            work,
        )
        .await?;
        // Stream retains rejected CAS requests too. Its retry key identifies
        // this exact publication attempt; the atomic locator owns semantic
        // operation identity across changing heads, gates and lease grants.
        let idempotency_key = work.key(
            self.placement.key_scope(),
            b"operation-attempt\0",
            &[
                &authority_id.into_bytes(),
                &commit.operation_id.into_bytes(),
                durable.digest.as_bytes(),
                cursor.anchor().record_commit_id.as_bytes(),
                &cursor.anchor().epoch_tail.to_le_bytes(),
                &gate_tail.to_le_bytes(),
                lease_path
                    .as_ref()
                    .map_or(&[][..], |path| path.as_str().as_bytes()),
            ],
        )?;
        Ok(Preparation::Ready(purpose.finish(
            self,
            PreparedAppend {
                receipt: AppendReceipt {
                    authority_id,
                    epoch,
                    commit,
                    durable,
                    records,
                    gate,
                    lease_path,
                },
                request: CommitRequest {
                    deadline_unix_millis: None,
                    conditions,
                    mutations,
                    idempotency_key,
                },
                deadline,
            },
            work,
        )?))
    }

    async fn append_admission<G: AuthorityGuard>(
        &self,
        request: &crate::GuardedAppend,
        view: &AppendView<'_>,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<Preparation<AppendFences>, AuthorityError<G::Error>> {
        let authority_id = request.authority_id;
        let epoch = request.epoch;
        let expected = request.expected;
        let commit = &request.commit;
        let permit = request.permit;
        let cursor = view.snapshot(self, authority_id, work).await?;
        let head = cursor.head();
        let resolve = view
            .projection
            .is_none_or(|projection| projection.origin == Some(commit.operation_id))
            || head != expected;
        if resolve
            && let Some(existing) = self
                .authority_find_operation(authority_id, commit.operation_id, work)
                .await?
        {
            seal(work, guard).await?;
            return Ok(Preparation::Outcome(
                if existing.fingerprint == commit.fingerprint {
                    FsAppendOutcome::AlreadyCommitted(existing)
                } else {
                    FsAppendOutcome::IdempotencyConflict {
                        committed_fingerprint: existing.fingerprint,
                    }
                },
            ));
        }
        if head.epoch != epoch {
            return Ok(Preparation::Outcome(FsAppendOutcome::Fenced {
                actual_epoch: head.epoch,
            }));
        }
        if head != expected {
            return Ok(Preparation::Outcome(FsAppendOutcome::Conflict {
                actual: head,
            }));
        }
        let (gate, gate_tail, active) = match view.projection {
            Some(projection) => (
                work.path(&self.placement, authority_id, "/publication-gate", None)?,
                projection.gate_tail,
                projection.active,
            ),
            None => self.reservation_gate(authority_id, work).await?,
        };
        let admitted = permits_append(permit, authority_id, expected, gate_tail, active);
        if !admitted {
            return Ok(Preparation::Outcome(FsAppendOutcome::Fenced {
                actual_epoch: head.epoch,
            }));
        }
        Ok(Preparation::Ready(AppendFences {
            cursor,
            gate,
            gate_tail,
        }))
    }

    async fn prepare_lease(
        &self,
        request: &crate::GuardedAppend,
        view: &AppendView<'_>,
        conditions: &mut Vec<CommitCondition>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Preparation<(Option<StreamPath>, Option<u64>)>, AuthorityStoreError> {
        let permit = request.permit;
        let epoch = request.epoch;
        let (lease_path, deadline) = if let PublicationPermit::Lease {
            workspace_id,
            lease_id,
            expires_at_millis,
            ..
        } = permit
        {
            let path = work.lease_path(
                crate::WorkspaceId::from_bytes(workspace_id),
                crate::OperationLeaseId::from_bytes(lease_id),
                expires_at_millis,
            )?;
            match view.tail(self, path.clone(), work).await {
                Ok(1) => {}
                Ok(_) => {
                    return Ok(Preparation::Outcome(FsAppendOutcome::Fenced {
                        actual_epoch: epoch,
                    }));
                }
                Err(error) if authority_missing(&error) => {
                    return Ok(Preparation::Outcome(FsAppendOutcome::Fenced {
                        actual_epoch: epoch,
                    }));
                }
                Err(error) => return Err(error),
            }
            conditions.push(CommitCondition::Tail {
                path: path.clone(),
                expected: 1,
            });
            (Some(path), Some(expires_at_millis))
        } else {
            (None, None)
        };
        Ok(Preparation::Ready((lease_path, deadline)))
    }

    async fn prepare_publication(
        &self,
        publication: PublicationAppend<'_>,
        view: &AppendView<'_>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        let PublicationAppend {
            request,
            sequence,
            conditions,
            mutations,
            fresh_lineage,
        } = publication;
        let authority_id = request.authority_id;
        let expected = request.expected;
        let commit = &request.commit;
        let Some(generation) =
            generation_from_payload(authority_id, sequence, &commit.payload, work)?
        else {
            return Ok(());
        };
        let (path, records) = self.publication_changes(
            authority_id,
            commit,
            generation,
            (expected.sequence != Sequence::GENESIS).then_some(expected),
            sequence,
            work,
        )?;
        let cached = view
            .projection
            .and_then(|projection| projection.changes)
            .filter(|(volume, _)| {
                commit
                    .publication
                    .as_ref()
                    .is_some_and(|seal| seal.volume() == *volume)
            })
            .map(|(_, tail)| tail);
        let tail = match view.staged(&path, work)? {
            Some((start, rows)) => start
                .checked_add(crate::foundation::usize_to_u64(rows.len()))
                .ok_or(crate::WorkError::Overflow)?,
            None if cached.is_some() => cached.ok_or(crate::WorkError::Overflow)?,
            None => match view.tail(self, path.clone(), work).await {
                Ok(tail) => tail,
                Err(error) if authority_missing(&error) => 0,
                Err(error) => return Err(error),
            },
        };
        conditions.push(if tail == 0 {
            CommitCondition::Absent { path: path.clone() }
        } else {
            CommitCondition::Tail {
                path: path.clone(),
                expected: tail,
            }
        });
        mutations.push(CommitMutation::Append { path, records });
        self.prepare_generation_lineage(
            GenerationAppend {
                generation,
                authority_id,
                conditions,
                mutations,
                fresh_lineage,
            },
            view,
            work,
        )
        .await
    }

    async fn lineage_tail(
        &self,
        lineage: &StreamPath,
        view: &AppendView<'_>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<u64, AuthorityStoreError> {
        Ok(match (view.staged(lineage, work)?, view.projection) {
            (Some((start, rows)), _) => start
                .checked_add(crate::foundation::usize_to_u64(rows.len()))
                .ok_or(crate::WorkError::Overflow)?,
            (None, Some(projection)) => projection.lineage_tail,
            (None, None) => match view.tail(self, lineage.clone(), work).await {
                Ok(tail) => tail,
                Err(error) if authority_missing(&error) => 0,
                Err(error) => return Err(error),
            },
        })
    }

    async fn prepare_generation_lineage(
        &self,
        publication: GenerationAppend<'_>,
        view: &AppendView<'_>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        let GenerationAppend {
            generation,
            authority_id,
            conditions,
            mutations,
            fresh_lineage,
        } = publication;
        let lineage = work.path(&self.placement, authority_id, "/lineage", None)?;
        let locator = work.path(
            &self.placement,
            authority_id,
            "/generations/",
            Some(generation),
        )?;
        use super::super::actor::GenerationKnowledge;
        let known = match (fresh_lineage, view.staged(&locator, work)?, view.projection) {
            (true, _, _) => GenerationKnowledge::Absent,
            (false, None, Some(projection)) => projection.generation(generation, work)?,
            _ => GenerationKnowledge::Unknown,
        };
        let tail = match known {
            GenerationKnowledge::Present(commit_id) => {
                // Bind the remembered locator to its actual creation receipt.
                // Deletion or replacement cannot pass this atomic condition.
                conditions.push(CommitCondition::Head {
                    path: locator,
                    expected: 1,
                    commit_id,
                });
                return Ok(());
            }
            GenerationKnowledge::Absent => None,
            GenerationKnowledge::Unknown => match view.tail(self, locator.clone(), work).await {
                Ok(tail) => Some(tail),
                Err(error) if authority_missing(&error) => None,
                Err(error) => return Err(error),
            },
        };
        match tail {
            Some(1) => {
                let location = view.value(self, locator, 0, work).await?;
                let tail = work
                    .decode_fixed::<8>(
                        &location.0,
                        LINEAGE_TAIL_DOMAIN,
                        "invalid Stream generation locator",
                    )
                    .map(u64::from_le_bytes)?;
                let from = tail.checked_sub(1).ok_or_else(|| {
                    authority_corrupt("generation locator selects an empty lineage", work)
                })?;
                let entry = view.value(self, lineage, from, work).await?;
                if decode_lineage_generation(&entry.0)? != generation {
                    return Err(authority_corrupt(
                        "generation locator selected another generation",
                        work,
                    ));
                }
            }
            None => {
                let tail = self.lineage_tail(&lineage, view, work).await?;
                conditions.push(if tail == 0 {
                    CommitCondition::Absent {
                        path: lineage.clone(),
                    }
                } else {
                    CommitCondition::Tail {
                        path: lineage.clone(),
                        expected: tail,
                    }
                });
                conditions.push(CommitCondition::Absent {
                    path: locator.clone(),
                });
                let entry = work.stream_bytes(
                    LINEAGE_DOMAIN,
                    &[generation.object_id().to_bytes().as_slice()],
                )?;
                work.append_record(mutations, lineage, [entry])?;
                let location = work.stream_bytes(
                    LINEAGE_TAIL_DOMAIN,
                    &[&tail.saturating_add(1).to_le_bytes()],
                )?;
                work.append_record(mutations, locator, [location])?;
            }
            Some(_) => {
                return Err(authority_corrupt(
                    "generation locator contains an invalid record count",
                    work,
                ));
            }
        }
        Ok(())
    }
}
fn prepare_record<'a>(
    request: &'a crate::GuardedAppend,
    work: &mut AuthorityWork<'_>,
) -> Result<
    (
        crate::streams_record::BorrowedStreamsDurableRecord<'a>,
        acyclic_stream::OwnedBytes,
    ),
    AuthorityStoreError,
> {
    let authority_id = request.authority_id;
    let epoch = request.epoch;
    let expected = request.expected;
    let commit = &request.commit;
    let sequence = expected
        .sequence
        .checked_next()
        .map_err(|_| AuthorityStoreError::SequenceExhausted)?;
    let mut durable = crate::streams_record::BorrowedStreamsDurableRecord {
        epoch,
        sequence,
        operation_id: commit.operation_id,
        fingerprint: commit.fingerprint,
        previous_digest: expected.digest,
        digest: Digest::ZERO,
        payload: &commit.payload,
    };
    durable.digest = work.durable_digest(authority_id, &durable)?;
    let encoded = StreamsDurableRecord::encode_borrowed_with(
        &durable,
        STREAM_RECORD_LIMIT,
        |domain, fields| work.stream_bytes(domain, fields),
    )
    .map_err(AuthorityStoreError::DurableCodec)??;
    Ok((durable, encoded))
}
