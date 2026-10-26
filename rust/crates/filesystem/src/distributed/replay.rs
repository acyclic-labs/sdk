use super::*;

fn fence_attempt_key(
    authority: AuthorityId,
    expected: Head,
    owner: [u8; 16],
    revision: u64,
    work: &mut AuthorityWork<'_>,
) -> Result<acyclic_stream::IdempotencyKey, AuthorityStoreError> {
    let copying = codec_work(0, 64, 0);
    work.admit(copying)?;
    let mut identity = [0; 64];
    identity[..16].copy_from_slice(&authority.into_bytes());
    identity[16..24].copy_from_slice(&expected.epoch.get().to_le_bytes());
    identity[24..32].copy_from_slice(&expected.sequence.get().to_le_bytes());
    identity[32..].copy_from_slice(expected.digest.as_bytes());
    work.charge(copying)?;
    // Each admitted actor transition is one fencing attempt. Independent
    // writers cannot reuse another writer's historical successful CAS.
    work.key(b"fence\0", &[&identity, &owner, &revision.to_le_bytes()])
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod fence_tests {
    use super::*;

    #[test]
    fn fence_attempt_identity_binds_the_actual_owner_and_admitted_revision()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancel = CancellationToken::new();
        let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancel);
        let authority = AuthorityId::from_bytes([65; 16]);
        let head = Head::genesis(Epoch::GENESIS);
        let first = acyclic_stream::OperationClaims::default();
        let second = acyclic_stream::OperationClaims::default();
        let key = fence_attempt_key(authority, head, first.owner_identity(), 1, &mut work)?;
        assert_eq!(
            key,
            fence_attempt_key(authority, head, first.owner_identity(), 1, &mut work)?
        );
        assert_ne!(
            key,
            fence_attempt_key(authority, head, second.owner_identity(), 1, &mut work)?,
            "independent writers shared a historical fence attempt"
        );
        assert_ne!(
            key,
            fence_attempt_key(authority, head, first.owner_identity(), 2, &mut work)?,
            "distinct admitted actor transitions shared a fence attempt"
        );
        Ok(())
    }
}

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    pub(super) async fn read_records(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, AuthorityAllocation), AuthorityStoreError> {
        work.check()?;
        if limit.records == 0 || limit.payload_bytes == 0 {
            return Err(AuthorityStoreError::InvalidReplayLimit);
        }
        let mut output = work.vec_owned((limit.records as usize).min(acyclic_stream::MAX_ITEMS))?;
        let path = work.path(&self.placement, authority, "/records", None)?;
        if !work.is_native() {
            work.admit(WorkCounters {
                authority_records_read: u64::from(limit.records),
                authority_bytes_read: STREAM_RECORD_LIMIT.saturating_mul(u64::from(limit.records)),
                backend_read_operations: 1,
                ..WorkCounters::UNCHARGED
            })?;
            work.charge(WorkCounters {
                backend_read_operations: 1,
                ..WorkCounters::UNCHARGED
            })?;
        }
        let result = self
            .authority_read(
                acyclic_stream::ReadRequest {
                    path,
                    from: after.get().saturating_add(1),
                    limit: limit.records,
                },
                work,
            )
            .await;
        // Native read failure, unlike ordinary read failure, precedes the late check.
        let mut stream = if work.is_native() {
            result?
        } else {
            work.check()?;
            result?
        };
        let mut bytes = 0_u64;
        for _ in 0..limit.records {
            work.check()?;
            let record = stream.next(work).await?;
            work.check()?;
            let Some(record) = record else {
                break;
            };
            let durable = authority_decode_borrowed(authority, &record.0.value, work)?;
            if durable.sequence.get() != record.0.sequence {
                return Err(authority_corrupt(
                    "Stream sequence does not match filesystem authority sequence",
                    work,
                ));
            }
            bytes = bytes.checked_add(durable.payload.len() as u64).ok_or(
                AuthorityStoreError::ReplayRecordTooLarge {
                    observed: u64::MAX,
                    maximum: limit.payload_bytes,
                },
            )?;
            if bytes > limit.payload_bytes {
                if output.value.is_empty() {
                    return Err(AuthorityStoreError::ReplayRecordTooLarge {
                        observed: bytes,
                        maximum: limit.payload_bytes,
                    });
                }
                break;
            }
            let payload = work.bytes(durable.payload)?;
            if output.value.len() == output.value.capacity() {
                return Err(AuthorityStoreError::RejectedStatic(
                    "replay exceeded its admitted vector capacity",
                ));
            }
            output.value.push(durable_commit(
                durable,
                record.0.committed_at_micros,
                payload,
            ));
        }
        work.check()?;
        Ok(crate::async_storage::authority_parts(output))
    }

    pub(super) async fn authority_fence(
        &self,
        authority: AuthorityId,
        expected: Head,
        work: &mut AuthorityWork<'_>,
    ) -> Result<FenceOutcome, AuthorityStoreError> {
        let actor = self.actor(authority, work)?;
        let mut journal = actor.lease(work).await?;
        let mut projection = self.activate(authority, None, &mut journal, work).await?;
        // Conflict.actual is linearizable, including when this owner learned
        // the preceding head before another writer took over. Revalidation
        // reuses the one cold initializer and keeps unchanged immutable facts.
        if projection.snapshot.head != expected {
            projection = self
                .revalidate_projection(authority, &mut journal, work)
                .await?;
        }
        let snapshot = projection.snapshot;
        let revision = actor.begin(journal)?;
        if snapshot.head != expected {
            actor.complete(revision, projection, work).await?;
            return Ok(FenceOutcome::Conflict {
                actual: snapshot.head,
            });
        }
        let epoch = expected
            .epoch
            .get()
            .checked_add(1)
            .and_then(|n| Epoch::new(n).ok())
            .ok_or(AuthorityStoreError::EpochExhausted)?;
        let projection = projection.prepare_epoch_fence(epoch)?;
        let records = work.path(&self.placement, authority, "/records", None)?;
        let epochs = work.path(&self.placement, authority, "/epochs", None)?;
        let retirement = work.path(&self.placement, authority, "/retirement", None)?;
        let idempotency_key = fence_attempt_key(
            authority,
            expected,
            self.operation_claims().owner_identity(),
            revision.get(),
            work,
        )?;
        let mut conditions = work.vec(3)?;
        conditions.extend([
            CommitCondition::Absent { path: retirement },
            CommitCondition::Head {
                path: records,
                expected: snapshot
                    .head
                    .sequence
                    .get()
                    .checked_add(1)
                    .ok_or(crate::WorkError::Overflow)?,
                commit_id: snapshot.record_commit_id,
            },
            CommitCondition::Tail {
                path: epochs.clone(),
                expected: snapshot.epoch_tail,
            },
        ]);
        let value = work.stream_bytes(EPOCH_DOMAIN, &[&epoch.get().to_le_bytes()])?;
        let mut mutations = work.vec(1)?;
        work.append_record(&mut mutations, epochs, [value])?;
        let output = self
            .commit(
                CommitRequest {
                    conditions,
                    mutations,
                    idempotency_key,
                },
                None,
                work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))?;
        match &output.0 {
            CommitOutcome::Committed(envelope) => {
                actor
                    .complete(revision, projection.seal(envelope.commit_id), work)
                    .await?;
                Ok(FenceOutcome::Advanced(Head { epoch, ..expected }))
            }
            CommitOutcome::Conflict(conflicts) => Ok(FenceOutcome::Conflict {
                actual: self
                    .recover_conflict_head(authority, snapshot, conflicts, work, |_, _, _| Ok(()))
                    .await?,
            }),
        }
    }

    pub(super) async fn authority_find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<DurableCommit>, AuthorityStoreError> {
        Ok(self
            .located_operation(authority, operation, work)
            .await?
            .map(|(commit, _)| commit))
    }
}
