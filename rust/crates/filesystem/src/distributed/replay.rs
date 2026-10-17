use super::*;

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
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
        let path = work.path(authority, "/records", None)?;
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
            let read = WorkCounters {
                authority_records_read: 1,
                authority_bytes_read: if work.is_native() {
                    0
                } else {
                    record.0.value.len() as u64
                },
                ..WorkCounters::UNCHARGED
            };
            work.admit(read)?;
            work.charge(read)?;
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
                if output.0.is_empty() {
                    return Err(AuthorityStoreError::ReplayRecordTooLarge {
                        observed: bytes,
                        maximum: limit.payload_bytes,
                    });
                }
                break;
            }
            let payload = work.bytes(durable.payload)?;
            if output.0.len() == output.0.capacity() {
                return Err(AuthorityStoreError::RejectedStatic(
                    "replay exceeded its admitted vector capacity",
                ));
            }
            output.0.push(durable_commit(
                durable,
                record.0.committed_at_micros,
                payload,
            ));
        }
        work.check()?;
        Ok(output)
    }

    pub(super) async fn authority_fence(
        &self,
        authority: AuthorityId,
        expected: Head,
        work: &mut AuthorityWork<'_>,
    ) -> Result<FenceOutcome, AuthorityStoreError> {
        work.check()?;
        let snapshot = self.fork_snapshot(authority, work).await?;
        if snapshot.head != expected {
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
        let records = work.path(authority, "/records", None)?;
        let epochs = work.path(authority, "/epochs", None)?;
        let retirement = work.path(authority, "/retirement", None)?;
        let copying = codec_work(0, 64, 0);
        work.admit(copying)?;
        let mut identity = [0; 64];
        identity[..16].copy_from_slice(&authority.into_bytes());
        identity[16..24].copy_from_slice(&expected.epoch.get().to_le_bytes());
        identity[24..32].copy_from_slice(&expected.sequence.get().to_le_bytes());
        identity[32..].copy_from_slice(expected.digest.as_bytes());
        work.charge(copying)?;
        let idempotency_key = work.key(b"fence\0", &[&identity])?;
        let mut conditions = work.vec(3)?;
        conditions.extend([
            CommitCondition::Absent { path: retirement },
            CommitCondition::Tail {
                path: records,
                expected: snapshot.head.sequence.get() + 1,
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
            CommitOutcome::Committed(_) => Ok(FenceOutcome::Advanced(Head { epoch, ..expected })),
            CommitOutcome::Conflict(_) => Ok(FenceOutcome::Conflict {
                actual: self.fork_snapshot(authority, work).await?.head,
            }),
        }
    }

    pub(super) async fn authority_find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<DurableCommit>, AuthorityStoreError> {
        self.operation_attempt(authority, operation, work)
            .await
            .map(|(_, _, record)| record)
    }

    pub(super) async fn operation_attempt(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(u64, acyclic_stream::IdempotencyKey, Option<DurableCommit>), AuthorityStoreError>
    {
        work.check()?;
        self.authority_require_live(authority, work).await?;
        let mut attempt = 0_u64;
        loop {
            let key = operation_key(authority, operation, attempt, work)?;
            let observed = self.authority_inspect(key.clone(), work).await;
            if !work.is_native() {
                work.check()?;
            }
            let observation = observed?;
            if work.is_native() {
                work.check()?;
            }
            let Some(observation) = &observation.0 else {
                return Ok((attempt, key, None));
            };
            let acyclic_stream::IdempotencyOutcome::Commit(outcome) = &observation.outcome else {
                return Err(authority_corrupt(
                    "operation identity retained another Stream mutation family",
                    work,
                ));
            };
            let CommitOutcome::Committed(envelope) = outcome else {
                #[cfg(all(test, acyclic_fs_loom, acyclic_loom))]
                if super::tests::append_retry::reuse_operation_key() {
                    return Ok((attempt, key, None));
                }
                // A retained failed CAS is immutable history, not a committed
                // filesystem operation. Advance through those existing provider
                // records without replacing the logical operation's primary key.
                attempt = attempt
                    .checked_add(1)
                    .ok_or(AuthorityStoreError::SequenceExhausted)?;
                continue;
            };
            let path = work.path(authority, "/records", None)?;
            let mut output = None;
            for mutation in &envelope.mutations {
                let acyclic_stream::CommittedMutation::Append(append) = mutation else {
                    continue;
                };
                if append.path != path {
                    continue;
                }
                let record = match append.records.as_slice() {
                    [record] => record,
                    [genesis, record] => {
                        work.decode_epoch(&genesis.value, GENESIS_DOMAIN)?;
                        record
                    }
                    _ => {
                        return Err(authority_corrupt(
                            "operation appended an invalid authority record count",
                            work,
                        ));
                    }
                };
                let reading = WorkCounters {
                    authority_records_read: 1,
                    authority_bytes_read: record.value.len() as u64,
                    ..WorkCounters::UNCHARGED
                };
                work.admit(reading)?;
                work.charge(reading)?;
                let durable = authority_decode_borrowed(authority, &record.value, work)?;
                if durable.operation_id != operation || output.is_some() {
                    return Err(authority_corrupt(
                        "operation envelope does not bind its authority operation",
                        work,
                    ));
                }
                let payload = work.bytes(durable.payload)?;
                output = Some(durable_commit(durable, record.committed_at_micros, payload));
            }
            if output.is_none() {
                return Err(authority_corrupt(
                    "operation envelope omits its authority record",
                    work,
                ));
            }
            work.check()?;
            return Ok((attempt, key, output));
        }
    }
}

pub(super) fn operation_key(
    authority: AuthorityId,
    operation: OperationId,
    attempt: u64,
    work: &mut AuthorityWork<'_>,
) -> Result<acyclic_stream::IdempotencyKey, AuthorityStoreError> {
    if attempt == 0 {
        work.key(
            b"operation\0",
            &[&authority.into_bytes(), &operation.into_bytes()],
        )
    } else {
        work.key(
            b"operation-attempt\0",
            &[
                &authority.into_bytes(),
                &operation.into_bytes(),
                &attempt.to_le_bytes(),
            ],
        )
    }
}

#[cfg(all(test, not(acyclic_fs_loom)))]
mod key_tests;
