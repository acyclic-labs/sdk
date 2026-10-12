use super::*;

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    pub(super) async fn authority_replay(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &mut AuthorityWork<'_>,
        kind: ReadKind,
    ) -> Result<(Vec<DurableCommit>, AuthorityAllocation), AuthorityStoreError> {
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        if limit.records == 0 || limit.payload_bytes == 0 {
            return Err(AuthorityStoreError::InvalidReplayLimit);
        }
        let mut output = if kind == ReadKind::Accounting {
            (Vec::new(), None)
        } else {
            work.check()?;
            self.authority_require_live(authority, work).await?;
            work.vec_owned((limit.records as usize).min(acyclic_stream::MAX_ITEMS))?
        };
        let path = if kind == ReadKind::Accounting {
            stream_path(authority, "/records")?
        } else {
            work.path(authority, "/records", None)?
        };
        let mut stream = self
            .authority_read(
                acyclic_stream::ReadRequest {
                    path,
                    from: after.get().saturating_add(1),
                    limit: limit.records,
                },
                work,
                kind,
            )
            .await?;
        let mut bytes = 0_u64;
        for _ in 0..limit.records {
            let Some(record) = stream.next(work, kind).await? else {
                break;
            };
            let durable = if kind == ReadKind::Accounting {
                decode_record(&record.0)?
            } else {
                authority_decode_borrowed(authority, &record.0.value, work)?
            };
            let historical = if kind == ReadKind::Accounting {
                let mut payload = Vec::new();
                payload
                    .try_reserve_exact(durable.payload.len())
                    .map_err(|_| {
                        AuthorityStoreError::Corrupt(
                            crate::streams_record::StreamsAuthorityRecordError::AllocationFailed
                                .to_string(),
                        )
                    })?;
                payload.extend_from_slice(durable.payload);
                validate_record(authority, &record.0, &durable)?;
                Some(Bytes::from(payload))
            } else {
                None
            };
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
            let payload = match historical {
                Some(payload) => payload,
                None => work.bytes(&[], &[durable.payload])?,
            };
            if kind != ReadKind::Accounting && output.0.len() == output.0.capacity() {
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
        if kind != ReadKind::Accounting {
            work.check()?;
        }
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
        let copying = WorkCounters {
            bytes_copied: 64,
            ..WorkCounters::UNCHARGED
        };
        work.admit(copying)?;
        let mut identity = [0; 64];
        identity[..16].copy_from_slice(&authority.into_bytes());
        identity[16..24].copy_from_slice(&expected.epoch.get().to_le_bytes());
        identity[24..32].copy_from_slice(&expected.sequence.get().to_le_bytes());
        identity[32..].copy_from_slice(expected.digest.as_bytes());
        work.charge(copying)?;
        let idempotency_key = work.stream_key(b"fence", &identity)?;
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
        work.check()?;
        self.authority_require_live(authority, work).await?;
        let key = work.operation_key(authority, operation)?;
        let observation = self.authority_inspect(key, work).await?;
        let Some(observation) = &observation.0 else {
            return Ok(None);
        };
        let acyclic_stream::IdempotencyOutcome::Commit(outcome) = &observation.outcome else {
            return Err(authority_corrupt(
                "operation identity retained another Stream mutation family",
                work,
            ));
        };
        let CommitOutcome::Committed(envelope) = outcome else {
            return Ok(None);
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
            let payload = work.bytes(&[], &[durable.payload])?;
            output = Some(durable_commit(durable, record.committed_at_micros, payload));
        }
        if output.is_none() {
            return Err(authority_corrupt(
                "operation envelope omits its authority record",
                work,
            ));
        }
        work.check()?;
        Ok(output)
    }
}
