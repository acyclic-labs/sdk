use super::*;

pub(super) fn authority_corrupt(
    message: &'static str,
    meter: &AuthorityWork<'_>,
) -> AuthorityStoreError {
    if meter.is_native() {
        AuthorityStoreError::CorruptStatic(message)
    } else {
        AuthorityStoreError::Corrupt(message.to_owned())
    }
}

pub(super) fn authority_stream_error(
    error: &AuthorityStoreError,
) -> Option<&acyclic_stream::StreamError> {
    match error {
        AuthorityStoreError::NativeStream(error) => Some(error),
        _ => None,
    }
}

pub(super) fn authority_missing(error: &AuthorityStoreError) -> bool {
    matches!(error, AuthorityStoreError::Missing)
        || matches!(
            authority_stream_error(error),
            Some(acyclic_stream::StreamError::NotFound)
        )
}

pub(super) fn authority_decode_borrowed<'a>(
    authority: AuthorityId,
    encoded: &'a [u8],
    meter: &mut AuthorityWork<'_>,
) -> Result<crate::streams_record::BorrowedStreamsDurableRecord<'a>, AuthorityStoreError> {
    let borrowed = meter.decode_durable(encoded)?;
    let digest = meter.durable_digest(authority, &borrowed)?;
    if digest != borrowed.digest {
        return Err(authority_corrupt(
            "Stream authority record digest mismatch",
            meter,
        ));
    }
    Ok(borrowed)
}

pub(super) fn durable_commit(
    borrowed: crate::streams_record::BorrowedStreamsDurableRecord<'_>,
    committed_at_micros: u64,
    payload: Bytes,
) -> DurableCommit {
    DurableCommit {
        epoch: borrowed.epoch,
        sequence: borrowed.sequence,
        operation_id: borrowed.operation_id,
        fingerprint: borrowed.fingerprint,
        previous_digest: borrowed.previous_digest,
        digest: borrowed.digest,
        payload,
        settled_at_micros: (committed_at_micros != 0).then_some(committed_at_micros),
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) type ProviderAllocation = Option<acyclic_native_runtime::ReadAllocation>;
#[cfg(target_arch = "wasm32")]
pub(super) type ProviderAllocation = Option<()>;

pub(super) enum AuthorityRead {
    Plain(acyclic_stream::RecordStream),
    #[cfg(not(target_arch = "wasm32"))]
    Native((acyclic_stream::NativeRecordStream, ProviderAllocation)),
}

#[derive(Clone, Copy, PartialEq)]
pub(super) enum ReadKind {
    Exact,
    Replay,
    Accounting,
}

impl AuthorityRead {
    pub(super) async fn next(
        &mut self,
        meter: &mut AuthorityWork<'_>,
        kind: ReadKind,
    ) -> Result<Option<(acyclic_stream::Record, ProviderAllocation)>, AuthorityStoreError> {
        if kind == ReadKind::Replay {
            meter.check()?;
        }
        if kind == ReadKind::Accounting {
            meter
                .cancellation
                .check()
                .map_err(|_| AuthorityStoreError::Cancelled)?;
        }
        let value = match self {
            Self::Plain(stream) => stream
                .next()
                .await
                .map(|record| record.map(|value| (value, None)).map_err(map_stream_error))
                .transpose()?,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(stream) => stream
                .0
                .next()
                .await
                .map(|record| meter.result(record))
                .transpose()?,
        };
        if kind == ReadKind::Replay {
            meter.check()?;
        }
        if let Some(record) = &value {
            let delta = WorkCounters {
                authority_records_read: 1,
                authority_bytes_read: record.0.value.len() as u64,
                ..WorkCounters::default()
            };
            if kind != ReadKind::Replay && !meter.is_native() {
                meter.work = meter.spent().checked_add(delta)?;
                meter
                    .cancellation
                    .check()
                    .map_err(|_| AuthorityStoreError::Cancelled)?;
                if kind == ReadKind::Exact {
                    meter.spent().verify(meter.budget)?;
                }
            } else {
                meter.admit(delta)?;
                meter.charge(delta)?;
            }
        }
        Ok(value)
    }
}

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    pub(super) async fn authority_read(
        &self,
        request: acyclic_stream::ReadRequest,
        meter: &mut AuthorityWork<'_>,
        kind: ReadKind,
    ) -> Result<AuthorityRead, AuthorityStoreError> {
        if kind == ReadKind::Replay || meter.is_native() {
            meter.check()?;
        }
        let admission = WorkCounters {
            authority_records_read: u64::from(request.limit),
            authority_bytes_read: STREAM_RECORD_LIMIT.saturating_mul(u64::from(request.limit)),
            backend_read_operations: u64::from(!meter.is_native()),
            ..WorkCounters::default()
        };
        #[cfg(not(target_arch = "wasm32"))]
        if meter.is_native() {
            if kind == ReadKind::Exact {
                meter.admit(admission)?;
            }
            let result = self
                .provider
                .native_read(request, meter.native_meter())
                .await;
            let stream = meter.result(result).map(AuthorityRead::Native)?;
            if kind == ReadKind::Exact {
                meter.check()?;
            }
            return Ok(stream);
        }
        if kind != ReadKind::Replay {
            if kind == ReadKind::Exact {
                let peak = meter
                    .allocations
                    .live_bytes()
                    .checked_add(STREAM_RECORD_LIMIT)
                    .ok_or(crate::WorkError::Overflow)?;
                meter.admit(WorkCounters {
                    peak_allocation_bytes: peak,
                    ..WorkCounters::default()
                })?;
            }
            meter.admit(admission)?;
            meter.work.backend_read_operations += 1;
        } else {
            meter.admit(admission)?;
            meter.charge(WorkCounters {
                backend_read_operations: 1,
                ..WorkCounters::default()
            })?;
        }
        let result = self
            .provider
            .read(request)
            .await
            .map(AuthorityRead::Plain)
            .map_err(map_stream_error);
        if kind != ReadKind::Replay {
            let stream = result?;
            if kind == ReadKind::Exact {
                meter
                    .cancellation
                    .check()
                    .map_err(|_| AuthorityStoreError::Cancelled)?;
            }
            Ok(stream)
        } else {
            meter.check()?;
            result
        }
    }

    pub(super) async fn authority_inspect(
        &self,
        key: acyclic_stream::IdempotencyKey,
        meter: &mut AuthorityWork<'_>,
    ) -> Result<
        (
            Option<acyclic_stream::IdempotencyObservation>,
            ProviderAllocation,
        ),
        AuthorityStoreError,
    > {
        meter.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if meter.is_native() {
            let result = self
                .provider
                .native_inspect_idempotency(key, meter.native_meter())
                .await;
            let observation = meter.result(result)?;
            meter.check()?;
            return Ok(observation);
        }
        meter.charge(WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::default()
        })?;
        let result = self
            .provider
            .inspect_idempotency(key)
            .await
            .map(|value| (value, None))
            .map_err(map_stream_error);
        meter.check()?;
        result
    }

    pub(super) async fn commit<G: AuthorityGuard>(
        &self,
        request: acyclic_stream::CommitRequest,
        deadline: Option<u64>,
        meter: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<(acyclic_stream::CommitOutcome, ProviderAllocation), AuthorityError<G::Error>> {
        let submitted = meter.request_write_work(&request)?;
        meter.admit(submitted)?;
        #[cfg(not(target_arch = "wasm32"))]
        if meter.is_native() {
            let mut child = meter.operation_work()?;
            let outcome = match guard.as_deref_mut() {
                Some(guard) => {
                    guard
                        .native_commit(self.provider.as_ref(), request, deadline, &mut child)
                        .await
                }
                None => {
                    ().native_commit(self.provider.as_ref(), request, deadline, &mut child)
                        .await
                        .map(|result| {
                            result.map_err(|error| {
                                AuthorityError::Authority(error.into_error(std::convert::identity))
                            })
                        })
                }
            }
            .map_err(|error| meter.native_io_failure(error))??;
            let (value, allocation) = outcome.into_parts();
            let outcome = (value, Some(allocation));
            meter.release_request()?;
            if matches!(&outcome.0, acyclic_stream::CommitOutcome::Committed(_)) {
                meter.charge(submitted)?;
            }
            meter.check()?;
            return Ok(outcome);
        }
        meter.check()?;
        seal(meter, guard.as_deref_mut()).await?;
        meter.check()?;
        meter.admit(submitted)?;
        let result = match deadline {
            Some(deadline) => self.provider.commit_before(request, deadline).await,
            None => self.provider.commit(request).await,
        };
        meter.charge(commit_work(submitted, &result))?;
        meter.check()?;
        result
            .map(|value| (value, None))
            .map_err(|error| AuthorityError::Authority(AuthorityStoreError::NativeStream(error)))
    }

    pub(super) async fn authority_replay(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        meter: &mut AuthorityWork<'_>,
        kind: ReadKind,
    ) -> Result<(Vec<DurableCommit>, AuthorityAllocation), AuthorityStoreError> {
        if kind == ReadKind::Accounting {
            meter
                .cancellation
                .check()
                .map_err(|_| AuthorityStoreError::Cancelled)?;
        }
        if limit.records == 0 || limit.payload_bytes == 0 {
            return Err(AuthorityStoreError::InvalidReplayLimit);
        }
        let mut output = if kind == ReadKind::Accounting {
            (Vec::new(), None)
        } else {
            meter.check()?;
            self.authority_require_live(authority, meter).await?;
            meter.vec_owned((limit.records as usize).min(acyclic_stream::MAX_ITEMS))?
        };
        let path = if kind == ReadKind::Accounting {
            stream_path(authority, "/records")?
        } else {
            meter.path(authority, "/records", None)?
        };
        let mut stream = self
            .authority_read(
                acyclic_stream::ReadRequest {
                    path,
                    from: after.get().saturating_add(1),
                    limit: limit.records,
                },
                meter,
                kind,
            )
            .await?;
        let mut payload_bytes = 0_u64;
        for _ in 0..limit.records {
            let Some(record) = stream.next(meter, kind).await? else {
                break;
            };
            let borrowed = if kind == ReadKind::Accounting {
                decode_record(&record.0)?
            } else {
                authority_decode_borrowed(authority, &record.0.value, meter)?
            };
            let payload = if kind == ReadKind::Accounting {
                let mut payload = Vec::new();
                payload
                    .try_reserve_exact(borrowed.payload.len())
                    .map_err(|_| {
                        AuthorityStoreError::Corrupt(
                            crate::streams_record::StreamsAuthorityRecordError::AllocationFailed
                                .to_string(),
                        )
                    })?;
                payload.extend_from_slice(borrowed.payload);
                validate_record(authority, &record.0, &borrowed)?;
                Some(Bytes::from(payload))
            } else {
                None
            };
            payload_bytes = payload_bytes
                .checked_add(borrowed.payload.len() as u64)
                .ok_or_else(|| AuthorityStoreError::ReplayRecordTooLarge {
                    observed: u64::MAX,
                    maximum: limit.payload_bytes,
                })?;
            if payload_bytes > limit.payload_bytes {
                if output.0.is_empty() {
                    return Err(AuthorityStoreError::ReplayRecordTooLarge {
                        observed: payload_bytes,
                        maximum: limit.payload_bytes,
                    });
                }
                break;
            }
            let payload = match payload {
                Some(payload) => payload,
                None => meter.bytes(&[], &[borrowed.payload])?,
            };
            let durable = durable_commit(borrowed, record.0.committed_at_micros, payload);
            if kind == ReadKind::Accounting {
                output.0.push(durable);
            } else {
                meter.push(&mut output.0, durable)?;
            }
        }
        if kind != ReadKind::Accounting {
            meter.check()?;
        }
        Ok(output)
    }

    pub(super) async fn authority_fence(
        &self,
        authority: AuthorityId,
        expected: Head,
        meter: &mut AuthorityWork<'_>,
    ) -> Result<FenceOutcome, AuthorityStoreError> {
        meter.check()?;
        let snapshot = self.fork_snapshot(authority, meter).await?;
        if snapshot.head != expected {
            return Ok(FenceOutcome::Conflict {
                actual: snapshot.head,
            });
        }
        let epoch = expected
            .epoch
            .get()
            .checked_add(1)
            .and_then(|next| Epoch::new(next).ok())
            .ok_or_else(|| AuthorityStoreError::EpochExhausted)?;
        let records = meter.path(authority, "/records", None)?;
        let epochs = meter.path(authority, "/epochs", None)?;
        let retirement = meter.path(authority, "/retirement", None)?;
        let copying = WorkCounters {
            bytes_copied: 64,
            ..WorkCounters::default()
        };
        meter.admit(copying)?;
        let mut identity = [0_u8; 64];
        identity[..16].copy_from_slice(&authority.into_bytes());
        identity[16..24].copy_from_slice(&expected.epoch.get().to_le_bytes());
        identity[24..32].copy_from_slice(&expected.sequence.get().to_le_bytes());
        identity[32..].copy_from_slice(expected.digest.as_bytes());
        meter.charge(copying)?;
        let idempotency_key = meter.stream_key(b"fence", &identity)?;
        let mut conditions = meter.vec(3)?;
        for condition in [
            acyclic_stream::CommitCondition::Absent { path: retirement },
            acyclic_stream::CommitCondition::Tail {
                path: records,
                expected: (snapshot.head.sequence.get() + 1),
            },
            acyclic_stream::CommitCondition::Tail {
                path: epochs.clone(),
                expected: snapshot.epoch_tail,
            },
        ] {
            meter.push(&mut conditions, condition)?;
        }
        let encoded = meter.stream_bytes(EPOCH_DOMAIN, &[&epoch.get().to_le_bytes()])?;
        let mut mutations = meter.vec(1)?;
        meter.append_record(&mut mutations, epochs, [encoded])?;
        let outcome = self
            .commit(
                acyclic_stream::CommitRequest {
                    conditions,
                    mutations,
                    idempotency_key,
                },
                None,
                meter,
                None::<&mut ()>,
            )
            .await
            .map_err(|failure| match failure {
                AuthorityError::Authority(failure) => failure,
                AuthorityError::Owner(_) => {
                    unreachable!("unguarded publication has no owner error")
                }
            })?;
        match &outcome.0 {
            acyclic_stream::CommitOutcome::Committed(_) => {
                Ok(FenceOutcome::Advanced(Head { epoch, ..expected }))
            }
            acyclic_stream::CommitOutcome::Conflict(_) => {
                let actual = self.fork_snapshot(authority, meter).await?.head;
                Ok(FenceOutcome::Conflict { actual })
            }
        }
    }

    pub(super) async fn authority_find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        meter: &mut AuthorityWork<'_>,
    ) -> Result<Option<DurableCommit>, AuthorityStoreError> {
        meter.check()?;
        self.authority_require_live(authority, meter).await?;
        let mut output = None;
        let key = meter.operation_key(authority, operation)?;
        let observation = self.authority_inspect(key, meter).await?;
        let Some(observation) = &observation.0 else {
            return Ok(output);
        };
        let acyclic_stream::IdempotencyOutcome::Commit(outcome) = &observation.outcome else {
            return Err(authority_corrupt(
                "operation identity retained another Stream mutation family",
                meter,
            ));
        };
        let acyclic_stream::CommitOutcome::Committed(envelope) = outcome else {
            return Ok(output);
        };
        let records = meter.path(authority, "/records", None)?;
        for mutation in &envelope.mutations {
            let acyclic_stream::CommittedMutation::Append(append) = mutation else {
                continue;
            };
            if append.path != records {
                continue;
            }
            let record = match append.records.as_slice() {
                [record] => record,
                [genesis, record] => match meter.decode_epoch(&genesis.value, GENESIS_DOMAIN) {
                    Ok(_) => record,
                    #[cfg(not(target_arch = "wasm32"))]
                    Err(failure)
                        if matches!(
                            &failure,
                            AuthorityStoreError::NativeOriginal(_) | AuthorityStoreError::Cancelled
                        ) =>
                    {
                        return Err(failure);
                    }
                    Err(_) => {
                        return Err(authority_corrupt(
                            "operation appended an invalid authority record count",
                            meter,
                        ));
                    }
                },
                _ => {
                    return Err(authority_corrupt(
                        "operation appended an invalid authority record count",
                        meter,
                    ));
                }
            };
            let reading = WorkCounters {
                authority_records_read: 1,
                authority_bytes_read: record.value.len() as u64,
                ..WorkCounters::default()
            };
            meter.admit(reading)?;
            meter.charge(reading)?;
            let borrowed = authority_decode_borrowed(authority, &record.value, meter)?;
            if borrowed.operation_id != operation || output.is_some() {
                return Err(authority_corrupt(
                    "operation envelope does not bind its authority operation",
                    meter,
                ));
            }
            let payload = meter.bytes(&[], &[borrowed.payload])?;
            output = Some(durable_commit(
                borrowed,
                record.committed_at_micros,
                payload,
            ));
        }
        if output.is_none() {
            return Err(authority_corrupt(
                "operation envelope omits its authority record",
                meter,
            ));
        }
        meter.check()?;
        Ok(output)
    }

    pub(super) async fn record(
        &self,
        path: acyclic_stream::StreamPath,
        from: u64,
        meter: &mut AuthorityWork<'_>,
    ) -> Result<(acyclic_stream::Record, ProviderAllocation), AuthorityStoreError> {
        let mut stream = self
            .authority_read(
                acyclic_stream::ReadRequest {
                    path,
                    from,
                    limit: 1,
                },
                meter,
                ReadKind::Exact,
            )
            .await?;
        let record = stream.next(meter, ReadKind::Exact).await?.ok_or_else(|| {
            if meter.is_native() {
                AuthorityStoreError::CorruptStatic("Stream authority path is missing its record")
            } else {
                AuthorityStoreError::Corrupt("Stream returned an empty exact read".to_owned())
            }
        })?;
        if !meter.is_native() && record.0.sequence != from {
            return Err(authority_corrupt(
                "Stream returned a non-contiguous record",
                meter,
            ));
        }
        Ok(record)
    }

    pub(super) async fn tail(
        &self,
        path: acyclic_stream::StreamPath,
        meter: &mut AuthorityWork<'_>,
    ) -> Result<u64, AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if meter.is_native() {
            meter.check()?;
            let result = self.provider.native_tail(path, meter.native_meter()).await;
            let tail = meter.result(result)?;
            return Ok(tail.0);
        }
        let read = WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::UNCHARGED
        };
        meter.admit(read)?;
        meter.record(read)?;
        let result = self.provider.tail(path).await;
        meter
            .cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        result.map_err(map_stream_error)
    }
}
