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
            let mut child = meter.operation()?;
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
