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

pub(super) fn authority_missing(error: &AuthorityStoreError) -> bool {
    matches!(
        error,
        AuthorityStoreError::Missing
            | AuthorityStoreError::NativeStream(acyclic_stream::StreamError::NotFound)
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

impl AuthorityRead {
    pub(super) async fn next(
        &mut self,
        work: &AuthorityWork<'_>,
    ) -> Result<Option<(acyclic_stream::Record, ProviderAllocation)>, AuthorityStoreError> {
        let _ = work;
        match self {
            Self::Plain(stream) => stream
                .next()
                .await
                .map(|record| record.map(|value| (value, None)).map_err(map_stream_error))
                .transpose(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(stream) => stream
                .0
                .next()
                .await
                .map(|record| work.result(record))
                .transpose(),
        }
    }
}

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    pub(super) async fn authority_read(
        &self,
        request: acyclic_stream::ReadRequest,
        work: &mut AuthorityWork<'_>,
    ) -> Result<AuthorityRead, AuthorityStoreError> {
        let _ = work;
        #[cfg(not(target_arch = "wasm32"))]
        if work.is_native() {
            let meter = work.native_meter()?;
            return work
                .result(self.provider.native_read(request, meter).await)
                .map(AuthorityRead::Native);
        }
        self.provider
            .read(request)
            .await
            .map(AuthorityRead::Plain)
            .map_err(map_stream_error)
    }

    pub(super) async fn authority_inspect(
        &self,
        key: acyclic_stream::IdempotencyKey,
        work: &mut AuthorityWork<'_>,
    ) -> Result<
        (
            Option<acyclic_stream::IdempotencyObservation>,
            ProviderAllocation,
        ),
        AuthorityStoreError,
    > {
        work.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if work.is_native() {
            let meter = work.native_meter()?;
            return work.result(self.provider.native_inspect_idempotency(key, meter).await);
        }
        work.charge(WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        self.provider
            .inspect_idempotency(key)
            .await
            .map(|value| (value, None))
            .map_err(map_stream_error)
    }

    pub(super) async fn record(
        &self,
        path: acyclic_stream::StreamPath,
        from: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(acyclic_stream::Record, ProviderAllocation), AuthorityStoreError> {
        work.check()?;
        work.admit(WorkCounters {
            authority_records_read: 1,
            backend_read_operations: u64::from(!work.is_native()),
            ..WorkCounters::UNCHARGED
        })?;
        work.record(WorkCounters {
            backend_read_operations: u64::from(!work.is_native()),
            ..WorkCounters::UNCHARGED
        })?;
        let mut stream = self
            .authority_read(
                acyclic_stream::ReadRequest {
                    path,
                    from,
                    limit: 1,
                },
                work,
            )
            .await?;
        if work.is_native() {
            work.check()?;
        }
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        let record = stream.next(work).await?.ok_or_else(|| {
            authority_corrupt(
                if work.is_native() {
                    "Stream authority path is missing its record"
                } else {
                    "Stream returned an empty exact read"
                },
                work,
            )
        })?;
        let completed = WorkCounters {
            authority_records_read: 1,
            authority_bytes_read: if work.is_native() {
                0
            } else {
                record.0.value.len() as u64
            },
            ..WorkCounters::UNCHARGED
        };
        work.charge(completed)?;
        if !work.is_native() {
            work.hold_opaque(record.0.value.len() as u64)?;
        }
        if !work.is_native() && record.0.sequence != from {
            return Err(authority_corrupt(
                "Stream returned a non-contiguous record",
                work,
            ));
        }
        Ok(record)
    }

    pub(super) async fn tail(
        &self,
        path: acyclic_stream::StreamPath,
        work: &mut AuthorityWork<'_>,
    ) -> Result<u64, AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.is_native() {
            work.check()?;
            let meter = work.native_meter()?;
            return work
                .result(self.provider.native_tail(path, meter).await)
                .map(|value| value.0);
        }
        let read = WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::UNCHARGED
        };
        work.admit(read)?;
        work.record(read)?;
        let result = self.provider.tail(path).await;
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        result.map_err(map_stream_error)
    }

    pub(super) async fn commit<G: AuthorityGuard>(
        &self,
        request: acyclic_stream::CommitRequest,
        deadline: Option<u64>,
        meter: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<(acyclic_stream::CommitOutcome, ProviderAllocation), AuthorityError<G::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        let mut guard = guard;
        let submitted = meter.request_write_work(&request)?;
        meter.admit(submitted)?;
        #[cfg(not(target_arch = "wasm32"))]
        if meter.is_native() {
            let outcome = match guard.as_deref_mut() {
                Some(guard) => {
                    guard
                        .native_commit(self.provider.as_ref(), request, deadline, meter)
                        .await
                }
                None => {
                    let result = self
                        .provider
                        .native_commit(request, deadline, meter.native_meter()?)
                        .await;
                    result.map(|result| result.map_err(AuthorityError::from))
                }
            }
            .map_err(|error| meter.native_io_failure(error))??;
            let (value, allocation) = outcome.into_parts();
            let outcome = (value, Some(allocation));
            meter.release_request()?;
            if matches!(&outcome.0, acyclic_stream::CommitOutcome::Committed(_)) {
                meter.charge(submitted)?;
            }
            return Ok(outcome);
        }
        meter.check()?;
        seal(meter, guard).await?;
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
}
