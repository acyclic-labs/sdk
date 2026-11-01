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
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<(acyclic_stream::Record, ProviderAllocation)>, AuthorityStoreError> {
        let record = match self {
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
        }?;
        if let Some((record, _)) = &record {
            work.charge(WorkCounters {
                authority_records_read: 1,
                authority_bytes_read: if work.is_native() {
                    0
                } else {
                    record.value.len() as u64
                },
                ..WorkCounters::UNCHARGED
            })?;
        }
        Ok(record)
    }
}

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    /// Captures exact named heads at one provider cut. Request storage and
    /// native output remain on the original caller's funding owner.
    pub(super) async fn authority_heads(
        &self,
        projection: acyclic_stream::HeadProjection,
        paths: &[StreamPath],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(acyclic_stream::Heads, ProviderAllocation), AuthorityStoreError> {
        work.check()?;
        let make_request = |work: &mut AuthorityWork<'_>| {
            let mut selected = acyclic_stream::HeadSet::new();
            if paths.len() > selected.inline_size() {
                selected = acyclic_stream::HeadSet::from_vec(work.vec(paths.len())?);
            }
            selected.extend(paths.iter().cloned());
            Ok::<_, AuthorityStoreError>(acyclic_stream::HeadsRequest {
                retained: Default::default(),
                durable_through: None,
                projection,
                paths: selected,
                children: None,
                limit: u32::try_from(paths.len()).map_err(|_| crate::WorkError::Overflow)?,
                at_least: Default::default(),
            })
        };
        let read = WorkCounters {
            backend_read_operations: u64::from(!work.is_native()),
            ..WorkCounters::UNCHARGED
        };
        work.admit(WorkCounters {
            authority_records_read: if projection == acyclic_stream::HeadProjection::History {
                paths.len() as u64
            } else {
                0
            },
            ..read
        })?;
        // Keep the selection for boundary validation; fund its forwarded copy
        // before allocating, including the multi-path SmallVec spill.
        let request = make_request(work)?;
        let forwarded = make_request(work)?;
        let observed = {
            #[cfg(not(target_arch = "wasm32"))]
            if work.is_native() {
                let meter = work.native_meter()?;
                let result = self.provider.native_heads(forwarded, meter).await;
                work.check()?;
                work.result(result)?
            } else {
                work.record(read)?;
                let result = self.provider.heads(forwarded).await;
                work.check()?;
                (result.map_err(map_stream_error)?, None)
            }
            #[cfg(target_arch = "wasm32")]
            {
                work.record(read)?;
                let result = self.provider.heads(forwarded).await;
                work.check()?;
                (result.map_err(map_stream_error)?, None)
            }
        };
        let heads = &observed.0;
        heads.validate_for(&request).map_err(map_stream_error)?;
        if !work.is_native() && heads.heads.spilled() {
            work.hold_opaque(
                (heads.heads.capacity() as u64)
                    .checked_mul(std::mem::size_of::<Option<acyclic_stream::PathHead>>() as u64)
                    .ok_or(crate::WorkError::Overflow)?,
            )?;
        }
        for record in heads
            .heads
            .iter()
            .flatten()
            .filter_map(|head| head.last.as_ref())
        {
            work.charge(WorkCounters {
                authority_records_read: 1,
                authority_bytes_read: if work.is_native() {
                    0
                } else {
                    record.value.len() as u64
                },
                ..WorkCounters::UNCHARGED
            })?;
            if !work.is_native() {
                work.hold_opaque(record.value.len() as u64)?;
            }
        }
        Ok(observed)
    }

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
                    key: None,
                    path,
                    from,
                    limit: 1,
                    at_least: Default::default(),
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
        if !work.is_native() {
            work.hold_opaque(record.0.value.len() as u64)?;
        }
        if record.0.sequence != from {
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
                        .native_commit(self.provider.native_provider(), request, deadline, meter)
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
        let mut request = request;
        request.deadline_unix_millis = request
            .deadline_unix_millis
            .into_iter()
            .chain(deadline)
            .min();
        let result = self.provider.commit(request).await;
        meter.charge(commit_work(submitted, &result))?;
        meter.check()?;
        result
            .map(|value| (value, None))
            .map_err(|error| AuthorityError::Authority(AuthorityStoreError::NativeStream(error)))
    }
}
