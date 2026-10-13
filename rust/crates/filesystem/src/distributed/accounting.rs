use super::*;

impl<P: acyclic_stream::StreamProvider> AsyncAuthorityStore for AccountingAuthority<'_, P> {
    async fn head(
        &self,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Head, AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(AuthorityStoreError::NativeOriginalUnsupported);
        }
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        let cancellation = CancellationToken::new();
        let mut historical = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let head = self
            .0
            .snapshot(authority, &mut historical)
            .await
            .map_err(|error| match error {
                AuthorityStoreError::CorruptStatic(message) => {
                    AuthorityStoreError::Corrupt(message.to_owned())
                }
                error => error,
            })?
            .head;
        work.record(authority_read_work(2))?;
        work.work.verify(work.budget)?;
        Ok(head)
    }

    async fn replay(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, AuthorityAllocation), AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(AuthorityStoreError::NativeOriginalUnsupported);
        }
        let mut meter = AuthorityWork::operation(work)?;
        let result = async {
            if limit.records == 0 || limit.payload_bytes == 0 {
                return Err(AuthorityStoreError::InvalidReplayLimit);
            }
            let path = stream_path(authority, "/records")?;
            meter.admit(WorkCounters {
                authority_records_read: u64::from(limit.records),
                authority_bytes_read: STREAM_RECORD_LIMIT.saturating_mul(u64::from(limit.records)),
                backend_read_operations: 1,
                ..WorkCounters::UNCHARGED
            })?;
            meter.work.backend_read_operations += 1;
            let mut stream = self
                .0
                .provider
                .read(acyclic_stream::ReadRequest {
                    path,
                    from: after.get().saturating_add(1),
                    limit: limit.records,
                })
                .await
                .map_err(map_stream_error)?;
            let mut output = Vec::new();
            let mut bytes = 0_u64;
            for _ in 0..limit.records {
                meter
                    .cancellation
                    .check()
                    .map_err(|_| AuthorityStoreError::Cancelled)?;
                let Some(record) = stream.next().await else {
                    break;
                };
                let record = record.map_err(map_stream_error)?;
                meter.record(WorkCounters {
                    authority_records_read: 1,
                    authority_bytes_read: record.value.len() as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                meter
                    .cancellation
                    .check()
                    .map_err(|_| AuthorityStoreError::Cancelled)?;
                let durable = decode_record(&record)?;
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
                validate_record(authority, &record, &durable)?;
                bytes = bytes.checked_add(durable.payload.len() as u64).ok_or(
                    AuthorityStoreError::ReplayRecordTooLarge {
                        observed: u64::MAX,
                        maximum: limit.payload_bytes,
                    },
                )?;
                if bytes > limit.payload_bytes {
                    if output.is_empty() {
                        return Err(AuthorityStoreError::ReplayRecordTooLarge {
                            observed: bytes,
                            maximum: limit.payload_bytes,
                        });
                    }
                    break;
                }
                output.push(durable_commit(
                    durable,
                    record.committed_at_micros,
                    Bytes::from(payload),
                ));
            }
            Ok((output, None))
        }
        .await;
        work.record(meter.spent())?;
        let value = result?;
        work.work.verify(work.budget)?;
        Ok(value)
    }
}

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    /// Reads a bounded historical accounting page, including retired authorities.
    pub async fn accounting_page<O: crate::AsyncObjectStore>(
        &self,
        objects: &O,
        authority_id: AuthorityId,
        cursor: crate::accounting::AccountingCursor,
        maximum_records: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<crate::accounting::AccountingPage, crate::accounting::AccountingError> {
        let historical = AccountingAuthority(self);
        let mut work = AuthorityWork::new(budget, cancellation);
        let head = historical
            .head(authority_id, &mut work)
            .await
            .map_err(|error| crate::accounting::AccountingError::Authority(error.to_string()))?;
        let remaining = work
            .spent()
            .remaining(budget)
            .map_err(crate::accounting::AccountingError::Work)?;
        let mut page = crate::accounting::accounting_page_at_head(
            &historical,
            objects,
            authority_id,
            cursor,
            maximum_records,
            head,
            remaining,
            cancellation,
        )
        .await?;
        page.work = work
            .spent()
            .checked_add(page.work)
            .map_err(crate::accounting::AccountingError::Work)?;
        Ok(page)
    }
}
