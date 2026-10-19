use super::*;

impl<P: acyclic_stream::StreamProvider> AsyncAuthorityStore for AccountingAuthority<'_, P> {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.0.operation_claims()
    }

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
        work.check()?;
        if limit.records == 0 || limit.payload_bytes == 0 {
            return Err(AuthorityStoreError::InvalidReplayLimit);
        }
        work.admit(WorkCounters {
            authority_records_read: u64::from(limit.records),
            authority_bytes_read: STREAM_RECORD_LIMIT.saturating_mul(u64::from(limit.records)),
            backend_read_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        // Historical accounting retains main's record/read costs, excluding codec work.
        let mut historical = AuthorityWork::new(WorkBudget::UNBOUNDED, work.cancellation);
        let result = self
            .0
            .read_records(authority, after, limit, &mut historical)
            .await;
        let spent = historical.spent();
        work.record(WorkCounters {
            authority_records_read: spent.authority_records_read,
            authority_bytes_read: spent.authority_bytes_read,
            backend_read_operations: spent.backend_read_operations,
            ..WorkCounters::UNCHARGED
        })?;
        let result = result?;
        work.work.verify(work.budget)?;
        Ok(result)
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
            (&historical, authority_id),
            objects,
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
