use super::*;

impl<P: acyclic_stream::StreamProvider> AsyncAuthorityStore for AccountingAuthority<'_, P> {
    async fn create_authority<G: AuthorityGuard>(
        &self,
        _: AuthorityId,
        _: Epoch,
        _: &mut AuthorityWork<'_>,
        _: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, AuthorityError<G::Error>> {
        Err(AuthorityStoreError::Retired.into())
    }

    async fn fork_authority<G: AuthorityGuard>(
        &self,
        _: crate::GenerationForkSource,
        _: AuthorityId,
        _: OperationId,
        _: &mut AuthorityWork<'_>,
        _: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, AuthorityError<G::Error>> {
        Err(AuthorityStoreError::Retired.into())
    }

    async fn compare_and_append<G: AuthorityGuard>(
        &self,
        _: crate::GuardedAppend,
        _: &mut AuthorityWork<'_>,
        _: Option<&mut G>,
    ) -> Result<FsAppendOutcome, AuthorityError<G::Error>> {
        Err(AuthorityStoreError::Retired.into())
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
        let head = self.0.historical_head(authority).await?;
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
        let result = self
            .0
            .authority_replay(authority, after, limit, &mut meter, ReadKind::Accounting)
            .await;
        work.record(meter.spent())?;
        let value = result?;
        work.work.verify(work.budget)?;
        Ok(value)
    }

    async fn fence(
        &self,
        _: AuthorityId,
        _: Head,
        _: &mut AuthorityWork<'_>,
    ) -> Result<FenceOutcome, AuthorityStoreError> {
        Err(AuthorityStoreError::Retired)
    }

    async fn find_operation(
        &self,
        _: AuthorityId,
        _: OperationId,
        _: &mut AuthorityWork<'_>,
    ) -> Result<Option<DurableCommit>, AuthorityStoreError> {
        Err(AuthorityStoreError::Retired)
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
