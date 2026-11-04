use super::*;

impl<P: crate::distributed::AuthorityProvider + ?Sized> AsyncAuthorityStore
    for AccountingAuthority<'_, P>
{
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.0.operation_claims()
    }
    fn actor_scope(&self) -> [u8; 16] {
        self.0.placement.actor_scope()
    }

    async fn head(
        &self,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Head, AuthorityStoreError> {
        let mut scope = work.scope()?;
        let snapshot = self.0.snapshot(authority, &mut scope).await?;
        scope.finish(snapshot.head)
    }

    async fn replay(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, AuthorityAllocation), AuthorityStoreError> {
        let mut scope = work.scope()?;
        let value = self
            .0
            .read_records(authority, after, limit, &mut scope)
            .await?;
        scope.finish(value)
    }
}

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
