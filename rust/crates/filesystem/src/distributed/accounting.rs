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
        Ok(crate::async_storage::authority_parts(
            scope.finish_owned(value).map_err(authority_error)?,
        ))
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
        work: &mut AuthorityWork<'_>,
    ) -> Result<crate::accounting::AccountingPage, crate::accounting::AccountingError> {
        crate::accounting::accounting_page(
            &AccountingAuthority(self),
            objects,
            authority_id,
            cursor,
            maximum_records,
            work,
        )
        .await
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
