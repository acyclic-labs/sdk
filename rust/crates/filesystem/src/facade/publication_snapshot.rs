use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Volume<A, O> {
    pub(crate) async fn publication_snapshot(
        &self,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::PublicationSnapshot, FsError> {
        let mut work = authority_work(accounting, cancellation)?;
        let result = self
            .fs
            .inner
            .authority
            .publication_snapshot(self.id, &mut work)
            .await;
        record_authority_work(accounting, &work)?;
        result.map_err(Into::into)
    }
}
