use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Creates or atomically replaces one complete UTF-8 file.
    pub async fn write_text(
        &self,
        path: &str,
        text: &str,
    ) -> Result<TransactionCommit<A, O>, WorkspaceError> {
        self.write(path, Bytes::copy_from_slice(text.as_bytes()))
            .await
    }

    /// Creates or replaces one complete file in a single atomic generation.
    pub async fn write(
        &self,
        path: &str,
        bytes: Bytes,
    ) -> Result<TransactionCommit<A, O>, WorkspaceError> {
        let mut transaction = self.begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map_err(|failure| failure.error)?.value;
        transaction.write(path, bytes, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map_err(|failure| failure.error)?;
        transaction.commit(None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }).await.map_err(|failure| failure.error).map(|receipt| receipt.value)
    }

    /// Removes one existing path in a single atomic generation publication.
    pub async fn remove(&self, path: &str) -> Result<TransactionCommit<A, O>, WorkspaceError> {
        let mut transaction = self.begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map_err(|failure| failure.error)?.value;
        transaction.remove(path, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map_err(|failure| failure.error)?;
        transaction.commit(None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }).await.map_err(|failure| failure.error).map(|receipt| receipt.value)
    }

    /// Copies one complete regular file within this workspace using immutable
    pub async fn copy(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<TransactionCommit<A, O>, WorkspaceError> {
        let mut transaction = self.begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map_err(|failure| failure.error)?.value;
        transaction.copy(source, destination).await?;
        transaction.commit(None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }).await.map_err(|failure| failure.error).map(|receipt| receipt.value)
    }

}
