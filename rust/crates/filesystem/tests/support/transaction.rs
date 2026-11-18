use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, PublicationPermit, Transaction,
    TransactionCommit, WorkspaceError,
};
use acyclic_fs_work::performance::WorkBudget;

#[path = "allow_publication.rs"]
mod allow_publication;
pub(crate) use allow_publication::allow;

pub(crate) async fn commit<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    transaction: &mut Transaction<A, O>,
) -> Result<TransactionCommit<A, O>, WorkspaceError> {
    transaction
        .commit::<WorkspaceError, _, _>(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            allow,
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
}
