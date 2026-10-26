use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, MeasuredResult, OperationReceipt,
    PublicationPermit, Transaction, TransactionCommit, WorkBudget, WorkCounters, WorkspaceError,
};

pub(super) async fn commit<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    transaction: &mut Transaction<A, O>,
) -> MeasuredResult<OperationReceipt<TransactionCommit<A, O>>, WorkspaceError> {
    transaction
        .commit(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            |_| async {
                Ok(OperationReceipt {
                    value: (),
                    work: WorkCounters::default(),
                })
            },
        )
        .await
}
