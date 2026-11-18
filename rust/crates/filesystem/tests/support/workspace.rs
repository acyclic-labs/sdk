use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, ForkOptions, FsError, Transaction,
    TransactionCommit, WorkBudget, Workspace, WorkspaceError,
};

pub(crate) fn allow<E>(
    _: WorkBudget,
) -> std::future::Ready<acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, E>> {
    std::future::ready(Ok(acyclic_fs::OperationReceipt {
        value: (),
        work: acyclic_fs::WorkCounters::UNCHARGED,
    }))
}

pub(crate) async fn fork_workspace<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
>(
    source: &Workspace<A, O>,
    name: &str,
    options: ForkOptions<A, O>,
) -> Result<Workspace<A, O>, WorkspaceError> {
    source
        .fork::<_, _, WorkspaceError>(
            name,
            options,
            acyclic_fs::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            &mut allow::<FsError>,
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
}

pub(crate) async fn commit<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    transaction: &mut Transaction<A, O>,
) -> Result<TransactionCommit<A, O>, WorkspaceError> {
    transaction
        .commit::<WorkspaceError, _, _>(
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            allow,
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
}
