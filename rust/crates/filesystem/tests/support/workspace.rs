use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, ForkOptions, Workspace,
    WorkspaceError,
};
use acyclic_fs_work::performance::WorkBudget;

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
            (),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
}
