use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, ForkOptions, FsError, WorkBudget,
    Workspace, WorkspaceError,
};

#[path = "transaction.rs"]
mod transaction;
pub(crate) use transaction::{allow, commit};

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
