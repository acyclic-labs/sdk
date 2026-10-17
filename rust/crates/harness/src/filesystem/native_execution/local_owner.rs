use super::*;

#[cfg(test)]
#[path = "local_owner_tests.rs"]
mod tests;

/// Create or recover an embedded native workspace with explicit initial policy.
pub async fn local_owner_workspace<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    filesystem: &acyclic_fs::Fs<A, O>,
    name: &str,
) -> Result<acyclic_fs::Workspace<A, O>> {
    let cancellation = acyclic_fs::CancellationToken::new();
    match filesystem
        .open_workspace(name, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
    {
        Ok(receipt) => return Ok(receipt.value),
        Err(failure)
            if matches!(
                failure.error,
                acyclic_fs::WorkspaceError::Filesystem(acyclic_fs::FsError::Authority(
                    acyclic_fs::AuthorityStoreError::Missing
                ))
            ) => {}
        Err(failure) => return Err(Error::Invalid(failure.error.to_string())),
    }
    let mut operation = [0; 16];
    operation.copy_from_slice(&blake3::hash(name.as_bytes()).as_bytes()[..16]);
    filesystem
        .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
            name,
            acyclic_fs::VolumeInitialization {
                config: acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable),
                access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
            },
            acyclic_fs::OperationId::from_bytes(operation),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
            None,
        )
        .await
        .map(|receipt| receipt.value.workspace())
        .map_err(|failure| Error::Invalid(failure.error.to_string()))
}

/// Bind an embedded host-owned workspace through the same signed authority
/// compiler as task mounts. The returned options retain their refresh owner.
pub async fn local_owner_mount_options<A, O>(
    workspace: &acyclic_fs::Workspace<A, O>,
    owner: &str,
    writable: bool,
) -> Result<acyclic_fs::MountOptions>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    use crate::core::{AggregateKind, Authority, AuthorityIssuer};
    acyclic_fs::initialize_native_mount_services()
        .map_err(|error| Error::Invalid(error.to_string()))?;
    acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| Error::Invalid(error.to_string()))?;
    let mut key = [0; 32];
    getrandom::fill(&mut key).map_err(|error| Error::Invalid(error.to_string()))?;
    let issuer = AuthorityIssuer::new(
        format!("host-managed:{owner}"),
        key,
        Authority {
            kind: AggregateKind::Agent,
            id: owner.into(),
        },
    );
    key.fill(0);
    owner_mount_options(workspace, &issuer, None, writable, &work).await
}
