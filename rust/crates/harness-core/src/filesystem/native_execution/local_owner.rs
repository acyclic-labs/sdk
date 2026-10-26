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
    acyclic_fs::initialize_native_mount_services()
        .map_err(|error| Error::Invalid(error.to_string()))?;
    acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| Error::Invalid(error.to_string()))?;
    let issuer = host_owner_root(owner)?;
    owner_mount_options(workspace, &issuer, None, writable, &work).await
}

/// Authenticate the embedded host identity for the shared raw-workspace facade.
/// The policy compiler still intersects this host grant with each volume policy.
pub fn local_owner_workspace_access(owner: &str) -> Result<WorkspaceAccess> {
    let issuer = host_owner_root(owner)?;
    let capability =
        acyclic_fs::access::Capability::whole_volume(None, &acyclic_fs::access::Action::ALL);
    let grants = crate::Capabilities::new([capability
        .to_scope_label()
        .map_err(|_| Error::FilesystemAccessConfiguration("host workspace"))?]);
    let scope = issuer.bootstrap_scope("host-workspace", &grants, None, &mut None)?;
    WorkspaceAccess::new(&issuer.verifier(&mut None)?, &scope, &grants, None)
}

fn host_owner_root(owner: &str) -> Result<crate::core::AuthorityRoot> {
    use crate::core::{AggregateKind, Authority, AuthorityRoot};
    let mut key = [0; 32];
    crate::identity::fill_random(&mut key).map_err(|error| Error::Invalid(error.to_string()))?;
    let issuer = AuthorityRoot::new(
        format!("host-managed:{owner}"),
        key,
        Authority {
            kind: AggregateKind::Agent,
            id: owner.into(),
        },
    );
    acyclic_native_runtime::owner_record::clear_secret(&mut key);
    Ok(issuer)
}
