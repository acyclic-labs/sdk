use super::*;
use acyclic_fs::native_mount::MountOptions;

pub(super) async fn options<A, O>(
    workspace: &acyclic_fs::Workspace<A, O>,
) -> Result<MountOptions, Failure>
where
    A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
    O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
{
    Ok(
        acyclic_harness::filesystem::local_owner_mount_options(workspace, "mount-benchmark", true)
            .await?
            .publication(acyclic_fs::MountPublication::Manual),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_fs::access::Action;

    #[tokio::test]
    async fn benchmark_mounts_bind_real_volume_policy() -> Result<(), Failure> {
        let fs = acyclic_fs::Fs::memory();
        let workspace = fs
            .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
                "bench-options",
                acyclic_fs::VolumeInitialization {
                    config: VolumeConfig::portable(Lifecycle::Ephemeral),
                    access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
                },
                OperationId::new(),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                None,
            )
            .await
            .map_err(|failure| failure.error)?
            .value
            .workspace();
        let options = options(&workspace).await?;
        let view = options.require_access_view()?.current()?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        assert!(view.allows_every_path(&Action::ALL, now));
        assert_eq!(
            view.volume(),
            uuid::Uuid::from_bytes(workspace.id().volume_id().into_bytes()).to_string()
        );
        // Preparation exercises the retained owner through the public API;
        // a compiled view alone (the former implementation) cannot pass it.
        let destination = tempfile::tempdir()?;
        let cancel = CancellationToken::new();
        let generation = workspace
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancel)
            .await
            .map_err(|failure| failure.error)?
            .value;
        let prepared = generation
            .prepare_native_working_set(
                &options,
                &MaterializeOptions::native(destination.path()),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancel,
            )
            .await?;
        drop(prepared);
        assert!(
            MountOptions::read_write().require_access_view().is_err(),
            "negative control must reject the former unbound options"
        );
        Ok(())
    }
}
