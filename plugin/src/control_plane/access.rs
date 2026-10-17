//! Host-managed mounts use the service-authenticated route, never request claims.
use super::*;

pub(super) async fn root_workspace(fs: &LocalFs, name: &str) -> Result<LocalWorkspace, String> {
    acyclic_harness::filesystem::local_owner_workspace(fs, name)
        .await
        .map_err(display)
}

pub(super) async fn mount_options(
    workspace: &LocalWorkspace,
    agent: &str,
) -> Result<MountOptions, String> {
    acyclic_harness::filesystem::local_owner_mount_options(workspace, agent, true)
        .await
        .map(|options| options.publication(acyclic_fs::MountPublication::Manual))
        .map_err(display)
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_fs::access::{Action, Effect, Policy};

    #[tokio::test]
    async fn host_managed_routes_bind_current_policy_and_exact_workspace() -> Result<(), String> {
        let root = tempfile::tempdir().map_err(display)?;
        let fs = LocalFs::local(acyclic_fs::LocalOptions::new(root.path()))
            .await
            .map_err(display)?;
        let workspace = root_workspace(&fs, "route").await?;
        let first = mount_options(&workspace, "authenticated-agent").await?;
        let view = first
            .require_access_view()
            .map_err(display)?
            .current()
            .map_err(display)?;
        let now = || {
            acyclic_stream::UnixMillisClock::now_unix_millis(&acyclic_stream::SystemUnixMillisClock)
                / 1000
        };
        assert!(view.decide(Action::Read, "secrets/key", now()).allowed);
        assert_eq!(
            view.volume(),
            uuid::Uuid::from_bytes(workspace.id().volume_id().into_bytes()).to_string()
        );
        let mut policy: Policy =
            serde_json::from_slice(acyclic_harness::filesystem::INITIAL_ACCESS_POLICY)
                .map_err(display)?;
        policy.version += 1;
        let mut deny = policy.layers[0][0].clone();
        deny.id = "hidden".into();
        deny.effect = Effect::Deny;
        deny.actions = [Action::Read, Action::List].into_iter().collect();
        deny.resource.path = "secrets/**".into();
        policy.layers[0].push(deny);
        let cancel = CancellationToken::new();
        let mut checkout = workspace
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                WorkBudget::UNBOUNDED,
                &cancel,
            )
            .await
            .map_err(display)?
            .value;
        checkout
            .write_access_policy(
                &serde_json::to_vec(&policy).map_err(display)?,
                acyclic_fs::NamedAttributeWriteMode::Replace,
                WorkBudget::UNBOUNDED,
                &cancel,
            )
            .await
            .map_err(display)?;
        checkout
            .commit(
                OperationId::new(),
                None,
                None,
                None,
                PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &cancel,
                |_| {
                    std::future::ready(Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::FsError>>(
                        acyclic_fs::OperationReceipt {
                            value: (),
                            work: Default::default(),
                        },
                    ))
                },
            )
            .await
            .map_err(display)?;
        // Both recovery and subagent creation use these same authenticated options.
        for owner in ["recovered-agent", "pending-child"] {
            let options = mount_options(&workspace, owner).await?;
            let view = options
                .require_access_view()
                .map_err(display)?
                .current()
                .map_err(display)?;
            assert!(!view.decide(Action::Read, "secrets/key", now()).allowed);
            assert!(!view.decide(Action::List, "secrets/key", now()).allowed);
            assert!(view.decide(Action::Read, "public/key", now()).allowed);
            assert_eq!(view.policy_version(), policy.version);
        }
        Ok(())
    }
}
