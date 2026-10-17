use super::*;
use crate::Fs;
use crate::access::{Action, INITIAL_POLICY, fixtures};
use proptest::prelude::*;

async fn workspace() -> Result<
    Workspace<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>,
    Box<dyn std::error::Error>,
> {
    Ok(Fs::memory()
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "coarse",
            crate::VolumeInitialization {
                config: crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral),
                access_policy: Some(INITIAL_POLICY),
            },
            OperationId::new(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            None,
        )
        .await?
        .value
        .workspace())
}

#[tokio::test]
async fn coarse_bindings_fence_policy_on_the_checkout_that_serves_data()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = workspace().await?;
    let cancel = CancellationToken::new();
    let reader = owner
        .clone()
        .bind_coarse_access(&[Action::Read, Action::List], &cancel)
        .await?;
    assert!(matches!(
        reader
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                WorkBudget::UNBOUNDED,
                &cancel
            )
            .await
            .err()
            .unwrap()
            .error,
        WorkspaceError::AccessDenied
    ));
    let bound = owner
        .clone()
        .bind_coarse_access(&Action::ALL, &cancel)
        .await?;
    let policy = fixtures::denied(Action::Read, "secrets/**");
    fixtures::publish_policy(&owner, &policy).await?;
    assert!(matches!(
        bound
            .head(WorkBudget::UNBOUNDED, &cancel)
            .await
            .err()
            .unwrap()
            .error,
        WorkspaceError::AccessDenied
    ));
    assert!(matches!(
        owner.bind_coarse_access(&Action::ALL, &cancel).await,
        Err(WorkspaceError::AccessDenied)
    ));
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn coarse_bindings_never_widen_a_path_or_action_restriction(
        action in 0..Action::ALL.len(), path in "[a-z]{1,8}", read_only in any::<bool>(),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        runtime.block_on(async {
            let workspace = workspace().await?;
            fixtures::publish_policy(&workspace, &fixtures::denied(Action::ALL[action], &path)).await?;
            let actions: &[Action] = if read_only { &[Action::List, Action::Read, Action::ReadHistory] }
                else { &Action::ALL };
            let expected = !actions.contains(&Action::ALL[action]);
            let actual = workspace.bind_coarse_access(actions, &CancellationToken::new()).await;
            assert_eq!(actual.is_ok(), expected);
            Ok::<_, Box<dyn std::error::Error>>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
