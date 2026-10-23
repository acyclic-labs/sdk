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

async fn policy_rebase(observe: bool, revoke: bool) -> Result<bool, Box<dyn std::error::Error>> {
    let owner = workspace().await?;
    let cancel = CancellationToken::new();
    let bound = owner
        .clone()
        .bind_coarse_access(&Action::ALL, &cancel)
        .await?;
    let fence = bound
        .access
        .as_ref()
        .ok_or("missing bound access")?
        .0
        .publication_fence()
        .ok_or("missing policy cut")?;
    let mut transaction = owner
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &cancel)
        .await?
        .value;
    if observe {
        transaction
            .observe_policy_fence(fence, WorkBudget::UNBOUNDED, &cancel)
            .await?;
    }
    transaction
        .write(
            "/accepted",
            Bytes::from_static(b"candidate"),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?;
    if revoke {
        fixtures::publish_policy(&owner, &fixtures::denied(Action::Write, "accepted")).await?;
    } else {
        owner.write_text("/unrelated", "other writer").await?;
    }
    let rebased = matches!(
        transaction
            .rebase(1, WorkBudget::UNBOUNDED, &cancel)
            .await?
            .value,
        TransactionRebase::Rebased(_)
    );
    if rebased {
        let committed = transaction
            .commit::<WorkspaceError, _, _>(
                None,
                crate::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &cancel,
                |_| async {
                    Ok(crate::OperationReceipt {
                        value: (),
                        work: Default::default(),
                    })
                },
            )
            .await?
            .value;
        assert!(matches!(committed, TransactionCommit::Committed(_)));
        assert_eq!(
            owner
                .read("/accepted", 64, WorkBudget::UNBOUNDED, &cancel)
                .await?
                .value
                .as_ref(),
            b"candidate"
        );
    }
    // A policy change before transaction admission fails at the same fence.
    if revoke {
        let mut next = owner
            .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &cancel)
            .await?
            .value;
        assert!(matches!(
            next.observe_policy_fence(fence, WorkBudget::UNBOUNDED, &cancel)
                .await,
            Err(crate::OperationFailure {
                error: WorkspaceError::AccessDenied,
                ..
            })
        ));
    }
    Ok(rebased)
}

#[tokio::test]
async fn policy_dependencies_allow_unrelated_data_but_fence_revocation_on_rebase()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(policy_rebase(true, false).await?);
    assert!(!policy_rebase(true, true).await?);
    assert!(
        policy_rebase(false, true).await?,
        "negative control did not expose a write crossing revocation"
    );
    Ok(())
}

#[tokio::test]
async fn explicit_generation_preconditions_survive_policy_fences()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = workspace().await?;
    let cancel = CancellationToken::new();
    let bound = owner
        .clone()
        .bind_coarse_access(&Action::ALL, &cancel)
        .await?;
    let fence = bound
        .access
        .as_ref()
        .ok_or("missing bound access")?
        .0
        .publication_fence()
        .ok_or("missing policy cut")?;
    owner.write_text("/unrelated", "other writer").await?;
    let mut transaction = owner
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &cancel)
        .await?
        .value;
    transaction
        .observe_policy_fence(fence, WorkBudget::UNBOUNDED, &cancel)
        .await?;
    assert!(matches!(
        transaction
            .observe_policy_fence(fence.pin_generation(), WorkBudget::UNBOUNDED, &cancel)
            .await,
        Err(crate::OperationFailure {
            error: WorkspaceError::AccessDenied,
            ..
        })
    ));
    Ok(())
}
