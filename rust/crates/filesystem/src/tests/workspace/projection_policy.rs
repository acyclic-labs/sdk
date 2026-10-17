use super::access_policy::{current, publish, recorded};
use super::*;
use crate::{AuthoredMutation, NamedAttributeWriteMode};

fn denied_policy() -> Vec<u8> {
    serde_json::to_vec(&crate::access::fixtures::denied(
        crate::access::Action::Write,
        "**",
    ))
    .unwrap()
}

fn assert_projection_denied<T>(result: Result<T, WorkspaceError>) {
    assert!(
        matches!(
            result,
            Err(WorkspaceError::Filesystem(FsError::Publication(
                crate::kernel::PublicationError::ActiveProjection
            )))
        ),
        "policy publication escaped projection fence"
    );
}

async fn set_metadata(protected: bool) -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    // Workspace identity is deterministic across independent memory stores.
    // Keep the concurrent negative control in a different authority identity.
    let name = if protected {
        "root-metadata-protected"
    } else {
        "root-metadata-control"
    };
    let workspace = unbounded!(fs, create_workspace(name))?;
    let mut donor = current(&workspace, true).await?;
    let policy = denied_policy();
    unbounded!(
        donor,
        write_access_policy(&policy, NamedAttributeWriteMode::Create)
    )?;
    let root = crate::kernel::NamespacePath::new(
        vec![],
        VolumeConfig::portable(Lifecycle::Ephemeral).limits,
    )?;
    let metadata = unbounded!(donor, read_metadata(&root))?;
    drop(donor);
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(
        transaction.checkout,
        apply_authored_transaction(vec![AuthoredMutation::SetMetadata {
            path: root,
            metadata,
        }])
    )?;
    let before = unbounded!(workspace, head())?.id();
    let projection = protected
        .then(|| crate::access::projection::enter(workspace.id().volume_id()))
        .transpose()?;
    assert_projection_denied(commit(&mut transaction).await);
    assert_eq!(unbounded!(workspace, head())?.id(), before);
    assert_eq!(
        recorded(&mut current(&workspace, false).await?).await?,
        None
    );
    drop(projection);
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        recorded(&mut current(&workspace, false).await?).await?,
        Some(policy)
    );
    Ok(())
}

#[tokio::test]
async fn root_set_metadata_transaction_is_fenced_at_publication() -> Result<(), Box<dyn Error>> {
    set_metadata(true).await
}

#[tokio::test]
#[should_panic(expected = "policy publication escaped projection fence")]
async fn root_set_metadata_missing_projection_negative_control() {
    set_metadata(false).await.unwrap();
}

async fn join(protected: bool) -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let (parent_name, child_name) = if protected {
        ("join-policy-protected", "join-child-protected")
    } else {
        ("join-policy-control", "join-child-control")
    };
    let parent = unbounded!(fs, create_workspace(parent_name))?;
    let base = unbounded!(parent, head())?;
    let child = fork_workspace(
        &parent,
        child_name,
        ForkOptions::from_generation(base, IdempotencyKey::new()),
    )
    .await?;
    let mut editor = current(&child, true).await?;
    let policy = denied_policy();
    unbounded!(
        editor,
        write_access_policy(&policy, NamedAttributeWriteMode::Create)
    )?;
    publish(&mut editor).await?;
    let plan = unbounded!(child.join_into(&parent), plan())?;
    let options = ApplyOptions {
        if_target: plan.target_head(),
        idempotency_key: IdempotencyKey::new(),
    };
    let before = unbounded!(parent, head())?.id();
    let projection = protected
        .then(|| crate::access::projection::enter(parent.id().volume_id()))
        .transpose()?;
    assert_projection_denied(
        plan.apply(
            options,
            BTreeMap::new(),
            crate::PublicationPermit::Unrestricted,
        )
        .await,
    );
    assert_eq!(unbounded!(parent, head())?.id(), before);
    assert_eq!(recorded(&mut current(&parent, false).await?).await?, None);
    drop(projection);
    assert!(matches!(
        plan.apply(
            options,
            BTreeMap::new(),
            crate::PublicationPermit::Unrestricted
        )
        .await?,
        JoinOutcome::Applied(_)
    ));
    assert_eq!(
        recorded(&mut current(&parent, false).await?).await?,
        Some(policy)
    );
    Ok(())
}

#[tokio::test]
async fn workspace_join_is_fenced_at_publication() -> Result<(), Box<dyn Error>> {
    join(true).await
}

#[tokio::test]
#[should_panic(expected = "policy publication escaped projection fence")]
async fn workspace_join_missing_projection_negative_control() {
    join(false).await.unwrap();
}

#[tokio::test]
async fn unchanged_policy_publishes_and_inflight_cas_excludes_new_projection()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("publication-projection"))?;
    let projection = crate::access::projection::enter(workspace.id().volume_id())?;
    workspace
        .write_text("/ordinary", "unrestricted content publication")
        .await?;
    drop(projection);
    let mut editor = current(&workspace, true).await?;
    unbounded!(
        editor,
        write_access_policy(&denied_policy(), NamedAttributeWriteMode::Create)
    )?;
    let volume = workspace.id().volume_id();
    editor
        .commit(
            crate::OperationId::new(),
            None,
            None,
            None,
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            move |_| async move {
                assert!(
                    crate::access::projection::enter(volume).is_err(),
                    "projection entered an in-flight policy CAS"
                );
                Ok::<_, crate::OperationFailure<FsError>>(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                })
            },
        )
        .await?;
    assert!(crate::access::projection::enter(volume).is_ok());
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    // The publication fence must preserve bounded failure identity through any
    // nesting of validation errors, including cancellation and exact counters.
    #[test]
    fn projection_validation_preserves_budget_and_cancellation(
        maximum in proptest::prelude::any::<u32>(), cancelled in proptest::prelude::any::<bool>(), depth in 1usize..4,
    ) {
        let work = crate::WorkError::BudgetExceeded {
            counter: "object_bytes_read", maximum: u64::from(maximum), observed: u64::from(maximum) + 1,
        };
        let mut cause = if cancelled { FsError::Cancelled(crate::CancellationError) } else { FsError::Work(work) };
        for _ in 0..depth {
            cause = FsError::Publication(crate::kernel::PublicationError::ProjectionValidation(Box::new(cause)));
        }
        let bounded = WorkspaceError::from(cause);
        if cancelled {
            proptest::prop_assert!(matches!(bounded, WorkspaceError::Cancelled(_)));
        } else {
            proptest::prop_assert!(matches!(bounded, WorkspaceError::Work(actual) if actual == work));
        }
    }
}
