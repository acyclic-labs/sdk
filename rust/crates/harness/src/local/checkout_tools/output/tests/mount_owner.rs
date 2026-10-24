//! Mount construction must retain the same live task owner through publication.
use super::*;
use acyclic_fs::facade::FsWork as _;

#[tokio::test]
async fn a_cancelled_owner_cannot_construct_a_task_mount() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let (harness, context, _directory, _state) = fixture(Some(&cancellation)).await?;
    let workspace = harness
        .workspace()
        .ok_or_else(|| Error::NotFound("checkout".into()))?;
    let tools = CheckoutTools::new(
        Arc::clone(workspace),
        &harness.project,
        Arc::new(acyclic_stream::SystemUnixMillisClock),
        &harness.signed,
        Arc::clone(&harness.task_files),
    )
    .await?;
    let owner = context
        .task()
        .owned_mailbox
        .as_ref()
        .ok_or_else(|| Error::Unauthorized("fixture owner missing".into()))?;
    let view = tools
        .workspace
        .view()
        .open_related(
            tools.workspace.view().workspace().clone(),
            &mut acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation),
        )
        .await
        .map_err(storage)?;
    cancellation.cancel();
    let mounted = tools
        .mount_task_view(view, owner, None, &harness.signed)
        .await;
    eprintln!("MOUNT_CANCELLED_OWNER constructed={}", mounted.is_ok());
    assert!(
        matches!(mounted, Err(Error::Indeterminate(_))),
        "cancelled mount owner must be rejected"
    );
    Ok(())
}

#[tokio::test]
async fn owner_loss_during_mount_construction_prevents_publication() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let (_harness, context, _directory, _state) = fixture(Some(&cancellation)).await?;
    let owner = context
        .task()
        .owned_mailbox
        .as_ref()
        .ok_or_else(|| Error::Unauthorized("fixture owner missing".into()))?;
    let constructed = super::super::super::task_views::with_mount_owner(owner, async {
        cancellation.cancel();
        Ok(())
    })
    .await;
    eprintln!("MOUNT_OWNER_LOSS published={}", constructed.is_ok());
    assert!(
        matches!(constructed, Err(Error::Indeterminate(_))),
        "owner must remain live when a mount is published"
    );
    Ok(())
}
