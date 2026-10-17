//! Native acknowledgement of the existing durable Git reconciliation step.
use super::*;
use acyclic_fs::{GitCompatState, GitCompatStore, GitGenerationRef};

// Lock order: the existing checkout guard is outermost. Git-state CAS
// returns owned data and releases its leaf record lock before any native
// publication or owner callback. Open completes recovery before exposing tools.
pub(in crate::local) type Pending = (acyclic_fs::OperationId, GitGenerationRef);

pub(in crate::local) async fn prepare(
    workspace: &LocalWorkspace,
    operation: acyclic_fs::OperationId,
    from: GitGenerationRef,
) -> Result<()> {
    // The repository root has no parent and cannot be rebased by a window.
    if from.workspace_id == workspace.view().workspace().id() {
        return Ok(());
    }
    replace(workspace, None, Some((operation, from))).await
}

async fn replace(
    workspace: &LocalWorkspace,
    expected: Option<Pending>,
    replacement: Option<Pending>,
) -> Result<()> {
    let root = workspace.view().workspace().id();
    let mut state = GitCompatStore::load(&workspace.store, root)
        .await
        .map_err(storage)?
        .unwrap_or_else(|| GitCompatState::new("main", root));
    let revision = update(&mut state, expected, replacement)?;
    if !GitCompatStore::compare_and_swap(&workspace.store, root, revision, state)
        .await
        .map_err(storage)?
    {
        return Err(Error::Conflict(
            "Git state changed during checkout reconciliation".into(),
        ));
    }
    Ok(())
}

fn update(
    state: &mut GitCompatState,
    expected: Option<Pending>,
    replacement: Option<Pending>,
) -> Result<u64> {
    if state.checkout_reconciliation != expected {
        return Err(Error::Conflict(
            "checkout reconciliation acknowledgement changed".into(),
        ));
    }
    let revision = state.revision;
    state.revision = revision
        .checked_add(1)
        .ok_or_else(|| Error::Conflict("Git state revision overflow".into()))?;
    state.checkout_reconciliation = replacement;
    Ok(revision)
}

pub(in crate::local) async fn recover<F, Fut>(
    workspace: &LocalWorkspace,
    view: &workspace::View,
    before_effect: &F,
) -> Result<bool>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<()>> + Send,
{
    let (_, pending) = workspace.checkout_selection().await?;
    recover_pending(workspace, view, pending, before_effect).await
}

pub(in crate::local) async fn recover_pending<F, Fut>(
    workspace: &LocalWorkspace,
    view: &workspace::View,
    pending: Option<Pending>,
    before_effect: &F,
) -> Result<bool>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<()>> + Send,
{
    let Some(pending) = pending else {
        return Ok(false);
    };
    let (_, source) = pending;
    if source.workspace_id != view.workspace().id() {
        return Err(Error::Conflict(
            "unpublished reconciliation belongs to another checkout".into(),
        ));
    }
    before_effect().await?;
    let outcome =
        acyclic_fs::OperationWindowCoordinator::new(workspace.filesystem.operation_window_store())
            .recover_workspace(
                view.workspace(),
                acyclic_stream::UnixMillisClock::now_unix_millis(
                    &acyclic_stream::SystemUnixMillisClock,
                ),
                acyclic_fs::OperationReconcileLimits::default(),
            )
            .await
            .map_err(storage)?;
    if let Some(outcome) = outcome {
        super::git::reconciliation_generation(outcome)?;
    }
    replay(workspace, view, pending, true, before_effect).await?;
    Ok(true)
}

pub(super) async fn publish<F, Fut>(
    workspace: &LocalWorkspace,
    view: &workspace::View,
    refresh_mount: bool,
    before_effect: &F,
) -> Result<()>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<()>> + Send,
{
    let root = workspace.view().workspace().id();
    if let Some(pending) = GitCompatStore::load(&workspace.store, root)
        .await
        .map_err(storage)?
        .and_then(|state| state.checkout_reconciliation)
    {
        replay(workspace, view, pending, refresh_mount, before_effect).await?;
    } else if refresh_mount {
        advance_mount(workspace, before_effect).await?;
    }
    Ok(())
}

async fn replay<F, Fut>(
    workspace: &LocalWorkspace,
    view: &workspace::View,
    pending: Pending,
    refresh_mount: bool,
    before_effect: &F,
) -> Result<()>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<()>> + Send,
{
    let (operation, source) = pending;
    if source.workspace_id != view.workspace().id() {
        return Err(Error::Conflict(
            "unpublished reconciliation belongs to another checkout".into(),
        ));
    }
    if workspace.mounted.is_some() {
        if refresh_mount {
            advance_mount(workspace, before_effect).await?;
        }
    } else {
        let cancellation = acyclic_fs::CancellationToken::new();
        let from = view
            .workspace()
            .generation(
                source.generation,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?
            .value;
        let to = view
            .workspace()
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value;
        Box::pin(crate::tools::git::publish_checkout_transition(
            &workspace.path,
            workspace.identity,
            &workspace.store,
            operation,
            from,
            to,
            before_effect,
        ))
        .await?;
    }
    // A crash even after Applied leaves this intent. The native journal then
    // acknowledges without replaying over subsequent user edits.
    before_effect().await?;
    replace(workspace, Some(pending), None).await?;
    Ok(())
}

pub(super) async fn advance_mount<F, Fut>(
    workspace: &LocalWorkspace,
    before_effect: &F,
) -> Result<()>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<()>> + Send,
{
    if let Some(mount) = &workspace.mounted {
        before_effect().await?;
        #[cfg(all(test, not(acyclic_harness_loom)))]
        workspace
            .mount_advances
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        mount.advance_to_head().await.map_err(storage)?;
        before_effect().await?;
    }
    Ok(())
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        // Only the exact operation/source marker can be acknowledged. A
        // mismatch or exhausted revision leaves every byte of state unchanged.
        #[test]
        fn acknowledgement_cannot_erase_another_reconciliation(
            operation in any::<[u8; 16]>(), generation in any::<[u8; 32]>(),
            revision in any::<u64>(), matched in any::<bool>(), overflow in any::<bool>(),
        ) {
            let root = acyclic_fs::WorkspaceId::from_bytes([1; 16]);
            let pending = (acyclic_fs::OperationId::from_bytes(operation), GitGenerationRef {
                workspace_id: root,
                generation: acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(generation)),
            });
            let mut state = GitCompatState::new("main", root);
            state.revision = if overflow { u64::MAX } else { revision.min(u64::MAX - 1) };
            state.checkout_reconciliation = Some(pending);
            let original = state.clone();
            let mut expected = pending;
            if !matched {
                let mut bytes = operation; bytes[0] ^= 1;
                expected.0 = acyclic_fs::OperationId::from_bytes(bytes);
            }
            let result = update(&mut state, Some(expected), None);
            prop_assert_eq!(result.is_ok(), matched && !overflow);
            if result.is_err() { prop_assert_eq!(state, original); }
            else {
                prop_assert_eq!(result.unwrap(), original.revision);
                let mut wanted = original; wanted.revision += 1;
                wanted.checkout_reconciliation = None;
                prop_assert_eq!(state, wanted);
            }
        }
    }
}
