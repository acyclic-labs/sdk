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

pub(in crate::local) fn recover<'a, F, Fut>(
    workspace: &'a LocalWorkspace,
    view: &'a workspace::View,
    before_effect: &'a F,
) -> BoxProviderFuture<'a, Result<bool>>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<()>> + Send + 'a,
{
    recover_selected(workspace, view, RecoverySelection::Current, before_effect)
}

pub(in crate::local) fn recover_pending<'a, F, Fut>(
    workspace: &'a LocalWorkspace,
    view: &'a workspace::View,
    pending: Option<Pending>,
    before_effect: &'a F,
) -> BoxProviderFuture<'a, Result<bool>>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<()>> + Send + 'a,
{
    recover_selected(
        workspace,
        view,
        RecoverySelection::Observed(pending),
        before_effect,
    )
}

enum RecoverySelection {
    Current,
    Observed(Option<Pending>),
}

fn recover_selected<'a, F, Fut>(
    workspace: &'a LocalWorkspace,
    view: &'a workspace::View,
    selection: RecoverySelection,
    before_effect: &'a F,
) -> BoxProviderFuture<'a, Result<bool>>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<()>> + Send + 'a,
{
    // Selection and reconciliation share the existing recovery allocation.
    // Both entry points expose one erased future instead of nesting a concrete
    // selection wrapper around the recovery frame at each caller's Send bound.
    Box::pin(async move {
        let pending = match selection {
            RecoverySelection::Current => workspace.checkout_selection().await?.1,
            RecoverySelection::Observed(pending) => pending,
        };
        let coordinator = acyclic_fs::OperationWindowCoordinator::new(
            workspace.filesystem.operation_window_store(),
        );
        let now = acyclic_stream::UnixMillisClock::now_unix_millis(
            &acyclic_stream::SystemUnixMillisClock,
        );
        let pending = match pending {
            Some(pending) => pending,
            None => {
                before_effect().await?;
                let Some(reconcile) = coordinator
                    .claim_reconcile(view.workspace().id(), now)
                    .await
                    .map_err(storage)?
                else {
                    return Ok(false);
                };
                // A branch switch can leave a core ticket without a native
                // marker. Preserve this recovery attempt's physical edits
                // before selecting the cut the parent rebase will publish.
                if workspace.mounted.is_none() {
                    let configuration = capture::configuration(workspace);
                    capture::capture(
                        (&configuration.0, &configuration.1),
                        (workspace, view),
                        &mut None,
                        (OperationId::new(), 0),
                        acyclic_fs::PublicationPermit::Unrestricted,
                    )
                    .await?;
                    before_effect().await?;
                }
                // Journal the captured cut before resuming the rebase, so
                // interruption during recovery still owes publication.
                let from = view
                    .workspace()
                    .head(
                        acyclic_fs::WorkBudget::UNBOUNDED,
                        &acyclic_fs::CancellationToken::new(),
                    )
                    .await
                    .map_err(storage)?
                    .value;
                let pending = (
                    reconcile.ticket,
                    GitGenerationRef {
                        workspace_id: view.workspace().id(),
                        generation: from.id(),
                    },
                );
                prepare(workspace, pending.0, pending.1).await?;
                pending
            }
        };
        let (_, source) = pending;
        if source.workspace_id != view.workspace().id() {
            return Err(Error::Conflict(
                "unpublished reconciliation belongs to another checkout".into(),
            ));
        }
        before_effect().await?;
        let outcome = coordinator
            .recover_workspace(
                view.workspace(),
                now,
                acyclic_fs::OperationReconcileLimits::default(),
            )
            .await
            .map_err(storage)?;
        // Conflicts complete the coordinator ticket. Publish and acknowledge
        // the unchanged checkout too, so the next command can capture edits
        // that resolve the conflict. Nonterminal failures keep their intent.
        let recovered = match outcome {
            Some(outcome) => {
                if !terminal(&outcome) {
                    return super::git::reconciliation_generation(outcome).map(|_| true);
                }
                super::git::reconciliation_generation(outcome).map(|_| ())
            }
            None => Ok(()),
        };
        replay(workspace, view, pending, true, before_effect).await?;
        recovered?;
        Ok(true)
    })
}

/// A file publication's owed capture: its operation and staged generation.
pub(in crate::local) type OwedWrite = (acyclic_fs::OperationId, GitGenerationRef);

/// The capture this checkout owes a file publication's physical write.
pub(in crate::local) async fn owed_write(workspace: &LocalWorkspace) -> Result<Option<OwedWrite>> {
    let root = workspace.view().workspace().id();
    Ok(GitCompatStore::load(&workspace.store, root)
        .await
        .map_err(storage)?
        .and_then(|state| state.write_capture))
}

/// Replaces the owed write capture, exactly when it is still `expected`.
pub(in crate::local) async fn replace_owed_write(
    workspace: &LocalWorkspace,
    expected: Option<OwedWrite>,
    replacement: Option<OwedWrite>,
) -> Result<()> {
    let root = workspace.view().workspace().id();
    let mut state = GitCompatStore::load(&workspace.store, root)
        .await
        .map_err(storage)?
        .unwrap_or_else(|| GitCompatState::new("main", root));
    if state.write_capture != expected {
        return Err(Error::Conflict("owed write capture changed".into()));
    }
    let revision = state.revision;
    state.revision = revision
        .checked_add(1)
        .ok_or_else(|| Error::Conflict("Git state revision overflow".into()))?;
    state.write_capture = replacement;
    if !GitCompatStore::compare_and_swap(&workspace.store, root, revision, state)
        .await
        .map_err(storage)?
    {
        return Err(Error::Conflict(
            "Git state changed while recording a write's capture".into(),
        ));
    }
    Ok(())
}

pub(in crate::local) async fn publish<F, Fut>(
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

pub(in crate::local) fn terminal<A: 'static, O: 'static>(
    outcome: &acyclic_fs::WorkspaceRebase<A, O>,
) -> bool {
    matches!(
        outcome,
        acyclic_fs::WorkspaceRebase::Rebased(_)
            | acyclic_fs::WorkspaceRebase::AlreadyRebased(_)
            | acyclic_fs::WorkspaceRebase::Current(_)
            | acyclic_fs::WorkspaceRebase::Conflicted { .. }
    )
}

pub(in crate::local) async fn advance_mount<F, Fut>(
    workspace: &LocalWorkspace,
    before_effect: &F,
) -> Result<()>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<()>> + Send,
{
    if let Some(mount) = &workspace.mounted {
        before_effect().await?;
        #[cfg(all(test, not(loom)))]
        workspace
            .mount_advances
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        mount.advance_to_head().await.map_err(storage)?;
        before_effect().await?;
    }
    Ok(())
}

#[cfg(test)]
#[cfg(not(loom))]
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
            sealed in any::<bool>(),
        ) {
            let root = acyclic_fs::WorkspaceId::from_bytes([1; 16]);
            let association = acyclic_fs::ObjectStorage {
                locator: acyclic_fs::Digest::from_bytes(generation),
                root_file_id: acyclic_fs::FileId::from_bytes(operation),
                origin_generation: acyclic_fs::Digest::from_bytes(generation),
            };
            let pending = (acyclic_fs::OperationId::from_bytes(operation), GitGenerationRef {
                workspace_id: root,
                generation: acyclic_fs::GenerationId::from_root(acyclic_fs::ObjectId {
                    kind: acyclic_fs::ObjectKind::GenerationRoot,
                    digest: acyclic_fs::Digest::from_bytes(generation),
                    storage: sealed.then_some(association),
                }),
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
            if result.is_err() { prop_assert_eq!(state, original.clone()); }
            else {
                prop_assert_eq!(result.unwrap(), original.revision);
                let mut wanted = original.clone(); wanted.revision += 1;
                wanted.checkout_reconciliation = None;
                prop_assert_eq!(state, wanted);
            }
            for field in 0..4 {
                let mut wrong = pending;
                let mut object = wrong.1.generation.object_id();
                let mut changed = association;
                let mut bytes = generation; bytes[0] ^= 1;
                match field {
                    0 => object.storage = if sealed { None } else { Some(association) },
                    1 => { changed.locator = acyclic_fs::Digest::from_bytes(bytes); object.storage = Some(changed); }
                    2 => { let mut bytes = operation; bytes[0] ^= 1; changed.root_file_id = acyclic_fs::FileId::from_bytes(bytes); object.storage = Some(changed); }
                    _ => { changed.origin_generation = acyclic_fs::Digest::from_bytes(bytes); object.storage = Some(changed); }
                }
                wrong.1.generation = acyclic_fs::GenerationId::from_root(object);
                prop_assert_eq!(wrong.1.generation.digest(), pending.1.generation.digest());
                let mut state = original.clone();
                prop_assert!(update(&mut state, Some(wrong), None).is_err());
                prop_assert_eq!(state, original.clone());
            }
        }
    }
}
