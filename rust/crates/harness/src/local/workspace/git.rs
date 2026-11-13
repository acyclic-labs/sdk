//! Shared workspace git facade; no tool dispatch or agent execution.
use super::super::checkout_tools::{State, capture, reconciliation};
use super::owner::ViewOwner;
use super::*;

#[cfg(all(test, not(loom)))]
mod tests;

pub(in crate::local) struct CommandContext<'a> {
    pub(in crate::local) owner: &'a dyn ViewOwner,
    pub(in crate::local) operation: OperationId,
    pub(in crate::local) scope: &'a RuntimeScope,
    /// The native request this command runs as, whose saved result a
    /// replay returns.
    pub(in crate::local) request: Option<acyclic_fs::GitRequest>,
}

pub(in crate::local) struct Commands<'a> {
    pub(in crate::local) workspace: &'a Arc<LocalWorkspace>,
    pub(in crate::local) signed: &'a Scope,
    /// Verifies the caller's scope, independently of a reused view's mount owner.
    pub(in crate::local) verifier: &'a crate::core::AuthorityVerifier,
    pub(in crate::local) clock: &'a dyn acyclic_stream::UnixMillisClock,
    pub(in crate::local) capture_options: &'a acyclic_fs::CaptureOptions,
    pub(in crate::local) capture_policy: &'a acyclic_fs::CapturePolicy,
}

pub(in crate::local) enum Outcome {
    Completed(Box<acyclic_fs::GitCommandOutput>),
    Rejected(Box<Error>),
}

struct Invocation<'a> {
    workspace: &'a LocalWorkspace,
    context: &'a CommandContext<'a>,
    checkout: &'a workspace::View,
    view: crate::tools::git::View<'a>,
    source: Option<&'a workspace::MountedVolume>,
    watcher: &'a mut Option<capture::Cursor>,
    access: &'a crate::filesystem::WorkspaceAccess,
    capture_cut: acyclic_fs::GenerationId,
}

/// A cancelled or failed settlement still owes its durable queue a drain.
struct PublicationSettlement<'a> {
    debt: &'a std::sync::atomic::AtomicBool,
    completed: bool,
}

impl Drop for PublicationSettlement<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.debt.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

impl Commands<'_> {
    /// One serialized entry to the git facade, shared by native hosts and
    /// platform-owned session integration. The existing capture, recovery,
    /// publication window and conflict projection all remain inside it.
    pub(in crate::local) fn invoke<'a>(
        &'a self,
        parsed: crate::tools::FsCommand,
        context: CommandContext<'a>,
        before: acyclic_stream::BoxProviderFuture<'a, Result<()>>,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<acyclic_fs::GitCommandOutput>> {
        Box::pin(async move {
            context.owner.verify().await?;
            self.fs_source(self.workspace, &parsed, context.scope)?;
            let mut state =
                crate::sync::owned::lock_owned(StateArc::clone(&self.workspace.state)).await;
            let selected: acyclic_stream::BoxProviderFuture<'_, _> =
                Box::pin(self.workspace.selected_view());
            let (selected, pending) = selected.await?;
            let verify = || context.owner.verify();
            let recovery: acyclic_stream::BoxProviderFuture<'_, _> =
                Box::pin(reconciliation::recover_pending(
                    self.workspace,
                    selected.as_ref().unwrap_or_else(|| self.workspace.view()),
                    pending,
                    &verify,
                ));
            recovery.await?;
            state.active = selected;
            let physical: acyclic_stream::BoxProviderFuture<'_, _> =
                Box::pin(self.workspace.selected_workspace(&mut state, context.owner));
            let physical = physical.await?;
            let settled: acyclic_stream::BoxProviderFuture<'_, _> =
                Box::pin(self.settle_publications(&context, &mut state));
            settled.await?;
            before.await?;
            let command: acyclic_stream::BoxProviderFuture<'_, Result<Outcome>> =
                Box::pin(self.fs(parsed, &context, &mut state, &physical));
            let result = command.await?;
            context.owner.verify().await?;
            match result {
                Outcome::Completed(output) => Ok(*output),
                Outcome::Rejected(error) => Err(*error),
            }
        })
    }

    pub(in crate::local) fn fs_arguments(
        &self,
        command: &str,
        scope: &RuntimeScope,
    ) -> Result<crate::tools::FsCommand> {
        let parsed = crate::tools::parse_fs_command(command)?;
        self.fs_source(self.workspace, &parsed, scope)?;
        Ok(parsed)
    }

    fn fs_source<'a>(
        &self,
        workspace: &'a LocalWorkspace,
        parsed: &crate::tools::FsCommand,
        scope: &RuntimeScope,
    ) -> Result<&'a Option<Arc<workspace::MountedVolume>>> {
        use acyclic_fs::access::Action;
        let (relative, mount) = workspace.mounts.named(parsed.volume.as_deref())?;
        let command = parsed
            .arguments
            .first()
            .ok_or_else(|| Error::Invalid("fs command is empty".into()))?;
        let read_only = matches!(command.as_str(), "diff" | "log" | "show" | "status");
        for (action, operation) in std::iter::once((Action::Read, VolumeOperation::Read))
            .chain((!read_only).then_some((Action::Write, VolumeOperation::Write)))
        {
            mount.check(action, "", self.clock.now_unix_millis() / 1000)?;
            if !crate::core::volume_operation_granted_with_work(
                scope.grants(),
                &mount.view.volume,
                operation,
                &mut None,
            )? {
                return Err(Error::Unauthorized(
                    "fs target volume is not granted".into(),
                ));
            }
        }
        if !relative.is_empty()
            && !read_only
            && !matches!(command.as_str(), "merge" | "add" | "discard")
        {
            return Err(Error::Unsupported(
                "--volume supports diff, log, show, status, merge, add and discard".into(),
            ));
        }
        if !relative.is_empty() && mount.view.source.is_none() {
            return Err(Error::NotFound("volume mount is not bound".into()));
        }
        Ok(&mount.view.source)
    }

    pub(in crate::local) async fn fs(
        &self,
        parsed: crate::tools::FsCommand,
        context: &CommandContext<'_>,
        state: &mut State,
        workspace: &Arc<LocalWorkspace>,
    ) -> Result<Outcome> {
        let project = parsed.volume.is_none();
        let result = self
            .fs_one(parsed, context, state, workspace)
            .await?
            .ok_or_else(|| {
                Error::Conflict("recovered a pending fs transition; retry the command".into())
            })?;
        if project && matches!(result, Outcome::Completed(_)) {
            self.drain_publications(context, state).await?;
        }
        Ok(result)
    }

    /// Hands a completed fork back without waiting for this checkout's
    /// command guard. The durable enqueue is the handback; an idle checkout
    /// publishes it now, and a busy one publishes it at its next command
    /// (see [`Self::settle_publications`]). A handback never waits on the
    /// work of the checkout it lands in.
    pub(in crate::local) async fn hand_back(
        &self,
        context: &CommandContext<'_>,
        branch: String,
        source: acyclic_fs::GitGenerationRef,
    ) -> Result<()> {
        self.workspace
            .defer_publication(context.operation, branch, source)
            .await?;
        let Ok(mut state) = StateArc::clone(&self.workspace.state).try_lock_owned() else {
            return Ok(());
        };
        self.settle_publications(context, &mut state).await
    }

    /// Publishes every deferred handback before the guard's holder reads the
    /// checkout. Costs one atomic load unless a handback raised the debt.
    pub(in crate::local) async fn settle_publications(
        &self,
        context: &CommandContext<'_>,
        state: &mut State,
    ) -> Result<()> {
        use std::sync::atomic::Ordering::SeqCst;
        if !self.workspace.publication_debt.swap(false, SeqCst) {
            return Ok(());
        }
        let mut settlement = PublicationSettlement {
            debt: &self.workspace.publication_debt,
            completed: false,
        };
        let drained = self.drain_publications(context, state).await;
        // Do not clear the flag here: another handback may have raised it
        // while this drain held the checkout guard.
        settlement.completed = drained.is_ok();
        drained
    }

    pub(in crate::local) async fn drain_publications(
        &self,
        context: &CommandContext<'_>,
        state: &mut State,
    ) -> Result<()> {
        loop {
            // A recovered checkout can select a different native mount.
            // Capture and publication must use that same selected view.
            let workspace = self
                .workspace
                .selected_workspace(state, context.owner)
                .await?;
            let repository = acyclic_fs::GitCompatRepository::new(
                workspace.view().workspace().id(),
                workspace.store.clone(),
            );
            let snapshot = repository.load().await.map_err(storage)?;
            if snapshot.pending.as_ref().is_some_and(|pending| {
                matches!(
                    pending.mutation,
                    acyclic_fs::GitPendingMutation::Join {
                        conflicts: Some(_),
                        ..
                    }
                )
            }) {
                return Ok(());
            }
            let Some((sequence, publication)) =
                self.workspace.next_publication(&repository).await?
            else {
                return Ok(());
            };
            // A branch is a disposable reference. The queued request owns
            // its source workspace independently, including across restart.
            match snapshot.branches.get(&publication.branch) {
                Some(branch) if branch.workspace_id != publication.source.workspace_id => {
                    return Err(Error::Conflict(
                        "deferred publication branch was rebound".into(),
                    ));
                }
                Some(_) => {}
                None => repository
                    .register_branch_workspace(
                        publication.branch.clone(),
                        publication.source.workspace_id,
                        None,
                        false,
                    )
                    .await
                    .map_err(storage)?,
            }
            let parsed =
                self.fs_arguments(&format!("merge {}", publication.branch), context.scope)?;
            let result = self
                .fs_one(
                    parsed,
                    &CommandContext {
                        owner: context.owner,
                        operation: publication.operation,
                        scope: context.scope,
                        request: Some(acyclic_fs::GitRequest {
                            merge_source: Some(publication.source),
                            operation: acyclic_fs::OperationId::from_bytes(
                                publication.operation.into_bytes(),
                            ),
                            unanswered: Arc::new(|_| true),
                        }),
                    },
                    state,
                    &workspace,
                )
                .await?;
            match result {
                // A different prepared transition just committed. Re-enter
                // from that durable result, with its newly selected cut.
                None => continue,
                Some(Outcome::Completed(_)) => {}
                Some(Outcome::Rejected(error)) => {
                    // Its projection is retained by this exact pending join.
                    let pending = acyclic_fs::GitCompatRepository::new(
                        workspace.view().workspace().id(),
                        workspace.store.clone(),
                    )
                    .pending_transition()
                    .await
                    .map_err(storage)?;
                    if !matches!(*error, Error::MergeConflict { .. })
                        || !pending.is_some_and(|pending| {
                            pending.request
                                == Some(acyclic_fs::OperationId::from_bytes(
                                    publication.operation.into_bytes(),
                                ))
                                && matches!(
                                    pending.mutation,
                                    acyclic_fs::GitPendingMutation::Join {
                                        conflicts: Some(_),
                                        ..
                                    }
                                )
                        })
                    {
                        return Err(*error);
                    }
                }
            }
            self.workspace
                .acknowledge_publication(sequence, publication.operation, &repository)
                .await?;
        }
    }

    async fn fs_one(
        &self,
        parsed: crate::tools::FsCommand,
        context: &CommandContext<'_>,
        state: &mut State,
        workspace: &Arc<LocalWorkspace>,
    ) -> Result<Option<Outcome>> {
        let source = self.fs_source(workspace, &parsed, context.scope)?.clone();
        let native = match &source {
            Some(source) => Some(source.publication().await?),
            None => None,
        };
        let native = native.as_ref().and_then(|(guard, _)| guard.as_ref());
        let State {
            active, watcher, ..
        } = state;
        let checkout = active.as_ref().unwrap_or_else(|| workspace.view());
        let view = source
            .as_ref()
            .map_or_else(|| checkout.into(), |source| source.workspace.view());
        let owner = context.owner;
        owner.verify().await?;
        if source.is_some()
            && let Some(outcome) = acyclic_fs::OperationWindowCoordinator::new(
                workspace.filesystem.operation_window_store(),
            )
            .recover_workspace(
                view.workspace(),
                acyclic_stream::UnixMillisClock::now_unix_millis(
                    &acyclic_stream::SystemUnixMillisClock,
                ),
                acyclic_fs::OperationReconcileLimits::default(),
            )
            .await
            .map_err(storage)?
        {
            if reconciliation::terminal(&outcome)
                && let Some(native) = native
            {
                owner.verify().await?;
                native.advance_to_head().await?;
            }
            reconciliation_generation(outcome)?;
        }
        // Runtime ownership and the checkout guard cover the whole command.
        // Capture user edits before reconciling the parent at command close;
        // a second, timed lease must not revoke this still-owned operation.
        let parent = head(view).await?;
        let access = crate::filesystem::WorkspaceAccess::new(
            self.verifier,
            self.signed,
            context.scope.grants(),
            Some(
                &workspace
                    .mounts
                    .named(parsed.volume.as_deref())?
                    .1
                    .view
                    .volume,
            ),
        )?;
        let result = self
            .execute(
                &parsed.arguments,
                Invocation {
                    workspace,
                    context,
                    checkout,
                    view,
                    source: source.as_deref(),
                    watcher,
                    access: &access,
                    capture_cut: parent.id(),
                },
            )
            .await;
        let before_close = head(view).await?;
        let parent = match view
            .workspace()
            .parent_advance(
                None,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
        {
            Ok(receipt) => receipt.value,
            Err(failure) if matches!(failure.error, acyclic_fs::WorkspaceError::NotFork) => None,
            Err(failure) => return Err(storage(failure)),
        };
        let journaled = parent.is_some()
            && source.is_none()
            && !result
                .as_ref()
                .is_ok_and(|output| output.switched.is_some());
        if journaled {
            self.prepare_reconciliation(workspace, context, b"close", &before_close)
                .await?;
        }
        let (publication, terminal) = if let Some(parent) = parent {
            match acyclic_fs::OperationWindowCoordinator::new(
                workspace.filesystem.operation_window_store(),
            )
            .parent_advanced_workspace(
                view.workspace(),
                parent,
                acyclic_fs::OperationReconcileLimits::default(),
            )
            .await
            {
                Ok(Some(outcome)) => {
                    let terminal = reconciliation::terminal(&outcome);
                    (reconciliation_generation(outcome).map(|_| ()), terminal)
                }
                Ok(None) => (Ok(()), true),
                Err(error) => (Err(storage(error)), false),
            }
        } else {
            (Ok(()), true)
        };
        // In-place actions publish directly to the workspace, including conflict
        // projections. Refresh its clean mount before another command can enter
        // this checkout. Switched actions select their destination mount below.
        let advanced = async {
            if let Some(native) = native {
                owner.verify().await?;
                native.advance_to_head().await?;
                owner.verify().await?;
            } else if source.is_none()
                && !result
                    .as_ref()
                    .is_ok_and(|output| output.switched.is_some())
            {
                if terminal {
                    self.publish_reconciliation(workspace, context, true)
                        .await?;
                } else {
                    reconciliation::advance_mount(workspace, &|| owner.verify()).await?;
                }
            }
            Ok::<(), Error>(())
        }
        .await;
        owner.verify().await?;
        self.finish_command(workspace, state, context, result, (publication, advanced))
            .await
    }

    async fn execute(
        &self,
        arguments: &[String],
        invocation: Invocation<'_>,
    ) -> Result<crate::tools::git::Output> {
        let Invocation {
            workspace,
            context,
            checkout,
            view,
            source,
            watcher,
            access,
            capture_cut,
        } = invocation;
        let owner = context.owner;
        owner.verify().await?;
        if source.is_none() {
            // A conflict continuation retains the Git request, but captures a
            // new checkout transition. Its selected cut distinguishes that
            // publication from an earlier capture under the same request.
            let capture_operation =
                reconciliation_operation(context.operation, b"capture", capture_cut, capture_cut);
            capture::capture(
                (self.capture_options, self.capture_policy),
                (workspace, checkout),
                watcher,
                (OperationId::from_bytes(capture_operation.into_bytes()), 0),
                acyclic_fs::PublicationPermit::Unrestricted,
            )
            .await?;
        }
        crate::tools::git::run_with_guard(
            crate::tools::git::Workspace {
                access,
                filesystem: &workspace.filesystem,
                store: &workspace.store,
                view,
                root: source.map_or(workspace.path.as_path(), |source| {
                    source.destination.as_path()
                }),
                repository: if source.is_some() {
                    view.workspace().id()
                } else {
                    workspace.view().workspace().id()
                },
                lease: None,
                checkout: (source.is_none() && workspace.mounted.is_none())
                    .then_some(workspace.identity),
                author: "LocalHarness",
                authored_at_seconds: i64::try_from(self.clock.now_unix_millis() / 1000)
                    .unwrap_or(i64::MAX),
                request: context.request.clone(),
            },
            arguments,
            || owner.verify(),
        )
        .await
    }

    async fn finish_command(
        &self,
        workspace: &Arc<LocalWorkspace>,
        state: &mut State,
        context: &CommandContext<'_>,
        result: Result<crate::tools::git::Output>,
        closed: (Result<()>, Result<()>),
    ) -> Result<Option<Outcome>> {
        let (publication, advanced) = closed;
        let owner = context.owner;
        let output = match result {
            Ok(output) => output,
            Err(
                error @ (Error::Conflict(_)
                | Error::MergeConflict { .. }
                | Error::Invalid(_)
                | Error::NotFound(_)
                | Error::Unsupported(_)),
            ) => {
                publication?;
                advanced?;
                return Ok(Some(Outcome::Rejected(Box::new(error))));
            }
            Err(error) => return Err(error),
        };
        if let Some(switched) = output.switched {
            state.active = Some(switched.into_checkout()?);
            workspace.selected_workspace(state, owner).await?;
        }
        // Persist the selected view in memory even when a recovered transition
        // has no command result or closing the old workspace failed.
        publication?;
        advanced?;
        Ok(output
            .command
            .map(|command| Outcome::Completed(Box::new(command))))
    }

    async fn prepare_reconciliation(
        &self,
        workspace: &LocalWorkspace,
        context: &CommandContext<'_>,
        phase: &[u8],
        from: &acyclic_fs::Generation<LocalAuthorityBackend, LocalObjectBackend>,
    ) -> Result<()> {
        let operation = reconciliation_operation(context.operation, phase, from.id(), from.id());
        reconciliation::prepare(
            workspace,
            operation,
            acyclic_fs::GitGenerationRef {
                workspace_id: from.workspace().id(),
                generation: from.id(),
            },
        )
        .await
    }

    async fn publish_reconciliation(
        &self,
        workspace: &LocalWorkspace,
        context: &CommandContext<'_>,
        refresh_mount: bool,
    ) -> Result<()> {
        let state = workspace.selected_view().await?.0;
        let view = state.as_ref().unwrap_or_else(|| workspace.view());
        reconciliation::publish(workspace, view, refresh_mount, &|| context.owner.verify()).await?;
        Ok(())
    }
}

pub(in crate::local) fn reconciliation_generation(
    outcome: acyclic_fs::WorkspaceRebase<LocalAuthorityBackend, LocalObjectBackend>,
) -> Result<acyclic_fs::Generation<LocalAuthorityBackend, LocalObjectBackend>> {
    match outcome {
        acyclic_fs::WorkspaceRebase::Rebased(generation)
        | acyclic_fs::WorkspaceRebase::AlreadyRebased(generation)
        | acyclic_fs::WorkspaceRebase::Current(generation) => Ok(generation),
        _ => Err(Error::Conflict(
            "checkout reconciliation did not settle".into(),
        )),
    }
}

fn reconciliation_operation(
    operation: OperationId,
    phase: &[u8],
    from: acyclic_fs::GenerationId,
    to: acyclic_fs::GenerationId,
) -> acyclic_fs::OperationId {
    let mut hash = blake3::Hasher::new_derive_key("acyclic.local.git-reconciliation/v2");
    hash.update(&operation.into_bytes());
    hash.update(phase);
    hash.update(from.to_bytes().as_slice());
    hash.update(to.to_bytes().as_slice());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    acyclic_fs::OperationId::from_bytes(bytes)
}

async fn head(
    view: crate::tools::git::View<'_>,
) -> Result<acyclic_fs::Generation<LocalAuthorityBackend, LocalObjectBackend>> {
    view.workspace()
        .head(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(storage)
}
