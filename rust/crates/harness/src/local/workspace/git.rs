//! Shared workspace git facade; no tool dispatch or agent execution.
use super::super::checkout_tools::{State, capture, reconciliation};
use super::*;
use crate::runtime::TaskContext;

pub(in crate::local) struct CommandContext<'a> {
    pub(in crate::local) task: &'a TaskContext,
    pub(in crate::local) operation: OperationId,
    pub(in crate::local) scope: &'a RuntimeScope,
}

pub(in crate::local) struct Commands<'a> {
    pub(in crate::local) workspace: &'a Arc<LocalWorkspace>,
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
    reserved: &'a [String],
    lease: &'a acyclic_fs::OperationWindowLease,
    watcher: &'a mut Option<capture::Cursor>,
}

impl Commands<'_> {
    pub(in crate::local) fn fs_arguments(
        &self,
        command: &str,
        scope: &RuntimeScope,
    ) -> Result<crate::tools::FsCommand> {
        let parsed = crate::tools::parse_fs_command(command)?;
        self.fs_source(self.workspace, &parsed, scope)?;
        Ok(parsed)
    }

    /// The volume an fs command targets with the root-relative paths its
    /// history reserves. Volume paths are relative to the project root; a
    /// volume repository owns no aliases of its own.
    fn fs_target_with_reserved_paths(
        &self,
        workspace: &LocalWorkspace,
        parsed: &crate::tools::FsCommand,
        scope: &RuntimeScope,
    ) -> Result<(Option<Arc<workspace::MountedVolume>>, Vec<String>)> {
        let source = self.fs_source(workspace, parsed, scope)?.clone();
        let reserved = if source.is_none() {
            workspace.volume_owned_paths()?
        } else {
            Vec::new()
        };
        Ok((source, reserved))
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
            if !scope
                .grants()
                .contains(&mount.view.volume.capability(operation, &mut None)?)
            {
                return Err(Error::Unauthorized(
                    "fs target volume is not granted".into(),
                ));
            }
        }
        if !relative.is_empty() && !read_only && command != "merge" {
            return Err(Error::Unsupported(
                "--volume supports diff, log, show, status and merge".into(),
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
        let (source, reserved) =
            self.fs_target_with_reserved_paths(workspace, &parsed, context.scope)?;
        let native = match &source {
            Some(source) => Some(source.native().await),
            None => None,
        };
        let native = native.as_ref().and_then(|guard| guard.as_ref());
        let State {
            active, watcher, ..
        } = state;
        let checkout = active.as_ref().unwrap_or_else(|| workspace.view());
        let view = source
            .as_ref()
            .map_or_else(|| checkout.into(), |source| source.workspace.view());
        let owner = context
            .task
            .owned_mailbox
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("git command has no execution owner".into()))?;
        owner.verify_owner(None)?.await?;
        let volume = view.workspace();
        let now = acyclic_stream::UnixMillisClock::now_unix_millis(
            &acyclic_stream::SystemUnixMillisClock,
        );
        let (parent, recovered) = self
            .recover_window(workspace, context, view, source.is_none(), now)
            .await?;
        if source.is_some()
            && recovered
            && let Some(native) = native
        {
            owner.verify_owner(None)?.await?;
            native.advance_to_head().await?;
            owner.verify_owner(None)?.await?;
        }
        let lease = begin_window(workspace, context, volume.id(), parent.id(), now).await?;
        let result = self
            .execute(
                &parsed.arguments,
                Invocation {
                    workspace,
                    context,
                    checkout,
                    view,
                    source: source.as_deref(),
                    reserved: &reserved,
                    lease: &lease,
                    watcher,
                },
            )
            .await;
        let before_close = head(view).await?;
        if source.is_none()
            && !result
                .as_ref()
                .is_ok_and(|output| output.switched.is_some())
        {
            self.prepare_reconciliation(workspace, context, b"close", &before_close)
                .await?;
        }
        let publication = finish_window(workspace, view, &lease).await;
        // In-place actions publish directly to the workspace, including conflict
        // projections. Refresh its clean mount before another command can enter
        // this checkout. Switched actions select their destination mount below.
        let advanced = async {
            if let Some(native) = native {
                owner.verify_owner(None)?.await?;
                native.advance_to_head().await?;
                owner.verify_owner(None)?.await?;
            } else if source.is_none()
                && !result
                    .as_ref()
                    .is_ok_and(|output| output.switched.is_some())
            {
                if publication.is_ok() {
                    self.publish_reconciliation(workspace, context, true)
                        .await?;
                } else {
                    reconciliation::advance_mount(workspace, &|| async {
                        owner.verify_owner(None)?.await
                    })
                    .await?;
                }
            }
            Ok::<(), Error>(())
        }
        .await;
        owner.verify_owner(None)?.await?;
        self.finish_command(workspace, state, context, result, (publication, advanced))
            .await
    }

    async fn recover_window(
        &self,
        workspace: &LocalWorkspace,
        context: &CommandContext<'_>,
        view: crate::tools::git::View<'_>,
        checkout: bool,
        now: u64,
    ) -> Result<(
        acyclic_fs::Generation<LocalAuthorityBackend, LocalObjectBackend>,
        bool,
    )> {
        let before = head(view).await?;
        if checkout {
            self.prepare_reconciliation(workspace, context, b"recover", &before)
                .await?;
        }
        // Expired operation windows must settle before another publication.
        let recovered = acyclic_fs::OperationWindowCoordinator::new(
            workspace.filesystem.operation_window_store(),
        )
        .recover_workspace(
            view.workspace(),
            now,
            acyclic_fs::OperationReconcileLimits::default(),
        )
        .await
        .map_err(storage)?;
        let changed = recovered.is_some();
        let parent = match recovered {
            Some(outcome) => reconciliation_generation(outcome)?,
            None => before,
        };
        if checkout {
            self.publish_reconciliation(workspace, context, changed)
                .await?;
        }
        Ok((parent, changed))
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
            reserved,
            lease,
            watcher,
        } = invocation;
        let owner = context
            .task
            .owned_mailbox
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("git command has no execution owner".into()))?;
        owner.verify_owner(None)?.await?;
        if source.is_none() {
            capture::capture(
                (self.capture_options, self.capture_policy),
                (workspace, checkout),
                watcher,
                (context.operation, 0),
                lease.publication_permit(),
                Some((context.task.ordinary_journal_owner::<LocalStream>()?, false)),
            )
            .await?;
        }
        crate::tools::git::run_with_guard(
            crate::tools::git::Workspace {
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
                lease: Some(lease.clone()),
                checkout: (source.is_none() && workspace.mounted.is_none())
                    .then_some(workspace.identity),
                reserved,
                author: "LocalHarness",
                authored_at_seconds: i64::try_from(self.clock.now_unix_millis() / 1000)
                    .unwrap_or(i64::MAX),
            },
            arguments,
            || async { owner.verify_owner(None)?.await },
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
    ) -> Result<Outcome> {
        let (publication, advanced) = closed;
        let owner = context
            .task
            .owned_mailbox
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("git command has no execution owner".into()))?;
        let output = match result {
            Ok(output) => output,
            Err(
                error @ (Error::Conflict(_)
                | Error::Invalid(_)
                | Error::NotFound(_)
                | Error::Unsupported(_)),
            ) => {
                publication?;
                advanced?;
                return Ok(Outcome::Rejected(Box::new(error)));
            }
            Err(error) => return Err(error),
        };
        if let Some(switched) = output.switched {
            state.active = Some(switched.into_checkout()?);
            workspace.selected_workspace(state, owner.as_ref()).await?;
        }
        // Persist the selected view in memory even when a recovered transition
        // has no command result or closing the old workspace failed.
        publication?;
        advanced?;
        let command = output.command.ok_or_else(|| {
            Error::Conflict("recovered a pending fs transition; retry the command".into())
        })?;
        Ok(Outcome::Completed(Box::new(command)))
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
        reconciliation::publish(workspace, view, refresh_mount, &|| async {
            context
                .task
                .owned_mailbox
                .as_ref()
                .ok_or_else(|| Error::Unauthorized("git command has no execution owner".into()))?
                .verify_owner(None)?
                .await
        })
        .await?;
        Ok(())
    }
}

async fn begin_window(
    workspace: &LocalWorkspace,
    context: &CommandContext<'_>,
    volume: acyclic_fs::WorkspaceId,
    parent: acyclic_fs::GenerationId,
    now: u64,
) -> Result<acyclic_fs::OperationWindowLease> {
    let owner = context
        .task
        .owned_mailbox
        .as_ref()
        .ok_or_else(|| Error::Unauthorized("git command has no execution owner".into()))?;
    let (task, fence) = owner.task_binding();
    const GIT_PUBLICATION_LEASE_MILLIS: u64 = 60_000;
    acyclic_fs::OperationWindowCoordinator::new(workspace.filesystem.operation_window_store())
        .begin_with_lease_id(
            volume,
            parent,
            context.operation.to_string(),
            now,
            now.saturating_add(GIT_PUBLICATION_LEASE_MILLIS),
            lease_identity(task, context.operation, fence),
        )
        .await
        .map_err(storage)
}

async fn finish_window(
    workspace: &LocalWorkspace,
    view: crate::tools::git::View<'_>,
    lease: &acyclic_fs::OperationWindowLease,
) -> Result<()> {
    // Closing can rebase the source after the command has published. The
    // caller preserves a switched destination rather than refreshing it here.
    let outcome =
        acyclic_fs::OperationWindowCoordinator::new(workspace.filesystem.operation_window_store())
            .finish_workspace(
                view.workspace(),
                lease,
                acyclic_stream::UnixMillisClock::now_unix_millis(
                    &acyclic_stream::SystemUnixMillisClock,
                ),
                acyclic_fs::OperationReconcileLimits::default(),
            )
            .await
            .map_err(storage)?;
    match outcome {
        acyclic_fs::WorkspaceOperationFinish::Reconciled(outcome) => {
            reconciliation_generation(outcome).map(|_| ())
        }
        _ => Ok(()),
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
    let mut hash = blake3::Hasher::new_derive_key("acyclic.local.git-reconciliation/v1");
    hash.update(&operation.into_bytes());
    hash.update(phase);
    hash.update(from.digest().as_bytes());
    hash.update(to.digest().as_bytes());
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

fn lease_identity(
    task: TaskId,
    operation: OperationId,
    fence: &crate::scheduler::LeaseFence,
) -> acyclic_fs::OperationLeaseId {
    let mut hash = blake3::Hasher::new_derive_key("acyclic.local.git-task-fence/v1");
    hash.update(&task.into_bytes());
    hash.update(&operation.into_bytes());
    for field in [&fence.placement, &fence.reservation_id] {
        hash.update(&(field.len() as u64).to_le_bytes());
        hash.update(field.as_bytes());
    }
    let mut identity = [0; 16];
    identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    acyclic_fs::OperationLeaseId::from_bytes(identity)
}

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
#[path = "../checkout_tools/git_tests.rs"]
mod tests;
