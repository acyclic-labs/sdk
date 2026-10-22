//! The shared git facade over the active checkout view.
use super::*;

impl CheckoutTools {
    pub(super) fn fs_arguments(
        &self,
        command: &str,
        scope: &RuntimeScope,
    ) -> Result<crate::tools::FsCommand> {
        let parsed = crate::tools::parse_fs_command(command)?;
        self.fs_source(&self.workspace, &parsed, scope)?;
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
        let read_only = matches!(
            parsed.arguments[0].as_str(),
            "diff" | "log" | "show" | "status"
        );
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
        if !relative.is_empty() && !read_only && parsed.arguments[0] != "merge" {
            return Err(Error::Unsupported(
                "--volume supports diff, log, show, status and merge".into(),
            ));
        }
        if !relative.is_empty() && mount.view.source.is_none() {
            return Err(Error::NotFound("volume mount is not bound".into()));
        }
        Ok(&mount.view.source)
    }

    pub(super) async fn fs(
        &self,
        parsed: crate::tools::FsCommand,
        context: &ToolContext,
        state: &mut State,
        workspace: &Arc<LocalWorkspace>,
    ) -> Result<ToolResult> {
        let (source, reserved) =
            self.fs_target_with_reserved_paths(workspace, &parsed, context.effective_scope())?;
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
            .task()
            .owned_mailbox
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("git command has no execution owner".into()))?;
        owner.verify_owner(None)?.await?;
        let (task, fence) = owner.task_binding();
        let lease_id = lease_identity(task, context.operation_id(), fence);
        let coordinator = acyclic_fs::OperationWindowCoordinator::new(
            workspace.filesystem.operation_window_store(),
        );
        let volume = view.workspace();
        let now = acyclic_stream::UnixMillisClock::now_unix_millis(
            &acyclic_stream::SystemUnixMillisClock,
        );
        let before_recovery = head(view).await?;
        if source.is_none() {
            self.prepare_reconciliation(workspace, context, b"recover", &before_recovery)
                .await?;
        }
        // A previous worker can disappear after opening its lease. Expiry
        // fences it; complete its durable reconciliation before opening ours.
        let recovery = coordinator
            .recover_workspace(volume, now, acyclic_fs::OperationReconcileLimits::default())
            .await
            .map_err(storage)?;
        let recovered = recovery.is_some();
        let parent = match recovery {
            Some(outcome) => reconciliation_generation(outcome)?,
            None => before_recovery.clone(),
        };
        // With no recovered window the preparation marker covers no changes.
        // Acknowledge it without refreshing a mount that is already current.
        if source.is_none() {
            self.publish_reconciliation(workspace, context, recovered)
                .await?;
        } else if recovered && let Some(native) = native {
            owner.verify_owner(None)?.await?;
            native.advance_to_head().await?;
            owner.verify_owner(None)?.await?;
        }
        const GIT_PUBLICATION_LEASE_MILLIS: u64 = 60_000;
        let lease = coordinator
            .begin_with_lease_id(
                volume.id(),
                parent.id(),
                context.operation_id().to_string(),
                now,
                now.saturating_add(GIT_PUBLICATION_LEASE_MILLIS),
                lease_id,
            )
            .await
            .map_err(storage)?;
        let result = async {
            owner.verify_owner(None)?.await?;
            if source.is_none() {
                self.capture(
                    (workspace, checkout),
                    watcher,
                    context.operation_id(),
                    0,
                    lease.publication_permit(),
                    Some((
                        context.task().ordinary_journal_owner::<LocalStream>()?,
                        false,
                    )),
                )
                .await?;
            }
            crate::tools::git::run_with_guard(
                crate::tools::git::Workspace {
                    filesystem: &workspace.filesystem,
                    store: &workspace.store,
                    view,
                    root: source.as_ref().map_or(workspace.path.as_path(), |source| {
                        source.destination.as_path()
                    }),
                    repository: if source.is_some() {
                        volume.id()
                    } else {
                        workspace.view().workspace().id()
                    },
                    lease: Some(lease.clone()),
                    checkout: (source.is_none() && workspace.mounted.is_none())
                        .then_some(workspace.identity),
                    reserved: &reserved,
                    author: "LocalHarness",
                    authored_at_seconds: i64::try_from(self.clock.now_unix_millis() / 1000)
                        .unwrap_or(i64::MAX),
                },
                &parsed.arguments,
                || async { owner.verify_owner(None)?.await },
            )
            .await
        }
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
        let closed = coordinator
            .finish_workspace(
                volume,
                &lease,
                acyclic_stream::UnixMillisClock::now_unix_millis(
                    &acyclic_stream::SystemUnixMillisClock,
                ),
                acyclic_fs::OperationReconcileLimits::default(),
            )
            .await
            .map_err(storage);
        // Closing can rebase a child after the command has already published.
        // A switched checkout belongs to the destination, so never apply the
        // source workspace's reconciliation over it.
        let publication = match closed {
            Ok(acyclic_fs::WorkspaceOperationFinish::Reconciled(outcome)) => {
                reconciliation_generation(outcome).map(|_| ())
            }
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        };
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
                return Ok(crate::tools::rejected_input(error));
            }
            Err(error) => return Err(error),
        };
        if let Some(switched) = output.switched {
            *active = Some(switched.into_checkout()?);
            self.selected_workspace(state, owner.as_ref()).await?;
        }
        // Persist the selected view in memory even when a recovered transition
        // has no command result or closing the old workspace failed.
        publication?;
        advanced?;
        let command = output.command.ok_or_else(|| {
            Error::Conflict("recovered a pending fs transition; retry the command".into())
        })?;
        Ok(ToolResult {
            value: serde_json::json!({"result": command}),
        })
    }

    async fn prepare_reconciliation(
        &self,
        workspace: &LocalWorkspace,
        context: &ToolContext,
        phase: &[u8],
        from: &acyclic_fs::Generation<LocalAuthorityBackend, LocalObjectBackend>,
    ) -> Result<()> {
        let operation =
            reconciliation_operation(context.operation_id(), phase, from.id(), from.id());
        reconciliation::prepare(
            &workspace,
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
        context: &ToolContext,
        refresh_mount: bool,
    ) -> Result<()> {
        let state = workspace.selected_view().await?.0;
        let view = state.as_ref().unwrap_or_else(|| workspace.view());
        reconciliation::publish(&workspace, view, refresh_mount, &|| async {
            context
                .task()
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

pub(super) fn reconciliation_generation(
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

#[cfg(all(test, not(acyclic_harness_loom)))]
#[path = "git_tests.rs"]
mod tests;
