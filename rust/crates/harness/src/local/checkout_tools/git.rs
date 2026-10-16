//! The shared git facade over the active checkout view.
use super::*;

impl CheckoutTools {
    pub(super) fn fs_arguments(
        &self,
        command: &str,
        scope: &RuntimeScope,
    ) -> Result<crate::tools::FsCommand> {
        let parsed = crate::tools::parse_fs_command(command)?;
        self.fs_source(&parsed, scope)?;
        Ok(parsed)
    }

    fn fs_source(
        &self,
        parsed: &crate::tools::FsCommand,
        scope: &RuntimeScope,
    ) -> Result<&Option<Arc<workspace::MountedVolume>>> {
        use acyclic_fs::access::Action;
        let (relative, mount) = self.workspace.mounts.named(parsed.volume.as_deref())?;
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
        if !relative.is_empty()
            && mount
                .view
                .source
                .as_ref()
                .and_then(|source| source.native.as_ref())
                .is_none()
        {
            return Err(Error::NotFound("volume mount is not bound".into()));
        }
        Ok(&mount.view.source)
    }

    pub(super) async fn fs(
        &self,
        parsed: crate::tools::FsCommand,
        context: &ToolContext,
        state: &mut State,
    ) -> Result<ToolResult> {
        let source = self.fs_source(&parsed, context.effective_scope())?.clone();
        let native = source.as_ref().and_then(|source| source.native.as_ref());
        let State {
            active, watcher, ..
        } = state;
        let checkout = active.as_ref().unwrap_or_else(|| self.workspace.view());
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
            self.workspace.filesystem.operation_window_store(),
        );
        let workspace = view.workspace();
        let now = acyclic_stream::UnixMillisClock::now_unix_millis(
            &acyclic_stream::SystemUnixMillisClock,
        );
        let before_recovery = head(view).await?;
        if native.is_none() {
            self.prepare_reconciliation(context, b"recover", &before_recovery)
                .await?;
        }
        // A previous worker can disappear after opening its lease. Expiry
        // fences it; complete its durable reconciliation before opening ours.
        let recovery = coordinator
            .recover_workspace(
                workspace,
                now,
                acyclic_fs::OperationReconcileLimits::default(),
            )
            .await
            .map_err(storage)?;
        let parent = match recovery {
            Some(outcome) => reconciliation_generation(outcome)?,
            None => before_recovery.clone(),
        };
        if native.is_none() {
            self.publish_reconciliation(context).await?;
        } else if let Some(native) = native {
            native.advance_to_head().await?;
        }
        const GIT_PUBLICATION_LEASE_MILLIS: u64 = 60_000;
        let lease = coordinator
            .begin_with_lease_id(
                workspace.id(),
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
            if native.is_none() {
                self.capture(
                    checkout,
                    watcher,
                    context.operation_id(),
                    0,
                    lease.publication_permit(),
                )
                .await?;
            }
            crate::tools::git::run_with_guard(
                crate::tools::git::Workspace {
                    filesystem: &self.workspace.filesystem,
                    store: &self.workspace.store,
                    view,
                    root: native.map_or(self.workspace.path.as_path(), |mount| mount.path()),
                    repository: if native.is_some() {
                        workspace.id()
                    } else {
                        self.workspace.view().workspace().id()
                    },
                    lease: Some(lease.clone()),
                    checkout: native.is_none().then_some(self.workspace.identity),
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
        if native.is_none()
            && !result
                .as_ref()
                .is_ok_and(|output| output.switched.is_some())
        {
            self.prepare_reconciliation(context, b"close", &before_close)
                .await?;
        }
        let closed = coordinator
            .finish_workspace(
                workspace,
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
                match reconciliation_generation(outcome) {
                    Ok(_) if native.is_some() => Ok(()),
                    Ok(_)
                        if !result
                            .as_ref()
                            .is_ok_and(|output| output.switched.is_some()) =>
                    {
                        self.publish_reconciliation(context).await
                    }
                    Ok(_) => Ok(()),
                    Err(error) => Err(error),
                }
            }
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        };
        // A conflict is an intentional publication, even when the command
        // returns an error. Refresh the selected mount before propagating it.
        if let Some(native) = native {
            native.advance_to_head().await?;
        }
        owner.verify_owner(None)?.await?;
        let output = match result {
            Ok(output) => output,
            Err(
                error @ (Error::Conflict(_)
                | Error::Invalid(_)
                | Error::NotFound(_)
                | Error::Unsupported(_)),
            ) => {
                // Acknowledged facade refusals and projected merge conflicts
                // are feedback for the model. Failed publication or ownership
                // checks remain execution failures and never enter this branch.
                publication?;
                return Ok(argument_error(error));
            }
            Err(error) => return Err(error),
        };
        if let Some(switched) = output.switched {
            *active = Some(switched.into_checkout()?);
        }
        // Persist the selected view in memory even when a recovered transition
        // has no command result or closing the old workspace failed.
        publication?;
        let command = output.command.ok_or_else(|| {
            Error::Conflict("recovered a pending fs transition; retry the command".into())
        })?;
        Ok(ToolResult {
            value: serde_json::json!({"result": command}),
        })
    }

    async fn prepare_reconciliation(
        &self,
        context: &ToolContext,
        phase: &[u8],
        from: &acyclic_fs::Generation<LocalAuthorityBackend, LocalObjectBackend>,
    ) -> Result<()> {
        let operation =
            reconciliation_operation(context.operation_id(), phase, from.id(), from.id());
        reconciliation::prepare(
            &self.workspace,
            operation,
            acyclic_fs::GitGenerationRef {
                workspace_id: from.workspace().id(),
                generation: from.id(),
            },
        )
        .await
    }

    async fn publish_reconciliation(&self, context: &ToolContext) -> Result<()> {
        let state = self.workspace.selected_view().await?.0;
        let view = state.as_ref().unwrap_or_else(|| self.workspace.view());
        reconciliation::publish(&self.workspace, view, &|| async {
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
