//! The shared git facade over the active checkout view.
use super::*;

impl CheckoutTools {
    pub(super) async fn fs(
        &self,
        command: &str,
        context: &ToolContext,
        state: &mut State,
    ) -> Result<ToolResult> {
        let State { active, watcher } = state;
        let view = active.as_ref().unwrap_or_else(|| self.workspace.view());
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
        self.publish_reconciliation(context, b"recover", before_recovery, parent.clone())
            .await?;
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
            self.capture(
                view,
                watcher,
                context.operation_id(),
                0,
                lease.publication_permit(),
            )
            .await?;
            self.fs_leased(view, command, context, &lease).await
        }
        .await;
        let before_close = head(view).await?;
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
                    Ok(to)
                        if !result
                            .as_ref()
                            .is_ok_and(|output| output.switched.is_some()) =>
                    {
                        self.publish_reconciliation(context, b"close", before_close, to)
                            .await
                    }
                    Ok(_) => Ok(()),
                    Err(error) => Err(error),
                }
            }
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        };
        let output = result?;
        if let Some(switched) = output.switched {
            *active = Some(switched);
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

    async fn fs_leased(
        &self,
        view: &workspace::View,
        command: &str,
        context: &ToolContext,
        lease: &acyclic_fs::OperationWindowLease,
    ) -> Result<crate::tools::git::Output> {
        let mut argv = crate::tools::parse_command(command)?;
        if argv.first().is_some_and(|arg| arg == "git") {
            argv.remove(0);
        }
        crate::tools::git::run_with_guard(
            crate::tools::git::Workspace {
                filesystem: &self.workspace.filesystem,
                store: &self.workspace.store,
                view,
                root: &self.workspace.path,
                repository: self.workspace.view().workspace().id(),
                lease: Some(lease.clone()),
                checkout: Some(self.workspace.identity),
                author: "LocalHarness",
                authored_at_seconds: i64::try_from(self.clock.now_unix_millis() / 1000)
                    .unwrap_or(i64::MAX),
            },
            &argv,
            || async {
                let owner = context.task().owned_mailbox.as_ref().ok_or_else(|| {
                    Error::Unauthorized("git command has no execution owner".into())
                })?;
                owner.verify_owner(None)?.await
            },
        )
        .await
    }

    async fn publish_reconciliation(
        &self,
        context: &ToolContext,
        phase: &[u8],
        from: acyclic_fs::Generation<LocalAuthorityBackend, LocalObjectBackend>,
        to: acyclic_fs::Generation<LocalAuthorityBackend, LocalObjectBackend>,
    ) -> Result<()> {
        if from.id() == to.id() {
            return Ok(());
        }
        let operation = reconciliation_operation(context.operation_id(), phase, from.id(), to.id());
        crate::tools::git::publish_checkout_transition(
            &self.workspace.path,
            self.workspace.identity,
            &self.workspace.store,
            operation,
            from,
            to,
            &|| async {
                context
                    .task()
                    .owned_mailbox
                    .as_ref()
                    .ok_or_else(|| {
                        Error::Unauthorized("git command has no execution owner".into())
                    })?
                    .verify_owner(None)?
                    .await
            },
        )
        .await
    }
}

fn reconciliation_generation(
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
    view: &workspace::View,
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
