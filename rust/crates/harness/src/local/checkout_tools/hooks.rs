use super::*;
use crate::hooks::{HookEvent, HookOutput, Hooks};

impl CheckoutTools {
    pub(super) async fn configured_hooks(
        &self,
        configuration: (&Hooks, [u8; 32]),
        event: HookEvent,
        input: crate::hooks::HookInput<'_>,
        registry: &ToolRegistry,
        context: ToolContext,
        reconcile_only: bool,
    ) -> Result<Option<HookOutput>> {
        if let Some(project) = self.child_project(&context)? {
            let selected = self.child_checkout(&context, project).await?;
            return selected
                .project_hooks(
                    configuration,
                    event,
                    input,
                    registry,
                    context,
                    reconcile_only,
                )
                .await;
        }
        self.project_hooks(
            configuration,
            event,
            input,
            registry,
            context,
            reconcile_only,
        )
        .await
    }

    async fn project_hooks(
        &self,
        configuration: (&Hooks, [u8; 32]),
        event: HookEvent,
        input: crate::hooks::HookInput<'_>,
        registry: &ToolRegistry,
        context: ToolContext,
        reconcile_only: bool,
    ) -> Result<Option<HookOutput>> {
        let (hooks, digest) = configuration;
        let scope = context.effective_scope();
        if !scope.grants().contains(&self.read_grant) || !scope.grants().contains(&self.write_grant)
        {
            return Err(Error::Unauthorized(
                "hooks require the selected project grants".into(),
            ));
        }
        let owner = context.task().ordinary_journal_owner::<LocalStream>()?;
        if !reconcile_only {
            super::super::context::binding::authorize(
                &self.task_files.inner,
                owner.task_id(),
                &digest,
                scope.limits().file_bytes,
                || async {
                    // Reconstruct the derived pointer from the owner's immutable
                    // admission, never from a request or current settings file.
                    let value = owner.admission().input.get("model").ok_or_else(|| {
                        Error::Unauthorized("hook task has no admitted model command".into())
                    })?;
                    let command: crate::workflow::WorkflowCommand =
                        serde::Deserialize::deserialize(value).map_err(storage)?;
                    if command.kind != crate::filesystem::MODEL_TASK_COMMAND_KIND {
                        return Err(Error::Unauthorized(
                            "hook task has no admitted model command".into(),
                        ));
                    }
                    let bytes = context.task().read_file(&command.payload, None).await?;
                    let command: crate::filesystem::ModelTaskCommand =
                        serde_json::from_slice(&bytes).map_err(storage)?;
                    Ok(command.context_manifest)
                },
            )
            .await?;
        }
        let operation = context.operation_id();
        crate::hooks::effects::run_once::<_, _, _, _, _, true>(
            &self.task_files.inner,
            (owner.task_id(), operation, event.name()),
            scope.limits().file_bytes,
            !reconcile_only,
            || async {
                let condition = owner
                    .condition(
                        if reconcile_only {
                            crate::distributed::JournalWrite::Settlement
                        } else {
                            crate::distributed::JournalWrite::Fresh
                        },
                        &mut None,
                    )
                    .await?;
                Ok((vec![condition], owner.deadline_epoch_ms()))
            },
            || async {
                let selected = if self.selected {
                    None
                } else {
                    self.select(&context).await?
                };
                let selected = selected.as_deref().unwrap_or(self);
                selected
                    .execute_hooks(hooks, event, input, registry, &context)
                    .await
            },
        )
        .await
    }

    async fn execute_hooks(
        &self,
        hooks: &Hooks,
        event: HookEvent,
        input: crate::hooks::HookInput<'_>,
        registry: &ToolRegistry,
        context: &ToolContext,
    ) -> Result<HookOutput> {
        // Prompt events share a command operation. Capture receipts must also
        // include the event, just like the retained hook outcome does, or a
        // second event's workspace edits reuse the first event's receipt.
        let operation = hook_capture_operation(context.operation_id(), event);
        serialized(&self.state, async |mut state| {
            let owner = context
                .task()
                .owned_mailbox
                .as_ref()
                .ok_or_else(|| Error::Unauthorized("hook task has no owner".into()))?;
            owner.verify_owner(None)?.await?;
            let workspace = self.selected_workspace(&mut state, owner.as_ref()).await?;
            let view = state
                .active
                .as_ref()
                .unwrap_or_else(|| self.workspace.view());
            if reconciliation::recover(&workspace, view, &|| async {
                owner.verify_owner(None)?.await
            })
            .await?
            {
                state.watcher = None;
            }
            workspace::volumes::sync(&state.mounts).await?;
            {
                let State {
                    active, watcher, ..
                } = &mut *state;
                let view = active.as_ref().unwrap_or_else(|| self.workspace.view());
                self.capture(
                    (&workspace, view),
                    watcher,
                    operation,
                    0,
                    PublicationPermit::Unrestricted,
                    Some((
                        context.task().ordinary_journal_owner::<LocalStream>()?,
                        false,
                    )),
                )
                .await?;
            }
            // Each blocking worker retains this guard through cancellation and
            // child-tree reaping; dropping the caller cannot switch its view.
            let check_workspace = Arc::clone(&workspace);
            let check_owner = Arc::clone(owner);
            let runtime = tokio::runtime::Handle::current();
            let check = move || {
                if acyclic_fs::capture_root_identity(&check_workspace.path)
                    .map_err(std::io::Error::other)?
                    != check_workspace.identity
                {
                    return Err(std::io::Error::other("hook workspace root was replaced"));
                }
                runtime.block_on(async {
                    tokio::time::timeout(
                        std::time::Duration::from_millis(u64::from(output::CONTROL_TIMEOUT_MS)),
                        check_owner
                            .verify_owner(None)
                            .map_err(std::io::Error::other)?,
                    )
                    .await
                    .map_err(std::io::Error::other)?
                    .map_err(std::io::Error::other)
                })
            };
            // Hooks share the shell's native execution scope: bound volumes are
            // projected for the run and retired, with their writes published,
            // before the checkout is captured.
            let mounts = Arc::clone(&state.mounts);
            let (result, mut state) = workspace::volumes::native_scope(&mounts, async |claims| {
                let (result, (state, claims)) =
                    with_workers::<_, _, _, _, true>((state, claims), check, |check| {
                        hooks.run_checked(
                            event,
                            (&workspace.path, &workspace.path),
                            input,
                            registry,
                            context.effective_scope().limits().file_bytes,
                            check,
                        )
                    })
                    .await?;
                owner.verify_owner(None)?.await?;
                Ok(((result, state), claims))
            })
            .await?;
            let State {
                active, watcher, ..
            } = &mut *state;
            let view = active.as_ref().unwrap_or_else(|| self.workspace.view());
            self.capture(
                (&workspace, view),
                watcher,
                operation,
                1,
                PublicationPermit::Unrestricted,
                Some((
                    context.task().ordinary_journal_owner::<LocalStream>()?,
                    true,
                )),
            )
            .await?;
            owner.verify_owner(None)?.await?;
            result
        })
        .await
    }
}

fn hook_capture_operation(operation: OperationId, event: HookEvent) -> OperationId {
    let mut hash = blake3::Hasher::new_derive_key("acyclic.local.hook-capture/v1");
    hash.update(&operation.into_bytes());
    hash.update(event.name().as_bytes());
    let mut identity = [0; 16];
    identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    OperationId::from_bytes(identity)
}

// The native runner and model share this whole ownership boundary. Only the
// caller can recover the guard, after all workers have released their checks.
async fn with_workers<T: Send + Sync + 'static, R, F, FF, const RETAIN: bool>(
    state: T,
    check: impl Fn() -> std::io::Result<()> + Send + Sync + 'static,
    execute: F,
) -> Result<(R, T)>
where
    F: FnOnce(crate::hooks::HookCheck) -> FF + Send,
    FF: std::future::Future<Output = R> + Send,
{
    let held = StateArc::new(state);
    let worker = StateArc::clone(&held);
    let check: crate::hooks::HookCheck = if RETAIN {
        Arc::new(move || {
            let _guard = &worker;
            check()
        })
    } else {
        drop(worker);
        Arc::new(check)
    };
    let result = execute(check).await;
    let state = StateArc::try_unwrap(held)
        .map_err(|_| Error::Conflict("hook cleanup still owns its selected view".into()))?;
    Ok((result, state))
}

#[cfg(all(test, acyclic_harness_loom))]
mod models;
