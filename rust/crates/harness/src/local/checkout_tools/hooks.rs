//! Configured lifecycle hooks run in the selected checkout like a shell
//! command. The checkout is captured before and after, the hooks share the
//! shell's native volume scope, and each event's outcome is retained once on
//! the agent's private volume.
use super::*;
use crate::hooks::{HookEvent, HookOutput, Hooks};
use acyclic_native_runtime::ProcessControl;

impl CheckoutTools {
    pub(super) async fn configured_hooks(
        &self,
        hooks: &Hooks,
        event: HookEvent,
        input: crate::hooks::HookInput<'_>,
        registry: &ToolRegistry,
        context: ToolContext,
        reconcile_only: bool,
    ) -> Result<Option<HookOutput>> {
        // Retained outcomes are immutable private files, read under this
        // task's own grant without waiting behind a live checkout command.
        crate::hooks::effects::run_once(
            &context,
            event,
            reconcile_only,
            (
                || self.execute_hooks(hooks, event, input, registry, &context),
                || hooks.interrupted(event, input, registry),
            ),
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
        // Prompt events share a turn operation, so the capture receipt names
        // the event too, or a second event's edits reuse the first's receipt.
        let operation = capture_operation(context.operation_id(), event);
        serialized(&self.state, async |mut state| {
            let workspace = self
                .command_workspace(&mut state, context, "local hooks")
                .await?;
            let view = state
                .active
                .as_ref()
                .unwrap_or_else(|| self.workspace.view());
            if reconciliation::recover(&workspace, view, &|| async { Ok(()) }).await? {
                state.watcher = None;
            }
            workspace::volumes::sync(&workspace.mounts).await?;
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
                )?
                .await?;
            }
            let control = ProcessControl::open(self.expiry_unix_ms).map_err(storage)?;
            // A dropped turn stops its hooks; their workers still reap them.
            let _stop = shell::StopOnDrop(control.clone());
            let wake = control.wake();
            let check: crate::hooks::HookCheck =
                Arc::new(owner_check(Arc::clone(&workspace), control));
            let mounts = Arc::clone(&workspace.mounts);
            let (result, mut state) = workspace::volumes::native_scope(&mounts, async |claims| {
                // Pin the active projections, after native_scope has mounted
                // them. Binding the dormant backing directory would fence a
                // different object and refuse every projected-volume hook.
                let mut environment = self.environment.clone();
                let grant = self.execution_grant.clone();
                let preparation_mounts = Arc::clone(&mounts);
                let preparation_check = Arc::clone(&check);
                let (execution, (state, claims)) =
                    crate::filesystem::native_process::prepare_with_custody(
                        (state, claims),
                        move |_| {
                            preparation_check().map_err(Error::process_io)?;
                            let grant = match grant {
                                Some(grant) => {
                                    crate::filesystem::native_process::shell::workspace_environment(
                                        &preparation_mounts,
                                        &mut environment,
                                    )?;
                                    Some(grant)
                                }
                                None => crate::filesystem::native_process::shell::workspace_grant(
                                    &preparation_mounts,
                                    &mut environment,
                                )?,
                            };
                            Ok(grant.map(|grant| {
                                Arc::new(crate::hooks::HookExecution { grant, environment })
                            }))
                        },
                    )
                    .await?;
                let (result, (state, claims)) = with_workers(
                    (state, claims),
                    move || check(),
                    |check| {
                        hooks.run_checked(
                            event,
                            (&workspace.path, &workspace.path),
                            input,
                            registry,
                            context.effective_scope().limits().file_bytes,
                            (check, Some(wake), execution),
                        )
                    },
                )
                .await?;
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
            )?
            .await?;
            result
        })
        .await
    }
}

fn owner_check(
    workspace: Arc<LocalWorkspace>,
    control: ProcessControl,
) -> impl Fn() -> std::io::Result<()> + Send + Sync + 'static {
    move || {
        // A served root's witness comes from its original mount owner. Asking
        // its kernel projection here can wait on callbacks owned by this check.
        if workspace.root_identity().map_err(std::io::Error::other)? != workspace.identity {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "hook workspace root was replaced",
            ));
        }
        control.check()
    }
}

fn capture_operation(operation: OperationId, event: HookEvent) -> OperationId {
    let mut hash = blake3::Hasher::new_derive_key("acyclic.local.hook-capture/v1");
    hash.update(&operation.into_bytes());
    hash.update(event.name().as_bytes());
    let mut identity = [0; 16];
    identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    OperationId::from_bytes(identity)
}

// Every hook worker's check holds the checkout guard, so a dropped turn cannot
// switch the view before the workers release it. Only then does the caller
// recover the guard.
async fn with_workers<T: Send + Sync + 'static, R, F, FF>(
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
    let check: crate::hooks::HookCheck = Arc::new(move || {
        let _guard = &worker;
        check()
    });
    let result = execute(check).await;
    let state = StateArc::try_unwrap(held)
        .map_err(|_| Error::Conflict("hook cleanup still owns its selected view".into()))?;
    Ok((result, state))
}

#[cfg(all(test, unix, not(loom)))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn owner_checks_reject_stopped_commands_and_replaced_physical_roots() -> Result<()> {
        let directory = tempfile::tempdir().map_err(storage)?;
        let root = directory.path().join("project");
        std::fs::create_dir(&root).map_err(storage)?;
        let harness = LocalHarness::open(
            crate::LocalHarnessConfig::native_host(&root)
                .workspace_mounting(WorkspaceMounting::Physical)
                .state_directory(directory.path().join("runtime"))
                .without_user_settings(),
        )
        .await?;
        let workspace = harness
            .workspace()
            .ok_or_else(|| Error::NotFound("physical workspace".into()))?;
        let control = ProcessControl::unbounded().map_err(storage)?;
        let check = owner_check(Arc::clone(workspace), control.clone());
        check().map_err(storage)?;
        control.stop();
        assert_eq!(
            check()
                .expect_err("stopped command retained authority")
                .kind(),
            std::io::ErrorKind::Interrupted
        );

        let check = owner_check(
            Arc::clone(workspace),
            ProcessControl::unbounded().map_err(storage)?,
        );
        std::fs::rename(&root, directory.path().join("retired")).map_err(storage)?;
        std::fs::create_dir(&root).map_err(storage)?;
        assert_eq!(
            check()
                .expect_err("replacement root retained authority")
                .kind(),
            std::io::ErrorKind::PermissionDenied
        );
        Ok(())
    }
}
