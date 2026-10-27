//! Configured lifecycle hooks run in the selected checkout like a shell
//! command. The checkout is captured before and after, the hooks share the
//! shell's native volume scope, and each event's outcome is retained once on
//! the agent's private volume.
use super::*;
use crate::filesystem::native_process::capture::OwnerControl;
use crate::hooks::{HookEvent, HookOutput, Hooks};

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
        let scope = context.effective_scope();
        if !scope.grants().contains(&self.read_grant) || !scope.grants().contains(&self.write_grant)
        {
            return Err(Error::Unauthorized(
                "hooks require the selected project grants".into(),
            ));
        }
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
                .workspace
                .selected_workspace(&mut state, context.task())
                .await?;
            let view = state
                .active
                .as_ref()
                .unwrap_or_else(|| self.workspace.view());
            if reconciliation::recover(&workspace, view, &|| async { Ok(()) }).await? {
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
                )?
                .await?;
            }
            let control = OwnerControl::open(self.expiry_unix_ms)?;
            // A dropped turn stops its hooks; their workers still reap them.
            let _stop = shell::StopOnDrop(control.clone());
            let wake = control.wake();
            let check = owner_check(Arc::clone(&workspace), control);
            let mounts = Arc::clone(&state.mounts);
            let (result, mut state) = workspace::volumes::native_scope(&mounts, async |claims| {
                let (result, (state, claims)) = with_workers((state, claims), check, |check| {
                    hooks.run_checked(
                        event,
                        (&workspace.path, &workspace.path),
                        input,
                        registry,
                        context.effective_scope().limits().file_bytes,
                        (check, Some(wake)),
                    )
                })
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
    control: OwnerControl,
) -> impl Fn() -> std::io::Result<()> + Send + Sync + 'static {
    move || {
        if acyclic_fs::capture_root_identity(&workspace.path).map_err(std::io::Error::other)?
            != workspace.identity
        {
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
