//! Default commands bound to the selected, identity-fenced checkout.
use super::*;
use crate::sync::owned::{Arc as StateArc, Mutex as AsyncMutex, OwnedMutexGuard, lock_owned};
use crate::{
    runtime::ToolContext,
    tool::{ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_fs::{NativeWatch, PublicationPermit};
use acyclic_stream::BoxProviderFuture;

pub(super) mod access;
pub(super) mod capture;
mod git;
#[cfg(any(test, feature = "hooks"))]
mod hooks;
mod output;
pub(super) mod reconciliation;
pub(super) mod shell;
#[cfg(test)]
#[cfg(loom)]
mod tests;

pub(super) struct CheckoutTools {
    workspace: Arc<LocalWorkspace>,
    signed: Scope,
    read_grant: String,
    write_grant: String,
    capture_options: acyclic_fs::CaptureOptions,
    capture_policy: acyclic_fs::CapturePolicy,
    clock: Arc<dyn acyclic_stream::UnixMillisClock>,
    /// The signed scope's authenticated expiry; a running command stops there.
    expiry_unix_ms: u64,
    // Lock order: checkout state is outermost; command-local git merge cache
    // and switched-view locks are innermost and never held together. Native
    // completion locks are also inner; neither callback reacquires state.
    // Child views live in the workspace's own index, never under this guard;
    // only forking a child from a selected branch takes the branch owner's
    // guard. No path holds two checkout guards. Bindings share their
    // workspace's guard.
    state: StateArc<AsyncMutex<State>>,
}

pub(in crate::local) struct State {
    pub(in crate::local) active: Option<workspace::View>,
    pub(in crate::local) initial_view: acyclic_fs::WorkspaceId,
    pub(in crate::local) workspace: Option<(acyclic_fs::WorkspaceId, Arc<LocalWorkspace>)>,
    pub(in crate::local) allocated_mounts:
        std::collections::BTreeMap<String, std::sync::Weak<workspace::Mounts>>,
    pub(in crate::local) watcher: Option<capture::Cursor>,
    pub(in crate::local) mounts: Arc<workspace::Mounts>,
    pub(in crate::local) initialized: bool,
}

impl State {
    pub(in crate::local) fn new(
        initial_view: acyclic_fs::WorkspaceId,
        mounts: Arc<workspace::Mounts>,
    ) -> Self {
        Self {
            active: None,
            initial_view,
            workspace: None,
            allocated_mounts: Default::default(),
            watcher: None,
            mounts,
            initialized: false,
        }
    }
}

// Keep the whole command, including its publication and cursor replacement,
// inside one guard. The async model exercises this same boundary.
async fn serialized<T, R>(
    state: &StateArc<AsyncMutex<T>>,
    command: impl AsyncFnOnce(OwnedMutexGuard<T>) -> R,
) -> R {
    command(lock_owned(StateArc::clone(state)).await).await
}

impl CheckoutTools {
    pub(super) async fn new(
        workspace: Arc<LocalWorkspace>,
        project: &VolumeRef,
        clock: Arc<dyn acyclic_stream::UnixMillisClock>,
        signed: &Scope,
        verifier: &AuthorityVerifier,
    ) -> Result<Self> {
        let expiry_unix_ms = verifier.expiry(signed)?.saturating_mul(1000);
        if workspace
            .filesystem
            .workspace_id(&project.storage_name(&mut None)?)
            .map_err(acyclic_fs::WorkspaceError::from)?
            != workspace.view().workspace().id()
        {
            return Err(Error::Unauthorized(
                "workspace differs from the bound project volume".into(),
            ));
        }
        let (capture_options, capture_policy) = capture::configuration(&workspace)?;
        let shared = StateArc::clone(&workspace.state);
        {
            let mut state = lock_owned(StateArc::clone(&shared)).await;
            if !state.initialized {
                let (active, pending) = workspace.selected_view().await?;
                state.active = active;
                let view = state.active.as_ref().unwrap_or_else(|| workspace.view());
                state.initial_view = view.workspace().id();
                let view = state.active.as_ref().unwrap_or_else(|| workspace.view());
                reconciliation::recover_pending(&workspace, view, pending, &|| async { Ok(()) })
                    .await?;
                state.watcher = if workspace.fresh {
                    Some(capture::Cursor::initial(&workspace, &capture_policy)?)
                } else {
                    None
                };
                state.initialized = true;
            }
        }
        Ok(Self {
            signed: signed.clone(),
            read_grant: project.capability(VolumeOperation::Read, &mut None)?,
            write_grant: project.capability(VolumeOperation::Write, &mut None)?,
            capture_options,
            capture_policy,
            workspace,
            clock,
            expiry_unix_ms,
            state: shared,
        })
    }
}

struct PreparedCommand {
    invocation: ToolInvocation,
    shell: Option<crate::tools::ShellInput>,
    fs: Option<crate::tools::FsCommand>,
    request: Vec<u8>,
    maximum: usize,
}

struct ShellCommand<'a> {
    invocation: &'a ToolInvocation,
    context: &'a ToolContext,
    shell: Option<crate::tools::ShellInput>,
    request: Vec<u8>,
    maximum: usize,
}

impl CheckoutTools {
    #[inline(never)]
    fn run_checkout_command<'a>(
        &'a self,
        command: PreparedCommand,
        context: &'a ToolContext,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ToolResult>> {
        crate::conversation::BoundedContentFuture::new(None, || async move {
            let PreparedCommand {
                invocation,
                shell,
                fs,
                request,
                maximum,
            } = command;
            let result = serialized(&self.state, async |mut state| {
                let owner = context.task();
                let workspace = self.workspace.selected_workspace(&mut state, owner).await?;
                let view = state
                    .active
                    .as_ref()
                    .unwrap_or_else(|| self.workspace.view());
                self.require_command_access(
                    view.workspace(),
                    context.effective_scope().grants(),
                    invocation.name == "fs",
                )
                .await?;
                if reconciliation::recover(&workspace, view, &|| async { Ok(()) }).await? {
                    state.watcher = None;
                }
                workspace::volumes::sync(&state.mounts).await?;
                if let Some(parsed) = fs {
                    self.fs(parsed, context, &mut state, &workspace).await
                } else {
                    self.shell_checkout(
                        ShellCommand {
                            invocation: &invocation,
                            context,
                            shell,
                            request,
                            maximum,
                        },
                        state,
                        &workspace,
                    )?
                    .await
                }
            })
            .await;
            #[cfg(test)]
            if let Err(error) = &result {
                eprintln!("checkout command {}: {error:?}", invocation.name);
            }
            result
        })
    }
    #[inline(never)]
    fn shell_checkout<'a>(
        &'a self,
        command: ShellCommand<'a>,
        mut state: OwnedMutexGuard<State>,
        workspace: &'a Arc<LocalWorkspace>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ToolResult>> {
        crate::conversation::BoundedContentFuture::new(None, || async move {
            let ShellCommand {
                invocation,
                context,
                shell,
                request,
                maximum,
            } = command;
            let State {
                active, watcher, ..
            } = &mut *state;
            let view = active.as_ref().unwrap_or_else(|| self.workspace.view());
            self.capture(
                (workspace, view),
                watcher,
                invocation.operation_id,
                0,
                PublicationPermit::Unrestricted,
            )?
            .await?;
            let input = shell.ok_or_else(|| Error::Invalid("shell input is absent".into()))?;
            let files = output::Output::open(context, request).await?;
            let mounts = Arc::clone(&state.mounts);
            let (result, mut state, mut files) =
                workspace::volumes::native_scope(&mounts, async |claims| {
                    let (result, state, files, claims) =
                        self.shell(input, maximum, state, files, claims).await?;
                    if !result.as_ref().is_ok_and(|result| {
                        result
                            .value
                            .get("stop")
                            .is_none_or(|stop| stop.is_null() || stop["cleanup_completed"] == true)
                    }) {
                        return Err(Error::Indeterminate(invocation.operation_id));
                    }
                    Ok(((result, state, files), claims))
                })
                .await?;
            let State {
                active, watcher, ..
            } = &mut *state;
            let view = active.as_ref().unwrap_or_else(|| self.workspace.view());
            self.capture(
                (workspace, view),
                watcher,
                invocation.operation_id,
                1,
                PublicationPermit::Unrestricted,
            )?
            .await?;
            if let Ok(result) = &result {
                files.finish(result).await?;
            }
            result
        })
    }
}

impl ToolExecutor for CheckoutTools {
    // A cancelled turn drops its call: the command's stop guard stops the
    // process tree, the spawned capture keeps the workspace guard until the
    // tree is reaped, and the next call reconciles from the durable capture.
    fn cancellable_on_drop(&self) -> bool {
        true
    }

    #[cfg(any(test, feature = "hooks"))]
    fn run_hooks<'a>(
        &'a self,
        configuration: (&'a crate::hooks::Hooks, [u8; 32]),
        event: crate::hooks::HookEvent,
        input: crate::hooks::HookInput<'a>,
        registry: &'a ToolRegistry,
        context: ToolContext,
        reconcile_only: bool,
    ) -> BoxProviderFuture<'a, Result<Option<crate::hooks::HookOutput>>> {
        Box::pin(self.configured_hooks(
            configuration.0,
            event,
            input,
            registry,
            context,
            reconcile_only,
        ))
    }

    fn authorize(
        &self,
        _scope: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        if work.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "local checkout commands require ordinary task execution",
            });
        }
        let context = context
            .ok_or_else(|| Error::Unauthorized("checkout command requires a task".into()))?;
        let scope = context.effective_scope();
        if (invocation.name != crate::tools::shell::PLATFORM.name && invocation.name != "fs")
            || context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
            || !(scope.grants().contains(&self.read_grant)
                && scope.grants().contains(&self.write_grant))
            || crate::tools::catalog()
                .find(|spec| spec.name == invocation.name)
                .is_none_or(|spec| !scope.grants().contains(&spec.capability()))
        {
            return Err(Error::Unauthorized(
                "checkout command differs from its task authority".into(),
            ));
        }
        Ok(())
    }

    fn execute<'a>(
        &'a self,
        mut invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            let original = context
                .as_ref()
                .ok_or_else(|| Error::Unauthorized("checkout task is absent".into()))?;
            serialized(&self.state, async |state| {
                let view = state
                    .active
                    .as_ref()
                    .unwrap_or_else(|| self.workspace.view());
                self.require_command_access(
                    view.workspace(),
                    original.effective_scope().grants(),
                    invocation.name == "fs",
                )
                .await
            })
            .await?;
            let request = if invocation.name == crate::tools::shell::PLATFORM.name {
                serde_json::to_vec(&invocation).map_err(storage)?
            } else {
                Vec::new()
            };
            let shell = if invocation.name == crate::tools::shell::PLATFORM.name {
                let input: crate::tools::ShellInput =
                    serde_json::from_value(std::mem::take(&mut invocation.arguments))
                        .map_err(|cause| Error::Invalid(cause.to_string()))?;
                input.validate()?;
                Some(input)
            } else {
                None
            };
            let command = if shell.is_none() {
                let command = invocation
                    .arguments
                    .as_object_mut()
                    .and_then(|arguments| arguments.remove("command"));
                let Some(serde_json::Value::String(command)) = command else {
                    return Err(Error::Invalid("checkout command must be text".into()));
                };
                Some(command)
            } else {
                None
            };
            let context = context
                .as_ref()
                .ok_or_else(|| Error::Unauthorized("checkout task is absent".into()))?;
            let fs = if let Some(command) = command {
                match self.fs_arguments(&command, context.effective_scope()) {
                    Ok(parsed) => Some(parsed),
                    Err(error) => return Ok(crate::tools::rejected_input(error)),
                }
            } else {
                None
            };
            let maximum = context.effective_scope().limits().render_bytes;
            let maximum = usize::try_from(maximum).map_err(storage)?;
            self.run_checkout_command(
                PreparedCommand {
                    invocation,
                    shell,
                    fs,
                    request,
                    maximum,
                },
                context,
            )?
            .await
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            // A dispatched process must never be launched again after a lost ack.
            Ok(None)
        })
    }
}

#[cfg(test)]
mod native_capture_storage_tests {
    use super::*;
    fn retained<Args, T>(_: impl FnOnce(Args) -> T) -> usize {
        std::mem::size_of::<T>()
    }
    #[test]
    fn capture_spine_retains_only_owned_polling_handles() {
        let measured = [
            (
                "run_checkout_command",
                retained(
                    |(tools, command, context): (
                        &'static CheckoutTools,
                        PreparedCommand,
                        &'static ToolContext,
                    )| tools.run_checkout_command(command, context),
                ),
            ),
            (
                "shell_checkout",
                retained(
                    |(tools, command, state, workspace): (
                        &'static CheckoutTools,
                        ShellCommand<'static>,
                        OwnedMutexGuard<State>,
                        &'static Arc<LocalWorkspace>,
                    )| tools.shell_checkout(command, state, workspace),
                ),
            ),
        ];
        eprintln!("capture spine retained bytes: {measured:?}");
        assert!(
            measured.into_iter().all(|(_, bytes)| bytes <= 512),
            "capture spine embeds concrete child futures"
        );
    }
}
