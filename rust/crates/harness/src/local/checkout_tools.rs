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

mod capture;
mod children;
mod git;
#[cfg(test)]
#[cfg(all(unix, not(acyclic_harness_loom)))]
mod git_mount_tests;
mod hooks;
mod output;
pub(super) mod publisher;
pub(super) mod reconciliation;
pub(super) mod shell;
#[cfg(all(
    test,
    feature = "filesystem-local",
    any(target_os = "linux", windows),
    not(acyclic_harness_loom)
))]
mod task_view_volumes_tests;
mod task_views;
#[cfg(test)]
#[cfg(acyclic_harness_loom)]
mod tests;

pub(super) struct CheckoutTools {
    workspace: Arc<LocalWorkspace>,
    signed: Scope,
    read_grant: String,
    write_grant: String,
    capture_options: acyclic_fs::CaptureOptions,
    capture_policy: acyclic_fs::CapturePolicy,
    clock: Arc<dyn acyclic_stream::UnixMillisClock>,
    task_files: Arc<crate::task_volume::TaskFiles<acyclic_stream::LocalStream>>,
    // Lock order: checkout state is outermost; command-local git merge cache
    // and switched-view locks are innermost and never held together. Native
    // completion locks are also inner; neither callback reacquires state.
    // Release the owner's flat task index before locking a selected ancestor;
    // release that guard before publishing its child's cache entry. No path
    // holds two checkout guards. Bindings share their workspace's guard.
    state: StateArc<AsyncMutex<State>>,
    selected: bool,
}

pub(in crate::local) struct State {
    pub(in crate::local) active: Option<workspace::View>,
    initial_view: acyclic_fs::WorkspaceId,
    workspace: Option<(acyclic_fs::WorkspaceId, Arc<LocalWorkspace>)>,
    children: std::collections::BTreeMap<TaskId, Arc<CheckoutTools>>,
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
            children: Default::default(),
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
        task_files: Arc<crate::task_volume::TaskFiles<acyclic_stream::LocalStream>>,
    ) -> Result<Self> {
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
            task_files,
            state: shared,
            selected: false,
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
    async fn run_checkout_command(
        &self,
        command: PreparedCommand,
        context: &ToolContext,
    ) -> Result<ToolResult> {
        let PreparedCommand {
            invocation,
            shell,
            fs,
            request,
            maximum,
        } = command;
        let result = serialized(&self.state, async |mut state| {
            let owner = context.task().owned_mailbox.as_ref().ok_or_else(|| {
                Error::Unauthorized("checkout command has no execution owner".into())
            })?;
            owner.verify_owner(None)?.await?;
            let workspace = self.selected_workspace(&mut state, owner.as_ref()).await?;
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
            if reconciliation::recover(&workspace, view, &|| async {
                owner.verify_owner(None)?.await
            })
            .await?
            {
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
                )
                .await
            }
        })
        .await;
        #[cfg(test)]
        if let Err(error) = &result {
            eprintln!("checkout command {}: {error:?}", invocation.name);
        }
        result
    }
    async fn shell_checkout(
        &self,
        command: ShellCommand<'_>,
        mut state: OwnedMutexGuard<State>,
        workspace: &Arc<LocalWorkspace>,
    ) -> Result<ToolResult> {
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
            Some((
                context.task().ordinary_journal_owner::<LocalStream>()?,
                false,
            )),
        )
        .await?;
        let input = shell.ok_or_else(|| Error::Invalid("shell input is absent".into()))?;
        let files = output::Output::open(context, Arc::clone(&self.task_files), request).await?;
        let mounts = Arc::clone(&state.mounts);
        let publication = context.task().ordinary_journal_owner::<LocalStream>()?;
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
                // Verify the journal owner before the scope publishes.
                publication.verify(true, &mut None).await?;
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
            Some((Arc::clone(&publication), true)),
        )
        .await?;
        publication.verify(true, &mut None).await?;
        if let Ok(result) = &result {
            files.finish(result).await?;
        }
        result
    }
}

impl ToolExecutor for CheckoutTools {
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
            configuration,
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
            || (!(scope.grants().contains(&self.read_grant)
                && scope.grants().contains(&self.write_grant))
                && self.child_project(context)?.is_none())
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
            if let Some(project) = self.child_project(original)? {
                let selected = self.child_checkout(original, project).await?;
                return selected.execute(invocation, context).await;
            }
            if !self.selected {
                let context = context
                    .as_ref()
                    .ok_or_else(|| Error::Invalid("tool context missing".into()))?;
                serialized(&self.state, async |state| {
                    let view = state
                        .active
                        .as_ref()
                        .unwrap_or_else(|| self.workspace.view());
                    self.require_command_access(
                        view.workspace(),
                        context.effective_scope().grants(),
                        invocation.name == "fs",
                    )
                    .await
                })
                .await?;
            }
            if !self.selected
                && let Some(selected) = self
                    .select(
                        context
                            .as_ref()
                            .ok_or_else(|| Error::Invalid("tool context missing".into()))?,
                    )
                    .await?
            {
                return selected.execute(invocation, context).await;
            }
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
            )
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
