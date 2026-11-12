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
#[cfg(any(test, not(target_arch = "wasm32")))]
mod hooks;
mod output;
pub(super) mod publisher;
pub(super) mod reconciliation;
pub(super) mod shell;
#[cfg(test)]
#[cfg(loom)]
mod tests;

#[derive(Clone)]
pub(super) struct CheckoutTools {
    workspace: Arc<LocalWorkspace>,
    signed: Scope,
    verifier: AuthorityVerifier,
    capture_options: acyclic_fs::CaptureOptions,
    capture_policy: acyclic_fs::CapturePolicy,
    clock: Arc<dyn acyclic_stream::UnixMillisClock>,
    /// The signed scope's authenticated expiry; a running command stops there.
    expiry_unix_ms: u64,
    /// Tool locations selected once by the native owner, never guest input.
    environment: std::collections::BTreeMap<String, String>,
    /// Host-approved shell/hook ceiling, independent of logical volume authority.
    execution_grant: Option<acyclic_native_runtime::execution::ExecutionGrant>,
    // Lock order: checkout state is outermost; command-local git merge cache
    // and switched-view locks are innermost and never held together. Native
    // completion locks are also inner; neither callback reacquires state.
    // Child views live in the workspace's own index, never under this guard;
    // only forking a child from a selected branch takes the branch owner's
    // guard. No path holds two checkout guards. Binding preparation and its
    // original producer event are owned separately from checkout selection.
    state: StateArc<AsyncMutex<State>>,
}

pub(in crate::local) struct State {
    pub(in crate::local) active: Option<workspace::View>,
    pub(in crate::local) initial_view: acyclic_fs::WorkspaceId,
    pub(in crate::local) workspace: Option<(acyclic_fs::WorkspaceId, Arc<LocalWorkspace>)>,
    pub(in crate::local) watcher: Option<capture::Cursor>,
    pub(in crate::local) initialized: bool,
    /// Calls of this process whose launch or tree cleanup did not complete:
    /// their process may still run, so reconciliation leaves them unknown.
    pub(in crate::local) unsettled: std::collections::BTreeSet<crate::OperationId>,
}

impl State {
    pub(in crate::local) fn new(initial_view: acyclic_fs::WorkspaceId) -> Self {
        Self {
            active: None,
            initial_view,
            workspace: None,
            watcher: None,
            initialized: false,
            unsettled: Default::default(),
        }
    }
}

// Keep the whole command, including its publication and cursor replacement,
// inside one guard.
async fn serialized<T, R>(
    state: &StateArc<AsyncMutex<T>>,
    command: impl AsyncFnOnce(OwnedMutexGuard<T>) -> R,
) -> R {
    command(lock_owned(StateArc::clone(state)).await).await
}

/// Runs the entire serialized call under its admission deadline, including
/// preparation and publication after acquiring the checkout guard.
async fn serialized_until<T, R>(
    state: &StateArc<AsyncMutex<T>>,
    deadline_unix_ms: Option<u64>,
    clock: &dyn acyclic_stream::UnixMillisClock,
    command: impl AsyncFnOnce(OwnedMutexGuard<T>) -> Result<R>,
) -> Result<R> {
    let Some(deadline) = deadline_unix_ms else {
        return serialized(state, command).await;
    };
    let timeout = || {
        Error::process_io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timeout_ms elapsed during the checkout call; recovery: retry, or raise timeout_ms",
        ))
    };
    if clock.now_unix_millis() >= deadline {
        return Err(timeout());
    }
    let alarm = std::pin::pin!(acyclic_native_runtime::WallAlarm::wait(deadline).map_err(storage)?);
    let command = std::pin::pin!(serialized(state, command));
    match futures::future::select(alarm, command).await {
        futures::future::Either::Right((result, _)) => {
            // A producer may finish a synchronous native step in the same
            // poll in which the alarm fires. It cannot acknowledge late success.
            if clock.now_unix_millis() >= deadline {
                return Err(timeout());
            }
            result
        }
        futures::future::Either::Left((fired, _)) => {
            fired.map_err(storage)?;
            Err(timeout())
        }
    }
}

impl CheckoutTools {
    // Tool construction must not embed checkout recovery in every caller.
    pub(super) fn new<'a>(
        workspace: Arc<LocalWorkspace>,
        project: &'a VolumeRef,
        clock: Arc<dyn acyclic_stream::UnixMillisClock>,
        signed: &'a Scope,
        verifier: &'a AuthorityVerifier,
        execution_grant: Option<acyclic_native_runtime::execution::ExecutionGrant>,
    ) -> BoxProviderFuture<'a, Result<Self>> {
        Box::pin(async move {
            let expiry_unix_ms = verifier.expiry(signed)?.saturating_mul(1000);
            let environment =
                crate::native_environment::tool_environment(acyclic_native_runtime::platform_shell)
                    .map_err(Error::process_io)?;
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
            let execution_grant = match execution_grant {
                Some(grant) => Some(
                    acyclic_native_runtime::run_blocking_io(move || grant.compile_native(), None)
                        .map_err(Error::process_io)?
                        .offloaded()
                        .await
                        .map_err(Error::process_io)?
                        .map_err(Error::process_io)?,
                ),
                None => None,
            };
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
                    reconciliation::recover_pending(&workspace, view, pending, &|| async {
                        Ok(())
                    })
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
                verifier: verifier.clone(),
                capture_options,
                capture_policy,
                workspace,
                clock,
                expiry_unix_ms,
                environment,
                execution_grant,
                state: shared,
            })
        })
    }

    /// Publishes a freshly imported source's baseline before any command, so
    /// the source is readable from the first turn, not only after the first
    /// shell. Every later capture finds the baseline already recorded.
    pub(super) async fn publish_baseline(&self) -> Result<()> {
        if !(self.workspace.fresh && self.workspace.import_source) {
            return Ok(());
        }
        serialized(&self.state, async |mut state| {
            let State {
                active, watcher, ..
            } = &mut *state;
            let view = active.as_ref().unwrap_or_else(|| self.workspace.view());
            // A certified import commits under the baseline's own operation;
            // an uncertified one is an ordinary capture, retried by the next.
            self.capture(
                (&self.workspace, view),
                watcher,
                crate::OperationId::new(),
                0,
                PublicationPermit::Unrestricted,
            )?
            .await
        })
        .await
    }
}

struct PreparedCommand {
    admitted: std::time::Instant,
    deadline_unix_ms: Option<u64>,
    invocation: ToolInvocation,
    shell: Option<crate::tools::ShellInput>,
    fs: Option<crate::tools::FsCommand>,
    request: Vec<u8>,
    maximum: usize,
    started: Option<tokio::sync::oneshot::Sender<ToolResult>>,
}

struct ShellCommand<'a> {
    invocation: &'a ToolInvocation,
    context: &'a ToolContext,
    shell: Option<crate::tools::ShellInput>,
    request: Vec<u8>,
    maximum: usize,
    started: Option<tokio::sync::oneshot::Sender<ToolResult>>,
    admitted: std::time::Instant,
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
                admitted,
                deadline_unix_ms,
                invocation,
                shell,
                fs,
                request,
                maximum,
                started,
            } = command;
            let result = serialized_until(
                &self.state,
                deadline_unix_ms,
                self.clock.as_ref(),
                async |mut state| {
                    let workspace = self
                        .command_workspace(
                            &mut state,
                            context,
                            if invocation.name == "fs" {
                                "local fs facade"
                            } else {
                                "local shell"
                            },
                        )
                        .await?;
                    let view = state
                        .active
                        .as_ref()
                        .unwrap_or_else(|| self.workspace.view());
                    if reconciliation::recover(&workspace, view, &|| async { Ok(()) }).await? {
                        state.watcher = None;
                    }
                    workspace::volumes::sync(&workspace.mounts).await?;
                    // A child that handed back while this checkout was busy left
                    // its publication to the next command; it lands before this
                    // command observes the checkout.
                    self.commands()
                        .settle_publications(
                            &workspace::git::CommandContext {
                                owner: context.task(),
                                operation: context.operation_id(),
                                scope: context.effective_scope(),
                                request: None,
                            },
                            &mut state,
                        )
                        .await?;
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
                                started,
                                admitted,
                            },
                            state,
                            &workspace,
                        )?
                        .await
                    }
                },
            )
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
                started,
                admitted,
            } = command;
            let execution_grant = context
                .execution_grant()
                .or(self.execution_grant.as_ref())
                .cloned();
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
            let background = input.background;
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let selected = view
                .share(&mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                    work: &work,
                    cancellation: &cancellation,
                })
                .map_err(storage)?;
            let mut selected_watcher = if background {
                state.watcher.take()
            } else {
                None
            };
            let files = output::Output::open(context, request).await?;
            let mounts = Arc::clone(&workspace.mounts);
            let (result, mut state, mut files) =
                workspace::volumes::native_scope(&mounts, async |claims| {
                    let (result, mut state, files, claims) = self
                        .shell(
                            (
                                input,
                                invocation.operation_id,
                                execution_grant,
                                started,
                                admitted,
                            ),
                            maximum,
                            state,
                            files,
                            claims,
                            context,
                        )
                        .await?;
                    // Native preparation refusals precede process acquisition.
                    // Later launch/cleanup errors keep the unknown outcome.
                    let refused =
                        matches!(&result, Err(Error::Unauthorized(_) | Error::Unsupported(_)));
                    if !refused
                        && !result.as_ref().is_ok_and(|result| {
                            result.value.get("stop").is_none_or(|stop| {
                                stop.is_null() || stop["cleanup_completed"] == true
                            })
                        })
                    {
                        tracing::error!(
                            operation = %invocation.operation_id,
                            error = ?result.as_ref().err(),
                            cleanup_completed = ?result.as_ref().ok()
                                .and_then(|result| result.value.get("stop"))
                                .and_then(|stop| stop.get("cleanup_completed"))
                                .and_then(serde_json::Value::as_bool),
                            "shell outcome requires reconciliation"
                        );
                        state.unsettled.insert(invocation.operation_id);
                        return Err(Error::CommandIndeterminate {
                            operation_id: invocation.operation_id,
                            cause: crate::CommandIndeterminacy {
                                command: invocation.name.clone(),
                                reason: match result {
                                    Err(error) => crate::CommandIndeterminacyReason::Failed {
                                        code: error.kind().into(),
                                        error: error.to_string(),
                                    },
                                    Ok(_) => crate::CommandIndeterminacyReason::Unsettled,
                                },
                            },
                        });
                    }
                    Ok(((result, state, files), claims))
                })
                .await?;
            let State {
                active, watcher, ..
            } = &mut *state;
            let current = active.as_ref().unwrap_or_else(|| self.workspace.view());
            let view = if background { &*selected } else { current };
            let watcher = if background {
                &mut selected_watcher
            } else {
                watcher
            };
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
    fn authorize_execution_grant<'a>(
        &'a self,
        invocation: &'a ToolInvocation,
        _context: &'a ToolContext,
        grant: &'a acyclic_native_runtime::execution::ExecutionGrant,
        work: acyclic_fs::NativeWork<'a>,
    ) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            if invocation.name != crate::tools::shell::PLATFORM.name {
                return Err(Error::Unsupported(
                "checkout physical grants apply to shell execution; recovery: authorize file operations through their filesystem grants".into(),
            ));
            }
            if let Some(ceiling) = &self.execution_grant {
                let meter = work.map(acyclic_fs::NativeMaterializationWork::runtime_meter);
                ceiling
                    .check_attenuation_async(grant, meter)
                    .await
                    .map_err(Error::process_io)?;
            }
            Ok(())
        })
    }

    fn read_path<'a>(
        &'a self,
        path: &'a str,
        kind: crate::tool::ReadPathKind,
        work: acyclic_fs::NativeWork<'_>,
    ) -> Result<(&'a VolumeRef, std::borrow::Cow<'a, str>)> {
        let (mount, relative) = self
            .workspace
            .mounts
            .path_with_work(None, path, kind, work)?;
        mount.check_with_work(
            kind.action(),
            &relative,
            self.clock.now_unix_millis() / 1000,
            work,
        )?;
        Ok((&mount.view.volume, relative))
    }
    // A cancelled turn drops its call: the command's stop guard stops the
    // process tree, the spawned capture keeps the workspace guard until the
    // tree is reaped, and the next call reconciles from the durable capture.
    fn cancellable_on_drop(&self) -> bool {
        true
    }

    #[cfg(any(test, not(target_arch = "wasm32")))]
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
        work: acyclic_fs::NativeWork<'_>,
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
        {
            return Err(Error::Unauthorized(
                "checkout command identity differs from its task".into(),
            ));
        }
        if crate::tools::catalog()
            .find(|spec| spec.name == invocation.name)
            .is_none_or(|spec| !scope.grants().contains(&spec.capability()))
        {
            return Err(Error::Unauthorized(
                "checkout command requires its tool-call grant".into(),
            ));
        }
        Ok(())
    }

    fn execute<'a>(
        &'a self,
        mut invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        let admitted = std::time::Instant::now();
        let admitted_unix_ms = self.clock.now_unix_millis();
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            // Command access is checked once, under the command's own guard:
            // an earlier read-only check would only queue behind a writer.
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
            let maximum = context
                .effective_scope()
                .limits()
                .render_bytes
                .map_or(crate::tool::VIEW_BYTES, |maximum| {
                    maximum.min(crate::tool::VIEW_BYTES)
                });
            let maximum = usize::try_from(maximum).map_err(storage)?;
            let background = shell.as_ref().is_some_and(|input| input.background);
            // Preserve admission across preparation and background dispatch;
            // scheduling the worker cannot restart the caller's timeout.
            let deadline_unix_ms = shell
                .as_ref()
                .and_then(|input| input.timeout_ms)
                .filter(|timeout| *timeout != 0)
                .map(|timeout| admitted_unix_ms.saturating_add(u64::from(timeout)));
            let (started, launched) = tokio::sync::oneshot::channel();
            let command = PreparedCommand {
                admitted,
                deadline_unix_ms,
                invocation,
                shell,
                fs,
                request,
                maximum,
                started: background.then_some(started),
            };
            if !background {
                return self.run_checkout_command(command, context)?.await;
            }
            let tools = self.clone();
            let context = context.clone();
            let task = acyclic_native_runtime::TaskSpawner::current().spawn(async move {
                let result = tools.run_checkout_command(command, &context)?.await;
                if let Err(error) = &result {
                    let terminal = serde_json::to_vec(&serde_json::json!({"error": error.to_string(), "indeterminate": error.leaves_outcome_uncertain()})).map_err(storage)?;
                    if let Err(cause) = output::writer::stage(&context, "result.json", &terminal, "application/json").await {
                        tracing::error!(?cause, "background shell terminal publication failed");
                    }
                }
                result
            });
            tokio::pin!(task);
            tokio::select! {
                result = &mut task => result.map_err(storage)?,
                result = launched => {
                    match result {
                        Ok(result) => Ok(result),
                        Err(_) => task.await.map_err(storage)?,
                    }
                }
            }
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            let context = context
                .as_ref()
                .ok_or_else(|| Error::Unauthorized("checkout task is absent".into()))?;
            // A started receipt is immutable private data. Its own read grant
            // authorizes replay without queueing behind the running command.
            if invocation.name == crate::tools::shell::PLATFORM.name
                && invocation
                    .arguments
                    .get("background")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
                && let (Some(file), _) = context
                    .observe_staged_file_once(
                        output::writer::operation(context, "started.json"),
                        None,
                    )
                    .await?
            {
                let bytes = context.task().read_file(&file, None).await?;
                let mut value: serde_json::Value =
                    serde_json::from_slice(&bytes).map_err(storage)?;
                value["output_volume"] = serde_json::to_value(file.volume()).map_err(storage)?;
                return Ok(Some(ToolResult { value }));
            }
            // A dispatched process is never launched again. A reopened owner
            // fences every tree its dead predecessor left before admitting
            // work, and a live command holds this guard until its tree is
            // reaped. Once the guard is held, the call's process is gone
            // unless this process failed to reap it, and its outcome was never
            // recorded: it settles as interrupted.
            let mut state = lock_owned(StateArc::clone(&self.state)).await;
            self.command_workspace(
                &mut state,
                context,
                if invocation.name == "fs" {
                    "local fs facade"
                } else {
                    "local shell"
                },
            )
            .await?;
            if state.unsettled.contains(&invocation.operation_id) {
                return Ok(None);
            }
            Err(Error::InteractionRejected(
                crate::InteractionRejection::Cancelled,
            ))
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

#[cfg(all(test, not(loom)))]
mod deadline_tests {
    use super::*;

    fn timed_out(result: &Result<()>) -> bool {
        matches!(
            result,
            Err(Error::Process(crate::ProcessError::Timeout { .. }))
        )
    }

    /// A call queued behind a command that never releases the checkout gets
    /// its timeout_ms failure from the wall alarm, and never runs.
    #[tokio::test]
    async fn queued_call_fails_at_its_deadline_without_running() -> Result<()> {
        let state = StateArc::new(AsyncMutex::new(()));
        let _dispatcher = lock_owned(StateArc::clone(&state)).await;
        let clock = acyclic_stream::SystemUnixMillisClock;
        let now = acyclic_stream::UnixMillisClock::now_unix_millis(&clock);
        // Both a deadline that expired in the queue and one that expires
        // while queued: the alarm event, not a recheck, fails the call.
        for deadline in [now.saturating_sub(1), now.saturating_add(20)] {
            let ran = std::sync::atomic::AtomicBool::new(false);
            let result = serialized_until(&state, Some(deadline), &clock, async |_| {
                ran.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            })
            .await;
            assert!(
                timed_out(&result),
                "queued call did not time out: {result:?}"
            );
            assert!(!ran.load(std::sync::atomic::Ordering::SeqCst));
            assert!(
                acyclic_stream::UnixMillisClock::now_unix_millis(&clock) >= deadline,
                "deadline failure fired early"
            );
        }
        Ok(())
    }

    #[test]
    fn admitted_call_deadline_covers_preparation_waits_and_late_success() -> Result<()> {
        use acyclic_native_runtime::testing::{Schedule, seeded_runtime};
        let runtime = seeded_runtime(73).map_err(storage)?;
        let (schedule, _) = Schedule::seeded(73);
        runtime.block_on(schedule.scope(async {
            tokio::time::pause();
            let state = StateArc::new(AsyncMutex::new(()));
            let preparation = StateArc::new(AsyncMutex::new(()));
            let _owner = lock_owned(StateArc::clone(&preparation)).await;
            let clock = acyclic_stream::SystemUnixMillisClock;
            for late_success in [false, true] {
                let entered = std::sync::atomic::AtomicBool::new(false);
                let deadline = acyclic_stream::UnixMillisClock::now_unix_millis(&clock) + 20;
                let result = serialized_until(&state, Some(deadline), &clock, async |_| {
                    entered.store(true, std::sync::atomic::Ordering::SeqCst);
                    if late_success {
                        // A native producer can finish its current poll after
                        // the deadline, before the alarm waiter is polled again.
                        schedule.set_unix_millis(deadline);
                    } else {
                        let _preparation = lock_owned(StateArc::clone(&preparation)).await;
                    }
                    Ok(())
                })
                .await;
                assert!(entered.load(std::sync::atomic::Ordering::SeqCst));
                assert!(
                    timed_out(&result),
                    "admitted call did not time out: {result:?}"
                );
                assert!(StateArc::clone(&state).try_lock_owned().is_ok());
                assert!(acyclic_stream::UnixMillisClock::now_unix_millis(&clock) >= deadline);
            }
            Ok(())
        }))
    }
}
