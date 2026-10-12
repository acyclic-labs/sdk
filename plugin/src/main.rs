//! Unified Acyclic CLI, service, hook bridge, MCP bridge, and installer.

#![allow(
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    reason = "the CLI entry points dispatch every subcommand in one place"
)]

mod adapter_state;
mod cli_output;
mod codex_hook_mcp;
mod control_plane;
mod control_protocol;
mod control_transport;
mod git;
mod guidance;
mod install;
mod native_root_source;
mod obs;
mod pi;
mod pi_binding;
mod pi_builtin;
mod pi_caller;
mod pi_conversation;
mod pi_fork;
mod pi_fork_host;
mod pi_inspection;
mod pi_lazy_capture;
mod pi_mail;
mod pi_native;
mod pi_native_host;
mod pi_operator;
mod pi_operator_host;
mod pi_owner;
mod pi_recovery;
mod pi_registration;
mod pi_root;
mod pi_root_allocation;
mod pi_root_inputs;
mod pi_root_provider;
mod pi_runtime;
mod pi_selected_host;
mod pi_task;
mod pi_workflow;
mod roots;
mod service;
mod service_log;
mod service_process;
mod service_session;
#[cfg(test)]
mod tests;
mod tool_input;
mod util;

use acyclic_fs::demand::native::NativeDemandSource;
use acyclic_fs::demand::{
    DemandDirectoryObserver, DemandSource, FilteredDemandSource, SourceReference,
};
use acyclic_fs::kernel::{FileKind, NamespacePath};
use acyclic_fs::model::{
    CheckoutMode, FilesystemProfile, GenerationSelector, Lifecycle, VolumeConfig, VolumeLimits,
};
use acyclic_fs::path::PortablePath;
use acyclic_fs::{
    ApplyOptions, CancellationToken, CaptureOptions, CapturePolicy, CheckoutCommitOutcome,
    DefaultTextMergeDriver, Generation, GitCommand, GitCommandOutput, GitCompatRepository,
    GitDirtyState, GitFilesystemAction, GitFilesystemExecutor, GitFilesystemResult,
    GitGenerationRef, GitIgnorePolicy, GitPublicationRecord, GitTreeRef, IdempotencyKey,
    JoinOutcome, LazyMount, LazyWorkspace, LineageMultiRootPublicationAuthorizer,
    LocalAuthorityBackend, LocalCoreStateStore, LocalFs, LocalObjectBackend, LocalOptions,
    MaterializeOptions, MaterializingWorkspaceMultiRootPublisher,
    MaterializingWorkspaceMultiRootPublisherError, MemoryMergeResolutionCache, MergeConflict,
    MergeDriverRegistry, MountOptions, MultiRootMaterializer, MultiRootMergeCandidate,
    MultiRootMergePlan, MultiRootMergeRoot, MultiRootPublication, MultiRootPublicationCoordinator,
    MultiRootPublicationError, MultiRootPublicationPhase, NativeWatch, NativeWatchOptions,
    OperationId, OperationReconcileLimits, OperationWindowCoordinator, OperationWindowLease,
    Publication, PublicationPermit, TransactionCommit, WatchBatch, WatchChange, WorkBudget,
    Workspace, WorkspaceContextId, WorkspaceContextRegistry, WorkspaceContextRoot,
    WorkspaceContextState, WorkspaceDelete, WorkspaceError, WorkspaceGraph,
    WorkspaceMultiRootPublisherError, WorkspaceOperationFinish, WorkspacePathApply,
    WorkspaceRestore, WorkspaceRootId, apply_git_patch, blame_git_generations, capture_baseline,
    capture_git_compatible_generation, capture_git_compatible_generation_at,
    capture_git_compatible_generation_incremental, capture_watch_batch, git_compatible_diff_counts,
    grep_git_generation, resolve_merge_plan, walk_git_tree,
};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Read, Seek, Write};
use std::path::Component;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
#[cfg(any(test, not(target_os = "linux")))]
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex as AsyncMutex, watch};

use acyclic_native_runtime::{Durability, RenameMode, durable_rename, sync_file, sync_parent};
use control_protocol::{ControlEnvelope, ControlLedger, LedgerDecision};
use fs2::FileExt as _;
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicU64, Ordering};

use adapter_state::*;
use cli_output::*;
use control_plane::*;
use control_transport::*;
use git::*;
use guidance::*;
use install::*;
use pi::*;
use pi_binding::*;
use roots::*;
use service::*;
use service_process::*;
use service_session::SessionController;
use tool_input::*;
use util::*;

/// Native failures retain their SDK ownership through the final CLI write.
#[derive(Debug)]
enum CliFailure {
    Native(acyclic_harness::Error),
    NativeControl(ControlRequestError),
    NativeIo(io::Error),
    Ordinary(Box<dyn std::error::Error + Send + Sync>),
    ClosedFailed(service::SessionCloseState),
}

impl std::fmt::Display for CliFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Native(error) => std::fmt::Display::fmt(error, formatter),
            Self::NativeControl(error) => std::fmt::Display::fmt(error, formatter),
            Self::NativeIo(error) => std::fmt::Display::fmt(error, formatter),
            Self::Ordinary(error) => std::fmt::Display::fmt(error, formatter),
            Self::ClosedFailed(_) => formatter.write_str("service shutdown state is failed"),
        }
    }
}

impl From<acyclic_harness::Error> for CliFailure {
    fn from(error: acyclic_harness::Error) -> Self {
        Self::Native(error)
    }
}

impl From<ControlRequestError> for CliFailure {
    fn from(error: ControlRequestError) -> Self {
        Self::NativeControl(error)
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for CliFailure {
    fn from(error: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self::Ordinary(error)
    }
}

impl From<io::Error> for CliFailure {
    fn from(error: io::Error) -> Self {
        Self::Ordinary(error.into())
    }
}

impl From<serde_json::Error> for CliFailure {
    fn from(error: serde_json::Error) -> Self {
        Self::Ordinary(error.into())
    }
}

impl From<String> for CliFailure {
    fn from(error: String) -> Self {
        Self::Ordinary(error.into())
    }
}

/// Unified Acyclic CLI, local service, host hook bridges, and installer.
fn main() -> std::process::ExitCode {
    obs::init();
    let result = main_result();
    obs::finish();
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn main_result() -> Result<(), CliFailure> {
    let mut arguments = env::args().skip(1);
    match arguments.next().as_deref() {
        Some("__hook") => {
            return run_native_hook(&arguments.collect::<Vec<_>>()).map_err(CliFailure::Ordinary);
        }
        Some("__mcp") => return codex_hook_mcp::serve().map_err(Into::into),
        Some("__pi") => return run_pi_control(&arguments.collect::<Vec<_>>()),
        _ => {}
    }
    if is_foreground_cli_invocation() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(run())?;
        return Ok(());
    }
    let result = std::thread::Builder::new()
        .name("acyclic".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| -> Result<(), CliFailure> {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(32 * 1024 * 1024)
                .build()?
                .block_on(run())
        })?
        .join()
        .map_err(|_| io::Error::other("control-plane thread panicked"))?;
    result?;
    Ok(())
}

/// Answers a native hook delivered as a process: the event on standard input,
/// the answer on standard output. A hook that stays inside this process
/// returns before any runtime, path resolution or state directory is touched.
fn run_native_hook(arguments: &[String]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let [host, event] = arguments else {
        return Err(io::Error::other("acyclic __hook requires a host and event").into());
    };
    let response = match read_native_hook_call() {
        Ok((cwd, input)) => {
            let failure = HookFailure::classify(host, event, Some((&cwd, &input)));
            match local_native_hook_answer(host, event, &input) {
                Ok(Some(answer)) => answer,
                Ok(None) => tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(display)
                    .and_then(|runtime| {
                        runtime
                            .block_on(forward_native_hook(
                                host,
                                event,
                                cwd,
                                input,
                                ServiceStart::Allowed,
                                None,
                            ))
                            .map(|(value, _)| value)
                            .map_err(display)
                    })
                    .unwrap_or_else(|error| failure.answer(&error)),
                Err(error) => failure.answer(&error),
            }
        }
        Err(error) => HookFailure::classify(host, event, None).answer(&error),
    };
    serde_json::to_writer(io::stdout().lock(), &response)?;
    Ok(())
}

/// Pi controls use the same service transport, but errors are not native-hook
/// fail-open answers and uncertain requests are never automatically resent.
fn run_pi_control(arguments: &[String]) -> Result<(), CliFailure> {
    let [event] = arguments else {
        return Err(acyclic_harness::Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "acyclic __pi requires one operation",
        }
        .into());
    };
    let cancellation = CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        pi_root_provider::allocation_budget(),
        &cancellation,
    )
    .map_err(|(cause, work)| acyclic_harness::Error::NativeConstruction { cause, work })?;
    let mut input = service_process::read_native_control_input(io::stdin().lock(), &work)?;
    if !input.0.is_object() {
        return Err(acyclic_harness::Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "Pi control must be an object",
        }
        .into());
    }
    if let Some(environment) = work
        .environment_variable(
            std::ffi::OsStr::new(pi_caller::CALLER_ENVIRONMENT),
            MAXIMUM_CONTROL_MESSAGE_BYTES,
        )
        .map_err(native_cli_work_error)?
    {
        let text = environment
            .0
            .to_str()
            .ok_or(acyclic_harness::Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "Pi caller scope is not UTF-8",
            })?;
        let mut caller = service_process::decode_native_control_input(text.as_bytes(), &work)?;
        drop(environment);
        // Transfer the real same-ledger custody before moving its value into
        // the request. No caller-supplied scope overrides this protected input.
        input
            .1
            .absorb(&mut caller.1)
            .map_err(native_cli_work_error)?;
        let mut key = guarded_cli_text(&["caller"], &work)?;
        input.1.absorb(&mut key.1).map_err(native_cli_work_error)?;
        input
            .0
            .as_object_mut()
            .expect("object checked above")
            .insert(std::mem::take(&mut key.0), std::mem::take(&mut caller.0));
    }
    let mut cwd = work.current_directory().map_err(native_cli_work_error)?;
    input.1.absorb(&mut cwd.1).map_err(native_cli_work_error)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(CliFailure::NativeIo)?;
    let response = runtime.block_on(forward_native_hook(
        "pi",
        event,
        std::mem::take(&mut cwd.0),
        std::mem::take(&mut input.0),
        ServiceStart::Allowed,
        Some(&work),
    ))?;
    drop(cwd);
    drop(input);
    if response.1.is_none() {
        return Err(acyclic_harness::Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Storage,
            reason: "native Pi control response lost its allocation custody",
        }
        .into());
    }
    let encoded = service_process::encode_native_control_value(&response.0, &work)?;
    work.admit(acyclic_fs::WorkCounters::UNCHARGED)
        .map_err(native_cli_work_error)?;
    let mut stdout = io::stdout().lock();
    stdout.write_all(&encoded.0).map_err(CliFailure::NativeIo)?;
    stdout.flush().map_err(CliFailure::NativeIo)?;
    drop(encoded);
    drop(response);
    Ok(())
}

fn native_cli_work_error(
    cause: acyclic_fs::NativeTreeMaterializationError,
) -> acyclic_harness::Error {
    acyclic_harness::Error::NativeWork {
        cause,
        custody: None,
    }
}

/// The working directory and input of the native hook on standard input.
fn read_native_hook_call() -> Result<(PathBuf, Value), String> {
    let mut input = Vec::new();
    io::stdin()
        .take((MAXIMUM_CONTROL_MESSAGE_BYTES + 1) as u64)
        .read_to_end(&mut input)
        .map_err(display)?;
    if input.len() > MAXIMUM_CONTROL_MESSAGE_BYTES {
        return Err("native hook input exceeds the 4 MiB bound".to_owned());
    }
    let input = serde_json::from_slice(&input).map_err(display)?;
    Ok((env::current_dir().map_err(display)?, input))
}

/// The answer to a native hook that never needs the service, or `None` when
/// it must be forwarded.
fn local_native_hook_answer(
    host: &str,
    event: &str,
    input: &Value,
) -> Result<Option<Value>, String> {
    if !matches!(host, "codex" | "claude-code" | "copilot" | "cursor") {
        return Err("unsupported native hook host".to_owned());
    }
    Ok(native_hook_is_process_local_noop(host, event, input).then(|| json!({})))
}

/// Whether a hook transport may start or advance the service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServiceStart {
    /// A hook process owns no lasting resources, so a service it starts
    /// outlives it.
    Allowed,
    /// A long-lived host-contained process (Codex's MCP servers run in a
    /// Windows Job that kills every member when it closes and forbids
    /// breakaway) must never parent the service.
    Forbidden,
}

/// Sends one native hook to the service and returns its response. Every
/// hook transport answers through this function; they differ only in how the
/// event arrives and whether they may start the service.
#[tracing::instrument(
    target = "acyclic_plugin",
    name = "acyclic.plugin.hook",
    level = "info",
    skip_all,
    fields(host = obs::hook_host(host))
)]
async fn forward_native_hook(
    host: &str,
    event: &str,
    cwd: PathBuf,
    input: Value,
    service_start: ServiceStart,
    original_work: Option<&acyclic_fs::NativeMaterializationWork>,
) -> Result<(Value, Option<acyclic_fs::NativeAllocation>), ControlRequestError> {
    // The service canonicalizes whichever path a hook resolves to. This is
    // still the single hook startup/retransmission policy for every host.
    let data = default_data_directory();
    let envelope = match original_work {
        Some(work) => {
            let mut name =
                guarded_hook_name(host, event, work).map_err(ControlRequestError::NativeWork)?;
            let request = ControlRequest {
                version: 1,
                command: ControlCommand::Hook,
                cwd,
                argv: Vec::new(),
                name: std::mem::take(&mut name.0),
                arguments: input,
            };
            let mut envelope = ControlEnvelope::new_with_native_work(request, work)
                .map_err(ControlRequestError::NativeWork)?;
            envelope.1.absorb(&mut name.1).map_err(|cause| {
                ControlRequestError::NativeWork(acyclic_harness::Error::NativeWork {
                    cause,
                    custody: None,
                })
            })?;
            (envelope.0, Some(envelope.1))
        }
        None => (
            ControlEnvelope::new(native_hook_request(host, event, cwd, input)),
            None,
        ),
    };
    if service_start == ServiceStart::Allowed && matches!(event, "SessionStart" | "sessionStart") {
        // Session boundaries may advance an idle service, never a live mount.
        ensure_service(&data)
            .await
            .map_err(ControlRequestError::Unavailable)?;
        return send_hook_envelope(&data, &envelope.0, original_work, false).await;
    }
    match send_hook_envelope(&data, &envelope.0, original_work, true).await {
        Ok(response) => Ok(response),
        // Only unavailability proves the service did not receive the envelope.
        // Uncertain receipts are never automatically resent.
        Err(ControlRequestError::Unavailable(_)) if service_start == ServiceStart::Allowed => {
            ensure_service(&data)
                .await
                .map_err(ControlRequestError::Unavailable)?;
            send_hook_envelope(&data, &envelope.0, original_work, false).await
        }
        Err(error) => Err(error),
    }
}

async fn send_hook_envelope(
    data: &Path,
    envelope: &ControlEnvelope<ControlRequest>,
    work: Option<&acyclic_fs::NativeMaterializationWork>,
    once: bool,
) -> Result<(Value, Option<acyclic_fs::NativeAllocation>), ControlRequestError> {
    match (work, once) {
        (Some(work), true) => {
            service_process::send_control_envelope_once_with_native_work(data, envelope, work)
                .await
                .map(|(value, custody)| (value, Some(custody)))
        }
        (Some(work), false) => {
            service_process::send_control_envelope_with_native_work(data, envelope, work)
                .await
                .map(|(value, custody)| (value, Some(custody)))
        }
        (None, true) => send_control_envelope_once(data, envelope)
            .await
            .map(|value| (value, None)),
        (None, false) => send_control_envelope(data, envelope)
            .await
            .map(|value| (value, None)),
    }
}

fn guarded_hook_name(
    host: &str,
    event: &str,
    work: &acyclic_fs::NativeMaterializationWork,
) -> acyclic_harness::Result<(String, acyclic_fs::NativeAllocation)> {
    guarded_cli_text(&[host, ":", event], work)
}

fn guarded_cli_text(
    parts: &[&str],
    work: &acyclic_fs::NativeMaterializationWork,
) -> acyclic_harness::Result<(String, acyclic_fs::NativeAllocation)> {
    let bytes = parts
        .iter()
        .try_fold(0_usize, |size, part| size.checked_add(part.len()))
        .filter(|size| *size <= MAXIMUM_CONTROL_MESSAGE_BYTES)
        .ok_or(acyclic_harness::Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "native CLI metadata exceeds its byte bound",
        })?;
    let allocation = work.claim(
        bytes as u64,
        u64::from((bytes as u64) != 0),
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    let mut name = String::new();
    name.try_reserve_exact(bytes)
        .map_err(acyclic_fs::NativeTreeMaterializationError::AllocationFailed)?;
    work.charge(acyclic_fs::WorkCounters {
        bytes_copied: bytes as u64,
        ..acyclic_fs::WorkCounters::UNCHARGED
    })?;
    for part in parts {
        name.push_str(part);
    }
    Ok((name, allocation))
}

/// The service request for one native hook event.
fn native_hook_request(host: &str, event: &str, cwd: PathBuf, input: Value) -> ControlRequest {
    ControlRequest {
        version: 1,
        command: ControlCommand::Hook,
        cwd,
        argv: Vec::new(),
        name: format!("{host}:{event}"),
        arguments: input,
    }
}

/// What the host does with a hook that Acyclic could not answer, whatever
/// the failure: an unreachable service, a lost response or a refusal. This is
/// the one place that decides which hooks may fail open.
///
/// Only a tool hook gates anything, so every other hook lets the host
/// continue, visibly. The root agent works in its own physical roots, which
/// need no rewrite, so its tools continue too. A tool that may act for an
/// isolated subagent, or that would spawn one, is denied: without its rewrite
/// it would act on the shared roots, and a spawn without its prepared
/// workspace would start a subagent that has none.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HookFailurePolicy {
    Continue,
    Deny,
}

/// A hook's answer should Acyclic fail to give one; see [`HookFailurePolicy`].
#[derive(Clone, Copy, Debug)]
struct HookFailure<'a> {
    host: &'a str,
    event: &'a str,
    policy: HookFailurePolicy,
}

impl<'a> HookFailure<'a> {
    /// Classifies a hook from its call alone, before anything can fail. A
    /// call that could not be read is a tool of unknown caller.
    fn classify(host: &'a str, event: &'a str, call: Option<(&Path, &Value)>) -> Self {
        let tool_hook = matches!(event, "PreToolUse" | "preToolUse");
        let isolated = || {
            call.is_none_or(|(cwd, input)| {
                tool_may_act_for_subagent(host, cwd, input, &default_data_directory())
            })
        };
        let policy = if tool_hook && isolated() {
            HookFailurePolicy::Deny
        } else {
            HookFailurePolicy::Continue
        };
        Self {
            host,
            event,
            policy,
        }
    }

    fn answer(&self, error: &str) -> Value {
        let (decision, notice) = match self.policy {
            HookFailurePolicy::Continue => (
                "allow",
                format!(
                    "Acyclic is unavailable; this tool will run without an isolated workspace: {error}"
                ),
            ),
            HookFailurePolicy::Deny => (
                "deny",
                format!("Acyclic denied the tool because workspace isolation failed: {error}"),
            ),
        };
        if !matches!(self.event, "PreToolUse" | "preToolUse") {
            return json!({"systemMessage": notice});
        }
        if self.host == "copilot" {
            return json!({
                "permissionDecision": decision,
                "permissionDecisionReason": notice,
                "systemMessage": notice
            });
        }
        json!({
            "systemMessage": notice,
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": decision,
                "permissionDecisionReason": notice,
                "additionalContext": notice
            }
        })
    }
}

/// Whether a tool call may belong to an isolated subagent, judged from the
/// call alone because the service that knows cannot answer. A subagent's
/// tool names its agent (`agent_id`), a Codex MCP call names a thread other
/// than the session's own, and a subagent without either runs inside its
/// workspace mount, which lies under the state directory. A spawn prepares
/// a subagent's workspace, and a call that names no tool cannot be told
/// apart, so both count too.
fn tool_may_act_for_subagent(host: &str, cwd: &Path, input: &Value, data: &Path) -> bool {
    let Some(tool) = hook_optional_string(input, "tool_name", "toolName") else {
        return true;
    };
    let session = hook_optional_string(input, "session_id", "sessionId");
    let thread = hook_optional_string(input, "thread_id", "threadId");
    let cwd = hook_path(input, "cwd").map_or_else(|| cwd.to_path_buf(), |path| cwd.join(path));
    is_spawn_tool(&tool)
        || (host == "copilot" && tool == "task")
        || input.get("agent_id").is_some()
        || input.get("agentId").is_some()
        || thread.is_some_and(|thread| session.as_ref() != Some(&thread))
        || path_is_within(&cwd, data)
}

/// Whether `path` lies lexically within `root`. Windows compares without
/// case and without a verbatim prefix, as hosts spell one path either way.
fn path_is_within(path: &Path, root: &Path) -> bool {
    #[cfg(windows)]
    {
        let spell = |path: &Path| {
            let text = path.to_string_lossy().replace('/', "\\").to_lowercase();
            text.strip_prefix(r"\\?\")
                .map_or_else(|| text.clone(), str::to_owned)
        };
        Path::new(&spell(path)).starts_with(spell(root))
    }
    #[cfg(not(windows))]
    path.starts_with(root)
}

fn is_foreground_cli_invocation() -> bool {
    let mut arguments = env::args_os().skip(1);
    let mut command = arguments.next();
    if command.as_deref() == Some(std::ffi::OsStr::new("-C")) {
        let _ = arguments.next();
        command = arguments.next();
    }
    command.as_deref().is_some_and(|command| {
        matches!(
            command.to_str(),
            Some(
                "git"
                    | "agents"
                    | "doctor"
                    | "discard"
                    | "install"
                    | "uninstall"
                    | "pi"
                    | "__service-drain"
                    | "__installer-rename"
                    | "__verify-certification"
                    | "--help"
                    | "-h"
                    | "--version"
                    | "-V"
            )
        )
    })
}

async fn run() -> Result<(), CliFailure> {
    let mut arguments = env::args().skip(1).collect::<Vec<_>>();
    let invocation_cwd = env::current_dir()?.canonicalize()?;
    let mut cwd = invocation_cwd.clone();
    if arguments.first().is_some_and(|argument| argument == "-C") {
        if arguments.len() < 2 {
            return Err(io::Error::other("acyclic -C requires a path").into());
        }
        cwd = PathBuf::from(arguments.remove(1)).canonicalize()?;
        arguments.remove(0);
    }
    if arguments
        .first()
        .is_some_and(|argument| matches!(argument.as_str(), "--help" | "-h"))
    {
        println!(
            "Acyclic {}\n\nUsage: acyclic [COMMAND]\n\nCommands:\n  install HOST       Install host integration\n  uninstall HOST [--purge]\n                       Remove integration; preserve durable state unless purged\n  doctor [--json]    Diagnose the release installation\n  git ARGS...        Run Git compatibility commands\n  agents [--json]    List recursive agent workspace status\n  discard WORKSPACE  Discard a child workspace\n  pi approvals --session SESSION --operation TOOL_ID [--json]\n                       Inspect an original Root operator approval\n  pi approve --session SESSION --operation TOOL_ID --digest DIGEST\n             --decision approve|decline\n                       Resolve and publish the exact Root-approved selection\n  mcp                 Serve MCP over standard input/output",
            env!("CARGO_PKG_VERSION")
        );
        return Ok(());
    }
    if arguments
        .first()
        .is_some_and(|argument| matches!(argument.as_str(), "--version" | "-V"))
    {
        println!("acyclic {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "__installer-rename")
    {
        let [_, from, to, mode] = arguments.as_slice() else {
            return Err(io::Error::other(
                "acyclic __installer-rename requires FROM TO replace|no-replace",
            )
            .into());
        };
        let mode = match mode.as_str() {
            "replace" => RenameMode::Replace,
            "no-replace" => RenameMode::NoReplace,
            _ => return Err(io::Error::other("invalid installer rename mode").into()),
        };
        durable_rename(Path::new(from), Path::new(to), mode, None)?;
        return Ok(());
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "__verify-certification")
    {
        let [_, receipt] = arguments.as_slice() else {
            return Err(io::Error::other("acyclic __verify-certification requires RECEIPT").into());
        };
        let bytes = fs::read(receipt)?;
        if bytes.len() > 1024 * 1024 {
            return Err(io::Error::other("certification receipt exceeds 1 MiB").into());
        }
        let receipt: Value = serde_json::from_slice(&bytes)?;
        let digest = blake3_file(&current_executable().map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
        if !valid_platform_receipt(&receipt, &digest) {
            return Err(io::Error::other(
                "certification receipt does not match this Acyclic executable",
            )
            .into());
        }
        return Ok(());
    }
    if arguments.first().is_some_and(|command| command == "pi") {
        let response_custody;
        let response;
        (response, response_custody) = pi_operator::run_cli(
            &default_data_directory(),
            cwd,
            arguments.get(1..).unwrap_or_default(),
        )
        .await?;
        let encoded_custody;
        let encoded;
        (encoded, encoded_custody) =
            service_process::encode_native_control_value(&response, response_custody.work())?;
        encoded_custody
            .work()
            .admit(acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(native_cli_work_error)?;
        {
            let mut stdout = io::stdout().lock();
            stdout.write_all(&encoded).map_err(CliFailure::NativeIo)?;
            stdout.write_all(b"\n").map_err(CliFailure::NativeIo)?;
            stdout.flush().map_err(CliFailure::NativeIo)?;
        }
        drop(encoded);
        drop(encoded_custody);
        drop(response);
        drop(response_custody);
        return Ok(());
    }
    if let Some(command) = arguments.first().map(String::as_str)
        && matches!(command, "git" | "agents" | "doctor" | "discard")
    {
        let json_output = if matches!(command, "agents" | "doctor") {
            parse_read_only_arguments(arguments.get(1..).unwrap_or_default())?
        } else {
            arguments
                .get(1)
                .is_some_and(|argument| argument == "--json")
        };
        let data = default_data_directory();
        let request = ControlRequest {
            version: 1,
            command: match command {
                "git" => ControlCommand::Git,
                "agents" => ControlCommand::Agents,
                "doctor" => ControlCommand::Doctor,
                "discard" => ControlCommand::Discard,
                _ => unreachable!(),
            },
            cwd: invocation_cwd,
            argv: arguments.get(1..).unwrap_or_default().to_vec(),
            name: String::new(),
            arguments: cli_routing(cwd),
        };
        let response = send_cli_control_request(&data, &request)
            .await
            .map_err(io::Error::other)?;
        let exit_code = if json_output {
            println!("{}", serde_json::to_string_pretty(&response)?);
            if command == "doctor" && response.get("ok").and_then(Value::as_bool) != Some(true) {
                1
            } else {
                0
            }
        } else if command == "doctor" {
            print_doctor_response(&response).map_err(io::Error::other)?
        } else {
            print_cli_response(response).map_err(io::Error::other)?
        };
        if exit_code != 0 {
            obs::finish();
            std::process::exit(exit_code);
        }
        return Ok(());
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "install")
    {
        return install_command(arguments.get(1..).unwrap_or_default())
            .map_err(io::Error::other)
            .map_err(Into::into);
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "uninstall")
    {
        return uninstall_command(arguments.get(1..).unwrap_or_default())
            .await
            .map_err(io::Error::other)
            .map_err(Into::into);
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "__service-drain")
    {
        if arguments.len() > 2 {
            return Err(
                io::Error::other("usage: acyclic __service-drain [EXPECTED_IDENTITY]").into(),
            );
        }
        let fence = drain_service(
            &default_data_directory(),
            arguments.get(1).map(String::as_str),
        )
        .await
        .map_err(io::Error::other)?;
        drop(fence);
        return Ok(());
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "__service-status")
    {
        serde_json::to_writer(
            io::stdout().lock(),
            &service_status(&default_data_directory())
                .await
                .map_err(io::Error::other)?,
        )?;
        return Ok(());
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "__service")
    {
        return match run_service(default_data_directory()).await? {
            service::SessionCloseState::Closed => Ok(()),
            failed @ service::SessionCloseState::Failed => Err(CliFailure::ClosedFailed(failed)),
        };
    }
    let commandless = arguments
        .first()
        .is_some_and(|argument| argument == "__mcp-commandless");
    let data = default_data_directory();
    ensure_service(&data).await.map_err(io::Error::other)?;
    let stdin = io::stdin();
    let stdout = io::stdout();
    run_rpc_proxy(&data, stdin.lock(), stdout.lock(), commandless)
        .await
        .map_err(io::Error::other)?;
    Ok(())
}
