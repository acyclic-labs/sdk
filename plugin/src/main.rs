//! Unified Acyclic CLI, service, hook bridge, MCP bridge, and installer.

#![allow(clippy::cognitive_complexity, clippy::too_many_lines)]

mod adapter_state;
mod cli_output;
mod codex_hook_mcp;
mod control_plane;
mod control_protocol;
mod control_transport;
mod git;
mod guidance;
mod install;
mod roots;
mod service;
mod service_process;
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
use acyclic_fs::native_host::HostRoot;
use acyclic_fs::path::PortablePath;
use acyclic_fs::{
    ApplyOptions, CancellationToken, CaptureOptions, CapturePolicy, CheckoutCommitOutcome,
    DefaultTextMergeDriver, DistributedFs, Generation, GitCommand, GitCommandOutput,
    GitCompatRepository, GitDirtyState, GitFilesystemAction, GitFilesystemExecutor,
    GitFilesystemResult, GitGenerationRef, GitIgnorePolicy, GitPublicationRecord, GitTreeRef,
    IdempotencyKey, JoinOutcome, LazyMount, LazyWorkspace, LineageMultiRootPublicationAuthorizer,
    LocalAuthorityBackend, LocalCoreStateStore, LocalFs, LocalObjectBackend, LocalOptions,
    MaterializeOptions, MaterializingWorkspaceMultiRootPublisher,
    MaterializingWorkspaceMultiRootPublisherError, MemoryMergeResolutionCache, MergeConflict,
    MergeDriverRegistry, MountOptions, MultiRootMaterializer, MultiRootMergeCandidate,
    MultiRootMergePlan, MultiRootMergeRoot, MultiRootPublication, MultiRootPublicationCoordinator,
    MultiRootPublicationError, MultiRootPublicationPhase, NativeWatch, NativeWatchOptions,
    OperationId, OperationReconcileLimits, OperationWindowLease, Publication, PublicationPermit,
    TransactionCommit, WatchBatch, WatchChange, WatchEpoch, WatchSequence, WorkBudget, Workspace,
    WorkspaceContextId, WorkspaceContextRoot, WorkspaceContextState, WorkspaceDelete,
    WorkspaceError, WorkspaceMultiRootPublisherError, WorkspaceOperationFinish, WorkspacePathApply,
    WorkspaceRestore, WorkspaceRootId, apply_git_patch_with_permit_if_current,
    blame_git_generations, capture_baseline_with_policy, capture_git_compatible_generation,
    capture_git_compatible_generation_at, capture_git_compatible_generation_incremental,
    capture_watch_batch_with_policy, git_compatible_diff_counts, grep_git_generation,
    resolve_merge_plan, walk_git_tree,
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
use roots::*;
use service::*;
use service_process::*;
use tool_input::*;
use util::*;

/// Unified Acyclic CLI, local service, host hook bridges, and installer.
fn main() {
    if let Err(error) = main_result() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn main_result() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut arguments = env::args().skip(1);
    match arguments.next().as_deref() {
        Some("__hook") => return run_native_hook(&arguments.collect::<Vec<_>>()),
        Some("__mcp") => return codex_hook_mcp::serve().map_err(Into::into),
        _ => {}
    }
    if is_foreground_cli_invocation() {
        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(display)?
            .block_on(run())
            .map_err(display);
        result.map_err(io::Error::other)?;
        return Ok(());
    }
    let result = std::thread::Builder::new()
        .name("acyclic".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(32 * 1024 * 1024)
                .build()
                .map_err(display)?
                .block_on(run())
                .map_err(display)
        })?
        .join()
        .map_err(|_| io::Error::other("control-plane thread panicked"))?;
    result.map_err(io::Error::other)?;
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
                        runtime.block_on(forward_native_hook(
                            host,
                            event,
                            cwd,
                            input,
                            ServiceStart::Allowed,
                        ))
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
async fn forward_native_hook(
    host: &str,
    event: &str,
    cwd: PathBuf,
    input: Value,
    service_start: ServiceStart,
) -> Result<Value, String> {
    // The service canonicalizes whichever path a hook resolves to.
    let data = default_data_directory();
    let envelope = ControlEnvelope::new(native_hook_request(host, event, cwd, input));
    if service_start == ServiceStart::Allowed && matches!(event, "SessionStart" | "sessionStart") {
        // Session boundaries are the one cheap, deterministic place to advance
        // an idle service to the installed binary. Tool hooks stay on the direct
        // single-round-trip path, and a service with live mounts remains intact.
        ensure_service(&data).await?;
        return send_control_envelope(&data, &envelope)
            .await
            .map_err(|error| error.to_string());
    }
    match send_control_envelope_once(&data, &envelope).await {
        Ok(response) => Ok(response),
        // An unavailable service never received the envelope, so this is the
        // only retransmission, and no deadline applies to it.
        Err(ControlRequestError::Unavailable(_)) if service_start == ServiceStart::Allowed => {
            ensure_service(&data).await?;
            send_control_envelope(&data, &envelope)
                .await
                .map_err(|error| error.to_string())
        }
        Err(error) => Err(error.to_string()),
    }
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

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
            "Acyclic {}\n\nUsage: acyclic [COMMAND]\n\nCommands:\n  install HOST       Install host integration\n  uninstall HOST [--purge]\n                       Remove integration; preserve durable state unless purged\n  doctor [--json]    Diagnose the release installation\n  git ARGS...        Run Git compatibility commands\n  agents [--json]    List recursive agent workspace status\n  discard WORKSPACE  Discard a child workspace\n  mcp                 Serve MCP over standard input/output",
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
        durable_rename(Path::new(from), Path::new(to), mode)?;
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
        return run_service(default_data_directory())
            .await
            .map_err(io::Error::other)
            .map_err(Into::into);
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
