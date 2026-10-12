use super::super::*;
use acyclic_harness::{
    Admission, Error, OperationId, TaskId,
    conversation::{FileRef, VolumeRef},
    distributed::WorkLease,
    runtime::RuntimeTask,
    store::{HistoryReadLimits, HistoryReader},
};
use native::{json_line, storage};
use serde::Deserialize;
use std::{path::Path, sync::Arc};

const PAGE: HistoryReadLimits = HistoryReadLimits {
    maximum_events: terminal::MAX_PAGE_EVENTS as u32,
    maximum_bytes: terminal::MAX_FRAME_BYTES as u64,
};

/// Publishes a caller-held identity before the attempt it names.
fn emit(
    output: &mut impl std::io::Write,
    line: std::fmt::Arguments<'_>,
) -> acyclic_harness::Result<()> {
    writeln!(output, "{line}")
        .and_then(|()| output.flush())
        .map_err(storage)
}

fn queued<T>(
    admission: Admission<RuntimeTask<T>>,
    kind: &str,
    operation: OperationId,
    accepted: impl FnOnce(String) -> String,
) -> acyclic_harness::Result<String> {
    match admission {
        Admission::Accepted(task) => Ok(accepted(task.identity())),
        Admission::Rejected { reason } => Err(Error::Conflict(format!(
            "{kind} rejected operation={operation}: {reason}"
        ))),
        Admission::Indeterminate { operation_id } => Err(Error::Indeterminate(operation_id)),
    }
}

fn utf8(bytes: &[u8]) -> acyclic_harness::Result<&str> {
    std::str::from_utf8(bytes).map_err(|_| Error::Unsupported("file is not UTF-8 text".into()))
}

fn spawn_bound(
    workers: &tokio::task::JoinSet<acyclic_harness::Result<String>>,
) -> acyclic_harness::Result<()> {
    if workers.len() >= terminal::MAX_PAGE_EVENTS {
        return Err(Error::Invalid(
            "active frontend worker bound reached".into(),
        ));
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Caller {
    caller: TaskId,
    lease: WorkLease,
}

pub(super) async fn execute(
    command: &str,
    backend: &mut Option<Backend>,
    parent: &mut Option<TaskId>,
    workers: &mut tokio::task::JoinSet<acyclic_harness::Result<String>>,
    output: &mut impl std::io::Write,
) -> acyclic_harness::Result<String> {
    let api = backend.as_ref().ok_or_else(|| {
        Error::Conflict(
            "session is closed after failed reopen; restart with the explicit config".into(),
        )
    })?;
    if command == "/reopen" {
        if !workers.is_empty() {
            return Err(Error::Conflict("active worker handles must settle or the process must close before native reopen; no implicit cancellation".into()));
        }
        let config = api.native()?.config();
        drop(backend.take());
        *backend = Some(Backend::Native(Arc::new(
            native::NativeSession::open(config).await?,
        )));
        return Ok("native providers reopened; no admission or effect resent\n".into());
    }
    let native = api.native()?;
    let (name, argument) = command.split_once(' ').unwrap_or(("", command));
    let path = Path::new(argument);
    match name {
        #[cfg(feature = "native-execution")]
        "/command" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct NativeCommand {
                caller: TaskId,
                lease: WorkLease,
                operation: OperationId,
                decision: Option<bool>,
            }
            let request: NativeCommand = read_json(path)?;
            native
                .command(
                    request.caller,
                    &request.lease,
                    request.operation,
                    request.decision,
                )
                .await
        }
        "/context" => native.configure_context(read_json(path)?).await,
        "/fork-selection" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Selection {
                caller: TaskId,
                lease: WorkLease,
                operation: OperationId,
                history: acyclic_harness::fork::ForkHistoryPolicy,
            }
            let request: Selection = read_json(path)?;
            native
                .select_fork(
                    request.caller,
                    &request.lease,
                    request.operation,
                    request.history,
                )
                .await
        }
        "/parent" => {
            *parent = if argument == "root" {
                None
            } else {
                let task = TaskId::parse(argument)?;
                native.outcome(task).await?;
                Some(task)
            };
            Ok(format!("selected parent-task={parent:?}\n"))
        }
        "/run" | "/claim" => {
            spawn_bound(workers)?;
            let task = TaskId::parse(argument)?;
            let worker = acyclic_harness::distributed::Worker {
                id: format!("graphcoder-{}", uuid::Uuid::new_v4()),
                available: Default::default(),
                labels: Default::default(),
            };
            let native = Arc::clone(native);
            match native.claim(task, &worker).await? {
                acyclic_harness::distributed::WorkPull::Idle => {
                    Ok(format!("worker idle task={task}\n"))
                }
                acyclic_harness::distributed::WorkPull::Unresolved { lease, error } => {
                    native::render_attempt(
                        acyclic_harness::filesystem::TaskWorkerAttempt::Unresolved { lease, error },
                    )
                }
                acyclic_harness::distributed::WorkPull::Claimed(lease) => {
                    emit(
                        output,
                        format_args!(
                            "worker-claim task={task} lease-json-hex={}",
                            native::lease_hex(&lease)?
                        ),
                    )?;
                    if name == "/claim" {
                        native.runtime.task_host().start_task(&lease, None).await?;
                        return Ok(format!(
                            "worker retained task={task}; no command dispatched\n"
                        ));
                    }
                    let _ = workers.spawn(async move { native.resume(lease).await });
                    Ok(format!(
                        "worker started task={task}; input and cancellation remain available\n"
                    ))
                }
            }
        }
        "/cancel-task" => native.cancel_task(TaskId::parse(argument)?).await,
        "/outcome" => native.outcome(TaskId::parse(argument)?).await,
        "/reconcile" => native.reconcile(OperationId::parse(argument)?).await,
        "/resume" => {
            spawn_bound(workers)?;
            let lease = read_json(path)?;
            let native = Arc::clone(native);
            let _ = workers.spawn(async move { native.resume(lease).await });
            Ok(
                "original lease submitted for authoritative reconciliation; input remains available\n"
                    .into(),
            )
        }
        "/children" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Children {
                task: TaskId,
                revision: Option<u64>,
                after: Option<String>,
            }
            let request: Children = read_json(path)?;
            native
                .children(request.task, request.revision, request.after)
                .await
        }
        "/prepare" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Preparation {
                operation: OperationId,
                parent: TaskId,
                caller_lease: WorkLease,
                request: acyclic_harness::resource_preparation::ResourcePreparationRequest,
            }
            let request: Preparation = read_json(path)?;
            let fork: acyclic_harness::fork::ForkRequest =
                serde_json::from_value(request.request.payload.clone()).map_err(|error| {
                    Error::Invalid(format!("invalid exact fork request: {error}"))
                })?;
            fork.validate(&mut None)?;
            emit(
                output,
                format_args!(
                    "preparation-attempt operation={} caller-task={} new-child-agent={} child-admission-operation={} fork-operation={}",
                    request.operation,
                    request.parent,
                    fork.child_agent,
                    request.request.child.operation_id,
                    fork.operation_id
                ),
            )?;
            let admission = native
                .queue_preparation(
                    request.operation,
                    request.parent,
                    &request.caller_lease,
                    request.request,
                )
                .await?;
            queued(admission, "preparation", request.operation, |task| {
                format!(
                    "queued preparation-task={task} admission-operation={} caller-task={} new-child-agent={}\n",
                    request.operation, request.parent, fork.child_agent
                )
            })
        }
        "/publish-fork" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Publish {
                caller: TaskId,
                lease: WorkLease,
                preparation: TaskId,
            }
            let request: Publish = read_json(path)?;
            native
                .publish_preparation(request.caller, &request.lease, request.preparation)
                .await
        }
        "/inspect" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Inspect {
                caller: TaskId,
                lease: WorkLease,
                source: TaskId,
            }
            let request: Inspect = read_json(path)?;
            native
                .caller_context(request.caller, &request.lease)
                .await?;
            let source = native
                .runtime
                .inspect_source(request.caller, request.source)
                .await?;
            json_line(
                "inspection",
                &serde_json::json!({
                    "caller": request.caller, "source_task": request.source,
                    "agent": source.agent()?, "source": source.source(),
                    "volumes": source.volumes(), "project": source.project()?,
                    "private_volume": source.private_volume()?, "generation_semantics": "original_allocation",
                }),
            )
        }
        "/history" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct History {
                caller: TaskId,
                lease: WorkLease,
                cursor: Option<acyclic_harness::store::HistoryCursor>,
            }
            let request: History = read_json(path)?;
            native
                .caller_context(request.caller, &request.lease)
                .await?;
            let binding = native.caller_binding(request.caller).await?;
            let verifier = binding.verifier()?;
            let reader =
                HistoryReader::new(&native.stream, &binding.authority, &verifier, &mut None)?;
            let cursor = match request.cursor {
                Some(cursor) if cursor.authority == binding.authority => cursor,
                Some(_) => {
                    return Err(Error::Unauthorized(
                        "history cursor belongs to a different caller conversation".into(),
                    ));
                }
                None => reader.pin(0, None).await?,
            };
            json_line("history", &reader.read_page(&cursor, PAGE, None).await?)
        }
        "/inherited-history" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Inherited {
                caller: TaskId,
                lease: WorkLease,
                seed: acyclic_harness::fork::ForkSeed,
                after_sequence: u64,
            }
            let request: Inherited = read_json(path)?;
            native
                .caller_context(request.caller, &request.lease)
                .await?;
            let child = native
                .runtime
                .inspect_source(request.caller, request.caller)
                .await?;
            let child_binding = native.caller_binding(request.caller).await?;
            if child.agent()? != request.seed.child_agent
                || child_binding.authority != request.seed.child
            {
                return Err(Error::Unauthorized(
                    "inherited history does not belong to this admitted caller".into(),
                ));
            }
            let parent = native
                .config
                .observed_authorities
                .iter()
                .chain([&native.config.owner])
                .find(|binding| binding.authority == request.seed.parent)
                .ok_or_else(|| {
                    Error::Unsupported("original parent history verifier unavailable".into())
                })?;
            let verifier = parent.verifier()?;
            let reader =
                HistoryReader::new(&native.stream, &parent.authority, &verifier, &mut None)?;
            let messages = reader
                .inherited_conversation_page(
                    &request.seed,
                    child.agent()?,
                    request.after_sequence,
                    PAGE,
                )
                .await?;
            json_line(
                "inherited-history",
                &serde_json::json!({
                    "next_after": messages.last().map(|message| message.sequence), "messages": messages,
                }),
            )
        }
        "/checkpoint" => {
            let request: Caller = read_json(path)?;
            native
                .caller_context(request.caller, &request.lease)
                .await?;
            let session = native
                .runtime
                .open_task(
                    request.caller,
                    acyclic_harness::scheduler::LeaseFence::from(&request.lease.reservation),
                )
                .await?;
            json_line(
                "checkpoint",
                &serde_json::json!({
                    "task": request.caller, "checkpoint": session.checkpoint(),
                    "transition": session.latest_transition().await?,
                }),
            )
        }
        "/read-path" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct ReadPath {
                caller: TaskId,
                lease: WorkLease,
                volume: VolumeRef,
                path: String,
                #[serde(default)]
                generation: Option<acyclic_harness::resources::GenerationRef>,
            }
            let request: ReadPath = read_json(path)?;
            let context = native
                .caller_context(request.caller, &request.lease)
                .await?;
            let full = request.volume.capability(
                acyclic_harness::conversation::VolumeOperation::Read,
                &mut None,
            )?;
            let directory = request.volume.directory_read_capability("", &mut None)?;
            if !context.scope().grants().contains(&full)
                && !context.scope().grants().contains(&directory)
            {
                return Err(Error::Unauthorized(
                    "caller cannot discover this current volume".into(),
                ));
            }
            let binding = native.caller_binding(request.caller).await?;
            let verifier = binding.verifier()?;
            let grant = acyclic_harness::conversation::ContentGrant::verify_directory_read(
                &verifier,
                &binding.scope,
                &request.volume,
                "",
                &mut None,
            )?;
            let (file, bytes) = native
                .files
                .read_private_path(
                    &request.volume,
                    &grant,
                    &request.path,
                    request.generation.as_ref(),
                    terminal::MAX_FRAME_BYTES as u64,
                )
                .await?;
            json_line(
                "file",
                &serde_json::json!({"file": file, "text": utf8(&bytes)?}),
            )
        }
        "/publication-open"
        | "/publication-observe"
        | "/publication-apply"
        | "/publication-reconcile" => {
            #[cfg(feature = "native-execution")]
            {
                let request: native_writeback::NativePublicationRequest = read_json(path)?;
                let result = match name {
                    "/publication-open" => native.publication_open(&request).await?,
                    "/publication-observe" => native.publication_observe(&request).await?,
                    "/publication-apply" => native.publication_apply(&request).await?,
                    _ => native.publication_reconcile(&request).await?,
                };
                return json_line("publication", &result);
            }
            #[cfg(not(feature = "native-execution"))]
            {
                return Err(Error::Unsupported(
                    "native root publication requires native-execution".into(),
                ));
            }
        }
        "/mail" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Mail {
                caller: TaskId,
                lease: WorkLease,
                recipient: TaskId,
                operation: OperationId,
                payload: FileRef,
            }
            let request: Mail = read_json(path)?;
            let receipt = native
                .caller_context(request.caller, &request.lease)
                .await?
                .send(request.recipient, request.operation, request.payload)
                .await?;
            json_line("mail", &receipt)
        }
        "/inbox" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Inbox {
                caller: TaskId,
                lease: WorkLease,
                after: u64,
            }
            let request: Inbox = read_json(path)?;
            let items = native
                .caller_context(request.caller, &request.lease)
                .await?
                .inbox(request.after, terminal::MAX_PAGE_EVENTS)
                .await?;
            json_line(
                "inbox",
                &serde_json::json!({
                    "task": request.caller, "after": request.after,
                    "next_after": items.last().map(|item| item.sequence), "items": items,
                    "consumption": "observation_only",
                }),
            )
        }
        "/queue-mailbox" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Mailbox {
                operation: OperationId,
                parent: TaskId,
                lease: WorkLease,
                input: serde_json::Value,
            }
            let request: Mailbox = read_json(path)?;
            emit(
                output,
                format_args!(
                    "mailbox-admission-attempt operation={} caller-task={}",
                    request.operation, request.parent
                ),
            )?;
            let admission = native
                .queue_mailbox(
                    request.operation,
                    request.parent,
                    &request.lease,
                    request.input,
                )
                .await?;
            queued(admission, "mailbox admission", request.operation, |task| {
                format!(
                    "queued task={task} admission-operation={}\n",
                    request.operation
                )
            })
        }
        "/write" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Write {
                caller: TaskId,
                lease: WorkLease,
                volume: VolumeRef,
                operation: OperationId,
                path: String,
                bytes: String,
            }
            let request: Write = read_json(path)?;
            let file = native
                .caller_context(request.caller, &request.lease)
                .await?
                .stage_file_in(
                    &request.volume,
                    request.operation,
                    &request.path,
                    request.bytes.as_bytes(),
                    "text/plain",
                    &request.path,
                )
                .await?;
            json_line("file", &serde_json::json!({"file": file}))
        }
        "/read" | "/file" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Read {
                caller: TaskId,
                lease: WorkLease,
                file: FileRef,
            }
            let request: Read = read_json(path)?;
            let bytes = native
                .caller_context(request.caller, &request.lease)
                .await?
                .read_file(&request.file)
                .await?;
            let text = utf8(&bytes)?;
            let line = json_line(
                "file",
                &serde_json::json!({"file": request.file, "text": text}),
            )?;
            if name == "/file" {
                return Ok(format!(
                    "file {}\n{}\n{line}",
                    terminal::text(request.file.path()),
                    terminal::text(text),
                ));
            }
            Ok(line)
        }
        "/diff" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Diff {
                caller: TaskId,
                lease: WorkLease,
                before: FileRef,
                after: FileRef,
            }
            let diff: Diff = read_json(path)?;
            let context = native.caller_context(diff.caller, &diff.lease).await?;
            let before = context.read_file(&diff.before).await?;
            let after = context.read_file(&diff.after).await?;
            Ok(format!(
                "diff exact before={} after={}\n--- before\n{}\n+++ after\n{}\n",
                terminal::text(diff.before.path()),
                terminal::text(diff.after.path()),
                terminal::text(utf8(&before)?),
                terminal::text(utf8(&after)?)
            ))
        }
        _ if !command.starts_with('/') || name == "/queue" => {
            let operation = OperationId::from_bytes(*uuid::Uuid::new_v4().as_bytes());
            let command = if name == "/queue" {
                read_json(path)?
            } else {
                let command_operation = OperationId::from_bytes(*uuid::Uuid::new_v4().as_bytes());
                emit(
                    output,
                    format_args!(
                        "prompt-publication-attempt command-operation={command_operation}"
                    ),
                )?;
                native.prompt_command(command, command_operation).await?
            };
            emit(
                output,
                format_args!(
                    "admission-attempt operation={operation} command-operation={}",
                    command.operation_id
                ),
            )?;
            let admission = native.queue(operation, command, *parent).await?;
            queued(admission, "admission", operation, |task| {
                format!("queued task={task} admission-operation={operation}\n")
            })
        }
        _ => Err(Error::Unsupported(
            "operation requires a durable product host; fixture input is not queued".into(),
        )),
    }
}
