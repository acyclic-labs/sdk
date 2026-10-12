use super::super::*;
use acyclic_harness::{
    Admission, Error, OperationId, TaskId,
    conversation::{FileRef, VolumeRef},
    distributed::WorkLease,
};
use std::{path::Path, sync::Arc};
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
    match (name, argument) {
        #[cfg(feature = "native-execution")]
        ("/command", path) => {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct NativeCommand {
                caller: TaskId,
                lease: WorkLease,
                operation: OperationId,
                decision: Option<bool>,
            }
            let request: NativeCommand = read_json(Path::new(path))?;
            return native
                .command(
                    request.caller,
                    &request.lease,
                    request.operation,
                    request.decision,
                )
                .await;
        }
        ("/context", path) => {
            return native.configure_context(read_json(Path::new(path))?).await;
        }
        ("/fork-selection", path) => {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Selection {
                caller: TaskId,
                lease: WorkLease,
                operation: OperationId,
                history: acyclic_harness::fork::ForkHistoryPolicy,
            }
            let request: Selection = read_json(Path::new(path))?;
            return native
                .select_fork(
                    request.caller,
                    &request.lease,
                    request.operation,
                    request.history,
                )
                .await;
        }
        ("/parent", task) => {
            *parent = if task == "root" {
                None
            } else {
                let task = TaskId::parse(task)?;
                native.outcome(task).await?;
                Some(task)
            };
            return Ok(format!("selected parent-task={parent:?}\n"));
        }
        ("/run" | "/claim", task) => {
            if workers.len() >= terminal::MAX_PAGE_EVENTS {
                return Err(Error::Invalid(
                    "active frontend worker bound reached".into(),
                ));
            }
            let task = TaskId::parse(task)?;
            let worker = acyclic_harness::distributed::Worker {
                id: format!("graphcoder-{}", uuid::Uuid::new_v4()),
                available: Default::default(),
                labels: Default::default(),
            };
            let native = Arc::clone(native);
            match native.claim(task, &worker).await? {
                acyclic_harness::distributed::WorkPull::Idle => {
                    return Ok(format!("worker idle task={task}\n"));
                }
                acyclic_harness::distributed::WorkPull::Unresolved { lease, error } => {
                    return native::render_attempt(
                        acyclic_harness::filesystem::TaskWorkerAttempt::Unresolved { lease, error },
                    );
                }
                acyclic_harness::distributed::WorkPull::Claimed(lease) => {
                    writeln!(
                        output,
                        "worker-claim task={task} lease-json-hex={}",
                        native::lease_hex(&lease)?
                    )
                    .and_then(|()| output.flush())
                    .map_err(|error| Error::Storage(error.to_string()))?;
                    if name == "/claim" {
                        native.runtime.task_host().start_task(&lease, None).await?;
                        return Ok(format!(
                            "worker retained task={task}; no command dispatched\n"
                        ));
                    }
                    let _ = workers.spawn(async move { native.resume(lease).await });
                    return Ok(format!(
                        "worker started task={task}; input and cancellation remain available\n"
                    ));
                }
            }
        }
        ("/cancel-task", task) => {
            return native.cancel_task(TaskId::parse(task)?).await;
        }
        ("/outcome", task) => {
            return native.outcome(TaskId::parse(task)?).await;
        }
        ("/reconcile", operation) => {
            return native.reconcile(OperationId::parse(operation)?).await;
        }
        ("/resume", path) => {
            if workers.len() >= terminal::MAX_PAGE_EVENTS {
                return Err(Error::Invalid(
                    "active frontend worker bound reached".into(),
                ));
            }
            let lease = read_json(Path::new(path))?;
            let native = Arc::clone(native);
            let _ = workers.spawn(async move { native.resume(lease).await });
            return Ok(
            "original lease submitted for authoritative reconciliation; input remains available\n"
                .into(),
        );
        }
        ("/children", path) => {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Children {
                task: TaskId,
                revision: Option<u64>,
                after: Option<String>,
            }
            let request: Children = read_json(Path::new(path))?;
            return native
                .children(request.task, request.revision, request.after)
                .await;
        }
        ("/prepare", path) => {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Preparation {
                operation: OperationId,
                parent: TaskId,
                caller_lease: WorkLease,
                request: acyclic_harness::resource_preparation::ResourcePreparationRequest,
            }
            let request: Preparation = read_json(Path::new(path))?;
            let fork: acyclic_harness::fork::ForkRequest =
                serde_json::from_value(request.request.payload.clone()).map_err(|error| {
                    Error::Invalid(format!("invalid exact fork request: {error}"))
                })?;
            fork.validate(&mut None)?;
            writeln!(output, "preparation-attempt operation={} caller-task={} new-child-agent={} child-admission-operation={} fork-operation={}",
                request.operation, request.parent, fork.child_agent,
                request.request.child.operation_id, fork.operation_id)
                .and_then(|()| output.flush()).map_err(|error| Error::Storage(error.to_string()))?;
            return match native
                .queue_preparation(
                    request.operation,
                    request.parent,
                    &request.caller_lease,
                    request.request,
                )
                .await?
            {
                Admission::Accepted(value) => Ok(format!(
                    "queued preparation-task={} admission-operation={} caller-task={} new-child-agent={}\n",
                    value.identity(),
                    request.operation,
                    request.parent,
                    fork.child_agent
                )),
                Admission::Rejected { reason } => Err(Error::Conflict(format!(
                    "preparation rejected operation={}: {reason}",
                    request.operation
                ))),
                Admission::Indeterminate { operation_id } => {
                    Err(Error::Indeterminate(operation_id))
                }
            };
        }
        ("/publish-fork", path) => {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Publish {
                caller: TaskId,
                lease: WorkLease,
                preparation: TaskId,
            }
            let request: Publish = read_json(Path::new(path))?;
            return native
                .publish_preparation(request.caller, &request.lease, request.preparation)
                .await;
        }
        ("/inspect", path) => {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Inspect {
                caller: TaskId,
                lease: WorkLease,
                source: TaskId,
            }
            let request: Inspect = read_json(Path::new(path))?;
            native
                .caller_context(request.caller, &request.lease)
                .await?;
            let source = native
                .runtime
                .inspect_source(request.caller, request.source)
                .await?;
            return Ok(format!(
                "inspection-json={}\n",
                serde_json::json!({
                    "caller": request.caller, "source_task": request.source,
                    "agent": source.agent()?, "source": source.source(),
                    "volumes": source.volumes(), "project": source.project()?,
                    "private_volume": source.private_volume()?, "generation_semantics": "original_allocation",
                })
            ));
        }
        ("/history", path) => {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct History {
                caller: TaskId,
                lease: WorkLease,
                cursor: Option<acyclic_harness::store::HistoryCursor>,
            }
            let request: History = read_json(Path::new(path))?;
            native
                .caller_context(request.caller, &request.lease)
                .await?;
            let binding = native.caller_binding(request.caller).await?;
            let verifier = acyclic_harness::core::AuthorityIssuer::new(
                &binding.issuer,
                binding.key,
                binding.authority.clone(),
            )
            .verifier(&mut None)
            .expect("verifier copy");
            let reader = acyclic_harness::store::HistoryReader::new(
                &native.stream,
                &binding.authority,
                &(verifier),
                &mut None,
            )?;
            let cursor = match request.cursor {
                Some(cursor) if cursor.authority == binding.authority => cursor,
                Some(_) => {
                    return Err(Error::Unauthorized(
                        "history cursor belongs to a different caller conversation".into(),
                    ));
                }
                None => reader.pin(0, None).await?,
            };
            let page = reader
                .read_page(
                    &cursor,
                    acyclic_harness::store::HistoryReadLimits {
                        maximum_events: terminal::MAX_PAGE_EVENTS as u32,
                        maximum_bytes: terminal::MAX_FRAME_BYTES as u64,
                    },
                    None,
                )
                .await?;
            return Ok(format!(
                "history-json={}\n",
                serde_json::to_string(&page).map_err(|error| Error::Invalid(error.to_string()))?
            ));
        }
        ("/inherited-history", path) => {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Inherited {
                caller: TaskId,
                lease: WorkLease,
                seed: acyclic_harness::fork::ForkSeed,
                after_sequence: u64,
            }
            let request: Inherited = read_json(Path::new(path))?;
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
                .find(|binding| binding.authority == request.seed.parent)
                .or_else(|| {
                    if native.config.owner.authority == request.seed.parent {
                        Some(&native.config.owner)
                    } else {
                        None
                    }
                })
                .ok_or_else(|| {
                    Error::Unsupported("original parent history verifier unavailable".into())
                })?;
            let verifier = acyclic_harness::core::AuthorityIssuer::new(
                &parent.issuer,
                parent.key,
                parent.authority.clone(),
            )
            .verifier(&mut None)
            .expect("verifier copy");
            let reader = acyclic_harness::store::HistoryReader::new(
                &native.stream,
                &parent.authority,
                &(verifier),
                &mut None,
            )?;
            let messages = reader
                .inherited_conversation_page(
                    &request.seed,
                    child.agent()?,
                    request.after_sequence,
                    acyclic_harness::store::HistoryReadLimits {
                        maximum_events: terminal::MAX_PAGE_EVENTS as u32,
                        maximum_bytes: terminal::MAX_FRAME_BYTES as u64,
                    },
                )
                .await?;
            return Ok(format!(
                "inherited-history-json={}\n",
                serde_json::json!({
                    "next_after": messages.last().map(|message| message.sequence), "messages": messages,
                })
            ));
        }
        ("/checkpoint", path) => {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Checkpoint {
                caller: TaskId,
                lease: WorkLease,
            }
            let request: Checkpoint = read_json(Path::new(path))?;
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
            return Ok(format!(
                "checkpoint-json={}\n",
                serde_json::json!({
                    "task": request.caller, "checkpoint": session.checkpoint(),
                    "transition": session.latest_transition().await?,
                })
            ));
        }
        ("/read-path", path) => {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct ReadPath {
                caller: TaskId,
                lease: WorkLease,
                volume: VolumeRef,
                path: String,
                #[serde(default)]
                generation: Option<acyclic_harness::resources::GenerationRef>,
            }
            let request: ReadPath = read_json(Path::new(path))?;
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
            let verifier = acyclic_harness::core::AuthorityIssuer::new(
                &binding.issuer,
                binding.key,
                binding.authority.clone(),
            )
            .verifier(&mut None)
            .expect("verifier copy");
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
            if bytes.len() > terminal::MAX_FRAME_BYTES {
                return Err(Error::Invalid(
                    "current file display exceeds terminal bounds".into(),
                ));
            }
            let text = std::str::from_utf8(&bytes)
                .map_err(|_| Error::Unsupported("file is not UTF-8 text".into()))?;
            return Ok(format!(
                "file-json={}\n",
                serde_json::json!({"file": file, "text": text})
            ));
        }
        (
            "/publication-open"
            | "/publication-observe"
            | "/publication-apply"
            | "/publication-reconcile",
            path,
        ) => {
            #[cfg(feature = "native-execution")]
            {
                let request: native_writeback::NativePublicationRequest =
                    read_json(Path::new(path))?;
                let result = if name == "/publication-open" {
                    native.publication_open(&request).await?
                } else if name == "/publication-observe" {
                    native.publication_observe(&request).await?
                } else if name == "/publication-apply" {
                    native.publication_apply(&request).await?
                } else {
                    native.publication_reconcile(&request).await?
                };
                return Ok(format!(
                    "publication-json={}\n",
                    serde_json::to_string(&result)
                        .map_err(|error| Error::Invalid(error.to_string()))?
                ));
            }
            #[cfg(not(feature = "native-execution"))]
            {
                let _ = path;
                return Err(Error::Unsupported(
                    "native root publication requires native-execution".into(),
                ));
            }
        }
        ("/mail", path) => {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Mail {
                caller: TaskId,
                lease: WorkLease,
                recipient: TaskId,
                operation: OperationId,
                payload: FileRef,
            }
            let request: Mail = read_json(Path::new(path))?;
            let receipt = native
                .caller_context(request.caller, &request.lease)
                .await?
                .send(request.recipient, request.operation, request.payload)
                .await?;
            return Ok(format!(
                "mail-json={}\n",
                serde_json::to_string(&receipt)
                    .map_err(|error| Error::Invalid(error.to_string()))?
            ));
        }
        ("/inbox", path) => {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Inbox {
                caller: TaskId,
                lease: WorkLease,
                after: u64,
            }
            let request: Inbox = read_json(Path::new(path))?;
            let items = native
                .caller_context(request.caller, &request.lease)
                .await?
                .inbox(request.after, terminal::MAX_PAGE_EVENTS)
                .await?;
            return Ok(format!(
                "inbox-json={}\n",
                serde_json::json!({
                    "task": request.caller, "after": request.after,
                    "next_after": items.last().map(|item| item.sequence), "items": items,
                    "consumption": "observation_only",
                })
            ));
        }
        ("/queue-mailbox", path) => {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Mailbox {
                operation: OperationId,
                parent: TaskId,
                lease: WorkLease,
                input: Value,
            }
            let request: Mailbox = read_json(Path::new(path))?;
            writeln!(
                output,
                "mailbox-admission-attempt operation={} caller-task={}",
                request.operation, request.parent
            )
            .and_then(|()| output.flush())
            .map_err(|error| Error::Storage(error.to_string()))?;
            return match native
                .queue_mailbox(
                    request.operation,
                    request.parent,
                    &request.lease,
                    request.input,
                )
                .await?
            {
                Admission::Accepted(value) => Ok(format!(
                    "queued task={} admission-operation={}\n",
                    value.identity(),
                    request.operation
                )),
                Admission::Rejected { reason } => Err(Error::Conflict(format!(
                    "mailbox admission rejected operation={}: {reason}",
                    request.operation
                ))),
                Admission::Indeterminate { operation_id } => {
                    Err(Error::Indeterminate(operation_id))
                }
            };
        }
        ("/write", path) => {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Write {
                caller: TaskId,
                lease: WorkLease,
                volume: VolumeRef,
                operation: OperationId,
                path: String,
                bytes: String,
            }
            let request: Write = read_json(Path::new(path))?;
            if request.bytes.len() > terminal::MAX_FRAME_BYTES / 2 {
                return Err(Error::Invalid(
                    "file staging exceeds terminal bounds".into(),
                ));
            }
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
            return Ok(format!("file-json={}\n", serde_json::json!({"file": file})));
        }
        ("/read" | "/file", path) => {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Read {
                caller: TaskId,
                lease: WorkLease,
                file: FileRef,
            }
            let request: Read = read_json(Path::new(path))?;
            if request.file.descriptor().byte_length() > terminal::MAX_FRAME_BYTES as u64 {
                return Err(Error::Invalid(
                    "file display exceeds terminal bounds".into(),
                ));
            }
            let bytes = native
                .caller_context(request.caller, &request.lease)
                .await?
                .read_file(&request.file)
                .await?;
            let text = std::str::from_utf8(&bytes)
                .map_err(|_| Error::Unsupported("file is not UTF-8 text".into()))?;
            if command.starts_with("/file ") {
                return Ok(format!(
                    "file {}\n{}\nfile-json={}\n",
                    terminal::text(request.file.path()),
                    terminal::text(text),
                    serde_json::json!({"file": request.file, "text": text})
                ));
            }
            return Ok(format!(
                "file-json={}\n",
                serde_json::json!({"file": request.file, "text": text})
            ));
        }
        ("/diff", path) => {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Diff {
                caller: TaskId,
                lease: WorkLease,
                before: FileRef,
                after: FileRef,
            }
            let diff: Diff = read_json(Path::new(path))?;
            if diff.before.descriptor().byte_length() > (terminal::MAX_FRAME_BYTES / 2) as u64
                || diff.after.descriptor().byte_length() > (terminal::MAX_FRAME_BYTES / 2) as u64
            {
                return Err(Error::Invalid(
                    "diff display exceeds terminal bounds".into(),
                ));
            }
            let context = native.caller_context(diff.caller, &diff.lease).await?;
            let before = context.read_file(&diff.before).await?;
            let after = context.read_file(&diff.after).await?;
            let before = std::str::from_utf8(&before)
                .map_err(|_| Error::Unsupported("before diff is not UTF-8".into()))?;
            let after = std::str::from_utf8(&after)
                .map_err(|_| Error::Unsupported("after diff is not UTF-8".into()))?;
            return Ok(format!(
                "diff exact before={} after={}\n--- before\n{}\n+++ after\n{}\n",
                terminal::text(diff.before.path()),
                terminal::text(diff.after.path()),
                terminal::text(before),
                terminal::text(after)
            ));
        }
        _ => {}
    }
    if !command.starts_with('/') || command.starts_with("/queue ") {
        let operation = OperationId::from_bytes(*uuid::Uuid::new_v4().as_bytes());
        let command = if let Some(path) = command.strip_prefix("/queue ") {
            read_json(Path::new(path))?
        } else {
            let command_operation = OperationId::from_bytes(*uuid::Uuid::new_v4().as_bytes());
            writeln!(
                output,
                "prompt-publication-attempt command-operation={command_operation}"
            )
            .and_then(|()| output.flush())
            .map_err(|error| Error::Storage(error.to_string()))?;
            native.prompt_command(command, command_operation).await?
        };
        // Publish the caller-held identity before attempting admission.
        writeln!(
            output,
            "admission-attempt operation={operation} command-operation={}",
            command.operation_id
        )
        .and_then(|()| output.flush())
        .map_err(|error| Error::Storage(error.to_string()))?;
        return match native.queue(operation, command, *parent).await? {
            Admission::Accepted(value) => Ok(format!(
                "queued task={} admission-operation={operation}\n",
                value.identity()
            )),
            Admission::Rejected { reason } => Err(Error::Conflict(format!(
                "admission rejected operation={operation}: {reason}"
            ))),
            Admission::Indeterminate { operation_id } => Err(Error::Indeterminate(operation_id)),
        };
    }

    Err(Error::Unsupported(
        "operation requires a durable product host; fixture input is not queued".into(),
    ))
}
