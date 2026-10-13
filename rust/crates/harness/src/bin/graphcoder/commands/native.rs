use super::super::*;
use acyclic_harness::{
    Admission, Error, OperationId, Result, TaskId,
    conversation::{ContentGrant, FileRef, VolumeOperation, VolumeRef},
    distributed::{WorkLease, WorkPull},
    fork::{ForkHistoryPolicy, ForkReport, ForkSeed},
    resource_preparation::ResourcePreparationRequest,
    resources::GenerationRef,
    runtime::{RuntimeTask, TaskDefinition, TaskSpawner as _, TaskStateProvider as _},
    scheduler::LeaseFence,
    store::{HistoryCursor, HistoryReadLimits, HistoryReader},
};
use native::{invalid, json_line, storage};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc};

const PAGE: HistoryReadLimits = HistoryReadLimits {
    maximum_events: terminal::MAX_PAGE_EVENTS as u32,
    maximum_bytes: terminal::MAX_FRAME_BYTES as u64,
};

struct Fields;
impl<'de> serde::de::Visitor<'de> for Fields {
    type Value = std::collections::BTreeMap<String, Box<serde_json::value::RawValue>>;
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a command object")
    }
    fn visit_map<M: serde::de::MapAccess<'de>>(
        self,
        mut map: M,
    ) -> std::result::Result<Self::Value, M::Error> {
        let mut fields = Self::Value::new();
        while let Some((key, value)) = map.next_entry()? {
            if fields.insert(key, value).is_some() {
                return Err(serde::de::Error::custom("duplicate command field"));
            }
        }
        Ok(fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    #[test]
    fn arguments_preserve_field_order_and_reject_ambiguous_json() -> Result<()> {
        for (body, accepted) in [
            (
                r#"{"revision":null,"authority":{"kind":"task","id":"caller"}}"#,
                true,
            ),
            (
                r#"{"authority":{"kind":"task","id":"caller","id":"other"}}"#,
                false,
            ),
            (
                r#"{"authority":{"kind":"task","id":"caller"},"revision":null,"revision":1}"#,
                false,
            ),
            (
                r#"{"authority":{"kind":"task","id":"caller"},"extra":false}"#,
                false,
            ),
        ] {
            let mut file = tempfile::NamedTempFile::new().map_err(storage)?;
            file.write_all(body.as_bytes()).map_err(storage)?;
            let parsed: Result<(acyclic_harness::core::Authority, Option<u64>)> =
                arguments(file.path(), &["authority", "revision"]);
            assert_eq!(parsed.is_ok(), accepted);
            if let Ok((authority, revision)) = parsed {
                assert_eq!(authority.id, "caller");
                assert_eq!(revision, None);
            }
        }
        Ok(())
    }
}

fn arguments<T: serde::de::DeserializeOwned>(path: &Path, names: &[&str]) -> Result<T> {
    let bytes = read_bytes(path)?;
    let mut decoder = serde_json::Deserializer::from_slice(&bytes);
    let mut fields = serde::Deserializer::deserialize_map(&mut decoder, Fields).map_err(invalid)?;
    decoder.end().map_err(invalid)?;
    if fields.keys().any(|key| !names.contains(&key.as_str())) {
        return Err(Error::Invalid("unknown command field".into()));
    }
    let values: Vec<_> = names.iter().map(|name| fields.remove(*name)).collect();
    serde_json::from_slice(&serde_json::to_vec(&values).map_err(invalid)?).map_err(invalid)
}

fn emit(output: &mut impl std::io::Write, line: std::fmt::Arguments<'_>) -> Result<()> {
    writeln!(output, "{line}")
        .and_then(|()| output.flush())
        .map_err(storage)
}

fn admitted<T>(admission: Admission<RuntimeTask<T>>) -> Result<String> {
    match admission {
        Admission::Accepted(task) => Ok(task.identity()),
        Admission::Rejected { reason } => Err(Error::Conflict(reason)),
        Admission::Indeterminate { operation_id } => Err(Error::Indeterminate(operation_id)),
    }
}

fn utf8(bytes: &[u8]) -> Result<&str> {
    std::str::from_utf8(bytes).map_err(|_| Error::Unsupported("file is not UTF-8 text".into()))
}

pub(super) async fn execute(
    command: &str,
    backend: &mut Option<Backend>,
    parent: &mut Option<TaskId>,
    workers: &mut tokio::task::JoinSet<Result<String>>,
    output: &mut impl std::io::Write,
) -> Result<String> {
    let api = backend
        .as_ref()
        .ok_or_else(|| Error::Conflict("session is closed".into()))?;
    if command == "/reopen" {
        if !workers.is_empty() {
            return Err(Error::Conflict(
                "active workers must settle before reopen".into(),
            ));
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
    if matches!(name, "/run" | "/claim" | "/resume") && workers.len() >= terminal::MAX_PAGE_EVENTS {
        return Err(Error::Invalid(
            "active frontend worker bound reached".into(),
        ));
    }
    match name {
        "/parent" => {
            *parent = if argument == "root" {
                None
            } else {
                let task = TaskId::parse(argument)?;
                native.runtime.task_host().observe_admission(task).await?;
                native.runtime.task_host().outcome(task).await?;
                Some(task)
            };
            return Ok(format!("selected parent-task={parent:?}\n"));
        }
        "/run" | "/claim" => {
            let task = TaskId::parse(argument)?;
            let worker = acyclic_harness::distributed::Worker {
                id: format!("graphcoder-{}", uuid::Uuid::new_v4()),
                available: Default::default(),
                labels: Default::default(),
            };
            let native = Arc::clone(native);
            return match native.claim(task, &worker).await? {
                WorkPull::Idle => Ok(format!("worker idle task={task}\n")),
                WorkPull::Unresolved { lease, error } => native::render_attempt(
                    acyclic_harness::filesystem::TaskWorkerAttempt::Unresolved { lease, error },
                ),
                WorkPull::Claimed(lease) => {
                    emit(
                        output,
                        format_args!(
                            "worker-claim task={task} lease-json-hex={}",
                            native::lease_hex(&lease)?
                        ),
                    )?;
                    if name == "/claim" {
                        native.runtime.task_host().start_task(&lease, None).await?;
                        return Ok(format!("worker retained task={task}\n"));
                    }
                    workers.spawn(async move { native.resume(lease).await });
                    Ok(format!("worker started task={task}\n"))
                }
            };
        }
        "/cancel-task" => {
            let task = TaskId::parse(argument)?;
            let admission = native.runtime.task_host().observe_admission(task).await?;
            native.runtime.task_host().cancel(task, None).await?;
            return Ok(format!(
                "cancellation requested task={task} admission-operation={}\n",
                admission.operation_id
            ));
        }
        "/outcome" => {
            let task = TaskId::parse(argument)?;
            let admission = native.runtime.task_host().observe_admission(task).await?;
            let outcome = native.runtime.task_host().outcome(task).await?;
            let operation = admission.operation_id;
            return Ok(terminal::text(&format!(
                "task={task} admission-operation={operation} outcome={outcome:?}\n"
            )) + &json_line(
                "task",
                &json!({"task": task, "admission_operation": operation, "outcome": outcome}),
            )?);
        }
        "/reconcile" => {
            let operation = OperationId::parse(argument)?;
            let found = native
                .runtime
                .harness()
                .reconcile_admission(operation)
                .await?;
            return Ok(terminal::text(&format!(
                "admission operation={operation} retained={found:?}\n"
            )));
        }
        "/resume" => {
            let lease = read_json(path)?;
            let native = Arc::clone(native);
            workers.spawn(async move { native.resume(lease).await });
            return Ok("original lease submitted for authoritative reconciliation\n".into());
        }
        _ if !command.starts_with('/') => {
            let operation = OperationId::new();
            let command_operation = OperationId::new();
            emit(
                output,
                format_args!("prompt-publication-attempt command-operation={command_operation}"),
            )?;
            let command = native.prompt_command(command, command_operation).await?;
            emit(
                output,
                format_args!(
                    "admission-attempt operation={operation} command-operation={}",
                    command.operation_id
                ),
            )?;
            return Ok(format!(
                "queued task={} admission-operation={operation}\n",
                admitted(native.queue(operation, command, *parent).await?)?
            ));
        }
        _ => {}
    }
    match name {
        #[cfg(feature = "native-execution")]
        "/command" => {
            let (caller, lease, operation, decision): (
                TaskId,
                WorkLease,
                OperationId,
                Option<bool>,
            ) = arguments(path, &["caller", "lease", "operation", "decision"])?;

            native.command(caller, &lease, operation, decision).await
        }
        "/context" => native.configure_context(read_json(path)?).await,
        "/fork-selection" => {
            let (caller, lease, operation, history): (
                TaskId,
                WorkLease,
                OperationId,
                ForkHistoryPolicy,
            ) = arguments(path, &["caller", "lease", "operation", "history"])?;
            native.select_fork(caller, &lease, operation, history).await
        }
        "/children" => {
            let (task, revision, after): (TaskId, Option<u64>, Option<String>) =
                arguments(path, &["task", "revision", "after"])?;

            let page = native
                .runtime
                .harness()
                .children(task, revision, after, terminal::MAX_PAGE_EVENTS)
                .await?;
            Ok(
                terminal::text(&format!("children parent-task={task} {page:?}\n"))
                    + &json_line("children", &page)?,
            )
        }
        "/prepare" => {
            let (operation, parent, caller_lease, request): (
                OperationId,
                TaskId,
                WorkLease,
                ResourcePreparationRequest,
            ) = arguments(path, &["operation", "parent", "caller_lease", "request"])?;

            request.validate(&mut None)?;
            if request.child.parent != Some(parent) {
                return Err(Error::Unauthorized(
                    "preparation belongs to another caller".into(),
                ));
            }
            let fork: acyclic_harness::fork::ForkRequest =
                serde_json::from_value(request.payload.clone()).map_err(invalid)?;
            fork.validate(&mut None)?;
            let agent = fork.child_agent;
            emit(
                output,
                format_args!(
                    "preparation-attempt operation={operation} caller-task={parent} new-child-agent={agent} child-admission-operation={} fork-operation={}",
                    request.child.operation_id, fork.operation_id
                ),
            )?;
            let definition: Arc<TaskDefinition<ResourcePreparationRequest, ForkReport>> = native
                .runtime
                .harness()
                .task("acyclic.fork_preparation@1")?;
            let task = admitted(
                native
                    .caller_context(parent, &caller_lease)
                    .await?
                    .admit(operation, &definition, request)
                    .await?,
            )?;
            Ok(format!(
                "queued preparation-task={task} admission-operation={operation} caller-task={parent} new-child-agent={agent}\n"
            ))
        }
        "/publish-fork" => {
            let (caller, lease, preparation): (TaskId, WorkLease, TaskId) =
                arguments(path, &["caller", "lease", "preparation"])?;
            native
                .publish_preparation(caller, &lease, preparation)
                .await
        }
        "/inspect" => {
            let (caller, lease, source): (TaskId, WorkLease, TaskId) =
                arguments(path, &["caller", "lease", "source"])?;

            native.caller_context(caller, &lease).await?;
            let inspection = native.runtime.inspect_source(caller, source).await?;
            json_line(
                "inspection",
                &json!({
                    "caller": caller, "source_task": source, "agent": inspection.agent()?,
                    "source": inspection.source(), "volumes": inspection.volumes(), "project": inspection.project()?,
                    "private_volume": inspection.private_volume()?, "generation_semantics": "original_allocation",
                }),
            )
        }
        "/history" => {
            let (caller, lease, cursor): (TaskId, WorkLease, Option<HistoryCursor>) =
                arguments(path, &["caller", "lease", "cursor"])?;

            native.caller_context(caller, &lease).await?;
            let binding = native.caller_binding(caller).await?;
            let verifier = binding.verifier()?;
            let reader =
                HistoryReader::new(&native.stream, &binding.authority, &verifier, &mut None)?;
            let cursor = match cursor {
                Some(cursor) if cursor.authority == binding.authority => cursor,
                Some(_) => {
                    return Err(Error::Unauthorized(
                        "history cursor belongs to another caller".into(),
                    ));
                }
                None => reader.pin(0, None).await?,
            };
            json_line("history", &reader.read_page(&cursor, PAGE, None).await?)
        }
        "/inherited-history" => {
            let (caller, lease, seed, after_sequence): (TaskId, WorkLease, ForkSeed, u64) =
                arguments(path, &["caller", "lease", "seed", "after_sequence"])?;

            native.caller_context(caller, &lease).await?;
            let agent = native
                .runtime
                .inspect_source(caller, caller)
                .await?
                .agent()?;
            let binding = native.caller_binding(caller).await?;
            if agent != seed.child_agent || binding.authority != seed.child {
                return Err(Error::Unauthorized(
                    "inherited history belongs to another caller".into(),
                ));
            }
            let parent = native
                .config
                .bindings()
                .find(|binding| binding.authority == seed.parent)
                .ok_or_else(|| Error::Unsupported("parent history verifier unavailable".into()))?;
            let verifier = parent.verifier()?;
            let reader =
                HistoryReader::new(&native.stream, &parent.authority, &verifier, &mut None)?;
            let messages = reader
                .inherited_conversation_page(&seed, agent, after_sequence, PAGE)
                .await?;
            json_line(
                "inherited-history",
                &json!({"next_after": messages.last().map(|message| message.sequence), "messages": messages}),
            )
        }
        "/checkpoint" => {
            let (caller, lease): (TaskId, WorkLease) = arguments(path, &["caller", "lease"])?;

            native.caller_context(caller, &lease).await?;
            let session = native
                .runtime
                .open_task(caller, LeaseFence::from(&lease.reservation))
                .await?;
            json_line(
                "checkpoint",
                &json!({"task": caller, "checkpoint": session.checkpoint(), "transition": session.latest_transition().await?}),
            )
        }
        "/read-path" => {
            let (caller, lease, volume, path, generation): (
                TaskId,
                WorkLease,
                VolumeRef,
                String,
                Option<GenerationRef>,
            ) = arguments(path, &["caller", "lease", "volume", "path", "generation"])?;

            let context = native.caller_context(caller, &lease).await?;
            let full = volume.capability(VolumeOperation::Read, &mut None)?;
            let directory = volume.directory_read_capability("", &mut None)?;
            if !context.scope().grants().contains(&full)
                && !context.scope().grants().contains(&directory)
            {
                return Err(Error::Unauthorized(
                    "caller cannot discover this volume".into(),
                ));
            }
            let binding = native.caller_binding(caller).await?;
            let verifier = binding.verifier()?;
            let grant = ContentGrant::verify_directory_read(
                &verifier,
                &binding.scope,
                &volume,
                "",
                &mut None,
            )?;
            let (file, bytes) = native
                .files
                .read_private_path(
                    &volume,
                    &grant,
                    &path,
                    generation.as_ref(),
                    terminal::MAX_FRAME_BYTES as u64,
                )
                .await?;
            json_line("file", &json!({"file": file, "text": utf8(&bytes)?}))
        }
        "/publication-open"
        | "/publication-observe"
        | "/publication-apply"
        | "/publication-reconcile" => {
            #[cfg(feature = "native-execution")]
            {
                let request = read_json(path)?;
                let result = native.publication(name, &request).await?;
                json_line("publication", &result)
            }
            #[cfg(not(feature = "native-execution"))]
            Err(Error::Unsupported(
                "native publication is unavailable".into(),
            ))
        }
        "/mail" => {
            let (caller, lease, recipient, operation, payload): (
                TaskId,
                WorkLease,
                TaskId,
                OperationId,
                FileRef,
            ) = arguments(
                path,
                &["caller", "lease", "recipient", "operation", "payload"],
            )?;

            let receipt = native
                .caller_context(caller, &lease)
                .await?
                .send(recipient, operation, payload)
                .await?;
            json_line("mail", &receipt)
        }
        "/inbox" => {
            let (caller, lease, after): (TaskId, WorkLease, u64) =
                arguments(path, &["caller", "lease", "after"])?;

            let items = native
                .caller_context(caller, &lease)
                .await?
                .inbox(after, terminal::MAX_PAGE_EVENTS)
                .await?;
            json_line(
                "inbox",
                &json!({"task": caller, "after": after, "next_after": items.last().map(|item| item.sequence),
                "items": items, "consumption": "observation_only"}),
            )
        }
        "/queue-mailbox" => {
            let (operation, parent, lease, input): (OperationId, TaskId, WorkLease, Value) =
                arguments(path, &["operation", "parent", "lease", "input"])?;

            emit(
                output,
                format_args!(
                    "mailbox-admission-attempt operation={operation} caller-task={parent}"
                ),
            )?;
            let definition: Arc<TaskDefinition<Value, Value>> =
                native.runtime.harness().task("acyclic.mailbox_model@1")?;
            let task = admitted(
                native
                    .caller_context(parent, &lease)
                    .await?
                    .admit(operation, &definition, input)
                    .await?,
            )?;
            Ok(format!(
                "queued task={task} admission-operation={operation}\n"
            ))
        }
        "/write" => {
            let (caller, lease, volume, operation, path, bytes): (
                TaskId,
                WorkLease,
                VolumeRef,
                OperationId,
                String,
                String,
            ) = arguments(
                path,
                &["caller", "lease", "volume", "operation", "path", "bytes"],
            )?;

            let file = native
                .caller_context(caller, &lease)
                .await?
                .stage_file(
                    Some(&volume),
                    operation,
                    &path,
                    bytes.as_bytes(),
                    "text/plain",
                    &path,
                )
                .await?;
            json_line("file", &json!({"file": file}))
        }
        "/read" | "/file" => {
            let (caller, lease, file): (TaskId, WorkLease, FileRef) =
                arguments(path, &["caller", "lease", "file"])?;

            let bytes = native
                .caller_context(caller, &lease)
                .await?
                .read_file(&file, None)
                .await?;
            let text = utf8(&bytes)?;
            let line = json_line("file", &json!({"file": file, "text": text}))?;
            if name == "/file" {
                return Ok(format!(
                    "file {}\n{}\n{line}",
                    terminal::text(file.path()),
                    terminal::text(text)
                ));
            }
            Ok(line)
        }
        "/diff" => {
            let (caller, lease, before, after): (TaskId, WorkLease, FileRef, FileRef) =
                arguments(path, &["caller", "lease", "before", "after"])?;

            let context = native.caller_context(caller, &lease).await?;
            let old = context.read_file(&before, None).await?;
            let new = context.read_file(&after, None).await?;
            Ok(format!(
                "diff exact before={} after={}\n--- before\n{}\n+++ after\n{}\n",
                terminal::text(before.path()),
                terminal::text(after.path()),
                terminal::text(utf8(&old)?),
                terminal::text(utf8(&new)?)
            ))
        }
        _ => Err(Error::Unsupported("unknown native command".into())),
    }
}
