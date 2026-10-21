//! Child tools read and write the parent's existing task-volume files.
use super::agent_files::files as bound_files;
use crate::{
    Error, Result,
    conversation::Limits,
    runtime::{RuntimeScope, ToolContext},
    task_volume::{
        ChildFile, ChildRequest, Component, Control, FileKey, FileValue, Query, TaskFiles,
    },
    tool::{ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use futures::StreamExt as _;
use serde_json::{Value, json};
use std::collections::BTreeSet;

mod arguments;
pub use arguments::{ArchiveInput, SendInput, SpawnInput, WaitInput, WaitMode};
mod files;
mod selection;

pub(crate) fn family(name: &str) -> bool {
    matches!(name, "spawn" | "send" | "wait" | "archive")
}

pub(super) struct Executor(pub &'static str);

impl Executor {
    async fn run(
        &self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
        reconcile: bool,
    ) -> Result<Option<ToolResult>> {
        self.authorize(None, context.as_ref(), &invocation, None)?;
        let context =
            context.ok_or_else(|| Error::Unauthorized("child context is absent".into()))?;
        let limits = context.effective_scope().limits();
        let owner = context.task().owned_mailbox.as_deref();
        #[cfg(feature = "filesystem")]
        let cancellation = context.task().invocation_cancellation();
        #[cfg(not(feature = "filesystem"))]
        let cancellation = None;
        let files = bound_files(&context).await?;
        let value = match self.0 {
            "spawn" => {
                let input: SpawnInput =
                    serde_json::from_value(invocation.arguments).map_err(invalid)?;
                if input.task.is_empty() {
                    return Err(invalid("child task is empty"));
                }
                crate::task_volume::child_scope(
                    context.effective_scope(),
                    None,
                    input.grants.as_deref(),
                )?;
                let reference = component(
                    input
                        .name
                        .unwrap_or_else(|| invocation.operation_id.to_string()),
                )?;
                let request = ChildRequest {
                    prompt: input.task,
                    model: None,
                    tools: None,
                    grants: input.grants,
                };
                let bytes = request.encode();
                ChildRequest::parse(&bytes, limits.file_bytes)?;
                if !files::write(
                    files.as_ref(),
                    &reference,
                    FileValue::new(
                        FileKey::Child {
                            reference: reference.clone(),
                            file: ChildFile::Request,
                        },
                        bytes,
                    ),
                    limits,
                    owner,
                    reconcile,
                )
                .await?
                {
                    return Ok(None);
                }
                if cancellable(
                    files::follow(files.as_ref(), &reference, limits, !reconcile, false),
                    cancellation,
                )
                .await?
                .is_none()
                {
                    return Ok(None);
                }
                json!({"agent":reference})
            }
            "send" => {
                let input: SendInput =
                    serde_json::from_value(invocation.arguments).map_err(invalid)?;
                if input.message.is_empty() {
                    return Err(invalid("child message is empty"));
                }
                let reference = component(input.agent)?;
                if !files::write(
                    files.as_ref(),
                    &reference,
                    FileValue::new(
                        FileKey::Child {
                            reference: reference.clone(),
                            file: ChildFile::Inbox(component(invocation.operation_id.to_string())?),
                        },
                        input.message.into_bytes(),
                    ),
                    limits,
                    owner,
                    reconcile,
                )
                .await?
                {
                    return Ok(None);
                }
                json!({"agent":reference,"queued":true})
            }
            "wait" => {
                let input: WaitInput =
                    serde_json::from_value(invocation.arguments).map_err(invalid)?;
                let result = cancellable(
                    wait(
                        files.as_ref(),
                        input,
                        limits,
                        invocation.operation_id,
                        !reconcile,
                        owner
                            .ok_or_else(|| invalid("wait owner is absent"))?
                            .task_id(),
                        owner,
                    ),
                    cancellation,
                )
                .await?;
                match result {
                    Some(value) => value,
                    None => return Ok(None),
                }
            }
            "archive" => {
                let input: ArchiveInput =
                    serde_json::from_value(invocation.arguments).map_err(invalid)?;
                let reference = component(input.agent)?;
                if !files::write(
                    files.as_ref(),
                    &reference,
                    FileValue::new(
                        FileKey::Child {
                            reference: reference.clone(),
                            file: ChildFile::Control,
                        },
                        Control::Archive.bytes().to_vec(),
                    ),
                    limits,
                    owner,
                    reconcile,
                )
                .await?
                {
                    return Ok(None);
                }
                if cancellable(
                    files::follow(files.as_ref(), &reference, limits, !reconcile, true),
                    cancellation,
                )
                .await?
                .is_none()
                {
                    return Ok(None);
                }
                json!({"agent":reference})
            }
            _ => return Err(invalid("unknown child tool")),
        };
        Ok(Some(ToolResult {
            value: files::bounded(value, limits)?,
        }))
    }
}

pub(crate) async fn cancellable<T>(
    future: impl std::future::Future<Output = Result<T>>,
    cancellation: Option<&acyclic_fs::CancellationToken>,
) -> Result<T> {
    let Some(token) = cancellation else {
        return future.await;
    };
    let cancelled = token.cancelled();
    futures::pin_mut!(future, cancelled);
    match futures::future::select(future, cancelled).await {
        futures::future::Either::Left((result, _)) => result,
        futures::future::Either::Right(_) => Err(Error::InteractionRejected(
            crate::InteractionRejection::Cancelled,
        )),
    }
}

async fn wait<P: StreamProvider + ?Sized>(
    files: &TaskFiles<P>,
    input: WaitInput,
    limits: Limits,
    operation: crate::OperationId,
    block: bool,
    task: crate::TaskId,
    owner: Option<&dyn crate::runtime::OwnedTaskMailbox>,
) -> Result<Option<Value>> {
    if input.agents.is_empty() || input.agents.len() > limits.attachments {
        return Err(invalid("wait requires a bounded nonempty agent selection"));
    }
    let binding = serde_json::to_value(&input).map_err(invalid)?;
    let selection = selection::key(task, operation)?;
    if input.mode == Some(WaitMode::Any) {
        if let Some(value) = selection::load(files, &selection, &binding).await? {
            return files::bounded(value, limits).map(Some);
        }
    }
    let agents = input
        .agents
        .into_iter()
        .map(component)
        .collect::<Result<Vec<_>>>()?;
    if agents.iter().collect::<BTreeSet<_>>().len() != agents.len() {
        return Err(invalid("wait agent references must be distinct"));
    }
    let waits = agents
        .iter()
        .map(|reference| Box::pin(files::follow(files, reference, limits, block, true)))
        .collect::<Vec<_>>();
    let results = if block && input.mode == Some(WaitMode::Any) {
        let (result, _, _) = futures::future::select_all(waits).await;
        vec![result?.ok_or(Error::Indeterminate(operation))?]
    } else {
        let results = futures::future::try_join_all(waits).await?;
        if input.mode == Some(WaitMode::Any) {
            match results.into_iter().flatten().next() {
                Some(result) => vec![result],
                None => return Ok(None),
            }
        } else {
            let Some(results) = results.into_iter().collect::<Option<Vec<_>>>() else {
                return Ok(None);
            };
            results
        }
    };
    // The settled barrier is the tool's contract. Detailed execution status
    // remains in status.json; avoid repeating it in every model request.
    let results: Vec<_> = results
        .into_iter()
        .map(|mut result| {
            let fields = result.as_object_mut().unwrap();
            fields.remove("execution");
            fields.remove("settled");
            result
        })
        .collect();
    let value = files::bounded(json!({"agents":results}), limits)?;
    if input.mode == Some(WaitMode::Any) {
        let saved = selection::save(files, selection, binding, value.clone(), owner).await;
        #[cfg(test)]
        #[cfg(not(acyclic_harness_loom))]
        if saved.is_err() && selection::SWALLOW_SAVE_ERROR.with(std::cell::Cell::get) {
            return Ok(Some(value));
        }
        saved?;
    }
    Ok(Some(value))
}

impl ToolExecutor for Executor {
    fn authorize(
        &self,
        _: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "child files require ordinary task execution",
            });
        }
        let _ = work;
        let context = context
            .ok_or_else(|| Error::Unauthorized("child tool requires its task context".into()))?;
        if invocation.name != self.0
            || context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
            || !context
                .effective_scope()
                .grants()
                .contains("tool:call:spawn")
            || !context
                .effective_scope()
                .grants()
                .contains(&format!("tool:call:{}", self.0))
            || context.task().owned_mailbox.is_none()
        {
            return Err(Error::Unauthorized(
                "child call differs from its task authority".into(),
            ));
        }
        if context.task().harness.task_files.is_none() {
            return Err(Error::Unsupported("child task files are not bound".into()));
        }
        Ok(())
    }
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let operation = invocation.operation_id;
            self.run(invocation, context, false)
                .await?
                .ok_or(Error::Indeterminate(operation))
        })
    }
    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(self.run(invocation, context, true))
    }
}
fn component(value: String) -> Result<Component> {
    Component::new(value).map_err(invalid)
}
fn invalid(error: impl std::fmt::Display) -> Error {
    Error::Invalid(error.to_string())
}
fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod tests;
