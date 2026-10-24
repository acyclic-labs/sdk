//! Runtime ownership of request-file subscriptions over the stock package.
use crate::{
    Error, OperationId, Result,
    client::{CommandExecution, CommandRequest, CommandResolution, CommandSession, CommandTool},
    local::LocalHarness,
    task_volume::FileValue,
};
use acyclic_stream::runtime::Executor;
use futures::{channel::oneshot, future::Either};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

tokio::task_local! {
    // This signal is scoped to the runtime-owned command, never to a task's
    // durable cancellation. Only a passive approval wait can consume it.
    static APPROVAL_ATTACHMENT: acyclic_fs::CancellationToken;
}

pub(crate) async fn approval_attachment_closed() {
    match APPROVAL_ATTACHMENT.try_with(Clone::clone) {
        Ok(attachment) => attachment.cancelled().await,
        Err(_) => std::future::pending().await,
    }
}

/// An attachment observes execution; releasing it only stops admission of new
/// requests. Work already admitted by its subscription remains runtime-owned.
pub struct CommandSubscription {
    completion: oneshot::Receiver<Result<()>>,
    attachment: Option<oneshot::Sender<()>>,
}

impl Future for CommandSubscription {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.completion).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Storage(
                "harness runtime stopped before request settlement".into(),
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl CommandSubscription {
    /// Separate admission lifetime from settlement so client shutdown can stop
    /// new requests and still join all runtime-owned work.
    pub(crate) fn take_attachment(&mut self) -> Option<oneshot::Sender<()>> {
        self.attachment.take()
    }

    /// Install a subscription on the caller's runtime executor. Its handle is
    /// disposable; only the executor owns the admitted command's future.
    pub fn start(
        local: Arc<LocalHarness>,
        executor: &dyn Executor,
        session: OperationId,
        only_session: bool,
    ) -> Self {
        let (attachment, detached) = oneshot::channel();
        let (settled, completion) = oneshot::channel();
        executor.spawn(Box::pin(async move {
            let result = run_requests(&local, session, only_session, detached).await;
            // The final connection or runtime worker retires native mounts,
            // after all admitted effects and publications have finished.
            let cleanup = match Arc::into_inner(local) {
                Some(local) => local.disconnect().await,
                None => Ok(()),
            };
            let _ = settled.send(result.and(cleanup));
        }));
        Self {
            completion,
            attachment: Some(attachment),
        }
    }
}

async fn run_requests(
    local: &LocalHarness,
    session: OperationId,
    only_session: bool,
    mut detached: oneshot::Receiver<()>,
) -> Result<()> {
    use acyclic_stream::StreamProvider;
    let stream = local.task_runtime().task_host().workflow_stream();
    let mut key = [0; 48];
    key[..16].copy_from_slice(&session.into_bytes());
    let namespace = format!(
        "harness-command-subscription:{}",
        local.task_files().volume().storage_name(&mut None)?
    );
    key[16..].copy_from_slice(blake3::hash(namespace.as_bytes()).as_bytes());
    let Some(_owner) =
        super::subscription_claim::acquire(stream.operation_claims(), key, &mut detached).await?
    else {
        return Ok(());
    };
    let mut commands = CommandSession::open(
        Arc::new(
            local
                .task_files()
                .inner
                .reopen()
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        ),
        uuid::Uuid::from_bytes(session.into_bytes()),
        65_536,
        4096,
    )
    .await?;
    loop {
        // Detachment may stop an idle follow, never an admitted handler/effect.
        let request =
            match futures::future::select(&mut detached, Box::pin(commands.next_request())).await {
                Either::Left(_) => return Ok(()),
                Either::Right((request, _)) => request?,
            };
        if only_session && !matches!(request.key, crate::task_volume::FileKey::Session { .. }) {
            continue;
        }
        let result = {
            let attachment = acyclic_fs::CancellationToken::new();
            let execution = APPROVAL_ATTACHMENT.scope(
                attachment.clone(),
                execute_request(&mut commands, local, &request),
            );
            futures::pin_mut!(execution);
            match futures::future::select(&mut detached, execution).await {
                Either::Left((_, execution)) => {
                    attachment.cancel();
                    // Admitted effects remain owned and are joined. The signal
                    // only releases a subsequently reached passive approval wait.
                    execution.await
                }
                Either::Right((result, _)) => result,
            }
        };
        if let Err(error) = result {
            if commands.failure_is_saved(&request, &error).await? {
                if only_session {
                    return Ok(());
                }
                continue;
            }
            if matches!(&error, Error::Conflict(message)
                if message == "command query advanced; retry from the new cut")
            {
                continue;
            }
            return Err(error);
        }
        if only_session {
            return Ok(());
        }
    }
}

/// Host-side delivery of an authenticated immutable command file. Presentation
/// clients install a subscription and publish files instead of driving this.
pub(crate) async fn execute_request<P: acyclic_stream::StreamProvider + ?Sized>(
    session: &mut CommandSession<P>,
    local: &LocalHarness,
    request: &FileValue,
) -> Result<Option<serde_json::Value>> {
    let settings = session.request_settings(request).await?;
    session
        .consume_with_recovery::<true, _, _, _, _>(
            request,
            |operation, target, resolution| {
                Box::pin(execute(local, operation, target, resolution, &settings))
            },
            |operation, target, resolution| {
                let settings = &settings;
                async move {
                    if matches!(
                        &resolution,
                        CommandResolution::Submit {
                            request: CommandRequest::Rewind
                        }
                    ) {
                        return Box::pin(execute(local, operation, target, resolution, settings))
                            .await;
                    }
                    if !matches!(
                        &resolution,
                        CommandResolution::Prompt { .. }
                            | CommandResolution::Tool {
                                tool: CommandTool::Spawn,
                                ..
                            }
                            | CommandResolution::Submit {
                                request: CommandRequest::Replace { .. }
                            }
                    ) {
                        return CommandExecution::from(Err(Error::Indeterminate(operation)));
                    }
                    let task = crate::runtime::task_id_for_admission(operation);
                    match local.resume(task).await {
                        Err(Error::NotFound(_)) => {
                            Box::pin(execute(local, operation, target, resolution, settings)).await
                        }
                        output => {
                            let result = async {
                                let mut value = serde_json::to_value(output?)
                                    .map_err(|error| Error::Invalid(error.to_string()))?;
                                let fields = value.as_object_mut().ok_or_else(|| {
                                    Error::Invalid("turn output must be an object".into())
                                })?;
                                fields.insert("task".into(), serde_json::json!(task));
                                fields.insert("selected_task".into(), serde_json::json!(task));
                                fields.insert(
                                    "admission_operation".into(),
                                    serde_json::json!(operation),
                                );
                                Ok(value)
                            }
                            .await;
                            CommandExecution::admitted(task, result)
                        }
                    }
                }
            },
        )
        .await
}

async fn execute(
    local: &LocalHarness,
    operation: OperationId,
    target: Option<crate::TaskId>,
    resolution: CommandResolution,
    settings: &serde_json::Value,
) -> CommandExecution {
    let mut admitted = None;
    let outcome = async {
        let target = target.filter(|task| task.to_string() != local.client_owner().id);
        let _driving = local.driver_claim(operation).await;
        let mut result = local
            .dispatch_configured_command(operation, target, resolution, settings)
            .await?;
        if result.get("admission_operation").is_some() {
            let task = result
                .get("task")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| Error::Invalid("command admission has no task".into()))?;
            let task = crate::TaskId::parse(task)?;
            admitted = Some(task);
            let output = serde_json::to_value(local.resume(task).await?)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            result
                .as_object_mut()
                .ok_or_else(|| Error::Invalid("command result must be an object".into()))?
                .extend(
                    output
                        .as_object()
                        .ok_or_else(|| Error::Invalid("turn output must be an object".into()))?
                        .clone(),
                );
        }
        Ok(result)
    }
    .await;
    match admitted {
        Some(task) => CommandExecution::admitted(task, outcome),
        None => outcome.into(),
    }
}
