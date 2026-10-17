//! Native request files share the existing task runtime and checkout publisher.
use super::*;
use crate::external_executor::local_paths;
use crate::{
    external_executor::{ExternalAction, ExternalRequest, RequestCommands, RequestConsumer},
    task_volume::{Component, FileKey, Query, TaskFile, TaskVolume},
};
use serde_json::Value;

#[cfg(all(feature = "native-execution", feature = "hooks"))]
mod process;
mod recovery;
mod registered;
#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

pub(crate) struct Volumes {
    pub(crate) requests: TaskVolume<LocalStream>,
    pub(crate) results: TaskVolume<LocalStream>,
}

impl Volumes {
    fn open(stream: Arc<LocalStream>, task: TaskId) -> impl Future<Output = Result<Self>> {
        Box::pin(async move {
            let requests = TaskVolume::open(
                stream.clone(),
                local_paths::path(local_paths::REQUESTS, task),
            )
            .await
            .map_err(storage)?;
            let results = TaskVolume::open(stream, local_paths::path(local_paths::RESULTS, task))
                .await
                .map_err(storage)?;
            Ok(Self { requests, results })
        })
    }
}

impl LocalHarness {
    async fn binding(
        &self,
        task: TaskId,
    ) -> Result<(
        crate::sync::owned::OwnedMutexGuard<Option<Arc<Volumes>>>,
        Arc<Volumes>,
    )> {
        let mut binding = self.external_bindings.lock(task).await;
        let volumes = match binding.as_ref() {
            Some(volumes) => Arc::clone(volumes),
            None => {
                let volumes = Arc::new(Volumes::open(self.task_stream(), task).await?);
                *binding = Some(Arc::clone(&volumes));
                volumes
            }
        };
        Ok((binding, volumes))
    }

    pub(crate) async fn external_volumes(&self, task: TaskId) -> Result<Arc<Volumes>> {
        self.binding(task).await.map(|(_, volumes)| volumes)
    }

    /// Reads an existing native result without admitting or executing work.
    #[cfg(feature = "hooks")]
    pub fn external_tool_result<'a>(
        &'a self,
        name: &'a str,
        arguments: Value,
    ) -> impl Future<Output = Result<Value>> + 'a {
        Box::pin(async move {
            #[cfg(feature = "native-execution")]
            let workspace = self
                .workspace_path()
                .map(|path| blake3::hash(path.as_os_str().as_encoded_bytes()).to_hex());
            #[cfg(not(feature = "native-execution"))]
            let workspace: Option<String> = None;
            let workspace = workspace.as_ref().map(|workspace| workspace.as_str());
            let key = crate::external_executor::native_result_key(&arguments, workspace)?;
            let crate::task_volume::FileKey::Task { task, .. } = key else {
                return Err(Error::Invalid("native result is not a task file".into()));
            };
            let id = uuid::Uuid::parse_str(task.as_str()).map_err(storage)?;
            let volumes = self
                .external_volumes(TaskId::from_bytes(id.into_bytes()))
                .await?;
            crate::external_executor::read_native_result(
                name,
                arguments,
                &volumes.requests,
                &volumes.results,
                workspace,
            )
            .await
        })
    }

    /// Publishes and drives one external request through the sole task owner.
    /// Identical retries read the immutable result without repeating an effect.
    pub fn external_request<'a>(
        &'a self,
        request: &'a ExternalRequest,
    ) -> impl Future<Output = Result<Value>> + 'a {
        Box::pin(async move {
            self.external_request_until(request, std::future::pending())
                .await?
                .ok_or_else(|| Error::Storage("native request follow ended".into()))
        })
    }

    /// A passive wait may stop on owner shutdown; an executing effect is always
    /// retained through settlement before shutdown is observed.
    pub(crate) fn external_request_until<'a>(
        &'a self,
        request: &'a ExternalRequest,
        stopped: impl Future<Output = Result<()>> + Send + 'a,
    ) -> impl Future<Output = Result<Option<Value>>> + 'a {
        Box::pin(async move {
            tokio::pin!(stopped);
            if matches!(request.action, ExternalAction::Join { .. }) {
                // Check cached/fenced outcomes before any child side effect.
                // Rejected stops therefore stay rejected after owner restart.
                return crate::external_executor::completion::after_delivery(
                    self.external_step(request),
                    || async {
                        let volumes = self.external_volumes(request.task_id()).await?;
                        let completion = RequestConsumer::child_completion(
                            &self.runtime,
                            &volumes.results,
                            request.task_id(),
                            request,
                        )
                        .await?;
                        // The parent's suspended join retains no task guard.
                        if self
                            .external_follow(&completion, stopped.as_mut())
                            .await?
                            .is_none()
                        {
                            return Ok(None);
                        }
                        self.external_follow(request, stopped.as_mut()).await
                    },
                )
                .await;
            }
            self.external_follow(request, stopped).await
        })
    }

    fn external_follow<'a>(
        &'a self,
        request: &'a ExternalRequest,
        stopped: impl Future<Output = Result<()>> + Send + 'a,
    ) -> impl Future<Output = Result<Option<Value>>> + 'a {
        Box::pin(async move {
            use futures::StreamExt as _;
            crate::external_executor::progress::follow(
                (),
                |cursor| async move { Ok((cursor, self.external_step(request).await?)) },
                async {
                    let changes = self.runtime.task_host().workflow_changes().await?;
                    let changes = changes.map(|record| record.map(|_| ()).map_err(storage));
                    let changes: acyclic_stream::BoxProviderStream<'static, Result<()>> =
                        if matches!(request.action, ExternalAction::RunTool { .. }) {
                            let volumes = self.external_volumes(request.task_id()).await?;
                            let cancel = crate::external_executor::cancellation::prepare(
                                &volumes.requests,
                                request,
                                Component::new(request.task_id().to_string()).map_err(storage)?,
                            )
                            .await?;
                            Box::pin(futures::stream::select(
                                changes,
                                futures::stream::once(async move { cancel.await.map(|_| ()) }),
                            ))
                        } else {
                            Box::pin(changes)
                        };
                    Ok(changes)
                },
                stopped,
            )
            .await
        })
    }

    fn external_step<'a>(
        &'a self,
        request: &'a ExternalRequest,
    ) -> impl Future<Output = Result<Option<Value>>> + 'a {
        Box::pin(async move {
            #[cfg(feature = "native-execution")]
            if let ExternalAction::Attach { workspace } = &request.action {
                if let Some(path) = self.workspace_path() {
                    if std::fs::canonicalize(workspace).map_err(storage)? != path {
                        return Err(Error::Unauthorized(
                            "native workspace differs from the selected checkout".into(),
                        ));
                    }
                }
            }
            let task_id = request.task_id();
            let (_binding, volumes) = self.binding(task_id).await?;
            let (requests, results) = (&volumes.requests, &volumes.results);
            let task = Component::new(task_id.to_string()).map_err(storage)?;
            let file = request.to_file(task.clone()).map_err(storage)?;
            crate::external_executor::publish_request(requests, &file, None, None)
                .await
                .map_err(storage)?;
            let key = request.result_key(task.clone()).map_err(storage)?;
            let cached = results
                .snapshot(&Query::File(key), 1)
                .await
                .map_err(storage)?;
            let publisher = FilesystemContentPublisher::new(
                self.files.clone(),
                &self.journal,
                &self.verifier,
                &self.signed,
                self.limits.file_bytes,
                None,
            )?;
            let commands =
                RequestCommands::new(&self.runtime, requests, results, publisher.as_ref());
            #[cfg(feature = "native-execution")]
            let commands = commands.with_child_admission(self);
            let idle = self.prepare_external_slot(request, requests).await?;
            let initial = matches!(request.action, ExternalAction::Attach { .. });
            let answered = !cached.files.is_empty();
            // An authenticated empty input slot has no retained effect to
            // recover. Admission rechecks its current cut before filling it.
            // Cached answers still repair settlement and projected task files.
            recovery::recover_before_delivery(initial, answered, idle, || {
                Box::pin(self.advance(task_id, &commands))
            })
            .await?;
            if let Some(file) = cached.files.first() {
                return crate::external_executor::volume::decode(&file.bytes)
                    .map(Some)
                    .map_err(storage);
            }
            let started = results
                .snapshot(
                    &Query::File(FileKey::Task {
                        task: task.clone(),
                        file: TaskFile::RequestStarted(
                            Component::new(request.operation_id.to_string()).map_err(storage)?,
                        ),
                    }),
                    1,
                )
                .await
                .map_err(storage)?;
            let workspace = match &request.action {
                ExternalAction::Attach { workspace } => workspace.as_str(),
                _ => "",
            };
            if started.files.is_empty() {
                RequestConsumer::new(
                    &self.runtime,
                    requests,
                    results,
                    publisher.as_ref(),
                    workspace,
                )
                .accept(&task, &file)
                .await?;
                Box::pin(self.advance(task_id, &commands)).await?;
            }
            let snapshot = results
                .snapshot(&Query::File(request.result_key(task).map_err(storage)?), 1)
                .await
                .map_err(storage)?;
            snapshot
                .files
                .first()
                .map(|file| crate::external_executor::volume::decode(&file.bytes).map_err(storage))
                .transpose()
        })
    }

    // Reuse the cancellation cut to identify an empty suspended input slot.
    // workflow_slot excludes cancellation and authenticates this task's state.
    async fn prepare_external_slot(
        &self,
        request: &ExternalRequest,
        requests: &TaskVolume<LocalStream>,
    ) -> Result<bool> {
        if !matches!(request.action, ExternalAction::RunTool { .. }) {
            return Ok(false);
        }
        let Some((_, slot)) = self
            .runtime
            .task_host()
            .workflow_slot(
                request.task_id(),
                Some(crate::scheduler::OperationPhase::Suspended),
                &mut None,
            )
            .await?
        else {
            return Ok(false);
        };
        if slot.waiting_command != Some(request.operation_id) {
            return Ok(slot.is_empty());
        }
        let cancel = crate::external_executor::cancellation::prepare(
            requests,
            request,
            Component::new(request.task_id().to_string()).map_err(storage)?,
        )
        .await?;
        use futures::FutureExt as _;
        if let Some(cancel) = cancel.now_or_never() {
            cancel?; // Verify the exact original host/session/call before waking.
            self.runtime
                .task_host()
                .wake_command(
                    request.task_id(),
                    slot.revision,
                    request.operation_id,
                    &mut None,
                )
                .await?;
        }
        Ok(false)
    }
}
