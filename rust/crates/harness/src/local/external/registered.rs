//! Registered children use the same worker, task gate and provider follows.
use super::*;
use crate::runtime::TaskStateProvider as _;
use futures::StreamExt as _;

impl LocalHarness {
    pub(crate) async fn external_child(
        &self,
        request: &ExternalRequest,
        result: &Value,
    ) -> Result<()> {
        if !matches!(&request.action, ExternalAction::RunTool {name, ..} if name == "acyclic_spawn")
        {
            return Ok(());
        }
        let Some(value) = result.get("Succeeded") else {
            return Ok(());
        };
        let child = TaskId::parse(
            value
                .as_str()
                .ok_or_else(|| Error::Invalid("spawn result is not a task ID".into()))?,
        )?;
        let admission = self.runtime.task_host().observe_admission(child).await?;
        if admission.parent != Some(request.task_id()) {
            return Err(Error::Unauthorized(
                "spawn result is not this task's child".into(),
            ));
        }
        // A replay of the immutable spawn result cannot make a terminal child
        // accept another attachment. Its request subtree already exists; the
        // provider's children replay discovers it independently after restart.
        if self.runtime.task_host().outcome(child).await?.is_some() {
            return Ok(());
        }
        // Opening the child's provider-backed task subtree is idempotent. The
        // owner's existing children follow discovers it, including after reopen.
        // No parent task gate is held while acquiring this child's binding.
        self.external_volumes(child).await?;
        #[cfg(feature = "native-execution")]
        if crate::external_executor::launch::requested(request) {
            let spawn = crate::external_executor::launch::Spawn::from_request(request)?;
            let workspace = self
                .workspace_path()
                .and_then(Path::to_str)
                .ok_or_else(|| {
                    Error::Unsupported("native child has no workspace binding".into())
                })?;
            let attach = spawn.attachment(request, workspace)?;
            if attach.task_id() != child {
                return Err(Error::Unauthorized(
                    "native admission selected another session".into(),
                ));
            }
            self.external_request(&attach).await?;
            self.discover_external_children(child).await?;
        }
        Ok(())
    }

    pub(crate) fn registered_until<'a>(
        &'a self,
        task: TaskId,
        stopped: impl Future<Output = Result<()>> + Send + 'a,
    ) -> impl Future<Output = Result<bool>> + 'a {
        Box::pin(async move {
            let admission = match self.runtime.task_host().observe_admission(task).await {
                Ok(admission) => admission,
                // A native client's first request stream precedes admission.
                Err(Error::NotFound(_)) => return Ok(false),
                Err(error) => return Err(error),
            };
            if admission.task.name == "acyclic.command_session"
                && admission
                    .input
                    .pointer("/command/kind")
                    .and_then(Value::as_str)
                    == Some(crate::external_executor::LIFECYCLE)
            {
                return Ok(false);
            }
            #[cfg(all(feature = "native-execution", feature = "hooks"))]
            let native_parent = if admission
                .input
                .pointer("/command/kind")
                .and_then(Value::as_str)
                == Some(crate::external_executor::launch::PROCESS_COMMAND)
            {
                Some(self.native_parent(&admission).await?)
            } else {
                None
            };
            #[cfg(not(all(feature = "native-execution", feature = "hooks")))]
            let native_parent: Option<TaskId> = None;
            drop(admission);
            let cancellation = acyclic_fs::CancellationToken::new();
            let progress = crate::external_executor::progress::follow(
                None,
                |revision| {
                    self.registered_step(task, revision, native_parent.map(|_| &cancellation))
                },
                async {
                    Ok(self
                        .runtime
                        .task_host()
                        .workflow_changes()
                        .await?
                        .map(|record| record.map(|_| ()).map_err(storage)))
                },
                async {
                    cancellation.cancelled().await;
                    Ok(())
                },
            );
            let stopped = async {
                let result = stopped.await;
                if let Some(parent) = native_parent {
                    // The custody child and any outstanding hook tools belong
                    // to this same subtree. Drain them before provider closure.
                    self.runtime.task_host().cancel(parent).await?;
                }
                result
            };
            let progress =
                crate::external_executor::cancellation::settle(progress, stopped, || {
                    cancellation.cancel();
                })
                .await;
            #[cfg(all(feature = "native-execution", feature = "hooks"))]
            if let Some(parent) = native_parent {
                self.finish_native_process(task, parent).await?;
            }
            progress?;
            Ok(true)
        })
    }

    fn registered_step<'a>(
        &'a self,
        task: TaskId,
        previous: Option<u64>,
        cancellation: Option<&'a acyclic_fs::CancellationToken>,
    ) -> impl Future<Output = Result<(Option<u64>, Option<()>)>> + 'a {
        Box::pin(async move {
            let (revision, terminal) = {
                let (_binding, volumes) = self.binding(task).await?;
                let revision = self
                    .runtime
                    .task_host()
                    .idle_workflow(task)
                    .await?
                    .map(|idle| idle.operation_revision);
                if revision.is_some() && previous == revision {
                    return Ok((previous, None));
                }
                let (cursor, terminal) =
                    crate::external_executor::progress::at_revision(previous, revision, || async {
                        #[cfg(all(feature = "native-execution", feature = "hooks"))]
                        if let Some(cancellation) = cancellation {
                            self.advance_process(task, volumes, cancellation).await?;
                            return self.runtime.task_host().outcome(task).await;
                        }
                        let _ = (&volumes, &cancellation);
                        if self.model.is_some() {
                            let commands = self.commands()?;
                            self.advance(task, &commands).await?;
                        } else {
                            self.advance(task, &self.runtime.commands()).await?;
                        }
                        self.runtime.task_host().outcome(task).await
                    })
                    .await?;
                (cursor, terminal)
            };
            // A registered child can admit its own children. Discover these
            // from the durable hierarchy after releasing its task gate.
            self.discover_external_children(task).await?;
            Ok((
                revision,
                match terminal {
                    Some(
                        crate::Outcome::Succeeded(_)
                        | crate::Outcome::Failed { .. }
                        | crate::Outcome::Cancelled,
                    ) => Some(()),
                    _ => None,
                },
            ))
        })
    }

    async fn discover_external_children(&self, task: TaskId) -> Result<()> {
        let mut after = None;
        loop {
            let children = self
                .runtime
                .harness()
                .children(task, None, after, 64)
                .await?;
            for child in children.entries {
                self.external_volumes(child.task_id).await?;
            }
            after = children.next_after;
            if after.is_none() {
                return Ok(());
            }
        }
    }
}

#[cfg(all(test, feature = "test-support", not(acyclic_harness_loom)))]
mod tests;
