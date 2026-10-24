//! Tool output is replayable task files, published under the current task fence.
use super::*;
use crate::{
    durable_host::TaskJournalOwner,
    task_volume::{Component, FileKey, Generation, TaskFile, TaskFiles, ToolFile, Write},
};
use acyclic_native_runtime::ProcessOutput;
use acyclic_stream::OwnedBytes;

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod tests;

/// The native contract's bound on one host control check. Checkout checks
/// never apply it as a wall-clock deadline: each completes when the journal
/// answers or the task fence is lost, so host load can delay a successful
/// command but never fail it.
pub(super) const CONTROL_TIMEOUT_MS: u32 = 1000;

/// Waits, from a native capture worker, for a fenced journal step: an output
/// publication or an owner check. It completes when the journal answers or
/// reports the fence lost, never on a wall-clock deadline.
pub(super) fn await_fence<T, F: std::future::Future<Output = std::io::Result<T>>>(
    runtime: &tokio::runtime::Handle,
    step: impl FnOnce() -> F,
) -> std::io::Result<T> {
    crate::filesystem::native_process::capture::wait_control(runtime, step)
}

pub(super) struct Output {
    pub(super) owner: Arc<TaskJournalOwner<LocalStream>>,
    volume: Arc<TaskFiles<acyclic_stream::LocalStream>>,
    generation: Generation,
    task: Component,
    call: Component,
    sequence: (u64, u64),
    step: u64,
    runtime: tokio::runtime::Handle,
}

pub(super) struct SearchOutput(AsyncMutex<(Output, usize)>);

impl SearchOutput {
    pub(super) fn new(output: Output) -> Self {
        Self(AsyncMutex::new((output, 0)))
    }

    pub(super) async fn finish(&self, result: &ToolResult) -> Result<()> {
        self.0.lock().await.0.finish(result).await
    }
}

impl crate::tools::git::SearchProgress for SearchOutput {
    fn publish<'a>(
        &'a self,
        progress: &'a crate::tools::git::GrepProgress,
    ) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut state = self.0.lock().await;
            let (output, matches_from) = &mut *state;
            // A matching line appears once in the stdout stream. Repeating
            // all accumulated matches for every metadata page would multiply
            // output by the size of a huge nonmatching remainder.
            let mut bytes = serde_json::to_vec(&serde_json::json!({
                "matches_from": *matches_from,
                "matches": &progress.matches[*matches_from..],
                "files_examined": progress.files_examined,
                "bytes_read": progress.bytes_read,
                "pending_directories": progress.pending_directories,
                "complete": progress.complete,
                "truncated": progress.truncated,
            }))
            .map_err(storage)?;
            bytes.push(b'\n');
            output.stdout(bytes.into()).await?;
            *matches_from = progress.matches.len();
            Ok(())
        })
    }
}

impl Output {
    pub(super) async fn open(
        context: &ToolContext,
        volume: Arc<TaskFiles<acyclic_stream::LocalStream>>,
        request: Vec<u8>,
    ) -> Result<Self> {
        let owner = context.task().ordinary_journal_owner::<LocalStream>()?;
        let task = Component::new(owner.task_id().to_string())?;
        // Runtime operation IDs are canonical and unique across model turns.
        // The request retains the provider's call_id unchanged.
        let call = Component::new(context.operation_id().to_string())?;
        let turn = volume
            .inner
            .model_turn(
                task.clone(),
                Component::new(context.model_turn.0.to_string())?,
                context.model_turn.1,
                None,
            )
            .await?;
        let generation = volume.inner.head().await?;
        let mut output = Self {
            owner,
            volume,
            generation,
            task,
            call,
            sequence: (0, 0),
            step: turn,
            runtime: tokio::runtime::Handle::current(),
        };
        output.publish(ToolFile::Request, request.into()).await?;
        Ok(output)
    }

    async fn publish(&mut self, file: ToolFile, bytes: OwnedBytes) -> Result<()> {
        let settlement = matches!(file, ToolFile::Result);
        let write = Write::new(
            FileKey::Task {
                task: self.task.clone(),
                file: TaskFile::Tool {
                    turn: self.step,
                    call: self.call.clone(),
                    file,
                },
            },
            bytes,
        );
        let owner = self.owner.as_ref();
        self.generation = crate::task_volume::publish(
            &self.volume.inner,
            self.generation,
            &write,
            || async move {
                let condition = owner
                    .condition(
                        if settlement {
                            crate::distributed::JournalWrite::Settlement
                        } else {
                            crate::distributed::JournalWrite::Fresh
                        },
                        &mut None,
                    )
                    .await?;
                Ok::<_, crate::Error>((
                    vec![condition],
                    if settlement {
                        None
                    } else {
                        owner.deadline_epoch_ms()
                    },
                ))
            },
        )
        .await?;
        Ok(())
    }

    pub(super) fn observe(&mut self, chunk: ProcessOutput<'_>) -> std::io::Result<()> {
        let (file, bytes, mut next) = match chunk {
            ProcessOutput::Stdout(bytes) => {
                (ToolFile::Stdout(self.sequence.0), bytes, self.sequence)
            }
            ProcessOutput::Stderr(bytes) => {
                (ToolFile::Stderr(self.sequence.1), bytes, self.sequence)
            }
        };
        let sequence = match &file {
            ToolFile::Stdout(_) => &mut next.0,
            _ => &mut next.1,
        };
        *sequence = sequence
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("tool output sequence overflow"))?;
        // The chunk is published when the journal accepts it under the task
        // fence; a lost fence fails the publication. No wall-clock deadline
        // applies, so a loaded host delays the command's output, never fails it.
        let runtime = self.runtime.clone();
        await_fence(&runtime, || async {
            self.publish(file, bytes.to_vec().into())
                .await
                .map_err(std::io::Error::other)
        })?;
        self.sequence = next;
        Ok(())
    }

    async fn stdout(&mut self, bytes: OwnedBytes) -> Result<()> {
        for chunk in bytes.chunks(64 * 1024) {
            let next = self
                .sequence
                .0
                .checked_add(1)
                .ok_or_else(|| Error::Storage("tool output sequence overflow".into()))?;
            self.publish(ToolFile::Stdout(self.sequence.0), chunk.to_vec().into())
                .await?;
            self.sequence.0 = next;
        }
        Ok(())
    }

    pub(super) async fn finish(&mut self, result: &ToolResult) -> Result<()> {
        self.publish(
            ToolFile::Result,
            serde_json::to_vec(&result.value).map_err(storage)?.into(),
        )
        .await
    }
}
