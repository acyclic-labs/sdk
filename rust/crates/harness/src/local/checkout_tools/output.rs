//! Tool output is replayable task files, published under the current task fence.
use super::*;
use crate::{
    durable_host::TaskJournalOwner,
    task_volume::{Component, FileKey, Generation, Query, TaskFile, TaskVolume, ToolFile, Write},
};
use acyclic_native_runtime::ProcessOutput;
use acyclic_stream::OwnedBytes;

pub(super) const CONTROL_TIMEOUT_MS: u32 = 1000;

pub(super) struct Output {
    pub(super) owner: Arc<TaskJournalOwner<LocalStream>>,
    volume: Arc<TaskVolume<LocalStream>>,
    generation: Generation,
    task: Component,
    call: Component,
    sequence: (u64, u64),
    step: u64,
    runtime: tokio::runtime::Handle,
}

impl Output {
    pub(super) async fn open(
        context: &ToolContext,
        volume: Arc<TaskVolume<LocalStream>>,
        request: Vec<u8>,
    ) -> Result<Self> {
        let owner = context.task().ordinary_journal_owner::<LocalStream>()?;
        let task = Component::new(owner.task_id().to_string())?;
        // Runtime operation IDs are canonical and unique across model turns.
        // The request retains the provider's call_id unchanged.
        let call = Component::new(context.operation_id().to_string())?;
        let generation = volume
            .snapshot(
                &Query::Tool {
                    task: task.clone(),
                    turn: u64::from(context.model_step),
                    call: call.clone(),
                },
                0,
            )
            .await?
            .generation;
        let mut output = Self {
            owner,
            volume,
            generation,
            task,
            call,
            sequence: (0, 0),
            step: u64::from(context.model_step),
            runtime: tokio::runtime::Handle::current(),
        };
        output.publish(ToolFile::Request, request.into()).await?;
        Ok(output)
    }

    async fn publish(&mut self, file: ToolFile, bytes: OwnedBytes) -> Result<()> {
        let condition = self
            .owner
            .condition(crate::distributed::JournalWrite::Fresh, &mut None)
            .await?;
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
        self.generation = self
            .volume
            .append_if(
                self.generation,
                &write,
                vec![condition],
                self.owner.deadline_epoch_ms(),
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
        let runtime = self.runtime.clone();
        runtime.block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_millis(u64::from(CONTROL_TIMEOUT_MS)),
                self.publish(file, bytes.to_vec().into()),
            )
            .await
            .map_err(std::io::Error::other)?
            .map_err(std::io::Error::other)
        })?;
        self.sequence = next;
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
