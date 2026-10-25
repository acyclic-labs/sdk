//! Tool output is replayable task files, published under the current task fence.
use super::*;
use crate::{
    durable_host::TaskJournalOwner,
    task_volume::{Component, FileKey, Generation, TaskFile, TaskFiles, ToolFile, Write},
};
use acyclic_native_runtime::ProcessOutput;
use acyclic_stream::OwnedBytes;

const OUTPUT_BATCH_BYTES: usize = 64 * 1024;

pub(super) struct Writer {
    pub(super) owner: Arc<TaskJournalOwner<LocalStream>>,
    volume: Arc<TaskFiles<acyclic_stream::LocalStream>>,
    generation: Generation,
    task: Component,
    call: Component,
    sequence: (u64, u64),
    pending: (Vec<u8>, Vec<u8>),
    batch_bytes: (usize, usize),
    step: u64,
    runtime: tokio::runtime::Handle,
}

impl Writer {
    pub(super) fn pending(&self) -> bool {
        !self.pending.0.is_empty() || !self.pending.1.is_empty()
    }

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
        let batch_bytes = (
            batch_bytes(&task, &call, turn, ToolFile::Stdout(u64::MAX))?,
            batch_bytes(&task, &call, turn, ToolFile::Stderr(u64::MAX))?,
        );
        let mut output = Self {
            owner,
            volume,
            generation,
            task,
            call,
            sequence: (0, 0),
            pending: (Vec::new(), Vec::new()),
            batch_bytes,
            step: turn,
            runtime: tokio::runtime::Handle::current(),
        };
        output
            .publish(ToolFile::Request, request.into(), false)
            .await?;
        Ok(output)
    }

    async fn publish(&mut self, file: ToolFile, bytes: OwnedBytes, settlement: bool) -> Result<()> {
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
        let (stderr, mut bytes) = match chunk {
            ProcessOutput::Stdout(bytes) => (false, bytes),
            ProcessOutput::Stderr(bytes) => (true, bytes),
        };
        while !bytes.is_empty() {
            let maximum = if stderr {
                self.batch_bytes.1
            } else {
                self.batch_bytes.0
            };
            let pending = if stderr {
                &mut self.pending.1
            } else {
                &mut self.pending.0
            };
            let amount = bytes.len().min(maximum - pending.len());
            let (prefix, rest) = bytes.split_at(amount);
            pending.try_reserve(amount).map_err(std::io::Error::other)?;
            pending.extend_from_slice(prefix);
            bytes = rest;
            if pending.len() == maximum {
                let runtime = self.runtime.clone();
                await_fence(&runtime, || async {
                    self.flush(stderr, false)
                        .await
                        .map_err(std::io::Error::other)
                })?;
            }
        }
        Ok(())
    }

    // Retain the bytes and sequence until publication succeeds, including when
    // an unrelated task-file writer advances the volume generation.
    pub(super) async fn flush(&mut self, stderr: bool, settlement: bool) -> Result<()> {
        let (pending, sequence) = if stderr {
            (&self.pending.1, self.sequence.1)
        } else {
            (&self.pending.0, self.sequence.0)
        };
        if pending.is_empty() {
            return Ok(());
        }
        let next = sequence
            .checked_add(1)
            .ok_or_else(|| Error::Storage("tool output sequence overflow".into()))?;
        let file = if stderr {
            ToolFile::Stderr(sequence)
        } else {
            ToolFile::Stdout(sequence)
        };
        self.publish(file, pending.clone().into(), settlement)
            .await?;
        if stderr {
            self.pending.1.clear();
            self.sequence.1 = next;
        } else {
            self.pending.0.clear();
            self.sequence.0 = next;
        }
        Ok(())
    }

    pub(super) async fn finish(&mut self, result: &ToolResult) -> Result<()> {
        // These bytes were captured before command cleanup. Settle them under
        // the original owner even when cancellation stopped further execution.
        self.flush(false, true).await?;
        self.flush(true, true).await?;
        let outcome = crate::local::tool_metadata::encode(
            result
                .value
                .as_object()
                .ok_or_else(|| Error::Invalid("process result must be an object".into()))?,
        )?;
        self.publish(ToolFile::Result, outcome.into(), true).await
    }
}

// The stream limit includes the task-file envelope. Reserve its largest
// sequence spelling once, so every subsequent batch fits without a raised cap.
fn batch_bytes(task: &Component, call: &Component, turn: u64, file: ToolFile) -> Result<usize> {
    let envelope = Write::new(
        FileKey::Task {
            task: task.clone(),
            file: TaskFile::Tool {
                turn,
                call: call.clone(),
                file,
            },
        },
        Vec::new(),
    );
    acyclic_stream::MAX_RECORD_BYTES
        .checked_sub(crate::task_volume::record_bytes(&envelope)?)
        .filter(|maximum| *maximum != 0)
        .map(|maximum| maximum.min(OUTPUT_BATCH_BYTES))
        .ok_or_else(|| Error::Storage("tool output envelope exceeds stream record limit".into()))
}
